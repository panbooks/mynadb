//! Union TableProvider Implementation
//!
//! Provides a TableProvider that combines results from multiple underlying providers.
//! This is used to merge cached (memtable) data with persisted (Parquet) data,
//! enabling immediate query visibility for fresh writes.
//!
//! ## Architecture
//!
//! ```text
//! Query
//!   ↓
//! UnionTableProvider
//!   ├── MemtableProvider (fresh writes)
//!   └── ParquetTableProvider (persisted data)
//!   ↓
//! UnionExec (combines both streams)
//!   ↓
//! Merged Results
//! ```

use async_trait::async_trait;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{project_schema, DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::ExecutionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tracing::debug;

/// A TableProvider that combines results from two underlying providers
///
/// This is used to merge memtable (cached) data with Parquet (persisted) data.
/// The union is performed at query time, providing a unified view of all data.
pub struct UnionTableProvider {
    /// First provider (typically MemtableProvider for fresh data)
    primary: Arc<dyn TableProvider>,
    /// Second provider (typically ParquetTableProvider for persisted data)
    secondary: Arc<dyn TableProvider>,
    /// Merged schema from both providers
    schema: SchemaRef,
    /// Table name for logging
    table_name: String,
}

impl fmt::Debug for UnionTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnionTableProvider")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl UnionTableProvider {
    /// Create a new UnionTableProvider
    ///
    /// # Arguments
    /// * `primary` - First provider (e.g., MemtableProvider)
    /// * `secondary` - Second provider (e.g., ParquetTableProvider)
    /// * `table_name` - Full table name for logging
    pub fn new(
        primary: Arc<dyn TableProvider>,
        secondary: Arc<dyn TableProvider>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        // Merge schemas from both providers
        let schema = merge_schemas(primary.schema(), secondary.schema())?;

        Ok(Self {
            primary,
            secondary,
            schema: Arc::new(schema),
            table_name: table_name.into(),
        })
    }

    /// Create with explicit schema (when you already know the merged schema)
    pub fn new_with_schema(
        primary: Arc<dyn TableProvider>,
        secondary: Arc<dyn TableProvider>,
        schema: SchemaRef,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            primary,
            secondary,
            schema,
            table_name: table_name.into(),
        }
    }
}

/// Merge two schemas, taking the union of all fields
///
/// Fields with the same name must have compatible types.
/// The result schema contains all fields from both schemas.
/// Dictionary-encoded types are unwrapped to their value type (e.g., Dictionary(Int32, Utf8) → Utf8)
/// to ensure downstream operators (aggregation, GROUP BY) work correctly.
fn merge_schemas(schema1: SchemaRef, schema2: SchemaRef) -> Result<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Add all fields from schema1, unwrapping Dictionary types
    for field in schema1.fields() {
        fields.push(unwrap_dictionary_field(field.as_ref()));
        seen_names.insert(field.name().clone());
    }

    // Add fields from schema2 that aren't in schema1
    for field in schema2.fields() {
        if !seen_names.contains(field.name()) {
            // Field not in schema1, add it (but make it nullable since it might not exist in all sources)
            let mut new_field = unwrap_dictionary_field(field.as_ref());
            // Make nullable since this field only exists in one source
            if !new_field.is_nullable() {
                new_field = Field::new(new_field.name(), new_field.data_type().clone(), true);
            }
            fields.push(new_field);
            seen_names.insert(field.name().clone());
        }
        // If field exists in both, we use schema1's definition (already added)
    }

    Ok(Schema::new(fields))
}

/// Unwrap a Dictionary-encoded field to its value type.
/// E.g., Dictionary(Int32, Utf8) → Utf8
pub(super) fn unwrap_dictionary_field(field: &Field) -> Field {
    match field.data_type() {
        DataType::Dictionary(_, value_type) => {
            Field::new(field.name(), value_type.as_ref().clone(), field.is_nullable())
        }
        _ => field.clone(),
    }
}

/// Cast a RecordBatch's columns to match the expected output schema.
///
/// Parquet readers may return Dictionary-encoded strings (Dictionary(Int32, Utf8))
/// while memtable data uses plain Utf8. This function casts any mismatched columns
/// so downstream operators (aggregation, GROUP BY) don't panic.
pub(super) fn normalize_batch(
    batch: &datafusion::arrow::array::RecordBatch,
    target_schema: &SchemaRef,
) -> Result<datafusion::arrow::array::RecordBatch> {
    let batch_schema = batch.schema();

    // Fast path: schemas already match
    if batch_schema.fields().len() == target_schema.fields().len()
        && batch_schema
            .fields()
            .iter()
            .zip(target_schema.fields().iter())
            .all(|(a, b)| a.data_type() == b.data_type())
    {
        return Ok(batch.clone());
    }

    let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            if i < target_schema.fields().len() {
                let target_type = target_schema.field(i).data_type();
                if col.data_type() != target_type {
                    // Cast Dictionary → primitive, or any other compatible cast
                    cast(col.as_ref(), target_type).unwrap_or_else(|_| col.clone())
                } else {
                    col.clone()
                }
            } else {
                col.clone()
            }
        })
        .collect();

    datafusion::arrow::array::RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(e, None))
}

#[async_trait]
impl TableProvider for UnionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Return inexact for all filters - both underlying providers will apply them
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Compute the projected schema
        let projected_schema = project_schema(&self.schema, projection)?;

        debug!(
            "UnionTableProvider scan: table={}, projection={:?}",
            self.table_name, projection
        );

        // Get execution plans from both providers
        let primary_plan = self.primary.scan(state, projection, filters, limit).await?;
        let secondary_plan = self
            .secondary
            .scan(state, projection, filters, limit)
            .await?;

        // Create union execution plan
        let exec = UnionExec::new(
            primary_plan,
            secondary_plan,
            projected_schema,
            self.table_name.clone(),
        );

        Ok(Arc::new(exec))
    }
}

/// Execution plan that unions results from two child plans
#[derive(Debug)]
pub struct UnionExec {
    /// First child plan (e.g., MemtableExec)
    primary: Arc<dyn ExecutionPlan>,
    /// Second child plan (e.g., ParquetExec)
    secondary: Arc<dyn ExecutionPlan>,
    /// Output schema
    schema: SchemaRef,
    /// Table name for display
    table_name: String,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
}

impl UnionExec {
    pub fn new(
        primary: Arc<dyn ExecutionPlan>,
        secondary: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        table_name: String,
    ) -> Self {
        // Total partitions = primary partitions + secondary partitions
        // But we'll use single partition and stream both
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            primary,
            secondary,
            schema,
            table_name,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }
}

impl DisplayAs for UnionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnionExec: table={}", self.table_name)
            }
        }
    }
}

impl ExecutionPlan for UnionExec {
    fn name(&self) -> &str {
        "UnionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.primary, &self.secondary]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(DataFusionError::Internal(
                "UnionExec requires exactly 2 children".to_string(),
            ));
        }

        Ok(Arc::new(UnionExec::new(
            children[0].clone(),
            children[1].clone(),
            self.schema.clone(),
            self.table_name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "UnionExec execute: table={}, partition={}",
            self.table_name, partition
        );

        // Execute both child plans
        let primary_stream = self.primary.execute(0, context.clone())?;
        let secondary_stream = self.secondary.execute(0, context)?;

        let schema = self.schema.clone();

        // Use select_all to interleave both streams (order doesn't matter for union)
        let streams = vec![primary_stream, secondary_stream];
        let combined = futures::stream::select_all(streams);

        // Normalize each batch so column types match the declared output schema.
        // Parquet readers may return Dictionary(Int32, Utf8) for string columns while
        // memtable uses plain Utf8 — without casting, downstream operators panic.
        let normalize_schema = schema.clone();
        let normalized = futures::StreamExt::map(combined, move |batch_result| {
            batch_result.and_then(|batch| normalize_batch(&batch, &normalize_schema))
        });

        let stream = RecordBatchStreamAdapter::new(schema, normalized);

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn test_merge_schemas_identical() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let schema2 = schema1.clone();

        let merged = merge_schemas(schema1, schema2).unwrap();
        assert_eq!(merged.fields().len(), 2);
    }

    #[test]
    fn test_merge_schemas_different() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let merged = merge_schemas(schema1, schema2).unwrap();
        assert_eq!(merged.fields().len(), 3);
        assert!(merged.field_with_name("id").is_ok());
        assert!(merged.field_with_name("name").is_ok());
        assert!(merged.field_with_name("value").is_ok());
    }

    #[test]
    fn test_merge_schemas_dictionary_unwrap() {
        // Parquet schema with Dictionary-encoded string column
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        // Memtable schema with plain Utf8
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));

        let merged = merge_schemas(schema1, schema2).unwrap();
        assert_eq!(merged.fields().len(), 2);
        // Dictionary should be unwrapped to Utf8
        assert_eq!(merged.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_merge_schemas_both_dictionary() {
        // Both schemas have Dictionary encoding
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tag", dict_type.clone(), true),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tag", dict_type, true),
        ]));

        let merged = merge_schemas(schema1, schema2).unwrap();
        // Both should be unwrapped to Utf8
        assert_eq!(merged.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_unwrap_dictionary_field() {
        let dict_field = Field::new(
            "host",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let unwrapped = unwrap_dictionary_field(&dict_field);
        assert_eq!(unwrapped.name(), "host");
        assert_eq!(unwrapped.data_type(), &DataType::Utf8);
        assert!(unwrapped.is_nullable());

        // Non-dictionary field passes through unchanged
        let plain = Field::new("value", DataType::Float64, false);
        let unchanged = unwrap_dictionary_field(&plain);
        assert_eq!(unchanged.data_type(), &DataType::Float64);
        assert!(!unchanged.is_nullable());
    }

    #[test]
    fn test_normalize_batch_dictionary_to_utf8() {
        // Target schema expects plain Utf8
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));

        // Batch has Dictionary-encoded column (as Parquet reader returns)
        let ids = Int64Array::from(vec![1, 2, 3]);
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values = StringArray::from(vec!["server-a", "server-b"]);
        let dict_array = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

        let dict_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            dict_schema,
            vec![Arc::new(ids), Arc::new(dict_array)],
        )
        .unwrap();

        let normalized = normalize_batch(&batch, &target_schema).unwrap();
        assert_eq!(normalized.schema(), target_schema);
        assert_eq!(normalized.column(1).data_type(), &DataType::Utf8);

        // Verify values survived the cast
        let host_col = normalized
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host_col.value(0), "server-a");
        assert_eq!(host_col.value(1), "server-b");
        assert_eq!(host_col.value(2), "server-a");
    }

    #[test]
    fn test_normalize_batch_already_matching() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ids = Int64Array::from(vec![1, 2]);
        let names = StringArray::from(vec![Some("a"), Some("b")]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])
                .unwrap();

        // Should return clone without any casting
        let result = normalize_batch(&batch, &schema).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_normalize_batch_multiple_dict_columns() {
        // Telegraf-like schema with multiple Dictionary-encoded tag columns
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dict_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", dict_type.clone(), true),
            Field::new("service", dict_type, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let times = Int64Array::from(vec![1000, 2000]);
        let host_keys = Int32Array::from(vec![0, 0]);
        let host_values = StringArray::from(vec!["myhost"]);
        let host_dict = DictionaryArray::try_new(host_keys, Arc::new(host_values)).unwrap();
        let svc_keys = Int32Array::from(vec![0, 1]);
        let svc_values = StringArray::from(vec!["web", "db"]);
        let svc_dict = DictionaryArray::try_new(svc_keys, Arc::new(svc_values)).unwrap();
        let values = datafusion::arrow::array::Float64Array::from(vec![1.5, 2.5]);

        let batch = RecordBatch::try_new(
            dict_schema,
            vec![
                Arc::new(times),
                Arc::new(host_dict),
                Arc::new(svc_dict),
                Arc::new(values),
            ],
        )
        .unwrap();

        let normalized = normalize_batch(&batch, &target_schema).unwrap();
        assert_eq!(normalized.schema(), target_schema);

        let host_col = normalized
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host_col.value(0), "myhost");
        assert_eq!(host_col.value(1), "myhost");

        let svc_col = normalized
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(svc_col.value(0), "web");
        assert_eq!(svc_col.value(1), "db");
    }
}
