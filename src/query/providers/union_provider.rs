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
use datafusion::arrow::array::new_null_array;
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

/// Remap projection indices from the union schema to a child's schema.
///
/// The union schema may have columns that only exist in one child provider.
/// This maps by column name to produce valid projection indices for the child.
pub(super) fn remap_projection_for_child(
    union_schema: &SchemaRef,
    child_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<usize>> {
    // Determine which column names are being projected from the union schema
    let projected_names: Vec<&str> = match projection {
        Some(indices) => indices
            .iter()
            .map(|&i| union_schema.field(i).name().as_str())
            .collect(),
        None => union_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect(),
    };

    // Empty projection (e.g., count(*)) — propagate as empty to avoid unnecessary full scans
    if projected_names.is_empty() {
        return Some(vec![]);
    }

    // Map those names to indices in the child's schema
    let child_indices: Vec<usize> = projected_names
        .iter()
        .filter_map(|name| child_schema.index_of(name).ok())
        .collect();

    if child_indices.is_empty() {
        None // No matching columns — let child return whatever it has
    } else {
        Some(child_indices)
    }
}

/// Align a RecordBatch to match the target schema by column name.
///
/// Handles three cases per column:
/// - Column exists with correct type → use as-is
/// - Column exists with different type (e.g., Dictionary → Utf8) → cast
/// - Column missing → fill with nulls
///
/// This is essential because child providers in a union may have different schemas
/// (e.g., memtable has all columns, parquet may have a subset).
pub(super) fn align_batch_to_schema(
    batch: &datafusion::arrow::array::RecordBatch,
    target_schema: &SchemaRef,
) -> Result<datafusion::arrow::array::RecordBatch> {
    let batch_schema = batch.schema();

    // Fast path: schemas are identical
    if *batch_schema == **target_schema {
        return Ok(batch.clone());
    }

    let num_rows = batch.num_rows();
    let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = target_schema
        .fields()
        .iter()
        .map(|field| {
            match batch_schema.index_of(field.name()) {
                Ok(idx) => {
                    let col = batch.column(idx);
                    if col.data_type() != field.data_type() {
                        // Cast type (e.g., Dictionary(Int32, Utf8) → Utf8)
                        cast(col.as_ref(), field.data_type()).unwrap_or_else(|_| col.clone())
                    } else {
                        col.clone()
                    }
                }
                Err(_) => {
                    // Column not in batch — fill with nulls
                    new_null_array(field.data_type(), num_rows)
                }
            }
        })
        .collect();

    // Use try_new_with_options to handle 0-column projections (e.g., count(*))
    // where RecordBatch::try_new would fail because it can't infer row count from 0 columns
    let options = datafusion::arrow::array::RecordBatchOptions::new()
        .with_row_count(Some(num_rows));
    datafusion::arrow::array::RecordBatch::try_new_with_options(
        target_schema.clone(),
        columns,
        &options,
    )
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
        // Compute the projected schema (based on union schema)
        let projected_schema = project_schema(&self.schema, projection)?;

        // Remap projection indices from union schema to each child's schema.
        // The union schema may have columns that only exist in one child,
        // so we map by column name to get valid indices for each child.
        let primary_proj =
            remap_projection_for_child(&self.schema, &self.primary.schema(), projection);
        let secondary_proj =
            remap_projection_for_child(&self.schema, &self.secondary.schema(), projection);

        debug!(
            "UnionTableProvider scan: table={}, union_projection={:?}, primary_proj={:?}, secondary_proj={:?}",
            self.table_name, projection, primary_proj, secondary_proj
        );

        // Get execution plans from both providers with remapped projections
        let primary_plan = self
            .primary
            .scan(state, primary_proj.as_ref(), filters, limit)
            .await?;
        let secondary_plan = self
            .secondary
            .scan(state, secondary_proj.as_ref(), filters, limit)
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

        // Align each batch to the union's output schema by column name.
        // Child providers may have different schemas (different columns, different types).
        // This handles: missing columns (null-filled), type mismatches (cast), reordering.
        let align_schema = schema.clone();
        let normalized = futures::StreamExt::map(combined, move |batch_result| {
            batch_result.and_then(|batch| align_batch_to_schema(&batch, &align_schema))
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
    fn test_align_batch_to_schema_dictionary_to_utf8() {
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

        let normalized = align_batch_to_schema(&batch, &target_schema).unwrap();
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
    fn test_align_batch_to_schema_already_matching() {
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
        let result = align_batch_to_schema(&batch, &schema).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_align_batch_to_schema_multiple_dict_columns() {
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

        let normalized = align_batch_to_schema(&batch, &target_schema).unwrap();
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

    #[test]
    fn test_remap_projection_for_child() {
        // Union schema: [time, host, usage_idle, date, node_id]
        let union_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("usage_idle", DataType::Float64, true),
            Field::new("date", DataType::Utf8, true),
            Field::new("node_id", DataType::Utf8, true),
        ]));

        // Child schema only has [date, node_id] (parquet with no data files)
        let child_schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Utf8, true),
            Field::new("node_id", DataType::Utf8, true),
        ]));

        // SELECT * → projection None → should remap to child's [date(0), node_id(1)]
        let remapped = remap_projection_for_child(&union_schema, &child_schema, None);
        assert_eq!(remapped, Some(vec![0, 1]));

        // SELECT time, host → projection [0, 1] → no matching columns in child
        let remapped = remap_projection_for_child(&union_schema, &child_schema, Some(&vec![0, 1]));
        assert_eq!(remapped, None);

        // SELECT time, date → projection [0, 3] → only date matches at child index 0
        let remapped = remap_projection_for_child(&union_schema, &child_schema, Some(&vec![0, 3]));
        assert_eq!(remapped, Some(vec![0]));

        // Full child has all columns — should pass through correctly
        let full_child = union_schema.clone();
        let remapped = remap_projection_for_child(&union_schema, &full_child, Some(&vec![0, 2, 4]));
        assert_eq!(remapped, Some(vec![0, 2, 4]));
    }

    #[test]
    fn test_align_batch_missing_columns_filled_with_nulls() {
        // Target schema wants [time, host, usage_idle, date]
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("usage_idle", DataType::Float64, true),
            Field::new("date", DataType::Utf8, true),
        ]));

        // Batch only has [date, time] (parquet with minimal columns, different order)
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Utf8, true),
            Field::new("time", DataType::Int64, false),
        ]));
        let dates = StringArray::from(vec![Some("2024-01-01"), Some("2024-01-02")]);
        let times = Int64Array::from(vec![1000, 2000]);
        let batch =
            RecordBatch::try_new(batch_schema, vec![Arc::new(dates), Arc::new(times)]).unwrap();

        let aligned = align_batch_to_schema(&batch, &target_schema).unwrap();
        assert_eq!(aligned.schema(), target_schema);
        assert_eq!(aligned.num_rows(), 2);
        assert_eq!(aligned.num_columns(), 4);

        // time should come from batch (mapped by name, not position)
        let time_col = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(time_col.value(0), 1000);
        assert_eq!(time_col.value(1), 2000);

        // host should be all nulls (missing)
        assert_eq!(aligned.column(1).null_count(), 2);

        // usage_idle should be all nulls (missing)
        assert_eq!(aligned.column(2).null_count(), 2);

        // date should come from batch
        let date_col = aligned
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(date_col.value(0), "2024-01-01");
        assert_eq!(date_col.value(1), "2024-01-02");
    }

    #[test]
    fn test_align_batch_to_empty_schema_for_count_star() {
        // count(*) produces an empty projection — 0 columns, just need row count
        let empty_schema = Arc::new(Schema::new(Vec::<Field>::new()));

        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let batch =
            RecordBatch::try_new(batch_schema, vec![Arc::new(ids), Arc::new(names)]).unwrap();

        let aligned = align_batch_to_schema(&batch, &empty_schema).unwrap();
        assert_eq!(aligned.num_rows(), 3);
        assert_eq!(aligned.num_columns(), 0);
    }

    #[test]
    fn test_remap_projection_empty_for_count_star() {
        let union_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        let child_schema = union_schema.clone();

        // Empty projection (count(*)) should return Some(vec![]), not None
        let remapped =
            remap_projection_for_child(&union_schema, &child_schema, Some(&vec![]));
        assert_eq!(remapped, Some(vec![]));
    }
}
