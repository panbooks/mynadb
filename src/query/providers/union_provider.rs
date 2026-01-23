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
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
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
fn merge_schemas(schema1: SchemaRef, schema2: SchemaRef) -> Result<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Add all fields from schema1
    for field in schema1.fields() {
        fields.push(field.as_ref().clone());
        seen_names.insert(field.name().clone());
    }

    // Add fields from schema2 that aren't in schema1
    for field in schema2.fields() {
        if !seen_names.contains(field.name()) {
            // Field not in schema1, add it (but make it nullable since it might not exist in all sources)
            let mut new_field = field.as_ref().clone();
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

        // Chain the streams: first primary (memtable), then secondary (parquet)
        let schema = self.schema.clone();

        // Use select_all to interleave both streams (order doesn't matter for union)
        let streams = vec![primary_stream, secondary_stream];
        let combined = futures::stream::select_all(streams);

        let stream = RecordBatchStreamAdapter::new(schema, combined);

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
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
}
