//! Memtable TableProvider Implementation
//!
//! Provides a TableProvider for querying in-memory cached data that hasn't been
//! flushed to Parquet yet. This enables immediate query visibility for fresh writes.
//!
//! ## Architecture
//!
//! ```text
//! DataCache (sharded RecordBatches)
//!     ↓
//! MemtableProvider (implements TableProvider)
//!     ↓
//! MemtableExec (implements ExecutionPlan)
//!     ↓
//! SendableRecordBatchStream (streaming results)
//! ```
//!
//! ## Memory Efficiency
//!
//! - Reads directly from cache without copying (batches are Arc'd)
//! - Uses streaming to avoid collecting all data in memory
//! - Supports filter pushdown for early filtering

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
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
use futures::stream;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tracing::debug;

use crate::storage::cache::DataCache;

/// A TableProvider that reads from in-memory DataCache
///
/// This provider allows DataFusion to query data that is still in the write cache
/// and hasn't been flushed to Parquet files yet.
pub struct MemtableProvider {
    /// Reference to the data cache
    cache: Arc<DataCache>,
    /// Catalog (database) name
    catalog: String,
    /// Schema (service) name
    schema_name: String,
    /// Table (measurement) name
    table_name: String,
    /// Table schema (inferred from cached data)
    schema: SchemaRef,
}

impl fmt::Debug for MemtableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemtableProvider")
            .field("catalog", &self.catalog)
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl MemtableProvider {
    /// Create a new MemtableProvider
    ///
    /// Returns None if there is no cached data or no schema available
    pub fn try_new(
        cache: Arc<DataCache>,
        catalog: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<Self> {
        // Get schema from cached data (includes date column added at write time)
        let schema = cache.get_table_schema(catalog, schema_name, table_name)?;

        Some(Self {
            cache,
            catalog: catalog.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
        })
    }

    /// Create a new MemtableProvider with a specific schema
    ///
    /// Use this when you already have the schema (e.g., from Parquet files)
    pub fn new_with_schema(
        cache: Arc<DataCache>,
        catalog: &str,
        schema_name: &str,
        table_name: &str,
        schema: SchemaRef,
    ) -> Self {
        Self {
            cache,
            catalog: catalog.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
        }
    }

    /// Get the full table name (catalog.schema.table)
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema_name, self.table_name)
    }

    /// Check if there is any cached data for this table
    pub fn has_data(&self) -> bool {
        self.cache
            .has_cached_data(&self.catalog, &self.schema_name, &self.table_name)
    }

    /// Get the number of cached rows
    pub fn cached_row_count(&self) -> usize {
        self.cache
            .get_table_cache_stats(&self.catalog, &self.schema_name, &self.table_name)
            .total_rows
    }
}

#[async_trait]
impl TableProvider for MemtableProvider {
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
        // We support inexact filter pushdown - filters will be applied after reading
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Compute the projected schema
        let projected_schema = project_schema(&self.schema, projection)?;

        debug!(
            "MemtableProvider scan: table={}, projection={:?}",
            self.full_name(),
            projection
        );

        // Create the execution plan
        let exec = MemtableExec::new(
            self.cache.clone(),
            self.catalog.clone(),
            self.schema_name.clone(),
            self.table_name.clone(),
            projected_schema,
            projection.cloned(),
        );

        Ok(Arc::new(exec))
    }
}

/// Execution plan for reading from in-memory cache
pub struct MemtableExec {
    /// Reference to the data cache
    cache: Arc<DataCache>,
    /// Catalog name
    catalog: String,
    /// Schema name
    schema_name: String,
    /// Table name
    table_name: String,
    /// Output schema (after projection)
    schema: SchemaRef,
    /// Column projection indices
    projection: Option<Vec<usize>>,
    /// Metrics for this execution
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
}

impl std::fmt::Debug for MemtableExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemtableExec")
            .field("catalog", &self.catalog)
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .finish()
    }
}

impl MemtableExec {
    pub fn new(
        cache: Arc<DataCache>,
        catalog: String,
        schema_name: String,
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        // Single partition for memtable reads
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            cache,
            catalog,
            schema_name,
            table_name,
            schema,
            projection,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    /// Get the full table name
    fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema_name, self.table_name)
    }
}

impl DisplayAs for MemtableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MemtableExec: table={}, projection={:?}",
                    self.full_name(),
                    self.projection
                )
            }
        }
    }
}

impl ExecutionPlan for MemtableExec {
    fn name(&self) -> &str {
        "MemtableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Get batches from cache with projection applied
        let batches = self.cache.get_table_batches(
            &self.catalog,
            &self.schema_name,
            &self.table_name,
            None,
            self.projection.as_deref(),
        );

        debug!(
            "MemtableExec execute: table={}, batches={}, projection={:?}",
            self.full_name(),
            batches.len(),
            self.projection
        );

        // Batches already have projection applied by DataCache
        let batch_results: Vec<Result<RecordBatch>> = batches.into_iter().map(Ok).collect();

        // Create stream from batches
        let stream = stream::iter(batch_results);
        let stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream);

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage::Storage;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec![Some("a"), Some("b"), None]);
        let values = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
        )
        .unwrap()
    }

    #[test]
    fn test_project_batch() {
        let batch = create_test_batch();

        // Test projection using Arrow's built-in project
        let projection = vec![0, 2]; // id and value columns
        let result = batch.project(&projection).unwrap();

        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "value");
    }
}
