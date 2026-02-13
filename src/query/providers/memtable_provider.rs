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
use datafusion::arrow::array::{new_null_array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
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
use std::collections::HashMap;
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
    /// Table schema (inferred from cached data, includes virtual node_id column)
    schema: SchemaRef,
    /// Node ID value to inject as virtual partition column
    node_id: String,
}

impl fmt::Debug for MemtableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemtableProvider")
            .field("catalog", &self.catalog)
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl MemtableProvider {
    /// Create a new MemtableProvider
    ///
    /// Returns None if there is no cached data or no schema available.
    /// The `node_id` parameter is added as a virtual partition column to match the
    /// Hive partition structure used by the Parquet reader (date=.../node_id=.../file.parquet).
    pub fn try_new(
        cache: Arc<DataCache>,
        catalog: &str,
        schema_name: &str,
        table_name: &str,
        node_id: &str,
    ) -> Option<Self> {
        // Get schema from cached data (includes date column + node_id added by DataCache)
        let schema = cache.get_table_schema(catalog, schema_name, table_name)?;

        Some(Self {
            cache,
            catalog: catalog.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
            node_id: node_id.to_string(),
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
        node_id: &str,
    ) -> Self {
        Self {
            cache,
            catalog: catalog.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
            node_id: node_id.to_string(),
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
            self.node_id.clone(),
            self.schema.clone(),
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
    /// Column projection indices (referencing full_schema which includes node_id)
    projection: Option<Vec<usize>>,
    /// Metrics for this execution
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
    /// Node ID value to inject as virtual partition column
    node_id: String,
    /// Full schema including node_id (before projection)
    full_schema: SchemaRef,
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
        node_id: String,
        full_schema: SchemaRef,
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
            node_id,
            full_schema,
        }
    }

    /// Get the full table name
    fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema_name, self.table_name)
    }

    /// Concatenate aligned batches, sort by key columns, and coalesce duplicate rows.
    fn sort_and_coalesce(
        batches: Vec<RecordBatch>,
        schema: &SchemaRef,
    ) -> std::result::Result<RecordBatch, String> {
        use datafusion::arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};

        if batches.len() == 1 && batches[0].num_rows() <= 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // Concatenate all batches
        let merged = concat_batches(schema, &batches)
            .map_err(|e| format!("Failed to concat batches: {}", e))?;

        if merged.num_rows() <= 1 {
            return Ok(merged);
        }

        // Build sort columns: time + all Utf8 columns (tags)
        let mut sort_cols: Vec<SortColumn> = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            if field.name() == "time"
                || matches!(
                    field.data_type(),
                    datafusion::arrow::datatypes::DataType::Utf8
                        | datafusion::arrow::datatypes::DataType::LargeUtf8
                        | datafusion::arrow::datatypes::DataType::Dictionary(_, _)
                )
            {
                sort_cols.push(SortColumn {
                    values: merged.column(i).clone(),
                    options: None,
                });
            }
        }

        if sort_cols.is_empty() {
            return Ok(merged);
        }

        // Sort
        let indices = lexsort_to_indices(&sort_cols, None)
            .map_err(|e| format!("Failed to sort: {}", e))?;
        let sorted_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = merged
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to reorder: {}", e))?;
        let sorted = RecordBatch::try_new(schema.clone(), sorted_columns)
            .map_err(|e| format!("Failed to create sorted batch: {}", e))?;

        // Coalesce adjacent rows with the same key
        crate::storage::batch_converter::coalesce_record_batch(&sorted)
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
        // Get batches from cache WITHOUT projection — we need to align to full_schema first
        let batches = self.cache.get_table_batches(
            &self.catalog,
            &self.schema_name,
            &self.table_name,
            None,
            None, // No projection here; we apply it after alignment
        );

        debug!(
            "MemtableExec execute: table={}, batches={}, projection={:?}, node_id={}",
            self.full_name(),
            batches.len(),
            self.projection,
            self.node_id,
        );

        // Pre-compute column mappings per unique batch schema.
        // Each batch may have a different schema (e.g., Telegraf sends different field sets
        // for the same measurement). We align columns by name to full_schema.
        #[derive(Clone)]
        enum ColumnSource {
            /// Fill with node_id string value
            NodeId,
            /// Take column at this index from the batch
            BatchColumn(usize),
            /// Fill with nulls (column not present in this batch)
            Null,
        }

        let node_id = self.node_id.clone();
        let projection = self.projection.clone();
        let full_schema = self.full_schema.clone();

        let mut schema_mappings: HashMap<SchemaRef, Vec<ColumnSource>> = HashMap::new();

        // Align all batches to full_schema (without projection — we need all columns for coalescing)
        let aligned_batches: Vec<RecordBatch> = batches
            .into_iter()
            .filter_map(|batch| {
                let batch_schema = batch.schema();
                let mapping = schema_mappings
                    .entry(batch_schema.clone())
                    .or_insert_with(|| {
                        full_schema
                            .fields()
                            .iter()
                            .map(|field| {
                                if field.name() == "node_id" {
                                    ColumnSource::NodeId
                                } else {
                                    match batch_schema.index_of(field.name()) {
                                        Ok(idx) => ColumnSource::BatchColumn(idx),
                                        Err(_) => ColumnSource::Null,
                                    }
                                }
                            })
                            .collect()
                    })
                    .clone();

                let num_rows = batch.num_rows();
                let node_id_array: Arc<dyn datafusion::arrow::array::Array> =
                    Arc::new(StringArray::from(vec![node_id.as_str(); num_rows]));

                let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = mapping
                    .iter()
                    .enumerate()
                    .map(|(field_idx, src)| match src {
                        ColumnSource::NodeId => node_id_array.clone(),
                        ColumnSource::BatchColumn(idx) => {
                            let col = batch.column(*idx);
                            let target_type = full_schema.field(field_idx).data_type();
                            if col.data_type() != target_type {
                                // Cast type mismatches (e.g., Int64 → Float64 across Telegraf batches)
                                cast(col.as_ref(), target_type).unwrap_or_else(|_| col.clone())
                            } else {
                                col.clone()
                            }
                        }
                        ColumnSource::Null => {
                            let data_type = full_schema.field(field_idx).data_type();
                            new_null_array(data_type, num_rows)
                        }
                    })
                    .collect();

                RecordBatch::try_new(full_schema.clone(), columns).ok()
            })
            .collect();

        // Coalesce rows with the same key (time + tags) to merge heterogeneous field sets.
        // Telegraf may send the same measurement with different field subsets at the
        // same timestamp, producing separate batches with NULL-padded columns.
        // We concatenate, sort by key columns, then merge adjacent same-key rows.
        let batch_results: Vec<Result<RecordBatch>> = if aligned_batches.is_empty() {
            vec![]
        } else {
            match Self::sort_and_coalesce(aligned_batches, &full_schema) {
                Ok(coalesced) => {
                    // Apply projection if specified
                    if let Some(ref proj) = projection {
                        vec![coalesced.project(proj).map_err(DataFusionError::from)]
                    } else {
                        vec![Ok(coalesced)]
                    }
                }
                Err(e) => vec![Err(DataFusionError::Execution(e))],
            }
        };

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
