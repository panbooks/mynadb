//! Custom execution plan for reading Parquet files

use super::config::{ParquetFileInfo, ParquetTableConfig};
use super::reader::read_parquet_file;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::ExecutionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Custom execution plan for reading Parquet files via ObjectStore
pub struct ParquetExecPlan {
    /// Schema after projection
    schema: SchemaRef,
    /// Files to read
    files: Vec<ParquetFileInfo>,
    /// Column projection (indices into original schema)
    projection: Option<Vec<usize>>,
    /// Pushed-down filters
    filters: Vec<Expr>,
    /// Optional row limit
    limit: Option<usize>,
    /// Configuration
    config: ParquetTableConfig,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
    /// ObjectStore for file access
    object_store: Arc<dyn ObjectStore>,
    /// Local parquet cache for cluster mode (optional)
    parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
}

impl fmt::Debug for ParquetExecPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetExecPlan")
            .field("schema", &self.schema)
            .field("files", &self.files.len())
            .field("projection", &self.projection)
            .field("filters", &self.filters.len())
            .field("limit", &self.limit)
            .field("config", &self.config)
            .field("parquet_cache", &self.parquet_cache.is_some())
            .finish()
    }
}

impl ParquetExecPlan {
    pub fn new(
        schema: SchemaRef,
        files: Vec<ParquetFileInfo>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        config: ParquetTableConfig,
        object_store: Arc<dyn ObjectStore>,
        parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
    ) -> Self {
        // Determine partitioning based on number of files
        let num_partitions = files.len().max(1);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(num_partitions),
            ExecutionMode::Bounded,
        );

        Self {
            schema,
            files,
            projection,
            filters,
            limit,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            object_store,
            parquet_cache,
        }
    }

    /// Get file count
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

impl DisplayAs for ParquetExecPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ParquetExecPlan: table={}, files={}, projection={:?}, filters={}, limit={:?}",
                    self.config.table_name,
                    self.files.len(),
                    self.projection,
                    self.filters.len(),
                    self.limit
                )
            }
        }
    }
}

impl ExecutionPlan for ParquetExecPlan {
    fn name(&self) -> &str {
        "ParquetExecPlan"
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // For single partition, read all files
        // For multiple partitions, each partition reads one file
        let files_to_read = if self.files.len() <= 1 || partition == 0 {
            self.files.clone()
        } else if partition < self.files.len() {
            vec![self.files[partition].clone()]
        } else {
            vec![]
        };

        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let batch_size = self.config.batch_size;
        let limit = self.limit;
        let partition_column = self.config.partition_column.clone();
        let node_id_column = self.config.node_id_column.clone();
        let filters = self.filters.clone();
        let object_store = Arc::clone(&self.object_store);
        let parquet_cache = self.parquet_cache.clone();

        // Create async stream that reads files
        let stream = stream::iter(files_to_read)
            .map(move |file_info| {
                let schema = schema.clone();
                let projection = projection.clone();
                let partition_column = partition_column.clone();
                let node_id_column = node_id_column.clone();
                let filters = filters.clone();
                let object_store = Arc::clone(&object_store);
                let parquet_cache = parquet_cache.clone();

                async move {
                    read_parquet_file(
                        &file_info,
                        schema,
                        projection,
                        batch_size,
                        limit,
                        &partition_column,
                        &node_id_column,
                        &filters,
                        &object_store,
                        parquet_cache.as_ref(),
                    )
                    .await
                }
            })
            .buffered(4) // Read up to 4 files concurrently
            .try_flatten();

        let stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream);
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
