//! Custom execution plan for reading Parquet files

use super::config::{ParquetFileInfo, ParquetTableConfig};
use super::reader::read_parquet_file;
use crate::query::cache::hot_partition_cache::{HotPartitionCache, PartitionKey};
use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::ExecutionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use futures::task::{Context, Poll};
use object_store::ObjectStore;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Custom execution plan for reading Parquet files via ObjectStore
pub struct ParquetExecPlan {
    /// Schema after projection (what the consumer expects)
    schema: SchemaRef,
    /// Full table schema before projection (used for caching full batches)
    full_schema: SchemaRef,
    /// Files to read
    files: Vec<ParquetFileInfo>,
    /// Column projection (indices into full_schema)
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
    /// Hot partition cache for decoded RecordBatches (optional)
    hot_partition_cache: Option<Arc<HotPartitionCache>>,
    /// Catalog name for cache key
    catalog: String,
    /// Schema name for cache key
    schema_name: String,
    /// Table name for cache key
    table_name: String,
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
            .field("hot_partition_cache", &self.hot_partition_cache.is_some())
            .finish()
    }
}

impl ParquetExecPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: SchemaRef,
        full_schema: SchemaRef,
        files: Vec<ParquetFileInfo>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        config: ParquetTableConfig,
        object_store: Arc<dyn ObjectStore>,
        parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
        hot_partition_cache: Option<Arc<HotPartitionCache>>,
        catalog: String,
        schema_name: String,
        table_name: String,
    ) -> Self {
        // Use a single partition: all files are read in one execute() call.
        // I/O parallelism is provided by buffered(4) within the stream.
        // This is critical for hot partition cache correctness — with multiple
        // partitions, each would overwrite the same cache key (keyed by date),
        // causing data loss on subsequent cache HITs.
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            schema,
            full_schema,
            files,
            projection,
            filters,
            limit,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            object_store,
            parquet_cache,
            hot_partition_cache,
            catalog,
            schema_name,
            table_name,
        }
    }

    /// Get file count
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Extract partition key from file info
    fn extract_partition_key(&self, file_info: &ParquetFileInfo) -> Option<PartitionKey> {
        // Get date from partition_values
        let date_str = file_info.partition_values.get("date")?;

        // Parse date string (YYYY-MM-DD format)
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
        let date_int = date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32;

        Some(PartitionKey::new(
            &self.catalog,
            &self.schema_name,
            &self.table_name,
            date_int,
        ))
    }

    /// Check hot partition cache for a given partition key.
    /// The cache stores FULL (non-projected) batches; projection is applied here.
    /// `current_file_count` is used to detect stale cache entries (e.g., after flush/compaction).
    fn check_hot_cache(&self, partition_key: &PartitionKey, current_file_count: usize) -> Option<Vec<RecordBatch>> {
        let cache = self.hot_partition_cache.as_ref()?;

        if !cache.is_enabled() {
            return None;
        }

        // Get full batches from cache, validate file count to detect staleness
        cache.get(partition_key, self.projection.as_deref(), &[], Some(current_file_count))
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
        // Single partition reads all files; buffered(4) provides I/O parallelism
        let files_to_read = if partition == 0 {
            self.files.clone()
        } else {
            vec![]
        };

        if files_to_read.is_empty() {
            let stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream::empty());
            return Ok(Box::pin(stream));
        }

        // CHECK HOT PARTITION CACHE
        // Cache stores full (non-projected) batches; check_hot_cache applies this query's projection
        if let Some(first_key) = self.extract_partition_key(&files_to_read[0]) {
            let all_same_partition = files_to_read.iter().all(|file| {
                self.extract_partition_key(file)
                    .map(|key| key.date == first_key.date)
                    .unwrap_or(false)
            });

            if all_same_partition {
                if let Some(cached_batches) = self.check_hot_cache(&first_key, files_to_read.len()) {
                    if !cached_batches.is_empty() {
                        info!(
                            "Hot partition cache HIT: {}.{}.{} date={}, returning {} batches ({} rows)",
                            first_key.catalog,
                            first_key.schema,
                            first_key.table,
                            first_key.date_str(),
                            cached_batches.len(),
                            cached_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                        );
                        return Ok(Box::pin(MemoryStream::new(
                            self.schema.clone(),
                            cached_batches,
                        )));
                    }
                    // Cache returned empty batches (projection mismatch) — treat as miss
                    debug!(
                        "Hot partition cache returned empty for {}.{}.{} date={}, treating as miss",
                        first_key.catalog, first_key.schema, first_key.table, first_key.date_str()
                    );
                }
            }
        }

        // CACHE MISS - read from Parquet and cache result
        // When caching is active, read ALL columns (full_schema) so the cache
        // stores complete batches that work with any future projection.
        let batch_size = self.config.batch_size;
        let limit = self.limit;
        let partition_column = self.config.partition_column.clone();
        let node_id_column = self.config.node_id_column.clone();
        let filters = self.filters.clone();
        let object_store = Arc::clone(&self.object_store);
        let parquet_cache = self.parquet_cache.clone();
        let hot_cache = self.hot_partition_cache.clone();
        let file_count = files_to_read.len();

        // Extract partition key for caching (if all files have same date)
        let partition_key_for_cache = if let Some(first_key) = self.extract_partition_key(&files_to_read[0]) {
            let all_same = files_to_read.iter().all(|file| {
                self.extract_partition_key(file)
                    .map(|key| key.date == first_key.date)
                    .unwrap_or(false)
            });
            if all_same {
                Some(first_key)
            } else {
                None
            }
        } else {
            None
        };

        // Determine if we should cache this partition
        let should_cache = partition_key_for_cache.as_ref().map_or(false, |pk| {
            hot_cache.as_ref().map_or(false, |c| c.is_enabled() && c.is_hot_date(pk.date))
        });

        // When caching, read with FULL schema (no projection) so cache stores complete data.
        // When not caching, read with projected schema for efficiency.
        let read_schema = if should_cache {
            self.full_schema.clone()
        } else {
            self.schema.clone()
        };
        let read_projection = if should_cache {
            None
        } else {
            self.projection.clone()
        };

        // Create async stream that reads files
        let batches_stream = stream::iter(files_to_read)
            .map(move |file_info| {
                let schema = read_schema.clone();
                let projection = read_projection.clone();
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

        // If we should cache this partition, collect full batches and store
        if let (Some(partition_key), true) = (partition_key_for_cache, should_cache) {
            let cache = hot_cache.unwrap(); // safe: should_cache implies hot_cache is Some
            let output_schema = self.schema.clone();
            let projection_for_output = self.projection.clone();

            // CachingStream: collects full batches for cache, projects before yielding
            let stream = CachingStream::new(
                output_schema,
                batches_stream,
                cache,
                partition_key,
                file_count,
                projection_for_output,
            );

            return Ok(Box::pin(stream));
        }

        // No caching needed, just return regular stream
        let stream = RecordBatchStreamAdapter::new(self.schema.clone(), batches_stream);
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream that serves RecordBatches from memory (cached data)
struct MemoryStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    index: usize,
}

impl MemoryStream {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches,
            index: 0,
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stream that reads full batches, caches them, and projects before yielding.
///
/// The inner stream produces FULL (non-projected) batches so the cache stores
/// complete data. Before yielding to the consumer, projection is applied so
/// the output matches the expected projected schema.
struct CachingStream<S> {
    /// Output schema (projected)
    schema: SchemaRef,
    inner: S,
    hot_cache: Arc<HotPartitionCache>,
    partition_key: PartitionKey,
    file_count: usize,
    /// Full batches collected for caching
    collected_batches: Vec<RecordBatch>,
    /// Projection to apply before yielding (None = no projection)
    projection: Option<Vec<usize>>,
    done: bool,
}

impl<S> CachingStream<S> {
    fn new(
        schema: SchemaRef,
        inner: S,
        hot_cache: Arc<HotPartitionCache>,
        partition_key: PartitionKey,
        file_count: usize,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            schema,
            inner,
            hot_cache,
            partition_key,
            file_count,
            collected_batches: Vec::new(),
            projection,
            done: false,
        }
    }
}

impl<S> Stream for CachingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Collect FULL batch for caching
                this.collected_batches.push(batch.clone());

                // Apply projection before yielding to consumer
                let output_batch = if let Some(ref proj) = this.projection {
                    match batch.project(proj) {
                        Ok(projected) => projected,
                        Err(e) => {
                            warn!("Failed to project cached batch: {}", e);
                            return Poll::Ready(Some(Err(
                                datafusion::common::DataFusionError::ArrowError(e, None),
                            )));
                        }
                    }
                } else {
                    batch
                };

                Poll::Ready(Some(Ok(output_batch)))
            }
            Poll::Ready(None) => {
                // Stream ended - store collected FULL batches in cache
                if !this.done && !this.collected_batches.is_empty() {
                    debug!(
                        "Caching partition: {}.{}.{} date={}, {} batches, {} rows (full schema)",
                        this.partition_key.catalog,
                        this.partition_key.schema,
                        this.partition_key.table,
                        this.partition_key.date_str(),
                        this.collected_batches.len(),
                        this.collected_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                    );

                    this.hot_cache.put(
                        this.partition_key.clone(),
                        this.collected_batches.clone(),
                        this.file_count,
                    );
                    this.done = true;
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> datafusion::physical_plan::RecordBatchStream for CachingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
