//! Query Engine
//!
//! The central query orchestrator that handles SQL execution against Parquet files
//! and in-memory data (memtable). Supports automatic table registration, query caching,
//! and InfluxDB-compatible GROUP BY time() syntax.

use crate::query::cache::hot_partition_cache::{
    HotPartitionCache, HotPartitionCacheConfig, SharedHotPartitionCache,
};
use crate::query::cache::metadata_cache::TableMetadataCache;
use crate::query::cache::query_cache::{
    QueryCacheConfig, QueryResultCache, SharedQueryResultCache,
};
use crate::query::create_custom_session_state;
use crate::query::functions::{
    create_first_value_udaf, create_geo_bearing_udf, create_geo_bounding_box_udf,
    create_geo_distance_udf, create_geo_hash_decode_lat_udf, create_geo_hash_decode_lon_udf,
    create_geo_hash_neighbors_udf, create_geo_hash_udf, create_geo_within_udf,
    create_last_value_udaf, create_time_bucket_udf,
};
use crate::storage::cache::DataCache;
use crate::storage::storage::Storage;
use datafusion::config::ConfigOptions;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// Sub-modules containing impl blocks for QueryEngine
mod cache;
mod discovery;
mod execution;
mod listing;
mod registration;

// Re-export types that are part of the public API
pub use crate::query::cache::hot_partition_cache::HotPartitionCacheStats;
pub use crate::query::cache::metadata_cache::{CacheStatistics, TableParquetStats};
pub use crate::query::cache::query_cache::QueryCacheStats;
pub use crate::query::types::{QueryAnalysis, QueryExplainResult};
pub use crate::flight_protocol::QueryScope;

/// Default memory limit for query execution (16GB)
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 16 * 1024 * 1024 * 1024;
/// Memory threshold fraction before spilling to disk (80%)
const MEMORY_LIMIT_FRACTION: f64 = 0.8;

pub struct QueryEngine {
    pub(crate) ctx: SessionContext,
    pub(crate) data_dir: String,
    pub(crate) storage: Option<Arc<Storage>>,
    pub(crate) registered_catalogs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    pub(crate) metadata_cache: Arc<TableMetadataCache>,
    pub(crate) query_cache: SharedQueryResultCache,
    pub(crate) hot_partition_cache: SharedHotPartitionCache,
    pub(crate) use_custom_table_provider: bool,
    pub(crate) write_cache: Option<Arc<DataCache>>,
    /// Local parquet file cache for cluster mode (object storage)
    pub(crate) parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
}

impl QueryEngine {
    /// Create optimized SessionContext with predicate push-down, parallel reading, and pruning enabled
    fn create_optimized_context() -> SessionContext {
        let mut config = ConfigOptions::new();

        // ===== Predicate Push-down =====
        // Push filter predicates to Parquet reader for early filtering
        config.execution.parquet.pushdown_filters = true;
        // Reorder filters for optimal execution (most selective first)
        config.execution.parquet.reorder_filters = true;

        // ===== Row Group & Page Pruning =====
        // Enable row group pruning using Parquet statistics (min/max)
        config.execution.parquet.pruning = true;
        // Enable page-level pruning using page index for finer-grained filtering
        config.execution.parquet.enable_page_index = true;
        // Enable bloom filter for string column filtering
        config.execution.parquet.bloom_filter_on_read = true;

        // ===== Parallel Parquet Reading =====
        // Use all available CPU cores for parallel partition processing
        let num_cpus = num_cpus::get();
        config.execution.target_partitions = num_cpus;
        // Optimal batch size for vectorized execution (8K rows per batch)
        config.execution.batch_size = 8192;
        // Enable parallel reading of row groups within a single file
        config.execution.parquet.maximum_parallel_row_group_writers = num_cpus;
        // Increase buffered rows for better I/O efficiency
        config
            .execution
            .parquet
            .maximum_buffered_record_batches_per_stream = 4;

        // ===== Columnar Projection Push-down =====
        // Enable projection pushdown to read only needed columns from Parquet
        // (This is enabled by default in DataFusion, but we ensure it's on)
        config.optimizer.enable_distinct_aggregation_soft_limit = true;

        // ===== Memory & Performance Tuning =====
        // Sort spill threshold (in rows) before spilling to disk
        config.execution.sort_spill_reservation_bytes = 100 * 1024 * 1024; // 100MB
                                                                           // Coalesce batches for better throughput
        config.execution.coalesce_batches = true;

        // ===== Time-series Workload Optimization =====
        // Disable round-robin repartition (not beneficial for time-series)
        config.optimizer.enable_round_robin_repartition = false;
        // Disable file scan repartition (we want partition-aware scanning)
        config.optimizer.repartition_file_scans = false;
        // Enable topk optimization for ORDER BY ... LIMIT queries
        config.optimizer.top_down_join_key_reordering = true;

        // Create session config
        let session_config = SessionConfig::from(config);

        // ===== Memory Management =====
        // Configure RuntimeEnv with memory limits and disk spilling
        let runtime_config = RuntimeConfig::new()
            // Set memory limit (4GB default, 80% threshold before spilling)
            .with_memory_limit(DEFAULT_MEMORY_LIMIT_BYTES, MEMORY_LIMIT_FRACTION)
            // Enable disk manager for spilling intermediate results
            .with_disk_manager(DiskManagerConfig::NewOs);

        let runtime_env = match RuntimeEnv::try_new(runtime_config) {
            Ok(env) => Arc::new(env),
            Err(e) => {
                warn!(
                    "Failed to create RuntimeEnv with memory limits: {}, using default",
                    e
                );
                Arc::new(RuntimeEnv::default())
            }
        };

        // Create base session context with optimized config and runtime environment
        let base_ctx = SessionContext::new_with_config_rt(session_config, runtime_env);

        // Apply custom analyzer and optimizer rules
        let custom_state = create_custom_session_state(base_ctx.state());
        let ctx = SessionContext::new_with_state(custom_state);

        // Register custom UDFs
        ctx.register_udf(create_time_bucket_udf());
        debug!("Registered time_bucket UDF");

        // Register geospatial UDFs
        ctx.register_udf(create_geo_distance_udf());
        ctx.register_udf(create_geo_hash_udf());
        ctx.register_udf(create_geo_hash_decode_lat_udf());
        ctx.register_udf(create_geo_hash_decode_lon_udf());
        ctx.register_udf(create_geo_hash_neighbors_udf());
        ctx.register_udf(create_geo_within_udf());
        ctx.register_udf(create_geo_bounding_box_udf());
        ctx.register_udf(create_geo_bearing_udf());
        debug!("Registered geospatial UDFs");

        // Register custom UDAFs for InfluxDB-compatible first/last functions
        ctx.register_udaf(create_first_value_udaf());
        ctx.register_udaf(create_last_value_udaf());
        debug!("Registered first_value and last_value UDAFs");

        ctx
    }

    pub fn new(data_dir: &str) -> Self {
        Self::new_with_options(
            data_dir,
            None,
            QueryCacheConfig::default(),
            HotPartitionCacheConfig::default(),
            false,
            None,
        )
    }

    pub fn new_with_query_cache(
        data_dir: &str,
        storage: Option<Arc<Storage>>,
        query_cache_config: QueryCacheConfig,
    ) -> Self {
        Self::new_with_options(
            data_dir,
            storage,
            query_cache_config,
            HotPartitionCacheConfig::default(),
            false,
            None,
        )
    }

    pub fn new_with_memtable(
        data_dir: &str,
        storage: Option<Arc<Storage>>,
        query_cache_config: QueryCacheConfig,
        hot_partition_cache_config: HotPartitionCacheConfig,
        use_custom_table_provider: bool,
        write_cache: Arc<DataCache>,
    ) -> Self {
        Self::new_with_options(
            data_dir,
            storage,
            query_cache_config,
            hot_partition_cache_config,
            use_custom_table_provider,
            Some(write_cache),
        )
    }

    pub fn new_with_options(
        data_dir: &str,
        storage: Option<Arc<Storage>>,
        query_cache_config: QueryCacheConfig,
        hot_partition_cache_config: HotPartitionCacheConfig,
        use_custom_table_provider: bool,
        write_cache: Option<Arc<DataCache>>,
    ) -> Self {
        let ctx = Self::create_optimized_context();
        let metadata_cache = Arc::new(TableMetadataCache::new());
        let query_cache = Arc::new(QueryResultCache::new(query_cache_config));
        let hot_partition_cache = Arc::new(HotPartitionCache::new(hot_partition_cache_config));

        // Register object store with DataFusion runtime if needed
        if let Some(ref storage) = storage {
            if !storage.is_local() {
                let url = url::Url::parse(storage.url_prefix())
                    .expect("Failed to parse storage URL prefix");
                ctx.runtime_env()
                    .register_object_store(&url, storage.object_store());
                info!(
                    "Registered object store with DataFusion: {}",
                    storage.url_prefix()
                );
            }
        }

        let storage_type = match &storage {
            Some(s) if s.is_local() => "local",
            Some(_) => "object_store",
            None => "local",
        };

        let table_provider_type = if use_custom_table_provider {
            "custom"
        } else {
            "listing"
        };
        let memtable_status = if write_cache.is_some() {
            "enabled"
        } else {
            "disabled"
        };
        info!(
            "QueryEngine initialized: storage={}, table_provider={}, metadata_cache=enabled, query_cache={}, hot_partition_cache={}, memtable={}",
            storage_type,
            table_provider_type,
            if query_cache.is_enabled() { "enabled" } else { "disabled" },
            if hot_partition_cache.is_enabled() { "enabled" } else { "disabled" },
            memtable_status
        );

        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
            storage,
            registered_catalogs: Arc::new(RwLock::new(HashMap::new())),
            metadata_cache,
            query_cache,
            hot_partition_cache,
            use_custom_table_provider,
            parquet_cache: None,
            write_cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_engine() {
        let _engine = QueryEngine::new("./data");
    }
}
