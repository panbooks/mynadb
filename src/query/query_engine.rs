//! Query Engine
//!
//! The central query orchestrator that handles SQL execution against Parquet files
//! and in-memory data (memtable). Supports automatic table registration, query caching,
//! and InfluxDB-compatible GROUP BY time() syntax.

use crate::query::cache::hot_partition_cache::{
    HotPartitionCache, HotPartitionCacheConfig, HotPartitionCacheStats, SharedHotPartitionCache,
};
use crate::query::cache::metadata_cache::{CacheStatistics, TableMetadataCache, TableParquetStats};
use crate::query::cache::query_cache::{
    QueryCacheConfig, QueryCacheKey, QueryCacheStats, QueryResultCache, SharedQueryResultCache,
};
use crate::query::create_custom_session_state;
use crate::query::functions::{
    create_first_value_udaf, create_last_value_udaf, create_time_bucket_udf,
};
use crate::query::providers::{
    MemtableProvider, ParquetTableConfig, ParquetTableProvider, UnionTableProvider,
};
use crate::query::sql_utils::{
    extract_optimization_info, extract_table_references, extract_time_range,
    inject_date_predicates, qualify_table_names, summarize_plan,
};
use crate::query::timeseries::{FillOption, FillProcessor, GroupByTimeTransformer};
use crate::query::types::{QueryAnalysis, QueryExplainResult};
use crate::storage::cache::DataCache;
use crate::storage::storage::Storage;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use parquet::file::metadata::ParquetMetaData;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Default memory limit for query execution (16GB)
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 16 * 1024 * 1024 * 1024;
/// Memory threshold fraction before spilling to disk (80%)
const MEMORY_LIMIT_FRACTION: f64 = 0.8;

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
    storage: Option<Arc<Storage>>,
    registered_catalogs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    metadata_cache: Arc<TableMetadataCache>,
    query_cache: SharedQueryResultCache,
    hot_partition_cache: SharedHotPartitionCache,
    use_custom_table_provider: bool,
    write_cache: Option<Arc<DataCache>>,
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
            write_cache,
        }
    }

    // ========================================================================
    // Cache Management
    // ========================================================================

    /// Get reference to the metadata cache
    pub fn metadata_cache(&self) -> &Arc<TableMetadataCache> {
        &self.metadata_cache
    }

    /// Get metadata cache statistics
    pub fn get_cache_stats(&self) -> CacheStatistics {
        self.metadata_cache.get_stats()
    }

    /// Get query result cache statistics
    pub fn get_query_cache_stats(&self) -> QueryCacheStats {
        self.query_cache.stats()
    }

    /// Get reference to the query result cache
    pub fn query_cache(&self) -> &SharedQueryResultCache {
        &self.query_cache
    }

    /// Get hot partition cache statistics
    pub fn get_hot_partition_cache_stats(&self) -> HotPartitionCacheStats {
        self.hot_partition_cache.stats()
    }

    /// Get reference to the hot partition cache
    pub fn hot_partition_cache(&self) -> &SharedHotPartitionCache {
        &self.hot_partition_cache
    }

    /// Invalidate cache for a table (call after writes)
    /// This invalidates metadata cache, query result cache, and hot partition cache
    pub fn invalidate_table_cache(&self, catalog: &str, schema: &str, table: &str) {
        self.metadata_cache.invalidate_table(catalog, schema, table);
        self.query_cache.invalidate_table(catalog, schema, table);
        self.hot_partition_cache
            .invalidate_table(catalog, schema, table);
    }

    /// Deregister a table from DataFusion session
    /// This is needed after flush so the table can be re-registered with proper providers
    /// (e.g., switching from memtable-only to parquet + memtable)
    pub fn deregister_table(&self, catalog: &str, schema: &str, table: &str) -> Result<(), String> {
        if let Some(catalog_provider) = self.ctx.catalog(catalog) {
            if let Some(schema_provider) = catalog_provider.schema(schema) {
                match schema_provider.deregister_table(table) {
                    Ok(Some(_)) => {
                        debug!(
                            "Deregistered table {}.{}.{} from DataFusion",
                            catalog, schema, table
                        );
                    }
                    Ok(None) => {
                        debug!("Table {}.{}.{} was not registered", catalog, schema, table);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deregister table {}.{}.{}: {}",
                            catalog, schema, table, e
                        );
                        return Err(format!("Failed to deregister table: {}", e));
                    }
                }
            }
        }
        Ok(())
    }

    /// Refresh table registration after flush
    /// Deregisters the table and invalidates caches so it gets re-registered on next query
    pub fn refresh_table_after_flush(&self, catalog: &str, schema: &str, table: &str) {
        // First deregister the table from DataFusion
        if let Err(e) = self.deregister_table(catalog, schema, table) {
            warn!("Failed to deregister table during refresh: {}", e);
        }
        // Then invalidate all caches
        self.invalidate_table_cache(catalog, schema, table);
        debug!(
            "Refreshed table registration for {}.{}.{} after flush",
            catalog, schema, table
        );
    }

    /// Invalidate all caches for a database
    pub fn invalidate_database_cache(&self, db: &str) {
        self.metadata_cache.invalidate_catalog(db);
        self.query_cache.invalidate_database(db);
        self.hot_partition_cache.invalidate_catalog(db);
    }

    /// Clear all caches (metadata + query results + hot partitions)
    pub fn clear_all_caches(&self) {
        self.metadata_cache.clear();
        self.query_cache.clear();
        self.hot_partition_cache.clear();
    }

    /// Clear only query result cache
    pub fn clear_query_cache(&self) {
        self.query_cache.clear();
    }

    // ========================================================================
    // Table Registration
    // ========================================================================

    /// Ensure catalog exists, create if needed
    async fn ensure_catalog(&self, catalog_name: &str) -> Result<(), String> {
        if self.ctx.catalog(catalog_name).is_none() {
            let catalog = Arc::new(MemoryCatalogProvider::new());
            self.ctx.register_catalog(catalog_name, catalog);
            info!("Created catalog: {}", catalog_name);
        }
        Ok(())
    }

    /// Ensure schema exists in catalog, create if needed
    async fn ensure_schema_in_catalog(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<(), String> {
        self.ensure_catalog(catalog_name).await?;

        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        if catalog.schema(schema_name).is_none() {
            let schema = Arc::new(MemorySchemaProvider::new());
            catalog
                .register_schema(schema_name, schema)
                .map_err(|e| format!("Failed to register schema: {}", e))?;
            info!("Created schema: {}.{}", catalog_name, schema_name);
        }
        Ok(())
    }

    /// Register a table in catalog.schema.table format with partition support
    /// Uses cache to avoid re-scanning directories on every query
    pub async fn register_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        // Check if table is already registered in DataFusion
        if let Some(catalog) = self.ctx.catalog(catalog_name) {
            if let Some(schema) = catalog.schema(schema_name) {
                if schema.table_exist(table_name) {
                    // Table already registered, skip
                    return Ok(());
                }
            }
        }

        // Build table URL based on storage backend
        // In cluster mode (object storage + write_cache), files are stored with node_id prefix
        let table_url = if let Some(ref storage) = self.storage {
            if !storage.is_local() {
                // Object storage in cluster mode: use node_id prefix
                if let Some(ref cache) = self.write_cache {
                    storage.table_url_with_node(cache.node_id(), catalog_name, schema_name, table_name)
                } else {
                    storage.table_url(catalog_name, schema_name, table_name)
                }
            } else {
                storage.table_url(catalog_name, schema_name, table_name)
            }
        } else {
            format!(
                "file://{}/{}/{}/{}",
                self.data_dir, catalog_name, schema_name, table_name
            )
        };

        // Build local path for existence check (only for local storage)
        let table_path = format!(
            "{}/{}/{}/{}",
            self.data_dir, catalog_name, schema_name, table_name
        );

        // For local storage, check if path exists
        let path_exists = if self.storage.is_none()
            || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true)
        {
            std::path::Path::new(&table_path).exists()
        } else {
            true
        };

        // Check cache first for table info
        let (has_partitions, partitions) = if let Some(cached) =
            self.metadata_cache
                .get_table_info(catalog_name, schema_name, table_name)
        {
            // Use cached partition info
            debug!(
                "Using cached table info for {}.{}.{}",
                catalog_name, schema_name, table_name
            );
            (cached.has_partitions, cached.partitions)
        } else {
            // Cache miss - need to scan directory
            if !path_exists {
                // Check if we have memtable data for this table
                if let Some(ref cache) = self.write_cache {
                    if cache.has_cached_data(catalog_name, schema_name, table_name) {
                        return self
                            .register_memtable_only(catalog_name, schema_name, table_name)
                            .await;
                    }
                }
                return Err(format!("Table path not found: {}", table_path));
            }

            // Discover partitions
            let partitions = if self.storage.is_none()
                || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true)
            {
                self.discover_table_partitions(&table_path)?
            } else {
                // For object storage, assume partitioned
                Vec::new()
            };

            let has_partitions = if self.storage.is_none()
                || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true)
            {
                !partitions.is_empty()
            } else {
                true
            };

            // Cache the result
            self.metadata_cache.set_table_info(
                catalog_name,
                schema_name,
                table_name,
                has_partitions,
                partitions.clone(),
            );
            debug!(
                "Cached table info for {}.{}.{} (partitions: {})",
                catalog_name,
                schema_name,
                table_name,
                partitions.len()
            );

            (has_partitions, partitions)
        };

        // Ensure catalog and schema exist
        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        if has_partitions {
            // Register partitioned table using ListingTable API
            self.register_partitioned_table(catalog_name, schema_name, table_name, &table_url)
                .await?;
            debug!(
                "Registered partitioned table: {}.{}.{} ({} partitions)",
                catalog_name,
                schema_name,
                table_name,
                partitions.len()
            );
        } else {
            // No partitions, use standard registration
            self.register_simple_table(catalog_name, schema_name, table_name, &table_url)
                .await?;
            debug!(
                "Registered non-partitioned table: {}.{}.{}",
                catalog_name, schema_name, table_name
            );
        }

        // Track registration
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

        Ok(())
    }

    /// Register a simple (non-partitioned) table - dispatches based on config
    async fn register_simple_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        if self.use_custom_table_provider {
            self.register_table_custom(catalog_name, schema_name, table_name, table_path, false)
                .await
        } else {
            let table_ref =
                datafusion::sql::TableReference::full(catalog_name, schema_name, table_name);
            self.ctx
                .register_parquet(table_ref, table_path, ParquetReadOptions::default())
                .await
                .map_err(|e| {
                    format!(
                        "Failed to register table '{}.{}.{}': {}",
                        catalog_name, schema_name, table_name, e
                    )
                })
        }
    }

    /// Register a table using custom ParquetTableProvider (consolidated method)
    async fn register_table_custom(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
        partitioned: bool,
    ) -> Result<(), String> {
        let local_path = table_path.strip_prefix("file://").unwrap_or(table_path);
        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);

        let config = ParquetTableConfig::new(full_name.clone())
            .with_partitioned(partitioned)
            .with_batch_size(8192);

        let provider = ParquetTableProvider::try_new(local_path, config)
            .map_err(|e| format!("Failed to create table provider for '{}': {}", full_name, e))?;

        self.register_provider(catalog_name, schema_name, table_name, Arc::new(provider))
    }

    /// Register a partitioned table - dispatches based on config
    async fn register_partitioned_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        if self.use_custom_table_provider {
            self.register_table_custom(catalog_name, schema_name, table_name, table_path, true)
                .await
        } else {
            self.register_partitioned_table_listing(
                catalog_name,
                schema_name,
                table_name,
                table_path,
            )
            .await
        }
    }

    /// Register a partitioned table using DataFusion's ListingTable
    async fn register_partitioned_table_listing(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        let table_url = ListingTableUrl::parse(table_path)
            .map_err(|e| format!("Failed to parse table URL: {}", e))?;

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![(
                "date".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            )]);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| format!("Failed to infer schema: {}", e))?;

        let listing_table = ListingTable::try_new(config)
            .map_err(|e| format!("Failed to create listing table: {}", e))?;

        self.register_provider(
            catalog_name,
            schema_name,
            table_name,
            Arc::new(listing_table),
        )
    }

    /// Helper to register a TableProvider in the catalog
    fn register_provider(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<(), String> {
        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog
            .schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        let final_provider =
            self.maybe_wrap_with_memtable(catalog_name, schema_name, table_name, provider)?;

        schema
            .register_table(table_name.to_string(), final_provider)
            .map_err(|e| {
                format!(
                    "Failed to register table '{}.{}.{}': {}",
                    catalog_name, schema_name, table_name, e
                )
            })?;

        Ok(())
    }

    /// Wrap a Parquet table provider with memtable data if available
    fn maybe_wrap_with_memtable(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        parquet_provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, String> {
        let cache = match &self.write_cache {
            Some(c) => c,
            None => return Ok(parquet_provider),
        };

        if !cache.has_cached_data(catalog_name, schema_name, table_name) {
            return Ok(parquet_provider);
        }

        let memtable_provider =
            match MemtableProvider::try_new(cache.clone(), catalog_name, schema_name, table_name) {
                Some(p) => Arc::new(p),
                None => return Ok(parquet_provider),
            };

        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);
        let union_provider =
            UnionTableProvider::new(memtable_provider, parquet_provider, full_name.clone())
                .map_err(|e| {
                    format!("Failed to create union provider for '{}': {}", full_name, e)
                })?;

        let stats = cache.get_table_cache_stats(catalog_name, schema_name, table_name);
        debug!(
            "Created memtable union for {}: {} cached rows in {} batches",
            full_name, stats.total_rows, stats.total_batches
        );

        Ok(Arc::new(union_provider))
    }

    /// Register a table with only memtable data (no Parquet files on disk)
    async fn register_memtable_only(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let cache = self
            .write_cache
            .as_ref()
            .ok_or_else(|| "Write cache not available".to_string())?;

        let memtable_provider =
            MemtableProvider::try_new(cache.clone(), catalog_name, schema_name, table_name)
                .ok_or_else(|| {
                    format!(
                        "No cached data available for {}.{}.{}",
                        catalog_name, schema_name, table_name
                    )
                })?;

        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);
        let stats = cache.get_table_cache_stats(catalog_name, schema_name, table_name);

        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog
            .schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        schema
            .register_table(table_name.to_string(), Arc::new(memtable_provider))
            .map_err(|e| {
                format!(
                    "Failed to register memtable '{}.{}.{}': {}",
                    catalog_name, schema_name, table_name, e
                )
            })?;

        debug!(
            "Registered memtable-only table {}: {} cached rows in {} batches",
            full_name, stats.total_rows, stats.total_batches
        );

        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

        Ok(())
    }

    // ========================================================================
    // Partition & File Discovery
    // ========================================================================

    /// Discover date partitions in a table directory (with caching)
    fn discover_table_partitions(&self, table_path: &str) -> Result<Vec<String>, String> {
        let path = std::path::Path::new(table_path);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let path_buf = path.to_path_buf();

        // Check file listing cache first
        if let Some(cached) = self.metadata_cache.get_file_listing(&path_buf) {
            // Extract partition names from cached file paths
            let partitions: Vec<String> = cached
                .files
                .iter()
                .filter_map(|p| {
                    p.file_name()
                        .and_then(|s| s.to_str())
                        .filter(|s| s.starts_with("date="))
                        .map(|s| s.to_string())
                })
                .collect();
            if !partitions.is_empty() {
                return Ok(partitions);
            }
        }

        let entries = std::fs::read_dir(path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        let mut partitions = Vec::new();
        let mut all_entries = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let entry_path = entry.path();
            all_entries.push(entry_path.clone());

            if entry_path.is_dir() {
                if let Some(dir_name) = entry_path.file_name().and_then(|s| s.to_str()) {
                    // Check for Hive-style partition format: date=YYYY-MM-DD
                    if dir_name.starts_with("date=") {
                        partitions.push(dir_name.to_string());
                    }
                }
            }
        }

        // Cache the directory listing
        self.metadata_cache.set_file_listing(path_buf, all_entries);

        partitions.sort();
        Ok(partitions)
    }

    /// Count Parquet files in a directory (with caching)
    fn count_parquet_files_cached(&self, dir_path: &str) -> usize {
        let path = PathBuf::from(dir_path);

        // Check cache first
        if let Some(cached) = self.metadata_cache.get_file_listing(&path) {
            return cached
                .files
                .iter()
                .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
                .count();
        }

        // Cache miss - scan and cache
        if let Ok(entries) = std::fs::read_dir(&path) {
            let files: Vec<PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();

            let parquet_count = files
                .iter()
                .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
                .count();

            self.metadata_cache.set_file_listing(path, files);
            parquet_count
        } else {
            0
        }
    }

    /// Read and cache Parquet metadata for a file
    /// Returns cached metadata if available, otherwise reads from disk and caches
    pub fn get_parquet_metadata_cached(&self, file_path: &PathBuf) -> Option<Arc<ParquetMetaData>> {
        // Check cache first
        if let Some(metadata) = self.metadata_cache.get_parquet_metadata(file_path) {
            return Some(metadata);
        }

        // Cache miss - read from disk
        match self.read_parquet_metadata(file_path) {
            Ok((metadata, file_size)) => {
                let arc_metadata = Arc::new(metadata);
                self.metadata_cache.set_parquet_metadata(
                    file_path.clone(),
                    arc_metadata.clone(),
                    file_size,
                );
                Some(arc_metadata)
            }
            Err(e) => {
                warn!("Failed to read Parquet metadata for {:?}: {}", file_path, e);
                None
            }
        }
    }

    /// Read Parquet metadata from disk
    /// Read Parquet metadata from disk
    fn read_parquet_metadata(&self, file_path: &PathBuf) -> Result<(ParquetMetaData, u64), String> {
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;
        use std::fs::File;

        let file = File::open(file_path).map_err(|e| format!("Failed to open file: {}", e))?;

        let file_size = file
            .metadata()
            .map_err(|e| format!("Failed to get file metadata: {}", e))?
            .len();

        let reader = SerializedFileReader::new(file)
            .map_err(|e| format!("Failed to create Parquet reader: {}", e))?;

        let metadata = reader.metadata().clone();
        Ok((metadata, file_size))
    }

    // ========================================================================
    // Cache Pre-warming
    // ========================================================================

    /// Pre-warm the metadata cache for a table
    /// This reads all Parquet file footers and caches them for faster query execution
    pub async fn prewarm_table_cache(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<usize, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
        let path = std::path::Path::new(&table_path);

        if !path.exists() {
            return Err(format!("Table path not found: {}", table_path));
        }

        let mut cached_count = 0;

        // Discover partitions
        let partitions = self.discover_table_partitions(&table_path)?;
        let has_partitions = !partitions.is_empty();

        if partitions.is_empty() {
            // Non-partitioned table - cache files directly
            cached_count += self.prewarm_directory_cache(&table_path)?;
        } else {
            // Partitioned table - cache files in each partition
            for partition in &partitions {
                let partition_path = format!("{}/{}", table_path, partition);
                cached_count += self.prewarm_directory_cache(&partition_path)?;
            }
        }

        // Also cache the table info
        self.metadata_cache
            .set_table_info(catalog, schema, table, has_partitions, partitions);

        info!(
            "Pre-warmed cache for {}.{}.{}: {} Parquet files cached",
            catalog, schema, table, cached_count
        );
        Ok(cached_count)
    }

    /// Pre-warm cache for all Parquet files in a directory
    fn prewarm_directory_cache(&self, dir_path: &str) -> Result<usize, String> {
        let path = PathBuf::from(dir_path);
        let entries =
            std::fs::read_dir(&path).map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut cached_count = 0;
        let mut all_files = Vec::new();

        for entry in entries.filter_map(|e| e.ok()) {
            let file_path = entry.path();
            all_files.push(file_path.clone());

            if file_path
                .extension()
                .map(|e| e == "parquet")
                .unwrap_or(false)
            {
                // Cache the Parquet metadata
                if self.get_parquet_metadata_cached(&file_path).is_some() {
                    cached_count += 1;
                }
            }
        }

        // Cache the file listing
        self.metadata_cache.set_file_listing(path, all_files);

        Ok(cached_count)
    }

    /// Pre-warm cache for an entire catalog (database)
    /// Pre-warm cache for an entire catalog (database)
    pub async fn prewarm_catalog_cache(&self, catalog: &str) -> Result<usize, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog);

        if !std::path::Path::new(&catalog_path).exists() {
            return Ok(0);
        }

        let mut total_cached = 0;

        // Iterate through schemas
        let schema_entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        for schema_entry in schema_entries.filter_map(|e| e.ok()) {
            let schema_path = schema_entry.path();
            if !schema_path.is_dir() {
                continue;
            }

            let schema_name = match schema_path.file_name().and_then(|s| s.to_str()) {
                Some(name) if !name.starts_with('.') => name,
                _ => continue,
            };

            // Iterate through tables
            let table_entries = match std::fs::read_dir(&schema_path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            for table_entry in table_entries.filter_map(|e| e.ok()) {
                let table_path = table_entry.path();
                if !table_path.is_dir() {
                    continue;
                }

                let table_name = match table_path.file_name().and_then(|s| s.to_str()) {
                    Some(name) if !name.starts_with('.') => name,
                    _ => continue,
                };

                match self
                    .prewarm_table_cache(catalog, schema_name, table_name)
                    .await
                {
                    Ok(count) => total_cached += count,
                    Err(e) => warn!(
                        "Failed to prewarm {}.{}.{}: {}",
                        catalog, schema_name, table_name, e
                    ),
                }
            }
        }

        info!(
            "Pre-warmed cache for catalog '{}': {} total Parquet files cached",
            catalog, total_cached
        );
        Ok(total_cached)
    }

    /// Get statistics about Parquet files in a table (using cached metadata)
    pub fn get_table_parquet_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableParquetStats, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
        let path = std::path::Path::new(&table_path);

        if !path.exists() {
            return Err(format!("Table path not found: {}", table_path));
        }

        let mut total_files = 0;
        let mut total_rows = 0u64;
        let mut total_size = 0u64;
        let mut total_row_groups = 0;

        let partitions = self.discover_table_partitions(&table_path)?;

        let dirs_to_scan: Vec<String> = if partitions.is_empty() {
            vec![table_path.clone()]
        } else {
            partitions
                .iter()
                .map(|p| format!("{}/{}", table_path, p))
                .collect()
        };

        for dir in dirs_to_scan {
            let path = PathBuf::from(&dir);
            if let Ok(entries) = std::fs::read_dir(&path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let file_path = entry.path();
                    if file_path
                        .extension()
                        .map(|e| e == "parquet")
                        .unwrap_or(false)
                    {
                        total_files += 1;

                        if let Ok(metadata) = std::fs::metadata(&file_path) {
                            total_size += metadata.len();
                        }

                        // Get row count from cached Parquet metadata
                        if let Some(pq_meta) = self.get_parquet_metadata_cached(&file_path) {
                            total_rows += pq_meta.file_metadata().num_rows() as u64;
                            total_row_groups += pq_meta.num_row_groups();
                        }
                    }
                }
            }
        }

        Ok(TableParquetStats {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            total_files,
            total_rows,
            total_size_bytes: total_size,
            total_row_groups,
            partition_count: partitions.len(),
        })
    }

    // ========================================================================
    // Auto-discovery
    // ========================================================================

    /// Auto-discover and register all tables in catalog.schema from disk
    pub async fn discover_and_register(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<usize, String> {
        let schema_path = format!("{}/{}/{}", self.data_dir, catalog_name, schema_name);

        if !std::path::Path::new(&schema_path).exists() {
            return Ok(0);
        }

        let entries = std::fs::read_dir(&schema_path)
            .map_err(|e| format!("Failed to read schema directory: {}", e))?;

        let mut count = 0;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !table_name.starts_with('.') {
                        if let Err(e) = self
                            .register_table(catalog_name, schema_name, table_name)
                            .await
                        {
                            warn!(
                                "Failed to register {}.{}.{}: {}",
                                catalog_name, schema_name, table_name, e
                            );
                        } else {
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(count)
    }

    /// Auto-discover all schemas in a catalog
    pub async fn discover_catalog(&self, catalog_name: &str) -> Result<usize, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog_name);

        if !std::path::Path::new(&catalog_path).exists() {
            return Ok(0);
        }

        let entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        let mut total_tables = 0;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(schema_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !schema_name.starts_with('.') {
                        match self.discover_and_register(catalog_name, schema_name).await {
                            Ok(count) => total_tables += count,
                            Err(e) => warn!(
                                "Failed to discover schema {}.{}: {}",
                                catalog_name, schema_name, e
                            ),
                        }
                    }
                }
            }
        }

        Ok(total_tables)
    }

    /// Ensure a schema is tracked in registered catalogs
    pub async fn ensure_schema(&self, catalog_name: &str, schema_name: &str) -> Result<(), String> {
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());
        Ok(())
    }

    /// Ensure all tables in a database are registered
    pub async fn ensure_database_registered(&self, db_name: &str) -> Result<(), String> {
        let count = self.discover_catalog(db_name).await?;
        if count > 0 {
            info!(
                "Auto-registered {} table(s) in catalog '{}'",
                count, db_name
            );
        }
        Ok(())
    }

    // ========================================================================
    // Query Execution
    // ========================================================================

    /// Execute a SQL query with database context
    /// Handles table registration, qualification, GROUP BY time() transformation, and caching
    pub async fn execute_query_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<Vec<RecordBatch>, String> {
        let cache_key = QueryCacheKey::new(db_name, sql);
        if let Some(cached_result) = self.query_cache.get(&cache_key) {
            debug!("Query cache HIT for db={}", db_name);
            return Ok(cached_result);
        }

        let table_refs = extract_table_references(sql, db_name);

        for (catalog, schema, table) in &table_refs {
            let _ = self.register_table(catalog, schema, table).await;
        }

        let qualified_sql = qualify_table_names(sql, db_name);
        let optimized_sql = inject_date_predicates(&qualified_sql);

        let transformer = GroupByTimeTransformer::new();
        let transformed = transformer
            .transform(&optimized_sql)
            .map_err(|e| format!("GROUP BY time() transformation error: {}", e))?;

        let final_sql = &transformed.sql;

        debug!("SQL: {}", sql);
        if qualified_sql != sql {
            debug!("Qualified: {}", qualified_sql);
        }
        if optimized_sql != qualified_sql {
            debug!("Optimized (date predicates injected): {}", optimized_sql);
        }
        if transformed.was_transformed {
            debug!("GROUP BY time() transformed: {}", final_sql);
        }

        let start_time = Instant::now();
        let mut result = self.execute_query(final_sql).await?;
        let query_time_ms = start_time.elapsed().as_millis() as u64;

        if transformed.was_transformed && !matches!(transformed.fill_option, FillOption::None) {
            if let Some(interval_nanos) = transformed.interval_nanos {
                let time_col_idx = if !result.is_empty() {
                    result[0]
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == "time")
                        .unwrap_or(0)
                } else {
                    0
                };

                let group_col_indices: Vec<usize> = if !result.is_empty() {
                    transformed
                        .group_columns
                        .iter()
                        .filter_map(|col_name| {
                            result[0]
                                .schema()
                                .fields()
                                .iter()
                                .position(|f| f.name() == col_name)
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                let processor = FillProcessor::new(
                    interval_nanos,
                    transformed.fill_option.clone(),
                    time_col_idx,
                    group_col_indices,
                );
                result = processor
                    .apply(result)
                    .map_err(|e| format!("Fill processing error: {}", e))?;
            }
        }

        let referenced_tables: Vec<String> = table_refs
            .iter()
            .map(|(c, s, t)| format!("{}.{}.{}", c, s, t))
            .collect();

        self.query_cache
            .put(cache_key, result.clone(), referenced_tables, query_time_ms);

        Ok(result)
    }

    /// Execute a raw SQL query without database context
    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Execution error: {}", e))?;
        Ok(batches)
    }

    /// Get the schema for a query without executing it
    pub async fn get_query_schema(
        &self,
        db_name: &str,
        sql: &str,
    ) -> Result<Arc<datafusion::arrow::datatypes::Schema>, String> {
        self.ensure_database_registered(db_name).await?;
        let qualified_sql = qualify_table_names(sql, db_name);
        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        Ok(df.schema().inner().clone())
    }

    // ========================================================================
    // EXPLAIN & ANALYZE
    // ========================================================================

    /// Explain a query's execution plan
    pub async fn explain_query(
        &self,
        sql: &str,
        db_name: &str,
        verbose: bool,
        analyze: bool,
    ) -> Result<QueryExplainResult, String> {
        // Register tables referenced in the query
        let table_refs = extract_table_references(sql, db_name);
        for (catalog, schema, table) in table_refs {
            let _ = self.register_table(&catalog, &schema, &table).await;
        }

        // Qualify table names and create DataFrame
        let qualified_sql = qualify_table_names(sql, db_name);
        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let logical_plan = format!("{:?}", df.logical_plan());

        // Get explain output
        let explain_df = if analyze {
            df.explain(verbose, true)
                .map_err(|e| format!("Explain error: {}", e))?
        } else {
            df.explain(verbose, false)
                .map_err(|e| format!("Explain error: {}", e))?
        };

        let batches = explain_df
            .collect()
            .await
            .map_err(|e| format!("Collect error: {}", e))?;

        let mut plan_lines = Vec::new();
        for batch in &batches {
            let plan_type_col = batch.column(0);
            let plan_col = batch.column(1);

            let plan_types = plan_type_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or("Failed to get plan_type column")?;
            let plans = plan_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or("Failed to get plan column")?;

            for i in 0..batch.num_rows() {
                let plan_type = plans.is_valid(i).then(|| plan_types.value(i)).unwrap_or("");
                let plan = plans.is_valid(i).then(|| plans.value(i)).unwrap_or("");
                plan_lines.push(format!("{}:\n{}", plan_type, plan));
            }
        }

        let physical_plan = plan_lines.join("\n\n");
        let optimizations = extract_optimization_info(&physical_plan);

        Ok(QueryExplainResult {
            sql: sql.to_string(),
            qualified_sql,
            logical_plan,
            physical_plan,
            optimizations,
        })
    }

    /// Analyze a query and return statistics about tables, partitions, and files
    pub async fn analyze_query(&self, sql: &str, db_name: &str) -> Result<QueryAnalysis, String> {
        let explain = self.explain_query(sql, db_name, true, false).await?;
        let time_range = extract_time_range(sql);
        let table_refs = extract_table_references(sql, db_name);

        let mut partition_count = 0;
        let mut total_files = 0;

        for (catalog, schema, table) in &table_refs {
            let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
            if let Ok(partitions) = self.discover_table_partitions(&table_path) {
                if let Some(ref tr) = time_range {
                    for partition in &partitions {
                        if let Some(date) = partition.strip_prefix("date=") {
                            let include = match (&tr.start_date, &tr.end_date) {
                                (Some(start), Some(end)) => {
                                    date >= start.as_str() && date <= end.as_str()
                                }
                                (Some(start), None) => date >= start.as_str(),
                                (None, Some(end)) => date <= end.as_str(),
                                (None, None) => true,
                            };
                            if include {
                                partition_count += 1;
                                let partition_path = format!("{}/{}", table_path, partition);
                                total_files += self.count_parquet_files_cached(&partition_path);
                            }
                        }
                    }
                } else {
                    partition_count += partitions.len();
                    for partition in &partitions {
                        let partition_path = format!("{}/{}", table_path, partition);
                        total_files += self.count_parquet_files_cached(&partition_path);
                    }
                }
            }
        }

        Ok(QueryAnalysis {
            sql: sql.to_string(),
            tables: table_refs
                .iter()
                .map(|(c, s, t)| format!("{}.{}.{}", c, s, t))
                .collect(),
            time_range,
            partitions_to_scan: partition_count,
            files_to_scan: total_files,
            optimizations: explain.optimizations,
            plan_summary: summarize_plan(&explain.physical_plan),
        })
    }

    // ========================================================================
    // JSON Output
    // ========================================================================

    /// Execute query and return as JSON (with DB context)
    pub async fn execute_query_json_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<String, String> {
        let batches = self.execute_query_with_db(sql, db_name).await?;

        if batches.is_empty() {
            return Ok("[]".to_string());
        }

        // Convert to JSON
        let mut results = Vec::new();
        for batch in batches {
            let json_rows = Self::batch_to_json(&batch)?;
            results.extend(json_rows);
        }

        serde_json::to_string_pretty(&results)
            .map_err(|e| format!("JSON serialization error: {}", e))
    }

    /// Convert RecordBatch to JSON objects
    fn batch_to_json(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, String> {
        let mut rows = Vec::new();
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut row_obj = serde_json::Map::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = Self::array_value_to_json(column, row_idx)?;
                row_obj.insert(field.name().clone(), value);
            }

            rows.push(serde_json::Value::Object(row_obj));
        }

        Ok(rows)
    }

    /// Convert Arrow array value to JSON
    fn array_value_to_json(
        array: &Arc<dyn datafusion::arrow::array::Array>,
        index: usize,
    ) -> Result<serde_json::Value, String> {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::{DataType, UInt16Type};

        if array.is_null(index) {
            return Ok(serde_json::Value::Null);
        }

        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Utf8View => {
                let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Timestamp(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Dictionary(_, value_type) => {
                // Handle dictionary-encoded strings (used for partition columns)
                match value_type.as_ref() {
                    DataType::Utf8 => {
                        let dict_array = array
                            .as_any()
                            .downcast_ref::<DictionaryArray<UInt16Type>>()
                            .unwrap();
                        let keys = dict_array.keys();
                        let key = keys.value(index);
                        let values = dict_array
                            .values()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        Ok(serde_json::json!(values.value(key as usize)))
                    }
                    _ => Ok(serde_json::json!(format!(
                        "Unsupported dictionary value type: {:?}",
                        value_type
                    ))),
                }
            }
            _ => Ok(serde_json::json!(format!(
                "Unsupported type: {:?}",
                array.data_type()
            ))),
        }
    }

    // ========================================================================
    // Schema & Table Listing
    // ========================================================================

    /// List all registered tables across all catalogs
    pub async fn list_tables(&self) -> Vec<String> {
        let mut all_tables = Vec::new();

        for catalog_name in self.ctx.catalog_names() {
            if let Some(catalog) = self.ctx.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            all_tables
                                .push(format!("{}.{}.{}", catalog_name, schema_name, table_name));
                        }
                    }
                }
            }
        }

        all_tables.sort();
        all_tables
    }

    /// Get schema for a table (supports both qualified "db.table" and unqualified "table")
    pub async fn get_table_schema(&self, table_name: &str) -> Result<String, String> {
        let table = self
            .ctx
            .table(table_name)
            .await
            .map_err(|e| format!("Table not found: {}", e))?;

        let schema = table.schema();
        Ok(format!("{:#?}", schema))
    }

    /// List all schemas (services) in a catalog
    pub async fn list_schemas(&self, catalog_name: &str) -> Result<Vec<String>, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog_name);

        if !std::path::Path::new(&catalog_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        let mut schemas = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(schema_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !schema_name.starts_with('.') && schema_name != "_system" {
                        schemas.push(schema_name.to_string());
                    }
                }
            }
        }

        schemas.sort();
        Ok(schemas)
    }

    /// List all tables in a catalog.schema
    pub async fn list_tables_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, String> {
        let schema_path = format!("{}/{}/{}", self.data_dir, catalog_name, schema_name);

        if !std::path::Path::new(&schema_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&schema_path)
            .map_err(|e| format!("Failed to read schema directory: {}", e))?;

        let mut tables = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !table_name.starts_with('.') {
                        tables.push(table_name.to_string());
                    }
                }
            }
        }

        tables.sort();
        Ok(tables)
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
