use crate::query::create_custom_session_state;
use crate::query::fill_processor::{FillError, FillProcessor};
use crate::query::functions::create_time_bucket_udf;
use crate::query::group_by_time::GroupByTimeTransformer;
use crate::query::table_provider::{ParquetTableConfig, ParquetTableProvider};
use crate::query::time_interval::FillOption;
use crate::query_cache::{QueryCacheConfig, QueryCacheKey, QueryCacheStats, QueryResultCache, SharedQueryResultCache};
use crate::storage::storage::Storage;
use chrono::{DateTime, TimeZone, Utc};
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
use parking_lot::RwLock as ParkingRwLock;
use parquet::file::metadata::ParquetMetaData;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

const DEFAULT_SCHEMA: &str = "default";

/// Cache TTL for table metadata (5 minutes)
const TABLE_CACHE_TTL_SECS: u64 = 300;

/// Nanoseconds per second
const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Nanoseconds per millisecond
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Cache TTL for file listings (60 seconds)
const FILE_LISTING_CACHE_TTL_SECS: u64 = 60;

/// Maximum entries in Parquet metadata cache
const PARQUET_METADATA_CACHE_MAX_ENTRIES: usize = 10000;

/// Cached table information
#[derive(Debug, Clone)]
struct CachedTableInfo {
    /// When this entry was cached
    cached_at: Instant,
    /// Table version (incremented on writes)
    version: u64,
    /// Whether the table has partitions
    has_partitions: bool,
    /// List of partition directories (date=YYYY-MM-DD)
    partitions: Vec<String>,
}

/// Cached file listing for a directory
#[derive(Debug, Clone)]
struct CachedFileListing {
    /// When this listing was cached
    cached_at: Instant,
    /// List of Parquet files
    files: Vec<PathBuf>,
    /// Total file count
    file_count: usize,
}

/// Parquet metadata cache entry
#[derive(Debug)]
struct CachedParquetMetadata {
    /// The cached metadata
    metadata: Arc<ParquetMetaData>,
    /// When this was cached
    cached_at: Instant,
    /// File size at cache time (for invalidation)
    file_size: u64,
}

/// Table metadata cache for fast lookups
pub struct TableMetadataCache {
    /// Table info cache: (catalog, schema, table) -> CachedTableInfo
    tables: ParkingRwLock<HashMap<(String, String, String), CachedTableInfo>>,
    /// File listing cache: directory path -> CachedFileListing
    file_listings: ParkingRwLock<HashMap<PathBuf, CachedFileListing>>,
    /// Parquet metadata cache: file path -> CachedParquetMetadata
    parquet_metadata: ParkingRwLock<HashMap<PathBuf, CachedParquetMetadata>>,
    /// Global version counter (incremented on any write)
    global_version: AtomicU64,
    /// Cache statistics
    stats: CacheStats,
}

/// Cache statistics for monitoring
#[derive(Debug, Default)]
struct CacheStats {
    table_hits: AtomicU64,
    table_misses: AtomicU64,
    file_listing_hits: AtomicU64,
    file_listing_misses: AtomicU64,
    parquet_metadata_hits: AtomicU64,
    parquet_metadata_misses: AtomicU64,
}

impl TableMetadataCache {
    pub fn new() -> Self {
        Self {
            tables: ParkingRwLock::new(HashMap::new()),
            file_listings: ParkingRwLock::new(HashMap::new()),
            parquet_metadata: ParkingRwLock::new(HashMap::new()),
            global_version: AtomicU64::new(0),
            stats: CacheStats::default(),
        }
    }

    /// Get cached table info if valid
    pub fn get_table_info(&self, catalog: &str, schema: &str, table: &str) -> Option<CachedTableInfo> {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        let tables = self.tables.read();

        if let Some(info) = tables.get(&key) {
            // Check TTL
            if info.cached_at.elapsed() < Duration::from_secs(TABLE_CACHE_TTL_SECS) {
                self.stats.table_hits.fetch_add(1, Ordering::Relaxed);
                return Some(info.clone());
            }
        }
        self.stats.table_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache table info
    pub fn set_table_info(&self, catalog: &str, schema: &str, table: &str, has_partitions: bool, partitions: Vec<String>) {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        let info = CachedTableInfo {
            cached_at: Instant::now(),
            version: self.global_version.load(Ordering::Relaxed),
            has_partitions,
            partitions,
        };
        self.tables.write().insert(key, info);
    }

    /// Get cached file listing if valid
    pub fn get_file_listing(&self, dir: &PathBuf) -> Option<CachedFileListing> {
        let listings = self.file_listings.read();

        if let Some(listing) = listings.get(dir) {
            if listing.cached_at.elapsed() < Duration::from_secs(FILE_LISTING_CACHE_TTL_SECS) {
                self.stats.file_listing_hits.fetch_add(1, Ordering::Relaxed);
                return Some(listing.clone());
            }
        }
        self.stats.file_listing_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache file listing
    pub fn set_file_listing(&self, dir: PathBuf, files: Vec<PathBuf>) {
        let listing = CachedFileListing {
            cached_at: Instant::now(),
            file_count: files.len(),
            files,
        };
        self.file_listings.write().insert(dir, listing);
    }

    /// Get cached Parquet metadata if valid
    pub fn get_parquet_metadata(&self, file: &PathBuf) -> Option<Arc<ParquetMetaData>> {
        let cache = self.parquet_metadata.read();

        if let Some(entry) = cache.get(file) {
            // Check if file size changed (quick invalidation check)
            if let Ok(metadata) = std::fs::metadata(file) {
                if metadata.len() == entry.file_size {
                    self.stats.parquet_metadata_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(entry.metadata.clone());
                }
            }
        }
        self.stats.parquet_metadata_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache Parquet metadata
    pub fn set_parquet_metadata(&self, file: PathBuf, metadata: Arc<ParquetMetaData>, file_size: u64) {
        let mut cache = self.parquet_metadata.write();

        // Evict old entries if cache is too large
        if cache.len() >= PARQUET_METADATA_CACHE_MAX_ENTRIES {
            // Remove oldest 10% of entries
            let to_remove: Vec<PathBuf> = cache.iter()
                .map(|(k, v)| (k.clone(), v.cached_at))
                .collect::<Vec<_>>()
                .into_iter()
                .take(PARQUET_METADATA_CACHE_MAX_ENTRIES / 10)
                .map(|(k, _)| k)
                .collect();

            for key in to_remove {
                cache.remove(&key);
            }
        }

        cache.insert(file, CachedParquetMetadata {
            metadata,
            cached_at: Instant::now(),
            file_size,
        });
    }

    /// Invalidate cache for a specific table (called after writes)
    pub fn invalidate_table(&self, catalog: &str, schema: &str, table: &str) {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        self.tables.write().remove(&key);

        // Increment global version
        self.global_version.fetch_add(1, Ordering::Relaxed);

        debug!("Invalidated cache for table: {}.{}.{}", catalog, schema, table);
    }

    /// Invalidate all cache entries for a catalog (database)
    pub fn invalidate_catalog(&self, catalog: &str) {
        let mut tables = self.tables.write();
        tables.retain(|(c, _, _), _| c != catalog);

        self.global_version.fetch_add(1, Ordering::Relaxed);
        debug!("Invalidated cache for catalog: {}", catalog);
    }

    /// Invalidate file listing cache for a directory
    pub fn invalidate_file_listing(&self, dir: &PathBuf) {
        self.file_listings.write().remove(dir);
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStatistics {
        CacheStatistics {
            table_cache_size: self.tables.read().len(),
            file_listing_cache_size: self.file_listings.read().len(),
            parquet_metadata_cache_size: self.parquet_metadata.read().len(),
            table_hits: self.stats.table_hits.load(Ordering::Relaxed),
            table_misses: self.stats.table_misses.load(Ordering::Relaxed),
            file_listing_hits: self.stats.file_listing_hits.load(Ordering::Relaxed),
            file_listing_misses: self.stats.file_listing_misses.load(Ordering::Relaxed),
            parquet_metadata_hits: self.stats.parquet_metadata_hits.load(Ordering::Relaxed),
            parquet_metadata_misses: self.stats.parquet_metadata_misses.load(Ordering::Relaxed),
        }
    }

    /// Clear all caches
    pub fn clear(&self) {
        self.tables.write().clear();
        self.file_listings.write().clear();
        self.parquet_metadata.write().clear();
        self.global_version.fetch_add(1, Ordering::Relaxed);
        info!("All query caches cleared");
    }
}

impl Default for TableMetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatistics {
    pub table_cache_size: usize,
    pub file_listing_cache_size: usize,
    pub parquet_metadata_cache_size: usize,
    pub table_hits: u64,
    pub table_misses: u64,
    pub file_listing_hits: u64,
    pub file_listing_misses: u64,
    pub parquet_metadata_hits: u64,
    pub parquet_metadata_misses: u64,
}

impl CacheStatistics {
    pub fn table_hit_rate(&self) -> f64 {
        let total = self.table_hits + self.table_misses;
        if total == 0 { 0.0 } else { self.table_hits as f64 / total as f64 * 100.0 }
    }

    pub fn parquet_hit_rate(&self) -> f64 {
        let total = self.parquet_metadata_hits + self.parquet_metadata_misses;
        if total == 0 { 0.0 } else { self.parquet_metadata_hits as f64 / total as f64 * 100.0 }
    }
}

/// Statistics about Parquet files in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableParquetStats {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub total_files: usize,
    pub total_rows: u64,
    pub total_size_bytes: u64,
    pub total_row_groups: usize,
    pub partition_count: usize,
}

/// Query optimization flags detected from the execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptimizations {
    /// Whether predicate push-down to Parquet is used
    pub predicate_pushdown: bool,
    /// Whether partition pruning is applied
    pub partition_pruning: bool,
    /// Whether row group pruning using statistics is enabled
    pub row_group_pruning: bool,
    /// Whether projection push-down is used
    pub projection_pushdown: bool,
    /// Whether LIMIT is pushed down
    pub limit_pushdown: bool,
}

/// Time range extracted from query WHERE clause
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp (nanoseconds since epoch)
    pub start_timestamp: Option<i64>,
    /// End timestamp (nanoseconds since epoch)
    pub end_timestamp: Option<i64>,
    /// Start date for partition filtering (YYYY-MM-DD)
    pub start_date: Option<String>,
    /// End date for partition filtering (YYYY-MM-DD)
    pub end_date: Option<String>,
}

impl TimeRange {
    /// Convert a nanosecond timestamp to a date string (YYYY-MM-DD)
    pub fn timestamp_nanos_to_date(timestamp_nanos: i64) -> String {
        let secs = timestamp_nanos / NANOS_PER_SEC;
        let datetime = DateTime::from_timestamp(secs, 0)
            .unwrap_or_else(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
        datetime.format("%Y-%m-%d").to_string()
    }

    /// Convert a millisecond timestamp to a date string (YYYY-MM-DD)
    pub fn timestamp_millis_to_date(timestamp_millis: i64) -> String {
        Self::timestamp_nanos_to_date(timestamp_millis * NANOS_PER_MILLI)
    }

    /// Check if a timestamp value looks like milliseconds (13 digits) vs nanoseconds (19 digits)
    pub fn is_likely_millis(value: i64) -> bool {
        // Milliseconds: 13 digits (e.g., 1704067200000)
        // Nanoseconds: 19 digits (e.g., 1704067200000000000)
        // If value is < 10^15, it's likely milliseconds
        value > 0 && value < 1_000_000_000_000_000
    }

    /// Convert timestamp to date, auto-detecting milliseconds vs nanoseconds
    pub fn timestamp_to_date(value: i64) -> String {
        if Self::is_likely_millis(value) {
            Self::timestamp_millis_to_date(value)
        } else {
            Self::timestamp_nanos_to_date(value)
        }
    }

    /// Get the derived date range from timestamps (for partition pruning)
    pub fn get_date_range(&self) -> (Option<String>, Option<String>) {
        let start = self.start_date.clone().or_else(|| {
            self.start_timestamp.map(Self::timestamp_to_date)
        });
        let end = self.end_date.clone().or_else(|| {
            self.end_timestamp.map(Self::timestamp_to_date)
        });
        (start, end)
    }
}

/// Result of EXPLAIN query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplainResult {
    /// Original SQL query
    pub sql: String,
    /// Qualified SQL (with full table paths)
    pub qualified_sql: String,
    /// Logical execution plan
    pub logical_plan: String,
    /// Physical execution plan
    pub physical_plan: String,
    /// Detected optimizations
    pub optimizations: QueryOptimizations,
}

/// Query analysis result with statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAnalysis {
    /// Original SQL query
    pub sql: String,
    /// Tables referenced in query
    pub tables: Vec<String>,
    /// Extracted time range (if any)
    pub time_range: Option<TimeRange>,
    /// Number of partitions to scan
    pub partitions_to_scan: usize,
    /// Number of Parquet files to scan
    pub files_to_scan: usize,
    /// Detected optimizations
    pub optimizations: QueryOptimizations,
    /// Brief summary of execution plan
    pub plan_summary: String,
}

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
    storage: Option<Arc<Storage>>,
    // Track registered catalogs (databases) and their schemas (services)
    registered_catalogs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    // Metadata cache for tables and Parquet files
    metadata_cache: Arc<TableMetadataCache>,
    // Query result cache
    query_cache: SharedQueryResultCache,
    // Use custom ParquetTableProvider instead of ListingTable
    use_custom_table_provider: bool,
}

/// Default memory limit for query execution (16GB)
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 16 * 1024 * 1024 * 1024;
/// Memory threshold fraction before spilling to disk (80%)
const MEMORY_LIMIT_FRACTION: f64 = 0.8;

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
        config.execution.parquet.maximum_buffered_record_batches_per_stream = 4;

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
                warn!("Failed to create RuntimeEnv with memory limits: {}, using default", e);
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

        ctx
    }

    pub fn new(data_dir: &str) -> Self {
        Self::new_with_options(data_dir, None, QueryCacheConfig::default(), false)
    }

    pub fn new_with_query_cache(data_dir: &str, storage: Option<Arc<Storage>>, query_cache_config: QueryCacheConfig) -> Self {
        Self::new_with_options(data_dir, storage, query_cache_config, false)
    }

    pub fn new_with_options(
        data_dir: &str,
        storage: Option<Arc<Storage>>,
        query_cache_config: QueryCacheConfig,
        use_custom_table_provider: bool,
    ) -> Self {
        let ctx = Self::create_optimized_context();
        let metadata_cache = Arc::new(TableMetadataCache::new());
        let query_cache = Arc::new(QueryResultCache::new(query_cache_config));

        // Register object store with DataFusion runtime if needed
        if let Some(ref storage) = storage {
            if !storage.is_local() {
                let url = url::Url::parse(storage.url_prefix())
                    .expect("Failed to parse storage URL prefix");
                ctx.runtime_env()
                    .register_object_store(&url, storage.object_store());
                info!("Registered object store with DataFusion: {}", storage.url_prefix());
            }
        }

        let storage_type = match &storage {
            Some(s) if s.is_local() => "local",
            Some(_) => "object_store",
            None => "local",
        };

        let table_provider_type = if use_custom_table_provider { "custom" } else { "listing" };
        info!("QueryEngine initialized: storage={}, table_provider={}, metadata_cache=enabled, query_cache={}",
              storage_type, table_provider_type, if query_cache.is_enabled() { "enabled" } else { "disabled" });

        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
            storage,
            registered_catalogs: Arc::new(RwLock::new(HashMap::new())),
            metadata_cache,
            query_cache,
            use_custom_table_provider,
        }
    }

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

    /// Invalidate cache for a table (call after writes)
    /// This invalidates both metadata cache and query result cache
    pub fn invalidate_table_cache(&self, catalog: &str, schema: &str, table: &str) {
        self.metadata_cache.invalidate_table(catalog, schema, table);
        self.query_cache.invalidate_table(catalog, schema, table);
    }

    /// Invalidate all caches for a database
    pub fn invalidate_database_cache(&self, db: &str) {
        self.metadata_cache.invalidate_catalog(db);
        self.query_cache.invalidate_database(db);
    }

    /// Clear all caches (metadata + query results)
    pub fn clear_all_caches(&self) {
        self.metadata_cache.clear();
        self.query_cache.clear();
    }

    /// Clear only query result cache
    pub fn clear_query_cache(&self) {
        self.query_cache.clear();
    }

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
        let table_url = if let Some(ref storage) = self.storage {
            storage.table_url(catalog_name, schema_name, table_name)
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

        // Check cache first for table info
        let (has_partitions, partitions) = if let Some(cached) = self.metadata_cache.get_table_info(catalog_name, schema_name, table_name) {
            // Use cached partition info
            debug!("Using cached table info for {}.{}.{}", catalog_name, schema_name, table_name);
            (cached.has_partitions, cached.partitions)
        } else {
            // Cache miss - need to scan directory
            // For local storage, check if path exists
            if self.storage.is_none() || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true) {
                if !std::path::Path::new(&table_path).exists() {
                    return Err(format!("Table path not found: {}", table_path));
                }
            }

            // Discover partitions
            let partitions = if self.storage.is_none() || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true) {
                self.discover_table_partitions(&table_path)?
            } else {
                // For object storage, assume partitioned
                Vec::new()
            };

            let has_partitions = if self.storage.is_none() || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true) {
                !partitions.is_empty()
            } else {
                true
            };

            // Cache the result
            self.metadata_cache.set_table_info(catalog_name, schema_name, table_name, has_partitions, partitions.clone());
            debug!("Cached table info for {}.{}.{} (partitions: {})", catalog_name, schema_name, table_name, partitions.len());

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
                catalog_name, schema_name, table_name, partitions.len()
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
            let table_ref = datafusion::sql::TableReference::full(catalog_name, schema_name, table_name);
            self.ctx
                .register_parquet(table_ref, table_path, ParquetReadOptions::default())
                .await
                .map_err(|e| format!("Failed to register table '{}.{}.{}': {}", catalog_name, schema_name, table_name, e))
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
            self.register_partitioned_table_listing(catalog_name, schema_name, table_name, table_path)
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
            .with_table_partition_cols(vec![("date".to_string(), datafusion::arrow::datatypes::DataType::Utf8)]);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| format!("Failed to infer schema: {}", e))?;

        let listing_table = ListingTable::try_new(config)
            .map_err(|e| format!("Failed to create listing table: {}", e))?;

        self.register_provider(catalog_name, schema_name, table_name, Arc::new(listing_table))
    }

    /// Helper to register a TableProvider in the catalog
    fn register_provider(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<(), String> {
        let catalog = self.ctx.catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog.schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        schema.register_table(table_name.to_string(), provider)
            .map_err(|e| format!("Failed to register table '{}.{}.{}': {}", catalog_name, schema_name, table_name, e))?;

        Ok(())
    }

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
            let partitions: Vec<String> = cached.files.iter()
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

        // Cache miss - scan directory
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
            return cached.files.iter()
                .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
                .count();
        }

        // Cache miss - scan and cache
        if let Ok(entries) = std::fs::read_dir(&path) {
            let files: Vec<PathBuf> = entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();

            let parquet_count = files.iter()
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
                self.metadata_cache.set_parquet_metadata(file_path.clone(), arc_metadata.clone(), file_size);
                Some(arc_metadata)
            }
            Err(e) => {
                warn!("Failed to read Parquet metadata for {:?}: {}", file_path, e);
                None
            }
        }
    }

    /// Read Parquet metadata from disk
    fn read_parquet_metadata(&self, file_path: &PathBuf) -> Result<(ParquetMetaData, u64), String> {
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;
        use std::fs::File;

        let file = File::open(file_path)
            .map_err(|e| format!("Failed to open file: {}", e))?;

        let file_size = file.metadata()
            .map_err(|e| format!("Failed to get file metadata: {}", e))?
            .len();

        let reader = SerializedFileReader::new(file)
            .map_err(|e| format!("Failed to create Parquet reader: {}", e))?;

        let metadata = reader.metadata().clone();
        Ok((metadata, file_size))
    }

    /// Pre-warm the metadata cache for a table
    /// This reads all Parquet file footers and caches them for faster query execution
    pub async fn prewarm_table_cache(&self, catalog: &str, schema: &str, table: &str) -> Result<usize, String> {
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
        self.metadata_cache.set_table_info(catalog, schema, table, has_partitions, partitions);

        info!("Pre-warmed cache for {}.{}.{}: {} Parquet files cached", catalog, schema, table, cached_count);
        Ok(cached_count)
    }

    /// Pre-warm cache for all Parquet files in a directory
    fn prewarm_directory_cache(&self, dir_path: &str) -> Result<usize, String> {
        let path = PathBuf::from(dir_path);

        let entries = std::fs::read_dir(&path)
            .map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut cached_count = 0;
        let mut all_files = Vec::new();

        for entry in entries.filter_map(|e| e.ok()) {
            let file_path = entry.path();
            all_files.push(file_path.clone());

            if file_path.extension().map(|e| e == "parquet").unwrap_or(false) {
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

                match self.prewarm_table_cache(catalog, schema_name, table_name).await {
                    Ok(count) => total_cached += count,
                    Err(e) => warn!("Failed to prewarm {}.{}.{}: {}", catalog, schema_name, table_name, e),
                }
            }
        }

        info!("Pre-warmed cache for catalog '{}': {} total Parquet files cached", catalog, total_cached);
        Ok(total_cached)
    }

    /// Get statistics about Parquet files in a table (using cached metadata)
    pub fn get_table_parquet_stats(&self, catalog: &str, schema: &str, table: &str) -> Result<TableParquetStats, String> {
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
            partitions.iter().map(|p| format!("{}/{}", table_path, p)).collect()
        };

        for dir in dirs_to_scan {
            let path = PathBuf::from(&dir);
            if let Ok(entries) = std::fs::read_dir(&path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let file_path = entry.path();
                    if file_path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        total_files += 1;

                        // Get file size
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

    /// Ensure schema exists (creates path if needed for write operations)
    pub async fn ensure_schema(&self, catalog_name: &str, schema_name: &str) -> Result<(), String> {
        // Just track it - actual registration happens when tables are written
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());
        Ok(())
    }

    /// Legacy compatibility: ensure database registered (now auto-discovers all schemas)
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

    /// Execute a SQL query with database context
    /// Automatically qualifies unqualified table names with catalog.schema
    /// Uses query result cache for repeated queries
    /// Injects date predicates for smart partition pruning when timestamp filters are present
    /// Supports InfluxDB-style GROUP BY time(interval) syntax
    pub async fn execute_query_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<Vec<RecordBatch>, String> {
        // Check query cache first
        let cache_key = QueryCacheKey::new(db_name, sql);
        if let Some(cached_result) = self.query_cache.get(&cache_key) {
            debug!("Query cache HIT for db={}", db_name);
            return Ok(cached_result);
        }

        // Extract table references from SQL and register only those tables
        let table_refs = self.extract_table_references(sql, db_name);

        for (catalog, schema, table) in &table_refs {
            // Try to register the table (will skip if already registered)
            // Ignore errors here - DataFusion will report if table doesn't exist
            let _ = self.register_table(catalog, schema, table).await;
        }

        // Qualify unqualified table names with catalog.schema
        let qualified_sql = self.qualify_table_names(sql, db_name);

        // Smart Partition Pruning: Inject date predicates based on timestamp filters
        // This converts timestamp filters to date partition filters for better pruning
        let optimized_sql = self.inject_date_predicates(&qualified_sql);

        // Transform GROUP BY time() syntax if present
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

        // Execute query and measure time
        let start_time = Instant::now();
        let mut result = self.execute_query(final_sql).await?;
        let query_time_ms = start_time.elapsed().as_millis() as u64;

        // Apply fill post-processing if needed
        if transformed.was_transformed && !matches!(transformed.fill_option, FillOption::None) {
            if let Some(interval_nanos) = transformed.interval_nanos {
                // Find the time column index (first column named "time")
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

                // Find group column indices
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

        // Cache the result
        let referenced_tables: Vec<String> = table_refs
            .iter()
            .map(|(c, s, t)| format!("{}.{}.{}", c, s, t))
            .collect();

        self.query_cache.put(cache_key, result.clone(), referenced_tables, query_time_ms);

        Ok(result)
    }

    /// Execute a SQL query (without DB context)
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

    /// Get the schema for a SQL query without executing it
    pub async fn get_query_schema(
        &self,
        db_name: &str,
        sql: &str,
    ) -> Result<Arc<datafusion::arrow::datatypes::Schema>, String> {
        // Ensure database is registered
        self.ensure_database_registered(db_name).await?;

        // Qualify table names
        let qualified_sql = self.qualify_table_names(sql, db_name);

        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        Ok(df.schema().inner().clone())
    }

    /// EXPLAIN a SQL query - returns the logical and physical execution plan
    /// This shows how DataFusion will execute the query including:
    /// - Predicate push-down decisions
    /// - Partition pruning
    /// - File scanning strategy
    pub async fn explain_query(
        &self,
        sql: &str,
        db_name: &str,
        verbose: bool,
        analyze: bool,
    ) -> Result<QueryExplainResult, String> {
        // Extract table references and register tables
        let table_refs = self.extract_table_references(sql, db_name);
        for (catalog, schema, table) in table_refs {
            let _ = self.register_table(&catalog, &schema, &table).await;
        }

        // Qualify table names
        let qualified_sql = self.qualify_table_names(sql, db_name);

        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        // Get logical plan
        let logical_plan = format!("{:?}", df.logical_plan());

        // Get physical plan explanation
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

        // Convert explain output to string
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

        // Extract optimization hints from the plan
        let physical_plan = plan_lines.join("\n\n");
        let optimizations = self.extract_optimization_info(&physical_plan);

        Ok(QueryExplainResult {
            sql: sql.to_string(),
            qualified_sql,
            logical_plan,
            physical_plan,
            optimizations,
        })
    }

    /// Extract optimization information from the physical plan
    fn extract_optimization_info(&self, plan: &str) -> QueryOptimizations {
        let plan_lower = plan.to_lowercase();

        QueryOptimizations {
            predicate_pushdown: plan_lower.contains("pushdown")
                || plan_lower.contains("predicate=")
                || plan_lower.contains("filter:"),
            partition_pruning: plan_lower.contains("partition")
                || plan_lower.contains("pruning")
                || plan_lower.contains("date="),
            row_group_pruning: plan_lower.contains("row_group")
                || plan_lower.contains("statistics")
                || plan_lower.contains("skip"),
            projection_pushdown: plan_lower.contains("projection=")
                || plan_lower.contains("project:"),
            limit_pushdown: plan_lower.contains("limit")
                || plan_lower.contains("topk")
                || plan_lower.contains("fetch="),
        }
    }

    /// Extract time range from WHERE clause for partition pruning hints
    /// Returns (start_timestamp, end_timestamp) if found
    pub fn extract_time_range(&self, sql: &str) -> Option<TimeRange> {
        let sql_upper = sql.to_uppercase();

        // Look for timestamp conditions in WHERE clause
        if let Some(where_pos) = sql_upper.find("WHERE") {
            let where_clause = &sql[where_pos + 5..];

            // Common patterns:
            // timestamp > X, timestamp >= X, timestamp < X, timestamp <= X
            // timestamp BETWEEN X AND Y
            // time > X, time >= X (common alias)

            let mut start_ts: Option<i64> = None;
            let mut end_ts: Option<i64> = None;
            let mut start_date: Option<String> = None;
            let mut end_date: Option<String> = None;

            // Look for timestamp/time comparisons
            let time_patterns = ["TIMESTAMP", "TIME", "TS"];
            for pattern in time_patterns {
                // Check for >= or >
                if let Some(pos) = where_clause.to_uppercase().find(&format!("{} >", pattern)) {
                    let after = &where_clause[pos + pattern.len() + 2..];
                    if let Some(val) = Self::extract_numeric_value(after) {
                        start_ts = Some(val);
                    }
                }
                if let Some(pos) = where_clause.to_uppercase().find(&format!("{} >=", pattern)) {
                    let after = &where_clause[pos + pattern.len() + 3..];
                    if let Some(val) = Self::extract_numeric_value(after) {
                        start_ts = Some(val);
                    }
                }
                // Check for <= or <
                if let Some(pos) = where_clause.to_uppercase().find(&format!("{} <", pattern)) {
                    let after = &where_clause[pos + pattern.len() + 2..];
                    if let Some(val) = Self::extract_numeric_value(after) {
                        end_ts = Some(val);
                    }
                }
                if let Some(pos) = where_clause.to_uppercase().find(&format!("{} <=", pattern)) {
                    let after = &where_clause[pos + pattern.len() + 3..];
                    if let Some(val) = Self::extract_numeric_value(after) {
                        end_ts = Some(val);
                    }
                }
            }

            // Look for date partition filter (date = 'YYYY-MM-DD')
            if let Some(pos) = where_clause.to_uppercase().find("DATE =") {
                let after = &where_clause[pos + 6..];
                if let Some(date) = Self::extract_date_value(after) {
                    start_date = Some(date.clone());
                    end_date = Some(date);
                }
            }
            if let Some(pos) = where_clause.to_uppercase().find("DATE >=") {
                let after = &where_clause[pos + 7..];
                if let Some(date) = Self::extract_date_value(after) {
                    start_date = Some(date);
                }
            }
            if let Some(pos) = where_clause.to_uppercase().find("DATE <=") {
                let after = &where_clause[pos + 7..];
                if let Some(date) = Self::extract_date_value(after) {
                    end_date = Some(date);
                }
            }

            if start_ts.is_some() || end_ts.is_some() || start_date.is_some() || end_date.is_some()
            {
                return Some(TimeRange {
                    start_timestamp: start_ts,
                    end_timestamp: end_ts,
                    start_date,
                    end_date,
                });
            }
        }

        None
    }

    /// Extract numeric value from SQL fragment
    fn extract_numeric_value(s: &str) -> Option<i64> {
        let trimmed = s.trim();
        let mut num_str = String::new();

        for ch in trimmed.chars() {
            if ch.is_ascii_digit() || ch == '-' {
                num_str.push(ch);
            } else if !num_str.is_empty() {
                break;
            }
        }

        num_str.parse().ok()
    }

    /// Extract date value from SQL fragment (e.g., '2024-01-15')
    fn extract_date_value(s: &str) -> Option<String> {
        let trimmed = s.trim();

        // Look for quoted date string
        if let Some(start) = trimmed.find('\'') {
            if let Some(end) = trimmed[start + 1..].find('\'') {
                let date = &trimmed[start + 1..start + 1 + end];
                // Validate date format (YYYY-MM-DD)
                if date.len() == 10 && date.chars().nth(4) == Some('-') && date.chars().nth(7) == Some('-') {
                    return Some(date.to_string());
                }
            }
        }

        None
    }

    /// Inject date predicates into SQL based on timestamp filters for smart partition pruning
    ///
    /// This converts timestamp predicates to date predicates, enabling Hive-style partition pruning.
    /// Example:
    ///   Input:  SELECT * FROM cpu WHERE timestamp >= 1704067200000000000
    ///   Output: SELECT * FROM cpu WHERE timestamp >= 1704067200000000000 AND date >= '2024-01-01'
    ///
    /// This allows DataFusion to prune partitions based on the date column.
    pub fn inject_date_predicates(&self, sql: &str) -> String {
        // Extract time range from the SQL
        let time_range = match self.extract_time_range(sql) {
            Some(tr) => tr,
            None => return sql.to_string(), // No time range found, return original SQL
        };

        // Get derived date range from timestamps
        let (start_date, end_date) = time_range.get_date_range();

        // If no dates can be derived, return original SQL
        if start_date.is_none() && end_date.is_none() {
            return sql.to_string();
        }

        // Check if date predicates already exist in the query
        let sql_upper = sql.to_uppercase();
        if sql_upper.contains("DATE =") || sql_upper.contains("DATE >=") ||
           sql_upper.contains("DATE <=") || sql_upper.contains("DATE >") ||
           sql_upper.contains("DATE <") || sql_upper.contains("DATE BETWEEN") {
            // Date predicates already exist, don't inject duplicates
            debug!("Date predicates already present in query, skipping injection");
            return sql.to_string();
        }

        // Build date predicate clause
        let date_predicates = match (&start_date, &end_date) {
            (Some(start), Some(end)) if start == end => {
                format!("date = '{}'", start)
            }
            (Some(start), Some(end)) => {
                format!("date >= '{}' AND date <= '{}'", start, end)
            }
            (Some(start), None) => {
                format!("date >= '{}'", start)
            }
            (None, Some(end)) => {
                format!("date <= '{}'", end)
            }
            (None, None) => {
                return sql.to_string();
            }
        };

        // Find WHERE clause position to inject the date predicates
        if let Some(where_pos) = sql_upper.find("WHERE") {
            // Insert date predicates after WHERE keyword
            let where_end = where_pos + 5; // "WHERE" is 5 characters
            let before = &sql[..where_end];
            let after = &sql[where_end..];

            let result = format!("{} {} AND{}", before, date_predicates, after);
            debug!("Injected date predicates: {} -> {}", sql, result);
            result
        } else {
            // No WHERE clause - we shouldn't inject date predicates without other conditions
            // This could cause issues with queries that have no filtering
            debug!("No WHERE clause found, cannot inject date predicates safely");
            sql.to_string()
        }
    }

    /// Get query analysis including estimated rows and partitions
    pub async fn analyze_query(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<QueryAnalysis, String> {
        // Get explain result first
        let explain = self.explain_query(sql, db_name, true, false).await?;

        // Extract time range
        let time_range = self.extract_time_range(sql);

        // Get referenced tables
        let table_refs = self.extract_table_references(sql, db_name);

        // Count partitions that would be scanned
        let mut partition_count = 0;
        let mut total_files = 0;

        for (catalog, schema, table) in &table_refs {
            let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
            if let Ok(partitions) = self.discover_table_partitions(&table_path) {
                // If we have time range, filter partitions
                if let Some(ref tr) = time_range {
                    for partition in &partitions {
                        // Extract date from partition name (date=YYYY-MM-DD)
                        if let Some(date) = partition.strip_prefix("date=") {
                            let include = match (&tr.start_date, &tr.end_date) {
                                (Some(start), Some(end)) => date >= start.as_str() && date <= end.as_str(),
                                (Some(start), None) => date >= start.as_str(),
                                (None, Some(end)) => date <= end.as_str(),
                                (None, None) => true,
                            };
                            if include {
                                partition_count += 1;
                                // Count files in partition (using cache)
                                let partition_path = format!("{}/{}", table_path, partition);
                                total_files += self.count_parquet_files_cached(&partition_path);
                            }
                        }
                    }
                } else {
                    partition_count += partitions.len();
                    // Count all files (using cache)
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
            plan_summary: Self::summarize_plan(&explain.physical_plan),
        })
    }

    /// Create a brief summary of the physical plan
    fn summarize_plan(plan: &str) -> String {
        let mut summary = Vec::new();

        if plan.contains("ParquetExec") || plan.contains("ParquetScan") {
            summary.push("Parquet scan");
        }
        if plan.contains("FilterExec") || plan.contains("filter=") {
            summary.push("Filter applied");
        }
        if plan.contains("ProjectionExec") || plan.contains("projection=") {
            summary.push("Projection pushdown");
        }
        if plan.contains("AggregateExec") || plan.contains("aggregate=") {
            summary.push("Aggregation");
        }
        if plan.contains("SortExec") || plan.contains("sort=") {
            summary.push("Sort");
        }
        if plan.contains("HashJoin") || plan.contains("MergeJoin") {
            summary.push("Join");
        }
        if plan.contains("GlobalLimitExec") || plan.contains("LocalLimitExec") {
            summary.push("Limit");
        }

        if summary.is_empty() {
            "Simple scan".to_string()
        } else {
            summary.join(" → ")
        }
    }

    /// Parse a table reference considering quoted identifiers
    /// Returns (parts, original_string)
    fn parse_table_reference(table_ref: &str) -> (Vec<String>, String) {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut quote_char = ' ';
        let chars: Vec<char> = table_ref.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let ch = chars[i];

            if !in_quotes && (ch == '"' || ch == '`') {
                in_quotes = true;
                quote_char = ch;
                i += 1;
                continue;
            }

            if in_quotes && ch == quote_char {
                in_quotes = false;
                // Add the current quoted part
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                i += 1;
                // Skip the dot after closing quote if present
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                }
                continue;
            }

            if !in_quotes && ch == '.' {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                i += 1;
                continue;
            }

            current.push(ch);
            i += 1;
        }

        if !current.is_empty() {
            parts.push(current);
        }

        (parts, table_ref.to_string())
    }

    /// Quote an identifier if it contains special characters or mixed case
    /// DataFusion requires quotes for:
    /// - Case-sensitive identifiers (any uppercase letters)
    /// - Identifiers with special characters (dots, hyphens, spaces)
    fn quote_identifier(ident: &str) -> String {
        let needs_quoting = ident.contains('.')
            || ident.contains('-')
            || ident.contains(' ')
            || ident.chars().any(|c| c.is_uppercase()); // Preserve case-sensitive names

        if needs_quoting {
            format!("\"{}\"", ident)
        } else {
            ident.to_string()
        }
    }

    /// Extract table references from SQL query
    /// Returns Vec<(catalog, schema, table)> with fully resolved names
    fn extract_table_references(&self, sql: &str, catalog_name: &str) -> Vec<(String, String, String)> {
        let sql_upper = sql.to_uppercase();
        let mut table_refs = Vec::new();

        let keywords = [
            "FROM",
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
        ];

        for keyword in &keywords {
            let pattern = format!("{} ", keyword);
            let mut search_pos = 0;

            while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos + pattern.len();
                let remaining = &sql[abs_pos..];

                // Extract all comma-separated table references
                let mut table_list_end = 0;
                let mut in_quotes = false;
                let mut quote_char = ' ';
                let mut paren_depth = 0;

                // Find the end of the table list (stops at WHERE, ORDER BY, GROUP BY, etc.)
                for (i, ch) in remaining.chars().enumerate() {
                    if !in_quotes && ch == '(' {
                        paren_depth += 1;
                    } else if !in_quotes && ch == ')' {
                        paren_depth -= 1;
                        if paren_depth < 0 {
                            table_list_end = i;
                            break;
                        }
                    } else if !in_quotes && (ch == '"' || ch == '`') {
                        in_quotes = true;
                        quote_char = ch;
                    } else if in_quotes && ch == quote_char {
                        in_quotes = false;
                    } else if !in_quotes && paren_depth == 0 {
                        // Check for SQL keywords that end the table list
                        let remaining_upper = &sql_upper[abs_pos + i..];
                        if remaining_upper.starts_with("WHERE ")
                            || remaining_upper.starts_with("GROUP ")
                            || remaining_upper.starts_with("ORDER ")
                            || remaining_upper.starts_with("LIMIT ")
                            || remaining_upper.starts_with("HAVING ")
                            || remaining_upper.starts_with("UNION ")
                            || remaining_upper.starts_with("EXCEPT ")
                            || remaining_upper.starts_with("INTERSECT ")
                            || ch == ';'
                        {
                            table_list_end = i;
                            break;
                        }
                    }
                }

                if table_list_end == 0 {
                    table_list_end = remaining.len();
                }

                let table_list = &remaining[..table_list_end];

                // Split by commas and process each table reference
                let mut current_table = String::new();
                let mut in_quotes = false;
                let mut quote_char = ' ';

                for ch in table_list.chars() {
                    if !in_quotes && (ch == '"' || ch == '`') {
                        in_quotes = true;
                        quote_char = ch;
                        current_table.push(ch);
                    } else if in_quotes && ch == quote_char {
                        in_quotes = false;
                        current_table.push(ch);
                    } else if !in_quotes && ch == ',' {
                        // Process the current table
                        self.process_table_ref(&current_table, catalog_name, &mut table_refs);
                        current_table.clear();
                    } else {
                        current_table.push(ch);
                    }
                }

                // Process the last table
                if !current_table.is_empty() {
                    self.process_table_ref(&current_table, catalog_name, &mut table_refs);
                }

                search_pos = abs_pos + table_list_end;
            }
        }

        // Deduplicate
        table_refs.sort();
        table_refs.dedup();
        table_refs
    }

    /// Helper to process a single table reference and add to the list
    fn process_table_ref(&self, table_ref: &str, catalog_name: &str, table_refs: &mut Vec<(String, String, String)>) {
        let table_ref = table_ref.trim();

        // Skip empty refs, subqueries, or aliases (words after whitespace)
        if table_ref.is_empty() || table_ref.starts_with('(') {
            return;
        }

        // Remove alias if present (e.g., "cpu c" -> "cpu")
        let table_ref = table_ref.split_whitespace().next().unwrap_or(table_ref);

        // Parse the table reference considering quotes
        let (parts, _) = Self::parse_table_reference(table_ref);

        // Determine catalog, schema, table based on number of parts
        let (catalog, schema, table) = match parts.len() {
            0 => return,
            1 => {
                // Simple name: cpu -> catalog.default.cpu
                (catalog_name.to_string(), DEFAULT_SCHEMA.to_string(), parts[0].clone())
            }
            2 => {
                // Partially qualified: schema.table -> catalog.schema.table
                (catalog_name.to_string(), parts[0].clone(), parts[1].clone())
            }
            _ => {
                // Fully qualified: catalog.schema.table
                (parts[0].clone(), parts[1].clone(), parts[2].clone())
            }
        };

        table_refs.push((catalog, schema, table));
    }

    /// Qualify unqualified or partially qualified table names with catalog.schema
    ///
    /// Examples (with catalog_name = "testdb"):
    /// - "cpu" -> "testdb.default.cpu"
    /// - "webapp.cpu" -> "testdb.webapp.cpu"
    /// - "testdb.webapp.cpu" -> "testdb.webapp.cpu" (already fully qualified)
    /// - "otherdb.schema.table" -> "otherdb.schema.table" (different catalog, unchanged)
    /// - "default.\"cat.transaction\"" -> "testdb.default.\"cat.transaction\""
    fn qualify_table_names(&self, sql: &str, catalog_name: &str) -> String {
        let sql_upper = sql.to_uppercase();
        let mut result = sql.to_string();
        let mut offset: i64 = 0; // Track the offset due to replacements

        let keywords = [
            "FROM",
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
        ];

        for keyword in &keywords {
            let pattern = format!("{} ", keyword);
            let mut search_pos = 0;

            while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos + pattern.len();
                let remaining = &sql[abs_pos..];

                // Find end of table reference, considering quotes
                let mut end_pos = 0;
                let mut in_quotes = false;
                let mut quote_char = ' ';

                for (i, ch) in remaining.chars().enumerate() {
                    if !in_quotes && (ch == '"' || ch == '`') {
                        in_quotes = true;
                        quote_char = ch;
                    } else if in_quotes && ch == quote_char {
                        in_quotes = false;
                    } else if !in_quotes
                        && (ch.is_whitespace() || ch == ',' || ch == ';' || ch == ')')
                    {
                        end_pos = i;
                        break;
                    }
                }

                if end_pos == 0 {
                    end_pos = remaining.len();
                }

                let table_ref = remaining[..end_pos].trim();

                // Skip empty refs or subqueries
                if table_ref.is_empty() || table_ref.starts_with('(') {
                    search_pos = abs_pos + end_pos;
                    continue;
                }

                // Parse the table reference considering quotes
                let (parts, _) = Self::parse_table_reference(table_ref);

                // Determine qualification based on number of parts
                let qualified = match parts.len() {
                    0 => {
                        search_pos = abs_pos + end_pos;
                        continue;
                    }
                    1 => {
                        // Simple name: cpu -> catalog.default.cpu
                        format!(
                            "{}.{}.{}",
                            catalog_name,
                            DEFAULT_SCHEMA,
                            Self::quote_identifier(&parts[0])
                        )
                    }
                    2 => {
                        // Partially qualified: schema.table -> catalog.schema.table
                        format!(
                            "{}.{}.{}",
                            catalog_name,
                            Self::quote_identifier(&parts[0]),
                            Self::quote_identifier(&parts[1])
                        )
                    }
                    _ => {
                        // Already fully qualified (catalog.schema.table), don't modify
                        // But reconstruct it with proper quoting
                        format!(
                            "{}.{}.{}",
                            Self::quote_identifier(&parts[0]),
                            Self::quote_identifier(&parts[1]),
                            Self::quote_identifier(&parts[2])
                        )
                    }
                };

                // Only replace if we actually changed something
                if qualified != table_ref {
                    // Calculate the actual position in the result string with offset
                    let result_pos = (abs_pos as i64 + offset) as usize;
                    let before_part = &result[..result_pos];
                    let after_part = &result[result_pos + table_ref.len()..];

                    result = format!("{}{}{}", before_part, qualified, after_part);

                    // Update offset
                    offset += qualified.len() as i64 - table_ref.len() as i64;
                }

                search_pos = abs_pos + end_pos;
            }
        }

        result
    }

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
        // Add tests here
    }

    #[test]
    fn test_qualify_table_names_unqualified() {
        let engine = QueryEngine::new("./data");

        // Unqualified table name: cpu -> testdb.default.cpu
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu WHERE usage > 50");
    }

    #[test]
    fn test_qualify_table_names_partially_qualified() {
        let engine = QueryEngine::new("./data");

        // Partially qualified: webapp.cpu -> testdb.webapp.cpu
        let sql = "SELECT * FROM webapp.cpu LIMIT 10";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.webapp.cpu LIMIT 10");
    }

    #[test]
    fn test_qualify_table_names_fully_qualified() {
        let engine = QueryEngine::new("./data");

        // Fully qualified: testdb.webapp.cpu -> testdb.webapp.cpu (unchanged)
        let sql = "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(
            result,
            "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0"
        );
    }

    #[test]
    fn test_qualify_table_names_different_catalog() {
        let engine = QueryEngine::new("./data");

        // Different catalog: otherdb.schema.table -> otherdb.schema.table (unchanged)
        let sql = "SELECT * FROM otherdb.webapp.cpu";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM otherdb.webapp.cpu");
    }

    #[test]
    fn test_qualify_table_names_multiple_tables() {
        let engine = QueryEngine::new("./data");

        // Multiple tables with different qualification levels
        let sql = "SELECT * FROM cpu JOIN webapp.memory ON cpu.hostname = memory.hostname";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu JOIN testdb.webapp.memory ON cpu.hostname = memory.hostname");
    }

    #[test]
    fn test_qualify_table_names_with_alias() {
        let engine = QueryEngine::new("./data");

        // Table with alias
        let sql = "SELECT c.* FROM cpu c WHERE c.usage > 50";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(
            result,
            "SELECT c.* FROM testdb.default.cpu c WHERE c.usage > 50"
        );
    }

    #[test]
    fn test_qualify_table_names_case_insensitive() {
        let engine = QueryEngine::new("./data");

        // Test case insensitivity (FROM, from, FrOm should all work)
        let sql1 = "select * from cpu";
        let sql2 = "SELECT * FROM cpu";
        let sql3 = "SeLeCt * FrOm cpu";

        let result1 = engine.qualify_table_names(sql1, "testdb");
        let result2 = engine.qualify_table_names(sql2, "testdb");
        let result3 = engine.qualify_table_names(sql3, "testdb");

        assert_eq!(result1, "select * from testdb.default.cpu");
        assert_eq!(result2, "SELECT * FROM testdb.default.cpu");
        assert_eq!(result3, "SeLeCt * FrOm testdb.default.cpu");
    }

    #[test]
    fn test_qualify_table_names_with_subquery() {
        let engine = QueryEngine::new("./data");

        // Subqueries - inner tables should be qualified
        let sql = "SELECT * FROM (SELECT * FROM cpu) AS subquery";
        let result = engine.qualify_table_names(sql, "testdb");
        // The inner FROM should be qualified
        assert!(result.contains("FROM testdb.default.cpu"));
    }

    #[test]
    fn test_qualify_table_names_comma_separated() {
        let engine = QueryEngine::new("./data");

        // Comma-separated tables (old-style join syntax)
        let sql = "SELECT * FROM cpu, memory WHERE cpu.hostname = memory.hostname";
        let result = engine.qualify_table_names(sql, "testdb");
        // Only the first table after FROM is qualified; comma-separated tables need special handling
        assert!(result.contains("testdb.default.cpu"));
    }

    #[test]
    fn test_qualify_table_names_with_quoted_identifiers() {
        let engine = QueryEngine::new("./data");

        // Quoted identifier with dot inside
        let sql = r#"SELECT * FROM testdb.default."cat.transaction""#;
        let result = engine.qualify_table_names(sql, "testdb");
        // Should remain unchanged as it's fully qualified
        assert_eq!(result, r#"SELECT * FROM testdb.default."cat.transaction""#);

        // Partially qualified with quoted identifier
        let sql2 = r#"SELECT * FROM default."cat.transaction""#;
        let result2 = engine.qualify_table_names(sql2, "testdb");
        assert_eq!(result2, r#"SELECT * FROM testdb.default."cat.transaction""#);

        // Unqualified quoted identifier
        let sql3 = r#"SELECT * FROM "cat.transaction""#;
        let result3 = engine.qualify_table_names(sql3, "testdb");
        assert_eq!(result3, r#"SELECT * FROM testdb.default."cat.transaction""#);
    }

    #[test]
    fn test_parse_table_reference() {
        // Simple name
        let (parts, _) = QueryEngine::parse_table_reference("cpu");
        assert_eq!(parts, vec!["cpu"]);

        // Schema.table
        let (parts, _) = QueryEngine::parse_table_reference("webapp.cpu");
        assert_eq!(parts, vec!["webapp", "cpu"]);

        // Catalog.schema.table
        let (parts, _) = QueryEngine::parse_table_reference("testdb.webapp.cpu");
        assert_eq!(parts, vec!["testdb", "webapp", "cpu"]);

        // Quoted identifier with dot
        let (parts, _) = QueryEngine::parse_table_reference(r#""cat.transaction""#);
        assert_eq!(parts, vec!["cat.transaction"]);

        // Schema with quoted table
        let (parts, _) = QueryEngine::parse_table_reference(r#"default."cat.transaction""#);
        assert_eq!(parts, vec!["default", "cat.transaction"]);

        // Fully qualified with quoted table
        let (parts, _) = QueryEngine::parse_table_reference(r#"testdb.default."cat.transaction""#);
        assert_eq!(parts, vec!["testdb", "default", "cat.transaction"]);
    }

    #[test]
    fn test_quote_identifier_case_sensitive() {
        // Case-sensitive identifiers (with uppercase) should be quoted
        assert_eq!(
            QueryEngine::quote_identifier("TestDiskUsage"),
            r#""TestDiskUsage""#
        );
        assert_eq!(
            QueryEngine::quote_identifier("Executor.ActiveThread"),
            r#""Executor.ActiveThread""#
        );

        // All lowercase - no quotes needed
        assert_eq!(QueryEngine::quote_identifier("cpu"), "cpu");

        // Special chars - needs quotes
        assert_eq!(QueryEngine::quote_identifier("test-api"), r#""test-api""#);
    }

    #[test]
    fn test_qualify_case_sensitive_table() {
        let engine = QueryEngine::new("./data");

        // Case-sensitive table name should be quoted in output
        let sql = r#"SELECT * FROM test-api."TestDiskUsage" LIMIT 2"#;
        let result = engine.qualify_table_names(sql, "_system");
        assert_eq!(
            result,
            r#"SELECT * FROM _system."test-api"."TestDiskUsage" LIMIT 2"#
        );

        // Partially qualified with case-sensitive table
        let sql2 = r#"SELECT * FROM "TestDiskUsage""#;
        let result2 = engine.qualify_table_names(sql2, "_system");
        assert_eq!(result2, r#"SELECT * FROM _system.default."TestDiskUsage""#);
    }

    #[test]
    fn test_extract_table_references() {
        let engine = QueryEngine::new("./data");

        // Simple unqualified table
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "default".to_string(), "cpu".to_string())]);

        // Partially qualified
        let sql = "SELECT * FROM webapp.cpu LIMIT 10";
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "webapp".to_string(), "cpu".to_string())]);

        // Fully qualified
        let sql = "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0";
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "webapp".to_string(), "cpu".to_string())]);

        // Multiple tables (JOIN)
        let sql = "SELECT * FROM cpu JOIN webapp.memory ON cpu.hostname = memory.hostname";
        let mut refs = engine.extract_table_references(sql, "testdb");
        refs.sort();
        assert_eq!(refs, vec![
            ("testdb".to_string(), "default".to_string(), "cpu".to_string()),
            ("testdb".to_string(), "webapp".to_string(), "memory".to_string())
        ]);

        // Multiple tables with different qualification levels
        let sql = "SELECT * FROM cpu, otherdb.webapp.disk, service.memory";
        let mut refs = engine.extract_table_references(sql, "testdb");
        refs.sort();
        assert_eq!(refs, vec![
            ("otherdb".to_string(), "webapp".to_string(), "disk".to_string()),
            ("testdb".to_string(), "default".to_string(), "cpu".to_string()),
            ("testdb".to_string(), "service".to_string(), "memory".to_string())
        ]);

        // With quoted identifiers
        let sql = r#"SELECT * FROM default."cat.transaction""#;
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "default".to_string(), "cat.transaction".to_string())]);
    }

    #[test]
    fn test_extract_time_range_timestamp() {
        let engine = QueryEngine::new("./data");

        // Test timestamp range extraction
        let sql = "SELECT * FROM cpu WHERE timestamp >= 1704067200000000000 AND timestamp < 1704153600000000000";
        let range = engine.extract_time_range(sql);
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start_timestamp, Some(1704067200000000000));
        assert_eq!(range.end_timestamp, Some(1704153600000000000));
    }

    #[test]
    fn test_extract_time_range_date() {
        let engine = QueryEngine::new("./data");

        // Test date range extraction
        let sql = "SELECT * FROM cpu WHERE date = '2024-01-15'";
        let range = engine.extract_time_range(sql);
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start_date, Some("2024-01-15".to_string()));
        assert_eq!(range.end_date, Some("2024-01-15".to_string()));
    }

    #[test]
    fn test_extract_time_range_date_range() {
        let engine = QueryEngine::new("./data");

        // Test date range extraction
        let sql = "SELECT * FROM cpu WHERE date >= '2024-01-10' AND date <= '2024-01-20'";
        let range = engine.extract_time_range(sql);
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start_date, Some("2024-01-10".to_string()));
        assert_eq!(range.end_date, Some("2024-01-20".to_string()));
    }

    #[test]
    fn test_extract_time_range_none() {
        let engine = QueryEngine::new("./data");

        // Test query without time range
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let range = engine.extract_time_range(sql);
        assert!(range.is_none());
    }

    #[test]
    fn test_optimized_context_settings() {
        // Verify optimized context is created with correct settings
        let ctx = QueryEngine::create_optimized_context();
        let state = ctx.state();
        let config = state.config();

        // Check predicate pushdown settings
        assert!(config.options().execution.parquet.pushdown_filters);
        assert!(config.options().execution.parquet.reorder_filters);
        assert!(config.options().execution.parquet.pruning);
        assert!(config.options().execution.parquet.enable_page_index);
        assert!(config.options().execution.parquet.bloom_filter_on_read);

        // Check parallel reading settings
        assert!(config.options().execution.target_partitions > 0);
        assert_eq!(config.options().execution.batch_size, 8192);
        assert!(config.options().execution.coalesce_batches);

        // Check time-series optimizations
        assert!(!config.options().optimizer.enable_round_robin_repartition);
        assert!(!config.options().optimizer.repartition_file_scans);
    }

    #[test]
    fn test_timestamp_nanos_to_date() {
        // 2024-01-01 00:00:00 UTC in nanoseconds
        let ts = 1704067200000000000i64;
        let date = TimeRange::timestamp_nanos_to_date(ts);
        assert_eq!(date, "2024-01-01");

        // 2024-01-15 12:30:45 UTC in nanoseconds
        let ts2 = 1705322245000000000i64;
        let date2 = TimeRange::timestamp_nanos_to_date(ts2);
        assert_eq!(date2, "2024-01-15");
    }

    #[test]
    fn test_timestamp_millis_to_date() {
        // 2024-01-01 00:00:00 UTC in milliseconds
        let ts = 1704067200000i64;
        let date = TimeRange::timestamp_millis_to_date(ts);
        assert_eq!(date, "2024-01-01");
    }

    #[test]
    fn test_is_likely_millis() {
        // Nanoseconds (19 digits)
        assert!(!TimeRange::is_likely_millis(1704067200000000000));

        // Milliseconds (13 digits)
        assert!(TimeRange::is_likely_millis(1704067200000));

        // Edge cases
        assert!(TimeRange::is_likely_millis(1000000000000)); // 12 digits, still millis range
        assert!(!TimeRange::is_likely_millis(1000000000000000)); // 16 digits, nanos range
    }

    #[test]
    fn test_timestamp_to_date_auto_detect() {
        // Nanoseconds
        let date1 = TimeRange::timestamp_to_date(1704067200000000000);
        assert_eq!(date1, "2024-01-01");

        // Milliseconds
        let date2 = TimeRange::timestamp_to_date(1704067200000);
        assert_eq!(date2, "2024-01-01");
    }

    #[test]
    fn test_inject_date_predicates_with_timestamp() {
        let engine = QueryEngine::new("./data");

        // Test with nanosecond timestamp
        let sql = "SELECT * FROM cpu WHERE timestamp >= 1704067200000000000";
        let result = engine.inject_date_predicates(sql);
        assert!(result.contains("date >= '2024-01-01'"));
        assert!(result.contains("timestamp >= 1704067200000000000"));
    }

    #[test]
    fn test_inject_date_predicates_with_timestamp_range() {
        let engine = QueryEngine::new("./data");

        // Test with timestamp range (2024-01-01 to 2024-01-02)
        let sql = "SELECT * FROM cpu WHERE timestamp >= 1704067200000000000 AND timestamp < 1704153600000000000";
        let result = engine.inject_date_predicates(sql);
        assert!(result.contains("date >= '2024-01-01'"));
        assert!(result.contains("date <= '2024-01-02'"));
    }

    #[test]
    fn test_inject_date_predicates_skips_existing_date() {
        let engine = QueryEngine::new("./data");

        // Test that existing date predicates are not duplicated
        let sql = "SELECT * FROM cpu WHERE timestamp >= 1704067200000000000 AND date = '2024-01-01'";
        let result = engine.inject_date_predicates(sql);
        // Should return unchanged since date predicate already exists
        assert_eq!(result, sql);
    }

    #[test]
    fn test_inject_date_predicates_no_time_filter() {
        let engine = QueryEngine::new("./data");

        // Test query without time filter
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let result = engine.inject_date_predicates(sql);
        // Should return unchanged since there's no time filter
        assert_eq!(result, sql);
    }

    #[test]
    fn test_inject_date_predicates_no_where_clause() {
        let engine = QueryEngine::new("./data");

        // Test query without WHERE clause - should not inject
        let sql = "SELECT * FROM cpu";
        let result = engine.inject_date_predicates(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn test_time_range_get_date_range() {
        // Test with both timestamp and explicit dates
        let range = TimeRange {
            start_timestamp: Some(1704067200000000000), // 2024-01-01
            end_timestamp: Some(1704153600000000000),   // 2024-01-02
            start_date: None,
            end_date: None,
        };
        let (start, end) = range.get_date_range();
        assert_eq!(start, Some("2024-01-01".to_string()));
        assert_eq!(end, Some("2024-01-02".to_string()));

        // Test with explicit dates (should take precedence)
        let range2 = TimeRange {
            start_timestamp: Some(1704067200000000000),
            end_timestamp: Some(1704153600000000000),
            start_date: Some("2024-01-10".to_string()),
            end_date: Some("2024-01-20".to_string()),
        };
        let (start2, end2) = range2.get_date_range();
        assert_eq!(start2, Some("2024-01-10".to_string()));
        assert_eq!(end2, Some("2024-01-20".to_string()));
    }
}
