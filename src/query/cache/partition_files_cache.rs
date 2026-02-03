//! Partition Files Cache
//!
//! Caches the list of Parquet files for each partition from object storage.
//! This avoids repeated S3 LIST operations at query time.
//!
//! ## Design
//!
//! ```text
//! Query Time:
//!   1. Check cache for partition file list
//!   2. If miss or expired → LIST from S3
//!   3. Cache result with TTL (default 30s)
//!   4. Return file paths
//! ```
//!
//! ## Key Design Decisions
//!
//! - **TTL-based expiration**: Short TTL (30-60s) ensures fresh data without excessive LIST calls
//! - **Partition-level granularity**: Cache per partition (catalog/schema/table/date)
//! - **Lazy loading**: Only fetch from S3 when needed (not preloaded)
//! - **Thread-safe**: Uses DashMap for concurrent access
//!
//! ## Integration with Table-Level Routing
//!
//! This cache works with table-level routing in cluster mode:
//! - Router uses Meta to find table owner (Data node)
//! - Data node uses this cache to list files from S3
//! - Files are discovered from object storage, not stored in Meta
//!
//! See `docs/architecture-meta-routing.md` for the full architecture.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::storage::storage::Storage;

/// Information about a cached parquet file
#[derive(Debug, Clone)]
pub struct CachedFileInfo {
    /// File path (relative to storage root)
    pub path: String,
    /// Compaction level extracted from filename (L0, L1, L2, etc.)
    pub level: u8,
    /// Size in bytes (if available from LIST metadata)
    pub size_bytes: Option<u64>,
}

impl CachedFileInfo {
    /// Extract compaction level from filename
    /// Expects format: L{level}_*.parquet
    fn extract_level(path: &str) -> u8 {
        let filename = path.rsplit('/').next().unwrap_or(path);
        if filename.starts_with("L") {
            // Try to parse L0, L1, L2, etc.
            if let Some(level_char) = filename.chars().nth(1) {
                if let Some(level) = level_char.to_digit(10) {
                    return level as u8;
                }
            }
        }
        0 // Default to L0 if pattern doesn't match
    }

    /// Create from a file path
    pub fn from_path(path: String, size_bytes: Option<u64>) -> Self {
        let level = Self::extract_level(&path);
        Self {
            path,
            level,
            size_bytes,
        }
    }
}

/// Cached file list for a partition
#[derive(Debug, Clone)]
struct CachedPartitionFiles {
    /// List of files in the partition
    files: Vec<CachedFileInfo>,
    /// When this entry was cached
    cached_at: Instant,
    /// When this entry expires
    ttl: Duration,
}

impl CachedPartitionFiles {
    fn new(files: Vec<CachedFileInfo>, ttl: Duration) -> Self {
        Self {
            files,
            cached_at: Instant::now(),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }
}

/// Statistics for the partition files cache
#[derive(Debug, Default, Clone)]
pub struct PartitionFilesCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub total_files_cached: usize,
}

/// Cache for partition file listings from object storage
///
/// Provides TTL-based caching of S3 LIST results to reduce latency
/// and API calls at query time.
#[derive(Clone)]
pub struct PartitionFilesCache {
    /// Cache entries: partition_key -> cached file list
    cache: Arc<DashMap<String, CachedPartitionFiles>>,
    /// Storage backend for LIST operations
    storage: Arc<Storage>,
    /// Default TTL for cache entries
    default_ttl: Duration,
    /// Cache statistics
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
    /// Maximum number of partitions to cache (LRU eviction)
    max_entries: usize,
}

impl PartitionFilesCache {
    /// Create a new partition files cache
    ///
    /// # Arguments
    /// - `storage`: Object storage backend
    /// - `ttl_secs`: Cache TTL in seconds (default 30)
    /// - `max_entries`: Maximum cached partitions (default 10000)
    pub fn new(storage: Arc<Storage>, ttl_secs: u64, max_entries: usize) -> Self {
        info!(
            "Initializing partition files cache: ttl={}s, max_entries={}",
            ttl_secs, max_entries
        );
        Self {
            cache: Arc::new(DashMap::new()),
            storage,
            default_ttl: Duration::from_secs(ttl_secs),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
            max_entries,
        }
    }

    /// Generate cache key for a partition
    fn partition_key(catalog: &str, schema: &str, table: &str, partition: &str) -> String {
        format!("{}/{}/{}/{}", catalog, schema, table, partition)
    }

    /// Generate S3 prefix for a partition
    fn partition_prefix(catalog: &str, schema: &str, table: &str, partition: &str) -> String {
        format!("{}/{}/{}/date={}/", catalog, schema, table, partition)
    }

    /// List parquet files for a partition
    ///
    /// Returns cached result if available and not expired, otherwise
    /// fetches from object storage and caches the result.
    pub async fn list_files(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition: &str,
    ) -> Result<Vec<CachedFileInfo>, String> {
        let key = Self::partition_key(catalog, schema, table, partition);

        // Check cache first
        if let Some(entry) = self.cache.get(&key) {
            if !entry.is_expired() {
                self.hits.fetch_add(1, Ordering::Relaxed);
                debug!("Cache hit for partition: {}", key);
                return Ok(entry.files.clone());
            }
            // Entry expired, will be refreshed below
            debug!("Cache expired for partition: {}", key);
        }

        // Cache miss - fetch from storage
        self.misses.fetch_add(1, Ordering::Relaxed);
        debug!("Cache miss for partition: {}, fetching from storage", key);

        let prefix = Self::partition_prefix(catalog, schema, table, partition);
        let paths: Vec<String> = self.storage.list(&prefix).await?;

        // Filter to only parquet files
        let files: Vec<CachedFileInfo> = paths
            .into_iter()
            .filter(|p: &String| p.ends_with(".parquet"))
            .map(|p: String| CachedFileInfo::from_path(p, None))
            .collect();

        debug!(
            "Listed {} parquet files for partition: {}",
            files.len(),
            key
        );

        // Cache the result
        self.cache_files(&key, files.clone());

        Ok(files)
    }

    /// List parquet files for a table (all partitions)
    ///
    /// More expensive operation - lists across all partitions.
    /// Consider using list_files() for specific partitions when possible.
    pub async fn list_files_for_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<CachedFileInfo>, String> {
        let prefix = format!("{}/{}/{}/", catalog, schema, table);
        let paths: Vec<String> = self.storage.list(&prefix).await?;

        let files: Vec<CachedFileInfo> = paths
            .into_iter()
            .filter(|p: &String| p.ends_with(".parquet"))
            .map(|p: String| CachedFileInfo::from_path(p, None))
            .collect();

        debug!(
            "Listed {} parquet files for table: {}.{}.{}",
            files.len(),
            catalog,
            schema,
            table
        );

        Ok(files)
    }

    /// Cache file list for a partition
    fn cache_files(&self, key: &str, files: Vec<CachedFileInfo>) {
        // Evict oldest entries if at capacity
        if self.cache.len() >= self.max_entries {
            self.evict_oldest();
        }

        let entry = CachedPartitionFiles::new(files, self.default_ttl);
        self.cache.insert(key.to_string(), entry);
    }

    /// Evict oldest entries (simple random eviction for now)
    fn evict_oldest(&self) {
        // Simple strategy: remove any 10% of entries
        let to_remove = self.max_entries / 10;
        let mut removed = 0;

        // Collect keys to remove (can't modify while iterating)
        let keys_to_remove: Vec<String> = self
            .cache
            .iter()
            .take(to_remove)
            .map(|e| e.key().clone())
            .collect();

        for key in keys_to_remove {
            if self.cache.remove(&key).is_some() {
                removed += 1;
            }
        }

        self.evictions.fetch_add(removed as u64, Ordering::Relaxed);
        debug!("Evicted {} cache entries", removed);
    }

    /// Invalidate cache entry for a partition
    ///
    /// Call this after files are added/removed from the partition.
    pub fn invalidate(&self, catalog: &str, schema: &str, table: &str, partition: &str) {
        let key = Self::partition_key(catalog, schema, table, partition);
        if self.cache.remove(&key).is_some() {
            debug!("Invalidated cache for partition: {}", key);
        }
    }

    /// Invalidate all cache entries for a table
    ///
    /// Call this when a table is compacted or modified significantly.
    pub fn invalidate_table(&self, catalog: &str, schema: &str, table: &str) {
        let prefix = format!("{}/{}/{}/", catalog, schema, table);
        let keys_to_remove: Vec<String> = self
            .cache
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| e.key().clone())
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.cache.remove(&key);
        }

        if count > 0 {
            info!(
                "Invalidated {} cache entries for table: {}.{}.{}",
                count, catalog, schema, table
            );
        }
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let count = self.cache.len();
        self.cache.clear();
        info!("Cleared {} partition file cache entries", count);
    }

    /// Get cache statistics
    pub fn stats(&self) -> PartitionFilesCacheStats {
        let total_files: usize = self.cache.iter().map(|e| e.files.len()).sum();

        PartitionFilesCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: self.cache.len(),
            total_files_cached: total_files,
        }
    }

    /// Pre-warm cache for a table's recent partitions
    ///
    /// Useful after startup to reduce cold-start latency.
    pub async fn prewarm_recent_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        recent_days: u32,
    ) -> Result<usize, String> {
        use chrono::{Duration as ChronoDuration, Utc};

        let mut total_files = 0;
        let today = Utc::now().date_naive();

        for days_ago in 0..recent_days {
            let date = today - ChronoDuration::days(days_ago as i64);
            let partition = date.format("%Y-%m-%d").to_string();

            match self.list_files(catalog, schema, table, &partition).await {
                Ok(files) => {
                    total_files += files.len();
                    debug!(
                        "Prewarmed partition {} with {} files",
                        partition,
                        files.len()
                    );
                }
                Err(e) => {
                    // Partition may not exist, that's OK
                    debug!("Skipping partition {}: {}", partition, e);
                }
            }
        }

        info!(
            "Prewarmed {} partitions for {}.{}.{}, total {} files",
            recent_days, catalog, schema, table, total_files
        );

        Ok(total_files)
    }
}

/// Shared reference to partition files cache
pub type SharedPartitionFilesCache = Arc<PartitionFilesCache>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_level() {
        assert_eq!(CachedFileInfo::extract_level("L0_123.parquet"), 0);
        assert_eq!(CachedFileInfo::extract_level("L1_123.parquet"), 1);
        assert_eq!(CachedFileInfo::extract_level("L2_123.parquet"), 2);
        assert_eq!(CachedFileInfo::extract_level("other.parquet"), 0);
        assert_eq!(
            CachedFileInfo::extract_level("data/catalog/L3_abc.parquet"),
            3
        );
    }

    #[test]
    fn test_partition_key() {
        let key = PartitionFilesCache::partition_key("mydb", "default", "cpu", "2024-01-15");
        assert_eq!(key, "mydb/default/cpu/2024-01-15");
    }

    #[test]
    fn test_partition_prefix() {
        let prefix = PartitionFilesCache::partition_prefix("mydb", "default", "cpu", "2024-01-15");
        assert_eq!(prefix, "mydb/default/cpu/date=2024-01-15/");
    }
}
