//! Hot Partition Cache for Query Engine
//!
//! Caches decoded RecordBatches for recent (hot) partitions to avoid
//! repeated Parquet file reads for frequently-accessed data.
//!
//! This is particularly effective for time-series workloads where
//! queries often focus on recent data (today, yesterday).

use chrono::{Datelike, Local, NaiveDate};
use datafusion::arrow::array::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::dictionary_encoding;
use super::predicate_eval;

/// Key for identifying a cached partition
#[derive(Clone, Debug)]
pub struct PartitionKey {
    /// Catalog (database) name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Date in YYYYMMDD format (e.g., 20240115)
    pub date: i32,
}

impl PartitionKey {
    pub fn new(catalog: &str, schema: &str, table: &str, date: i32) -> Self {
        Self {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date,
        }
    }

    /// Create from date string (YYYY-MM-DD format)
    pub fn from_date_str(catalog: &str, schema: &str, table: &str, date_str: &str) -> Option<Self> {
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
        let date_int = date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32;
        Some(Self::new(catalog, schema, table, date_int))
    }

    /// Get date as string (YYYY-MM-DD format)
    pub fn date_str(&self) -> String {
        let year = self.date / 10000;
        let month = (self.date % 10000) / 100;
        let day = self.date % 100;
        format!("{:04}-{:02}-{:02}", year, month, day)
    }
}

impl PartialEq for PartitionKey {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.schema == other.schema
            && self.table == other.table
            && self.date == other.date
    }
}

impl Eq for PartitionKey {}

impl Hash for PartitionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.catalog.hash(state);
        self.schema.hash(state);
        self.table.hash(state);
        self.date.hash(state);
    }
}

/// Cached partition data
struct CachedPartition {
    /// Cached record batches
    batches: Vec<RecordBatch>,
    /// When this entry was created
    created_at: Instant,
    /// Estimated size in bytes
    size_bytes: usize,
    /// Total row count
    row_count: usize,
    /// Number of files this came from
    file_count: usize,
    /// Hit count for this entry
    hit_count: AtomicU64,
}

impl CachedPartition {
    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

/// Statistics counters for the cache
#[derive(Default)]
struct CacheStatsCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
}

/// Configuration for hot partition cache
#[derive(Debug, Clone)]
pub struct HotPartitionCacheConfig {
    /// Enable/disable the cache
    pub enabled: bool,
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,
    /// Number of recent days to consider "hot" (0 = today only, 1 = today + yesterday)
    pub hot_days: u32,
    /// TTL for cached entries in seconds
    pub ttl_secs: u64,
}

impl Default for HotPartitionCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_size_bytes: 1024 * 1024 * 1024, // 1GB
            hot_days: 2,                        // Today + yesterday
            ttl_secs: 1800,                     // 30 minutes
        }
    }
}

impl From<&crate::config::HotPartitionCacheConfig> for HotPartitionCacheConfig {
    fn from(config: &crate::config::HotPartitionCacheConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_size_bytes: (config.max_size_mb as usize) * 1024 * 1024,
            hot_days: config.hot_days,
            ttl_secs: config.ttl_secs,
        }
    }
}

/// Statistics for hot partition cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotPartitionCacheStats {
    pub enabled: bool,
    pub entry_count: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
    pub total_rows: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

impl HotPartitionCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64 * 100.0
        }
    }

    pub fn usage_percent(&self) -> f64 {
        if self.max_size_bytes == 0 {
            0.0
        } else {
            self.size_bytes as f64 / self.max_size_bytes as f64 * 100.0
        }
    }
}

/// Hot partition cache for frequently-accessed recent data
pub struct HotPartitionCache {
    /// Cache entries
    cache: RwLock<HashMap<PartitionKey, CachedPartition>>,
    /// Configuration
    config: HotPartitionCacheConfig,
    /// Current cache size in bytes
    current_size: AtomicU64,
    /// Total cached rows
    total_rows: AtomicU64,
    /// Statistics counters
    stats: CacheStatsCounters,
}

impl HotPartitionCache {
    /// Create a new hot partition cache
    pub fn new(config: HotPartitionCacheConfig) -> Self {
        info!(
            "HotPartitionCache initialized: enabled={}, max_size={}MB, hot_days={}, ttl={}s",
            config.enabled,
            config.max_size_bytes / 1024 / 1024,
            config.hot_days,
            config.ttl_secs
        );

        Self {
            cache: RwLock::new(HashMap::new()),
            config,
            current_size: AtomicU64::new(0),
            total_rows: AtomicU64::new(0),
            stats: CacheStatsCounters::default(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(HotPartitionCacheConfig::default())
    }

    /// Check if caching is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Check if a date is considered "hot" (recent enough to cache)
    pub fn is_hot_date(&self, date: i32) -> bool {
        if !self.config.enabled {
            return false;
        }

        let today = Local::now();
        let today_int = today.year() * 10000 + today.month() as i32 * 100 + today.day() as i32;

        // Calculate the oldest hot date
        let oldest_hot = if self.config.hot_days == 0 {
            today_int
        } else {
            let oldest_date = today - chrono::Duration::days(self.config.hot_days as i64);
            oldest_date.year() * 10000 + oldest_date.month() as i32 * 100 + oldest_date.day() as i32
        };

        date >= oldest_hot
    }

    /// Get cached partition data if available and not expired
    ///
    /// Parameters:
    /// - projection: If provided, only the specified columns are returned
    /// - predicates: If provided, filter batches to only include matching rows
    /// - expected_file_count: If provided, validates against cached file count to detect staleness
    pub fn get(
        &self,
        key: &PartitionKey,
        projection: Option<&[usize]>,
        predicates: &[Arc<dyn PhysicalExpr>],
        expected_file_count: Option<usize>,
    ) -> Option<Vec<RecordBatch>> {
        if !self.config.enabled {
            return None;
        }

        let cache = self.cache.read();
        let ttl = Duration::from_secs(self.config.ttl_secs);

        if let Some(entry) = cache.get(key) {
            if entry.is_expired(ttl) {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            // Staleness check: if file count changed, cache is stale
            // (new files written by flush, or files removed by compaction)
            if let Some(expected) = expected_file_count {
                if entry.file_count != expected {
                    debug!(
                        "Hot partition cache STALE: {}.{}.{} date={}, cached_files={}, current_files={}",
                        key.catalog, key.schema, key.table, key.date_str(),
                        entry.file_count, expected
                    );
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }

            entry.hit_count.fetch_add(1, Ordering::Relaxed);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);

            let mut batches = entry.batches.clone();

            // Apply predicates first (before projection to reduce data volume)
            if !predicates.is_empty() {
                match predicate_eval::filter_batches_with_predicates(&batches, predicates) {
                    Ok(filtered) => batches = filtered,
                    Err(e) => {
                        warn!("Failed to apply predicates in cache: {}, returning unfiltered", e);
                    }
                }
            }

            // Apply projection if requested
            if let Some(proj) = projection {
                batches = batches
                    .iter()
                    .filter_map(|batch| batch.project(proj).ok())
                    .collect();
            }

            debug!(
                "Hot partition cache HIT: {}.{}.{} date={}, rows={}, size={}KB, projection={:?}, predicates={}",
                key.catalog,
                key.schema,
                key.table,
                key.date_str(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                entry.size_bytes / 1024,
                projection,
                predicates.len()
            );

            return Some(batches);
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Put partition data into cache
    /// Returns true if cached, false if skipped
    pub fn put(&self, key: PartitionKey, batches: Vec<RecordBatch>, file_count: usize) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Only cache hot dates
        if !self.is_hot_date(key.date) {
            debug!(
                "Hot partition cache SKIP (not hot): {}.{}.{} date={}",
                key.catalog,
                key.schema,
                key.table,
                key.date_str()
            );
            return false;
        }

        // Apply dictionary encoding to reduce memory usage
        let encoded_batches: Vec<RecordBatch> = batches
            .iter()
            .filter_map(|batch| match dictionary_encoding::encode_batch(batch) {
                Ok(encoded) => Some(encoded),
                Err(e) => {
                    warn!("Failed to encode batch: {}, using original", e);
                    Some(batch.clone())
                }
            })
            .collect();

        // Calculate size after encoding
        let size_bytes = Self::estimate_batch_size(&encoded_batches);
        let row_count: usize = encoded_batches.iter().map(|b| b.num_rows()).sum();

        // Skip very large partitions (> 25% of max cache)
        if size_bytes > self.config.max_size_bytes / 4 {
            debug!(
                "Hot partition cache SKIP (too large): {}.{}.{} date={}, size={}MB",
                key.catalog,
                key.schema,
                key.table,
                key.date_str(),
                size_bytes / 1024 / 1024
            );
            return false;
        }

        // Ensure space is available
        self.ensure_space(size_bytes);

        let entry = CachedPartition {
            batches: encoded_batches,
            created_at: Instant::now(),
            size_bytes,
            row_count,
            file_count,
            hit_count: AtomicU64::new(0),
        };

        let mut cache = self.cache.write();

        // If replacing existing entry, adjust size and row count
        if let Some(old) = cache.insert(key.clone(), entry) {
            self.current_size
                .fetch_sub(old.size_bytes as u64, Ordering::Relaxed);
            self.total_rows
                .fetch_sub(old.row_count as u64, Ordering::Relaxed);
        }

        self.current_size
            .fetch_add(size_bytes as u64, Ordering::Relaxed);
        self.total_rows
            .fetch_add(row_count as u64, Ordering::Relaxed);

        debug!(
            "Hot partition cache PUT: {}.{}.{} date={}, rows={}, size={}KB (encoded), files={}",
            key.catalog,
            key.schema,
            key.table,
            key.date_str(),
            row_count,
            size_bytes / 1024,
            file_count
        );

        true
    }

    /// Invalidate cache entries for a specific table (all partitions)
    pub fn invalidate_table(&self, catalog: &str, schema: &str, table: &str) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.cache.write();

        let keys_to_remove: Vec<PartitionKey> = cache
            .iter()
            .filter(|(k, _)| k.catalog == catalog && k.schema == schema && k.table == table)
            .map(|(k, _)| k.clone())
            .collect();

        let removed_count = keys_to_remove.len();

        for key in keys_to_remove {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
                self.total_rows
                    .fetch_sub(entry.row_count as u64, Ordering::Relaxed);
            }
        }

        if removed_count > 0 {
            self.stats
                .invalidations
                .fetch_add(removed_count as u64, Ordering::Relaxed);
            debug!(
                "Hot partition cache INVALIDATE: table={}.{}.{}, removed={} partitions",
                catalog, schema, table, removed_count
            );
        }
    }

    /// Invalidate a specific partition
    pub fn invalidate_partition(&self, key: &PartitionKey) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.cache.write();

        if let Some(entry) = cache.remove(key) {
            self.current_size
                .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
            self.total_rows
                .fetch_sub(entry.row_count as u64, Ordering::Relaxed);
            self.stats.invalidations.fetch_add(1, Ordering::Relaxed);

            debug!(
                "Hot partition cache INVALIDATE: {}.{}.{} date={}",
                key.catalog,
                key.schema,
                key.table,
                key.date_str()
            );
        }
    }

    /// Invalidate all cache entries for a catalog (database)
    pub fn invalidate_catalog(&self, catalog: &str) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.cache.write();

        let keys_to_remove: Vec<PartitionKey> = cache
            .iter()
            .filter(|(k, _)| k.catalog == catalog)
            .map(|(k, _)| k.clone())
            .collect();

        let removed_count = keys_to_remove.len();

        for key in keys_to_remove {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
                self.total_rows
                    .fetch_sub(entry.row_count as u64, Ordering::Relaxed);
            }
        }

        if removed_count > 0 {
            self.stats
                .invalidations
                .fetch_add(removed_count as u64, Ordering::Relaxed);
            debug!(
                "Hot partition cache INVALIDATE DB: catalog={}, removed={} partitions",
                catalog, removed_count
            );
        }
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        let count = cache.len();
        cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
        self.total_rows.store(0, Ordering::Relaxed);
        info!("Hot partition cache CLEARED: {} entries removed", count);
    }

    /// Get cache statistics
    pub fn stats(&self) -> HotPartitionCacheStats {
        let cache = self.cache.read();
        HotPartitionCacheStats {
            enabled: self.config.enabled,
            entry_count: cache.len(),
            size_bytes: self.current_size.load(Ordering::Relaxed) as usize,
            max_size_bytes: self.config.max_size_bytes,
            total_rows: self.total_rows.load(Ordering::Relaxed),
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            invalidations: self.stats.invalidations.load(Ordering::Relaxed),
        }
    }

    /// Estimate size of record batches in bytes
    fn estimate_batch_size(batches: &[RecordBatch]) -> usize {
        batches
            .iter()
            .map(|b| {
                b.columns()
                    .iter()
                    .map(|col| col.get_buffer_memory_size())
                    .sum::<usize>()
            })
            .sum()
    }

    /// Ensure there's space for new entry by evicting if necessary
    fn ensure_space(&self, needed_bytes: usize) {
        let current = self.current_size.load(Ordering::Relaxed) as usize;
        let max = self.config.max_size_bytes;

        if current + needed_bytes <= max {
            return;
        }

        let mut cache = self.cache.write();
        let ttl = Duration::from_secs(self.config.ttl_secs);

        // First evict expired entries
        self.evict_expired(&mut cache, ttl);

        // If still over, evict LRU entries
        while self.current_size.load(Ordering::Relaxed) as usize + needed_bytes > max
            && !cache.is_empty()
        {
            self.evict_lru(&mut cache);
        }
    }

    /// Evict expired entries
    fn evict_expired(&self, cache: &mut HashMap<PartitionKey, CachedPartition>, ttl: Duration) {
        let expired: Vec<PartitionKey> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired(ttl))
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
                self.total_rows
                    .fetch_sub(entry.row_count as u64, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Evict least recently used entry (lowest hit count / age)
    fn evict_lru(&self, cache: &mut HashMap<PartitionKey, CachedPartition>) {
        // Find entry with lowest score (hit_count / age_seconds)
        let victim = cache
            .iter()
            .map(|(k, e)| {
                let age_secs = e.created_at.elapsed().as_secs().max(1);
                let hits = e.hit_count.load(Ordering::Relaxed);
                let score = hits as f64 / age_secs as f64;
                (k.clone(), score)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(k, _)| k);

        if let Some(key) = victim {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
                self.total_rows
                    .fetch_sub(entry.row_count as u64, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "Hot partition cache EVICT: {}.{}.{} date={}, size={}KB",
                    key.catalog,
                    key.schema,
                    key.table,
                    key.date_str(),
                    entry.size_bytes / 1024
                );
            }
        }
    }
}

impl Default for HotPartitionCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Thread-safe wrapper for hot partition cache
pub type SharedHotPartitionCache = Arc<HotPartitionCache>;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        let name_array =
            StringArray::from((0..rows).map(|i| format!("name_{}", i)).collect::<Vec<_>>());

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    fn today_date_int() -> i32 {
        let today = Local::now();
        today.year() * 10000 + today.month() as i32 * 100 + today.day() as i32
    }

    #[test]
    fn test_partition_key_from_date_str() {
        let key = PartitionKey::from_date_str("catalog", "schema", "table", "2024-01-15").unwrap();
        assert_eq!(key.date, 20240115);
        assert_eq!(key.date_str(), "2024-01-15");
    }

    #[test]
    fn test_is_hot_date() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            hot_days: 1,
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let today = today_date_int();
        assert!(cache.is_hot_date(today));

        // Very old date should not be hot
        assert!(!cache.is_hot_date(20200101));
    }

    #[test]
    fn test_cache_put_get() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            hot_days: 30, // Make sure test date is hot
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let today = today_date_int();
        let key = PartitionKey::new("testdb", "default", "cpu", today);
        let batch = create_test_batch(100);

        // Put into cache
        assert!(cache.put(key.clone(), vec![batch], 1));

        // Get from cache (no projection, no predicates)
        let result = cache.get(&key, None, &[], None);
        assert!(result.is_some());
        let cached = result.unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].num_rows(), 100);
        assert_eq!(cached[0].num_columns(), 2);

        // Get with projection (only first column)
        let result = cache.get(&key, Some(&[0]), &[], None);
        assert!(result.is_some());
        let cached = result.unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].num_rows(), 100);
        assert_eq!(cached[0].num_columns(), 1);

        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_cache_miss() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let key = PartitionKey::new("testdb", "default", "cpu", 20240115);
        let result = cache.get(&key, None, &[], None);

        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            hot_days: 30,
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let today = today_date_int();
        let yesterday = today - 1;

        // Add entries for two dates
        let key1 = PartitionKey::new("testdb", "default", "cpu", today);
        let key2 = PartitionKey::new("testdb", "default", "cpu", yesterday);
        let batch = create_test_batch(10);

        cache.put(key1.clone(), vec![batch.clone()], 1);
        cache.put(key2.clone(), vec![batch], 1);

        assert_eq!(cache.stats().entry_count, 2);

        // Invalidate table
        cache.invalidate_table("testdb", "default", "cpu");

        assert_eq!(cache.stats().entry_count, 0);
        assert!(cache.get(&key1, None, &[], None).is_none());
        assert!(cache.get(&key2, None, &[], None).is_none());
    }

    #[test]
    fn test_skip_old_date() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            hot_days: 1, // Only today and yesterday
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        // Try to cache very old date
        let key = PartitionKey::new("testdb", "default", "cpu", 20200101);
        let batch = create_test_batch(10);

        // Should be skipped (not hot)
        assert!(!cache.put(key, vec![batch], 1));
        assert_eq!(cache.stats().entry_count, 0);
    }

    #[test]
    fn test_cache_disabled() {
        let config = HotPartitionCacheConfig {
            enabled: false,
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let today = today_date_int();
        let key = PartitionKey::new("testdb", "default", "cpu", today);
        let batch = create_test_batch(10);

        assert!(!cache.put(key.clone(), vec![batch], 1));
        assert!(cache.get(&key, None, &[], None).is_none());
    }

    #[test]
    fn test_cache_clear() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            hot_days: 30,
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let today = today_date_int();
        let key = PartitionKey::new("testdb", "default", "cpu", today);
        let batch = create_test_batch(10);

        cache.put(key, vec![batch], 1);
        assert_eq!(cache.stats().entry_count, 1);

        cache.clear();
        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(cache.stats().size_bytes, 0);
    }

    #[test]
    fn test_cache_stale_file_count() {
        let config = HotPartitionCacheConfig {
            enabled: true,
            hot_days: 30,
            ..Default::default()
        };
        let cache = HotPartitionCache::new(config);

        let today = today_date_int();
        let key = PartitionKey::new("testdb", "default", "cpu", today);
        let batch = create_test_batch(100);

        // Put with file_count=3
        assert!(cache.put(key.clone(), vec![batch], 3));

        // Get with matching file count → HIT
        let result = cache.get(&key, None, &[], Some(3));
        assert!(result.is_some());

        // Get with different file count (new file written) → MISS (stale)
        let result = cache.get(&key, None, &[], Some(4));
        assert!(result.is_none());

        // Get with no file count check → HIT
        let result = cache.get(&key, None, &[], None);
        assert!(result.is_some());

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }
}
