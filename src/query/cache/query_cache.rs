use datafusion::arrow::array::RecordBatch;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Default TTL for cached query results (5 minutes)
const DEFAULT_CACHE_TTL_SECS: u64 = 300;

/// Default maximum cache size in bytes (256 MB)
const DEFAULT_MAX_CACHE_SIZE_BYTES: usize = 256 * 1024 * 1024;

/// Default maximum number of cached entries
const DEFAULT_MAX_ENTRIES: usize = 1000;

/// Cache key for query results
#[derive(Clone, Debug)]
pub struct QueryCacheKey {
    /// Database name
    pub db: String,
    /// Normalized SQL query (lowercase, trimmed)
    pub sql_normalized: String,
    /// Hash of the SQL for fast comparison
    sql_hash: u64,
}

impl QueryCacheKey {
    pub fn new(db: &str, sql: &str) -> Self {
        let sql_normalized = Self::normalize_sql(sql);
        let sql_hash = Self::hash_sql(&sql_normalized);
        Self {
            db: db.to_string(),
            sql_normalized,
            sql_hash,
        }
    }

    /// Normalize SQL for caching (lowercase, trim whitespace)
    fn normalize_sql(sql: &str) -> String {
        // Remove extra whitespace and normalize
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    }

    /// Compute hash of normalized SQL
    fn hash_sql(sql: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        hasher.finish()
    }
}

impl PartialEq for QueryCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.db == other.db
            && self.sql_hash == other.sql_hash
            && self.sql_normalized == other.sql_normalized
    }
}

impl Eq for QueryCacheKey {}

impl Hash for QueryCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.db.hash(state);
        self.sql_hash.hash(state);
    }
}

/// Cached query result entry
struct CachedQueryResult {
    /// The cached record batches
    batches: Vec<RecordBatch>,
    /// When this entry was created
    created_at: Instant,
    /// Time-to-live for this entry
    ttl: Duration,
    /// Estimated size in bytes
    size_bytes: usize,
    /// Number of rows in result
    row_count: usize,
    /// Tables referenced by this query (for invalidation)
    referenced_tables: Vec<String>,
    /// Hit count for this entry
    hit_count: AtomicU64,
}

impl CachedQueryResult {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Configuration for query result cache
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    /// Enable/disable the cache
    pub enabled: bool,
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Default TTL for entries
    pub default_ttl_secs: u64,
    /// Minimum query execution time (ms) to cache result
    /// Only cache queries that take longer than this threshold
    pub min_query_time_ms: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size_bytes: DEFAULT_MAX_CACHE_SIZE_BYTES,
            max_entries: DEFAULT_MAX_ENTRIES,
            default_ttl_secs: DEFAULT_CACHE_TTL_SECS,
            min_query_time_ms: 10, // Only cache queries taking >10ms
        }
    }
}

impl From<&crate::config::QueryCacheConfig> for QueryCacheConfig {
    fn from(config: &crate::config::QueryCacheConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_size_bytes: (config.max_size_mb as usize) * 1024 * 1024,
            max_entries: config.max_entries,
            default_ttl_secs: config.ttl_secs,
            min_query_time_ms: config.min_query_time_ms,
        }
    }
}

/// Statistics for query result cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCacheStats {
    pub enabled: bool,
    pub entry_count: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

impl QueryCacheStats {
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

/// Query result cache
pub struct QueryResultCache {
    /// Cache entries
    cache: RwLock<HashMap<QueryCacheKey, CachedQueryResult>>,
    /// Configuration
    config: QueryCacheConfig,
    /// Current cache size in bytes
    current_size: AtomicU64,
    /// Statistics
    stats: CacheStatsCounters,
}

#[derive(Default)]
struct CacheStatsCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
}

impl QueryResultCache {
    pub fn new(config: QueryCacheConfig) -> Self {
        info!(
            "QueryResultCache initialized: enabled={}, max_size={}MB, max_entries={}, ttl={}s",
            config.enabled,
            config.max_size_bytes / 1024 / 1024,
            config.max_entries,
            config.default_ttl_secs
        );

        Self {
            cache: RwLock::new(HashMap::new()),
            config,
            current_size: AtomicU64::new(0),
            stats: CacheStatsCounters::default(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(QueryCacheConfig::default())
    }

    /// Check if caching is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get cached query result if available and not expired
    pub fn get(&self, key: &QueryCacheKey) -> Option<Vec<RecordBatch>> {
        if !self.config.enabled {
            return None;
        }

        let cache = self.cache.read();

        if let Some(entry) = cache.get(key) {
            if entry.is_expired() {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            entry.hit_count.fetch_add(1, Ordering::Relaxed);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);

            debug!(
                "Query cache HIT: db={}, sql_hash={}, rows={}, size={}",
                key.db, key.sql_hash, entry.row_count, entry.size_bytes
            );

            return Some(entry.batches.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Put query result into cache
    /// Returns true if cached, false if skipped
    pub fn put(
        &self,
        key: QueryCacheKey,
        batches: Vec<RecordBatch>,
        referenced_tables: Vec<String>,
        query_time_ms: u64,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Skip caching if query was too fast
        if query_time_ms < self.config.min_query_time_ms {
            debug!(
                "Query cache SKIP (too fast): db={}, time={}ms < {}ms",
                key.db, query_time_ms, self.config.min_query_time_ms
            );
            return false;
        }

        // Calculate size
        let size_bytes = Self::estimate_batch_size(&batches);
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Skip very large results
        if size_bytes > self.config.max_size_bytes / 4 {
            debug!(
                "Query cache SKIP (too large): db={}, size={}MB",
                key.db,
                size_bytes / 1024 / 1024
            );
            return false;
        }

        // Ensure space is available
        self.ensure_space(size_bytes);

        let entry = CachedQueryResult {
            batches,
            created_at: Instant::now(),
            ttl: Duration::from_secs(self.config.default_ttl_secs),
            size_bytes,
            row_count,
            referenced_tables,
            hit_count: AtomicU64::new(0),
        };

        let mut cache = self.cache.write();

        // Check max entries
        if cache.len() >= self.config.max_entries {
            self.evict_lru(&mut cache);
        }

        // If replacing existing entry, adjust size
        if let Some(old) = cache.insert(key.clone(), entry) {
            self.current_size
                .fetch_sub(old.size_bytes as u64, Ordering::Relaxed);
        }

        self.current_size
            .fetch_add(size_bytes as u64, Ordering::Relaxed);

        debug!(
            "Query cache PUT: db={}, rows={}, size={}, tables={:?}",
            key.db,
            row_count,
            size_bytes,
            key.sql_normalized.chars().take(50).collect::<String>()
        );

        true
    }

    /// Invalidate cache entries for a specific table
    pub fn invalidate_table(&self, catalog: &str, schema: &str, table: &str) {
        if !self.config.enabled {
            return;
        }

        let table_ref = format!("{}.{}.{}", catalog, schema, table);
        let mut cache = self.cache.write();

        let keys_to_remove: Vec<QueryCacheKey> = cache
            .iter()
            .filter(|(_, entry)| {
                entry
                    .referenced_tables
                    .iter()
                    .any(|t| t == &table_ref || t.ends_with(&format!(".{}", table)))
            })
            .map(|(k, _)| k.clone())
            .collect();

        let removed_count = keys_to_remove.len();

        for key in keys_to_remove {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
            }
        }

        if removed_count > 0 {
            self.stats
                .invalidations
                .fetch_add(removed_count as u64, Ordering::Relaxed);
            debug!(
                "Query cache INVALIDATE: table={}, removed={} entries",
                table_ref, removed_count
            );
        }
    }

    /// Invalidate all cache entries for a database
    pub fn invalidate_database(&self, db: &str) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.cache.write();

        let keys_to_remove: Vec<QueryCacheKey> = cache
            .iter()
            .filter(|(k, _)| k.db == db)
            .map(|(k, _)| k.clone())
            .collect();

        let removed_count = keys_to_remove.len();

        for key in keys_to_remove {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
            }
        }

        if removed_count > 0 {
            self.stats
                .invalidations
                .fetch_add(removed_count as u64, Ordering::Relaxed);
            debug!(
                "Query cache INVALIDATE DB: db={}, removed={} entries",
                db, removed_count
            );
        }
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        let count = cache.len();
        cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
        info!("Query cache CLEARED: {} entries removed", count);
    }

    /// Get cache statistics
    pub fn stats(&self) -> QueryCacheStats {
        let cache = self.cache.read();
        QueryCacheStats {
            enabled: self.config.enabled,
            entry_count: cache.len(),
            size_bytes: self.current_size.load(Ordering::Relaxed) as usize,
            max_size_bytes: self.config.max_size_bytes,
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
        self.evict_expired(&mut cache);

        // If still over, evict LRU entries
        while self.current_size.load(Ordering::Relaxed) as usize + needed_bytes > max
            && !cache.is_empty()
        {
            self.evict_lru(&mut cache);
        }
    }

    /// Evict expired entries
    fn evict_expired(&self, cache: &mut HashMap<QueryCacheKey, CachedQueryResult>) {
        let expired: Vec<QueryCacheKey> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired {
            if let Some(entry) = cache.remove(&key) {
                self.current_size
                    .fetch_sub(entry.size_bytes as u64, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Evict least recently used entry (lowest hit count + oldest)
    fn evict_lru(&self, cache: &mut HashMap<QueryCacheKey, CachedQueryResult>) {
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
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "Query cache EVICT: db={}, size={}",
                    key.db, entry.size_bytes
                );
            }
        }
    }

    /// Get configuration
    pub fn config(&self) -> &QueryCacheConfig {
        &self.config
    }
}

impl Default for QueryResultCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Thread-safe wrapper for query result cache
pub type SharedQueryResultCache = Arc<QueryResultCache>;

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

    #[test]
    fn test_cache_key_normalization() {
        let key1 = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let key2 = QueryCacheKey::new("testdb", "select * from cpu");
        let key3 = QueryCacheKey::new("testdb", "SELECT  *   FROM   cpu");

        assert_eq!(key1.sql_normalized, key2.sql_normalized);
        assert_eq!(key1.sql_normalized, key3.sql_normalized);
        assert_eq!(key1, key2);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_cache_key_different_db() {
        let key1 = QueryCacheKey::new("db1", "SELECT * FROM cpu");
        let key2 = QueryCacheKey::new("db2", "SELECT * FROM cpu");

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_put_get() {
        let config = QueryCacheConfig {
            min_query_time_ms: 0, // Don't skip any queries
            ..Default::default()
        };
        let cache = QueryResultCache::new(config);

        let key = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let batch = create_test_batch(100);
        let batches = vec![batch];

        // Put into cache
        assert!(cache.put(
            key.clone(),
            batches.clone(),
            vec!["testdb.default.cpu".to_string()],
            100
        ));

        // Get from cache
        let result = cache.get(&key);
        assert!(result.is_some());
        let cached_batches = result.unwrap();
        assert_eq!(cached_batches.len(), 1);
        assert_eq!(cached_batches[0].num_rows(), 100);

        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryResultCache::with_defaults();

        let key = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let result = cache.get(&key);

        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let config = QueryCacheConfig {
            min_query_time_ms: 0,
            ..Default::default()
        };
        let cache = QueryResultCache::new(config);

        // Add some entries
        let key1 = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let key2 = QueryCacheKey::new("testdb", "SELECT * FROM memory");
        let batch = create_test_batch(10);

        cache.put(
            key1.clone(),
            vec![batch.clone()],
            vec!["testdb.default.cpu".to_string()],
            100,
        );
        cache.put(
            key2.clone(),
            vec![batch],
            vec!["testdb.default.memory".to_string()],
            100,
        );

        assert_eq!(cache.stats().entry_count, 2);

        // Invalidate one table
        cache.invalidate_table("testdb", "default", "cpu");

        assert_eq!(cache.stats().entry_count, 1);
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_some());
    }

    #[test]
    fn test_cache_database_invalidation() {
        let config = QueryCacheConfig {
            min_query_time_ms: 0,
            ..Default::default()
        };
        let cache = QueryResultCache::new(config);

        // Add entries from different databases
        let key1 = QueryCacheKey::new("db1", "SELECT * FROM cpu");
        let key2 = QueryCacheKey::new("db2", "SELECT * FROM cpu");
        let batch = create_test_batch(10);

        cache.put(
            key1.clone(),
            vec![batch.clone()],
            vec!["db1.default.cpu".to_string()],
            100,
        );
        cache.put(
            key2.clone(),
            vec![batch],
            vec!["db2.default.cpu".to_string()],
            100,
        );

        assert_eq!(cache.stats().entry_count, 2);

        // Invalidate one database
        cache.invalidate_database("db1");

        assert_eq!(cache.stats().entry_count, 1);
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_some());
    }

    #[test]
    fn test_cache_clear() {
        let config = QueryCacheConfig {
            min_query_time_ms: 0,
            ..Default::default()
        };
        let cache = QueryResultCache::new(config);

        let key = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let batch = create_test_batch(10);

        cache.put(
            key,
            vec![batch],
            vec!["testdb.default.cpu".to_string()],
            100,
        );
        assert_eq!(cache.stats().entry_count, 1);

        cache.clear();
        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(cache.stats().size_bytes, 0);
    }

    #[test]
    fn test_cache_skip_fast_queries() {
        let config = QueryCacheConfig {
            min_query_time_ms: 100, // Only cache queries >100ms
            ..Default::default()
        };
        let cache = QueryResultCache::new(config);

        let key = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let batch = create_test_batch(10);

        // Fast query should be skipped
        assert!(!cache.put(key.clone(), vec![batch.clone()], vec![], 50));
        assert!(cache.get(&key).is_none());

        // Slow query should be cached
        assert!(cache.put(key.clone(), vec![batch], vec![], 150));
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn test_cache_disabled() {
        let config = QueryCacheConfig {
            enabled: false,
            ..Default::default()
        };
        let cache = QueryResultCache::new(config);

        let key = QueryCacheKey::new("testdb", "SELECT * FROM cpu");
        let batch = create_test_batch(10);

        assert!(!cache.put(key.clone(), vec![batch], vec![], 100));
        assert!(cache.get(&key).is_none());
    }
}
