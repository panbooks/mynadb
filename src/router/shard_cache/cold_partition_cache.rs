//! Cold Partition Cache - For data already flushed to S3/Parquet
//!
//! Source of Truth: S3 LIST (immutable once written)
//! Eviction: LRU with max_entries (NOT TTL - data never changes!)
//!
//! Key properties:
//! - Contains historical partitions discovered via S3 LIST
//! - Data is IMMUTABLE - once discovered, mapping never changes
//! - LRU eviction based on access pattern, not time
//! - Bounded by max_entries configuration

use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use tracing::debug;

/// Cold partition cache for S3/Parquet data routing
///
/// Caches partition -> node_id mappings discovered via S3 LIST.
/// Uses LRU eviction to bound memory usage while keeping frequently
/// accessed partitions cached.
pub struct ColdPartitionCache {
    /// LRU cache: partition_key -> node_ids
    /// Key: "catalog/schema/table:date" (e.g., "mydb/default/cpu:2024-01-15")
    /// Value: List of node_ids that have parquet files for this partition
    cache: Mutex<LruCache<String, ColdPartitionInfo>>,

    /// Maximum number of entries (for stats)
    max_entries: usize,
}

/// Per-partition info for cold (S3) data
#[derive(Debug, Clone)]
pub struct ColdPartitionInfo {
    /// Nodes with parquet files (from S3 LIST discovery)
    /// IMMUTABLE - once discovered, this never changes
    pub node_ids: Vec<String>,

    /// When discovered (for metrics/debugging only, NOT for eviction)
    pub discovered_at: i64,
}

impl ColdPartitionCache {
    /// Create a new cold partition cache with specified max entries
    pub fn new(max_entries: usize) -> Self {
        let capacity = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            cache: Mutex::new(LruCache::new(capacity)),
            max_entries,
        }
    }

    /// Generate cache key
    fn cache_key(catalog: &str, schema: &str, table: &str, date: &str) -> String {
        format!("{}/{}/{}:{}", catalog, schema, table, date)
    }

    /// Insert a discovered partition into the cache
    ///
    /// Called after S3 LIST discovers which nodes have data for a partition.
    /// LRU eviction happens automatically when cache is full.
    pub fn insert(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
        node_ids: Vec<String>,
    ) {
        if node_ids.is_empty() {
            return;
        }

        let key = Self::cache_key(catalog, schema, table, date);
        let now = chrono::Utc::now().timestamp_millis();

        let info = ColdPartitionInfo {
            node_ids: node_ids.clone(),
            discovered_at: now,
        };

        let mut cache = self.cache.lock();
        cache.put(key.clone(), info);

        debug!(
            "Cold cache: inserted {} -> {:?} (size: {}/{})",
            key,
            node_ids,
            cache.len(),
            self.max_entries
        );
    }

    /// Get nodes with cold (S3) data for a partition
    ///
    /// Returns None if partition not in cache (triggers S3 LIST discovery).
    /// Updates LRU recency on access.
    pub fn get(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Option<Vec<String>> {
        let key = Self::cache_key(catalog, schema, table, date);
        let mut cache = self.cache.lock();

        cache.get(&key).map(|info| info.node_ids.clone())
    }

    /// Peek at nodes without updating LRU recency
    ///
    /// Useful for read-only checks without affecting eviction order.
    pub fn peek(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Option<Vec<String>> {
        let key = Self::cache_key(catalog, schema, table, date);
        let cache = self.cache.lock();

        cache.peek(&key).map(|info| info.node_ids.clone())
    }

    /// Get nodes for multiple dates (for range queries)
    ///
    /// Returns (found_nodes, missing_dates):
    /// - found_nodes: All nodes for dates that are in cache
    /// - missing_dates: Dates that need S3 LIST discovery
    ///
    /// This allows partial cache hits - we don't fail the entire lookup
    /// just because one date is missing.
    pub fn get_for_dates(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        dates: &[String],
    ) -> (Vec<String>, Vec<String>) {
        let mut all_nodes = std::collections::HashSet::new();
        let mut missing_dates = Vec::new();
        let mut cache = self.cache.lock();

        for date in dates {
            let key = Self::cache_key(catalog, schema, table, date);
            if let Some(info) = cache.get(&key) {
                for node_id in &info.node_ids {
                    all_nodes.insert(node_id.clone());
                }
            } else {
                missing_dates.push(date.clone());
            }
        }

        (all_nodes.into_iter().collect(), missing_dates)
    }

    /// Remove a node from all cached partitions (for scale-in)
    ///
    /// Called when a node permanently leaves the cluster.
    /// Unlike hot cache, we DON'T remove the node - the data still exists
    /// in S3 and can be read by any node (shared storage).
    ///
    /// Instead, we mark the partition as needing re-discovery on next access.
    /// This is a no-op for now since any node can read S3 data.
    ///
    /// Returns the number of partitions that contained this node.
    pub fn mark_node_departed(&self, node_id: &str) -> usize {
        let cache = self.cache.lock();
        let mut affected = 0;

        for (_, info) in cache.iter() {
            if info.node_ids.contains(&node_id.to_string()) {
                affected += 1;
                // Note: We don't remove the entry because:
                // 1. Data still exists in S3 under node_id=<departed_node>
                // 2. Any active node can read it via shared S3 storage
                // 3. Query routing will use fallback node for departed node's partitions
            }
        }

        if affected > 0 {
            debug!(
                "Cold cache: {} partitions reference departed node {}",
                affected, node_id
            );
        }

        affected
    }

    /// Remove specific partition from cache
    ///
    /// Called when partition data is deleted (e.g., retention policy).
    pub fn remove(&self, catalog: &str, schema: &str, table: &str, date: &str) {
        let key = Self::cache_key(catalog, schema, table, date);
        let mut cache = self.cache.lock();
        cache.pop(&key);
    }

    /// Remove all partitions for a table
    ///
    /// Called when a table is dropped.
    pub fn remove_table(&self, catalog: &str, schema: &str, table: &str) {
        let prefix = format!("{}/{}/{}:", catalog, schema, table);
        let mut cache = self.cache.lock();

        // Collect keys to remove (can't modify while iterating)
        let keys_to_remove: Vec<String> = cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            cache.pop(&key);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> ColdCacheStats {
        let cache = self.cache.lock();
        let mut total_node_refs = 0;

        for (_, info) in cache.iter() {
            total_node_refs += info.node_ids.len();
        }

        ColdCacheStats {
            partition_count: cache.len(),
            max_entries: self.max_entries,
            total_node_refs,
            utilization_percent: if self.max_entries > 0 {
                (cache.len() * 100) / self.max_entries
            } else {
                0
            },
        }
    }

    /// Clear all entries (for testing or reset)
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
    }

    /// Resize the cache (for dynamic configuration)
    pub fn resize(&self, new_max_entries: usize) {
        let capacity =
            NonZeroUsize::new(new_max_entries).unwrap_or(NonZeroUsize::new(1).unwrap());
        let mut cache = self.cache.lock();
        cache.resize(capacity);
    }

    /// Preload partitions from a list (for warm-up on startup)
    ///
    /// Takes a list of (catalog, schema, table, date, node_ids) tuples
    /// and populates the cache. Useful for pre-warming after restart.
    pub fn preload(&self, entries: Vec<(String, String, String, String, Vec<String>)>) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut cache = self.cache.lock();

        for (catalog, schema, table, date, node_ids) in entries {
            if node_ids.is_empty() {
                continue;
            }

            let key = Self::cache_key(&catalog, &schema, &table, &date);
            let info = ColdPartitionInfo {
                node_ids,
                discovered_at: now,
            };
            cache.put(key, info);
        }

        debug!("Cold cache: preloaded {} entries", cache.len());
    }
}

/// Statistics for cold partition cache
#[derive(Debug, Clone)]
pub struct ColdCacheStats {
    pub partition_count: usize,
    pub max_entries: usize,
    pub total_node_refs: usize,
    pub utilization_percent: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let cache = ColdPartitionCache::new(100);

        cache.insert(
            "mydb",
            "default",
            "cpu",
            "2024-01-15",
            vec!["node-1".to_string(), "node-2".to_string()],
        );

        let nodes = cache.get("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&"node-1".to_string()));
        assert!(nodes.contains(&"node-2".to_string()));
    }

    #[test]
    fn test_cache_miss() {
        let cache = ColdPartitionCache::new(100);

        let nodes = cache.get("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let cache = ColdPartitionCache::new(2); // Very small cache

        // Insert 3 entries, first one should be evicted
        cache.insert("mydb", "default", "cpu", "2024-01-15", vec!["node-1".to_string()]);
        cache.insert("mydb", "default", "cpu", "2024-01-16", vec!["node-2".to_string()]);
        cache.insert("mydb", "default", "cpu", "2024-01-17", vec!["node-3".to_string()]);

        // First entry should be evicted
        assert!(cache.get("mydb", "default", "cpu", "2024-01-15").is_none());
        // Later entries should exist
        assert!(cache.get("mydb", "default", "cpu", "2024-01-16").is_some());
        assert!(cache.get("mydb", "default", "cpu", "2024-01-17").is_some());
    }

    #[test]
    fn test_lru_access_updates_recency() {
        let cache = ColdPartitionCache::new(2);

        cache.insert("mydb", "default", "cpu", "2024-01-15", vec!["node-1".to_string()]);
        cache.insert("mydb", "default", "cpu", "2024-01-16", vec!["node-2".to_string()]);

        // Access first entry (makes it most recent)
        cache.get("mydb", "default", "cpu", "2024-01-15");

        // Insert third entry - should evict 2024-01-16 (now least recent)
        cache.insert("mydb", "default", "cpu", "2024-01-17", vec!["node-3".to_string()]);

        // 2024-01-15 should still exist (was accessed recently)
        assert!(cache.get("mydb", "default", "cpu", "2024-01-15").is_some());
        // 2024-01-16 should be evicted
        assert!(cache.get("mydb", "default", "cpu", "2024-01-16").is_none());
    }

    #[test]
    fn test_get_for_dates_partial() {
        let cache = ColdPartitionCache::new(100);

        cache.insert("mydb", "default", "cpu", "2024-01-15", vec!["node-1".to_string()]);
        cache.insert("mydb", "default", "cpu", "2024-01-16", vec!["node-2".to_string()]);
        // 2024-01-17 not in cache

        let dates = vec![
            "2024-01-15".to_string(),
            "2024-01-16".to_string(),
            "2024-01-17".to_string(),
        ];

        let (found_nodes, missing_dates) =
            cache.get_for_dates("mydb", "default", "cpu", &dates);

        assert_eq!(found_nodes.len(), 2);
        assert_eq!(missing_dates, vec!["2024-01-17".to_string()]);
    }

    #[test]
    fn test_remove_table() {
        let cache = ColdPartitionCache::new(100);

        cache.insert("mydb", "default", "cpu", "2024-01-15", vec!["node-1".to_string()]);
        cache.insert("mydb", "default", "cpu", "2024-01-16", vec!["node-1".to_string()]);
        cache.insert("mydb", "default", "mem", "2024-01-15", vec!["node-1".to_string()]);

        cache.remove_table("mydb", "default", "cpu");

        // cpu entries should be gone
        assert!(cache.get("mydb", "default", "cpu", "2024-01-15").is_none());
        assert!(cache.get("mydb", "default", "cpu", "2024-01-16").is_none());
        // mem entry should remain
        assert!(cache.get("mydb", "default", "mem", "2024-01-15").is_some());
    }

    #[test]
    fn test_stats() {
        let cache = ColdPartitionCache::new(100);

        cache.insert(
            "mydb",
            "default",
            "cpu",
            "2024-01-15",
            vec!["node-1".to_string(), "node-2".to_string()],
        );
        cache.insert("mydb", "default", "cpu", "2024-01-16", vec!["node-1".to_string()]);

        let stats = cache.stats();
        assert_eq!(stats.partition_count, 2);
        assert_eq!(stats.total_node_refs, 3);
        assert_eq!(stats.max_entries, 100);
        assert_eq!(stats.utilization_percent, 2);
    }
}
