//! Hot Partition Cache - For data still in memtable (not yet flushed to S3)
//!
//! Source of Truth: Meta Service (via write notifications from WriteRouter)
//! TTL: Configurable, default 5 minutes (matches flush_interval)
//!
//! Key properties:
//! - Contains partitions with recent write activity
//! - Per-node TTL tracking (each node's entry expires independently)
//! - Bounded by TTL eviction (no unbounded growth)
//! - Provides cache affinity for memtable queries

use dashmap::DashMap;
use std::collections::HashMap;
use tracing::debug;

/// Hot partition cache for memtable data routing
///
/// Tracks which nodes have recently written data to which partitions.
/// Used to route queries to nodes that have data in memtable (cache affinity).
pub struct HotPartitionCache {
    /// Partition -> per-node write timestamps
    /// Key: "catalog/schema/table:date" (e.g., "mydb/default/cpu:2024-01-15")
    /// Value: Map of node_id -> last_write_timestamp_ms
    partitions: DashMap<String, HotPartitionInfo>,

    /// TTL in milliseconds (default: 5 min = flush interval)
    ttl_ms: i64,
}

/// Per-partition info tracking which nodes have hot (memtable) data
#[derive(Debug, Clone)]
pub struct HotPartitionInfo {
    /// Map of node_id -> last write timestamp (ms)
    /// Each node's entry expires independently based on its last_write time
    pub node_last_write: HashMap<String, i64>,
}

impl HotPartitionInfo {
    pub fn new() -> Self {
        Self {
            node_last_write: HashMap::new(),
        }
    }

    /// Get nodes with writes within TTL
    pub fn get_active_nodes(&self, now_ms: i64, ttl_ms: i64) -> Vec<String> {
        self.node_last_write
            .iter()
            .filter(|(_, &last_write)| now_ms - last_write < ttl_ms)
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }

    /// Check if all nodes have expired
    pub fn is_all_expired(&self, now_ms: i64, ttl_ms: i64) -> bool {
        self.node_last_write
            .iter()
            .all(|(_, &last_write)| now_ms - last_write >= ttl_ms)
    }
}

impl Default for HotPartitionInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl HotPartitionCache {
    /// Create a new hot partition cache with specified TTL
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            partitions: DashMap::new(),
            ttl_ms: (ttl_secs * 1000) as i64,
        }
    }

    /// Generate cache key
    fn cache_key(catalog: &str, schema: &str, table: &str, date: &str) -> String {
        format!("{}/{}/{}:{}", catalog, schema, table, date)
    }

    /// Record a write to a partition from a specific node
    ///
    /// Called by WriteRouter after successfully routing a write.
    /// Updates the last_write timestamp for the node.
    pub fn record_write(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
        node_id: &str,
    ) {
        let key = Self::cache_key(catalog, schema, table, date);
        let now = chrono::Utc::now().timestamp_millis();

        self.partitions
            .entry(key.clone())
            .and_modify(|info| {
                info.node_last_write.insert(node_id.to_string(), now);
            })
            .or_insert_with(|| {
                let mut info = HotPartitionInfo::new();
                info.node_last_write.insert(node_id.to_string(), now);
                debug!(
                    "Hot cache: new partition {} -> [{}]",
                    key, node_id
                );
                info
            });
    }

    /// Get nodes with hot (memtable) data for a partition
    ///
    /// Returns nodes that have written to this partition within TTL.
    /// Returns None if no active nodes found.
    pub fn get(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Option<Vec<String>> {
        let key = Self::cache_key(catalog, schema, table, date);
        let now = chrono::Utc::now().timestamp_millis();

        self.partitions.get(&key).and_then(|info| {
            let active_nodes = info.get_active_nodes(now, self.ttl_ms);
            if active_nodes.is_empty() {
                None
            } else {
                Some(active_nodes)
            }
        })
    }

    /// Get nodes for multiple dates (for range queries)
    ///
    /// Returns all nodes that have hot data for ANY of the requested dates.
    /// Unlike cold cache, returns partial results (doesn't require all dates to be present).
    pub fn get_for_dates(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        dates: &[String],
    ) -> Vec<String> {
        let now = chrono::Utc::now().timestamp_millis();
        let mut all_nodes = std::collections::HashSet::new();

        for date in dates {
            let key = Self::cache_key(catalog, schema, table, date);
            if let Some(info) = self.partitions.get(&key) {
                for node_id in info.get_active_nodes(now, self.ttl_ms) {
                    all_nodes.insert(node_id);
                }
            }
        }

        all_nodes.into_iter().collect()
    }

    /// Remove a node from all partitions (for scale-in)
    ///
    /// Called when a node is being removed from the cluster.
    /// Returns the number of partitions affected.
    pub fn remove_node(&self, node_id: &str) -> usize {
        let mut affected = 0;

        for mut entry in self.partitions.iter_mut() {
            if entry.node_last_write.remove(node_id).is_some() {
                affected += 1;
            }
        }

        if affected > 0 {
            debug!(
                "Hot cache: removed node {} from {} partitions",
                node_id, affected
            );
        }

        affected
    }

    /// Evict expired entries from the cache
    ///
    /// Called periodically to prevent unbounded growth.
    /// Removes partition entries where ALL nodes have expired.
    pub fn evict_expired(&self) -> usize {
        let now = chrono::Utc::now().timestamp_millis();
        let mut evicted = 0;

        // Find partitions where all nodes have expired
        let keys_to_remove: Vec<String> = self
            .partitions
            .iter()
            .filter(|entry| entry.is_all_expired(now, self.ttl_ms))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.partitions.remove(&key);
            evicted += 1;
        }

        if evicted > 0 {
            debug!("Hot cache: evicted {} expired partitions", evicted);
        }

        evicted
    }

    /// Get cache statistics
    pub fn stats(&self) -> HotCacheStats {
        let now = chrono::Utc::now().timestamp_millis();
        let mut total_entries = 0;
        let mut active_node_refs = 0;
        let mut expired_node_refs = 0;

        for entry in self.partitions.iter() {
            total_entries += 1;
            for (_, &last_write) in &entry.node_last_write {
                if now - last_write < self.ttl_ms {
                    active_node_refs += 1;
                } else {
                    expired_node_refs += 1;
                }
            }
        }

        HotCacheStats {
            partition_count: total_entries,
            active_node_refs,
            expired_node_refs,
            ttl_ms: self.ttl_ms,
        }
    }

    /// Clear all entries (for testing or reset)
    pub fn clear(&self) {
        self.partitions.clear();
    }
}

/// Statistics for hot partition cache
#[derive(Debug, Clone)]
pub struct HotCacheStats {
    pub partition_count: usize,
    pub active_node_refs: usize,
    pub expired_node_refs: usize,
    pub ttl_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_record_and_get() {
        let cache = HotPartitionCache::new(300); // 5 min TTL

        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-1");

        let nodes = cache.get("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        assert_eq!(nodes.unwrap(), vec!["node-1"]);
    }

    #[test]
    fn test_multiple_nodes() {
        let cache = HotPartitionCache::new(300);

        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-1");
        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-2");
        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-3");

        let nodes = cache.get("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_ttl_expiry() {
        let cache = HotPartitionCache::new(1); // 1 second TTL for testing

        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-1");

        // Should exist immediately
        assert!(cache.get("mydb", "default", "cpu", "2024-01-15").is_some());

        // Wait for TTL
        sleep(Duration::from_millis(1100));

        // Should be expired
        assert!(cache.get("mydb", "default", "cpu", "2024-01-15").is_none());
    }

    #[test]
    fn test_remove_node() {
        let cache = HotPartitionCache::new(300);

        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-1");
        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-2");
        cache.record_write("mydb", "default", "cpu", "2024-01-16", "node-1");

        let affected = cache.remove_node("node-1");
        assert_eq!(affected, 2);

        // node-1 should be removed
        let nodes = cache.get("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        assert_eq!(nodes.unwrap(), vec!["node-2"]);
    }

    #[test]
    fn test_get_for_dates() {
        let cache = HotPartitionCache::new(300);

        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-1");
        cache.record_write("mydb", "default", "cpu", "2024-01-16", "node-2");
        cache.record_write("mydb", "default", "cpu", "2024-01-17", "node-1");

        let dates = vec![
            "2024-01-15".to_string(),
            "2024-01-16".to_string(),
            "2024-01-17".to_string(),
            "2024-01-18".to_string(), // Not in cache
        ];

        let nodes = cache.get_for_dates("mydb", "default", "cpu", &dates);
        assert_eq!(nodes.len(), 2); // node-1 and node-2
    }

    #[test]
    fn test_evict_expired() {
        let cache = HotPartitionCache::new(1); // 1 second TTL

        cache.record_write("mydb", "default", "cpu", "2024-01-15", "node-1");
        cache.record_write("mydb", "default", "cpu", "2024-01-16", "node-2");

        sleep(Duration::from_millis(1100));

        let evicted = cache.evict_expired();
        assert_eq!(evicted, 2);

        let stats = cache.stats();
        assert_eq!(stats.partition_count, 0);
    }
}
