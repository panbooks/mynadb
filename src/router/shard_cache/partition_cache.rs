//! Partition cache methods for ShardCache
//!
//! Handles partition→node mapping for smart query routing:
//! - Record which nodes have data for which partitions
//! - Query routing based on partition data location
//! - Cache eviction and cleanup

use super::types::CachedPartitionInfo;
use super::ShardCache;
use tracing::{debug, info};

impl ShardCache {
    // ========================================================================
    // Partition Cache (Smart Query Routing)
    // ========================================================================

    /// Generate partition cache key
    fn partition_cache_key(catalog: &str, schema: &str, table: &str, date: &str) -> String {
        format!("{}/{}/{}:{}", catalog, schema, table, date)
    }

    /// Record that a node has data for a partition
    ///
    /// Called after a successful write/flush to inform the Router which
    /// node has data for which partition. This enables smart query routing.
    pub fn record_partition_node(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
        node_id: &str,
    ) {
        let key = Self::partition_cache_key(catalog, schema, table, date);
        let now = chrono::Utc::now().timestamp_millis();

        self.partition_nodes
            .entry(key.clone())
            .and_modify(|info| {
                if !info.node_ids.contains(&node_id.to_string()) {
                    info.node_ids.push(node_id.to_string());
                    info.cached_at = now;
                    debug!(
                        "Added node {} to partition cache: {} (total: {})",
                        node_id,
                        key,
                        info.node_ids.len()
                    );
                }
            })
            .or_insert_with(|| {
                debug!("Created partition cache entry: {} -> [{}]", key, node_id);
                CachedPartitionInfo {
                    node_ids: vec![node_id.to_string()],
                    cached_at: now,
                }
            });
    }

    /// Get nodes that have data for a specific partition
    ///
    /// Returns None if the partition is not in cache (fallback to table-level routing).
    /// Returns Some(empty) if the partition exists but has no nodes (shouldn't happen).
    pub fn get_partition_nodes(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Option<Vec<String>> {
        let key = Self::partition_cache_key(catalog, schema, table, date);
        self.partition_nodes
            .get(&key)
            .map(|info| info.node_ids.clone())
    }

    /// Get nodes for multiple date partitions (for range queries)
    ///
    /// Returns a deduplicated list of all nodes that have data for any of the
    /// requested partitions. Falls back to table owners if any partition is not cached.
    pub fn get_partition_nodes_for_dates(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        dates: &[String],
    ) -> Option<Vec<String>> {
        let mut all_nodes = std::collections::HashSet::new();

        for date in dates {
            let key = Self::partition_cache_key(catalog, schema, table, date);
            if let Some(info) = self.partition_nodes.get(&key) {
                for node_id in &info.node_ids {
                    all_nodes.insert(node_id.clone());
                }
            } else {
                // If any partition is not cached, return None to trigger fallback
                return None;
            }
        }

        let mut result: Vec<_> = all_nodes.into_iter().collect();
        result.sort(); // Consistent ordering for hash-based selection
        Some(result)
    }

    /// Remove a node from all partition cache entries
    ///
    /// Called when a node leaves the cluster (scale-in or failure).
    /// Returns the number of partition entries affected.
    pub fn remove_node_from_partitions(&self, node_id: &str) -> usize {
        let mut affected = 0;
        let mut empty_partitions = Vec::new();

        for mut entry in self.partition_nodes.iter_mut() {
            let initial_len = entry.node_ids.len();
            entry.node_ids.retain(|id| id != node_id);

            if entry.node_ids.len() < initial_len {
                affected += 1;

                // Mark for removal if no nodes left
                if entry.node_ids.is_empty() {
                    empty_partitions.push(entry.key().clone());
                }
            }
        }

        // Remove empty partition entries
        for key in empty_partitions {
            self.partition_nodes.remove(&key);
        }

        if affected > 0 {
            info!(
                "Removed node {} from {} partition cache entries",
                node_id, affected
            );
        }
        affected
    }

    /// Clear partition cache for a specific table
    ///
    /// Called when a table is dropped or reorganized.
    pub fn clear_partition_cache_for_table(&self, catalog: &str, schema: &str, table: &str) {
        let prefix = format!("{}/{}/{}:", catalog, schema, table);
        let keys_to_remove: Vec<String> = self
            .partition_nodes
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();

        for key in &keys_to_remove {
            self.partition_nodes.remove(key);
        }

        if !keys_to_remove.is_empty() {
            info!(
                "Cleared {} partition cache entries for table {}/{}/{}",
                keys_to_remove.len(),
                catalog,
                schema,
                table
            );
        }
    }

    /// Clear all partition cache entries
    pub fn clear_partition_cache(&self) {
        let count = self.partition_nodes.len();
        self.partition_nodes.clear();
        info!("Cleared all {} partition cache entries", count);
    }

    /// Get partition cache stats
    pub fn partition_cache_stats(&self) -> (usize, usize) {
        let entry_count = self.partition_nodes.len();
        let total_nodes: usize = self
            .partition_nodes
            .iter()
            .map(|entry| entry.node_ids.len())
            .sum();
        (entry_count, total_nodes)
    }

    /// Evict stale partition cache entries older than TTL
    ///
    /// Called periodically to prevent unbounded cache growth.
    pub fn evict_stale_partitions(&self, ttl_ms: i64) {
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - ttl_ms;

        let keys_to_remove: Vec<String> = self
            .partition_nodes
            .iter()
            .filter(|entry| entry.cached_at < cutoff)
            .map(|entry| entry.key().clone())
            .collect();

        for key in &keys_to_remove {
            self.partition_nodes.remove(key);
        }

        if !keys_to_remove.is_empty() {
            debug!(
                "Evicted {} stale partition cache entries (older than {}ms)",
                keys_to_remove.len(),
                ttl_ms
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cache() -> ShardCache {
        ShardCache::new()
    }

    #[test]
    fn test_record_and_get_partition_node() {
        let cache = create_test_cache();

        // Record a partition
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");

        // Get the partition nodes
        let nodes = cache.get_partition_nodes("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(&"data-1".to_string()));
    }

    #[test]
    fn test_partition_multiple_nodes() {
        let cache = create_test_cache();

        // Record same partition from multiple nodes
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-2");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-3");

        // Duplicate should not be added
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");

        let nodes = cache.get_partition_nodes("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains(&"data-1".to_string()));
        assert!(nodes.contains(&"data-2".to_string()));
        assert!(nodes.contains(&"data-3".to_string()));
    }

    #[test]
    fn test_partition_not_found() {
        let cache = create_test_cache();

        // Query non-existent partition
        let nodes = cache.get_partition_nodes("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_none());
    }

    #[test]
    fn test_partition_nodes_for_dates() {
        let cache = create_test_cache();

        // Record partitions for multiple dates
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-16", "data-2");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-17", "data-1");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-17", "data-3");

        // Query multiple dates
        let dates = vec![
            "2024-01-15".to_string(),
            "2024-01-16".to_string(),
            "2024-01-17".to_string(),
        ];
        let nodes = cache.get_partition_nodes_for_dates("mydb", "default", "cpu", &dates);
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 3); // data-1, data-2, data-3
    }

    #[test]
    fn test_partition_nodes_for_dates_partial_cache() {
        let cache = create_test_cache();

        // Record only some partitions
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        // 2024-01-16 is NOT in cache

        // Query should return None (fallback to table-level routing)
        let dates = vec!["2024-01-15".to_string(), "2024-01-16".to_string()];
        let nodes = cache.get_partition_nodes_for_dates("mydb", "default", "cpu", &dates);
        assert!(nodes.is_none());
    }

    #[test]
    fn test_remove_node_from_partitions() {
        let cache = create_test_cache();

        // Record partitions from multiple nodes
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-2");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-16", "data-1");
        cache.record_partition_node("mydb", "default", "mem", "2024-01-15", "data-1");

        // Remove data-1 from all partitions
        let affected = cache.remove_node_from_partitions("data-1");
        assert_eq!(affected, 3); // 3 partition entries had data-1

        // Check that data-1 is removed
        let nodes = cache.get_partition_nodes("mydb", "default", "cpu", "2024-01-15");
        assert!(nodes.is_some());
        let nodes = nodes.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes.contains(&"data-2".to_string()));
        assert!(!nodes.contains(&"data-1".to_string()));

        // Partition with only data-1 should be removed entirely
        let nodes = cache.get_partition_nodes("mydb", "default", "cpu", "2024-01-16");
        assert!(nodes.is_none()); // Entry removed because it had no nodes left
    }

    #[test]
    fn test_clear_partition_cache_for_table() {
        let cache = create_test_cache();

        // Record partitions for multiple tables
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-16", "data-1");
        cache.record_partition_node("mydb", "default", "mem", "2024-01-15", "data-1");

        // Clear cache for cpu table
        cache.clear_partition_cache_for_table("mydb", "default", "cpu");

        // cpu partitions should be gone
        assert!(cache
            .get_partition_nodes("mydb", "default", "cpu", "2024-01-15")
            .is_none());
        assert!(cache
            .get_partition_nodes("mydb", "default", "cpu", "2024-01-16")
            .is_none());

        // mem partitions should remain
        assert!(cache
            .get_partition_nodes("mydb", "default", "mem", "2024-01-15")
            .is_some());
    }

    #[test]
    fn test_partition_cache_stats() {
        let cache = create_test_cache();

        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-2");
        cache.record_partition_node("mydb", "default", "cpu", "2024-01-16", "data-1");

        let (entries, node_refs) = cache.partition_cache_stats();
        assert_eq!(entries, 2); // 2 date partitions
        assert_eq!(node_refs, 3); // 2 + 1 node references
    }

    #[test]
    fn test_stats_includes_partition_cache() {
        let cache = create_test_cache();

        cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");

        let stats = cache.stats();
        assert_eq!(stats.partition_cache_entries, 1);
        assert_eq!(stats.partition_cache_node_refs, 1);
    }
}
