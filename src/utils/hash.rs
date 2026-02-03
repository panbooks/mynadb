//! Common utility functions
//!
//! This module provides shared utilities used across the codebase.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute a hash value for any hashable type
///
/// Uses `DefaultHasher` which provides consistent hashing within a process.
/// Note: Hash values are NOT stable across process restarts or Rust versions.
///
/// # Example
/// ```ignore
/// let hash = compute_hash(&"my_key");
/// let idx = hash as usize % num_buckets;
/// ```
#[inline]
pub fn compute_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Select an index from a collection using consistent hashing
///
/// Given a hashable key and collection size, returns a deterministic index.
/// The same key always maps to the same index (given the same collection size).
///
/// # Example
/// ```ignore
/// let nodes = vec!["node1", "node2", "node3"];
/// let idx = hash_select_index(&table_key, nodes.len());
/// let selected = &nodes[idx];
/// ```
#[inline]
pub fn hash_select_index<T: Hash>(key: &T, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    (compute_hash(key) as usize) % len
}

/// Select an item from a slice using consistent hashing
///
/// Returns None if the slice is empty.
///
/// # Example
/// ```ignore
/// let nodes = vec![node1, node2, node3];
/// if let Some(node) = hash_select(&table_key, &nodes) {
///     // Use selected node
/// }
/// ```
#[inline]
pub fn hash_select<'a, T: Hash, V>(key: &T, items: &'a [V]) -> Option<&'a V> {
    if items.is_empty() {
        return None;
    }
    let idx = hash_select_index(key, items.len());
    Some(&items[idx])
}

/// Select and clone an item from a slice using consistent hashing
///
/// Convenience function when you need an owned value.
#[inline]
pub fn hash_select_clone<T: Hash, V: Clone>(key: &T, items: &[V]) -> Option<V> {
    hash_select(key, items).cloned()
}

/// Filter legacy prefixes to only those assigned to this node via consistent hash
///
/// When a table has legacy prefixes from retired nodes (scale-in), we use
/// consistent hashing to distribute responsibility across surviving primary nodes.
/// Each primary node only scans the legacy prefixes it's responsible for.
///
/// # Arguments
/// * `my_node_id` - This node's ID
/// * `all_primary_node_ids` - All surviving primary node IDs (MUST be sorted for consistency)
/// * `legacy_prefixes` - All legacy prefixes for the table
///
/// # Returns
/// The subset of legacy prefixes this node should scan
///
/// # Example
/// ```ignore
/// // Node data-5 out of [data-1..data-50] with 50 legacy prefixes [data-51..data-100]
/// let my_prefixes = filter_legacy_prefixes_for_node(
///     "data-5",
///     &["data-1", "data-2", ..., "data-50"],  // sorted
///     &["data-51", "data-52", ..., "data-100"],
/// );
/// // Returns ~1 prefix that hashes to data-5
/// ```
pub fn filter_legacy_prefixes_for_node(
    my_node_id: &str,
    all_primary_node_ids: &[String],
    legacy_prefixes: &[String],
) -> Vec<String> {
    if all_primary_node_ids.is_empty() || legacy_prefixes.is_empty() {
        return Vec::new();
    }

    legacy_prefixes
        .iter()
        .filter(|prefix| {
            // Use consistent hash to assign this prefix to a primary node
            let assigned_idx = hash_select_index(prefix, all_primary_node_ids.len());
            all_primary_node_ids.get(assigned_idx).map(|s| s.as_str()) == Some(my_node_id)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_hash_deterministic() {
        let key = "test_key";
        let hash1 = compute_hash(&key);
        let hash2 = compute_hash(&key);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_compute_hash_different_keys() {
        let hash1 = compute_hash(&"key1");
        let hash2 = compute_hash(&"key2");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_select_index_deterministic() {
        let key = "table_key";
        let len = 10;
        let idx1 = hash_select_index(&key, len);
        let idx2 = hash_select_index(&key, len);
        assert_eq!(idx1, idx2);
        assert!(idx1 < len);
    }

    #[test]
    fn test_hash_select_index_empty() {
        let idx = hash_select_index(&"key", 0);
        assert_eq!(idx, 0);
    }

    #[test]
    fn test_hash_select_returns_item() {
        let items = vec!["a", "b", "c", "d", "e"];
        let selected = hash_select(&"my_key", &items);
        assert!(selected.is_some());
        assert!(items.contains(selected.unwrap()));
    }

    #[test]
    fn test_hash_select_empty_slice() {
        let items: Vec<&str> = vec![];
        let selected = hash_select(&"key", &items);
        assert!(selected.is_none());
    }

    #[test]
    fn test_hash_select_clone() {
        let items = vec!["node1".to_string(), "node2".to_string()];
        let selected = hash_select_clone(&"key", &items);
        assert!(selected.is_some());
    }

    #[test]
    fn test_hash_distribution() {
        // Test that hashing distributes keys reasonably
        let num_buckets = 10;
        let mut counts = vec![0; num_buckets];

        for i in 0..1000 {
            let key = format!("key_{}", i);
            let idx = hash_select_index(&key, num_buckets);
            counts[idx] += 1;
        }

        // Each bucket should have roughly 100 items (1000/10)
        // Allow 50% deviation
        for count in counts {
            assert!(count > 50, "Bucket has too few items: {}", count);
            assert!(count < 150, "Bucket has too many items: {}", count);
        }
    }

    #[test]
    fn test_filter_legacy_prefixes_for_node_empty() {
        // Empty inputs
        let result = filter_legacy_prefixes_for_node("node-1", &[], &["prefix-1".to_string()]);
        assert!(result.is_empty());

        let result = filter_legacy_prefixes_for_node("node-1", &["node-1".to_string()], &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_filter_legacy_prefixes_for_node_single_node() {
        // Single surviving node gets all prefixes
        let primaries = vec!["node-1".to_string()];
        let legacy = vec![
            "retired-1".to_string(),
            "retired-2".to_string(),
            "retired-3".to_string(),
        ];

        let result = filter_legacy_prefixes_for_node("node-1", &primaries, &legacy);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_filter_legacy_prefixes_for_node_distribution() {
        // Multiple nodes should distribute prefixes
        let primaries: Vec<String> = (1..=5).map(|i| format!("node-{}", i)).collect();
        let legacy: Vec<String> = (1..=50).map(|i| format!("retired-{}", i)).collect();

        let mut total_assigned = 0;
        for node_id in &primaries {
            let result = filter_legacy_prefixes_for_node(node_id, &primaries, &legacy);
            total_assigned += result.len();
            // Each node should get roughly 10 prefixes (50/5)
            assert!(
                result.len() >= 5,
                "Node {} got too few: {}",
                node_id,
                result.len()
            );
            assert!(
                result.len() <= 15,
                "Node {} got too many: {}",
                node_id,
                result.len()
            );
        }

        // All prefixes should be assigned exactly once
        assert_eq!(total_assigned, 50);
    }

    #[test]
    fn test_filter_legacy_prefixes_for_node_deterministic() {
        let primaries: Vec<String> = (1..=3).map(|i| format!("node-{}", i)).collect();
        let legacy: Vec<String> = (1..=10).map(|i| format!("retired-{}", i)).collect();

        let result1 = filter_legacy_prefixes_for_node("node-2", &primaries, &legacy);
        let result2 = filter_legacy_prefixes_for_node("node-2", &primaries, &legacy);

        // Same input should give same output
        assert_eq!(result1, result2);
    }
}
