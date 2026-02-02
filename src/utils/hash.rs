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
}
