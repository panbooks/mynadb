//! Tests for cache module

use super::cardinality::{TableCardinalityTracker, MAX_TRACKED_DISTINCT_VALUES};
use super::shard::CacheShard;
use super::CacheKey;
use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_cardinality_tracker_basic() {
    let mut tracker = TableCardinalityTracker::new();

    // Add points with tags
    let mut tags1 = HashMap::new();
    tags1.insert("region".to_string(), "us-west".to_string());
    tags1.insert("host".to_string(), "server01".to_string());
    tracker.track_point(&tags1);

    let mut tags2 = HashMap::new();
    tags2.insert("region".to_string(), "us-east".to_string());
    tags2.insert("host".to_string(), "server02".to_string());
    tracker.track_point(&tags2);

    let mut tags3 = HashMap::new();
    tags3.insert("region".to_string(), "us-west".to_string()); // duplicate region
    tags3.insert("host".to_string(), "server03".to_string()); // new host
    tracker.track_point(&tags3);

    // region has 2 distinct values, host has 3
    assert_eq!(tracker.get_cardinality("region"), 2);
    assert_eq!(tracker.get_cardinality("host"), 3);

    // Verify sort order: low cardinality first
    let sorted = tracker.get_tags_by_cardinality();
    assert_eq!(sorted[0], "region"); // cardinality 2
    assert_eq!(sorted[1], "host"); // cardinality 3
}

#[test]
fn test_cardinality_tracker_high_cardinality() {
    let mut tracker = TableCardinalityTracker::new();

    // Add many distinct values for "request_id" to trigger high cardinality
    for i in 0..MAX_TRACKED_DISTINCT_VALUES + 10 {
        let mut tags = HashMap::new();
        tags.insert("request_id".to_string(), format!("req_{}", i));
        tags.insert("region".to_string(), "us-west".to_string());
        tracker.track_point(&tags);
    }

    // request_id should be marked as high cardinality
    assert_eq!(tracker.get_cardinality("request_id"), usize::MAX);
    assert_eq!(tracker.get_cardinality("region"), 1);

    // Verify sort order: low cardinality (region) before high cardinality (request_id)
    let sorted = tracker.get_tags_by_cardinality();
    assert_eq!(sorted[0], "region");
    assert_eq!(sorted[1], "request_id");
}

#[test]
fn test_cardinality_tracker_clear() {
    let mut tracker = TableCardinalityTracker::new();

    let mut tags = HashMap::new();
    tags.insert("host".to_string(), "server01".to_string());
    tracker.track_point(&tags);

    assert_eq!(tracker.get_cardinality("host"), 1);

    tracker.clear();

    assert_eq!(tracker.get_cardinality("host"), 0);
    assert!(tracker.get_tags_by_cardinality().is_empty());
}

// =========================================================================
// Flush Checkpoint Protocol Tests
// =========================================================================

fn create_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));

    let time_array = TimestampNanosecondArray::from(vec![1000000000i64; num_rows]);
    let value_array = Int64Array::from(vec![42i64; num_rows]);

    RecordBatch::try_new(schema, vec![Arc::new(time_array), Arc::new(value_array)]).unwrap()
}

#[test]
fn test_cache_shard_add_with_seq() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batch with seq=5
    let batch = create_test_batch(10);
    let size = shard.add(key.clone(), 5, batch);
    assert!(size > 0);
    assert!(!shard.is_empty());

    // Verify the batch is stored with correct sequence
    let data = shard.data.lock();
    let batches = data.get(&key).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0, 5); // seq should be 5
    assert_eq!(batches[0].1.num_rows(), 10);
}

#[test]
fn test_cache_shard_clear_up_to_seq_basic() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batches with different sequence numbers
    shard.add(key.clone(), 1, create_test_batch(10));
    shard.add(key.clone(), 2, create_test_batch(10));
    shard.add(key.clone(), 3, create_test_batch(10));
    shard.add(key.clone(), 5, create_test_batch(10));
    shard.add(key.clone(), 7, create_test_batch(10));

    // Clear all batches with seq <= 3
    let cleared = shard.clear_up_to_seq(3);
    assert!(cleared > 0);

    // Verify only seq 5 and 7 remain
    let data = shard.data.lock();
    let batches = data.get(&key).unwrap();
    assert_eq!(batches.len(), 2);

    let seqs: Vec<u64> = batches.iter().map(|(seq, _)| *seq).collect();
    assert!(seqs.contains(&5));
    assert!(seqs.contains(&7));
    assert!(!seqs.contains(&1));
    assert!(!seqs.contains(&2));
    assert!(!seqs.contains(&3));
}

#[test]
fn test_cache_shard_clear_up_to_seq_all_cleared() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batches with low sequence numbers
    shard.add(key.clone(), 1, create_test_batch(10));
    shard.add(key.clone(), 2, create_test_batch(10));
    shard.add(key.clone(), 3, create_test_batch(10));

    // Clear with high flushed_seq - should clear everything
    let cleared = shard.clear_up_to_seq(100);
    assert!(cleared > 0);

    // Shard should be empty now
    assert!(shard.is_empty());
    assert_eq!(shard.size(), 0);
}

#[test]
fn test_cache_shard_clear_up_to_seq_none_cleared() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batches with high sequence numbers
    shard.add(key.clone(), 10, create_test_batch(10));
    shard.add(key.clone(), 20, create_test_batch(10));
    shard.add(key.clone(), 30, create_test_batch(10));

    let original_size = shard.size();

    // Clear with low flushed_seq - should clear nothing
    let cleared = shard.clear_up_to_seq(5);
    assert_eq!(cleared, 0);

    // All batches should remain
    let data = shard.data.lock();
    let batches = data.get(&key).unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(shard.size(), original_size);
}

#[test]
fn test_cache_shard_clear_up_to_seq_edge_case_zero() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batch with seq=0 (Primary/local writes)
    shard.add(key.clone(), 0, create_test_batch(10));
    // Add batches with replicated sequence numbers
    shard.add(key.clone(), 1, create_test_batch(10));
    shard.add(key.clone(), 2, create_test_batch(10));

    // Clear with flushed_seq=0 - should only clear seq=0
    let cleared = shard.clear_up_to_seq(0);
    assert!(cleared > 0);

    let data = shard.data.lock();
    let batches = data.get(&key).unwrap();
    assert_eq!(batches.len(), 2);

    let seqs: Vec<u64> = batches.iter().map(|(seq, _)| *seq).collect();
    assert!(!seqs.contains(&0));
    assert!(seqs.contains(&1));
    assert!(seqs.contains(&2));
}

#[test]
fn test_cache_shard_clear_up_to_seq_multiple_keys() {
    let shard = CacheShard::new();
    let key1: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table1".to_string(),
        19000,
    );
    let key2: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table2".to_string(),
        19000,
    );

    // Add batches to both keys
    shard.add(key1.clone(), 1, create_test_batch(10));
    shard.add(key1.clone(), 5, create_test_batch(10));
    shard.add(key2.clone(), 2, create_test_batch(10));
    shard.add(key2.clone(), 6, create_test_batch(10));

    // Clear up to seq=3 - should affect both keys
    shard.clear_up_to_seq(3);

    let data = shard.data.lock();

    // key1: only seq=5 should remain
    let batches1 = data.get(&key1).unwrap();
    assert_eq!(batches1.len(), 1);
    assert_eq!(batches1[0].0, 5);

    // key2: only seq=6 should remain
    let batches2 = data.get(&key2).unwrap();
    assert_eq!(batches2.len(), 1);
    assert_eq!(batches2[0].0, 6);
}

#[test]
fn test_cache_shard_clear_up_to_seq_exact_boundary() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batches with specific sequence numbers
    shard.add(key.clone(), 5, create_test_batch(10));
    shard.add(key.clone(), 10, create_test_batch(10));
    shard.add(key.clone(), 15, create_test_batch(10));

    // Clear with flushed_seq=10 (exact match)
    shard.clear_up_to_seq(10);

    let data = shard.data.lock();
    let batches = data.get(&key).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0, 15); // Only seq=15 should remain
}

#[test]
fn test_cache_shard_clear_removes_empty_entries() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add single batch
    shard.add(key.clone(), 1, create_test_batch(10));

    // Clear it
    shard.clear_up_to_seq(1);

    // The key should be completely removed (not just empty vec)
    // Note: Check data and is_empty separately to avoid deadlock
    // (parking_lot::Mutex is not reentrant)
    {
        let data = shard.data.lock();
        assert!(!data.contains_key(&key));
    }
    assert!(shard.is_empty());
}

#[test]
fn test_cache_shard_size_tracking_after_clear() {
    let shard = CacheShard::new();
    let key: CacheKey = (
        "db".to_string(),
        "schema".to_string(),
        "table".to_string(),
        19000,
    );

    // Add batches and track size
    let size1 = shard.add(key.clone(), 1, create_test_batch(100));
    let size2 = shard.add(key.clone(), 2, create_test_batch(100));
    let total_size = shard.size();
    assert_eq!(total_size, size1 + size2);

    // Clear seq=1, check size is updated
    let cleared = shard.clear_up_to_seq(1);
    assert_eq!(cleared, size1);
    assert_eq!(shard.size(), size2);
}
