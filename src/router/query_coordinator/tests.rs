//! Unit tests for QueryCoordinator

use super::sql_rewrite::rewrite_sql_with_node_filter;
use super::types::{TableRef, TimeRange};
use super::QueryCoordinator;
use crate::meta::types::DataNodeMode;
use crate::router::client::data_node_pool::PoolConfig;
use crate::router::client::DataNodePool;
use crate::router::shard_cache::{DataNodeInfo, NodeState, ShardCache, TableKey};
use crate::utils::hash::hash_select_index;
use std::collections::HashSet;
use std::sync::Arc;

fn create_test_node(id: &str, state: NodeState) -> DataNodeInfo {
    DataNodeInfo {
        node_id: id.to_string(),
        grpc_addr: format!("127.0.0.1:{}", 7000 + id.len()),
        state,
        mode: DataNodeMode::Primary,
        backing_standby: None,
    }
}

fn create_active_nodes(ids: &[&str]) -> Vec<DataNodeInfo> {
    ids.iter()
        .map(|id| create_test_node(id, NodeState::Active))
        .collect()
}

/// Get today's date as YYYY-MM-DD string
fn today_date() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

// =========================================================================
// Tests for is_queryable()
// =========================================================================

#[test]
fn test_is_queryable_active() {
    let node = create_test_node("node1", NodeState::Active);
    assert!(QueryCoordinator::is_queryable(&node));
}

#[test]
fn test_is_queryable_retiring() {
    let node = create_test_node("node1", NodeState::Retiring);
    assert!(QueryCoordinator::is_queryable(&node));
}

#[test]
fn test_is_queryable_unhealthy() {
    let node = create_test_node("node1", NodeState::Unhealthy);
    assert!(!QueryCoordinator::is_queryable(&node));
}

#[test]
fn test_is_queryable_draining() {
    let node = create_test_node("node1", NodeState::Draining);
    assert!(!QueryCoordinator::is_queryable(&node));
}

#[test]
fn test_is_queryable_drained() {
    let node = create_test_node("node1", NodeState::Drained);
    assert!(!QueryCoordinator::is_queryable(&node));
}

// =========================================================================
// Tests for table key utilities
// =========================================================================

#[test]
fn test_table_key_to_path() {
    let key = TableKey::new("catalog", "schema", "table");
    assert_eq!(key.to_path(), "catalog/schema/table");
}

#[test]
fn test_table_key_components() {
    let key = TableKey::new("mydb", "metrics", "cpu");
    assert_eq!(key.catalog, "mydb");
    assert_eq!(key.schema, "metrics");
    assert_eq!(key.table, "cpu");
}

// =========================================================================
// Tests for node state transitions
// =========================================================================

#[test]
fn test_node_states_for_query() {
    let states_queryable = vec![NodeState::Active, NodeState::Retiring];
    let states_not_queryable = vec![
        NodeState::Unhealthy,
        NodeState::Draining,
        NodeState::Drained,
        NodeState::Suspect,
    ];

    for state in states_queryable {
        let node = create_test_node("test", state);
        assert!(
            QueryCoordinator::is_queryable(&node),
            "{:?} should be queryable",
            state
        );
    }

    for state in states_not_queryable {
        let node = create_test_node("test", state);
        assert!(
            !QueryCoordinator::is_queryable(&node),
            "{:?} should NOT be queryable",
            state
        );
    }
}

// =========================================================================
// Tests for aggregation detection
// =========================================================================

#[test]
fn test_needs_aggregation_detection() {
    let aggregation_keywords = [
        "GROUP BY", "ORDER BY", "LIMIT", " COUNT(", " SUM(", " AVG(", " MIN(", " MAX(",
    ];

    for keyword in aggregation_keywords {
        let sql = format!("SELECT * FROM test {}", keyword);
        let sql_upper = sql.to_uppercase();
        let needs_agg = aggregation_keywords.iter().any(|kw| sql_upper.contains(kw));
        assert!(needs_agg, "Should detect aggregation for: {}", sql);
    }

    // Simple query without aggregation
    let simple_sql = "SELECT * FROM test WHERE time > 0";
    let sql_upper = simple_sql.to_uppercase();
    let needs_agg = aggregation_keywords.iter().any(|kw| sql_upper.contains(kw));
    assert!(!needs_agg, "Simple query should not need aggregation");
}

// =========================================================================
// Tests for hash stability across table keys
// =========================================================================

#[test]
fn test_hash_stability_same_table() {
    let key1 = TableKey::new("mydb", "metrics", "cpu");
    let key2 = TableKey::new("mydb", "metrics", "cpu");

    let nodes = create_active_nodes(&["n1", "n2", "n3", "n4"]);

    let idx1 = hash_select_index(&key1, nodes.len());
    let idx2 = hash_select_index(&key2, nodes.len());

    assert_eq!(idx1, idx2, "Same table should hash to same index");
}

#[test]
fn test_hash_different_tables() {
    let tables = vec![
        TableKey::new("mydb", "metrics", "cpu"),
        TableKey::new("mydb", "metrics", "memory"),
        TableKey::new("mydb", "logs", "app"),
        TableKey::new("other", "metrics", "cpu"),
    ];

    let nodes = create_active_nodes(&["n1", "n2", "n3", "n4"]);
    let mut indices: HashSet<usize> = HashSet::new();

    for table in &tables {
        let idx = hash_select_index(table, nodes.len());
        indices.insert(idx);
    }

    // Different tables should distribute (not all same index)
    assert!(
        indices.len() >= 2,
        "Different tables should hash to different nodes"
    );
}

// =========================================================================
// Tests for partition-level routing
// =========================================================================

#[test]
fn test_generate_date_range_single_date() {
    let shard_cache = Arc::new(ShardCache::new());
    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let dates = coordinator.generate_date_range("2024-01-15", "2024-01-15");
    assert_eq!(dates.len(), 1);
    assert_eq!(dates[0], "2024-01-15");
}

#[test]
fn test_generate_date_range_multi_day() {
    let shard_cache = Arc::new(ShardCache::new());
    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let dates = coordinator.generate_date_range("2024-01-10", "2024-01-15");
    assert_eq!(dates.len(), 6);
    assert_eq!(dates[0], "2024-01-10");
    assert_eq!(dates[5], "2024-01-15");
}

#[test]
fn test_timestamp_to_date() {
    let shard_cache = Arc::new(ShardCache::new());
    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    // 2024-01-01 00:00:00 UTC in nanoseconds
    let ts = 1704067200_000_000_000i64;
    let date = coordinator.timestamp_to_date(ts);
    assert_eq!(date, "2024-01-01");
}

#[test]
fn test_extract_date_filters_with_date_range() {
    let shard_cache = Arc::new(ShardCache::new());
    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let time_range = Some(TimeRange {
        min_time: None,
        max_time: None,
        start_date: Some("2024-01-10".to_string()),
        end_date: Some("2024-01-12".to_string()),
    });

    let dates = coordinator.extract_date_filters(&time_range);
    assert!(dates.is_some());
    let dates = dates.unwrap();
    assert_eq!(dates.len(), 3);
    assert!(dates.contains(&"2024-01-10".to_string()));
    assert!(dates.contains(&"2024-01-11".to_string()));
    assert!(dates.contains(&"2024-01-12".to_string()));
}

#[test]
fn test_extract_date_filters_no_range() {
    let shard_cache = Arc::new(ShardCache::new());
    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let dates = coordinator.extract_date_filters(&None);
    assert!(dates.is_none());
}

#[tokio::test]
async fn test_partition_routing_with_cache_hit() {
    let shard_cache = Arc::new(ShardCache::new());

    // Set table owners (hot data routing is now derived from table owners)
    // The table owner nodes have memtable data for recent partitions
    let table_key = TableKey::new("mydb", "default", "cpu");
    shard_cache.set_table_owners(&table_key, vec!["data-1".to_string(), "data-2".to_string()], 1);

    // Add node info
    shard_cache.update_data_nodes(vec![
        DataNodeInfo {
            node_id: "data-1".to_string(),
            grpc_addr: "127.0.0.1:7001".to_string(),
            state: NodeState::Active,
            mode: DataNodeMode::Primary,
            backing_standby: None,
        },
        DataNodeInfo {
            node_id: "data-2".to_string(),
            grpc_addr: "127.0.0.1:7002".to_string(),
            state: NodeState::Active,
            mode: DataNodeMode::Primary,
            backing_standby: None,
        },
    ]);

    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let tables = vec![TableRef {
        schema: "default".to_string(),
        table: "cpu".to_string(),
    }];

    // Use today's date so it's considered "recent" (table owners have memtable)
    let today = today_date();
    let time_range = Some(TimeRange {
        min_time: None,
        max_time: None,
        start_date: Some(today.clone()),
        end_date: Some(today),
    });

    let all_nodes = create_active_nodes(&["data-1", "data-2", "data-3", "data-4"]);

    let result = coordinator
        .try_partition_routing("mydb", &tables, &time_range, &all_nodes)
        .await;

    assert!(result.is_some());
    let routing_result = result.unwrap();
    // Should only route to table owners (data-1 and data-2), not data-3 and data-4
    assert_eq!(routing_result.target_nodes.len(), 2);
    let node_ids: HashSet<_> = routing_result
        .target_nodes
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    assert!(node_ids.contains("data-1"));
    assert!(node_ids.contains("data-2"));

    // Verify partition assignments
    assert_eq!(routing_result.partition_assignments.len(), 2);
    // Each node should be assigned its own partition (cache affinity)
    assert!(routing_result
        .partition_assignments
        .get("data-1")
        .map(|v| v.contains(&"data-1".to_string()))
        .unwrap_or(false));
    assert!(routing_result
        .partition_assignments
        .get("data-2")
        .map(|v| v.contains(&"data-2".to_string()))
        .unwrap_or(false));
}

#[tokio::test]
async fn test_partition_routing_cache_miss_fallback() {
    let shard_cache = Arc::new(ShardCache::new());
    // Empty partition cache - should fall back to table-level routing
    // (No storage configured, so S3 LIST discovery will be skipped)

    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let tables = vec![TableRef {
        schema: "default".to_string(),
        table: "cpu".to_string(),
    }];

    let time_range = Some(TimeRange {
        min_time: None,
        max_time: None,
        start_date: Some("2024-01-15".to_string()),
        end_date: Some("2024-01-15".to_string()),
    });

    let all_nodes = create_active_nodes(&["data-1", "data-2"]);

    let result = coordinator
        .try_partition_routing("mydb", &tables, &time_range, &all_nodes)
        .await;

    // Should return None (fallback to table-level routing)
    assert!(result.is_none());
}

// =========================================================================
// Tests for SQL rewriting with node_id filter
// =========================================================================

#[test]
fn test_sql_rewrite_simple_select() {
    // Simple SELECT without WHERE
    let sql = "SELECT * FROM cpu";
    let partitions = vec!["data-1".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(rewritten, "SELECT * FROM cpu WHERE node_id IN ('data-1')");
}

#[test]
fn test_sql_rewrite_with_existing_where() {
    // SELECT with existing WHERE clause
    let sql = "SELECT * FROM cpu WHERE date='2024-01-15'";
    let partitions = vec!["data-1".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT * FROM cpu WHERE date='2024-01-15' AND node_id IN ('data-1')"
    );
}

#[test]
fn test_sql_rewrite_with_multiple_partitions() {
    // Multiple partition assignments
    let sql = "SELECT * FROM cpu WHERE time > 0";
    let partitions = vec!["data-1".to_string(), "data-3".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT * FROM cpu WHERE time > 0 AND node_id IN ('data-1', 'data-3')"
    );
}

#[test]
fn test_sql_rewrite_with_order_by() {
    // SELECT with ORDER BY (no WHERE)
    let sql = "SELECT * FROM cpu ORDER BY time DESC";
    let partitions = vec!["data-2".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT * FROM cpu WHERE node_id IN ('data-2') ORDER BY time DESC"
    );
}

#[test]
fn test_sql_rewrite_with_where_and_order_by() {
    // SELECT with WHERE and ORDER BY
    let sql = "SELECT * FROM cpu WHERE value > 50 ORDER BY time DESC";
    let partitions = vec!["data-1".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT * FROM cpu WHERE value > 50 AND node_id IN ('data-1') ORDER BY time DESC"
    );
}

#[test]
fn test_sql_rewrite_with_group_by() {
    // SELECT with GROUP BY
    let sql = "SELECT host, COUNT(*) FROM cpu GROUP BY host";
    let partitions = vec!["data-1".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT host, COUNT(*) FROM cpu WHERE node_id IN ('data-1') GROUP BY host"
    );
}

#[test]
fn test_sql_rewrite_with_limit() {
    // SELECT with LIMIT
    let sql = "SELECT * FROM cpu LIMIT 100";
    let partitions = vec!["data-1".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT * FROM cpu WHERE node_id IN ('data-1') LIMIT 100"
    );
}

#[test]
fn test_sql_rewrite_complex_query() {
    // Complex query with WHERE, GROUP BY, ORDER BY, LIMIT
    let sql = "SELECT host, AVG(value) FROM cpu WHERE date='2024-01-15' GROUP BY host ORDER BY host LIMIT 10";
    let partitions = vec!["data-1".to_string(), "data-2".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT host, AVG(value) FROM cpu WHERE date='2024-01-15' AND node_id IN ('data-1', 'data-2') GROUP BY host ORDER BY host LIMIT 10"
    );
}

#[test]
fn test_sql_rewrite_empty_partitions() {
    // Empty partitions list should return original SQL
    let sql = "SELECT * FROM cpu";
    let partitions: Vec<String> = vec![];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_sql_rewrite_escapes_quotes() {
    // Node ID with single quote should be escaped
    let sql = "SELECT * FROM cpu";
    let partitions = vec!["data's-node".to_string()];
    let rewritten = rewrite_sql_with_node_filter(sql, &partitions);
    assert_eq!(
        rewritten,
        "SELECT * FROM cpu WHERE node_id IN ('data''s-node')"
    );
}

// =========================================================================
// Tests for partition routing with fallback (node down)
// =========================================================================

#[tokio::test]
async fn test_partition_routing_with_fallback() {
    let shard_cache = Arc::new(ShardCache::new());

    // Set table owners (hot data routing is now derived from table owners)
    // All 3 nodes are owners (distributed table)
    let table_key = TableKey::new("mydb", "default", "cpu");
    shard_cache.set_table_owners(
        &table_key,
        vec!["data-1".to_string(), "data-2".to_string(), "data-3".to_string()],
        1,
    );

    // Add node info - data-2 is UNHEALTHY (down)
    shard_cache.update_data_nodes(vec![
        DataNodeInfo {
            node_id: "data-1".to_string(),
            grpc_addr: "127.0.0.1:7001".to_string(),
            state: NodeState::Active,
            mode: DataNodeMode::Primary,
            backing_standby: None,
        },
        DataNodeInfo {
            node_id: "data-2".to_string(),
            grpc_addr: "127.0.0.1:7002".to_string(),
            state: NodeState::Unhealthy, // DOWN!
            mode: DataNodeMode::Primary,
            backing_standby: None,
        },
        DataNodeInfo {
            node_id: "data-3".to_string(),
            grpc_addr: "127.0.0.1:7003".to_string(),
            state: NodeState::Active,
            mode: DataNodeMode::Primary,
            backing_standby: None,
        },
    ]);

    let data_node_pool = Arc::new(DataNodePool::new(PoolConfig::default()));
    let coordinator = QueryCoordinator::new(shard_cache, data_node_pool, 10);

    let tables = vec![TableRef {
        schema: "default".to_string(),
        table: "cpu".to_string(),
    }];

    // Use today's date so it's considered "recent" (table owners have memtable)
    let today = today_date();
    let time_range = Some(TimeRange {
        min_time: None,
        max_time: None,
        start_date: Some(today.clone()),
        end_date: Some(today),
    });

    // All primary nodes (only active ones for fallback)
    let all_nodes = create_active_nodes(&["data-1", "data-3", "data-4"]);

    let result = coordinator
        .try_partition_routing("mydb", &tables, &time_range, &all_nodes)
        .await;

    assert!(result.is_some());
    let routing_result = result.unwrap();

    // With table owners, all 3 owners are returned (including down node data-2)
    // The routing filters out unhealthy nodes, keeping only active ones
    // data-1 and data-3 are active owners
    let active_owner_count = routing_result.target_nodes.len();
    assert!(active_owner_count >= 2, "Should have at least 2 active owners");

    // Verify data-1 and data-3 are in target nodes (active owners)
    let node_ids: HashSet<_> = routing_result
        .target_nodes
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    assert!(node_ids.contains("data-1"), "data-1 should be a target (active owner)");
    assert!(node_ids.contains("data-3"), "data-3 should be a target (active owner)");

    // Note: This test was originally checking partition-level fallback,
    // but with the new design, we route by table owners (not partitions).
    // The fallback logic happens at query execution time, not routing time.

    // data-2 should NOT be a target node (it's unhealthy, filtered by routing)
    assert!(
        !node_ids.contains("data-2"),
        "Unhealthy data-2 should not be a target node"
    );
}
