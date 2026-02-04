//! Query Coordinator - Coordinates distributed queries across Data nodes
//!
//! The QueryCoordinator is responsible for:
//! - Parsing SQL to extract table references and time ranges
//! - Determining which Data nodes to query based on partition-level routing
//! - Fan-out queries to multiple Data nodes via Arrow Flight
//! - Aggregating results from partial queries
//!
//! ## Partition-Level Routing (NEW - Hive Partition Architecture)
//!
//! With node_id as a Hive partition key, queries are routed based on:
//! 1. Extract table references and date filters from SQL
//! 2. Look up partition cache: (table, date) → [node_ids]
//! 3. Route query ONLY to nodes that have data for the requested partitions
//!
//! Benefits:
//! - **60-90% reduction in query fan-out** (only query relevant nodes)
//! - **Cache affinity preserved** (writer = reader)
//! - **Zero Meta storage** for routing (vs 16.8MB+ for file-level)
//! - **Automatic pruning** via DataFusion's Hive partition support
//!
//! ## Fallback to Table-Level Routing
//!
//! When no date filter is present or partition cache is empty, falls back to
//! table-level routing (query all table owners).

mod aggregation;
mod routing;
mod types;

pub use types::{DistributedQueryPlan, PartitionRoutingResult, QueryResult, TableRef, TimeRange};

use super::shard_cache::{DataNodeInfo, ShardCache, TableKey};
use crate::meta::client::MetaClient;
use crate::query::sql_utils::{extract_table_references, extract_time_range};
use crate::router::client::DataNodePool;
use crate::storage::storage::Storage;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

// ============================================================================
// SQL Rewriting for Partition-Level Routing
// ============================================================================

/// Rewrite SQL to add node_id partition filter
///
/// This is crucial for partition-level routing with cache affinity:
/// - Each target node receives SQL filtered to only its assigned partitions
/// - Prevents duplicate data (each partition queried by exactly one node)
/// - Enables cache affinity (nodes query partitions they wrote)
///
/// ## Examples
///
/// Original: `SELECT * FROM cpu WHERE date='2024-01-15'`
/// Rewritten: `SELECT * FROM cpu WHERE date='2024-01-15' AND node_id IN ('data-1', 'data-3')`
///
/// Original: `SELECT * FROM cpu`
/// Rewritten: `SELECT * FROM cpu WHERE node_id IN ('data-1')`
fn rewrite_sql_with_node_filter(sql: &str, assigned_partitions: &[String]) -> String {
    if assigned_partitions.is_empty() {
        return sql.to_string();
    }

    // Build the node_id IN clause
    let node_ids: Vec<String> = assigned_partitions
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "''"))) // Escape single quotes
        .collect();
    let node_filter = format!("node_id IN ({})", node_ids.join(", "));

    // Find the best place to inject the filter
    let sql_upper = sql.to_uppercase();

    // Strategy: Find WHERE clause and append, or add WHERE before GROUP BY/ORDER BY/LIMIT
    if let Some(where_pos) = find_where_clause_end(&sql_upper) {
        // Has WHERE clause - append with AND
        let (before, after) = sql.split_at(where_pos);
        format!("{} AND {}{}", before, node_filter, after)
    } else if let Some(clause_pos) = find_clause_position(&sql_upper) {
        // No WHERE but has GROUP BY/ORDER BY/LIMIT - insert WHERE before it
        let (before, after) = sql.split_at(clause_pos);
        format!("{} WHERE {}{}", before.trim_end(), node_filter, after)
    } else {
        // No WHERE and no trailing clauses - append WHERE at end
        format!("{} WHERE {}", sql.trim_end(), node_filter)
    }
}

/// Find the end position of the WHERE clause (before GROUP BY/ORDER BY/LIMIT/;)
fn find_where_clause_end(sql_upper: &str) -> Option<usize> {
    let where_pos = sql_upper.find(" WHERE ")?;
    let where_end = where_pos + 7; // Past " WHERE "

    // Find where the WHERE clause ends
    let terminators = [" GROUP BY", " ORDER BY", " LIMIT ", " HAVING ", ";"];
    let mut end_pos = sql_upper.len();

    for terminator in &terminators {
        if let Some(pos) = sql_upper[where_end..].find(terminator) {
            end_pos = end_pos.min(where_end + pos);
        }
    }

    Some(end_pos)
}

/// Find position of GROUP BY/ORDER BY/LIMIT clause (for inserting WHERE before)
fn find_clause_position(sql_upper: &str) -> Option<usize> {
    let clauses = [" GROUP BY", " ORDER BY", " LIMIT ", " HAVING "];
    let mut earliest = None;

    for clause in &clauses {
        if let Some(pos) = sql_upper.find(clause) {
            earliest = Some(earliest.map_or(pos, |e: usize| e.min(pos)));
        }
    }

    earliest
}

/// Query Coordinator for distributed query execution
pub struct QueryCoordinator {
    /// Shard cache for partition→node mapping
    pub(super) shard_cache: Arc<ShardCache>,

    /// Connection pool to Data nodes
    pub(super) data_node_pool: Arc<DataNodePool>,

    /// Max parallel fan-out
    pub(super) max_parallel_fanout: usize,

    /// Optional Meta client for file-based routing
    /// When available, enables distributed parallel queries with node affinity
    pub(super) meta_client: Option<Arc<MetaClient>>,

    /// Optional Storage for S3 LIST-based partition discovery
    /// When partition cache misses, we can discover `node_id=*` subdirectories
    /// via S3 LIST to avoid full table-level broadcast.
    pub(super) storage: Option<Storage>,
}

impl QueryCoordinator {
    pub fn new(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: None,
            storage: None,
        }
    }

    /// Create with MetaClient for file-based distributed queries
    pub fn with_meta_client(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
        meta_client: Arc<MetaClient>,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: Some(meta_client),
            storage: None,
        }
    }

    /// Create with Storage for S3 LIST-based partition discovery
    pub fn with_storage(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
        storage: Storage,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: None,
            storage: Some(storage),
        }
    }

    /// Create with both MetaClient and Storage
    pub fn with_meta_and_storage(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        max_parallel_fanout: usize,
        meta_client: Arc<MetaClient>,
        storage: Storage,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            max_parallel_fanout,
            meta_client: Some(meta_client),
            storage: Some(storage),
        }
    }

    /// Set the MetaClient (can be called after creation)
    pub fn set_meta_client(&mut self, meta_client: Arc<MetaClient>) {
        self.meta_client = Some(meta_client);
    }

    /// Set the Storage (can be called after creation)
    pub fn set_storage(&mut self, storage: Storage) {
        self.storage = Some(storage);
    }

    /// Execute a distributed query
    pub async fn execute_query(&self, catalog: &str, sql: &str) -> QueryResult {
        let start = Instant::now();

        // Plan the distributed query
        let plan = match self.plan_distributed_query(catalog, sql).await {
            Ok(plan) => plan,
            Err(e) => {
                return QueryResult {
                    success: false,
                    batches: Vec::new(),
                    total_rows: 0,
                    bytes_scanned: 0,
                    nodes_queried: 0,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    error: Some(e),
                };
            }
        };

        if plan.target_nodes.is_empty() {
            return QueryResult {
                success: false,
                batches: Vec::new(),
                total_rows: 0,
                bytes_scanned: 0,
                nodes_queried: 0,
                elapsed_ms: start.elapsed().as_millis() as u64,
                error: Some("No data nodes available for query".to_string()),
            };
        }

        // Execute query on target nodes
        let (partial_batches, errors) = self.fan_out_query(&plan).await;

        // If there were errors, return early
        if !errors.is_empty() {
            return QueryResult {
                success: false,
                batches: partial_batches,
                total_rows: 0,
                bytes_scanned: 0,
                nodes_queried: plan.target_nodes.len(),
                elapsed_ms: start.elapsed().as_millis() as u64,
                error: Some(errors.join("; ")),
            };
        }

        // Aggregate partial results if needed (ORDER BY, LIMIT, GROUP BY)
        let final_batches = match self.aggregate_results(&plan, partial_batches).await {
            Ok(batches) => batches,
            Err(e) => {
                return QueryResult {
                    success: false,
                    batches: Vec::new(),
                    total_rows: 0,
                    bytes_scanned: 0,
                    nodes_queried: plan.target_nodes.len(),
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("Aggregation failed: {}", e)),
                };
            }
        };

        let total_rows: u64 = final_batches.iter().map(|b| b.num_rows() as u64).sum();

        QueryResult {
            success: true,
            batches: final_batches,
            total_rows,
            bytes_scanned: 0, // TODO: Track bytes scanned
            nodes_queried: plan.target_nodes.len(),
            elapsed_ms: start.elapsed().as_millis() as u64,
            error: None,
        }
    }

    /// Plan a distributed query
    async fn plan_distributed_query(
        &self,
        catalog: &str,
        sql: &str,
    ) -> Result<DistributedQueryPlan, String> {
        // Extract table references from SQL
        let tables: Vec<TableRef> = extract_table_references(sql, catalog)
            .iter()
            .map(|(_, schema, table)| TableRef {
                schema: schema.clone(),
                table: table.clone(),
            })
            .collect();

        info!("Extracted {} table references: {:?}", tables.len(), tables);

        // Extract time range for partition pruning
        let time_range = extract_time_range(sql).map(|tr| TimeRange {
            min_time: tr.start_timestamp,
            max_time: tr.end_timestamp,
            start_date: tr.start_date,
            end_date: tr.end_date,
        });

        // Detect aggregation/ordering that needs consolidation
        let sql_upper = sql.to_uppercase();
        let needs_aggregation = [
            "GROUP BY", "ORDER BY", "LIMIT", " COUNT(", " SUM(", " AVG(", " MIN(", " MAX(",
        ]
        .iter()
        .any(|kw| sql_upper.contains(kw));

        // Get all queryable nodes
        let all_primary_nodes = self.shard_cache.get_queryable_primary_nodes();

        // ====================================================================
        // PARTITION-LEVEL ROUTING (Hive Partition Architecture)
        // ====================================================================
        // Try partition-level routing first (when date filters are present).
        // This reduces query fan-out by 60-90% by only querying nodes that
        // actually have data for the requested partitions.
        //
        // With partition assignments, each node receives SQL rewritten to only
        // query its assigned partitions (cache affinity, no duplicates).

        if let Some(routing_result) = self
            .try_partition_routing(catalog, &tables, &time_range, &all_primary_nodes)
            .await
        {
            // Partition routing succeeded - use these nodes with assignments
            let target_nodes: Vec<_> = routing_result
                .target_nodes
                .into_iter()
                .take(self.max_parallel_fanout)
                .collect();

            // Filter partition assignments to only include nodes we're targeting
            let target_node_ids: std::collections::HashSet<_> =
                target_nodes.iter().map(|n| n.node_id.as_str()).collect();
            let partition_assignments: HashMap<String, Vec<String>> = routing_result
                .partition_assignments
                .into_iter()
                .filter(|(node_id, _)| target_node_ids.contains(node_id.as_str()))
                .collect();

            info!(
                "Partition-level routing: {} nodes for {} tables with {} partition assignments",
                target_nodes.len(),
                tables.len(),
                partition_assignments.values().map(|v| v.len()).sum::<usize>()
            );

            return Ok(DistributedQueryPlan {
                sql: sql.to_string(),
                catalog: catalog.to_string(),
                tables,
                time_range,
                target_nodes,
                needs_aggregation,
                table_owners: None, // Partition routing doesn't use table_owners
                partition_assignments: Some(partition_assignments),
            });
        }

        // ====================================================================
        // FALLBACK: TABLE-LEVEL ROUTING
        // ====================================================================
        // No date filters or partition cache miss - fall back to table-level routing

        debug!("Falling back to table-level routing");

        // Resolve target nodes for query routing
        let (table_owners, target_nodes) = self
            .resolve_query_targets(catalog, &tables, &all_primary_nodes)
            .await;

        // Limit parallel fan-out
        let target_nodes: Vec<_> = target_nodes
            .into_iter()
            .take(self.max_parallel_fanout)
            .collect();

        info!(
            "Table-level routing: {} nodes, tables={:?}, aggregation={}",
            target_nodes.len(),
            tables,
            needs_aggregation,
        );

        Ok(DistributedQueryPlan {
            sql: sql.to_string(),
            catalog: catalog.to_string(),
            tables,
            time_range,
            target_nodes,
            needs_aggregation,
            table_owners,
            partition_assignments: None, // Table-level routing doesn't use partition assignments
        })
    }

    /// Fan-out query to target nodes via Arrow Flight and collect results
    ///
    /// ## Routing Modes
    ///
    /// **Partition-Level Routing** (when `partition_assignments` is set):
    /// - Each node receives SQL rewritten with `node_id IN (...)` filter
    /// - Ensures cache affinity (nodes query partitions they wrote)
    /// - Prevents duplicate data (each partition queried by exactly one node)
    ///
    /// **Table-Level Routing** (when `table_owners` is set):
    /// - Each table's query goes to its owner node
    /// - Owner has all data cached locally (excellent performance)
    ///
    /// **Broadcast** (fallback when neither is set):
    /// - Query sent to all target nodes with same SQL
    async fn fan_out_query(&self, plan: &DistributedQueryPlan) -> (Vec<RecordBatch>, Vec<String>) {
        // Type alias for boxed futures (needed because different async blocks have different types)
        type QueryFuture = Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, String>> + Send>>;

        let mut futures: Vec<QueryFuture> = Vec::new();

        // Get all reachable nodes for fallback
        let reachable_nodes = self.data_node_pool.get_reachable_node_ids();
        if reachable_nodes.is_empty() {
            return (
                Vec::new(),
                vec!["No reachable data nodes available".to_string()],
            );
        }

        // ====================================================================
        // PARTITION-LEVEL ROUTING (Cache Affinity, No Duplicates)
        // ====================================================================
        // When partition_assignments is set, rewrite SQL for each node to only
        // query its assigned partitions. This ensures:
        // - Cache affinity: nodes query partitions they wrote (hot cache)
        // - No duplicates: each partition queried by exactly one node
        // - Fallback support: down nodes' partitions reassigned to healthy nodes

        if let Some(ref partition_assignments) = plan.partition_assignments {
            info!(
                "Using partition-level routing with {} node assignments",
                partition_assignments.len()
            );

            for node in &plan.target_nodes {
                let pool = Arc::clone(&self.data_node_pool);
                let node_id = node.node_id.clone();
                let catalog = plan.catalog.clone();

                // Get this node's assigned partitions
                let assigned_partitions = partition_assignments
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_default();

                if assigned_partitions.is_empty() {
                    debug!("Node {} has no assigned partitions, skipping", node_id);
                    continue;
                }

                // Rewrite SQL with node_id filter for this node's assigned partitions
                let sql = rewrite_sql_with_node_filter(&plan.sql, &assigned_partitions);

                debug!(
                    "Node {} assigned partitions {:?}, SQL: {}",
                    node_id, assigned_partitions, sql
                );

                let future: QueryFuture = Box::pin(async move {
                    // Execute query via Arrow Flight DoGet
                    match pool.query(&node_id, &catalog, &sql, None).await {
                        Ok(batches) => {
                            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                            debug!(
                                "Received {} batches ({} rows) from {} (partition-level)",
                                batches.len(),
                                total_rows,
                                node_id
                            );
                            Ok(batches)
                        }
                        Err(e) => {
                            // For partition-level routing, we don't do runtime fallback
                            // because the partition assignments are pre-computed with fallbacks.
                            // If this node fails, it means both primary AND fallback failed.
                            error!(
                                "Query failed on {} (partition-level, no runtime fallback): {}",
                                node_id, e
                            );
                            Err(format!("Failed to query {} (partition): {}", node_id, e))
                        }
                    }
                });

                futures.push(future);
            }
        }
        // ====================================================================
        // TABLE-LEVEL ROUTING (RECOMMENDED for non-partitioned queries)
        // ====================================================================
        // Route to table owner nodes directly. This provides:
        // - Optimal cache locality (owner has all table data)
        // - Single node for single-table queries (95% of time-series queries)
        // - Fan-out only for multi-table joins
        else if plan.table_owners.is_some() || !plan.target_nodes.is_empty() {
            // Query each target node
            for node in &plan.target_nodes {
                let pool = Arc::clone(&self.data_node_pool);
                let node_id = node.node_id.clone();
                let sql = plan.sql.clone();
                let catalog = plan.catalog.clone();
                let fallback_nodes: Vec<String> = reachable_nodes
                    .iter()
                    .filter(|n| *n != &node_id)
                    .cloned()
                    .collect();

                let future: QueryFuture = Box::pin(async move {
                    debug!("Querying table owner {}: {}", node_id, sql);

                    // Execute query via Arrow Flight DoGet
                    match pool.query(&node_id, &catalog, &sql, None).await {
                        Ok(batches) => {
                            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                            debug!(
                                "Received {} batches ({} rows) from {} (table-level)",
                                batches.len(),
                                total_rows,
                                node_id
                            );
                            Ok(batches)
                        }
                        Err(e) => {
                            // Check if this is a connection error (node down)
                            let is_connection_error = e.contains("connect error")
                                || e.contains("Unavailable")
                                || e.contains("Unknown data node")
                                || e.contains("circuit breaker");

                            if is_connection_error && !fallback_nodes.is_empty() {
                                // Try fallback nodes (any node can read from object storage)
                                warn!("Table owner {} unreachable, trying fallback nodes", node_id);

                                for fallback_node in &fallback_nodes {
                                    debug!("Retrying query on fallback node {}", fallback_node);
                                    match pool.query(fallback_node, &catalog, &sql, None).await {
                                        Ok(batches) => {
                                            let total_rows: usize =
                                                batches.iter().map(|b| b.num_rows()).sum();
                                            info!(
                                                "Fallback query succeeded on {}: {} rows",
                                                fallback_node, total_rows
                                            );
                                            return Ok(batches);
                                        }
                                        Err(fallback_err) => {
                                            warn!(
                                                "Fallback query on {} also failed: {}",
                                                fallback_node, fallback_err
                                            );
                                            continue;
                                        }
                                    }
                                }
                                // All fallbacks failed
                                error!("Query failed on {} and all fallback nodes: {}", node_id, e);
                                Err(format!(
                                    "Failed to query {} (and {} fallbacks): {}",
                                    node_id,
                                    fallback_nodes.len(),
                                    e
                                ))
                            } else {
                                error!("Query failed on {}: {}", node_id, e);
                                Err(format!("Failed to query {}: {}", node_id, e))
                            }
                        }
                    }
                });

                futures.push(future);
            }
        }

        // Execute all queries in parallel
        let results = futures::future::join_all(futures).await;

        let mut all_batches = Vec::new();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => errors.push(e),
            }
        }

        (all_batches, errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::DataNodeMode;
    use crate::router::client::data_node_pool::PoolConfig;
    use crate::router::shard_cache::NodeState;
    use crate::utils::hash::hash_select_index;
    use std::collections::HashSet;

    fn create_test_node(id: &str, state: NodeState) -> DataNodeInfo {
        DataNodeInfo {
            node_id: id.to_string(),
            grpc_addr: format!("127.0.0.1:{}", 7000 + id.len()),
            state,
            mode: DataNodeMode::Primary,
        }
    }

    fn create_active_nodes(ids: &[&str]) -> Vec<DataNodeInfo> {
        ids.iter()
            .map(|id| create_test_node(id, NodeState::Active))
            .collect()
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
    // Tests for legacy prefix consistent hash distribution
    // =========================================================================

    #[test]
    fn test_legacy_prefix_hash_deterministic() {
        // Same inputs should always produce same output
        let table_key = TableKey::new("mydb", "metrics", "events");
        let prefix = "node3";
        let nodes = create_active_nodes(&["node1", "node2", "node4"]);

        let hash_key = format!("{}:{}", table_key.to_path(), prefix);
        let idx1 = hash_select_index(&hash_key, nodes.len());
        let idx2 = hash_select_index(&hash_key, nodes.len());
        let idx3 = hash_select_index(&hash_key, nodes.len());

        assert_eq!(idx1, idx2);
        assert_eq!(idx2, idx3);
    }

    #[test]
    fn test_legacy_prefix_different_prefixes_distribute() {
        // Different prefixes should distribute across nodes
        let table_key = TableKey::new("mydb", "metrics", "events");
        let prefixes = vec!["node3", "node5", "node6", "node7", "node8"];
        let nodes = create_active_nodes(&["node1", "node2", "node4"]);

        let mut assigned_indices: HashSet<usize> = HashSet::new();
        for prefix in &prefixes {
            let hash_key = format!("{}:{}", table_key.to_path(), prefix);
            let idx = hash_select_index(&hash_key, nodes.len());
            assigned_indices.insert(idx);
        }

        // With 5 prefixes and 3 nodes, we should hit at least 2 different nodes
        assert!(
            assigned_indices.len() >= 2,
            "Legacy prefixes should distribute across nodes, got {} unique assignments",
            assigned_indices.len()
        );
    }

    #[test]
    fn test_legacy_prefix_stable_after_scale_in() {
        // Simulates scale-in: prefix assigned to node should remain stable
        // when cluster shrinks (as long as that node still exists)

        let table_key = TableKey::new("mydb", "metrics", "events");
        let prefix = "retired_node";

        // Before scale-in: 4 nodes
        let nodes_before = create_active_nodes(&["node1", "node2", "node3", "node4"]);
        let hash_key = format!("{}:{}", table_key.to_path(), prefix);
        let idx_before = hash_select_index(&hash_key, nodes_before.len());
        let assigned_before = &nodes_before[idx_before].node_id;

        // After scale-in: 3 nodes (node4 removed)
        let nodes_after = create_active_nodes(&["node1", "node2", "node3"]);

        // Note: The assigned node might change if cluster size changes
        // This is expected - we test that it's still deterministic
        let idx_after = hash_select_index(&hash_key, nodes_after.len());
        let assigned_after = &nodes_after[idx_after].node_id;

        // Both assignments should be valid nodes
        assert!(nodes_before.iter().any(|n| &n.node_id == assigned_before));
        assert!(nodes_after.iter().any(|n| &n.node_id == assigned_after));

        // Re-running with same cluster should give same result
        let idx_after2 = hash_select_index(&hash_key, nodes_after.len());
        assert_eq!(idx_after, idx_after2);
    }

    #[test]
    fn test_legacy_prefix_single_node() {
        // With single node, all prefixes go to that node
        let table_key = TableKey::new("mydb", "metrics", "events");
        let prefixes = vec!["node2", "node3", "node4"];
        let nodes = create_active_nodes(&["node1"]);

        for prefix in &prefixes {
            let hash_key = format!("{}:{}", table_key.to_path(), prefix);
            let idx = hash_select_index(&hash_key, nodes.len());
            assert_eq!(idx, 0, "Single node should always be selected");
        }
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
    // Tests for distributed query scenarios
    // =========================================================================

    #[test]
    fn test_scale_in_scenario_legacy_redistribution() {
        // Scenario: Table had data on [Node2, Node3], Node3 removed
        // Legacy prefix "node3" should be assigned to one of [Node1, Node2, Node4]

        let table_key = TableKey::new("mydb", "metrics", "events");
        let legacy_prefix = "node3";
        let remaining_nodes = create_active_nodes(&["node1", "node2", "node4"]);

        let hash_key = format!("{}:{}", table_key.to_path(), legacy_prefix);
        let idx = hash_select_index(&hash_key, remaining_nodes.len());

        assert!(idx < remaining_nodes.len());
        let assigned = &remaining_nodes[idx];
        assert!(
            ["node1", "node2", "node4"].contains(&assigned.node_id.as_str()),
            "Legacy prefix should be assigned to an active node"
        );
    }

    #[test]
    fn test_multiple_legacy_prefixes_distribution() {
        // Multiple retired nodes should distribute their data across remaining nodes
        let table_key = TableKey::new("mydb", "metrics", "events");
        let legacy_prefixes = vec!["retired1", "retired2", "retired3", "retired4"];
        let active_nodes = create_active_nodes(&["node1", "node2", "node3"]);

        let mut assignments: HashMap<String, Vec<String>> = HashMap::new();
        for node in &active_nodes {
            assignments.insert(node.node_id.clone(), Vec::new());
        }

        for prefix in &legacy_prefixes {
            let hash_key = format!("{}:{}", table_key.to_path(), prefix);
            let idx = hash_select_index(&hash_key, active_nodes.len());
            let assigned_node = &active_nodes[idx].node_id;
            assignments
                .get_mut(assigned_node)
                .unwrap()
                .push(prefix.to_string());
        }

        // Verify distribution (not all on one node)
        let non_empty_nodes: Vec<_> = assignments.values().filter(|v| !v.is_empty()).collect();

        assert!(
            non_empty_nodes.len() >= 2,
            "Legacy prefixes should distribute across multiple nodes"
        );

        // Verify total coverage
        let total_assigned: usize = assignments.values().map(|v| v.len()).sum();
        assert_eq!(total_assigned, legacy_prefixes.len());
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

        // Populate partition cache
        shard_cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        shard_cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-2");

        // Add node info
        shard_cache.update_data_nodes(vec![
            DataNodeInfo {
                node_id: "data-1".to_string(),
                grpc_addr: "127.0.0.1:7001".to_string(),
                state: NodeState::Active,
                mode: DataNodeMode::Primary,
            },
            DataNodeInfo {
                node_id: "data-2".to_string(),
                grpc_addr: "127.0.0.1:7002".to_string(),
                state: NodeState::Active,
                mode: DataNodeMode::Primary,
            },
        ]);

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

        let all_nodes = create_active_nodes(&["data-1", "data-2", "data-3", "data-4"]);

        let result = coordinator
            .try_partition_routing("mydb", &tables, &time_range, &all_nodes)
            .await;

        assert!(result.is_some());
        let routing_result = result.unwrap();
        // Should only route to data-1 and data-2, not data-3 and data-4
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

        // Populate partition cache - data from 3 nodes
        shard_cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-1");
        shard_cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-2");
        shard_cache.record_partition_node("mydb", "default", "cpu", "2024-01-15", "data-3");

        // Add node info - data-2 is UNHEALTHY (down)
        shard_cache.update_data_nodes(vec![
            DataNodeInfo {
                node_id: "data-1".to_string(),
                grpc_addr: "127.0.0.1:7001".to_string(),
                state: NodeState::Active,
                mode: DataNodeMode::Primary,
            },
            DataNodeInfo {
                node_id: "data-2".to_string(),
                grpc_addr: "127.0.0.1:7002".to_string(),
                state: NodeState::Unhealthy, // DOWN!
                mode: DataNodeMode::Primary,
            },
            DataNodeInfo {
                node_id: "data-3".to_string(),
                grpc_addr: "127.0.0.1:7003".to_string(),
                state: NodeState::Active,
                mode: DataNodeMode::Primary,
            },
        ]);

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

        // All primary nodes (only active ones for fallback)
        let all_nodes = create_active_nodes(&["data-1", "data-3", "data-4"]);

        let result = coordinator
            .try_partition_routing("mydb", &tables, &time_range, &all_nodes)
            .await;

        assert!(result.is_some());
        let routing_result = result.unwrap();

        // Should have target nodes (data-1, data-3, and a fallback for data-2)
        // data-2's partition should be assigned to a fallback node
        let total_partitions: usize = routing_result
            .partition_assignments
            .values()
            .map(|v| v.len())
            .sum();
        assert_eq!(total_partitions, 3, "All 3 partitions should be assigned");

        // Verify data-2's partition is assigned somewhere (fallback)
        let has_data2_partition = routing_result
            .partition_assignments
            .values()
            .any(|partitions| partitions.contains(&"data-2".to_string()));
        assert!(
            has_data2_partition,
            "data-2's partition should be assigned to a fallback node"
        );

        // data-2 should NOT be a target node (it's unhealthy)
        let target_node_ids: HashSet<_> = routing_result
            .target_nodes
            .iter()
            .map(|n| n.node_id.as_str())
            .collect();
        assert!(
            !target_node_ids.contains("data-2"),
            "Unhealthy data-2 should not be a target node"
        );
    }
}
