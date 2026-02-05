//! Query Execution - Fan-out and result collection
//!
//! This module handles executing distributed queries across data nodes,
//! including fan-out to multiple nodes and collecting partial results.

use super::sql_rewrite::rewrite_sql_with_node_filter;
use super::types::{DistributedQueryPlan, QueryResult};
use super::QueryCoordinator;
use arrow::record_batch::RecordBatch;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

impl QueryCoordinator {
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
    pub(super) async fn fan_out_query(
        &self,
        plan: &DistributedQueryPlan,
    ) -> (Vec<RecordBatch>, Vec<String>) {
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
