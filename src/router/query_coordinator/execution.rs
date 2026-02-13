//! Query Execution - Fan-out and result collection
//!
//! This module handles executing distributed queries across data nodes,
//! including fan-out to multiple nodes and collecting partial results.
//!
//! ## Standby Failover
//!
//! When a Primary node fails, the query is split into two parts:
//! - Memtable data: Query the backing Standby (has replicated memtable)
//! - Parquet data: Query any fallback Primary (S3 is shared storage)
//!
//! This ensures no data is lost during failover, as long as the Standby
//! has been receiving memtable replications from the Primary.

use super::sql_rewrite::rewrite_sql_with_node_filter;
use super::types::{DistributedQueryPlan, QueryResult};
use super::QueryCoordinator;
use crate::router::client::QueryScope;
use crate::router::client::DataNodePool;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Type alias for boxed query futures
type QueryFuture = Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, String>> + Send>>;

/// Context for executing a query with failover support
struct QueryContext {
    pool: Arc<DataNodePool>,
    node_id: String,
    catalog: String,
    sql: String,
    backing_standby: Option<String>,
    fallback_nodes: Vec<String>,
}

impl QueryCoordinator {
    /// Execute a distributed query
    pub async fn execute_query(&self, catalog: &str, sql: &str) -> QueryResult {
        info!("Query: db={} sql={}", catalog, sql);
        let start = Instant::now();

        let plan = match self.plan_distributed_query(catalog, sql).await {
            Ok(plan) => plan,
            Err(e) => return QueryResult::error(e, start),
        };

        if plan.target_nodes.is_empty() {
            return QueryResult::error("No data nodes available for query".to_string(), start);
        }

        let (partial_batches, errors) = self.fan_out_query(&plan).await;

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

        let final_batches = match self.aggregate_results(&plan, partial_batches).await {
            Ok(batches) => batches,
            Err(e) => {
                return QueryResult::error_with_nodes(
                    format!("Aggregation failed: {}", e),
                    plan.target_nodes.len(),
                    start,
                )
            }
        };

        let total_rows: u64 = final_batches.iter().map(|b| b.num_rows() as u64).sum();
        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(
            "Query completed: db={} rows={} nodes={} cost={}ms",
            catalog,
            total_rows,
            plan.target_nodes.len(),
            elapsed_ms
        );

        QueryResult {
            success: true,
            batches: final_batches,
            total_rows,
            bytes_scanned: 0,
            nodes_queried: plan.target_nodes.len(),
            elapsed_ms,
            error: None,
        }
    }

    /// Fan-out query to target nodes via Arrow Flight and collect results
    pub(super) async fn fan_out_query(
        &self,
        plan: &DistributedQueryPlan,
    ) -> (Vec<RecordBatch>, Vec<String>) {
        let reachable_nodes = self.data_node_pool.get_reachable_node_ids();
        if reachable_nodes.is_empty() {
            return (
                Vec::new(),
                vec!["No reachable data nodes available".to_string()],
            );
        }

        let futures = if plan.partition_assignments.is_some() {
            self.build_partition_level_queries(plan)
        } else {
            self.build_table_level_queries(plan, &reachable_nodes)
        };

        Self::collect_query_results(futures).await
    }

    /// Build queries for partition-level routing (cache affinity, no duplicates)
    fn build_partition_level_queries(&self, plan: &DistributedQueryPlan) -> Vec<QueryFuture> {
        let partition_assignments = match &plan.partition_assignments {
            Some(assignments) => assignments,
            None => return Vec::new(),
        };

        info!(
            "Using partition-level routing with {} node assignments",
            partition_assignments.len()
        );

        let mut futures = Vec::new();

        for node in &plan.target_nodes {
            let assigned_partitions = partition_assignments
                .get(&node.node_id)
                .cloned()
                .unwrap_or_default();

            if assigned_partitions.is_empty() {
                debug!("Node {} has no assigned partitions, skipping", node.node_id);
                continue;
            }

            let pool = Arc::clone(&self.data_node_pool);
            let node_id = node.node_id.clone();
            let catalog = plan.catalog.clone();
            let sql = rewrite_sql_with_node_filter(&plan.sql, &assigned_partitions);

            debug!(
                "Node {} assigned partitions {:?}, SQL: {}",
                node_id, assigned_partitions, sql
            );

            futures.push(Self::create_partition_query(pool, node_id, catalog, sql));
        }

        futures
    }

    /// Create a single partition-level query future
    fn create_partition_query(
        pool: Arc<DataNodePool>,
        node_id: String,
        catalog: String,
        sql: String,
    ) -> QueryFuture {
        Box::pin(async move {
            match pool.query(&node_id, &catalog, &sql, None).await {
                Ok(batches) => {
                    log_query_success(&batches, &node_id, "partition-level");
                    Ok(batches)
                }
                Err(e) => {
                    error!(
                        "Query failed on {} (partition-level, no runtime fallback): {}",
                        node_id, e
                    );
                    Err(format!("Failed to query {} (partition): {}", node_id, e))
                }
            }
        })
    }

    /// Build queries for table-level routing with failover support
    fn build_table_level_queries(
        &self,
        plan: &DistributedQueryPlan,
        reachable_nodes: &[String],
    ) -> Vec<QueryFuture> {
        let reachable_set: HashSet<_> = reachable_nodes.iter().cloned().collect();

        plan.target_nodes
            .iter()
            .map(|node| {
                let fallback_nodes: Vec<String> = reachable_set
                    .iter()
                    .filter(|n| *n != &node.node_id)
                    .cloned()
                    .collect();

                let ctx = QueryContext {
                    pool: Arc::clone(&self.data_node_pool),
                    node_id: node.node_id.clone(),
                    catalog: plan.catalog.clone(),
                    sql: plan.sql.clone(),
                    backing_standby: node.backing_standby.clone(),
                    fallback_nodes,
                };

                Self::create_table_query_with_failover(ctx)
            })
            .collect()
    }

    /// Create a table-level query future with failover support
    fn create_table_query_with_failover(ctx: QueryContext) -> QueryFuture {
        Box::pin(async move { execute_with_failover(ctx).await })
    }

    /// Collect results from all query futures
    async fn collect_query_results(futures: Vec<QueryFuture>) -> (Vec<RecordBatch>, Vec<String>) {
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

// ============================================================================
// Query Execution with Failover (standalone async functions to reduce nesting)
// ============================================================================

/// Execute query with automatic failover on connection errors
async fn execute_with_failover(ctx: QueryContext) -> Result<Vec<RecordBatch>, String> {
    debug!("Querying table owner {}: {}", ctx.node_id, ctx.sql);

    // Try primary node first
    match ctx.pool.query(&ctx.node_id, &ctx.catalog, &ctx.sql, None).await {
        Ok(batches) => {
            log_query_success(&batches, &ctx.node_id, "table-level");
            return Ok(batches);
        }
        Err(e) => {
            if !is_connection_error(&e) {
                error!("Query failed on {}: {}", ctx.node_id, e);
                return Err(format!("Failed to query {}: {}", ctx.node_id, e));
            }
            // Connection error - proceed with failover
            debug!("Connection error on {}, attempting failover: {}", ctx.node_id, e);
        }
    }

    // Primary failed with connection error - try failover
    handle_failover(&ctx).await
}

/// Handle failover when primary node is unreachable
async fn handle_failover(ctx: &QueryContext) -> Result<Vec<RecordBatch>, String> {
    // Try standby failover if available
    if let Some(standby_id) = &ctx.backing_standby {
        info!(
            "Primary {} down, using standby failover (standby: {})",
            ctx.node_id, standby_id
        );
        if let Ok(batches) = try_standby_failover(ctx, standby_id).await {
            return Ok(batches);
        }
    }

    // Fall back to simple failover (parquet only)
    if ctx.fallback_nodes.is_empty() {
        return Err(format!(
            "Primary {} down with no fallback nodes available",
            ctx.node_id
        ));
    }

    warn!(
        "Primary {} down, trying simple fallback (parquet only)",
        ctx.node_id
    );
    try_simple_fallback(ctx).await
}

/// Try standby failover: query standby for memtable + fallback for parquet
async fn try_standby_failover(
    ctx: &QueryContext,
    standby_id: &str,
) -> Result<Vec<RecordBatch>, String> {
    let mut all_batches = Vec::new();
    let mut had_success = false;

    // Step 1: Query Standby for memtable data
    if let Some(batches) = query_standby_memtable(ctx, standby_id).await {
        all_batches.extend(batches);
        had_success = true;
    }

    // Step 2: Query fallback Primary for parquet data
    if let Some(batches) = query_fallback_parquet(ctx).await {
        all_batches.extend(batches);
        had_success = true;
    }

    if !had_success {
        error!("Standby failover failed: both standby and all fallbacks failed");
        return Err(format!(
            "Standby failover failed for {}: standby {} and {} fallbacks all failed",
            ctx.node_id,
            standby_id,
            ctx.fallback_nodes.len()
        ));
    }

    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    info!(
        "Standby failover succeeded: {} total rows (memtable + parquet)",
        total_rows
    );
    Ok(all_batches)
}

/// Query standby node for memtable data
async fn query_standby_memtable(ctx: &QueryContext, standby_id: &str) -> Option<Vec<RecordBatch>> {
    match ctx
        .pool
        .query_with_scope(
            standby_id,
            &ctx.catalog,
            &ctx.sql,
            None,
            QueryScope::MemtableOnly,
        )
        .await
    {
        Ok(batches) => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            info!("Standby {} returned {} memtable rows", standby_id, total_rows);
            Some(batches)
        }
        Err(e) => {
            warn!(
                "Standby {} memtable query failed: {} (memtable data may be lost)",
                standby_id, e
            );
            None
        }
    }
}

/// Query fallback nodes for parquet data (try each until one succeeds)
async fn query_fallback_parquet(ctx: &QueryContext) -> Option<Vec<RecordBatch>> {
    for fallback_node in &ctx.fallback_nodes {
        match ctx
            .pool
            .query_with_scope(
                fallback_node,
                &ctx.catalog,
                &ctx.sql,
                None,
                QueryScope::ParquetOnly,
            )
            .await
        {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                info!(
                    "Fallback {} returned {} parquet rows",
                    fallback_node, total_rows
                );
                return Some(batches);
            }
            Err(e) => {
                warn!("Fallback {} parquet query failed: {}", fallback_node, e);
            }
        }
    }
    None
}

/// Try simple fallback (no standby, parquet only)
async fn try_simple_fallback(ctx: &QueryContext) -> Result<Vec<RecordBatch>, String> {
    for fallback_node in &ctx.fallback_nodes {
        debug!("Retrying query on fallback node {}", fallback_node);

        match ctx
            .pool
            .query(fallback_node, &ctx.catalog, &ctx.sql, None)
            .await
        {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                info!(
                    "Fallback query succeeded on {}: {} rows",
                    fallback_node, total_rows
                );
                return Ok(batches);
            }
            Err(e) => {
                warn!("Fallback query on {} also failed: {}", fallback_node, e);
            }
        }
    }

    Err(format!(
        "Failed to query {} and all {} fallback nodes",
        ctx.node_id,
        ctx.fallback_nodes.len()
    ))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Check if error indicates a connection failure (node down)
fn is_connection_error(e: &str) -> bool {
    e.contains("connect error")
        || e.contains("Unavailable")
        || e.contains("Unknown data node")
        || e.contains("circuit breaker")
}

/// Log successful query result
fn log_query_success(batches: &[RecordBatch], node_id: &str, routing_type: &str) {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    debug!(
        "Received {} batches ({} rows) from {} ({})",
        batches.len(),
        total_rows,
        node_id,
        routing_type
    );
}

// ============================================================================
// QueryResult Helper Methods
// ============================================================================

impl QueryResult {
    fn error(msg: String, start: Instant) -> Self {
        Self {
            success: false,
            batches: Vec::new(),
            total_rows: 0,
            bytes_scanned: 0,
            nodes_queried: 0,
            elapsed_ms: start.elapsed().as_millis() as u64,
            error: Some(msg),
        }
    }

    fn error_with_nodes(msg: String, nodes: usize, start: Instant) -> Self {
        Self {
            success: false,
            batches: Vec::new(),
            total_rows: 0,
            bytes_scanned: 0,
            nodes_queried: nodes,
            elapsed_ms: start.elapsed().as_millis() as u64,
            error: Some(msg),
        }
    }
}
