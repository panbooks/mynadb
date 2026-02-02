//! Query Coordinator - Coordinates distributed queries across Data nodes
//!
//! The QueryCoordinator is responsible for:
//! - Parsing SQL to extract table references and time ranges
//! - Determining which Data nodes to query based on table ownership
//! - Fan-out queries to multiple Data nodes via Arrow Flight
//! - Aggregating results from partial queries
//!
//! ## Table-Level Routing (RECOMMENDED)
//!
//! When a MetaClient is available, the QueryCoordinator uses table-level routing:
//! 1. Extract table references from SQL
//! 2. Look up table owner from Meta (get_table_owner)
//! 3. Route query to the owner node
//!
//! Benefits over file-based routing:
//! - **Single lookup**: O(1) per table vs O(N) for files
//! - **Excellent cache locality**: All table data on one node
//! - **600x fewer Meta entries**: 100K tables vs 60M files
//!
//! For multi-table queries, fan-out to each table's owner node.
//! For single-table queries (95%+ of time-series queries), single node query.

use super::shard_cache::{DataNodeInfo, ShardCache, TableKey};
use crate::meta::client::MetaClient;
use crate::query::sql_utils::{extract_table_references, extract_time_range};
use crate::router::client::DataNodePool;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Query result from distributed execution
#[derive(Debug)]
pub struct QueryResult {
    pub success: bool,
    pub batches: Vec<RecordBatch>,
    pub total_rows: u64,
    pub bytes_scanned: u64,
    pub nodes_queried: usize,
    pub elapsed_ms: u64,
    pub error: Option<String>,
}

/// Distributed query plan
#[derive(Debug)]
pub struct DistributedQueryPlan {
    /// Original SQL query
    pub sql: String,
    /// Catalog/database
    pub catalog: String,
    /// Tables referenced in the query
    pub tables: Vec<TableRef>,
    /// Time range filter (if present)
    pub time_range: Option<TimeRange>,
    /// Data nodes to query (after partition pruning)
    pub target_nodes: Vec<DataNodeInfo>,
    /// Whether results need aggregation
    pub needs_aggregation: bool,
    /// Table-level routing: table → owner node_id (RECOMMENDED)
    /// When set, routes each table's query to its owner node
    pub table_owners: Option<HashMap<String, String>>,
    /// File-based routing: node_id → file paths (DEPRECATED)
    /// When set, each node receives only its own files for maximum cache efficiency
    pub node_files: Option<HashMap<String, Vec<String>>>,
}

/// Table reference in a query
#[derive(Debug, Clone)]
pub struct TableRef {
    pub schema: String,
    pub table: String,
}

/// Time range for partition pruning
#[derive(Debug, Clone)]
pub struct TimeRange {
    pub min_time: Option<i64>,
    pub max_time: Option<i64>,
}

/// Query Coordinator for distributed query execution
pub struct QueryCoordinator {
    /// Shard cache for partition→node mapping
    shard_cache: Arc<ShardCache>,

    /// Connection pool to Data nodes
    data_node_pool: Arc<DataNodePool>,

    /// Max parallel fan-out
    max_parallel_fanout: usize,

    /// Optional Meta client for file-based routing
    /// When available, enables distributed parallel queries with node affinity
    meta_client: Option<Arc<MetaClient>>,
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
        }
    }

    /// Set the MetaClient (can be called after creation)
    pub fn set_meta_client(&mut self, meta_client: Arc<MetaClient>) {
        self.meta_client = Some(meta_client);
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
        let table_refs = extract_table_references(sql, catalog);
        let tables: Vec<TableRef> = table_refs
            .iter()
            .map(|(_, schema, table)| TableRef {
                schema: schema.clone(),
                table: table.clone(),
            })
            .collect();

        info!(
            "Extracted {} table references from query: {:?}",
            tables.len(),
            tables
        );

        // Extract time range from WHERE clause for partition pruning
        let time_range = extract_time_range(sql).map(|tr| TimeRange {
            min_time: tr.start_timestamp,
            max_time: tr.end_timestamp,
        });

        if let Some(ref tr) = time_range {
            debug!(
                "Extracted time range: min={:?}, max={:?}",
                tr.min_time, tr.max_time
            );
        }

        // Determine if query has aggregation/ordering that needs consolidation
        let sql_upper = sql.to_uppercase();
        let needs_aggregation = sql_upper.contains("GROUP BY")
            || sql_upper.contains("ORDER BY")
            || sql_upper.contains("LIMIT")
            || sql_upper.contains(" COUNT(")
            || sql_upper.contains(" SUM(")
            || sql_upper.contains(" AVG(")
            || sql_upper.contains(" MIN(")
            || sql_upper.contains(" MAX(");

        // Get queryable Primary data nodes (Active or Retiring)
        let all_primary_nodes = self.shard_cache.get_queryable_primary_nodes();

        // ====================================================================
        // TABLE-LEVEL ROUTING (RECOMMENDED)
        // ====================================================================
        // For time-series workloads:
        // - 95% of queries access today's data (single partition)
        // - Each table is owned by one node
        // - Route to owner → excellent cache locality

        let (table_owners, target_nodes) = if let Some(ref meta_client) = self.meta_client {
            let mut owners: HashMap<String, String> = HashMap::new();
            let mut nodes_to_query: HashMap<String, DataNodeInfo> = HashMap::new();

            for table in &tables {
                let table_key = TableKey::new(catalog, &table.schema, &table.table);

                // First check local cache
                if let Some(owner_id) = self.shard_cache.get_table_owner(&table_key) {
                    if let Some(node) = self.shard_cache.get_data_node(&owner_id) {
                        if node.state == super::shard_cache::NodeState::Active
                            || node.state == super::shard_cache::NodeState::Retiring {
                            owners.insert(table_key.to_path(), owner_id.clone());
                            nodes_to_query.insert(owner_id, node);
                            continue;
                        }
                    }
                }

                // Cache miss - query Meta
                match meta_client.get_table_owner(catalog, &table.schema, &table.table).await {
                    Ok(Some(table_meta)) => {
                        // Get all owners for scatter-gather
                        let all_owners = table_meta.all_owners();
                        let primary_owner = table_meta.primary_owner().unwrap_or("").to_string();

                        // Update local cache with primary owner and all owners
                        self.shard_cache.set_table_owners(
                            &table_key,
                            all_owners.to_vec(),
                            table_meta.version,
                        );

                        // Add all available owners to query set for scatter-gather
                        for owner_id in all_owners {
                            if let Some(node) = self.shard_cache.get_data_node(owner_id) {
                                if node.state == super::shard_cache::NodeState::Active
                                    || node.state == super::shard_cache::NodeState::Retiring
                                {
                                    owners.insert(table_key.to_path(), owner_id.clone());
                                    nodes_to_query.insert(owner_id.clone(), node);
                                } else {
                                    debug!(
                                        "Table {} owner {} is {:?}, skipping",
                                        table_key.to_path(),
                                        owner_id,
                                        node.state
                                    );
                                }
                            } else {
                                debug!(
                                    "Table {} owner {} not found in node list",
                                    table_key.to_path(),
                                    owner_id
                                );
                            }
                        }

                        // If no owners are available, log for fallback
                        if !owners.contains_key(&table_key.to_path()) {
                            debug!(
                                "Table {} has no available owners, will use fallback",
                                table_key.to_path()
                            );
                        }
                    }
                    Ok(None) => {
                        debug!("Table {} not found in Meta, will broadcast", table_key.to_path());
                    }
                    Err(e) => {
                        warn!(
                            "Failed to get table owner from Meta: {}, will broadcast",
                            e
                        );
                    }
                }
            }

            if !owners.is_empty() {
                let nodes: Vec<DataNodeInfo> = nodes_to_query.into_values().collect();
                info!(
                    "Table-level routing: {} tables -> {} nodes",
                    owners.len(),
                    nodes.len()
                );
                (Some(owners), nodes)
            } else {
                // No table owners found, fall back to broadcast
                debug!("No table owners found, falling back to broadcast query");
                (None, all_primary_nodes.clone())
            }
        } else {
            // No MetaClient, use broadcast
            (None, all_primary_nodes.clone())
        };

        // Limit parallel fan-out
        let target_nodes: Vec<_> = target_nodes
            .into_iter()
            .take(self.max_parallel_fanout)
            .collect();

        info!(
            "Planned distributed query to {} nodes for catalog {}: tables={:?}, needs_aggregation={}, table_routing={}",
            target_nodes.len(),
            catalog,
            tables,
            needs_aggregation,
            table_owners.is_some()
        );

        Ok(DistributedQueryPlan {
            sql: sql.to_string(),
            catalog: catalog.to_string(),
            tables,
            time_range,
            target_nodes,
            needs_aggregation,
            table_owners,
            node_files: None, // Deprecated: not using file-based routing
        })
    }

    /// Fan-out query to target nodes via Arrow Flight and collect results
    ///
    /// Uses table-level routing when `table_owners` is available:
    /// - Each table's query goes to its owner node
    /// - Owner has all data cached locally (excellent performance)
    ///
    /// Falls back to broadcast query when table owners are unknown.
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
        // TABLE-LEVEL ROUTING (RECOMMENDED)
        // ====================================================================
        // Route to table owner nodes directly. This provides:
        // - Optimal cache locality (owner has all table data)
        // - Single node for single-table queries (95% of time-series queries)
        // - Fan-out only for multi-table joins

        if plan.table_owners.is_some() || !plan.target_nodes.is_empty() {
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
                                warn!(
                                    "Table owner {} unreachable, trying fallback nodes",
                                    node_id
                                );

                                for fallback_node in &fallback_nodes {
                                    debug!("Retrying query on fallback node {}", fallback_node);
                                    match pool
                                        .query(fallback_node, &catalog, &sql, None)
                                        .await
                                    {
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
                                error!(
                                    "Query failed on {} and all fallback nodes: {}",
                                    node_id, e
                                );
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

    /// Aggregate partial results if needed
    ///
    /// For distributed queries with GROUP BY, ORDER BY, or LIMIT, we need to
    /// re-aggregate the partial results from each Data node.
    async fn aggregate_results(
        &self,
        plan: &DistributedQueryPlan,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, String> {
        if batches.is_empty() {
            return Ok(batches);
        }

        // If no aggregation needed, just return concatenated results
        if !plan.needs_aggregation {
            return Ok(batches);
        }

        // Get the schema from the first batch
        let schema = batches[0].schema();
        let batch_count = batches.len();

        // Create a temporary in-memory table with the partial results
        let mem_table = MemTable::try_new(schema.clone(), vec![batches])
            .map_err(|e| format!("Failed to create memory table: {}", e))?;

        // Create a session context for the final aggregation
        let ctx = SessionContext::new();
        ctx.register_table("partial_results", Arc::new(mem_table))
            .map_err(|e| format!("Failed to register table: {}", e))?;

        // Build the final aggregation query based on the original SQL
        let final_sql = self.build_final_aggregation_sql(&plan.sql);

        debug!("Executing final aggregation: {}", final_sql);

        // Execute the final aggregation
        let df = ctx
            .sql(&final_sql)
            .await
            .map_err(|e| format!("Failed to parse final aggregation SQL: {}", e))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| format!("Failed to execute final aggregation: {}", e))?;

        info!(
            "Aggregated {} partial batches into {} result batches",
            batch_count,
            result_batches.len()
        );

        Ok(result_batches)
    }

    /// Build SQL for final aggregation of partial results
    ///
    /// This transforms the original SQL to work on the "partial_results" table.
    /// For simple ORDER BY/LIMIT, we just apply them to the partial results.
    /// For GROUP BY with aggregates, we re-aggregate the partial results.
    fn build_final_aggregation_sql(&self, original_sql: &str) -> String {
        let sql_upper = original_sql.to_uppercase();

        // Check if this is a simple ORDER BY / LIMIT query (no aggregation)
        let has_group_by = sql_upper.contains("GROUP BY");
        let has_order_by = sql_upper.contains("ORDER BY");
        let has_limit = sql_upper.contains("LIMIT");

        if !has_group_by && (has_order_by || has_limit) {
            // Simple case: just apply ORDER BY and/or LIMIT to partial results
            let mut final_sql = "SELECT * FROM partial_results".to_string();

            // Extract and append ORDER BY clause
            if let Some(order_pos) = sql_upper.find("ORDER BY") {
                let order_clause = &original_sql[order_pos..];
                // Find where ORDER BY ends (at LIMIT or end of string)
                let end_pos = order_clause
                    .to_uppercase()
                    .find("LIMIT")
                    .unwrap_or(order_clause.len());
                final_sql.push(' ');
                final_sql.push_str(&order_clause[..end_pos].trim());
            }

            // Extract and append LIMIT clause
            if let Some(limit_pos) = sql_upper.find("LIMIT") {
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }

            return final_sql;
        }

        // For GROUP BY queries, we need to re-aggregate
        // This is a simplified approach - a full implementation would parse
        // the SELECT clause and rebuild proper aggregations
        if has_group_by {
            // For now, just apply ORDER BY and LIMIT if present
            // The partial results already have the GROUP BY applied
            let mut final_sql = "SELECT * FROM partial_results".to_string();

            if let Some(order_pos) = sql_upper.find("ORDER BY") {
                let order_clause = &original_sql[order_pos..];
                let end_pos = order_clause
                    .to_uppercase()
                    .find("LIMIT")
                    .unwrap_or(order_clause.len());
                final_sql.push(' ');
                final_sql.push_str(&order_clause[..end_pos].trim());
            }

            if let Some(limit_pos) = sql_upper.find("LIMIT") {
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }

            warn!(
                "GROUP BY re-aggregation not fully implemented. \
                 Results may be incorrect for queries with SUM/COUNT/AVG across partitions."
            );

            return final_sql;
        }

        // Default: just select all from partial results
        "SELECT * FROM partial_results".to_string()
    }
}
