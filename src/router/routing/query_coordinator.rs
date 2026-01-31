//! Query Coordinator - Coordinates distributed queries across Data nodes
//!
//! The QueryCoordinator is responsible for:
//! - Parsing SQL to extract table references and time ranges
//! - Determining which Data nodes to query based on partition pruning
//! - Fan-out queries to multiple Data nodes via Arrow Flight
//! - Aggregating results from partial queries
//!
//! ## Distributed Parallel Query Mode
//!
//! When a MetaClient is available, the QueryCoordinator uses file-based routing:
//! 1. Query Meta for files (get_files_for_query)
//! 2. Group files by `created_by` (node_id that wrote the file)
//! 3. Send each node only its own files via `query_with_files`
//!
//! This enables:
//! - **Node affinity**: Each node reads only files it created
//! - **Local cache hit**: Files are cached on the node that wrote them
//! - **Parallel execution**: N nodes = N× query throughput

use super::shard_cache::{DataNodeInfo, ShardCache};
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
    /// File-based routing: node_id → file paths
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
        // Note: We only query Primary nodes - Standby nodes are cold backups.
        // Retiring nodes are included because they may have unflushed memtable data.
        // Partition-level pruning would require a reverse index (table→partitions→nodes),
        // which adds complexity. Since queries are typically fast and parallelized,
        // the overhead of querying extra nodes is minimal compared to building/maintaining
        // a reverse index. The time_range is used for partition pruning at the data node level.
        let target_nodes = self.shard_cache.get_queryable_primary_nodes();

        // Limit parallel fan-out
        let target_nodes: Vec<_> = target_nodes
            .into_iter()
            .take(self.max_parallel_fanout)
            .collect();

        info!(
            "Planned distributed query to {} nodes for catalog {}: tables={:?}, needs_aggregation={}",
            target_nodes.len(),
            catalog,
            tables,
            needs_aggregation
        );

        // Try file-based routing if MetaClient is available
        let node_files = if let Some(ref meta_client) = self.meta_client {
            // Query Meta for files grouped by created_by (node_id)
            let table_names: Vec<String> = tables
                .iter()
                .map(|t| format!("{}.{}", t.schema, t.table))
                .collect();

            let min_time = time_range.as_ref().and_then(|tr| tr.min_time);
            let max_time = time_range.as_ref().and_then(|tr| tr.max_time);

            match meta_client
                .get_files_for_query(catalog, table_names, min_time, max_time)
                .await
            {
                Ok((files, _nodes)) => {
                    // Group files by created_by (node_id)
                    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
                    for file in files {
                        grouped
                            .entry(file.created_by.clone())
                            .or_default()
                            .push(file.path.clone());
                    }

                    if !grouped.is_empty() {
                        info!(
                            "File-based routing: {} nodes with {} total files",
                            grouped.len(),
                            grouped.values().map(|v| v.len()).sum::<usize>()
                        );
                        Some(grouped)
                    } else {
                        debug!("No files found in Meta, falling back to broadcast query");
                        None
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to get files from Meta: {}, falling back to broadcast query",
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(DistributedQueryPlan {
            sql: sql.to_string(),
            catalog: catalog.to_string(),
            tables,
            time_range,
            target_nodes,
            needs_aggregation,
            node_files,
        })
    }

    /// Fan-out query to target nodes via Arrow Flight and collect results
    ///
    /// When `node_files` is available, uses file-based routing for optimal cache affinity.
    /// Otherwise, falls back to broadcast query to all nodes.
    async fn fan_out_query(&self, plan: &DistributedQueryPlan) -> (Vec<RecordBatch>, Vec<String>) {
        // Type alias for boxed futures (needed because different async blocks have different types)
        type QueryFuture = Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, String>> + Send>>;

        let mut futures: Vec<QueryFuture> = Vec::new();

        // Use file-based routing if available (optimal for distributed parallel queries)
        if let Some(ref node_files) = plan.node_files {
            // Pre-check node availability and redistribute files from unavailable nodes
            let reachable_nodes = self.data_node_pool.get_reachable_node_ids();
            if reachable_nodes.is_empty() {
                return (
                    Vec::new(),
                    vec!["No reachable data nodes available".to_string()],
                );
            }

            // Separate files into reachable and unreachable node assignments
            let mut reachable_node_files: HashMap<String, Vec<String>> = HashMap::new();
            let mut orphaned_files: Vec<String> = Vec::new();

            for (node_id, file_paths) in node_files {
                if self.data_node_pool.is_node_reachable(node_id) {
                    reachable_node_files
                        .entry(node_id.clone())
                        .or_default()
                        .extend(file_paths.clone());
                } else {
                    // Node is unavailable (scaled in or circuit breaker open)
                    warn!(
                        "Node {} is unavailable, redistributing {} files to other nodes",
                        node_id,
                        file_paths.len()
                    );
                    orphaned_files.extend(file_paths.clone());
                }
            }

            // Redistribute orphaned files to reachable nodes (round-robin)
            if !orphaned_files.is_empty() {
                let reachable_list: Vec<_> = reachable_nodes.iter().collect();
                for (i, file) in orphaned_files.iter().enumerate() {
                    let target_node = &reachable_list[i % reachable_list.len()];
                    reachable_node_files
                        .entry((*target_node).clone())
                        .or_default()
                        .push(file.clone());
                }
                info!(
                    "Redistributed {} orphaned files to {} reachable nodes",
                    orphaned_files.len(),
                    reachable_list.len()
                );
            }

            // Create query futures for reachable nodes
            for (node_id, file_paths) in reachable_node_files {
                let pool = Arc::clone(&self.data_node_pool);
                let node_id_clone = node_id.clone();
                let sql = plan.sql.clone();
                let catalog = plan.catalog.clone();
                let files = file_paths;

                let future: QueryFuture = Box::pin(async move {
                    debug!(
                        "Querying {} with {} files: {}",
                        node_id_clone,
                        files.len(),
                        sql
                    );

                    // Execute query with explicit file list (node reads only its own files)
                    match pool
                        .query_with_files(&node_id_clone, &catalog, &sql, files.clone())
                        .await
                    {
                        Ok(batches) => {
                            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                            debug!(
                                "Received {} batches ({} rows) from {} (file-based)",
                                batches.len(),
                                total_rows,
                                node_id_clone
                            );
                            Ok(batches)
                        }
                        Err(e) => {
                            error!("Query failed on {}: {}", node_id_clone, e);
                            Err(format!("Failed to query {}: {}", node_id_clone, e))
                        }
                    }
                });

                futures.push(future);
            }
        } else {
            // Fallback: broadcast query to all active nodes
            for node in &plan.target_nodes {
                let pool = Arc::clone(&self.data_node_pool);
                let node_id = node.node_id.clone();
                let sql = plan.sql.clone();
                let catalog = plan.catalog.clone();

                let future: QueryFuture = Box::pin(async move {
                    debug!("Querying {} with: {}", node_id, sql);

                    // Execute query via Arrow Flight DoGet (broadcast mode)
                    match pool.query(&node_id, &catalog, &sql, None).await {
                        Ok(batches) => {
                            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                            debug!(
                                "Received {} batches ({} rows) from {} (broadcast)",
                                batches.len(),
                                total_rows,
                                node_id
                            );
                            Ok(batches)
                        }
                        Err(e) => {
                            error!("Query failed on {}: {}", node_id, e);
                            Err(format!("Failed to query {}: {}", node_id, e))
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
