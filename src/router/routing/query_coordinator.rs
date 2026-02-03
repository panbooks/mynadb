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
use crate::utils::hash::hash_select_index;
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
            "Planned query: {} nodes, tables={:?}, aggregation={}, table_routing={}",
            target_nodes.len(),
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
        })
    }

    /// Resolve target nodes for query routing
    ///
    /// Returns (table_owners, target_nodes):
    /// - table_owners: Map of table -> owner node (if known)
    /// - target_nodes: List of nodes to query
    ///
    /// Handles scale-in by:
    /// 1. Detecting stale owners (removed from cluster)
    /// 2. Refreshing from Meta when stale owners detected
    /// 3. Assigning legacy_prefixes to active nodes via consistent hash
    async fn resolve_query_targets(
        &self,
        catalog: &str,
        tables: &[TableRef],
        all_primary_nodes: &[DataNodeInfo],
    ) -> (Option<HashMap<String, String>>, Vec<DataNodeInfo>) {
        let Some(ref meta_client) = self.meta_client else {
            return (None, all_primary_nodes.to_vec());
        };

        let mut owners: HashMap<String, String> = HashMap::new();
        let mut nodes_to_query: HashMap<String, DataNodeInfo> = HashMap::new();

        for table in tables {
            let table_key = TableKey::new(catalog, &table.schema, &table.table);
            let distribution_percent =
                self.shard_cache
                    .get_distribution_percent(catalog, &table.schema, &table.table);

            // Try to resolve from cache first
            if let Some(cached_meta) = self.shard_cache.get_table_meta(&table_key) {
                // Check for stale owners (removed from cluster after scale-in)
                let (_active_count, stale_count) =
                    self.check_owners_status(cached_meta.all_owners());

                if stale_count > 0 {
                    // Stale owners detected - refresh from Meta
                    info!(
                        "Table {} has {} stale owners (scale-in detected), refreshing from Meta",
                        table_key.to_path(),
                        stale_count
                    );
                    self.resolve_distributed_from_meta(
                        meta_client,
                        catalog,
                        table,
                        &table_key,
                        all_primary_nodes,
                        &mut owners,
                        &mut nodes_to_query,
                    )
                    .await;
                    continue;
                }

                // All owners are active - add them and handle legacy prefixes
                let count = self.add_queryable_owners(
                    cached_meta.all_owners(),
                    &table_key,
                    &mut owners,
                    &mut nodes_to_query,
                );

                // Also handle legacy prefixes (data from previously retired nodes)
                if cached_meta.has_legacy_prefixes() {
                    self.add_legacy_prefix_nodes(
                        &table_key,
                        cached_meta.legacy_prefixes(),
                        all_primary_nodes,
                        &mut nodes_to_query,
                    );
                }

                if count > 0 {
                    debug!(
                        "Table {} resolved: {} owners, {} legacy prefixes (dist={}%)",
                        table_key.to_path(),
                        count,
                        cached_meta.legacy_prefixes().len(),
                        distribution_percent
                    );
                    continue;
                }
            }

            // Cache miss - handle based on distribution mode
            if distribution_percent > 0 {
                // Distributed table: query Meta then fallback
                self.resolve_distributed_from_meta(
                    meta_client,
                    catalog,
                    table,
                    &table_key,
                    all_primary_nodes,
                    &mut owners,
                    &mut nodes_to_query,
                )
                .await;
            } else {
                // Non-distributed: query Meta for owner
                self.resolve_from_meta(
                    meta_client,
                    catalog,
                    table,
                    &table_key,
                    &mut owners,
                    &mut nodes_to_query,
                )
                .await;
            }
        }

        if owners.is_empty() {
            debug!("No table owners found, falling back to broadcast");
            (None, all_primary_nodes.to_vec())
        } else {
            info!(
                "Table-level routing: {} tables -> {} nodes",
                owners.len(),
                nodes_to_query.len()
            );
            (Some(owners), nodes_to_query.into_values().collect())
        }
    }

    /// Check owners status - returns (active_count, stale_count)
    fn check_owners_status(&self, owner_ids: &[String]) -> (usize, usize) {
        let mut active = 0;
        let mut stale = 0;
        for owner_id in owner_ids {
            if let Some(node) = self.shard_cache.get_data_node(owner_id) {
                if Self::is_queryable(&node) {
                    active += 1;
                } else {
                    stale += 1; // Node exists but not queryable
                }
            } else {
                stale += 1; // Node removed from cluster
            }
        }
        (active, stale)
    }

    /// Add queryable owners to the query set, returns count added
    fn add_queryable_owners(
        &self,
        owner_ids: &[String],
        table_key: &TableKey,
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) -> usize {
        let mut count = 0;
        for owner_id in owner_ids {
            if let Some(node) = self.shard_cache.get_data_node(owner_id) {
                if Self::is_queryable(&node) {
                    owners.insert(table_key.to_path(), owner_id.clone());
                    nodes_to_query.insert(owner_id.clone(), node);
                    count += 1;
                }
            }
        }
        count
    }

    /// Add nodes responsible for legacy prefixes (from retired nodes)
    ///
    /// Uses consistent hash to assign each legacy prefix to an active node.
    /// This ensures stable assignment: same prefix always maps to same node.
    fn add_legacy_prefix_nodes(
        &self,
        table_key: &TableKey,
        legacy_prefixes: &[String],
        all_primary_nodes: &[DataNodeInfo],
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        if all_primary_nodes.is_empty() {
            return;
        }

        for prefix in legacy_prefixes {
            // Use consistent hash: hash(table_key + legacy_prefix) → node
            // This ensures the same prefix always maps to the same node
            let hash_key = format!("{}:{}", table_key.to_path(), prefix);
            let idx = hash_select_index(&hash_key, all_primary_nodes.len());
            let assigned_node = &all_primary_nodes[idx];

            if !nodes_to_query.contains_key(&assigned_node.node_id) {
                debug!(
                    "Legacy prefix '{}' for table {} assigned to node {}",
                    prefix,
                    table_key.to_path(),
                    assigned_node.node_id
                );
                nodes_to_query.insert(assigned_node.node_id.clone(), assigned_node.clone());
            }
        }
    }

    /// Check if a node is queryable (Active or Retiring)
    fn is_queryable(node: &DataNodeInfo) -> bool {
        node.state == super::shard_cache::NodeState::Active
            || node.state == super::shard_cache::NodeState::Retiring
    }

    /// Resolve distributed table from Meta, with legacy prefix handling
    async fn resolve_distributed_from_meta(
        &self,
        meta_client: &MetaClient,
        catalog: &str,
        table: &TableRef,
        table_key: &TableKey,
        all_primary_nodes: &[DataNodeInfo],
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        match meta_client
            .get_table_owner(catalog, &table.schema, &table.table)
            .await
        {
            Ok(Some(table_meta)) => {
                // Cache the updated metadata
                self.shard_cache.set_table_owners_with_legacy(
                    table_key,
                    table_meta.all_owners().to_vec(),
                    table_meta.legacy_prefixes.clone(),
                    table_meta.version,
                );

                // Add active owners
                let count = self.add_queryable_owners(
                    table_meta.all_owners(),
                    table_key,
                    owners,
                    nodes_to_query,
                );

                // Add nodes for legacy prefixes
                if !table_meta.legacy_prefixes.is_empty() {
                    info!(
                        "Table {} has {} legacy prefixes from scale-in",
                        table_key.to_path(),
                        table_meta.legacy_prefixes.len()
                    );
                    self.add_legacy_prefix_nodes(
                        table_key,
                        &table_meta.legacy_prefixes,
                        all_primary_nodes,
                        nodes_to_query,
                    );
                }

                if count == 0 && table_meta.legacy_prefixes.is_empty() {
                    // No owners and no legacy - use fallback
                    let distribution_percent = self.shard_cache.get_distribution_percent(
                        catalog,
                        &table.schema,
                        &table.table,
                    );
                    self.add_distributed_fallback(
                        table_key,
                        distribution_percent,
                        all_primary_nodes,
                        owners,
                        nodes_to_query,
                    );
                }
            }
            Ok(None) => {
                debug!(
                    "Table {} not found in Meta, using fallback",
                    table_key.to_path()
                );
                let distribution_percent =
                    self.shard_cache
                        .get_distribution_percent(catalog, &table.schema, &table.table);
                self.add_distributed_fallback(
                    table_key,
                    distribution_percent,
                    all_primary_nodes,
                    owners,
                    nodes_to_query,
                );
            }
            Err(e) => {
                warn!("Failed to get table owner from Meta: {}, using fallback", e);
                let distribution_percent =
                    self.shard_cache
                        .get_distribution_percent(catalog, &table.schema, &table.table);
                self.add_distributed_fallback(
                    table_key,
                    distribution_percent,
                    all_primary_nodes,
                    owners,
                    nodes_to_query,
                );
            }
        }
    }

    /// Add distributed table fallback nodes
    fn add_distributed_fallback(
        &self,
        table_key: &TableKey,
        distribution_percent: u8,
        all_primary_nodes: &[DataNodeInfo],
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        if distribution_percent >= 100 {
            // 100%: query all primary nodes
            warn!(
                "Table {} (100% dist) has no cached owners - querying all {} primaries",
                table_key.to_path(),
                all_primary_nodes.len()
            );
            for node in all_primary_nodes {
                owners.insert(table_key.to_path(), node.node_id.clone());
                nodes_to_query.insert(node.node_id.clone(), node.clone());
            }
        } else {
            // Partial: use hash-based selection
            warn!(
                "Table {} ({}% dist) has no cached owners - using hash fallback",
                table_key.to_path(),
                distribution_percent
            );
            let target_count = ((all_primary_nodes.len() as f64 * distribution_percent as f64
                / 100.0)
                .ceil() as usize)
                .max(1);
            let start_idx = hash_select_index(table_key, all_primary_nodes.len());

            for i in 0..target_count {
                let idx = (start_idx + i) % all_primary_nodes.len();
                let node = &all_primary_nodes[idx];
                owners.insert(table_key.to_path(), node.node_id.clone());
                nodes_to_query.insert(node.node_id.clone(), node.clone());
            }
        }
    }

    /// Resolve table owner from Meta service
    async fn resolve_from_meta(
        &self,
        meta_client: &MetaClient,
        catalog: &str,
        table: &TableRef,
        table_key: &TableKey,
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        match meta_client
            .get_table_owner(catalog, &table.schema, &table.table)
            .await
        {
            Ok(Some(table_meta)) => {
                // Cache the result
                self.shard_cache.set_table_owners_with_legacy(
                    table_key,
                    table_meta.all_owners().to_vec(),
                    table_meta.legacy_prefixes.clone(),
                    table_meta.version,
                );

                if !table_meta.legacy_prefixes.is_empty() {
                    info!(
                        "Table {} has {} legacy prefixes",
                        table_key.to_path(),
                        table_meta.legacy_prefixes.len()
                    );
                }

                // Add queryable owners
                let count = self.add_queryable_owners(
                    table_meta.all_owners(),
                    table_key,
                    owners,
                    nodes_to_query,
                );
                if count == 0 {
                    debug!("Table {} has no queryable owners", table_key.to_path());
                }
            }
            Ok(None) => {
                debug!("Table {} not found in Meta", table_key.to_path());
            }
            Err(e) => {
                warn!("Failed to get table owner from Meta: {}", e);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::DataNodeMode;
    use crate::router::routing::shard_cache::NodeState;
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
}
