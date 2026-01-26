//! Query Coordinator - Coordinates distributed queries across Data nodes
//!
//! The QueryCoordinator is responsible for:
//! - Parsing SQL to extract table references and time ranges
//! - Determining which Data nodes to query based on partition pruning
//! - Fan-out queries to multiple Data nodes via Arrow Flight
//! - Aggregating results from partial queries

use super::shard_cache::{DataNodeInfo, ShardCache};
use crate::router::client::DataNodePool;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

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
        }
    }

    /// Execute a distributed query
    pub async fn execute_query(
        &self,
        catalog: &str,
        sql: &str,
    ) -> QueryResult {
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
        let (batches, errors) = self.fan_out_query(&plan).await;

        let total_rows: u64 = batches
            .iter()
            .map(|b| b.num_rows() as u64)
            .sum();

        let success = errors.is_empty();
        let error = if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        };

        QueryResult {
            success,
            batches,
            total_rows,
            bytes_scanned: 0, // TODO: Track bytes scanned
            nodes_queried: plan.target_nodes.len(),
            elapsed_ms: start.elapsed().as_millis() as u64,
            error,
        }
    }

    /// Plan a distributed query
    async fn plan_distributed_query(
        &self,
        catalog: &str,
        sql: &str,
    ) -> Result<DistributedQueryPlan, String> {
        // TODO: Parse SQL to extract table references and time ranges
        // For now, we query all active nodes

        let target_nodes = self.shard_cache.get_active_data_nodes();

        // Limit parallel fan-out
        let target_nodes: Vec<_> = target_nodes
            .into_iter()
            .take(self.max_parallel_fanout)
            .collect();

        debug!(
            "Planned distributed query to {} nodes for catalog {}",
            target_nodes.len(),
            catalog
        );

        Ok(DistributedQueryPlan {
            sql: sql.to_string(),
            catalog: catalog.to_string(),
            tables: Vec::new(), // TODO: Parse from SQL
            time_range: None,   // TODO: Extract from WHERE clause
            target_nodes,
            needs_aggregation: true, // Assume aggregation needed for now
        })
    }

    /// Fan-out query to target nodes via Arrow Flight and collect results
    async fn fan_out_query(
        &self,
        plan: &DistributedQueryPlan,
    ) -> (Vec<RecordBatch>, Vec<String>) {
        let mut futures = Vec::new();

        for node in &plan.target_nodes {
            let pool = Arc::clone(&self.data_node_pool);
            let node_id = node.node_id.clone();
            let sql = plan.sql.clone();
            let catalog = plan.catalog.clone();

            let future = async move {
                debug!("Querying {} with: {}", node_id, sql);

                // Execute query via Arrow Flight DoGet
                match pool.query(&node_id, &catalog, &sql, None).await {
                    Ok(batches) => {
                        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                        debug!(
                            "Received {} batches ({} rows) from {}",
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
            };

            futures.push(future);
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
    #[allow(dead_code)]
    async fn aggregate_results(
        &self,
        plan: &DistributedQueryPlan,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, String> {
        if !plan.needs_aggregation || batches.is_empty() {
            return Ok(batches);
        }

        // TODO: Implement aggregation for GROUP BY, ORDER BY, LIMIT
        // For now, just return the concatenated results
        Ok(batches)
    }
}
