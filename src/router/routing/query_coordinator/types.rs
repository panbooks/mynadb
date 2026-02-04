//! Query coordinator types
//!
//! Core data structures for distributed query planning and execution.

use super::super::shard_cache::DataNodeInfo;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

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
    /// Explicit date filter (e.g., date = '2024-01-15')
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}
