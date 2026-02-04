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
    /// Partition-level routing: target_node_id → [node_id partitions to query]
    ///
    /// When partition routing is used, each target node is assigned specific
    /// `node_id` partitions to query. This enables:
    /// - **Cache affinity**: Each node queries partitions it wrote (cached locally)
    /// - **No duplicates**: Each partition is queried by exactly one node
    /// - **Fallback support**: If a node is down, its partitions are reassigned
    ///
    /// Example:
    /// ```text
    /// {
    ///   "data-1": ["data-1"],        // data-1 queries its own partition
    ///   "data-2": ["data-2"],        // data-2 queries its own partition
    ///   "data-3": ["data-4"],        // data-3 is fallback for data-4 (down)
    /// }
    /// ```
    ///
    /// When this is Some, fan_out_query() will rewrite SQL to add:
    /// `AND node_id IN ('data-1')` for each target node's assigned partitions.
    pub partition_assignments: Option<HashMap<String, Vec<String>>>,
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

/// Result of partition-level routing
///
/// Contains both target nodes and their partition assignments for
/// cache-affinity-aware distributed query execution.
#[derive(Debug)]
pub struct PartitionRoutingResult {
    /// Target nodes to query
    pub target_nodes: Vec<DataNodeInfo>,
    /// Map of target_node_id → [node_id partitions to query]
    /// Each target node is assigned specific partitions to ensure:
    /// - No duplicate data (each partition queried by exactly one node)
    /// - Cache affinity (nodes query their own partitions when possible)
    pub partition_assignments: HashMap<String, Vec<String>>,
}
