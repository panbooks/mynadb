//! Scale-in (retirement) types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{current_timestamp_ms, NodeState};

/// Status of a node retirement (scale-in) operation
///
/// Tracks the progress of retiring a node from the cluster.
/// The node goes through: Active → Retiring → Drained → Stopped
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetireStatus {
    /// Node ID being retired
    pub node_id: String,
    /// Current state of the node
    pub state: NodeState,
    /// When retirement started (Unix timestamp ms)
    pub started_at: i64,
    /// Total tables this node was owning
    pub tables_total: usize,
    /// Number of tables already reassigned to other nodes
    pub tables_reassigned: usize,
    /// Remaining rows in memtable (0 when fully flushed to S3)
    pub memtable_rows_remaining: u64,
    /// Estimated seconds until retirement completes
    pub estimated_completion_secs: u64,
}

impl RetireStatus {
    pub fn new(node_id: String, tables_total: usize) -> Self {
        Self {
            node_id,
            state: NodeState::Retiring,
            started_at: current_timestamp_ms(),
            tables_total,
            tables_reassigned: 0,
            memtable_rows_remaining: 0,
            estimated_completion_secs: 0,
        }
    }

    /// Check if retirement is complete (all tables reassigned, memtable empty)
    pub fn is_complete(&self) -> bool {
        self.tables_reassigned >= self.tables_total && self.memtable_rows_remaining == 0
    }

    /// Calculate progress percentage (0-100)
    pub fn progress_percent(&self) -> u8 {
        if self.tables_total == 0 {
            100
        } else {
            ((self.tables_reassigned as f64 / self.tables_total as f64) * 100.0) as u8
        }
    }
}

/// Request to scale in (shrink) the cluster
///
/// Supports two modes:
/// 1. Explicit: Specify exact nodes to retire
/// 2. Auto-select: Specify counts, let Meta choose nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInRequest {
    /// Explicit list of node IDs to retire
    #[serde(default)]
    pub nodes: Vec<String>,
    /// Number of Primary nodes to reduce (Meta auto-selects)
    #[serde(default)]
    pub reduce_primaries: Option<u32>,
    /// Number of Standby nodes to reduce (Meta auto-selects)
    #[serde(default)]
    pub reduce_standbys: Option<u32>,
}

impl ScaleInRequest {
    /// Create a request to retire specific nodes
    pub fn with_nodes(nodes: Vec<String>) -> Self {
        Self {
            nodes,
            reduce_primaries: None,
            reduce_standbys: None,
        }
    }

    /// Create a request to auto-select nodes to retire
    pub fn with_counts(reduce_primaries: u32, reduce_standbys: u32) -> Self {
        Self {
            nodes: Vec::new(),
            reduce_primaries: Some(reduce_primaries),
            reduce_standbys: Some(reduce_standbys),
        }
    }

    /// Check if this is an explicit node list request
    pub fn is_explicit(&self) -> bool {
        !self.nodes.is_empty()
    }
}

/// Recommendation for which nodes to scale in
///
/// Returned by the /scale-in/recommend endpoint.
/// Operator can review and approve before executing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInRecommendation {
    /// Recommended Primary nodes to retire
    pub recommended_primaries: Vec<String>,
    /// Recommended Standby nodes to retire
    pub recommended_standbys: Vec<String>,
    /// Reasons for each recommendation (node_id -> reason)
    pub reasons: HashMap<String, String>,
    /// Warnings about potential issues
    pub warnings: Vec<String>,
}

impl ScaleInRecommendation {
    pub fn new() -> Self {
        Self {
            recommended_primaries: Vec::new(),
            recommended_standbys: Vec::new(),
            reasons: HashMap::new(),
            warnings: Vec::new(),
        }
    }

    /// Add a Primary node recommendation
    pub fn add_primary(&mut self, node_id: String, reason: String) {
        self.recommended_primaries.push(node_id.clone());
        self.reasons.insert(node_id, reason);
    }

    /// Add a Standby node recommendation
    pub fn add_standby(&mut self, node_id: String, reason: String) {
        self.recommended_standbys.push(node_id.clone());
        self.reasons.insert(node_id, reason);
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Total nodes recommended for retirement
    pub fn total_nodes(&self) -> usize {
        self.recommended_primaries.len() + self.recommended_standbys.len()
    }
}

impl Default for ScaleInRecommendation {
    fn default() -> Self {
        Self::new()
    }
}
