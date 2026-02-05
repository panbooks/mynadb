//! Cluster types for Admin Console

use serde::{Deserialize, Serialize};

/// Overall cluster health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub healthy: bool,
    pub meta_leader: Option<String>,
    pub meta_term: u64,
    pub node_counts: NodeCounts,
    pub total_tables: u32,
    pub total_partitions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Node counts by role and state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeCounts {
    pub meta: RoleCounts,
    pub router: RoleCounts,
    pub data: DataNodeCounts,
    pub compactor: RoleCounts,
}

/// Counts for a single role
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoleCounts {
    pub total: u32,
    pub active: u32,
    pub inactive: u32,
}

/// Counts for data nodes (includes mode breakdown)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataNodeCounts {
    pub total: u32,
    pub active: u32,
    pub inactive: u32,
    pub primary: u32,
    pub standby: u32,
    pub retiring: u32,
    pub drained: u32,
}

/// Raft status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftStatus {
    pub node_id: String,
    pub role: String,
    pub term: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<String>,
    pub committed_index: u64,
    pub applied_index: u64,
}

/// API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            message: None,
        }
    }

    pub fn error(error: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error.into()),
            message: None,
        }
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}
