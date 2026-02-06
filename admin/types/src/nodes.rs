//! Node types for Admin Console

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Unknown,
    Meta,
    Router,
    Data,
    Compactor,
}

impl Default for NodeRole {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Meta => write!(f, "Meta"),
            Self::Router => write!(f, "Router"),
            Self::Data => write!(f, "Data"),
            Self::Compactor => write!(f, "Compactor"),
        }
    }
}

impl From<i32> for NodeRole {
    fn from(v: i32) -> Self {
        match v {
            1 => NodeRole::Meta,
            2 => NodeRole::Router,
            3 => NodeRole::Data,
            4 => NodeRole::Compactor,
            _ => NodeRole::Unknown,
        }
    }
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    Unknown,
    Starting,
    Active,
    Draining,
    Inactive,
    Retiring,
    Drained,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Starting => write!(f, "Starting"),
            Self::Active => write!(f, "Active"),
            Self::Draining => write!(f, "Draining"),
            Self::Inactive => write!(f, "Inactive"),
            Self::Retiring => write!(f, "Retiring"),
            Self::Drained => write!(f, "Drained"),
        }
    }
}

impl From<i32> for NodeState {
    fn from(v: i32) -> Self {
        match v {
            1 => NodeState::Starting,
            2 => NodeState::Active,
            3 => NodeState::Draining,
            4 => NodeState::Inactive,
            5 => NodeState::Retiring,
            6 => NodeState::Drained,
            _ => NodeState::Unknown,
        }
    }
}

/// Data node operational mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DataNodeMode {
    #[default]
    Primary,
    Standby,
}

impl std::fmt::Display for DataNodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "Primary"),
            Self::Standby => write!(f, "Standby"),
        }
    }
}

impl From<i32> for DataNodeMode {
    fn from(v: i32) -> Self {
        match v {
            1 => DataNodeMode::Standby,
            _ => DataNodeMode::Primary,
        }
    }
}

/// Information about a registered node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub role: NodeRole,
    pub state: NodeState,
    pub advertise_addr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flight_addr: Option<String>,
    pub registered_at: i64,
    pub last_heartbeat: i64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<DataNodeMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backing_standby: Option<String>,
    #[serde(default)]
    pub backed_primaries: Vec<String>,
}

impl NodeInfo {
    /// Check if this is a Primary data node
    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Data && self.mode == Some(DataNodeMode::Primary)
    }

    /// Check if this is a Standby data node
    pub fn is_standby(&self) -> bool {
        self.role == NodeRole::Data && self.mode == Some(DataNodeMode::Standby)
    }

    /// Get the availability zone label if present
    pub fn zone(&self) -> Option<&str> {
        self.labels
            .get("topology.kubernetes.io/zone")
            .map(|s| s.as_str())
    }

    /// Get the region label if present
    pub fn region(&self) -> Option<&str> {
        self.labels
            .get("topology.kubernetes.io/region")
            .map(|s| s.as_str())
    }
}

/// Summary of node for list views
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSummary {
    pub node_id: String,
    pub role: NodeRole,
    pub state: NodeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<DataNodeMode>,
    pub advertise_addr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    pub tables_owned: u32,
    pub last_heartbeat_ago_secs: i64,
}

/// Query parameters for listing nodes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListNodesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

/// Response for list nodes endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListNodesResponse {
    pub nodes: Vec<NodeSummary>,
    pub total: usize,
}

/// Response for node detail endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDetailResponse {
    pub node: NodeInfo,
    pub tables_owned: Vec<String>,
    pub health: NodeHealth,
}

/// Node health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub healthy: bool,
    pub last_heartbeat_ms: i64,
    pub heartbeat_age_secs: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
