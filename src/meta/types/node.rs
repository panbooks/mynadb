//! Node types for the Meta service

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::current_timestamp_ms;

/// Standard Kubernetes topology labels for node placement
///
/// These labels are used for zone-aware replica placement to ensure
/// leader and standby nodes are in different failure domains.
pub mod node_labels {
    /// Kubernetes availability zone label
    /// Example values: "us-east-1a", "eu-west-1b"
    pub const ZONE: &str = "topology.kubernetes.io/zone";

    /// Kubernetes region label
    /// Example values: "us-east-1", "eu-west-1"
    pub const REGION: &str = "topology.kubernetes.io/region";

    /// Custom rack/shelf label for physical server placement
    /// Example values: "rack-1", "shelf-a"
    pub const RACK: &str = "mynadb.io/rack";

    /// Node pool or instance type label
    /// Example values: "high-memory", "standard"
    pub const POOL: &str = "mynadb.io/pool";
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeRole {
    Unknown = 0,
    Meta = 1,
    Router = 2,
    Data = 3,
    Compactor = 4,
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

impl From<NodeRole> for i32 {
    fn from(role: NodeRole) -> Self {
        role as i32
    }
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeState {
    Unknown = 0,
    Starting = 1,
    Active = 2,
    /// Legacy draining state for graceful shutdown (maintenance)
    Draining = 3,
    Inactive = 4,
    /// Scale-in: Node is retiring, no new writes, flushing memtable to S3
    /// Tables being reassigned to other nodes
    Retiring = 5,
    /// Scale-in complete: Memtable empty, all tables reassigned, safe to stop
    Drained = 6,
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

impl From<NodeState> for i32 {
    fn from(state: NodeState) -> Self {
        state as i32
    }
}

/// Data node operational mode (Cold Standby Architecture)
///
/// In the Cold Standby architecture (No-Promotion design), Data nodes are either
/// Primary (actively serving traffic) or Standby (cold backup only).
///
/// Key properties:
/// - Primary nodes: Serve writes and queries, assigned as partition leaders
/// - Standby nodes: Only receive replicated data, never serve queries
/// - Roles are IMMUTABLE: Standby never promotes to Primary, Primary never demotes
/// - On Primary failure: Standby flushes cache to S3, K8s replaces the Primary pod
/// - This design eliminates split-brain risk and simplifies failover logic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum DataNodeMode {
    /// Actively serves writes and queries, assigned as partition leaders
    #[default]
    Primary = 0,
    /// Cold standby - only receives replication, flushes cache on Primary failure
    Standby = 1,
}

impl From<i32> for DataNodeMode {
    fn from(v: i32) -> Self {
        match v {
            0 => DataNodeMode::Primary,
            1 => DataNodeMode::Standby,
            _ => DataNodeMode::Primary,
        }
    }
}

impl From<DataNodeMode> for i32 {
    fn from(mode: DataNodeMode) -> Self {
        mode as i32
    }
}

impl std::fmt::Display for DataNodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataNodeMode::Primary => write!(f, "Primary"),
            DataNodeMode::Standby => write!(f, "Standby"),
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
    pub http_addr: Option<String>,
    pub flight_addr: Option<String>,
    pub registered_at: i64,
    pub last_heartbeat: i64,
    pub labels: HashMap<String, String>,
    /// Data node mode (only relevant for NodeRole::Data)
    ///
    /// - Primary: Actively serves writes and queries
    /// - Standby: Cold backup, only receives replication (never promoted)
    ///
    /// None for non-Data nodes (Meta, Router, Compactor).
    /// Auto-assigned by Meta based on `primary_standby_ratio` config.
    ///
    /// Note: Do NOT use skip_serializing_if here - it breaks bincode which
    /// expects a fixed structure. JSON handles it fine but bincode doesn't.
    #[serde(default)]
    pub mode: Option<DataNodeMode>,

    // ===== Node-Level Standby Pairing =====
    //
    // Each Primary is paired with a specific Standby at registration time.
    // All data from the Primary replicates to its assigned Standby.
    // This ensures complete data recovery on Primary crash.
    /// For Primary nodes: The Standby node that backs this Primary.
    ///
    /// All data written to this Primary is replicated to this Standby.
    /// On Primary crash, this Standby has a complete copy of the data.
    /// None if no Standby is available yet (queued for pairing).
    #[serde(default)]
    pub backing_standby: Option<String>,

    /// For Standby nodes: List of Primary nodes this Standby backs.
    ///
    /// Based on the primary_standby_ratio (e.g., 2:1 means 2 Primaries per Standby).
    /// This Standby receives replication from all listed Primaries.
    /// Empty for Primary nodes.
    #[serde(default)]
    pub backed_primaries: Vec<String>,
}

impl NodeInfo {
    pub fn new(node_id: String, role: NodeRole, advertise_addr: String) -> Self {
        let now = current_timestamp_ms();
        Self {
            node_id,
            role,
            // Nodes are ready to serve traffic immediately after registration
            state: NodeState::Active,
            advertise_addr,
            http_addr: None,
            flight_addr: None,
            registered_at: now,
            last_heartbeat: now,
            labels: HashMap::new(),
            mode: None,               // Auto-assigned by Meta for Data nodes
            backing_standby: None,    // Assigned when Standby becomes available
            backed_primaries: vec![], // Populated when Standby is assigned Primaries
        }
    }

    pub fn with_http(mut self, addr: String) -> Self {
        self.http_addr = Some(addr);
        self
    }

    pub fn with_flight(mut self, addr: String) -> Self {
        self.flight_addr = Some(addr);
        self
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    /// Set the data node mode (Primary or Standby)
    ///
    /// Only meaningful for Data nodes. Typically auto-assigned by Meta
    /// based on the cluster's `primary_standby_ratio` configuration.
    pub fn with_mode(mut self, mode: DataNodeMode) -> Self {
        self.mode = Some(mode);
        self
    }

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
        self.labels.get(node_labels::ZONE).map(|s| s.as_str())
    }

    /// Get the region label if present
    pub fn region(&self) -> Option<&str> {
        self.labels.get(node_labels::REGION).map(|s| s.as_str())
    }

    /// Get the rack label if present
    pub fn rack(&self) -> Option<&str> {
        self.labels.get(node_labels::RACK).map(|s| s.as_str())
    }

    /// Get the pool label if present
    pub fn pool(&self) -> Option<&str> {
        self.labels.get(node_labels::POOL).map(|s| s.as_str())
    }

    /// Check if this node is in a different zone than another node
    ///
    /// Returns true if both nodes have zone labels and they differ.
    /// Returns false if either node lacks a zone label (can't determine).
    pub fn is_different_zone(&self, other: &NodeInfo) -> bool {
        match (self.zone(), other.zone()) {
            (Some(z1), Some(z2)) => z1 != z2,
            _ => false,
        }
    }

    /// Check if this node is in a different rack than another node
    pub fn is_different_rack(&self, other: &NodeInfo) -> bool {
        match (self.rack(), other.rack()) {
            (Some(r1), Some(r2)) => r1 != r2,
            _ => false,
        }
    }

    /// Check if this node is in a different region than another node
    pub fn is_different_region(&self, other: &NodeInfo) -> bool {
        match (self.region(), other.region()) {
            (Some(r1), Some(r2)) => r1 != r2,
            _ => false,
        }
    }

    /// Check if this node is in any different failure domain than another node
    ///
    /// Checks (in priority order): zone, rack
    /// Returns true if any difference is found.
    pub fn is_different_failure_domain(&self, other: &NodeInfo) -> bool {
        self.is_different_zone(other) || self.is_different_rack(other)
    }
}
