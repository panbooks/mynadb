//! Node identity management
//!
//! Defines node identity, roles, and states.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Unique node identifier
pub type NodeId = String;

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Compute-only node (handles reads and writes)
    Compute,
    /// Compactor-only node (handles background compaction)
    Compactor,
    /// Hybrid node (handles both)
    Hybrid,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Hybrid
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Compute => write!(f, "compute"),
            NodeRole::Compactor => write!(f, "compactor"),
            NodeRole::Hybrid => write!(f, "hybrid"),
        }
    }
}

impl std::str::FromStr for NodeRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "compute" => Ok(NodeRole::Compute),
            "compactor" => Ok(NodeRole::Compactor),
            "hybrid" => Ok(NodeRole::Hybrid),
            other => Err(format!("Unknown role: {}", other)),
        }
    }
}

/// Node state in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    /// Node is starting up
    Starting,
    /// Node is active and ready
    Active,
    /// Node is draining (preparing to leave)
    Draining,
    /// Node has left the cluster
    Left,
    /// Node is unhealthy
    Unhealthy,
}

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Starting
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Starting => write!(f, "starting"),
            NodeState::Active => write!(f, "active"),
            NodeState::Draining => write!(f, "draining"),
            NodeState::Left => write!(f, "left"),
            NodeState::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Information about a node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier
    pub node_id: NodeId,

    /// Address advertised to other nodes
    pub advertise_addr: String,

    /// Node role
    pub role: NodeRole,

    /// Current state
    pub state: NodeState,

    /// When the node started
    pub started_at: DateTime<Utc>,

    /// When the node last reported health
    pub last_heartbeat: DateTime<Utc>,

    /// Version information
    pub version: String,

    /// Hostname
    pub hostname: String,

    /// HTTP port
    pub http_port: u16,

    /// Flight port
    pub flight_port: u16,

    /// MySQL port (if enabled)
    pub mysql_port: Option<u16>,

    /// Whether this node is the compaction leader
    pub is_leader: bool,

    /// Additional metadata
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

impl NodeInfo {
    /// Create a new NodeInfo
    pub fn new(
        node_id: NodeId,
        advertise_addr: String,
        role: NodeRole,
        http_port: u16,
        flight_port: u16,
    ) -> Self {
        let now = Utc::now();
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        Self {
            node_id,
            advertise_addr,
            role,
            state: NodeState::Starting,
            started_at: now,
            last_heartbeat: now,
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname,
            http_port,
            flight_port,
            mysql_port: None,
            is_leader: false,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Update the heartbeat timestamp
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
    }

    /// Set the node state
    pub fn set_state(&mut self, state: NodeState) {
        self.state = state;
        self.heartbeat();
    }

    /// Set leader status
    pub fn set_leader(&mut self, is_leader: bool) {
        self.is_leader = is_leader;
        self.heartbeat();
    }

    /// Get the HTTP address
    pub fn http_addr(&self) -> String {
        format!(
            "{}:{}",
            self.advertise_addr.split(':').next().unwrap_or("0.0.0.0"),
            self.http_port
        )
    }

    /// Get the Flight address
    pub fn flight_addr(&self) -> String {
        format!(
            "{}:{}",
            self.advertise_addr.split(':').next().unwrap_or("0.0.0.0"),
            self.flight_port
        )
    }

    /// Get the etcd key for this node
    pub fn etcd_key(&self) -> String {
        format!("/mynadb/nodes/{}", self.node_id)
    }

    /// Serialize to JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|e| format!("Failed to serialize NodeInfo: {}", e))
    }

    /// Deserialize from JSON bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("Failed to deserialize NodeInfo: {}", e))
    }
}

/// Generate a unique node ID
pub fn generate_node_id() -> NodeId {
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let uuid = uuid::Uuid::new_v4();
    format!("{}-{}", hostname, &uuid.to_string()[..8])
}

/// Parse a socket address from a string, handling IPv4/IPv6
pub fn parse_advertise_addr(addr: &str, default_port: u16) -> Result<SocketAddr, String> {
    // Try to parse as-is first
    if let Ok(addr) = addr.parse::<SocketAddr>() {
        return Ok(addr);
    }

    // Try adding the default port
    let with_port = format!("{}:{}", addr, default_port);
    with_port
        .parse::<SocketAddr>()
        .map_err(|e| format!("Invalid address '{}': {}", addr, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_role_parsing() {
        assert_eq!("compute".parse::<NodeRole>().unwrap(), NodeRole::Compute);
        assert_eq!(
            "compactor".parse::<NodeRole>().unwrap(),
            NodeRole::Compactor
        );
        assert_eq!("hybrid".parse::<NodeRole>().unwrap(), NodeRole::Hybrid);
        assert_eq!("HYBRID".parse::<NodeRole>().unwrap(), NodeRole::Hybrid);
    }

    #[test]
    fn test_node_info_serialization() {
        let info = NodeInfo::new(
            "test-node".to_string(),
            "192.168.1.1:7000".to_string(),
            NodeRole::Hybrid,
            7000,
            7001,
        );

        let bytes = info.to_bytes().unwrap();
        let restored = NodeInfo::from_bytes(&bytes).unwrap();

        assert_eq!(info.node_id, restored.node_id);
        assert_eq!(info.role, restored.role);
    }

    #[test]
    fn test_generate_node_id() {
        let id1 = generate_node_id();
        let id2 = generate_node_id();
        assert_ne!(id1, id2);
        assert!(id1.contains('-'));
    }
}
