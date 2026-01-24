//! Cluster status API endpoints

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::cluster::node::{NodeInfo, NodeState, HealthStatus};
use crate::cluster::election::LeaderState;

/// Cluster status response
#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    /// Whether cluster mode is enabled
    pub enabled: bool,
    /// This node's ID
    pub node_id: String,
    /// This node's role
    pub role: String,
    /// This node's state
    pub state: String,
    /// Whether this node is the leader
    pub is_leader: bool,
    /// Current leader's node ID (if known)
    pub leader_id: Option<String>,
    /// Total number of nodes
    pub total_nodes: usize,
    /// Number of healthy nodes
    pub healthy_nodes: usize,
    /// Coordinator type
    pub coordinator_type: String,
    /// Coordinator connected
    pub coordinator_connected: bool,
}

/// Node list response
#[derive(Debug, Serialize)]
pub struct NodesResponse {
    /// List of nodes
    pub nodes: Vec<NodeInfoResponse>,
}

/// Node info in response
#[derive(Debug, Serialize)]
pub struct NodeInfoResponse {
    /// Node ID
    pub node_id: String,
    /// Advertise address
    pub advertise_addr: String,
    /// Role
    pub role: String,
    /// State
    pub state: String,
    /// Is this the leader
    pub is_leader: bool,
    /// HTTP port
    pub http_port: u16,
    /// Flight port
    pub flight_port: u16,
    /// MySQL port
    pub mysql_port: Option<u16>,
    /// Last heartbeat timestamp
    pub last_heartbeat: String,
    /// Version
    pub version: String,
    /// Hostname
    pub hostname: String,
}

impl From<NodeInfo> for NodeInfoResponse {
    fn from(info: NodeInfo) -> Self {
        Self {
            node_id: info.node_id,
            advertise_addr: info.advertise_addr,
            role: info.role.to_string(),
            state: info.state.to_string(),
            is_leader: info.is_leader,
            http_port: info.http_port,
            flight_port: info.flight_port,
            mysql_port: info.mysql_port,
            last_heartbeat: info.last_heartbeat.to_rfc3339(),
            version: info.version,
            hostname: info.hostname,
        }
    }
}

/// Leader response
#[derive(Debug, Serialize)]
pub struct LeaderResponse {
    /// Current leader's node ID
    pub leader_id: Option<String>,
    /// Whether this node is the leader
    pub is_this_node: bool,
    /// Leader election state
    pub election_state: String,
}

/// Health response
#[derive(Debug, Serialize)]
pub struct ClusterHealthResponse {
    /// Overall health status
    pub status: String,
    /// Individual node health
    pub nodes: Vec<NodeHealthResponse>,
    /// Timestamp
    pub timestamp: String,
}

/// Node health in response
#[derive(Debug, Serialize)]
pub struct NodeHealthResponse {
    /// Node ID
    pub node_id: String,
    /// Health status
    pub status: String,
    /// State
    pub state: String,
    /// Last seen
    pub last_seen: String,
}

/// Cluster state for API handlers
#[derive(Clone)]
pub struct ClusterState {
    /// Whether cluster mode is enabled
    pub enabled: bool,
    /// This node's info
    pub node_info: Option<Arc<parking_lot::RwLock<NodeInfo>>>,
    /// Node registration (for listing nodes)
    pub registration: Option<Arc<crate::cluster::node::NodeRegistration>>,
    /// Leader election
    pub election: Option<Arc<crate::cluster::election::LeaderElection>>,
    /// Health checker
    pub health_checker: Option<Arc<crate::cluster::node::HealthChecker>>,
    /// Coordinator
    pub coordinator: Option<Arc<dyn crate::cluster::coordinator::Coordinator>>,
}

impl ClusterState {
    /// Create a disabled cluster state
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            node_info: None,
            registration: None,
            election: None,
            health_checker: None,
            coordinator: None,
        }
    }
}

/// GET /cluster/status - Get cluster status
pub async fn cluster_status_handler(
    Extension(cluster): Extension<ClusterState>,
) -> Result<Json<ClusterStatusResponse>, (StatusCode, String)> {
    if !cluster.enabled {
        return Ok(Json(ClusterStatusResponse {
            enabled: false,
            node_id: "standalone".to_string(),
            role: "hybrid".to_string(),
            state: "active".to_string(),
            is_leader: true,
            leader_id: None,
            total_nodes: 1,
            healthy_nodes: 1,
            coordinator_type: "none".to_string(),
            coordinator_connected: false,
        }));
    }

    let node_info = cluster.node_info
        .as_ref()
        .map(|n| n.read().clone())
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "Node info not available".to_string()))?;

    let is_leader = cluster.election
        .as_ref()
        .map(|e| e.is_leader())
        .unwrap_or(true);

    let leader_id = if let Some(ref election) = cluster.election {
        election.current_leader().await.ok().flatten()
    } else {
        None
    };

    let (total_nodes, healthy_nodes) = if let Some(ref registration) = cluster.registration {
        let nodes = registration.list_nodes().await.unwrap_or_default();
        let healthy = nodes.iter().filter(|n| n.state == NodeState::Active).count();
        (nodes.len(), healthy)
    } else {
        (1, 1)
    };

    let coordinator_type = cluster.coordinator
        .as_ref()
        .map(|c| c.coordinator_type().to_string())
        .unwrap_or_else(|| "none".to_string());

    let coordinator_connected = if let Some(ref coordinator) = cluster.coordinator {
        coordinator.is_connected().await
    } else {
        false
    };

    Ok(Json(ClusterStatusResponse {
        enabled: true,
        node_id: node_info.node_id,
        role: node_info.role.to_string(),
        state: node_info.state.to_string(),
        is_leader,
        leader_id,
        total_nodes,
        healthy_nodes,
        coordinator_type,
        coordinator_connected,
    }))
}

/// GET /cluster/nodes - List all nodes
pub async fn cluster_nodes_handler(
    Extension(cluster): Extension<ClusterState>,
) -> Result<Json<NodesResponse>, (StatusCode, String)> {
    if !cluster.enabled {
        return Ok(Json(NodesResponse { nodes: vec![] }));
    }

    let registration = cluster.registration
        .as_ref()
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "Registration not available".to_string()))?;

    let nodes = registration.list_nodes().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let node_responses: Vec<NodeInfoResponse> = nodes.into_iter().map(Into::into).collect();

    Ok(Json(NodesResponse { nodes: node_responses }))
}

/// GET /cluster/leader - Get current leader
pub async fn cluster_leader_handler(
    Extension(cluster): Extension<ClusterState>,
) -> Result<Json<LeaderResponse>, (StatusCode, String)> {
    if !cluster.enabled {
        return Ok(Json(LeaderResponse {
            leader_id: None,
            is_this_node: true,
            election_state: "not_started".to_string(),
        }));
    }

    let election = cluster.election
        .as_ref()
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "Election not available".to_string()))?;

    let leader_id = election.current_leader().await.ok().flatten();
    let is_this_node = election.is_leader();
    let election_state = election.state().to_string();

    Ok(Json(LeaderResponse {
        leader_id,
        is_this_node,
        election_state,
    }))
}

/// GET /cluster/health - Get cluster health
pub async fn cluster_health_handler(
    Extension(cluster): Extension<ClusterState>,
) -> Result<Json<ClusterHealthResponse>, (StatusCode, String)> {
    if !cluster.enabled {
        return Ok(Json(ClusterHealthResponse {
            status: "healthy".to_string(),
            nodes: vec![],
            timestamp: chrono::Utc::now().to_rfc3339(),
        }));
    }

    let registration = cluster.registration
        .as_ref()
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "Registration not available".to_string()))?;

    let health_checker = cluster.health_checker.as_ref();

    let nodes = registration.list_nodes().await.unwrap_or_default();

    let node_health: Vec<NodeHealthResponse> = nodes.iter().map(|node| {
        let status = health_checker
            .and_then(|hc| hc.get_node_health(&node.node_id))
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                if node.state == NodeState::Active { "healthy" } else { "unknown" }.to_string()
            });

        NodeHealthResponse {
            node_id: node.node_id.clone(),
            status,
            state: node.state.to_string(),
            last_seen: node.last_heartbeat.to_rfc3339(),
        }
    }).collect();

    let overall_status = if node_health.iter().all(|n| n.status == "healthy") {
        "healthy"
    } else if node_health.iter().any(|n| n.status == "unhealthy") {
        "unhealthy"
    } else {
        "degraded"
    };

    Ok(Json(ClusterHealthResponse {
        status: overall_status.to_string(),
        nodes: node_health,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }))
}
