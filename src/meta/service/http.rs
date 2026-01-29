//! Meta Node HTTP API handlers
//!
//! Provides HTTP endpoints for cluster management and scale-in operations.
//!
//! ## Scale-In Endpoints
//!
//! - `POST /nodes/{node_id}/retire` - Start graceful retirement for a node
//! - `GET /nodes/{node_id}/retire/status` - Get retirement progress
//! - `POST /nodes/{node_id}/retire/cancel` - Cancel retirement
//! - `GET /cluster/scale-in/recommend` - Get recommended nodes to scale in
//! - `POST /cluster/scale-in/execute` - Batch retire multiple nodes

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::meta_node::MetaNode;
use crate::meta::types::NodeState;

/// Meta node HTTP state
#[derive(Clone)]
pub struct MetaHttpState {
    pub node_id: u64,
    pub start_time: Instant,
    pub meta_node: Arc<MetaNode>,
}

/// Health response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub node_id: u64,
    pub node_type: &'static str,
    pub is_leader: bool,
}

/// Version response
#[derive(Serialize)]
pub struct VersionResponse {
    pub version: &'static str,
    pub git_hash: &'static str,
}

/// Build the Meta node HTTP API router
pub fn build_router(state: MetaHttpState) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/version", get(version))
        // Node endpoints
        .route("/nodes", get(list_nodes))
        .route("/nodes/:node_id", get(get_node))
        // Replica endpoints
        .route("/replicas", get(list_replicas))
        // File endpoints
        .route("/files/:catalog/:schema/:table", get(list_files))
        // Scale-in endpoints
        .route("/nodes/:node_id/retire", post(retire_node))
        .route("/nodes/:node_id/retire/status", get(retire_status))
        .route("/nodes/:node_id/retire/cancel", post(cancel_retire))
        .route("/cluster/scale-in/recommend", get(recommend_scale_in))
        .route("/cluster/scale-in/execute", post(execute_scale_in))
        .with_state(state)
}

/// Home page
async fn home() -> &'static str {
    "MynaDB Meta Node\n\n\
     Cluster Endpoints:\n\
       GET  /health                      - Health check\n\
       GET  /version                     - Version info\n\
       GET  /nodes                       - List all registered nodes\n\
       GET  /nodes/{node_id}             - Get specific node info\n\
       GET  /replicas                    - List all partition replicas\n\
       GET  /files/{catalog}/{schema}/{table} - List files for a table\n\n\
     Scale-In Endpoints:\n\
       POST /nodes/{node_id}/retire      - Start graceful retirement\n\
       GET  /nodes/{node_id}/retire/status - Get retirement progress\n\
       POST /nodes/{node_id}/retire/cancel - Cancel retirement\n\
       GET  /cluster/scale-in/recommend  - Get recommended nodes\n\
       POST /cluster/scale-in/execute    - Batch retire multiple nodes\n\n\
     Primary protocol: gRPC\n"
}

/// Health check
async fn health(State(state): State<MetaHttpState>) -> impl IntoResponse {
    let is_leader = state.meta_node.is_leader().await;
    let response = HealthResponse {
        status: "healthy",
        node_id: state.node_id,
        node_type: "meta",
        is_leader,
    };
    (StatusCode::OK, Json(response))
}

/// Version info
async fn version() -> impl IntoResponse {
    let response = VersionResponse {
        version: env!("GIT_VERSION"),
        git_hash: env!("GIT_HASH"),
    };
    (StatusCode::OK, Json(response))
}

// ========================================================================
// Node Management Endpoints
// ========================================================================

/// Query params for listing nodes
#[derive(Deserialize, Default)]
pub struct ListNodesQuery {
    /// Filter by role: "meta", "router", "data", "compactor"
    #[serde(default)]
    pub role: Option<String>,
    /// Filter by state: "active", "retiring", "drained", etc.
    #[serde(default)]
    pub state: Option<String>,
    /// Filter by mode: "primary", "standby" (data nodes only)
    #[serde(default)]
    pub mode: Option<String>,
}

/// Node info response (simplified for JSON)
#[derive(Serialize)]
pub struct NodeInfoResponse {
    pub node_id: String,
    pub role: String,
    pub state: String,
    pub mode: Option<String>,
    pub advertise_addr: String,
    pub http_addr: Option<String>,
    pub flight_addr: Option<String>,
    pub zone: Option<String>,
    pub registered_at: i64,
    pub last_heartbeat: i64,
}

impl From<crate::meta::types::NodeInfo> for NodeInfoResponse {
    fn from(info: crate::meta::types::NodeInfo) -> Self {
        let role_str = match info.role {
            crate::meta::types::NodeRole::Meta => "meta",
            crate::meta::types::NodeRole::Router => "router",
            crate::meta::types::NodeRole::Data => "data",
            crate::meta::types::NodeRole::Compactor => "compactor",
            crate::meta::types::NodeRole::Unknown => "unknown",
        };
        let state_str = match info.state {
            NodeState::Active => "active",
            NodeState::Starting => "starting",
            NodeState::Draining => "draining",
            NodeState::Inactive => "inactive",
            NodeState::Retiring => "retiring",
            NodeState::Drained => "drained",
            NodeState::Unknown => "unknown",
        };
        let mode_str = info.mode.map(|m| match m {
            crate::meta::types::DataNodeMode::Primary => "primary".to_string(),
            crate::meta::types::DataNodeMode::Standby => "standby".to_string(),
        });
        // Get zone before moving other fields
        let zone = info.zone().map(String::from);
        NodeInfoResponse {
            node_id: info.node_id,
            role: role_str.to_string(),
            state: state_str.to_string(),
            mode: mode_str,
            advertise_addr: info.advertise_addr,
            http_addr: info.http_addr,
            flight_addr: info.flight_addr,
            zone,
            registered_at: info.registered_at,
            last_heartbeat: info.last_heartbeat,
        }
    }
}

/// List all registered nodes
async fn list_nodes(
    State(state): State<MetaHttpState>,
    Query(query): Query<ListNodesQuery>,
) -> impl IntoResponse {
    use crate::meta::types::{DataNodeMode, NodeRole};

    // Parse role filter
    let role_filter = query.role.as_ref().and_then(|r| match r.to_lowercase().as_str() {
        "meta" => Some(NodeRole::Meta),
        "router" => Some(NodeRole::Router),
        "data" => Some(NodeRole::Data),
        "compactor" => Some(NodeRole::Compactor),
        _ => None,
    });

    // Parse state filter
    let state_filter = query.state.as_ref().and_then(|s| match s.to_lowercase().as_str() {
        "active" => Some(NodeState::Active),
        "starting" => Some(NodeState::Starting),
        "draining" => Some(NodeState::Draining),
        "inactive" => Some(NodeState::Inactive),
        "retiring" => Some(NodeState::Retiring),
        "drained" => Some(NodeState::Drained),
        _ => None,
    });

    // Parse mode filter
    let mode_filter = query.mode.as_ref().and_then(|m| match m.to_lowercase().as_str() {
        "primary" => Some(DataNodeMode::Primary),
        "standby" => Some(DataNodeMode::Standby),
        _ => None,
    });

    let nodes = state.meta_node.list_nodes(role_filter, state_filter).await;

    // Apply mode filter if specified
    let filtered_nodes: Vec<NodeInfoResponse> = nodes
        .into_iter()
        .filter(|n| {
            if let Some(mode) = mode_filter {
                n.mode == Some(mode)
            } else {
                true
            }
        })
        .map(NodeInfoResponse::from)
        .collect();

    (StatusCode::OK, Json(filtered_nodes))
}

/// Get a specific node by ID
async fn get_node(
    State(state): State<MetaHttpState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match state.meta_node.get_node(&node_id).await {
        Some(info) => (StatusCode::OK, Json(Some(NodeInfoResponse::from(info)))),
        None => (StatusCode::NOT_FOUND, Json(None)),
    }
}

// ========================================================================
// Replica Management Endpoints
// ========================================================================

/// Query params for listing replicas
#[derive(Deserialize, Default)]
pub struct ListReplicasQuery {
    /// Filter by leader node ID
    #[serde(default)]
    pub leader: Option<String>,
}

/// Replica info response
#[derive(Serialize)]
pub struct ReplicaInfoResponse {
    pub partition_key: String,
    pub leader_node_id: String,
    pub standby_node_ids: Vec<String>,
    pub replication_factor: u8,
    pub state: String,
    pub last_replicated_seq: u64,
    pub replication_lag_ms: u64,
}

impl From<crate::meta::types::PartitionReplica> for ReplicaInfoResponse {
    fn from(r: crate::meta::types::PartitionReplica) -> Self {
        let state_str = match r.state {
            crate::meta::types::ReplicaState::InSync => "in_sync",
            crate::meta::types::ReplicaState::CatchingUp => "catching_up",
            crate::meta::types::ReplicaState::Offline => "offline",
        };
        ReplicaInfoResponse {
            partition_key: r.partition_key,
            leader_node_id: r.leader_node_id,
            standby_node_ids: r.standby_node_ids,
            replication_factor: r.replication_factor,
            state: state_str.to_string(),
            last_replicated_seq: r.last_replicated_seq,
            replication_lag_ms: r.replication_lag_ms,
        }
    }
}

/// List all partition replicas
async fn list_replicas(
    State(state): State<MetaHttpState>,
    Query(query): Query<ListReplicasQuery>,
) -> impl IntoResponse {
    let replicas = state
        .meta_node
        .list_partition_replicas(query.leader.as_deref())
        .await;

    let responses: Vec<ReplicaInfoResponse> = replicas.into_iter().map(ReplicaInfoResponse::from).collect();

    (StatusCode::OK, Json(responses))
}

// ========================================================================
// File Management Endpoints
// ========================================================================

/// File info response
#[derive(Serialize)]
pub struct FileInfoResponse {
    pub path: String,
    pub partition: String,
    pub level: u8,
    pub size_bytes: u64,
    pub row_count: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub created_by: String,
}

impl From<crate::meta::types::FileInfo> for FileInfoResponse {
    fn from(f: crate::meta::types::FileInfo) -> Self {
        FileInfoResponse {
            path: f.path,
            partition: f.partition,
            level: f.level,
            size_bytes: f.size_bytes,
            row_count: f.row_count,
            min_time: f.min_time,
            max_time: f.max_time,
            created_by: f.created_by,
        }
    }
}

/// List files for a table
async fn list_files(
    State(state): State<MetaHttpState>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let files = state
        .meta_node
        .list_files_for_table(&catalog, &schema, &table)
        .await;

    let responses: Vec<FileInfoResponse> = files.into_iter().map(FileInfoResponse::from).collect();

    (StatusCode::OK, Json(responses))
}

// ========================================================================
// Scale-In (Retirement) Endpoints
// ========================================================================

/// Retire request (optional parameters)
#[derive(Deserialize, Default)]
pub struct RetireRequest {
    /// Force retirement even if memtable has data (will trigger flush)
    #[serde(default)]
    pub force_flush: bool,
}

/// Retire response
#[derive(Serialize)]
pub struct RetireResponse {
    pub success: bool,
    pub status: String,
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_to_reassign: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Retire status response
#[derive(Serialize)]
pub struct RetireStatusResponse {
    pub node_id: String,
    pub state: String,
    pub is_retiring: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_reassigned: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memtable_rows_remaining: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_percent: Option<u32>,
}

/// Start graceful retirement for a node
///
/// This endpoint:
/// 1. Marks the node as Retiring in Meta
/// 2. Router stops sending new writes to this node
/// 3. Partitions get reassigned to other nodes
/// 4. Once drained, node can be safely stopped
async fn retire_node(
    State(state): State<MetaHttpState>,
    Path(node_id): Path<String>,
    Json(_request): Json<RetireRequest>,
) -> impl IntoResponse {
    match state.meta_node.start_retire(&node_id).await {
        Ok(partitions_to_reassign) => (
            StatusCode::OK,
            Json(RetireResponse {
                success: true,
                status: "retiring".to_string(),
                node_id,
                partitions_to_reassign: Some(partitions_to_reassign),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RetireResponse {
                success: false,
                status: "error".to_string(),
                node_id,
                partitions_to_reassign: None,
                error: Some(e),
            }),
        ),
    }
}

/// Get retirement status for a node
async fn retire_status(
    State(state): State<MetaHttpState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match state.meta_node.get_retire_status(&node_id).await {
        Ok(status) => {
            let is_retiring =
                status.state == NodeState::Retiring || status.state == NodeState::Drained;

            let state_str = match status.state {
                NodeState::Retiring => "retiring",
                NodeState::Drained => "drained",
                NodeState::Active => "active",
                _ => "unknown",
            };

            // Compute progress before moving status.node_id
            let progress = if is_retiring {
                Some(status.progress_percent() as u32)
            } else {
                None
            };

            (
                StatusCode::OK,
                Json(RetireStatusResponse {
                    node_id: status.node_id,
                    state: state_str.to_string(),
                    is_retiring,
                    started_at: if status.started_at > 0 {
                        Some(status.started_at)
                    } else {
                        None
                    },
                    partitions_total: if is_retiring {
                        Some(status.partitions_total)
                    } else {
                        None
                    },
                    partitions_reassigned: if is_retiring {
                        Some(status.partitions_reassigned)
                    } else {
                        None
                    },
                    memtable_rows_remaining: if is_retiring {
                        Some(status.memtable_rows_remaining)
                    } else {
                        None
                    },
                    progress_percent: progress,
                }),
            )
        }
        Err(_e) => (
            StatusCode::NOT_FOUND,
            Json(RetireStatusResponse {
                node_id,
                state: "unknown".to_string(),
                is_retiring: false,
                started_at: None,
                partitions_total: None,
                partitions_reassigned: None,
                memtable_rows_remaining: None,
                progress_percent: None,
            }),
        ),
    }
}

/// Cancel retirement for a node
async fn cancel_retire(
    State(state): State<MetaHttpState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match state.meta_node.cancel_retire(&node_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(RetireResponse {
                success: true,
                status: "active".to_string(),
                node_id,
                partitions_to_reassign: None,
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RetireResponse {
                success: false,
                status: "error".to_string(),
                node_id,
                partitions_to_reassign: None,
                error: Some(e),
            }),
        ),
    }
}

/// Scale-in recommendation query params
#[derive(Deserialize, Default)]
pub struct RecommendQuery {
    /// Number of primaries to recommend for scale-in
    #[serde(default)]
    pub primaries: u32,
    /// Number of standbys to recommend for scale-in
    #[serde(default)]
    pub standbys: u32,
    /// Total count if not specifying primaries/standbys separately
    #[serde(default)]
    pub count: u32,
}

/// Scale-in recommendation response
#[derive(Serialize)]
pub struct RecommendResponse {
    pub recommended_primaries: Vec<String>,
    pub recommended_standbys: Vec<String>,
    pub reasons: HashMap<String, String>,
    pub warnings: Vec<String>,
}

/// Get recommended nodes to scale in
///
/// Returns nodes sorted by preference (least loaded first for primaries,
/// most redundant first for standbys).
async fn recommend_scale_in(
    State(state): State<MetaHttpState>,
    Query(query): Query<RecommendQuery>,
) -> impl IntoResponse {
    // Default to count if primaries/standbys not specified
    let primaries = if query.primaries > 0 {
        query.primaries
    } else {
        query.count
    };
    let standbys = query.standbys;

    match state
        .meta_node
        .recommend_scale_in(primaries as usize, standbys as usize)
        .await
    {
        Ok(recommendation) => (
            StatusCode::OK,
            Json(RecommendResponse {
                recommended_primaries: recommendation.recommended_primaries,
                recommended_standbys: recommendation.recommended_standbys,
                reasons: recommendation.reasons,
                warnings: recommendation.warnings,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RecommendResponse {
                recommended_primaries: vec![],
                recommended_standbys: vec![],
                reasons: HashMap::new(),
                warnings: vec![format!("Error: {}", e)],
            }),
        ),
    }
}

/// Batch scale-in execute request
#[derive(Deserialize)]
pub struct ExecuteScaleInRequest {
    /// List of node IDs to retire
    pub nodes: Vec<String>,
    /// Force flush on all nodes
    #[serde(default)]
    pub force_flush: bool,
}

/// Batch scale-in execute response
#[derive(Serialize)]
pub struct ExecuteScaleInResponse {
    pub success: bool,
    pub nodes_retired: Vec<String>,
    pub nodes_failed: Vec<String>,
    pub errors: HashMap<String, String>,
}

/// Batch retire multiple nodes
///
/// Retires all specified nodes in sequence. Each node goes through
/// the full retirement flow: Retiring → reassign partitions → Drained.
async fn execute_scale_in(
    State(state): State<MetaHttpState>,
    Json(request): Json<ExecuteScaleInRequest>,
) -> impl IntoResponse {
    let mut nodes_retired = Vec::new();
    let mut nodes_failed = Vec::new();
    let mut errors = HashMap::new();

    for node_id in &request.nodes {
        match state.meta_node.start_retire(node_id).await {
            Ok(_) => {
                nodes_retired.push(node_id.clone());
                tracing::info!("Started retirement for node {}", node_id);
            }
            Err(e) => {
                nodes_failed.push(node_id.clone());
                errors.insert(node_id.clone(), e.clone());
                tracing::warn!("Failed to retire node {}: {}", node_id, e);
            }
        }
    }

    let success = nodes_failed.is_empty();

    (
        if success {
            StatusCode::OK
        } else {
            StatusCode::MULTI_STATUS
        },
        Json(ExecuteScaleInResponse {
            success,
            nodes_retired,
            nodes_failed,
            errors,
        }),
    )
}

/// Start HTTP server for Meta node
pub async fn start_http_server(
    host: &str,
    port: u16,
    state: MetaHttpState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = build_router(state);
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Meta node HTTP server started on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}
