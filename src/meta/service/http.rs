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
     Endpoints:\n\
       GET  /health                      - Health check\n\
       GET  /version                     - Version info\n\n\
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
