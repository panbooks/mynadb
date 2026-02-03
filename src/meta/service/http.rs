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
use crate::meta::scale_in::{
    ScaleInConfig, ScaleInManager, ScaleInPlan, ScaleInPlanRequest, ScaleInResult, ScaleInStrategy,
};
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
    use axum::routing::delete;

    Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/version", get(version))
        // Node endpoints
        .route("/nodes", get(list_nodes))
        .route("/nodes/:node_id", get(get_node))
        // Scale-in endpoints
        .route("/nodes/:node_id/retire", post(retire_node))
        .route("/nodes/:node_id/retire/status", get(retire_status))
        .route("/nodes/:node_id/retire/cancel", post(cancel_retire))
        .route("/cluster/scale-in/recommend", get(recommend_scale_in))
        .route("/cluster/scale-in/execute", post(execute_scale_in))
        // Enhanced scale-in endpoints with Golden Rule enforcement
        .route("/cluster/scale-in/plan", post(plan_scale_in))
        .route("/cluster/scale-in/execute-plan", post(execute_scale_in_plan))
        // Distribution config endpoints
        .route("/distribution", get(list_distribution_configs))
        .route("/distribution/config", get(get_distribution_config))
        .route("/distribution/config", post(set_distribution_config))
        .route("/distribution/config", delete(delete_distribution_config))
        .with_state(state)
}

/// Home page
async fn home() -> &'static str {
    "MynaDB Meta Node\n\n\
     Cluster Endpoints:\n\
       GET  /health                      - Health check\n\
       GET  /version                     - Version info\n\
       GET  /nodes                       - List all registered nodes\n\
       GET  /nodes/{node_id}             - Get specific node info\n\n\
     Scale-In Endpoints:\n\
       POST /nodes/{node_id}/retire      - Start graceful retirement\n\
       GET  /nodes/{node_id}/retire/status - Get retirement progress\n\
       POST /nodes/{node_id}/retire/cancel - Cancel retirement\n\
       GET  /cluster/scale-in/recommend  - Get recommended nodes\n\
       POST /cluster/scale-in/execute    - Batch retire multiple nodes\n\
       POST /cluster/scale-in/plan       - Generate scale-in plan (supports dry_run)\n\
       POST /cluster/scale-in/execute-plan - Execute with Golden Rule enforcement\n\n\
     Distribution Config Endpoints:\n\
       GET  /distribution                - List all distribution configs\n\
       GET  /distribution/config         - Get distribution config (supports effective=true)\n\
       POST /distribution/config         - Set distribution config\n\
       DELETE /distribution/config       - Delete distribution config\n\n\
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
    let role_filter = query
        .role
        .as_ref()
        .and_then(|r| match r.to_lowercase().as_str() {
            "meta" => Some(NodeRole::Meta),
            "router" => Some(NodeRole::Router),
            "data" => Some(NodeRole::Data),
            "compactor" => Some(NodeRole::Compactor),
            _ => None,
        });

    // Parse state filter
    let state_filter = query
        .state
        .as_ref()
        .and_then(|s| match s.to_lowercase().as_str() {
            "active" => Some(NodeState::Active),
            "starting" => Some(NodeState::Starting),
            "draining" => Some(NodeState::Draining),
            "inactive" => Some(NodeState::Inactive),
            "retiring" => Some(NodeState::Retiring),
            "drained" => Some(NodeState::Drained),
            _ => None,
        });

    // Parse mode filter
    let mode_filter = query
        .mode
        .as_ref()
        .and_then(|m| match m.to_lowercase().as_str() {
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

// ========================================================================
// Enhanced Scale-In API (with Golden Rule enforcement)
// ========================================================================

/// Request to generate a scale-in plan
#[derive(Deserialize)]
pub struct PlanScaleInRequest {
    /// Total number of nodes to remove (simplest option, Ratio-Preserving)
    ///
    /// Auto-calculates primaries/standbys based on ratio.
    /// Constraint: actual_ratio <= config_ratio (each Standby backs AT MOST `ratio` Primaries)
    ///
    /// Priority: reduce_primaries/reduce_standbys > reduce_num
    #[serde(default)]
    pub reduce_num: usize,

    /// Number of Primary nodes to remove (takes priority over reduce_num)
    #[serde(default)]
    pub reduce_primaries: usize,

    /// Number of Standby nodes to remove (takes priority over reduce_num)
    #[serde(default)]
    pub reduce_standbys: usize,

    /// Strategy: "least_loaded" (default), "first_in", "last_in", "ha_unit"
    #[serde(default)]
    pub strategy: Option<String>,

    /// Whether this is a dry-run (no changes, just preview)
    #[serde(default)]
    pub dry_run: bool,

    /// Force flush memtables before retirement
    #[serde(default = "default_true")]
    pub force_flush: bool,
}

fn default_true() -> bool {
    true
}

/// Scale-in plan response
#[derive(Serialize)]
pub struct PlanScaleInResponse {
    pub success: bool,
    pub plan_id: String,
    pub dry_run: bool,
    pub phases: Vec<PhaseInfo>,
    pub primaries_to_retire: Vec<String>,
    pub standbys_to_retire: Vec<String>,
    pub tables_to_migrate: usize,
    pub warnings: Vec<String>,
    pub final_state: FinalStateInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct PhaseInfo {
    pub phase_number: u8,
    pub phase_type: String,
    pub description: String,
    pub affected_nodes: Vec<String>,
    pub operations_count: usize,
}

#[derive(Serialize)]
pub struct FinalStateInfo {
    pub primary_count: usize,
    pub standby_count: usize,
    pub ratio_maintained: bool,
    pub total_tables: usize,
}

/// Generate a detailed scale-in plan
///
/// This endpoint generates a plan that follows the Golden Rule:
/// 1. Never retire a Standby while it backs up active Primaries
/// 2. Never retire a Primary while it owns partitions
///
/// Use `dry_run=true` to preview without executing.
async fn plan_scale_in(
    State(state): State<MetaHttpState>,
    Json(request): Json<PlanScaleInRequest>,
) -> impl IntoResponse {
    // Parse strategy
    let strategy = match request.strategy.as_deref() {
        Some("least_loaded") | None => ScaleInStrategy::LeastLoaded,
        Some("first_in") => ScaleInStrategy::FirstIn,
        Some("last_in") => ScaleInStrategy::LastIn,
        Some("ha_unit") => ScaleInStrategy::HAUnit,
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PlanScaleInResponse {
                    success: false,
                    plan_id: String::new(),
                    dry_run: request.dry_run,
                    phases: vec![],
                    primaries_to_retire: vec![],
                    standbys_to_retire: vec![],
                    tables_to_migrate: 0,
                    warnings: vec![],
                    final_state: FinalStateInfo {
                        primary_count: 0,
                        standby_count: 0,
                        ratio_maintained: false,
                        total_tables: 0,
                    },
                    error: Some(format!("Invalid strategy: {}. Valid options: least_loaded, first_in, last_in, ha_unit", other)),
                }),
            );
        }
    };

    // Create scale-in manager
    let config = ScaleInConfig {
        force_flush: request.force_flush,
        strategy,
        ..ScaleInConfig::default()
    };
    let manager = ScaleInManager::new(state.meta_node.clone(), config);

    // Create plan request
    let plan_request = ScaleInPlanRequest {
        reduce_num: request.reduce_num,
        reduce_primaries: request.reduce_primaries,
        reduce_standbys: request.reduce_standbys,
        strategy,
        dry_run: request.dry_run,
        force_flush: request.force_flush,
    };

    // Generate plan
    match manager.plan(plan_request).await {
        Ok(plan) => {
            let phases: Vec<PhaseInfo> = plan
                .phases
                .iter()
                .map(|p| PhaseInfo {
                    phase_number: p.phase_number,
                    phase_type: p.phase_type.to_string(),
                    description: p.description.clone(),
                    affected_nodes: p.affected_nodes.clone(),
                    operations_count: p.operations.len(),
                })
                .collect();

            (
                StatusCode::OK,
                Json(PlanScaleInResponse {
                    success: true,
                    plan_id: plan.plan_id,
                    dry_run: plan.dry_run,
                    phases,
                    primaries_to_retire: plan.primaries_to_retire,
                    standbys_to_retire: plan.standbys_to_retire,
                    tables_to_migrate: plan.migrations.len(),
                    warnings: plan.warnings,
                    final_state: FinalStateInfo {
                        primary_count: plan.final_state.primary_count,
                        standby_count: plan.final_state.standby_count,
                        ratio_maintained: plan.final_state.ratio_maintained,
                        total_tables: plan.final_state.total_tables,
                    },
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(PlanScaleInResponse {
                success: false,
                plan_id: String::new(),
                dry_run: request.dry_run,
                phases: vec![],
                primaries_to_retire: vec![],
                standbys_to_retire: vec![],
                tables_to_migrate: 0,
                warnings: vec![],
                final_state: FinalStateInfo {
                    primary_count: 0,
                    standby_count: 0,
                    ratio_maintained: false,
                    total_tables: 0,
                },
                error: Some(e.to_string()),
            }),
        ),
    }
}

/// Request to execute a scale-in plan
#[derive(Deserialize)]
pub struct ExecutePlanRequest {
    /// Total number of nodes to remove (simplest option)
    #[serde(default)]
    pub reduce_num: usize,

    /// Number of Primary nodes to remove (takes priority over reduce_num)
    #[serde(default)]
    pub reduce_primaries: usize,

    /// Number of Standby nodes to remove (takes priority over reduce_num)
    #[serde(default)]
    pub reduce_standbys: usize,

    /// Strategy: "least_loaded" (default), "first_in", "last_in", "ha_unit"
    #[serde(default)]
    pub strategy: Option<String>,

    /// Force flush memtables before retirement
    #[serde(default = "default_true")]
    pub force_flush: bool,
}

/// Scale-in execution response
#[derive(Serialize)]
pub struct ExecutePlanResponse {
    pub success: bool,
    pub plan_id: String,
    pub primaries_retired: Vec<String>,
    pub standbys_retired: Vec<String>,
    pub tables_migrated: usize,
    pub duration_ms: u64,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

/// Execute a scale-in plan with Golden Rule enforcement
///
/// This endpoint:
/// 1. Generates a validated plan following the Golden Rule
/// 2. Executes the plan in three phases:
///    - Phase 1: Migrate partitions from target Primaries
///    - Phase 2: Retire empty Primaries (Standbys still protect cluster)
///    - Phase 3: Retire orphaned Standbys
async fn execute_scale_in_plan(
    State(state): State<MetaHttpState>,
    Json(request): Json<ExecutePlanRequest>,
) -> impl IntoResponse {
    // Parse strategy
    let strategy = match request.strategy.as_deref() {
        Some("least_loaded") | None => ScaleInStrategy::LeastLoaded,
        Some("first_in") => ScaleInStrategy::FirstIn,
        Some("last_in") => ScaleInStrategy::LastIn,
        Some("ha_unit") => ScaleInStrategy::HAUnit,
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ExecutePlanResponse {
                    success: false,
                    plan_id: String::new(),
                    primaries_retired: vec![],
                    standbys_retired: vec![],
                    tables_migrated: 0,
                    duration_ms: 0,
                    errors: vec![format!("Invalid strategy: {}. Valid options: least_loaded, first_in, last_in, ha_unit", other)],
                    warnings: vec![],
                }),
            );
        }
    };

    // Create scale-in manager
    let config = ScaleInConfig {
        force_flush: request.force_flush,
        strategy,
        ..ScaleInConfig::default()
    };
    let manager = ScaleInManager::new(state.meta_node.clone(), config);

    // Create plan request
    let plan_request = ScaleInPlanRequest {
        reduce_num: request.reduce_num,
        reduce_primaries: request.reduce_primaries,
        reduce_standbys: request.reduce_standbys,
        strategy,
        dry_run: false,
        force_flush: request.force_flush,
    };

    // Execute scale-in (plan + execute)
    match manager.scale_in(plan_request).await {
        Ok(result) => (
            if result.success {
                StatusCode::OK
            } else {
                StatusCode::MULTI_STATUS
            },
            Json(ExecutePlanResponse {
                success: result.success,
                plan_id: result.plan_id,
                primaries_retired: result.primaries_retired,
                standbys_retired: result.standbys_retired,
                tables_migrated: result.tables_migrated,
                duration_ms: result.duration_ms,
                errors: result.errors,
                warnings: result.warnings,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ExecutePlanResponse {
                success: false,
                plan_id: String::new(),
                primaries_retired: vec![],
                standbys_retired: vec![],
                tables_migrated: 0,
                duration_ms: 0,
                errors: vec![e.to_string()],
                warnings: vec![],
            }),
        ),
    }
}

// ========================================================================
// Distribution Config Endpoints
// ========================================================================

/// Distribution config request
#[derive(Deserialize)]
pub struct SetDistributionConfigRequest {
    /// Catalog name (required)
    pub catalog: String,
    /// Schema name (optional - empty for catalog-level)
    #[serde(default)]
    pub schema: String,
    /// Table name (optional - empty for schema/catalog-level)
    #[serde(default)]
    pub table: String,
    /// Distribution percentage [0-100]
    /// 0 = single node (default), 100 = all nodes
    pub percent: u8,
}

/// Distribution config response
#[derive(Serialize)]
pub struct DistributionConfigResponse {
    pub success: bool,
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub scope: String,
    pub percent: u8,
    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Distribution config info
#[derive(Serialize)]
pub struct DistributionConfigInfo {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub scope: String,
    pub percent: u8,
    pub created_at: i64,
    pub last_modified: i64,
    pub version: u64,
}

impl From<crate::meta::types::DistributionConfig> for DistributionConfigInfo {
    fn from(config: crate::meta::types::DistributionConfig) -> Self {
        let scope_str = match config.scope {
            crate::meta::types::DistributionScope::Catalog => "catalog",
            crate::meta::types::DistributionScope::Schema => "schema",
            crate::meta::types::DistributionScope::Table => "table",
        };
        DistributionConfigInfo {
            catalog: config.catalog,
            schema: config.schema,
            table: config.table,
            scope: scope_str.to_string(),
            percent: config.percent,
            created_at: config.created_at,
            last_modified: config.last_modified,
            version: config.version,
        }
    }
}

/// Query params for distribution config
#[derive(Deserialize, Default)]
pub struct DistributionConfigQuery {
    /// Catalog name (required for exact lookup)
    #[serde(default)]
    pub catalog: String,
    /// Schema name (optional)
    #[serde(default)]
    pub schema: String,
    /// Table name (optional)
    #[serde(default)]
    pub table: String,
    /// Use hierarchical lookup (effective config)
    #[serde(default)]
    pub effective: bool,
}

/// Set distribution config
async fn set_distribution_config(
    State(state): State<MetaHttpState>,
    Json(request): Json<SetDistributionConfigRequest>,
) -> impl IntoResponse {
    // Validate percent
    if request.percent > 100 {
        return (
            StatusCode::BAD_REQUEST,
            Json(DistributionConfigResponse {
                success: false,
                catalog: request.catalog,
                schema: request.schema,
                table: request.table,
                scope: String::new(),
                percent: request.percent,
                version: 0,
                error: Some("percent must be between 0 and 100".to_string()),
            }),
        );
    }

    // Determine scope
    let scope_str = if !request.table.is_empty() {
        "table"
    } else if !request.schema.is_empty() {
        "schema"
    } else {
        "catalog"
    };

    match state
        .meta_node
        .set_distribution_config(&request.catalog, &request.schema, &request.table, request.percent)
        .await
    {
        Ok(response) => match response {
            crate::meta::types::MetaResponse::DistributionConfigSet { version, .. } => (
                StatusCode::OK,
                Json(DistributionConfigResponse {
                    success: true,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: request.percent,
                    version,
                    error: None,
                }),
            ),
            crate::meta::types::MetaResponse::Error { message } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DistributionConfigResponse {
                    success: false,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: request.percent,
                    version: 0,
                    error: Some(message),
                }),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DistributionConfigResponse {
                    success: false,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: request.percent,
                    version: 0,
                    error: Some("Unexpected response".to_string()),
                }),
            ),
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DistributionConfigResponse {
                success: false,
                catalog: request.catalog,
                schema: request.schema,
                table: request.table,
                scope: scope_str.to_string(),
                percent: request.percent,
                version: 0,
                error: Some(e),
            }),
        ),
    }
}

/// Delete distribution config
async fn delete_distribution_config(
    State(state): State<MetaHttpState>,
    Json(request): Json<SetDistributionConfigRequest>,
) -> impl IntoResponse {
    let scope_str = if !request.table.is_empty() {
        "table"
    } else if !request.schema.is_empty() {
        "schema"
    } else {
        "catalog"
    };

    match state
        .meta_node
        .delete_distribution_config(&request.catalog, &request.schema, &request.table)
        .await
    {
        Ok(response) => match response {
            crate::meta::types::MetaResponse::DistributionConfigDeleted { .. } => (
                StatusCode::OK,
                Json(DistributionConfigResponse {
                    success: true,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: 0,
                    version: 0,
                    error: None,
                }),
            ),
            crate::meta::types::MetaResponse::DistributionConfigNotFound { .. } => (
                StatusCode::NOT_FOUND,
                Json(DistributionConfigResponse {
                    success: false,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: 0,
                    version: 0,
                    error: Some("Distribution config not found".to_string()),
                }),
            ),
            crate::meta::types::MetaResponse::Error { message } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DistributionConfigResponse {
                    success: false,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: 0,
                    version: 0,
                    error: Some(message),
                }),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DistributionConfigResponse {
                    success: false,
                    catalog: request.catalog,
                    schema: request.schema,
                    table: request.table,
                    scope: scope_str.to_string(),
                    percent: 0,
                    version: 0,
                    error: Some("Unexpected response".to_string()),
                }),
            ),
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DistributionConfigResponse {
                success: false,
                catalog: request.catalog,
                schema: request.schema,
                table: request.table,
                scope: scope_str.to_string(),
                percent: 0,
                version: 0,
                error: Some(e),
            }),
        ),
    }
}

/// Get distribution config
async fn get_distribution_config(
    State(state): State<MetaHttpState>,
    Query(query): Query<DistributionConfigQuery>,
) -> impl IntoResponse {
    let config = if query.effective {
        // Hierarchical lookup
        state
            .meta_node
            .get_effective_distribution_config(&query.catalog, &query.schema, &query.table)
            .await
    } else {
        // Exact match
        state
            .meta_node
            .get_distribution_config(&query.catalog, &query.schema, &query.table)
            .await
    };

    match config {
        Some(c) => (StatusCode::OK, Json(Some(DistributionConfigInfo::from(c)))),
        None => (StatusCode::NOT_FOUND, Json(None)),
    }
}

/// List distribution configs query
#[derive(Deserialize, Default)]
pub struct ListDistributionConfigsQuery {
    /// Filter by catalog (optional)
    #[serde(default)]
    pub catalog: Option<String>,
}

/// List distribution configs
async fn list_distribution_configs(
    State(state): State<MetaHttpState>,
    Query(query): Query<ListDistributionConfigsQuery>,
) -> impl IntoResponse {
    let configs = state
        .meta_node
        .list_distribution_configs(query.catalog.as_deref())
        .await;

    let infos: Vec<DistributionConfigInfo> = configs.into_iter().map(DistributionConfigInfo::from).collect();

    (StatusCode::OK, Json(infos))
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
