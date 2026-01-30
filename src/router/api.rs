//! Router-specific HTTP API handlers
//!
//! Provides HTTP endpoints for the Router node that route requests
//! through WriteRouter and QueryCoordinator to Data nodes.

use super::state::RouterAppState;
use crate::utils::json::batches_to_json;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Request/Response Types
// ============================================================================

/// Query parameters for SQL queries (GET request)
#[derive(Debug, Deserialize)]
pub struct QueryParams {
    /// SQL query string
    #[serde(default)]
    pub sql: Option<String>,
    /// Query format (json, csv, etc.)
    #[serde(default)]
    pub format: Option<String>,
}

/// Query request body for POST requests
#[derive(Debug, Deserialize)]
pub struct QueryRequestBody {
    /// SQL query string
    pub sql: String,
}

/// Query response
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    pub rows: u64,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Write response
#[derive(Debug, Serialize)]
pub struct WriteResponse {
    pub success: bool,
    pub points_written: u64,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Router status response
#[derive(Debug, Serialize)]
pub struct RouterStatus {
    pub node_type: &'static str,
    pub node_id: String,
    pub active_data_nodes: usize,
    pub meta_connected: bool,
    pub http_port: u16,
    pub flight_port: u16,
    pub mysql_enabled: bool,
}

// ============================================================================
// Router Builder
// ============================================================================

/// Build Router node HTTP API
pub fn build_router(state: RouterAppState) -> Router {
    Router::new()
        // Basic endpoints
        .route("/", get(home))
        .route("/health", get(health))
        .route("/version", get(version_handler))
        // Write endpoint - routes to Data nodes
        .route("/write/:db", post(write_handler))
        .route("/write", post(write_handler_query_param))
        // Query endpoint - distributed query via Data nodes
        .route("/query/:db", get(query_handler))
        .route("/query/:db", post(query_handler_post))
        // Router-specific endpoints
        .route("/router/status", get(router_status_handler))
        .route("/router/nodes", get(list_data_nodes_handler))
        .route("/stats", get(stats_handler))
        .route("/flush", post(flush_handler))
        .with_state(state)
}

// ============================================================================
// Basic Handlers
// ============================================================================

/// Home page
pub async fn home() -> &'static str {
    "MynaDB Router Node\n\n\
     Endpoints:\n\
       POST /write/:db          - Write data (InfluxDB line protocol)\n\
       GET/POST /query/:db      - Execute SQL query\n\
       GET /health              - Health check\n\
       GET /version             - Version info\n\
       GET /stats               - Router statistics\n\
       POST /flush              - Flush all data nodes\n\
       GET /router/status       - Router status\n\
       GET /router/nodes        - List data nodes\n"
}

/// Health check
pub async fn health() -> &'static str {
    "OK"
}

/// Version information
pub async fn version_handler() -> impl IntoResponse {
    let version_info = serde_json::json!({
        "version": env!("GIT_VERSION"),
        "git_hash": env!("GIT_HASH"),
        "build_time": env!("BUILD_TIME"),
        "node_type": "router"
    });
    (StatusCode::OK, Json(version_info))
}

// ============================================================================
// Write Handlers
// ============================================================================

/// Write handler - routes writes to Data nodes
pub async fn write_handler(
    Path(db): Path<String>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    let result = state.write_router.route_write(&db, &body).await;

    let response = WriteResponse {
        success: result.success,
        points_written: result.points_written,
        elapsed_ms: result.elapsed_ms,
        error: result.error,
    };

    if result.success {
        (StatusCode::NO_CONTENT, Json(response))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

/// Write handler with database in query parameter
pub async fn write_handler_query_param(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    let db = params
        .get("db")
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    let result = state.write_router.route_write(&db, &body).await;

    let response = WriteResponse {
        success: result.success,
        points_written: result.points_written,
        elapsed_ms: result.elapsed_ms,
        error: result.error,
    };

    if result.success {
        (StatusCode::NO_CONTENT, Json(response))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

// ============================================================================
// Query Handlers
// ============================================================================

/// Query handler - distributed query across Data nodes (GET)
pub async fn query_handler(
    Path(db): Path<String>,
    Query(params): Query<QueryParams>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let sql = match params.sql {
        Some(sql) => sql,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(QueryResponse {
                    success: false,
                    data: None,
                    rows: 0,
                    elapsed_ms: 0,
                    error: Some("Missing 'sql' query parameter".to_string()),
                }),
            );
        }
    };

    execute_query(&state, &db, &sql).await
}

/// Query handler - distributed query across Data nodes (POST)
/// Accepts JSON body: {"sql": "SELECT ..."}
/// Or plain text body with just the SQL query
pub async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    // Try to parse as JSON first, then fall back to treating body as raw SQL
    let sql = if let Ok(request) = serde_json::from_str::<QueryRequestBody>(&body) {
        request.sql
    } else {
        // Treat body as raw SQL
        body
    };
    execute_query(&state, &db, &sql).await
}

/// Execute a distributed query
async fn execute_query(
    state: &RouterAppState,
    db: &str,
    sql: &str,
) -> (StatusCode, Json<QueryResponse>) {
    let result = state.query_coordinator.execute_query(db, sql).await;

    // Convert batches to JSON
    let data = if result.batches.is_empty() {
        Some(serde_json::json!([]))
    } else {
        match batches_to_json(&result.batches) {
            Ok(rows) => Some(serde_json::Value::Array(rows)),
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse {
                        success: false,
                        data: None,
                        rows: 0,
                        elapsed_ms: result.elapsed_ms,
                        error: Some(format!("JSON conversion error: {}", e)),
                    }),
                );
            }
        }
    };

    let response = QueryResponse {
        success: result.success,
        data,
        rows: result.total_rows,
        elapsed_ms: result.elapsed_ms,
        error: result.error,
    };

    if result.success {
        (StatusCode::OK, Json(response))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

// ============================================================================
// Router Status Handlers
// ============================================================================

/// Router status handler
pub async fn router_status_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    let status = RouterStatus {
        node_type: "router",
        node_id: state.router_config.node_id.clone(),
        active_data_nodes: state.active_data_node_count(),
        meta_connected: state.is_meta_connected(),
        http_port: state.router_config.http_port,
        flight_port: state.router_config.flight_port,
        mysql_enabled: state.router_config.mysql_enabled,
    };

    (StatusCode::OK, Json(status))
}

/// List active Data nodes
pub async fn list_data_nodes_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    use super::routing::shard_cache::NodeState;

    let nodes = state.shard_cache.get_active_data_nodes();

    let node_list: Vec<_> = nodes
        .iter()
        .map(|n| {
            serde_json::json!({
                "node_id": n.node_id,
                "grpc_addr": n.grpc_addr,
                "state": match n.state {
                    NodeState::Active => "active",
                    NodeState::Draining => "draining",
                    NodeState::Unhealthy => "unhealthy",
                    NodeState::Suspect => "suspect",
                    NodeState::Retiring => "retiring",
                    NodeState::Drained => "drained",
                },
                "load_score": n.load_score,
            })
        })
        .collect();

    (StatusCode::OK, Json(node_list))
}

/// Stats handler - aggregate stats from data nodes
pub async fn stats_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_data_nodes();

    let stats = serde_json::json!({
        "node_type": "router",
        "node_id": state.router_config.node_id,
        "active_data_nodes": nodes.len(),
        "meta_connected": state.is_meta_connected(),
        "queries_routed": 0,  // TODO: Add query counter
        "writes_routed": 0,   // TODO: Add write counter
    });

    (StatusCode::OK, Json(stats))
}

/// Flush handler - broadcast flush to all data nodes
pub async fn flush_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    // Broadcast flush to all data nodes via Flight
    let result = state.write_router.broadcast_flush().await;

    match result {
        Ok(count) => {
            let response = serde_json::json!({
                "success": true,
                "nodes_flushed": count,
            });
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            let response = serde_json::json!({
                "success": false,
                "error": e,
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        }
    }
}
