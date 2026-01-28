//! Data Node HTTP API handlers
//!
//! Provides basic HTTP endpoints for monitoring and administration.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

use crate::storage::cache::DataCache;

/// Data node HTTP state
#[derive(Clone)]
pub struct DataNodeState {
    pub node_id: String,
    pub start_time: Instant,
    pub cache: Arc<DataCache>,
}

/// Health response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub node_id: String,
    pub node_type: &'static str,
}

/// Stats response
#[derive(Serialize)]
pub struct StatsResponse {
    pub node_id: String,
    pub node_type: &'static str,
    pub uptime_secs: u64,
    pub cache_size_bytes: usize,
    pub cache_entries: usize,
    pub total_rows: usize,
}

/// Version response
#[derive(Serialize)]
pub struct VersionResponse {
    pub version: &'static str,
    pub git_hash: &'static str,
}

/// Build the Data node HTTP API router
pub fn build_router(state: DataNodeState) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/stats", get(stats))
        .with_state(state)
}

/// Home page
async fn home() -> &'static str {
    "MynaDB Data Node\n\n\
     Endpoints:\n\
       GET /health   - Health check\n\
       GET /version  - Version info\n\
       GET /stats    - Node statistics\n\n\
     Primary protocol: Arrow Flight\n"
}

/// Health check
async fn health(State(state): State<DataNodeState>) -> impl IntoResponse {
    let response = HealthResponse {
        status: "healthy",
        node_id: state.node_id.clone(),
        node_type: "data",
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

/// Statistics
async fn stats(State(state): State<DataNodeState>) -> impl IntoResponse {
    let cache_stats = state.cache.stats().await;
    let uptime = state.start_time.elapsed().as_secs();

    let response = StatsResponse {
        node_id: state.node_id.clone(),
        node_type: "data",
        uptime_secs: uptime,
        cache_size_bytes: cache_stats.size_bytes,
        cache_entries: cache_stats.num_measurements,
        total_rows: cache_stats.total_rows,
    };
    (StatusCode::OK, Json(response))
}

/// Start HTTP server for data node
pub async fn start_http_server(
    host: &str,
    port: u16,
    state: DataNodeState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = build_router(state);
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Data node HTTP server started on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}
