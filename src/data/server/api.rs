//! Data Node HTTP API handlers
//!
//! Provides basic HTTP endpoints for monitoring and administration.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::meta::client::MetaClient;
use crate::meta::types::NodeState;
use crate::storage::cache::DataCache;

/// Data node HTTP state
#[derive(Clone)]
pub struct DataNodeState {
    pub node_id: String,
    pub start_time: Instant,
    pub cache: Arc<DataCache>,
    /// Meta client for scale-in operations (optional)
    pub meta_client: Option<Arc<MetaClient>>,
    /// Whether this node is currently retiring
    pub is_retiring: Arc<AtomicBool>,
    /// Retirement status details
    pub retire_status: Arc<RwLock<Option<RetireStatusDetail>>>,
}

/// Detailed retirement status
#[derive(Clone, Serialize)]
pub struct RetireStatusDetail {
    pub state: String,
    pub started_at: i64,
    pub partitions_to_reassign: usize,
    pub memtable_rows_remaining: u64,
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
        .route("/retire", post(retire))
        .route("/retire/status", get(retire_status))
        .route("/retire/cancel", post(cancel_retire))
        .with_state(state)
}

/// Home page
async fn home() -> &'static str {
    "MynaDB Data Node\n\n\
     Endpoints:\n\
       GET  /health         - Health check\n\
       GET  /version        - Version info\n\
       GET  /stats          - Node statistics\n\
       POST /retire         - Start graceful retirement (scale-in)\n\
       GET  /retire/status  - Get retirement progress\n\
       POST /retire/cancel  - Cancel retirement\n\n\
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
    pub partitions_to_reassign: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Retire status response
#[derive(Serialize)]
pub struct RetireStatusResponse {
    pub state: String,
    pub is_retiring: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_to_reassign: Option<usize>,
    pub memtable_rows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_completion_secs: Option<u64>,
}

/// Start graceful retirement (scale-in)
///
/// This endpoint:
/// 1. Tells Meta service to mark this node as Retiring
/// 2. Router stops sending new writes to this node
/// 3. Waits for memtable to flush to S3
/// 4. Partitions get reassigned to other nodes
/// 5. Once drained, node can be safely stopped
async fn retire(
    State(state): State<DataNodeState>,
    Json(request): Json<RetireRequest>,
) -> impl IntoResponse {
    // Check if already retiring
    if state.is_retiring.load(Ordering::Relaxed) {
        return (
            StatusCode::OK,
            Json(RetireResponse {
                success: true,
                status: "already_retiring".to_string(),
                partitions_to_reassign: 0,
                error: None,
            }),
        );
    }

    // Need meta client for retirement
    let meta_client = match &state.meta_client {
        Some(mc) => mc,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(RetireResponse {
                    success: false,
                    status: "error".to_string(),
                    partitions_to_reassign: 0,
                    error: Some(
                        "Meta client not available - node running in standalone mode".to_string(),
                    ),
                }),
            );
        }
    };

    // Start retirement with Meta service
    match meta_client.start_retire(&state.node_id).await {
        Ok(partitions_to_reassign) => {
            // Mark local state as retiring
            state.is_retiring.store(true, Ordering::SeqCst);

            // Store retirement details
            *state.retire_status.write() = Some(RetireStatusDetail {
                state: "retiring".to_string(),
                started_at: chrono::Utc::now().timestamp_millis(),
                partitions_to_reassign,
                memtable_rows_remaining: state.cache.stats().await.total_rows as u64,
            });

            // If force_flush requested, trigger a flush
            if request.force_flush {
                if let Err(e) = state.cache.flush().await {
                    tracing::warn!("Error during force flush: {}", e);
                }
            }

            tracing::info!(
                "Node {} started retirement: {} partitions to reassign",
                state.node_id,
                partitions_to_reassign
            );

            (
                StatusCode::OK,
                Json(RetireResponse {
                    success: true,
                    status: "retiring".to_string(),
                    partitions_to_reassign,
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RetireResponse {
                success: false,
                status: "error".to_string(),
                partitions_to_reassign: 0,
                error: Some(e),
            }),
        ),
    }
}

/// Get retirement status
async fn retire_status(State(state): State<DataNodeState>) -> impl IntoResponse {
    let is_retiring = state.is_retiring.load(Ordering::Relaxed);
    let cache_stats = state.cache.stats().await;

    if let Some(status) = state.retire_status.read().as_ref() {
        // Currently retiring or drained
        let current_state = if cache_stats.total_rows == 0 {
            "drained" // Memtable is empty, safe to stop
        } else {
            &status.state
        };

        // Estimate completion time based on flush rate
        let estimated_secs = if cache_stats.total_rows > 0 {
            // Rough estimate: assume 10k rows/sec flush rate
            Some((cache_stats.total_rows as u64 / 10000).max(1) * 60) // 60 sec per 10k rows
        } else {
            Some(0)
        };

        (
            StatusCode::OK,
            Json(RetireStatusResponse {
                state: current_state.to_string(),
                is_retiring,
                started_at: Some(status.started_at),
                partitions_to_reassign: Some(status.partitions_to_reassign),
                memtable_rows: cache_stats.total_rows as u64,
                estimated_completion_secs: estimated_secs,
            }),
        )
    } else {
        // Not retiring
        (
            StatusCode::OK,
            Json(RetireStatusResponse {
                state: "active".to_string(),
                is_retiring: false,
                started_at: None,
                partitions_to_reassign: None,
                memtable_rows: cache_stats.total_rows as u64,
                estimated_completion_secs: None,
            }),
        )
    }
}

/// Cancel retirement (restore to Active)
async fn cancel_retire(State(state): State<DataNodeState>) -> impl IntoResponse {
    if !state.is_retiring.load(Ordering::Relaxed) {
        return (
            StatusCode::OK,
            Json(RetireResponse {
                success: true,
                status: "not_retiring".to_string(),
                partitions_to_reassign: 0,
                error: None,
            }),
        );
    }

    let meta_client = match &state.meta_client {
        Some(mc) => mc,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(RetireResponse {
                    success: false,
                    status: "error".to_string(),
                    partitions_to_reassign: 0,
                    error: Some("Meta client not available".to_string()),
                }),
            );
        }
    };

    match meta_client.cancel_retire(&state.node_id).await {
        Ok(()) => {
            state.is_retiring.store(false, Ordering::SeqCst);
            *state.retire_status.write() = None;

            tracing::info!("Node {} retirement cancelled", state.node_id);

            (
                StatusCode::OK,
                Json(RetireResponse {
                    success: true,
                    status: "active".to_string(),
                    partitions_to_reassign: 0,
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RetireResponse {
                success: false,
                status: "error".to_string(),
                partitions_to_reassign: 0,
                error: Some(e),
            }),
        ),
    }
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
