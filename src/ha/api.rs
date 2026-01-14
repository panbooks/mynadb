//! HA API endpoints
//!
//! Provides HTTP endpoints for HA status and management.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{info, warn};

use super::{HaManager, PromotionResult};

/// HA application state
#[derive(Clone)]
pub struct HaState {
    pub ha_manager: Arc<HaManager>,
}

/// Request to promote slave to master
#[derive(Debug, Deserialize)]
pub struct PromoteRequest {
    /// Optional confirmation flag
    #[serde(default)]
    pub confirm: bool,
}

/// Get HA status
pub async fn ha_status_handler(State(state): State<HaState>) -> impl IntoResponse {
    let status = state.ha_manager.status().await;
    (StatusCode::OK, Json(status))
}

/// Promote slave to master
pub async fn ha_promote_handler(
    State(state): State<HaState>,
    Json(req): Json<PromoteRequest>,
) -> impl IntoResponse {
    if !req.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Promotion requires confirmation",
                "message": "Set 'confirm: true' in request body to proceed"
            })),
        );
    }

    info!("Manual promotion requested");

    match state.ha_manager.promote().await {
        PromotionResult::Success => {
            info!("Manual promotion successful");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "success",
                    "message": "Promoted to master successfully"
                })),
            )
        }
        PromotionResult::AlreadyMaster => {
            warn!("Promotion rejected: already master");
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "status": "rejected",
                    "message": "Node is already master"
                })),
            )
        }
        PromotionResult::InProgress => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "status": "in_progress",
                "message": "Promotion already in progress"
            })),
        ),
        PromotionResult::Failed(e) => {
            warn!("Promotion failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "status": "failed",
                    "error": e
                })),
            )
        }
        PromotionResult::NotAllowed(reason) => {
            warn!("Promotion not allowed: {}", reason);
            (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "status": "not_allowed",
                    "reason": reason
                })),
            )
        }
    }
}

/// Health check for HA (useful for load balancers)
pub async fn ha_health_handler(State(state): State<HaState>) -> impl IntoResponse {
    let status = state.ha_manager.status().await;

    if state.ha_manager.is_master() {
        // Master is always ready for writes
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "healthy",
                "role": "master",
                "ready": true
            })),
        )
    } else {
        // Slave health depends on connection to master
        let connected = status
            .replication
            .map(|r| r.connected)
            .unwrap_or(false);

        if connected {
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "healthy",
                    "role": "slave",
                    "connected": true,
                    "ready": true
                })),
            )
        } else {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "status": "unhealthy",
                    "role": "slave",
                    "connected": false,
                    "ready": false
                })),
            )
        }
    }
}

/// Check if node is master (for routing writes)
pub async fn ha_is_master_handler(State(state): State<HaState>) -> impl IntoResponse {
    if state.ha_manager.is_master() {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "is_master": true,
                "ready_for_writes": true
            })),
        )
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "is_master": false,
                "ready_for_writes": false
            })),
        )
    }
}
