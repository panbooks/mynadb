//! Cluster management API endpoints

use axum::{
    extract::{Extension, Query},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};

use super::status::ClusterState;

/// Query params for node operations
#[derive(Debug, Deserialize)]
pub struct NodeIdQuery {
    pub node_id: String,
}

/// Drain node response
#[derive(Debug, Serialize)]
pub struct DrainNodeResponse {
    /// Success
    pub success: bool,
    /// Node ID
    pub node_id: String,
    /// Message
    pub message: String,
}

/// Remove node response
#[derive(Debug, Serialize)]
pub struct RemoveNodeResponse {
    /// Success
    pub success: bool,
    /// Node ID
    pub node_id: String,
    /// Message
    pub message: String,
}

/// POST /cluster/drain?node_id=xxx - Drain a node (prepare for removal)
pub async fn drain_node_handler(
    Query(query): Query<NodeIdQuery>,
    Extension(cluster): Extension<ClusterState>,
) -> Result<Json<DrainNodeResponse>, (StatusCode, String)> {
    let node_id = query.node_id;
    if !cluster.enabled {
        return Err((
            StatusCode::BAD_REQUEST,
            "Cluster mode is not enabled".to_string(),
        ));
    }

    let registration = cluster.registration.as_ref().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Registration not available".to_string(),
    ))?;

    // Check if this is our own node
    let this_node_id = registration.node_id();
    if this_node_id == node_id {
        // Drain ourselves
        registration
            .drain()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        return Ok(Json(DrainNodeResponse {
            success: true,
            node_id,
            message: "Node is now draining".to_string(),
        }));
    }

    // For remote nodes, we would need to send a message
    // For now, just return an error
    Err((
        StatusCode::NOT_IMPLEMENTED,
        "Remote node draining not yet implemented".to_string(),
    ))
}

/// DELETE /cluster/node?node_id=xxx - Remove a node from the cluster
pub async fn remove_node_handler(
    Query(query): Query<NodeIdQuery>,
    Extension(cluster): Extension<ClusterState>,
) -> Result<Json<RemoveNodeResponse>, (StatusCode, String)> {
    let node_id = query.node_id;
    if !cluster.enabled {
        return Err((
            StatusCode::BAD_REQUEST,
            "Cluster mode is not enabled".to_string(),
        ));
    }

    let registration = cluster.registration.as_ref().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Registration not available".to_string(),
    ))?;

    let coordinator = cluster.coordinator.as_ref().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Coordinator not available".to_string(),
    ))?;

    // Check if this is our own node
    let this_node_id = registration.node_id();
    if this_node_id == node_id {
        // Deregister ourselves
        registration
            .deregister()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        return Ok(Json(RemoveNodeResponse {
            success: true,
            node_id,
            message: "Node has been removed".to_string(),
        }));
    }

    // For remote nodes, delete their key from the coordinator
    let key = format!("/mynadb/nodes/{}", node_id);
    coordinator
        .delete(&key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(RemoveNodeResponse {
        success: true,
        node_id,
        message: "Node has been removed from the cluster".to_string(),
    }))
}
