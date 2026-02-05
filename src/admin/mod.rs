//! Admin Console Backend Module
//!
//! Provides HTTP API and WebSocket endpoints for the admin console.

pub mod api;
pub mod state;
pub mod static_files;
pub mod websocket;

use axum::Router;

use crate::config::AdminConfig;
use state::AdminState;

/// Build the admin router
pub fn build_admin_router(config: &AdminConfig, meta_endpoints: Vec<String>) -> Router {
    let state = AdminState::new(config.clone(), meta_endpoints);

    Router::new()
        // API routes under /api/v1 (must come before static fallback)
        .nest("/api/v1", api::routes())
        // WebSocket for events
        .route("/api/v1/events/ws", axum::routing::get(websocket::handler))
        // Static files (serve index.html for SPA) - fallback catches remaining routes
        .merge(static_files::routes())
        .with_state(state)
}
