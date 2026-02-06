//! Admin Console Backend Module
//!
//! Provides HTTP API and WebSocket endpoints for the admin console.

mod api;
mod state;
mod static_files;
mod websocket;

use axum::Router;

use crate::config::AdminConfig;
use state::AdminState;

/// Build the admin router
pub fn build_admin_router(config: &AdminConfig, meta_endpoints: Vec<String>) -> Router {
    let state = AdminState::new(config.clone(), meta_endpoints);

    Router::new()
        .nest("/api/v1", api::routes())
        .merge(static_files::routes())
        .with_state(state)
}
