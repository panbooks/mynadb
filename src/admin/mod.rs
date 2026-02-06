//! Admin Console Backend Module
//!
//! Provides HTTP API and WebSocket endpoints for the admin console.

mod api;
pub mod events;
mod state;
mod static_files;
mod websocket;

use axum::Router;
use std::sync::Arc;

use crate::config::AdminConfig;
use state::AdminState;

/// Build the admin router
pub fn build_admin_router(config: &AdminConfig, meta_endpoints: Vec<String>) -> Router {
    let state = AdminState::new(config.clone(), meta_endpoints.clone());

    // Start the event bridge in the background with caching
    let event_tx = state.event_tx.clone();
    let state_clone = state.clone();

    tokio::spawn(async move {
        // Create a caching callback that stores events in AdminState
        let state_for_cache = Arc::new(state_clone);
        let cache_callback: events::EventCacheCallback = Box::new(move |event| {
            let state = Arc::clone(&state_for_cache);
            // Use a blocking spawn since we're in a sync callback
            tokio::spawn(async move {
                state.cache_event(event).await;
            });
        });

        events::start_event_bridge_with_cache(meta_endpoints, event_tx, Some(cache_callback)).await;
    });

    Router::new()
        .nest("/api/v1", api::routes())
        .merge(static_files::routes())
        .with_state(state)
}
