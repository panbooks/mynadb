//! Built-in monitoring dashboard
//!
//! Serves a self-contained HTML page with zero-dependency inline charts
//! for system metrics (CPU, memory, disk, network) and MynaDB internal metrics.
//!
//! Layout:
//!   dashboard/
//!     mod.rs       — this file (handlers)
//!     index.html   — main dashboard page (inline CSS + JS, no CDN deps)

use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
};

const DASHBOARD_HTML: &str = include_str!("index.html");

pub async fn dashboard_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        DASHBOARD_HTML,
    )
}
