//! Static file serving for admin console
//!
//! Serves the React admin dashboard from admin-ui/dist/

use axum::{
    body::Body,
    extract::Path,
    http::{header, Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::path::PathBuf;
use tokio::fs;

use super::state::AdminState;

/// Directory containing the built React app
const DIST_DIR: &str = "admin-ui/dist";

/// Build static file routes
pub fn routes() -> Router<AdminState> {
    Router::new()
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/favicon.svg", get(serve_favicon))
        .route("/assets/*path", get(serve_asset))
        // SPA routes - serve index.html for known SPA paths
        .route("/nodes", get(serve_index))
        .route("/nodes/*path", get(serve_index))
        .route("/scale-in", get(serve_index))
        .route("/tables", get(serve_index))
        .route("/events", get(serve_index))
        .route("/meta-operations", get(serve_index))
        .route("/data-operations", get(serve_index))
        .route("/router-operations", get(serve_index))
        .route("/settings", get(serve_index))
}

/// Serve the index.html file
async fn serve_index() -> impl IntoResponse {
    let index_path = PathBuf::from(DIST_DIR).join("index.html");

    match fs::read(&index_path).await {
        Ok(contents) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(contents))
            .unwrap(),
        Err(_) => serve_not_built_error(),
    }
}

/// Serve the favicon
async fn serve_favicon() -> impl IntoResponse {
    let favicon_path = PathBuf::from(DIST_DIR).join("favicon.svg");

    match fs::read(&favicon_path).await {
        Ok(contents) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "image/svg+xml")
            .header(header::CACHE_CONTROL, "public, max-age=86400")
            .body(Body::from(contents))
            .unwrap(),
        Err(_) => not_found(),
    }
}

/// Serve asset files (JS, CSS, etc.)
async fn serve_asset(Path(path): Path<String>) -> impl IntoResponse {
    let asset_path = PathBuf::from(DIST_DIR).join("assets").join(&path);

    // Determine content type based on file extension
    let content_type = match asset_path.extension().and_then(|e| e.to_str()) {
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("map") => "application/json",
        _ => "application/octet-stream",
    };

    match fs::read(&asset_path).await {
        Ok(contents) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
            .body(Body::from(contents))
            .unwrap(),
        Err(_) => not_found(),
    }
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"error":"Not found"}"#))
        .unwrap()
}

/// Return error when the React app is not built
fn serve_not_built_error() -> Response<Body> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"error":"Frontend not built","message":"Run 'cd admin-ui && npm install && npm run build' to build the frontend"}"#
        ))
        .unwrap()
}
