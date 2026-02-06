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
        Err(_) => serve_fallback_index(),
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
        .body(Body::from("Not Found"))
        .unwrap()
}

/// Serve a fallback index.html when the React app is not built
fn serve_fallback_index() -> Response<Body> {
    let html = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MynaDB Admin Console</title>
    <style>
        body {
            font-family: system-ui, -apple-system, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            margin: 0;
            background: #f3f4f6;
        }
        .container {
            max-width: 600px;
            padding: 2rem;
            background: white;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        h1 { color: #3b82f6; margin-top: 0; }
        code {
            background: #f3f4f6;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 14px;
        }
        pre {
            background: #1f2937;
            color: #f9fafb;
            padding: 1rem;
            border-radius: 8px;
            overflow-x: auto;
        }
        .note {
            background: #dbeafe;
            border-left: 4px solid #3b82f6;
            padding: 1rem;
            margin-top: 1rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>MynaDB Admin Console</h1>
        <p>The admin frontend is not yet built. To build and serve the frontend:</p>

        <h3>Development Mode</h3>
        <pre>cd admin-ui
npm install
npm run dev</pre>
        <p>Then access the admin console at <code>http://localhost:3000/admin</code></p>

        <h3>Production Build</h3>
        <pre>cd admin-ui
npm install
npm run build</pre>
        <p>The built files will be in <code>admin-ui/dist/</code></p>
        <p>After building, restart the server to serve the built files.</p>

        <div class="note">
            <strong>API Available:</strong> The API is available at <code>/admin/api/v1/</code>
            <br>Try: <a href="/admin/api/v1/cluster/status">/admin/api/v1/cluster/status</a>
        </div>
    </div>
</body>
</html>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(html))
        .unwrap()
}
