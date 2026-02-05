//! Static file serving for admin console

use axum::{
    body::Body,
    extract::Path,
    http::{header, Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

use super::state::AdminState;

/// Build static file routes
pub fn routes() -> Router<AdminState> {
    Router::new()
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/assets/*path", get(serve_asset))
        .route("/style/*path", get(serve_style))
        // SPA fallback - serve index.html for all unmatched routes
        .fallback(serve_index)
}

/// Serve the index.html file
async fn serve_index() -> impl IntoResponse {
    // In production, this would use rust-embed to serve embedded files
    // For now, return a placeholder that explains how to build the frontend
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
        h1 { color: #1a56db; margin-top: 0; }
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
            background: #fef3c7;
            border-left: 4px solid #f59e0b;
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
        <pre>cd admin-frontend
trunk serve</pre>
        <p>Then access the admin console at <code>http://localhost:8080</code></p>

        <h3>Production Build</h3>
        <pre>cd admin-frontend
trunk build --release</pre>
        <p>The built files will be in <code>admin-frontend/dist/</code></p>

        <div class="note">
            <strong>Note:</strong> The API is available at <code>/admin/api/v1/</code>
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

/// Serve asset files (JS, WASM, etc.)
async fn serve_asset(Path(_path): Path<String>) -> impl IntoResponse {
    // In production with rust-embed, this would serve actual files
    not_found()
}

/// Serve style files (CSS)
async fn serve_style(Path(_path): Path<String>) -> impl IntoResponse {
    // In production with rust-embed, this would serve actual files
    not_found()
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not Found"))
        .unwrap()
}
