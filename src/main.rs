use axum::{
    extract::Request,
    response::IntoResponse,
    routing::get,
    Router,
};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(home))
        .fallback(log_request);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7000")
        .await
        .unwrap();

    println!("🚀 Server running on http://0.0.0.0:7000");

    axum::serve(listener, app).await.unwrap();
}

async fn home() -> &'static str {
    "Hello from Stats server!"
}

async fn log_request(req: Request) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();

    // Extract body
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default();

    println!("\n=== New Request ===");
    println!("Method: {}", method);
    println!("URL: {}", uri);

    // Log headers
    println!("Headers:");
    for (key, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            println!("  {}: {}", key, v);
        }
    }

    // Log body
    if !body.is_empty() {
        println!("Body ({} bytes):", body.len());
        if let Ok(text) = String::from_utf8(body.to_vec()) {
            println!("{}", text);
        } else {
            println!("[binary data]");
        }
    }
    println!("==================\n");

    format!("Logged {} request to {}", method, uri)
}