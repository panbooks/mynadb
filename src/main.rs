mod line_protocol;
mod parquet_writer;
mod cache;

use axum::{
    extract::{Path, Request},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use line_protocol::parse_multiple_lines;
use parquet_writer::ParquetWriter;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // Create data directory
    std::fs::create_dir_all("./data").expect("Failed to create data directory");

    let app = Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/write/:db", post(write_handler))
        .fallback(log_request);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7000")
        .await
        .unwrap();

    println!("🚀 Time Series Database Server running on http://0.0.0.0:7000");
    println!("📊 Data directory: ./data");
    println!("\n📝 Write data with:");
    println!("   curl -X POST http://localhost:7000/write/mydb -d 'cpu,host=server01 usage=23.5'");

    axum::serve(listener, app).await.unwrap();
}

async fn home() -> &'static str {
    "Time Series Database Server\n\nEndpoints:\n  POST /write/:db - Write data in line protocol\n  GET /health - Health check\n"
}

async fn health() -> &'static str {
    "OK"
}

async fn write_handler(
    Path(db): Path<String>,
    req: Request,
) -> impl IntoResponse {
    println!("\n=== Write Request ===");
    println!("Database: {}", db);

    // Read body
    let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error reading body: {}", e);
            return (StatusCode::BAD_REQUEST, format!("Error reading body: {}", e));
        }
    };

    let data = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Invalid UTF-8: {}", e);
            return (StatusCode::BAD_REQUEST, format!("Invalid UTF-8: {}", e));
        }
    };

    println!("Raw data:\n{}", data);

    // Parse line protocol
    let points = parse_multiple_lines(&data);
    println!("Parsed {} points", points.len());

    if points.is_empty() {
        return (StatusCode::BAD_REQUEST, "No valid points to write".to_string());
    }

    // Group points by measurement
    let mut grouped: HashMap<String, Vec<_>> = HashMap::new();
    for point in points {
        grouped
            .entry(point.measurement.clone())
            .or_default()
            .push(point);
    }

    // Write to Parquet
    let writer = ParquetWriter::new("./data");
    let mut written_files = Vec::new();

    for (measurement, points) in grouped {
        println!("Writing {} points for measurement '{}'", points.len(), measurement);
        match writer.write_points(&db, &measurement, points) {
            Ok(filename) => {
                println!("✅ Written to: {}", filename);
                written_files.push(filename);
            }
            Err(e) => {
                eprintln!("❌ Error writing: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error writing data: {}", e),
                );
            }
        }
    }

    println!("==================\n");

    (
        StatusCode::OK,
        format!("Successfully written {} file(s)", written_files.len()),
    )
}

async fn log_request(req: Request) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();

    println!("\n=== Request ===");
    println!("Method: {}", method);
    println!("URL: {}", uri);
    println!("===============\n");

    (
        StatusCode::NOT_FOUND,
        format!("Endpoint not found: {} {}", method, uri),
    )
}