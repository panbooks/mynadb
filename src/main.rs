mod cache;
mod line_protocol;
mod parquet_writer;
mod query_engine;

use axum::{
    extract::{Path, Query as AxumQuery, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use cache::DataCache;
use line_protocol::parse_multiple_lines;
use query_engine::QueryEngine;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    cache: DataCache,
    query_engine: Arc<QueryEngine>,
}

#[derive(Deserialize)]
struct QueryParams {
    sql: String,
}

#[tokio::main]
async fn main() {
    // Create data directory
    std::fs::create_dir_all("./data").expect("Failed to create data directory");

    // Initialize cache
    let cache = DataCache::new("./data");

    // Start background flush task (every 30 seconds)
    cache.clone().start_background_flush(30);

    // Initialize query engine
    let query_engine = Arc::new(QueryEngine::new("./data"));

    let state = AppState { cache, query_engine };

    let app = Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/write/:db", post(write_handler))
        .route("/stats", get(stats_handler))
        .route("/flush", post(flush_handler))
        .route("/query/:db", get(query_handler))
        .route("/query/:db", post(query_handler_post))
        .route("/tables/:db", get(list_tables_handler))
        .route("/schema/:db/:table", get(schema_handler))
        .fallback(log_request)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7000")
        .await
        .unwrap();

    println!("🚀 Time Series Database Server running on http://0.0.0.0:7000");
    println!("📊 Data directory: ./data");
    println!("⏱️  Flush interval: 30 seconds");
    println!("🔍 Query engine: DataFusion SQL");
    println!("\n📝 Write data:");
    println!("   curl -X POST http://localhost:7000/write/mydb -d 'cpu,host=server01 usage=23.5'");
    println!("\n🔎 Query data:");
    println!("   curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 10'");

    axum::serve(listener, app).await.unwrap();
}

async fn home() -> &'static str {
    "Time Series Database Server\n\n\
     Endpoints:\n\
       POST /write/:db              - Write data in line protocol\n\
       GET  /query/:db?sql=<query>  - Query data with SQL (GET)\n\
       POST /query/:db              - Query data with SQL (POST body)\n\
       GET  /tables/:db             - List available tables\n\
       GET  /schema/:db/:table      - Get table schema\n\
       GET  /health                 - Health check\n\
       GET  /stats                  - Cache statistics\n\
       POST /flush                  - Manually flush cache to disk\n"
}

async fn health() -> &'static str {
    "OK"
}

async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.cache.stats().await;
    (
        StatusCode::OK,
        format!(
            "Cache Statistics:\n  Points: {}\n  Measurements: {}\n",
            stats.total_points, stats.num_measurements
        ),
    )
}

async fn flush_handler(State(state): State<AppState>) -> impl IntoResponse {
    println!("\n=== Manual Flush Request ===");

    match state.cache.flush().await {
        Ok(count) => {
            println!("Manual flush completed successfully");
            (
                StatusCode::OK,
                format!("Flushed {} file(s) to disk", count),
            )
        }
        Err(e) => {
            eprintln!("Manual flush error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Flush error: {}", e))
        }
    }
}

async fn query_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== Query Request (GET) ===");
    println!("Database: {}", db);
    println!("SQL: {}", params.sql);

    execute_query(db, params.sql, state).await
}

async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    println!("\n=== Query Request (POST) ===");
    println!("Database: {}", db);
    println!("SQL: {}", body);

    execute_query(db, body, state).await
}

async fn execute_query(
    db: String,
    sql: String,
    state: AppState,
) -> impl IntoResponse {
    // Register database tables
    match state.query_engine.register_database(&db).await {
        Ok(tables) => {
            if tables.is_empty() {
                println!("⚠️  No tables found in database '{}'", db);
                return (
                    StatusCode::NOT_FOUND,
                    format!("No tables found in database '{}'", db),
                );
            }
            println!("📋 Registered {} table(s): {:?}", tables.len(), tables);
        }
        Err(e) => {
            eprintln!("❌ Error registering database: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error registering database: {}", e),
            );
        }
    }

    // Execute query
    match state.query_engine.execute_query_json(&sql).await {
        Ok(json) => {
            println!("✅ Query executed successfully");
            println!("==================\n");
            (StatusCode::OK, json)
        }
        Err(e) => {
            eprintln!("❌ Query error: {}", e);
            println!("==================\n");
            (StatusCode::BAD_REQUEST, format!("Query error: {}", e))
        }
    }
}

async fn list_tables_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== List Tables Request ===");
    println!("Database: {}", db);

    match state.query_engine.register_database(&db).await {
        Ok(tables) => {
            println!("Found {} table(s)", tables.len());
            let response = serde_json::json!({
                "database": db,
                "tables": tables,
                "count": tables.len()
            });
            (StatusCode::OK, serde_json::to_string_pretty(&response).unwrap())
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

async fn schema_handler(
    Path((db, table)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== Schema Request ===");
    println!("Database: {}, Table: {}", db, table);

    // Register database first
    if let Err(e) = state.query_engine.register_database(&db).await {
        eprintln!("Error registering database: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", e),
        );
    }

    let full_table_name = format!("{}_{}", db, table);
    match state.query_engine.get_table_schema(&full_table_name).await {
        Ok(schema) => (StatusCode::OK, schema),
        Err(e) => {
            eprintln!("Error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

async fn write_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
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

    // Add to cache
    for (measurement, points) in grouped {
        println!("Adding {} points for measurement '{}' to cache", points.len(), measurement);
        state.cache.add_points(&db, &measurement, points).await;
    }

    println!("==================\n");

    (StatusCode::OK, "Data added to cache\n".to_string())
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