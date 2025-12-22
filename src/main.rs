mod cache;
mod database_manager;
mod line_protocol;
mod parquet_writer;
mod query_engine;

use axum::{
    extract::{Path, Query as AxumQuery, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use cache::DataCache;
use database_manager::DatabaseManager;
use line_protocol::parse_multiple_lines;
use query_engine::QueryEngine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    cache: DataCache,
    query_engine: Arc<QueryEngine>,
    db_manager: Arc<DatabaseManager>,
}

#[derive(Deserialize)]
struct QueryParams {
    sql: String,
}

#[derive(Deserialize, Serialize)]
struct CreateDatabaseRequest {
    name: String,
}

#[derive(Deserialize, Serialize)]
struct CreateTableRequest {
    name: String,
    #[serde(default)]
    partition_by: Vec<String>,
}

#[tokio::main]
async fn main() {
    // Create data directory
    std::fs::create_dir_all("./data").expect("Failed to create data directory");

    // Initialize components
    let cache = DataCache::new("./data");
    cache.clone().start_background_flush(30);

    let query_engine = Arc::new(QueryEngine::new("./data"));
    let db_manager = Arc::new(DatabaseManager::new("./data"));

    let state = AppState {
        cache,
        query_engine,
        db_manager,
    };

    let app = Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        // Write endpoints
        .route("/write/:db", post(write_handler))
        // Cache management
        .route("/stats", get(stats_handler))
        .route("/flush", post(flush_handler))
        // Query endpoints
        .route("/query/:db", get(query_handler))
        .route("/query/:db", post(query_handler_post))
        // Database management
        .route("/databases", get(list_databases_handler))
        .route("/databases", post(create_database_handler))
        .route("/databases/:db", delete(delete_database_handler))
        .route("/databases/:db", get(get_database_info_handler))
        // Table management
        .route("/databases/:db/tables", post(create_table_handler))
        .route("/databases/:db/tables", get(list_tables_handler))
        .route("/databases/:db/tables/:table", delete(delete_table_handler))
        .route("/schema/:db/:table", get(schema_handler))
        .fallback(log_request)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7000")
        .await
        .unwrap();

    println!("🚀 Time Series Database Server running on http://0.0.0.0:7000");
    println!("📊 Data directory: ./data");
    println!("⏱️  Flush interval: 30 seconds");
    println!("🔍 Query engine: DataFusion SQL (auto-registration)");
    println!("\n📝 Quick start:");
    println!("   # Create database");
    println!("   curl -X POST http://localhost:7000/databases -H 'Content-Type: application/json' -d '{{\"name\":\"mydb\"}}'");
    println!("\n   # Write data");
    println!("   curl -X POST http://localhost:7000/write/mydb -d 'cpu,host=server01 usage=23.5'");
    println!("\n   # Query data");
    println!("   curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 10'");

    axum::serve(listener, app).await.unwrap();
}

async fn home() -> &'static str {
    "Time Series Database Server\n\n\
     Database Management:\n\
       GET    /databases              - List all databases\n\
       POST   /databases              - Create database\n\
       DELETE /databases/:db          - Delete database\n\
       GET    /databases/:db          - Get database info\n\
       POST   /databases/:db/tables   - Create table (with partitioning)\n\
       GET    /databases/:db/tables   - List tables\n\
       DELETE /databases/:db/tables/:table - Delete table\n\n\
     Data Operations:\n\
       POST   /write/:db              - Write data (line protocol)\n\
       GET    /query/:db?sql=<query>  - Query data (GET)\n\
       POST   /query/:db              - Query data (POST body)\n\
       GET    /schema/:db/:table      - Get table schema\n\n\
     Cache Management:\n\
       GET    /stats                  - Cache statistics\n\
       POST   /flush                  - Flush cache to disk\n\
       GET    /health                 - Health check\n"
}

// ===== Database Management Handlers =====

async fn list_databases_handler(State(state): State<AppState>) -> impl IntoResponse {
    println!("\n=== List Databases Request ===");

    match state.db_manager.list_databases() {
        Ok(databases) => {
            let response = serde_json::json!({
                "databases": databases,
                "count": databases.len()
            });
            (StatusCode::OK, Json(response).to_string())
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

async fn create_database_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateDatabaseRequest>,
) -> impl IntoResponse {
    println!("\n=== Create Database Request ===");
    println!("Database: {}", req.name);

    match state.db_manager.create_database(&req.name) {
        Ok(msg) => {
            println!("✅ {}", msg);
            (StatusCode::CREATED, msg)
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Error: {}", e))
        }
    }
}

async fn delete_database_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== Delete Database Request ===");
    println!("Database: {}", db);

    match state.db_manager.delete_database(&db) {
        Ok(msg) => {
            println!("✅ {}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

async fn get_database_info_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== Get Database Info Request ===");
    println!("Database: {}", db);

    match state.db_manager.get_database_info(&db) {
        Ok(info) => {
            let json = serde_json::to_string_pretty(&info).unwrap();
            (StatusCode::OK, json)
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

async fn create_table_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
    Json(req): Json<CreateTableRequest>,
) -> impl IntoResponse {
    println!("\n=== Create Table Request ===");
    println!("Database: {}, Table: {}", db, req.name);
    println!("Partition by: {:?}", req.partition_by);

    match state.db_manager.create_table(&db, &req.name, req.partition_by) {
        Ok(msg) => {
            println!("✅ {}", msg);
            (StatusCode::CREATED, msg)
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Error: {}", e))
        }
    }
}

async fn delete_table_handler(
    Path((db, table)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== Delete Table Request ===");
    println!("Database: {}, Table: {}", db, table);

    match state.db_manager.delete_table(&db, &table) {
        Ok(msg) => {
            println!("✅ {}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

async fn health() -> &'static str {
    "OK"
}

async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.cache.stats().await;
    (
        StatusCode::OK,
        format!(
            "Cache Statistics:\n  Rows: {}\n  Measurements: {}\n",
            stats.total_rows, stats.num_measurements
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
    // Auto-register database tables (simplified!)
    if let Err(e) = state.query_engine.ensure_database_registered(&db).await {
        eprintln!("❌ Error auto-registering database: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", e),
        );
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

    // Auto-register database
    if let Err(e) = state.query_engine.ensure_database_registered(&db).await {
        eprintln!("Error: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e));
    }

    let tables = state.query_engine.list_tables().await;
    let db_tables: Vec<String> = tables
        .into_iter()
        .filter(|t| t.starts_with(&format!("{}_", db)))
        .collect();

    let response = serde_json::json!({
        "database": db,
        "tables": db_tables,
        "count": db_tables.len()
    });
    (StatusCode::OK, serde_json::to_string_pretty(&response).unwrap())
}

async fn schema_handler(
    Path((db, table)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("\n=== Schema Request ===");
    println!("Database: {}, Table: {}", db, table);

    // Auto-register database
    if let Err(e) = state.query_engine.ensure_database_registered(&db).await {
        eprintln!("Error: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e));
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