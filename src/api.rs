/// API handlers for the stats service
///
/// This module contains all HTTP request handlers organized by functionality:
/// - Database management
/// - Table management
/// - Query and write operations
/// - Cache and memory management
/// - Profiling (pprof-compatible)
/// - Compaction
/// - Partition management

use axum::{
    extract::{Path, Query as AxumQuery, Request, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::cache::DataCache;
use crate::compactor;
use crate::config;
use crate::line_protocol::parse_multiple_lines;
use crate::profiling;
use crate::query_engine::QueryEngine;

// ===== Types and State =====

#[derive(Clone)]
pub struct AppState {
    pub cache: DataCache,
    pub query_engine: Arc<QueryEngine>,
    pub db_manager: Arc<crate::database_manager::DatabaseManager>,
    pub compactor: Arc<crate::compactor::Compactor>,
    pub config: Arc<config::Config>,
    pub wal: Option<Arc<crate::wal::Wal>>,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct WriteQueryParams {
    pub db: String,
}

#[derive(Deserialize, Serialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
}

#[derive(Deserialize, Serialize)]
pub struct CreateTableRequest {
    pub name: String,
    #[serde(default)]
    pub partition_by: Vec<String>,
}

#[derive(Deserialize)]
pub struct RetentionRequest {
    /// Delete partitions before this date (YYYY-MM-DD)
    #[serde(default)]
    pub before_date: Option<String>,
    /// Delete partitions older than N days
    #[serde(default)]
    pub older_than_days: Option<u32>,
}

fn default_profile_duration() -> u64 {
    10
}

fn default_profile_format() -> String {
    "protobuf".to_string()
}

#[derive(Deserialize)]
pub struct ProfileParams {
    /// Profile duration in seconds (default: 10)
    #[serde(default = "default_profile_duration")]
    pub duration: u64,
    /// Output format: protobuf, flamegraph, text (default: protobuf)
    #[serde(default = "default_profile_format")]
    pub format: String,
    /// Add Content-Disposition header for download (default: false)
    #[serde(default)]
    pub download: bool,
}

// ===== Home and Health =====

pub async fn home() -> &'static str {
    "Time Series Database Server\n\n\
     Database Management:\n\
       GET    /databases              - List all databases\n\
       POST   /databases              - Create database\n\
       DELETE /databases/:db          - Delete database\n\
       GET    /databases/:db          - Get database info\n\
       GET    /databases/:db/schemas  - List all schemas (services) in database\n\
       GET    /databases/:db/schemas/:schema/tables - List tables in schema\n\
       POST   /databases/:db/tables   - Create table (with partitioning)\n\
       GET    /databases/:db/tables   - List tables\n\
       DELETE /databases/:db/tables/:table - Delete table\n\n\
     Data Operations:\n\
       POST   /write/:db              - Write data (line protocol)\n\
       POST   /write?db=:db           - Write data (InfluxDB-compatible)\n\
       GET    /query/:db?sql=<query>  - Query data (GET)\n\
       POST   /query/:db              - Query data (POST body)\n\
       GET    /schema/:db/:table      - Get table schema\n\n\
     Cache Management:\n\
       GET    /stats                  - Cache statistics\n\
       GET    /memory                 - Memory usage breakdown\n\
       POST   /flush                  - Flush cache to disk\n\
       GET    /health                 - Health check\n\n\
     Profiling (pprof-compatible):\n\
       GET    /debug/heap?duration=10&format=protobuf  - Heap allocation profile\n\
       GET    /debug/cpu?duration=10&format=protobuf   - CPU profile\n\
       Query parameters:\n\
         duration - Profile duration in seconds (default: 10, max: 300)\n\
         format   - Output format: protobuf, flamegraph, text (default: protobuf)\n\
         download - Add Content-Disposition header (default: false)\n\n\
     Compaction:\n\
       POST   /compact/:catalog       - Compact all tables in catalog\n\
       POST   /compact/:catalog/:schema/:table - Compact all partitions in table\n\
       POST   /compact/:catalog/:schema/:table/:date - Compact specific partition\n\n\
     Partition Management:\n\
       GET    /partitions/:catalog/:schema/:table - List all partitions\n\
       DELETE /partitions/:catalog/:schema/:table/:date - Delete specific partition\n\
       POST   /retention/:catalog/:schema/:table - Delete old partitions (body: {older_than_days: N} or {before_date: \"YYYY-MM-DD\"})\n"
}

pub async fn health() -> &'static str {
    "OK"
}

pub async fn version_handler() -> impl IntoResponse {
    let version_info = serde_json::json!({
        "version": env!("GIT_VERSION"),
        "git_hash": env!("GIT_HASH"),
        "git_commit_date": env!("GIT_COMMIT_DATE"),
        "build_time": env!("BUILD_TIME"),
        "cargo_version": env!("CARGO_PKG_VERSION"),
        "rustc_version": env!("RUSTC_VERSION"),
    });
    (StatusCode::OK, Json(version_info))
}

// ===== Database Management Handlers =====

pub async fn list_databases_handler(State(state): State<AppState>) -> impl IntoResponse {
    debug!("List Databases Request");

    match state.db_manager.list_databases() {
        Ok(databases) => {
            let response = serde_json::json!({
                "databases": databases,
                "count": databases.len()
            });
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List databases error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

pub async fn create_database_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateDatabaseRequest>,
) -> impl IntoResponse {
    info!("Create Database Request: {}", req.name);

    match state.db_manager.create_database(&req.name) {
        Ok(msg) => {
            info!("Database created: {}", msg);
            (StatusCode::CREATED, msg)
        }
        Err(e) => {
            error!("Create database error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Error: {}", e))
        }
    }
}

pub async fn delete_database_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("Delete Database Request: {}", db);

    match state.db_manager.delete_database(&db) {
        Ok(msg) => {
            info!("Database deleted: {}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            error!("Delete database error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

pub async fn get_database_info_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Get Database Info Request: {}", db);

    match state.db_manager.get_database_info(&db) {
        Ok(info) => {
            let json = serde_json::to_string_pretty(&info).unwrap();
            (StatusCode::OK, json)
        }
        Err(e) => {
            error!("Get database info error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

// ===== Table Management Handlers =====

pub async fn create_table_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
    Json(req): Json<CreateTableRequest>,
) -> impl IntoResponse {
    info!(
        "Create Table Request: db={}, table={}, partition_by={:?}",
        db, req.name, req.partition_by
    );

    match state
        .db_manager
        .create_table(&db, &req.name, req.partition_by)
    {
        Ok(msg) => {
            info!("Table created: {}", msg);
            (StatusCode::CREATED, msg)
        }
        Err(e) => {
            error!("Create table error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Error: {}", e))
        }
    }
}

pub async fn delete_table_handler(
    Path((db, table)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("Delete Table Request: db={}, table={}", db, table);

    match state.db_manager.delete_table(&db, &table) {
        Ok(msg) => {
            info!("Table deleted: {}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            error!("Delete table error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

pub async fn list_tables_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("List Tables Request: db={}", db);

    // Auto-register database
    if let Err(e) = state.query_engine.ensure_database_registered(&db).await {
        error!("Error registering database: {}", e);
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
    (
        StatusCode::OK,
        serde_json::to_string_pretty(&response).unwrap(),
    )
}

pub async fn schema_handler(
    Path((db, table)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Schema Request: db={}, table={}", db, table);

    // Auto-register database
    if let Err(e) = state.query_engine.ensure_database_registered(&db).await {
        error!("Error registering database: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e));
    }

    let full_table_name = format!("{}_{}", db, table);
    match state.query_engine.get_table_schema(&full_table_name).await {
        Ok(schema) => (StatusCode::OK, schema),
        Err(e) => {
            error!("Get schema error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

pub async fn list_schemas_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("List Schemas Request: db={}", db);

    match state.query_engine.list_schemas(&db).await {
        Ok(schemas) => {
            let response = serde_json::json!({
                "database": db,
                "schemas": schemas,
                "count": schemas.len()
            });
            info!("Found {} schema(s)", schemas.len());
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List schemas error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

pub async fn list_schema_tables_handler(
    Path((db, schema)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("List Schema Tables Request: db={}, schema={}", db, schema);

    match state.query_engine.list_tables_in_schema(&db, &schema).await {
        Ok(tables) => {
            let response = serde_json::json!({
                "database": db,
                "schema": schema,
                "tables": tables,
                "count": tables.len()
            });
            info!("Found {} table(s)", tables.len());
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List schema tables error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

// ===== Cache and Memory Management Handlers =====

pub async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.cache.stats().await;

    // Format bytes in human-readable format
    let format_bytes = |bytes: usize| -> String {
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
        } else if bytes >= 1024 * 1024 {
            format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
        } else if bytes >= 1024 {
            format!("{:.2} KB", bytes as f64 / 1024.0)
        } else {
            format!("{} B", bytes)
        }
    };

    (
        StatusCode::OK,
        format!(
            "Cache Statistics:\n\
             ─────────────────────────────\n\
               Rows:         {}\n\
               Tables:       {}\n\
               Size:         {} / {} ({}%)\n\
               Size (bytes): {} / {}\n\
             ─────────────────────────────\n",
            stats.total_rows,
            stats.num_measurements,
            format_bytes(stats.size_bytes),
            format_bytes(stats.max_size_bytes),
            stats.usage_percent(),
            stats.size_bytes,
            stats.max_size_bytes
        ),
    )
}

pub async fn memory_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cache_stats = state.cache.stats().await;

    // Format bytes
    let format_bytes = |bytes: usize| -> String {
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
        } else if bytes >= 1024 * 1024 {
            format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
        } else if bytes >= 1024 {
            format!("{:.2} KB", bytes as f64 / 1024.0)
        } else {
            format!("{} B", bytes)
        }
    };

    let mut output = String::new();
    output.push_str("Memory Usage Statistics:\n");
    output.push_str("══════════════════════════════════════════\n\n");

    // Cache memory
    output.push_str(&format!("Data Cache:\n"));
    output.push_str(&format!("  Size: {} ({} bytes)\n",
        format_bytes(cache_stats.size_bytes), cache_stats.size_bytes));
    output.push_str(&format!("  Limit: {} ({} bytes)\n\n",
        format_bytes(cache_stats.max_size_bytes), cache_stats.max_size_bytes));

    output.push_str("──────────────────────────────────────────\n");
    output.push_str(&format!("Total: {} ({} bytes)\n",
        format_bytes(cache_stats.size_bytes), cache_stats.size_bytes));
    output.push_str("══════════════════════════════════════════\n");

    (StatusCode::OK, output)
}

pub async fn flush_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("Manual Flush Request");

    match state.cache.flush().await {
        Ok(count) => {
            info!("Manual flush completed: {} files", count);
            (StatusCode::OK, format!("Flushed {} file(s) to disk", count))
        }
        Err(e) => {
            error!("Manual flush error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Flush error: {}", e),
            )
        }
    }
}

// ===== Profiling Handlers =====

pub async fn heap_profile_handler(
    AxumQuery(params): AxumQuery<ProfileParams>,
) -> impl IntoResponse {
    use axum::response::Response;
    use axum::body::Body;
    use axum::http::header;

    info!("Heap Profile Request: duration={}s, format={}", params.duration, params.format);

    // Validate duration (max 300 seconds = 5 minutes)
    if params.duration > 300 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Duration too long (max 300 seconds)"))
            .unwrap();
    }

    if params.duration == 0 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Duration must be greater than 0"))
            .unwrap();
    }

    // Parse format
    let format = match profiling::ProfileFormat::from_str(&params.format) {
        Ok(f) => f,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(e))
                .unwrap();
        }
    };

    let download = params.download;

    // Generate profile in a blocking task to avoid blocking the async runtime
    let profile_result = tokio::task::spawn_blocking(move || {
        profiling::generate_heap_profile(params.duration, format)
    })
    .await;

    match profile_result {
        Ok(Ok(data)) => {
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, format.content_type());

            if download {
                let filename = format!("heap_profile_{}.{}",
                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                    format.file_extension());
                response = response.header(header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", filename));
            }

            info!("Heap profile generated successfully: {} bytes", data.len());
            response.body(Body::from(data)).unwrap()
        }
        Ok(Err(e)) => {
            error!("Failed to generate heap profile: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(format!("Failed to generate profile: {}", e)))
                .unwrap()
        }
        Err(e) => {
            error!("Profile task panicked: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("Profile task failed"))
                .unwrap()
        }
    }
}

pub async fn cpu_profile_handler(
    AxumQuery(params): AxumQuery<ProfileParams>,
) -> impl IntoResponse {
    use axum::response::Response;
    use axum::body::Body;
    use axum::http::header;

    info!("CPU Profile Request: duration={}s, format={}", params.duration, params.format);

    // Validate duration (max 300 seconds = 5 minutes)
    if params.duration > 300 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Duration too long (max 300 seconds)"))
            .unwrap();
    }

    if params.duration == 0 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Duration must be greater than 0"))
            .unwrap();
    }

    // Parse format
    let format = match profiling::ProfileFormat::from_str(&params.format) {
        Ok(f) => f,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(e))
                .unwrap();
        }
    };

    let download = params.download;

    // Generate profile in a blocking task to avoid blocking the async runtime
    let profile_result = tokio::task::spawn_blocking(move || {
        profiling::generate_cpu_profile(params.duration, format)
    })
    .await;

    match profile_result {
        Ok(Ok(data)) => {
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, format.content_type());

            if download {
                let filename = format!("cpu_profile_{}.{}",
                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                    format.file_extension());
                response = response.header(header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", filename));
            }

            info!("CPU profile generated successfully: {} bytes", data.len());
            response.body(Body::from(data)).unwrap()
        }
        Ok(Err(e)) => {
            error!("Failed to generate CPU profile: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(format!("Failed to generate profile: {}", e)))
                .unwrap()
        }
        Err(e) => {
            error!("Profile task panicked: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("Profile task failed"))
                .unwrap()
        }
    }
}

// ===== Query Handlers =====

pub async fn query_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Query Request (GET): db={}, sql={}", db, params.sql);
    execute_query(db, params.sql, state).await
}

pub async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    debug!("Query Request (POST): db={}, sql={}", db, body);
    execute_query(db, body, state).await
}

async fn execute_query(db: String, sql: String, state: AppState) -> impl IntoResponse {
    // Execute query with database context (auto-prefix table names)
    // Tables will be registered on-demand based on the query
    match state
        .query_engine
        .execute_query_json_with_db(&sql, &db)
        .await
    {
        Ok(json) => {
            info!("Query executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            error!("Query error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Query error: {}", e))
        }
    }
}

// ===== EXPLAIN/ANALYZE Query Handlers =====

/// Query parameters for EXPLAIN endpoint
#[derive(Debug, Deserialize)]
pub struct ExplainParams {
    pub sql: String,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub analyze: bool,
}

/// EXPLAIN query handler - returns execution plan
pub async fn explain_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<ExplainParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Explain Request: db={}, sql={}, verbose={}, analyze={}",
           db, params.sql, params.verbose, params.analyze);

    match state
        .query_engine
        .explain_query(&params.sql, &db, params.verbose, params.analyze)
        .await
    {
        Ok(result) => {
            info!("Explain executed successfully");
            match serde_json::to_string_pretty(&result) {
                Ok(json) => (StatusCode::OK, json),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JSON error: {}", e)),
            }
        }
        Err(e) => {
            error!("Explain error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Explain error: {}", e))
        }
    }
}

/// EXPLAIN query handler (POST)
pub async fn explain_handler_post(
    Path(db): Path<String>,
    State(state): State<AppState>,
    axum::Json(params): axum::Json<ExplainParams>,
) -> impl IntoResponse {
    debug!("Explain Request (POST): db={}, sql={}", db, params.sql);

    match state
        .query_engine
        .explain_query(&params.sql, &db, params.verbose, params.analyze)
        .await
    {
        Ok(result) => {
            info!("Explain executed successfully");
            match serde_json::to_string_pretty(&result) {
                Ok(json) => (StatusCode::OK, json),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JSON error: {}", e)),
            }
        }
        Err(e) => {
            error!("Explain error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Explain error: {}", e))
        }
    }
}

/// ANALYZE query handler - returns query analysis with statistics
pub async fn analyze_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Analyze Request: db={}, sql={}", db, params.sql);

    match state.query_engine.analyze_query(&params.sql, &db).await {
        Ok(result) => {
            info!("Analyze executed successfully");
            match serde_json::to_string_pretty(&result) {
                Ok(json) => (StatusCode::OK, json),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JSON error: {}", e)),
            }
        }
        Err(e) => {
            error!("Analyze error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Analyze error: {}", e))
        }
    }
}

/// ANALYZE query handler (POST)
pub async fn analyze_handler_post(
    Path(db): Path<String>,
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    debug!("Analyze Request (POST): db={}, sql={}", db, body);

    match state.query_engine.analyze_query(&body, &db).await {
        Ok(result) => {
            info!("Analyze executed successfully");
            match serde_json::to_string_pretty(&result) {
                Ok(json) => (StatusCode::OK, json),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("JSON error: {}", e)),
            }
        }
        Err(e) => {
            error!("Analyze error: {}", e);
            (StatusCode::BAD_REQUEST, format!("Analyze error: {}", e))
        }
    }
}

// ===== Write Handlers =====

pub async fn write_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
    req: Request,
) -> impl IntoResponse {
    debug!("Write Request: db={}", db);

    // Read body
    let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("Error reading body: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Error reading body: {}", e),
            );
        }
    };

    let data = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            error!("Invalid UTF-8: {}", e);
            return (StatusCode::BAD_REQUEST, format!("Invalid UTF-8: {}", e));
        }
    };

    process_write_data(&db, &data, state).await
}

pub async fn write_handler_query_param(
    AxumQuery(params): AxumQuery<WriteQueryParams>,
    State(state): State<AppState>,
    req: Request,
) -> impl IntoResponse {
    debug!("Write Request (InfluxDB-compatible): db={}", params.db);

    // Read body
    let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("Error reading body: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Error reading body: {}", e),
            );
        }
    };

    let data = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            error!("Invalid UTF-8: {}", e);
            return (StatusCode::BAD_REQUEST, format!("Invalid UTF-8: {}", e));
        }
    };

    process_write_data(&params.db, &data, state).await
}

async fn process_write_data(db: &str, data: &str, state: AppState) -> (StatusCode, String) {
    let data_len = data.len();
    debug!("Processing write data: {} bytes", data_len);

    // Check WAL sync mode
    let sync_on_write = state.wal.as_ref().map(|w| w.is_sync_on_write()).unwrap_or(false);

    // STEP 1: WAL write - sync or async based on configuration
    if let Some(ref wal) = state.wal {
        if sync_on_write {
            // Strict durability: wait for WAL write before proceeding
            if let Err(e) = wal.log_data_write(db, data).await {
                error!("WAL write failed: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("WAL error: {}\n", e),
                );
            }
        } else {
            // High throughput: WAL write in background
            let wal = wal.clone();
            let db_owned = db.to_string();
            let data_owned = data.to_string();
            tokio::spawn(async move {
                if let Err(e) = wal.log_data_write(&db_owned, &data_owned).await {
                    error!("WAL write failed (async): {}", e);
                }
            });
        }
    }

    // STEP 2: Parse line protocol (fast, no need for spawn_blocking)
    let points = parse_multiple_lines(data, &state.config.schema);

    if points.is_empty() {
        return (StatusCode::BAD_REQUEST, "No valid points to write".to_string());
    }

    let point_count = points.len();

    // STEP 3: Group and add to cache
    // Pre-size: typically few unique (schema, table) combinations per request
    let mut grouped: HashMap<(String, String), Vec<_>> = HashMap::with_capacity(8);
    for point in points {
        let (schema, table) = match state.config.schema.strategy {
            config::SchemaTableStrategy::ServiceAsSchema => {
                (point.service.clone(), point.measurement.clone())
            }
            config::SchemaTableStrategy::ServiceAsTable => {
                (state.config.schema.default_name.clone(), point.service.clone())
            }
        };
        grouped
            .entry((schema, table))
            .or_insert_with(|| Vec::with_capacity(point_count / 4))
            .push(point);
    }

    // Add to cache (sharded, fast)
    for ((schema, table), points) in grouped {
        if let Err(e) = state.cache.add_points(db, &schema, &table, points).await {
            error!("Cache write failed: {}", e);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Cache full, try again later: {}\n", e),
            );
        }
    }

    debug!("Added {} points to cache", point_count);
    (StatusCode::NO_CONTENT, "".to_string())
}

/// Helper function to parse line protocol and add to cache
/// This is used by both the normal write path and WAL recovery
pub async fn ingest_line_protocol_to_cache(
    db: &str,
    line_protocol: &str,
    cache: &DataCache,
    query_engine: Option<&Arc<QueryEngine>>,
    schema_config: &config::SchemaConfig,
) -> Result<usize, String> {
    // Parse line protocol
    let points = parse_multiple_lines(line_protocol, schema_config);

    if points.is_empty() {
        return Err("No valid points to write".to_string());
    }

    let point_count = points.len();

    // Group by schema and table based on strategy
    let mut grouped: HashMap<(String, String), Vec<_>> = HashMap::new();
    for point in points {
        let (schema, table) = match schema_config.strategy {
            config::SchemaTableStrategy::ServiceAsSchema => {
                // Current behavior: service → schema, measurement → table
                (point.service.clone(), point.measurement.clone())
            }
            config::SchemaTableStrategy::ServiceAsTable => {
                // New behavior: "default" → schema, service → table
                (schema_config.default_name.clone(), point.service.clone())
            }
        };

        let key = (schema, table);
        grouped.entry(key).or_default().push(point);
    }

    // Add to cache and register catalog.schema
    for ((schema, table), points) in grouped {
        debug!(
            "Adding {} points for database={}, schema={}, table={}",
            points.len(),
            db,
            schema,
            table
        );

        // Ensure catalog.schema is registered in query engine (if provided)
        if let Some(qe) = query_engine {
            if let Err(e) = qe.ensure_schema(db, &schema).await {
                warn!("Failed to register schema {}.{}: {}", db, schema, e);
            }
        }

        // Add to cache
        cache
            .add_points(db, &schema, &table, points)
            .await
            .map_err(|e| format!("Cache error: {}", e))?;
    }

    Ok(point_count)
}

// ===== Compaction Handlers =====

pub async fn compact_handler(
    Path(catalog): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("Compact Catalog Request: catalog={}", catalog);

    // Convert config to compactor's CompactionConfig
    let config = compactor::CompactionConfig::from(&state.config.compaction);

    match state.compactor.compact_catalog(&catalog, &config).await {
        Ok(results) => {
            if results.is_empty() {
                (
                    StatusCode::OK,
                    "No compaction needed (minimum file threshold not met)".to_string(),
                )
            } else {
                let summary = results
                    .iter()
                    .map(|r| {
                        format!(
                            "{}.{}.{}/{}: L0 files {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB, {} rows)",
                            r.catalog,
                            r.schema,
                            r.table,
                            r.date_partition,
                            r.l0_files_before,
                            r.l0_files_after,
                            r.files_before,
                            r.files_after,
                            r.bytes_before as f64 / 1024.0 / 1024.0,
                            r.bytes_after as f64 / 1024.0 / 1024.0,
                            r.rows_compacted
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                (
                    StatusCode::OK,
                    format!(
                        "Compacted {} partition(s) in catalog '{}'\n\n{}",
                        results.len(),
                        catalog,
                        summary
                    ),
                )
            }
        }
        Err(e) => {
            error!("Compaction error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Compaction error: {}", e),
            )
        }
    }
}

pub async fn compact_table_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!(
        "Compact Table Request: catalog={}, schema={}, table={}",
        catalog, schema, table
    );

    // Convert config to compactor's CompactionConfig
    let config = compactor::CompactionConfig::from(&state.config.compaction);

    match state
        .compactor
        .compact_table_all_partitions(&catalog, &schema, &table, &config)
        .await
    {
        Ok(results) => {
            if results.is_empty() {
                (
                    StatusCode::OK,
                    format!(
                        "No compaction needed for {}.{}.{} (minimum file threshold not met)",
                        catalog, schema, table
                    ),
                )
            } else {
                let summary = results
                    .iter()
                    .map(|r| {
                        format!(
                            "{}: L0 files {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB, {} rows)",
                            r.date_partition,
                            r.l0_files_before,
                            r.l0_files_after,
                            r.files_before,
                            r.files_after,
                            r.bytes_before as f64 / 1024.0 / 1024.0,
                            r.bytes_after as f64 / 1024.0 / 1024.0,
                            r.rows_compacted
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                (
                    StatusCode::OK,
                    format!(
                        "Compacted {} partition(s) in {}.{}.{}\n\n{}",
                        results.len(),
                        catalog,
                        schema,
                        table,
                        summary
                    ),
                )
            }
        }
        Err(e) => {
            error!("Compaction error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Compaction error: {}", e),
            )
        }
    }
}

pub async fn compact_partition_handler(
    Path((catalog, schema, table, date)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!(
        "Compact Partition Request: catalog={}, schema={}, table={}, date={}",
        catalog, schema, table, date
    );

    // Format as date partition (date=YYYY-MM-DD)
    let date_partition = if date.starts_with("date=") {
        date.clone()
    } else {
        format!("date={}", date)
    };

    // Convert config to compactor's CompactionConfig
    let config = compactor::CompactionConfig::from(&state.config.compaction);

    match state
        .compactor
        .compact_table_partition(&catalog, &schema, &table, &date_partition, &config)
        .await
    {
        Ok(Some(result)) => {
            let message = format!(
                "Compacted {}.{}.{}/{}: L0 files {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB, {} rows)",
                result.catalog,
                result.schema,
                result.table,
                result.date_partition,
                result.l0_files_before,
                result.l0_files_after,
                result.files_before,
                result.files_after,
                result.bytes_before as f64 / 1024.0 / 1024.0,
                result.bytes_after as f64 / 1024.0 / 1024.0,
                result.rows_compacted
            );
            info!("{}", message);
            (StatusCode::OK, message)
        }
        Ok(None) => {
            let message = format!(
                "No compaction needed for {}.{}.{}/{} (minimum file threshold not met)",
                catalog, schema, table, date_partition
            );
            info!("{}", message);
            (StatusCode::OK, message)
        }
        Err(e) => {
            error!("Compaction error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Compaction error: {}", e),
            )
        }
    }
}

// ===== Partition Management Handlers =====

pub async fn list_partitions_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!(
        "List Partitions Request: catalog={}, schema={}, table={}",
        catalog, schema, table
    );

    match state
        .db_manager
        .discover_table_partitions(&catalog, &schema, &table)
    {
        Ok(partitions) => {
            let response = serde_json::json!({
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "partitions": partitions.iter().map(|p| {
                    serde_json::json!({
                        "partition": p.partition,
                        "date": p.date,
                        "file_count": p.file_count,
                        "size_mb": p.total_size_bytes as f64 / 1024.0 / 1024.0
                    })
                }).collect::<Vec<_>>(),
                "count": partitions.len(),
                "total_size_mb": partitions.iter().map(|p| p.total_size_bytes).sum::<u64>() as f64 / 1024.0 / 1024.0
            });
            info!("Found {} partition(s)", partitions.len());
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List partitions error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

pub async fn delete_partition_handler(
    Path((catalog, schema, table, date)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!(
        "Delete Partition Request: catalog={}, schema={}, table={}, date={}",
        catalog, schema, table, date
    );

    match state
        .db_manager
        .delete_partition(&catalog, &schema, &table, &date)
    {
        Ok(msg) => {
            info!("{}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            error!("Delete partition error: {}", e);
            (StatusCode::NOT_FOUND, format!("Error: {}", e))
        }
    }
}

pub async fn delete_old_partitions_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(req): Json<RetentionRequest>,
) -> impl IntoResponse {
    info!(
        "Delete Old Partitions Request: catalog={}, schema={}, table={}",
        catalog, schema, table
    );

    let deleted = if let Some(days) = req.older_than_days {
        info!("Deleting partitions older than {} days", days);
        state
            .db_manager
            .delete_partitions_older_than_days(&catalog, &schema, &table, days)
    } else if let Some(before_date) = req.before_date {
        info!("Deleting partitions before {}", before_date);
        state
            .db_manager
            .delete_partitions_before(&catalog, &schema, &table, &before_date)
    } else {
        return (
            StatusCode::BAD_REQUEST,
            "Must specify either 'older_than_days' or 'before_date'".to_string(),
        );
    };

    match deleted {
        Ok(messages) => {
            if messages.is_empty() {
                (StatusCode::OK, "No partitions to delete".to_string())
            } else {
                let summary = format!(
                    "Deleted {} partition(s):\n{}",
                    messages.len(),
                    messages.join("\n")
                );
                info!("{}", summary);
                (StatusCode::OK, summary)
            }
        }
        Err(e) => {
            error!("Delete partitions error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

// ===== Fallback Handler =====

pub async fn log_request(req: Request) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();

    warn!("Endpoint not found: {} {}", method, uri);

    (
        StatusCode::NOT_FOUND,
        format!("Endpoint not found: {} {}", method, uri),
    )
}
