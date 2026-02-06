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
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::prometheus;
use super::shared;
use crate::config;
use crate::jobs::compactor;
use crate::jobs::profiling;
use crate::query::catalog::{MetadataManager, SharedMetadataManager};
use crate::query::query_engine::QueryEngine;
use crate::storage::cache::DataCache;
use crate::storage::file_cache::{FileCache, SharedFileCache};
use crate::storage::line_protocol::parse_multiple_lines;

// ===== Types and State =====

#[derive(Clone)]
pub struct AppState {
    pub cache: DataCache,
    pub query_engine: Arc<QueryEngine>,
    pub db_manager: Arc<crate::jobs::database_manager::DatabaseManager>,
    pub compactor: Arc<compactor::Compactor>,
    pub config: Arc<config::Config>,
    pub wal: Option<Arc<crate::storage::wal::Wal>>,
    /// Centralized metadata manager for catalog/schema/table/file metadata
    pub metadata_manager: Option<SharedMetadataManager>,
    /// File cache for Object Store Parquet files
    pub file_cache: Option<SharedFileCache>,
    /// Query tracer for observability
    pub query_tracer: Option<Arc<crate::observability::QueryTracer>>,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub sql: String,
}

/// InfluxDB-compatible query parameters
#[derive(Deserialize)]
pub struct InfluxQueryParams {
    /// The InfluxQL query
    pub q: String,
    /// Database name (optional, can be specified in path)
    #[serde(default)]
    pub db: Option<String>,
    /// Response format (json or csv)
    #[serde(default)]
    pub format: Option<String>,
    /// Epoch precision for timestamps
    #[serde(default)]
    pub epoch: Option<String>,
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
       POST   /databases/:db/tables   - Create table (with partitioning)\n\
       DELETE /databases/:db/tables/:table - Delete table\n\n\
     Schema/Table Endpoints:\n\
       GET    /schemas/:db            - List schemas\n\
       GET    /tables/:db             - List all tables\n\
       GET    /tables/:db/:schema     - List tables in schema\n\
       GET    /schema/:db/:schema/:table - Get table schema\n\n\
     Data Operations:\n\
       POST   /write/:db              - Write data (line protocol)\n\
       POST   /write?db=:db           - Write data (InfluxDB-compatible)\n\
       GET    /query/:db?sql=<query>  - Query data (SQL)\n\
       POST   /query/:db              - Query data (SQL, POST body)\n\n\
     InfluxQL Query (InfluxDB-compatible):\n\
       GET    /influxql/query?q=<query>&db=<db>  - InfluxQL query\n\
       POST   /influxql/query?db=<db>            - InfluxQL query (POST body)\n\
       GET    /influxql/query/:db?q=<query>      - InfluxQL query with db in path\n\n\
     Prometheus Remote Storage:\n\
       POST   /api/v1/write/:db       - Remote write (snappy protobuf)\n\
       POST   /api/v1/read/:db        - Remote read (protobuf)\n\n\
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

/// Prometheus-compatible metrics endpoint
///
/// Exports metrics in Prometheus exposition format for monitoring and alerting.
/// Scrape this endpoint with Prometheus using:
/// ```yaml
/// scrape_configs:
///   - job_name: 'mynadb'
///     static_configs:
///       - targets: ['localhost:7000']
///     metrics_path: '/metrics'
/// ```
/// Prometheus metrics endpoint (comprehensive dashboard metrics)
pub async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    use crate::metrics::{to_prometheus_format, METRICS};

    let snapshot = METRICS.snapshot();
    let mut output = to_prometheus_format(&snapshot);

    // Add legacy data cache metrics (backward compatibility)
    let cache_stats = state.cache.stats().await;
    output.push_str("# HELP mynadb_data_cache_rows_total Total number of rows in the data cache\n");
    output.push_str("# TYPE mynadb_data_cache_rows_total gauge\n");
    output.push_str(&format!("mynadb_data_cache_rows_total {}\n", cache_stats.total_rows));
    output.push_str("# HELP mynadb_data_cache_tables_total Number of tables in the data cache\n");
    output.push_str("# TYPE mynadb_data_cache_tables_total gauge\n");
    output.push_str(&format!("mynadb_data_cache_tables_total {}\n", cache_stats.num_measurements));

    // File cache metrics (if available)
    if let Some(fc) = &state.file_cache {
        let fc_stats = fc.stats().await;
        output.push_str("# HELP mynadb_file_cache_files_total Number of files in file cache\n");
        output.push_str("# TYPE mynadb_file_cache_files_total gauge\n");
        output.push_str(&format!("mynadb_file_cache_files_total {}\n", fc_stats.file_count));
        output.push_str("# HELP mynadb_file_cache_size_bytes File cache size in bytes\n");
        output.push_str("# TYPE mynadb_file_cache_size_bytes gauge\n");
        output.push_str(&format!("mynadb_file_cache_size_bytes {}\n", fc_stats.total_size_bytes));
        output.push_str("# HELP mynadb_file_cache_hits_total Total file cache hits\n");
        output.push_str("# TYPE mynadb_file_cache_hits_total counter\n");
        output.push_str(&format!("mynadb_file_cache_hits_total {}\n", fc_stats.hits));
        output.push_str("# HELP mynadb_file_cache_misses_total Total file cache misses\n");
        output.push_str("# TYPE mynadb_file_cache_misses_total counter\n");
        output.push_str(&format!("mynadb_file_cache_misses_total {}\n", fc_stats.misses));
    }

    // Build info
    output.push_str("# HELP mynadb_build_info Build information\n");
    output.push_str("# TYPE mynadb_build_info gauge\n");
    output.push_str(&format!(
        "mynadb_build_info{{version=\"{}\",git_hash=\"{}\"}} 1\n",
        env!("GIT_VERSION"),
        env!("GIT_HASH")
    ));

    // Up metric
    output.push_str("# HELP mynadb_up Whether the MynaDB server is up\n");
    output.push_str("# TYPE mynadb_up gauge\n");
    output.push_str("mynadb_up 1\n");

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
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

/// JSON metrics endpoint for dashboard consumption
pub async fn metrics_json_handler() -> impl IntoResponse {
    use crate::metrics::METRICS;

    let snapshot = METRICS.snapshot();
    (StatusCode::OK, Json(snapshot))
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

    match shared::list_tables_impl(&state.query_engine, &db).await {
        Ok(response) => (
            StatusCode::OK,
            serde_json::to_string_pretty(&response).unwrap(),
        ),
        Err(e) => {
            error!("List tables error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))
        }
    }
}

pub async fn schema_handler(
    Path((db, schema, table)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!(
        "Schema Request: db={}, schema={}, table={}",
        db, schema, table
    );

    // Construct full table path: schema.table
    let full_table = format!("{}.{}", schema, table);
    match shared::get_table_schema_impl(&state.query_engine, &db, &full_table).await {
        Ok(schema) => (StatusCode::OK, schema),
        Err(e) => {
            error!("Get schema error: {}", e);
            (StatusCode::NOT_FOUND, e)
        }
    }
}

pub async fn list_schemas_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("List Schemas Request: db={}", db);

    match shared::list_schemas_impl(&state.query_engine, &db).await {
        Ok(response) => {
            info!("Found {} schema(s)", response["count"]);
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List schemas error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e)
        }
    }
}

pub async fn list_schema_tables_handler(
    Path((db, schema)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("List Schema Tables Request: db={}, schema={}", db, schema);

    match shared::list_schema_tables_impl(&state.query_engine, &db, &schema).await {
        Ok(response) => {
            info!("Found {} table(s)", response["count"]);
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List schema tables error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e)
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
    let output = shared::get_memory_stats(&cache_stats);
    (StatusCode::OK, output)
}

pub async fn flush_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("Manual Flush Request");

    match state.cache.flush().await {
        Ok(files) => {
            let count = files.len();
            info!("Manual flush completed: {} files", count);

            // Collect unique tables for cache invalidation
            let mut invalidated_tables: std::collections::HashSet<(String, String, String)> =
                std::collections::HashSet::new();

            // Register files with metadata manager if available
            if let Some(ref manager) = state.metadata_manager {
                for file in &files {
                    let file_name = file
                        .path
                        .rsplit('/')
                        .next()
                        .unwrap_or(&file.path)
                        .to_string();

                    let file_info = crate::query::catalog::FileInfo {
                        path: file.path.clone(),
                        file_name,
                        catalog: file.catalog.clone(),
                        schema: file.schema.clone(),
                        table: file.table.clone(),
                        partition_key: file.partition_key.clone(),
                        size_bytes: file.size_bytes,
                        row_count: file.row_count,
                        level: file.level,
                        min_timestamp: None,
                        max_timestamp: None,
                        created_at: chrono::Utc::now().timestamp_millis(),
                        row_group_count: 1,
                        column_stats: None,
                        etag: None,
                    };
                    if let Err(e) = manager.register_file(file_info).await {
                        warn!("Failed to register file {}: {}", file.path, e);
                    }

                    // Track table for cache invalidation
                    invalidated_tables.insert((
                        file.catalog.clone(),
                        file.schema.clone(),
                        file.table.clone(),
                    ));
                }
                // Save metadata after registering new files
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after manual flush: {}", e);
                }
            } else {
                // Still need to track tables for cache invalidation even without metadata manager
                for file in &files {
                    invalidated_tables.insert((
                        file.catalog.clone(),
                        file.schema.clone(),
                        file.table.clone(),
                    ));
                }
            }

            // Refresh table registrations for all affected tables
            // This deregisters tables from DataFusion so they get re-registered with proper providers
            // (e.g., switching from memtable-only to parquet + memtable)
            for (catalog, schema, table) in &invalidated_tables {
                state
                    .query_engine
                    .refresh_table_after_flush(catalog, schema, table);
            }

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

// ===== Query Cache Management Handlers =====

/// Get query cache statistics (metadata + query result caches)
pub async fn query_cache_stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let response = shared::get_query_cache_stats_impl(&state.query_engine);
    (StatusCode::OK, axum::Json(response))
}

/// Pre-warm the query cache for a catalog (database)
pub async fn prewarm_cache_handler(
    State(state): State<AppState>,
    Path(catalog): Path<String>,
) -> impl IntoResponse {
    info!("Pre-warming cache for catalog: {}", catalog);

    match shared::prewarm_cache_impl(&state.query_engine, &catalog).await {
        Ok(count) => {
            info!("Pre-warmed cache for '{}': {} files", catalog, count);
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "status": "success",
                    "catalog": catalog,
                    "files_cached": count
                })),
            )
        }
        Err(e) => {
            error!("Cache pre-warm error for '{}': {}", catalog, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({
                    "status": "error",
                    "message": e
                })),
            )
        }
    }
}

/// Pre-warm the query cache for a specific table
pub async fn prewarm_table_cache_handler(
    State(state): State<AppState>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
) -> impl IntoResponse {
    info!(
        "Pre-warming cache for table: {}.{}.{}",
        catalog, schema, table
    );

    match shared::prewarm_table_cache_impl(&state.query_engine, &catalog, &schema, &table).await {
        Ok(count) => {
            info!(
                "Pre-warmed cache for '{}.{}.{}': {} files",
                catalog, schema, table, count
            );
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "status": "success",
                    "catalog": catalog,
                    "schema": schema,
                    "table": table,
                    "files_cached": count
                })),
            )
        }
        Err(e) => {
            error!(
                "Cache pre-warm error for '{}.{}.{}': {}",
                catalog, schema, table, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({
                    "status": "error",
                    "message": e
                })),
            )
        }
    }
}

/// Clear all caches (metadata + query results)
pub async fn clear_query_cache_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("Clearing all caches");
    state.query_engine.clear_all_caches();
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "status": "success",
            "message": "All caches cleared (metadata + query results)"
        })),
    )
}

/// Clear only query result cache (keeps metadata cache)
pub async fn clear_query_result_cache_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("Clearing query result cache");
    state.query_engine.clear_query_cache();
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "status": "success",
            "message": "Query result cache cleared"
        })),
    )
}

/// Get Parquet statistics for a table (using cached metadata)
pub async fn table_parquet_stats_handler(
    State(state): State<AppState>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
) -> impl IntoResponse {
    match state
        .query_engine
        .get_table_parquet_stats(&catalog, &schema, &table)
    {
        Ok(stats) => (StatusCode::OK, axum::Json(serde_json::json!(stats))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({
                "status": "error",
                "message": e
            })),
        ),
    }
}

// ===== Profiling Handlers =====

pub async fn heap_profile_handler(
    AxumQuery(params): AxumQuery<ProfileParams>,
) -> impl IntoResponse {
    use axum::body::Body;
    use axum::http::header;
    use axum::response::Response;

    info!(
        "Heap Profile Request: duration={}s, format={}",
        params.duration, params.format
    );

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
                let filename = format!(
                    "heap_profile_{}.{}",
                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                    format.file_extension()
                );
                response = response.header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", filename),
                );
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

pub async fn cpu_profile_handler(AxumQuery(params): AxumQuery<ProfileParams>) -> impl IntoResponse {
    use axum::body::Body;
    use axum::http::header;
    use axum::response::Response;

    info!(
        "CPU Profile Request: duration={}s, format={}",
        params.duration, params.format
    );

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
                let filename = format!(
                    "cpu_profile_{}.{}",
                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                    format.file_extension()
                );
                response = response.header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", filename),
                );
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
    use crate::metrics::METRICS;

    let query_handle = METRICS.record_query_start();

    match shared::execute_query_impl(&state.query_engine, &db, &sql).await {
        Ok(json) => {
            query_handle.finish(true);
            info!("Query executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            query_handle.finish(false);
            error!("Query error: {}", e);
            (StatusCode::BAD_REQUEST, e)
        }
    }
}

// ===== InfluxQL Query Handler =====

use crate::query::influxql::{
    batches_to_response, get_measurement_name, has_tag_grouping, influxql_to_sql, parse,
    InfluxResponse, TranspileContext,
};

/// InfluxDB-compatible query endpoint (GET /query)
pub async fn influxql_query_handler(
    AxumQuery(params): AxumQuery<InfluxQueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let db = params.db.unwrap_or_else(|| "default".to_string());
    debug!("InfluxQL Query (GET): db={}, q={}", db, params.q);
    execute_influxql_query(db, params.q, state).await
}

/// InfluxDB-compatible query endpoint (POST /query)
pub async fn influxql_query_handler_post(
    AxumQuery(params): AxumQuery<InfluxQueryParams>,
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    // Query can be in body or in q parameter
    let query = if body.starts_with("q=") {
        // URL-encoded form body
        body.strip_prefix("q=")
            .map(|s| urlencoding::decode(s).unwrap_or_default().into_owned())
            .unwrap_or(body)
    } else if !body.is_empty() {
        body
    } else {
        params.q.clone()
    };

    let db = params.db.unwrap_or_else(|| "default".to_string());
    debug!("InfluxQL Query (POST): db={}, q={}", db, query);
    execute_influxql_query(db, query, state).await
}

/// InfluxDB-compatible query endpoint with database in path
pub async fn influxql_query_db_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<InfluxQueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("InfluxQL Query: db={}, q={}", db, params.q);
    execute_influxql_query(db, params.q, state).await
}

async fn execute_influxql_query(
    db: String,
    query: String,
    state: AppState,
) -> impl IntoResponse {
    use crate::metrics::METRICS;

    let query_handle = METRICS.record_query_start();

    // Parse InfluxQL
    let stmt = match parse(&query) {
        Ok(s) => s,
        Err(e) => {
            query_handle.finish(false);
            let response = InfluxResponse::error(0, format!("Parse error: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(response),
            );
        }
    };

    // Get measurement name for response
    let measurement = get_measurement_name(&stmt).unwrap_or_else(|| "result".to_string());

    // Get tag columns for grouping
    let tag_columns = has_tag_grouping(&stmt);

    // Transpile to SQL
    let ctx = TranspileContext {
        database: db.clone(),
        schema: state.config.schema.default_name.clone(),
    };

    let sql = match influxql_to_sql(&query, &ctx) {
        Ok(s) => s,
        Err(e) => {
            query_handle.finish(false);
            let response = InfluxResponse::error(0, format!("Transpile error: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(response),
            );
        }
    };

    debug!("InfluxQL transpiled to SQL: {}", sql);

    // Execute SQL
    match state.query_engine.execute_query_with_db(&sql, &db).await {
        Ok(batches) => {
            query_handle.finish(true);

            // Convert to InfluxDB response format
            let response = if let Some(ref tags) = tag_columns {
                let tag_refs: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
                crate::query::influxql::batches_to_grouped_response(
                    &batches,
                    &measurement,
                    &tag_refs,
                    0,
                )
            } else {
                batches_to_response(&batches, &measurement, 0)
            };

            info!("InfluxQL query executed successfully");
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            query_handle.finish(false);
            error!("InfluxQL query error: {}", e);
            let response = InfluxResponse::error(0, format!("Query error: {}", e));
            (
                StatusCode::BAD_REQUEST,
                Json(response),
            )
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
    debug!(
        "Explain Request: db={}, sql={}, verbose={}, analyze={}",
        db, params.sql, params.verbose, params.analyze
    );

    match shared::explain_query_impl(
        &state.query_engine,
        &db,
        &params.sql,
        params.verbose,
        params.analyze,
    )
    .await
    {
        Ok(json) => {
            info!("Explain executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            error!("Explain error: {}", e);
            (StatusCode::BAD_REQUEST, e)
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

    match shared::explain_query_impl(
        &state.query_engine,
        &db,
        &params.sql,
        params.verbose,
        params.analyze,
    )
    .await
    {
        Ok(json) => {
            info!("Explain executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            error!("Explain error: {}", e);
            (StatusCode::BAD_REQUEST, e)
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

    match shared::analyze_query_impl(&state.query_engine, &db, &params.sql).await {
        Ok(json) => {
            info!("Analyze executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            error!("Analyze error: {}", e);
            (StatusCode::BAD_REQUEST, e)
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

    match shared::analyze_query_impl(&state.query_engine, &db, &body).await {
        Ok(json) => {
            info!("Analyze executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            error!("Analyze error: {}", e);
            (StatusCode::BAD_REQUEST, e)
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
    use crate::metrics::METRICS;

    debug!("Processing write data: {} bytes", data.len());

    let start = std::time::Instant::now();
    let data_len = data.len() as u64;

    match shared::process_write_impl(&state.cache, &state.config, state.wal.as_ref(), db, data)
        .await
    {
        Ok(point_count) => {
            let latency_ms = start.elapsed().as_micros() as f64 / 1000.0;
            METRICS.record_write(point_count as u64, data_len, latency_ms);
            debug!("Added {} points to cache", point_count);
            (StatusCode::NO_CONTENT, "".to_string())
        }
        Err(e) => {
            METRICS.record_write_failure();
            if e.contains("WAL error") {
                error!("WAL write failed: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{}\n", e))
            } else if e.contains("Cache full") {
                error!("Cache write failed: {}", e);
                (StatusCode::SERVICE_UNAVAILABLE, format!("{}\n", e))
            } else {
                (StatusCode::BAD_REQUEST, e)
            }
        }
    }
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

    let config = compactor::CompactionConfig::from(&state.config.compaction);

    match shared::compact_catalog_impl(&state.compactor, &catalog, &config).await {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => {
            error!("Compaction error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e)
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

    let config = compactor::CompactionConfig::from(&state.config.compaction);

    match shared::compact_table_impl(&state.compactor, &catalog, &schema, &table, &config).await {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => {
            error!("Compaction error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e)
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

    let config = compactor::CompactionConfig::from(&state.config.compaction);

    match shared::compact_partition_impl(
        &state.compactor,
        &catalog,
        &schema,
        &table,
        &date,
        &config,
    )
    .await
    {
        Ok(msg) => {
            info!("{}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            error!("Compaction error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e)
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

    match shared::list_partitions_impl(&state.db_manager, &catalog, &schema, &table) {
        Ok(response) => {
            info!("Found {} partition(s)", response["count"]);
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("List partitions error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e)
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

    match shared::delete_partition_impl(&state.db_manager, &catalog, &schema, &table, &date) {
        Ok(msg) => {
            info!("{}", msg);
            (StatusCode::OK, msg)
        }
        Err(e) => {
            error!("Delete partition error: {}", e);
            (StatusCode::NOT_FOUND, e)
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

    match shared::delete_old_partitions_impl(
        &state.db_manager,
        &catalog,
        &schema,
        &table,
        req.older_than_days,
        req.before_date.as_deref(),
    ) {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => {
            if e.contains("Must specify") {
                (StatusCode::BAD_REQUEST, e)
            } else {
                error!("Delete partitions error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e)
            }
        }
    }
}

// ===== Query Tracing & Profiling Handlers =====

#[derive(Deserialize)]
pub struct TraceListParams {
    /// Maximum number of timelines to return (default: 100)
    #[serde(default = "default_trace_limit")]
    pub limit: usize,
}

fn default_trace_limit() -> usize {
    100
}

/// GET /traces/recent - Get recent query timelines
pub async fn recent_traces_handler(
    State(state): State<AppState>,
    AxumQuery(params): AxumQuery<TraceListParams>,
) -> impl IntoResponse {
    debug!("Recent Traces Request: limit={}", params.limit);

    if let Some(ref tracer) = state.query_tracer {
        let timelines = tracer.get_recent_timelines(params.limit);
        (StatusCode::OK, Json(serde_json::json!(timelines)))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Query tracing is not enabled"
            })),
        )
    }
}

/// GET /traces/:query_id - Get a specific query timeline
pub async fn get_trace_handler(
    Path(query_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Get Trace Request: query_id={}", query_id);

    if let Some(ref tracer) = state.query_tracer {
        if let Some(timeline) = tracer.get_timeline(&query_id) {
            (StatusCode::OK, Json(serde_json::json!(timeline)))
        } else {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("Timeline not found: {}", query_id)
                })),
            )
        }
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Query tracing is not enabled"
            })),
        )
    }
}

/// GET /traces/slow - Get recent slow queries
pub async fn slow_queries_handler(
    State(state): State<AppState>,
    AxumQuery(params): AxumQuery<TraceListParams>,
) -> impl IntoResponse {
    debug!("Slow Queries Request: limit={}", params.limit);

    if let Some(ref tracer) = state.query_tracer {
        let slow_queries = tracer.get_recent_slow_queries(params.limit);
        (StatusCode::OK, Json(serde_json::json!(slow_queries)))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Query tracing is not enabled"
            })),
        )
    }
}

/// POST /traces/clear - Clear all trace data
pub async fn clear_traces_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("Clear Traces Request");

    if let Some(ref tracer) = state.query_tracer {
        tracer.clear();
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "Query traces cleared successfully"
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Query tracing is not enabled"
            })),
        )
    }
}

// ===== Prometheus Remote Storage API =====

use axum::body::Bytes;

/// Handle Prometheus remote write requests
/// POST /api/v1/write/:db
pub async fn prometheus_write_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> impl IntoResponse {
    let prom_state = Arc::new(prometheus::remote_write::RemoteWriteState {
        cache: state.cache.clone(),
        default_schema: state.config.schema.default_name.clone(),
    });

    prometheus::remote_write_handler(Path(db), State(prom_state), body).await
}

/// Handle Prometheus remote read requests
/// POST /api/v1/read/:db
pub async fn prometheus_read_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> impl IntoResponse {
    let prom_state = Arc::new(prometheus::remote_read::RemoteReadState {
        query_engine: state.query_engine.clone(),
        default_schema: state.config.schema.default_name.clone(),
    });

    prometheus::remote_read_handler(Path(db), State(prom_state), body).await
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

// ===== Router Builder =====

use super::auth;

/// Build the Axum router with all routes (standalone/data node mode)
pub fn build_router(state: AppState, auth_middleware: Arc<auth::AuthMiddleware>) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/metrics.json", get(metrics_json_handler))
        .route("/version", get(version_handler))
        // Write endpoints
        .route("/write/:db", post(write_handler))
        .route("/write", post(write_handler_query_param))
        // Cache management
        .route("/stats", get(stats_handler))
        .route("/memory", get(memory_handler))
        .route("/flush", post(flush_handler))
        // Query cache management
        .route("/cache/stats", get(query_cache_stats_handler))
        .route("/cache/prewarm/:catalog", post(prewarm_cache_handler))
        .route(
            "/cache/prewarm/:catalog/:schema/:table",
            post(prewarm_table_cache_handler),
        )
        .route("/cache/clear", post(clear_query_cache_handler))
        .route(
            "/cache/clear/results",
            post(clear_query_result_cache_handler),
        )
        .route(
            "/cache/parquet/:catalog/:schema/:table",
            get(table_parquet_stats_handler),
        )
        // Profiling endpoints
        .route("/debug/heap", get(heap_profile_handler))
        .route("/debug/cpu", get(cpu_profile_handler))
        .route("/debug/profile/heap", get(heap_profile_handler))
        .route("/debug/profile/cpu", get(cpu_profile_handler))
        // Query endpoints (SQL)
        .route("/query/:db", get(query_handler))
        .route("/query/:db", post(query_handler_post))
        // InfluxQL query endpoints (InfluxDB-compatible)
        .route("/influxql/query", get(influxql_query_handler))
        .route("/influxql/query", post(influxql_query_handler_post))
        .route("/influxql/query/:db", get(influxql_query_db_handler))
        // Query analysis endpoints
        .route("/explain/:db", get(explain_handler))
        .route("/explain/:db", post(explain_handler_post))
        .route("/analyze/:db", get(analyze_handler))
        .route("/analyze/:db", post(analyze_handler_post))
        // Grafana datasource endpoints
        .route("/grafana/search", get(super::grafana::search_handler))
        .route("/grafana/query", post(super::grafana::query_handler))
        .route("/grafana/tag-keys", get(super::grafana::tag_keys_handler))
        .route("/grafana/tag-values", get(super::grafana::tag_values_handler))
        .route("/grafana/health", get(super::grafana::health_handler))
        // Database management
        .route("/databases", get(list_databases_handler))
        .route("/databases", post(create_database_handler))
        .route("/databases/:db", delete(delete_database_handler))
        .route("/databases/:db", get(get_database_info_handler))
        // Schema/Table endpoints (short paths)
        .route("/schemas/:db", get(list_schemas_handler))
        .route("/tables/:db", get(list_tables_handler))
        .route("/tables/:db/:schema", get(list_schema_tables_handler))
        .route("/schema/:db/:schema/:table", get(schema_handler))
        // Table management
        .route("/databases/:db/tables", post(create_table_handler))
        .route("/databases/:db/tables", get(list_tables_handler))
        .route("/databases/:db/tables/:table", delete(delete_table_handler))
        // Compaction endpoints
        .route("/compact/:catalog", post(compact_handler))
        .route(
            "/compact/:catalog/:schema/:table",
            post(compact_table_handler),
        )
        .route(
            "/compact/:catalog/:schema/:table/:date",
            post(compact_partition_handler),
        )
        // Partition management
        .route(
            "/partitions/:catalog/:schema/:table",
            get(list_partitions_handler),
        )
        .route(
            "/partitions/:catalog/:schema/:table/:date",
            delete(delete_partition_handler),
        )
        .route(
            "/retention/:catalog/:schema/:table",
            post(delete_old_partitions_handler),
        )
        // Query tracing endpoints
        .route("/traces/recent", get(recent_traces_handler))
        .route("/traces/slow", get(slow_queries_handler))
        .route("/traces/:query_id", get(get_trace_handler))
        .route("/traces/clear", post(clear_traces_handler))
        // Prometheus Remote Storage API
        .route("/api/v1/write/:db", post(prometheus_write_handler))
        .route("/api/v1/read/:db", post(prometheus_read_handler))
        .fallback(log_request)
        .with_state(state)
        .layer(axum::middleware::from_fn(auth::auth_middleware_fn))
        .layer(axum::Extension(auth_middleware))
}
