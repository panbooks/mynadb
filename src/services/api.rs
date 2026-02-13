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
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query as AxumQuery, Request, State,
    },
    http::StatusCode,
    response::{
        sse::{Event as SseEvent, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{delete, get, post, put},
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
    /// User store for runtime user management (only set when auth is enabled)
    pub user_store: Option<Arc<dyn crate::auth::UserStore>>,
    /// Role store for custom role resolution
    pub role_store: Option<Arc<crate::auth::RoleStore>>,
    /// Whether authentication is enabled
    pub auth_enabled: bool,
    /// Audit logger for security events
    pub audit_logger: Option<Arc<crate::auth::AuditLogger>>,
    /// Streaming query manager for WebSocket/SSE subscriptions
    pub streaming_manager: Option<Arc<crate::query::streaming::StreamingQueryManager>>,
    /// Downsampling policy storage for automated downsampling
    pub downsampling_storage: Option<Arc<crate::jobs::downsampling::DownsamplingStorage>>,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub sql: String,
}

/// InfluxDB-compatible query parameters
#[derive(Deserialize)]
pub struct InfluxQueryParams {
    /// The InfluxQL query (optional for POST where q is in body)
    #[serde(default)]
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
     InfluxDB-Compatible (Grafana InfluxDB datasource):\n\
       GET    /ping                              - Connection test (returns 204)\n\
       GET    /query?db=<db>&q=<query>&epoch=ms  - InfluxQL query (standard endpoint)\n\
       POST   /query?db=<db>&epoch=ms            - InfluxQL query (POST body)\n\
       GET    /influxql/query?q=<query>&db=<db>  - InfluxQL query (alternative path)\n\
       POST   /influxql/query?db=<db>            - InfluxQL query (alternative path)\n\
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

/// InfluxDB-compatible /ping endpoint
/// Grafana's InfluxDB datasource uses this to test the connection
pub async fn influxdb_ping() -> impl IntoResponse {
    (
        StatusCode::NO_CONTENT,
        [
            ("X-Influxdb-Version", env!("GIT_VERSION")),
        ],
    )
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
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    debug!("Query Request (GET): db={}, sql={}", db, params.sql);
    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    execute_query(db, params.sql, state, ctx).await
}

pub async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<AppState>,
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
    body: String,
) -> impl IntoResponse {
    debug!("Query Request (POST): db={}, sql={}", db, body);
    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    execute_query(db, body, state, ctx).await
}

async fn execute_query(
    db: String,
    sql: String,
    state: AppState,
    auth_ctx: crate::auth::AuthContext,
) -> impl IntoResponse {
    use crate::metrics::METRICS;

    let query_handle = METRICS.record_query_start();

    match shared::execute_query_impl_auth(&state.query_engine, &db, &sql, &auth_ctx).await {
        Ok(json) => {
            query_handle.finish(true);
            info!("Query executed successfully");
            (StatusCode::OK, json)
        }
        Err(e) => {
            query_handle.finish(false);
            if e.contains("Access denied") {
                error!("Query access denied: {}", e);
                (StatusCode::FORBIDDEN, e)
            } else {
                error!("Query error: {}", e);
                (StatusCode::BAD_REQUEST, e)
            }
        }
    }
}

// ===== InfluxQL Query Handler =====

use crate::query::influxql::{self, InfluxResponse};

/// InfluxDB-compatible query endpoint (GET /query)
pub async fn influxql_query_handler(
    AxumQuery(params): AxumQuery<InfluxQueryParams>,
    State(state): State<AppState>,
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    let db = params.db.unwrap_or_else(|| "default".to_string());
    let epoch = params.epoch.clone();
    debug!("InfluxQL Query (GET): db={}, q={}", db, params.q);
    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    execute_influxql_query(db, params.q, epoch, state, ctx).await
}

/// InfluxDB-compatible query endpoint (POST /query)
pub async fn influxql_query_handler_post(
    AxumQuery(params): AxumQuery<InfluxQueryParams>,
    State(state): State<AppState>,
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
    body: String,
) -> impl IntoResponse {
    let query = influxql::extract_query_from_body(&body, Some(&params.q))
        .unwrap_or_else(|| params.q.clone());
    let db = params.db.unwrap_or_else(|| "default".to_string());
    let epoch = params.epoch.clone();
    debug!("InfluxQL Query (POST): db={}, q={}", db, query);
    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    execute_influxql_query(db, query, epoch, state, ctx).await
}

/// InfluxDB-compatible query endpoint with database in path
pub async fn influxql_query_db_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<InfluxQueryParams>,
    State(state): State<AppState>,
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    let epoch = params.epoch.clone();
    debug!("InfluxQL Query: db={}, q={}", db, params.q);
    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    execute_influxql_query(db, params.q, epoch, state, ctx).await
}

async fn execute_influxql_query(
    db: String,
    query: String,
    epoch: Option<String>,
    state: AppState,
    auth_ctx: crate::auth::AuthContext,
) -> impl IntoResponse {
    use crate::metrics::METRICS;
    use crate::query::influxql::Statement;

    let query_handle = METRICS.record_query_start();

    // Check database access
    if !auth_ctx.can_read(&db) {
        query_handle.finish(false);
        let response = InfluxResponse::error(0, format!("Access denied to database: {}", db));
        return (StatusCode::FORBIDDEN, Json(response));
    }

    // Handle metadata queries directly (without going through DataFusion SQL)
    // DataFusion's information_schema only knows about lazily-registered tables,
    // so we scan the filesystem/cache directly for accurate results.
    if let Ok(stmt) = influxql::parse(&query) {
        match stmt {
            Statement::ShowDatabases => {
                query_handle.finish(true);
                let databases = state.query_engine.list_databases().await;
                let response = influxql::build_show_databases_response(databases);
                return (StatusCode::OK, Json(response));
            }
            Statement::ShowMeasurements(ref s) => {
                query_handle.finish(true);
                let target_db = s.database.as_deref().unwrap_or(&db);
                let schema = &state.config.schema.default_name;
                let tables = state.query_engine.list_tables_in_schema(target_db, schema).await.unwrap_or_default();
                let response = influxql::build_show_measurements_response(tables);
                return (StatusCode::OK, Json(response));
            }
            Statement::ShowRetentionPolicies(_) => {
                query_handle.finish(true);
                let response = influxql::build_show_retention_policies_response();
                return (StatusCode::OK, Json(response));
            }
            _ => {}
        }
    }

    // Parse and transpile InfluxQL to SQL
    let prepared = match influxql::prepare_query(&query, &db, &state.config.schema.default_name) {
        Ok(p) => p,
        Err(response) => {
            query_handle.finish(false);
            return (StatusCode::BAD_REQUEST, Json(response));
        }
    };

    debug!("InfluxQL transpiled to SQL: {}", prepared.sql);

    // Execute SQL
    match state.query_engine.execute_query_with_db(&prepared.sql, &db).await {
        Ok(batches) => {
            query_handle.finish(true);
            let response = influxql::format_result(&batches, &prepared, epoch.as_deref());
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            query_handle.finish(false);
            error!("InfluxQL query error: {}", e);
            let response = InfluxResponse::error(0, format!("Query error: {}", e));
            (StatusCode::BAD_REQUEST, Json(response))
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
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    debug!(
        "Explain Request: db={}, sql={}, verbose={}, analyze={}",
        db, params.sql, params.verbose, params.analyze
    );

    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    match shared::explain_query_impl_auth(
        &state.query_engine,
        &db,
        &params.sql,
        params.verbose,
        params.analyze,
        &ctx,
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
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
    axum::Json(params): axum::Json<ExplainParams>,
) -> impl IntoResponse {
    debug!("Explain Request (POST): db={}, sql={}", db, params.sql);

    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    match shared::explain_query_impl_auth(
        &state.query_engine,
        &db,
        &params.sql,
        params.verbose,
        params.analyze,
        &ctx,
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
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    debug!("Analyze Request: db={}, sql={}", db, params.sql);

    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    match shared::analyze_query_impl_auth(&state.query_engine, &db, &params.sql, &ctx).await {
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
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
    body: String,
) -> impl IntoResponse {
    debug!("Analyze Request (POST): db={}, sql={}", db, body);

    let ctx = auth_ctx.map(|e| e.0).unwrap_or_default();
    match shared::analyze_query_impl_auth(&state.query_engine, &db, &body, &ctx).await {
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

    let data = match extract_write_body(req).await {
        Ok(s) => s,
        Err(e) => return e,
    };

    process_write_data(&db, &data, state).await
}

pub async fn write_handler_query_param(
    AxumQuery(params): AxumQuery<WriteQueryParams>,
    State(state): State<AppState>,
    req: Request,
) -> impl IntoResponse {
    debug!("Write Request (InfluxDB-compatible): db={}", params.db);

    let data = match extract_write_body(req).await {
        Ok(s) => s,
        Err(e) => return e,
    };

    process_write_data(&params.db, &data, state).await
}

/// Extract write body from request, handling gzip Content-Encoding.
async fn extract_write_body(req: Request) -> Result<String, (StatusCode, String)> {
    let is_gzip = req
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("gzip"))
        .unwrap_or(false);

    let body = axum::body::to_bytes(req.into_body(), 250 * 1024 * 1024)
        .await
        .map_err(|e| {
            error!("Error reading body: {}", e);
            (StatusCode::BAD_REQUEST, format!("Error reading body: {}", e))
        })?;

    let bytes = if is_gzip {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(&body[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            error!("Gzip decompression failed: {}", e);
            (
                StatusCode::BAD_REQUEST,
                format!("Gzip decompression failed: {}", e),
            )
        })?;
        decompressed
    } else {
        body.to_vec()
    };

    String::from_utf8(bytes).map_err(|e| {
        error!("Invalid UTF-8: {}", e);
        (StatusCode::BAD_REQUEST, format!("Invalid UTF-8: {}", e))
    })
}

async fn process_write_data(db: &str, data: &str, state: AppState) -> (StatusCode, String) {
    use crate::metrics::METRICS;

    debug!("Processing write data: {} bytes", data.len());

    let start = std::time::Instant::now();
    let data_len = data.len() as u64;

    match shared::process_write_impl(&state.cache, &state.config, state.wal.as_ref(), db, data)
        .await
    {
        Ok(result) => {
            let latency_ms = start.elapsed().as_micros() as f64 / 1000.0;
            METRICS.record_write(result.point_count as u64, data_len, latency_ms);
            debug!("Added {} points to cache", result.point_count);

            // Notify streaming subscriptions of the write
            if let Some(ref streaming_mgr) = state.streaming_manager {
                let db_owned = db.to_string();
                let mgr = streaming_mgr.clone();
                let debounce_ms = state.config.streaming.update_debounce_ms;
                tokio::spawn(async move {
                    // Debounce: wait briefly to batch rapid writes
                    if debounce_ms > 0 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(debounce_ms)).await;
                    }
                    for (schema, table) in result.tables {
                        mgr.on_data_written(crate::query::streaming::WriteEvent {
                            catalog: db_owned.clone(),
                            schema,
                            table,
                        })
                        .await;
                    }
                });
            }

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
        Ok(msg) => {
            // Invalidate caches after compaction so queries see fresh data
            state.query_engine.invalidate_database_cache(&catalog);
            (StatusCode::OK, msg)
        }
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
        Ok(msg) => {
            // Invalidate caches after compaction so queries see fresh data
            state.query_engine.invalidate_table_cache(&catalog, &schema, &table);
            (StatusCode::OK, msg)
        }
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
            // Invalidate caches after compaction so queries see fresh data
            state.query_engine.invalidate_table_cache(&catalog, &schema, &table);
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

// ===== User Management Handlers =====

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub databases: Option<Vec<String>>,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct UpdateUserRequest {
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub roles: Option<Vec<String>>,
    #[serde(default)]
    pub databases: Option<Vec<String>>,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct UserResponse {
    pub username: String,
    pub roles: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub databases: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
}

/// GET /auth/users - List all users
pub async fn list_users_handler(
    State(state): State<AppState>,
    axum::Extension(auth_ctx): axum::Extension<crate::auth::AuthContext>,
) -> impl IntoResponse {
    if state.auth_enabled && !auth_ctx.has_permission(crate::auth::Permission::Admin) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Admin permission required"}))).into_response();
    }

    let Some(ref user_store) = state.user_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Authentication is not enabled"}))).into_response();
    };

    let usernames = user_store.list_users();
    let mut users = Vec::new();
    for username in &usernames {
        if let Some(user) = user_store.get_user(username) {
            users.push(UserResponse {
                username: user.username.clone(),
                roles: user.roles.clone(),
                databases: user.databases.clone(),
                tables: user.tables.clone(),
            });
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "users": users,
        "count": users.len()
    }))).into_response()
}

/// GET /auth/users/:username - Get a specific user
pub async fn get_user_handler(
    Path(username): Path<String>,
    State(state): State<AppState>,
    axum::Extension(auth_ctx): axum::Extension<crate::auth::AuthContext>,
) -> impl IntoResponse {
    if state.auth_enabled && !auth_ctx.has_permission(crate::auth::Permission::Admin) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Admin permission required"}))).into_response();
    }

    let Some(ref user_store) = state.user_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Authentication is not enabled"}))).into_response();
    };

    match user_store.get_user(&username) {
        Some(user) => {
            let response = UserResponse {
                username: user.username.clone(),
                roles: user.roles.clone(),
                databases: user.databases.clone(),
                tables: user.tables.clone(),
            };
            (StatusCode::OK, Json(serde_json::json!(response))).into_response()
        }
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": format!("User not found: {}", username)}))).into_response(),
    }
}

/// POST /auth/users - Create a new user
pub async fn create_user_handler(
    State(state): State<AppState>,
    axum::Extension(auth_ctx): axum::Extension<crate::auth::AuthContext>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if state.auth_enabled && !auth_ctx.has_permission(crate::auth::Permission::Admin) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Admin permission required"}))).into_response();
    }

    let Some(ref user_store) = state.user_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Authentication is not enabled"}))).into_response();
    };

    // Check if user already exists
    if user_store.get_user(&req.username).is_some() {
        return (StatusCode::CONFLICT, Json(serde_json::json!({"error": format!("User already exists: {}", req.username)}))).into_response();
    }

    // Hash the password
    let password_hash = match crate::auth::hash_password(&req.password) {
        Ok(h) => h,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": format!("Failed to hash password: {}", e)}))).into_response();
        }
    };

    let mut user = crate::auth::User::new(&req.username, &password_hash, req.roles, req.databases);
    user.tables = req.tables;

    if let Err(e) = user_store.upsert_user(user) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))).into_response();
    }

    // Log audit event
    if let Some(ref audit) = state.audit_logger {
        audit.log_user_management(
            auth_ctx.username().unwrap_or("unknown"),
            "user_created",
            &req.username,
        );
    }

    info!("User created: {}", req.username);
    (StatusCode::CREATED, Json(serde_json::json!({"message": format!("User created: {}", req.username)}))).into_response()
}

/// PUT /auth/users/:username - Update an existing user
pub async fn update_user_handler(
    Path(username): Path<String>,
    State(state): State<AppState>,
    axum::Extension(auth_ctx): axum::Extension<crate::auth::AuthContext>,
    Json(req): Json<UpdateUserRequest>,
) -> impl IntoResponse {
    if state.auth_enabled && !auth_ctx.has_permission(crate::auth::Permission::Admin) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Admin permission required"}))).into_response();
    }

    let Some(ref user_store) = state.user_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Authentication is not enabled"}))).into_response();
    };

    let Some(mut user) = user_store.get_user(&username) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": format!("User not found: {}", username)}))).into_response();
    };

    // Update password if provided
    if let Some(ref password) = req.password {
        match crate::auth::hash_password(password) {
            Ok(h) => user.password_hash = h,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": format!("Failed to hash password: {}", e)}))).into_response();
            }
        }
    }

    // Update roles if provided
    if let Some(roles) = req.roles {
        user.roles = roles;
    }

    // Update databases if provided
    if let Some(databases) = req.databases {
        user.databases = Some(databases);
    }

    // Update tables if provided
    if let Some(tables) = req.tables {
        user.tables = Some(tables);
    }

    if let Err(e) = user_store.upsert_user(user) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))).into_response();
    }

    // Log audit event
    if let Some(ref audit) = state.audit_logger {
        audit.log_user_management(
            auth_ctx.username().unwrap_or("unknown"),
            "user_updated",
            &username,
        );
    }

    info!("User updated: {}", username);
    (StatusCode::OK, Json(serde_json::json!({"message": format!("User updated: {}", username)}))).into_response()
}

/// DELETE /auth/users/:username - Delete a user
pub async fn delete_user_handler(
    Path(username): Path<String>,
    State(state): State<AppState>,
    axum::Extension(auth_ctx): axum::Extension<crate::auth::AuthContext>,
) -> impl IntoResponse {
    if state.auth_enabled && !auth_ctx.has_permission(crate::auth::Permission::Admin) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "Admin permission required"}))).into_response();
    }

    let Some(ref user_store) = state.user_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Authentication is not enabled"}))).into_response();
    };

    // Prevent deleting the last admin user
    if let Some(user) = user_store.get_user(&username) {
        if user.roles.contains(&"admin".to_string()) {
            let admin_count = user_store.list_users().iter().filter(|u| {
                user_store.get_user(u).map_or(false, |user| user.roles.contains(&"admin".to_string()))
            }).count();
            if admin_count <= 1 {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Cannot delete the last admin user"}))).into_response();
            }
        }
    }

    match user_store.remove_user(&username) {
        Ok(true) => {
            // Log audit event
            if let Some(ref audit) = state.audit_logger {
                audit.log_user_management(
                    auth_ctx.username().unwrap_or("unknown"),
                    "user_deleted",
                    &username,
                );
            }

            info!("User deleted: {}", username);
            (StatusCode::OK, Json(serde_json::json!({"message": format!("User deleted: {}", username)}))).into_response()
        }
        Ok(false) => {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": format!("User not found: {}", username)}))).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))).into_response()
        }
    }
}

// ===== Streaming Query Handlers =====

/// WebSocket streaming query endpoint
/// WS /stream/query - Client sends StreamQueryRequest JSON, server pushes results
pub async fn ws_stream_query_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    let ctx = auth_ctx
        .map(|ext| ext.0)
        .unwrap_or_else(crate::auth::AuthContext::anonymous);

    ws.on_upgrade(move |socket| handle_query_websocket(socket, state, ctx))
}

async fn handle_query_websocket(
    socket: WebSocket,
    state: AppState,
    auth_ctx: crate::auth::AuthContext,
) {
    use crate::query::streaming::{StreamMessage, StreamQueryRequest};
    use futures_util::{SinkExt, StreamExt};
    use tokio::time::{interval, Duration};

    let Some(ref streaming_mgr) = state.streaming_manager else {
        return;
    };

    let (mut sender, mut receiver) = socket.split();
    let heartbeat_secs = state.config.streaming.heartbeat_interval_secs;
    let max_per_conn = state.config.streaming.max_subscriptions_per_connection;

    // Track subscriptions for this connection
    let active_subs: Arc<tokio::sync::Mutex<Vec<uuid::Uuid>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let active_subs_cleanup = active_subs.clone();
    let mgr = streaming_mgr.clone();
    let mgr_cleanup = streaming_mgr.clone();

    // Sender channel — multiple tasks forward messages here, one task sends to WS
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel::<String>(256);

    // Task: forward messages from ws_tx to the WebSocket
    let send_task = tokio::spawn(async move {
        while let Some(msg) = ws_rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Task: heartbeat
    let ws_tx_hb = ws_tx.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(heartbeat_secs));
        loop {
            ticker.tick().await;
            let hb = StreamMessage::Heartbeat {
                timestamp: chrono::Utc::now().to_rfc3339(),
            };
            let json = serde_json::to_string(&hb).unwrap_or_default();
            if ws_tx_hb.send(json).await.is_err() {
                break;
            }
        }
    });

    // Task: receive messages from client and create subscriptions
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                let req: StreamQueryRequest = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = serde_json::json!({"type": "error", "data": format!("Invalid request: {}", e)});
                        let _ = ws_tx.send(err.to_string()).await;
                        continue;
                    }
                };

                // Check per-connection limit
                let sub_count = active_subs.lock().await.len();
                if sub_count >= max_per_conn {
                    let err = serde_json::json!({
                        "type": "error",
                        "data": format!("Max subscriptions per connection reached ({})", max_per_conn)
                    });
                    let _ = ws_tx.send(err.to_string()).await;
                    continue;
                }

                // Subscribe
                match mgr.subscribe(&req.database, &req.query, &auth_ctx).await {
                    Ok(handle) => {
                        let sub_id = handle.id;
                        active_subs.lock().await.push(sub_id);

                        // Spawn a task to forward subscription messages to the WS sender
                        let ws_tx_sub = ws_tx.clone();
                        tokio::spawn(async move {
                            let mut rx = handle.receiver;
                            while let Some(msg) = rx.recv().await {
                                let json = serde_json::to_string(&msg).unwrap_or_default();
                                if ws_tx_sub.send(json).await.is_err() {
                                    break;
                                }
                            }
                        });

                        let ack = serde_json::json!({
                            "type": "subscribed",
                            "subscription_id": sub_id.to_string(),
                            "database": req.database,
                            "query": req.query,
                        });
                        let _ = ws_tx.send(ack.to_string()).await;
                    }
                    Err(e) => {
                        let err = serde_json::json!({"type": "error", "data": e});
                        let _ = ws_tx.send(err.to_string()).await;
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // Cleanup: cancel heartbeat, send task, and remove all subscriptions
    heartbeat_task.abort();
    send_task.abort();
    let subs = active_subs_cleanup.lock().await;
    for id in subs.iter() {
        mgr_cleanup.unsubscribe(id);
    }
}

/// SSE streaming query parameters
#[derive(Deserialize)]
pub struct SseStreamParams {
    pub sql: String,
}

/// SSE streaming query endpoint
/// GET /stream/query/:db?sql=SELECT+*+FROM+cpu
pub async fn sse_stream_query_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<SseStreamParams>,
    State(state): State<AppState>,
    auth_ctx: Option<axum::Extension<crate::auth::AuthContext>>,
) -> impl IntoResponse {
    use futures_util::StreamExt;

    let ctx = auth_ctx
        .map(|ext| ext.0)
        .unwrap_or_else(crate::auth::AuthContext::anonymous);

    let Some(ref streaming_mgr) = state.streaming_manager else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Streaming queries not enabled",
        )
            .into_response();
    };

    let handle = match streaming_mgr.subscribe(&db, &params.sql, &ctx).await {
        Ok(h) => h,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    let sub_id = handle.id;
    let mgr = streaming_mgr.clone();

    let stream = tokio_stream::wrappers::ReceiverStream::new(handle.receiver).map(
        move |msg| -> Result<SseEvent, std::convert::Infallible> {
            let json = serde_json::to_string(&msg).unwrap_or_default();
            Ok(SseEvent::default().data(json))
        },
    );

    // Wrap in a stream that cleans up subscription on drop
    let cleanup_stream = StreamWithCleanup {
        inner: Box::pin(stream),
        mgr: Some(mgr),
        sub_id,
    };

    Sse::new(cleanup_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// A stream wrapper that cleans up the subscription when dropped
struct StreamWithCleanup {
    inner: std::pin::Pin<
        Box<
            dyn futures_util::Stream<Item = Result<SseEvent, std::convert::Infallible>>
                + Send,
        >,
    >,
    mgr: Option<Arc<crate::query::streaming::StreamingQueryManager>>,
    sub_id: uuid::Uuid,
}

impl futures_util::Stream for StreamWithCleanup {
    type Item = Result<SseEvent, std::convert::Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for StreamWithCleanup {
    fn drop(&mut self) {
        if let Some(ref mgr) = self.mgr {
            mgr.unsubscribe(&self.sub_id);
        }
    }
}

/// GET /stream/subscriptions - List active streaming subscriptions
pub async fn list_subscriptions_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let Some(ref streaming_mgr) = state.streaming_manager else {
        return Json(serde_json::json!({
            "enabled": false,
            "subscriptions": [],
            "total": 0
        }));
    };

    let subs = streaming_mgr.list_subscriptions();
    Json(serde_json::json!({
        "enabled": true,
        "subscriptions": subs,
        "total": streaming_mgr.subscription_count()
    }))
}

// ===== Downsampling Policy Management =====

/// GET /downsampling/policies - List all downsampling policies
pub async fn list_downsampling_policies_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let Some(ref ds_storage) = state.downsampling_storage else {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "enabled": false,
                "policies": []
            })),
        );
    };

    match ds_storage.list_policies().await {
        Ok(policies) => {
            let mut policy_infos = Vec::new();
            for p in &policies {
                let status = ds_storage.get_status(&p.name).await.unwrap_or(None);
                policy_infos.push(serde_json::json!({
                    "name": p.name,
                    "source_table": p.source_table,
                    "enabled": p.enabled,
                    "tiers": p.tiers.iter().map(|t| serde_json::json!({
                        "name": t.name,
                        "after_days": t.after_days,
                        "interval": t.interval,
                        "aggregates": t.aggregates.iter().map(|a| {
                            if a.function == crate::jobs::downsampling::AggregateFn::Count {
                                "count(*)".to_string()
                            } else {
                                format!("{}({})", a.function.sql_name(), a.column)
                            }
                        }).collect::<Vec<_>>(),
                        "retention_days": t.retention_days,
                    })).collect::<Vec<_>>(),
                    "status": status,
                }));
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "enabled": true,
                    "policies": policy_infos
                })),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

/// POST /downsampling/policies - Create a new downsampling policy
pub async fn create_downsampling_policy_handler(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let Some(ref ds_storage) = state.downsampling_storage else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Downsampling is not enabled"})),
        );
    };

    // Parse request
    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Missing 'name' field"})),
            )
        }
    };

    let source_table = match body.get("source_table").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Missing 'source_table' field"})),
            )
        }
    };

    // Validate source_table format
    if source_table.split('.').count() != 3 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "source_table must be in catalog.schema.table format"})),
        );
    }

    let tiers_arr = match body.get("tiers").and_then(|v| v.as_array()) {
        Some(t) => t,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Missing 'tiers' array"})),
            )
        }
    };

    let mut tiers = Vec::new();
    for tier_val in tiers_arr {
        let tier_name = tier_val
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unnamed")
            .to_string();
        let after_days = tier_val
            .get("after_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(7) as u32;
        let interval = tier_val
            .get("interval")
            .and_then(|v| v.as_str())
            .unwrap_or("1 minute")
            .to_string();
        let retention_days = tier_val
            .get("retention_days")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        let mut aggregates = Vec::new();
        if let Some(agg_arr) = tier_val.get("aggregates").and_then(|v| v.as_array()) {
            for agg_val in agg_arr {
                if let Some(agg_str) = agg_val.as_str() {
                    match crate::jobs::downsampling::types::parse_aggregate_str(agg_str) {
                        Ok(spec) => aggregates.push(spec),
                        Err(e) => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({"error": format!("Invalid aggregate: {}", e)})),
                            )
                        }
                    }
                }
            }
        }

        tiers.push(crate::jobs::downsampling::DownsamplingTier {
            name: tier_name,
            after_days,
            interval,
            aggregates,
            retention_days,
        });
    }

    if tiers.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "At least one tier is required"})),
        );
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    let policy = crate::jobs::downsampling::DownsamplingPolicy {
        name: name.clone(),
        source_table,
        tiers,
        enabled: body.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
        created_at: now,
        modified_at: now,
    };

    match ds_storage.save_policy(&policy).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"status": "created", "name": name})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

/// GET /downsampling/policies/:name - Get a specific policy
pub async fn get_downsampling_policy_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let Some(ref ds_storage) = state.downsampling_storage else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Downsampling is not enabled"})),
        );
    };

    match ds_storage.get_policy(&name).await {
        Ok(Some(policy)) => {
            let status = ds_storage.get_status(&name).await.unwrap_or(None);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "policy": policy,
                    "status": status,
                })),
            )
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Policy not found: {}", name)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

/// DELETE /downsampling/policies/:name - Delete a policy
pub async fn delete_downsampling_policy_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let Some(ref ds_storage) = state.downsampling_storage else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Downsampling is not enabled"})),
        );
    };

    match ds_storage.get_policy(&name).await {
        Ok(Some(_)) => match ds_storage.delete_policy(&name).await {
            Ok(()) => (
                StatusCode::OK,
                Json(serde_json::json!({"status": "deleted", "name": name})),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            ),
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Policy not found: {}", name)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
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

// ===== Router Builder =====

use super::auth;

/// Build the Axum router with all routes (standalone/data node mode)
pub fn build_router(state: AppState, auth_middleware: Arc<auth::AuthMiddleware>) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        .route("/ping", get(influxdb_ping))
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
        // InfluxDB-compatible query endpoints (used by Grafana InfluxDB datasource)
        // GET/POST /query?db=X&q=Y&epoch=ms — standard InfluxDB endpoint
        .route("/query", get(influxql_query_handler))
        .route("/query", post(influxql_query_handler_post))
        // InfluxQL query endpoints (alternative paths)
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
        // User management endpoints (RBAC)
        .route("/auth/users", get(list_users_handler))
        .route("/auth/users", post(create_user_handler))
        .route("/auth/users/:username", get(get_user_handler))
        .route("/auth/users/:username", put(update_user_handler))
        .route("/auth/users/:username", delete(delete_user_handler))
        // Streaming query endpoints (WebSocket & SSE)
        .route("/stream/query", get(ws_stream_query_handler))
        .route("/stream/query/:db", get(sse_stream_query_handler))
        .route("/stream/subscriptions", get(list_subscriptions_handler))
        // Downsampling policy management endpoints
        .route("/downsampling/policies", get(list_downsampling_policies_handler))
        .route("/downsampling/policies", post(create_downsampling_policy_handler))
        .route("/downsampling/policies/:name", get(get_downsampling_policy_handler))
        .route("/downsampling/policies/:name", delete(delete_downsampling_policy_handler))
        .fallback(log_request)
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::max(250 * 1024 * 1024)) // 250MB
        .layer(axum::middleware::from_fn(auth::auth_middleware_fn))
        .layer(axum::Extension(auth_middleware))
}
