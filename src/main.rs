mod cache;
mod compactor;
mod config;
mod database_manager;
mod flight_service;
mod line_protocol;
mod logger;
mod mysql_service;
mod parquet_writer;
mod query_engine;
mod series_tracker;

use axum::{
    extract::{Path, Query as AxumQuery, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use cache::DataCache;
use clap::Parser;
use compactor::Compactor;
use config::Config;
use database_manager::DatabaseManager;
use flight_service::StatsFlightService;
use mysql_service::MysqlService;
use line_protocol::parse_multiple_lines;
use query_engine::QueryEngine;
use serde::{Deserialize, Serialize};
use series_tracker::SeriesTracker;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{debug, error, info, warn};

/// Stats Service - Time series database with Arrow Flight API
#[derive(Parser, Debug)]
#[command(name = "stats-service")]
#[command(version = "0.1.0")]
#[command(about = "Time series database with Arrow Flight API", long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short = 'c', long = "config", default_value = "config.toml")]
    config: String,
}

#[derive(Clone)]
struct AppState {
    cache: DataCache,
    query_engine: Arc<QueryEngine>,
    db_manager: Arc<DatabaseManager>,
    compactor: Arc<Compactor>,
    series_tracker: Arc<SeriesTracker>,
    config: Arc<Config>,
}

#[derive(Deserialize)]
struct QueryParams {
    sql: String,
}

#[derive(Deserialize)]
struct WriteQueryParams {
    db: String,
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

#[derive(Deserialize)]
struct RetentionRequest {
    /// Delete partitions before this date (YYYY-MM-DD)
    #[serde(default)]
    before_date: Option<String>,
    /// Delete partitions older than N days
    #[serde(default)]
    older_than_days: Option<u32>,
}

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let cli = Cli::parse();

    // Load configuration (CLI arg takes precedence over env var)
    let config_path = cli.config;
    let config = Arc::new(Config::load_or_default(&config_path));

    // Initialize logging
    if let Err(e) = logger::init_logging(&config.logging) {
        eprintln!("Failed to initialize logging: {}", e);
        std::process::exit(1);
    }

    // Welcome messages (only these go to stdout)
    welcome!("═══════════════════════════════════════════════════════");
    welcome!("  📊 Stats Service - Time Series Stats Database");
    welcome!("═══════════════════════════════════════════════════════");
    welcome!("");
    welcome!("🚀 Starting server...");
    welcome!("   HTTP API: http://{}", config.http_addr());
    welcome!("   Flight API: {}", config.flight_addr());
    if config.server.mysql_enabled {
        welcome!("   MySQL API: {}", config.mysql_addr());
    }
    welcome!("   Data Directory: {}", config.storage.data_dir);
    welcome!("   Log Directory: {}", config.logging.log_dir);
    welcome!("   Log Level: {}", config.logging.level);
    welcome!("");

    info!("Stats Service starting with config: {:?}", *config);

    // Create data directory
    std::fs::create_dir_all(&config.storage.data_dir).expect("Failed to create data directory");

    // Initialize components
    info!("Initializing data cache");
    let cache = DataCache::new(&config.storage.data_dir, Arc::new(config.schema.clone()));
    cache
        .clone()
        .start_background_flush(config.cache.flush_interval_secs);

    info!("Initializing query engine");
    let query_engine = Arc::new(QueryEngine::new(&config.storage.data_dir));

    info!("Initializing database manager");
    let db_manager = Arc::new(DatabaseManager::new(&config.storage.data_dir));

    info!("Initializing compactor");
    let compactor = Arc::new(Compactor::new(&config.storage.data_dir));

    // Start background compaction if enabled
    if config.compaction.interval_secs > 0 {
        let compaction_config = compactor::CompactionConfig {
            min_files: config.compaction.min_files,
            major_min_files: config.compaction.major_min_files,
            max_file_size: config.compaction.max_file_size_mb * 1024 * 1024,
            target_file_size: config.compaction.target_file_size_mb * 1024 * 1024,
            max_level: config.compaction.max_level,
        };
        Arc::clone(&compactor)
            .start_background_compaction(config.compaction.interval_secs, compaction_config);
    }

    info!("Initializing series tracker");
    let series_tracker = Arc::new(SeriesTracker::new(
        &config.storage.data_dir,
        Arc::new(config.schema.clone()),
    ));
    Arc::clone(&series_tracker).start_background_flush(config.series_tracker.flush_interval_secs);

    let state = AppState {
        cache,
        query_engine,
        db_manager,
        compactor,
        series_tracker,
        config: config.clone(),
    };

    let app = Router::new()
        .route("/", get(home))
        .route("/health", get(health))
        // Write endpoints
        .route("/write/:db", post(write_handler))
        .route("/write", post(write_handler_query_param)) // InfluxDB-compatible
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
        // Schema (service) management
        .route("/databases/:db/schemas", get(list_schemas_handler))
        .route(
            "/databases/:db/schemas/:schema/tables",
            get(list_schema_tables_handler),
        )
        // Table management
        .route("/databases/:db/tables", post(create_table_handler))
        .route("/databases/:db/tables", get(list_tables_handler))
        .route("/databases/:db/tables/:table", delete(delete_table_handler))
        .route("/schema/:db/:table", get(schema_handler))
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
        // Partition management endpoints
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
        // Series cardinality tracking
        .route("/pmc/:db", get(series_stats_handler))
        .fallback(log_request)
        .with_state(state.clone());

    // Start HTTP server
    let http_addr = config.http_addr();
    let http_listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();

    info!("HTTP REST API bound to: {}", http_addr);
    info!("Arrow Flight API will bind to: {}", config.flight_addr());
    info!("Data directory: {}", config.storage.data_dir);
    info!(
        "Cache flush interval: {} seconds",
        config.cache.flush_interval_secs
    );
    info!(
        "Series tracker flush interval: {} seconds",
        config.series_tracker.flush_interval_secs
    );
    if config.compaction.interval_secs > 0 {
        info!(
            "Background compaction interval: {} seconds",
            config.compaction.interval_secs
        );
    } else {
        info!("Background compaction: disabled");
    }

    // Start Arrow Flight server
    let flight_service = StatsFlightService::new(state.query_engine.clone());
    let flight_addr = config.flight_addr().parse().unwrap();

    let flight_server = tokio::spawn(async move {
        info!("Arrow Flight server starting on {}", flight_addr);
        Server::builder()
            .add_service(flight_service.into_server())
            .serve(flight_addr)
            .await
            .expect("Flight server failed");
    });

    // Start MySQL server (if enabled)
    let mysql_server = if config.server.mysql_enabled {
        let mysql_service = MysqlService::new(
            state.query_engine.clone(),
            tokio::runtime::Handle::current(),
            config.storage.data_dir.clone(),
        );
        let mysql_addr = config.mysql_addr();
        Some(tokio::task::spawn_blocking(move || {
            info!("MySQL server starting on {}", mysql_addr);
            if let Err(e) = mysql_service.run(&mysql_addr) {
                error!("MySQL server error: {}", e);
            }
        }))
    } else {
        None
    };

    // Start HTTP server
    let http_addr_display = http_addr.clone();
    let http_server = tokio::spawn(async move {
        info!("HTTP server starting on {}", http_addr_display);
        welcome!("✅ Server started successfully!");
        welcome!("═══════════════════════════════════════════════════════");
        axum::serve(http_listener, app).await.unwrap();
    });

    // Wait for servers
    tokio::select! {
        _ = flight_server => {
            error!("Flight server stopped unexpectedly");
            welcome!("❌ Flight server stopped");
        },
        _ = http_server => {
            error!("HTTP server stopped unexpectedly");
            welcome!("❌ HTTP server stopped");
        },
        _ = async {
            if let Some(mysql) = mysql_server {
                mysql.await.ok();
            } else {
                // If MySQL is not enabled, this future never completes
                std::future::pending::<()>().await
            }
        } => {
            error!("MySQL server stopped unexpectedly");
            welcome!("❌ MySQL server stopped");
        },
    }
}

async fn home() -> &'static str {
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
       POST   /flush                  - Flush cache to disk\n\
       GET    /health                 - Health check\n\n\
     Compaction:\n\
       POST   /compact/:catalog       - Compact all tables in catalog\n\
       POST   /compact/:catalog/:schema/:table - Compact all partitions in table\n\
       POST   /compact/:catalog/:schema/:table/:date - Compact specific partition\n\n\
     Partition Management:\n\
       GET    /partitions/:catalog/:schema/:table - List all partitions\n\
       DELETE /partitions/:catalog/:schema/:table/:date - Delete specific partition\n\
       POST   /retention/:catalog/:schema/:table - Delete old partitions (body: {older_than_days: N} or {before_date: \"YYYY-MM-DD\"})\n"
}

// ===== Database Management Handlers =====

async fn list_databases_handler(State(state): State<AppState>) -> impl IntoResponse {
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

async fn create_database_handler(
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

async fn delete_database_handler(
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

async fn get_database_info_handler(
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

async fn create_table_handler(
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

async fn delete_table_handler(
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

async fn query_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Query Request (GET): db={}, sql={}", db, params.sql);

    execute_query(db, params.sql, state).await
}

async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    debug!("Query Request (POST): db={}, sql={}", db, body);

    execute_query(db, body, state).await
}

async fn execute_query(db: String, sql: String, state: AppState) -> impl IntoResponse {
    // Auto-register database tables (simplified!)
    if let Err(e) = state.query_engine.ensure_database_registered(&db).await {
        error!("Error auto-registering database: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e));
    }

    // Execute query with database context (auto-prefix table names)
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

async fn list_tables_handler(
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

async fn schema_handler(
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

async fn write_handler(
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

async fn write_handler_query_param(
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
    debug!("Processing write data: {} bytes", data.len());

    // Parse line protocol
    let points = parse_multiple_lines(data);
    debug!("Parsed {} points", points.len());

    if points.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "No valid points to write".to_string(),
        );
    }

    // Track series cardinality (batch operation, in-memory cache only)
    state.series_tracker.track_points(db, &points).await;

    let mut grouped: HashMap<(String, String), Vec<_>> = HashMap::new();
    for point in points {
        let schema = state.cache.extract_schema(&point.tags);
        let key = (schema, point.measurement.clone());
        grouped.entry(key).or_default().push(point);
    }

    // Add to cache and register catalog.schema
    for ((schema, measurement), points) in grouped {
        debug!(
            "Adding {} points for catalog={}, schema={}, table={}",
            points.len(),
            db,
            schema,
            measurement
        );

        // Ensure catalog.schema is registered in query engine
        if let Err(e) = state.query_engine.ensure_schema(db, &schema).await {
            warn!("Failed to register schema {}.{}: {}", db, schema, e);
        }

        state
            .cache
            .add_points(db, &schema, &measurement, points)
            .await;
    }

    (StatusCode::OK, "Data added to cache\n".to_string())
}

async fn log_request(req: Request) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();

    warn!("Endpoint not found: {} {}", method, uri);

    (
        StatusCode::NOT_FOUND,
        format!("Endpoint not found: {} {}", method, uri),
    )
}

// ===== Compaction Handlers =====

async fn compact_handler(
    Path(catalog): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("Compact Catalog Request: catalog={}", catalog);

    // Convert config to compactor's CompactionConfig
    let config = compactor::CompactionConfig {
        min_files: state.config.compaction.min_files,
        major_min_files: state.config.compaction.major_min_files,
        max_file_size: state.config.compaction.max_file_size_mb * 1024 * 1024,
        target_file_size: state.config.compaction.target_file_size_mb * 1024 * 1024,
        max_level: state.config.compaction.max_level,
    };

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

async fn compact_table_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!(
        "Compact Table Request: catalog={}, schema={}, table={}",
        catalog, schema, table
    );

    // Convert config to compactor's CompactionConfig
    let config = compactor::CompactionConfig {
        min_files: state.config.compaction.min_files,
        major_min_files: state.config.compaction.major_min_files,
        max_file_size: state.config.compaction.max_file_size_mb * 1024 * 1024,
        target_file_size: state.config.compaction.target_file_size_mb * 1024 * 1024,
        max_level: state.config.compaction.max_level,
    };

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

async fn compact_partition_handler(
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
    let config = compactor::CompactionConfig {
        min_files: state.config.compaction.min_files,
        major_min_files: state.config.compaction.major_min_files,
        max_file_size: state.config.compaction.max_file_size_mb * 1024 * 1024,
        target_file_size: state.config.compaction.target_file_size_mb * 1024 * 1024,
        max_level: state.config.compaction.max_level,
    };

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

// ===== Series Cardinality Handler =====

async fn series_stats_handler(
    Path(db): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    debug!("Series Stats Request: db={}", db);

    match state.series_tracker.get_series_stats(&db).await {
        Ok(stats) => {
            if stats.is_empty() {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "database": db,
                        "services": {},
                        "total_series": 0,
                        "message": "No series data available"
                    }))
                    .to_string(),
                );
            }

            // Calculate total series across all services
            let total_series: usize = stats.values().map(|s| s.total_series).sum();

            // Format response
            let response = serde_json::json!({
                "database": db,
                "total_series": total_series,
                "service_count": stats.len(),
                "services": stats.iter().map(|(service, stats)| {
                    serde_json::json!({
                        "service": service,
                        "total_series": stats.total_series,
                        "metrics": stats.metrics
                    })
                }).collect::<Vec<_>>()
            });

            info!(
                "Returned stats for {} services, {} total series",
                stats.len(),
                total_series
            );
            (
                StatusCode::OK,
                serde_json::to_string_pretty(&response).unwrap(),
            )
        }
        Err(e) => {
            error!("Error getting series stats: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error getting series stats: {}", e),
            )
        }
    }
}

// ===== Schema and Table Listing Handlers =====

async fn list_schemas_handler(
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

async fn list_schema_tables_handler(
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

// ===== Partition Management Handlers =====

async fn list_partitions_handler(
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

async fn delete_partition_handler(
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

async fn delete_old_partitions_handler(
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
