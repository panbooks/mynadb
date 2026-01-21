mod api;
mod auth;
mod cache;
mod compactor;
mod config;
mod database_manager;
mod flight_service;
mod line_protocol;
mod logger;
mod mysql_service;
mod parquet_writer;
mod profiling;
mod query_cache;
mod query_engine;
mod rate_limiter;
mod retention_manager;
mod storage;
mod wal;

use axum::{
    routing::{delete, get, post},
    Router,
};
use clap::{Parser, Subcommand};
use config::Config;
use flight_service::StatsFlightService;
use mysql_service::MysqlService;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{debug, error, info, warn};

/// Version information compiled at build time
pub mod version {
    pub const GIT_VERSION: &str = env!("GIT_VERSION");
    pub const GIT_HASH: &str = env!("GIT_HASH");
    pub const GIT_COMMIT_DATE: &str = env!("GIT_COMMIT_DATE");
    pub const BUILD_TIME: &str = env!("BUILD_TIME");
    pub const RUSTC_VERSION: &str = env!("RUSTC_VERSION");
    pub const CARGO_VERSION: &str = env!("CARGO_PKG_VERSION");

    pub fn print_version() {
        println!("Stats Service");
        println!("  Version:       {}", GIT_VERSION);
        println!("  Git Hash:      {}", GIT_HASH);
        println!("  Commit Date:   {}", GIT_COMMIT_DATE);
        println!("  Build Time:    {}", BUILD_TIME);
        println!("  Cargo Version: {}", CARGO_VERSION);
        println!("  Rust Version:  {}", RUSTC_VERSION);
    }
}

/// Stats Service - Time series database with Arrow Flight API
#[derive(Parser, Debug)]
#[command(name = "stats-service")]
#[command(version = env!("GIT_VERSION"))]
#[command(about = "Time series database with Arrow Flight API", long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short = 'c', long = "config", default_value = "config.toml")]
    config: String,

    /// Subcommand to execute
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show version information
    Version,
}

/// Initialize and print welcome banner
fn print_welcome_banner(config: &Config) {
    welcome!("═══════════════════════════════════════════════════════");
    welcome!("  📊 Stats Service - Time Series Stats Database");
    welcome!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    welcome!("  Built: {}", env!("BUILD_TIME"));
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
}

/// Initialize WAL (Write-Ahead Log) with object store support
async fn initialize_wal(config: &Config, storage: Arc<storage::Storage>) -> Option<Arc<wal::Wal>> {
    if !config.wal.enabled {
        info!("WAL is disabled");
        return None;
    }

    info!(
        "Initializing WAL at {} (sync_on_write={}, rotate_size={}MB, buffer_entries={})",
        config.wal.wal_dir,
        config.wal.sync_on_write,
        config.wal.rotate_size_mb,
        config.wal.buffer_entries
    );

    match wal::Wal::new(storage, &config.wal).await {
        Ok(w) => {
            info!("WAL initialized successfully - recovery will happen after components initialize");
            Some(Arc::new(w))
        }
        Err(e) => {
            error!("Failed to initialize WAL: {}", e);
            None
        }
    }
}

/// Perform WAL recovery by replaying unflushed writes
async fn perform_wal_recovery(
    wal: &Arc<wal::Wal>,
    cache: &cache::DataCache,
    query_engine: &Arc<query_engine::QueryEngine>,
    config: &Arc<Config>,
) {
    info!("Starting WAL recovery - replaying unflushed writes...");
    welcome!("");
    welcome!("🔄 Replaying WAL files...");

    wal.set_recovery_mode(true);

    let cache_for_recovery = cache.clone();
    let query_engine_for_recovery = Arc::clone(query_engine);
    let config_for_recovery = config.clone();

    let recovered_data = wal
        .replay_data_writes(|db, line_protocol| {
            debug!("WAL recovery: replaying {} bytes to {}", line_protocol.len(), db);

            match tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(api::ingest_line_protocol_to_cache(
                    db,
                    line_protocol,
                    &cache_for_recovery,
                    Some(&query_engine_for_recovery),
                    &config_for_recovery.schema,
                ))
            }) {
                Ok(count) => {
                    debug!("WAL recovery: replayed {} points to {}", count, db);
                    Ok(())
                }
                Err(e) => {
                    error!("WAL recovery failed for {}: {}", db, e);
                    Err(e)
                }
            }
        })
        .await;

    match recovered_data {
        Ok(count) => {
            if count > 0 {
                info!("WAL recovery: replayed {} data write entries", count);
                welcome!("   Replayed {} data entries", count);

                info!("Flushing recovered data to Parquet for durability...");
                welcome!("   Flushing recovered data to Parquet...");
                match cache.flush().await {
                    Ok(files) => {
                        info!("Recovery flush complete: {} file(s) written", files);
                        welcome!("   ✅ Recovery complete: {} Parquet file(s) written", files);

                        wal.set_recovery_mode(false);
                        if let Err(e) = wal.cleanup_old_data_wal_files().await {
                            warn!("Failed to cleanup old WAL files after recovery: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Recovery flush failed: {}", e);
                        welcome!("   ⚠️  Recovery flush failed: {}", e);
                        wal.set_recovery_mode(false);
                    }
                }
            } else {
                info!("WAL recovery: no data writes to replay");
                welcome!("   ✅ No WAL entries to replay");
                wal.set_recovery_mode(false);
            }
        }
        Err(e) => {
            error!("WAL recovery failed: {}", e);
            welcome!("   ⚠️  WAL recovery failed: {}", e);
            wal.set_recovery_mode(false);
        }
    }
}

/// Start background WAL cleanup task
fn start_wal_cleanup_task(wal: &Arc<wal::Wal>, config: &Config) {
    if config.wal.cleanup_interval_secs == 0 {
        return;
    }

    let wal_clone = Arc::clone(wal);
    let retention_hours = config.wal.retention_hours;
    let interval_secs = config.wal.cleanup_interval_secs;

    tokio::spawn(async move {
        use tokio::time::{interval, Duration};
        let mut ticker = interval(Duration::from_secs(interval_secs));

        info!("Background WAL cleanup started: every {}s, retention {}h", interval_secs, retention_hours);

        loop {
            ticker.tick().await;

            debug!("Running WAL cleanup...");
            match wal_clone.cleanup_old_files(retention_hours).await {
                Ok(count) if count > 0 => {
                    info!("WAL cleanup: deleted {} old files", count);
                }
                Ok(_) => {
                    debug!("WAL cleanup: no files to delete");
                }
                Err(e) => {
                    error!("WAL cleanup error: {}", e);
                }
            }
        }
    });
}

/// Build the Axum router with all routes
fn build_router(
    state: api::AppState,
    auth_middleware: Arc<auth::AuthMiddleware>,
) -> Router {
    Router::new()
        .route("/", get(api::home))
        .route("/health", get(api::health))
        .route("/version", get(api::version_handler))
        // Write endpoints
        .route("/write/:db", post(api::write_handler))
        .route("/write", post(api::write_handler_query_param))
        // Cache management
        .route("/stats", get(api::stats_handler))
        .route("/memory", get(api::memory_handler))
        .route("/flush", post(api::flush_handler))
        // Query cache management
        .route("/cache/stats", get(api::query_cache_stats_handler))
        .route("/cache/prewarm/:catalog", post(api::prewarm_cache_handler))
        .route("/cache/prewarm/:catalog/:schema/:table", post(api::prewarm_table_cache_handler))
        .route("/cache/clear", post(api::clear_query_cache_handler))
        .route("/cache/clear/results", post(api::clear_query_result_cache_handler))
        .route("/cache/parquet/:catalog/:schema/:table", get(api::table_parquet_stats_handler))
        // Profiling endpoints
        .route("/debug/heap", get(api::heap_profile_handler))
        .route("/debug/cpu", get(api::cpu_profile_handler))
        .route("/debug/profile/heap", get(api::heap_profile_handler))
        .route("/debug/profile/cpu", get(api::cpu_profile_handler))
        // Query endpoints
        .route("/query/:db", get(api::query_handler))
        .route("/query/:db", post(api::query_handler_post))
        // Query analysis endpoints
        .route("/explain/:db", get(api::explain_handler))
        .route("/explain/:db", post(api::explain_handler_post))
        .route("/analyze/:db", get(api::analyze_handler))
        .route("/analyze/:db", post(api::analyze_handler_post))
        // Database management
        .route("/databases", get(api::list_databases_handler))
        .route("/databases", post(api::create_database_handler))
        .route("/databases/:db", delete(api::delete_database_handler))
        .route("/databases/:db", get(api::get_database_info_handler))
        // Schema management
        .route("/databases/:db/schemas", get(api::list_schemas_handler))
        .route(
            "/databases/:db/schemas/:schema/tables",
            get(api::list_schema_tables_handler),
        )
        // Table management
        .route("/databases/:db/tables", post(api::create_table_handler))
        .route("/databases/:db/tables", get(api::list_tables_handler))
        .route("/databases/:db/tables/:table", delete(api::delete_table_handler))
        .route("/schema/:db/:table", get(api::schema_handler))
        // Compaction endpoints
        .route("/compact/:catalog", post(api::compact_handler))
        .route(
            "/compact/:catalog/:schema/:table",
            post(api::compact_table_handler),
        )
        .route(
            "/compact/:catalog/:schema/:table/:date",
            post(api::compact_partition_handler),
        )
        // Partition management
        .route(
            "/partitions/:catalog/:schema/:table",
            get(api::list_partitions_handler),
        )
        .route(
            "/partitions/:catalog/:schema/:table/:date",
            delete(api::delete_partition_handler),
        )
        .route(
            "/retention/:catalog/:schema/:table",
            post(api::delete_old_partitions_handler),
        )
        .fallback(api::log_request)
        .with_state(state)
        .layer(axum::middleware::from_fn(auth::auth_middleware_fn))
        .layer(axum::Extension(auth_middleware))
}

/// Print server configuration information
fn print_server_info(config: &Config) {
    info!("HTTP REST API bound to: {}", config.http_addr());
    info!("Arrow Flight API will bind to: {}", config.flight_addr());
    info!("Data directory: {}", config.storage.data_dir);
    info!(
        "Cache flush interval: {} seconds",
        config.cache.flush_interval_secs
    );
    if config.compaction.interval_secs > 0 {
        info!(
            "Background compaction interval: {} seconds",
            config.compaction.interval_secs
        );
    } else {
        info!("Background compaction: disabled");
    }
    if config.retention.enabled && config.retention.interval_secs > 0 {
        info!(
            "Background retention: enabled (interval: {}s, retention: {} days)",
            config.retention.interval_secs, config.retention.default_days
        );
    } else {
        info!("Background retention: disabled");
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Some(Commands::Version) = cli.command {
        version::print_version();
        return;
    }

    let config = Arc::new(Config::load_or_default(&cli.config));

    if let Err(e) = logger::init_logging(&config.logging) {
        eprintln!("Failed to initialize logging: {}", e);
        std::process::exit(1);
    }

    print_welcome_banner(&config);
    info!("Stats Service starting with config: {:?}", *config);

    std::fs::create_dir_all(&config.storage.data_dir).expect("Failed to create data directory");

    // Initialize storage backend first (WAL depends on it)
    info!(
        "Initializing storage backend: {}",
        config.storage.object_store.backend
    );
    let storage = Arc::new(
        storage::Storage::from_config(&config.storage.object_store)
            .expect("Failed to initialize storage"),
    );

    // Initialize WAL (requires storage)
    let wal = initialize_wal(&config, storage.clone()).await;

    // Initialize cache
    info!(
        "Initializing data cache (max: {}MB, flush threshold: 80%)",
        config.cache.max_size_mb
    );
    let cache = cache::DataCache::new(
        storage.clone(),
        config.cache.max_size_mb,
        wal.as_ref().map(Arc::clone),
    );
    cache
        .clone()
        .start_background_flush(config.cache.flush_interval_secs);

    // Initialize core components
    info!("Initializing query engine");
    let query_cache_config = query_cache::QueryCacheConfig::from(&config.query_cache);
    let query_engine = Arc::new(query_engine::QueryEngine::new_with_query_cache(
        &config.storage.data_dir,
        Some(storage.clone()),
        query_cache_config,
    ));

    info!("Initializing database manager");
    let db_manager = Arc::new(database_manager::DatabaseManager::with_storage(
        &config.storage.data_dir,
        storage.clone(),
    ));

    info!("Initializing compactor");
    let compaction_config = compactor::CompactionConfig::from(&config.compaction);
    let compactor = Arc::new(compactor::Compactor::with_storage(
        &config.storage.data_dir,
        storage.clone(),
        &compaction_config,
    ));

    if config.compaction.interval_secs > 0 {
        Arc::clone(&compactor).start_background_lsm_compaction(
            config.compaction.interval_secs,
            compaction_config.clone(),
        );
    }

    if config.compaction.full_compaction_enabled && config.compaction.full_compaction_interval_secs > 0 {
        Arc::clone(&compactor).start_background_full_compaction(
            config.compaction.full_compaction_interval_secs,
            compaction_config,
        );
    }

    // WAL Recovery
    if let Some(ref wal_arc) = wal {
        perform_wal_recovery(wal_arc, &cache, &query_engine, &config).await;
    } else {
        info!("WAL disabled, skipping recovery");
        welcome!("");
        welcome!("ℹ️  WAL disabled, skipping recovery");
    }

    // Initialize retention manager
    info!("Initializing retention manager");
    let retention_manager = Arc::new(retention_manager::RetentionManager::with_storage(
        Arc::clone(&db_manager),
        &config.storage.data_dir,
        storage.clone(),
    ));

    if config.retention.enabled && config.retention.interval_secs > 0 {
        Arc::clone(&retention_manager)
            .start_background_retention(config.retention.clone());
    }

    // Start background WAL tasks
    if let Some(ref wal_arc) = wal {
        // Start periodic flush (every second)
        wal_arc.start_background_flush();
        // Start cleanup task
        start_wal_cleanup_task(wal_arc, &config);
    }

    // Build application state
    let state = api::AppState {
        cache,
        query_engine: query_engine.clone(),
        db_manager,
        compactor,
        config: config.clone(),
        wal,
    };

    // Initialize authentication
    let auth_middleware = Arc::new(auth::AuthMiddleware::new(Arc::new(config.auth.clone())));
    info!(
        "Authentication: {} (username: {})",
        if config.auth.enabled { "ENABLED" } else { "DISABLED" },
        config.auth.username
    );

    let app = build_router(state.clone(), auth_middleware);

    print_server_info(&config);

    // Start Arrow Flight server
    let flight_service = StatsFlightService::new(query_engine);
    let flight_addr = config.flight_addr().parse().unwrap();
    let flight_server = tokio::spawn(async move {
        info!("Arrow Flight server starting on {}", flight_addr);
        welcome!("✅ Arrow Flight server started successfully!");
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
            welcome!("✅ MySQL server started successfully!");
            if let Err(e) = mysql_service.run(&mysql_addr) {
                error!("MySQL server error: {}", e);
            }
        }))
    } else {
        None
    };

    // Start HTTP server
    let http_addr = config.http_addr();
    let http_listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
    let http_addr_display = http_addr.clone();
    let http_server = tokio::spawn(async move {
        info!("HTTP server starting on {}", http_addr_display);
        welcome!("✅ HTTP server started successfully!");
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
                std::future::pending::<()>().await
            }
        } => {
            error!("MySQL server stopped unexpectedly");
            welcome!("❌ MySQL server stopped");
        },
    }
}
