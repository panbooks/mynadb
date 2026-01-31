//! Standalone mode runner - single-process, all-in-one
//!
//! This is the default mode when running `mynadb` or `mynadb standalone`.
//! It runs all components (HTTP, Flight, MySQL, WAL, compaction, retention)
//! in a single process with no cluster coordination.

use super::common;
use crate::config::Config;
use crate::jobs::{compactor, database_manager, retention_manager};
use crate::services::{api, auth};
use crate::storage::cache;
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Macro for welcome messages (prints to stdout with colored output)
macro_rules! welcome {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

/// Run MynaDB in standalone mode
pub async fn run(config_path: &str) {
    let config = Arc::new(Config::load_or_default(config_path));

    // Initialize logging
    common::init_logging(&config);

    // Print welcome banner
    print_welcome_banner(&config);
    info!("MynaDB starting with config: {:?}", *config);

    // Create data directory
    std::fs::create_dir_all(&config.storage.data_dir).expect("Failed to create data directory");

    // Initialize storage backend
    let storage = common::init_storage(&config);

    // Initialize WAL
    let wal = common::init_wal(&config, storage.clone()).await;

    // Initialize cache with "standalone" node_id for single-node deployments
    let cache = common::init_data_cache(
        storage.clone(),
        &config,
        wal.as_ref().map(Arc::clone),
        "standalone",
    );

    // Initialize metadata manager
    let metadata_manager = common::init_metadata_manager(&config).await;

    // Initialize query engine FIRST (needed for background flush callback)
    let cache_arc = Arc::new(cache.clone());
    let query_engine = common::init_query_engine(&config, storage.clone(), cache_arc.clone());

    // Start background flush with metadata registration AND query engine refresh
    // This ensures that after flush, DataFusion table registrations are updated
    // so queries can find the new Parquet files
    common::start_background_flush_with_query_engine(
        cache.clone(),
        config.cache.flush_interval_secs,
        metadata_manager.clone(),
        query_engine.clone(),
    );

    // Initialize database manager
    info!("Initializing database manager");
    let db_manager = Arc::new(database_manager::DatabaseManager::with_storage(
        &config.storage.data_dir,
        storage.clone(),
    ));

    // Initialize compactor
    info!("Initializing compactor");
    let compaction_config = compactor::CompactionConfig::from(&config.compaction);
    let compactor = Arc::new(compactor::Compactor::with_storage_and_metadata(
        &config.storage.data_dir,
        storage.clone(),
        &compaction_config,
        metadata_manager.clone(),
    ));

    // Start background compaction if enabled
    if config.compaction.interval_secs > 0 {
        Arc::clone(&compactor).start_background_lsm_compaction(
            config.compaction.interval_secs,
            compaction_config.clone(),
        );
    }

    if config.compaction.full_compaction_enabled
        && config.compaction.full_compaction_interval_secs > 0
    {
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
        welcome!("  WAL disabled, skipping recovery");
    }

    // Initialize retention manager
    info!("Initializing retention manager");
    let retention_manager = Arc::new(retention_manager::RetentionManager::with_storage(
        Arc::clone(&db_manager),
        &config.storage.data_dir,
        storage.clone(),
    ));

    if config.retention.enabled && config.retention.interval_secs > 0 {
        Arc::clone(&retention_manager).start_background_retention(config.retention.clone());
    }

    // Start background WAL tasks
    if let Some(ref wal_arc) = wal {
        wal_arc.start_background_flush();
        start_wal_cleanup_task(wal_arc, &config);
    }

    // Initialize file cache (only for object storage backends)
    let file_cache = common::init_file_cache(&config, &storage).await;

    // Build application state
    let state = api::AppState {
        cache,
        query_engine: query_engine.clone(),
        db_manager,
        compactor,
        config: config.clone(),
        wal,
        metadata_manager: Some(metadata_manager),
        file_cache,
    };

    // Initialize authentication
    let auth_middleware = Arc::new(auth::AuthMiddleware::new(Arc::new(config.auth.clone())));
    info!(
        "Authentication: {} (username: {})",
        if config.auth.enabled {
            "ENABLED"
        } else {
            "DISABLED"
        },
        config.auth.username
    );

    // Build router (standalone mode)
    let app = build_router(state.clone(), auth_middleware);

    print_server_info(&config);

    // Start Arrow Flight server
    let flight_handle =
        common::start_flight_server(&config.flight_addr(), query_engine.clone()).await;
    welcome!("  Arrow Flight server started successfully!");

    // Start MySQL server (if enabled)
    let mysql_handle = if config.server.mysql_enabled {
        let handle = common::start_mysql_server(
            &config.mysql_addr(),
            state.query_engine.clone(),
            config.storage.data_dir.clone(),
            Arc::new(state.cache.clone()),
        );
        welcome!("  MySQL server started successfully!");
        Some(handle)
    } else {
        None
    };

    // Start HTTP server
    let http_handle = common::start_http_server(&config.http_addr(), app).await;
    welcome!("  HTTP server started successfully!");
    welcome!("===============================================");

    // Wait for servers
    tokio::select! {
        _ = flight_handle => {
            error!("Flight server stopped unexpectedly");
        },
        _ = http_handle => {
            error!("HTTP server stopped unexpectedly");
        },
        _ = async {
            if let Some(mysql) = mysql_handle {
                mysql.await.ok();
            } else {
                std::future::pending::<()>().await
            }
        } => {
            error!("MySQL server stopped unexpectedly");
        },
    }
}

/// Print welcome banner
fn print_welcome_banner(config: &Config) {
    welcome!("===============================================");
    welcome!("  MynaDB - The Voice of Your Machines");
    welcome!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    welcome!("  Built: {}", env!("BUILD_TIME"));
    welcome!("===============================================");
    welcome!("");
    welcome!("  Starting server...");
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

/// Print server configuration info
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

/// Perform WAL recovery by replaying unflushed writes
async fn perform_wal_recovery(
    wal: &Arc<crate::storage::wal::Wal>,
    cache: &cache::DataCache,
    query_engine: &Arc<crate::query::query_engine::QueryEngine>,
    config: &Arc<Config>,
) {
    info!("Starting WAL recovery - replaying unflushed writes...");
    welcome!("");
    welcome!("  Replaying WAL files...");

    wal.set_recovery_mode(true);

    let cache_for_recovery = cache.clone();
    let query_engine_for_recovery = Arc::clone(query_engine);
    let config_for_recovery = config.clone();

    let recovered_data = wal
        .replay_data_writes(|db, line_protocol| {
            debug!(
                "WAL recovery: replaying {} bytes to {}",
                line_protocol.len(),
                db
            );

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
                        info!("Recovery flush complete: {} file(s) written", files.len());
                        welcome!(
                            "     Recovery complete: {} Parquet file(s) written",
                            files.len()
                        );

                        wal.set_recovery_mode(false);
                        if let Err(e) = wal.cleanup_old_data_wal_files().await {
                            warn!("Failed to cleanup old WAL files after recovery: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Recovery flush failed: {}", e);
                        welcome!("     Recovery flush failed: {}", e);
                        wal.set_recovery_mode(false);
                    }
                }
            } else {
                info!("WAL recovery: no data writes to replay");
                welcome!("     No WAL entries to replay");
                wal.set_recovery_mode(false);
            }
        }
        Err(e) => {
            error!("WAL recovery failed: {}", e);
            welcome!("     WAL recovery failed: {}", e);
            wal.set_recovery_mode(false);
        }
    }
}

/// Start background WAL cleanup task
fn start_wal_cleanup_task(wal: &Arc<crate::storage::wal::Wal>, config: &Config) {
    if config.wal.cleanup_interval_secs == 0 {
        return;
    }

    let wal_clone = Arc::clone(wal);
    let retention_hours = config.wal.retention_hours;
    let interval_secs = config.wal.cleanup_interval_secs;

    tokio::spawn(async move {
        use tokio::time::{interval, Duration};
        let mut ticker = interval(Duration::from_secs(interval_secs));

        info!(
            "Background WAL cleanup started: every {}s, retention {}h",
            interval_secs, retention_hours
        );

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

/// Build the Axum router with all routes (standalone mode)
fn build_router(state: api::AppState, auth_middleware: Arc<auth::AuthMiddleware>) -> Router {
    Router::new()
        .route("/", get(api::home))
        .route("/health", get(api::health))
        .route("/metrics", get(api::metrics_handler))
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
        .route(
            "/cache/prewarm/:catalog/:schema/:table",
            post(api::prewarm_table_cache_handler),
        )
        .route("/cache/clear", post(api::clear_query_cache_handler))
        .route(
            "/cache/clear/results",
            post(api::clear_query_result_cache_handler),
        )
        .route(
            "/cache/parquet/:catalog/:schema/:table",
            get(api::table_parquet_stats_handler),
        )
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
        // Schema/Table shorthand endpoints
        .route("/tables/:db", get(api::list_tables_handler))
        .route("/schemas/:db", get(api::list_schemas_handler))
        // Schema management (full path)
        .route("/databases/:db/schemas", get(api::list_schemas_handler))
        .route(
            "/databases/:db/schemas/:schema/tables",
            get(api::list_schema_tables_handler),
        )
        // Table management
        .route("/databases/:db/tables", post(api::create_table_handler))
        .route("/databases/:db/tables", get(api::list_tables_handler))
        .route(
            "/databases/:db/tables/:table",
            delete(api::delete_table_handler),
        )
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
