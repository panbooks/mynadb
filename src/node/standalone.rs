//! Standalone mode runner - single-process, all-in-one
//!
//! This is the default mode when running `mynadb` or `mynadb standalone`.
//! It runs all components (HTTP, Flight, MySQL, WAL, compaction, retention)
//! in a single process with no cluster coordination.
//!
//! Usage:
//! - `mynadb` or `mynadb standalone` - Start in standalone mode
//! - `mynadb stop` or `mynadb standalone stop` - Stop a running instance

use super::common;
use crate::config::Config;
use crate::jobs::{compactor, database_manager, recompression, retention_manager};
use crate::services::{api, auth};
use crate::storage::cache;
use crate::storage::compression::CompressionStrategy;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

const NODE_TYPE: &str = "standalone";

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

    // Create data directory with standardized layout
    let layout = crate::storage::layout::DataLayout::new(&config.storage.data_dir);
    layout.ensure_dirs().expect("Failed to create data directory layout");

    // Write PID file for stop command
    if let Err(e) = common::write_pid_file(&config.storage.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Initialize storage backend
    let storage = common::init_storage(&config);

    // Initialize WAL
    let wal = common::init_wal(&config, storage.clone()).await;

    // Initialize compression strategy from config
    let compression_strategy = CompressionStrategy::from_config(&config.storage.compression);
    if compression_strategy.is_adaptive() {
        info!("Compression strategy: ADAPTIVE (per-column, age-based)");
    } else {
        info!("Compression strategy: FIXED");
    }

    // Initialize cache with "standalone" node_id for single-node deployments
    let mut cache = common::init_data_cache(
        storage.clone(),
        &config,
        wal.as_ref().map(Arc::clone),
        "standalone",
    );
    cache.set_compression_strategy(compression_strategy.clone());

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

    // Initialize compactor (with compression strategy)
    info!("Initializing compactor");
    let compaction_config = compactor::CompactionConfig::from(&config.compaction);
    let compactor = Arc::new(compactor::Compactor::with_metadata_and_compression(
        storage.clone(),
        &compaction_config,
        Some(metadata_manager.clone()),
        compression_strategy.clone(),
    ));

    // Start background compaction if enabled
    // Create cache invalidation callback so compaction invalidates stale caches
    let qe_for_compaction = query_engine.clone();
    let compaction_cache_invalidation: Option<compactor::CacheInvalidationFn> =
        Some(Arc::new(move |catalog: &str, schema: &str, table: &str| {
            qe_for_compaction.invalidate_table_cache(catalog, schema, table);
        }));

    if config.compaction.interval_secs > 0 {
        Arc::clone(&compactor).start_background_lsm_compaction_with_callback(
            config.compaction.interval_secs,
            compaction_config.clone(),
            compaction_cache_invalidation.clone(),
        );
    }

    if config.compaction.full_compaction_enabled
        && config.compaction.full_compaction_interval_secs > 0
    {
        Arc::clone(&compactor).start_background_full_compaction_with_callback(
            config.compaction.full_compaction_interval_secs,
            compaction_config,
            compaction_cache_invalidation,
        );
    }

    // WAL Recovery
    if let Some(ref wal_arc) = wal {
        welcome!("");
        welcome!("  Replaying WAL files...");
        common::perform_wal_recovery(wal_arc, &cache, &query_engine, &config).await;
        welcome!("  WAL recovery complete");
    } else {
        info!("WAL disabled, skipping recovery");
        welcome!("");
        welcome!("  WAL disabled, skipping recovery");
    }

    // Initialize retention manager
    info!("Initializing retention manager");
    let retention_manager = Arc::new(retention_manager::RetentionManager::new(
        Arc::clone(&db_manager),
        storage.clone(),
    ));

    if config.retention.enabled && config.retention.interval_secs > 0 {
        Arc::clone(&retention_manager).start_background_retention(config.retention.clone());
    }

    // Start background recompression job (only for adaptive compression)
    if config.storage.compression.recompression_enabled && compression_strategy.is_adaptive() {
        let recompression_job = Arc::new(recompression::RecompressionJob::new(
            storage.clone(),
            db_manager.clone(),
            compression_strategy.clone(),
            config.storage.compression.recompression_interval_secs,
        ));
        recompression_job.start_background();
        info!(
            "Recompression: ENABLED (interval: {}s)",
            config.storage.compression.recompression_interval_secs
        );
    } else {
        debug!("Recompression: DISABLED");
    }

    // Start background metrics updater
    info!("Starting metrics updater");
    let metrics_config = Arc::clone(&config);
    let metrics_cache = Arc::clone(&cache_arc);
    let metrics_wal = wal.clone();
    tokio::spawn(async move {
        crate::metrics::start_metrics_updater(metrics_config, metrics_cache, metrics_wal).await;
    });

    // Start system metrics collector
    if config.system_metrics.enabled {
        let sys_cache = Arc::clone(&cache_arc);
        let sys_config = Arc::clone(&config);
        tokio::spawn(crate::metrics::system_collector::start(sys_cache, sys_config));
        info!("System metrics: ENABLED (db: {}, interval: {}s)", config.system_metrics.database, config.system_metrics.interval_secs);
    } else {
        debug!("System metrics: DISABLED");
    }

    // Start background WAL tasks
    if let Some(ref wal_arc) = wal {
        wal_arc.start_background_flush();
        start_wal_cleanup_task(wal_arc, &config);
    }

    // Initialize file cache (only for object storage backends)
    let file_cache = common::init_file_cache(&config, &storage).await;

    // Clone cache and data_dir for shutdown handler
    let cache_for_shutdown = cache.clone();
    let data_dir_for_shutdown = config.storage.data_dir.clone();

    // Initialize query tracer if enabled
    let query_tracer = if config.tracing.enabled || config.tracing.slow_query_logging {
        use crate::observability::{QueryTracer, SlowQueryLogger};
        use crate::observability::timeline::TimelineStore;

        let timeline_store = if config.tracing.timeline_enabled {
            Some(Arc::new(TimelineStore::new(config.tracing.max_timelines)))
        } else {
            None
        };

        let slow_query_logger = if config.tracing.slow_query_logging {
            Some(Arc::new(SlowQueryLogger::new(
                config.tracing.slow_query_threshold_ms,
                true,
            )))
        } else {
            None
        };

        Some(Arc::new(QueryTracer::new(timeline_store, slow_query_logger)))
    } else {
        None
    };

    // Initialize authentication (RBAC) and audit logging
    let role_configs: Vec<crate::auth::role_store::CustomRoleConfig> = config
        .roles
        .custom_roles
        .iter()
        .map(|r| crate::auth::role_store::CustomRoleConfig {
            name: r.name.clone(),
            permissions: r.permissions.clone(),
        })
        .collect();
    let role_store = Arc::new(crate::auth::RoleStore::new(role_configs));
    let audit_logger = Arc::new(crate::auth::AuditLogger::new(&config.audit));
    let auth_middleware = Arc::new(auth::AuthMiddleware::with_role_store_and_audit(
        &config.auth,
        role_store,
        audit_logger.clone(),
    ));
    info!(
        "Authentication: {} ({} users configured)",
        if config.auth.enabled {
            "ENABLED"
        } else {
            "DISABLED"
        },
        config.auth.users.len()
    );
    if audit_logger.is_enabled() {
        info!("Audit logging: ENABLED");
    }

    // Initialize streaming query manager
    let streaming_manager = if config.streaming.enabled {
        info!("Streaming queries: ENABLED (max {} subscriptions)", config.streaming.max_subscriptions);
        Some(Arc::new(crate::query::streaming::StreamingQueryManager::new(
            query_engine.clone(),
            config.streaming.clone(),
        )))
    } else {
        debug!("Streaming queries: DISABLED");
        None
    };

    // Initialize downsampling
    let downsampling_storage = if config.downsampling.enabled {
        let ds_storage = Arc::new(crate::jobs::downsampling::DownsamplingStorage::new(
            &config.storage.data_dir,
        ));
        if let Err(e) = ds_storage.initialize().await {
            error!("Failed to initialize downsampling storage: {}", e);
        }

        // Start background downsampling job (with compression strategy)
        let ds_job = Arc::new(crate::jobs::downsampling::DownsamplingJob::with_compression(
            ds_storage.clone(),
            query_engine.clone(),
            db_manager.clone(),
            storage.clone(),
            config.downsampling.clone(),
            compression_strategy.clone(),
        ));
        ds_job.start_background();

        info!(
            "Downsampling: ENABLED (interval: {}s, max_concurrent: {})",
            config.downsampling.check_interval_secs,
            config.downsampling.max_concurrent_jobs
        );
        Some(ds_storage)
    } else {
        debug!("Downsampling: DISABLED");
        None
    };

    // Build application state
    let user_store_for_state = if config.auth.enabled {
        Some(auth_middleware.user_store())
    } else {
        None
    };
    let role_store_for_state = if config.auth.enabled {
        Some(auth_middleware.role_store())
    } else {
        None
    };
    let state = api::AppState {
        cache,
        query_engine: query_engine.clone(),
        db_manager,
        compactor,
        config: config.clone(),
        wal,
        metadata_manager: Some(metadata_manager),
        file_cache,
        query_tracer,
        user_store: user_store_for_state,
        role_store: role_store_for_state,
        auth_enabled: config.auth.enabled,
        audit_logger: Some(audit_logger),
        streaming_manager,
        downsampling_storage,
    };

    // Build router (standalone mode)
    let app = api::build_router(state.clone(), auth_middleware.clone());

    print_server_info(&config);

    // Validate TLS configuration if enabled
    if config.tls.enabled {
        if let Err(e) = config.tls.validate() {
            error!("TLS configuration error: {}", e);
            std::process::exit(1);
        }
        welcome!("  TLS/SSL: ENABLED");
    }

    // Get user store for other protocols
    let user_store = auth_middleware.user_store();
    let auth_enabled = auth_middleware.is_enabled();
    let tls_opt = if config.tls.enabled {
        Some(&config.tls)
    } else {
        None
    };

    // Start Arrow Flight server (with auth + TLS)
    let flight_handle = common::start_flight_server_with_auth(
        &config.flight_addr(),
        query_engine.clone(),
        Some(user_store.clone()),
        auth_enabled,
        tls_opt,
    )
    .await;
    welcome!("  Arrow Flight server started successfully!");

    // Start MySQL server (if enabled, with auth + TLS)
    let mysql_handle = if config.server.mysql_enabled {
        let handle = common::start_mysql_server_with_auth(
            &config.mysql_addr(),
            state.query_engine.clone(),
            config.storage.data_dir.clone(),
            Arc::new(state.cache.clone()),
            Some(user_store.clone()),
            auth_enabled,
            tls_opt,
        );
        welcome!("  MySQL server started successfully!");
        Some(handle)
    } else {
        None
    };

    // Start HTTP server (with TLS)
    let http_handle =
        common::start_http_server_with_tls(&config.http_addr(), app, tls_opt).await;
    welcome!("  HTTP server started successfully!");
    welcome!("===============================================");

    // Wait for servers or shutdown signal
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
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down MynaDB...");
            info!("Received shutdown signal, cleaning up...");

            // Flush memtable data to disk before shutdown (with timeout)
            // This ensures no data loss on graceful shutdown
            let cache_stats = cache_for_shutdown.stats().await;
            if cache_stats.total_rows > 0 {
                println!("  Flushing {} cached rows to disk...", cache_stats.total_rows);
                info!(
                    "Shutdown: flushing {} rows from memtable to disk...",
                    cache_stats.total_rows
                );
                let flush_start = Instant::now();
                match tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    cache_for_shutdown.flush(),
                ).await {
                    Ok(Ok(files_written)) => {
                        println!("  Flushed {} file(s) in {:.1?}", files_written.len(), flush_start.elapsed());
                        info!(
                            "Shutdown: flushed {} files in {:?}",
                            files_written.len(),
                            flush_start.elapsed()
                        );
                    }
                    Ok(Err(e)) => {
                        println!("  Flush failed: {}", e);
                        error!("Shutdown: failed to flush memtable: {}", e);
                    }
                    Err(_) => {
                        println!("  Flush timed out after 10s, exiting anyway");
                        warn!("Shutdown: memtable flush timed out after 10s");
                    }
                }
            } else {
                println!("  No cached data to flush");
                info!("Shutdown: memtable empty, no flush needed");
            }

            // Remove PID file
            common::remove_pid_file(&data_dir_for_shutdown, NODE_TYPE);

            println!("Goodbye!");
            info!("Shutdown complete");
            std::process::exit(0);
        },
    }
}

/// Print welcome banner
fn print_welcome_banner(config: &Config) {
    common::print_banner("Standalone");
    welcome!("  Starting server...");
    let scheme = if config.tls.enabled { "https" } else { "http" };
    welcome!("   HTTP API: {}://{}", scheme, config.http_addr());
    welcome!("   Flight API: {}", config.flight_addr());
    if config.server.mysql_enabled {
        welcome!("   MySQL API: {}", config.mysql_addr());
    }
    welcome!("");
    print_storage_info(config);
    welcome!("   Log Directory: {}", config.logging.log_dir);
    welcome!("   Log Level: {}", config.logging.level);
    welcome!("");
}

/// Print storage configuration details
fn print_storage_info(config: &Config) {
    let backend = config.storage.backend.as_str();
    match backend {
        "s3" | "minio" | "oss" => {
            let label = match backend {
                "s3" => "Amazon S3",
                "minio" => "MinIO (S3-compatible)",
                "oss" => "Aliyun OSS",
                _ => backend,
            };
            welcome!("   Storage: {}", label);
            welcome!("     Bucket: {}", config.storage.bucket);
            if let Some(ref endpoint) = config.storage.endpoint {
                welcome!("     Endpoint: {}", endpoint);
            }
            welcome!("     Region: {}", config.storage.region);
            if let Some(ref prefix) = config.storage.prefix {
                welcome!("     Prefix: {}", prefix);
            }
            welcome!("     Local cache: {}", config.storage.data_dir);
            if let Some(cache_gb) = config.storage.file_cache_size_gb {
                welcome!("     File cache: {} GB", cache_gb);
            }
        }
        _ => {
            welcome!("   Storage: Local filesystem");
            welcome!("     Data dir: {}", config.storage.data_dir);
        }
    }
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

/// Stop a running standalone instance
///
/// Sends SIGTERM to the running process, which triggers the graceful shutdown
/// handler that flushes memtable data before exiting.
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    common::stop_node(&config.storage.data_dir, NODE_TYPE);
}
