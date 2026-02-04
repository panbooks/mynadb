//! Common initialization routines shared across node types

use crate::config::Config;
use crate::jobs::logger;
use crate::query::cache::{hot_partition_cache, query_cache};
use crate::query::{catalog, query_engine};
use crate::services::{api, flight_service, mysql_service};
use crate::storage::{cache, file_cache, parquet_writer, storage as storage_mod, wal};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{debug, error, info, warn};

/// Initialize logging for any node type
pub fn init_logging(config: &Config) {
    if let Err(e) = logger::init_logging(&config.logging) {
        eprintln!("Failed to initialize logging: {}", e);
        std::process::exit(1);
    }
}

/// Initialize storage backend
pub fn init_storage(config: &Config) -> Arc<storage_mod::Storage> {
    info!("Initializing storage backend: {}", config.storage.backend);
    Arc::new(
        storage_mod::Storage::from_config(&config.storage).expect("Failed to initialize storage"),
    )
}

/// Initialize WAL (Write-Ahead Log) with object store support
pub async fn init_wal(
    config: &Config,
    storage: Arc<storage_mod::Storage>,
) -> Option<Arc<wal::Wal>> {
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
            info!(
                "WAL initialized successfully - recovery will happen after components initialize"
            );
            Some(Arc::new(w))
        }
        Err(e) => {
            error!("Failed to initialize WAL: {}", e);
            None
        }
    }
}

/// Initialize data cache
pub fn init_data_cache(
    storage: Arc<storage_mod::Storage>,
    config: &Config,
    wal: Option<Arc<wal::Wal>>,
    node_id: &str,
) -> cache::DataCache {
    info!(
        "Initializing data cache (max: {}MB, flush threshold: 80%, node_id: {})",
        config.cache.max_size_mb, node_id
    );
    cache::DataCache::new(storage, config.cache.max_size_mb, wal, node_id.to_string())
}

/// Initialize metadata manager
pub async fn init_metadata_manager(config: &Config) -> catalog::SharedMetadataManager {
    // Use "standalone" as default node_id for backward compatibility
    init_metadata_manager_for_node(config, "standalone").await
}

/// Initialize metadata manager for a specific node (cluster mode)
/// Scans: {data_dir}/{node_id}/{catalog}/{schema}/{table}/date=YYYY-MM-DD/*.parquet
pub async fn init_metadata_manager_for_node(
    config: &Config,
    node_id: &str,
) -> catalog::SharedMetadataManager {
    info!("Initializing metadata manager for node: {}", node_id);
    let metadata_dir = format!("{}/_metadata", config.storage.data_dir);
    let metadata_manager: catalog::SharedMetadataManager =
        Arc::new(catalog::MemoryMetadataManager::new(&metadata_dir));

    // Try to load existing metadata
    if let Err(e) = metadata_manager.load().await {
        warn!("Failed to load metadata (starting fresh): {}", e);
    }
    // Rescan the data directory for this node's files
    if let Err(e) = metadata_manager
        .rescan_node(&config.storage.data_dir, node_id)
        .await
    {
        warn!("Failed to rescan metadata for node {}: {}", node_id, e);
    }
    // Save the metadata
    if let Err(e) = metadata_manager.save().await {
        warn!("Failed to save metadata: {}", e);
    }

    metadata_manager
}

/// Initialize query engine with memtable support
pub fn init_query_engine(
    config: &Config,
    storage: Arc<storage_mod::Storage>,
    cache: Arc<cache::DataCache>,
) -> Arc<query_engine::QueryEngine> {
    init_query_engine_with_parquet_cache(config, storage, cache, None)
}

/// Initialize query engine with memtable and optional parquet cache support
pub fn init_query_engine_with_parquet_cache(
    config: &Config,
    storage: Arc<storage_mod::Storage>,
    cache: Arc<cache::DataCache>,
    parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
) -> Arc<query_engine::QueryEngine> {
    info!("Initializing query engine with memtable support");
    let query_cache_config = query_cache::QueryCacheConfig::from(&config.query_cache);
    let hot_partition_cache_config =
        hot_partition_cache::HotPartitionCacheConfig::from(&config.hot_partition_cache);

    let mut engine = query_engine::QueryEngine::new_with_memtable(
        &config.storage.data_dir,
        Some(storage),
        query_cache_config,
        hot_partition_cache_config,
        config.query.use_custom_table_provider,
        cache,
    );

    // Set parquet cache for cluster mode (object storage)
    if let Some(cache) = parquet_cache {
        engine.set_parquet_cache(cache);
        info!("Parquet local cache enabled for query engine");
    }

    Arc::new(engine)
}

/// Initialize file cache for object storage backends
pub async fn init_file_cache(
    config: &Config,
    storage: &storage_mod::Storage,
) -> Option<file_cache::SharedFileCache> {
    if !storage.is_local() {
        info!("Initializing file cache for object storage");
        let cache_dir = format!("{}/_cache/parquet", config.storage.data_dir);
        let max_size = config.storage.file_cache_size_gb.unwrap_or(100) * 1024 * 1024 * 1024;
        match file_cache::DiskFileCache::new_at(&cache_dir, max_size).await {
            Ok(cache) => {
                info!(
                    "File cache initialized: dir={}, max_size={}GB",
                    cache_dir,
                    max_size / 1024 / 1024 / 1024
                );
                Some(Arc::new(cache))
            }
            Err(e) => {
                warn!("Failed to initialize file cache: {}", e);
                None
            }
        }
    } else {
        debug!("File cache disabled for local storage");
        None
    }
}

/// Start background flush with metadata registration AND query engine table refresh
///
/// This is the correct function to use for standalone mode. It ensures that after
/// background flush writes new Parquet files, the QueryEngine's table registrations
/// are refreshed so queries can find the new data.
pub fn start_background_flush_with_query_engine(
    cache: cache::DataCache,
    interval_secs: u64,
    metadata_manager: catalog::SharedMetadataManager,
    query_engine: Arc<query_engine::QueryEngine>,
) {
    let metadata_for_flush = metadata_manager.clone();
    let query_engine_for_flush = query_engine.clone();

    cache.start_background_flush_with_callback(
        interval_secs,
        Some(move |files: Vec<parquet_writer::FileWriteResult>| {
            let manager = metadata_for_flush.clone();
            let qe = query_engine_for_flush.clone();

            tokio::spawn(async move {
                // Track unique tables that were flushed
                let mut flushed_tables: HashSet<(String, String, String)> = HashSet::new();

                for file in files {
                    let file_name = file
                        .path
                        .rsplit('/')
                        .next()
                        .unwrap_or(&file.path)
                        .to_string();

                    // Track this table for later refresh
                    flushed_tables.insert((
                        file.catalog.clone(),
                        file.schema.clone(),
                        file.table.clone(),
                    ));

                    let file_info = catalog::FileInfo {
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
                }

                // CRITICAL: Refresh QueryEngine table registrations for all flushed tables
                // This ensures DataFusion can find the new Parquet files
                for (catalog, schema, table) in &flushed_tables {
                    debug!(
                        "Refreshing table after flush: {}.{}.{}",
                        catalog, schema, table
                    );
                    qe.refresh_table_after_flush(catalog, schema, table);
                }

                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after flush: {}", e);
                }

                if !flushed_tables.is_empty() {
                    info!(
                        "Background flush complete: {} tables refreshed",
                        flushed_tables.len()
                    );
                }
            });
        }),
    );
}

/// Start background flush with table-level Meta registration (RECOMMENDED)
///
/// After flush completes, registers/updates tables with Meta service using
/// table-level routing. This is the recommended approach for cluster mode.
///
/// Benefits over file-level registration:
/// - 600x fewer Meta entries (100K tables vs 60M files)
/// - Zero daily growth (new partitions don't create entries)
/// - Excellent cache locality for time-series workloads
///
/// The Data node owns all partitions for tables it creates. Files are
/// discovered via S3 listing at query time, not stored in Meta.
///
/// When `replication_manager` is provided (Primary nodes with replication),
/// also broadcasts FlushCheckpoint to all Standby nodes after each flush.
pub fn start_background_flush_with_table_routing(
    cache: cache::DataCache,
    interval_secs: u64,
    meta_client: Arc<crate::meta::client::MetaClient>,
    query_engine: Arc<query_engine::QueryEngine>,
    node_id: String,
    replication_manager: Option<Arc<crate::data::replication::ReplicationManager>>,
) {
    use crate::meta::types::TableStatsDelta;

    let meta_for_flush = meta_client;
    let qe_for_flush = query_engine;
    let repl_for_flush = replication_manager;
    let node_id_for_flush = node_id;

    cache.start_background_flush_with_callback(
        interval_secs,
        Some(move |files: Vec<parquet_writer::FileWriteResult>| {
            let client = meta_for_flush.clone();
            let qe = qe_for_flush.clone();
            let repl_manager = repl_for_flush.clone();
            let node_id = node_id_for_flush.clone();

            tokio::spawn(async move {
                if files.is_empty() {
                    return;
                }

                // For replication: capture sequence number before processing
                let repl_context = repl_manager.as_ref().map(|rm| {
                    let seq = rm.current_seq();
                    let total_rows: u64 = files.iter().map(|f| f.row_count).sum();
                    let files_count = files.len() as u32;
                    (rm.clone(), seq, files_count, total_rows)
                });

                // Aggregate stats per table
                let mut table_stats: HashMap<(String, String, String), TableStatsDelta> =
                    HashMap::new();
                let mut seen_partitions: HashMap<(String, String, String), HashSet<String>> =
                    HashMap::new();

                for file in &files {
                    let key = (
                        file.catalog.clone(),
                        file.schema.clone(),
                        file.table.clone(),
                    );

                    let partitions = seen_partitions.entry(key.clone()).or_default();
                    let is_new_partition = partitions.insert(file.partition_key.clone());

                    let delta = table_stats.entry(key).or_default();
                    delta.bytes_delta += file.size_bytes as i64;
                    delta.rows_delta += file.row_count as i64;
                    delta.files_delta += 1;
                    if is_new_partition {
                        delta.partition_count_delta += 1;
                    }
                    if delta.newest_partition.is_none()
                        || file.partition_key > *delta.newest_partition.as_ref().unwrap()
                    {
                        delta.newest_partition = Some(file.partition_key.clone());
                    }
                }

                // Register/update each table with Meta
                for ((catalog, schema, table), delta) in &table_stats {
                    // Ensure table is registered (idempotent)
                    match client
                        .register_table(catalog, schema, table, &node_id)
                        .await
                    {
                        Ok((already_exists, current_owner, _)) => {
                            if already_exists && current_owner != node_id {
                                warn!(
                                    "Table {}.{}.{} owned by {} but {} is writing to it",
                                    catalog, schema, table, current_owner, node_id
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to register table {}.{}.{}: {}",
                                catalog, schema, table, e
                            );
                            continue;
                        }
                    }

                    // Update stats
                    match client
                        .update_table_stats(catalog, schema, table, delta.clone())
                        .await
                    {
                        Ok(version) => {
                            debug!(
                                "Updated table stats {}.{}.{} v{} (+{} bytes, +{} rows, +{} files)",
                                catalog, schema, table, version,
                                delta.bytes_delta, delta.rows_delta, delta.files_delta
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to update table stats {}.{}.{}: {}",
                                catalog, schema, table, e
                            );
                        }
                    }

                    // Refresh QueryEngine registration
                    debug!("Refreshing table after flush: {}.{}.{}", catalog, schema, table);
                    qe.refresh_table_after_flush(catalog, schema, table);
                }

                // Replication: broadcast FlushCheckpoint to standbys
                if let Some((rm, flushed_seq, files_count, total_rows)) = repl_context {
                    use crate::data::replication::types::FlushCheckpoint;

                    rm.record_flush(flushed_seq);
                    let checkpoint = FlushCheckpoint::with_metrics(
                        node_id.clone(),
                        flushed_seq,
                        files_count,
                        total_rows,
                    );
                    let notified = rm.broadcast_flush_checkpoint(checkpoint).await;

                    info!(
                        "Background flush complete: {} files, {} tables, {} standbys notified (seq={})",
                        files_count, table_stats.len(), notified, flushed_seq
                    );
                } else {
                    info!(
                        "Background flush complete: {} files across {} tables",
                        files.len(), table_stats.len()
                    );
                }
            });
        }),
    );
}

/// Start Arrow Flight server
pub async fn start_flight_server(
    addr: &str,
    query_engine: Arc<query_engine::QueryEngine>,
) -> tokio::task::JoinHandle<()> {
    let flight_service = flight_service::StatsFlightService::new(query_engine);
    let addr = addr.parse().expect("Invalid flight address");

    tokio::spawn(async move {
        info!("Arrow Flight server starting on {}", addr);
        Server::builder()
            .add_service(flight_service.into_server())
            .serve(addr)
            .await
            .expect("Flight server failed");
    })
}

/// Start MySQL server (returns None if disabled)
pub fn start_mysql_server(
    addr: &str,
    query_engine: Arc<query_engine::QueryEngine>,
    data_dir: String,
    cache: Arc<cache::DataCache>,
) -> tokio::task::JoinHandle<()> {
    let mysql_service = mysql_service::MysqlService::new(
        query_engine,
        tokio::runtime::Handle::current(),
        data_dir,
        cache,
    );
    let addr = addr.to_string();

    tokio::task::spawn_blocking(move || {
        info!("MySQL server starting on {}", addr);
        if let Err(e) = mysql_service.run(&addr) {
            error!("MySQL server error: {}", e);
        }
    })
}

/// Start HTTP server with Axum router
pub async fn start_http_server(addr: &str, app: axum::Router) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind HTTP address");
    let addr_display = addr.to_string();

    tokio::spawn(async move {
        info!("HTTP server starting on {}", addr_display);
        axum::serve(listener, app)
            .await
            .expect("HTTP server failed");
    })
}

// ============================================================================
// PID File Management for start/stop commands
// ============================================================================

/// Get PID file path for a node type
pub fn pid_file_path(data_dir: &str, node_type: &str) -> std::path::PathBuf {
    Path::new(data_dir).join(format!("{}.pid", node_type))
}

/// Write PID file for the current process
pub fn write_pid_file(data_dir: &str, node_type: &str) -> std::io::Result<()> {
    let pid_file = pid_file_path(data_dir, node_type);

    // Ensure parent directory exists
    if let Some(parent) = pid_file.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&pid_file, std::process::id().to_string())?;
    info!("PID file written: {}", pid_file.display());
    Ok(())
}

/// Remove PID file on shutdown
pub fn remove_pid_file(data_dir: &str, node_type: &str) {
    let pid_file = pid_file_path(data_dir, node_type);
    if let Err(e) = fs::remove_file(&pid_file) {
        warn!("Failed to remove PID file {}: {}", pid_file.display(), e);
    } else {
        info!("PID file removed: {}", pid_file.display());
    }
}

/// Stop a running node by sending SIGTERM
pub fn stop_node(data_dir: &str, node_type: &str) {
    let pid_file = pid_file_path(data_dir, node_type);

    if !pid_file.exists() {
        eprintln!(
            "{} node is not running (no PID file at {})",
            node_type,
            pid_file.display()
        );
        std::process::exit(1);
    }

    let pid_str = match fs::read_to_string(&pid_file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read PID file: {}", e);
            std::process::exit(1);
        }
    };

    let pid: i32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Invalid PID in file: {}", e);
            std::process::exit(1);
        }
    };

    // Send SIGTERM
    let result = unsafe { libc::kill(pid, libc::SIGTERM) };

    if result == 0 {
        println!("Sent SIGTERM to {} node (PID: {})", node_type, pid);
        // Remove PID file
        if let Err(e) = fs::remove_file(&pid_file) {
            warn!("Failed to remove PID file: {}", e);
        }
    } else {
        let errno = std::io::Error::last_os_error();
        eprintln!("Failed to send signal to PID {}: {}", pid, errno);
        // If process doesn't exist, clean up the stale PID file
        if errno.raw_os_error() == Some(libc::ESRCH) {
            eprintln!("Process {} not found, removing stale PID file", pid);
            let _ = fs::remove_file(&pid_file);
        }
        std::process::exit(1);
    }
}

/// Check if a node is running
pub fn is_node_running(data_dir: &str, node_type: &str) -> bool {
    let pid_file = pid_file_path(data_dir, node_type);

    if !pid_file.exists() {
        return false;
    }

    let pid_str = match fs::read_to_string(&pid_file) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let pid: i32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(_) => return false,
    };

    // Check if process exists by sending signal 0
    let result = unsafe { libc::kill(pid, 0) };
    result == 0
}

/// Perform WAL recovery by replaying unflushed writes
/// Shared by standalone and data nodes
pub async fn perform_wal_recovery(
    wal: &Arc<wal::Wal>,
    cache: &cache::DataCache,
    query_engine: &Arc<query_engine::QueryEngine>,
    config: &Arc<Config>,
) {
    info!("Starting WAL recovery - replaying unflushed writes...");

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

                info!("Flushing recovered data to Parquet for durability...");
                match cache.flush().await {
                    Ok(files) => {
                        info!("Recovery flush complete: {} file(s) written", files.len());
                        wal.set_recovery_mode(false);
                        // Note: cleanup_old_data_wal_files() is already called by cache.flush()
                    }
                    Err(e) => {
                        error!("Recovery flush failed: {}", e);
                        wal.set_recovery_mode(false);
                    }
                }
            } else {
                info!("WAL recovery: no data writes to replay");
                wal.set_recovery_mode(false);
            }
        }
        Err(e) => {
            error!("WAL recovery failed: {}", e);
            wal.set_recovery_mode(false);
        }
    }
}
