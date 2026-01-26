//! Common initialization routines shared across node types

use crate::config::Config;
use crate::jobs::logger;
use crate::query::cache::{hot_partition_cache, query_cache};
use crate::query::{catalog, query_engine};
use crate::services::{flight_service, mysql_service};
use crate::storage::{cache, file_cache, parquet_writer, storage as storage_mod, wal};
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
            info!("WAL initialized successfully - recovery will happen after components initialize");
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
) -> cache::DataCache {
    info!(
        "Initializing data cache (max: {}MB, flush threshold: 80%)",
        config.cache.max_size_mb
    );
    cache::DataCache::new(storage, config.cache.max_size_mb, wal)
}

/// Initialize metadata manager
pub async fn init_metadata_manager(config: &Config) -> catalog::SharedMetadataManager {
    info!("Initializing metadata manager");
    let metadata_dir = format!("{}/_metadata", config.storage.data_dir);
    let metadata_manager: catalog::SharedMetadataManager =
        Arc::new(catalog::MemoryMetadataManager::new(&metadata_dir));

    // Try to load existing metadata
    if let Err(e) = metadata_manager.load().await {
        warn!("Failed to load metadata (starting fresh): {}", e);
    }
    // Rescan the data directory to rebuild metadata
    if let Err(e) = metadata_manager.rescan(&config.storage.data_dir).await {
        warn!("Failed to rescan metadata: {}", e);
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
    info!("Initializing query engine with memtable support");
    let query_cache_config = query_cache::QueryCacheConfig::from(&config.query_cache);
    let hot_partition_cache_config =
        hot_partition_cache::HotPartitionCacheConfig::from(&config.hot_partition_cache);

    Arc::new(query_engine::QueryEngine::new_with_memtable(
        &config.storage.data_dir,
        Some(storage),
        query_cache_config,
        hot_partition_cache_config,
        config.query.use_custom_table_provider,
        cache,
    ))
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

/// Start background flush with metadata registration callback
pub fn start_background_flush_with_metadata(
    cache: cache::DataCache,
    interval_secs: u64,
    metadata_manager: catalog::SharedMetadataManager,
) {
    let metadata_for_flush = metadata_manager.clone();
    cache.start_background_flush_with_callback(
        interval_secs,
        Some(move |files: Vec<parquet_writer::FileWriteResult>| {
            let manager = metadata_for_flush.clone();
            tokio::spawn(async move {
                for file in files {
                    let file_name = file
                        .path
                        .rsplit('/')
                        .next()
                        .unwrap_or(&file.path)
                        .to_string();

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
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after flush: {}", e);
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
pub async fn start_http_server(
    addr: &str,
    app: axum::Router,
) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind HTTP address");
    let addr_display = addr.to_string();

    tokio::spawn(async move {
        info!("HTTP server starting on {}", addr_display);
        axum::serve(listener, app).await.expect("HTTP server failed");
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
        eprintln!("{} node is not running (no PID file at {})", node_type, pid_file.display());
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
