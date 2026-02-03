//! Data node runner - Storage and query execution
//!
//! Usage:
//! - `mynadb data start` - Start the data node
//! - `mynadb data stop` - Stop a running data node

use super::common;
use crate::config::Config;
use crate::data::cache::ParquetLocalCache;
use crate::data::config::DataNodeConfig;
use crate::data::replication::ReplicationManager;
use crate::data::server::api::{start_http_server, DataNodeState};
use crate::data::server::flight::{start_flight_server, DataNodeFlightService};
use crate::meta::client::MetaClient;
use crate::meta::types::{current_timestamp_ms, Event, EventType, NodeInfo, NodeRole, NodeState};
use crate::query::query_engine::QueryEngine;
use crate::storage::cache;
use crate::storage::storage as storage_mod;
use crate::storage::wal;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

const NODE_TYPE: &str = "data";

/// Start the Data node
pub async fn start(config_path: &str) {
    let config = Arc::new(Config::load_or_default(config_path));
    common::init_logging(&config);
    print_welcome_banner();

    let data_config = config.data.clone().unwrap_or_else(DataNodeConfig::default);
    info!(
        "Starting Data node {} on {}",
        data_config.node_id, data_config.advertise_addr
    );

    if let Err(e) = common::write_pid_file(&config.storage.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Initialize core components
    let storage = common::init_storage(&config);
    let wal = common::init_wal(&config, storage.clone()).await;
    let meta_client = connect_to_meta(&data_config).await;
    let data_cache = init_data_cache(&config, &data_config, storage.clone(), wal.clone());

    // Register with Meta and start heartbeat
    let lease_id = Arc::new(AtomicU64::new(0));
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let backing_standby_info = register_with_meta(
        &meta_client,
        &data_config,
        Arc::clone(&lease_id),
        Arc::clone(&shutdown_flag),
    )
    .await;

    // Initialize query components
    let parquet_cache = init_parquet_cache(&config, &data_config, storage.clone());
    let query_engine = common::init_query_engine_with_parquet_cache(
        &config,
        storage.clone(),
        data_cache.clone(),
        parquet_cache.clone(),
    );
    perform_wal_recovery(&wal, &data_cache, &query_engine, &config).await;

    // Initialize replication and background flush
    let replication_manager = init_replication(&data_config);
    setup_background_flush(
        &data_cache,
        &config,
        &meta_client,
        &replication_manager,
        query_engine.clone(),
        &data_config.node_id,
    );

    // Build Flight service
    let flight_service = build_flight_service(
        data_cache.clone(),
        query_engine.clone(),
        meta_client.clone(),
        &config,
        &data_config,
        parquet_cache.clone(),
        replication_manager,
        backing_standby_info,
    );

    // Spawn background tasks
    spawn_event_subscriber(
        meta_client.clone(),
        data_config.node_id.clone(),
        data_cache.clone(),
    );
    spawn_shutdown_handler(
        config.storage.data_dir.clone(),
        meta_client.clone(),
        Arc::clone(&lease_id),
        Arc::clone(&shutdown_flag),
        data_cache.clone(),
    );

    // Start servers
    start_servers(
        &config,
        &data_config,
        data_cache,
        query_engine,
        meta_client,
        wal,
        flight_service,
    )
    .await;
}

/// Stop a running Data node
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    common::stop_node(&config.storage.data_dir, NODE_TYPE);
}

// =============================================================================
// Helper Functions
// =============================================================================

fn print_welcome_banner() {
    println!("===============================================");
    println!("  MynaDB Data Node");
    println!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    println!("===============================================");
}

/// Connect to Meta service with retry
async fn connect_to_meta(config: &DataNodeConfig) -> Option<Arc<MetaClient>> {
    match MetaClient::connect_with_node_id(&config.meta_endpoints, config.node_id.clone()).await {
        Ok(client) => {
            info!("Connected to Meta service");
            Some(Arc::new(client))
        }
        Err(e) => {
            warn!(
                "Failed to connect to Meta service: {}. Running without coordination.",
                e
            );
            None
        }
    }
}

/// Initialize data cache
fn init_data_cache(
    config: &Config,
    data_config: &DataNodeConfig,
    storage: Arc<storage_mod::Storage>,
    wal: Option<Arc<wal::Wal>>,
) -> Arc<cache::DataCache> {
    info!(
        "Initializing data cache (max: {}MB, node_id: {})",
        config.cache.max_size_mb, data_config.node_id
    );
    Arc::new(cache::DataCache::new(
        storage,
        config.cache.max_size_mb,
        wal,
        data_config.node_id.clone(),
    ))
}

/// Register with Meta service and start heartbeat loop
///
/// Returns backing_standby info if this is a Primary node with assigned standby
async fn register_with_meta(
    meta_client: &Option<Arc<MetaClient>>,
    config: &DataNodeConfig,
    lease_id: Arc<AtomicU64>,
    shutdown_flag: Arc<AtomicBool>,
) -> Option<(String, String)> {
    let client = meta_client.as_ref()?;

    const MAX_RETRIES: u32 = 15;
    const INITIAL_DELAY_MS: u64 = 500;

    for attempt in 0..MAX_RETRIES {
        let node_info = NodeInfo {
            node_id: config.node_id.clone(),
            role: NodeRole::Data,
            state: NodeState::Active,
            advertise_addr: config.advertise_addr.clone(),
            http_addr: None,
            flight_addr: Some(config.advertise_addr.clone()),
            registered_at: current_timestamp_ms(),
            last_heartbeat: current_timestamp_ms(),
            labels: HashMap::new(),
            mode: None,
            backing_standby: None,
            backed_primaries: vec![],
        };

        match client.register_node(node_info, 10000).await {
            Ok(result) => {
                info!(
                    "Registered with Meta service, lease_id={}, mode={:?}, backing_standby={:?} (attempt {})",
                    result.lease_id,
                    result.assigned_mode,
                    result.backing_standby.as_ref().map(|(id, _)| id),
                    attempt + 1
                );
                lease_id.store(result.lease_id, Ordering::SeqCst);

                // Start heartbeat loop
                let client_clone = Arc::clone(client);
                let lease_id_clone = Arc::clone(&lease_id);
                let shutdown_clone = Arc::clone(&shutdown_flag);
                tokio::spawn(async move {
                    heartbeat_loop(client_clone, lease_id_clone, shutdown_clone).await;
                });

                // Return backing_standby info if present
                if let Some((standby_id, standby_addr)) = result.backing_standby {
                    info!(
                        "Node-level standby: This Primary is backed by {} at {}",
                        standby_id, standby_addr
                    );
                    return Some((standby_id, standby_addr));
                }
                return None;
            }
            Err(e) => {
                if attempt < MAX_RETRIES - 1 {
                    let delay = INITIAL_DELAY_MS * (1 << attempt.min(5));
                    info!(
                        "Failed to register with Meta ({}), retrying in {}ms (attempt {}/{})",
                        e,
                        delay,
                        attempt + 1,
                        MAX_RETRIES
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                } else {
                    warn!(
                        "Failed to register with Meta service after {} attempts: {}",
                        MAX_RETRIES, e
                    );
                }
            }
        }
    }
    None
}

/// Initialize parquet local cache for cluster mode
fn init_parquet_cache(
    config: &Config,
    data_config: &DataNodeConfig,
    storage: Arc<storage_mod::Storage>,
) -> Option<Arc<ParquetLocalCache>> {
    let cache_size_mb = config.cache.parquet_cache_mb?;

    info!(
        "Initializing parquet local cache (max: {}MB, node_id: {})",
        cache_size_mb, data_config.node_id
    );

    let cache_dir = PathBuf::from(&config.storage.data_dir).join("_parquet_cache");
    let cache = ParquetLocalCache::new(cache_dir, cache_size_mb, data_config.node_id.clone())
        .with_storage(storage);

    let cache = Arc::new(cache);
    let cache_for_scan = cache.clone();
    tokio::spawn(async move {
        if let Ok(count) = cache_for_scan.scan_existing().await {
            if count > 0 {
                info!(
                    "Parquet cache warmed up: {} files from previous session",
                    count
                );
            }
        }
    });

    Some(cache)
}

/// Perform WAL recovery
async fn perform_wal_recovery(
    wal: &Option<Arc<wal::Wal>>,
    data_cache: &Arc<cache::DataCache>,
    query_engine: &Arc<QueryEngine>,
    config: &Arc<Config>,
) {
    if let Some(ref wal_arc) = wal {
        println!("  Replaying WAL files...");
        common::perform_wal_recovery(wal_arc, data_cache, query_engine, config).await;
        println!("  WAL recovery complete");
    } else {
        info!("WAL disabled, skipping recovery");
    }
}

/// Initialize replication manager
fn init_replication(config: &DataNodeConfig) -> Option<Arc<ReplicationManager>> {
    if !config.replication.enabled {
        return None;
    }

    use crate::data::replication::ReplicationConfig as ReplicationTypesConfig;

    let replication_config = ReplicationTypesConfig::from(&config.replication);
    let manager = Arc::new(ReplicationManager::new(
        config.node_id.clone(),
        replication_config,
    ));

    info!(
        "Replication enabled: mode={}, batch_interval={}ms",
        config.replication.mode, config.replication.batch_interval_ms
    );

    Some(manager)
}

/// Setup background flush with appropriate mode
fn setup_background_flush(
    data_cache: &Arc<cache::DataCache>,
    config: &Config,
    meta_client: &Option<Arc<MetaClient>>,
    replication_manager: &Option<Arc<ReplicationManager>>,
    query_engine: Arc<QueryEngine>,
    node_id: &str,
) {
    match (meta_client, replication_manager) {
        (Some(client), Some(repl_manager)) => {
            common::start_background_flush_with_replication(
                (**data_cache).clone(),
                config.cache.flush_interval_secs,
                Arc::clone(client),
                query_engine,
                Arc::clone(repl_manager),
                node_id.to_string(),
            );
            info!("Background flush enabled with replication checkpoint broadcast");
        }
        (Some(client), None) => {
            common::start_background_flush_with_table_routing(
                (**data_cache).clone(),
                config.cache.flush_interval_secs,
                Arc::clone(client),
                query_engine,
                node_id.to_string(),
            );
        }
        (None, _) => {
            (**data_cache)
                .clone()
                .start_background_flush(config.cache.flush_interval_secs);
        }
    }
}

/// Build the Flight service with all components
#[allow(clippy::too_many_arguments)]
fn build_flight_service(
    data_cache: Arc<cache::DataCache>,
    query_engine: Arc<QueryEngine>,
    meta_client: Option<Arc<MetaClient>>,
    config: &Config,
    data_config: &DataNodeConfig,
    parquet_cache: Option<Arc<ParquetLocalCache>>,
    replication_manager: Option<Arc<ReplicationManager>>,
    backing_standby_info: Option<(String, String)>,
) -> DataNodeFlightService {
    let db_manager = Arc::new(
        crate::jobs::database_manager::DatabaseManager::with_storage_and_node_id(
            &config.storage.data_dir,
            common::init_storage(config),
            &data_config.node_id,
        ),
    );

    let is_retiring = Arc::new(AtomicBool::new(false));
    let in_flight_queries = Arc::new(AtomicU64::new(0));

    let mut service = DataNodeFlightService::new(
        data_cache,
        query_engine,
        meta_client,
        config.schema.clone(),
        data_config.node_id.clone(),
    )
    .with_db_manager(db_manager)
    .with_is_retiring(is_retiring)
    .with_in_flight_queries(in_flight_queries);

    if let Some(cache) = parquet_cache {
        service = service.with_parquet_cache(cache);
    }

    if let Some(repl_manager) = replication_manager {
        service = service.with_replication_manager(repl_manager);
    }

    if let Some((standby_id, standby_addr)) = backing_standby_info {
        service = service.with_backing_standby(standby_id, standby_addr);
    }

    service
}

/// Spawn event subscriber for recovery and state change events
fn spawn_event_subscriber(
    meta_client: Option<Arc<MetaClient>>,
    node_id: String,
    data_cache: Arc<cache::DataCache>,
) {
    let client = match meta_client {
        Some(c) => c,
        None => return,
    };

    tokio::spawn(async move {
        event_subscription_loop(client, node_id, data_cache).await;
    });
}

/// Event subscription loop - handles recovery and retire events
async fn event_subscription_loop(
    meta_client: Arc<MetaClient>,
    node_id: String,
    data_cache: Arc<cache::DataCache>,
) {
    let topics = vec!["recovery".to_string(), "nodes".to_string()];
    let mut retry_count: u32 = 0;
    const MAX_RETRY_DELAY_SECS: u64 = 30;

    loop {
        match meta_client.subscribe(topics.clone(), 0).await {
            Ok(mut rx) => {
                retry_count = 0;
                info!("Subscribed to recovery events from Meta");

                while let Some(event) = rx.recv().await {
                    handle_meta_event(&event, &node_id, &data_cache).await;
                }
                warn!("Event subscription ended, reconnecting...");
            }
            Err(e) => {
                let delay = std::cmp::min(1 << retry_count, MAX_RETRY_DELAY_SECS);
                warn!(
                    "Failed to subscribe to events: {}. Retrying in {}s...",
                    e, delay
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
                retry_count = retry_count.saturating_add(1);
            }
        }
    }
}

/// Handle individual Meta events
async fn handle_meta_event(event: &Event, node_id: &str, data_cache: &Arc<cache::DataCache>) {
    match event.event_type {
        EventType::FlushForRecovery => {
            handle_recovery_flush_event(event, node_id, data_cache).await;
        }
        EventType::NodeStateChanged => {
            handle_node_state_changed_event(event, node_id, data_cache).await;
        }
        _ => {}
    }
}

/// Handle FlushForRecovery event - Standby flushes crashed Primary's data
async fn handle_recovery_flush_event(
    event: &Event,
    node_id: &str,
    data_cache: &Arc<cache::DataCache>,
) {
    let payload = match serde_json::from_slice::<serde_json::Value>(&event.payload) {
        Ok(p) => p,
        Err(_) => return,
    };

    let standby_id = payload
        .get("standby_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let crashed_primary_id = payload
        .get("crashed_primary_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if standby_id != node_id {
        return;
    }

    warn!(
        "RECOVERY: Received flush trigger for crashed Primary {}. Flushing all data...",
        crashed_primary_id
    );

    match data_cache.flush().await {
        Ok(results) => {
            let total_files = results.len();
            let total_rows: u64 = results.iter().map(|r| r.row_count).sum();
            info!(
                "RECOVERY: Flush complete for crashed Primary {}. Wrote {} files, {} rows",
                crashed_primary_id, total_files, total_rows
            );
        }
        Err(e) => {
            error!(
                "RECOVERY: Flush failed for crashed Primary {}: {}",
                crashed_primary_id, e
            );
        }
    }
}

/// Handle NodeStateChanged event - flush data when node is retiring
async fn handle_node_state_changed_event(
    event: &Event,
    node_id: &str,
    data_cache: &Arc<cache::DataCache>,
) {
    let payload = match serde_json::from_slice::<serde_json::Value>(&event.payload) {
        Ok(p) => p,
        Err(_) => return,
    };

    let event_node_id = payload
        .get("node_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let new_state = payload
        .get("new_state")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if event_node_id != node_id || new_state != "Retiring" {
        return;
    }

    info!("RETIRE: Received retire event for this node. Flushing memtable data...");

    match data_cache.flush().await {
        Ok(results) => {
            let total_files = results.len();
            let total_rows: u64 = results.iter().map(|r| r.row_count).sum();
            info!(
                "RETIRE: Pre-retire flush complete. Wrote {} files, {} rows",
                total_files, total_rows
            );
        }
        Err(e) => {
            error!("RETIRE: Pre-retire flush failed: {}", e);
        }
    }
}

/// Spawn shutdown handler for graceful shutdown
fn spawn_shutdown_handler(
    data_dir: String,
    meta_client: Option<Arc<MetaClient>>,
    lease_id: Arc<AtomicU64>,
    shutdown_flag: Arc<AtomicBool>,
    data_cache: Arc<cache::DataCache>,
) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");

        shutdown_flag.store(true, Ordering::SeqCst);

        // Flush memtable data
        flush_on_shutdown(&data_cache).await;

        // Deregister from Meta
        deregister_on_shutdown(&meta_client, &lease_id).await;

        common::remove_pid_file(&data_dir, NODE_TYPE);
        std::process::exit(0);
    });
}

/// Flush memtable data on shutdown
async fn flush_on_shutdown(data_cache: &Arc<cache::DataCache>) {
    let cache_stats = data_cache.stats().await;
    if cache_stats.total_rows == 0 {
        info!("Shutdown: memtable empty, no flush needed");
        return;
    }

    info!(
        "Shutdown: flushing {} rows from memtable to disk...",
        cache_stats.total_rows
    );

    let flush_start = Instant::now();
    match data_cache.flush().await {
        Ok(files_written) => {
            info!(
                "Shutdown: flushed {} files in {:?}",
                files_written.len(),
                flush_start.elapsed()
            );
        }
        Err(e) => {
            error!("Shutdown: failed to flush memtable: {}", e);
        }
    }
}

/// Deregister from Meta on shutdown
async fn deregister_on_shutdown(meta_client: &Option<Arc<MetaClient>>, lease_id: &Arc<AtomicU64>) {
    let client = match meta_client {
        Some(c) => c,
        None => return,
    };

    let id = lease_id.load(Ordering::SeqCst);
    if id == 0 {
        return;
    }

    match client.deregister_node(id).await {
        Ok(_) => info!("Deregistered from Meta service"),
        Err(e) => warn!("Failed to deregister from Meta service: {}", e),
    }
}

/// Start HTTP and Flight servers
async fn start_servers(
    config: &Config,
    data_config: &DataNodeConfig,
    data_cache: Arc<cache::DataCache>,
    query_engine: Arc<QueryEngine>,
    meta_client: Option<Arc<MetaClient>>,
    wal: Option<Arc<wal::Wal>>,
    flight_service: DataNodeFlightService,
) {
    let addr = data_config.advertise_addr.parse().expect("Invalid address");

    println!("  Data node started on {}", data_config.advertise_addr);
    println!("  Protocol: Arrow Flight");

    // Start HTTP server if enabled
    if data_config.http_port > 0 {
        let http_state = create_http_state(
            config,
            data_config,
            data_cache,
            query_engine,
            meta_client,
            wal,
        );
        let http_port = data_config.http_port;
        tokio::spawn(async move {
            if let Err(e) = start_http_server("0.0.0.0", http_port, http_state).await {
                error!("Data node HTTP server failed: {}", e);
            }
        });
        println!("  HTTP API on port {}", data_config.http_port);
    }

    println!("===============================================");

    // Start Flight server (blocking)
    if let Err(e) = start_flight_server(addr, flight_service).await {
        error!("Data Flight server failed: {}", e);
        common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
    }
}

/// Create HTTP server state
fn create_http_state(
    config: &Config,
    data_config: &DataNodeConfig,
    data_cache: Arc<cache::DataCache>,
    query_engine: Arc<QueryEngine>,
    meta_client: Option<Arc<MetaClient>>,
    wal: Option<Arc<wal::Wal>>,
) -> DataNodeState {
    let db_manager = Arc::new(
        crate::jobs::database_manager::DatabaseManager::with_storage_and_node_id(
            &config.storage.data_dir,
            common::init_storage(config),
            &data_config.node_id,
        ),
    );

    DataNodeState {
        node_id: data_config.node_id.clone(),
        start_time: Instant::now(),
        cache: data_cache,
        meta_client,
        is_retiring: Arc::new(AtomicBool::new(false)),
        retire_status: Arc::new(parking_lot::RwLock::new(None)),
        query_engine: Some(query_engine),
        config: Some(Arc::new(config.clone())),
        wal,
        compactor: None,
        db_manager: Some(db_manager),
        in_flight_queries: Some(Arc::new(AtomicU64::new(0))),
    }
}

/// Heartbeat loop to keep the lease alive with Meta service
async fn heartbeat_loop(
    client: Arc<MetaClient>,
    lease_id: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        interval.tick().await;

        if shutdown.load(Ordering::SeqCst) {
            info!("Heartbeat loop stopping due to shutdown");
            break;
        }

        let id = lease_id.load(Ordering::SeqCst);
        if id == 0 {
            continue;
        }

        match client.heartbeat(id).await {
            Ok(next_interval_ms) => {
                if next_interval_ms > 0 && next_interval_ms != 2000 {
                    interval =
                        tokio::time::interval(Duration::from_millis(next_interval_ms as u64));
                }
            }
            Err(e) => {
                warn!("Heartbeat failed: {}. Will retry...", e);
            }
        }
    }
}
