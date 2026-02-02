//! Data node runner - Storage and query execution
//!
//! Usage:
//! - `mynadb data start` - Start the data node
//! - `mynadb data stop` - Stop a running data node

use super::common;
use crate::config::Config;
use crate::data::cache::ParquetLocalCache;
use crate::data::config::DataNodeConfig;
use crate::data::server::api::{start_http_server, DataNodeState};
use crate::data::server::flight::{start_flight_server, DataNodeFlightService};
use crate::meta::client::MetaClient;
use crate::meta::types::{current_timestamp_ms, NodeInfo, NodeRole, NodeState};
use crate::storage::cache;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Macro for welcome messages
macro_rules! welcome {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

const NODE_TYPE: &str = "data";

/// Start the Data node
pub async fn start(config_path: &str) {
    let config = Arc::new(Config::load_or_default(config_path));

    // Initialize logging
    common::init_logging(&config);

    welcome!("===============================================");
    welcome!("  MynaDB Data Node");
    welcome!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    welcome!("===============================================");

    // Load Data-specific config from [data] section or use defaults
    let data_config = config.data.clone().unwrap_or_else(DataNodeConfig::default);

    info!(
        "Starting Data node {} on {}",
        data_config.node_id, data_config.advertise_addr
    );

    // Write PID file for stop command
    if let Err(e) = common::write_pid_file(&config.storage.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Initialize storage backend
    let storage = common::init_storage(&config);

    // Initialize WAL
    let wal = common::init_wal(&config, storage.clone()).await;

    // Connect to Meta service first (needed for file registration during flush)
    // Use connect_with_node_id so heartbeats use the correct data node ID
    let meta_client = match MetaClient::connect_with_node_id(
        &data_config.meta_endpoints,
        data_config.node_id.clone(),
    )
    .await
    {
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
    };

    // Initialize data cache with node_id for distributed parallel queries
    info!(
        "Initializing data cache (max: {}MB, node_id: {})",
        config.cache.max_size_mb, data_config.node_id
    );
    let data_cache = Arc::new(cache::DataCache::new(
        storage.clone(),
        config.cache.max_size_mb,
        wal.as_ref().map(Arc::clone),
        data_config.node_id.clone(),
    ));

    // NOTE: Background flush is started AFTER query_engine initialization
    // (see below) to ensure table registrations are refreshed after flush

    // Register with Meta service and start heartbeat
    let lease_id = Arc::new(AtomicU64::new(0));
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Store backing_standby info from registration for flight_service
    // This enables node-level replication where all data goes to one Standby
    let mut backing_standby_info: Option<(String, String)> = None;

    if let Some(ref client) = meta_client {
        // Register with Meta service (retry with exponential backoff)
        // This handles the case where data nodes start before Raft leader is elected
        const MAX_RETRIES: u32 = 15;
        const INITIAL_DELAY_MS: u64 = 500;

        for attempt in 0..MAX_RETRIES {
            let node_info = NodeInfo {
                node_id: data_config.node_id.clone(),
                role: NodeRole::Data,
                state: NodeState::Active, // Register as Active so router can find us immediately
                advertise_addr: data_config.advertise_addr.clone(),
                http_addr: None,
                flight_addr: Some(data_config.advertise_addr.clone()),
                registered_at: current_timestamp_ms(),
                last_heartbeat: current_timestamp_ms(),
                labels: HashMap::new(),
                mode: None, // Mode auto-assigned by Meta based on standby ratio
                // Node-level standby pairing - assigned by Meta during registration
                backing_standby: None,
                backed_primaries: vec![],
            };

            // Register with 10 second lease TTL (faster failure detection)
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

                    // Store backing_standby info for flight service
                    if let Some((standby_id, standby_addr)) = result.backing_standby {
                        info!(
                            "Node-level standby: This Primary is backed by {} at {}",
                            standby_id, standby_addr
                        );
                        backing_standby_info = Some((standby_id, standby_addr));
                    }

                    // Start heartbeat loop
                    let client_clone = Arc::clone(client);
                    let lease_id_clone = Arc::clone(&lease_id);
                    let shutdown_clone = Arc::clone(&shutdown_flag);
                    tokio::spawn(async move {
                        heartbeat_loop(client_clone, lease_id_clone, shutdown_clone).await;
                    });
                    break;
                }
                Err(e) => {
                    if attempt < MAX_RETRIES - 1 {
                        let delay = INITIAL_DELAY_MS * (1 << attempt.min(5)); // Cap at 16 seconds
                        info!(
                            "Failed to register with Meta ({}), retrying in {}ms (attempt {}/{})",
                            e,
                            delay,
                            attempt + 1,
                            MAX_RETRIES
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    } else {
                        warn!(
                            "Failed to register with Meta service after {} attempts: {}",
                            MAX_RETRIES, e
                        );
                    }
                }
            }
        }
    }

    // Initialize parquet local cache for cluster mode (if configured)
    // This must be done before query engine initialization
    let parquet_cache: Option<Arc<ParquetLocalCache>> = if let Some(cache_size_mb) =
        config.cache.parquet_cache_mb
    {
        info!(
            "Initializing parquet local cache (max: {}MB, node_id: {})",
            cache_size_mb, data_config.node_id
        );
        let cache_dir = PathBuf::from(&config.storage.data_dir).join("_parquet_cache");
        let cache = ParquetLocalCache::new(cache_dir, cache_size_mb, data_config.node_id.clone())
            .with_storage(storage.clone());

        // Scan existing cache directory to rebuild tracking
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
    } else {
        None
    };

    // Initialize query engine for local query execution (with parquet cache if available)
    let query_engine = common::init_query_engine_with_parquet_cache(
        &config,
        storage.clone(),
        data_cache.clone(),
        parquet_cache.clone(),
    );

    // WAL Recovery - replay unflushed writes before accepting new data
    if let Some(ref wal_arc) = wal {
        welcome!("  Replaying WAL files...");
        common::perform_wal_recovery(wal_arc, &data_cache, &query_engine, &config).await;
        welcome!("  WAL recovery complete");
    } else {
        info!("WAL disabled, skipping recovery");
    }

    // Initialize replication manager if enabled (BEFORE background flush setup)
    // This allows the flush callback to broadcast FlushCheckpoint to standbys
    let replication_manager: Option<Arc<crate::data::replication::ReplicationManager>> =
        if data_config.replication.enabled {
            use crate::data::replication::{
                ReplicationConfig as ReplicationTypesConfig, ReplicationManager,
            };

            let replication_config = ReplicationTypesConfig::from(&data_config.replication);
            let replication_manager = Arc::new(ReplicationManager::new(
                data_config.node_id.clone(),
                replication_config,
            ));

            info!(
                "Replication enabled: mode={}, batch_interval={}ms",
                data_config.replication.mode, data_config.replication.batch_interval_ms
            );

            Some(replication_manager)
        } else {
            None
        };

    // Start background flush with appropriate registration
    // Must be done AFTER query_engine is created so we can refresh tables after flush
    match (&meta_client, &replication_manager) {
        (Some(client), Some(repl_manager)) => {
            // With Meta AND replication: Register files, refresh tables, AND broadcast FlushCheckpoint
            common::start_background_flush_with_replication(
                (*data_cache).clone(),
                config.cache.flush_interval_secs,
                Arc::clone(client),
                query_engine.clone(),
                Arc::clone(repl_manager),
                data_config.node_id.clone(),
            );
            info!("Background flush enabled with replication checkpoint broadcast");
        }
        (Some(client), None) => {
            // With Meta: Use table-level routing (register table + update stats)
            common::start_background_flush_with_table_routing(
                (*data_cache).clone(),
                config.cache.flush_interval_secs,
                Arc::clone(client),
                query_engine.clone(),
                data_config.node_id.clone(),
            );
        }
        (None, _) => {
            // Without Meta: Simple flush without registration
            (*data_cache)
                .clone()
                .start_background_flush(config.cache.flush_interval_secs);
        }
    }

    // Clone meta_client before moving to FlightService (needed for shutdown, event subscription, and HTTP API)
    let meta_client_for_shutdown = meta_client.clone();
    let meta_client_for_events = meta_client.clone();
    let meta_client_for_http = meta_client.clone();

    // Initialize database manager for partition operations
    let db_manager = Arc::new(
        crate::jobs::database_manager::DatabaseManager::with_storage_and_node_id(
            &config.storage.data_dir,
            storage.clone(),
            &data_config.node_id,
        ),
    );
    let db_manager_for_http = db_manager.clone();

    // Clone components for HTTP API (before they're moved to Flight service)
    let data_cache_for_http = data_cache.clone();
    let data_cache_for_shutdown = data_cache.clone();
    let data_cache_for_recovery = data_cache.clone();
    let query_engine_for_http = query_engine.clone();
    let config_for_http = config.clone();
    let wal_for_http = wal.clone();

    // Create shared is_retiring flag (used by both Flight and HTTP services)
    // When true, the node rejects new writes to allow graceful scale-in
    let is_retiring = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let is_retiring_for_http = Arc::clone(&is_retiring);

    // Create shared in_flight_queries counter (used by Flight, shared with HTTP for status)
    let in_flight_queries = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let in_flight_queries_for_http = Arc::clone(&in_flight_queries);

    // Create Flight service (replaces gRPC)
    let mut flight_service = DataNodeFlightService::new(
        data_cache,
        query_engine,
        meta_client,
        config.schema.clone(),
        data_config.node_id.clone(),
    )
    .with_db_manager(db_manager)
    .with_is_retiring(is_retiring)
    .with_in_flight_queries(in_flight_queries);

    // Add parquet cache if available
    if let Some(ref cache) = parquet_cache {
        flight_service = flight_service.with_parquet_cache(cache.clone());
    }

    // Add replication manager to flight service if enabled
    if let Some(ref repl_manager) = replication_manager {
        flight_service = flight_service.with_replication_manager(Arc::clone(repl_manager));
    }

    // Set backing standby for node-level replication
    if let Some((standby_id, standby_addr)) = backing_standby_info {
        flight_service = flight_service.with_backing_standby(standby_id, standby_addr);
    }

    // Clone for reporting task
    let replication_manager_for_reporting = replication_manager;

    // Start replication state reporting task if both replication and Meta are available
    if let (Some(ref client), Some(ref repl_manager)) =
        (&meta_client_for_events, &replication_manager_for_reporting)
    {
        use crate::meta::types::ReplicaState;

        let meta_client_for_report = Arc::clone(client);
        let repl_manager_for_report = Arc::clone(repl_manager);
        let report_interval_secs = 5u64; // Report every 5 seconds

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(report_interval_secs));

            loop {
                interval.tick().await;

                // Get per-partition stats from replication manager
                let partition_stats = repl_manager_for_report.partition_stats();

                // Report each partition's state to Meta
                for stats in partition_stats {
                    let state = if stats.in_sync {
                        ReplicaState::InSync
                    } else if stats.max_lag_ms < 10000 {
                        ReplicaState::CatchingUp
                    } else {
                        ReplicaState::Offline
                    };

                    if let Err(e) = meta_client_for_report
                        .update_replica_state(
                            &stats.partition_key,
                            state,
                            stats.last_replicated_seq,
                            stats.max_lag_ms,
                        )
                        .await
                    {
                        warn!(
                            "Failed to report replica state for {}: {}",
                            stats.partition_key, e
                        );
                    }
                }
            }
        });

        info!(
            "Started replication state reporting task (every {}s)",
            report_interval_secs
        );
    }

    // Subscribe to replica and recovery events from Meta
    // This keeps replica_cache updated and handles recovery flush triggers
    if let Some(ref client) = meta_client_for_events {
        use crate::meta::types::{EventType, PartitionReplica};

        let replica_cache = flight_service.replica_cache();
        let node_id = data_config.node_id.clone();
        let meta_client_for_sub = Arc::clone(client);
        let data_cache_for_sub = data_cache_for_recovery;

        tokio::spawn(async move {
            // Subscribe to replica and recovery events
            let topics = vec!["replicas".to_string(), "recovery".to_string()];
            let mut retry_count: u32 = 0;
            const MAX_RETRY_DELAY_SECS: u64 = 30;

            loop {
                match meta_client_for_sub.subscribe(topics.clone(), 0).await {
                    Ok(mut rx) => {
                        retry_count = 0; // Reset on successful connection
                        info!("Subscribed to replica and recovery events from Meta");

                        while let Some(event) = rx.recv().await {
                            match event.event_type {
                                EventType::ReplicaAssigned | EventType::ReplicaPromoted => {
                                    // Parse and cache the replica info
                                    match serde_json::from_slice::<PartitionReplica>(&event.payload)
                                    {
                                        Ok(replica) => {
                                            let is_leader = replica.leader_node_id == node_id;
                                            let is_standby =
                                                replica.standby_node_ids.contains(&node_id);

                                            if is_leader || is_standby {
                                                info!(
                                                    "Replica event for {}: leader={}, standbys={:?} (I'm {})",
                                                    replica.partition_key,
                                                    replica.leader_node_id,
                                                    replica.standby_node_ids,
                                                    if is_leader { "leader" } else { "standby" }
                                                );

                                                // Note: ReplicaPromoted event is no longer used for promotion
                                                // (No-Promotion architecture). Just update the replica cache.

                                                replica_cache
                                                    .insert(replica.partition_key.clone(), replica);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to parse replica event: {}", e);
                                        }
                                    }
                                }
                                EventType::ReplicaStateChanged => {
                                    // Update cached replica state
                                    if let Ok(replica) =
                                        serde_json::from_slice::<PartitionReplica>(&event.payload)
                                    {
                                        if replica.leader_node_id == node_id
                                            || replica.standby_node_ids.contains(&node_id)
                                        {
                                            replica_cache
                                                .insert(replica.partition_key.clone(), replica);
                                        }
                                    }
                                }
                                EventType::FlushForRecovery => {
                                    // Recovery event: Standby needs to flush crashed Primary's data
                                    if let Ok(payload) =
                                        serde_json::from_slice::<serde_json::Value>(&event.payload)
                                    {
                                        let standby_id = payload
                                            .get("standby_id")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("");
                                        let crashed_primary_id = payload
                                            .get("crashed_primary_id")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("");

                                        // Only process if this event is for us
                                        if standby_id == node_id {
                                            warn!(
                                                "RECOVERY: Received flush trigger for crashed Primary {}. Flushing all data...",
                                                crashed_primary_id
                                            );

                                            // Trigger immediate flush of all cached data
                                            match data_cache_for_sub.flush().await {
                                                Ok(results) => {
                                                    let total_files = results.len();
                                                    let total_rows: u64 =
                                                        results.iter().map(|r| r.row_count).sum();
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
                                    }
                                }
                                _ => {}
                            }
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
        });
    }

    // Start Flight server
    let addr = data_config.advertise_addr.parse().expect("Invalid address");
    welcome!("  Data node started on {}", data_config.advertise_addr);
    welcome!("  Protocol: Arrow Flight");

    // Start HTTP server for health checks and monitoring (if port > 0)
    let http_port = data_config.http_port;
    if http_port > 0 {
        let http_state = DataNodeState {
            node_id: data_config.node_id.clone(),
            start_time: Instant::now(),
            cache: data_cache_for_http,
            meta_client: meta_client_for_http,
            is_retiring: is_retiring_for_http, // Shared with Flight service
            retire_status: Arc::new(parking_lot::RwLock::new(None)),
            query_engine: Some(query_engine_for_http),
            config: Some(config_for_http),
            wal: wal_for_http,
            compactor: None, // Compaction handled by leader node in cluster mode
            db_manager: Some(db_manager_for_http),
            in_flight_queries: Some(in_flight_queries_for_http), // Shared with Flight service
        };
        tokio::spawn(async move {
            if let Err(e) = start_http_server("0.0.0.0", http_port, http_state).await {
                error!("Data node HTTP server failed: {}", e);
            }
        });
        welcome!("  HTTP API on port {}", http_port);
    }

    welcome!("===============================================");

    // Set up signal handler for graceful shutdown
    let data_dir = config.storage.data_dir.clone();
    let shutdown_meta_client = meta_client_for_shutdown;
    let shutdown_lease_id = Arc::clone(&lease_id);
    let shutdown_flag_clone = Arc::clone(&shutdown_flag);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");

        // Signal heartbeat loop to stop
        shutdown_flag_clone.store(true, Ordering::SeqCst);

        // Flush memtable data to disk before shutdown
        // This ensures no data loss on graceful shutdown
        let cache_stats = data_cache_for_shutdown.stats().await;
        if cache_stats.total_rows > 0 {
            info!(
                "Shutdown: flushing {} rows from memtable to disk...",
                cache_stats.total_rows
            );
            let flush_start = Instant::now();
            match data_cache_for_shutdown.flush().await {
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
        } else {
            info!("Shutdown: memtable empty, no flush needed");
        }

        // Deregister from Meta service
        if let Some(ref client) = shutdown_meta_client {
            let id = shutdown_lease_id.load(Ordering::SeqCst);
            if id > 0 {
                if let Err(e) = client.deregister_node(id).await {
                    warn!("Failed to deregister from Meta service: {}", e);
                } else {
                    info!("Deregistered from Meta service");
                }
            }
        }

        common::remove_pid_file(&data_dir, NODE_TYPE);
        std::process::exit(0);
    });

    if let Err(e) = start_flight_server(addr, flight_service).await {
        error!("Data Flight server failed: {}", e);
        common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
    }
}

/// Stop a running Data node
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    common::stop_node(&config.storage.data_dir, NODE_TYPE);
}

/// Heartbeat loop to keep the lease alive with Meta service
///
/// Sends heartbeats every 2 seconds to maintain the 10-second lease.
/// This gives us 5 heartbeat attempts before lease expires, providing
/// resilience against temporary network issues.
async fn heartbeat_loop(
    client: Arc<MetaClient>,
    lease_id: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) {
    // Heartbeat every 2 seconds (5 attempts within 10s lease TTL)
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        interval.tick().await;

        // Check if shutdown was requested
        if shutdown.load(Ordering::SeqCst) {
            info!("Heartbeat loop stopping due to shutdown");
            break;
        }

        let id = lease_id.load(Ordering::SeqCst);
        if id == 0 {
            continue; // Not registered yet
        }

        match client.heartbeat(id).await {
            Ok(next_interval_ms) => {
                // Adjust interval if server suggests different timing
                if next_interval_ms > 0 && next_interval_ms != 2000 {
                    interval =
                        tokio::time::interval(Duration::from_millis(next_interval_ms as u64));
                }
            }
            Err(e) => {
                warn!("Heartbeat failed: {}. Will retry...", e);
                // Could implement re-registration logic here if needed
            }
        }
    }
}
