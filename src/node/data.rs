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
    let meta_client = match MetaClient::connect(&data_config.meta_endpoints).await {
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
        config.cache.max_size_mb,
        data_config.node_id
    );
    let data_cache = Arc::new(cache::DataCache::new(
        storage.clone(),
        config.cache.max_size_mb,
        wal.as_ref().map(Arc::clone),
        data_config.node_id.clone(),
    ));

    // Start background flush with appropriate registration
    if let Some(ref client) = meta_client {
        // With Meta: Register files including created_by for distributed queries
        common::start_background_flush_with_meta_registration(
            (*data_cache).clone(),
            config.cache.flush_interval_secs,
            Arc::clone(client),
            data_config.node_id.clone(),
        );
    } else {
        // Without Meta: Simple flush without registration
        (*data_cache)
            .clone()
            .start_background_flush(config.cache.flush_interval_secs);
    }

    // Register with Meta service and start heartbeat
    let lease_id = Arc::new(AtomicU64::new(0));
    let shutdown_flag = Arc::new(AtomicBool::new(false));

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
            };

            // Register with 10 second lease TTL (faster failure detection)
            match client.register_node(node_info, 10000).await {
                Ok(id) => {
                    info!(
                        "Registered with Meta service, lease_id={} (attempt {})",
                        id,
                        attempt + 1
                    );
                    lease_id.store(id, Ordering::SeqCst);

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
    let parquet_cache: Option<Arc<ParquetLocalCache>> =
        if let Some(cache_size_mb) = config.cache.parquet_cache_mb {
            info!(
                "Initializing parquet local cache (max: {}MB, node_id: {})",
                cache_size_mb, data_config.node_id
            );
            let cache_dir = PathBuf::from(&config.storage.data_dir).join("_parquet_cache");
            let cache = ParquetLocalCache::new(
                cache_dir,
                cache_size_mb,
                data_config.node_id.clone(),
            )
            .with_storage(storage.clone());

            // Scan existing cache directory to rebuild tracking
            let cache = Arc::new(cache);
            let cache_for_scan = cache.clone();
            tokio::spawn(async move {
                if let Ok(count) = cache_for_scan.scan_existing().await {
                    if count > 0 {
                        info!("Parquet cache warmed up: {} files from previous session", count);
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

    // Clone meta_client before moving to FlightService (needed for shutdown handler)
    let meta_client_for_shutdown = meta_client.clone();

    // Create Flight service (replaces gRPC)
    let data_cache_for_http = data_cache.clone();
    let mut flight_service = DataNodeFlightService::new(
        data_cache,
        query_engine,
        meta_client,
        config.schema.clone(),
        data_config.node_id.clone(),
    );

    // Add parquet cache if available
    if let Some(ref cache) = parquet_cache {
        flight_service = flight_service.with_parquet_cache(cache.clone());
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
