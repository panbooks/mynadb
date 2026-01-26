//! Data node runner - Storage and query execution
//!
//! Usage:
//! - `mynadb data start` - Start the data node
//! - `mynadb data stop` - Stop a running data node

use super::common;
use crate::config::Config;
use crate::data::config::DataNodeConfig;
use crate::data::server::flight::{start_flight_server, DataNodeFlightService};
use crate::meta::client::MetaClient;
use crate::storage::cache;
use std::sync::Arc;
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

    // Initialize data cache
    info!(
        "Initializing data cache (max: {}MB)",
        config.cache.max_size_mb
    );
    let data_cache = Arc::new(cache::DataCache::new(
        storage.clone(),
        config.cache.max_size_mb,
        wal.as_ref().map(Arc::clone),
    ));

    // Start background flush
    (*data_cache).clone().start_background_flush(config.cache.flush_interval_secs);

    // Connect to Meta service (optional)
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

    // Initialize query engine for local query execution
    let query_engine = common::init_query_engine(&config, storage.clone(), data_cache.clone());

    // Create Flight service (replaces gRPC)
    let flight_service = DataNodeFlightService::new(
        data_cache,
        query_engine,
        meta_client,
        config.schema.clone(),
        data_config.node_id.clone(),
    );

    // Start Flight server
    let addr = data_config
        .advertise_addr
        .parse()
        .expect("Invalid address");
    welcome!("  Data node started on {}", data_config.advertise_addr);
    welcome!("  Protocol: Arrow Flight");
    welcome!("===============================================");

    // Set up signal handler for graceful shutdown
    let data_dir = config.storage.data_dir.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");
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
