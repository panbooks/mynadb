//! Meta node runner - Raft-based metadata service
//!
//! Usage:
//! - `mynadb meta start` - Start the meta node
//! - `mynadb meta stop` - Stop a running meta node

use super::common;
use crate::config::Config;
use crate::meta::config::MetaConfig;
use crate::meta::service::grpc::{start_grpc_server, MetaGrpcService};
use crate::meta::service::meta_node::MetaNode;
use std::sync::Arc;
use tracing::{error, info};

/// Macro for welcome messages
macro_rules! welcome {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

const NODE_TYPE: &str = "meta";

/// Start the Meta node
pub async fn start(config_path: &str) {
    let config = Arc::new(Config::load_or_default(config_path));

    // Initialize logging
    common::init_logging(&config);

    welcome!("===============================================");
    welcome!("  MynaDB Meta Node");
    welcome!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    welcome!("===============================================");

    // Load Meta-specific config from [meta] section or use defaults
    let meta_config = config.meta.clone().unwrap_or_else(MetaConfig::default);

    info!(
        "Starting Meta node {} on {}",
        meta_config.node_id, meta_config.advertise_addr
    );

    // Write PID file for stop command
    if let Err(e) = common::write_pid_file(&config.storage.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Create and start the Meta node
    let node = match MetaNode::new(meta_config.clone()).await {
        Ok(node) => Arc::new(node),
        Err(e) => {
            error!("Failed to create Meta node: {}", e);
            common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
            std::process::exit(1);
        }
    };

    // Start background tasks
    node.start_background_tasks();

    // Create gRPC service
    let grpc_service = MetaGrpcService::new(Arc::clone(&node));

    // Start gRPC server
    let addr = meta_config
        .advertise_addr
        .parse()
        .expect("Invalid address");
    welcome!("  Meta node started on {}", meta_config.advertise_addr);
    welcome!("===============================================");

    // Set up signal handler for graceful shutdown
    let data_dir = config.storage.data_dir.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");
        common::remove_pid_file(&data_dir, NODE_TYPE);
        std::process::exit(0);
    });

    if let Err(e) = start_grpc_server(addr, grpc_service).await {
        error!("Meta gRPC server failed: {}", e);
        common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
    }
}

/// Stop a running Meta node
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    common::stop_node(&config.storage.data_dir, NODE_TYPE);
}
