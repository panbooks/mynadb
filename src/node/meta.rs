//! Meta node runner - Raft-based metadata service
//!
//! Usage:
//! - `mynadb meta start` - Start the meta node
//! - `mynadb meta stop` - Stop a running meta node

use super::common;
use crate::config::Config;
use crate::meta::config::MetaConfig;
use crate::meta::service::grpc::{start_grpc_server, MetaGrpcService, RaftInternalService};
use crate::meta::service::http::{start_http_server, MetaHttpState};
use crate::meta::service::meta_node::MetaNode;
use std::sync::Arc;
use std::time::Instant;
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
    let meta_config = config.meta.clone().unwrap_or_default();

    info!(
        "Starting Meta node {} on {}",
        meta_config.node_id, meta_config.advertise_addr
    );

    // Write PID file for stop command (use meta-specific data_dir)
    if let Err(e) = common::write_pid_file(&meta_config.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Create and start the Meta node
    let node = match MetaNode::new(meta_config.clone()).await {
        Ok(node) => Arc::new(node),
        Err(e) => {
            error!("Failed to create Meta node: {}", e);
            common::remove_pid_file(&meta_config.data_dir, NODE_TYPE);
            std::process::exit(1);
        }
    };

    // Auto-bootstrap cluster if not initialized and this is node 1
    if !node.is_initialized().await {
        if meta_config.node_id == 1 {
            info!("Cluster not initialized, bootstrapping from node 1");
            if let Err(e) = node.initialize_cluster().await {
                error!("Failed to initialize cluster: {}", e);
                common::remove_pid_file(&meta_config.data_dir, NODE_TYPE);
                std::process::exit(1);
            }

            // Wait a moment for the leader election to complete
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Add configured peers as learners, then promote to voters
            // This handles the case where all peers are specified in config
            if !meta_config.peers.is_empty() {
                info!(
                    "Adding {} peer(s) to cluster: {:?}",
                    meta_config.peers.len(),
                    meta_config.peers
                );

                let mut peer_ids = std::collections::BTreeSet::new();
                peer_ids.insert(1u64); // Node 1 is already a voter

                for (i, peer_addr) in meta_config.peers.iter().enumerate() {
                    let peer_id = (i + 2) as u64; // Peers are nodes 2, 3, ...
                    peer_ids.insert(peer_id);

                    // Add peer as learner with retry
                    for attempt in 0..5 {
                        match node.add_learner(peer_id, peer_addr.clone()).await {
                            Ok(_) => {
                                info!("Added node {} ({}) as learner", peer_id, peer_addr);
                                break;
                            }
                            Err(e) => {
                                if attempt < 4 {
                                    info!("Failed to add learner {} ({}), retrying...", peer_id, e);
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                } else {
                                    error!("Failed to add learner {}: {}", peer_id, e);
                                }
                            }
                        }
                    }
                }

                // Promote all learners to voters
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                match node.change_membership(peer_ids).await {
                    Ok(_) => info!("Promoted all learners to voters"),
                    Err(e) => error!("Failed to change membership: {}", e),
                }
            }
        } else {
            info!(
                "Cluster not initialized, waiting for node 1 to bootstrap (this is node {})",
                meta_config.node_id
            );
        }
    } else {
        info!("Cluster already initialized, joining existing cluster");
    }

    // Start background tasks
    node.start_background_tasks();

    // Create gRPC services
    let grpc_service = MetaGrpcService::new(Arc::clone(&node));
    let raft_service = RaftInternalService::new(Arc::clone(&node));

    // Start gRPC server
    let addr = meta_config.advertise_addr.parse().expect("Invalid address");
    welcome!("  Meta node started on {}", meta_config.advertise_addr);
    welcome!("  HTTP API on port {}", meta_config.http_port);
    welcome!("===============================================");

    // Set up signal handler for graceful shutdown
    let data_dir = meta_config.data_dir.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");
        common::remove_pid_file(&data_dir, NODE_TYPE);
        std::process::exit(0);
    });

    // Start HTTP server for scale-in and management API
    let http_port = meta_config.http_port;
    let node_id = meta_config.node_id;
    if http_port > 0 {
        let http_state = MetaHttpState {
            node_id,
            start_time: Instant::now(),
            meta_node: Arc::clone(&node),
        };
        tokio::spawn(async move {
            if let Err(e) = start_http_server("0.0.0.0", http_port, http_state).await {
                error!("Meta HTTP server failed: {}", e);
            }
        });
    }

    if let Err(e) = start_grpc_server(addr, grpc_service, raft_service).await {
        error!("Meta gRPC server failed: {}", e);
        common::remove_pid_file(&meta_config.data_dir, NODE_TYPE);
    }
}

/// Stop a running Meta node
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    let meta_config = config.meta.unwrap_or_default();
    common::stop_node(&meta_config.data_dir, NODE_TYPE);
}
