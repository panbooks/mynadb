//! Router node runner - Client protocol handler and request router
//!
//! Usage:
//! - `mynadb router start` - Start the router node
//! - `mynadb router stop` - Stop a running router node

use super::common;
use crate::config::Config;
use crate::meta::client::SubscriberConfig;
use crate::router::api as router_api;
use crate::router::config::RouterConfig;
use crate::router::flight::RouterFlightService;
use crate::router::routing::CacheInvalidationHandler;
use crate::router::state::RouterAppState;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{error, info};

/// Macro for welcome messages
macro_rules! welcome {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

const NODE_TYPE: &str = "router";

/// Start the Router node
pub async fn start(config_path: &str) {
    let config = Arc::new(Config::load_or_default(config_path));

    // Initialize logging
    common::init_logging(&config);

    welcome!("===============================================");
    welcome!("  MynaDB Router Node");
    welcome!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    welcome!("===============================================");

    // Load Router-specific config
    let router_config = config.router.clone().unwrap_or_else(RouterConfig::default);

    info!("Starting Router node {}", router_config.node_id);
    info!("  HTTP port: {}", router_config.http_port);
    info!("  Flight port: {}", router_config.flight_port);
    if router_config.mysql_enabled {
        info!("  MySQL port: {}", router_config.mysql_port);
    }

    // Write PID file for stop command
    if let Err(e) = common::write_pid_file(&config.storage.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Create router state
    let state = match RouterAppState::new(config.clone()).await {
        Ok(state) => state,
        Err(e) => {
            error!("Failed to initialize router state: {}", e);
            common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
            std::process::exit(1);
        }
    };

    // Initialize shard cache from Meta (fetch current data nodes with retry)
    // This must happen BEFORE subscribing to events to avoid race conditions
    // when router and data nodes start simultaneously.
    let mut cache_handler = CacheInvalidationHandler::new(
        Arc::clone(&state.shard_cache),
        Arc::clone(&state.data_node_pool),
    );
    if let Some(ref meta_client) = state.meta_client {
        if let Err(e) = cache_handler.init_from_meta(meta_client).await {
            error!(
                "Failed to initialize cache from Meta: {}. Will rely on events.",
                e
            );
        }
    }

    // Start cache invalidation handler (subscribe to events for updates)
    let subscriber_config = SubscriberConfig {
        endpoints: router_config.meta.endpoints.clone(),
        topics: vec!["nodes".to_string(), "files".to_string()],
        ..Default::default()
    };
    cache_handler.start(subscriber_config);

    // Build HTTP API
    let app = router_api::build_router(state.clone());

    // Start HTTP server
    let http_addr = format!("0.0.0.0:{}", router_config.http_port);
    let http_handle = common::start_http_server(&http_addr, app).await;
    welcome!("  HTTP server started on {}", http_addr);

    // Start Flight server
    let flight_addr = format!("0.0.0.0:{}", router_config.flight_port);
    let flight_service = RouterFlightService::new(state.clone());
    let flight_addr_parsed = flight_addr.parse().expect("Invalid flight address");
    let flight_handle = tokio::spawn(async move {
        info!("Router Flight server starting on {}", flight_addr);
        Server::builder()
            .add_service(flight_service.into_server())
            .serve(flight_addr_parsed)
            .await
            .expect("Router Flight server failed");
    });
    welcome!(
        "  Flight server started on 0.0.0.0:{}",
        router_config.flight_port
    );

    // Start MySQL server (if enabled)
    // Note: Router MySQL would need to route through QueryCoordinator
    // For now, skip MySQL support in router mode
    let mysql_handle: Option<tokio::task::JoinHandle<()>> = if router_config.mysql_enabled {
        info!("MySQL support in router mode not yet implemented");
        welcome!("  MySQL: not yet implemented in router mode");
        None
    } else {
        None
    };

    welcome!("  Router node started");
    welcome!("===============================================");

    // Set up signal handler for graceful shutdown
    let data_dir = config.storage.data_dir.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");
        common::remove_pid_file(&data_dir, NODE_TYPE);
        std::process::exit(0);
    });

    // Wait for servers
    tokio::select! {
        _ = http_handle => {
            error!("HTTP server stopped unexpectedly");
        },
        _ = flight_handle => {
            error!("Flight server stopped unexpectedly");
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

    common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
}

/// Stop a running Router node
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    common::stop_node(&config.storage.data_dir, NODE_TYPE);
}
