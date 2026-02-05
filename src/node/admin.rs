//! Admin console node runner - Web-based cluster management interface
//!
//! Usage:
//! - `mynadb admin start` - Start the admin console
//! - `mynadb admin stop` - Stop a running admin console

use super::common;
use crate::admin;
use crate::config::Config;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

/// Macro for welcome messages
macro_rules! welcome {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

const NODE_TYPE: &str = "admin";

/// Start the Admin console
pub async fn start(config_path: &str) {
    let config = Arc::new(Config::load_or_default(config_path));

    // Initialize logging
    common::init_logging(&config);

    welcome!("===============================================");
    welcome!("  MynaDB Admin Console");
    welcome!("  Version: {} ({})", env!("GIT_VERSION"), env!("GIT_HASH"));
    welcome!("===============================================");

    // Load Admin-specific config
    let admin_config = config.admin.clone().unwrap_or_default();

    info!("Starting Admin console");
    info!("  HTTP host: {}", admin_config.http_host);
    info!("  HTTP port: {}", admin_config.http_port);
    info!("  Meta endpoints: {:?}", admin_config.meta_endpoints);

    // Write PID file for stop command
    if let Err(e) = common::write_pid_file(&config.storage.data_dir, NODE_TYPE) {
        error!("Failed to write PID file: {}", e);
        std::process::exit(1);
    }

    // Build admin router and nest under /admin/
    let admin_router = admin::build_admin_router(&admin_config, admin_config.meta_endpoints.clone());
    let app = axum::Router::new().nest("/admin", admin_router);

    // Add CORS layer for development
    let app = app.layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );

    // Start HTTP server
    let http_addr = format!("{}:{}", admin_config.http_host, admin_config.http_port);
    let http_handle = common::start_http_server(&http_addr, app).await;
    welcome!("  HTTP server started on {}", http_addr);

    welcome!("  Admin console started");
    welcome!("===============================================");
    welcome!("");
    welcome!("  Access the admin console at:");
    welcome!("    http://{}:{}/admin/", admin_config.http_host, admin_config.http_port);
    welcome!("");
    welcome!("  API available at:");
    welcome!("    http://{}:{}/admin/api/v1/", admin_config.http_host, admin_config.http_port);
    welcome!("");

    // Set up signal handler for graceful shutdown
    let data_dir = config.storage.data_dir.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, cleaning up...");
        common::remove_pid_file(&data_dir, NODE_TYPE);
        std::process::exit(0);
    });

    // Wait for server
    http_handle.await.ok();

    common::remove_pid_file(&config.storage.data_dir, NODE_TYPE);
}

/// Stop a running Admin console
pub fn stop(config_path: &str) {
    let config = Config::load_or_default(config_path);
    common::stop_node(&config.storage.data_dir, NODE_TYPE);
}
