//! Background Health Checker
//!
//! Periodically pings Data nodes to detect failures faster than lease expiration.
//! This provides an additional layer of failure detection beyond the Meta service's
//! lease-based mechanism.

use super::shard_cache::ShardCache;
use crate::router::client::DataNodePool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Interval between health checks
    pub interval: Duration,
    /// Timeout for health check requests
    pub timeout: Duration,
    /// Number of consecutive failures before marking node suspect
    pub failure_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            failure_threshold: 2,
        }
    }
}

/// Background health checker for Data nodes
pub struct HealthChecker {
    pool: Arc<DataNodePool>,
    shard_cache: Arc<ShardCache>,
    config: HealthCheckConfig,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HealthChecker {
    pub fn new(
        pool: Arc<DataNodePool>,
        shard_cache: Arc<ShardCache>,
        config: HealthCheckConfig,
    ) -> Self {
        Self {
            pool,
            shard_cache,
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Start the background health check loop
    pub fn start(&mut self) {
        let pool = Arc::clone(&self.pool);
        let shard_cache = Arc::clone(&self.shard_cache);
        let config = self.config.clone();
        let shutdown = Arc::clone(&self.shutdown);

        info!(
            "Starting background health checker (interval: {:?})",
            config.interval
        );

        let handle = tokio::spawn(async move {
            Self::health_check_loop(pool, shard_cache, config, shutdown).await;
        });

        self.handle = Some(handle);
    }

    /// Stop the health checker
    pub async fn stop(&mut self) {
        info!("Stopping background health checker");
        self.shutdown.store(true, Ordering::SeqCst);

        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// The main health check loop
    async fn health_check_loop(
        pool: Arc<DataNodePool>,
        shard_cache: Arc<ShardCache>,
        config: HealthCheckConfig,
        shutdown: Arc<AtomicBool>,
    ) {
        let mut interval = tokio::time::interval(config.interval);

        loop {
            interval.tick().await;

            if shutdown.load(Ordering::SeqCst) {
                info!("Health check loop stopping due to shutdown");
                break;
            }

            // Get all nodes from the shard cache
            let nodes = shard_cache.get_active_data_nodes();

            if nodes.is_empty() {
                debug!("No active data nodes to health check");
                continue;
            }

            // Check each node in parallel
            let mut futures = Vec::new();

            for node in nodes {
                let pool = Arc::clone(&pool);
                let timeout = config.timeout;

                let future = async move {
                    let result = Self::check_node_health(&pool, &node.node_id, timeout).await;
                    (node.node_id, result)
                };

                futures.push(future);
            }

            let results = futures::future::join_all(futures).await;

            // Process results
            for (node_id, is_healthy) in results {
                if !is_healthy {
                    warn!("Health check failed for node {}", node_id);
                    shard_cache.mark_node_suspect(&node_id);
                } else {
                    // Node is healthy - clear suspect state if any
                    shard_cache.record_node_success(&node_id);
                }
            }
        }
    }

    /// Check health of a single node
    async fn check_node_health(pool: &DataNodePool, node_id: &str, timeout: Duration) -> bool {
        // Use DoAction with HEALTH action type
        let result = tokio::time::timeout(timeout, pool.do_action(node_id, "HEALTH", b"{}")).await;

        match result {
            Ok(Ok(_)) => {
                debug!("Health check passed for node {}", node_id);
                true
            }
            Ok(Err(e)) => {
                debug!("Health check failed for node {}: {}", node_id, e);
                false
            }
            Err(_) => {
                debug!("Health check timed out for node {}", node_id);
                false
            }
        }
    }
}

impl Drop for HealthChecker {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}
