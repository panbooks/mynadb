//! Router node application state
//!
//! Contains the shared state for the Router node's HTTP/Flight/MySQL services.

use super::client::DataNodePool;
use super::config::RouterConfig;
use super::routing::{QueryCoordinator, ShardCache, WriteRouter};
use crate::config::{Config, SchemaConfig};
use crate::meta::client::MetaClient;
use std::sync::Arc;
use tracing::{info, warn};

/// Application state for Router node
///
/// Different from standalone AppState - no local cache/query engine.
/// Routes all requests through WriteRouter and QueryCoordinator.
#[derive(Clone)]
pub struct RouterAppState {
    /// Full configuration
    pub config: Arc<Config>,

    /// Router-specific configuration
    pub router_config: RouterConfig,

    /// Write router for dispatching writes to Data nodes
    pub write_router: Arc<WriteRouter>,

    /// Query coordinator for distributed queries
    pub query_coordinator: Arc<QueryCoordinator>,

    /// Shard cache (partition -> node mapping)
    pub shard_cache: Arc<ShardCache>,

    /// Connection pool to Data nodes
    pub data_node_pool: Arc<DataNodePool>,

    /// Optional: Meta client for metadata queries
    pub meta_client: Option<Arc<MetaClient>>,
}

impl RouterAppState {
    /// Create a new RouterAppState from configuration
    pub async fn new(
        config: Arc<Config>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let router_config = config.router.clone().unwrap_or_default();

        // Initialize shard cache
        let shard_cache = Arc::new(ShardCache::new());

        // Initialize data node pool
        let pool_config = super::client::data_node_pool::PoolConfig::default();
        let data_node_pool = Arc::new(DataNodePool::new(pool_config));

        // Connect to Meta service (optional)
        let meta_client = match MetaClient::connect(&router_config.meta.endpoints).await {
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

        // Initialize write router (with MetaClient for replica auto-assignment if available)
        let write_router = {
            let base = WriteRouter::new(
                Arc::clone(&shard_cache),
                Arc::clone(&data_node_pool),
                config.schema.clone(),
            );
            if let Some(ref client) = meta_client {
                Arc::new(base.with_meta_client(Arc::clone(client)))
            } else {
                Arc::new(base)
            }
        };

        // Initialize query coordinator (with MetaClient for file-based routing if available)
        let query_coordinator = if let Some(ref client) = meta_client {
            Arc::new(QueryCoordinator::with_meta_client(
                Arc::clone(&shard_cache),
                Arc::clone(&data_node_pool),
                router_config.query.max_parallel_fanout,
                Arc::clone(client),
            ))
        } else {
            Arc::new(QueryCoordinator::new(
                Arc::clone(&shard_cache),
                Arc::clone(&data_node_pool),
                router_config.query.max_parallel_fanout,
            ))
        };

        Ok(Self {
            config,
            router_config,
            write_router,
            query_coordinator,
            shard_cache,
            data_node_pool,
            meta_client,
        })
    }

    /// Check if connected to Meta service
    pub fn is_meta_connected(&self) -> bool {
        self.meta_client.is_some()
    }

    /// Get active data node count
    pub fn active_data_node_count(&self) -> usize {
        self.shard_cache.get_active_data_nodes().len()
    }
}
