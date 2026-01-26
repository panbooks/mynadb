//! Data Node Connection Pool
//!
//! Manages gRPC connections to Data nodes with connection pooling.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

// Import generated protobuf types
pub mod proto {
    tonic::include_proto!("datanode");
}

use proto::data_node_service_client::DataNodeServiceClient;

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Max connections per Data node
    pub max_connections: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
        }
    }
}

/// Data node address info
#[derive(Debug, Clone)]
pub struct DataNodeAddr {
    pub node_id: String,
    pub grpc_addr: String,
}

/// Connection pool for Data nodes
pub struct DataNodePool {
    /// Node ID → gRPC channel mapping
    channels: DashMap<String, Channel>,

    /// Node ID → address mapping
    addresses: DashMap<String, String>,

    /// Pool configuration
    config: PoolConfig,
}

impl DataNodePool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            channels: DashMap::new(),
            addresses: DashMap::new(),
            config,
        }
    }

    /// Add or update a Data node address
    pub fn add_node(&self, node_id: &str, grpc_addr: &str) {
        info!("Adding data node to pool: {} at {}", node_id, grpc_addr);
        self.addresses.insert(node_id.to_string(), grpc_addr.to_string());
    }

    /// Remove a Data node
    pub fn remove_node(&self, node_id: &str) {
        info!("Removing data node from pool: {}", node_id);
        self.addresses.remove(node_id);
        self.channels.remove(node_id);
    }

    /// Get a gRPC client for a Data node
    pub async fn get_client(
        &self,
        node_id: &str,
    ) -> Result<DataNodeServiceClient<Channel>, String> {
        let channel = self.get_or_create_channel(node_id).await?;
        Ok(DataNodeServiceClient::new(channel))
    }

    /// Get or create a channel to a Data node
    async fn get_or_create_channel(&self, node_id: &str) -> Result<Channel, String> {
        // Check if we have a cached channel
        if let Some(channel) = self.channels.get(node_id) {
            return Ok(channel.clone());
        }

        // Get the address
        let addr = self
            .addresses
            .get(node_id)
            .map(|v| v.clone())
            .ok_or_else(|| format!("Unknown data node: {}", node_id))?;

        // Create a new channel
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("Invalid address {}: {}", addr, e))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        debug!("Connecting to data node {} at {}", node_id, addr);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {}: {}", addr, e))?;

        // Cache the channel
        self.channels.insert(node_id.to_string(), channel.clone());

        info!("Connected to data node {} at {}", node_id, addr);
        Ok(channel)
    }

    /// Check if a node is in the pool
    pub fn has_node(&self, node_id: &str) -> bool {
        self.addresses.contains_key(node_id)
    }

    /// Get all node IDs in the pool
    pub fn get_node_ids(&self) -> Vec<String> {
        self.addresses.iter().map(|e| e.key().clone()).collect()
    }

    /// Get the number of nodes in the pool
    pub fn node_count(&self) -> usize {
        self.addresses.len()
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            node_count: self.addresses.len(),
            connected_count: self.channels.len(),
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub node_count: usize,
    pub connected_count: usize,
}
