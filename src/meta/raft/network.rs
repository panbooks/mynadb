//! Network implementation for peer-to-peer communication
//!
//! This module provides connection management for Meta service peers.
//! Full Raft RPC implementation will be added in a later iteration.

use crate::meta::raft::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tonic::transport::Channel;

/// Network factory that creates connections to peers
pub struct MetaNetworkFactory {
    /// Map of node ID to gRPC channel
    channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
    /// Map of node ID to address
    peers: Arc<RwLock<HashMap<NodeId, String>>>,
}

impl MetaNetworkFactory {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a peer address
    pub fn add_peer(&self, node_id: NodeId, addr: String) {
        self.peers.write().insert(node_id, addr);
    }

    /// Remove a peer
    pub fn remove_peer(&self, node_id: NodeId) {
        self.peers.write().remove(&node_id);
        self.channels.write().remove(&node_id);
    }

    /// Get all peer addresses
    pub fn get_peers(&self) -> Vec<(NodeId, String)> {
        self.peers
            .read()
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect()
    }

    /// Get or create a channel to a peer
    pub async fn get_channel(&self, node_id: NodeId) -> Result<Channel, std::io::Error> {
        // Check if we have a cached channel
        if let Some(channel) = self.channels.read().get(&node_id).cloned() {
            return Ok(channel);
        }

        // Get the peer address
        let addr = self
            .peers
            .read()
            .get(&node_id)
            .cloned()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No address for node {}", node_id),
                )
            })?;

        // Create a new channel
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    e.to_string(),
                )
            })?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30));

        let channel = endpoint.connect().await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;

        // Cache the channel
        self.channels.write().insert(node_id, channel.clone());

        Ok(channel)
    }
}

impl Default for MetaNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MetaNetworkFactory {
    fn clone(&self) -> Self {
        Self {
            channels: Arc::clone(&self.channels),
            peers: Arc::clone(&self.peers),
        }
    }
}
