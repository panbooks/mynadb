//! Network implementation for Raft peer-to-peer communication
//!
//! This module implements the OpenRaft network traits for Meta service
//! peer communication using gRPC.

use super::{NodeId, TypeConfig};
use crate::meta::proto;
use crate::meta::proto::raft_internal_client::RaftInternalClient;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, BasicNode, RaftNetwork, RaftNetworkFactory};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

/// Network factory that creates connections to peers
#[derive(Clone)]
pub struct MetaNetworkFactory {
    /// Map of node ID to gRPC channel
    channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
    /// Map of node ID to address (for peer management)
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
        debug!("Adding peer {} at {}", node_id, addr);
        self.peers.write().insert(node_id, addr);
    }

    /// Remove a peer
    pub fn remove_peer(&self, node_id: NodeId) {
        debug!("Removing peer {}", node_id);
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
    async fn get_channel(&self, node_id: NodeId, addr: &str) -> Result<Channel, String> {
        // Check if we have a cached channel
        if let Some(channel) = self.channels.read().get(&node_id).cloned() {
            return Ok(channel);
        }

        // Create a new channel
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("Invalid address {}: {}", addr, e))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30));

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {}: {}", addr, e))?;

        // Cache the channel
        self.channels.write().insert(node_id, channel.clone());
        debug!("Connected to peer node {} at {}", node_id, addr);

        Ok(channel)
    }
}

impl Default for MetaNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<TypeConfig> for MetaNetworkFactory {
    type Network = MetaNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        MetaNetworkConnection {
            target,
            target_addr: node.addr.clone(),
            factory: self.clone(),
        }
    }
}

/// A single network connection to a peer node
pub struct MetaNetworkConnection {
    /// Target node ID
    target: NodeId,
    /// Target node address
    target_addr: String,
    /// Reference to factory for channel reuse
    factory: MetaNetworkFactory,
}

impl MetaNetworkConnection {
    /// Get a gRPC client for the target node
    async fn get_client(&self) -> Result<RaftInternalClient<Channel>, Unreachable> {
        let channel = self
            .factory
            .get_channel(self.target, &self.target_addr)
            .await
            .map_err(|e| {
                warn!("Failed to get channel to {}: {}", self.target, e);
                Unreachable::from(AnyError::error(e))
            })?;

        Ok(RaftInternalClient::new(channel))
    }
}

impl RaftNetwork<TypeConfig> for MetaNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let mut client = self.get_client().await?;

        // Serialize the request
        let data = bincode::serialize(&rpc).map_err(|e| {
            error!("Failed to serialize AppendEntries request: {}", e);
            Unreachable::from(AnyError::error(e.to_string()))
        })?;

        let request = tonic::Request::new(proto::RaftMessage { data });

        // Send the RPC
        let response = client
            .append_entries(request)
            .await
            .map_err(|e| {
                warn!("AppendEntries RPC to {} failed: {}", self.target, e);
                Unreachable::from(AnyError::error(e.to_string()))
            })?;

        // Deserialize the response
        let resp: AppendEntriesResponse<NodeId> =
            bincode::deserialize(&response.into_inner().data).map_err(|e| {
                error!("Failed to deserialize AppendEntries response: {}", e);
                Unreachable::from(AnyError::error(e.to_string()))
            })?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let mut client = self.get_client().await?;

        // Serialize the request
        let data = bincode::serialize(&rpc).map_err(|e| {
            error!("Failed to serialize InstallSnapshot request: {}", e);
            Unreachable::from(AnyError::error(e.to_string()))
        })?;

        let request = tonic::Request::new(proto::RaftMessage { data });

        // Send the RPC
        let response = client
            .install_snapshot(request)
            .await
            .map_err(|e| {
                warn!("InstallSnapshot RPC to {} failed: {}", self.target, e);
                Unreachable::from(AnyError::error(e.to_string()))
            })?;

        // Deserialize the response
        let resp: InstallSnapshotResponse<NodeId> =
            bincode::deserialize(&response.into_inner().data).map_err(|e| {
                error!("Failed to deserialize InstallSnapshot response: {}", e);
                Unreachable::from(AnyError::error(e.to_string()))
            })?;

        Ok(resp)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let mut client = self.get_client().await?;

        // Serialize the request
        let data = bincode::serialize(&rpc).map_err(|e| {
            error!("Failed to serialize Vote request: {}", e);
            Unreachable::from(AnyError::error(e.to_string()))
        })?;

        let request = tonic::Request::new(proto::RaftMessage { data });

        // Send the RPC
        let response = client
            .vote(request)
            .await
            .map_err(|e| {
                warn!("Vote RPC to {} failed: {}", self.target, e);
                Unreachable::from(AnyError::error(e.to_string()))
            })?;

        // Deserialize the response
        let resp: VoteResponse<NodeId> =
            bincode::deserialize(&response.into_inner().data).map_err(|e| {
                error!("Failed to deserialize Vote response: {}", e);
                Unreachable::from(AnyError::error(e.to_string()))
            })?;

        Ok(resp)
    }
}
