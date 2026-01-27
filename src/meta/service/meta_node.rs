//! MetaNode - Main coordinator for the Meta service
//!
//! This module provides Raft-based coordination for the Meta service,
//! enabling high availability through consensus-based replication.

use crate::meta::config::MetaConfig;
use crate::meta::proto;
use crate::meta::raft::{
    MetaNetworkFactory, MetaRaft, MetaStateMachine, NodeId, SledLogStore, TypeConfig,
};
use crate::meta::types::*;
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Config as RaftConfig};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// MetaNode manages a Raft-based metadata service
pub struct MetaNode {
    /// State machine (handles all metadata operations)
    /// Kept for direct read operations (reads don't need consensus)
    state_machine: MetaStateMachine,

    /// Node ID
    node_id: NodeId,

    /// Configuration
    config: MetaConfig,

    /// Network factory for peer connections (kept for peer management)
    network: MetaNetworkFactory,

    /// Raft consensus instance
    raft: MetaRaft,
}

impl MetaNode {
    /// Create and initialize a new MetaNode
    pub async fn new(config: MetaConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node_id = config.node_id;

        // Create directories
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(config.sled_path())?;
        std::fs::create_dir_all(config.raft_log_path())?;

        // Open sled database for state machine
        let state_machine_db = sled::open(config.sled_path())?;
        // Create two state machines that share the same sled DB
        // One for Raft to own, one for direct reads
        let state_machine_for_raft = MetaStateMachine::new(state_machine_db.clone());
        let state_machine_for_reads = MetaStateMachine::new(state_machine_db);

        // Open sled database for Raft log store
        let log_store_db = sled::open(config.raft_log_path())?;
        let log_store = SledLogStore::new(log_store_db);

        // Create network factory - one for Raft, one for peer management
        let network_for_raft = MetaNetworkFactory::new();
        let network_for_management = MetaNetworkFactory::new();

        // Add peers to both network instances
        for (i, peer_addr) in config.peers.iter().enumerate() {
            let peer_id = (i + 1) as u64;
            if peer_id != node_id {
                network_for_raft.add_peer(peer_id, peer_addr.clone());
                network_for_management.add_peer(peer_id, peer_addr.clone());
            }
        }

        // Create Raft configuration
        let raft_config = RaftConfig {
            cluster_name: "mynadb-meta".to_string(),
            heartbeat_interval: config.raft.heartbeat_interval_ms,
            election_timeout_min: config.raft.election_timeout_min_ms,
            election_timeout_max: config.raft.election_timeout_max_ms,
            max_payload_entries: config.raft.max_payload_entries,
            ..Default::default()
        };

        // Create Raft instance (takes ownership of network, log_store, state_machine)
        let raft = openraft::Raft::<TypeConfig>::new(
            node_id,
            Arc::new(raft_config),
            network_for_raft,
            log_store,
            state_machine_for_raft,
        )
        .await?;

        info!(
            "MetaNode {} initialized at {} with Raft consensus",
            node_id, config.advertise_addr
        );

        Ok(Self {
            state_machine: state_machine_for_reads,
            node_id,
            config,
            network: network_for_management,
            raft,
        })
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the state machine (for read operations)
    pub fn state_machine(&self) -> &MetaStateMachine {
        &self.state_machine
    }

    /// Get the Raft instance (for internal use and gRPC handlers)
    pub fn raft(&self) -> &MetaRaft {
        &self.raft
    }

    /// Check if this node is the Raft leader
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// Get the current Raft leader
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Get Raft metrics
    pub fn metrics(&self) -> MetaMetrics {
        let raft_metrics = self.raft.metrics().borrow().clone();
        MetaMetrics {
            node_id: self.node_id,
            is_leader: raft_metrics.current_leader == Some(self.node_id),
            current_term: raft_metrics.current_term,
        }
    }

    // ========================================================================
    // Write Operations (in full implementation, these go through Raft)
    // ========================================================================

    /// Register a node
    pub async fn register_node(&self, info: NodeInfo, lease_ttl_ms: u64) -> Result<u64, String> {
        let cmd = MetaCommand::RegisterNode {
            info,
            lease_id: 0,
            lease_ttl_ms,
        };
        self.apply_command(cmd).await
    }

    /// Send heartbeat for a node
    pub async fn heartbeat(
        &self,
        node_id: &str,
        lease_id: u64,
        timestamp: i64,
    ) -> Result<(), String> {
        let cmd = MetaCommand::Heartbeat {
            node_id: node_id.to_string(),
            lease_id,
            timestamp,
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Deregister a node
    pub async fn deregister_node(&self, node_id: &str) -> Result<(), String> {
        let cmd = MetaCommand::DeregisterNode {
            node_id: node_id.to_string(),
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Register files
    pub async fn register_files(&self, files: Vec<FileInfo>) -> Result<u64, String> {
        let cmd = MetaCommand::RegisterFiles { files };
        self.apply_command(cmd).await
    }

    /// Remove files
    pub async fn remove_files(&self, paths: Vec<String>) -> Result<u64, String> {
        let cmd = MetaCommand::RemoveFiles { paths };
        self.apply_command(cmd).await
    }

    /// Try to acquire a lock
    pub async fn try_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<(bool, u64), String> {
        let cmd = MetaCommand::AcquireLock {
            key: key.to_string(),
            holder: holder.to_string(),
            ttl_ms,
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::LockAcquired { fencing_token } => Ok((true, fencing_token)),
                MetaResponse::LockNotAcquired { .. } => Ok((false, 0)),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response".to_string()),
            },
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding try_lock to leader node {}", leader_id);
                        return self.forward_try_lock_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward try_lock to leader (returns complex response)
    async fn forward_try_lock_to_leader(
        &self,
        leader_id: NodeId,
        cmd: MetaCommand,
    ) -> Result<(bool, u64), String> {
        let channel = self
            .network
            .get_channel_to(leader_id)
            .await
            .map_err(|e| format!("Failed to connect to leader {}: {}", leader_id, e))?;

        let mut client = proto::raft_internal_client::RaftInternalClient::new(channel);

        let command_data = bincode::serialize(&cmd)
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        let request = tonic::Request::new(proto::ForwardCommandRequest { command_data });

        let response = client
            .forward_command(request)
            .await
            .map_err(|e| format!("Forward to leader failed: {}", e))?
            .into_inner();

        if response.success {
            // Check if we have response_data for the full MetaResponse
            if !response.response_data.is_empty() {
                let meta_resp: MetaResponse = bincode::deserialize(&response.response_data)
                    .map_err(|e| format!("Failed to deserialize response: {}", e))?;
                match meta_resp {
                    MetaResponse::LockAcquired { fencing_token } => Ok((true, fencing_token)),
                    MetaResponse::LockNotAcquired { .. } => Ok((false, 0)),
                    MetaResponse::Error { message } => Err(message),
                    _ => Err("Unexpected response".to_string()),
                }
            } else {
                // Simple result - fencing_token in result field
                Ok((true, response.result))
            }
        } else {
            Err(response.error)
        }
    }

    /// Release a lock
    pub async fn unlock(
        &self,
        key: &str,
        holder: &str,
        fencing_token: u64,
    ) -> Result<(), String> {
        let cmd = MetaCommand::ReleaseLock {
            key: key.to_string(),
            holder: holder.to_string(),
            fencing_token,
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Renew a lock
    pub async fn renew_lock(
        &self,
        key: &str,
        holder: &str,
        fencing_token: u64,
        ttl_ms: u64,
    ) -> Result<i64, String> {
        let cmd = MetaCommand::RenewLock {
            key: key.to_string(),
            holder: holder.to_string(),
            fencing_token,
            ttl_ms,
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::LockRenewed { expires_at } => Ok(expires_at),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response".to_string()),
            },
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding renew_lock to leader node {}", leader_id);
                        return self.forward_renew_lock_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward renew_lock to leader
    async fn forward_renew_lock_to_leader(
        &self,
        leader_id: NodeId,
        cmd: MetaCommand,
    ) -> Result<i64, String> {
        let channel = self
            .network
            .get_channel_to(leader_id)
            .await
            .map_err(|e| format!("Failed to connect to leader {}: {}", leader_id, e))?;

        let mut client = proto::raft_internal_client::RaftInternalClient::new(channel);

        let command_data = bincode::serialize(&cmd)
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        let request = tonic::Request::new(proto::ForwardCommandRequest { command_data });

        let response = client
            .forward_command(request)
            .await
            .map_err(|e| format!("Forward to leader failed: {}", e))?
            .into_inner();

        if response.success {
            if !response.response_data.is_empty() {
                let meta_resp: MetaResponse = bincode::deserialize(&response.response_data)
                    .map_err(|e| format!("Failed to deserialize response: {}", e))?;
                match meta_resp {
                    MetaResponse::LockRenewed { expires_at } => Ok(expires_at),
                    MetaResponse::Error { message } => Err(message),
                    _ => Err("Unexpected response".to_string()),
                }
            } else {
                // result contains expires_at as u64, convert to i64
                Ok(response.result as i64)
            }
        } else {
            Err(response.error)
        }
    }

    /// Campaign for leadership in an election
    pub async fn campaign(
        &self,
        election: &str,
        node_id: &str,
        ttl_ms: u64,
    ) -> Result<(bool, u64), String> {
        let cmd = MetaCommand::Campaign {
            election: election.to_string(),
            node_id: node_id.to_string(),
            ttl_ms,
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::LeaderElected { term } => Ok((true, term)),
                MetaResponse::LeaderExists { .. } => Ok((false, 0)),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response".to_string()),
            },
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding campaign to leader node {}", leader_id);
                        return self.forward_campaign_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward campaign to leader
    async fn forward_campaign_to_leader(
        &self,
        leader_id: NodeId,
        cmd: MetaCommand,
    ) -> Result<(bool, u64), String> {
        let channel = self
            .network
            .get_channel_to(leader_id)
            .await
            .map_err(|e| format!("Failed to connect to leader {}: {}", leader_id, e))?;

        let mut client = proto::raft_internal_client::RaftInternalClient::new(channel);

        let command_data = bincode::serialize(&cmd)
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        let request = tonic::Request::new(proto::ForwardCommandRequest { command_data });

        let response = client
            .forward_command(request)
            .await
            .map_err(|e| format!("Forward to leader failed: {}", e))?
            .into_inner();

        if response.success {
            if !response.response_data.is_empty() {
                let meta_resp: MetaResponse = bincode::deserialize(&response.response_data)
                    .map_err(|e| format!("Failed to deserialize response: {}", e))?;
                match meta_resp {
                    MetaResponse::LeaderElected { term } => Ok((true, term)),
                    MetaResponse::LeaderExists { .. } => Ok((false, 0)),
                    MetaResponse::Error { message } => Err(message),
                    _ => Err("Unexpected response".to_string()),
                }
            } else {
                // result contains term
                Ok((true, response.result))
            }
        } else {
            Err(response.error)
        }
    }

    /// Resign from leadership
    pub async fn resign(&self, election: &str, node_id: &str, term: u64) -> Result<(), String> {
        let cmd = MetaCommand::Resign {
            election: election.to_string(),
            node_id: node_id.to_string(),
            term,
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Publish an event
    pub async fn publish_event(
        &self,
        topic: &str,
        event_type: EventType,
        payload: Vec<u8>,
        source_node: &str,
    ) -> Result<u64, String> {
        let cmd = MetaCommand::PublishEvent {
            topic: topic.to_string(),
            event_type,
            payload,
            source_node: source_node.to_string(),
        };
        self.apply_command(cmd).await
    }

    /// Apply a command through Raft consensus
    /// If this node is not the leader, transparently forwards to the leader
    async fn apply_command(&self, cmd: MetaCommand) -> Result<u64, String> {
        // Route through Raft for consensus
        let result = self.raft.client_write(cmd.clone()).await;

        match result {
            Ok(response) => {
                // Extract result from the MetaResponse
                match response.data {
                    MetaResponse::Success => Ok(0),
                    MetaResponse::NodeRegistered { lease_id } => Ok(lease_id),
                    MetaResponse::EventPublished { sequence } => Ok(sequence),
                    MetaResponse::LockAcquired { fencing_token } => Ok(fencing_token),
                    MetaResponse::LockReleased => Ok(0),
                    MetaResponse::LeaderElected { term } => Ok(term),
                    MetaResponse::Error { message } => Err(message),
                    _ => Ok(0),
                }
            }
            Err(e) => {
                // Handle Raft errors
                match e {
                    RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                        if let Some(leader_id) = forward.leader_id {
                            // Forward the command to the leader transparently
                            debug!(
                                "Forwarding command to leader node {} (this node: {})",
                                leader_id, self.node_id
                            );
                            self.forward_to_leader(leader_id, cmd).await
                        } else {
                            Err("No leader available".to_string())
                        }
                    }
                    _ => Err(format!("Raft error: {}", e)),
                }
            }
        }
    }

    /// Forward a command to the leader node
    async fn forward_to_leader(&self, leader_id: NodeId, cmd: MetaCommand) -> Result<u64, String> {
        // Get a gRPC channel to the leader
        let channel = self
            .network
            .get_channel_to(leader_id)
            .await
            .map_err(|e| format!("Failed to connect to leader {}: {}", leader_id, e))?;

        // Create the RaftInternal client (we use the internal service for forwarding)
        let mut client = proto::raft_internal_client::RaftInternalClient::new(channel);

        // Serialize the command
        let command_data = bincode::serialize(&cmd)
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        let request = tonic::Request::new(proto::ForwardCommandRequest { command_data });

        // Forward to leader
        let response = client
            .forward_command(request)
            .await
            .map_err(|e| format!("Forward to leader failed: {}", e))?
            .into_inner();

        if response.success {
            Ok(response.result)
        } else {
            Err(response.error)
        }
    }

    /// Execute a forwarded command (called on the leader)
    /// This is used by the gRPC handler when receiving ForwardCommand requests
    pub async fn execute_forwarded_command(
        &self,
        command_data: Vec<u8>,
    ) -> Result<(u64, Option<MetaResponse>), String> {
        // Deserialize the command
        let cmd: MetaCommand = bincode::deserialize(&command_data)
            .map_err(|e| format!("Failed to deserialize command: {}", e))?;

        // Apply through Raft (we're the leader, so this should succeed)
        let result = self.raft.client_write(cmd).await;

        match result {
            Ok(response) => {
                let (result_value, meta_response) = match &response.data {
                    MetaResponse::Success => (0, None),
                    MetaResponse::NodeRegistered { lease_id } => (*lease_id, None),
                    MetaResponse::EventPublished { sequence } => (*sequence, None),
                    MetaResponse::LockAcquired { fencing_token } => {
                        // Return full response for lock operations
                        (*fencing_token, Some(response.data.clone()))
                    }
                    MetaResponse::LockNotAcquired { .. } => {
                        // Return full response so client can see it wasn't acquired
                        (0, Some(response.data.clone()))
                    }
                    MetaResponse::LockReleased => (0, None),
                    MetaResponse::LockRenewed { expires_at } => {
                        (*expires_at as u64, Some(response.data.clone()))
                    }
                    MetaResponse::LeaderElected { term } => {
                        (*term, Some(response.data.clone()))
                    }
                    MetaResponse::LeaderExists { .. } => {
                        // Return full response so client can see leader exists
                        (0, Some(response.data.clone()))
                    }
                    MetaResponse::Error { message } => return Err(message.clone()),
                    // For any other responses, return the full response
                    other => (0, Some(other.clone())),
                };
                Ok((result_value, meta_response))
            }
            Err(e) => Err(format!("Raft error on leader: {}", e)),
        }
    }

    /// Apply a command directly to state machine (bypassing Raft)
    /// Use only for read operations or internal state queries
    fn apply_command_local(&self, cmd: MetaCommand) -> MetaResponse {
        self.state_machine.apply_command(cmd)
    }

    // ========================================================================
    // Read Operations (direct from state machine)
    // ========================================================================

    /// List nodes
    pub async fn list_nodes(
        &self,
        role: Option<NodeRole>,
        state: Option<NodeState>,
    ) -> Vec<NodeInfo> {
        self.state_machine.list_nodes(role, state)
    }

    /// Get a node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        self.state_machine.get_node(node_id)
    }

    /// List files for a partition
    pub async fn list_files_for_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition: &str,
    ) -> Vec<FileInfo> {
        self.state_machine
            .list_files_for_partition(catalog, schema, table, partition)
    }

    /// List files for a table
    pub async fn list_files_for_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Vec<FileInfo> {
        self.state_machine
            .list_files_for_table(catalog, schema, table)
    }

    /// Get lock info
    pub async fn get_lock(&self, key: &str) -> Option<LockInfo> {
        self.state_machine.get_lock(key)
    }

    /// Get leader info
    pub async fn get_leader(&self, election: &str) -> Option<LeaderInfo> {
        self.state_machine.get_leader(election)
    }

    /// Get events from a starting sequence
    pub async fn get_events_from(&self, start_seq: u64, limit: usize) -> Vec<Event> {
        self.state_machine.get_events_from(start_seq, limit)
    }

    /// Get current event sequence
    pub async fn current_sequence(&self) -> u64 {
        self.state_machine.current_sequence()
    }

    /// Subscribe to real-time event notifications
    ///
    /// Returns a broadcast receiver that will receive events as they are published.
    /// This provides instant push-based notification without polling.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.state_machine.subscribe_events()
    }

    // ========================================================================
    // Cluster Management
    // ========================================================================

    /// Initialize the Raft cluster with this node as the first member
    ///
    /// This should only be called on the first node in a new cluster.
    /// For joining existing clusters, use `add_learner` and `change_membership`.
    pub async fn initialize_cluster(&self) -> Result<(), String> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.config.advertise_addr.clone(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| format!("Failed to initialize cluster: {}", e))?;

        info!("Cluster initialized with node {} as first member", self.node_id);
        Ok(())
    }

    /// Add a learner node to the cluster
    ///
    /// Learner nodes receive log replication but don't vote in elections.
    /// Use `change_membership` to promote a learner to a voting member.
    pub async fn add_learner(&self, learner_id: NodeId, addr: String) -> Result<(), String> {
        let node = BasicNode { addr };
        self.raft
            .add_learner(learner_id, node, true)
            .await
            .map_err(|e| format!("Failed to add learner: {}", e))?;

        info!("Added learner node {} to cluster", learner_id);
        Ok(())
    }

    /// Change cluster membership (add or remove voting members)
    ///
    /// The new membership must include all nodes that should be voting members.
    pub async fn change_membership(
        &self,
        members: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), String> {
        self.raft
            .change_membership(members, false)
            .await
            .map_err(|e| format!("Failed to change membership: {}", e))?;

        info!("Cluster membership changed");
        Ok(())
    }

    /// Check if the cluster is initialized
    pub async fn is_initialized(&self) -> bool {
        self.raft.is_initialized().await.unwrap_or(false)
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    /// Start background tasks
    pub fn start_background_tasks(self: &Arc<Self>) {
        let node = Arc::clone(self);

        // Lease expiration task
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;

                // Only run on leader
                if !node.is_leader().await {
                    continue;
                }

                // Expire leases
                let cmd = MetaCommand::ExpireLeases;
                if let Err(e) = node.apply_command(cmd).await {
                    warn!("Failed to expire leases: {}", e);
                }
            }
        });
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down MetaNode {}", self.node_id);

        // Shutdown Raft (triggers graceful shutdown of replication tasks)
        if let Err(e) = self.raft.shutdown().await {
            error!("Error during Raft shutdown: {}", e);
        }

        Ok(())
    }
}

/// Simplified metrics
pub struct MetaMetrics {
    pub node_id: NodeId,
    pub is_leader: bool,
    pub current_term: u64,
}
