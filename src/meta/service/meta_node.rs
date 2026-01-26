//! MetaNode - Main coordinator for the Meta service
//!
//! This is a simplified implementation that provides the core functionality.
//! The full Raft integration will be refined in subsequent iterations.

use crate::meta::config::MetaConfig;
use crate::meta::raft::{MetaNetworkFactory, MetaStateMachine, NodeConfig, NodeId};
use crate::meta::types::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// MetaNode manages a Raft-based metadata service
pub struct MetaNode {
    /// State machine (handles all metadata operations)
    state_machine: Arc<MetaStateMachine>,

    /// Node ID
    node_id: NodeId,

    /// Configuration
    config: MetaConfig,

    /// Network factory for peer connections
    network: Arc<MetaNetworkFactory>,

    /// Whether this node is the leader (simplified single-node mode for now)
    is_leader: Arc<RwLock<bool>>,
}

impl MetaNode {
    /// Create and initialize a new MetaNode
    pub async fn new(config: MetaConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node_id = config.node_id;

        // Create directories
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(config.sled_path())?;

        // Open sled database for state machine
        let state_machine_db = sled::open(config.sled_path())?;
        let state_machine = Arc::new(MetaStateMachine::new(state_machine_db));

        // Create network factory
        let network = Arc::new(MetaNetworkFactory::new());

        // Add peers to network
        for (i, peer_addr) in config.peers.iter().enumerate() {
            let peer_id = (i + 1) as u64;
            if peer_id != node_id {
                network.add_peer(peer_id, peer_addr.clone());
            }
        }

        info!(
            "MetaNode {} initialized at {}",
            node_id, config.advertise_addr
        );

        // In single-node mode, this node is always the leader
        let is_leader = config.peers.is_empty() || config.peers.len() == 1;

        Ok(Self {
            state_machine,
            node_id,
            config,
            network,
            is_leader: Arc::new(RwLock::new(is_leader)),
        })
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the state machine (for read operations)
    pub fn state_machine(&self) -> &Arc<MetaStateMachine> {
        &self.state_machine
    }

    /// Check if this node is the Raft leader
    pub async fn is_leader(&self) -> bool {
        *self.is_leader.read().await
    }

    /// Get the current Raft leader
    pub async fn current_leader(&self) -> Option<NodeId> {
        if *self.is_leader.read().await {
            Some(self.node_id)
        } else {
            None
        }
    }

    /// Get basic metrics (simplified)
    pub fn metrics(&self) -> MetaMetrics {
        MetaMetrics {
            node_id: self.node_id,
            is_leader: true, // Simplified
            current_term: 1,
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

        let response = self.state_machine.apply_command(cmd);
        match response {
            MetaResponse::LockAcquired { fencing_token } => Ok((true, fencing_token)),
            MetaResponse::LockNotAcquired { .. } => Ok((false, 0)),
            MetaResponse::Error { message } => Err(message),
            _ => Err("Unexpected response".to_string()),
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

        let response = self.state_machine.apply_command(cmd);
        match response {
            MetaResponse::LockRenewed { expires_at } => Ok(expires_at),
            MetaResponse::Error { message } => Err(message),
            _ => Err("Unexpected response".to_string()),
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

        let response = self.state_machine.apply_command(cmd);
        match response {
            MetaResponse::LeaderElected { term } => Ok((true, term)),
            MetaResponse::LeaderExists { .. } => Ok((false, 0)),
            MetaResponse::Error { message } => Err(message),
            _ => Err("Unexpected response".to_string()),
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

    /// Apply a command (simplified - direct to state machine)
    async fn apply_command(&self, cmd: MetaCommand) -> Result<u64, String> {
        let response = self.state_machine.apply_command(cmd);
        match response {
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
        Ok(())
    }
}

/// Simplified metrics
pub struct MetaMetrics {
    pub node_id: NodeId,
    pub is_leader: bool,
    pub current_term: u64,
}
