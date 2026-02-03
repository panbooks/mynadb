//! MetaNode - Main coordinator for the Meta service
//!
//! This module provides Raft-based coordination for the Meta service,
//! enabling high availability through consensus-based replication.

mod cluster;
mod commands;
mod distribution;
mod elections;
mod lifecycle;
mod locks;
mod nodes;
mod reads;
mod scale_in;
mod tables;

use crate::meta::config::MetaConfig;
use crate::meta::raft::{
    MetaNetworkFactory, MetaRaft, MetaStateMachine, NodeId, SledLogStore, TypeConfig,
};
use openraft::Config as RaftConfig;
use std::sync::Arc;
use tracing::info;

/// MetaNode manages a Raft-based metadata service
pub struct MetaNode {
    /// State machine (handles all metadata operations)
    /// Kept for direct read operations (reads don't need consensus)
    /// The state machine has a db() method for direct database access
    pub(crate) state_machine: MetaStateMachine,

    /// Node ID
    pub(crate) node_id: NodeId,

    /// Configuration
    pub(crate) config: MetaConfig,

    /// Network factory for peer connections (kept for peer management)
    pub(crate) network: MetaNetworkFactory,

    /// Raft consensus instance
    pub(crate) raft: MetaRaft,
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
        // Create one state machine - the original goes to Raft (which applies writes)
        // Clone is kept for reads - they share the same underlying sled::Db via Arc
        let state_machine = MetaStateMachine::new(state_machine_db, config.standby.clone());
        // Clone for reads (stored in MetaNode)
        let state_machine_for_reads = state_machine.clone();
        // Original goes to Raft
        let state_machine_for_raft = state_machine;

        // Open sled database for Raft log store
        let log_store_db = sled::open(config.raft_log_path())?;
        let log_store = SledLogStore::new(log_store_db);

        // Create network factory - one for Raft, one for peer management
        let network_for_raft = MetaNetworkFactory::new();
        let network_for_management = MetaNetworkFactory::new();

        // Add peers to both network instances
        // The peers config contains addresses for OTHER nodes (excluding self).
        // We map each peer to consecutive node IDs, skipping our own node_id.
        let mut peer_id = 0u64;
        for peer_addr in config.peers.iter() {
            peer_id += 1;
            // Skip our own node_id
            if peer_id == node_id {
                peer_id += 1;
            }
            network_for_raft.add_peer(peer_id, peer_addr.clone());
            network_for_management.add_peer(peer_id, peer_addr.clone());
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
}

/// Simplified metrics
pub struct MetaMetrics {
    pub node_id: NodeId,
    pub is_leader: bool,
    pub current_term: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::raft::MetaStateMachine;
    use crate::meta::types::{keys, DataNodeMode, MetaCommand, MetaResponse, NodeInfo, NodeRole, NodeState};

    /// Test that sled::Db clones share the same underlying data
    #[test]
    fn test_sled_db_clone_shares_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Open database and write data
        let db1 = sled::open(&db_path).unwrap();
        db1.insert(b"test_key", b"test_value").unwrap();
        db1.flush().unwrap();

        // Clone the db handle
        let db2 = db1.clone();

        // Verify clone can see the data
        let value = db2.get(b"test_key").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"test_value");

        // Write through clone
        db2.insert(b"another_key", b"another_value").unwrap();
        db2.flush().unwrap();

        // Verify original can see clone's write
        let value = db1.get(b"another_key").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"another_value");
    }

    /// Test that MetaStateMachine clones share database state
    #[test]
    fn test_state_machine_clone_shares_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_sm_db");

        // Create state machine
        let db = sled::open(&db_path).unwrap();
        let sm1 = MetaStateMachine::new(db, crate::meta::config::StandbyConfig::default());

        // Clone the state machine
        let sm2 = sm1.clone();

        // Write a node directly through sm1's db
        let node_info = NodeInfo {
            node_id: "test-node-1".to_string(),
            role: NodeRole::Data,
            state: NodeState::Active,
            advertise_addr: "127.0.0.1:7200".to_string(),
            http_addr: None,
            flight_addr: Some("127.0.0.1:7200".to_string()),
            registered_at: 1000,
            last_heartbeat: 1000,
            labels: std::collections::HashMap::new(),
            mode: Some(DataNodeMode::Primary),
            backing_standby: None,
            backed_primaries: vec![],
        };

        let key = format!("{}{}", keys::PREFIX_NODE, node_info.node_id);
        let serialized = bincode::serialize(&node_info).unwrap();
        sm1.db().insert(key.as_bytes(), serialized).unwrap();
        sm1.db().flush().unwrap();

        // Verify sm2 (clone) can see the data via scan
        let prefix = keys::PREFIX_NODE;
        let mut found = false;
        for item in sm2.db().scan_prefix(prefix) {
            if let Ok((k, v)) = item {
                let key_str = String::from_utf8_lossy(&k);
                if key_str.contains("test-node-1") {
                    found = true;
                    let info: NodeInfo = bincode::deserialize(&v).unwrap();
                    assert_eq!(info.node_id, "test-node-1");
                    assert_eq!(info.role, NodeRole::Data);
                }
            }
        }
        assert!(found, "Node should be visible through cloned state machine");

        // Also verify via list_nodes method
        let nodes = sm2.list_nodes(None, None);
        assert!(
            !nodes.is_empty(),
            "list_nodes should return the inserted node"
        );
        assert_eq!(nodes[0].node_id, "test-node-1");
    }

    /// Test that writes through apply_command are visible to clones
    #[test]
    fn test_apply_command_visible_to_clone() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_apply_db");

        // Create state machine
        let db = sled::open(&db_path).unwrap();
        let sm = MetaStateMachine::new(db, crate::meta::config::StandbyConfig::default());

        // Clone for "reads" (simulating MetaNode's setup)
        let sm_for_reads = sm.clone();

        // Create a node info for registration
        let node_info = NodeInfo {
            node_id: "applied-node".to_string(),
            role: NodeRole::Data,
            state: NodeState::Active,
            advertise_addr: "127.0.0.1:7201".to_string(),
            http_addr: None,
            flight_addr: Some("127.0.0.1:7201".to_string()),
            registered_at: 2000,
            last_heartbeat: 2000,
            labels: std::collections::HashMap::new(),
            mode: Some(DataNodeMode::Primary),
            backing_standby: None,
            backed_primaries: vec![],
        };

        // Apply command through original state machine (simulating Raft apply)
        let cmd = MetaCommand::RegisterNode {
            info: node_info,
            lease_id: 0,
            lease_ttl_ms: 10000,
        };
        let response = sm.apply_command(cmd);
        assert!(matches!(response, MetaResponse::NodeRegistered { .. }));

        // Flush to ensure visibility
        sm.db().flush().unwrap();

        // Verify the clone can see the applied data
        let nodes = sm_for_reads.list_nodes(None, None);
        assert!(
            !nodes.is_empty(),
            "Clone should see node registered via apply_command"
        );
        assert_eq!(nodes[0].node_id, "applied-node");
    }

    /// Test direct db access for reads
    #[test]
    fn test_direct_db_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_direct_db");

        // Open database
        let db = sled::open(&db_path).unwrap();

        // Create state machine with this db
        let sm = MetaStateMachine::new(db.clone(), crate::meta::config::StandbyConfig::default());

        // Write through state machine's apply_command
        let node_info = NodeInfo {
            node_id: "direct-read-node".to_string(),
            role: NodeRole::Data,
            state: NodeState::Active,
            advertise_addr: "127.0.0.1:7202".to_string(),
            http_addr: None,
            flight_addr: Some("127.0.0.1:7202".to_string()),
            registered_at: 3000,
            last_heartbeat: 3000,
            labels: std::collections::HashMap::new(),
            mode: Some(DataNodeMode::Standby),
            backing_standby: None,
            backed_primaries: vec![],
        };

        let cmd = MetaCommand::RegisterNode {
            info: node_info.clone(),
            lease_id: 0,
            lease_ttl_ms: 10000,
        };
        sm.apply_command(cmd);
        sm.db().flush().unwrap();

        // Read directly from the original db reference
        let key = format!("{}{}", keys::PREFIX_NODE, "direct-read-node");
        db.flush().unwrap(); // Ensure sync
        let result = db.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "Direct db read should find the node");

        let info: NodeInfo = bincode::deserialize(&result.unwrap()).unwrap();
        assert_eq!(info.node_id, "direct-read-node");
        assert_eq!(info.mode, Some(DataNodeMode::Standby));
    }

    /// Test that state_machine.db() returns working reference
    #[test]
    fn test_state_machine_db_method() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_db_method");

        let db = sled::open(&db_path).unwrap();
        let sm = MetaStateMachine::new(db, crate::meta::config::StandbyConfig::default());

        // Write through db() method
        sm.db().insert(b"method_key", b"method_value").unwrap();
        sm.db().flush().unwrap();

        // Read back through same method
        let value = sm.db().get(b"method_key").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"method_value");

        // Clone and verify
        let sm2 = sm.clone();
        let value = sm2.db().get(b"method_key").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"method_value");
    }
}
