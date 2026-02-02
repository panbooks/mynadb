//! Query Operations for MetaStateMachine
//!
//! Read-only operations for querying state machine data.

use crate::meta::types::{keys, Event, LeaderInfo, LockInfo, NodeInfo, NodeRole, NodeState, PartitionReplica};
use std::sync::atomic::Ordering;

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Query Operations (read-only)
    // ========================================================================

    /// List all nodes (optionally filtered by role and state)
    pub fn list_nodes(&self, role: Option<NodeRole>, state: Option<NodeState>) -> Vec<NodeInfo> {
        let prefix = keys::PREFIX_NODE;
        let mut nodes = Vec::new();
        let mut scanned = 0;
        let mut deserialized = 0;
        let mut matched = 0;

        // Force sync to see latest writes from other handles
        let _ = self.db.flush();

        for item in self.db.scan_prefix(prefix) {
            scanned += 1;
            if let Ok((k, v)) = item {
                match bincode::deserialize::<NodeInfo>(&v) {
                    Ok(info) => {
                        deserialized += 1;
                        let role_match = role.map_or(true, |r| info.role == r);
                        let state_match = state.map_or(true, |s| info.state == s);
                        if role_match && state_match {
                            matched += 1;
                            nodes.push(info);
                        }
                    }
                    Err(e) => {
                        let key_str = String::from_utf8_lossy(&k);
                        tracing::warn!(
                            "list_nodes: failed to deserialize node key={}, len={}, error={:?}",
                            key_str,
                            v.len(),
                            e
                        );
                    }
                }
            }
        }

        tracing::info!(
            "list_nodes: role={:?}, state={:?}, scanned={}, deserialized={}, matched={}",
            role,
            state,
            scanned,
            deserialized,
            matched
        );

        nodes
    }

    /// Get a specific node by ID
    pub fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        let key = format!("{}{}", keys::PREFIX_NODE, node_id);
        self.db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// Get lock info
    pub fn get_lock(&self, key: &str) -> Option<LockInfo> {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);
        self.db
            .get(storage_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// Get leader info
    pub fn get_leader(&self, election: &str) -> Option<LeaderInfo> {
        let storage_key = format!("{}{}", keys::PREFIX_LEADER, election);
        self.db
            .get(storage_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
            .filter(|l: &LeaderInfo| !l.is_expired())
    }

    /// Get events from a starting sequence
    pub fn get_events_from(&self, start_seq: u64, limit: usize) -> Vec<Event> {
        let start_key = format!("{}{:020}", keys::PREFIX_EVENT, start_seq);
        let mut events = Vec::new();

        for item in self.db.range(start_key.as_bytes()..) {
            if let Ok((k, v)) = item {
                // Check if still in events prefix
                if !k.starts_with(keys::PREFIX_EVENT.as_bytes()) {
                    break;
                }
                if let Ok(event) = bincode::deserialize::<Event>(&v) {
                    events.push(event);
                    if events.len() >= limit {
                        break;
                    }
                }
            }
        }

        events
    }

    /// Get current event sequence
    pub fn current_sequence(&self) -> u64 {
        self.event_sequence.load(Ordering::SeqCst)
    }

    /// Get partition replica info
    pub fn get_replica(&self, partition_key: &str) -> Option<PartitionReplica> {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);
        self.db
            .get(storage_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// List all partition replicas
    pub fn list_replicas(&self) -> Vec<PartitionReplica> {
        let prefix = keys::PREFIX_REPLICA;
        let mut replicas = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(replica) = bincode::deserialize::<PartitionReplica>(&v) {
                    replicas.push(replica);
                }
            }
        }

        replicas
    }

    /// Get replicas by leader node ID
    pub fn get_replicas_by_leader(&self, leader_node_id: &str) -> Vec<PartitionReplica> {
        self.list_replicas()
            .into_iter()
            .filter(|r| r.leader_node_id == leader_node_id)
            .collect()
    }
}
