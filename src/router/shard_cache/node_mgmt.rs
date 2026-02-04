//! Node management methods for ShardCache
//!
//! Handles data node lifecycle including:
//! - Adding, updating, removing nodes
//! - Querying active/queryable nodes
//! - Node state management (suspect, healthy)
//! - Read node selection

use super::types::{DataNodeInfo, NodeState};
use super::ShardCache;
use crate::meta::types::DataNodeMode;
use std::sync::atomic::Ordering;
use tracing::{debug, info, warn};

impl ShardCache {
    // ========================================================================
    // Node Management
    // ========================================================================

    /// Get a DataNodeInfo by node_id
    pub fn get_data_node(&self, node_id: &str) -> Option<DataNodeInfo> {
        self.data_nodes.read().get(node_id).cloned()
    }

    /// Get all active Data nodes
    /// Get all active data nodes, sorted by node_id for consistent routing.
    pub fn get_active_data_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get any active node (for metadata queries that any node can answer)
    ///
    /// Returns the first active node, or None if no nodes are available.
    /// Used for schema/table queries where any node has the same metadata.
    pub fn get_any_active_node(&self) -> Option<DataNodeInfo> {
        self.data_nodes
            .read()
            .values()
            .find(|n| n.state == NodeState::Active)
            .cloned()
    }

    /// Get all data nodes that can serve reads (Active or Retiring)
    ///
    /// Retiring nodes still have data in memtable and should serve queries
    /// until the memtable is flushed and they transition to Drained.
    /// Returns nodes sorted by node_id for consistent routing.
    pub fn get_queryable_data_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active || n.state == NodeState::Retiring)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get all queryable PRIMARY Data nodes (for query routing)
    ///
    /// Includes Active and Retiring Primary nodes. Retiring nodes
    /// may have unflushed memtable data that should still be queryable.
    /// Returns nodes sorted by node_id for consistent routing.
    pub fn get_queryable_primary_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| {
                (n.state == NodeState::Active || n.state == NodeState::Retiring)
                    && n.mode == DataNodeMode::Primary
            })
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get all active PRIMARY Data nodes (for query routing)
    ///
    /// In Cold Standby architecture, only Primary nodes should serve queries.
    /// Standby nodes are cold backups that only receive replication.
    /// Returns nodes sorted by node_id for consistent hash-based routing.
    pub fn get_active_primary_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active && n.mode == DataNodeMode::Primary)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get all active STANDBY Data nodes
    ///
    /// Standby nodes only receive replication (never promoted in No-Promotion architecture).
    /// Returns nodes sorted by node_id for consistent routing.
    pub fn get_active_standby_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active && n.mode == DataNodeMode::Standby)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Update the list of Data nodes (replaces all)
    pub fn update_data_nodes(&self, nodes: Vec<DataNodeInfo>) {
        let active_count = nodes
            .iter()
            .filter(|n| n.state == NodeState::Active)
            .count();
        info!(
            "Updating data nodes: {} total, {} active",
            nodes.len(),
            active_count
        );

        let mut map = self.data_nodes.write();
        map.clear();
        for node in nodes {
            map.insert(node.node_id.clone(), node);
        }
    }

    /// Add or update a Data node
    pub fn upsert_data_node(&self, node: DataNodeInfo) {
        self.data_nodes.write().insert(node.node_id.clone(), node);
    }

    /// Remove a Data node
    pub fn remove_data_node(&self, node_id: &str) {
        self.data_nodes.write().remove(node_id);
    }

    /// Mark a node as suspect (recent write failure)
    ///
    /// Suspect nodes are temporarily avoided for new writes but can recover
    /// automatically when the next health check succeeds or after a timeout.
    pub fn mark_node_suspect(&self, node_id: &str) {
        let mut nodes = self.data_nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            if node.state == NodeState::Active {
                info!("Marking node {} as suspect due to write failure", node_id);
                node.state = NodeState::Suspect;
            }
        }
    }

    /// Record successful write to a node (clears suspect state)
    pub fn record_node_success(&self, node_id: &str) {
        let mut nodes = self.data_nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            if node.state == NodeState::Suspect {
                debug!("Node {} recovered from suspect state", node_id);
                node.state = NodeState::Active;
            }
        }
    }

    // update_node_mode() removed - No-Promotion architecture
    // Mode is set at registration time (via upsert_data_node) and never changes

    /// Select node with load balancing (for reads)
    ///
    /// In Cold Standby architecture, only Primary nodes serve queries.
    /// Standby nodes are cold backups that never serve traffic.
    /// Retiring nodes can still serve reads (they may have unflushed memtable data).
    pub fn select_node_for_read(&self) -> Option<DataNodeInfo> {
        // Prefer queryable Primary nodes for reads (Active or Retiring)
        let primary_nodes = self.get_queryable_primary_nodes();
        if !primary_nodes.is_empty() {
            let seq = self.last_event_seq.load(Ordering::Relaxed);
            let idx = (seq as usize) % primary_nodes.len();
            return Some(primary_nodes[idx].clone());
        }

        // Fall back to all queryable nodes if no Primary nodes
        // (legacy mode without Cold Standby architecture)
        let nodes = self.get_queryable_data_nodes();
        if nodes.is_empty() {
            return None;
        }

        // Simple round-robin via event sequence (good enough for now)
        let seq = self.last_event_seq.load(Ordering::Relaxed);
        let idx = (seq as usize) % nodes.len();
        Some(nodes[idx].clone())
    }

    /// Update last event sequence
    pub fn set_last_event_seq(&self, seq: u64) {
        self.last_event_seq.store(seq, Ordering::Relaxed);
    }

    /// Get last event sequence
    pub fn get_last_event_seq(&self) -> u64 {
        self.last_event_seq.load(Ordering::Relaxed)
    }
}
