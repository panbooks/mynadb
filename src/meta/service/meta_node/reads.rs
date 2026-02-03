//! Read operations (direct from state machine)

use super::MetaNode;
use crate::meta::types::{
    DataNodeMode, Event, LeaderInfo, LockInfo, NodeInfo, NodeRole, NodeState,
};
use tracing::{info, warn};

impl MetaNode {
    /// List nodes
    pub async fn list_nodes(
        &self,
        role: Option<NodeRole>,
        state: Option<NodeState>,
    ) -> Vec<NodeInfo> {
        // Read directly from db to avoid state machine clone issues
        let prefix = crate::meta::types::keys::PREFIX_NODE;
        let mut nodes = Vec::new();

        // Flush to ensure we see latest writes
        let flush_result = self.state_machine.db().flush();
        info!(
            "list_nodes: flush result={:?}, prefix={:?}, db_size_on_disk={:?}",
            flush_result,
            prefix,
            self.state_machine.db().size_on_disk()
        );

        let mut scanned = 0;
        for item in self.state_machine.db().scan_prefix(prefix) {
            scanned += 1;
            match item {
                Ok((k, v)) => {
                    let key_str = String::from_utf8_lossy(&k);
                    info!("list_nodes: scanned key={}, value_len={}", key_str, v.len());
                    if let Ok(info) = bincode::deserialize::<NodeInfo>(&v) {
                        let role_match = role.map_or(true, |r| info.role == r);
                        let state_match = state.map_or(true, |s| info.state == s);
                        if role_match && state_match {
                            nodes.push(info);
                        }
                    }
                }
                Err(e) => {
                    warn!("list_nodes: scan error: {:?}", e);
                }
            }
        }

        info!(
            "list_nodes: scanned={}, matched={}, role={:?}, state={:?}",
            scanned,
            nodes.len(),
            role,
            state
        );

        nodes
    }

    /// Get a node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        // Read directly from db to avoid state machine clone issues
        let key = format!("{}{}", crate::meta::types::keys::PREFIX_NODE, node_id);
        let _ = self.state_machine.db().flush();
        self.state_machine
            .db()
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize::<NodeInfo>(&v).ok())
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
    // Data Node Mode Management (Cold Standby Architecture)
    // ========================================================================

    // promote_data_node() removed - using No-Promotion architecture
    // forward_promote_data_node_to_leader() removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    /// Get a data node's current operational mode
    pub async fn get_data_node_mode(&self, node_id: &str) -> Result<DataNodeMode, String> {
        self.state_machine
            .get_node(node_id)
            .and_then(|n| n.mode)
            .ok_or_else(|| format!("Node {} not found or has no mode", node_id))
    }
}
