//! Node Operations for MetaStateMachine
//!
//! Handles node registration, heartbeat, deregistration, and state updates.

use crate::meta::types::{keys, DataNodeMode, EventType, LeaseInfo, MetaResponse, NodeInfo, NodeRole, NodeState};
use tracing::{info, warn};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Node Operations
    // ========================================================================

    pub(crate) fn register_node(&self, mut info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, info.node_id);
        let lease_id = self.next_lease_id();

        tracing::info!(
            "register_node: registering node_id={}, key={}, role={:?}, state={:?}",
            info.node_id,
            key,
            info.role,
            info.state
        );

        // Cold Standby Architecture: Auto-assign mode for Data nodes
        if info.role == NodeRole::Data && info.mode.is_none() {
            // Check if node already exists (re-registration after restart)
            let existing_node = self.get_node(&info.node_id);
            let existing_mode = existing_node.as_ref().and_then(|n| n.mode);

            if let Some(mode) = existing_mode {
                // Preserve existing mode and pairings on re-registration
                info.mode = Some(mode);
                if let Some(ref existing) = existing_node {
                    info.backing_standby = existing.backing_standby.clone();
                    info.backed_primaries = existing.backed_primaries.clone();
                }
                info!(
                    "Preserved existing mode {} for re-registering Data node {}",
                    mode, info.node_id
                );
            } else {
                // New node: assign mode based on ratio
                let (primary_count, standby_count) = self.count_data_nodes_by_mode();
                let assigned_mode = self.standby_config.mode_for_new_node(primary_count, standby_count);
                info.mode = Some(assigned_mode);

                info!(
                    "Auto-assigned mode {} to new Data node {} (primaries={}, standbys={})",
                    assigned_mode, info.node_id, primary_count, standby_count
                );
            }
        }

        // Node-level standby pairing (only for new nodes, not re-registration)
        if info.role == NodeRole::Data && info.backing_standby.is_none() && info.backed_primaries.is_empty() {
            match info.mode {
                Some(DataNodeMode::Primary) => {
                    // Find an available Standby for this Primary
                    if let Some(standby_id) = self.find_available_standby_for_primary() {
                        info.backing_standby = Some(standby_id.clone());
                        // Update the Standby's backed_primaries list
                        self.add_primary_to_standby(&standby_id, &info.node_id);
                        info!(
                            "Node-level pairing: Primary {} -> Standby {}",
                            info.node_id, standby_id
                        );
                    } else {
                        info!(
                            "No available Standby for Primary {} (will be paired when Standby joins)",
                            info.node_id
                        );
                    }
                }
                Some(DataNodeMode::Standby) => {
                    // Find Primaries that need a Standby
                    let primaries = self.find_primaries_needing_standby();
                    if !primaries.is_empty() {
                        info.backed_primaries = primaries.clone();
                        // Update each Primary's backing_standby field
                        for primary_id in &primaries {
                            self.set_primary_backing_standby(primary_id, &info.node_id);
                        }
                        info!(
                            "Node-level pairing: Standby {} backs Primaries {:?}",
                            info.node_id, primaries
                        );
                    } else {
                        info!(
                            "No Primaries need Standby {} (will be assigned when Primary joins)",
                            info.node_id
                        );
                    }
                }
                None => {}
            }
        }

        // Create lease
        let lease = LeaseInfo::new(lease_id, info.node_id.clone(), lease_ttl_ms);
        let _ = self.db.insert(
            lease.storage_key().as_bytes(),
            bincode::serialize(&lease).unwrap(),
        );

        // Store node info
        let serialized = bincode::serialize(&info).unwrap();
        let insert_result = self.db.insert(key.as_bytes(), serialized.clone());

        // Verify the insert worked
        match insert_result {
            Ok(_) => {
                // Flush to ensure durability and visibility across clones
                let _ = self.db.flush();

                // Try to read it back immediately
                match self.db.get(key.as_bytes()) {
                    Ok(Some(data)) => {
                        tracing::info!(
                            "register_node: INSERT OK - key={}, wrote {} bytes, read back {} bytes",
                            key,
                            serialized.len(),
                            data.len()
                        );
                    }
                    Ok(None) => {
                        tracing::error!(
                            "register_node: INSERT FAILED - key={} not found after insert!",
                            key
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "register_node: GET FAILED after insert - key={}, error={:?}",
                            key,
                            e
                        );
                    }
                }
            }
            Err(e) => {
                tracing::error!("register_node: INSERT FAILED - key={}, error={:?}", key, e);
            }
        }

        // Publish event
        let event_payload = serde_json::to_vec(&info).unwrap_or_default();
        self.publish_event("nodes", EventType::NodeJoined, event_payload, &info.node_id);

        MetaResponse::NodeRegistered { lease_id }
    }

    pub(crate) fn heartbeat(&self, node_id: &str, lease_id: u64, timestamp: i64) -> MetaResponse {
        // Update lease
        let lease_key = format!("{}{}", keys::PREFIX_LEASE, lease_id);
        let lease_updated = match self.db.get(lease_key.as_bytes()) {
            Ok(Some(data)) => match bincode::deserialize::<LeaseInfo>(&data) {
                Ok(mut lease) => {
                    if lease.node_id == node_id {
                        lease.renew();
                        let _ = self
                            .db
                            .insert(lease_key.as_bytes(), bincode::serialize(&lease).unwrap());
                        true
                    } else {
                        warn!(
                            "heartbeat: lease node_id mismatch - lease.node_id={}, request node_id={}",
                            lease.node_id,
                            node_id
                        );
                        false
                    }
                }
                Err(e) => {
                    warn!(
                        "heartbeat: failed to deserialize lease {}: {}",
                        lease_key,
                        e
                    );
                    false
                }
            },
            Ok(None) => {
                warn!(
                    "heartbeat: lease not found - key={}, lease_id={}",
                    lease_key,
                    lease_id
                );
                false
            }
            Err(e) => {
                warn!("heartbeat: db.get failed for lease {}: {}", lease_key, e);
                false
            }
        };

        // Update node heartbeat timestamp
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);
        let node_updated = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => match bincode::deserialize::<NodeInfo>(&data) {
                Ok(mut info) => {
                    info.last_heartbeat = timestamp;
                    if info.state == NodeState::Starting {
                        info.state = NodeState::Active;
                    }
                    let _ = self
                        .db
                        .insert(node_key.as_bytes(), bincode::serialize(&info).unwrap());
                    true
                }
                Err(e) => {
                    warn!("heartbeat: failed to deserialize node {}: {}", node_key, e);
                    false
                }
            },
            Ok(None) => {
                warn!("heartbeat: node not found - key={}", node_key);
                false
            }
            Err(e) => {
                warn!("heartbeat: db.get failed for node {}: {}", node_key, e);
                false
            }
        };

        // Flush to ensure updates are visible
        if lease_updated || node_updated {
            let _ = self.db.flush();
        }

        MetaResponse::Success
    }

    pub(crate) fn deregister_node(&self, node_id: &str) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get node info for event
        let info = self
            .db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize::<NodeInfo>(&data).ok());

        // Remove node
        let _ = self.db.remove(key.as_bytes());

        // Remove any leases for this node
        let prefix = keys::PREFIX_LEASE;
        for item in self.db.scan_prefix(prefix) {
            if let Ok((k, v)) = item {
                if let Ok(lease) = bincode::deserialize::<LeaseInfo>(&v) {
                    if lease.node_id == node_id {
                        let _ = self.db.remove(k);
                    }
                }
            }
        }

        // No-Promotion Architecture: Primary crash handled by RecoveryManager
        // - Standby flushes cache to S3 via FlushForRecovery event
        // - K8s replaces the failed Primary pod
        // - No automatic promotion, eliminating split-brain risk

        // Publish event
        if let Some(info) = info {
            let event_payload = serde_json::to_vec(&info).unwrap_or_default();
            self.publish_event("nodes", EventType::NodeLeft, event_payload, node_id);
        }

        MetaResponse::Success
    }

    pub(crate) fn update_node_state(&self, node_id: &str, state: NodeState) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, node_id);

        if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(mut info) = bincode::deserialize::<NodeInfo>(&data) {
                let old_state = info.state;
                info.state = state;
                let _ = self
                    .db
                    .insert(key.as_bytes(), bincode::serialize(&info).unwrap());

                // Publish event
                let payload = serde_json::json!({
                    "node_id": node_id,
                    "old_state": old_state as u8,
                    "new_state": state as u8
                });
                self.publish_event(
                    "nodes",
                    EventType::NodeStateChanged,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    node_id,
                );
            }
        }

        MetaResponse::Success
    }
}
