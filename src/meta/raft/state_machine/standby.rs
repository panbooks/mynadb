//! Cold Standby Architecture Operations for MetaStateMachine
//!
//! Handles standby pairing and node-level standby management.

use crate::meta::types::{keys, DataNodeMode, NodeInfo, NodeRole, NodeState};
use tracing::info;

use super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Cold Standby Architecture Operations
    // ========================================================================

    /// Count Data nodes by mode (Primary vs Standby)
    pub(crate) fn count_data_nodes_by_mode(&self) -> (usize, usize) {
        let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let primary_count = nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary))
            .count();
        let standby_count = nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .count();
        (primary_count, standby_count)
    }

    // ========================================================================
    // Node-Level Standby Pairing
    // ========================================================================

    /// Find a Standby that has capacity to back another Primary.
    ///
    /// Uses the primary_standby_ratio to determine capacity.
    /// Returns the Standby with the fewest backed Primaries (load balancing).
    pub(crate) fn find_available_standby_for_primary(&self) -> Option<String> {
        let ratio = self.standby_config.primary_standby_ratio as usize;

        // Get all active Standby nodes
        let standbys: Vec<NodeInfo> = self
            .list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
            .into_iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        // Find Standby with capacity (backed_primaries.len() < ratio)
        // Prefer the one with fewest backed primaries (load balancing)
        standbys
            .into_iter()
            .filter(|s| s.backed_primaries.len() < ratio)
            .min_by_key(|s| s.backed_primaries.len())
            .map(|s| s.node_id)
    }

    /// Find Primaries that don't have a backing Standby assigned.
    ///
    /// Limited by primary_standby_ratio (e.g., ratio=2 means return at most 2 Primaries).
    pub(crate) fn find_primaries_needing_standby(&self) -> Vec<String> {
        let ratio = self.standby_config.primary_standby_ratio as usize;

        // Get all active Primary nodes without a backing_standby
        let primaries: Vec<String> = self
            .list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
            .into_iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary) && n.backing_standby.is_none())
            .map(|n| n.node_id)
            .take(ratio) // Only take up to ratio Primaries
            .collect();

        primaries
    }

    /// Add a Primary to a Standby's backed_primaries list.
    pub(crate) fn add_primary_to_standby(&self, standby_id: &str, primary_id: &str) {
        let key = format!("{}{}", keys::PREFIX_NODE, standby_id);
        if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(mut standby_info) = bincode::deserialize::<NodeInfo>(&data) {
                if !standby_info
                    .backed_primaries
                    .contains(&primary_id.to_string())
                {
                    standby_info.backed_primaries.push(primary_id.to_string());
                    let _ = self
                        .db
                        .insert(key.as_bytes(), bincode::serialize(&standby_info).unwrap());
                    let _ = self.db.flush();
                    info!(
                        "Added Primary {} to Standby {} backed_primaries list",
                        primary_id, standby_id
                    );
                }
            }
        }
    }

    /// Set a Primary's backing_standby field.
    pub(crate) fn set_primary_backing_standby(&self, primary_id: &str, standby_id: &str) {
        let key = format!("{}{}", keys::PREFIX_NODE, primary_id);
        if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(mut primary_info) = bincode::deserialize::<NodeInfo>(&data) {
                primary_info.backing_standby = Some(standby_id.to_string());
                let _ = self
                    .db
                    .insert(key.as_bytes(), bincode::serialize(&primary_info).unwrap());
                let _ = self.db.flush();
                info!(
                    "Set Primary {} backing_standby = {}",
                    primary_id, standby_id
                );
            }
        }
    }

    /// Remove a Primary from a Standby's backed_primaries list.
    /// Used when Primary is unregistered or fails.
    pub(crate) fn remove_primary_from_standby(&self, standby_id: &str, primary_id: &str) {
        let key = format!("{}{}", keys::PREFIX_NODE, standby_id);
        if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(mut standby_info) = bincode::deserialize::<NodeInfo>(&data) {
                standby_info.backed_primaries.retain(|id| id != primary_id);
                let _ = self
                    .db
                    .insert(key.as_bytes(), bincode::serialize(&standby_info).unwrap());
                let _ = self.db.flush();
                info!(
                    "Removed Primary {} from Standby {} backed_primaries list",
                    primary_id, standby_id
                );
            }
        }
    }

    /// Clear a Primary's backing_standby field.
    /// Used when Standby is unregistered or fails.
    pub(crate) fn clear_primary_backing_standby(&self, primary_id: &str) {
        let key = format!("{}{}", keys::PREFIX_NODE, primary_id);
        if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(mut primary_info) = bincode::deserialize::<NodeInfo>(&data) {
                primary_info.backing_standby = None;
                let _ = self
                    .db
                    .insert(key.as_bytes(), bincode::serialize(&primary_info).unwrap());
                let _ = self.db.flush();
                info!("Cleared Primary {} backing_standby", primary_id);
            }
        }
    }
}
