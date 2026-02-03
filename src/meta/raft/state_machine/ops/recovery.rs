//! Recovery Operations for MetaStateMachine
//!
//! Handles failover and recovery without promotion.
//! Uses table-level operations for routing and node-level pairing for standby management.

use crate::meta::types::{
    current_timestamp_ms, DataNodeMode, EventType, MetaResponse, NodeRole, NodeState,
    ScaleInRecommendation,
};
use tracing::{error, info, warn};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Recovery Operations (failover without promotion)
    // ========================================================================

    /// Trigger Standby to flush a crashed Primary's replicated data
    ///
    /// This publishes an event that the Standby node subscribes to.
    /// The Standby will then flush all replicated data to Parquet.
    pub(crate) fn trigger_standby_flush(
        &self,
        standby_id: &str,
        crashed_primary_id: &str,
    ) -> MetaResponse {
        info!(
            "Recovery: Triggering Standby {} to flush data for crashed Primary {}",
            standby_id, crashed_primary_id
        );

        // Publish event for Standby to process
        let payload = serde_json::json!({
            "standby_id": standby_id,
            "crashed_primary_id": crashed_primary_id,
            "action": "flush_for_recovery",
            "timestamp": current_timestamp_ms(),
        });

        self.publish_event(
            "recovery",
            EventType::FlushForRecovery,
            serde_json::to_vec(&payload).unwrap_or_default(),
            "meta",
        );

        MetaResponse::StandbyFlushTriggered {
            standby_id: standby_id.to_string(),
            crashed_primary_id: crashed_primary_id.to_string(),
        }
    }

    /// Assign a spare Standby to back specific Primaries (node-level pairing)
    ///
    /// Updates the node info to establish Primary-Standby pairing.
    pub(crate) fn assign_standby_to_primaries(
        &self,
        standby_id: &str,
        primary_ids: &[String],
    ) -> MetaResponse {
        info!(
            "Recovery: Assigning spare Standby {} to Primaries: {:?}",
            standby_id, primary_ids
        );

        // Update the Standby node's backed_primaries
        if let Some(mut standby_info) = self.get_node(standby_id) {
            for primary_id in primary_ids {
                if !standby_info.backed_primaries.contains(primary_id) {
                    standby_info.backed_primaries.push(primary_id.clone());
                }
            }
            standby_info.last_heartbeat = current_timestamp_ms();

            let key = format!("{}{}", crate::meta::types::keys::PREFIX_NODE, standby_id);
            let _ = self
                .db
                .insert(key.as_bytes(), bincode::serialize(&standby_info).unwrap());
        }

        // Update each Primary's backing_standby
        for primary_id in primary_ids {
            if let Some(mut primary_info) = self.get_node(primary_id) {
                if primary_info.backing_standby.is_none() {
                    primary_info.backing_standby = Some(standby_id.to_string());
                    primary_info.last_heartbeat = current_timestamp_ms();

                    let key = format!("{}{}", crate::meta::types::keys::PREFIX_NODE, primary_id);
                    let _ = self
                        .db
                        .insert(key.as_bytes(), bincode::serialize(&primary_info).unwrap());
                }
            }
        }

        // Publish event for replication to start
        let payload = serde_json::json!({
            "standby_id": standby_id,
            "primary_ids": primary_ids,
            "action": "start_replication",
        });
        self.publish_event(
            "recovery",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            "meta",
        );

        info!(
            "Recovery: Assigned Standby {} to {} Primaries",
            standby_id,
            primary_ids.len()
        );

        MetaResponse::StandbyAssignedToPrimaries {
            standby_id: standby_id.to_string(),
            primary_ids: primary_ids.to_vec(),
        }
    }

    /// Mark a Primary as unprotected (no Standby backup)
    ///
    /// Publishes a warning event that monitoring systems can subscribe to.
    pub(crate) fn mark_primary_unprotected(&self, primary_id: &str, reason: &str) -> MetaResponse {
        // Log warning - no event needed, just internal tracking
        warn!(
            "Recovery: Primary {} is now UNPROTECTED: {}",
            primary_id, reason
        );

        MetaResponse::PrimaryMarkedUnprotected {
            primary_id: primary_id.to_string(),
        }
    }

    /// Redistribute tables from a crashed Primary to remaining Primaries
    ///
    /// Evenly distributes tables across the target Primaries.
    pub(crate) fn redistribute_partitions(
        &self,
        crashed_primary_id: &str,
        target_primaries: &[String],
    ) -> MetaResponse {
        if target_primaries.is_empty() {
            error!("Recovery: Cannot redistribute tables - no target Primaries available");
            return MetaResponse::Error {
                message: "No target Primaries available for redistribution".to_string(),
            };
        }

        info!(
            "Recovery: Redistributing tables from crashed Primary {} to {} targets",
            crashed_primary_id,
            target_primaries.len()
        );

        // Get all tables owned by the crashed primary
        let tables = self.list_tables_for_node(crashed_primary_id);
        let mut redistributed_count = 0;
        let num_targets = target_primaries.len();

        for table in tables.iter() {
            let target_idx = redistributed_count % num_targets;
            let new_owner = &target_primaries[target_idx];

            // Reassign the table to the new owner
            let _ = self.reassign_table(&table.catalog, &table.schema, &table.table, new_owner);

            redistributed_count += 1;

            // Publish redistribution event
            let payload = serde_json::json!({
                "table_key": table.table_key(),
                "old_owner": crashed_primary_id,
                "new_owner": new_owner,
                "action": "redistributed",
            });
            self.publish_event(
                "tables",
                EventType::TableReassigned,
                serde_json::to_vec(&payload).unwrap_or_default(),
                "meta",
            );
        }

        info!(
            "Recovery: Redistributed {} tables from {} to {:?}",
            redistributed_count, crashed_primary_id, target_primaries
        );

        MetaResponse::TablesRedistributed {
            crashed_primary_id: crashed_primary_id.to_string(),
            tables_count: redistributed_count,
            target_primaries: target_primaries.to_vec(),
        }
    }

    /// Recommend nodes for scale-in based on load and redundancy
    ///
    /// Selection criteria for Primaries:
    /// - Prefer nodes with fewer tables (least disruption)
    /// - Ensure at least one Primary remains
    ///
    /// Selection criteria for Standbys:
    /// - Prefer nodes backing fewer Primaries
    /// - Ensure minimum standby coverage
    pub fn recommend_scale_in(
        &self,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> ScaleInRecommendation {
        let mut recommendation = ScaleInRecommendation::new();

        let active_nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let table_counts = self.get_table_counts_per_node();

        // Collect Primary and Standby nodes
        let mut primaries: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary))
            .collect();
        let mut standbys: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        // Sort Primaries by table count (ascending - prefer least-loaded)
        primaries.sort_by_key(|n| table_counts.get(&n.node_id).unwrap_or(&0));

        // Sort Standbys by backed_primaries count (ascending - prefer least-loaded)
        standbys.sort_by_key(|n| n.backed_primaries.len());

        // Recommend Primaries to retire
        let primaries_available = primaries.len();
        if reduce_primaries >= primaries_available {
            recommendation.add_warning(format!(
                "Cannot reduce {} Primaries - only {} available (need at least 1)",
                reduce_primaries, primaries_available
            ));
        } else {
            for node in primaries.iter().take(reduce_primaries) {
                let count = table_counts.get(&node.node_id).unwrap_or(&0);
                recommendation.add_primary(
                    node.node_id.clone(),
                    format!("lowest_table_count ({} tables)", count),
                );
            }
        }

        // Recommend Standbys to retire
        let standbys_available = standbys.len();
        if reduce_standbys > standbys_available {
            recommendation.add_warning(format!(
                "Cannot reduce {} Standbys - only {} available",
                reduce_standbys, standbys_available
            ));
        }
        for node in standbys
            .iter()
            .take(reduce_standbys.min(standbys_available))
        {
            let backed_count = node.backed_primaries.len();
            recommendation.add_standby(
                node.node_id.clone(),
                format!("standby_backing {} Primaries", backed_count),
            );
        }

        recommendation
    }

    /// Get table count per node for load balancing
    ///
    /// Counts tables where the node is the primary owner.
    pub(crate) fn get_table_counts_per_node(&self) -> std::collections::HashMap<String, usize> {
        let tables = self.list_all_tables();
        let mut counts = std::collections::HashMap::new();

        for table in tables {
            // Count based on primary owner
            if let Some(owner) = table.primary_owner() {
                *counts.entry(owner.to_string()).or_insert(0) += 1;
            }
        }

        counts
    }
}
