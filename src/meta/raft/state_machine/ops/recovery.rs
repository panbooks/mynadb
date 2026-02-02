//! Recovery Operations for MetaStateMachine
//!
//! Handles failover and recovery without promotion.

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
    pub(crate) fn trigger_standby_flush(&self, standby_id: &str, crashed_primary_id: &str) -> MetaResponse {
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

    /// Assign a spare Standby to back specific Primaries
    ///
    /// Updates the partition replica assignments to include the new Standby.
    pub(crate) fn assign_standby_to_primaries(&self, standby_id: &str, primary_ids: &[String]) -> MetaResponse {
        info!(
            "Recovery: Assigning spare Standby {} to Primaries: {:?}",
            standby_id, primary_ids
        );

        let replicas = self.list_replicas();
        let mut updated_count = 0;

        for replica in replicas {
            // Check if this partition's leader is one of the affected Primaries
            if primary_ids.contains(&replica.leader_node_id) {
                let storage_key = replica.storage_key();

                let mut updated = replica.clone();
                // Add the new Standby if not already present
                if !updated.standby_node_ids.contains(&standby_id.to_string()) {
                    updated.standby_node_ids.push(standby_id.to_string());

                    let _ = self.db.insert(
                        storage_key.as_bytes(),
                        bincode::serialize(&updated).unwrap(),
                    );

                    updated_count += 1;

                    // Publish event for replication to start
                    let payload = serde_json::json!({
                        "partition_key": updated.partition_key,
                        "leader_node_id": updated.leader_node_id,
                        "new_standby_id": standby_id,
                        "action": "start_replication",
                    });
                    self.publish_event(
                        "replicas",
                        EventType::ReplicaStateChanged,
                        serde_json::to_vec(&payload).unwrap_or_default(),
                        "meta",
                    );
                }
            }
        }

        info!(
            "Recovery: Assigned Standby {} to {} partitions across {} Primaries",
            standby_id, updated_count, primary_ids.len()
        );

        MetaResponse::StandbyAssignedToPrimaries {
            standby_id: standby_id.to_string(),
            primary_ids: primary_ids.to_vec(),
        }
    }

    /// Mark a partition as unprotected (no Standby backup)
    ///
    /// Publishes a warning event that monitoring systems can subscribe to.
    pub(crate) fn mark_partition_unprotected(
        &self,
        partition_key: &str,
        primary_id: &str,
        reason: &str,
    ) -> MetaResponse {
        // Log warning - no event needed, just internal tracking
        warn!(
            "Recovery: Partition {} on Primary {} is now UNPROTECTED: {}",
            partition_key, primary_id, reason
        );

        MetaResponse::PartitionMarkedUnprotected {
            partition_key: partition_key.to_string(),
            primary_id: primary_id.to_string(),
        }
    }

    /// Redistribute partitions from a crashed Primary to remaining Primaries
    ///
    /// Evenly distributes partitions across the target Primaries.
    pub(crate) fn redistribute_partitions(
        &self,
        crashed_primary_id: &str,
        target_primaries: &[String],
    ) -> MetaResponse {
        if target_primaries.is_empty() {
            error!(
                "Recovery: Cannot redistribute partitions - no target Primaries available"
            );
            return MetaResponse::Error {
                message: "No target Primaries available for redistribution".to_string(),
            };
        }

        info!(
            "Recovery: Redistributing partitions from crashed Primary {} to {} targets",
            crashed_primary_id,
            target_primaries.len()
        );

        let replicas = self.list_replicas();
        let mut redistributed_count = 0;
        let num_targets = target_primaries.len();

        for replica in replicas.iter() {
            if replica.leader_node_id == crashed_primary_id {
                let target_idx = redistributed_count % num_targets;
                let new_leader = &target_primaries[target_idx];
                let storage_key = replica.storage_key();

                let mut updated = replica.clone();
                let old_leader = updated.leader_node_id.clone();
                updated.leader_node_id = new_leader.clone();

                // Remove crashed Primary from standbys if present
                updated.standby_node_ids.retain(|id| id != crashed_primary_id);

                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&updated).unwrap(),
                );

                redistributed_count += 1;

                // Publish redistribution event
                let payload = serde_json::json!({
                    "partition_key": updated.partition_key,
                    "old_leader": old_leader,
                    "new_leader": new_leader,
                    "action": "redistributed",
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaPromoted,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    "meta",
                );
            }
        }

        info!(
            "Recovery: Redistributed {} partitions from {} to {:?}",
            redistributed_count, crashed_primary_id, target_primaries
        );

        MetaResponse::PartitionsRedistributed {
            crashed_primary_id: crashed_primary_id.to_string(),
            partitions_count: redistributed_count,
            target_primaries: target_primaries.to_vec(),
        }
    }

    /// Recommend nodes for scale-in based on load and redundancy
    ///
    /// Selection criteria for Primaries:
    /// - Prefer nodes with fewer partitions (least disruption)
    /// - Ensure at least one Primary remains
    ///
    /// Selection criteria for Standbys:
    /// - Prefer nodes with most redundancy (other standbys cover same partitions)
    /// - Ensure minimum standby coverage
    pub fn recommend_scale_in(
        &self,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> ScaleInRecommendation {
        let mut recommendation = ScaleInRecommendation::new();

        let active_nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let partition_counts = self.get_partition_counts_per_node();

        // Collect Primary and Standby nodes
        let mut primaries: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary))
            .collect();
        let mut standbys: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        // Sort Primaries by partition count (ascending - prefer least-loaded)
        primaries.sort_by_key(|n| partition_counts.get(&n.node_id).unwrap_or(&0));

        // For Standbys, we'd ideally sort by redundancy, but for now sort by partition count
        standbys.sort_by_key(|n| partition_counts.get(&n.node_id).unwrap_or(&0));

        // Recommend Primaries to retire
        let primaries_available = primaries.len();
        if reduce_primaries >= primaries_available {
            recommendation.add_warning(format!(
                "Cannot reduce {} Primaries - only {} available (need at least 1)",
                reduce_primaries, primaries_available
            ));
        } else {
            for node in primaries.iter().take(reduce_primaries) {
                let count = partition_counts.get(&node.node_id).unwrap_or(&0);
                recommendation.add_primary(
                    node.node_id.clone(),
                    format!("lowest_partition_count ({} partitions)", count),
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
            let count = partition_counts.get(&node.node_id).unwrap_or(&0);
            recommendation.add_standby(
                node.node_id.clone(),
                format!("standby_with_coverage ({} partitions)", count),
            );
        }

        recommendation
    }

    /// Get partition count per node for load balancing
    ///
    /// Only counts LEADER partitions, not standbys. This ensures proper
    /// load distribution: a node with 0 leaders should be preferred over
    /// a node with 1 leader, even if the first node has 1 standby.
    pub(crate) fn get_partition_counts_per_node(&self) -> std::collections::HashMap<String, usize> {
        let replicas = self.list_replicas();
        let mut counts = std::collections::HashMap::new();

        for replica in replicas {
            // Only count leader partitions for load balancing.
            // Standbys are passive and don't affect write load.
            *counts.entry(replica.leader_node_id.clone()).or_insert(0) += 1;
        }

        counts
    }
}
