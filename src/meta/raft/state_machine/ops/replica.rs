//! Partition Replica Operations for MetaStateMachine
//!
//! Handles partition replica assignment, updates, and rebalancing.

use crate::meta::types::{
    current_timestamp_ms, keys, EventType, MetaResponse, NodeRole, NodeState, PartitionReplica,
    ReplicaState,
};
use tracing::info;

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Partition Replica Operations (Warm Standby)
    // ========================================================================

    /// Assign a partition replica (leader + standbys)
    pub(crate) fn assign_replica(
        &self,
        partition_key: &str,
        leader_node_id: &str,
        standby_node_ids: Vec<String>,
        replication_factor: u8,
    ) -> MetaResponse {
        let replica = PartitionReplica::new(
            partition_key.to_string(),
            leader_node_id.to_string(),
            standby_node_ids.clone(),
            replication_factor,
        );

        let storage_key = replica.storage_key();
        let _ = self.db.insert(
            storage_key.as_bytes(),
            bincode::serialize(&replica).unwrap(),
        );

        // Publish event for Router cache invalidation
        let payload = serde_json::json!({
            "partition_key": partition_key,
            "leader_node_id": leader_node_id,
            "standby_node_ids": standby_node_ids,
            "replication_factor": replication_factor,
        });
        self.publish_event(
            "replicas",
            EventType::ReplicaAssigned,
            serde_json::to_vec(&payload).unwrap_or_default(),
            leader_node_id,
        );

        MetaResponse::ReplicaAssigned {
            partition_key: partition_key.to_string(),
            leader_node_id: leader_node_id.to_string(),
        }
    }

    // promote_standby() removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    /// Update replica state (replication progress)
    pub(crate) fn update_replica_state(
        &self,
        partition_key: &str,
        state: ReplicaState,
        last_replicated_seq: u64,
        replication_lag_ms: u64,
    ) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(mut replica) = bincode::deserialize::<PartitionReplica>(&data) {
                replica.state = state;
                replica.last_replicated_seq = last_replicated_seq;
                replica.replication_lag_ms = replication_lag_ms;
                replica.leader_last_seen = current_timestamp_ms();

                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&replica).unwrap(),
                );

                // Publish event
                let payload = serde_json::json!({
                    "partition_key": partition_key,
                    "state": format!("{:?}", state),
                    "last_replicated_seq": last_replicated_seq,
                    "replication_lag_ms": replication_lag_ms,
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaStateChanged,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    &replica.leader_node_id,
                );

                return MetaResponse::ReplicaUpdated {
                    partition_key: partition_key.to_string(),
                };
            }
        }

        MetaResponse::ReplicaNotFound {
            partition_key: partition_key.to_string(),
        }
    }

    /// Remove a partition replica
    pub(crate) fn remove_replica(&self, partition_key: &str) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        if self.db.remove(storage_key.as_bytes()).is_ok() {
            return MetaResponse::ReplicaRemoved {
                partition_key: partition_key.to_string(),
            };
        }

        MetaResponse::ReplicaNotFound {
            partition_key: partition_key.to_string(),
        }
    }

    /// Auto-assign a replica for a new partition using ReplicaAssigner
    ///
    /// Uses the configured load balancing strategy and zone-awareness
    /// to select leader and standbys.
    pub(crate) fn auto_assign_replica(&self, partition_key: &str) -> MetaResponse {
        // Check if replica already exists
        if self.get_replica(partition_key).is_some() {
            return MetaResponse::ReplicaAlreadyExists {
                partition_key: partition_key.to_string(),
            };
        }

        // Get active Data nodes
        let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        if nodes.is_empty() {
            return MetaResponse::NoDataNodesAvailable;
        }

        // Build partition counts for load balancing
        let partition_counts = self.get_partition_counts_per_node();

        // Use default config for auto-assignment
        // In production, this would come from MetaConfig
        let config = crate::meta::config::ReplicationAssignmentConfig::default();
        let assigner = crate::meta::replica_assigner::ReplicaAssigner::new(config);

        match assigner.assign_new_partition(partition_key, &nodes, &partition_counts) {
            Some(replica) => {
                let leader_node_id = replica.leader_node_id.clone();
                let standby_node_ids = replica.standby_node_ids.clone();
                let replication_factor = replica.replication_factor;

                // Store the replica
                let storage_key = replica.storage_key();
                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&replica).unwrap(),
                );

                // Publish event
                let payload = serde_json::json!({
                    "partition_key": partition_key,
                    "leader_node_id": &leader_node_id,
                    "standby_node_ids": &standby_node_ids,
                    "replication_factor": replication_factor,
                    "auto_assigned": true,
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaAssigned,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    "meta",
                );

                info!(
                    "Auto-assigned replica for partition {}: leader={}, standbys={:?}, rf={}",
                    partition_key, leader_node_id, standby_node_ids, replication_factor
                );

                MetaResponse::ReplicaAutoAssigned {
                    partition_key: partition_key.to_string(),
                    leader_node_id,
                    standby_node_ids,
                    replication_factor,
                }
            }
            None => MetaResponse::NoDataNodesAvailable,
        }
    }

    /// Rebalance replicas when a new node joins
    ///
    /// Adds missing standbys to under-replicated partitions.
    pub(crate) fn rebalance_on_node_join(&self, node_id: &str) -> MetaResponse {
        let node = match self.get_node(node_id) {
            Some(n) => n,
            None => {
                return MetaResponse::Error {
                    message: format!("Node {} not found", node_id),
                }
            }
        };

        if node.role != NodeRole::Data || node.state != NodeState::Active {
            return MetaResponse::RebalanceCompleted { updated_count: 0 };
        }

        let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let replicas = self.list_replicas();

        let config = crate::meta::config::ReplicationAssignmentConfig::default();
        let assigner = crate::meta::replica_assigner::ReplicaAssigner::new(config);

        let updates = assigner.rebalance_on_node_join(&node, &replicas, &nodes);
        let updated_count = updates.len();

        for updated in updates {
            let storage_key = updated.storage_key();
            let _ = self.db.insert(
                storage_key.as_bytes(),
                bincode::serialize(&updated).unwrap(),
            );

            // Publish event
            let payload = serde_json::json!({
                "partition_key": &updated.partition_key,
                "leader_node_id": &updated.leader_node_id,
                "standby_node_ids": &updated.standby_node_ids,
                "rebalance_reason": "node_join",
                "new_node": node_id,
            });
            self.publish_event(
                "replicas",
                EventType::ReplicaAssigned,
                serde_json::to_vec(&payload).unwrap_or_default(),
                "meta",
            );

            info!(
                "Rebalanced replica {} after node {} join: added standby",
                updated.partition_key, node_id
            );
        }

        MetaResponse::RebalanceCompleted { updated_count }
    }
}
