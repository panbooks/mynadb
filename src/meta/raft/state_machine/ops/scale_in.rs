//! Scale-In (Retirement) Operations for MetaStateMachine
//!
//! Handles graceful node retirement for cluster scale-in.

use crate::meta::types::{
    current_timestamp_ms, keys, DataNodeMode, EventType, MetaResponse, NodeInfo, NodeRole,
    NodeState, PartitionReplica, TableMeta,
};
use tracing::{debug, info, warn};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Scale-In (Retirement) Operations
    // ========================================================================

    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins partition reassignment to other nodes
    /// - Node continues serving existing queries until Drained
    pub(crate) fn start_retire(&self, node_id: &str) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Check if node is in a state that can be retired
        if node.state != NodeState::Active {
            return MetaResponse::NodeNotRetirable {
                node_id: node_id.to_string(),
                current_state: node.state,
            };
        }

        // Count partitions this node leads (for Primary) or replicates (for Standby)
        let partitions_to_reassign = if node.mode == Some(DataNodeMode::Primary) {
            self.get_replicas_by_leader(node_id).len()
        } else {
            // For Standby, count partitions where this node is a standby
            self.list_replicas()
                .iter()
                .filter(|r| r.standby_node_ids.contains(&node_id.to_string()))
                .count()
        };

        // Check if there are other nodes to take over
        let active_nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let other_primaries: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.node_id != node_id && n.mode == Some(DataNodeMode::Primary))
            .collect();

        if node.mode == Some(DataNodeMode::Primary) && other_primaries.is_empty() {
            return MetaResponse::CannotRetireNoAlternative {
                node_id: node_id.to_string(),
                reason: "No other Primary nodes available to take over partitions".to_string(),
            };
        }

        // Update node state to Retiring
        node.state = NodeState::Retiring;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Starting retirement of node {} ({:?}): {} partitions to reassign",
            node_id, node.mode, partitions_to_reassign
        );

        // Publish event for Router to stop routing to this node
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Active",
            "new_state": "Retiring",
            "partitions_to_reassign": partitions_to_reassign,
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireStarted {
            node_id: node_id.to_string(),
            partitions_to_reassign,
        }
    }

    /// Mark a node as fully drained (scale-in complete)
    pub(crate) fn complete_retire(&self, node_id: &str) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Can only complete retirement if currently Retiring
        if node.state != NodeState::Retiring {
            return MetaResponse::Error {
                message: format!(
                    "Node {} is not in Retiring state (current: {:?})",
                    node_id, node.state
                ),
            };
        }

        // Update node state to Drained
        node.state = NodeState::Drained;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Retirement complete for node {}: now Drained, safe to stop",
            node_id
        );

        // Publish event
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Retiring",
            "new_state": "Drained",
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireCompleted {
            node_id: node_id.to_string(),
        }
    }

    /// Reassign a partition from a retiring node to another Primary
    ///
    /// For Object Store clusters:
    /// - New leader takes over writes for this partition
    /// - Old data remains in S3 at the retiring node's prefix
    /// - New leader can read old data from S3 (any node can read any prefix)
    pub(crate) fn reassign_partition_for_retire(
        &self,
        partition_key: &str,
        from_node_id: &str,
        to_node_id: &str,
    ) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        // Get current replica info
        let replica_data = match self.db.get(storage_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::ReplicaNotFound {
                    partition_key: partition_key.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut replica: PartitionReplica = match bincode::deserialize(&replica_data) {
            Ok(r) => r,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Verify the current leader is the retiring node
        if replica.leader_node_id != from_node_id {
            return MetaResponse::Error {
                message: format!(
                    "Partition {} leader is {}, not {}",
                    partition_key, replica.leader_node_id, from_node_id
                ),
            };
        }

        // Verify target node exists and is a Primary
        let target_node = match self.get_node(to_node_id) {
            Some(n) => n,
            None => {
                return MetaResponse::DataNodeNotFound {
                    node_id: to_node_id.to_string(),
                }
            }
        };

        if target_node.mode != Some(DataNodeMode::Primary) {
            return MetaResponse::Error {
                message: format!("Target node {} is not a Primary", to_node_id),
            };
        }

        if target_node.state != NodeState::Active {
            return MetaResponse::Error {
                message: format!("Target node {} is not Active", to_node_id),
            };
        }

        // Update leader
        let old_leader = replica.leader_node_id.clone();
        replica.leader_node_id = to_node_id.to_string();

        // Remove target from standbys if it was there
        replica.standby_node_ids.retain(|id| id != to_node_id);

        // Don't add the retiring node as standby (it's going away)
        // Just remove it completely
        replica.standby_node_ids.retain(|id| id != from_node_id);

        replica.leader_last_seen = current_timestamp_ms();

        // Save updated replica
        let _ = self.db.insert(
            storage_key.as_bytes(),
            bincode::serialize(&replica).unwrap(),
        );

        info!(
            "Reassigned partition {} from retiring node {} to {}",
            partition_key, from_node_id, to_node_id
        );

        // Publish event for Router cache invalidation
        let payload = serde_json::json!({
            "partition_key": partition_key,
            "old_leader_id": old_leader,
            "new_leader_id": to_node_id,
            "reason": "retire",
        });
        self.publish_event(
            "replicas",
            EventType::ReplicaPromoted,
            serde_json::to_vec(&payload).unwrap_or_default(),
            to_node_id,
        );

        // Record the old leader's node_id as a legacy prefix in the affected table.
        // This ensures queries can still find data under the old prefix after scale-in.
        self.record_legacy_prefix_for_partition(partition_key, &old_leader);

        MetaResponse::PartitionReassigned {
            partition_key: partition_key.to_string(),
            from_node_id: from_node_id.to_string(),
            to_node_id: to_node_id.to_string(),
        }
    }

    /// Cancel a node retirement (scale-in abort)
    pub(crate) fn cancel_retire(&self, node_id: &str) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Can only cancel if currently Retiring (not yet Drained)
        if node.state != NodeState::Retiring {
            return MetaResponse::Error {
                message: format!(
                    "Cannot cancel retirement: node {} is {:?}, not Retiring",
                    node_id, node.state
                ),
            };
        }

        // Restore to Active state
        node.state = NodeState::Active;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Cancelled retirement of node {}: restored to Active",
            node_id
        );

        // Publish event
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Retiring",
            "new_state": "Active",
            "reason": "cancelled",
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireCancelled {
            node_id: node_id.to_string(),
        }
    }

    /// Update retirement status (progress tracking)
    pub(crate) fn update_retire_status(
        &self,
        node_id: &str,
        partitions_reassigned: usize,
        memtable_rows_remaining: u64,
    ) -> MetaResponse {
        // For now, just log the progress
        // A more complete implementation would store RetireStatus separately
        info!(
            "Retirement progress for {}: partitions_reassigned={}, memtable_rows={}",
            node_id, partitions_reassigned, memtable_rows_remaining
        );

        // Calculate progress (approximate)
        let replicas = self.get_replicas_by_leader(node_id);
        let total = replicas.len() + partitions_reassigned;
        let progress = if total > 0 {
            ((partitions_reassigned as f64 / total as f64) * 100.0) as u8
        } else {
            100
        };

        MetaResponse::RetireStatusUpdated {
            node_id: node_id.to_string(),
            progress_percent: progress,
        }
    }

    /// Remove a standby from all partition replicas (for Standby scale-in)
    pub(crate) fn remove_standby_from_replicas(&self, standby_node_id: &str) -> MetaResponse {
        let replicas = self.list_replicas();
        let mut affected_count = 0;

        for replica in replicas {
            if replica
                .standby_node_ids
                .contains(&standby_node_id.to_string())
            {
                let storage_key = replica.storage_key();

                let mut updated = replica.clone();
                updated.standby_node_ids.retain(|id| id != standby_node_id);

                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&updated).unwrap(),
                );

                affected_count += 1;

                // Publish event
                let payload = serde_json::json!({
                    "partition_key": updated.partition_key,
                    "removed_standby": standby_node_id,
                    "remaining_standbys": updated.standby_node_ids,
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaStateChanged,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    "meta",
                );
            }
        }

        info!(
            "Removed standby {} from {} partition replicas",
            standby_node_id, affected_count
        );

        MetaResponse::StandbyRemovedFromReplicas {
            standby_node_id: standby_node_id.to_string(),
            partitions_affected: affected_count,
        }
    }

    // ========================================================================
    // Legacy Prefix Recording (Scale-In Support)
    // ========================================================================

    /// Record legacy prefix in TableMeta for the table affected by partition reassignment
    ///
    /// When a partition is reassigned during scale-in, the old leader's node_id is
    /// recorded as a legacy prefix. This ensures queries can still find parquet files
    /// stored under the old prefix (e.g., "data-50/catalog/schema/table/").
    ///
    /// # Arguments
    /// * `partition_key` - Partition key like "{catalog}/{schema}/{table}/date=YYYY-MM-DD"
    /// * `old_node_id` - The retiring node's ID to record as a legacy prefix
    fn record_legacy_prefix_for_partition(&self, partition_key: &str, old_node_id: &str) {
        // Parse partition_key: {catalog}/{schema}/{table}/date=YYYY-MM-DD
        let parts: Vec<&str> = partition_key.split('/').collect();
        if parts.len() < 3 {
            warn!(
                "Cannot record legacy prefix: invalid partition_key format: {}",
                partition_key
            );
            return;
        }

        let (catalog, schema, table) = (parts[0], parts[1], parts[2]);
        let table_key = format!("{}{}/{}/{}", keys::PREFIX_TABLE, catalog, schema, table);

        // Read current TableMeta
        let table_data = match self.db.get(table_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                debug!(
                    "TableMeta not found for {}/{}/{}, skipping legacy prefix recording",
                    catalog, schema, table
                );
                return;
            }
            Err(e) => {
                warn!(
                    "Failed to read TableMeta for {}/{}/{}: {}",
                    catalog, schema, table, e
                );
                return;
            }
        };

        let mut meta: TableMeta = match bincode::deserialize(&table_data) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to deserialize TableMeta for {}/{}/{}: {}",
                    catalog, schema, table, e
                );
                return;
            }
        };

        // Add the legacy prefix
        if meta.add_legacy_prefix(old_node_id.to_string()) {
            // Save updated TableMeta
            if let Err(e) = self
                .db
                .insert(table_key.as_bytes(), bincode::serialize(&meta).unwrap())
            {
                warn!(
                    "Failed to save TableMeta with legacy prefix for {}/{}/{}: {}",
                    catalog, schema, table, e
                );
                return;
            }

            info!(
                "Added legacy prefix '{}' to table {}/{}/{} (now has {} legacy prefixes)",
                old_node_id,
                catalog,
                schema,
                table,
                meta.legacy_prefixes().len()
            );

            // Publish event for cache invalidation
            let payload = serde_json::json!({
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "legacy_prefix": old_node_id,
                "all_legacy_prefixes": meta.legacy_prefixes(),
            });
            self.publish_event(
                "tables",
                EventType::TableLegacyPrefixAdded,
                serde_json::to_vec(&payload).unwrap_or_default(),
                old_node_id,
            );
        } else {
            debug!(
                "Legacy prefix '{}' already exists for table {}/{}/{}",
                old_node_id, catalog, schema, table
            );
        }
    }
}
