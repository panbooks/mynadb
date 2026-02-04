//! Scale-In (Retirement) Operations for MetaStateMachine
//!
//! Handles graceful node retirement for cluster scale-in.
//! Uses table-level operations for routing and node-level pairing for standby management.

use crate::meta::types::{
    current_timestamp_ms, keys, DataNodeMode, EventType, MetaResponse, NodeInfo, NodeRole,
    NodeState, TableMeta,
};
use tracing::{debug, info, warn};

use super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Scale-In (Retirement) Operations
    // ========================================================================

    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins table reassignment to other nodes
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

        // Count tables this node owns (for Primary) or backs (for Standby)
        let tables_to_reassign = if node.mode == Some(DataNodeMode::Primary) {
            self.list_tables_for_node(node_id).len()
        } else {
            // For Standby, count the primaries this standby backs
            node.backed_primaries.len()
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
                reason: "No other Primary nodes available to take over tables".to_string(),
            };
        }

        // Update node state to Retiring
        node.state = NodeState::Retiring;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Starting retirement of node {} ({:?}): {} tables to reassign",
            node_id, node.mode, tables_to_reassign
        );

        // Publish event for Router to stop routing to this node
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Active",
            "new_state": "Retiring",
            "tables_to_reassign": tables_to_reassign,
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireStarted {
            node_id: node_id.to_string(),
            tables_to_reassign,
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
        tables_reassigned: usize,
        memtable_rows_remaining: u64,
    ) -> MetaResponse {
        // For now, just log the progress
        // A more complete implementation would store RetireStatus separately
        info!(
            "Retirement progress for {}: tables_reassigned={}, memtable_rows={}",
            node_id, tables_reassigned, memtable_rows_remaining
        );

        // Calculate progress (approximate)
        let tables = self.list_tables_for_node(node_id);
        let total = tables.len() + tables_reassigned;
        let progress = if total > 0 {
            ((tables_reassigned as f64 / total as f64) * 100.0) as u8
        } else {
            100
        };

        MetaResponse::RetireStatusUpdated {
            node_id: node_id.to_string(),
            progress_percent: progress,
        }
    }

    // ========================================================================
    // Legacy Prefix Recording (Scale-In Support)
    // ========================================================================

    /// Record legacy prefix in TableMeta when a table is reassigned
    ///
    /// When a table is reassigned during scale-in, the old owner's node_id is
    /// recorded as a legacy prefix. This ensures queries can still find parquet files
    /// stored under the old prefix (e.g., "data-50/catalog/schema/table/").
    ///
    /// # Arguments
    /// * `table_key` - Table key like "{catalog}/{schema}/{table}"
    /// * `old_node_id` - The retiring node's ID to record as a legacy prefix
    pub(crate) fn record_legacy_prefix_for_table(&self, table_key: &str, old_node_id: &str) {
        // Parse table_key: {catalog}/{schema}/{table}
        let parts: Vec<&str> = table_key.split('/').collect();
        if parts.len() < 3 {
            warn!(
                "Cannot record legacy prefix: invalid table_key format: {}",
                table_key
            );
            return;
        }

        let (catalog, schema, table) = (parts[0], parts[1], parts[2]);
        let storage_key = format!("{}{}/{}/{}", keys::PREFIX_TABLE, catalog, schema, table);

        // Read current TableMeta
        let table_data = match self.db.get(storage_key.as_bytes()) {
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
                .insert(storage_key.as_bytes(), bincode::serialize(&meta).unwrap())
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
