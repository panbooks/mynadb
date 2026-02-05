//! Table-level routing methods for ShardCache
//!
//! Handles table→node ownership mappings including:
//! - Get/set table owners
//! - Pending registration guards
//! - Node selection for tables

use super::types::{CachedTableMeta, DataNodeInfo, NodeState, TableKey};
use super::ShardCache;
use crate::meta::types::DataNodeMode;
use crate::utils::hash::{hash_select_clone, hash_select_index};
use tracing::{debug, info, warn};

impl ShardCache {
    // ========================================================================
    // Table-Level Routing (RECOMMENDED)
    // ========================================================================

    /// Get the primary owner node_id for a table
    pub fn get_table_owner(&self, key: &TableKey) -> Option<String> {
        self.table_owners
            .get(&key.to_path())
            .and_then(|v| v.primary_owner().map(|s| s.to_string()))
    }

    /// Get cached table metadata
    pub fn get_table_meta(&self, key: &TableKey) -> Option<CachedTableMeta> {
        self.table_owners.get(&key.to_path()).map(|v| v.clone())
    }

    /// Set a single table owner (for backward compatibility)
    pub fn set_table_owner(&self, key: &TableKey, node_id: String, version: u64) {
        self.set_table_owners(key, vec![node_id], version);
    }

    /// Set all table owners (for multi-node tables)
    pub fn set_table_owners(&self, key: &TableKey, owner_node_ids: Vec<String>, version: u64) {
        debug!(
            "Setting table owners: {} -> {:?} (v{})",
            key.to_path(),
            owner_node_ids,
            version
        );
        self.table_owners.insert(
            key.to_path(),
            CachedTableMeta {
                owner_node_ids,
                version,
                cached_at: chrono::Utc::now().timestamp_millis(),
            },
        );
    }

    /// Add an owner to a table (for rerouted writes)
    pub fn add_table_owner(&self, key: &TableKey, node_id: String, version: u64) {
        let path = key.to_path();
        if let Some(mut entry) = self.table_owners.get_mut(&path) {
            if !entry.is_owner(&node_id) {
                entry.owner_node_ids.push(node_id.clone());
                entry.version = version;
                entry.cached_at = chrono::Utc::now().timestamp_millis();
                debug!(
                    "Added table owner: {} -> {:?} (v{})",
                    path, entry.owner_node_ids, version
                );
            }
        } else {
            // Table not cached yet, add it
            self.set_table_owners(key, vec![node_id], version);
        }
    }

    /// Remove a table owner (e.g., when table is dropped)
    pub fn remove_table_owner(&self, key: &TableKey) {
        self.table_owners.remove(&key.to_path());
    }

    /// Get all owners for a table
    pub fn get_table_owners(&self, key: &TableKey) -> Option<Vec<String>> {
        self.table_owners
            .get(&key.to_path())
            .map(|entry| entry.owner_node_ids.clone())
    }

    /// Get primary owner for a table
    pub fn get_table_primary_owner(&self, key: &TableKey) -> Option<String> {
        self.table_owners
            .get(&key.to_path())
            .and_then(|entry| entry.primary_owner().map(|s| s.to_string()))
    }

    // ========================================================================
    // Pending Registration Guard (prevents duplicate Meta calls)
    // ========================================================================

    /// Try to start a registration for table+node pair
    ///
    /// Returns true if this is the first caller (should proceed with Meta call)
    /// Returns false if another write is already registering (should skip Meta call)
    ///
    /// This prevents N concurrent writes from making N Meta calls when a node fails.
    pub fn try_start_registration(&self, table_key: &TableKey, node_id: &str) -> bool {
        let key = format!("{}:{}", table_key.to_path(), node_id);
        self.pending_registrations.insert(key)
    }

    /// Finish a pending registration (always call after Meta call completes)
    pub fn finish_registration(&self, table_key: &TableKey, node_id: &str) {
        let key = format!("{}:{}", table_key.to_path(), node_id);
        self.pending_registrations.remove(&key);
    }

    /// Check if node is already registered as owner for table
    pub fn is_table_owner(&self, table_key: &TableKey, node_id: &str) -> bool {
        if let Some(entry) = self.table_owners.get(&table_key.to_path()) {
            return entry.is_owner(node_id);
        }
        false
    }

    // ========================================================================
    // Node Selection for Tables
    // ========================================================================

    /// Remove a node from all table ownership entries and partition cache
    /// Called when a node permanently leaves the cluster
    ///
    /// Returns the number of tables where this node was removed as an owner.
    /// Tables with remaining owners are kept; tables with no owners are removed.
    /// Also cleans up partition cache entries for the removed node.
    pub fn remove_tables_for_node(&self, node_id: &str) -> usize {
        let mut affected = 0;
        let mut tables_to_remove = Vec::new();

        // First pass: remove node from owner lists
        for mut entry in self.table_owners.iter_mut() {
            if entry.value().is_owner(node_id) {
                affected += 1;
                entry.value_mut().owner_node_ids.retain(|id| id != node_id);

                // If no owners left, mark for removal
                if entry.value().owner_node_ids.is_empty() {
                    tables_to_remove.push(entry.key().clone());
                }
            }
        }

        // Second pass: remove tables with no owners
        for table_path in tables_to_remove {
            self.table_owners.remove(&table_path);
            debug!("Removed table {} with no remaining owners", table_path);
        }

        // Also clean up partition cache entries for this node
        let partition_affected = self.remove_node_from_partitions(node_id);

        if affected > 0 || partition_affected > 0 {
            info!(
                "Cleaned up {} table entries and {} partition entries for departed node {}",
                affected, partition_affected, node_id
            );
        }
        affected
    }

    /// Select node for a table using table-level routing
    ///
    /// Selection priority:
    /// 1. Cached table owner (if Active)
    /// 2. Hash-based selection among Active Primary nodes
    pub fn select_node_for_table(&self, key: &TableKey) -> Option<DataNodeInfo> {
        // Check cached table owner first
        if let Some(owner_id) = self.get_table_owner(key) {
            let nodes = self.data_nodes.read();
            if let Some(owner) = nodes.get(&owner_id) {
                // Owner must be Active AND Primary
                if owner.state == NodeState::Active && owner.mode == DataNodeMode::Primary {
                    return Some(owner.clone());
                }
                // Owner is unhealthy/retiring - fall through to hash-based
                debug!(
                    "Table owner {} is {:?}/{:?}, falling back to hash routing",
                    owner_id, owner.state, owner.mode
                );
            }
        }

        // Hash-based selection among Active Primary nodes
        let primary_nodes = self.get_active_primary_nodes();
        if !primary_nodes.is_empty() {
            let idx = hash_select_index(key, primary_nodes.len());
            let selected = &primary_nodes[idx];
            debug!(
                "Hash routing for table {}: -> {}",
                key.to_path(),
                selected.node_id
            );
            return Some(selected.clone());
        }

        warn!(
            "No active Primary nodes available for table {}",
            key.to_path()
        );
        None
    }

    /// Get list of tables where a node is an owner (primary or secondary)
    pub fn get_tables_for_node(&self, node_id: &str) -> Vec<TableKey> {
        self.table_owners
            .iter()
            .filter(|entry| entry.value().is_owner(node_id))
            .filter_map(|entry| {
                let parts: Vec<&str> = entry.key().split('/').collect();
                if parts.len() == 3 {
                    Some(TableKey {
                        catalog: parts[0].to_string(),
                        schema: parts[1].to_string(),
                        table: parts[2].to_string(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get list of tables where a node is the PRIMARY owner
    pub fn get_tables_for_primary_node(&self, node_id: &str) -> Vec<TableKey> {
        self.table_owners
            .iter()
            .filter(|entry| entry.value().primary_owner() == Some(node_id))
            .filter_map(|entry| {
                let parts: Vec<&str> = entry.key().split('/').collect();
                if parts.len() == 3 {
                    Some(TableKey {
                        catalog: parts[0].to_string(),
                        schema: parts[1].to_string(),
                        table: parts[2].to_string(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Select node for table excluding specified nodes (for retry after failure)
    ///
    /// When a write fails, we retry on a different node. This method selects
    /// the next candidate using consistent hashing, skipping nodes we've already tried.
    pub fn select_node_for_table_excluding(
        &self,
        key: &TableKey,
        exclude: &std::collections::HashSet<String>,
    ) -> Option<DataNodeInfo> {
        // First try: Active Primary nodes that aren't excluded
        let primary_nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| {
                n.state == NodeState::Active
                    && n.mode == DataNodeMode::Primary
                    && !exclude.contains(&n.node_id)
            })
            .cloned()
            .collect();

        if !primary_nodes.is_empty() {
            return hash_select_clone(key, &primary_nodes);
        }

        // Fall back to all active nodes if no Primary nodes available
        let nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active && !exclude.contains(&n.node_id))
            .cloned()
            .collect();

        if nodes.is_empty() {
            warn!(
                "No active data nodes available after excluding {:?}",
                exclude
            );
            return None;
        }

        // Consistent hashing based on table key
        hash_select_clone(key, &nodes)
    }
}
