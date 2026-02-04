//! Table Operations for MetaStateMachine
//!
//! Implements table-level routing - the recommended approach for cluster mode.
//! Tables can have multiple owner nodes for:
//! - Write reroute: When primary owner fails, writes go to fallback node
//! - Large table distribution: Future feature to spread data across nodes
//!
//! Key benefits over file-level routing:
//! - 600x fewer Meta entries (100K vs 60M)
//! - Zero daily growth (new partitions don't create entries)
//! - Single routing lookup per query (scatter-gather for multi-owner)
//! - Excellent cache locality for time-series workloads

use crate::meta::types::{
    keys, Event, EventType, MetaResponse, NodeState, TableMeta, TableStatsDelta,
};
use tracing::{debug, info, warn};

use super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Table Operations (Write Path)
    // ========================================================================

    /// Register a new table with its owner node
    ///
    /// Called when a Data node first receives data for a new table.
    /// The node that first writes to a table becomes its primary owner.
    ///
    /// This is idempotent - if the table already exists, it returns
    /// TableAlreadyExists with all current owners.
    pub(crate) fn register_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        owner_node_id: &str,
    ) -> MetaResponse {
        let key = TableMeta::key_for(catalog, schema, table);

        // Check if table already exists
        if let Ok(Some(existing)) = self.db.get(key.as_bytes()) {
            if let Ok(meta) = bincode::deserialize::<TableMeta>(&existing) {
                let primary = meta.primary_owner().unwrap_or("").to_string();
                debug!(
                    "Table already exists: {}.{}.{} owned by {:?} (primary: {})",
                    catalog, schema, table, meta.owner_node_ids, primary
                );
                return MetaResponse::TableAlreadyExists {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                    current_owner: primary,
                    all_owners: meta.owner_node_ids.clone(),
                };
            }
        }

        // Create new table metadata
        let meta = TableMeta::new(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            owner_node_id.to_string(),
        );

        // Store in sled
        if let Err(e) = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&meta).unwrap())
        {
            warn!("Failed to register table: {}", e);
            return MetaResponse::Error {
                message: format!("Failed to register table: {}", e),
            };
        }

        info!(
            "Table registered: {}.{}.{} -> {}",
            catalog, schema, table, owner_node_id
        );

        // Publish event for cache invalidation
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "owner_node_ids": [owner_node_id],
        });
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::SchemaChanged,
            format!("{}.{}.{}", catalog, schema, table),
            serde_json::to_vec(&payload).unwrap_or_default(),
            owner_node_id.to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::TableRegistered {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            owner_node_id: owner_node_id.to_string(),
            version: meta.version,
        }
    }

    /// Add an additional owner node to a table
    ///
    /// Called when a write is rerouted to a fallback node after primary failure.
    /// The new node becomes a secondary owner - queries should scatter-gather
    /// to all owners to get complete data.
    pub(crate) fn add_table_owner(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        new_owner_node_id: &str,
    ) -> MetaResponse {
        let key = TableMeta::key_for(catalog, schema, table);

        // Load existing table
        let existing = match self.db.get(key.as_bytes()) {
            Ok(Some(data)) => match bincode::deserialize::<TableMeta>(&data) {
                Ok(meta) => meta,
                Err(e) => {
                    return MetaResponse::Error {
                        message: format!("Failed to deserialize table: {}", e),
                    }
                }
            },
            Ok(None) => {
                // Table doesn't exist - register it with this node as primary
                return self.register_table(catalog, schema, table, new_owner_node_id);
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Failed to read table: {}", e),
                }
            }
        };

        // Check if already an owner
        if existing.is_owner(new_owner_node_id) {
            return MetaResponse::TableOwnerExists {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
                node_id: new_owner_node_id.to_string(),
                all_owners: existing.owner_node_ids.clone(),
            };
        }

        // Add new owner
        let mut updated = existing;
        updated.add_owner(new_owner_node_id.to_string());

        // Store updated table
        if let Err(e) = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&updated).unwrap())
        {
            return MetaResponse::Error {
                message: format!("Failed to add table owner: {}", e),
            };
        }

        info!(
            "Table owner added: {}.{}.{} now owned by {:?}",
            catalog, schema, table, updated.owner_node_ids
        );

        // Publish event for cache invalidation
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "added_owner": new_owner_node_id,
            "all_owners": updated.owner_node_ids,
        });
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::TableOwnerAdded,
            format!("{}.{}.{}", catalog, schema, table),
            serde_json::to_vec(&payload).unwrap_or_default(),
            new_owner_node_id.to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::TableOwnerAdded {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            new_owner: new_owner_node_id.to_string(),
            all_owners: updated.owner_node_ids,
            version: updated.version,
        }
    }

    /// Remove an owner node from a table
    ///
    /// Called when a node leaves the cluster permanently or when consolidating
    /// ownership after data migration.
    pub(crate) fn remove_table_owner(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        node_id: &str,
    ) -> MetaResponse {
        let key = TableMeta::key_for(catalog, schema, table);

        // Load existing table
        let existing = match self.db.get(key.as_bytes()) {
            Ok(Some(data)) => match bincode::deserialize::<TableMeta>(&data) {
                Ok(meta) => meta,
                Err(e) => {
                    return MetaResponse::Error {
                        message: format!("Failed to deserialize table: {}", e),
                    }
                }
            },
            Ok(None) => {
                return MetaResponse::TableNotFound {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Failed to read table: {}", e),
                }
            }
        };

        // Check if this is the only owner
        if existing.owner_node_ids.len() == 1 && existing.is_owner(node_id) {
            return MetaResponse::Error {
                message: format!(
                    "Cannot remove last owner {} from table {}.{}.{}",
                    node_id, catalog, schema, table
                ),
            };
        }

        // Check if node is an owner
        if !existing.is_owner(node_id) {
            return MetaResponse::Error {
                message: format!(
                    "Node {} is not an owner of table {}.{}.{}",
                    node_id, catalog, schema, table
                ),
            };
        }

        // Remove owner
        let mut updated = existing;
        updated.remove_owner(node_id);

        // Store updated table
        if let Err(e) = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&updated).unwrap())
        {
            return MetaResponse::Error {
                message: format!("Failed to remove table owner: {}", e),
            };
        }

        info!(
            "Table owner removed: {}.{}.{} now owned by {:?}",
            catalog, schema, table, updated.owner_node_ids
        );

        // Publish event
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "removed_owner": node_id,
            "all_owners": updated.owner_node_ids,
        });
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::TableOwnerRemoved,
            format!("{}.{}.{}", catalog, schema, table),
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id.to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::TableOwnerRemoved {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            removed_owner: node_id.to_string(),
            all_owners: updated.owner_node_ids,
            version: updated.version,
        }
    }

    /// Update table statistics after flush
    ///
    /// Called periodically (not per-flush) to sync aggregated stats.
    /// Data nodes batch stats locally and sync every few minutes.
    pub(crate) fn update_table_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        delta: TableStatsDelta,
    ) -> MetaResponse {
        let key = TableMeta::key_for(catalog, schema, table);

        // Load existing table
        let existing = match self.db.get(key.as_bytes()) {
            Ok(Some(data)) => match bincode::deserialize::<TableMeta>(&data) {
                Ok(meta) => meta,
                Err(e) => {
                    return MetaResponse::Error {
                        message: format!("Failed to deserialize table: {}", e),
                    }
                }
            },
            Ok(None) => {
                return MetaResponse::TableNotFound {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Failed to read table: {}", e),
                }
            }
        };

        // Apply delta
        let mut updated = existing;
        updated.apply_stats_delta(&delta);

        // Store updated table
        if let Err(e) = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&updated).unwrap())
        {
            return MetaResponse::Error {
                message: format!("Failed to update table stats: {}", e),
            };
        }

        debug!(
            "Table stats updated: {}.{}.{} v{} (+{} bytes, +{} rows)",
            catalog, schema, table, updated.version, delta.bytes_delta, delta.rows_delta
        );

        MetaResponse::TableStatsUpdated {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            version: updated.version,
        }
    }

    /// Reassign a table to a different primary owner node
    ///
    /// Used for:
    /// - Load balancing (move hot tables)
    /// - Node failure recovery (promote secondary to primary)
    /// - Manual rebalancing
    ///
    /// The new owner becomes the primary (first in owner list).
    /// Other owners are preserved for scatter-gather queries.
    pub(crate) fn reassign_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        new_owner_node_id: &str,
    ) -> MetaResponse {
        let key = TableMeta::key_for(catalog, schema, table);

        // Load existing table
        let existing = match self.db.get(key.as_bytes()) {
            Ok(Some(data)) => match bincode::deserialize::<TableMeta>(&data) {
                Ok(meta) => meta,
                Err(e) => {
                    return MetaResponse::Error {
                        message: format!("Failed to deserialize table: {}", e),
                    }
                }
            },
            Ok(None) => {
                return MetaResponse::TableNotFound {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Failed to read table: {}", e),
                }
            }
        };

        let old_primary = existing.primary_owner().unwrap_or("").to_string();

        // Set new primary owner (moves to front, adds if not present)
        let mut updated = existing;
        updated.set_primary_owner(new_owner_node_id.to_string());

        // Store updated table
        if let Err(e) = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&updated).unwrap())
        {
            return MetaResponse::Error {
                message: format!("Failed to reassign table: {}", e),
            };
        }

        info!(
            "Table reassigned: {}.{}.{} primary {} -> {}, all owners: {:?}",
            catalog, schema, table, old_primary, new_owner_node_id, updated.owner_node_ids
        );

        // Publish event for cache invalidation
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "old_primary": old_primary,
            "new_primary": new_owner_node_id,
            "all_owners": updated.owner_node_ids,
        });
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::TableReassigned,
            format!("{}.{}.{}", catalog, schema, table),
            serde_json::to_vec(&payload).unwrap_or_default(),
            new_owner_node_id.to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::TableReassigned {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            old_owner: old_primary,
            new_owner: new_owner_node_id.to_string(),
            all_owners: updated.owner_node_ids,
            version: updated.version,
        }
    }

    /// Remove a table from the registry
    ///
    /// Called when a table is dropped or all data is deleted.
    pub(crate) fn remove_table(&self, catalog: &str, schema: &str, table: &str) -> MetaResponse {
        let key = TableMeta::key_for(catalog, schema, table);

        // Check if table exists
        match self.db.get(key.as_bytes()) {
            Ok(Some(_)) => {
                // Remove the table
                if let Err(e) = self.db.remove(key.as_bytes()) {
                    return MetaResponse::Error {
                        message: format!("Failed to remove table: {}", e),
                    };
                }

                info!("Table removed: {}.{}.{}", catalog, schema, table);

                // Publish event
                let payload = serde_json::json!({
                    "catalog": catalog,
                    "schema": schema,
                    "table": table,
                });
                let seq = self.next_sequence();
                let event = Event::new(
                    seq,
                    EventType::SchemaChanged,
                    format!("{}.{}.{}", catalog, schema, table),
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    "system".to_string(),
                );
                let _ = self.db.insert(
                    event.storage_key().as_bytes(),
                    bincode::serialize(&event).unwrap(),
                );

                MetaResponse::TableRemoved {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                }
            }
            Ok(None) => MetaResponse::TableNotFound {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            },
            Err(e) => MetaResponse::Error {
                message: format!("Failed to check table: {}", e),
            },
        }
    }

    // ========================================================================
    // Table Operations (Read Path / Queries)
    // ========================================================================

    /// Get the primary owner node for a table (for new write routing)
    ///
    /// This returns the first non-Drained owner for routing new writes.
    /// Filters out Drained nodes to avoid routing to departed nodes.
    /// If a node doesn't exist in Meta, it's returned (could be legacy/external).
    /// For queries, use get_table_all_owners() to scatter-gather.
    pub fn get_table_owner(&self, catalog: &str, schema: &str, table: &str) -> Option<String> {
        let key = TableMeta::key_for(catalog, schema, table);

        let meta = self
            .db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize::<TableMeta>(&data).ok())?;

        // Return first non-Drained owner
        // - If node exists and is Active → return (good)
        // - If node exists and is Drained → skip (departed)
        // - If node doesn't exist → return (legacy/external, let Router handle)
        for owner_id in &meta.owner_node_ids {
            match self.get_node(owner_id) {
                Some(node) if node.state == NodeState::Drained => continue, // Skip drained
                _ => return Some(owner_id.clone()), // Active or unknown → return
            }
        }
        None
    }

    /// Get all owner nodes for a table (for scatter-gather queries)
    ///
    /// Returns all non-Drained nodes that have data for this table.
    /// Filters out Drained nodes to avoid routing to departed nodes.
    /// If a node doesn't exist in Meta, it's included (could be legacy/external).
    /// Queries should fan-out to all owners to get complete results.
    pub fn get_table_all_owners(&self, catalog: &str, schema: &str, table: &str) -> Vec<String> {
        let key = TableMeta::key_for(catalog, schema, table);

        let meta = match self
            .db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize::<TableMeta>(&data).ok())
        {
            Some(m) => m,
            None => return Vec::new(),
        };

        // Filter out Drained owners, keep Active and unknown (legacy/external)
        meta.owner_node_ids
            .into_iter()
            .filter(|owner_id| {
                match self.get_node(owner_id) {
                    Some(node) => node.state != NodeState::Drained,
                    None => true, // Unknown node → include (legacy/external)
                }
            })
            .collect()
    }

    /// Get full table metadata
    pub fn get_table_meta(&self, catalog: &str, schema: &str, table: &str) -> Option<TableMeta> {
        let key = TableMeta::key_for(catalog, schema, table);

        self.db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize::<TableMeta>(&data).ok())
    }

    /// List all tables where a node is an owner (primary or secondary)
    ///
    /// Used for:
    /// - Rebalancing decisions
    /// - Node failure recovery (find all tables affected)
    /// - Admin dashboards
    pub fn list_tables_for_node(&self, node_id: &str) -> Vec<TableMeta> {
        let prefix = keys::PREFIX_TABLE;
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    if meta.is_owner(node_id) {
                        tables.push(meta);
                    }
                }
            }
        }

        tables
    }

    /// List all tables where a node is the PRIMARY owner
    ///
    /// Used for counting tables assigned for writes to a node.
    pub fn list_tables_for_primary_node(&self, node_id: &str) -> Vec<TableMeta> {
        let prefix = keys::PREFIX_TABLE;
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    if meta.primary_owner() == Some(node_id) {
                        tables.push(meta);
                    }
                }
            }
        }

        tables
    }

    /// List all tables in a schema
    pub fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Vec<TableMeta> {
        let prefix = TableMeta::schema_prefix(catalog, schema);
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    tables.push(meta);
                }
            }
        }

        tables
    }

    /// List all tables in a catalog
    pub fn list_tables_in_catalog(&self, catalog: &str) -> Vec<TableMeta> {
        let prefix = TableMeta::catalog_prefix(catalog);
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    tables.push(meta);
                }
            }
        }

        tables
    }

    /// List all tables (for admin/monitoring)
    pub fn list_all_tables(&self) -> Vec<TableMeta> {
        let prefix = TableMeta::all_prefix();
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    tables.push(meta);
                }
            }
        }

        tables
    }

    /// Count tables per node (for load balancing)
    ///
    /// Returns counts for primary ownership only (for write distribution).
    /// For full ownership counts including secondary owners, use count_all_table_owners().
    pub fn count_tables_per_node(&self) -> std::collections::HashMap<String, usize> {
        let prefix = keys::PREFIX_TABLE;
        let mut counts = std::collections::HashMap::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    if let Some(primary) = meta.primary_owner() {
                        *counts.entry(primary.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }

        counts
    }

    /// Count all table ownerships per node (including secondary owners)
    ///
    /// A node may appear multiple times if it's owner of multiple tables.
    /// Secondary owners (from rerouted writes) are also counted.
    pub fn count_all_table_owners(&self) -> std::collections::HashMap<String, usize> {
        let prefix = keys::PREFIX_TABLE;
        let mut counts = std::collections::HashMap::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    for owner in &meta.owner_node_ids {
                        *counts.entry(owner.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        counts
    }

    /// Find tables with multiple owners (for consolidation/monitoring)
    pub fn list_multi_owner_tables(&self) -> Vec<TableMeta> {
        let prefix = keys::PREFIX_TABLE;
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    if meta.is_multi_owner() {
                        tables.push(meta);
                    }
                }
            }
        }

        tables
    }

    /// Get total statistics across all tables
    pub fn get_total_table_stats(&self) -> (usize, u64, u64) {
        let prefix = keys::PREFIX_TABLE;
        let mut table_count = 0;
        let mut total_bytes = 0u64;
        let mut total_rows = 0u64;

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    table_count += 1;
                    total_bytes += meta.total_bytes;
                    total_rows += meta.total_rows;
                }
            }
        }

        (table_count, total_bytes, total_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_state_machine() -> MetaStateMachine {
        let dir = tempdir().unwrap();
        let db = sled::open(dir.path()).unwrap();
        MetaStateMachine::new(db, crate::meta::config::StandbyConfig::default())
    }

    #[test]
    fn test_register_table() {
        let sm = create_test_state_machine();

        let response = sm.register_table("mydb", "default", "cpu", "data-1");
        match response {
            MetaResponse::TableRegistered {
                catalog,
                schema,
                table,
                owner_node_id,
                version,
            } => {
                assert_eq!(catalog, "mydb");
                assert_eq!(schema, "default");
                assert_eq!(table, "cpu");
                assert_eq!(owner_node_id, "data-1");
                assert_eq!(version, 1);
            }
            _ => panic!("Expected TableRegistered"),
        }

        // Verify owner
        assert_eq!(
            sm.get_table_owner("mydb", "default", "cpu"),
            Some("data-1".to_string())
        );
    }

    #[test]
    fn test_register_table_idempotent() {
        let sm = create_test_state_machine();

        // First registration
        sm.register_table("mydb", "default", "cpu", "data-1");

        // Second registration (same owner)
        let response = sm.register_table("mydb", "default", "cpu", "data-2");
        match response {
            MetaResponse::TableAlreadyExists { current_owner, .. } => {
                assert_eq!(current_owner, "data-1");
            }
            _ => panic!("Expected TableAlreadyExists"),
        }
    }

    #[test]
    fn test_update_table_stats() {
        let sm = create_test_state_machine();

        // Register table first
        sm.register_table("mydb", "default", "cpu", "data-1");

        // Update stats
        let delta = TableStatsDelta {
            partition_count_delta: 1,
            bytes_delta: 1024 * 1024,
            rows_delta: 10000,
            files_delta: 1,
            newest_partition: Some("date=2024-01-15".to_string()),
            min_time: 1705276800000000000,
            max_time: 1705363199000000000,
        };

        let response = sm.update_table_stats("mydb", "default", "cpu", delta);
        match response {
            MetaResponse::TableStatsUpdated { version, .. } => {
                assert_eq!(version, 2); // Version incremented
            }
            _ => panic!("Expected TableStatsUpdated"),
        }

        // Verify stats
        let meta = sm.get_table_meta("mydb", "default", "cpu").unwrap();
        assert_eq!(meta.partition_count, 1);
        assert_eq!(meta.total_bytes, 1024 * 1024);
        assert_eq!(meta.total_rows, 10000);
    }

    #[test]
    fn test_reassign_table() {
        let sm = create_test_state_machine();

        // Register table
        sm.register_table("mydb", "default", "cpu", "data-1");

        // Reassign
        let response = sm.reassign_table("mydb", "default", "cpu", "data-2");
        match response {
            MetaResponse::TableReassigned {
                old_owner,
                new_owner,
                ..
            } => {
                assert_eq!(old_owner, "data-1");
                assert_eq!(new_owner, "data-2");
            }
            _ => panic!("Expected TableReassigned"),
        }

        // Verify new owner
        assert_eq!(
            sm.get_table_owner("mydb", "default", "cpu"),
            Some("data-2".to_string())
        );
    }

    #[test]
    fn test_remove_table() {
        let sm = create_test_state_machine();

        // Register table
        sm.register_table("mydb", "default", "cpu", "data-1");
        assert!(sm.get_table_owner("mydb", "default", "cpu").is_some());

        // Remove table
        let response = sm.remove_table("mydb", "default", "cpu");
        assert!(matches!(response, MetaResponse::TableRemoved { .. }));

        // Verify removed
        assert!(sm.get_table_owner("mydb", "default", "cpu").is_none());
    }

    #[test]
    fn test_list_tables_for_node() {
        let sm = create_test_state_machine();

        // Register tables on different nodes
        sm.register_table("mydb", "default", "cpu", "data-1");
        sm.register_table("mydb", "default", "memory", "data-1");
        sm.register_table("mydb", "default", "disk", "data-2");

        // List for data-1
        let tables = sm.list_tables_for_node("data-1");
        assert_eq!(tables.len(), 2);

        // List for data-2
        let tables = sm.list_tables_for_node("data-2");
        assert_eq!(tables.len(), 1);
    }

    #[test]
    fn test_count_tables_per_node() {
        let sm = create_test_state_machine();

        // Register tables
        sm.register_table("mydb", "default", "cpu", "data-1");
        sm.register_table("mydb", "default", "memory", "data-1");
        sm.register_table("mydb", "default", "disk", "data-2");
        sm.register_table("mydb", "svc1", "metrics", "data-2");

        // Count
        let counts = sm.count_tables_per_node();
        assert_eq!(counts.get("data-1"), Some(&2));
        assert_eq!(counts.get("data-2"), Some(&2));
    }
}
