//! Table Operations for MetaStateMachine
//!
//! Implements table-level routing - the recommended approach for cluster mode.
//! Each table is owned by a single Data node which handles all partitions.
//!
//! Key benefits over file-level routing:
//! - 600x fewer Meta entries (100K vs 60M)
//! - Zero daily growth (new partitions don't create entries)
//! - Single routing lookup per query
//! - Excellent cache locality for time-series workloads

use crate::meta::types::{keys, Event, EventType, MetaResponse, TableMeta, TableStatsDelta};
use tracing::{debug, info, warn};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Table Operations (Write Path)
    // ========================================================================

    /// Register a new table with its owner node
    ///
    /// Called when a Data node first receives data for a new table.
    /// The node that first writes to a table becomes its owner.
    ///
    /// This is idempotent - if the table already exists with the same owner,
    /// it returns TableAlreadyExists (not an error).
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
                debug!(
                    "Table already exists: {}.{}.{} owned by {}",
                    catalog, schema, table, meta.owner_node_id
                );
                return MetaResponse::TableAlreadyExists {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                    current_owner: meta.owner_node_id,
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
            "owner_node_id": owner_node_id,
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

    /// Reassign a table to a different owner node
    ///
    /// Used for:
    /// - Load balancing (move hot tables)
    /// - Node failure recovery
    /// - Manual rebalancing
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

        let old_owner = existing.owner_node_id.clone();

        // Update owner
        let mut updated = existing;
        updated.owner_node_id = new_owner_node_id.to_string();
        updated.last_modified = crate::meta::types::current_timestamp_ms();
        updated.version += 1;

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
            "Table reassigned: {}.{}.{} {} -> {}",
            catalog, schema, table, old_owner, new_owner_node_id
        );

        // Publish event for cache invalidation
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "old_owner": old_owner,
            "new_owner": new_owner_node_id,
        });
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::SchemaChanged,
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
            old_owner,
            new_owner: new_owner_node_id.to_string(),
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

    /// Get the owner node for a table (primary routing operation)
    ///
    /// This is the main query for routing - O(1) hash lookup.
    /// Returns None if table doesn't exist.
    pub fn get_table_owner(&self, catalog: &str, schema: &str, table: &str) -> Option<String> {
        let key = TableMeta::key_for(catalog, schema, table);

        self.db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize::<TableMeta>(&data).ok())
            .map(|meta| meta.owner_node_id)
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

    /// List all tables owned by a specific node
    ///
    /// Used for:
    /// - Rebalancing decisions
    /// - Node failure recovery
    /// - Admin dashboards
    pub fn list_tables_for_node(&self, node_id: &str) -> Vec<TableMeta> {
        let prefix = keys::PREFIX_TABLE;
        let mut tables = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    if meta.owner_node_id == node_id {
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
    pub fn count_tables_per_node(&self) -> std::collections::HashMap<String, usize> {
        let prefix = keys::PREFIX_TABLE;
        let mut counts = std::collections::HashMap::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(meta) = bincode::deserialize::<TableMeta>(&v) {
                    *counts.entry(meta.owner_node_id).or_insert(0) += 1;
                }
            }
        }

        counts
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
