//! Table CRUD and ownership management operations

use super::MetaNode;
use crate::meta::types::{MetaCommand, MetaResponse, TableMeta, TableStatsDelta};

impl MetaNode {
    /// Register a new table with its owner node
    pub async fn register_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        owner_node_id: &str,
    ) -> Result<MetaResponse, String> {
        let cmd = MetaCommand::RegisterTable {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            owner_node_id: owner_node_id.to_string(),
        };

        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(response) => Ok(response.data),
            Err(e) => Err(format!("Failed to register table: {}", e)),
        }
    }

    /// Update table statistics after flush
    pub async fn update_table_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        delta: TableStatsDelta,
    ) -> Result<MetaResponse, String> {
        let cmd = MetaCommand::UpdateTableStats {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            delta,
        };

        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(response) => Ok(response.data),
            Err(e) => Err(format!("Failed to update table stats: {}", e)),
        }
    }

    /// Reassign a table to a different owner node
    pub async fn reassign_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        new_owner_node_id: &str,
    ) -> Result<MetaResponse, String> {
        let cmd = MetaCommand::ReassignTable {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            new_owner_node_id: new_owner_node_id.to_string(),
        };

        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(response) => Ok(response.data),
            Err(e) => Err(format!("Failed to reassign table: {}", e)),
        }
    }

    /// Remove a table from the registry
    pub async fn remove_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<MetaResponse, String> {
        let cmd = MetaCommand::RemoveTable {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        };

        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(response) => Ok(response.data),
            Err(e) => Err(format!("Failed to remove table: {}", e)),
        }
    }

    /// Add an additional owner to a table (for rerouted writes)
    ///
    /// Returns (already_owner, all_owners, version) on success
    pub async fn add_table_owner(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        owner_node_id: &str,
    ) -> Result<(bool, Vec<String>, u64), String> {
        let cmd = MetaCommand::AddTableOwner {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            owner_node_id: owner_node_id.to_string(),
        };

        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::TableOwnerAdded {
                    all_owners,
                    version,
                    ..
                } => Ok((false, all_owners, version)),
                MetaResponse::TableOwnerExists { all_owners, .. } => Ok((true, all_owners, 0)),
                MetaResponse::TableRegistered {
                    owner_node_id,
                    version,
                    ..
                } => Ok((false, vec![owner_node_id], version)),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response".to_string()),
            },
            Err(e) => Err(format!("Failed to add table owner: {}", e)),
        }
    }

    /// Remove an owner from a table
    ///
    /// Returns (remaining_owners, version) on success
    pub async fn remove_table_owner(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        owner_node_id: &str,
    ) -> Result<(Vec<String>, u64), String> {
        let cmd = MetaCommand::RemoveTableOwner {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            owner_node_id: owner_node_id.to_string(),
        };

        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::TableOwnerRemoved {
                    all_owners,
                    version,
                    ..
                } => Ok((all_owners, version)),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response".to_string()),
            },
            Err(e) => Err(format!("Failed to remove table owner: {}", e)),
        }
    }

    /// Get the primary owner node for a table (for new write routing)
    pub async fn get_table_owner(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<String> {
        self.state_machine.get_table_owner(catalog, schema, table)
    }

    /// Get all owner nodes for a table (for scatter-gather queries)
    pub async fn get_table_all_owners(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Vec<String> {
        self.state_machine
            .get_table_all_owners(catalog, schema, table)
    }

    /// Get full table metadata
    pub async fn get_table_meta(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<TableMeta> {
        self.state_machine.get_table_meta(catalog, schema, table)
    }

    /// List all tables owned by a specific node
    pub async fn list_tables_for_node(&self, node_id: &str) -> Vec<TableMeta> {
        self.state_machine.list_tables_for_node(node_id)
    }

    /// List all tables in a schema
    pub async fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Vec<TableMeta> {
        self.state_machine.list_tables_in_schema(catalog, schema)
    }

    /// List all tables in a catalog
    pub async fn list_tables_in_catalog(&self, catalog: &str) -> Vec<TableMeta> {
        self.state_machine.list_tables_in_catalog(catalog)
    }

    /// List all tables
    pub async fn list_all_tables(&self) -> Vec<TableMeta> {
        self.state_machine.list_all_tables()
    }

    /// Count tables per node (for load balancing)
    pub async fn count_tables_per_node(&self) -> std::collections::HashMap<String, usize> {
        self.state_machine.count_tables_per_node()
    }

    /// Reassign a table for scale-in retirement
    ///
    /// Takes a table_key in format "catalog/schema/table" and reassigns to new owner.
    /// Used during scale-in to migrate tables from retiring nodes.
    pub async fn reassign_table_for_retire(
        &self,
        table_key: &str,
        _from_node: &str,
        to_node: &str,
    ) -> Result<(), String> {
        // Parse table_key: "catalog/schema/table"
        let parts: Vec<&str> = table_key.split('/').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid table_key format: {}", table_key));
        }

        let catalog = parts[0];
        let schema = parts[1];
        let table = parts[2];

        match self.reassign_table(catalog, schema, table, to_node).await {
            Ok(MetaResponse::TableReassigned { .. }) => Ok(()),
            Ok(MetaResponse::Error { message }) => Err(message),
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(e),
        }
    }

    /// Remove a standby from all table secondary assignments
    ///
    /// Used during scale-in to clean up standby assignments before retirement.
    /// Returns the number of tables affected.
    pub async fn remove_standby_from_tables(&self, standby_node_id: &str) -> Result<usize, String> {
        // For table-level routing, standbys don't have explicit table assignments
        // They back up all primaries at the node level.
        // This method is a no-op for table-level routing but kept for API compatibility.
        tracing::debug!(
            "remove_standby_from_tables: {} (no-op for table-level routing)",
            standby_node_id
        );
        Ok(0)
    }
}
