//! Background tasks, rebalancing, and shutdown

use super::MetaNode;
use crate::meta::types::{keys, MetaCommand, NodeRole, NodeState};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

impl MetaNode {
    /// Start background tasks
    pub fn start_background_tasks(self: &Arc<Self>) {
        // Lease expiration task
        {
            let node = Arc::clone(self);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;

                    // Only run on leader
                    if !node.is_leader().await {
                        continue;
                    }

                    // Expire leases
                    let cmd = MetaCommand::ExpireLeases;
                    if let Err(e) = node.apply_command(cmd).await {
                        warn!("Failed to expire leases: {}", e);
                    }
                }
            });
        }

        // Table rebalancer task (only runs on leader)
        if self.config.rebalancer.enabled {
            let node = Arc::clone(self);
            let interval_secs = self.config.rebalancer.interval_secs;
            let auto_remove = self.config.rebalancer.auto_remove_dead_owners;
            let auto_reassign = self.config.rebalancer.auto_reassign_orphans;

            info!(
                "Starting table rebalancer (interval: {}s, remove_dead: {}, reassign: {})",
                interval_secs, auto_remove, auto_reassign
            );

            tokio::spawn(async move {
                // Initial delay to let the cluster stabilize
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;

                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                loop {
                    interval.tick().await;

                    // Only run on leader
                    if !node.is_leader().await {
                        continue;
                    }

                    // Run rebalance check (removes dead owners from tables)
                    node.run_table_rebalance(auto_remove, auto_reassign).await;

                    // Clean up Drained nodes that have no remaining table ownership
                    node.cleanup_drained_nodes().await;
                }
            });
        }
    }

    /// Run table rebalancing check
    ///
    /// Scans for tables with inactive owners and:
    /// 1. Removes dead owners from table ownership lists
    /// 2. Reassigns orphaned tables (no owners left) to healthy nodes
    pub(crate) async fn run_table_rebalance(&self, auto_remove: bool, auto_reassign: bool) {
        // Get all active data nodes
        let active_nodes: std::collections::HashSet<String> = self
            .state_machine
            .list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
            .into_iter()
            .map(|n| n.node_id)
            .collect();

        if active_nodes.is_empty() {
            debug!("No active data nodes, skipping rebalance");
            return;
        }

        // Get all tables
        let tables = self.state_machine.list_all_tables();
        let mut dead_removed = 0usize;
        let mut tables_reassigned = 0usize;

        for table in tables {
            let table_path = format!("{}.{}.{}", table.catalog, table.schema, table.table);

            // Find dead owners
            let dead_owners: Vec<String> = table
                .owner_node_ids
                .iter()
                .filter(|owner_id| !active_nodes.contains(*owner_id))
                .cloned()
                .collect();

            if dead_owners.is_empty() {
                continue;
            }

            debug!("Table {} has dead owners: {:?}", table_path, dead_owners);

            // Remove dead owners
            if auto_remove {
                for dead_owner in &dead_owners {
                    let cmd = MetaCommand::RemoveTableOwner {
                        catalog: table.catalog.clone(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        owner_node_id: dead_owner.clone(),
                    };
                    match self.apply_command(cmd).await {
                        Ok(_) => {
                            info!(
                                "Removed dead owner {} from table {}",
                                dead_owner, table_path
                            );
                            dead_removed += 1;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to remove dead owner {} from {}: {}",
                                dead_owner, table_path, e
                            );
                        }
                    }
                }
            }

            // Check if table is now orphaned
            let live_owners: Vec<&String> = table
                .owner_node_ids
                .iter()
                .filter(|owner_id| active_nodes.contains(*owner_id))
                .collect();

            if live_owners.is_empty() && auto_reassign {
                // Select new owner using hash-based selection
                let active_list: Vec<&String> = active_nodes.iter().collect();
                if let Some(new_owner) = active_list.first() {
                    let cmd = MetaCommand::ReassignTable {
                        catalog: table.catalog.clone(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        new_owner_node_id: (*new_owner).clone(),
                    };
                    match self.apply_command(cmd).await {
                        Ok(_) => {
                            info!("Reassigned orphaned table {} to {}", table_path, new_owner);
                            tables_reassigned += 1;
                        }
                        Err(e) => {
                            warn!("Failed to reassign orphaned table {}: {}", table_path, e);
                        }
                    }
                }
            }
        }

        if dead_removed > 0 || tables_reassigned > 0 {
            info!(
                "Rebalance completed: {} dead owners removed, {} tables reassigned",
                dead_removed, tables_reassigned
            );
        }
    }

    /// Clean up Drained nodes that have no remaining table ownership
    ///
    /// After deregister_node marks a node as Drained, the rebalancer removes it
    /// from table ownership. This function removes the node entry once all its
    /// tables have been cleaned up.
    ///
    /// Optimized: Single table scan O(M) + O(N) node checks instead of O(N × M)
    pub(crate) async fn cleanup_drained_nodes(&self) {
        // Get all Drained data nodes
        let drained_nodes = self
            .state_machine
            .list_nodes(Some(NodeRole::Data), Some(NodeState::Drained));

        if drained_nodes.is_empty() {
            return;
        }

        debug!(
            "Found {} Drained nodes to check for cleanup",
            drained_nodes.len()
        );

        // Single scan: build set of all nodes that still own tables - O(M)
        let all_tables = self.state_machine.list_all_tables();
        let nodes_with_tables: std::collections::HashSet<String> = all_tables
            .iter()
            .flat_map(|t| t.owner_node_ids.iter().cloned())
            .collect();

        // Check each drained node with O(1) lookup
        let mut removed_count = 0;
        for node in &drained_nodes {
            if !nodes_with_tables.contains(&node.node_id) {
                // Safe to remove - no more table ownership
                let key = format!("{}{}", keys::PREFIX_NODE, node.node_id);
                if let Err(e) = self.state_machine.db().remove(key.as_bytes()) {
                    warn!("Failed to remove Drained node {}: {}", node.node_id, e);
                } else {
                    removed_count += 1;
                    debug!(
                        "Removed Drained node {} (no remaining table ownership)",
                        node.node_id
                    );
                }
            }
        }

        // Single flush after all removals
        if removed_count > 0 {
            let _ = self.state_machine.db().flush();
            info!(
                "Cleaned up {} Drained nodes ({} still have tables)",
                removed_count,
                drained_nodes.len() - removed_count
            );
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down MetaNode {}", self.node_id);

        // Shutdown Raft (triggers graceful shutdown of replication tasks)
        if let Err(e) = self.raft.shutdown().await {
            error!("Error during Raft shutdown: {}", e);
        }

        Ok(())
    }
}
