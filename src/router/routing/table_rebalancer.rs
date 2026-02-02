//! Table Rebalancer - Handles orphaned table reassignment
//!
//! Periodically scans for tables with inactive owners and:
//! 1. Removes dead owners from table ownership lists
//! 2. Reassigns orphaned tables (no owners left) to healthy nodes
//!
//! This is critical for maintaining data availability after node failures
//! when write rerouting has distributed table ownership across multiple nodes.

use super::shard_cache::{NodeState, ShardCache, TableKey};
use crate::meta::client::MetaClient;
use crate::meta::types::{NodeRole, NodeState as MetaNodeState};
use crate::router::config::RebalancerConfig;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Statistics from a rebalancing run
#[derive(Debug, Default)]
pub struct RebalanceStats {
    /// Number of tables scanned
    pub tables_scanned: usize,
    /// Number of tables with at least one dead owner
    pub tables_with_dead_owners: usize,
    /// Number of dead owners removed
    pub dead_owners_removed: usize,
    /// Number of orphaned tables (no owners left)
    pub orphaned_tables: usize,
    /// Number of tables successfully reassigned
    pub tables_reassigned: usize,
    /// Errors encountered
    pub errors: Vec<String>,
}

/// Table Rebalancer for maintaining table ownership health
pub struct TableRebalancer {
    /// Meta client for querying and updating table ownership
    meta_client: Arc<MetaClient>,

    /// Shard cache for local routing state
    shard_cache: Arc<ShardCache>,
}

impl TableRebalancer {
    pub fn new(meta_client: Arc<MetaClient>, shard_cache: Arc<ShardCache>) -> Self {
        Self {
            meta_client,
            shard_cache,
        }
    }

    /// Start the background rebalancing task
    pub fn start_background_rebalancer(self: Arc<Self>, config: RebalancerConfig) {
        if !config.enabled {
            info!("Table rebalancer is disabled");
            return;
        }

        if config.interval_secs == 0 {
            warn!("Rebalancer interval is 0, background rebalancer disabled");
            return;
        }

        info!(
            "Starting background table rebalancer (interval: {}s, auto_remove_dead: {}, auto_reassign: {})",
            config.interval_secs, config.auto_remove_dead_owners, config.auto_reassign_orphans
        );

        tokio::spawn(async move {
            // Initial delay to let the cluster stabilize
            sleep(Duration::from_secs(30)).await;

            loop {
                sleep(Duration::from_secs(config.interval_secs)).await;

                debug!("Running table rebalance check...");
                match self.run_rebalance_check(&config).await {
                    Ok(stats) => {
                        if stats.dead_owners_removed > 0 || stats.tables_reassigned > 0 {
                            info!(
                                "Rebalance completed: {} tables scanned, {} dead owners removed, {} orphaned tables reassigned",
                                stats.tables_scanned,
                                stats.dead_owners_removed,
                                stats.tables_reassigned
                            );
                        } else {
                            debug!(
                                "Rebalance check completed: {} tables scanned, all healthy",
                                stats.tables_scanned
                            );
                        }

                        if !stats.errors.is_empty() {
                            warn!("Rebalance had {} errors: {:?}", stats.errors.len(), stats.errors);
                        }
                    }
                    Err(e) => {
                        error!("Rebalance check failed: {}", e);
                    }
                }
            }
        });
    }

    /// Run a single rebalance check
    pub async fn run_rebalance_check(
        &self,
        config: &RebalancerConfig,
    ) -> Result<RebalanceStats, String> {
        let mut stats = RebalanceStats::default();

        // Get all active data nodes
        let active_nodes = self.get_active_data_node_ids().await?;
        if active_nodes.is_empty() {
            return Err("No active data nodes available".to_string());
        }

        debug!("Active data nodes: {:?}", active_nodes);

        // Get all tables from Meta
        let tables = self
            .meta_client
            .list_tables(None, None)
            .await
            .map_err(|e| format!("Failed to list tables: {}", e))?;

        stats.tables_scanned = tables.len();

        for table in tables {
            let table_key = TableKey::new(&table.catalog, &table.schema, &table.table);
            let table_path = format!("{}.{}.{}", table.catalog, table.schema, table.table);

            // Find dead owners for this table
            let dead_owners: Vec<String> = table
                .owner_node_ids
                .iter()
                .filter(|owner_id| !active_nodes.contains(*owner_id))
                .cloned()
                .collect();

            if dead_owners.is_empty() {
                continue;
            }

            stats.tables_with_dead_owners += 1;
            debug!(
                "Table {} has dead owners: {:?} (live: {:?})",
                table_path, dead_owners, table.owner_node_ids
            );

            // Remove dead owners
            if config.auto_remove_dead_owners {
                for dead_owner in &dead_owners {
                    match self
                        .meta_client
                        .remove_table_owner(&table.catalog, &table.schema, &table.table, dead_owner)
                        .await
                    {
                        Ok((remaining_owners, version)) => {
                            info!(
                                "Removed dead owner {} from table {} (remaining: {:?}, v{})",
                                dead_owner, table_path, remaining_owners, version
                            );
                            stats.dead_owners_removed += 1;

                            // Update local cache
                            if remaining_owners.is_empty() {
                                self.shard_cache.remove_table_owner(&table_key);
                            } else {
                                self.shard_cache
                                    .set_table_owners(&table_key, remaining_owners, version);
                            }
                        }
                        Err(e) => {
                            let err =
                                format!("Failed to remove dead owner {} from {}: {}", dead_owner, table_path, e);
                            warn!("{}", err);
                            stats.errors.push(err);
                        }
                    }
                }
            }

            // Check if table is now orphaned (all owners were dead)
            let live_owners: Vec<&String> = table
                .owner_node_ids
                .iter()
                .filter(|owner_id| active_nodes.contains(*owner_id))
                .collect();

            if live_owners.is_empty() {
                stats.orphaned_tables += 1;

                // Reassign to a healthy node
                if config.auto_reassign_orphans {
                    // Select a new primary owner using consistent hashing
                    if let Some(new_owner) = self.shard_cache.select_node_for_table(&table_key) {
                        match self
                            .meta_client
                            .reassign_table(
                                &table.catalog,
                                &table.schema,
                                &table.table,
                                &new_owner.node_id,
                            )
                            .await
                        {
                            Ok((_old_primary, _new_primary, all_owners, version)) => {
                                info!(
                                    "Reassigned orphaned table {} to {} (v{})",
                                    table_path, new_owner.node_id, version
                                );
                                stats.tables_reassigned += 1;

                                // Update local cache
                                self.shard_cache
                                    .set_table_owners(&table_key, all_owners, version);
                            }
                            Err(e) => {
                                let err = format!(
                                    "Failed to reassign orphaned table {} to {}: {}",
                                    table_path, new_owner.node_id, e
                                );
                                warn!("{}", err);
                                stats.errors.push(err);
                            }
                        }
                    } else {
                        let err = format!(
                            "No available node to reassign orphaned table {}",
                            table_path
                        );
                        warn!("{}", err);
                        stats.errors.push(err);
                    }
                }
            }
        }

        Ok(stats)
    }

    /// Get all active data node IDs from Meta
    async fn get_active_data_node_ids(&self) -> Result<HashSet<String>, String> {
        let nodes = self
            .meta_client
            .list_nodes(Some(NodeRole::Data), Some(MetaNodeState::Active))
            .await
            .map_err(|e| format!("Failed to list active data nodes: {}", e))?;

        Ok(nodes.into_iter().map(|n| n.node_id).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_stats_default() {
        let stats = RebalanceStats::default();
        assert_eq!(stats.tables_scanned, 0);
        assert_eq!(stats.dead_owners_removed, 0);
        assert_eq!(stats.orphaned_tables, 0);
        assert_eq!(stats.tables_reassigned, 0);
        assert!(stats.errors.is_empty());
    }
}
