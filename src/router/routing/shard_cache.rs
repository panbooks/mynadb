//! Shard Cache - Caches table→node mappings from Meta service
//!
//! The ShardCache maintains a mapping of table keys to Data node assignments.
//! It is updated via events from the Meta service when tables are registered or
//! nodes join/leave the cluster.
//!
//! Design: Stores only node_id in table_owners, with full DataNodeInfo in
//! data_nodes (single source of truth). This prevents stale state issues when
//! node health changes.
//!
//! ## Table-Level Routing
//!
//! All partitions of a table are routed to the same Data node. This provides:
//! - 600x fewer Meta entries (100K tables vs 60M files/partitions)
//! - Single routing lookup per query
//! - Excellent cache locality for hot data

use crate::meta::types::{DataNodeMode, DistributionConfig, DistributionScope};
use crate::utils::hash::{hash_select_clone, hash_select_index};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// Data node information
#[derive(Debug, Clone)]
pub struct DataNodeInfo {
    /// Unique node identifier
    pub node_id: String,
    /// gRPC address for internal communication
    pub grpc_addr: String,
    /// Node state
    pub state: NodeState,
    /// Data node mode (Primary or Standby)
    ///
    /// In Cold Standby architecture:
    /// - Primary nodes serve writes and queries
    /// - Standby nodes only receive replication (never serve queries)
    pub mode: DataNodeMode,
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Draining,
    Unhealthy,
    /// Suspect - recent write failure, may recover
    Suspect,
    /// Retiring - node is being scaled in, no new writes allowed
    /// Old data is still queryable until the node is fully drained
    Retiring,
    /// Drained - scale-in complete, safe to stop the node
    Drained,
}

/// Partition key for routing
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PartitionKey {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date: String, // YYYY-MM-DD
}

impl PartitionKey {
    pub fn new(catalog: &str, schema: &str, table: &str, date: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date: date.to_string(),
        }
    }

    /// Create from a partition path like "catalog/schema/table/date=YYYY-MM-DD"
    pub fn from_path(path: &str) -> Option<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() >= 4 {
            let date = parts[3].strip_prefix("date=").unwrap_or(parts[3]);
            Some(Self {
                catalog: parts[0].to_string(),
                schema: parts[1].to_string(),
                table: parts[2].to_string(),
                date: date.to_string(),
            })
        } else {
            None
        }
    }

    /// Convert to path string
    pub fn to_path(&self) -> String {
        format!(
            "{}/{}/{}/date={}",
            self.catalog, self.schema, self.table, self.date
        )
    }

    /// Convert date string (YYYY-MM-DD) to i32 (days since Unix epoch)
    pub fn date_as_i32(&self) -> i32 {
        // Parse YYYY-MM-DD format
        if let Ok(date) = chrono::NaiveDate::parse_from_str(&self.date, "%Y-%m-%d") {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            (date - epoch).num_days() as i32
        } else {
            // Fallback to today if parse fails
            let today = chrono::Utc::now().date_naive();
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            (today - epoch).num_days() as i32
        }
    }
}


/// Table key for routing (table-level routing)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TableKey {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl TableKey {
    pub fn new(catalog: &str, schema: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    /// Convert to path string
    pub fn to_path(&self) -> String {
        format!("{}/{}/{}", self.catalog, self.schema, self.table)
    }
}

/// Cached table metadata for routing
#[derive(Debug, Clone)]
pub struct CachedTableMeta {
    /// All owner node IDs (first is primary)
    pub owner_node_ids: Vec<String>,
    /// Legacy storage prefixes from retired nodes during scale-in.
    /// Used by query engine to scan orphaned data after cluster shrinks.
    pub legacy_prefixes: Vec<String>,
    pub version: u64,
    pub cached_at: i64,
}

/// Cached distribution config for fast lookup
#[derive(Debug, Clone)]
pub struct CachedDistributionConfig {
    /// Distribution percentage [0-100]
    /// 0 = single node (default), 100 = all primary nodes
    pub percent: u8,
    /// Version for cache invalidation
    pub version: u64,
    /// When the config was cached
    pub cached_at: i64,
}

impl CachedTableMeta {
    /// Get the primary owner (first in list)
    pub fn primary_owner(&self) -> Option<&str> {
        self.owner_node_ids.first().map(|s| s.as_str())
    }

    /// Get all owners
    pub fn all_owners(&self) -> &[String] {
        &self.owner_node_ids
    }

    /// Check if a node is an owner
    pub fn is_owner(&self, node_id: &str) -> bool {
        self.owner_node_ids.iter().any(|id| id == node_id)
    }

    /// Get all storage prefixes (current owners + legacy)
    ///
    /// Used by query engine to scan all paths where data might exist.
    pub fn all_storage_prefixes(&self) -> Vec<&str> {
        let mut prefixes: Vec<&str> = self.owner_node_ids.iter().map(|s| s.as_str()).collect();
        prefixes.extend(self.legacy_prefixes.iter().map(|s| s.as_str()));
        prefixes
    }

    /// Check if this table has legacy prefixes from retired nodes
    pub fn has_legacy_prefixes(&self) -> bool {
        !self.legacy_prefixes.is_empty()
    }

    /// Get the legacy prefixes (read-only)
    pub fn legacy_prefixes(&self) -> &[String] {
        &self.legacy_prefixes
    }
}

/// Shard cache for table→node mapping
///
/// Uses table-level routing where all partitions of a table are routed to the same node.
///
/// Lookup structure:
/// 1. table_owners: table_path → owner_node_id
/// 2. data_nodes: node_id → DataNodeInfo (single source of truth)
/// 3. distribution_configs: hierarchical cache for write distribution
///
/// Table-level routing provides:
/// - 600x fewer Meta entries (100K tables vs 60M files)
/// - Single routing lookup per query
/// - Excellent cache locality for hot data
pub struct ShardCache {
    /// Table → owner node_id
    /// Key: "catalog/schema/table"
    table_owners: DashMap<String, CachedTableMeta>,

    /// node_id → DataNodeInfo (single source of truth for node state)
    data_nodes: RwLock<HashMap<String, DataNodeInfo>>,

    /// Last sequence number from Meta event stream
    last_event_seq: AtomicU64,

    /// Distribution configs cache
    /// Key: "catalog" or "catalog/schema" or "catalog/schema/table"
    /// Hierarchical lookup: table > schema > catalog
    distribution_configs: DashMap<String, CachedDistributionConfig>,
}

impl ShardCache {
    pub fn new() -> Self {
        Self {
            table_owners: DashMap::new(),
            data_nodes: RwLock::new(HashMap::new()),
            last_event_seq: AtomicU64::new(0),
            distribution_configs: DashMap::new(),
        }
    }

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
        self.set_table_owners_with_legacy(key, owner_node_ids, Vec::new(), version);
    }

    /// Set all table owners and legacy prefixes
    ///
    /// Used when caching TableMeta that includes legacy prefixes from scale-in operations.
    pub fn set_table_owners_with_legacy(
        &self,
        key: &TableKey,
        owner_node_ids: Vec<String>,
        legacy_prefixes: Vec<String>,
        version: u64,
    ) {
        debug!(
            "Setting table owners: {} -> {:?} (legacy: {:?}, v{})",
            key.to_path(),
            owner_node_ids,
            legacy_prefixes,
            version
        );
        self.table_owners.insert(
            key.to_path(),
            CachedTableMeta {
                owner_node_ids,
                legacy_prefixes,
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

    /// Get all legacy prefixes for a table
    pub fn get_table_legacy_prefixes(&self, key: &TableKey) -> Option<Vec<String>> {
        self.table_owners
            .get(&key.to_path())
            .map(|entry| entry.legacy_prefixes.clone())
    }

    /// Get all storage prefixes for a table (owners + legacy)
    pub fn get_table_all_prefixes(&self, key: &TableKey) -> Option<Vec<String>> {
        self.table_owners.get(&key.to_path()).map(|entry| {
            entry
                .all_storage_prefixes()
                .iter()
                .map(|s| s.to_string())
                .collect()
        })
    }

    /// Add a legacy prefix to an existing cached table
    ///
    /// Called when receiving TableLegacyPrefixAdded event.
    /// Returns true if the prefix was added.
    pub fn add_table_legacy_prefix(&self, key: &TableKey, legacy_prefix: String, version: u64) {
        let path = key.to_path();
        if let Some(mut entry) = self.table_owners.get_mut(&path) {
            if !entry.legacy_prefixes.contains(&legacy_prefix) {
                entry.legacy_prefixes.push(legacy_prefix.clone());
                entry.version = version;
                entry.cached_at = chrono::Utc::now().timestamp_millis();
                debug!(
                    "Added legacy prefix to table: {} -> {:?} (v{})",
                    path, entry.legacy_prefixes, version
                );
            }
        } else {
            debug!(
                "Table {} not cached, cannot add legacy prefix {}",
                path, legacy_prefix
            );
        }
    }

    /// Remove a node from all table ownership entries
    /// Called when a node permanently leaves the cluster
    ///
    /// Returns the number of tables where this node was removed as an owner.
    /// Tables with remaining owners are kept; tables with no owners are removed.
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

        if affected > 0 {
            info!(
                "Cleaned up {} table entries for departed node {}",
                affected, node_id
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

        warn!("No active Primary nodes available for table {}", key.to_path());
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

    // ========================================================================
    // Distribution Config Cache
    // ========================================================================

    /// Get the distribution percent for a table (hierarchical lookup)
    ///
    /// Lookup order: table > schema > catalog
    /// Returns 0 (single-node routing) if no config is found.
    ///
    /// This is the hot-path method used by WriteRouter to determine
    /// if writes should be distributed across multiple nodes.
    pub fn get_distribution_percent(&self, catalog: &str, schema: &str, table: &str) -> u8 {
        // Try table-level first (most specific)
        let table_key = format!("{}/{}/{}", catalog, schema, table);
        if let Some(config) = self.distribution_configs.get(&table_key) {
            return config.percent;
        }

        // Try schema-level
        let schema_key = format!("{}/{}", catalog, schema);
        if let Some(config) = self.distribution_configs.get(&schema_key) {
            return config.percent;
        }

        // Try catalog-level
        if let Some(config) = self.distribution_configs.get(catalog) {
            return config.percent;
        }

        // Default: single-node routing
        0
    }

    /// Check if writes should be distributed for a table
    ///
    /// Returns true if percent > 0 (i.e., writes should go to multiple nodes)
    pub fn is_distributed(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.get_distribution_percent(catalog, schema, table) > 0
    }

    /// Set distribution config in cache
    ///
    /// The key format depends on scope:
    /// - Catalog: "catalog"
    /// - Schema: "catalog/schema"
    /// - Table: "catalog/schema/table"
    pub fn set_distribution_config(&self, config: &DistributionConfig) {
        let key = match config.scope {
            DistributionScope::Catalog => config.catalog.clone(),
            DistributionScope::Schema => format!("{}/{}", config.catalog, config.schema),
            DistributionScope::Table => {
                format!("{}/{}/{}", config.catalog, config.schema, config.table)
            }
        };

        debug!(
            "Caching distribution config: {} -> {}% (v{})",
            key, config.percent, config.version
        );

        self.distribution_configs.insert(
            key,
            CachedDistributionConfig {
                percent: config.percent,
                version: config.version,
                cached_at: chrono::Utc::now().timestamp_millis(),
            },
        );
    }

    /// Remove distribution config from cache
    pub fn remove_distribution_config(&self, catalog: &str, schema: &str, table: &str) {
        let key = if !table.is_empty() {
            format!("{}/{}/{}", catalog, schema, table)
        } else if !schema.is_empty() {
            format!("{}/{}", catalog, schema)
        } else {
            catalog.to_string()
        };

        debug!("Removing distribution config from cache: {}", key);
        self.distribution_configs.remove(&key);
    }

    /// Invalidate all distribution configs for a catalog
    ///
    /// Used when a catalog-level config changes, which affects all
    /// tables in that catalog.
    pub fn invalidate_distribution_configs_for_catalog(&self, catalog: &str) {
        let prefix = format!("{}/", catalog);
        let keys_to_remove: Vec<String> = self
            .distribution_configs
            .iter()
            .filter(|entry| entry.key() == catalog || entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.distribution_configs.remove(&key);
        }
        debug!(
            "Invalidated distribution configs for catalog: {}",
            catalog
        );
    }

    /// Clear all distribution configs from cache
    pub fn clear_distribution_configs(&self) {
        self.distribution_configs.clear();
        info!("Cleared all distribution configs from cache");
    }

    /// Get all cached distribution configs (for debugging/stats)
    pub fn list_distribution_configs(&self) -> Vec<(String, u8, u64)> {
        self.distribution_configs
            .iter()
            .map(|entry| (entry.key().clone(), entry.percent, entry.version))
            .collect()
    }

    // ========================================================================
    // Node Management
    // ========================================================================

    /// Get a DataNodeInfo by node_id
    pub fn get_data_node(&self, node_id: &str) -> Option<DataNodeInfo> {
        self.data_nodes.read().get(node_id).cloned()
    }

    /// Get all active Data nodes
    /// Get all active data nodes, sorted by node_id for consistent routing.
    pub fn get_active_data_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get any active node (for metadata queries that any node can answer)
    ///
    /// Returns the first active node, or None if no nodes are available.
    /// Used for schema/table queries where any node has the same metadata.
    pub fn get_any_active_node(&self) -> Option<DataNodeInfo> {
        self.data_nodes
            .read()
            .values()
            .find(|n| n.state == NodeState::Active)
            .cloned()
    }

    /// Get all data nodes that can serve reads (Active or Retiring)
    ///
    /// Retiring nodes still have data in memtable and should serve queries
    /// until the memtable is flushed and they transition to Drained.
    /// Returns nodes sorted by node_id for consistent routing.
    pub fn get_queryable_data_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active || n.state == NodeState::Retiring)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get all queryable PRIMARY Data nodes (for query routing)
    ///
    /// Includes Active and Retiring Primary nodes. Retiring nodes
    /// may have unflushed memtable data that should still be queryable.
    /// Returns nodes sorted by node_id for consistent routing.
    pub fn get_queryable_primary_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| {
                (n.state == NodeState::Active || n.state == NodeState::Retiring)
                    && n.mode == DataNodeMode::Primary
            })
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get all active PRIMARY Data nodes (for query routing)
    ///
    /// In Cold Standby architecture, only Primary nodes should serve queries.
    /// Standby nodes are cold backups that only receive replication.
    /// Returns nodes sorted by node_id for consistent hash-based routing.
    pub fn get_active_primary_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active && n.mode == DataNodeMode::Primary)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Get all active STANDBY Data nodes
    ///
    /// Standby nodes only receive replication (never promoted in No-Promotion architecture).
    /// Returns nodes sorted by node_id for consistent routing.
    pub fn get_active_standby_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Active && n.mode == DataNodeMode::Standby)
            .cloned()
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        nodes
    }

    /// Update the list of Data nodes (replaces all)
    pub fn update_data_nodes(&self, nodes: Vec<DataNodeInfo>) {
        let active_count = nodes
            .iter()
            .filter(|n| n.state == NodeState::Active)
            .count();
        info!(
            "Updating data nodes: {} total, {} active",
            nodes.len(),
            active_count
        );

        let mut map = self.data_nodes.write();
        map.clear();
        for node in nodes {
            map.insert(node.node_id.clone(), node);
        }
    }

    /// Add or update a Data node
    pub fn upsert_data_node(&self, node: DataNodeInfo) {
        self.data_nodes.write().insert(node.node_id.clone(), node);
    }

    /// Remove a Data node
    pub fn remove_data_node(&self, node_id: &str) {
        self.data_nodes.write().remove(node_id);
    }

    /// Mark a node as suspect (recent write failure)
    ///
    /// Suspect nodes are temporarily avoided for new writes but can recover
    /// automatically when the next health check succeeds or after a timeout.
    pub fn mark_node_suspect(&self, node_id: &str) {
        let mut nodes = self.data_nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            if node.state == NodeState::Active {
                info!("Marking node {} as suspect due to write failure", node_id);
                node.state = NodeState::Suspect;
            }
        }
    }

    /// Record successful write to a node (clears suspect state)
    pub fn record_node_success(&self, node_id: &str) {
        let mut nodes = self.data_nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            if node.state == NodeState::Suspect {
                debug!("Node {} recovered from suspect state", node_id);
                node.state = NodeState::Active;
            }
        }
    }

    // update_node_mode() removed - No-Promotion architecture
    // Mode is set at registration time (via upsert_data_node) and never changes

    /// Select node with load balancing (for reads)
    ///
    /// In Cold Standby architecture, only Primary nodes serve queries.
    /// Standby nodes are cold backups that never serve traffic.
    /// Retiring nodes can still serve reads (they may have unflushed memtable data).
    pub fn select_node_for_read(&self) -> Option<DataNodeInfo> {
        // Prefer queryable Primary nodes for reads (Active or Retiring)
        let primary_nodes = self.get_queryable_primary_nodes();
        if !primary_nodes.is_empty() {
            let seq = self.last_event_seq.load(Ordering::Relaxed);
            let idx = (seq as usize) % primary_nodes.len();
            return Some(primary_nodes[idx].clone());
        }

        // Fall back to all queryable nodes if no Primary nodes
        // (legacy mode without Cold Standby architecture)
        let nodes = self.get_queryable_data_nodes();
        if nodes.is_empty() {
            return None;
        }

        // Simple round-robin via event sequence (good enough for now)
        let seq = self.last_event_seq.load(Ordering::Relaxed);
        let idx = (seq as usize) % nodes.len();
        Some(nodes[idx].clone())
    }

    /// Update last event sequence
    pub fn set_last_event_seq(&self, seq: u64) {
        self.last_event_seq.store(seq, Ordering::Relaxed);
    }

    /// Get last event sequence
    pub fn get_last_event_seq(&self) -> u64 {
        self.last_event_seq.load(Ordering::Relaxed)
    }

    /// Get stats about the cache
    pub fn stats(&self) -> ShardCacheStats {
        let nodes = self.data_nodes.read();
        ShardCacheStats {
            table_count: self.table_owners.len(),
            data_node_count: nodes.len(),
            active_node_count: nodes
                .values()
                .filter(|n| n.state == NodeState::Active)
                .count(),
            last_event_seq: self.last_event_seq.load(Ordering::Relaxed),
            distribution_config_count: self.distribution_configs.len(),
        }
    }
}

impl Default for ShardCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Stats about the shard cache
#[derive(Debug, Clone)]
pub struct ShardCacheStats {
    pub table_count: usize,
    pub data_node_count: usize,
    pub active_node_count: usize,
    pub last_event_seq: u64,
    pub distribution_config_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod distribution_cache_tests {
        use super::*;

        fn create_test_cache() -> ShardCache {
            ShardCache::new()
        }

        #[test]
        fn test_get_distribution_percent_default() {
            let cache = create_test_cache();

            // No config set, should return 0 (single node routing)
            let percent = cache.get_distribution_percent("mydb", "metrics", "cpu");
            assert_eq!(percent, 0);
        }

        #[test]
        fn test_set_and_get_distribution_config_catalog() {
            let cache = create_test_cache();

            // Set catalog-level config
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "".to_string(),
                "".to_string(),
                100,
            );
            cache.set_distribution_config(&config);

            // Should inherit catalog-level config
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 100);
            assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 100);
            assert_eq!(cache.get_distribution_percent("mydb", "", ""), 100);

            // Different catalog should not be affected
            assert_eq!(cache.get_distribution_percent("other", "metrics", "cpu"), 0);
        }

        #[test]
        fn test_set_and_get_distribution_config_schema() {
            let cache = create_test_cache();

            // Set schema-level config
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "".to_string(),
                50,
            );
            cache.set_distribution_config(&config);

            // Should inherit schema-level config
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 50);
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 50);

            // Different schema should not be affected
            assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 0);
        }

        #[test]
        fn test_set_and_get_distribution_config_table() {
            let cache = create_test_cache();

            // Set table-level config
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                75,
            );
            cache.set_distribution_config(&config);

            // Should match table-level config
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 75);

            // Different table should not be affected
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 0);
        }

        #[test]
        fn test_hierarchical_override_table_over_schema() {
            let cache = create_test_cache();

            // Set schema-level config: 50%
            let schema_config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "".to_string(),
                50,
            );
            cache.set_distribution_config(&schema_config);

            // Set table-level config: 0% (override)
            let table_config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                0,
            );
            cache.set_distribution_config(&table_config);

            // cpu table should use table-level config (0%)
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);

            // Other tables in same schema should use schema-level config (50%)
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 50);
        }

        #[test]
        fn test_hierarchical_override_schema_over_catalog() {
            let cache = create_test_cache();

            // Set catalog-level config: 100%
            let catalog_config = DistributionConfig::new(
                "mydb".to_string(),
                "".to_string(),
                "".to_string(),
                100,
            );
            cache.set_distribution_config(&catalog_config);

            // Set schema-level config: 25% (override)
            let schema_config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "".to_string(),
                25,
            );
            cache.set_distribution_config(&schema_config);

            // metrics schema should use schema-level config (25%)
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 25);

            // Other schemas should use catalog-level config (100%)
            assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 100);
        }

        #[test]
        fn test_hierarchical_override_all_levels() {
            let cache = create_test_cache();

            // Set all three levels
            let catalog_config = DistributionConfig::new(
                "mydb".to_string(),
                "".to_string(),
                "".to_string(),
                100,
            );
            cache.set_distribution_config(&catalog_config);

            let schema_config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "".to_string(),
                50,
            );
            cache.set_distribution_config(&schema_config);

            let table_config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                0,
            );
            cache.set_distribution_config(&table_config);

            // Table level wins
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);

            // Schema level for other tables in metrics
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 50);

            // Catalog level for other schemas
            assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 100);
        }

        #[test]
        fn test_is_distributed() {
            let cache = create_test_cache();

            // No config = not distributed
            assert!(!cache.is_distributed("mydb", "metrics", "cpu"));

            // Set 0% = not distributed
            let config_0 = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                0,
            );
            cache.set_distribution_config(&config_0);
            assert!(!cache.is_distributed("mydb", "metrics", "cpu"));

            // Set 1% = distributed
            let config_1 = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "mem".to_string(),
                1,
            );
            cache.set_distribution_config(&config_1);
            assert!(cache.is_distributed("mydb", "metrics", "mem"));

            // Set 100% = distributed
            let config_100 = DistributionConfig::new(
                "mydb".to_string(),
                "logs".to_string(),
                "".to_string(),
                100,
            );
            cache.set_distribution_config(&config_100);
            assert!(cache.is_distributed("mydb", "logs", "app"));
        }

        #[test]
        fn test_remove_distribution_config() {
            let cache = create_test_cache();

            // Set config
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                100,
            );
            cache.set_distribution_config(&config);
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 100);

            // Remove config
            cache.remove_distribution_config("mydb", "metrics", "cpu");
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);
        }

        #[test]
        fn test_clear_distribution_configs() {
            let cache = create_test_cache();

            // Set multiple configs
            let config1 = DistributionConfig::new("db1".to_string(), "".to_string(), "".to_string(), 100);
            let config2 = DistributionConfig::new("db2".to_string(), "s".to_string(), "".to_string(), 50);
            let config3 = DistributionConfig::new("db3".to_string(), "s".to_string(), "t".to_string(), 25);

            cache.set_distribution_config(&config1);
            cache.set_distribution_config(&config2);
            cache.set_distribution_config(&config3);

            assert_eq!(cache.stats().distribution_config_count, 3);

            // Clear all
            cache.clear_distribution_configs();

            assert_eq!(cache.stats().distribution_config_count, 0);
            assert_eq!(cache.get_distribution_percent("db1", "", ""), 0);
            assert_eq!(cache.get_distribution_percent("db2", "s", ""), 0);
            assert_eq!(cache.get_distribution_percent("db3", "s", "t"), 0);
        }

        #[test]
        fn test_list_distribution_configs() {
            let cache = create_test_cache();

            let config1 = DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
            let config2 = DistributionConfig::new("mydb".to_string(), "metrics".to_string(), "".to_string(), 50);

            cache.set_distribution_config(&config1);
            cache.set_distribution_config(&config2);

            let configs = cache.list_distribution_configs();
            assert_eq!(configs.len(), 2);

            // Check that both configs are present
            let keys: Vec<String> = configs.iter().map(|(k, _, _)| k.clone()).collect();
            assert!(keys.contains(&"mydb".to_string()));
            assert!(keys.contains(&"mydb/metrics".to_string()));
        }

        #[test]
        fn test_invalidate_distribution_configs_for_catalog() {
            let cache = create_test_cache();

            // Set configs for multiple catalogs
            let config1 = DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
            let config2 = DistributionConfig::new("mydb".to_string(), "metrics".to_string(), "".to_string(), 50);
            let config3 = DistributionConfig::new("mydb".to_string(), "metrics".to_string(), "cpu".to_string(), 25);
            let config4 = DistributionConfig::new("other".to_string(), "".to_string(), "".to_string(), 75);

            cache.set_distribution_config(&config1);
            cache.set_distribution_config(&config2);
            cache.set_distribution_config(&config3);
            cache.set_distribution_config(&config4);

            assert_eq!(cache.stats().distribution_config_count, 4);

            // Invalidate only mydb catalog
            cache.invalidate_distribution_configs_for_catalog("mydb");

            // Only "other" catalog should remain
            assert_eq!(cache.stats().distribution_config_count, 1);
            assert_eq!(cache.get_distribution_percent("mydb", "", ""), 0);
            assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);
            assert_eq!(cache.get_distribution_percent("other", "", ""), 75);
        }

        #[test]
        fn test_stats_includes_distribution_count() {
            let cache = create_test_cache();

            let stats = cache.stats();
            assert_eq!(stats.distribution_config_count, 0);

            let config = DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
            cache.set_distribution_config(&config);

            let stats = cache.stats();
            assert_eq!(stats.distribution_config_count, 1);
        }
    }
}
