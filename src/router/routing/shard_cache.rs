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

use crate::meta::types::DataNodeMode;
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
    pub owner_node_id: String,
    pub version: u64,
    pub cached_at: i64,
}

/// Shard cache for table→node mapping
///
/// Uses table-level routing where all partitions of a table are routed to the same node.
///
/// Lookup structure:
/// 1. table_owners: table_path → owner_node_id
/// 2. data_nodes: node_id → DataNodeInfo (single source of truth)
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
}

impl ShardCache {
    pub fn new() -> Self {
        Self {
            table_owners: DashMap::new(),
            data_nodes: RwLock::new(HashMap::new()),
            last_event_seq: AtomicU64::new(0),
        }
    }

    // ========================================================================
    // Table-Level Routing (RECOMMENDED)
    // ========================================================================

    /// Get the owner node_id for a table
    pub fn get_table_owner(&self, key: &TableKey) -> Option<String> {
        self.table_owners
            .get(&key.to_path())
            .map(|v| v.owner_node_id.clone())
    }

    /// Get cached table metadata
    pub fn get_table_meta(&self, key: &TableKey) -> Option<CachedTableMeta> {
        self.table_owners.get(&key.to_path()).map(|v| v.clone())
    }

    /// Set the owner node_id for a table
    pub fn set_table_owner(&self, key: &TableKey, node_id: String, version: u64) {
        debug!(
            "Setting table owner: {} -> {} (v{})",
            key.to_path(),
            node_id,
            version
        );
        self.table_owners.insert(
            key.to_path(),
            CachedTableMeta {
                owner_node_id: node_id,
                version,
                cached_at: chrono::Utc::now().timestamp_millis(),
            },
        );
    }

    /// Remove a table owner (e.g., when table is dropped)
    pub fn remove_table_owner(&self, key: &TableKey) {
        self.table_owners.remove(&key.to_path());
    }

    /// Remove all table ownership entries for a specific node
    /// Called when a node permanently leaves the cluster
    pub fn remove_tables_for_node(&self, node_id: &str) -> usize {
        let mut removed = 0;
        self.table_owners.retain(|_, meta| {
            if meta.owner_node_id == node_id {
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0 {
            info!(
                "Cleaned up {} table entries for departed node {}",
                removed, node_id
            );
        }
        removed
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

    /// Get list of tables owned by a node
    pub fn get_tables_for_node(&self, node_id: &str) -> Vec<TableKey> {
        self.table_owners
            .iter()
            .filter(|entry| entry.value().owner_node_id == node_id)
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
}
