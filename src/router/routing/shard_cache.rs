//! Shard Cache - Caches partition→node mappings from Meta service
//!
//! The ShardCache maintains a mapping of partition keys to Data node assignments.
//! It is updated via events from the Meta service when files are registered or
//! nodes join/leave the cluster.
//!
//! Design: Stores only node_id in partition_owners, with full DataNodeInfo in
//! data_nodes (single source of truth). This prevents stale state issues when
//! node health changes.

use crate::meta::types::DataNodeMode;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
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

/// Partition replica info stored in shard cache (local copy of Meta state)
#[derive(Debug, Clone)]
pub struct PartitionReplicaInfo {
    /// Partition key (catalog/schema/table/date=YYYY-MM-DD)
    pub partition_key: String,
    /// Current leader node ID
    pub leader_node_id: String,
    /// Standby node IDs for failover
    pub standby_node_ids: Vec<String>,
}

/// Shard cache for partition→node mapping
///
/// Uses a two-level lookup:
/// 1. partition_owners: partition_path → node_id (reference only)
/// 2. data_nodes: node_id → DataNodeInfo (single source of truth)
/// 3. partition_replicas: partition_key → PartitionReplicaInfo (for warm standby failover)
///
/// This design ensures node state is always fresh - when a node becomes
/// unhealthy, the lookup automatically fails over to hash-based routing
/// without needing to update partition_owners.
pub struct ShardCache {
    /// Partition → owner node_id (reference only, not full DataNodeInfo)
    partition_owners: DashMap<String, String>,

    /// node_id → DataNodeInfo (single source of truth for node state)
    data_nodes: RwLock<HashMap<String, DataNodeInfo>>,

    /// Partition replicas for warm standby failover (partition_key → replica info)
    partition_replicas: DashMap<String, PartitionReplicaInfo>,

    /// Last sequence number from Meta event stream
    last_event_seq: AtomicU64,
}

impl ShardCache {
    pub fn new() -> Self {
        Self {
            partition_owners: DashMap::new(),
            data_nodes: RwLock::new(HashMap::new()),
            partition_replicas: DashMap::new(),
            last_event_seq: AtomicU64::new(0),
        }
    }

    /// Get the owner node_id for a partition
    pub fn get_partition_owner(&self, key: &PartitionKey) -> Option<String> {
        self.partition_owners.get(&key.to_path()).map(|v| v.clone())
    }

    /// Set the owner node_id for a partition
    pub fn set_partition_owner(&self, key: &PartitionKey, node_id: String) {
        debug!("Setting partition owner: {} -> {}", key.to_path(), node_id);
        self.partition_owners.insert(key.to_path(), node_id);
    }

    /// Remove a partition owner (e.g., when partition is deleted)
    pub fn remove_partition_owner(&self, key: &PartitionKey) {
        self.partition_owners.remove(&key.to_path());
    }

    /// Remove all partition ownership entries for a specific node
    /// Called when a node permanently leaves the cluster
    pub fn remove_partitions_for_node(&self, node_id: &str) -> usize {
        let mut removed = 0;
        self.partition_owners.retain(|_, owner_id| {
            if owner_id == node_id {
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0 {
            info!(
                "Cleaned up {} partition entries for departed node {}",
                removed, node_id
            );
        }
        removed
    }

    // ========================================================================
    // Partition Replica Operations (Warm Standby)
    // ========================================================================

    /// Set partition replica info (leader + standbys)
    ///
    /// Called when Meta publishes ReplicaAssigned event.
    pub fn set_partition_replica(
        &self,
        partition_key: &str,
        leader_node_id: String,
        standby_node_ids: Vec<String>,
    ) {
        debug!(
            "Setting partition replica: {} -> leader={}, standbys={:?}",
            partition_key, leader_node_id, standby_node_ids
        );
        self.partition_replicas.insert(
            partition_key.to_string(),
            PartitionReplicaInfo {
                partition_key: partition_key.to_string(),
                leader_node_id,
                standby_node_ids,
            },
        );
    }

    /// Get partition replica info
    pub fn get_partition_replica(&self, partition_key: &str) -> Option<PartitionReplicaInfo> {
        self.partition_replicas
            .get(partition_key)
            .map(|r| r.clone())
    }

    // promote_partition_leader() removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    /// Remove partition replica
    pub fn remove_partition_replica(&self, partition_key: &str) {
        self.partition_replicas.remove(partition_key);
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

    /// Select a Data node for a partition using replica-aware routing
    ///
    /// **Zero-Downtime Failover**: If the partition has replica info (leader + standbys),
    /// we check leader health first. If leader is unhealthy, we IMMEDIATELY route to
    /// standby - no waiting for Meta to detect failure!
    ///
    /// Selection priority:
    /// 1. Partition replica leader (if Active)
    /// 2. Partition replica standby (if leader is Suspect/Unhealthy) - INSTANT FAILOVER!
    /// 3. Cached owner (legacy, for partitions without replica info)
    /// 4. Hash-based selection among Active nodes
    /// 5. Suspect nodes (last resort)
    pub fn select_node_for_partition(&self, key: &PartitionKey) -> Option<DataNodeInfo> {
        let partition_key = key.to_path();

        // === PRIORITY 1 & 2: Check partition replica for zero-downtime failover ===
        if let Some(replica) = self.partition_replicas.get(&partition_key) {
            let nodes = self.data_nodes.read();

            // Check leader health AND mode
            // Leader must be Active (not Retiring/Drained) AND Primary (not Standby node)
            if let Some(leader) = nodes.get(&replica.leader_node_id) {
                if leader.state == NodeState::Active && leader.mode == DataNodeMode::Primary {
                    // Leader healthy and is Primary - use it
                    return Some(leader.clone());
                }
            }

            // Leader unhealthy/retiring/standby - IMMEDIATELY use standby (zero-downtime!)
            // Note: partition standby replicas should be on Primary nodes
            for standby_id in &replica.standby_node_ids {
                if let Some(standby) = nodes.get(standby_id) {
                    if standby.state == NodeState::Active && standby.mode == DataNodeMode::Primary {
                        info!(
                            "Leader {} unavailable, instant failover to standby {} for partition {}",
                            replica.leader_node_id, standby_id, partition_key
                        );
                        return Some(standby.clone());
                    }
                }
            }
            // All replicas unhealthy - fall through to hash-based
        }

        // === PRIORITY 3: Check cached owner (legacy partitions without replica info) ===
        if let Some(owner_id) = self.get_partition_owner(key) {
            let nodes = self.data_nodes.read();
            if let Some(owner) = nodes.get(&owner_id) {
                // Owner must be Active AND Primary
                if owner.state == NodeState::Active && owner.mode == DataNodeMode::Primary {
                    return Some(owner.clone());
                }
            }
            // Owner unhealthy/retiring/standby → fall through to hash-based
        }

        // === PRIORITY 4: Hash-based selection among Active PRIMARY nodes ===
        // In Cold Standby architecture, only route to Primary nodes
        let primary_nodes = self.get_active_primary_nodes();
        if !primary_nodes.is_empty() {
            let hash = self.hash_partition_key(key);
            let idx = (hash as usize) % primary_nodes.len();
            let selected = &primary_nodes[idx];
            info!(
                "Hash routing: partition={} hash={} num_nodes={} idx={} -> {}",
                partition_key, hash, primary_nodes.len(), idx, selected.node_id
            );
            return Some(selected.clone());
        }

        // Fall back to all active nodes if no Primary nodes
        // (legacy mode without Cold Standby architecture)
        let active_nodes = self.get_active_data_nodes();
        if !active_nodes.is_empty() {
            let hash = self.hash_partition_key(key);
            let idx = (hash as usize) % active_nodes.len();
            return Some(active_nodes[idx].clone());
        }

        // === PRIORITY 5: Suspect nodes (last resort) ===
        let suspect_nodes: Vec<_> = self
            .data_nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Suspect && n.mode == DataNodeMode::Primary)
            .cloned()
            .collect();

        if !suspect_nodes.is_empty() {
            warn!(
                "No active nodes, using suspect Primary node as last resort for partition {}",
                partition_key
            );
            let hash = self.hash_partition_key(key);
            let idx = (hash as usize) % suspect_nodes.len();
            return Some(suspect_nodes[idx].clone());
        }

        warn!("No active or suspect Primary data nodes available");
        None
    }

    /// Select node for partition excluding specified nodes (for retry after failure)
    ///
    /// When a write fails, we retry on a different node. This method selects
    /// the next candidate using consistent hashing, skipping nodes we've already tried.
    ///
    /// In Cold Standby architecture, prefers Primary nodes over Standby nodes.
    pub fn select_node_for_partition_excluding(
        &self,
        key: &PartitionKey,
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
            let hash = self.hash_partition_key(key);
            let idx = (hash as usize) % primary_nodes.len();
            return Some(primary_nodes[idx].clone());
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

        // Consistent hashing based on partition key
        let hash = self.hash_partition_key(key);
        let idx = (hash as usize) % nodes.len();
        Some(nodes[idx].clone())
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

    /// Hash a partition key for consistent routing
    fn hash_partition_key(&self, key: &PartitionKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get stats about the cache
    pub fn stats(&self) -> ShardCacheStats {
        let nodes = self.data_nodes.read();
        ShardCacheStats {
            partition_count: self.partition_owners.len(),
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
    pub partition_count: usize,
    pub data_node_count: usize,
    pub active_node_count: usize,
    pub last_event_seq: u64,
}
