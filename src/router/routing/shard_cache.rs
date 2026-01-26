//! Shard Cache - Caches partition→node mappings from Meta service
//!
//! The ShardCache maintains a mapping of partition keys to Data node assignments.
//! It is updated via events from the Meta service when files are registered or
//! nodes join/leave the cluster.

use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
    /// Load score for weighted routing (0.0-1.0, lower is better)
    pub load_score: f32,
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Draining,
    Unhealthy,
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

/// Shard cache for partition→node mapping
pub struct ShardCache {
    /// Partition → assigned Data node mapping
    partition_owners: DashMap<String, DataNodeInfo>,

    /// Active Data nodes (healthy only)
    data_nodes: RwLock<Vec<DataNodeInfo>>,

    /// Last sequence number from Meta event stream
    last_event_seq: AtomicU64,
}

impl ShardCache {
    pub fn new() -> Self {
        Self {
            partition_owners: DashMap::new(),
            data_nodes: RwLock::new(Vec::new()),
            last_event_seq: AtomicU64::new(0),
        }
    }

    /// Get the assigned Data node for a partition
    pub fn get_partition_owner(&self, key: &PartitionKey) -> Option<DataNodeInfo> {
        self.partition_owners.get(&key.to_path()).map(|v| v.clone())
    }

    /// Set the assigned Data node for a partition
    pub fn set_partition_owner(&self, key: &PartitionKey, node: DataNodeInfo) {
        debug!(
            "Setting partition owner: {} -> {}",
            key.to_path(),
            node.node_id
        );
        self.partition_owners.insert(key.to_path(), node);
    }

    /// Remove a partition owner (e.g., when partition is deleted)
    pub fn remove_partition_owner(&self, key: &PartitionKey) {
        self.partition_owners.remove(&key.to_path());
    }

    /// Get all active Data nodes
    pub fn get_active_data_nodes(&self) -> Vec<DataNodeInfo> {
        self.data_nodes
            .read()
            .iter()
            .filter(|n| n.state == NodeState::Active)
            .cloned()
            .collect()
    }

    /// Update the list of Data nodes
    pub fn update_data_nodes(&self, nodes: Vec<DataNodeInfo>) {
        let active_count = nodes.iter().filter(|n| n.state == NodeState::Active).count();
        info!(
            "Updating data nodes: {} total, {} active",
            nodes.len(),
            active_count
        );
        *self.data_nodes.write() = nodes;
    }

    /// Add or update a Data node
    pub fn upsert_data_node(&self, node: DataNodeInfo) {
        let mut nodes = self.data_nodes.write();
        if let Some(existing) = nodes.iter_mut().find(|n| n.node_id == node.node_id) {
            *existing = node;
        } else {
            nodes.push(node);
        }
    }

    /// Remove a Data node
    pub fn remove_data_node(&self, node_id: &str) {
        self.data_nodes.write().retain(|n| n.node_id != node_id);
    }

    /// Select a Data node for a partition using consistent hashing
    ///
    /// This provides sticky routing so the same partition usually goes to the same node,
    /// which improves cache efficiency and reduces lock contention.
    pub fn select_node_for_partition(&self, key: &PartitionKey) -> Option<DataNodeInfo> {
        // First check if we have a cached owner
        if let Some(owner) = self.get_partition_owner(key) {
            if owner.state == NodeState::Active {
                return Some(owner);
            }
        }

        // Fall back to hash-based selection
        let nodes = self.get_active_data_nodes();
        if nodes.is_empty() {
            warn!("No active data nodes available");
            return None;
        }

        // Consistent hashing based on partition key
        let hash = self.hash_partition_key(key);
        let idx = (hash as usize) % nodes.len();
        Some(nodes[idx].clone())
    }

    /// Select node with load balancing (for reads)
    pub fn select_node_for_read(&self) -> Option<DataNodeInfo> {
        let nodes = self.get_active_data_nodes();
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
            active_node_count: nodes.iter().filter(|n| n.state == NodeState::Active).count(),
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
