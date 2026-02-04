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

mod distribution;
mod node_mgmt;
mod partition_cache;
mod table_routing;
pub mod types;

pub use types::{
    CachedDistributionConfig, CachedPartitionInfo, CachedTableMeta, DataNodeInfo, NodeState,
    PartitionKey, ShardCacheStats, TableKey,
};

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

/// Shard cache for table→node mapping
///
/// Uses table-level routing where all partitions of a table are routed to the same node.
///
/// Lookup structure:
/// 1. table_owners: table_path → owner_node_id
/// 2. data_nodes: node_id → DataNodeInfo (single source of truth)
/// 3. distribution_configs: hierarchical cache for write distribution
/// 4. partition_nodes: (table_path, date) → [node_ids] (for smart query routing)
///
/// Table-level routing provides:
/// - 600x fewer Meta entries (100K tables vs 60M files)
/// - Single routing lookup per query
/// - Excellent cache locality for hot data
///
/// Partition-level cache provides:
/// - Smart query routing when date filters are present
/// - Avoids broadcasting to nodes without data for requested partition
pub struct ShardCache {
    /// Table → owner node_id
    /// Key: "catalog/schema/table"
    pub(crate) table_owners: DashMap<String, CachedTableMeta>,

    /// node_id → DataNodeInfo (single source of truth for node state)
    pub(crate) data_nodes: RwLock<HashMap<String, DataNodeInfo>>,

    /// Last sequence number from Meta event stream
    pub(crate) last_event_seq: AtomicU64,

    /// Distribution configs cache
    /// Key: "catalog" or "catalog/schema" or "catalog/schema/table"
    /// Hierarchical lookup: table > schema > catalog
    pub(crate) distribution_configs: DashMap<String, CachedDistributionConfig>,

    /// Pending owner registrations (prevents duplicate concurrent Meta calls)
    /// Key: "catalog/schema/table:node_id"
    /// When a registration is in progress, other writes skip the Meta call
    pub(crate) pending_registrations: DashSet<String>,

    /// Partition → node_ids cache for smart query routing
    /// Key: "catalog/schema/table:date" (e.g., "mydb/default/cpu:2024-01-15")
    /// Value: List of node_ids that have data for this partition
    ///
    /// This enables the Router to:
    /// - Skip nodes that don't have data for a requested date partition
    /// - Route queries directly to nodes with data (cache affinity)
    /// - Support queries across multiple nodes when data is distributed
    pub(crate) partition_nodes: DashMap<String, CachedPartitionInfo>,
}

impl ShardCache {
    pub fn new() -> Self {
        Self {
            table_owners: DashMap::new(),
            data_nodes: RwLock::new(HashMap::new()),
            last_event_seq: AtomicU64::new(0),
            distribution_configs: DashMap::new(),
            pending_registrations: DashSet::new(),
            partition_nodes: DashMap::new(),
        }
    }

    /// Get stats about the cache
    pub fn stats(&self) -> ShardCacheStats {
        let nodes = self.data_nodes.read();
        let (partition_entries, partition_node_refs) = self.partition_cache_stats();
        ShardCacheStats {
            table_count: self.table_owners.len(),
            data_node_count: nodes.len(),
            active_node_count: nodes
                .values()
                .filter(|n| n.state == NodeState::Active)
                .count(),
            last_event_seq: self.last_event_seq.load(std::sync::atomic::Ordering::Relaxed),
            distribution_config_count: self.distribution_configs.len(),
            partition_cache_entries: partition_entries,
            partition_cache_node_refs: partition_node_refs,
        }
    }
}

impl Default for ShardCache {
    fn default() -> Self {
        Self::new()
    }
}
