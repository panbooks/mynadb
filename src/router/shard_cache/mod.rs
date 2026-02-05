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
//!
//! ## Split Partition Cache Architecture
//!
//! Partition routing uses a split cache design:
//!
//! ### Hot Partition Cache (memtable data)
//! - Source of Truth: Meta Service (via write notifications)
//! - TTL: 5 minutes (matches flush_interval)
//! - Contains partitions with recent write activity
//! - Provides cache affinity for memtable queries
//!
//! ### Cold Partition Cache (S3/Parquet data)
//! - Source of Truth: S3 LIST (immutable once written)
//! - Eviction: LRU with max_entries (NOT TTL - data never changes!)
//! - Contains historical partitions discovered via S3 LIST
//! - Bounded by max_entries configuration

pub mod cold_partition_cache;
mod distribution;
pub mod hot_partition_cache;
mod node_mgmt;
mod partition_cache;
mod table_routing;
pub mod types;

pub use cold_partition_cache::{ColdCacheStats, ColdPartitionCache, ColdPartitionInfo};
pub use hot_partition_cache::{HotCacheStats, HotPartitionCache, HotPartitionInfo};
pub use types::{
    CachedDistributionConfig, CachedPartitionInfo, CachedTableMeta, DataNodeInfo, NodeState,
    PartitionKey, ShardCacheStats, TableKey,
};

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Default hot cache TTL in seconds (matches flush_interval)
const DEFAULT_HOT_CACHE_TTL_SECS: u64 = 300;

/// Default cold cache max entries
const DEFAULT_COLD_CACHE_MAX_ENTRIES: usize = 100_000;

/// Shard cache for table→node mapping
///
/// Uses table-level routing where all partitions of a table are routed to the same node.
///
/// Lookup structure:
/// 1. table_owners: table_path → owner_node_id
/// 2. data_nodes: node_id → DataNodeInfo (single source of truth)
/// 3. distribution_configs: hierarchical cache for write distribution
/// 4. hot_partition_cache: Recent writes (memtable data) - TTL-based
/// 5. cold_partition_cache: Historical data (S3) - LRU-based
///
/// Table-level routing provides:
/// - 600x fewer Meta entries (100K tables vs 60M files)
/// - Single routing lookup per query
/// - Excellent cache locality for hot data
///
/// Split partition cache provides:
/// - Hot cache for memtable data (TTL = flush_interval)
/// - Cold cache for S3 data (LRU eviction, bounded size)
/// - Correct handling of midnight edge cases
/// - Proper scale-in/out behavior
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

    /// DEPRECATED: Legacy partition cache (kept for backward compatibility)
    /// Use hot_partition_cache and cold_partition_cache instead.
    /// TODO: Remove after migration is complete
    pub(crate) partition_nodes: DashMap<String, CachedPartitionInfo>,

    /// Hot partition cache for memtable data
    /// - Source of Truth: Meta Service (via write notifications)
    /// - TTL: Configurable (default 5 min = flush_interval)
    /// - Purpose: Cache affinity for recent writes
    pub hot_partition_cache: Arc<HotPartitionCache>,

    /// Cold partition cache for S3/Parquet data
    /// - Source of Truth: S3 LIST (immutable once written)
    /// - Eviction: LRU with max_entries
    /// - Purpose: Avoid repeated S3 LIST for historical queries
    pub cold_partition_cache: Arc<ColdPartitionCache>,
}

impl ShardCache {
    /// Create a new ShardCache with default settings
    pub fn new() -> Self {
        Self::with_config(DEFAULT_HOT_CACHE_TTL_SECS, DEFAULT_COLD_CACHE_MAX_ENTRIES)
    }

    /// Create a new ShardCache with custom configuration
    pub fn with_config(hot_cache_ttl_secs: u64, cold_cache_max_entries: usize) -> Self {
        Self {
            table_owners: DashMap::new(),
            data_nodes: RwLock::new(HashMap::new()),
            last_event_seq: AtomicU64::new(0),
            distribution_configs: DashMap::new(),
            pending_registrations: DashSet::new(),
            partition_nodes: DashMap::new(),
            hot_partition_cache: Arc::new(HotPartitionCache::new(hot_cache_ttl_secs)),
            cold_partition_cache: Arc::new(ColdPartitionCache::new(cold_cache_max_entries)),
        }
    }

    /// Get stats about the cache
    pub fn stats(&self) -> ShardCacheStats {
        let nodes = self.data_nodes.read();
        let (partition_entries, partition_node_refs) = self.partition_cache_stats();
        let hot_stats = self.hot_partition_cache.stats();
        let cold_stats = self.cold_partition_cache.stats();

        ShardCacheStats {
            table_count: self.table_owners.len(),
            data_node_count: nodes.len(),
            active_node_count: nodes
                .values()
                .filter(|n| n.state == NodeState::Active)
                .count(),
            last_event_seq: self.last_event_seq.load(std::sync::atomic::Ordering::Relaxed),
            distribution_config_count: self.distribution_configs.len(),
            // Legacy partition cache stats
            partition_cache_entries: partition_entries,
            partition_cache_node_refs: partition_node_refs,
            // New split cache stats
            hot_cache_partitions: hot_stats.partition_count,
            hot_cache_active_node_refs: hot_stats.active_node_refs,
            cold_cache_partitions: cold_stats.partition_count,
            cold_cache_max_entries: cold_stats.max_entries,
            cold_cache_utilization_percent: cold_stats.utilization_percent,
        }
    }

    /// Get the hot partition cache (for external access)
    pub fn hot_cache(&self) -> &Arc<HotPartitionCache> {
        &self.hot_partition_cache
    }

    /// Get the cold partition cache (for external access)
    pub fn cold_cache(&self) -> &Arc<ColdPartitionCache> {
        &self.cold_partition_cache
    }

    /// Remove a node from all caches (for scale-in)
    ///
    /// Called when a node leaves the cluster. Cleans up:
    /// - Table owners
    /// - Hot partition cache
    /// - Cold partition cache (marks as departed)
    pub fn remove_node_from_all_caches(&self, node_id: &str) -> (usize, usize, usize) {
        let tables_affected = self.remove_tables_for_node(node_id);
        let hot_affected = self.hot_partition_cache.remove_node(node_id);
        let cold_affected = self.cold_partition_cache.mark_node_departed(node_id);

        (tables_affected, hot_affected, cold_affected)
    }
}

impl Default for ShardCache {
    fn default() -> Self {
        Self::new()
    }
}
