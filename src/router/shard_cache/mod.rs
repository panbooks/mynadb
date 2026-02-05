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
//! ## Partition Routing Architecture
//!
//! ### Hot Data (memtable) - Derived from Table Owners
//! - Source of Truth: Meta Service (table_owners, already synced via pub/sub)
//! - No separate cache needed - table owner has memtable for recent partitions
//! - Consistent across all Router instances (no local state!)
//! - For recent dates (within flush_interval), query table owners
//!
//! ### Cold Data (S3/Parquet) - LRU Cache
//! - Source of Truth: S3 LIST (immutable once written)
//! - Eviction: LRU with max_entries (NOT TTL - data never changes!)
//! - Contains historical partitions discovered via S3 LIST
//! - Bounded by max_entries configuration
//!
//! This design eliminates the consistency problem of hot_partition_cache
//! in multi-router environments by deriving hot data location from
//! table ownership (which is already synced via Meta pub/sub).

pub mod cold_partition_cache;
mod distribution;
mod node_mgmt;
mod partition_cache;
mod table_routing;
pub mod types;

pub use cold_partition_cache::ColdPartitionCache;
pub use types::{
    CachedDistributionConfig, CachedPartitionInfo, CachedTableMeta, DataNodeInfo, NodeState,
    PartitionKey, ShardCacheStats, TableKey,
};

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Default cold cache max entries
const DEFAULT_COLD_CACHE_MAX_ENTRIES: usize = 100_000;

/// Default flush interval in seconds (used to determine "recent" partitions)
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 300;

/// Shard cache for table→node mapping
///
/// Uses table-level routing where all partitions of a table are routed to the same node.
///
/// Lookup structure:
/// 1. table_owners: table_path → owner_node_id (from Meta, consistent across routers)
/// 2. data_nodes: node_id → DataNodeInfo (single source of truth)
/// 3. distribution_configs: hierarchical cache for write distribution
/// 4. cold_partition_cache: Historical data (S3) - LRU-based
///
/// ## Hot Data Routing (memtable)
///
/// Hot data location is **derived** from table_owners (not cached separately):
/// - Table owner node has memtable data for recent partitions
/// - No consistency issues across multiple Router instances
/// - "Recent" = within flush_interval (default 5 min)
///
/// ## Cold Data Routing (S3/Parquet)
///
/// Cold cache provides LRU caching of S3 LIST results:
/// - Source of Truth: S3 (immutable once written)
/// - Bounded by max_entries configuration
/// - Any node can serve (shared S3 storage)
pub struct ShardCache {
    /// Table → owner node_id
    /// Key: "catalog/schema/table"
    /// Also used for HOT data routing - owner has memtable!
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
    /// TODO: Remove after migration is complete
    pub(crate) partition_nodes: DashMap<String, CachedPartitionInfo>,

    /// Cold partition cache for S3/Parquet data
    /// - Source of Truth: S3 LIST (immutable once written)
    /// - Eviction: LRU with max_entries
    /// - Purpose: Avoid repeated S3 LIST for historical queries
    pub cold_partition_cache: Arc<ColdPartitionCache>,

    /// Flush interval in seconds (used to determine "recent" partitions)
    flush_interval_secs: u64,
}

impl ShardCache {
    /// Create a new ShardCache with default settings
    pub fn new() -> Self {
        Self::with_config(DEFAULT_COLD_CACHE_MAX_ENTRIES, DEFAULT_FLUSH_INTERVAL_SECS)
    }

    /// Create a new ShardCache with custom configuration
    pub fn with_config(cold_cache_max_entries: usize, flush_interval_secs: u64) -> Self {
        Self {
            table_owners: DashMap::new(),
            data_nodes: RwLock::new(HashMap::new()),
            last_event_seq: AtomicU64::new(0),
            distribution_configs: DashMap::new(),
            pending_registrations: DashSet::new(),
            partition_nodes: DashMap::new(),
            cold_partition_cache: Arc::new(ColdPartitionCache::new(cold_cache_max_entries)),
            flush_interval_secs,
        }
    }

    /// Get stats about the cache
    pub fn stats(&self) -> ShardCacheStats {
        let nodes = self.data_nodes.read();
        let (partition_entries, partition_node_refs) = self.partition_cache_stats();
        let cold_stats = self.cold_partition_cache.stats();

        ShardCacheStats {
            table_count: self.table_owners.len(),
            data_node_count: nodes.len(),
            active_node_count: nodes
                .values()
                .filter(|n| n.state == NodeState::Active)
                .count(),
            last_event_seq: self
                .last_event_seq
                .load(std::sync::atomic::Ordering::Relaxed),
            distribution_config_count: self.distribution_configs.len(),
            // Legacy partition cache stats
            partition_cache_entries: partition_entries,
            partition_cache_node_refs: partition_node_refs,
            // Cold cache stats
            cold_cache_partitions: cold_stats.partition_count,
            cold_cache_max_entries: cold_stats.max_entries,
            cold_cache_utilization_percent: cold_stats.utilization_percent,
        }
    }

    /// Get the cold partition cache (for external access)
    pub fn cold_cache(&self) -> &Arc<ColdPartitionCache> {
        &self.cold_partition_cache
    }

    /// Check if a date is "recent" (within flush_interval)
    ///
    /// Recent partitions have data in memtable (not yet flushed to S3).
    /// For these, we query the table owner directly.
    pub fn is_recent_partition(&self, date: &str) -> bool {
        // Parse date string (YYYY-MM-DD format)
        let parsed_date = match chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => return true, // If can't parse, assume recent (safer)
        };

        let now = chrono::Utc::now().naive_utc();
        let partition_datetime = parsed_date.and_hms_opt(23, 59, 59).unwrap();
        let age_secs = (now - partition_datetime).num_seconds();

        // Consider "recent" if within flush_interval (plus some buffer for clock skew)
        // Buffer: partition could be for "today" even if slightly past midnight
        age_secs < (self.flush_interval_secs as i64 + 86400) // flush_interval + 1 day buffer
    }

    /// Get table owners for hot data routing
    ///
    /// Returns the owner node IDs for a table. These nodes have memtable data
    /// for recent partitions (data not yet flushed to S3).
    pub fn get_table_owners_for_hot_data(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Vec<String> {
        let table_key = format!("{}/{}/{}", catalog, schema, table);
        self.table_owners
            .get(&table_key)
            .map(|meta| meta.owner_node_ids.clone())
            .unwrap_or_default()
    }

    /// Remove a node from all caches (for scale-in)
    ///
    /// Called when a node leaves the cluster. Cleans up:
    /// - Table owners
    /// - Cold partition cache (marks as departed)
    pub fn remove_node_from_all_caches(&self, node_id: &str) -> (usize, usize) {
        let tables_affected = self.remove_tables_for_node(node_id);
        let cold_affected = self.cold_partition_cache.mark_node_departed(node_id);

        (tables_affected, cold_affected)
    }
}

impl Default for ShardCache {
    fn default() -> Self {
        Self::new()
    }
}
