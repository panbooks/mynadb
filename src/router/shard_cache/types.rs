//! Type definitions for ShardCache
//!
//! Contains all the structs and enums used by the shard cache:
//! - DataNodeInfo, NodeState - Node information
//! - PartitionKey, TableKey - Routing keys
//! - CachedTableMeta, CachedDistributionConfig, CachedPartitionInfo - Cache entries
//! - ShardCacheStats - Cache statistics

use crate::meta::types::DataNodeMode;

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
    /// - Standby nodes receive replication and serve memtable queries on failover
    pub mode: DataNodeMode,
    /// For Primary nodes: the Standby node that backs this Primary
    ///
    /// When this Primary fails, its backing Standby has a copy of the
    /// memtable data (via replication) and can serve queries for hot data.
    /// The Standby only serves memtable data, not Parquet (S3 is shared).
    pub backing_standby: Option<String>,
}

/// Query scope for routing - determines what data to return
///
/// Used when a Primary fails and we need to split the query:
/// - Memtable data: Query the backing Standby (has replicated memtable)
/// - Parquet data: Query any fallback Primary (S3 is shared storage)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryScope {
    /// Query both memtable and Parquet (normal operation)
    #[default]
    Full,
    /// Query only memtable (for Standby nodes on Primary failover)
    MemtableOnly,
    /// Query only Parquet/S3 (for fallback Primary on failover)
    ParquetOnly,
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
    pub version: u64,
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

/// Cached partition info for smart query routing
#[derive(Debug, Clone)]
pub struct CachedPartitionInfo {
    /// Node IDs that have data for this partition
    pub node_ids: Vec<String>,
    /// When the partition was last seen (for TTL-based eviction)
    pub cached_at: i64,
}

/// Stats about the shard cache
#[derive(Debug, Clone)]
pub struct ShardCacheStats {
    pub table_count: usize,
    pub data_node_count: usize,
    pub active_node_count: usize,
    pub last_event_seq: u64,
    pub distribution_config_count: usize,

    /// LEGACY: Number of partition cache entries (deprecated)
    pub partition_cache_entries: usize,
    /// LEGACY: Total node references in partition cache (deprecated)
    pub partition_cache_node_refs: usize,

    /// Hot cache: Number of partition entries
    pub hot_cache_partitions: usize,
    /// Hot cache: Active (non-expired) node references
    pub hot_cache_active_node_refs: usize,

    /// Cold cache: Number of partition entries
    pub cold_cache_partitions: usize,
    /// Cold cache: Maximum entries (LRU limit)
    pub cold_cache_max_entries: usize,
    /// Cold cache: Utilization percentage (0-100)
    pub cold_cache_utilization_percent: usize,
}
