//! Table types for the Meta service

use serde::{Deserialize, Serialize};

use super::{current_timestamp_ms, keys};

/// Table-level metadata for routing and statistics
///
/// This is the primary routing mechanism for cluster mode. Each table is owned
/// by a single Data node which handles all partitions for that table.
///
/// Key design principles:
/// - One Meta entry per table (not per partition or per file)
/// - 100K tables = 100K entries (vs 60M for file-level)
/// - Data node manages its own partitions locally
/// - File lists derived from S3 listing, not stored in Meta
///
/// This design optimizes for time-series workloads where 95%+ queries
/// access today's partition, making table-level routing optimal.
///
/// ## Multi-Node Ownership
///
/// A table can have multiple owner nodes in two scenarios:
/// 1. **Write reroute**: When primary owner fails, writes go to fallback node
/// 2. **Large table distribution**: Future feature to spread data across nodes
///
/// The `owner_node_ids` vector maintains all nodes that have data for this table.
/// The first node is the "primary owner" (original or designated).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    // Identity
    pub catalog: String,
    pub schema: String,
    pub table: String,

    // Routing (primary purpose)
    /// All Data nodes that own partitions for this table.
    /// First element is the primary owner (for new writes).
    /// Additional elements are nodes with rerouted data or distributed partitions.
    pub owner_node_ids: Vec<String>,

    // Cached statistics (updated periodically, not per-flush)
    /// Number of date partitions (e.g., 30 for a month of data)
    pub partition_count: u32,
    /// Total bytes across all partitions
    pub total_bytes: u64,
    /// Total rows across all partitions
    pub total_rows: u64,
    /// Oldest partition key (e.g., "date=2024-01-01")
    pub oldest_partition: Option<String>,
    /// Newest partition key (e.g., "date=2024-01-31")
    pub newest_partition: Option<String>,
    /// Minimum timestamp across all data
    pub min_time: i64,
    /// Maximum timestamp across all data
    pub max_time: i64,

    // Metadata
    pub created_at: i64,
    pub last_modified: i64,
    /// Version for optimistic concurrency and cache invalidation
    pub version: u64,
}

impl TableMeta {
    /// Create a new TableMeta for a newly discovered table
    pub fn new(catalog: String, schema: String, table: String, owner_node_id: String) -> Self {
        let now = current_timestamp_ms();
        Self {
            catalog,
            schema,
            table,
            owner_node_ids: vec![owner_node_id],
            partition_count: 0,
            total_bytes: 0,
            total_rows: 0,
            oldest_partition: None,
            newest_partition: None,
            min_time: i64::MAX,
            max_time: i64::MIN,
            created_at: now,
            last_modified: now,
            version: 1,
        }
    }

    /// Get the primary owner node (first in the list)
    pub fn primary_owner(&self) -> Option<&str> {
        self.owner_node_ids.first().map(|s| s.as_str())
    }

    /// Check if a node is an owner of this table
    pub fn is_owner(&self, node_id: &str) -> bool {
        self.owner_node_ids.iter().any(|id| id == node_id)
    }

    /// Add a new owner node (for rerouted writes or distribution)
    /// Returns true if the node was added, false if already present
    pub fn add_owner(&mut self, node_id: String) -> bool {
        if !self.is_owner(&node_id) {
            self.owner_node_ids.push(node_id);
            self.last_modified = current_timestamp_ms();
            self.version += 1;
            true
        } else {
            false
        }
    }

    /// Remove an owner node (for node failure cleanup)
    /// Returns true if the node was removed
    pub fn remove_owner(&mut self, node_id: &str) -> bool {
        let initial_len = self.owner_node_ids.len();
        self.owner_node_ids.retain(|id| id != node_id);
        if self.owner_node_ids.len() < initial_len {
            self.last_modified = current_timestamp_ms();
            self.version += 1;
            true
        } else {
            false
        }
    }

    /// Set primary owner (moves to front of list)
    pub fn set_primary_owner(&mut self, node_id: String) {
        // Remove if already in list
        self.owner_node_ids.retain(|id| id != &node_id);
        // Insert at front
        self.owner_node_ids.insert(0, node_id);
        self.last_modified = current_timestamp_ms();
        self.version += 1;
    }

    /// Get all owner node IDs
    pub fn all_owners(&self) -> &[String] {
        &self.owner_node_ids
    }

    /// Check if table has multiple owners (rerouted or distributed)
    pub fn is_multi_owner(&self) -> bool {
        self.owner_node_ids.len() > 1
    }

    // Legacy compatibility - returns primary owner
    #[deprecated(note = "Use primary_owner() or all_owners() instead")]
    pub fn owner_node_id(&self) -> &str {
        self.primary_owner().unwrap_or("")
    }

    /// Generate the storage key for this table
    pub fn storage_key(&self) -> String {
        Self::key_for(&self.catalog, &self.schema, &self.table)
    }

    /// Get the table key in format "catalog/schema/table"
    pub fn table_key(&self) -> String {
        format!("{}/{}/{}", self.catalog, self.schema, self.table)
    }

    /// Generate a storage key for a table
    pub fn key_for(catalog: &str, schema: &str, table: &str) -> String {
        format!("{}{}/{}/{}", keys::PREFIX_TABLE, catalog, schema, table)
    }

    /// Generate a prefix for listing all tables in a schema
    pub fn schema_prefix(catalog: &str, schema: &str) -> String {
        format!("{}{}/{}/", keys::PREFIX_TABLE, catalog, schema)
    }

    /// Generate a prefix for listing all tables in a catalog
    pub fn catalog_prefix(catalog: &str) -> String {
        format!("{}{}/", keys::PREFIX_TABLE, catalog)
    }

    /// Generate a prefix for listing all tables
    pub fn all_prefix() -> String {
        keys::PREFIX_TABLE.to_string()
    }

    /// Update statistics with a delta (after flush)
    pub fn apply_stats_delta(&mut self, delta: &TableStatsDelta) {
        self.partition_count = (self.partition_count as i64 + delta.partition_count_delta) as u32;
        self.total_bytes = (self.total_bytes as i64 + delta.bytes_delta) as u64;
        self.total_rows = (self.total_rows as i64 + delta.rows_delta) as u64;

        if let Some(ref partition) = delta.newest_partition {
            // Update newest if this is newer
            if self.newest_partition.is_none()
                || partition > self.newest_partition.as_ref().unwrap()
            {
                self.newest_partition = Some(partition.clone());
            }
        }

        if delta.min_time < self.min_time {
            self.min_time = delta.min_time;
        }
        if delta.max_time > self.max_time {
            self.max_time = delta.max_time;
        }

        self.last_modified = current_timestamp_ms();
        self.version += 1;
    }
}

/// Delta for updating table statistics
///
/// Used after flush operations to update Meta with aggregated changes.
/// This avoids per-file updates and reduces Meta traffic significantly.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStatsDelta {
    /// Change in partition count (+1 for new partition, 0 for existing)
    pub partition_count_delta: i64,
    /// Change in total bytes (positive for flush, negative for delete)
    pub bytes_delta: i64,
    /// Change in total rows
    pub rows_delta: i64,
    /// Files added in this operation
    pub files_delta: i64,
    /// Newest partition affected (for updating newest_partition)
    pub newest_partition: Option<String>,
    /// Minimum timestamp in this delta
    pub min_time: i64,
    /// Maximum timestamp in this delta
    pub max_time: i64,
}

impl TableStatsDelta {
    pub fn new() -> Self {
        Self {
            partition_count_delta: 0,
            bytes_delta: 0,
            rows_delta: 0,
            files_delta: 0,
            newest_partition: None,
            min_time: i64::MAX,
            max_time: i64::MIN,
        }
    }

    /// Create from flush results
    pub fn from_flush(
        bytes: u64,
        rows: u64,
        files: u32,
        partition: &str,
        is_new_partition: bool,
    ) -> Self {
        Self {
            partition_count_delta: if is_new_partition { 1 } else { 0 },
            bytes_delta: bytes as i64,
            rows_delta: rows as i64,
            files_delta: files as i64,
            newest_partition: Some(partition.to_string()),
            min_time: i64::MAX,
            max_time: i64::MIN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_meta_new() {
        let meta = TableMeta::new(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
        );

        assert_eq!(meta.catalog, "mydb");
        assert_eq!(meta.schema, "default");
        assert_eq!(meta.table, "cpu");
        assert_eq!(meta.primary_owner(), Some("data-1"));
    }

    #[test]
    fn test_multi_owner() {
        let mut meta = TableMeta::new(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
        );

        // Add a second owner
        meta.add_owner("data-2".to_string());
        assert!(meta.is_multi_owner());
        assert_eq!(meta.all_owners().len(), 2);
    }

    #[test]
    fn test_serialization() {
        let meta = TableMeta::new(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
        );

        // Test bincode serialization
        let encoded = bincode::serialize(&meta).unwrap();
        let decoded: TableMeta = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded.catalog, meta.catalog);
        assert_eq!(decoded.schema, meta.schema);
        assert_eq!(decoded.table, meta.table);
    }
}
