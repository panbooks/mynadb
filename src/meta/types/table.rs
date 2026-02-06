//! Table types for the Meta service

use serde::{Deserialize, Serialize};

use super::{current_timestamp_ms, keys};

/// Default TTL for table ownership in milliseconds (10 minutes)
pub const DEFAULT_OWNER_TTL_MS: u64 = 600_000;

/// Entry representing a single owner of a table with TTL
///
/// Each owner has an associated TTL that must be refreshed by periodic heartbeats.
/// Expired owners are automatically cleaned up by the background task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableOwnerEntry {
    /// Node ID of the owner
    pub node_id: String,
    /// TTL in milliseconds for this ownership
    pub ttl_ms: u64,
    /// Timestamp when ownership was acquired (ms since epoch)
    pub acquired_at: i64,
    /// Timestamp of last write activity (ms since epoch)
    pub last_write_at: i64,
    /// Timestamp when ownership expires (ms since epoch)
    pub expires_at: i64,
}

impl TableOwnerEntry {
    /// Create a new owner entry with the given TTL
    pub fn new(node_id: String, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            node_id,
            ttl_ms,
            acquired_at: now,
            last_write_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    /// Create a new owner entry with default TTL
    pub fn with_default_ttl(node_id: String) -> Self {
        Self::new(node_id, DEFAULT_OWNER_TTL_MS)
    }

    /// Check if this ownership has expired
    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    /// Refresh the TTL, extending the expiration time
    pub fn refresh(&mut self) {
        let now = current_timestamp_ms();
        self.last_write_at = now;
        self.expires_at = now + self.ttl_ms as i64;
    }

    /// Refresh with a specific TTL (can change the TTL)
    pub fn refresh_with_ttl(&mut self, ttl_ms: u64) {
        let now = current_timestamp_ms();
        self.ttl_ms = ttl_ms;
        self.last_write_at = now;
        self.expires_at = now + ttl_ms as i64;
    }

    /// Get remaining TTL in milliseconds (0 if expired)
    pub fn remaining_ttl_ms(&self) -> u64 {
        let now = current_timestamp_ms();
        if self.expires_at > now {
            (self.expires_at - now) as u64
        } else {
            0
        }
    }
}

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
/// ## Ownership TTL
///
/// Each owner entry has a TTL that must be refreshed by periodic heartbeats.
/// This allows automatic cleanup of stale owners when nodes crash without
/// proper deregistration. The first non-expired owner is the "primary owner".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    // Identity
    pub catalog: String,
    pub schema: String,
    pub table: String,

    // Routing (primary purpose)
    /// All Data nodes that own partitions for this table with TTL.
    /// First non-expired element is the primary owner (for new writes).
    /// Additional elements are nodes with rerouted data or distributed partitions.
    pub owners: Vec<TableOwnerEntry>,

    // Legacy field for backward compatibility during migration
    // Will be removed in a future version
    #[serde(default)]
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
        Self::new_with_ttl(catalog, schema, table, owner_node_id, DEFAULT_OWNER_TTL_MS)
    }

    /// Create a new TableMeta with a specific TTL for the owner
    pub fn new_with_ttl(
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
        ttl_ms: u64,
    ) -> Self {
        let now = current_timestamp_ms();
        Self {
            catalog,
            schema,
            table,
            owners: vec![TableOwnerEntry::new(owner_node_id, ttl_ms)],
            owner_node_ids: Vec::new(), // Legacy field, empty for new tables
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

    /// Migrate legacy owner_node_ids to owners entries (for backward compatibility)
    pub fn migrate_legacy_owners(&mut self) {
        if !self.owner_node_ids.is_empty() && self.owners.is_empty() {
            for node_id in std::mem::take(&mut self.owner_node_ids) {
                self.owners.push(TableOwnerEntry::with_default_ttl(node_id));
            }
        }
    }

    /// Get all active (non-expired) owner entries
    pub fn active_owners(&self) -> impl Iterator<Item = &TableOwnerEntry> {
        self.owners.iter().filter(|o| !o.is_expired())
    }

    /// Get all active owner node IDs
    pub fn active_owner_ids(&self) -> Vec<String> {
        self.owners
            .iter()
            .filter(|o| !o.is_expired())
            .map(|o| o.node_id.clone())
            .collect()
    }

    /// Get the primary owner node (first non-expired in the list)
    pub fn primary_owner(&self) -> Option<&str> {
        self.owners
            .iter()
            .find(|o| !o.is_expired())
            .map(|o| o.node_id.as_str())
    }

    /// Get the primary owner entry (first non-expired in the list)
    pub fn primary_owner_entry(&self) -> Option<&TableOwnerEntry> {
        self.owners.iter().find(|o| !o.is_expired())
    }

    /// Check if a node is an active (non-expired) owner of this table
    pub fn is_owner(&self, node_id: &str) -> bool {
        self.owners
            .iter()
            .any(|o| o.node_id == node_id && !o.is_expired())
    }

    /// Check if a node is an owner (including expired) of this table
    pub fn has_owner_entry(&self, node_id: &str) -> bool {
        self.owners.iter().any(|o| o.node_id == node_id)
    }

    /// Get an owner entry by node ID
    pub fn get_owner_entry(&self, node_id: &str) -> Option<&TableOwnerEntry> {
        self.owners.iter().find(|o| o.node_id == node_id)
    }

    /// Get a mutable owner entry by node ID
    pub fn get_owner_entry_mut(&mut self, node_id: &str) -> Option<&mut TableOwnerEntry> {
        self.owners.iter_mut().find(|o| o.node_id == node_id)
    }

    /// Add a new owner node (for rerouted writes or distribution)
    /// Returns true if the node was added, false if already present
    pub fn add_owner(&mut self, node_id: String) -> bool {
        self.add_owner_with_ttl(node_id, DEFAULT_OWNER_TTL_MS)
    }

    /// Add a new owner node with specific TTL
    /// Returns true if the node was added, false if already present
    pub fn add_owner_with_ttl(&mut self, node_id: String, ttl_ms: u64) -> bool {
        if !self.has_owner_entry(&node_id) {
            self.owners.push(TableOwnerEntry::new(node_id, ttl_ms));
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
        let initial_len = self.owners.len();
        self.owners.retain(|o| o.node_id != node_id);
        if self.owners.len() < initial_len {
            self.last_modified = current_timestamp_ms();
            self.version += 1;
            true
        } else {
            false
        }
    }

    /// Remove all expired owner entries
    /// Returns the number of entries removed
    pub fn cleanup_expired_owners(&mut self) -> usize {
        let initial_len = self.owners.len();
        self.owners.retain(|o| !o.is_expired());
        let removed = initial_len - self.owners.len();
        if removed > 0 {
            self.last_modified = current_timestamp_ms();
            self.version += 1;
        }
        removed
    }

    /// Get all expired owner entries (for event emission)
    pub fn expired_owners(&self) -> Vec<&TableOwnerEntry> {
        self.owners.iter().filter(|o| o.is_expired()).collect()
    }

    /// Refresh TTL for a specific owner
    /// Returns true if the owner was found and refreshed
    pub fn refresh_owner_ttl(&mut self, node_id: &str) -> bool {
        if let Some(entry) = self.get_owner_entry_mut(node_id) {
            entry.refresh();
            self.last_modified = current_timestamp_ms();
            true
        } else {
            false
        }
    }

    /// Refresh TTL for a specific owner with a new TTL value
    /// Returns true if the owner was found and refreshed
    pub fn refresh_owner_ttl_with(&mut self, node_id: &str, ttl_ms: u64) -> bool {
        if let Some(entry) = self.get_owner_entry_mut(node_id) {
            entry.refresh_with_ttl(ttl_ms);
            self.last_modified = current_timestamp_ms();
            true
        } else {
            false
        }
    }

    /// Set primary owner (moves to front of list)
    pub fn set_primary_owner(&mut self, node_id: String) {
        self.set_primary_owner_with_ttl(node_id, DEFAULT_OWNER_TTL_MS)
    }

    /// Set primary owner with specific TTL (moves to front of list)
    pub fn set_primary_owner_with_ttl(&mut self, node_id: String, ttl_ms: u64) {
        // Remove existing entry if present
        let existing_entry = self
            .owners
            .iter()
            .find(|o| o.node_id == node_id)
            .cloned();
        self.owners.retain(|o| o.node_id != node_id);

        // Create new entry or refresh existing
        let entry = if let Some(mut e) = existing_entry {
            e.refresh_with_ttl(ttl_ms);
            e
        } else {
            TableOwnerEntry::new(node_id, ttl_ms)
        };

        // Insert at front
        self.owners.insert(0, entry);
        self.last_modified = current_timestamp_ms();
        self.version += 1;
    }

    /// Get all owner node IDs (including expired, for backward compatibility)
    pub fn all_owners(&self) -> Vec<String> {
        self.owners.iter().map(|o| o.node_id.clone()).collect()
    }

    /// Get all owner entries
    pub fn all_owner_entries(&self) -> &[TableOwnerEntry] {
        &self.owners
    }

    /// Check if table has multiple active owners (rerouted or distributed)
    pub fn is_multi_owner(&self) -> bool {
        self.owners.iter().filter(|o| !o.is_expired()).count() > 1
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
    fn test_table_owner_entry_new() {
        let entry = TableOwnerEntry::new("data-1".to_string(), 60_000);
        assert_eq!(entry.node_id, "data-1");
        assert_eq!(entry.ttl_ms, 60_000);
        assert!(!entry.is_expired());
        assert!(entry.remaining_ttl_ms() > 0);
    }

    #[test]
    fn test_table_owner_entry_expiration() {
        // Create entry with 0ms TTL (immediately expired)
        let entry = TableOwnerEntry::new("data-1".to_string(), 0);
        // Small sleep to ensure time advances past expires_at
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(entry.is_expired());
        assert_eq!(entry.remaining_ttl_ms(), 0);
    }

    #[test]
    fn test_table_owner_entry_refresh() {
        let mut entry = TableOwnerEntry::new("data-1".to_string(), 1000);
        let original_expires = entry.expires_at;

        // Small sleep to ensure time progresses
        std::thread::sleep(std::time::Duration::from_millis(10));

        entry.refresh();
        assert!(entry.expires_at > original_expires);
    }

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
        assert_eq!(meta.owners.len(), 1);
        assert_eq!(meta.owners[0].ttl_ms, DEFAULT_OWNER_TTL_MS);
    }

    #[test]
    fn test_table_meta_new_with_ttl() {
        let meta = TableMeta::new_with_ttl(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
            30_000, // 30 seconds
        );

        assert_eq!(meta.owners[0].ttl_ms, 30_000);
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
    fn test_add_owner_with_ttl() {
        let mut meta = TableMeta::new(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
        );

        meta.add_owner_with_ttl("data-2".to_string(), 120_000);
        assert_eq!(meta.owners.len(), 2);
        assert_eq!(meta.owners[1].ttl_ms, 120_000);
    }

    #[test]
    fn test_refresh_owner_ttl() {
        let mut meta = TableMeta::new(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
        );

        let original_expires = meta.owners[0].expires_at;
        std::thread::sleep(std::time::Duration::from_millis(10));

        assert!(meta.refresh_owner_ttl("data-1"));
        assert!(meta.owners[0].expires_at > original_expires);

        // Non-existent owner
        assert!(!meta.refresh_owner_ttl("data-2"));
    }

    #[test]
    fn test_active_owners() {
        let mut meta = TableMeta::new_with_ttl(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
            DEFAULT_OWNER_TTL_MS,
        );

        // Add an owner with 0ms TTL (immediately expired)
        meta.owners.push(TableOwnerEntry::new("data-2".to_string(), 0));

        // Small sleep to ensure time advances past expires_at
        std::thread::sleep(std::time::Duration::from_millis(2));

        let active: Vec<_> = meta.active_owners().collect();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].node_id, "data-1");

        let active_ids = meta.active_owner_ids();
        assert_eq!(active_ids, vec!["data-1".to_string()]);
    }

    #[test]
    fn test_cleanup_expired_owners() {
        let mut meta = TableMeta::new_with_ttl(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
            DEFAULT_OWNER_TTL_MS,
        );

        // Add expired owner
        meta.owners.push(TableOwnerEntry::new("data-2".to_string(), 0));
        assert_eq!(meta.owners.len(), 2);

        // Small sleep to ensure time advances past expires_at
        std::thread::sleep(std::time::Duration::from_millis(2));

        let removed = meta.cleanup_expired_owners();
        assert_eq!(removed, 1);
        assert_eq!(meta.owners.len(), 1);
        assert_eq!(meta.owners[0].node_id, "data-1");
    }

    #[test]
    fn test_set_primary_owner_with_ttl() {
        let mut meta = TableMeta::new(
            "mydb".to_string(),
            "default".to_string(),
            "cpu".to_string(),
            "data-1".to_string(),
        );

        meta.add_owner("data-2".to_string());
        meta.set_primary_owner_with_ttl("data-2".to_string(), 30_000);

        assert_eq!(meta.primary_owner(), Some("data-2"));
        assert_eq!(meta.owners[0].ttl_ms, 30_000);
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
        assert_eq!(decoded.owners.len(), meta.owners.len());
        assert_eq!(decoded.owners[0].node_id, meta.owners[0].node_id);
    }

    #[test]
    fn test_migrate_legacy_owners() {
        // Simulate legacy data by creating meta with empty owners but populated owner_node_ids
        let now = current_timestamp_ms();
        let mut meta = TableMeta {
            catalog: "mydb".to_string(),
            schema: "default".to_string(),
            table: "cpu".to_string(),
            owners: Vec::new(),
            owner_node_ids: vec!["data-1".to_string(), "data-2".to_string()],
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
        };

        meta.migrate_legacy_owners();

        assert_eq!(meta.owners.len(), 2);
        assert!(meta.owner_node_ids.is_empty());
        assert_eq!(meta.owners[0].node_id, "data-1");
        assert_eq!(meta.owners[1].node_id, "data-2");
    }
}
