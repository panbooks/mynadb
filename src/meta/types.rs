//! Core types for the Meta service

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Key prefixes for sled storage
pub mod keys {
    pub const PREFIX_NODE: &str = "/nodes/";
    pub const PREFIX_SCHEMA: &str = "/schemas/";
    pub const PREFIX_TABLE: &str = "/tables/";
    pub const PREFIX_FILE: &str = "/files/"; // Deprecated: use table-level routing
    pub const PREFIX_LOCK: &str = "/locks/";
    pub const PREFIX_LEADER: &str = "/leaders/";
    pub const PREFIX_EVENT: &str = "/events/";
    pub const PREFIX_LEASE: &str = "/leases/";
    pub const PREFIX_REPLICA: &str = "/replicas/";
    pub const PREFIX_DISTRIBUTION: &str = "/distribution/";
    pub const META_SEQUENCE: &str = "/meta/sequence";
    pub const META_FENCING: &str = "/meta/fencing";
}

/// Standard Kubernetes topology labels for node placement
///
/// These labels are used for zone-aware replica placement to ensure
/// leader and standby nodes are in different failure domains.
pub mod node_labels {
    /// Kubernetes availability zone label
    /// Example values: "us-east-1a", "eu-west-1b"
    pub const ZONE: &str = "topology.kubernetes.io/zone";

    /// Kubernetes region label
    /// Example values: "us-east-1", "eu-west-1"
    pub const REGION: &str = "topology.kubernetes.io/region";

    /// Custom rack/shelf label for physical server placement
    /// Example values: "rack-1", "shelf-a"
    pub const RACK: &str = "mynadb.io/rack";

    /// Node pool or instance type label
    /// Example values: "high-memory", "standard"
    pub const POOL: &str = "mynadb.io/pool";
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeRole {
    Unknown = 0,
    Meta = 1,
    Router = 2,
    Data = 3,
    Compactor = 4,
}

impl From<i32> for NodeRole {
    fn from(v: i32) -> Self {
        match v {
            1 => NodeRole::Meta,
            2 => NodeRole::Router,
            3 => NodeRole::Data,
            4 => NodeRole::Compactor,
            _ => NodeRole::Unknown,
        }
    }
}

impl From<NodeRole> for i32 {
    fn from(role: NodeRole) -> Self {
        role as i32
    }
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeState {
    Unknown = 0,
    Starting = 1,
    Active = 2,
    /// Legacy draining state for graceful shutdown (maintenance)
    Draining = 3,
    Inactive = 4,
    /// Scale-in: Node is retiring, no new writes, flushing memtable to S3
    /// Partitions being reassigned to other nodes
    Retiring = 5,
    /// Scale-in complete: Memtable empty, all partitions reassigned, safe to stop
    Drained = 6,
}

impl From<i32> for NodeState {
    fn from(v: i32) -> Self {
        match v {
            1 => NodeState::Starting,
            2 => NodeState::Active,
            3 => NodeState::Draining,
            4 => NodeState::Inactive,
            5 => NodeState::Retiring,
            6 => NodeState::Drained,
            _ => NodeState::Unknown,
        }
    }
}

impl From<NodeState> for i32 {
    fn from(state: NodeState) -> Self {
        state as i32
    }
}

/// Data node operational mode (Cold Standby Architecture)
///
/// In the Cold Standby architecture (No-Promotion design), Data nodes are either
/// Primary (actively serving traffic) or Standby (cold backup only).
///
/// Key properties:
/// - Primary nodes: Serve writes and queries, assigned as partition leaders
/// - Standby nodes: Only receive replicated data, never serve queries
/// - Roles are IMMUTABLE: Standby never promotes to Primary, Primary never demotes
/// - On Primary failure: Standby flushes cache to S3, K8s replaces the Primary pod
/// - This design eliminates split-brain risk and simplifies failover logic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum DataNodeMode {
    /// Actively serves writes and queries, assigned as partition leaders
    #[default]
    Primary = 0,
    /// Cold standby - only receives replication, flushes cache on Primary failure
    Standby = 1,
}

impl From<i32> for DataNodeMode {
    fn from(v: i32) -> Self {
        match v {
            0 => DataNodeMode::Primary,
            1 => DataNodeMode::Standby,
            _ => DataNodeMode::Primary,
        }
    }
}

impl From<DataNodeMode> for i32 {
    fn from(mode: DataNodeMode) -> Self {
        mode as i32
    }
}

impl std::fmt::Display for DataNodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataNodeMode::Primary => write!(f, "Primary"),
            DataNodeMode::Standby => write!(f, "Standby"),
        }
    }
}

/// Information about a registered node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub role: NodeRole,
    pub state: NodeState,
    pub advertise_addr: String,
    pub http_addr: Option<String>,
    pub flight_addr: Option<String>,
    pub registered_at: i64,
    pub last_heartbeat: i64,
    pub labels: HashMap<String, String>,
    /// Data node mode (only relevant for NodeRole::Data)
    ///
    /// - Primary: Actively serves writes and queries
    /// - Standby: Cold backup, only receives replication (never promoted)
    ///
    /// None for non-Data nodes (Meta, Router, Compactor).
    /// Auto-assigned by Meta based on `primary_standby_ratio` config.
    ///
    /// Note: Do NOT use skip_serializing_if here - it breaks bincode which
    /// expects a fixed structure. JSON handles it fine but bincode doesn't.
    #[serde(default)]
    pub mode: Option<DataNodeMode>,

    // ===== Node-Level Standby Pairing =====
    //
    // Each Primary is paired with a specific Standby at registration time.
    // All data from the Primary replicates to its assigned Standby.
    // This ensures complete data recovery on Primary crash.

    /// For Primary nodes: The Standby node that backs this Primary.
    ///
    /// All data written to this Primary is replicated to this Standby.
    /// On Primary crash, this Standby has a complete copy of the data.
    /// None if no Standby is available yet (queued for pairing).
    #[serde(default)]
    pub backing_standby: Option<String>,

    /// For Standby nodes: List of Primary nodes this Standby backs.
    ///
    /// Based on the primary_standby_ratio (e.g., 2:1 means 2 Primaries per Standby).
    /// This Standby receives replication from all listed Primaries.
    /// Empty for Primary nodes.
    #[serde(default)]
    pub backed_primaries: Vec<String>,
}

impl NodeInfo {
    pub fn new(node_id: String, role: NodeRole, advertise_addr: String) -> Self {
        let now = current_timestamp_ms();
        Self {
            node_id,
            role,
            // Nodes are ready to serve traffic immediately after registration
            state: NodeState::Active,
            advertise_addr,
            http_addr: None,
            flight_addr: None,
            registered_at: now,
            last_heartbeat: now,
            labels: HashMap::new(),
            mode: None,              // Auto-assigned by Meta for Data nodes
            backing_standby: None,   // Assigned when Standby becomes available
            backed_primaries: vec![], // Populated when Standby is assigned Primaries
        }
    }

    pub fn with_http(mut self, addr: String) -> Self {
        self.http_addr = Some(addr);
        self
    }

    pub fn with_flight(mut self, addr: String) -> Self {
        self.flight_addr = Some(addr);
        self
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    /// Set the data node mode (Primary or Standby)
    ///
    /// Only meaningful for Data nodes. Typically auto-assigned by Meta
    /// based on the cluster's `primary_standby_ratio` configuration.
    pub fn with_mode(mut self, mode: DataNodeMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Check if this is a Primary data node
    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Data && self.mode == Some(DataNodeMode::Primary)
    }

    /// Check if this is a Standby data node
    pub fn is_standby(&self) -> bool {
        self.role == NodeRole::Data && self.mode == Some(DataNodeMode::Standby)
    }

    /// Get the availability zone label if present
    pub fn zone(&self) -> Option<&str> {
        self.labels.get(node_labels::ZONE).map(|s| s.as_str())
    }

    /// Get the region label if present
    pub fn region(&self) -> Option<&str> {
        self.labels.get(node_labels::REGION).map(|s| s.as_str())
    }

    /// Get the rack label if present
    pub fn rack(&self) -> Option<&str> {
        self.labels.get(node_labels::RACK).map(|s| s.as_str())
    }

    /// Get the pool label if present
    pub fn pool(&self) -> Option<&str> {
        self.labels.get(node_labels::POOL).map(|s| s.as_str())
    }

    /// Check if this node is in a different zone than another node
    ///
    /// Returns true if both nodes have zone labels and they differ.
    /// Returns false if either node lacks a zone label (can't determine).
    pub fn is_different_zone(&self, other: &NodeInfo) -> bool {
        match (self.zone(), other.zone()) {
            (Some(z1), Some(z2)) => z1 != z2,
            _ => false,
        }
    }

    /// Check if this node is in a different rack than another node
    pub fn is_different_rack(&self, other: &NodeInfo) -> bool {
        match (self.rack(), other.rack()) {
            (Some(r1), Some(r2)) => r1 != r2,
            _ => false,
        }
    }

    /// Check if this node is in a different region than another node
    pub fn is_different_region(&self, other: &NodeInfo) -> bool {
        match (self.region(), other.region()) {
            (Some(r1), Some(r2)) => r1 != r2,
            _ => false,
        }
    }

    /// Check if this node is in any different failure domain than another node
    ///
    /// Checks (in priority order): zone, rack
    /// Returns true if any difference is found.
    pub fn is_different_failure_domain(&self, other: &NodeInfo) -> bool {
        self.is_different_zone(other) || self.is_different_rack(other)
    }
}

/// File metadata in the registry
/// DEPRECATED: Use TableMeta for routing. File lists should be derived from S3 listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub partition: String,
    pub level: u8,
    pub size_bytes: u64,
    pub row_count: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub created_at: i64,
    pub created_by: String,
}

impl FileInfo {
    /// Generate the storage key for this file
    /// DEPRECATED: Use TableMeta for routing
    pub fn storage_key(&self) -> String {
        format!(
            "{}{}/{}/{}/{}/{}",
            keys::PREFIX_FILE,
            self.catalog,
            self.schema,
            self.table,
            self.partition,
            self.path.split('/').last().unwrap_or(&self.path)
        )
    }

    /// Generate a prefix key for listing files in a partition
    /// DEPRECATED: Use S3 listing instead
    pub fn partition_prefix(catalog: &str, schema: &str, table: &str, partition: &str) -> String {
        format!(
            "{}{}/{}/{}/{}/",
            keys::PREFIX_FILE,
            catalog,
            schema,
            table,
            partition
        )
    }

    /// Generate a prefix key for listing files in a table
    /// DEPRECATED: Use S3 listing instead
    pub fn table_prefix(catalog: &str, schema: &str, table: &str) -> String {
        format!("{}{}/{}/{}/", keys::PREFIX_FILE, catalog, schema, table)
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

// ============================================================================
// Write Distribution Configuration
// ============================================================================

/// Scope level for distribution configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DistributionScope {
    /// Apply to all tables in a catalog
    Catalog = 1,
    /// Apply to all tables in a schema
    Schema = 2,
    /// Apply to a specific table
    Table = 3,
}

impl From<i32> for DistributionScope {
    fn from(v: i32) -> Self {
        match v {
            1 => DistributionScope::Catalog,
            2 => DistributionScope::Schema,
            3 => DistributionScope::Table,
            _ => DistributionScope::Table,
        }
    }
}

impl From<DistributionScope> for i32 {
    fn from(scope: DistributionScope) -> Self {
        scope as i32
    }
}

/// Write distribution configuration
///
/// Allows distributing writes for a table across multiple primary nodes
/// for better load balancing on high-throughput tables.
///
/// Configuration is hierarchical: table > schema > catalog
/// The most specific matching config wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionConfig {
    /// Catalog name (required)
    pub catalog: String,
    /// Schema name (optional - if empty, applies to all schemas in catalog)
    pub schema: String,
    /// Table name (optional - if empty, applies to all tables in schema)
    pub table: String,
    /// Scope level (derived from catalog/schema/table presence)
    pub scope: DistributionScope,
    /// Distribution percentage [0-100]
    /// - 0: No distribution, single node (default behavior)
    /// - 50: Distribute across 50% of primary nodes
    /// - 100: Distribute across ALL primary nodes
    pub percent: u8,
    /// When config was created
    pub created_at: i64,
    /// When config was last modified
    pub last_modified: i64,
    /// Version for cache invalidation
    pub version: u64,
}

impl DistributionConfig {
    /// Create a new distribution config
    pub fn new(catalog: String, schema: String, table: String, percent: u8) -> Self {
        let scope = if !table.is_empty() {
            DistributionScope::Table
        } else if !schema.is_empty() {
            DistributionScope::Schema
        } else {
            DistributionScope::Catalog
        };
        let now = current_timestamp_ms();
        Self {
            catalog,
            schema,
            table,
            scope,
            percent: percent.min(100),
            created_at: now,
            last_modified: now,
            version: 1,
        }
    }

    /// Generate storage key for this config
    pub fn storage_key(&self) -> String {
        Self::key_for(&self.catalog, &self.schema, &self.table)
    }

    /// Generate storage key for a distribution config
    pub fn key_for(catalog: &str, schema: &str, table: &str) -> String {
        if !table.is_empty() {
            format!("{}{}/{}/{}", keys::PREFIX_DISTRIBUTION, catalog, schema, table)
        } else if !schema.is_empty() {
            format!("{}{}/{}", keys::PREFIX_DISTRIBUTION, catalog, schema)
        } else {
            format!("{}{}", keys::PREFIX_DISTRIBUTION, catalog)
        }
    }

    /// Generate prefix for listing all distribution configs
    pub fn all_prefix() -> String {
        keys::PREFIX_DISTRIBUTION.to_string()
    }

    /// Check if this config is more specific than another
    pub fn is_more_specific_than(&self, other: &Self) -> bool {
        (self.scope as u8) > (other.scope as u8)
    }

    /// Check if this config applies to the given table
    pub fn applies_to(&self, catalog: &str, schema: &str, table: &str) -> bool {
        if self.catalog != catalog {
            return false;
        }
        match self.scope {
            DistributionScope::Catalog => true,
            DistributionScope::Schema => self.schema == schema,
            DistributionScope::Table => self.schema == schema && self.table == table,
        }
    }

    /// Calculate number of nodes to use based on percentage and total primary nodes
    pub fn nodes_to_use(&self, total_primary_nodes: usize) -> usize {
        if self.percent == 0 || total_primary_nodes == 0 {
            1 // Single node (default)
        } else if self.percent >= 100 {
            total_primary_nodes
        } else {
            let count = (total_primary_nodes as u64 * self.percent as u64 / 100).max(1) as usize;
            count.min(total_primary_nodes)
        }
    }
}

/// Lock information with fencing token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    pub key: String,
    pub holder: String,
    pub fencing_token: u64,
    pub acquired_at: i64,
    pub expires_at: i64,
}

impl LockInfo {
    pub fn new(key: String, holder: String, fencing_token: u64, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            key,
            holder,
            fencing_token,
            acquired_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_LOCK, self.key)
    }
}

/// Leader election information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub election: String,
    pub leader: String,
    pub term: u64,
    pub acquired_at: i64,
    pub expires_at: i64,
}

impl LeaderInfo {
    pub fn new(election: String, leader: String, term: u64, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            election,
            leader,
            term,
            acquired_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_LEADER, self.election)
    }
}

/// Node lease for auto-deregistration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub lease_id: u64,
    pub node_id: String,
    pub ttl_ms: u64,
    pub created_at: i64,
    pub expires_at: i64,
}

impl LeaseInfo {
    pub fn new(lease_id: u64, node_id: String, ttl_ms: u64) -> Self {
        let now = current_timestamp_ms();
        Self {
            lease_id,
            node_id,
            ttl_ms,
            created_at: now,
            expires_at: now + ttl_ms as i64,
        }
    }

    pub fn renew(&mut self) {
        let now = current_timestamp_ms();
        self.expires_at = now + self.ttl_ms as i64;
    }

    pub fn is_expired(&self) -> bool {
        current_timestamp_ms() > self.expires_at
    }

    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_LEASE, self.lease_id)
    }
}

/// Replica state for warm standby replication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ReplicaState {
    /// Standby is fully caught up with leader
    InSync = 0,
    /// Standby is behind, receiving data
    CatchingUp = 1,
    /// Standby is unreachable
    Offline = 2,
}

impl From<i32> for ReplicaState {
    fn from(v: i32) -> Self {
        match v {
            0 => ReplicaState::InSync,
            1 => ReplicaState::CatchingUp,
            2 => ReplicaState::Offline,
            _ => ReplicaState::Offline,
        }
    }
}

impl From<ReplicaState> for i32 {
    fn from(state: ReplicaState) -> Self {
        state as i32
    }
}

/// Partition replica information for warm standby replication
///
/// Each partition has a leader node that handles writes and flushes to Parquet,
/// and one or more standby nodes that maintain in-memory copies for instant failover.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionReplica {
    /// Partition key: "catalog/schema/table/date=YYYY-MM-DD"
    pub partition_key: String,
    /// Current leader node ID (handles writes, flushes to Parquet)
    pub leader_node_id: String,
    /// Standby node IDs (maintain memtable copies, no flush)
    pub standby_node_ids: Vec<String>,
    /// Replication factor (1 = no standby, 2 = 1 standby, 3 = 2 standbys)
    pub replication_factor: u8,
    /// Last sequence number replicated to standbys
    pub last_replicated_seq: u64,
    /// Replication lag in milliseconds
    pub replication_lag_ms: u64,
    /// Current replica state
    pub state: ReplicaState,
    /// When this replica assignment was created
    pub created_at: i64,
    /// When leader was last confirmed healthy
    pub leader_last_seen: i64,
}

impl PartitionReplica {
    pub fn new(
        partition_key: String,
        leader_node_id: String,
        standby_node_ids: Vec<String>,
        replication_factor: u8,
    ) -> Self {
        let now = current_timestamp_ms();
        Self {
            partition_key,
            leader_node_id,
            standby_node_ids,
            replication_factor,
            last_replicated_seq: 0,
            replication_lag_ms: 0,
            state: ReplicaState::CatchingUp,
            created_at: now,
            leader_last_seen: now,
        }
    }

    /// Generate storage key for this partition replica
    pub fn storage_key(&self) -> String {
        format!("{}{}", keys::PREFIX_REPLICA, self.partition_key)
    }

    /// Generate prefix for listing all replicas
    pub fn prefix() -> String {
        keys::PREFIX_REPLICA.to_string()
    }

    /// Check if this partition has a healthy standby for failover
    pub fn has_healthy_standby(&self) -> bool {
        !self.standby_node_ids.is_empty() && self.state == ReplicaState::InSync
    }

    /// Get the first standby for failover (if any)
    pub fn first_standby(&self) -> Option<&String> {
        self.standby_node_ids.first()
    }

    // Note: promote_standby() removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement
}

/// Status of a node retirement (scale-in) operation
///
/// Tracks the progress of retiring a node from the cluster.
/// The node goes through: Active → Retiring → Drained → Stopped
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetireStatus {
    /// Node ID being retired
    pub node_id: String,
    /// Current state of the node
    pub state: NodeState,
    /// When retirement started (Unix timestamp ms)
    pub started_at: i64,
    /// Total partitions this node was leading
    pub partitions_total: usize,
    /// Number of partitions already reassigned to other nodes
    pub partitions_reassigned: usize,
    /// Remaining rows in memtable (0 when fully flushed to S3)
    pub memtable_rows_remaining: u64,
    /// Estimated seconds until retirement completes
    pub estimated_completion_secs: u64,
}

impl RetireStatus {
    pub fn new(node_id: String, partitions_total: usize) -> Self {
        Self {
            node_id,
            state: NodeState::Retiring,
            started_at: current_timestamp_ms(),
            partitions_total,
            partitions_reassigned: 0,
            memtable_rows_remaining: 0,
            estimated_completion_secs: 0,
        }
    }

    /// Check if retirement is complete (all partitions reassigned, memtable empty)
    pub fn is_complete(&self) -> bool {
        self.partitions_reassigned >= self.partitions_total && self.memtable_rows_remaining == 0
    }

    /// Calculate progress percentage (0-100)
    pub fn progress_percent(&self) -> u8 {
        if self.partitions_total == 0 {
            100
        } else {
            ((self.partitions_reassigned as f64 / self.partitions_total as f64) * 100.0) as u8
        }
    }
}

/// Request to scale in (shrink) the cluster
///
/// Supports two modes:
/// 1. Explicit: Specify exact nodes to retire
/// 2. Auto-select: Specify counts, let Meta choose nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInRequest {
    /// Explicit list of node IDs to retire
    #[serde(default)]
    pub nodes: Vec<String>,
    /// Number of Primary nodes to reduce (Meta auto-selects)
    #[serde(default)]
    pub reduce_primaries: Option<u32>,
    /// Number of Standby nodes to reduce (Meta auto-selects)
    #[serde(default)]
    pub reduce_standbys: Option<u32>,
}

impl ScaleInRequest {
    /// Create a request to retire specific nodes
    pub fn with_nodes(nodes: Vec<String>) -> Self {
        Self {
            nodes,
            reduce_primaries: None,
            reduce_standbys: None,
        }
    }

    /// Create a request to auto-select nodes to retire
    pub fn with_counts(reduce_primaries: u32, reduce_standbys: u32) -> Self {
        Self {
            nodes: Vec::new(),
            reduce_primaries: Some(reduce_primaries),
            reduce_standbys: Some(reduce_standbys),
        }
    }

    /// Check if this is an explicit node list request
    pub fn is_explicit(&self) -> bool {
        !self.nodes.is_empty()
    }
}

/// Recommendation for which nodes to scale in
///
/// Returned by the /scale-in/recommend endpoint.
/// Operator can review and approve before executing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInRecommendation {
    /// Recommended Primary nodes to retire
    pub recommended_primaries: Vec<String>,
    /// Recommended Standby nodes to retire
    pub recommended_standbys: Vec<String>,
    /// Reasons for each recommendation (node_id -> reason)
    pub reasons: HashMap<String, String>,
    /// Warnings about potential issues
    pub warnings: Vec<String>,
}

impl ScaleInRecommendation {
    pub fn new() -> Self {
        Self {
            recommended_primaries: Vec::new(),
            recommended_standbys: Vec::new(),
            reasons: HashMap::new(),
            warnings: Vec::new(),
        }
    }

    /// Add a Primary node recommendation
    pub fn add_primary(&mut self, node_id: String, reason: String) {
        self.recommended_primaries.push(node_id.clone());
        self.reasons.insert(node_id, reason);
    }

    /// Add a Standby node recommendation
    pub fn add_standby(&mut self, node_id: String, reason: String) {
        self.recommended_standbys.push(node_id.clone());
        self.reasons.insert(node_id, reason);
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Total nodes recommended for retirement
    pub fn total_nodes(&self) -> usize {
        self.recommended_primaries.len() + self.recommended_standbys.len()
    }
}

impl Default for ScaleInRecommendation {
    fn default() -> Self {
        Self::new()
    }
}

/// Event types for the event log
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventType {
    Unknown = 0,
    NodeJoined = 1,
    NodeLeft = 2,
    NodeStateChanged = 3,
    FilesRegistered = 4,
    FilesRemoved = 5,
    LeaderChanged = 6,
    CacheInvalidate = 7,
    SchemaChanged = 8,
    /// Partition replica assigned (leader + standbys)
    ReplicaAssigned = 9,
    /// Replica leader changed (reassignment, not promotion)
    /// Note: In No-Promotion architecture, this is only for partition reassignment
    ReplicaPromoted = 10, // Kept for backward compatibility, but no longer used for promotion
    /// Replica state changed (InSync/CatchingUp/Offline)
    ReplicaStateChanged = 11,
    /// Data node mode changed (Cold Standby Architecture)
    /// Note: In No-Promotion architecture, mode changes only happen at registration
    DataNodeModeChanged = 12,

    /// Standby needs to flush data for crashed Primary (Recovery)
    ///
    /// Published when a Primary crashes and its Standby needs to flush replicated data.
    /// Payload: {"standby_id": "...", "crashed_primary_id": "..."}
    FlushForRecovery = 13,

    /// Table ownership added (for rerouted writes)
    ///
    /// Published when a secondary owner is added to a table.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "added_owner": "...", "all_owners": [...]}
    TableOwnerAdded = 14,

    /// Table ownership removed
    ///
    /// Published when an owner is removed from a table.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "removed_owner": "...", "all_owners": [...]}
    TableOwnerRemoved = 15,

    /// Table primary owner reassigned
    ///
    /// Published when a table's primary owner changes.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "old_primary": "...", "new_primary": "...", "all_owners": [...]}
    TableReassigned = 16,

    /// Distribution config changed
    ///
    /// Published when write distribution config is set or deleted.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "percent": N, "deleted": bool}
    DistributionConfigChanged = 17,
}

impl From<i32> for EventType {
    fn from(v: i32) -> Self {
        match v {
            1 => EventType::NodeJoined,
            2 => EventType::NodeLeft,
            3 => EventType::NodeStateChanged,
            4 => EventType::FilesRegistered,
            5 => EventType::FilesRemoved,
            6 => EventType::LeaderChanged,
            7 => EventType::CacheInvalidate,
            8 => EventType::SchemaChanged,
            9 => EventType::ReplicaAssigned,
            10 => EventType::ReplicaPromoted,
            11 => EventType::ReplicaStateChanged,
            12 => EventType::DataNodeModeChanged,
            13 => EventType::FlushForRecovery,
            14 => EventType::TableOwnerAdded,
            15 => EventType::TableOwnerRemoved,
            16 => EventType::TableReassigned,
            _ => EventType::Unknown,
        }
    }
}

impl From<EventType> for i32 {
    fn from(et: EventType) -> Self {
        et as i32
    }
}

/// Event in the event log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub sequence: u64,
    pub event_type: EventType,
    pub topic: String,
    pub payload: Vec<u8>,
    pub timestamp: i64,
    pub source_node: String,
}

impl Event {
    pub fn new(
        sequence: u64,
        event_type: EventType,
        topic: String,
        payload: Vec<u8>,
        source_node: String,
    ) -> Self {
        Self {
            sequence,
            event_type,
            topic,
            payload,
            timestamp: current_timestamp_ms(),
            source_node,
        }
    }

    pub fn storage_key(&self) -> String {
        // Use zero-padded sequence for proper ordering
        format!("{}{:020}", keys::PREFIX_EVENT, self.sequence)
    }
}

/// Commands that can be applied to the Raft state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    // Node operations
    RegisterNode {
        info: NodeInfo,
        lease_id: u64,
        lease_ttl_ms: u64,
    },
    Heartbeat {
        node_id: String,
        lease_id: u64,
        timestamp: i64,
    },
    DeregisterNode {
        node_id: String,
    },
    UpdateNodeState {
        node_id: String,
        state: NodeState,
    },

    // ========================================================================
    // Table-Level Operations (Recommended for routing)
    // ========================================================================

    /// Register a new table with its owner node
    ///
    /// Called when a Data node first receives data for a new table.
    /// The node that first writes to a table becomes its owner.
    RegisterTable {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
    },

    /// Add an additional owner to a table (for rerouted writes)
    ///
    /// Called when a write is rerouted to a fallback node after primary failure.
    /// The new node becomes a secondary owner.
    AddTableOwner {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
    },

    /// Remove an owner from a table
    ///
    /// Called when a node leaves the cluster or during ownership consolidation.
    RemoveTableOwner {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
    },

    /// Update table statistics after flush
    ///
    /// Called periodically (not per-flush) to sync aggregated stats.
    /// Data nodes batch stats locally and sync every few minutes.
    UpdateTableStats {
        catalog: String,
        schema: String,
        table: String,
        delta: TableStatsDelta,
    },

    /// Reassign a table to a different owner node
    ///
    /// Used for:
    /// - Load balancing (move hot tables)
    /// - Node failure recovery
    /// - Manual rebalancing
    ReassignTable {
        catalog: String,
        schema: String,
        table: String,
        new_owner_node_id: String,
    },

    /// Remove a table from the registry
    ///
    /// Called when a table is dropped or all data is deleted.
    RemoveTable {
        catalog: String,
        schema: String,
        table: String,
    },

    // ========================================================================
    // Write Distribution Configuration
    // ========================================================================

    /// Set distribution config for catalog/schema/table
    ///
    /// Configures how writes are distributed across primary nodes.
    /// percent=0 means single node (default), percent=100 means all nodes.
    SetDistributionConfig {
        catalog: String,
        schema: String,
        table: String,
        percent: u8,
    },

    /// Delete distribution config
    ///
    /// Removes distribution config, reverting to default (single node).
    DeleteDistributionConfig {
        catalog: String,
        schema: String,
        table: String,
    },

    // Lock operations
    AcquireLock {
        key: String,
        holder: String,
        ttl_ms: u64,
    },
    ReleaseLock {
        key: String,
        holder: String,
        fencing_token: u64,
    },
    RenewLock {
        key: String,
        holder: String,
        fencing_token: u64,
        ttl_ms: u64,
    },

    // Leader election
    Campaign {
        election: String,
        node_id: String,
        ttl_ms: u64,
    },
    Resign {
        election: String,
        node_id: String,
        term: u64,
    },

    // Events
    PublishEvent {
        topic: String,
        event_type: EventType,
        payload: Vec<u8>,
        source_node: String,
    },

    // Lease management
    ExpireLeases,

    // Partition replica operations (warm standby)
    AssignReplica {
        partition_key: String,
        leader_node_id: String,
        standby_node_ids: Vec<String>,
        replication_factor: u8,
    },
    // PromoteStandby removed - using No-Promotion architecture
    UpdateReplicaState {
        partition_key: String,
        state: ReplicaState,
        last_replicated_seq: u64,
        replication_lag_ms: u64,
    },
    RemoveReplica {
        partition_key: String,
    },

    /// Auto-assign replica for a partition using ReplicaAssigner
    ///
    /// Called when a new partition is first written to.
    /// Meta will automatically select leader and standbys based on:
    /// - Load balancing (least-loaded node as leader)
    /// - Zone-awareness (standbys in different zones)
    /// - Replication factor from config
    AutoAssignReplica {
        partition_key: String,
    },

    /// Rebalance replicas after a node joins
    ///
    /// Adds missing standbys to under-replicated partitions.
    RebalanceOnNodeJoin {
        node_id: String,
    },

    // PromoteDataNode removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    // UpdateDataNodeMode removed - No-Promotion architecture
    // Mode is set at registration time and never changes dynamically

    // Scale-in (retirement) operations
    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins partition reassignment to other nodes
    /// - Node continues serving existing queries until Drained
    StartRetire {
        node_id: String,
    },

    /// Mark a node as fully drained (scale-in complete)
    ///
    /// Called when:
    /// - All partitions have been reassigned
    /// - Memtable has been flushed to S3
    /// - Node is safe to stop
    CompleteRetire {
        node_id: String,
    },

    /// Reassign a partition from a retiring node to another Primary
    ///
    /// For Object Store clusters:
    /// - New leader takes over writes for this partition
    /// - Old data remains in S3 at the retiring node's prefix
    /// - New leader can read old data from S3 (any node can read any prefix)
    ReassignPartitionForRetire {
        partition_key: String,
        from_node_id: String,
        to_node_id: String,
    },

    /// Cancel a node retirement (scale-in abort)
    ///
    /// Restores the node to Active state.
    /// Only works if retirement hasn't completed.
    CancelRetire {
        node_id: String,
    },

    /// Update retirement status (internal)
    ///
    /// Updates progress information during retirement.
    UpdateRetireStatus {
        node_id: String,
        partitions_reassigned: usize,
        memtable_rows_remaining: u64,
    },

    /// Remove a standby from partition replicas (for Standby scale-in)
    ///
    /// When scaling in a Standby node:
    /// - Remove it from all partition's standby_node_ids
    /// - Stop replication streams to this node
    RemoveStandbyFromReplicas {
        standby_node_id: String,
    },

    // Recovery operations (failover without promotion)

    /// Trigger Standby to flush a crashed Primary's replicated data
    ///
    /// When a Primary crashes (No-Promotion architecture):
    /// 1. Find Standby backing the crashed Primary
    /// 2. Standby flushes all replicated memtable data to Parquet
    /// 3. This ensures no data loss before partition redistribution
    TriggerStandbyFlush {
        standby_id: String,
        crashed_primary_id: String,
    },

    /// Assign a spare Standby to back specific Primaries
    ///
    /// When an active Standby crashes:
    /// 1. Find spare Standby from pool
    /// 2. Assign it to back the affected Primaries
    /// 3. Start replication from Primaries to new Standby
    AssignStandbyToPrimaries {
        standby_id: String,
        primary_ids: Vec<String>,
    },

    /// Mark a partition as unprotected (no Standby backup)
    ///
    /// Used when:
    /// - A Standby crashes and no spare is available
    /// - Primary has no replication target
    /// - Emits warning based on unprotected_write_policy
    MarkPartitionUnprotected {
        partition_key: String,
        primary_id: String,
        reason: String,
    },

    /// Redistribute partitions from a crashed Primary to remaining Primaries
    ///
    /// Called during Primary crash recovery (No-Promotion architecture):
    /// 1. Get all partitions owned by crashed Primary
    /// 2. Distribute evenly to remaining active Primaries
    /// 3. Update replica assignments
    RedistributePartitions {
        crashed_primary_id: String,
        target_primaries: Vec<String>,
    },
}

/// Response from applying a MetaCommand
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaResponse {
    Success,
    NodeRegistered {
        lease_id: u64,
    },

    // ========================================================================
    // Table-Level Responses
    // ========================================================================

    /// Table registered successfully
    TableRegistered {
        catalog: String,
        schema: String,
        table: String,
        owner_node_id: String,
        version: u64,
    },

    /// Table already exists (idempotent registration)
    TableAlreadyExists {
        catalog: String,
        schema: String,
        table: String,
        current_owner: String,
        /// All owner nodes (for multi-node tables)
        all_owners: Vec<String>,
    },

    /// Table stats updated successfully
    TableStatsUpdated {
        catalog: String,
        schema: String,
        table: String,
        version: u64,
    },

    /// Table reassigned to new primary owner
    TableReassigned {
        catalog: String,
        schema: String,
        table: String,
        old_owner: String,
        new_owner: String,
        /// All owner nodes after reassignment
        all_owners: Vec<String>,
        version: u64,
    },

    /// Additional owner added to table (for rerouted writes)
    TableOwnerAdded {
        catalog: String,
        schema: String,
        table: String,
        new_owner: String,
        /// All owner nodes after addition
        all_owners: Vec<String>,
        version: u64,
    },

    /// Owner removed from table
    TableOwnerRemoved {
        catalog: String,
        schema: String,
        table: String,
        removed_owner: String,
        /// Remaining owner nodes
        all_owners: Vec<String>,
        version: u64,
    },

    /// Node is already an owner of the table
    TableOwnerExists {
        catalog: String,
        schema: String,
        table: String,
        node_id: String,
        all_owners: Vec<String>,
    },

    /// Table removed successfully
    TableRemoved {
        catalog: String,
        schema: String,
        table: String,
    },

    /// Table not found
    TableNotFound {
        catalog: String,
        schema: String,
        table: String,
    },

    // ========================================================================
    // Distribution Config Responses
    // ========================================================================

    /// Distribution config set successfully
    DistributionConfigSet {
        catalog: String,
        schema: String,
        table: String,
        percent: u8,
        version: u64,
    },

    /// Distribution config deleted successfully
    DistributionConfigDeleted {
        catalog: String,
        schema: String,
        table: String,
    },

    /// Distribution config not found (when deleting)
    DistributionConfigNotFound {
        catalog: String,
        schema: String,
        table: String,
    },

    // ========================================================================
    // Lock Responses
    // ========================================================================

    LockAcquired {
        fencing_token: u64,
    },
    LockNotAcquired {
        current_holder: String,
        expires_at: i64,
    },
    LockReleased,
    LockRenewed {
        expires_at: i64,
    },
    LeaderElected {
        term: u64,
    },
    LeaderExists {
        current_leader: String,
        term: u64,
    },
    EventPublished {
        sequence: u64,
    },
    ReplicaAssigned {
        partition_key: String,
        leader_node_id: String,
    },
    // StandbyPromoted removed - using No-Promotion architecture
    ReplicaUpdated {
        partition_key: String,
    },
    ReplicaRemoved {
        partition_key: String,
    },
    ReplicaNotFound {
        partition_key: String,
    },
    // NoStandbyAvailable removed - using No-Promotion architecture
    ReplicaAutoAssigned {
        partition_key: String,
        leader_node_id: String,
        standby_node_ids: Vec<String>,
        replication_factor: u8,
    },
    ReplicaAlreadyExists {
        partition_key: String,
    },
    NoDataNodesAvailable,
    RebalanceCompleted {
        updated_count: usize,
    },
    // DataNodePromoted removed - using No-Promotion architecture
    // DataNodeModeUpdated removed - No-Promotion architecture (mode set at registration)
    /// Data node not found
    DataNodeNotFound {
        node_id: String,
    },

    // Scale-in (retirement) responses
    /// Node retirement started successfully
    RetireStarted {
        node_id: String,
        partitions_to_reassign: usize,
    },
    /// Node retirement completed, safe to stop
    RetireCompleted {
        node_id: String,
    },
    /// Node retirement cancelled, restored to Active
    RetireCancelled {
        node_id: String,
    },
    /// Retirement status updated
    RetireStatusUpdated {
        node_id: String,
        progress_percent: u8,
    },
    /// Partition reassigned from retiring node
    PartitionReassigned {
        partition_key: String,
        from_node_id: String,
        to_node_id: String,
    },
    /// Cannot retire node - no other nodes to take partitions
    CannotRetireNoAlternative {
        node_id: String,
        reason: String,
    },
    /// Node is not in a retirable state
    NodeNotRetirable {
        node_id: String,
        current_state: NodeState,
    },
    /// Standby removed from all replicas
    StandbyRemovedFromReplicas {
        standby_node_id: String,
        partitions_affected: usize,
    },
    /// Scale-in recommendation generated
    ScaleInRecommended {
        recommendation: ScaleInRecommendation,
    },

    // Recovery responses

    /// Standby flush triggered for crashed Primary
    StandbyFlushTriggered {
        standby_id: String,
        crashed_primary_id: String,
    },

    /// Spare Standby assigned to Primaries
    StandbyAssignedToPrimaries {
        standby_id: String,
        primary_ids: Vec<String>,
    },

    /// Partition marked as unprotected (no Standby)
    PartitionMarkedUnprotected {
        partition_key: String,
        primary_id: String,
    },

    /// Partitions redistributed from crashed Primary
    PartitionsRedistributed {
        crashed_primary_id: String,
        partitions_count: usize,
        target_primaries: Vec<String>,
    },

    /// No spare Standby available
    NoSpareStandbyAvailable,

    Error {
        message: String,
    },
}

/// Get current timestamp in milliseconds
pub fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    mod distribution_config_tests {
        use super::*;

        #[test]
        fn test_distribution_config_new_catalog_scope() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "".to_string(),
                "".to_string(),
                100,
            );

            assert_eq!(config.catalog, "mydb");
            assert_eq!(config.schema, "");
            assert_eq!(config.table, "");
            assert_eq!(config.scope, DistributionScope::Catalog);
            assert_eq!(config.percent, 100);
        }

        #[test]
        fn test_distribution_config_new_schema_scope() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "".to_string(),
                50,
            );

            assert_eq!(config.catalog, "mydb");
            assert_eq!(config.schema, "metrics");
            assert_eq!(config.table, "");
            assert_eq!(config.scope, DistributionScope::Schema);
            assert_eq!(config.percent, 50);
        }

        #[test]
        fn test_distribution_config_new_table_scope() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                0,
            );

            assert_eq!(config.catalog, "mydb");
            assert_eq!(config.schema, "metrics");
            assert_eq!(config.table, "cpu");
            assert_eq!(config.scope, DistributionScope::Table);
            assert_eq!(config.percent, 0);
        }

        #[test]
        fn test_distribution_config_storage_key_catalog() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "".to_string(),
                "".to_string(),
                100,
            );

            // Catalog-level: /distribution/catalog
            assert_eq!(config.storage_key(), "/distribution/mydb");
        }

        #[test]
        fn test_distribution_config_storage_key_schema() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "".to_string(),
                50,
            );

            // Schema-level: /distribution/catalog/schema
            assert_eq!(config.storage_key(), "/distribution/mydb/metrics");
        }

        #[test]
        fn test_distribution_config_storage_key_table() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                100,
            );

            // Table-level: /distribution/catalog/schema/table
            assert_eq!(config.storage_key(), "/distribution/mydb/metrics/cpu");
        }

        #[test]
        fn test_distribution_config_key_for() {
            // key_for generates the same keys as storage_key
            assert_eq!(
                DistributionConfig::key_for("mydb", "", ""),
                "/distribution/mydb"
            );
            assert_eq!(
                DistributionConfig::key_for("mydb", "metrics", ""),
                "/distribution/mydb/metrics"
            );
            assert_eq!(
                DistributionConfig::key_for("mydb", "metrics", "cpu"),
                "/distribution/mydb/metrics/cpu"
            );
        }

        #[test]
        fn test_distribution_config_all_prefix() {
            assert_eq!(DistributionConfig::all_prefix(), "/distribution/");
        }

        #[test]
        fn test_distribution_scope_ordering() {
            // Catalog < Schema < Table (more specific is higher)
            assert!((DistributionScope::Catalog as u8) < (DistributionScope::Schema as u8));
            assert!((DistributionScope::Schema as u8) < (DistributionScope::Table as u8));
        }

        #[test]
        fn test_distribution_config_serialization() {
            let config = DistributionConfig::new(
                "mydb".to_string(),
                "metrics".to_string(),
                "cpu".to_string(),
                75,
            );

            // Test bincode serialization (used in sled storage)
            let encoded = bincode::serialize(&config).unwrap();
            let decoded: DistributionConfig = bincode::deserialize(&encoded).unwrap();

            assert_eq!(decoded.catalog, config.catalog);
            assert_eq!(decoded.schema, config.schema);
            assert_eq!(decoded.table, config.table);
            assert_eq!(decoded.scope, config.scope);
            assert_eq!(decoded.percent, config.percent);
        }

        #[test]
        fn test_distribution_config_percent_boundaries() {
            // Test 0% (single node)
            let config_0 = DistributionConfig::new(
                "db".to_string(),
                "".to_string(),
                "".to_string(),
                0,
            );
            assert_eq!(config_0.percent, 0);

            // Test 100% (all nodes)
            let config_100 = DistributionConfig::new(
                "db".to_string(),
                "".to_string(),
                "".to_string(),
                100,
            );
            assert_eq!(config_100.percent, 100);

            // Test mid-range
            let config_50 = DistributionConfig::new(
                "db".to_string(),
                "".to_string(),
                "".to_string(),
                50,
            );
            assert_eq!(config_50.percent, 50);
        }
    }
}
