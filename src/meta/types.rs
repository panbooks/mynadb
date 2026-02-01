//! Core types for the Meta service

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Key prefixes for sled storage
pub mod keys {
    pub const PREFIX_NODE: &str = "/nodes/";
    pub const PREFIX_SCHEMA: &str = "/schemas/";
    pub const PREFIX_FILE: &str = "/files/";
    pub const PREFIX_LOCK: &str = "/locks/";
    pub const PREFIX_LEADER: &str = "/leaders/";
    pub const PREFIX_EVENT: &str = "/events/";
    pub const PREFIX_LEASE: &str = "/leases/";
    pub const PREFIX_REPLICA: &str = "/replicas/";
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
/// In the Cold Standby architecture, Data nodes are either Primary (actively
/// serving traffic) or Standby (cold backup only). This prevents avalanche
/// failures: when a Primary fails, a dedicated Standby takes over completely
/// instead of adding load to other Primaries.
///
/// Key properties:
/// - Primary nodes: Serve writes and queries, assigned as partition leaders
/// - Standby nodes: Only receive replicated data, never serve queries until promoted
/// - On failover: Standby becomes Primary (no extra load on other Primaries)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum DataNodeMode {
    /// Actively serves writes and queries, assigned as partition leaders
    #[default]
    Primary = 0,
    /// Cold standby - only receives replication, promoted on Primary failure
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
    /// - Standby: Cold backup, only receives replication until promoted
    ///
    /// None for non-Data nodes (Meta, Router, Compactor).
    /// Auto-assigned by Meta based on `primary_standby_ratio` config.
    ///
    /// Note: Do NOT use skip_serializing_if here - it breaks bincode which
    /// expects a fixed structure. JSON handles it fine but bincode doesn't.
    #[serde(default)]
    pub mode: Option<DataNodeMode>,
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
            mode: None, // Auto-assigned by Meta for Data nodes
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
    pub fn table_prefix(catalog: &str, schema: &str, table: &str) -> String {
        format!("{}{}/{}/{}/", keys::PREFIX_FILE, catalog, schema, table)
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

    /// Promote first standby to leader, returning the new leader ID
    pub fn promote_standby(&mut self) -> Option<String> {
        if let Some(new_leader) = self.standby_node_ids.first().cloned() {
            let old_leader = std::mem::replace(&mut self.leader_node_id, new_leader.clone());
            self.standby_node_ids.remove(0);
            // Old leader could become standby if it recovers
            self.standby_node_ids.push(old_leader);
            self.leader_last_seen = current_timestamp_ms();
            Some(new_leader)
        } else {
            None
        }
    }
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
    /// Standby promoted to leader (failover)
    ReplicaPromoted = 10,
    /// Replica state changed (InSync/CatchingUp/Offline)
    ReplicaStateChanged = 11,
    /// Data node mode changed (Cold Standby Architecture)
    ///
    /// Published when a Standby node is promoted to Primary.
    /// Payload contains: {"node_id": "...", "old_mode": "Standby", "new_mode": "Primary"}
    DataNodeModeChanged = 12,

    /// Standby needs to flush data for crashed Primary (Recovery)
    ///
    /// Published when a Primary crashes and its Standby needs to flush replicated data.
    /// Payload: {"standby_id": "...", "crashed_primary_id": "..."}
    FlushForRecovery = 13,
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

    // File operations
    RegisterFiles {
        files: Vec<FileInfo>,
    },
    RemoveFiles {
        paths: Vec<String>,
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
    PromoteStandby {
        partition_key: String,
    },
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

    /// Promote a Standby data node to Primary (Cold Standby Architecture)
    ///
    /// Called when a Primary node fails. The Standby takes over
    /// the failed Primary's partition leadership.
    PromoteDataNode {
        standby_node_id: String,
        failed_primary_id: Option<String>,
    },

    /// Update a data node's mode (Primary/Standby)
    ///
    /// Used internally during auto-assignment and promotion.
    UpdateDataNodeMode {
        node_id: String,
        mode: DataNodeMode,
    },

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
    /// When a Primary crashes with promote_standby=false:
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
    /// Called during Primary crash recovery (promote_standby=false):
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
    StandbyPromoted {
        partition_key: String,
        new_leader_id: String,
        old_leader_id: String,
    },
    ReplicaUpdated {
        partition_key: String,
    },
    ReplicaRemoved {
        partition_key: String,
    },
    ReplicaNotFound {
        partition_key: String,
    },
    NoStandbyAvailable {
        partition_key: String,
    },
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
    /// Data node promoted from Standby to Primary
    DataNodePromoted {
        node_id: String,
        old_mode: DataNodeMode,
        new_mode: DataNodeMode,
    },
    /// Data node mode updated
    DataNodeModeUpdated {
        node_id: String,
        mode: DataNodeMode,
    },
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
