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
    Draining = 3,
    Inactive = 4,
}

impl From<i32> for NodeState {
    fn from(v: i32) -> Self {
        match v {
            1 => NodeState::Starting,
            2 => NodeState::Active,
            3 => NodeState::Draining,
            4 => NodeState::Inactive,
            _ => NodeState::Unknown,
        }
    }
}

impl From<NodeState> for i32 {
    fn from(state: NodeState) -> Self {
        state as i32
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
}

impl NodeInfo {
    pub fn new(node_id: String, role: NodeRole, advertise_addr: String) -> Self {
        let now = current_timestamp_ms();
        Self {
            node_id,
            role,
            state: NodeState::Starting,
            advertise_addr,
            http_addr: None,
            flight_addr: None,
            registered_at: now,
            last_heartbeat: now,
            labels: HashMap::new(),
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
