//! Meta Service Configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for a Meta node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaConfig {
    /// Unique node ID within the Raft cluster (1, 2, 3, etc.)
    #[serde(default = "default_node_id")]
    pub node_id: u64,

    /// Address to bind the gRPC server
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,

    /// Address advertised to other nodes for Raft communication
    #[serde(default = "default_advertise_addr")]
    pub advertise_addr: String,

    /// Directory for sled database and Raft logs
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// Initial cluster peers (address:port)
    #[serde(default)]
    pub peers: Vec<String>,

    /// Raft-specific configuration
    #[serde(default)]
    pub raft: RaftConfig,

    /// Event log configuration
    #[serde(default)]
    pub events: EventConfig,

    /// Replication assignment configuration for warm standby
    #[serde(default)]
    pub replication: ReplicationAssignmentConfig,
}

/// Raft consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Heartbeat interval in milliseconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    /// Minimum election timeout in milliseconds
    #[serde(default = "default_election_timeout_min")]
    pub election_timeout_min_ms: u64,

    /// Maximum election timeout in milliseconds
    #[serde(default = "default_election_timeout_max")]
    pub election_timeout_max_ms: u64,

    /// Number of log entries before taking a snapshot
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,

    /// Maximum Raft log size before compaction (MB)
    #[serde(default = "default_max_log_size")]
    pub max_log_size_mb: u64,

    /// Maximum number of entries per append request
    #[serde(default = "default_max_payload_entries")]
    pub max_payload_entries: u64,
}

/// Event log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfig {
    /// Maximum number of events to retain
    #[serde(default = "default_max_events")]
    pub max_events: u64,

    /// Event retention duration in seconds
    #[serde(default = "default_event_retention_secs")]
    pub retention_secs: u64,
}

/// Configuration for automatic replica assignment
///
/// Controls how Meta service assigns leaders and standbys to partitions
/// for warm standby replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationAssignmentConfig {
    /// Enable automatic replica assignment for new partitions
    ///
    /// When enabled, Meta will automatically assign a leader and standbys
    /// when a new partition is created (first write to partition).
    /// When disabled, replica assignments must be made via API.
    #[serde(default = "default_auto_assign_enabled")]
    pub auto_assign_enabled: bool,

    /// Default replication factor for new partitions
    ///
    /// - 1: No replication (leader only)
    /// - 2: 1 standby (leader + 1 standby)
    /// - 3: 2 standbys (leader + 2 standbys)
    #[serde(default = "default_replication_factor")]
    pub default_replication_factor: u8,

    /// Enable zone-aware placement
    ///
    /// When enabled, Meta will prefer placing standbys in different
    /// availability zones than the leader for better fault tolerance.
    #[serde(default = "default_zone_aware")]
    pub zone_aware: bool,

    /// Minimum number of Data nodes required to enable replication
    ///
    /// If fewer nodes are available, partitions will be created with RF=1.
    #[serde(default = "default_min_nodes_for_replication")]
    pub min_nodes_for_replication: usize,

    /// Node lease TTL in seconds
    ///
    /// How long before a node is considered dead after missing heartbeats.
    /// Shorter = faster detection, more false positives.
    /// Longer = slower detection, fewer false positives.
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: u64,

    /// Enable automatic standby promotion on leader failure
    ///
    /// When enabled, Meta will automatically promote a standby to leader
    /// when the current leader's lease expires.
    #[serde(default = "default_auto_promote")]
    pub auto_promote: bool,

    /// Load balancing strategy for leader assignment
    ///
    /// - "least_loaded": Assign to node with fewest partitions (default)
    /// - "round_robin": Rotate through nodes
    /// - "random": Random assignment
    #[serde(default = "default_load_balance_strategy")]
    pub load_balance_strategy: String,
}

fn default_auto_assign_enabled() -> bool {
    true
}

fn default_replication_factor() -> u8 {
    2 // Leader + 1 standby
}

fn default_zone_aware() -> bool {
    true
}

fn default_min_nodes_for_replication() -> usize {
    2
}

fn default_lease_ttl_secs() -> u64 {
    10
}

fn default_auto_promote() -> bool {
    true
}

fn default_load_balance_strategy() -> String {
    "least_loaded".to_string()
}

impl Default for ReplicationAssignmentConfig {
    fn default() -> Self {
        Self {
            auto_assign_enabled: default_auto_assign_enabled(),
            default_replication_factor: default_replication_factor(),
            zone_aware: default_zone_aware(),
            min_nodes_for_replication: default_min_nodes_for_replication(),
            lease_ttl_secs: default_lease_ttl_secs(),
            auto_promote: default_auto_promote(),
            load_balance_strategy: default_load_balance_strategy(),
        }
    }
}

impl ReplicationAssignmentConfig {
    /// Get the effective replication factor based on available nodes
    pub fn effective_replication_factor(&self, available_nodes: usize) -> u8 {
        if available_nodes < self.min_nodes_for_replication {
            1 // Not enough nodes for replication
        } else {
            // Cap at available nodes
            std::cmp::min(self.default_replication_factor, available_nodes as u8)
        }
    }

    /// Get lease TTL as Duration
    pub fn lease_ttl(&self) -> Duration {
        Duration::from_secs(self.lease_ttl_secs)
    }
}

// Default value functions
fn default_node_id() -> u64 {
    1
}

fn default_bind_addr() -> String {
    "0.0.0.0:7100".to_string()
}

fn default_advertise_addr() -> String {
    "127.0.0.1:7100".to_string()
}

fn default_data_dir() -> String {
    "/data/meta".to_string()
}

fn default_heartbeat_interval() -> u64 {
    150
}

fn default_election_timeout_min() -> u64 {
    300
}

fn default_election_timeout_max() -> u64 {
    500
}

fn default_snapshot_threshold() -> u64 {
    10000
}

fn default_max_log_size() -> u64 {
    256
}

fn default_max_payload_entries() -> u64 {
    300
}

fn default_max_events() -> u64 {
    100000
}

fn default_event_retention_secs() -> u64 {
    3600 // 1 hour
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            bind_addr: default_bind_addr(),
            advertise_addr: default_advertise_addr(),
            data_dir: default_data_dir(),
            peers: vec![],
            raft: RaftConfig::default(),
            events: EventConfig::default(),
            replication: ReplicationAssignmentConfig::default(),
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: default_heartbeat_interval(),
            election_timeout_min_ms: default_election_timeout_min(),
            election_timeout_max_ms: default_election_timeout_max(),
            snapshot_threshold: default_snapshot_threshold(),
            max_log_size_mb: default_max_log_size(),
            max_payload_entries: default_max_payload_entries(),
        }
    }
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            max_events: default_max_events(),
            retention_secs: default_event_retention_secs(),
        }
    }
}

impl MetaConfig {
    /// Get heartbeat interval as Duration
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.raft.heartbeat_interval_ms)
    }

    /// Get election timeout range
    pub fn election_timeout_range(&self) -> (Duration, Duration) {
        (
            Duration::from_millis(self.raft.election_timeout_min_ms),
            Duration::from_millis(self.raft.election_timeout_max_ms),
        )
    }

    /// Get the sled database path
    pub fn sled_path(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.data_dir).join("sled")
    }

    /// Get the Raft log path
    pub fn raft_log_path(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.data_dir).join("raft")
    }
}
