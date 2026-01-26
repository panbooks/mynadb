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
