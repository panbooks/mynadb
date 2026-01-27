//! Data Node configuration

use serde::{Deserialize, Serialize};

/// Data Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataNodeConfig {
    /// Unique node identifier
    pub node_id: String,

    /// Advertised address for Router nodes to connect
    pub advertise_addr: String,

    /// gRPC port for internal communication
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,

    /// Meta service endpoints
    #[serde(default)]
    pub meta_endpoints: Vec<String>,

    /// Meta client connection timeout
    #[serde(default = "default_meta_connect_timeout_ms")]
    pub meta_connect_timeout_ms: u64,

    /// Meta client request timeout
    #[serde(default = "default_meta_request_timeout_ms")]
    pub meta_request_timeout_ms: u64,

    /// Replication configuration for warm standby
    #[serde(default)]
    pub replication: ReplicationConfig,
}

/// Replication configuration for warm standby
///
/// When enabled, the leader node streams writes to standby nodes for
/// zero-downtime failover. Standbys maintain an in-memory copy but
/// don't flush to Parquet (only the leader does).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Whether replication is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Replication mode: "async" or "sync"
    /// - async: ACK immediately after local write (default)
    /// - sync: Wait for at least one standby ACK
    #[serde(default = "default_replication_mode")]
    pub mode: String,

    /// How often to batch replication entries (ms)
    #[serde(default = "default_batch_interval_ms")]
    pub batch_interval_ms: u64,

    /// Maximum entries per batch
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    /// Timeout for standby ACK (ms)
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,

    /// Alert if standby replication lag exceeds this (ms)
    #[serde(default = "default_max_lag_alert_ms")]
    pub max_lag_alert_ms: u64,
}

fn default_replication_mode() -> String {
    "async".to_string()
}

fn default_batch_interval_ms() -> u64 {
    100
}

fn default_max_batch_size() -> usize {
    1000
}

fn default_ack_timeout_ms() -> u64 {
    5000
}

fn default_max_lag_alert_ms() -> u64 {
    10000
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: default_replication_mode(),
            batch_interval_ms: default_batch_interval_ms(),
            max_batch_size: default_max_batch_size(),
            ack_timeout_ms: default_ack_timeout_ms(),
            max_lag_alert_ms: default_max_lag_alert_ms(),
        }
    }
}

fn default_grpc_port() -> u16 {
    7200
}

fn default_meta_connect_timeout_ms() -> u64 {
    5000
}

fn default_meta_request_timeout_ms() -> u64 {
    30000
}

impl Default for DataNodeConfig {
    fn default() -> Self {
        Self {
            node_id: format!(
                "data-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            ),
            advertise_addr: "127.0.0.1:7200".to_string(),
            grpc_port: default_grpc_port(),
            meta_endpoints: vec!["127.0.0.1:7100".to_string()],
            meta_connect_timeout_ms: default_meta_connect_timeout_ms(),
            meta_request_timeout_ms: default_meta_request_timeout_ms(),
            replication: ReplicationConfig::default(),
        }
    }
}
