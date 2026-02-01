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

    /// HTTP API port for scale-in and management operations
    #[serde(default = "default_http_port")]
    pub http_port: u16,

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

    /// Cold Standby Architecture configuration
    ///
    /// Controls Primary/Standby mode assignment for Data nodes.
    /// Standbys are cold backup nodes that don't serve queries.
    #[serde(default)]
    pub standby: StandbyConfig,

    /// Failover configuration
    ///
    /// Controls how the cluster handles node failures.
    #[serde(default)]
    pub failover: FailoverConfig,
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

/// Failover Configuration
///
/// Controls how the cluster handles node failures.
/// This implements a NO-PROMOTION strategy: when a Primary crashes,
/// its Standby flushes data and partitions are redistributed.
///
/// # Primary Crash Handling
///
/// When a Primary crashes:
/// 1. Related Standby flushes the crashed Primary's replicated data
/// 2. Partitions are redistributed to remaining Primaries
/// 3. Result: N-1 Primaries, same Standbys (ratio improves)
///
/// # Standby Crash Handling
///
/// When a Standby crashes:
/// 1. Try to assign a spare Standby from pool (if available)
/// 2. If no spare, try flexible assignment to existing Standbys (if enabled)
/// 3. If still no coverage, leave Primary unprotected (with warning)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Trigger Standby to flush when Primary crashes
    ///
    /// Ensures no data loss by having Standby write all
    /// replicated data to Parquet before redistribution.
    #[serde(default = "default_flush_on_primary_crash")]
    pub flush_on_primary_crash: bool,

    /// Policy for writes to unprotected partitions (no Standby)
    ///
    /// - "allow": Accept writes silently
    /// - "warn": Accept writes but log warning and emit metric
    /// - "reject": Reject writes with error
    #[serde(default = "default_unprotected_write_policy")]
    pub unprotected_write_policy: String,

    /// Timeout for Standby flush operation (seconds)
    ///
    /// How long to wait for Standby to flush crashed Primary's data.
    #[serde(default = "default_flush_timeout_secs")]
    pub flush_timeout_secs: u64,

    /// Timeout for partition redistribution (seconds)
    #[serde(default = "default_redistribute_timeout_secs")]
    pub redistribute_timeout_secs: u64,
}

fn default_flush_on_primary_crash() -> bool {
    true
}

fn default_unprotected_write_policy() -> String {
    "warn".to_string()
}

fn default_flush_timeout_secs() -> u64 {
    60 // 1 minute
}

fn default_redistribute_timeout_secs() -> u64 {
    300 // 5 minutes
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            flush_on_primary_crash: default_flush_on_primary_crash(),
            unprotected_write_policy: default_unprotected_write_policy(),
            flush_timeout_secs: default_flush_timeout_secs(),
            redistribute_timeout_secs: default_redistribute_timeout_secs(),
        }
    }
}

/// Unprotected write policy enum for type-safe access
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnprotectedWritePolicy {
    Allow,
    Warn,
    Reject,
}

impl FailoverConfig {
    /// Parse unprotected_write_policy string to enum
    pub fn get_unprotected_write_policy(&self) -> UnprotectedWritePolicy {
        match self.unprotected_write_policy.to_lowercase().as_str() {
            "allow" => UnprotectedWritePolicy::Allow,
            "reject" => UnprotectedWritePolicy::Reject,
            _ => UnprotectedWritePolicy::Warn, // Default to warn
        }
    }
}

/// Cold Standby Architecture Configuration
///
/// Controls how Data nodes are assigned as Primary (active) or Standby (cold backup).
/// This architecture prevents avalanche failures: when a Primary fails, a dedicated
/// Standby takes over completely instead of adding load to other Primaries.
///
/// # Architecture
///
/// ```text
/// ┌───────────────────────────────────────────────────────────────┐
/// │  data-0 (Primary) ──┐                                         │
/// │  data-1 (Primary) ──┼──> data-3 (Standby) - Cold, no queries  │
/// │  data-2 (Primary) ──┘    Replicates from data-0,1,2           │
/// │                                                               │
/// │  Ratio: 3:1 (3 Primaries : 1 Standby)                        │
/// └───────────────────────────────────────────────────────────────┘
///
/// On data-1 failure:
///   1. data-3 promoted to Primary
///   2. No other Primary sees increased load!
/// ```
///
/// # Configuration Example
///
/// ```toml
/// [meta.standby]
/// enabled = true
/// primary_standby_ratio = 2    # 2 Primaries per 1 Standby
/// auto_assign_mode = true
/// spare_standbys = 1           # Keep 1 spare for quick replacement
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandbyConfig {
    /// Enable cold standby architecture
    ///
    /// When enabled:
    /// - Data nodes are assigned as Primary or Standby on registration
    /// - Standby nodes only receive replication, never serve queries
    /// - On Primary failure, Standby flushes data, partitions redistributed
    ///
    /// When disabled:
    /// - All Data nodes are Primary (legacy ISR-like behavior)
    /// - Partitions can have any node as leader or standby
    #[serde(default = "default_standby_enabled")]
    pub enabled: bool,

    /// Ratio of Primary nodes to Standby nodes
    ///
    /// Examples:
    /// - 2 means 2:1 ratio (6 nodes → 4 Primary, 2 Standby)
    /// - 3 means 3:1 ratio (8 nodes → 6 Primary, 2 Standby)
    /// - 1 means 1:1 ratio (equal Primary and Standby)
    ///
    /// Higher ratio = more compute capacity, less redundancy
    /// Lower ratio = more redundancy, less compute capacity
    #[serde(default = "default_primary_standby_ratio")]
    pub primary_standby_ratio: u32,

    /// Auto-assign mode when Data nodes register
    ///
    /// When enabled, Meta automatically assigns Primary or Standby mode
    /// to new Data nodes based on the current ratio.
    /// When disabled, all new nodes default to Primary.
    #[serde(default = "default_standby_auto_assign_mode")]
    pub auto_assign_mode: bool,

    /// Minimum Primary nodes before assigning Standbys
    ///
    /// Ensures cluster has minimum compute capacity before allocating
    /// cold standby nodes. If fewer nodes available, all become Primary.
    #[serde(default = "default_min_primary_nodes")]
    pub min_primary_nodes: usize,

    /// Number of spare Standbys to maintain in pool
    ///
    /// Spare Standbys are idle nodes ready to replace failed Standbys.
    /// When an active Standby fails, a spare is automatically assigned.
    ///
    /// Recommended: 1-2 for production clusters
    #[serde(default = "default_spare_standbys")]
    pub spare_standbys: u32,

    /// Strictly guarantee the configured Primary:Standby ratio
    ///
    /// When true (strict mode):
    /// - Only use spare pool for Standby replacement
    /// - If no spare, leave Primaries unprotected
    /// - No risk of Standby overload
    ///
    /// When false (flexible mode):
    /// - First try spare pool
    /// - Then try existing Standbys with capacity (up to max_primary_standby_ratio)
    /// - More Primaries protected, but Standbys may be overloaded
    #[serde(default = "default_ratio_guarantee")]
    pub ratio_guarantee: bool,

    /// Maximum Primaries a single Standby can back (flexible mode only)
    ///
    /// Only used when ratio_guarantee=false.
    /// Prevents excessive Standby overload by capping how many Primaries
    /// a single Standby can back.
    ///
    /// Example: If configured ratio is 2 and max is 3:
    /// - Normal: Each Standby backs 2 Primaries
    /// - After failure: Standby can back up to 3 Primaries (50% overload)
    #[serde(default = "default_max_primary_standby_ratio")]
    pub max_primary_standby_ratio: u32,
}

fn default_standby_enabled() -> bool {
    false // Disabled by default for backward compatibility
}

fn default_primary_standby_ratio() -> u32 {
    2 // 2:1 ratio (2 Primaries per 1 Standby)
}

fn default_standby_auto_assign_mode() -> bool {
    true
}

fn default_min_primary_nodes() -> usize {
    2 // At least 2 Primary nodes before assigning Standbys
}

fn default_spare_standbys() -> u32 {
    0 // No spares by default, user should configure
}

fn default_ratio_guarantee() -> bool {
    true // Strict mode by default (safer)
}

fn default_max_primary_standby_ratio() -> u32 {
    3 // Allow up to 3 Primaries per Standby in flexible mode
}

impl Default for StandbyConfig {
    fn default() -> Self {
        Self {
            enabled: default_standby_enabled(),
            primary_standby_ratio: default_primary_standby_ratio(),
            auto_assign_mode: default_standby_auto_assign_mode(),
            min_primary_nodes: default_min_primary_nodes(),
            spare_standbys: default_spare_standbys(),
            ratio_guarantee: default_ratio_guarantee(),
            max_primary_standby_ratio: default_max_primary_standby_ratio(),
        }
    }
}

impl StandbyConfig {
    /// Calculate how many Standby nodes should exist for given Primary count
    ///
    /// Returns the target number of Standby nodes based on the ratio.
    /// Example: with ratio=2 and 4 Primaries, target is 2 Standbys.
    pub fn target_standby_count(&self, primary_count: usize) -> usize {
        if !self.enabled || primary_count < self.min_primary_nodes {
            return 0;
        }
        // Round down: 5 Primaries with ratio=2 → 2 Standbys
        primary_count / self.primary_standby_ratio as usize
    }

    /// Determine mode for a new node based on current cluster state
    ///
    /// Returns Primary if:
    /// - Standby architecture is disabled
    /// - Not enough Primaries yet
    /// - Already have enough Standbys for the ratio
    ///
    /// Returns Standby if:
    /// - Have enough Primaries and need more Standbys for ratio
    pub fn mode_for_new_node(
        &self,
        current_primary_count: usize,
        current_standby_count: usize,
    ) -> super::types::DataNodeMode {
        use super::types::DataNodeMode;

        if !self.enabled || !self.auto_assign_mode {
            return DataNodeMode::Primary;
        }

        // Need minimum Primaries first
        if current_primary_count < self.min_primary_nodes {
            return DataNodeMode::Primary;
        }

        // Calculate target Standby count for current Primary count
        let target_standbys = self.target_standby_count(current_primary_count);

        if current_standby_count < target_standbys {
            DataNodeMode::Standby
        } else {
            DataNodeMode::Primary
        }
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

fn default_http_port() -> u16 {
    9000 // Meta HTTP API for scale-in and management
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
            http_port: default_http_port(),
            peers: vec![],
            raft: RaftConfig::default(),
            events: EventConfig::default(),
            replication: ReplicationAssignmentConfig::default(),
            standby: StandbyConfig::default(),
            failover: FailoverConfig::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failover_config_defaults() {
        let config = FailoverConfig::default();

        // Verify default values
        assert!(config.flush_on_primary_crash);
        assert_eq!(config.unprotected_write_policy, "warn");
        assert_eq!(config.flush_timeout_secs, 60);
        assert_eq!(config.redistribute_timeout_secs, 300);
    }

    #[test]
    fn test_failover_config_serde() {
        // Test that config can be serialized and deserialized
        let toml_str = r#"
            flush_on_primary_crash = true
            unprotected_write_policy = "reject"
            flush_timeout_secs = 120
            redistribute_timeout_secs = 600
        "#;

        let config: FailoverConfig = toml::from_str(toml_str).unwrap();

        assert!(config.flush_on_primary_crash);
        assert_eq!(config.unprotected_write_policy, "reject");
        assert_eq!(config.flush_timeout_secs, 120);
        assert_eq!(config.redistribute_timeout_secs, 600);
    }

    #[test]
    fn test_standby_config_defaults() {
        let config = StandbyConfig::default();

        // Core settings
        assert!(!config.enabled); // Disabled by default
        assert_eq!(config.primary_standby_ratio, 2);
        assert!(config.auto_assign_mode);
        assert_eq!(config.min_primary_nodes, 2);

        // Spare and flexible assignment settings
        assert_eq!(config.spare_standbys, 0);
        assert!(config.ratio_guarantee); // Strict mode by default
        assert_eq!(config.max_primary_standby_ratio, 3);
    }

    #[test]
    fn test_standby_config_strict_mode() {
        // Strict mode: ratio_guarantee=true
        // Should only use spare pool, never overload existing Standbys
        let config = StandbyConfig {
            ratio_guarantee: true,
            max_primary_standby_ratio: 3, // Ignored in strict mode
            ..Default::default()
        };

        assert!(config.ratio_guarantee);
        assert_eq!(config.max_primary_standby_ratio, 3);
    }

    #[test]
    fn test_standby_config_flexible_mode() {
        // Flexible mode: ratio_guarantee=false
        // Can use existing Standbys up to max_primary_standby_ratio
        let config = StandbyConfig {
            ratio_guarantee: false,
            max_primary_standby_ratio: 4, // Allow 4:1 overload
            ..Default::default()
        };

        assert!(!config.ratio_guarantee);
        assert_eq!(config.max_primary_standby_ratio, 4);
    }

    #[test]
    fn test_standby_config_serde() {
        let toml_str = r#"
            enabled = true
            primary_standby_ratio = 3
            auto_assign_mode = true
            min_primary_nodes = 4
            spare_standbys = 2
            ratio_guarantee = false
            max_primary_standby_ratio = 5
        "#;

        let config: StandbyConfig = toml::from_str(toml_str).unwrap();

        assert!(config.enabled);
        assert_eq!(config.primary_standby_ratio, 3);
        assert!(config.auto_assign_mode);
        assert_eq!(config.min_primary_nodes, 4);
        assert_eq!(config.spare_standbys, 2);
        assert!(!config.ratio_guarantee);
        assert_eq!(config.max_primary_standby_ratio, 5);
    }

    #[test]
    fn test_standby_config_partial_serde() {
        // Test that missing fields use defaults
        let toml_str = r#"
            spare_standbys = 1
        "#;

        let config: StandbyConfig = toml::from_str(toml_str).unwrap();

        // Explicit value
        assert_eq!(config.spare_standbys, 1);

        // Defaults
        assert!(!config.enabled);
        assert!(config.ratio_guarantee);
        assert_eq!(config.max_primary_standby_ratio, 3);
    }

    #[test]
    fn test_standby_config_target_standby_count() {
        let config = StandbyConfig {
            enabled: true,
            primary_standby_ratio: 2,
            min_primary_nodes: 2,
            ..Default::default()
        };

        // Not enough primaries
        assert_eq!(config.target_standby_count(1), 0);

        // At min primaries
        assert_eq!(config.target_standby_count(2), 1);

        // More primaries
        assert_eq!(config.target_standby_count(4), 2);
        assert_eq!(config.target_standby_count(5), 2); // Rounds down
        assert_eq!(config.target_standby_count(6), 3);
    }
}
