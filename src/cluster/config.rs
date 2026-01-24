//! Cluster configuration module
//!
//! Defines configuration structures for cluster mode.

use serde::{Deserialize, Serialize};

/// Main cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Enable cluster mode
    #[serde(default)]
    pub enabled: bool,

    /// Node ID (auto-generated if empty)
    #[serde(default)]
    pub node_id: String,

    /// Address this node advertises to other nodes
    #[serde(default = "default_advertise_addr")]
    pub advertise_addr: String,

    /// Node role: "compute", "compactor", or "hybrid"
    #[serde(default = "default_role")]
    pub role: String,

    /// Coordinator configuration
    #[serde(default)]
    pub coordinator: CoordinatorConfig,

    /// Cache invalidation configuration
    #[serde(default)]
    pub cache: CacheInvalidationConfig,

    /// Write coordination configuration
    #[serde(default)]
    pub write: WriteConfig,

    /// Compaction coordination configuration
    #[serde(default)]
    pub compaction: CompactionCoordinationConfig,

    /// Health check configuration
    #[serde(default)]
    pub health: HealthConfig,
}

/// Coordinator (etcd/Redis) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    /// Coordinator type: "etcd" or "redis" or "none"
    #[serde(default = "default_coordinator_type")]
    pub coordinator_type: String,

    /// Coordinator endpoints
    #[serde(default = "default_coordinator_endpoints")]
    pub endpoints: Vec<String>,

    /// Username for authentication (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password for authentication (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Lock key prefix
    #[serde(default = "default_lock_prefix")]
    pub lock_prefix: String,

    /// Cache key prefix
    #[serde(default = "default_cache_prefix")]
    pub cache_prefix: String,

    /// Lock TTL in seconds
    #[serde(default = "default_lock_ttl_secs")]
    pub lock_ttl_secs: u64,

    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

/// Cache invalidation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInvalidationConfig {
    /// Invalidation channel/topic name
    #[serde(default = "default_invalidation_channel")]
    pub invalidation_channel: String,

    /// Invalidation method: "pubsub" or "ttl"
    #[serde(default = "default_invalidation_method")]
    pub invalidation_method: String,

    /// TTL for cache entries in seconds (when using TTL method)
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,
}

/// Write coordination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteConfig {
    /// Timeout for acquiring partition lock (seconds)
    #[serde(default = "default_acquire_lock_timeout_secs")]
    pub acquire_lock_timeout_secs: u64,

    /// Notify other nodes on flush
    #[serde(default = "default_flush_notify")]
    pub flush_notify: bool,

    /// Enable write coordination (locks)
    #[serde(default = "default_write_coordination_enabled")]
    pub enabled: bool,
}

/// Compaction coordination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionCoordinationConfig {
    /// Only leader runs compaction
    #[serde(default = "default_leader_only")]
    pub leader_only: bool,

    /// Election key for compaction leader
    #[serde(default = "default_election_key")]
    pub election_key: String,

    /// Lease TTL for leader election (seconds)
    #[serde(default = "default_election_lease_ttl_secs")]
    pub election_lease_ttl_secs: u64,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Health check interval in seconds
    #[serde(default = "default_health_check_interval_secs")]
    pub check_interval_secs: u64,

    /// Number of failed checks before marking unhealthy
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,

    /// Enable health endpoint
    #[serde(default = "default_health_enabled")]
    pub enabled: bool,
}

// Default value functions
fn default_advertise_addr() -> String {
    "0.0.0.0:7000".to_string()
}

fn default_role() -> String {
    "hybrid".to_string()
}

fn default_coordinator_type() -> String {
    "none".to_string()
}

fn default_coordinator_endpoints() -> Vec<String> {
    vec!["localhost:2379".to_string()]
}

fn default_lock_prefix() -> String {
    "/mynadb/locks/".to_string()
}

fn default_cache_prefix() -> String {
    "/mynadb/cache/".to_string()
}

fn default_lock_ttl_secs() -> u64 {
    30
}

fn default_heartbeat_interval_secs() -> u64 {
    5
}

fn default_connect_timeout_secs() -> u64 {
    5
}

fn default_request_timeout_secs() -> u64 {
    10
}

fn default_invalidation_channel() -> String {
    "/mynadb/cache/invalidate".to_string()
}

fn default_invalidation_method() -> String {
    "pubsub".to_string()
}

fn default_cache_ttl_secs() -> u64 {
    60
}

fn default_acquire_lock_timeout_secs() -> u64 {
    10
}

fn default_flush_notify() -> bool {
    true
}

fn default_write_coordination_enabled() -> bool {
    true
}

fn default_leader_only() -> bool {
    true
}

fn default_election_key() -> String {
    "/mynadb/leader/compactor".to_string()
}

fn default_election_lease_ttl_secs() -> u64 {
    30
}

fn default_health_check_interval_secs() -> u64 {
    10
}

fn default_unhealthy_threshold() -> u32 {
    3
}

fn default_health_enabled() -> bool {
    true
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: String::new(),
            advertise_addr: default_advertise_addr(),
            role: default_role(),
            coordinator: CoordinatorConfig::default(),
            cache: CacheInvalidationConfig::default(),
            write: WriteConfig::default(),
            compaction: CompactionCoordinationConfig::default(),
            health: HealthConfig::default(),
        }
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            coordinator_type: default_coordinator_type(),
            endpoints: default_coordinator_endpoints(),
            username: None,
            password: None,
            lock_prefix: default_lock_prefix(),
            cache_prefix: default_cache_prefix(),
            lock_ttl_secs: default_lock_ttl_secs(),
            heartbeat_interval_secs: default_heartbeat_interval_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
            request_timeout_secs: default_request_timeout_secs(),
        }
    }
}

impl Default for CacheInvalidationConfig {
    fn default() -> Self {
        Self {
            invalidation_channel: default_invalidation_channel(),
            invalidation_method: default_invalidation_method(),
            cache_ttl_secs: default_cache_ttl_secs(),
        }
    }
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            acquire_lock_timeout_secs: default_acquire_lock_timeout_secs(),
            flush_notify: default_flush_notify(),
            enabled: default_write_coordination_enabled(),
        }
    }
}

impl Default for CompactionCoordinationConfig {
    fn default() -> Self {
        Self {
            leader_only: default_leader_only(),
            election_key: default_election_key(),
            election_lease_ttl_secs: default_election_lease_ttl_secs(),
        }
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: default_health_check_interval_secs(),
            unhealthy_threshold: default_unhealthy_threshold(),
            enabled: default_health_enabled(),
        }
    }
}

impl ClusterConfig {
    /// Check if cluster mode is effectively enabled
    /// Returns true only if cluster is enabled AND coordinator is configured
    pub fn is_active(&self) -> bool {
        self.enabled && self.coordinator.coordinator_type != "none"
    }

    /// Get the effective node ID (generate if not set)
    pub fn effective_node_id(&self) -> String {
        if self.node_id.is_empty() {
            // Generate a node ID based on hostname and UUID
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            let uuid = uuid::Uuid::new_v4();
            format!("{}-{}", hostname, &uuid.to_string()[..8])
        } else {
            self.node_id.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClusterConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.role, "hybrid");
        assert_eq!(config.coordinator.coordinator_type, "none");
    }

    #[test]
    fn test_effective_node_id() {
        let mut config = ClusterConfig::default();

        // When node_id is empty, should generate one
        let id1 = config.effective_node_id();
        assert!(!id1.is_empty());

        // When node_id is set, should return it
        config.node_id = "my-node-1".to_string();
        assert_eq!(config.effective_node_id(), "my-node-1");
    }

    #[test]
    fn test_is_active() {
        let mut config = ClusterConfig::default();

        // Not active by default
        assert!(!config.is_active());

        // Enabled but no coordinator
        config.enabled = true;
        assert!(!config.is_active());

        // Enabled with coordinator
        config.coordinator.coordinator_type = "etcd".to_string();
        assert!(config.is_active());
    }
}
