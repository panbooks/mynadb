//! Router Node configuration

use serde::{Deserialize, Serialize};

/// Router Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    /// Unique node identifier
    pub node_id: String,

    /// HTTP port for client-facing API
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// Arrow Flight port
    #[serde(default = "default_flight_port")]
    pub flight_port: u16,

    /// MySQL wire protocol port
    #[serde(default = "default_mysql_port")]
    pub mysql_port: u16,

    /// Whether MySQL protocol is enabled
    #[serde(default)]
    pub mysql_enabled: bool,

    /// Meta service configuration
    #[serde(default)]
    pub meta: MetaClientConfig,

    /// Routing configuration
    #[serde(default)]
    pub routing: RoutingConfig,

    /// Query configuration
    #[serde(default)]
    pub query: QueryConfig,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,
}

/// Meta client configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaClientConfig {
    /// Meta service endpoints
    #[serde(default = "default_meta_endpoints")]
    pub endpoints: Vec<String>,

    /// Connection timeout in milliseconds
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Request timeout in milliseconds
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

/// Routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Shard cache refresh interval in milliseconds (via Meta events)
    #[serde(default = "default_cache_refresh_interval_ms")]
    pub cache_refresh_interval_ms: u64,

    /// Connection pool size per Data node
    #[serde(default = "default_data_node_pool_size")]
    pub data_node_pool_size: usize,

    /// Data node connection timeout in milliseconds
    #[serde(default = "default_data_node_connect_timeout_ms")]
    pub data_node_connect_timeout_ms: u64,

    /// Data node request timeout in milliseconds
    #[serde(default = "default_data_node_request_timeout_ms")]
    pub data_node_request_timeout_ms: u64,

    /// DEPRECATED: Legacy partition cache TTL (use hot_cache_ttl_secs instead)
    /// Set to 0 to disable automatic eviction
    #[serde(default = "default_partition_cache_ttl_secs")]
    pub partition_cache_ttl_secs: u64,

    /// DEPRECATED: Legacy eviction interval (use hot_cache_eviction_interval_secs instead)
    #[serde(default = "default_partition_cache_eviction_interval_secs")]
    pub partition_cache_eviction_interval_secs: u64,

    // ========================================================================
    // Partition Cache Configuration
    // ========================================================================

    /// Flush interval in seconds - determines "recent" partitions
    /// Recent partitions (within this interval) are routed to table owners
    /// (who have memtable data). Should match cache.flush_interval_secs.
    /// NOTE: Config key kept as hot_cache_ttl_secs for backward compatibility
    #[serde(default = "default_hot_cache_ttl_secs")]
    pub hot_cache_ttl_secs: u64,

    /// DEPRECATED: Hot cache eviction interval - no longer used
    /// Hot data routing is now derived from table owners (consistent across routers)
    #[serde(default = "default_hot_cache_eviction_interval_secs")]
    pub hot_cache_eviction_interval_secs: u64,

    /// Cold cache max entries (for S3/Parquet data)
    /// LRU eviction when exceeded. Historical data is immutable, so no TTL needed.
    #[serde(default = "default_cold_cache_max_entries")]
    pub cold_cache_max_entries: usize,
}

/// Query configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Whether query result caching is enabled
    #[serde(default = "default_cache_enabled")]
    pub cache_enabled: bool,

    /// Max cache size in MB
    #[serde(default = "default_cache_max_size_mb")]
    pub cache_max_size_mb: u64,

    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,

    /// Slow query threshold for caching in milliseconds
    #[serde(default = "default_slow_query_threshold_ms")]
    pub slow_query_threshold_ms: u64,

    /// Max parallel data nodes for fan-out queries
    #[serde(default = "default_max_parallel_fanout")]
    pub max_parallel_fanout: usize,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct AuthConfig {
    /// Whether authentication is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Username for basic auth
    #[serde(default)]
    pub username: String,

    /// Password for basic auth
    #[serde(default)]
    pub password: String,
}

// Default values

fn default_http_port() -> u16 {
    7000
}

fn default_flight_port() -> u16 {
    7001
}

fn default_mysql_port() -> u16 {
    7002
}

fn default_meta_endpoints() -> Vec<String> {
    vec!["127.0.0.1:7100".to_string()]
}

fn default_connect_timeout_ms() -> u64 {
    5000
}

fn default_request_timeout_ms() -> u64 {
    30000
}

fn default_cache_refresh_interval_ms() -> u64 {
    1000
}

fn default_data_node_pool_size() -> usize {
    10
}

fn default_data_node_connect_timeout_ms() -> u64 {
    5000
}

fn default_data_node_request_timeout_ms() -> u64 {
    60000
}

fn default_partition_cache_ttl_secs() -> u64 {
    300 // 5 minutes (DEPRECATED)
}

fn default_partition_cache_eviction_interval_secs() -> u64 {
    60 // Run eviction every minute (DEPRECATED)
}

fn default_hot_cache_ttl_secs() -> u64 {
    300 // 5 minutes - matches flush_interval
}

fn default_hot_cache_eviction_interval_secs() -> u64 {
    60 // Run eviction every minute
}

fn default_cold_cache_max_entries() -> usize {
    100_000 // 100K partitions (bounded, LRU eviction)
}

fn default_cache_enabled() -> bool {
    true
}

fn default_cache_max_size_mb() -> u64 {
    512
}

fn default_cache_ttl_secs() -> u64 {
    300
}

fn default_slow_query_threshold_ms() -> u64 {
    100
}

fn default_max_parallel_fanout() -> usize {
    16
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            node_id: format!(
                "router-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            ),
            http_port: default_http_port(),
            flight_port: default_flight_port(),
            mysql_port: default_mysql_port(),
            mysql_enabled: true,
            meta: MetaClientConfig::default(),
            routing: RoutingConfig::default(),
            query: QueryConfig::default(),
            auth: AuthConfig::default(),
        }
    }
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            endpoints: default_meta_endpoints(),
            connect_timeout_ms: default_connect_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
        }
    }
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            cache_refresh_interval_ms: default_cache_refresh_interval_ms(),
            data_node_pool_size: default_data_node_pool_size(),
            data_node_connect_timeout_ms: default_data_node_connect_timeout_ms(),
            data_node_request_timeout_ms: default_data_node_request_timeout_ms(),
            partition_cache_ttl_secs: default_partition_cache_ttl_secs(),
            partition_cache_eviction_interval_secs: default_partition_cache_eviction_interval_secs(),
            // Split cache config
            hot_cache_ttl_secs: default_hot_cache_ttl_secs(),
            hot_cache_eviction_interval_secs: default_hot_cache_eviction_interval_secs(),
            cold_cache_max_entries: default_cold_cache_max_entries(),
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            cache_enabled: default_cache_enabled(),
            cache_max_size_mb: default_cache_max_size_mb(),
            cache_ttl_secs: default_cache_ttl_secs(),
            slow_query_threshold_ms: default_slow_query_threshold_ms(),
            max_parallel_fanout: default_max_parallel_fanout(),
        }
    }
}

