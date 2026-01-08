use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use tracing::{error, info};

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub schema: SchemaConfig,
    pub cache: CacheConfig,
    pub compaction: CompactionConfig,
    pub series_tracker: SeriesTrackerConfig,
    pub logging: LoggingConfig,
    pub retention: RetentionConfig,
    pub wal: WalConfig,
    pub auth: AuthConfig,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP REST API host
    #[serde(default = "default_http_host")]
    pub http_host: String,

    /// HTTP REST API port
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// Arrow Flight gRPC host
    #[serde(default = "default_flight_host")]
    pub flight_host: String,

    /// Arrow Flight gRPC port
    #[serde(default = "default_flight_port")]
    pub flight_port: u16,

    /// MySQL protocol host
    #[serde(default = "default_mysql_host")]
    pub mysql_host: String,

    /// MySQL protocol port
    #[serde(default = "default_mysql_port")]
    pub mysql_port: u16,

    /// Enable MySQL protocol
    #[serde(default = "default_mysql_enabled")]
    pub mysql_enabled: bool,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Data directory path
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// System directory name (for metadata like series stats, retention exclusions)
    #[serde(default = "default_system_dir")]
    pub system_dir: String,
}

/// Schema/Table naming strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaTableStrategy {
    /// Current: service tag → schema, measurement → table
    ServiceAsSchema,
    /// New: "default" → schema, service tag → table
    ServiceAsTable,
}

/// Schema extraction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// Schema/Table naming strategy
    /// - ServiceAsSchema: service tag → schema, measurement → table (default)
    /// - ServiceAsTable: "default" → schema, service tag → table
    #[serde(default = "default_schema_strategy")]
    pub strategy: SchemaTableStrategy,

    /// Tag keys to extract schema from (default: ["service"])
    /// The first matching tag key will be used as the schema
    #[serde(default = "default_schema_tag_keys")]
    pub tag_keys: Vec<String>,

    /// Default schema name when tag is not present
    #[serde(default = "default_schema_name")]
    pub default_name: String,

    /// Whether to prefix purely numeric schemas (e.g., "123" -> "service_123")
    #[serde(default = "default_schema_prefix_numeric")]
    pub prefix_numeric: bool,
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Background flush interval in seconds
    #[serde(default = "default_cache_flush_interval")]
    pub flush_interval_secs: u64,

    /// Maximum cache size in MB before forcing flush
    #[serde(default = "default_cache_max_size_mb")]
    pub max_size_mb: u64,
}

/// Compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Minimum number of L0 files to trigger minor compaction
    #[serde(default = "default_compaction_min_files")]
    pub min_files: usize,

    /// Minimum number of files at L1+ to trigger major compaction
    #[serde(default = "default_compaction_major_min_files")]
    pub major_min_files: usize,

    /// Target file size in MB after LSM compaction
    #[serde(default = "default_compaction_target_file_size_mb")]
    pub target_file_size_mb: u64,

    /// Maximum compaction level
    #[serde(default = "default_compaction_max_level")]
    pub max_level: u32,

    /// Background LSM compaction interval in seconds (0 = disabled)
    #[serde(default = "default_compaction_interval_secs")]
    pub interval_secs: u64,

    /// Enable full compaction for yesterday's partitions (date-based)
    #[serde(default = "default_full_compaction_enabled")]
    pub full_compaction_enabled: bool,

    /// Hour to run full compaction (0-23, default: 3 for 03:00)
    #[serde(default = "default_full_compaction_hour")]
    pub full_compaction_hour: u32,

    /// Full compaction check interval in seconds (default: 3600 = 1 hour)
    #[serde(default = "default_full_compaction_interval_secs")]
    pub full_compaction_interval_secs: u64,

    /// Target file size for full compaction in MB (default: 200MB)
    /// Partitions larger than this will be skipped
    #[serde(default = "default_full_compaction_target_size_mb")]
    pub full_compaction_target_size_mb: u64,

    // Rate limiting settings
    /// Maximum concurrent compaction operations (0 = unlimited, default: 8)
    /// Limits number of partitions that can be compacted in parallel (LSM compaction)
    #[serde(default = "default_compaction_max_concurrent")]
    pub max_concurrent_compactions: usize,

    /// Maximum concurrent full compaction operations (0 = unlimited, default: 2)
    /// Limits number of partitions for full compaction in parallel
    /// Set lower than max_concurrent_compactions to avoid OOM during full compaction
    #[serde(default = "default_full_compaction_max_concurrent")]
    pub full_compaction_max_concurrent: usize,

    /// Read rate limit in MB/sec (0 = unlimited, default: 200)
    /// Throttles parquet file reads to prevent I/O saturation
    #[serde(default = "default_compaction_read_rate_limit")]
    pub read_rate_limit_mb_per_sec: u64,

    /// Write rate limit in MB/sec (0 = unlimited, default: 200)
    /// Throttles parquet file writes to prevent I/O saturation
    #[serde(default = "default_compaction_write_rate_limit")]
    pub write_rate_limit_mb_per_sec: u64,
}

/// Series tracker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesTrackerConfig {
    /// Track-only mode: only track series cardinality, skip data ingestion
    /// When enabled, writes only update series stats without storing data
    #[serde(default = "default_series_track_only")]
    pub track_only: bool,

    /// Background flush interval in seconds
    #[serde(default = "default_series_flush_interval")]
    pub flush_interval_secs: u64,

    /// System tags to exclude for UMC (user metric count) calculation
    /// These tags are excluded when calculating UMC hash
    /// PMC (physical metric count) includes all tags
    /// Examples: ["MONITOR.ip", "MONITOR.hostName", "MONITOR.hostIP"]
    #[serde(default)]
    pub system_tags: Vec<String>,

    /// Number of days to keep in cache (and load on startup)
    #[serde(default = "default_series_cache_retention_days")]
    pub cache_retention_days: u32,

    /// Maximum concurrent tables to load during startup (default: 8)
    /// Higher values speed up startup but use more memory and file handles
    #[serde(default = "default_series_max_concurrent_tables")]
    pub max_concurrent_tables: usize,

    /// Maximum concurrent files to read per table during cache loading (default: 4)
    /// Higher values speed up I/O but use more threads and memory
    #[serde(default = "default_series_max_concurrent_files")]
    pub max_concurrent_files: usize,

    /// Default deduplication strategy: "hashset" or "bloom"
    #[serde(default = "default_dedup_strategy")]
    pub default_strategy: String,

    /// Databases that use bloom filter (supports wildcards like "prod_*")
    #[serde(default)]
    pub bloom_databases: Vec<String>,

    /// Default bloom filter settings
    #[serde(default)]
    pub bloom: BloomConfig,

    /// Per-database bloom filter customization
    #[serde(default)]
    pub bloom_custom: Vec<BloomDatabaseConfig>,
}

/// Bloom filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomConfig {
    #[serde(default = "default_bloom_expected_series")]
    pub expected_series_per_day: usize,

    #[serde(default = "default_bloom_fp_rate")]
    pub false_positive_rate: f64,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            expected_series_per_day: default_bloom_expected_series(),
            false_positive_rate: default_bloom_fp_rate(),
        }
    }
}

/// Per-database bloom filter customization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomDatabaseConfig {
    pub database: String,
    /// Optional schema filter (pmc or umc). If not specified, applies to all schemas.
    #[serde(default)]
    pub schema: Option<String>,
    pub expected_series_per_day: usize,
    pub false_positive_rate: f64,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log directory path
    #[serde(default = "default_log_dir")]
    pub log_dir: String,

    /// Log file name prefix
    #[serde(default = "default_log_file_prefix")]
    pub file_prefix: String,

    /// Log file rotation (daily, hourly, never)
    #[serde(default = "default_log_rotation")]
    pub rotation: String,

    /// Maximum number of log files to keep
    #[serde(default = "default_log_max_files")]
    pub max_files: usize,

    /// Enable JSON formatted logs
    #[serde(default = "default_log_json")]
    pub json_format: bool,
}

/// Retention policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Enable background retention checks
    #[serde(default = "default_retention_enabled")]
    pub enabled: bool,

    /// Default retention period in days (partitions older than this will be deleted)
    #[serde(default = "default_retention_days")]
    pub default_days: u32,

    /// Background retention check interval in seconds (0 = disabled)
    #[serde(default = "default_retention_interval_secs")]
    pub interval_secs: u64,

    /// Databases to exclude from automatic retention (optional)
    #[serde(default = "default_retention_exclude_databases")]
    pub exclude_databases: Vec<String>,

    /// Tables to exclude from automatic retention (optional, format: "catalog.schema.table")
    #[serde(default = "default_retention_exclude_tables")]
    pub exclude_tables: Vec<String>,
}

/// Write-Ahead Log (WAL) configuration for durability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Enable WAL for data writes and series tracking
    #[serde(default = "default_wal_enabled")]
    pub enabled: bool,

    /// Sync to disk after each write (true=durable, false=faster)
    #[serde(default = "default_wal_sync_on_write")]
    pub sync_on_write: bool,

    /// Rotate WAL file when it exceeds this size (MB)
    #[serde(default = "default_wal_rotate_size_mb")]
    pub rotate_size_mb: u64,

    /// Background cleanup interval in seconds
    #[serde(default = "default_wal_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    /// Retention period for WAL files in hours (files older than this can be deleted)
    #[serde(default = "default_wal_retention_hours")]
    pub retention_hours: u64,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Enable authentication (default: false for backward compatibility)
    #[serde(default = "default_auth_enabled")]
    pub enabled: bool,

    /// Username for basic authentication
    #[serde(default = "default_auth_username")]
    pub username: String,

    /// Password for basic authentication (plaintext in config)
    #[serde(default = "default_auth_password")]
    pub password: String,

    /// Endpoints that are always public (even if auth is enabled)
    #[serde(default = "default_auth_public_endpoints")]
    pub public_endpoints: Vec<String>,
}

// Default value functions
fn default_http_host() -> String {
    "0.0.0.0".to_string()
}

fn default_http_port() -> u16 {
    7000
}

fn default_flight_host() -> String {
    "0.0.0.0".to_string()
}

fn default_flight_port() -> u16 {
    7001
}

fn default_mysql_host() -> String {
    "0.0.0.0".to_string()
}

fn default_mysql_port() -> u16 {
    7002
}

fn default_mysql_enabled() -> bool {
    true
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_system_dir() -> String {
    "_system".to_string()
}

fn default_cache_flush_interval() -> u64 {
    30
}

fn default_cache_max_size_mb() -> u64 {
    4096 // 4GB
}

fn default_compaction_min_files() -> usize {
    5
}

fn default_compaction_major_min_files() -> usize {
    3
}

fn default_compaction_target_file_size_mb() -> u64 {
    200
}

fn default_compaction_max_level() -> u32 {
    4 // L0, L1, L2, L3, L4
}

fn default_compaction_interval_secs() -> u64 {
    60 // LSM compaction every 60 seconds
}

fn default_full_compaction_enabled() -> bool {
    true
}

fn default_full_compaction_hour() -> u32 {
    3 // 03:00
}

fn default_full_compaction_interval_secs() -> u64 {
    3600 // Check every hour (1 hour)
}

fn default_full_compaction_target_size_mb() -> u64 {
    200 // 200MB
}

fn default_compaction_max_concurrent() -> usize {
    8 // Max 8 concurrent compaction operations
}

fn default_full_compaction_max_concurrent() -> usize {
    2 // Max 2 concurrent full compaction operations (lower to avoid OOM)
}

fn default_compaction_read_rate_limit() -> u64 {
    200
}

fn default_compaction_write_rate_limit() -> u64 {
    0
}

fn default_series_track_only() -> bool {
    false
}

fn default_series_flush_interval() -> u64 {
    60
}

fn default_series_cache_retention_days() -> u32 {
    2
}

fn default_series_max_concurrent_tables() -> usize {
    8
}

fn default_series_max_concurrent_files() -> usize {
    4
}

fn default_dedup_strategy() -> String {
    "hashset".to_string()
}

fn default_bloom_expected_series() -> usize {
    1_000_000_000 // 1 billion series per day
}

fn default_bloom_fp_rate() -> f64 {
    0.01 // 1% false positive rate
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_dir() -> String {
    "./logs".to_string()
}

fn default_log_file_prefix() -> String {
    "stats-service".to_string()
}

fn default_log_rotation() -> String {
    "daily".to_string()
}

fn default_log_max_files() -> usize {
    30
}

fn default_log_json() -> bool {
    false
}

fn default_schema_tag_keys() -> Vec<String> {
    vec!["service".to_string()]
}

fn default_schema_name() -> String {
    "default".to_string()
}

fn default_schema_prefix_numeric() -> bool {
    true
}

fn default_schema_strategy() -> SchemaTableStrategy {
    SchemaTableStrategy::ServiceAsSchema
}

fn default_retention_enabled() -> bool {
    false
}

fn default_retention_days() -> u32 {
    7
}

fn default_retention_interval_secs() -> u64 {
    3600 // 1 hour
}

fn default_retention_exclude_databases() -> Vec<String> {
    vec![]
}

fn default_retention_exclude_tables() -> Vec<String> {
    vec![]
}

fn default_wal_enabled() -> bool {
    true
}

fn default_wal_sync_on_write() -> bool {
    false // Default to durable mode
}

fn default_wal_rotate_size_mb() -> u64 {
    64
}

fn default_wal_cleanup_interval_secs() -> u64 {
    300 // 5 minutes - more frequent cleanup to prevent empty file accumulation
}

fn default_wal_retention_hours() -> u64 {
    24 // Keep WAL files for 24 hours
}

fn default_auth_enabled() -> bool {
    false // Disabled by default for backward compatibility
}

fn default_auth_username() -> String {
    "admin".to_string()
}

fn default_auth_password() -> String {
    "".to_string() // Empty by default, must be set if auth is enabled
}

fn default_auth_public_endpoints() -> Vec<String> {
    vec![
        "/health".to_string(),
        "/stats".to_string(),
    ]
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig {
                http_host: default_http_host(),
                http_port: default_http_port(),
                flight_host: default_flight_host(),
                flight_port: default_flight_port(),
                mysql_host: default_mysql_host(),
                mysql_port: default_mysql_port(),
                mysql_enabled: default_mysql_enabled(),
            },
            storage: StorageConfig {
                data_dir: default_data_dir(),
                system_dir: default_system_dir(),
            },
            schema: SchemaConfig {
                strategy: default_schema_strategy(),
                tag_keys: default_schema_tag_keys(),
                default_name: default_schema_name(),
                prefix_numeric: default_schema_prefix_numeric(),
            },
            cache: CacheConfig {
                flush_interval_secs: default_cache_flush_interval(),
                max_size_mb: default_cache_max_size_mb(),
            },
            compaction: CompactionConfig {
                min_files: default_compaction_min_files(),
                major_min_files: default_compaction_major_min_files(),
                target_file_size_mb: default_compaction_target_file_size_mb(),
                max_level: default_compaction_max_level(),
                interval_secs: default_compaction_interval_secs(),
                full_compaction_enabled: default_full_compaction_enabled(),
                full_compaction_hour: default_full_compaction_hour(),
                full_compaction_interval_secs: default_full_compaction_interval_secs(),
                full_compaction_target_size_mb: default_full_compaction_target_size_mb(),
                max_concurrent_compactions: default_compaction_max_concurrent(),
                full_compaction_max_concurrent: default_full_compaction_max_concurrent(),
                read_rate_limit_mb_per_sec: default_compaction_read_rate_limit(),
                write_rate_limit_mb_per_sec: default_compaction_write_rate_limit(),
            },
            series_tracker: SeriesTrackerConfig {
                track_only: default_series_track_only(),
                flush_interval_secs: default_series_flush_interval(),
                system_tags: vec![],
                cache_retention_days: default_series_cache_retention_days(),
                max_concurrent_tables: default_series_max_concurrent_tables(),
                max_concurrent_files: default_series_max_concurrent_files(),
                default_strategy: default_dedup_strategy(),
                bloom_databases: vec![],
                bloom: BloomConfig::default(),
                bloom_custom: vec![],
            },
            logging: LoggingConfig {
                level: default_log_level(),
                log_dir: default_log_dir(),
                file_prefix: default_log_file_prefix(),
                rotation: default_log_rotation(),
                max_files: default_log_max_files(),
                json_format: default_log_json(),
            },
            retention: RetentionConfig {
                enabled: default_retention_enabled(),
                default_days: default_retention_days(),
                interval_secs: default_retention_interval_secs(),
                exclude_databases: default_retention_exclude_databases(),
                exclude_tables: default_retention_exclude_tables(),
            },
            wal: WalConfig {
                enabled: default_wal_enabled(),
                sync_on_write: default_wal_sync_on_write(),
                rotate_size_mb: default_wal_rotate_size_mb(),
                cleanup_interval_secs: default_wal_cleanup_interval_secs(),
                retention_hours: default_wal_retention_hours(),
            },
            auth: AuthConfig {
                enabled: default_auth_enabled(),
                username: default_auth_username(),
                password: default_auth_password(),
                public_endpoints: default_auth_public_endpoints(),
            },
        }
    }
}

impl Config {
    /// Load configuration from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read config file: {}", e))?;

        let config: Config =
            toml::from_str(&content).map_err(|e| format!("Failed to parse config file: {}", e))?;

        Ok(config)
    }

    /// Load configuration with fallback to default
    /// Tries to load from file, if not found uses default
    pub fn load_or_default(path: &str) -> Self {
        match Self::from_file(path) {
            Ok(config) => {
                // Note: This is called before logging is initialized, so we use eprintln
                // It will only show during startup before logging takes over
                info!("Loaded configuration from: {}", path);
                config
            }
            Err(e) => {
                error!(
                    "Could not load config file, path: {}, error: {}. Using default configuration",
                    path, e
                );
                Config::default()
            }
        }
    }

    /// Get HTTP server address
    pub fn http_addr(&self) -> String {
        format!("{}:{}", self.server.http_host, self.server.http_port)
    }

    /// Get Flight server address
    pub fn flight_addr(&self) -> String {
        format!("{}:{}", self.server.flight_host, self.server.flight_port)
    }

    /// Get MySQL server address
    pub fn mysql_addr(&self) -> String {
        format!("{}:{}", self.server.mysql_host, self.server.mysql_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.http_port, 7000);
        assert_eq!(config.server.flight_port, 7001);
        assert_eq!(config.cache.flush_interval_secs, 30);
    }

    #[test]
    fn test_toml_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.server.http_port, parsed.server.http_port);
    }
}
