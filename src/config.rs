use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub schema: SchemaConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub query_cache: QueryCacheConfig,
    #[serde(default)]
    pub hot_partition_cache: HotPartitionCacheConfig,
    #[serde(default)]
    pub partition_files_cache: PartitionFilesCacheConfig,
    #[serde(default)]
    pub query: QueryConfig,
    /// Streaming query configuration (WebSocket/SSE subscriptions)
    #[serde(default)]
    pub streaming: StreamingConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub wal: WalConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    /// Custom role definitions for RBAC
    #[serde(default)]
    pub roles: RolesConfig,
    /// Audit logging configuration
    #[serde(default)]
    pub audit: AuditConfig,
    /// Continuous aggregates configuration (materialized views with auto-refresh)
    #[serde(default)]
    pub continuous_aggregates: crate::aggregates::ContinuousAggregateConfig,
    /// Meta node configuration (for --role meta)
    #[serde(default)]
    pub meta: Option<crate::meta::config::MetaConfig>,
    /// Router node configuration (for --role router)
    #[serde(default)]
    pub router: Option<crate::router::config::RouterConfig>,
    /// Data node configuration (for --role data)
    #[serde(default)]
    pub data: Option<crate::data::config::DataNodeConfig>,
    /// Admin console configuration (for --role admin)
    #[serde(default)]
    pub admin: Option<AdminConfig>,
    /// Query tracing and profiling configuration
    #[serde(default)]
    pub tracing: crate::observability::TracingConfig,
    /// Automated downsampling configuration
    #[serde(default)]
    pub downsampling: DownsamplingConfig,
    /// TLS/SSL encryption configuration
    #[serde(default)]
    pub tls: TlsConfig,
}

/// Admin console configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminConfig {
    /// HTTP server host
    #[serde(default = "default_admin_host")]
    pub http_host: String,

    /// HTTP server port
    #[serde(default = "default_admin_port")]
    pub http_port: u16,

    /// Meta service endpoints (gRPC)
    #[serde(default)]
    pub meta_endpoints: Vec<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            http_host: default_admin_host(),
            http_port: default_admin_port(),
            meta_endpoints: Vec::new(),
        }
    }
}

fn default_admin_host() -> String {
    "0.0.0.0".to_string()
}

fn default_admin_port() -> u16 {
    8080
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

/// Storage configuration (supports local filesystem and S3-compatible storage)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend: "local", "s3", "minio", "oss"
    #[serde(default = "default_storage_backend")]
    pub backend: String,

    /// Data directory for local storage
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// Compression strategy configuration
    #[serde(default)]
    pub compression: CompressionConfig,

    /// S3 bucket name
    #[serde(default)]
    pub bucket: String,

    /// S3 region
    #[serde(default = "default_s3_region")]
    pub region: String,

    /// S3 access key ID
    #[serde(default)]
    pub access_key: Option<String>,

    /// S3 secret access key
    #[serde(default)]
    pub secret_key: Option<String>,

    /// Custom endpoint URL (for MinIO, Aliyun OSS)
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Path prefix within bucket
    #[serde(default)]
    pub prefix: Option<String>,

    /// Allow HTTP connections (for local MinIO)
    #[serde(default)]
    pub allow_http: bool,

    /// File cache size in GB (for object store backends)
    #[serde(default)]
    pub file_cache_size_gb: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            data_dir: default_data_dir(),
            compression: CompressionConfig::default(),
            bucket: String::new(),
            region: default_s3_region(),
            access_key: None,
            secret_key: None,
            endpoint: None,
            prefix: None,
            allow_http: false,
            file_cache_size_gb: None,
        }
    }
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

/// Cache configuration (write cache)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Background flush interval in seconds
    #[serde(default = "default_cache_flush_interval")]
    pub flush_interval_secs: u64,

    /// Maximum cache size in MB before forcing flush
    #[serde(default = "default_cache_max_size_mb")]
    pub max_size_mb: u64,

    /// Local disk cache size for Parquet files in MB (cluster mode only)
    ///
    /// When set, Data nodes cache Parquet files on local disk for fast repeated access.
    /// Only files owned by this node (prefixed with node_id) are cached.
    /// Uses LRU eviction when cache is full.
    ///
    /// Recommended: 2-4x the size of the write cache for optimal query performance.
    #[serde(default)]
    pub parquet_cache_mb: Option<usize>,
}

/// Query result cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCacheConfig {
    /// Enable query result caching
    #[serde(default = "default_query_cache_enabled")]
    pub enabled: bool,

    /// Maximum cache size in MB
    #[serde(default = "default_query_cache_max_size_mb")]
    pub max_size_mb: u64,

    /// Maximum number of cached entries
    #[serde(default = "default_query_cache_max_entries")]
    pub max_entries: usize,

    /// TTL for cached entries in seconds
    #[serde(default = "default_query_cache_ttl_secs")]
    pub ttl_secs: u64,

    /// Minimum query execution time (ms) to cache result
    /// Only cache queries that take longer than this threshold
    #[serde(default = "default_query_cache_min_query_time_ms")]
    pub min_query_time_ms: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_query_cache_enabled(),
            max_size_mb: default_query_cache_max_size_mb(),
            max_entries: default_query_cache_max_entries(),
            ttl_secs: default_query_cache_ttl_secs(),
            min_query_time_ms: default_query_cache_min_query_time_ms(),
        }
    }
}

fn default_query_cache_enabled() -> bool {
    true
}

fn default_query_cache_max_size_mb() -> u64 {
    256
}

fn default_query_cache_max_entries() -> usize {
    1000
}

fn default_query_cache_ttl_secs() -> u64 {
    300 // 5 minutes
}

fn default_query_cache_min_query_time_ms() -> u64 {
    10
}

/// Hot partition cache configuration
/// Caches decoded RecordBatches for recent partitions (today, yesterday)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotPartitionCacheConfig {
    /// Enable hot partition caching
    #[serde(default = "default_hot_partition_enabled")]
    pub enabled: bool,

    /// Maximum cache size in MB
    #[serde(default = "default_hot_partition_max_size_mb")]
    pub max_size_mb: u64,

    /// Number of recent days to cache (0 = today only, 1 = today + yesterday)
    #[serde(default = "default_hot_partition_hot_days")]
    pub hot_days: u32,

    /// TTL for cached entries in seconds
    #[serde(default = "default_hot_partition_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for HotPartitionCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_hot_partition_enabled(),
            max_size_mb: default_hot_partition_max_size_mb(),
            hot_days: default_hot_partition_hot_days(),
            ttl_secs: default_hot_partition_ttl_secs(),
        }
    }
}

fn default_hot_partition_enabled() -> bool {
    true
}

fn default_hot_partition_max_size_mb() -> u64 {
    1024 // 1GB
}

fn default_hot_partition_hot_days() -> u32 {
    2 // Today + yesterday
}

fn default_hot_partition_ttl_secs() -> u64 {
    1800 // 30 minutes
}

/// Partition files cache configuration
///
/// Caches S3 LIST results for partition file discovery at query time.
/// Used with table-level routing (see docs/architecture-meta-routing.md).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFilesCacheConfig {
    /// Enable partition files caching
    #[serde(default = "default_partition_files_cache_enabled")]
    pub enabled: bool,

    /// Cache TTL in seconds (how long to cache file listings)
    #[serde(default = "default_partition_files_cache_ttl_secs")]
    pub ttl_secs: u64,

    /// Maximum number of partitions to cache
    #[serde(default = "default_partition_files_cache_max_entries")]
    pub max_entries: usize,
}

impl Default for PartitionFilesCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_partition_files_cache_enabled(),
            ttl_secs: default_partition_files_cache_ttl_secs(),
            max_entries: default_partition_files_cache_max_entries(),
        }
    }
}

fn default_partition_files_cache_enabled() -> bool {
    true
}

fn default_partition_files_cache_ttl_secs() -> u64 {
    30 // 30 seconds - short TTL to see new files quickly
}

fn default_partition_files_cache_max_entries() -> usize {
    10000 // 10K partitions
}

/// Query engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Use custom ParquetTableProvider instead of DataFusion's ListingTable
    /// When false (default): Uses DataFusion's built-in ListingTable and register_parquet
    /// When true: Uses custom ParquetTableProvider with manual Parquet reading
    #[serde(default = "default_use_custom_table_provider")]
    pub use_custom_table_provider: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            use_custom_table_provider: default_use_custom_table_provider(),
        }
    }
}

fn default_use_custom_table_provider() -> bool {
    false // Default to original ListingTable approach
}

/// Streaming query configuration (WebSocket & SSE subscriptions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// Enable streaming query endpoints
    #[serde(default = "default_streaming_enabled")]
    pub enabled: bool,
    /// Maximum concurrent subscriptions across all connections
    #[serde(default = "default_streaming_max_subscriptions")]
    pub max_subscriptions: usize,
    /// Maximum subscriptions per individual connection
    #[serde(default = "default_streaming_max_per_connection")]
    pub max_subscriptions_per_connection: usize,
    /// Debounce interval for batching update notifications (milliseconds)
    #[serde(default = "default_streaming_debounce_ms")]
    pub update_debounce_ms: u64,
    /// Maximum buffered messages before applying backpressure (drop oldest)
    #[serde(default = "default_streaming_backpressure")]
    pub backpressure_threshold: usize,
    /// WebSocket heartbeat interval (seconds)
    #[serde(default = "default_streaming_heartbeat_secs")]
    pub heartbeat_interval_secs: u64,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            enabled: default_streaming_enabled(),
            max_subscriptions: default_streaming_max_subscriptions(),
            max_subscriptions_per_connection: default_streaming_max_per_connection(),
            update_debounce_ms: default_streaming_debounce_ms(),
            backpressure_threshold: default_streaming_backpressure(),
            heartbeat_interval_secs: default_streaming_heartbeat_secs(),
        }
    }
}

fn default_streaming_enabled() -> bool {
    false
}
fn default_streaming_max_subscriptions() -> usize {
    10000
}
fn default_streaming_max_per_connection() -> usize {
    10
}
fn default_streaming_debounce_ms() -> u64 {
    100
}
fn default_streaming_backpressure() -> usize {
    1000
}
fn default_streaming_heartbeat_secs() -> u64 {
    30
}

/// Automated downsampling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsamplingConfig {
    /// Enable automated downsampling
    #[serde(default = "default_downsampling_enabled")]
    pub enabled: bool,
    /// Interval in seconds between downsampling checks
    #[serde(default = "default_downsampling_check_interval")]
    pub check_interval_secs: u64,
    /// Maximum concurrent downsampling jobs
    #[serde(default = "default_downsampling_max_concurrent")]
    pub max_concurrent_jobs: usize,
    /// Enable transparent query rewriting to use downsampled data
    #[serde(default = "default_downsampling_query_rewriting")]
    pub query_rewriting_enabled: bool,
}

impl Default for DownsamplingConfig {
    fn default() -> Self {
        Self {
            enabled: default_downsampling_enabled(),
            check_interval_secs: default_downsampling_check_interval(),
            max_concurrent_jobs: default_downsampling_max_concurrent(),
            query_rewriting_enabled: default_downsampling_query_rewriting(),
        }
    }
}

fn default_downsampling_enabled() -> bool {
    false
}
fn default_downsampling_check_interval() -> u64 {
    3600
}
fn default_downsampling_max_concurrent() -> usize {
    4
}
fn default_downsampling_query_rewriting() -> bool {
    true
}

/// Compression strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Compression strategy: "fixed" or "adaptive"
    #[serde(default = "default_compression_strategy")]
    pub strategy: String,
    /// Default compression codec: "zstd", "snappy", "lz4", "uncompressed"
    #[serde(default = "default_compression_codec")]
    pub default_codec: String,
    /// ZSTD compression level (1-22, only used when codec is "zstd")
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    /// Number of days before data is considered "hot" (uses fast codec)
    #[serde(default = "default_hot_days")]
    pub hot_days: u32,
    /// Number of days before data is considered "cold" (uses max compression)
    #[serde(default = "default_cold_days")]
    pub cold_days: u32,
    /// Codec for hot/recent data (default: "snappy")
    #[serde(default = "default_hot_codec")]
    pub hot_codec: String,
    /// Codec for cold/old data (default: "zstd")
    #[serde(default = "default_cold_codec")]
    pub cold_codec: String,
    /// ZSTD level for cold data (default: 9)
    #[serde(default = "default_cold_zstd_level")]
    pub cold_zstd_level: i32,
    /// Codec for integer/float columns (default: "lz4")
    #[serde(default = "default_numeric_codec")]
    pub numeric_codec: String,
    /// Codec for string columns with high cardinality (default: "zstd")
    #[serde(default = "default_string_codec")]
    pub string_codec: String,
    /// Enable background recompression job
    #[serde(default)]
    pub recompression_enabled: bool,
    /// Recompression check interval in seconds
    #[serde(default = "default_recompression_interval")]
    pub recompression_interval_secs: u64,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            strategy: default_compression_strategy(),
            default_codec: default_compression_codec(),
            zstd_level: default_zstd_level(),
            hot_days: default_hot_days(),
            cold_days: default_cold_days(),
            hot_codec: default_hot_codec(),
            cold_codec: default_cold_codec(),
            cold_zstd_level: default_cold_zstd_level(),
            numeric_codec: default_numeric_codec(),
            string_codec: default_string_codec(),
            recompression_enabled: false,
            recompression_interval_secs: default_recompression_interval(),
        }
    }
}

fn default_compression_strategy() -> String {
    "fixed".to_string()
}
fn default_compression_codec() -> String {
    "zstd".to_string()
}
fn default_zstd_level() -> i32 {
    3
}
fn default_hot_days() -> u32 {
    7
}
fn default_cold_days() -> u32 {
    30
}
fn default_hot_codec() -> String {
    "snappy".to_string()
}
fn default_cold_codec() -> String {
    "zstd".to_string()
}
fn default_cold_zstd_level() -> i32 {
    9
}
fn default_numeric_codec() -> String {
    "lz4".to_string()
}
fn default_string_codec() -> String {
    "zstd".to_string()
}
fn default_recompression_interval() -> u64 {
    86400 // Daily
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

    /// WAL directory path (relative to storage root, default: "wal")
    #[serde(default = "default_wal_dir")]
    pub wal_dir: String,

    /// Sync to disk after each write (true=durable, false=faster)
    /// Note: For object stores, this controls whether to flush buffer after each write
    #[serde(default = "default_wal_sync_on_write")]
    pub sync_on_write: bool,

    /// Rotate WAL file when it exceeds this size (MB)
    #[serde(default = "default_wal_rotate_size_mb")]
    pub rotate_size_mb: u64,

    /// Maximum entries to buffer before forcing a write (for object stores)
    #[serde(default = "default_wal_buffer_entries")]
    pub buffer_entries: usize,

    /// Background cleanup interval in seconds
    #[serde(default = "default_wal_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    /// Retention period for WAL files in hours (files older than this can be deleted)
    #[serde(default = "default_wal_retention_hours")]
    pub retention_hours: u64,
}

/// Authentication and authorization configuration (RBAC)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Enable authentication (default: false)
    #[serde(default = "default_auth_enabled")]
    pub enabled: bool,

    /// User definitions with roles and permissions
    #[serde(default)]
    pub users: Vec<AuthUserConfig>,

    /// Path to external users file (optional, alternative to inline users)
    #[serde(default)]
    pub users_file: Option<String>,
}

/// User configuration for RBAC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUserConfig {
    /// Username (unique identifier)
    pub username: String,

    /// Password hash (Argon2) - use `mynadb hash-password` to generate
    pub password_hash: String,

    /// Role names: "admin" (all permissions), "writer" (read+write), "reader" (read-only)
    #[serde(default)]
    pub roles: Vec<String>,

    /// Optional: restrict access to specific databases (supports wildcards like "prod_*")
    #[serde(default)]
    pub databases: Option<Vec<String>>,

    /// Optional: restrict access to specific tables (format: "database.schema.table")
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

/// Custom role definitions for RBAC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolesConfig {
    /// Custom role definitions
    #[serde(default)]
    pub custom_roles: Vec<CustomRoleConfig>,
}

impl Default for RolesConfig {
    fn default() -> Self {
        Self {
            custom_roles: Vec::new(),
        }
    }
}

/// A custom role mapping a name to a set of permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomRoleConfig {
    /// Role name (unique identifier)
    pub name: String,
    /// Permissions granted by this role: "read", "write", "admin"
    #[serde(default)]
    pub permissions: Vec<String>,
}

/// Audit logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Enable audit logging (default: false)
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,

    /// Directory for audit log files
    #[serde(default = "default_audit_log_dir")]
    pub log_dir: String,

    /// Audit log file name prefix
    #[serde(default = "default_audit_file_prefix")]
    pub file_prefix: String,

    /// Log file rotation: "daily", "hourly", "never"
    #[serde(default = "default_audit_rotation")]
    pub rotation: String,

    /// Maximum number of audit log files to keep
    #[serde(default = "default_audit_max_files")]
    pub max_files: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_enabled(),
            log_dir: default_audit_log_dir(),
            file_prefix: default_audit_file_prefix(),
            rotation: default_audit_rotation(),
            max_files: default_audit_max_files(),
        }
    }
}

fn default_audit_enabled() -> bool {
    false
}

fn default_audit_log_dir() -> String {
    "./logs".to_string()
}

fn default_audit_file_prefix() -> String {
    "audit".to_string()
}

fn default_audit_rotation() -> String {
    "daily".to_string()
}

fn default_audit_max_files() -> usize {
    90
}

/// TLS/SSL encryption configuration
///
/// When enabled, all protocols (HTTP, Arrow Flight, MySQL) use TLS encryption.
/// Requires PEM-encoded certificate and private key files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS encryption (default: false)
    #[serde(default = "default_tls_enabled")]
    pub enabled: bool,

    /// Path to PEM-encoded certificate file (server certificate chain)
    #[serde(default)]
    pub cert_file: String,

    /// Path to PEM-encoded private key file
    #[serde(default)]
    pub key_file: String,

    /// Path to PEM-encoded CA certificate file (optional, enables mTLS)
    #[serde(default)]
    pub ca_file: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: default_tls_enabled(),
            cert_file: String::new(),
            key_file: String::new(),
            ca_file: None,
        }
    }
}

impl TlsConfig {
    /// Validate the TLS configuration
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.cert_file.is_empty() {
            return Err("TLS enabled but cert_file is not set".to_string());
        }
        if self.key_file.is_empty() {
            return Err("TLS enabled but key_file is not set".to_string());
        }
        if !Path::new(&self.cert_file).exists() {
            return Err(format!("TLS cert_file not found: {}", self.cert_file));
        }
        if !Path::new(&self.key_file).exists() {
            return Err(format!("TLS key_file not found: {}", self.key_file));
        }
        if let Some(ref ca_file) = self.ca_file {
            if !ca_file.is_empty() && !Path::new(ca_file).exists() {
                return Err(format!("TLS ca_file not found: {}", ca_file));
            }
        }
        Ok(())
    }
}

fn default_tls_enabled() -> bool {
    false
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

fn default_storage_backend() -> String {
    "local".to_string()
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

fn default_cache_flush_interval() -> u64 {
    60
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

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_dir() -> String {
    "./logs".to_string()
}

fn default_log_file_prefix() -> String {
    "mynadb".to_string()
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

fn default_wal_dir() -> String {
    "wal".to_string()
}

fn default_wal_sync_on_write() -> bool {
    false // Default to faster mode
}

fn default_wal_rotate_size_mb() -> u64 {
    256
}

fn default_wal_buffer_entries() -> usize {
    1_000_000 // Buffer up to 1000000 entries before forcing write (for object stores)
}

fn default_wal_cleanup_interval_secs() -> u64 {
    300 // 5 minutes - more frequent cleanup to prevent empty file accumulation
}

fn default_wal_retention_hours() -> u64 {
    24 // Keep WAL files for 24 hours
}

fn default_auth_enabled() -> bool {
    false
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_host: default_http_host(),
            http_port: default_http_port(),
            flight_host: default_flight_host(),
            flight_port: default_flight_port(),
            mysql_host: default_mysql_host(),
            mysql_port: default_mysql_port(),
            mysql_enabled: default_mysql_enabled(),
        }
    }
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            strategy: default_schema_strategy(),
            tag_keys: default_schema_tag_keys(),
            default_name: default_schema_name(),
            prefix_numeric: default_schema_prefix_numeric(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            flush_interval_secs: default_cache_flush_interval(),
            max_size_mb: default_cache_max_size_mb(),
            parquet_cache_mb: None, // Disabled by default, enable in cluster mode
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: default_wal_enabled(),
            wal_dir: default_wal_dir(),
            sync_on_write: default_wal_sync_on_write(),
            rotate_size_mb: default_wal_rotate_size_mb(),
            buffer_entries: default_wal_buffer_entries(),
            cleanup_interval_secs: default_wal_cleanup_interval_secs(),
            retention_hours: default_wal_retention_hours(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: default_auth_enabled(),
            users: Vec::new(),
            users_file: None,
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
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
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            log_dir: default_log_dir(),
            file_prefix: default_log_file_prefix(),
            rotation: default_log_rotation(),
            max_files: default_log_max_files(),
            json_format: default_log_json(),
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: default_retention_enabled(),
            default_days: default_retention_days(),
            interval_secs: default_retention_interval_secs(),
            exclude_databases: default_retention_exclude_databases(),
            exclude_tables: default_retention_exclude_tables(),
        }
    }
}


/// Resolve environment variables in a string
///
/// Supports patterns:
/// - `${VAR}` - replaced with env var value, error if not set
/// - `${VAR:-default}` - replaced with env var value, or default if not set
///
/// The default value itself can contain env vars for nested resolution:
/// - `${VAR:-${OTHER_VAR}}` - use OTHER_VAR if VAR is not set
pub fn resolve_env_vars(content: &str) -> Result<String, String> {
    // Pattern matches ${VAR} or ${VAR:-default}
    // Captures: VAR name and optional default value
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
        .map_err(|e| format!("Invalid regex: {}", e))?;

    let mut result = content.to_string();
    let mut iterations = 0;
    const MAX_ITERATIONS: usize = 10; // Prevent infinite loops in nested resolution

    // Keep resolving until no more patterns or max iterations
    while re.is_match(&result) && iterations < MAX_ITERATIONS {
        let mut new_result = result.clone();

        for cap in re.captures_iter(&result) {
            let full_match = cap.get(0).unwrap().as_str();
            let var_name = cap.get(1).unwrap().as_str();
            let default_value = cap.get(2).map(|m| m.as_str());

            let replacement = match std::env::var(var_name) {
                Ok(val) => val,
                Err(_) => match default_value {
                    Some(default) => default.to_string(),
                    None => {
                        return Err(format!(
                            "Environment variable '{}' is not set and no default provided",
                            var_name
                        ));
                    }
                },
            };

            new_result = new_result.replacen(full_match, &replacement, 1);
        }

        result = new_result;
        iterations += 1;
    }

    Ok(result)
}

/// Resolve a single environment variable with optional default
///
/// Returns the value of the environment variable, or the default if not set.
/// If no default is provided and the variable is not set, returns an error.
pub fn resolve_env_var(var_name: &str, default: Option<&str>) -> Result<String, String> {
    match std::env::var(var_name) {
        Ok(val) => Ok(val),
        Err(_) => match default {
            Some(d) => Ok(d.to_string()),
            None => Err(format!("Environment variable '{}' is not set", var_name)),
        },
    }
}

/// Get stable node ID from environment
///
/// Priority: MYNADB_NODE_ID > HOSTNAME > generated UUID
pub fn get_stable_node_id(prefix: &str) -> String {
    std::env::var("MYNADB_NODE_ID")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| {
            format!(
                "{}-{}",
                prefix,
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            )
        })
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

    /// Load configuration from TOML file with environment variable resolution
    ///
    /// Supports patterns:
    /// - `${VAR}` - replaced with env var value, error if not set
    /// - `${VAR:-default}` - replaced with env var value, or default if not set
    ///
    /// Example TOML:
    /// ```toml
    /// [data_node]
    /// node_id = "${MYNADB_NODE_ID:-${HOSTNAME}}"
    /// advertise_addr = "${MYNADB_ADVERTISE_ADDR}"
    /// ```
    pub fn from_file_with_env<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let content =
            fs::read_to_string(&path).map_err(|e| format!("Failed to read config file: {}", e))?;

        // Resolve environment variables in the TOML content
        let resolved = resolve_env_vars(&content)?;

        let config: Config =
            toml::from_str(&resolved).map_err(|e| format!("Failed to parse config file: {}", e))?;

        Ok(config)
    }

    /// Load configuration with environment variable support and fallback to default
    ///
    /// Priority for configuration values:
    /// 1. TOML file values (with env var resolution)
    /// 2. Direct environment variable overrides (MYNADB_*)
    /// 3. Default values
    pub fn load_with_env(path: &str) -> Self {
        let mut config = match Self::from_file_with_env(path) {
            Ok(config) => {
                eprintln!("[config] Loaded configuration from: {}", path);
                config
            }
            Err(e) => {
                eprintln!("[config] Could not load config file '{}': {}", path, e);
                eprintln!("[config] Using default configuration");
                Config::default()
            }
        };

        // Apply direct environment variable overrides
        config.apply_env_overrides();
        config
    }

    /// Load configuration with fallback to default
    /// Tries to load from file, if not found uses default
    pub fn load_or_default(path: &str) -> Self {
        match Self::from_file(path) {
            Ok(config) => {
                // Note: This is called before logging is initialized, so we use eprintln
                eprintln!("[config] Loaded configuration from: {}", path);
                config
            }
            Err(e) => {
                // Use eprintln since logging is not yet initialized
                eprintln!(
                    "[config] ERROR: Could not load config file '{}': {}",
                    path, e
                );
                eprintln!("[config] Using default configuration");
                Config::default()
            }
        }
    }

    /// Apply direct environment variable overrides
    ///
    /// Supported variables:
    /// - MYNADB_HTTP_PORT
    /// - MYNADB_FLIGHT_PORT
    /// - MYNADB_DATA_DIR
    /// - MYNADB_LOG_LEVEL
    fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var("MYNADB_HTTP_PORT") {
            if let Ok(port) = val.parse() {
                self.server.http_port = port;
            }
        }
        if let Ok(val) = std::env::var("MYNADB_FLIGHT_PORT") {
            if let Ok(port) = val.parse() {
                self.server.flight_port = port;
            }
        }
        if let Ok(val) = std::env::var("MYNADB_MYSQL_PORT") {
            if let Ok(port) = val.parse() {
                self.server.mysql_port = port;
            }
        }
        if let Ok(val) = std::env::var("MYNADB_DATA_DIR") {
            self.storage.data_dir = val;
        }
        if let Ok(val) = std::env::var("MYNADB_LOG_LEVEL") {
            self.logging.level = val;
        }
        if let Ok(val) = std::env::var("MYNADB_LOG_DIR") {
            self.logging.log_dir = val;
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
        assert_eq!(config.cache.flush_interval_secs, 60);
    }

    #[test]
    fn test_toml_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.server.http_port, parsed.server.http_port);
    }

    #[test]
    fn test_resolve_env_vars_simple() {
        std::env::set_var("TEST_VAR_1", "hello");
        let result = resolve_env_vars("value is ${TEST_VAR_1}").unwrap();
        assert_eq!(result, "value is hello");
        std::env::remove_var("TEST_VAR_1");
    }

    #[test]
    fn test_resolve_env_vars_with_default() {
        std::env::remove_var("TEST_VAR_UNSET");
        let result = resolve_env_vars("value is ${TEST_VAR_UNSET:-default_value}").unwrap();
        assert_eq!(result, "value is default_value");
    }

    #[test]
    fn test_resolve_env_vars_override_default() {
        std::env::set_var("TEST_VAR_2", "actual");
        let result = resolve_env_vars("value is ${TEST_VAR_2:-default}").unwrap();
        assert_eq!(result, "value is actual");
        std::env::remove_var("TEST_VAR_2");
    }

    #[test]
    fn test_resolve_env_vars_missing_no_default() {
        std::env::remove_var("TEST_VAR_MISSING");
        let result = resolve_env_vars("value is ${TEST_VAR_MISSING}");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("TEST_VAR_MISSING"));
    }

    #[test]
    fn test_resolve_env_vars_nested() {
        std::env::set_var("TEST_OUTER", "from_outer");
        std::env::remove_var("TEST_INNER");
        // When INNER is not set, it uses the default which references OUTER
        let result = resolve_env_vars("${TEST_INNER:-${TEST_OUTER}}").unwrap();
        assert_eq!(result, "from_outer");
        std::env::remove_var("TEST_OUTER");
    }

    #[test]
    fn test_resolve_env_vars_multiple() {
        std::env::set_var("TEST_A", "aaa");
        std::env::set_var("TEST_B", "bbb");
        let result = resolve_env_vars("${TEST_A}-${TEST_B}").unwrap();
        assert_eq!(result, "aaa-bbb");
        std::env::remove_var("TEST_A");
        std::env::remove_var("TEST_B");
    }

    #[test]
    fn test_resolve_env_vars_toml() {
        std::env::set_var("TEST_NODE_ID", "data-1");
        std::env::set_var("TEST_PORT", "7200");
        let toml_content = r#"
[data_node]
node_id = "${TEST_NODE_ID}"
grpc_port = ${TEST_PORT}
"#;
        let result = resolve_env_vars(toml_content).unwrap();
        assert!(result.contains("node_id = \"data-1\""));
        assert!(result.contains("grpc_port = 7200"));
        std::env::remove_var("TEST_NODE_ID");
        std::env::remove_var("TEST_PORT");
    }

    // Combined into single test to avoid env var races
    #[test]
    fn test_get_stable_node_id() {
        // Test 1: from env var
        std::env::set_var("MYNADB_NODE_ID", "my-custom-id");
        let id = get_stable_node_id("data");
        assert_eq!(id, "my-custom-id");

        // Test 2: from hostname (clear MYNADB_NODE_ID first)
        std::env::remove_var("MYNADB_NODE_ID");
        std::env::set_var("HOSTNAME", "mynadb-data-0");
        let id = get_stable_node_id("data");
        assert_eq!(id, "mynadb-data-0");

        // Test 3: fallback (clear both)
        std::env::remove_var("HOSTNAME");
        let id = get_stable_node_id("data");
        assert!(id.starts_with("data-"));
    }
}
