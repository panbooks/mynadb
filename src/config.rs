use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use tracing::{error, info};

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub cache: CacheConfig,
    pub compaction: CompactionConfig,
    pub series_tracker: SeriesTrackerConfig,
    pub logging: LoggingConfig,
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
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Data directory path
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// System directory name (for metadata)
    #[serde(default = "default_system_dir")]
    pub system_dir: String,
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

    /// Maximum file size in MB before compaction
    #[serde(default = "default_compaction_max_file_size_mb")]
    pub max_file_size_mb: u64,

    /// Target file size in MB after compaction
    #[serde(default = "default_compaction_target_file_size_mb")]
    pub target_file_size_mb: u64,

    /// Maximum compaction level
    #[serde(default = "default_compaction_max_level")]
    pub max_level: u32,
    
    /// Background compaction interval in seconds (0 = disabled)
    #[serde(default = "default_compaction_interval_secs")]
    pub interval_secs: u64,
}

/// Series tracker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesTrackerConfig {
    /// Background flush interval in seconds
    #[serde(default = "default_series_flush_interval")]
    pub flush_interval_secs: u64,

    /// Series stats directory name
    #[serde(default = "default_series_stats_dir")]
    pub stats_dir: String,
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
    1024 // 1GB
}

fn default_compaction_min_files() -> usize {
    5
}

fn default_compaction_major_min_files() -> usize {
    4
}

fn default_compaction_max_file_size_mb() -> u64 {
    100
}

fn default_compaction_target_file_size_mb() -> u64 {
    500
}

fn default_compaction_max_level() -> u32 {
    4 // L0, L1, L2, L3, L4
}

fn default_compaction_interval_secs() -> u64 {
    60 // 5 min
}

fn default_series_flush_interval() -> u64 {
    60
}

fn default_series_stats_dir() -> String {
    "series_stats".to_string()
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

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig {
                http_host: default_http_host(),
                http_port: default_http_port(),
                flight_host: default_flight_host(),
                flight_port: default_flight_port(),
            },
            storage: StorageConfig {
                data_dir: default_data_dir(),
                system_dir: default_system_dir(),
            },
            cache: CacheConfig {
                flush_interval_secs: default_cache_flush_interval(),
                max_size_mb: default_cache_max_size_mb(),
            },
            compaction: CompactionConfig {
                min_files: default_compaction_min_files(),
                major_min_files: default_compaction_major_min_files(),
                max_file_size_mb: default_compaction_max_file_size_mb(),
                target_file_size_mb: default_compaction_target_file_size_mb(),
                max_level: default_compaction_max_level(),
                interval_secs: default_compaction_interval_secs(),
            },
            series_tracker: SeriesTrackerConfig {
                flush_interval_secs: default_series_flush_interval(),
                stats_dir: default_series_stats_dir(),
            },
            logging: LoggingConfig {
                level: default_log_level(),
                log_dir: default_log_dir(),
                file_prefix: default_log_file_prefix(),
                rotation: default_log_rotation(),
                max_files: default_log_max_files(),
                json_format: default_log_json(),
            },
        }
    }
}

impl Config {
    /// Load configuration from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;

        let config: Config = toml::from_str(&content)
            .map_err(|e| format!("Failed to parse config file: {}", e))?;

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
                error!("Could not load config file, path: {}, error: {}. Using default configuration", path, e);
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
