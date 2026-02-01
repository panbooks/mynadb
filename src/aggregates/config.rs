//! Configuration for Continuous Aggregates
//!
//! This module provides configuration structures that integrate with
//! the main MynaDB configuration system.

use serde::{Deserialize, Serialize};

/// Configuration for the continuous aggregates feature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousAggregateConfig {
    /// Enable or disable the continuous aggregates feature
    /// When disabled, no aggregates are refreshed and query rewriting is skipped
    #[serde(default)]
    pub enabled: bool,

    /// Interval in seconds between refresh checks
    /// The job will check all aggregates for pending refreshes at this interval
    #[serde(default = "default_refresh_check_interval")]
    pub refresh_check_interval_secs: u64,

    /// Maximum concurrent refresh operations
    /// Limits resource usage during heavy refresh periods
    #[serde(default = "default_max_concurrent_refreshes")]
    pub max_concurrent_refreshes: usize,

    /// Default refresh interval for new aggregates (in seconds)
    /// Individual aggregates can override this
    #[serde(default = "default_refresh_interval")]
    pub default_refresh_interval_secs: u64,

    /// Default retention days for aggregate data
    /// Individual aggregates can override this
    #[serde(default = "default_retention_days")]
    pub default_retention_days: u32,

    /// Enable query rewriting to use aggregates
    /// When enabled, queries are automatically rewritten to use pre-aggregated data
    #[serde(default = "default_query_rewriting")]
    pub query_rewriting_enabled: bool,

    /// Minimum time bucket ratio for query rewriting
    /// Query bucket must be >= N times the aggregate bucket to use rewrite
    /// E.g., if aggregate is 1min and ratio is 1, then 1min+ queries use aggregate
    #[serde(default = "default_min_bucket_ratio")]
    pub min_bucket_ratio: u32,

    /// Directory for storing aggregate definitions (relative to data_dir)
    #[serde(default = "default_definitions_dir")]
    pub definitions_dir: String,

    /// Enable real-time union queries
    /// When true, queries combine aggregate data with real-time raw data
    /// When false, only aggregate data is returned (slightly stale)
    #[serde(default = "default_realtime_union")]
    pub realtime_union_enabled: bool,

    /// Lag tolerance in seconds for real-time queries
    /// If watermark is within this time of now, skip real-time union
    #[serde(default = "default_lag_tolerance")]
    pub lag_tolerance_secs: u64,

    /// Enable hierarchical aggregates (aggregates on aggregates)
    #[serde(default = "default_hierarchical")]
    pub hierarchical_enabled: bool,
}

impl Default for ContinuousAggregateConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for backward compatibility
            refresh_check_interval_secs: default_refresh_check_interval(),
            max_concurrent_refreshes: default_max_concurrent_refreshes(),
            default_refresh_interval_secs: default_refresh_interval(),
            default_retention_days: default_retention_days(),
            query_rewriting_enabled: default_query_rewriting(),
            min_bucket_ratio: default_min_bucket_ratio(),
            definitions_dir: default_definitions_dir(),
            realtime_union_enabled: default_realtime_union(),
            lag_tolerance_secs: default_lag_tolerance(),
            hierarchical_enabled: default_hierarchical(),
        }
    }
}

fn default_refresh_check_interval() -> u64 {
    60 // Check every minute
}

fn default_max_concurrent_refreshes() -> usize {
    4 // Up to 4 aggregates refreshing at once
}

fn default_refresh_interval() -> u64 {
    300 // 5 minutes default
}

fn default_retention_days() -> u32 {
    365 // 1 year default
}

fn default_query_rewriting() -> bool {
    true // Enable query rewriting by default when feature is enabled
}

fn default_min_bucket_ratio() -> u32 {
    1 // Query bucket must be >= aggregate bucket
}

fn default_definitions_dir() -> String {
    "_aggregates".to_string()
}

fn default_realtime_union() -> bool {
    true // Combine aggregate + real-time data
}

fn default_lag_tolerance() -> u64 {
    60 // 1 minute tolerance
}

fn default_hierarchical() -> bool {
    true // Allow aggregates on aggregates
}

impl ContinuousAggregateConfig {
    /// Create a config with the feature enabled
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Builder-style method to set refresh interval
    pub fn with_refresh_interval(mut self, secs: u64) -> Self {
        self.refresh_check_interval_secs = secs;
        self
    }

    /// Builder-style method to set max concurrent refreshes
    pub fn with_max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent_refreshes = n;
        self
    }

    /// Builder-style method to disable query rewriting
    pub fn without_query_rewriting(mut self) -> Self {
        self.query_rewriting_enabled = false;
        self
    }

    /// Builder-style method to disable real-time union
    pub fn without_realtime_union(mut self) -> Self {
        self.realtime_union_enabled = false;
        self
    }
}

/// Validate the configuration
impl ContinuousAggregateConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.refresh_check_interval_secs == 0 {
            return Err("refresh_check_interval_secs must be > 0".to_string());
        }
        if self.max_concurrent_refreshes == 0 {
            return Err("max_concurrent_refreshes must be > 0".to_string());
        }
        if self.default_refresh_interval_secs == 0 {
            return Err("default_refresh_interval_secs must be > 0".to_string());
        }
        if self.min_bucket_ratio == 0 {
            return Err("min_bucket_ratio must be > 0".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ContinuousAggregateConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.refresh_check_interval_secs, 60);
        assert!(config.query_rewriting_enabled);
    }

    #[test]
    fn test_enabled_config() {
        let config = ContinuousAggregateConfig::enabled();
        assert!(config.enabled);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ContinuousAggregateConfig::enabled()
            .with_refresh_interval(30)
            .with_max_concurrent(8)
            .without_query_rewriting();

        assert!(config.enabled);
        assert_eq!(config.refresh_check_interval_secs, 30);
        assert_eq!(config.max_concurrent_refreshes, 8);
        assert!(!config.query_rewriting_enabled);
    }

    #[test]
    fn test_validation() {
        let config = ContinuousAggregateConfig::default();
        assert!(config.validate().is_ok());

        let bad_config = ContinuousAggregateConfig {
            refresh_check_interval_secs: 0,
            ..Default::default()
        };
        assert!(bad_config.validate().is_err());
    }
}
