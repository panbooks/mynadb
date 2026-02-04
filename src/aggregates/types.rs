//! Core types for Continuous Aggregates
//!
//! These types are designed to be serializable for storage in the Meta service
//! or local disk, and are isolated from the rest of the codebase.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// State of a continuous aggregate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AggregateState {
    /// Aggregate is being created, initial refresh pending
    Creating = 0,
    /// Aggregate is active and being refreshed
    Active = 1,
    /// Aggregate is paused (no automatic refresh)
    Paused = 2,
    /// Aggregate is being dropped
    Dropping = 3,
    /// Aggregate has an error and needs attention
    Error = 4,
}

impl Default for AggregateState {
    fn default() -> Self {
        Self::Creating
    }
}

impl std::fmt::Display for AggregateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Creating => write!(f, "creating"),
            Self::Active => write!(f, "active"),
            Self::Paused => write!(f, "paused"),
            Self::Dropping => write!(f, "dropping"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Watermark information for tracking refresh progress
///
/// The watermark tracks the boundary between materialized data and real-time data.
/// Data before the watermark is in the aggregate table; data after must be queried
/// from the raw table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkInfo {
    /// Aggregate name this watermark belongs to
    pub aggregate_name: String,

    /// High watermark: all data before this time is materialized
    /// Stored as nanoseconds since Unix epoch
    pub high_watermark_ns: i64,

    /// Low watermark: data before this has been compacted/merged
    /// Used for incremental refresh to know where to start
    pub low_watermark_ns: i64,

    /// Last refresh timestamp (when the aggregate was last updated)
    pub last_refresh_at: i64,

    /// Number of rows in the materialized view
    pub materialized_rows: u64,

    /// Size in bytes of the materialized data
    pub materialized_bytes: u64,

    /// Last partition date that was fully processed
    pub last_partition_date: Option<String>,
}

impl WatermarkInfo {
    pub fn new(aggregate_name: &str) -> Self {
        Self {
            aggregate_name: aggregate_name.to_string(),
            high_watermark_ns: 0,
            low_watermark_ns: 0,
            last_refresh_at: 0,
            materialized_rows: 0,
            materialized_bytes: 0,
            last_partition_date: None,
        }
    }

    /// Update the watermark after a successful refresh
    pub fn update(&mut self, new_high_watermark: i64, rows_added: u64, bytes_added: u64) {
        self.high_watermark_ns = new_high_watermark;
        self.last_refresh_at = current_timestamp_ns();
        self.materialized_rows += rows_added;
        self.materialized_bytes += bytes_added;
    }
}

/// Result of a refresh operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshResult {
    /// Name of the aggregate that was refreshed
    pub aggregate_name: String,

    /// Whether the refresh was successful
    pub success: bool,

    /// Error message if refresh failed
    pub error: Option<String>,

    /// Time range that was refreshed (start, end in nanoseconds)
    pub refreshed_range: Option<(i64, i64)>,

    /// Number of rows processed from source
    pub rows_processed: u64,

    /// Number of rows written to aggregate
    pub rows_written: u64,

    /// Duration of the refresh in milliseconds
    pub duration_ms: u64,

    /// New watermark after refresh
    pub new_watermark_ns: i64,

    /// Partitions that were processed
    pub partitions_processed: Vec<String>,
}

impl RefreshResult {
    pub fn success(
        name: &str,
        range: (i64, i64),
        rows_processed: u64,
        rows_written: u64,
        duration_ms: u64,
        new_watermark: i64,
        partitions: Vec<String>,
    ) -> Self {
        Self {
            aggregate_name: name.to_string(),
            success: true,
            error: None,
            refreshed_range: Some(range),
            rows_processed,
            rows_written,
            duration_ms,
            new_watermark_ns: new_watermark,
            partitions_processed: partitions,
        }
    }

    pub fn failure(name: &str, error: String) -> Self {
        Self {
            aggregate_name: name.to_string(),
            success: false,
            error: Some(error),
            refreshed_range: None,
            rows_processed: 0,
            rows_written: 0,
            duration_ms: 0,
            new_watermark_ns: 0,
            partitions_processed: vec![],
        }
    }

    pub fn skipped(name: &str, reason: &str) -> Self {
        Self {
            aggregate_name: name.to_string(),
            success: true,
            error: Some(format!("Skipped: {}", reason)),
            refreshed_range: None,
            rows_processed: 0,
            rows_written: 0,
            duration_ms: 0,
            new_watermark_ns: 0,
            partitions_processed: vec![],
        }
    }
}

/// Statistics for a continuous aggregate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateStats {
    /// Name of the aggregate
    pub name: String,

    /// Current state
    pub state: AggregateState,

    /// Watermark info
    pub watermark: WatermarkInfo,

    /// Total number of refreshes performed
    pub total_refreshes: u64,

    /// Number of failed refreshes
    pub failed_refreshes: u64,

    /// Average refresh duration in milliseconds
    pub avg_refresh_duration_ms: u64,

    /// Last error message (if any)
    pub last_error: Option<String>,

    /// Created at timestamp
    pub created_at: i64,
}

/// Time bucket granularity for aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeBucketGranularity {
    /// 1 second
    Second,
    /// 1 minute
    Minute,
    /// 5 minutes
    FiveMinutes,
    /// 15 minutes
    FifteenMinutes,
    /// 1 hour
    Hour,
    /// 1 day
    Day,
    /// Custom interval in nanoseconds
    Custom(i64),
}

impl TimeBucketGranularity {
    /// Convert to nanoseconds
    pub fn to_nanos(self) -> i64 {
        match self {
            Self::Second => 1_000_000_000,
            Self::Minute => 60 * 1_000_000_000,
            Self::FiveMinutes => 5 * 60 * 1_000_000_000,
            Self::FifteenMinutes => 15 * 60 * 1_000_000_000,
            Self::Hour => 3600 * 1_000_000_000,
            Self::Day => 86400 * 1_000_000_000_i64,
            Self::Custom(nanos) => nanos,
        }
    }
}

impl std::str::FromStr for TimeBucketGranularity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();

        if s == "1s" || s == "second" {
            return Ok(Self::Second);
        }
        if s == "1m" || s == "minute" {
            return Ok(Self::Minute);
        }
        if s == "5m" {
            return Ok(Self::FiveMinutes);
        }
        if s == "15m" {
            return Ok(Self::FifteenMinutes);
        }
        if s == "1h" || s == "hour" {
            return Ok(Self::Hour);
        }
        if s == "1d" || s == "day" {
            return Ok(Self::Day);
        }

        // Try parsing custom interval
        if let Some(num_str) = s.strip_suffix('s') {
            if let Ok(secs) = num_str.parse::<i64>() {
                return Ok(Self::Custom(secs * 1_000_000_000));
            }
        }
        if let Some(num_str) = s.strip_suffix('m') {
            if let Ok(mins) = num_str.parse::<i64>() {
                return Ok(Self::Custom(mins * 60 * 1_000_000_000));
            }
        }
        if let Some(num_str) = s.strip_suffix('h') {
            if let Ok(hours) = num_str.parse::<i64>() {
                return Ok(Self::Custom(hours * 3600 * 1_000_000_000));
            }
        }
        if let Some(num_str) = s.strip_suffix('d') {
            if let Ok(days) = num_str.parse::<i64>() {
                return Ok(Self::Custom(days * 86400 * 1_000_000_000_i64));
            }
        }

        Err(format!("Invalid time bucket granularity: {}", s))
    }
}

impl std::fmt::Display for TimeBucketGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Second => write!(f, "1s"),
            Self::Minute => write!(f, "1m"),
            Self::FiveMinutes => write!(f, "5m"),
            Self::FifteenMinutes => write!(f, "15m"),
            Self::Hour => write!(f, "1h"),
            Self::Day => write!(f, "1d"),
            Self::Custom(nanos) => {
                let secs = nanos / 1_000_000_000;
                if secs % 86400 == 0 {
                    write!(f, "{}d", secs / 86400)
                } else if secs % 3600 == 0 {
                    write!(f, "{}h", secs / 3600)
                } else if secs % 60 == 0 {
                    write!(f, "{}m", secs / 60)
                } else {
                    write!(f, "{}s", secs)
                }
            }
        }
    }
}

/// Get current timestamp in nanoseconds
fn current_timestamp_ns() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_time_bucket_granularity() {
        assert_eq!(TimeBucketGranularity::Minute.to_nanos(), 60_000_000_000);
        assert_eq!(TimeBucketGranularity::Hour.to_nanos(), 3600_000_000_000);

        let parsed = TimeBucketGranularity::from_str("5m").unwrap();
        assert_eq!(parsed, TimeBucketGranularity::FiveMinutes);

        let parsed = TimeBucketGranularity::from_str("2h").unwrap();
        assert_eq!(parsed.to_nanos(), 2 * 3600 * 1_000_000_000);
    }

    #[test]
    fn test_watermark_update() {
        let mut wm = WatermarkInfo::new("test_agg");
        assert_eq!(wm.high_watermark_ns, 0);

        wm.update(1000000000, 100, 1024);
        assert_eq!(wm.high_watermark_ns, 1000000000);
        assert_eq!(wm.materialized_rows, 100);
        assert_eq!(wm.materialized_bytes, 1024);
    }
}
