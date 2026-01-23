//! Query Types and Data Structures
//!
//! Contains type definitions for query execution, analysis, and optimization.

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// Nanoseconds per second
const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Nanoseconds per millisecond
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Query optimization flags detected from the execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptimizations {
    /// Whether predicate push-down to Parquet is used
    pub predicate_pushdown: bool,
    /// Whether partition pruning is applied
    pub partition_pruning: bool,
    /// Whether row group pruning using statistics is enabled
    pub row_group_pruning: bool,
    /// Whether projection push-down is used
    pub projection_pushdown: bool,
    /// Whether LIMIT is pushed down
    pub limit_pushdown: bool,
}

/// Time range extracted from query WHERE clause
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp (nanoseconds since epoch)
    pub start_timestamp: Option<i64>,
    /// End timestamp (nanoseconds since epoch)
    pub end_timestamp: Option<i64>,
    /// Start date for partition filtering (YYYY-MM-DD)
    pub start_date: Option<String>,
    /// End date for partition filtering (YYYY-MM-DD)
    pub end_date: Option<String>,
}

impl TimeRange {
    /// Convert a nanosecond timestamp to a date string (YYYY-MM-DD)
    pub fn timestamp_nanos_to_date(timestamp_nanos: i64) -> String {
        let secs = timestamp_nanos / NANOS_PER_SEC;
        let datetime = DateTime::from_timestamp(secs, 0)
            .unwrap_or_else(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
        datetime.format("%Y-%m-%d").to_string()
    }

    /// Convert a millisecond timestamp to a date string (YYYY-MM-DD)
    pub fn timestamp_millis_to_date(timestamp_millis: i64) -> String {
        Self::timestamp_nanos_to_date(timestamp_millis * NANOS_PER_MILLI)
    }

    /// Check if a timestamp value looks like milliseconds (13 digits) vs nanoseconds (19 digits)
    pub fn is_likely_millis(value: i64) -> bool {
        // Milliseconds: 13 digits (e.g., 1704067200000)
        // Nanoseconds: 19 digits (e.g., 1704067200000000000)
        // If value is < 10^15, it's likely milliseconds
        value > 0 && value < 1_000_000_000_000_000
    }

    /// Convert timestamp to date, auto-detecting milliseconds vs nanoseconds
    pub fn timestamp_to_date(value: i64) -> String {
        if Self::is_likely_millis(value) {
            Self::timestamp_millis_to_date(value)
        } else {
            Self::timestamp_nanos_to_date(value)
        }
    }

    /// Get the derived date range from timestamps (for partition pruning)
    pub fn get_date_range(&self) -> (Option<String>, Option<String>) {
        let start = self.start_date.clone().or_else(|| {
            self.start_timestamp.map(Self::timestamp_to_date)
        });
        let end = self.end_date.clone().or_else(|| {
            self.end_timestamp.map(Self::timestamp_to_date)
        });
        (start, end)
    }
}

/// Result of EXPLAIN query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplainResult {
    /// Original SQL query
    pub sql: String,
    /// Qualified SQL (with full table paths)
    pub qualified_sql: String,
    /// Logical execution plan
    pub logical_plan: String,
    /// Physical execution plan
    pub physical_plan: String,
    /// Detected optimizations
    pub optimizations: QueryOptimizations,
}

/// Query analysis result with statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAnalysis {
    /// Original SQL query
    pub sql: String,
    /// Tables referenced in query
    pub tables: Vec<String>,
    /// Extracted time range (if any)
    pub time_range: Option<TimeRange>,
    /// Number of partitions to scan
    pub partitions_to_scan: usize,
    /// Number of Parquet files to scan
    pub files_to_scan: usize,
    /// Detected optimizations
    pub optimizations: QueryOptimizations,
    /// Brief summary of execution plan
    pub plan_summary: String,
}
