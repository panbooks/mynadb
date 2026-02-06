use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{warn, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryLog {
    /// When the query was executed
    pub timestamp: DateTime<Utc>,

    /// The SQL query text
    pub query: String,

    /// Database/catalog name
    pub database: String,

    /// Query execution time in milliseconds
    pub duration_ms: u64,

    /// Number of rows returned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,

    /// Number of partitions scanned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_scanned: Option<usize>,

    /// Number of files read
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_read: Option<usize>,

    /// Bytes scanned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,

    /// Error message if query failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Logger for slow queries
pub struct SlowQueryLogger {
    threshold_ms: u64,
    enabled: bool,
    recent_logs: Arc<RwLock<Vec<SlowQueryLog>>>,
    max_logs: usize,
}

impl SlowQueryLogger {
    pub fn new(threshold_ms: u64, enabled: bool) -> Self {
        Self {
            threshold_ms,
            enabled,
            recent_logs: Arc::new(RwLock::new(Vec::new())),
            max_logs: 1000,
        }
    }

    pub fn with_max_logs(mut self, max_logs: usize) -> Self {
        self.max_logs = max_logs;
        self
    }

    /// Log a query if it exceeds the threshold
    pub fn log_if_slow(
        &self,
        query: String,
        database: String,
        duration_ms: u64,
        row_count: Option<usize>,
        partitions_scanned: Option<usize>,
        files_read: Option<usize>,
        bytes_scanned: Option<u64>,
        error: Option<String>,
    ) {
        if !self.enabled || duration_ms < self.threshold_ms {
            return;
        }

        let log = SlowQueryLog {
            timestamp: Utc::now(),
            query: query.clone(),
            database: database.clone(),
            duration_ms,
            row_count,
            partitions_scanned,
            files_read,
            bytes_scanned,
            error: error.clone(),
        };

        // Log to tracing
        if let Some(ref err) = error {
            warn!(
                target: "slow_query",
                duration_ms = duration_ms,
                database = %database,
                query = %query,
                error = %err,
                "Slow query failed"
            );
        } else {
            info!(
                target: "slow_query",
                duration_ms = duration_ms,
                database = %database,
                query = %query,
                row_count = row_count,
                partitions_scanned = partitions_scanned,
                files_read = files_read,
                bytes_scanned = bytes_scanned,
                "Slow query detected"
            );
        }

        // Store in memory
        let mut logs = self.recent_logs.write();
        logs.push(log);

        // Keep only the most recent logs
        if logs.len() > self.max_logs {
            let drain_count = logs.len() - self.max_logs;
            logs.drain(0..drain_count);
        }
    }

    /// Get recent slow queries
    pub fn get_recent(&self, limit: usize) -> Vec<SlowQueryLog> {
        let logs = self.recent_logs.read();
        let start = logs.len().saturating_sub(limit);
        logs[start..].to_vec()
    }

    /// Get all slow queries since a timestamp
    pub fn get_since(&self, since: DateTime<Utc>) -> Vec<SlowQueryLog> {
        let logs = self.recent_logs.read();
        logs.iter()
            .filter(|log| log.timestamp > since)
            .cloned()
            .collect()
    }

    /// Clear slow query logs
    pub fn clear(&self) {
        let mut logs = self.recent_logs.write();
        logs.clear();
    }

    /// Get the current threshold
    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ms
    }

    /// Set a new threshold
    pub fn set_threshold(&mut self, threshold_ms: u64) {
        self.threshold_ms = threshold_ms;
    }

    /// Check if slow query logging is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable or disable slow query logging
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_query_logger() {
        let logger = SlowQueryLogger::new(100, true);

        // Should not log (below threshold)
        logger.log_if_slow(
            "SELECT * FROM fast_table".to_string(),
            "mydb".to_string(),
            50,
            Some(10),
            None,
            None,
            None,
            None,
        );

        let recent = logger.get_recent(10);
        assert_eq!(recent.len(), 0);

        // Should log (above threshold)
        logger.log_if_slow(
            "SELECT * FROM slow_table".to_string(),
            "mydb".to_string(),
            150,
            Some(1000),
            Some(5),
            Some(10),
            Some(1024000),
            None,
        );

        let recent = logger.get_recent(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].duration_ms, 150);
        assert_eq!(recent[0].row_count, Some(1000));
    }

    #[test]
    fn test_slow_query_logger_disabled() {
        let logger = SlowQueryLogger::new(100, false);

        logger.log_if_slow(
            "SELECT * FROM table".to_string(),
            "mydb".to_string(),
            200,
            Some(10),
            None,
            None,
            None,
            None,
        );

        let recent = logger.get_recent(10);
        assert_eq!(recent.len(), 0); // Should not log when disabled
    }
}
