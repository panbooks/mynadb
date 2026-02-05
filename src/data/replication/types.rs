//! Replication types for warm standby
//!
//! These types are shared between leader and standby for data replication.
//!
//! ## Data Integrity
//!
//! Replication entries include CRC32 checksums to detect data corruption
//! during network transfer. The checksum covers the serialized batch data.

use arrow::array::RecordBatch;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

/// A replication entry containing data to be replicated to standby
#[derive(Debug, Clone)]
pub struct ReplicationEntry {
    /// Sequence number for ordering
    pub seq: u64,
    /// Catalog name (database)
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Date as days since Unix epoch (Date32)
    pub date: i32,
    /// The data batch to replicate
    pub batch: RecordBatch,
    /// Timestamp when this entry was created (ms since epoch)
    pub created_at: i64,
    /// CRC32 checksum of the serialized batch data for integrity verification
    /// Computed on the leader, verified on the standby
    pub checksum: Option<u32>,
}

impl ReplicationEntry {
    pub fn new(
        seq: u64,
        catalog: String,
        schema: String,
        table: String,
        date: i32,
        batch: RecordBatch,
    ) -> Self {
        Self {
            seq,
            catalog,
            schema,
            table,
            date,
            batch,
            created_at: chrono::Utc::now().timestamp_millis(),
            checksum: None,
        }
    }

    /// Create a new entry with checksum computed from the batch
    pub fn new_with_checksum(
        seq: u64,
        catalog: String,
        schema: String,
        table: String,
        date: i32,
        batch: RecordBatch,
        batch_bytes: &[u8],
    ) -> Self {
        let checksum = compute_checksum(batch_bytes);
        Self {
            seq,
            catalog,
            schema,
            table,
            date,
            batch,
            created_at: chrono::Utc::now().timestamp_millis(),
            checksum: Some(checksum),
        }
    }

    /// Get the partition key for this entry
    pub fn partition_key(&self) -> String {
        format!(
            "{}/{}/{}/date={}",
            self.catalog,
            self.schema,
            self.table,
            crate::storage::line_protocol::format_date32_as_string(self.date)
        )
    }

    /// Verify the checksum matches the provided data
    ///
    /// Returns Ok(()) if checksum matches or if no checksum is set,
    /// Returns Err with details if checksum mismatch detected.
    pub fn verify_checksum(&self, batch_bytes: &[u8]) -> Result<(), ChecksumError> {
        match self.checksum {
            Some(expected) => {
                let actual = compute_checksum(batch_bytes);
                if actual == expected {
                    Ok(())
                } else {
                    Err(ChecksumError::Mismatch {
                        expected,
                        actual,
                        seq: self.seq,
                        partition_key: self.partition_key(),
                    })
                }
            }
            None => Ok(()), // No checksum to verify
        }
    }

    /// Check if this entry has a checksum
    pub fn has_checksum(&self) -> bool {
        self.checksum.is_some()
    }
}

/// Checksum computation error
#[derive(Debug, Clone)]
pub enum ChecksumError {
    /// Checksum mismatch - data corruption detected
    Mismatch {
        expected: u32,
        actual: u32,
        seq: u64,
        partition_key: String,
    },
}

impl std::fmt::Display for ChecksumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChecksumError::Mismatch {
                expected,
                actual,
                seq,
                partition_key,
            } => {
                write!(
                    f,
                    "Checksum mismatch for seq {} partition {}: expected {:#x}, got {:#x}",
                    seq, partition_key, expected, actual
                )
            }
        }
    }
}

impl std::error::Error for ChecksumError {}

/// Compute CRC32 checksum for data
pub fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Verify a checksum matches the data
pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
    compute_checksum(data) == expected
}

/// Request to replicate entries to a standby
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateRequest {
    /// Source node ID (leader)
    pub source_node_id: String,
    /// Partition key this replication is for
    pub partition_key: String,
    /// Sequence numbers being replicated
    pub seq_range: (u64, u64),
    /// Number of batches in this request
    pub batch_count: usize,
    /// Total bytes being sent (for progress tracking)
    pub total_bytes: u64,
    /// CRC32 checksum of all batch data combined
    pub checksum: Option<u32>,
}

/// Response from standby after receiving replicated data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateResponse {
    /// Standby node ID
    pub standby_node_id: String,
    /// Last sequence number successfully applied
    pub last_applied_seq: u64,
    /// Number of rows applied
    pub rows_applied: u64,
    /// Any error message (empty if success)
    pub error: Option<String>,
    /// Whether checksum verification passed (if checksum was provided)
    pub checksum_verified: bool,
}

/// Replication state for a partition on the leader
#[derive(Debug, Clone)]
pub struct PartitionReplicationState {
    /// Partition key
    pub partition_key: String,
    /// Last sequence number sent to each standby
    pub standby_seqs: std::collections::HashMap<String, u64>,
    /// Last sequence number acknowledged by each standby
    pub standby_acked: std::collections::HashMap<String, u64>,
    /// Replication lag in milliseconds for each standby
    pub standby_lag_ms: std::collections::HashMap<String, u64>,
}

impl PartitionReplicationState {
    pub fn new(partition_key: String) -> Self {
        Self {
            partition_key,
            standby_seqs: std::collections::HashMap::new(),
            standby_acked: std::collections::HashMap::new(),
            standby_lag_ms: std::collections::HashMap::new(),
        }
    }

    /// Get replication lag for a standby
    pub fn get_lag(&self, standby_id: &str) -> u64 {
        *self.standby_lag_ms.get(standby_id).unwrap_or(&0)
    }

    /// Check if all standbys are in sync (lag < threshold)
    pub fn is_in_sync(&self, threshold_ms: u64) -> bool {
        self.standby_lag_ms.values().all(|&lag| lag < threshold_ms)
    }
}

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum ReplicationMode {
    /// Async: ACK immediately after local write, replicate in background
    #[default]
    Async,
    /// Sync: Wait for at least one standby to ACK before ACKing client
    Sync,
    /// SyncAll: Wait for all standbys to ACK (strongest durability)
    SyncAll,
}


/// Configuration for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Whether replication is enabled
    pub enabled: bool,
    /// Replication mode (async/sync)
    pub mode: ReplicationMode,
    /// How often to batch replication entries (ms)
    pub batch_interval_ms: u64,
    /// Maximum entries per batch
    pub max_batch_size: usize,
    /// Timeout for standby ACK (ms)
    pub ack_timeout_ms: u64,
    /// Alert if standby lag exceeds this (ms)
    pub max_lag_alert_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: ReplicationMode::Async,
            batch_interval_ms: 100,
            max_batch_size: 1000,
            ack_timeout_ms: 5000,
            max_lag_alert_ms: 10000,
        }
    }
}

/// FlushCheckpoint message sent from Primary to Standby after flush
///
/// When Primary flushes data to Parquet, it sends this message to all Standbys.
/// Standbys should clear their cache for all batches with seq <= flushed_seq.
/// This ensures Standby cache stays synchronized with Primary's persistent state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushCheckpoint {
    /// Source node ID (Primary that flushed)
    pub source_node_id: String,
    /// Partition key this checkpoint applies to (or empty for all partitions)
    pub partition_key: Option<String>,
    /// Highest sequence number that was flushed to Parquet
    /// Standby should clear cache for all seq <= this value
    pub flushed_seq: u64,
    /// Timestamp when the flush occurred (ms since epoch)
    pub timestamp: i64,
    /// Number of files written in this flush (for metrics)
    pub files_written: u32,
    /// Number of rows flushed (for metrics)
    pub rows_flushed: u64,
}

impl FlushCheckpoint {
    pub fn new(source_node_id: String, flushed_seq: u64) -> Self {
        Self {
            source_node_id,
            partition_key: None,
            flushed_seq,
            timestamp: chrono::Utc::now().timestamp_millis(),
            files_written: 0,
            rows_flushed: 0,
        }
    }

    pub fn with_metrics(
        source_node_id: String,
        flushed_seq: u64,
        files_written: u32,
        rows_flushed: u64,
    ) -> Self {
        Self {
            source_node_id,
            partition_key: None,
            flushed_seq,
            timestamp: chrono::Utc::now().timestamp_millis(),
            files_written,
            rows_flushed,
        }
    }

    pub fn for_partition(source_node_id: String, partition_key: String, flushed_seq: u64) -> Self {
        Self {
            source_node_id,
            partition_key: Some(partition_key),
            flushed_seq,
            timestamp: chrono::Utc::now().timestamp_millis(),
            files_written: 0,
            rows_flushed: 0,
        }
    }
}

/// Response from Standby after receiving FlushCheckpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushCheckpointResponse {
    /// Standby node ID
    pub standby_node_id: String,
    /// Acknowledged flushed sequence
    pub acked_seq: u64,
    /// Number of bytes cleared from cache
    pub bytes_cleared: u64,
    /// Any error message (empty if success)
    pub error: Option<String>,
}

impl From<&crate::data::config::ReplicationConfig> for ReplicationConfig {
    fn from(config: &crate::data::config::ReplicationConfig) -> Self {
        let mode = match config.mode.as_str() {
            "sync" => ReplicationMode::Sync,
            "sync_all" => ReplicationMode::SyncAll,
            _ => ReplicationMode::Async,
        };

        Self {
            enabled: config.enabled,
            mode,
            batch_interval_ms: config.batch_interval_ms,
            max_batch_size: config.max_batch_size,
            ack_timeout_ms: config.ack_timeout_ms,
            max_lag_alert_ms: config.max_lag_alert_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_checksum() {
        let data = b"hello world";
        let checksum = compute_checksum(data);

        // CRC32 should be deterministic
        assert_eq!(checksum, compute_checksum(data));

        // Different data should have different checksum
        let other_data = b"hello world!";
        assert_ne!(checksum, compute_checksum(other_data));
    }

    #[test]
    fn test_verify_checksum() {
        let data = b"test data for checksum";
        let checksum = compute_checksum(data);

        assert!(verify_checksum(data, checksum));
        assert!(!verify_checksum(data, checksum + 1));
    }

    #[test]
    fn test_checksum_error_display() {
        let err = ChecksumError::Mismatch {
            expected: 0x12345678,
            actual: 0xdeadbeef,
            seq: 42,
            partition_key: "db/schema/table/date=2024-01-01".to_string(),
        };

        let msg = format!("{}", err);
        assert!(msg.contains("Checksum mismatch"));
        assert!(msg.contains("seq 42"));
        assert!(msg.contains("0x12345678"));
        assert!(msg.contains("0xdeadbeef"));
    }

    #[test]
    fn test_replication_config_default() {
        let config = ReplicationConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.mode, ReplicationMode::Async);
        assert_eq!(config.batch_interval_ms, 100);
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.ack_timeout_ms, 5000);
        assert_eq!(config.max_lag_alert_ms, 10000);
    }

    // =========================================================================
    // FlushCheckpoint Tests
    // =========================================================================

    #[test]
    fn test_flush_checkpoint_new() {
        let checkpoint = FlushCheckpoint::new("node-1".to_string(), 100);

        assert_eq!(checkpoint.source_node_id, "node-1");
        assert_eq!(checkpoint.flushed_seq, 100);
        assert!(checkpoint.partition_key.is_none());
        assert_eq!(checkpoint.files_written, 0);
        assert_eq!(checkpoint.rows_flushed, 0);
        assert!(checkpoint.timestamp > 0);
    }

    #[test]
    fn test_flush_checkpoint_with_metrics() {
        let checkpoint = FlushCheckpoint::with_metrics("node-1".to_string(), 200, 5, 10000);

        assert_eq!(checkpoint.source_node_id, "node-1");
        assert_eq!(checkpoint.flushed_seq, 200);
        assert_eq!(checkpoint.files_written, 5);
        assert_eq!(checkpoint.rows_flushed, 10000);
        assert!(checkpoint.partition_key.is_none());
    }

    #[test]
    fn test_flush_checkpoint_for_partition() {
        let checkpoint = FlushCheckpoint::for_partition(
            "node-1".to_string(),
            "db/schema/table/date=2024-01-15".to_string(),
            300,
        );

        assert_eq!(checkpoint.source_node_id, "node-1");
        assert_eq!(checkpoint.flushed_seq, 300);
        assert_eq!(
            checkpoint.partition_key,
            Some("db/schema/table/date=2024-01-15".to_string())
        );
    }

    #[test]
    fn test_flush_checkpoint_serialization() {
        let checkpoint = FlushCheckpoint::with_metrics("node-1".to_string(), 100, 3, 5000);

        // Serialize to JSON
        let json = serde_json::to_string(&checkpoint).unwrap();
        assert!(json.contains("\"source_node_id\":\"node-1\""));
        assert!(json.contains("\"flushed_seq\":100"));
        assert!(json.contains("\"files_written\":3"));
        assert!(json.contains("\"rows_flushed\":5000"));

        // Deserialize back
        let deserialized: FlushCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.source_node_id, checkpoint.source_node_id);
        assert_eq!(deserialized.flushed_seq, checkpoint.flushed_seq);
        assert_eq!(deserialized.files_written, checkpoint.files_written);
        assert_eq!(deserialized.rows_flushed, checkpoint.rows_flushed);
    }

    #[test]
    fn test_flush_checkpoint_edge_case_zero_seq() {
        // Edge case: flushed_seq=0 is valid (first flush)
        let checkpoint = FlushCheckpoint::new("node-1".to_string(), 0);
        assert_eq!(checkpoint.flushed_seq, 0);

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: FlushCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.flushed_seq, 0);
    }

    #[test]
    fn test_flush_checkpoint_edge_case_max_seq() {
        // Edge case: maximum u64 value
        let checkpoint = FlushCheckpoint::new("node-1".to_string(), u64::MAX);
        assert_eq!(checkpoint.flushed_seq, u64::MAX);

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: FlushCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.flushed_seq, u64::MAX);
    }

    #[test]
    fn test_flush_checkpoint_response_success() {
        let response = FlushCheckpointResponse {
            standby_node_id: "standby-1".to_string(),
            acked_seq: 100,
            bytes_cleared: 1024 * 1024,
            error: None,
        };

        assert_eq!(response.standby_node_id, "standby-1");
        assert_eq!(response.acked_seq, 100);
        assert_eq!(response.bytes_cleared, 1024 * 1024);
        assert!(response.error.is_none());
    }

    #[test]
    fn test_flush_checkpoint_response_with_error() {
        let response = FlushCheckpointResponse {
            standby_node_id: "standby-1".to_string(),
            acked_seq: 50, // Partially acked
            bytes_cleared: 0,
            error: Some("Cache full, cannot process checkpoint".to_string()),
        };

        assert!(response.error.is_some());
        assert!(response.error.unwrap().contains("Cache full"));
    }

    #[test]
    fn test_flush_checkpoint_response_serialization() {
        let response = FlushCheckpointResponse {
            standby_node_id: "standby-1".to_string(),
            acked_seq: 100,
            bytes_cleared: 2048,
            error: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: FlushCheckpointResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.standby_node_id, response.standby_node_id);
        assert_eq!(deserialized.acked_seq, response.acked_seq);
        assert_eq!(deserialized.bytes_cleared, response.bytes_cleared);
        assert_eq!(deserialized.error, response.error);
    }
}
