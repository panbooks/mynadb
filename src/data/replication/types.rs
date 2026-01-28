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
pub enum ReplicationMode {
    /// Async: ACK immediately after local write, replicate in background
    Async,
    /// Sync: Wait for at least one standby to ACK before ACKing client
    Sync,
    /// SyncAll: Wait for all standbys to ACK (strongest durability)
    SyncAll,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        ReplicationMode::Async
    }
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
}
