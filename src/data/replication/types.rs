//! Replication types for warm standby
//!
//! These types are shared between leader and standby for data replication.

use arrow::array::RecordBatch;
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
