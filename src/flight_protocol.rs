//! Flight Protocol Types
//!
//! Shared type definitions for Arrow Flight communication between Router and Data nodes.
//! These types are serialized as JSON in Flight ticket/descriptor payloads.

use serde::{Deserialize, Serialize};

/// Query scope for routing - determines what data to return
///
/// Used when a Primary fails and we need to split the query:
/// - Memtable data: Query the backing Standby (has replicated memtable)
/// - Parquet data: Query any fallback Primary (S3 is shared storage)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum QueryScope {
    /// Query both memtable and Parquet (normal operation)
    #[default]
    Full,
    /// Query only memtable (for Standby nodes on Primary failover)
    MemtableOnly,
    /// Query only Parquet/S3 (for fallback Primary on failover)
    ParquetOnly,
}

/// Write request metadata sent via Flight Ticket (legacy line protocol format)
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteTicket {
    pub catalog: String,
}

/// Batch write request metadata sent via Flight Descriptor
/// Used when Router sends pre-converted RecordBatch to Data node
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchWriteTicket {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date: i32, // Days since Unix epoch (Date32 format)
}

/// Query request metadata sent via Flight Ticket
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryTicket {
    pub catalog: String,
    pub sql: String,
    pub partitions: Option<Vec<String>>,
    /// Explicit file paths to read (for distributed parallel queries)
    /// When set, the Data node reads ONLY these specific files.
    /// Format: ["node-1/mydb/default/cpu/date=2024-01-15/L0_xxx.parquet", ...]
    #[serde(default)]
    pub file_paths: Option<Vec<String>>,
    /// Query scope - determines what data to return (memtable, parquet, or both)
    /// Used for standby failover: Standby returns memtable-only, fallback Primary returns parquet-only
    #[serde(default)]
    pub query_scope: QueryScope,
}

/// Replication write ticket - used by leader to send data to standby
/// This uses add_batch_standby which doesn't trigger flush
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationTicket {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date: i32,
    pub seq: u64,
    #[serde(default)]
    pub source_node_id: String,
    /// Marker to identify this as replication data
    pub is_replication: bool,
    /// CRC32 checksum of the batch data for integrity verification
    #[serde(default)]
    pub checksum: Option<u32>,
    /// Total bytes in the batch (for verification)
    #[serde(default)]
    pub total_bytes: Option<u64>,
}
