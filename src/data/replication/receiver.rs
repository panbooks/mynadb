//! Standby Receiver - Receives replicated data from leader
//!
//! The StandbyReceiver handles incoming replication data from the leader node.
//! Key characteristics:
//! - Applies data directly to memtable (no local WAL needed)
//! - Does NOT flush to Parquet (only leader does that)
//! - Tracks replication sequence for ordering
//! - Reports sync status to Meta for failover decisions

use super::types::{ReplicateRequest, ReplicateResponse, ReplicationEntry};
use crate::storage::cache::DataCache;
use arrow::array::RecordBatch;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// State for receiving replication on a single partition
#[derive(Debug)]
pub struct PartitionReceiveState {
    /// Partition key
    pub partition_key: String,
    /// Last sequence number received
    pub last_received_seq: AtomicU64,
    /// Last sequence number applied to memtable
    pub last_applied_seq: AtomicU64,
    /// Total rows received
    pub total_rows: AtomicU64,
    /// Whether this partition is actively receiving
    pub active: AtomicBool,
    /// Leader node ID for this partition
    pub leader_node_id: RwLock<Option<String>>,
}

impl PartitionReceiveState {
    pub fn new(partition_key: String) -> Self {
        Self {
            partition_key,
            last_received_seq: AtomicU64::new(0),
            last_applied_seq: AtomicU64::new(0),
            total_rows: AtomicU64::new(0),
            active: AtomicBool::new(true),
            leader_node_id: RwLock::new(None),
        }
    }

    /// Check if we have a gap in sequence numbers
    pub fn has_sequence_gap(&self, incoming_seq: u64) -> bool {
        let last = self.last_received_seq.load(Ordering::Relaxed);
        incoming_seq > last + 1
    }
}

/// Receiver for replicated data on standby nodes
pub struct StandbyReceiver {
    /// Local node ID
    node_id: String,
    /// Data cache to apply replication to (memtable only, no flush)
    cache: Arc<DataCache>,
    /// Per-partition receive state
    partitions: RwLock<HashMap<String, Arc<PartitionReceiveState>>>,
    /// Whether this node is in standby mode
    is_standby: AtomicBool,
}

impl StandbyReceiver {
    pub fn new(node_id: String, cache: Arc<DataCache>) -> Self {
        Self {
            node_id,
            cache,
            partitions: RwLock::new(HashMap::new()),
            is_standby: AtomicBool::new(false),
        }
    }

    /// Enable standby mode for this receiver
    pub fn enable_standby_mode(&self) {
        self.is_standby.store(true, Ordering::SeqCst);
        info!("Standby mode enabled for node {}", self.node_id);
    }

    /// Disable standby mode (e.g., when promoted to leader)
    pub fn disable_standby_mode(&self) {
        self.is_standby.store(false, Ordering::SeqCst);
        info!("Standby mode disabled for node {} (promoted to leader)", self.node_id);
    }

    /// Check if this node is in standby mode
    pub fn is_standby(&self) -> bool {
        self.is_standby.load(Ordering::Relaxed)
    }

    /// Register as standby for a partition
    pub fn register_partition(
        &self,
        partition_key: &str,
        leader_node_id: Option<String>,
    ) -> Arc<PartitionReceiveState> {
        let mut partitions = self.partitions.write();

        if let Some(state) = partitions.get(partition_key) {
            // Update leader if provided
            if let Some(leader) = leader_node_id {
                *state.leader_node_id.write() = Some(leader);
            }
            return Arc::clone(state);
        }

        let state = Arc::new(PartitionReceiveState::new(partition_key.to_string()));
        if let Some(leader) = leader_node_id {
            *state.leader_node_id.write() = Some(leader);
        }

        partitions.insert(partition_key.to_string(), Arc::clone(&state));
        info!("Registered as standby for partition {}", partition_key);
        state
    }

    /// Unregister from a partition (e.g., when no longer standby)
    pub fn unregister_partition(&self, partition_key: &str) {
        if let Some(state) = self.partitions.write().remove(partition_key) {
            state.active.store(false, Ordering::SeqCst);
            info!("Unregistered from partition {}", partition_key);
        }
    }

    /// Receive and apply a replicated batch
    ///
    /// This is the core method called when a leader sends data to this standby.
    /// The data is applied directly to the memtable without writing to WAL or Parquet.
    pub async fn receive_batch(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        seq: u64,
        batch: RecordBatch,
        source_node_id: &str,
    ) -> Result<ReplicateResponse, String> {
        let partition_key = format!(
            "{}/{}/{}/date={}",
            catalog,
            schema,
            table,
            crate::storage::line_protocol::format_date32_as_string(date)
        );

        // Get or create partition state
        let state = self.register_partition(&partition_key, Some(source_node_id.to_string()));

        // Check for sequence gaps
        if state.has_sequence_gap(seq) {
            warn!(
                "Sequence gap detected for partition {}: expected {} but got {}",
                partition_key,
                state.last_received_seq.load(Ordering::Relaxed) + 1,
                seq
            );
            // In a production system, we might request missing entries
            // For now, we accept the gap and continue
        }

        // Update received sequence
        state.last_received_seq.store(seq, Ordering::SeqCst);

        let row_count = batch.num_rows() as u64;

        // Apply batch directly to cache (no WAL on standby)
        // Using add_batch_no_wal which we'll need to add to DataCache
        match self
            .cache
            .add_batch_standby(catalog, schema, table, date, batch)
            .await
        {
            Ok(_) => {
                state.last_applied_seq.store(seq, Ordering::SeqCst);
                state.total_rows.fetch_add(row_count, Ordering::Relaxed);

                debug!(
                    "Applied {} rows for partition {} (seq {})",
                    row_count, partition_key, seq
                );

                Ok(ReplicateResponse {
                    standby_node_id: self.node_id.clone(),
                    last_applied_seq: seq,
                    rows_applied: row_count,
                    error: None,
                })
            }
            Err(e) => {
                error!(
                    "Failed to apply replication for partition {}: {}",
                    partition_key, e
                );
                Ok(ReplicateResponse {
                    standby_node_id: self.node_id.clone(),
                    last_applied_seq: state.last_applied_seq.load(Ordering::Relaxed),
                    rows_applied: 0,
                    error: Some(format!("Apply failed: {}", e)),
                })
            }
        }
    }

    /// Get the last applied sequence for a partition
    pub fn get_last_applied_seq(&self, partition_key: &str) -> Option<u64> {
        self.partitions
            .read()
            .get(partition_key)
            .map(|s| s.last_applied_seq.load(Ordering::Relaxed))
    }

    /// Get stats about standby replication
    pub fn stats(&self) -> StandbyStats {
        let partitions = self.partitions.read();
        let mut total_rows = 0u64;

        for state in partitions.values() {
            total_rows += state.total_rows.load(Ordering::Relaxed);
        }

        StandbyStats {
            is_standby: self.is_standby(),
            partition_count: partitions.len(),
            total_rows_received: total_rows,
        }
    }

    /// Handle promotion to leader
    ///
    /// When this standby is promoted to leader, it needs to:
    /// 1. Stop accepting replication
    /// 2. Enable flush to Parquet
    /// 3. Start accepting direct writes
    pub fn promote_to_leader(&self, partition_key: &str) {
        info!(
            "Node {} promoted to leader for partition {}",
            self.node_id, partition_key
        );

        // Mark partition as no longer in standby mode for this partition
        if let Some(state) = self.partitions.read().get(partition_key) {
            state.active.store(false, Ordering::SeqCst);
        }

        // Remove from standby partitions
        self.unregister_partition(partition_key);

        // The cache will now start flushing this partition to Parquet
        // (handled by the caller who promotes this node)
    }

    /// Get partitions this standby is responsible for
    pub fn get_standby_partitions(&self) -> Vec<String> {
        self.partitions
            .read()
            .keys()
            .cloned()
            .collect()
    }
}

/// Statistics for standby receiver
#[derive(Debug, Clone)]
pub struct StandbyStats {
    pub is_standby: bool,
    pub partition_count: usize,
    pub total_rows_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_receive_state() {
        let state = PartitionReceiveState::new("test/default/cpu/date=2024-01-01".to_string());

        // Initially no gap
        assert!(!state.has_sequence_gap(1));

        // Receive seq 1
        state.last_received_seq.store(1, Ordering::Relaxed);

        // Seq 2 is expected, no gap
        assert!(!state.has_sequence_gap(2));

        // Seq 4 would be a gap (missing 3)
        assert!(state.has_sequence_gap(4));
    }
}
