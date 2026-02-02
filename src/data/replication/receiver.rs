//! Standby Receiver - Receives replicated data from leader
//!
//! The StandbyReceiver handles incoming replication data from the leader node.
//! Key characteristics:
//! - Applies data directly to memtable (no local WAL needed)
//! - Does NOT flush to Parquet (only leader does that)
//! - Tracks replication sequence for ordering
//! - Reports sync status to Meta for failover decisions
//! - Verifies CRC32 checksums to detect data corruption

use super::types::{compute_checksum, FlushCheckpoint, FlushCheckpointResponse, ReplicateResponse};
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

    /// Disable standby mode
    /// Note: In No-Promotion architecture, this is only used for testing/debugging
    pub fn disable_standby_mode(&self) {
        self.is_standby.store(false, Ordering::SeqCst);
        info!("Standby mode disabled for node {}", self.node_id);
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

    /// Verify checksum of raw batch data
    ///
    /// This should be called before deserializing the batch to detect corruption.
    /// Returns Ok(()) if checksum matches or no checksum provided.
    /// Returns Err with details if checksum mismatch detected.
    pub fn verify_batch_checksum(
        &self,
        raw_data: &[u8],
        expected_checksum: Option<u32>,
        partition_key: &str,
        seq: u64,
    ) -> Result<(), String> {
        if let Some(expected) = expected_checksum {
            let actual = compute_checksum(raw_data);
            if actual != expected {
                let err_msg = format!(
                    "Checksum mismatch for seq {} partition {}: expected {:#x}, got {:#x}",
                    seq, partition_key, expected, actual
                );
                warn!("{}", err_msg);
                return Err(err_msg);
            }
            debug!(
                "Checksum verified for seq {} partition {} (checksum: {:#x})",
                seq, partition_key, actual
            );
        }
        Ok(())
    }

    /// Receive and apply a replicated batch with optional checksum verification
    ///
    /// This is the core method called when a leader sends data to this standby.
    /// The data is applied directly to the memtable without writing to WAL or Parquet.
    /// If raw_data and expected_checksum are provided, verifies integrity before applying.
    pub async fn receive_batch_with_checksum(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        seq: u64,
        batch: RecordBatch,
        source_node_id: &str,
        raw_data: Option<&[u8]>,
        expected_checksum: Option<u32>,
    ) -> Result<ReplicateResponse, String> {
        let partition_key = format!(
            "{}/{}/{}/date={}",
            catalog,
            schema,
            table,
            crate::storage::line_protocol::format_date32_as_string(date)
        );

        // Verify checksum if provided
        let checksum_verified = if let (Some(data), Some(checksum)) = (raw_data, expected_checksum)
        {
            match self.verify_batch_checksum(data, Some(checksum), &partition_key, seq) {
                Ok(()) => true,
                Err(e) => {
                    return Ok(ReplicateResponse {
                        standby_node_id: self.node_id.clone(),
                        last_applied_seq: self.get_last_applied_seq(&partition_key).unwrap_or(0),
                        rows_applied: 0,
                        error: Some(e),
                        checksum_verified: false,
                    });
                }
            }
        } else {
            // No checksum provided or no raw data - skip verification
            expected_checksum.is_none()
        };

        // Call the original receive_batch logic
        self.receive_batch_internal(
            catalog,
            schema,
            table,
            date,
            seq,
            batch,
            source_node_id,
            checksum_verified,
        )
        .await
    }

    /// Receive and apply a replicated batch (legacy method without checksum)
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
        // Call without checksum verification
        self.receive_batch_internal(
            catalog,
            schema,
            table,
            date,
            seq,
            batch,
            source_node_id,
            false,
        )
        .await
    }

    /// Internal method to receive and apply a replicated batch
    async fn receive_batch_internal(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        seq: u64,
        batch: RecordBatch,
        source_node_id: &str,
        checksum_verified: bool,
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

        // Apply batch directly to cache with sequence number (no WAL on standby)
        // The sequence number is preserved for FlushCheckpoint synchronization
        match self
            .cache
            .add_batch_standby_with_seq(catalog, schema, table, date, seq, batch)
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
                    checksum_verified,
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
                    checksum_verified: false,
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

    // promote_to_leader() removed - using No-Promotion architecture
    // Standby nodes never promote to Primary; K8s handles node replacement

    /// Get partitions this standby is responsible for
    pub fn get_standby_partitions(&self) -> Vec<String> {
        self.partitions.read().keys().cloned().collect()
    }

    /// Handle a FlushCheckpoint from the Primary
    ///
    /// This clears cached batches with seq <= flushed_seq to stay in sync with Primary.
    /// The Primary has persisted these batches to Parquet, so the Standby no longer
    /// needs them in memory (they'll be read from Parquet if needed).
    pub fn handle_flush_checkpoint(
        &self,
        checkpoint: &FlushCheckpoint,
    ) -> FlushCheckpointResponse {
        let flushed_seq = checkpoint.flushed_seq;

        info!(
            "Received flush checkpoint from {}: seq={}, files={}, rows={}",
            checkpoint.source_node_id,
            flushed_seq,
            checkpoint.files_written,
            checkpoint.rows_flushed
        );

        // Clear cache for all batches with seq <= flushed_seq
        let bytes_cleared = self.cache.clear_up_to_seq(flushed_seq) as u64;

        info!(
            "Cleared {} bytes from cache (seq <= {})",
            bytes_cleared, flushed_seq
        );

        FlushCheckpointResponse {
            standby_node_id: self.node_id.clone(),
            acked_seq: flushed_seq,
            bytes_cleared,
            error: None,
        }
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

    #[test]
    fn test_partition_receive_state_sequence_gap_edge_cases() {
        let state = PartitionReceiveState::new("test/default/cpu/date=2024-01-01".to_string());

        // Seq 0 to 1 - no gap (first entry)
        assert!(!state.has_sequence_gap(1));

        // After receiving seq 5, seq 6 has no gap
        state.last_received_seq.store(5, Ordering::Relaxed);
        assert!(!state.has_sequence_gap(6));

        // Same sequence number is not a gap
        assert!(!state.has_sequence_gap(5));

        // Lower sequence number (resend) is not a gap
        assert!(!state.has_sequence_gap(3));

        // But higher sequence (missing entries) is a gap
        assert!(state.has_sequence_gap(8));
    }

    #[test]
    fn test_standby_receiver_mode() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-standby-mode")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        // Initially not in standby mode
        assert!(!receiver.is_standby());

        // Enable standby mode
        receiver.enable_standby_mode();
        assert!(receiver.is_standby());

        // Disable (promotion to leader)
        receiver.disable_standby_mode();
        assert!(!receiver.is_standby());
    }

    #[test]
    fn test_standby_receiver_partition_registration() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-partition-reg")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        // Register a partition
        let state = receiver.register_partition(
            "db/schema/table/date=2024-01-01",
            Some("leader-1".to_string()),
        );
        assert!(state.active.load(Ordering::Relaxed));

        // Verify leader is set
        assert_eq!(
            *state.leader_node_id.read(),
            Some("leader-1".to_string())
        );

        // Re-register same partition with different leader
        let state2 = receiver.register_partition(
            "db/schema/table/date=2024-01-01",
            Some("leader-2".to_string()),
        );

        // Should return same state with updated leader
        assert_eq!(
            *state2.leader_node_id.read(),
            Some("leader-2".to_string())
        );

        // List partitions
        let partitions = receiver.get_standby_partitions();
        assert_eq!(partitions.len(), 1);
        assert!(partitions.contains(&"db/schema/table/date=2024-01-01".to_string()));
    }

    #[test]
    fn test_standby_receiver_unregister_partition() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-unregister")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        // Register and then unregister
        let state = receiver.register_partition("db/schema/table/date=2024-01-01", None);
        assert!(state.active.load(Ordering::Relaxed));

        receiver.unregister_partition("db/schema/table/date=2024-01-01");

        // State should be marked inactive
        assert!(!state.active.load(Ordering::Relaxed));

        // Partition list should be empty
        assert!(receiver.get_standby_partitions().is_empty());
    }

    // test_standby_receiver_promote_to_leader removed - using No-Promotion architecture

    #[test]
    fn test_standby_receiver_stats() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-stats")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        // Initial stats
        let stats = receiver.stats();
        assert!(!stats.is_standby);
        assert_eq!(stats.partition_count, 0);
        assert_eq!(stats.total_rows_received, 0);

        // Enable standby mode and register partitions
        receiver.enable_standby_mode();
        receiver.register_partition("partition-1", None);
        receiver.register_partition("partition-2", None);

        let stats = receiver.stats();
        assert!(stats.is_standby);
        assert_eq!(stats.partition_count, 2);
    }

    #[test]
    fn test_flush_checkpoint_handler() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-flush-checkpoint")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);
        receiver.enable_standby_mode();

        // Create a flush checkpoint
        let checkpoint = FlushCheckpoint::with_metrics(
            "primary-1".to_string(),
            100, // flushed_seq
            5,   // files_written
            1000, // rows_flushed
        );

        // Handle the checkpoint (cache is empty so bytes_cleared should be 0)
        let response = receiver.handle_flush_checkpoint(&checkpoint);

        assert_eq!(response.standby_node_id, "standby-1");
        assert_eq!(response.acked_seq, 100);
        assert!(response.error.is_none());
        // bytes_cleared is 0 because cache was empty
        assert_eq!(response.bytes_cleared, 0);
    }

    #[test]
    fn test_flush_checkpoint_response_node_id() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-response-node-id")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("my-unique-node-id".to_string(), cache);

        let checkpoint = FlushCheckpoint::new("primary".to_string(), 50);
        let response = receiver.handle_flush_checkpoint(&checkpoint);

        // Response should contain the receiver's node ID
        assert_eq!(response.standby_node_id, "my-unique-node-id");
    }

    #[test]
    fn test_verify_batch_checksum_success() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-checksum-success")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        let data = b"test batch data";
        let checksum = compute_checksum(data);

        let result = receiver.verify_batch_checksum(
            data,
            Some(checksum),
            "partition-1",
            1,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_batch_checksum_failure() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-checksum-fail")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        let data = b"test batch data";
        let wrong_checksum = 0xDEADBEEF;

        let result = receiver.verify_batch_checksum(
            data,
            Some(wrong_checksum),
            "partition-1",
            1,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Checksum mismatch"));
    }

    #[test]
    fn test_verify_batch_checksum_no_checksum() {
        use crate::storage::storage::Storage;

        let storage = Storage::new_local("/tmp/test-no-checksum")
            .expect("Failed to create storage");
        let cache = Arc::new(DataCache::new(
            Arc::new(storage),
            64,
            None,
            "test-standby".to_string(),
        ));

        let receiver = StandbyReceiver::new("standby-1".to_string(), cache);

        let data = b"test batch data";

        // No checksum provided - should pass
        let result = receiver.verify_batch_checksum(
            data,
            None,
            "partition-1",
            1,
        );
        assert!(result.is_ok());
    }
}
