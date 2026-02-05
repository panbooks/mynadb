//! Replication Stream - Leader sends data to standbys
//!
//! The ReplicationStream maintains a gRPC connection to a standby node and
//! sends replication entries as they are written to the leader.
//!
//! Key design:
//! - Batches entries for efficiency (configurable interval)
//! - Tracks sequence numbers for each standby
//! - Reports replication lag to Meta for monitoring
//! - Handles reconnection on failure

use super::types::{
    compute_checksum, FlushCheckpoint, FlushCheckpointResponse, ReplicateResponse,
    ReplicationConfig, ReplicationEntry,
};
use arrow::array::RecordBatch;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_client::FlightServiceClient, FlightData,
    FlightDescriptor,
};
use futures::{stream, StreamExt};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

/// Replication stream from leader to a single standby
pub struct ReplicationStream {
    /// Standby node ID
    standby_node_id: String,
    /// Standby gRPC address
    standby_addr: String,
    /// Partition key this stream replicates
    partition_key: String,
    /// gRPC channel (lazily connected)
    channel: RwLock<Option<Channel>>,
    /// Pending entries to send
    pending: RwLock<VecDeque<ReplicationEntry>>,
    /// Last sequence number sent
    last_sent_seq: AtomicU64,
    /// Last sequence number acknowledged by standby
    last_acked_seq: AtomicU64,
    /// Replication lag in milliseconds
    lag_ms: AtomicU64,
    /// Configuration
    config: ReplicationConfig,
    /// Whether the stream is active
    active: AtomicBool,
    /// Notify for flush
    flush_notify: Notify,
    /// Background flush task handle
    flush_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl ReplicationStream {
    /// Create a new replication stream to a standby
    pub fn new(
        standby_node_id: String,
        standby_addr: String,
        partition_key: String,
        config: ReplicationConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            standby_node_id,
            standby_addr,
            partition_key,
            channel: RwLock::new(None),
            pending: RwLock::new(VecDeque::new()),
            last_sent_seq: AtomicU64::new(0),
            last_acked_seq: AtomicU64::new(0),
            lag_ms: AtomicU64::new(0),
            config,
            active: AtomicBool::new(false),
            flush_notify: Notify::new(),
            flush_handle: RwLock::new(None),
        })
    }

    /// Start the background flush task
    pub fn start(self: &Arc<Self>) {
        if self.active.swap(true, Ordering::SeqCst) {
            return; // Already started
        }

        let stream = Arc::clone(self);
        let handle = tokio::spawn(async move {
            stream.background_flush_loop().await;
        });

        *self.flush_handle.write() = Some(handle);
        info!(
            "Started replication stream to {} for partition {}",
            self.standby_node_id, self.partition_key
        );
    }

    /// Stop the replication stream
    pub fn stop(&self) {
        self.active.store(false, Ordering::SeqCst);
        self.flush_notify.notify_one();
        info!(
            "Stopping replication stream to {} for partition {}",
            self.standby_node_id, self.partition_key
        );
    }

    /// Queue an entry for replication (non-blocking)
    pub fn replicate(&self, entry: ReplicationEntry) {
        self.pending.write().push_back(entry);

        // If we have enough entries, trigger flush immediately
        if self.pending.read().len() >= self.config.max_batch_size {
            self.flush_notify.notify_one();
        }
    }

    /// Get last acknowledged sequence number
    pub fn last_acked_seq(&self) -> u64 {
        self.last_acked_seq.load(Ordering::Relaxed)
    }

    /// Get replication lag in milliseconds
    pub fn lag_ms(&self) -> u64 {
        self.lag_ms.load(Ordering::Relaxed)
    }

    /// Check if standby is in sync
    pub fn is_in_sync(&self, threshold_ms: u64) -> bool {
        self.lag_ms() < threshold_ms
    }

    /// Background loop that flushes pending entries
    async fn background_flush_loop(self: Arc<Self>) {
        let interval = Duration::from_millis(self.config.batch_interval_ms);

        while self.active.load(Ordering::Relaxed) {
            // Wait for interval or notification
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.flush_notify.notified() => {}
            }

            if !self.active.load(Ordering::Relaxed) {
                break;
            }

            // Flush pending entries
            if let Err(e) = self.flush_to_standby().await {
                warn!(
                    "Failed to flush to standby {}: {}. Will retry.",
                    self.standby_node_id, e
                );
                // Exponential backoff on failure
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }

        debug!(
            "Background flush loop stopped for standby {}",
            self.standby_node_id
        );
    }

    /// Flush pending entries to standby
    async fn flush_to_standby(&self) -> Result<(), String> {
        // Take all pending entries
        let entries: Vec<ReplicationEntry> = self.pending.write().drain(..).collect();

        if entries.is_empty() {
            return Ok(());
        }

        let batch_count = entries.len();
        let seq_start = entries.first().map(|e| e.seq).unwrap_or(0);
        let seq_end = entries.last().map(|e| e.seq).unwrap_or(0);

        debug!(
            "Flushing {} entries (seq {}-{}) to standby {}",
            batch_count, seq_start, seq_end, self.standby_node_id
        );

        // Ensure connection
        let channel = self.ensure_connected().await?;
        let mut client = FlightServiceClient::new(channel);

        // Send each entry as a DoPut request with REPLICATE action
        let start_time = Instant::now();
        let mut total_rows = 0u64;

        for entry in entries {
            total_rows += entry.batch.num_rows() as u64;

            // Encode the batch as FlightData (without descriptor first, we'll add it after computing checksum)
            let batch_stream =
                stream::iter(vec![Ok::<_, arrow_flight::error::FlightError>(entry.batch)]);
            let flight_data_stream = FlightDataEncoderBuilder::new().build(batch_stream);

            let mut flight_data: Vec<FlightData> = flight_data_stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Encode error: {}", e))?;

            // Compute checksum from serialized batch data
            let mut batch_bytes: Vec<u8> = Vec::new();
            for fd in &flight_data {
                batch_bytes.extend_from_slice(&fd.data_header);
                batch_bytes.extend_from_slice(&fd.data_body);
            }
            let checksum = compute_checksum(&batch_bytes);
            let total_bytes = batch_bytes.len() as u64;

            // Update ticket with checksum
            let ticket_json = serde_json::json!({
                "catalog": entry.catalog,
                "schema": entry.schema,
                "table": entry.table,
                "date": entry.date,
                "seq": entry.seq,
                "is_replication": true,
                "checksum": checksum,
                "total_bytes": total_bytes,
            });

            let descriptor = FlightDescriptor::new_cmd(
                serde_json::to_vec(&ticket_json).map_err(|e| format!("Serialize error: {}", e))?,
            );

            // Add descriptor to first flight data
            if let Some(first) = flight_data.first_mut() {
                first.flight_descriptor = Some(descriptor);
            }

            // Send via DoPut
            let request_stream = stream::iter(flight_data.into_iter());
            match client.do_put(request_stream).await {
                Ok(response) => {
                    let mut result_stream = response.into_inner();
                    while let Some(result) = result_stream.next().await {
                        match result {
                            Ok(put_result) => {
                                // Parse response to get acked seq
                                if let Ok(resp) = serde_json::from_slice::<ReplicateResponse>(
                                    &put_result.app_metadata,
                                ) {
                                    self.last_acked_seq
                                        .store(resp.last_applied_seq, Ordering::SeqCst);
                                }
                            }
                            Err(e) => {
                                warn!("Replication result error: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    // Put entries back on failure
                    warn!("Replication DoPut failed: {}", e);
                    return Err(format!("DoPut failed: {}", e));
                }
            }
        }

        // Update metrics
        self.last_sent_seq.store(seq_end, Ordering::SeqCst);
        let elapsed_ms = start_time.elapsed().as_millis() as u64;
        self.lag_ms.store(elapsed_ms, Ordering::SeqCst);

        debug!(
            "Flushed {} rows to standby {} in {}ms",
            total_rows, self.standby_node_id, elapsed_ms
        );

        Ok(())
    }

    /// Ensure we have a connected channel
    async fn ensure_connected(&self) -> Result<Channel, String> {
        // Check if we already have a connection
        if let Some(channel) = self.channel.read().as_ref() {
            return Ok(channel.clone());
        }

        // Create new connection
        let endpoint = format!("http://{}", self.standby_addr);
        debug!("Connecting to standby at {}", endpoint);

        let channel = Channel::from_shared(endpoint)
            .map_err(|e| format!("Invalid endpoint: {}", e))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_millis(self.config.ack_timeout_ms))
            .connect()
            .await
            .map_err(|e| format!("Connect failed: {}", e))?;

        *self.channel.write() = Some(channel.clone());
        info!(
            "Connected to standby {} at {}",
            self.standby_node_id, self.standby_addr
        );

        Ok(channel)
    }

    /// Send a FlushCheckpoint to this standby (immediate, not buffered)
    ///
    /// This is sent after a successful flush to notify the standby to clear its cache
    /// for all entries with seq <= flushed_seq.
    pub async fn send_flush_checkpoint(
        &self,
        checkpoint: &FlushCheckpoint,
    ) -> Result<FlushCheckpointResponse, String> {
        let channel = self.ensure_connected().await?;
        let mut client = FlightServiceClient::new(channel);

        // Encode checkpoint as a DoAction request
        let action_body =
            serde_json::to_vec(checkpoint).map_err(|e| format!("Serialize error: {}", e))?;

        let action = arrow_flight::Action {
            r#type: "flush_checkpoint".to_string(),
            body: action_body.into(),
        };

        match client.do_action(action).await {
            Ok(response) => {
                let mut result_stream = response.into_inner();
                if let Some(result) = result_stream.next().await {
                    match result {
                        Ok(action_result) => {
                            serde_json::from_slice::<FlushCheckpointResponse>(&action_result.body)
                                .map_err(|e| format!("Deserialize response error: {}", e))
                        }
                        Err(e) => Err(format!("Action result error: {}", e)),
                    }
                } else {
                    // No response, assume success with default response
                    Ok(FlushCheckpointResponse {
                        standby_node_id: self.standby_node_id.clone(),
                        acked_seq: checkpoint.flushed_seq,
                        bytes_cleared: 0,
                        error: None,
                    })
                }
            }
            Err(e) => Err(format!("DoAction failed: {}", e)),
        }
    }
}

/// Manager for multiple replication streams (one per partition-standby pair)
pub struct ReplicationManager {
    /// Streams keyed by "partition_key:standby_node_id"
    streams: RwLock<std::collections::HashMap<String, Arc<ReplicationStream>>>,
    /// Default configuration
    config: ReplicationConfig,
    /// Local node ID
    node_id: String,
    /// Global sequence counter for replication entries
    seq_counter: AtomicU64,
    /// Last sequence number that was flushed to Parquet
    /// Used for sending FlushCheckpoint to standbys
    last_flushed_seq: AtomicU64,
}

impl ReplicationManager {
    pub fn new(node_id: String, config: ReplicationConfig) -> Self {
        Self {
            streams: RwLock::new(std::collections::HashMap::new()),
            config,
            node_id,
            seq_counter: AtomicU64::new(0),
            last_flushed_seq: AtomicU64::new(0),
        }
    }

    /// Get the current sequence counter value (next seq to be assigned)
    pub fn current_seq(&self) -> u64 {
        self.seq_counter.load(Ordering::Relaxed)
    }

    /// Record a flush operation with the sequence number up to which data was flushed
    ///
    /// This should be called after a successful flush to Parquet.
    /// The flushed_seq represents the highest sequence number that was persisted.
    pub fn record_flush(&self, flushed_seq: u64) {
        self.last_flushed_seq.store(flushed_seq, Ordering::SeqCst);
        debug!(
            "Recorded flush checkpoint: seq {} for node {}",
            flushed_seq, self.node_id
        );
    }

    /// Get the last flushed sequence number
    pub fn last_flushed_seq(&self) -> u64 {
        self.last_flushed_seq.load(Ordering::Relaxed)
    }

    /// Get or create a replication stream for a partition-standby pair
    pub fn get_or_create_stream(
        &self,
        partition_key: &str,
        standby_node_id: &str,
        standby_addr: &str,
    ) -> Arc<ReplicationStream> {
        let key = format!("{}:{}", partition_key, standby_node_id);

        // Check if stream exists
        if let Some(stream) = self.streams.read().get(&key) {
            return Arc::clone(stream);
        }

        // Create new stream
        let stream = ReplicationStream::new(
            standby_node_id.to_string(),
            standby_addr.to_string(),
            partition_key.to_string(),
            self.config.clone(),
        );

        stream.start();
        self.streams.write().insert(key, Arc::clone(&stream));
        stream
    }

    /// Replicate a batch to all standbys for a partition
    #[allow(clippy::too_many_arguments)]
    pub fn replicate_to_standbys(
        &self,
        partition_key: &str,
        standbys: &[(String, String)], // (node_id, addr)
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batch: RecordBatch,
    ) {
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst);
        let entry = ReplicationEntry::new(
            seq,
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            date,
            batch,
        );

        for (standby_id, standby_addr) in standbys {
            let stream = self.get_or_create_stream(partition_key, standby_id, standby_addr);
            stream.replicate(entry.clone());
        }
    }

    /// Stop all replication streams
    pub fn stop_all(&self) {
        for stream in self.streams.read().values() {
            stream.stop();
        }
    }

    /// Remove streams for a partition (e.g., when partition ownership changes)
    pub fn remove_partition_streams(&self, partition_key: &str) {
        let keys_to_remove: Vec<String> = self
            .streams
            .read()
            .keys()
            .filter(|k| k.starts_with(&format!("{}:", partition_key)))
            .cloned()
            .collect();

        let mut streams = self.streams.write();
        for key in keys_to_remove {
            if let Some(stream) = streams.remove(&key) {
                stream.stop();
            }
        }
    }

    /// Get replication stats
    pub fn stats(&self) -> ReplicationStats {
        let streams = self.streams.read();
        let mut total_lag_ms = 0u64;
        let mut max_lag_ms = 0u64;
        let mut in_sync_count = 0usize;

        for stream in streams.values() {
            let lag = stream.lag_ms();
            total_lag_ms += lag;
            max_lag_ms = max_lag_ms.max(lag);
            if stream.is_in_sync(self.config.max_lag_alert_ms) {
                in_sync_count += 1;
            }
        }

        let count = streams.len();
        ReplicationStats {
            stream_count: count,
            avg_lag_ms: if count > 0 {
                total_lag_ms / count as u64
            } else {
                0
            },
            max_lag_ms,
            in_sync_count,
        }
    }

    /// Broadcast a FlushCheckpoint to all connected standbys
    ///
    /// This is called after a successful flush to notify all standbys to clear their cache.
    /// Returns the number of standbys successfully notified.
    pub async fn broadcast_flush_checkpoint(&self, checkpoint: FlushCheckpoint) -> usize {
        let streams: Vec<Arc<ReplicationStream>> = self.streams.read().values().cloned().collect();

        if streams.is_empty() {
            debug!("No standbys to broadcast flush checkpoint to");
            return 0;
        }

        info!(
            "Broadcasting flush checkpoint (seq={}) to {} standby(s)",
            checkpoint.flushed_seq,
            streams.len()
        );

        let mut success_count = 0;

        // Send to all standbys concurrently
        let futures: Vec<_> = streams
            .iter()
            .map(|stream| stream.send_flush_checkpoint(&checkpoint))
            .collect();

        let results = futures::future::join_all(futures).await;

        for (stream, result) in streams.iter().zip(results) {
            match result {
                Ok(response) => {
                    if response.error.is_none() {
                        debug!(
                            "Standby {} acked flush checkpoint seq={}, cleared {} bytes",
                            response.standby_node_id, response.acked_seq, response.bytes_cleared
                        );
                        success_count += 1;
                    } else {
                        warn!(
                            "Standby {} returned error for flush checkpoint: {:?}",
                            response.standby_node_id, response.error
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to send flush checkpoint to standby {}: {}",
                        stream.standby_node_id, e
                    );
                }
            }
        }

        info!(
            "Flush checkpoint broadcast complete: {}/{} standbys notified",
            success_count,
            streams.len()
        );

        success_count
    }

    /// Get per-partition replication stats for reporting to Meta
    ///
    /// Returns a list of (partition_key, last_acked_seq, lag_ms, is_in_sync) tuples
    pub fn partition_stats(&self) -> Vec<PartitionReplicationStats> {
        let streams = self.streams.read();
        let mut partition_stats: std::collections::HashMap<String, PartitionReplicationStats> =
            std::collections::HashMap::new();

        for (key, stream) in streams.iter() {
            // Key format: "partition_key:standby_node_id"
            if let Some(partition_key) = key.split(':').next() {
                let lag = stream.lag_ms();
                let acked = stream.last_acked_seq();
                let in_sync = stream.is_in_sync(self.config.max_lag_alert_ms);

                let entry = partition_stats
                    .entry(partition_key.to_string())
                    .or_insert_with(|| PartitionReplicationStats {
                        partition_key: partition_key.to_string(),
                        last_replicated_seq: 0,
                        max_lag_ms: 0,
                        in_sync: true,
                    });

                // Update with max values across standbys
                entry.last_replicated_seq = entry.last_replicated_seq.max(acked);
                entry.max_lag_ms = entry.max_lag_ms.max(lag);
                entry.in_sync = entry.in_sync && in_sync;
            }
        }

        partition_stats.into_values().collect()
    }
}

/// Replication statistics (aggregate)
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub stream_count: usize,
    pub avg_lag_ms: u64,
    pub max_lag_ms: u64,
    pub in_sync_count: usize,
}

/// Per-partition replication statistics for reporting to Meta
#[derive(Debug, Clone)]
pub struct PartitionReplicationStats {
    pub partition_key: String,
    pub last_replicated_seq: u64,
    pub max_lag_ms: u64,
    pub in_sync: bool,
}
