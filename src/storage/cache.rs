use super::batch_converter::points_to_record_batch;
use super::line_protocol::{format_date32_as_partition, Point};
use super::parquet_writer::{FileWriteResult, ParquetWriter};
use super::storage::Storage;
use super::wal::Wal;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// Maximum distinct values to track per tag before marking as "high cardinality"
/// This bounds memory usage while still allowing cardinality-based sorting
const MAX_TRACKED_DISTINCT_VALUES: usize = 1000;

/// Tag cardinality tracker for a single table
/// Uses bounded HashSets to efficiently track distinct values during writes
#[derive(Default)]
struct TableCardinalityTracker {
    /// Maps tag_key -> set of distinct values (bounded)
    tag_values: HashMap<String, HashSet<String>>,
    /// Tags that exceeded MAX_TRACKED_DISTINCT_VALUES
    high_cardinality_tags: HashSet<String>,
}

impl TableCardinalityTracker {
    fn new() -> Self {
        Self::default()
    }

    /// Track tag values from a point, respecting the memory bound
    fn track_point(&mut self, tags: &HashMap<String, String>) {
        for (key, value) in tags {
            // Skip if already marked as high cardinality
            if self.high_cardinality_tags.contains(key) {
                continue;
            }

            let values = self.tag_values.entry(key.clone()).or_default();

            // Check if we've hit the threshold
            if values.len() >= MAX_TRACKED_DISTINCT_VALUES {
                // Mark as high cardinality and clear the set to free memory
                self.high_cardinality_tags.insert(key.clone());
                self.tag_values.remove(key);
            } else {
                values.insert(value.clone());
            }
        }
    }

    /// Get cardinality estimate for a tag key
    /// Returns usize::MAX for high cardinality tags
    fn get_cardinality(&self, key: &str) -> usize {
        if self.high_cardinality_tags.contains(key) {
            usize::MAX
        } else {
            self.tag_values.get(key).map(|s| s.len()).unwrap_or(0)
        }
    }

    /// Get all tag keys sorted by cardinality (low to high)
    fn get_tags_by_cardinality(&self) -> Vec<String> {
        let mut all_tags: HashSet<String> = self.tag_values.keys().cloned().collect();
        all_tags.extend(self.high_cardinality_tags.iter().cloned());

        let mut tags_with_cardinality: Vec<(String, usize)> = all_tags
            .into_iter()
            .map(|key| {
                let card = self.get_cardinality(&key);
                (key, card)
            })
            .collect();

        // Sort by cardinality (ascending - low cardinality first)
        tags_with_cardinality.sort_by_key(|(_, card)| *card);

        tags_with_cardinality.into_iter().map(|(k, _)| k).collect()
    }

    /// Clear the tracker (called after flush)
    fn clear(&mut self) {
        self.tag_values.clear();
        self.high_cardinality_tags.clear();
    }
}

/// Table key: (catalog, schema, table) - ignoring date partition
type TableKey = (String, String, String);

/// Cache key: (catalog/database, schema/service, table/measurement, date)
type CacheKey = (String, String, String, i32);

/// A batch with its replication sequence number (0 for non-replicated/Primary data)
/// Sequence numbers enable Standby cache synchronization:
/// - Primary assigns seq when replicating to Standby
/// - Standby stores seq with batch
/// - On Primary flush, Standby clears batches with seq <= flushed_seq
type SeqBatch = (u64, RecordBatch);

/// Internal cache data structure: key -> list of (sequence, batch) pairs
type CacheData = HashMap<CacheKey, Vec<SeqBatch>>;

/// Flush threshold percentage (trigger flush at 80% capacity)
const FLUSH_THRESHOLD_PERCENT: usize = 80;

/// Number of shards for reducing lock contention
const NUM_SHARDS: usize = 32;

/// A single shard of the cache
struct CacheShard {
    data: Mutex<CacheData>,
    size_bytes: AtomicUsize,
}

impl CacheShard {
    fn new() -> Self {
        CacheShard {
            data: Mutex::new(HashMap::new()),
            size_bytes: AtomicUsize::new(0),
        }
    }

    /// Add batch to this shard with sequence number, returns batch size
    fn add(&self, key: CacheKey, seq: u64, batch: RecordBatch) -> usize {
        let batch_size = batch.get_array_memory_size();
        {
            let mut data = self.data.lock();
            data.entry(key).or_insert_with(Vec::new).push((seq, batch));
        }
        self.size_bytes.fetch_add(batch_size, Ordering::Relaxed);
        batch_size
    }

    /// Take all data from this shard (for flushing)
    fn take(&self) -> CacheData {
        let mut data = self.data.lock();
        self.size_bytes.store(0, Ordering::Relaxed);
        std::mem::take(&mut *data)
    }

    /// Clear batches with sequence <= flushed_seq (for Standby sync)
    /// Returns number of bytes cleared
    fn clear_up_to_seq(&self, flushed_seq: u64) -> usize {
        let mut data = self.data.lock();
        let mut cleared_bytes = 0usize;

        for batches in data.values_mut() {
            let mut i = 0;
            while i < batches.len() {
                if batches[i].0 <= flushed_seq {
                    cleared_bytes += batches[i].1.get_array_memory_size();
                    batches.swap_remove(i);
                } else {
                    i += 1;
                }
            }
        }

        // Remove empty entries
        data.retain(|_, v| !v.is_empty());

        // Update size
        if cleared_bytes > 0 {
            let current = self.size_bytes.load(Ordering::Relaxed);
            self.size_bytes
                .store(current.saturating_sub(cleared_bytes), Ordering::Relaxed);
        }

        cleared_bytes
    }

    fn size(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    fn is_empty(&self) -> bool {
        self.data.lock().is_empty()
    }
}

#[derive(Clone)]
pub struct DataCache {
    /// Sharded cache for reduced lock contention
    shards: Arc<Vec<CacheShard>>,
    writer: Arc<ParquetWriter>,
    /// Total cache size (sum of all shards)
    total_size_bytes: Arc<AtomicUsize>,
    /// Maximum cache size in bytes
    max_size_bytes: usize,
    /// Notify channel for size-based flush trigger
    flush_notify: Arc<Notify>,
    /// WAL for durability (optional)
    wal: Option<Arc<Wal>>,
    /// Tag cardinality tracker per table (for optimal sort ordering)
    cardinality_tracker: Arc<RwLock<HashMap<TableKey, TableCardinalityTracker>>>,
    /// Node ID for path prefixing (enables distributed parallel queries)
    node_id: String,
}

impl DataCache {
    /// Create with storage backend and node_id
    ///
    /// The node_id is used as a prefix for all Parquet file paths, enabling:
    /// - Distributed parallel queries (each node reads only its own files)
    /// - Local cache affinity (node caches only files it wrote)
    /// - Zero collision risk (different directories per node)
    pub fn new(
        storage: Arc<Storage>,
        max_size_mb: u64,
        wal: Option<Arc<Wal>>,
        node_id: String,
    ) -> Self {
        let shards: Vec<CacheShard> = (0..NUM_SHARDS).map(|_| CacheShard::new()).collect();
        let writer = Arc::new(ParquetWriter::new(storage));
        let max_size_bytes = (max_size_mb as usize) * 1024 * 1024;

        DataCache {
            shards: Arc::new(shards),
            writer,
            total_size_bytes: Arc::new(AtomicUsize::new(0)),
            max_size_bytes,
            flush_notify: Arc::new(Notify::new()),
            wal,
            cardinality_tracker: Arc::new(RwLock::new(HashMap::new())),
            node_id,
        }
    }

    /// Create with local storage (backward compatibility for standalone mode)
    /// Uses "standalone" as the default node_id
    pub fn with_max_size_and_wal(data_dir: &str, max_size_mb: u64, wal: Option<Arc<Wal>>) -> Self {
        let storage = Storage::new_local(data_dir).expect("Failed to create local storage");
        Self::new(
            Arc::new(storage),
            max_size_mb,
            wal,
            "standalone".to_string(),
        )
    }

    /// Get the node_id for this cache
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get shard index for a key (hash-based distribution)
    #[inline]
    fn shard_index(&self, key: &CacheKey) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }

    /// Add points to the in-memory cache as RecordBatch
    /// catalog = database, schema = service, table = measurement
    /// Returns Ok(()) on success, Err if cache is full
    pub async fn add_points(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        points: Vec<Point>,
    ) -> Result<(), CacheFullError> {
        if points.is_empty() {
            return Ok(());
        }

        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            points[0].date,
        );

        // Track tag cardinality for optimal sort ordering
        // This is done before batch conversion for efficiency (avoid re-iteration)
        {
            let table_key = (catalog.to_string(), schema.to_string(), table.to_string());
            let mut tracker = self.cardinality_tracker.write();
            let table_tracker = tracker
                .entry(table_key)
                .or_insert_with(TableCardinalityTracker::new);
            for point in &points {
                table_tracker.track_point(&point.tags);
            }
        }

        // Convert points to RecordBatch using shared converter
        let batch = points_to_record_batch(&points).map_err(|e| {
            error!("Failed to convert points to RecordBatch: {}", e);
            CacheFullError::ConversionError(e)
        })?;

        let batch_size = batch.get_array_memory_size();
        let current_size = self.total_size_bytes.load(Ordering::Relaxed);

        // P0: Reject write if cache is full
        if current_size + batch_size > self.max_size_bytes {
            warn!(
                "Cache full: current={}MB, batch={}KB, max={}MB",
                current_size / 1024 / 1024,
                batch_size / 1024,
                self.max_size_bytes / 1024 / 1024
            );
            return Err(CacheFullError::CacheFull {
                current_bytes: current_size,
                max_bytes: self.max_size_bytes,
                batch_bytes: batch_size,
            });
        }

        // Add to appropriate shard (minimal lock contention)
        // seq=0 for Primary/local writes (non-replicated data)
        let shard_idx = self.shard_index(&key);
        let added_size = self.shards[shard_idx].add(key, 0, batch);

        // Update total size counter
        let new_size = self
            .total_size_bytes
            .fetch_add(added_size, Ordering::Relaxed)
            + added_size;

        // P1: Trigger flush if threshold exceeded (80% capacity)
        let threshold = self.max_size_bytes * FLUSH_THRESHOLD_PERCENT / 100;
        if new_size >= threshold {
            debug!(
                "Cache at {}% capacity ({}MB/{}MB), triggering flush",
                new_size * 100 / self.max_size_bytes,
                new_size / 1024 / 1024,
                self.max_size_bytes / 1024 / 1024
            );
            self.flush_notify.notify_one();
        }

        Ok(())
    }

    /// Add a pre-converted RecordBatch directly to the cache
    ///
    /// This is used by cluster mode where the Router has already converted
    /// Points to RecordBatch. The batch is added directly without re-parsing.
    ///
    /// Parameters:
    /// - catalog: database name
    /// - schema: service/schema name
    /// - table: measurement/table name
    /// - date: date partition as i32 (days since Unix epoch)
    /// - batch: the RecordBatch to add
    pub async fn add_batch(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batch: RecordBatch,
    ) -> Result<(), CacheFullError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            date,
        );

        let batch_size = batch.get_array_memory_size();
        let current_size = self.total_size_bytes.load(Ordering::Relaxed);

        // P0: Reject write if cache is full
        if current_size + batch_size > self.max_size_bytes {
            warn!(
                "Cache full: current={}MB, batch={}KB, max={}MB",
                current_size / 1024 / 1024,
                batch_size / 1024,
                self.max_size_bytes / 1024 / 1024
            );
            return Err(CacheFullError::CacheFull {
                current_bytes: current_size,
                max_bytes: self.max_size_bytes,
                batch_bytes: batch_size,
            });
        }

        // Add to appropriate shard (minimal lock contention)
        // seq=0 for Primary/local writes (non-replicated data)
        let shard_idx = self.shard_index(&key);
        let added_size = self.shards[shard_idx].add(key, 0, batch);

        // Update total size counter
        let new_size = self
            .total_size_bytes
            .fetch_add(added_size, Ordering::Relaxed)
            + added_size;

        // P1: Trigger flush if threshold exceeded (80% capacity)
        let threshold = self.max_size_bytes * FLUSH_THRESHOLD_PERCENT / 100;
        if new_size >= threshold {
            debug!(
                "Cache at {}% capacity ({}MB/{}MB), triggering flush",
                new_size * 100 / self.max_size_bytes,
                new_size / 1024 / 1024,
                self.max_size_bytes / 1024 / 1024
            );
            self.flush_notify.notify_one();
        }

        Ok(())
    }

    /// Add a pre-converted RecordBatch for standby replication with sequence tracking
    ///
    /// This is used by standby nodes receiving replicated data from the leader.
    /// The sequence number enables cache synchronization when Primary flushes:
    /// - Primary sends FlushCheckpoint with flushed_seq after flush
    /// - Standby clears all batches with seq <= flushed_seq
    ///
    /// Key differences from add_batch:
    /// - Does NOT trigger automatic flush (standby doesn't write Parquet)
    /// - Does NOT write to WAL (leader has the authoritative WAL)
    /// - Only stores in memtable for fast failover
    pub async fn add_batch_standby_with_seq(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        seq: u64,
        batch: RecordBatch,
    ) -> Result<(), CacheFullError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            date,
        );

        let batch_size = batch.get_array_memory_size();
        let current_size = self.total_size_bytes.load(Ordering::Relaxed);

        // Reject write if cache is full (standby also has memory limits)
        if current_size + batch_size > self.max_size_bytes {
            warn!(
                "Standby cache full: current={}MB, batch={}KB, max={}MB",
                current_size / 1024 / 1024,
                batch_size / 1024,
                self.max_size_bytes / 1024 / 1024
            );
            return Err(CacheFullError::CacheFull {
                current_bytes: current_size,
                max_bytes: self.max_size_bytes,
                batch_bytes: batch_size,
            });
        }

        // Add to appropriate shard with sequence number
        let shard_idx = self.shard_index(&key);
        let added_size = self.shards[shard_idx].add(key, seq, batch);

        // Update total size counter
        self.total_size_bytes
            .fetch_add(added_size, Ordering::Relaxed);

        Ok(())
    }

    /// Clear all cached batches with sequence <= flushed_seq
    ///
    /// Called by Standby when receiving FlushCheckpoint from Primary.
    /// This ensures Standby cache stays in sync with Primary's flush operations.
    pub fn clear_up_to_seq(&self, flushed_seq: u64) -> usize {
        let mut total_cleared = 0usize;

        for shard in self.shards.iter() {
            total_cleared += shard.clear_up_to_seq(flushed_seq);
        }

        // Update total size counter
        if total_cleared > 0 {
            let current = self.total_size_bytes.load(Ordering::Relaxed);
            self.total_size_bytes
                .store(current.saturating_sub(total_cleared), Ordering::Relaxed);

            info!(
                "Cleared {} bytes from Standby cache (seq <= {})",
                total_cleared, flushed_seq
            );
        }

        total_cleared
    }

    /// Flush all cached data to disk
    /// Uses swap-and-flush pattern: takes data from shards instantly, then writes without blocking
    /// Returns list of written files with metadata for registration
    ///
    /// ## WAL Consistency
    ///
    /// The flush operation maintains WAL consistency by rotating the WAL BEFORE
    /// taking data from cache. This ensures:
    /// - Old WAL files contain exactly the data being flushed
    /// - New WAL file receives writes that arrive during/after flush
    /// - No data loss if crash occurs during flush
    ///
    /// ```text
    /// Timeline:
    /// 1. Write A → WAL file 1, Cache [A]
    /// 2. Write B → WAL file 1, Cache [A, B]
    /// 3. flush() starts
    ///    3a. Rotate WAL → create WAL file 2 (new writes go here)
    ///    3b. Take [A, B] from cache
    /// 4. Write C → WAL file 2, Cache [C]
    /// 5. Parquet write complete (contains A, B)
    /// 6. Delete WAL file 1 (safe - A, B are in Parquet)
    /// 7. Crash → Recovery reads WAL file 2 → C recovered
    /// ```
    pub async fn flush(&self) -> Result<Vec<FileWriteResult>, String> {
        let start = Instant::now();

        // Phase 0: Rotate WAL BEFORE taking data from cache
        // This ensures writes arriving during flush go to a NEW WAL file,
        // while old WAL files contain exactly the data we're about to flush.
        // Critical for consistency: WAL files deleted = cache data flushed (not more, not less)
        if let Some(ref wal) = self.wal {
            debug!("Pre-flush: Rotating WAL to isolate flush data from new writes");
            if let Err(e) = wal.rotate_data_wal().await {
                warn!("Failed to rotate WAL before flush: {}", e);
                // Continue with flush - we'll be more conservative with WAL cleanup
            }
        }

        // Phase 1: Quickly take data from all shards (very fast, minimal blocking)
        // This data corresponds to what's in the OLD WAL files (pre-rotation)
        let mut all_data: CacheData = HashMap::new();
        let mut flushed_size = 0usize;

        for shard in self.shards.iter() {
            let shard_size = shard.size();
            if shard_size > 0 {
                let shard_data = shard.take();
                flushed_size += shard_size;
                for (key, mut batches) in shard_data {
                    all_data.entry(key).or_default().append(&mut batches);
                }
            }
        }

        // Reset total size counter
        self.total_size_bytes.store(0, Ordering::Relaxed);

        // Extract cardinality info and clear tracker
        let cardinality_info: HashMap<TableKey, Vec<String>> = {
            let mut tracker = self.cardinality_tracker.write();
            let info: HashMap<TableKey, Vec<String>> = tracker
                .iter()
                .map(|(k, v)| (k.clone(), v.get_tags_by_cardinality()))
                .collect();
            // Clear tracker for next flush cycle
            for (_, table_tracker) in tracker.iter_mut() {
                table_tracker.clear();
            }
            info
        };

        if all_data.is_empty() {
            debug!("Flush: Cache is empty, nothing to write");
            return Ok(Vec::new());
        }

        info!(
            "Flushing cache to disk: {}MB in {} table(s)...",
            flushed_size / 1024 / 1024,
            all_data.len()
        );

        // Phase 2: Write to disk (no locks held, writers can continue)
        let mut written_files = Vec::new();
        let mut errors = Vec::new();

        for ((catalog, schema, table, date), seq_batches) in all_data {
            // Extract RecordBatches from SeqBatch tuples (discard sequence numbers during flush)
            let batches: Vec<RecordBatch> = seq_batches.into_iter().map(|(_, b)| b).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let date_partition = format_date32_as_partition(date);

            // Get optimal tag sort order for this table (low cardinality first)
            let table_key = (catalog.clone(), schema.clone(), table.clone());
            let tag_order = cardinality_info
                .get(&table_key)
                .cloned()
                .unwrap_or_default();

            if !tag_order.is_empty() {
                debug!(
                    "Writing {} rows for {}.{}.{}/{} (sort: {} -> time)",
                    total_rows,
                    catalog,
                    schema,
                    table,
                    date_partition,
                    tag_order.join(" -> ")
                );
            } else {
                debug!(
                    "Writing {} rows for {}.{}.{}/{}",
                    total_rows, catalog, schema, table, date_partition
                );
            }

            match self.writer.write_batches_partitioned_with_sort_sync(
                &self.node_id,
                &catalog,
                &schema,
                &table,
                date,
                batches,
                &tag_order,
            ) {
                Ok(file_result) => {
                    debug!("Written to: {}", file_result.path);
                    written_files.push(file_result);
                }
                Err(e) => {
                    error!(
                        "Error writing {}.{}.{}/{}: {}",
                        catalog, schema, table, date_partition, e
                    );
                    errors.push(format!(
                        "{}.{}.{}/{}: {}",
                        catalog, schema, table, date_partition, e
                    ));
                }
            }
        }

        let elapsed = start.elapsed();

        if !errors.is_empty() {
            return Err(format!(
                "Flush completed with {} error(s) in {:?}",
                errors.len(),
                elapsed
            ));
        }

        info!(
            "Flush complete: {} file(s), {}MB written in {:?}",
            written_files.len(),
            flushed_size / 1024 / 1024,
            elapsed
        );

        // Delete old WAL files after successful flush
        // WAL was already rotated BEFORE taking cache data (Phase 0), so now we just
        // clean up the old files that contain the data we just flushed to Parquet.
        // The current WAL file (created during rotation) contains only new writes.
        if let Some(ref wal) = self.wal {
            debug!("Post-flush: Cleaning up old WAL files (data now in Parquet)");
            if let Err(e) = wal.cleanup_old_data_wal_files().await {
                warn!("Failed to cleanup old WAL files after flush: {}", e);
                // Don't fail the flush operation, just log warning
                // Old WAL files will be cleaned up on next flush or by retention policy
            } else {
                info!("WAL cleanup complete - old files deleted, new writes preserved");
            }
        }

        Ok(written_files)
    }

    /// Get current cache statistics
    pub async fn stats(&self) -> CacheStats {
        let mut total_rows = 0usize;
        let mut num_measurements = 0usize;

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for batches in data.values() {
                total_rows += batches.iter().map(|(_, b)| b.num_rows()).sum::<usize>();
            }
            num_measurements += data.len();
        }

        let size_bytes = self.total_size_bytes.load(Ordering::Relaxed);

        CacheStats {
            total_rows,
            num_measurements,
            size_bytes,
            max_size_bytes: self.max_size_bytes,
        }
    }

    /// Start background flush task that runs every interval OR when size threshold is reached
    pub fn start_background_flush(self, flush_interval_secs: u64) {
        self.start_background_flush_with_callback(
            flush_interval_secs,
            None::<fn(Vec<FileWriteResult>)>,
        );
    }

    /// Start background flush task with optional callback for file registration
    /// The callback receives a list of written files after each successful flush
    pub fn start_background_flush_with_callback<F>(
        self,
        flush_interval_secs: u64,
        on_files_written: Option<F>,
    ) where
        F: Fn(Vec<FileWriteResult>) + Send + Sync + 'static,
    {
        let flush_notify = self.flush_notify.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            info!(
                "Background flush started: every {}s or at {}% capacity ({}MB), {} shards",
                flush_interval_secs,
                FLUSH_THRESHOLD_PERCENT,
                self.max_size_bytes * FLUSH_THRESHOLD_PERCENT / 100 / 1024 / 1024,
                NUM_SHARDS
            );

            // Skip the first immediate tick to avoid flushing right after startup/recovery
            ticker.tick().await;
            debug!("Background flush: skipped first immediate tick");

            loop {
                // Wait for either: timer tick OR size-based flush notification
                tokio::select! {
                    _ = ticker.tick() => {
                        debug!("Flush triggered by timer");
                    }
                    _ = flush_notify.notified() => {
                        info!("Flush triggered by size threshold ({}% capacity)", FLUSH_THRESHOLD_PERCENT);
                    }
                }

                match self.flush().await {
                    Ok(files) => {
                        if !files.is_empty() {
                            if let Some(ref callback) = on_files_written {
                                callback(files);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Background flush error: {}", e);
                    }
                }
            }
        });
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub total_rows: usize,
    pub num_measurements: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
}

impl CacheStats {
    /// Get cache usage as percentage (0-100)
    pub fn usage_percent(&self) -> usize {
        if self.max_size_bytes == 0 {
            0
        } else {
            self.size_bytes * 100 / self.max_size_bytes
        }
    }
}

/// Error returned when cache operations fail
#[derive(Debug)]
pub enum CacheFullError {
    /// Cache is at capacity, cannot accept more data
    CacheFull {
        current_bytes: usize,
        max_bytes: usize,
        batch_bytes: usize,
    },
    /// Error converting points to RecordBatch
    ConversionError(String),
}

impl std::fmt::Display for CacheFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheFullError::CacheFull {
                current_bytes,
                max_bytes,
                batch_bytes,
            } => {
                write!(
                    f,
                    "Cache full: {}MB used of {}MB max, cannot add {}KB batch",
                    current_bytes / 1024 / 1024,
                    max_bytes / 1024 / 1024,
                    batch_bytes / 1024
                )
            }
            CacheFullError::ConversionError(e) => {
                write!(f, "Failed to convert points: {}", e)
            }
        }
    }
}

impl std::error::Error for CacheFullError {}

// ============================================================================
// Memtable Read API
// ============================================================================
// These methods allow the query engine to read cached data that hasn't been
// flushed to Parquet yet, enabling immediate query visibility for fresh writes.

impl DataCache {
    /// Check if there is any cached data for a specific table
    /// This is a fast check that avoids collecting data
    pub fn has_cached_data(&self, catalog: &str, schema: &str, table: &str) -> bool {
        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && key.1 == schema && key.2 == table && !batches.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    /// Get all RecordBatches for a specific table across all date partitions
    /// Returns cloned batches to avoid holding locks during query execution
    ///
    /// # Arguments
    /// * `catalog` - Database/catalog name
    /// * `schema` - Schema/service name
    /// * `table` - Table/measurement name
    /// * `date_filter` - Optional date partition filter (date as i32)
    pub fn get_table_batches(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_filter: Option<i32>,
    ) -> Vec<RecordBatch> {
        let mut result = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                // Check if key matches (catalog, schema, table)
                if key.0 == catalog && key.1 == schema && key.2 == table {
                    // Apply date filter if provided
                    if let Some(date) = date_filter {
                        if key.3 != date {
                            continue;
                        }
                    }
                    // Clone batches to avoid holding lock (extract RecordBatch from SeqBatch tuple)
                    result.extend(batches.iter().map(|(_, b)| b.clone()));
                }
            }
        }

        result
    }

    /// Get all RecordBatches for a table with date range filter
    ///
    /// # Arguments
    /// * `catalog` - Database/catalog name
    /// * `schema` - Schema/service name
    /// * `table` - Table/measurement name
    /// * `min_date` - Minimum date (inclusive), None for no lower bound
    /// * `max_date` - Maximum date (inclusive), None for no upper bound
    pub fn get_table_batches_in_range(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        min_date: Option<i32>,
        max_date: Option<i32>,
    ) -> Vec<RecordBatch> {
        let mut result = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && key.1 == schema && key.2 == table {
                    let date = key.3;
                    // Apply date range filter
                    if let Some(min) = min_date {
                        if date < min {
                            continue;
                        }
                    }
                    if let Some(max) = max_date {
                        if date > max {
                            continue;
                        }
                    }
                    result.extend(batches.iter().map(|(_, b)| b.clone()));
                }
            }
        }

        result
    }

    /// Get the schema for cached data of a specific table
    /// Returns the schema from the first available batch, or None if no data cached
    pub fn get_table_schema(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<Arc<Schema>> {
        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && key.1 == schema && key.2 == table {
                    if let Some((_, batch)) = batches.first() {
                        return Some(batch.schema());
                    }
                }
            }
        }
        None
    }

    /// Get list of date partitions that have cached data for a table
    pub fn get_cached_partitions(&self, catalog: &str, schema: &str, table: &str) -> Vec<i32> {
        let mut dates: Vec<i32> = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && key.1 == schema && key.2 == table && !batches.is_empty() {
                    if !dates.contains(&key.3) {
                        dates.push(key.3);
                    }
                }
            }
        }

        dates.sort();
        dates
    }

    /// List all tables that have cached data in this catalog
    /// Returns a set of (schema, table) tuples
    pub fn list_cached_tables_in_catalog(
        &self,
        catalog: &str,
    ) -> std::collections::HashSet<(String, String)> {
        let mut tables = std::collections::HashSet::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && !batches.is_empty() {
                    tables.insert((key.1.clone(), key.2.clone()));
                }
            }
        }

        tables
    }

    /// Get statistics about cached data for a table
    pub fn get_table_cache_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> TableCacheStats {
        let mut total_rows = 0usize;
        let mut total_batches = 0usize;
        let mut total_size_bytes = 0usize;
        let mut partition_count = 0usize;
        let mut partitions_seen: Vec<i32> = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && key.1 == schema && key.2 == table {
                    for (_, batch) in batches {
                        total_rows += batch.num_rows();
                        total_size_bytes += batch.get_array_memory_size();
                        total_batches += 1;
                    }
                    if !partitions_seen.contains(&key.3) {
                        partitions_seen.push(key.3);
                        partition_count += 1;
                    }
                }
            }
        }

        TableCacheStats {
            total_rows,
            total_batches,
            total_size_bytes,
            partition_count,
        }
    }

    /// Get list of all catalogs (databases) that have cached data
    pub fn get_cached_catalogs(&self) -> Vec<String> {
        let mut catalogs: Vec<String> = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if !batches.is_empty() && !catalogs.contains(&key.0) {
                    catalogs.push(key.0.clone());
                }
            }
        }

        catalogs.sort();
        catalogs
    }

    /// Get list of all schemas for a catalog that have cached data
    pub fn get_cached_schemas(&self, catalog: &str) -> Vec<String> {
        let mut schemas: Vec<String> = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog && !batches.is_empty() && !schemas.contains(&key.1) {
                    schemas.push(key.1.clone());
                }
            }
        }

        schemas.sort();
        schemas
    }

    /// Get list of all tables for a catalog/schema that have cached data
    pub fn get_cached_tables(&self, catalog: &str, schema: &str) -> Vec<String> {
        let mut tables: Vec<String> = Vec::new();

        for shard in self.shards.iter() {
            let data = shard.data.lock();
            for (key, batches) in data.iter() {
                if key.0 == catalog
                    && key.1 == schema
                    && !batches.is_empty()
                    && !tables.contains(&key.2)
                {
                    tables.push(key.2.clone());
                }
            }
        }

        tables.sort();
        tables
    }
}

/// Statistics about cached data for a specific table
#[derive(Debug, Clone)]
pub struct TableCacheStats {
    pub total_rows: usize,
    pub total_batches: usize,
    pub total_size_bytes: usize,
    pub partition_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_tracker_basic() {
        let mut tracker = TableCardinalityTracker::new();

        // Add points with tags
        let mut tags1 = HashMap::new();
        tags1.insert("region".to_string(), "us-west".to_string());
        tags1.insert("host".to_string(), "server01".to_string());
        tracker.track_point(&tags1);

        let mut tags2 = HashMap::new();
        tags2.insert("region".to_string(), "us-east".to_string());
        tags2.insert("host".to_string(), "server02".to_string());
        tracker.track_point(&tags2);

        let mut tags3 = HashMap::new();
        tags3.insert("region".to_string(), "us-west".to_string()); // duplicate region
        tags3.insert("host".to_string(), "server03".to_string()); // new host
        tracker.track_point(&tags3);

        // region has 2 distinct values, host has 3
        assert_eq!(tracker.get_cardinality("region"), 2);
        assert_eq!(tracker.get_cardinality("host"), 3);

        // Verify sort order: low cardinality first
        let sorted = tracker.get_tags_by_cardinality();
        assert_eq!(sorted[0], "region"); // cardinality 2
        assert_eq!(sorted[1], "host"); // cardinality 3
    }

    #[test]
    fn test_cardinality_tracker_high_cardinality() {
        let mut tracker = TableCardinalityTracker::new();

        // Add many distinct values for "request_id" to trigger high cardinality
        for i in 0..MAX_TRACKED_DISTINCT_VALUES + 10 {
            let mut tags = HashMap::new();
            tags.insert("request_id".to_string(), format!("req_{}", i));
            tags.insert("region".to_string(), "us-west".to_string());
            tracker.track_point(&tags);
        }

        // request_id should be marked as high cardinality
        assert_eq!(tracker.get_cardinality("request_id"), usize::MAX);
        assert_eq!(tracker.get_cardinality("region"), 1);

        // Verify sort order: low cardinality (region) before high cardinality (request_id)
        let sorted = tracker.get_tags_by_cardinality();
        assert_eq!(sorted[0], "region");
        assert_eq!(sorted[1], "request_id");
    }

    #[test]
    fn test_cardinality_tracker_clear() {
        let mut tracker = TableCardinalityTracker::new();

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server01".to_string());
        tracker.track_point(&tags);

        assert_eq!(tracker.get_cardinality("host"), 1);

        tracker.clear();

        assert_eq!(tracker.get_cardinality("host"), 0);
        assert!(tracker.get_tags_by_cardinality().is_empty());
    }

    // =========================================================================
    // Flush Checkpoint Protocol Tests
    // =========================================================================

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        use arrow::array::{Int64Array, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));

        let time_array = TimestampNanosecondArray::from(vec![1000000000i64; num_rows]);
        let value_array = Int64Array::from(vec![42i64; num_rows]);

        RecordBatch::try_new(schema, vec![Arc::new(time_array), Arc::new(value_array)]).unwrap()
    }

    #[test]
    fn test_cache_shard_add_with_seq() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batch with seq=5
        let batch = create_test_batch(10);
        let size = shard.add(key.clone(), 5, batch);
        assert!(size > 0);
        assert!(!shard.is_empty());

        // Verify the batch is stored with correct sequence
        let data = shard.data.lock();
        let batches = data.get(&key).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, 5); // seq should be 5
        assert_eq!(batches[0].1.num_rows(), 10);
    }

    #[test]
    fn test_cache_shard_clear_up_to_seq_basic() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batches with different sequence numbers
        shard.add(key.clone(), 1, create_test_batch(10));
        shard.add(key.clone(), 2, create_test_batch(10));
        shard.add(key.clone(), 3, create_test_batch(10));
        shard.add(key.clone(), 5, create_test_batch(10));
        shard.add(key.clone(), 7, create_test_batch(10));

        // Clear all batches with seq <= 3
        let cleared = shard.clear_up_to_seq(3);
        assert!(cleared > 0);

        // Verify only seq 5 and 7 remain
        let data = shard.data.lock();
        let batches = data.get(&key).unwrap();
        assert_eq!(batches.len(), 2);

        let seqs: Vec<u64> = batches.iter().map(|(seq, _)| *seq).collect();
        assert!(seqs.contains(&5));
        assert!(seqs.contains(&7));
        assert!(!seqs.contains(&1));
        assert!(!seqs.contains(&2));
        assert!(!seqs.contains(&3));
    }

    #[test]
    fn test_cache_shard_clear_up_to_seq_all_cleared() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batches with low sequence numbers
        shard.add(key.clone(), 1, create_test_batch(10));
        shard.add(key.clone(), 2, create_test_batch(10));
        shard.add(key.clone(), 3, create_test_batch(10));

        // Clear with high flushed_seq - should clear everything
        let cleared = shard.clear_up_to_seq(100);
        assert!(cleared > 0);

        // Shard should be empty now
        assert!(shard.is_empty());
        assert_eq!(shard.size(), 0);
    }

    #[test]
    fn test_cache_shard_clear_up_to_seq_none_cleared() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batches with high sequence numbers
        shard.add(key.clone(), 10, create_test_batch(10));
        shard.add(key.clone(), 20, create_test_batch(10));
        shard.add(key.clone(), 30, create_test_batch(10));

        let original_size = shard.size();

        // Clear with low flushed_seq - should clear nothing
        let cleared = shard.clear_up_to_seq(5);
        assert_eq!(cleared, 0);

        // All batches should remain
        let data = shard.data.lock();
        let batches = data.get(&key).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(shard.size(), original_size);
    }

    #[test]
    fn test_cache_shard_clear_up_to_seq_edge_case_zero() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batch with seq=0 (Primary/local writes)
        shard.add(key.clone(), 0, create_test_batch(10));
        // Add batches with replicated sequence numbers
        shard.add(key.clone(), 1, create_test_batch(10));
        shard.add(key.clone(), 2, create_test_batch(10));

        // Clear with flushed_seq=0 - should only clear seq=0
        let cleared = shard.clear_up_to_seq(0);
        assert!(cleared > 0);

        let data = shard.data.lock();
        let batches = data.get(&key).unwrap();
        assert_eq!(batches.len(), 2);

        let seqs: Vec<u64> = batches.iter().map(|(seq, _)| *seq).collect();
        assert!(!seqs.contains(&0));
        assert!(seqs.contains(&1));
        assert!(seqs.contains(&2));
    }

    #[test]
    fn test_cache_shard_clear_up_to_seq_multiple_keys() {
        let shard = CacheShard::new();
        let key1: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table1".to_string(),
            19000,
        );
        let key2: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table2".to_string(),
            19000,
        );

        // Add batches to both keys
        shard.add(key1.clone(), 1, create_test_batch(10));
        shard.add(key1.clone(), 5, create_test_batch(10));
        shard.add(key2.clone(), 2, create_test_batch(10));
        shard.add(key2.clone(), 6, create_test_batch(10));

        // Clear up to seq=3 - should affect both keys
        shard.clear_up_to_seq(3);

        let data = shard.data.lock();

        // key1: only seq=5 should remain
        let batches1 = data.get(&key1).unwrap();
        assert_eq!(batches1.len(), 1);
        assert_eq!(batches1[0].0, 5);

        // key2: only seq=6 should remain
        let batches2 = data.get(&key2).unwrap();
        assert_eq!(batches2.len(), 1);
        assert_eq!(batches2[0].0, 6);
    }

    #[test]
    fn test_cache_shard_clear_up_to_seq_exact_boundary() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batches with specific sequence numbers
        shard.add(key.clone(), 5, create_test_batch(10));
        shard.add(key.clone(), 10, create_test_batch(10));
        shard.add(key.clone(), 15, create_test_batch(10));

        // Clear with flushed_seq=10 (exact match)
        shard.clear_up_to_seq(10);

        let data = shard.data.lock();
        let batches = data.get(&key).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, 15); // Only seq=15 should remain
    }

    #[test]
    fn test_cache_shard_clear_removes_empty_entries() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add single batch
        shard.add(key.clone(), 1, create_test_batch(10));

        // Clear it
        shard.clear_up_to_seq(1);

        // The key should be completely removed (not just empty vec)
        let data = shard.data.lock();
        assert!(!data.contains_key(&key));
        assert!(shard.is_empty());
    }

    #[test]
    fn test_cache_shard_size_tracking_after_clear() {
        let shard = CacheShard::new();
        let key: CacheKey = (
            "db".to_string(),
            "schema".to_string(),
            "table".to_string(),
            19000,
        );

        // Add batches and track size
        let size1 = shard.add(key.clone(), 1, create_test_batch(100));
        let size2 = shard.add(key.clone(), 2, create_test_batch(100));
        let total_size = shard.size();
        assert_eq!(total_size, size1 + size2);

        // Clear seq=1, check size is updated
        let cleared = shard.clear_up_to_seq(1);
        assert_eq!(cleared, size1);
        assert_eq!(shard.size(), size2);
    }
}
