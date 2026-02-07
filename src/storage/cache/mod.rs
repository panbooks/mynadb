//! Write cache with sharding and background flush
//!
//! The cache stores incoming data in memory and flushes to Parquet periodically.
//! Uses sharding for reduced lock contention on writes.

mod cardinality;
mod shard;
mod stats;

#[cfg(test)]
mod tests;

pub use stats::{CacheFullError, CacheStats, TableCacheStats};

use cardinality::TableCardinalityTracker;
use shard::{CacheData, CacheShard};

use super::batch_converter::points_to_record_batch;
use super::line_protocol::{format_date32_as_partition, Point};
use super::parquet_writer::{FileWriteResult, ParquetWriter};
use super::storage::Storage;
use super::wal::Wal;
use crate::utils::hash::compute_hash;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// Cache key: (catalog/database, schema/service, table/measurement, date)
pub type CacheKey = (String, String, String, i32);

/// Table key: (catalog, schema, table) - ignoring date partition
type TableKey = (String, String, String);

/// Flush threshold percentage (trigger flush at 80% capacity)
const FLUSH_THRESHOLD_PERCENT: usize = 80;

/// Number of shards for reducing lock contention
const NUM_SHARDS: usize = 32;

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
    /// Mutex to prevent concurrent flush operations
    /// This prevents race conditions where cleanup_old_data_wal_files() could
    /// delete WAL files that another concurrent flush is still relying on
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    /// Hot partition cache (for invalidation on flush)
    hot_partition_cache: Option<Arc<crate::query::cache::hot_partition_cache::HotPartitionCache>>,
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
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            hot_partition_cache: None,
        }
    }

    /// Set the compression strategy on the underlying ParquetWriter
    pub fn set_compression_strategy(&mut self, strategy: super::compression::CompressionStrategy) {
        self.writer = Arc::new(ParquetWriter::with_compression(
            self.writer.storage().clone(),
            strategy,
        ));
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

    /// Set hot partition cache (for invalidation on flush)
    pub fn set_hot_partition_cache(&mut self, cache: Arc<crate::query::cache::hot_partition_cache::HotPartitionCache>) {
        self.hot_partition_cache = Some(cache);
    }

    /// Get shard index for a key (hash-based distribution)
    #[inline]
    fn shard_index(&self, key: &CacheKey) -> usize {
        (compute_hash(key) as usize) % NUM_SHARDS
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
                .or_default();
            for point in &points {
                table_tracker.track_point(&point.tags);
            }
        }

        // Convert points to RecordBatch using shared converter
        let batch = points_to_record_batch(&points).map_err(|e| {
            error!("Failed to convert points to RecordBatch: {}", e);
            CacheFullError::ConversionError(e)
        })?;

        // Apply dictionary encoding to reduce memory usage for tag columns
        let batch = crate::query::cache::dictionary_encoding::encode_batch(&batch)
            .unwrap_or_else(|e| {
                warn!("Failed to encode batch: {}, using original", e);
                batch
            });

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

        // Apply dictionary encoding to reduce memory usage for tag columns
        let batch = crate::query::cache::dictionary_encoding::encode_batch(&batch)
            .unwrap_or_else(|e| {
                warn!("Failed to encode batch: {}, using original", e);
                batch
            });

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

        // Apply dictionary encoding to reduce memory usage for tag columns
        let batch = crate::query::cache::dictionary_encoding::encode_batch(&batch)
            .unwrap_or_else(|e| {
                warn!("Failed to encode batch: {}, using original", e);
                batch
            });

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
    /// ## Concurrency Safety
    ///
    /// This method uses a mutex to prevent concurrent flush operations. This is critical
    /// because the WAL cleanup at the end of flush deletes all WAL files except the current
    /// one. Without the lock, a race condition could occur where one flush deletes WAL files
    /// that another concurrent flush is still relying on.
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
        // Acquire flush lock to prevent concurrent flush operations
        // This prevents race conditions in WAL cleanup where one flush could
        // delete WAL files that another concurrent flush is still using
        let _flush_guard = self.flush_lock.lock().await;

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

        // Invalidate hot partition cache for flushed partitions
        if let Some(ref hot_cache) = self.hot_partition_cache {
            for file_result in &written_files {
                // Extract catalog, schema, table from path
                // Path format: data_dir/catalog/schema/table/date=YYYY-MM-DD/node_id=XXX/file.parquet
                let parts: Vec<&str> = file_result.path.split('/').collect();

                if parts.len() >= 5 {
                    let catalog = parts[parts.len() - 5];
                    let schema = parts[parts.len() - 4];
                    let table = parts[parts.len() - 3];

                    debug!("Invalidating hot partition cache for {}.{}.{}", catalog, schema, table);
                    hot_cache.invalidate_table(catalog, schema, table);
                }
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
        projection: Option<&[usize]>,
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
                    if let Some(proj) = projection {
                        // Apply column projection
                        for (_, batch) in batches.iter() {
                            if let Ok(projected) = batch.project(proj) {
                                result.push(projected);
                            }
                        }
                    } else {
                        result.extend(batches.iter().map(|(_, b)| b.clone()));
                    }
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
        projection: Option<&[usize]>,
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
                    // Apply column projection if provided
                    if let Some(proj) = projection {
                        for (_, batch) in batches.iter() {
                            if let Ok(projected) = batch.project(proj) {
                                result.push(projected);
                            }
                        }
                    } else {
                        result.extend(batches.iter().map(|(_, b)| b.clone()));
                    }
                }
            }
        }

        result
    }

    /// Get the schema for cached data of a specific table
    /// Returns the schema from the first available batch, or None if no data cached.
    /// Appends a `node_id` field (Utf8) if not already present, mirroring the Hive
    /// partition column that the Parquet reader adds for on-disk data.
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
                        let batch_schema = batch.schema();
                        // Add node_id field if not already present (mirrors Parquet Hive partition column)
                        if batch_schema.field_with_name("node_id").is_err() {
                            let mut fields: Vec<Field> = batch_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
                            fields.push(Field::new("node_id", DataType::Utf8, true));
                            return Some(Arc::new(Schema::new(fields)));
                        }
                        return Some(batch_schema);
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
                if key.0 == catalog && key.1 == schema && key.2 == table && !batches.is_empty()
                    && !dates.contains(&key.3) {
                        dates.push(key.3);
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
