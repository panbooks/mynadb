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

/// Internal cache data structure
type CacheData = HashMap<CacheKey, Vec<RecordBatch>>;

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

    /// Add batch to this shard, returns batch size
    fn add(&self, key: CacheKey, batch: RecordBatch) -> usize {
        let batch_size = batch.get_array_memory_size();
        {
            let mut data = self.data.lock();
            data.entry(key).or_insert_with(Vec::new).push(batch);
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
        let shard_idx = self.shard_index(&key);
        let added_size = self.shards[shard_idx].add(key, batch);

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
        let shard_idx = self.shard_index(&key);
        let added_size = self.shards[shard_idx].add(key, batch);

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

    /// Add a pre-converted RecordBatch for standby replication (no flush trigger)
    ///
    /// This is used by standby nodes receiving replicated data from the leader.
    /// Key differences from add_batch:
    /// - Does NOT trigger automatic flush (standby doesn't write Parquet)
    /// - Does NOT write to WAL (leader has the authoritative WAL)
    /// - Only stores in memtable for fast failover
    ///
    /// When promoted to leader, the standby will start normal flush operations.
    pub async fn add_batch_standby(
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

        // Add to appropriate shard (minimal lock contention)
        let shard_idx = self.shard_index(&key);
        let added_size = self.shards[shard_idx].add(key, batch);

        // Update total size counter
        self.total_size_bytes
            .fetch_add(added_size, Ordering::Relaxed);

        // NOTE: Standby does NOT trigger flush - only leader flushes to Parquet
        // When standby is promoted, it will start flushing normally

        Ok(())
    }

    /// Flush all cached data to disk
    /// Uses swap-and-flush pattern: takes data from shards instantly, then writes without blocking
    /// Returns list of written files with metadata for registration
    pub async fn flush(&self) -> Result<Vec<FileWriteResult>, String> {
        let start = Instant::now();

        // Phase 1: Quickly take data from all shards (very fast, minimal blocking)
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

        for ((catalog, schema, table, date), batches) in all_data {
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

        // Rotate WAL and delete old file after successful flush
        // This ensures only unflushed data remains in WAL (prevents duplicate replay)
        if let Some(ref wal) = self.wal {
            debug!("Rotating and cleaning up WAL after successful flush");
            if let Err(e) = wal.rotate_and_delete_old_wal().await {
                warn!("Failed to rotate WAL after flush: {}", e);
                // Don't fail the flush operation, just log warning
            } else {
                info!("WAL rotated - old WAL deleted, only unflushed data remains");
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
                total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
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
                    // Clone batches to avoid holding lock
                    result.extend(batches.iter().cloned());
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
                    result.extend(batches.iter().cloned());
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
                    if let Some(batch) = batches.first() {
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
                    for batch in batches {
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
}
