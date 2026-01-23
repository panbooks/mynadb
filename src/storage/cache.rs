use crate::line_protocol::{format_date32_as_partition, FieldValue, Point};
use crate::parquet_writer::{FileWriteResult, ParquetWriter};
use crate::storage::storage::Storage;
use crate::wal::Wal;
use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// Schema cache key includes tag keys and field keys with their types
/// Format: "tags:host,region|fields:cpu:f,mem:i,active:b"
#[derive(Hash, Eq, PartialEq, Clone)]
struct SchemaCacheKey {
    tag_keys: Vec<String>,
    field_keys: Vec<(String, FieldType)>,
}

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
enum FieldType {
    Float,
    Integer,
    String,
    Boolean,
}

impl From<&FieldValue> for FieldType {
    fn from(v: &FieldValue) -> Self {
        match v {
            FieldValue::Float(_) => FieldType::Float,
            FieldValue::Integer(_) => FieldType::Integer,
            FieldValue::String(_) => FieldType::String,
            FieldValue::Boolean(_) => FieldType::Boolean,
        }
    }
}

/// Global schema cache to avoid rebuilding schemas for the same tag/field set
static SCHEMA_CACHE: std::sync::LazyLock<RwLock<HashMap<SchemaCacheKey, Arc<Schema>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::with_capacity(1024)));

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
}

impl DataCache {
    /// Create with storage backend
    pub fn new(storage: Arc<Storage>, max_size_mb: u64, wal: Option<Arc<Wal>>) -> Self {
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
        }
    }

    /// Create with local storage (backward compatibility)
    pub fn with_max_size_and_wal(data_dir: &str, max_size_mb: u64, wal: Option<Arc<Wal>>) -> Self {
        let storage = Storage::new_local(data_dir).expect("Failed to create local storage");
        Self::new(Arc::new(storage), max_size_mb, wal)
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

        // Convert points to RecordBatch
        let batch = Self::points_to_record_batch(&points).map_err(|e| {
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

    /// Get or create schema from cache based on tag keys and field keys with types
    fn get_or_create_schema(
        tag_keys: &[String],
        field_keys: &[(String, FieldType)],
    ) -> Arc<Schema> {
        let cache_key = SchemaCacheKey {
            tag_keys: tag_keys.to_vec(),
            field_keys: field_keys.to_vec(),
        };

        // Try read lock first (fast path)
        {
            let cache = SCHEMA_CACHE.read();
            if let Some(schema) = cache.get(&cache_key) {
                return schema.clone();
            }
        }

        // Need to create schema - take write lock
        let mut cache = SCHEMA_CACHE.write();

        // Double-check after acquiring write lock
        if let Some(schema) = cache.get(&cache_key) {
            return schema.clone();
        }

        // Build schema: time + measurement + tags + fields
        let mut fields = Vec::with_capacity(2 + tag_keys.len() + field_keys.len());

        // Time column (InfluxQL compatible)
        fields.push(Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        // Measurement
        fields.push(Field::new("measurement", DataType::Utf8, false));

        // Tags (all strings, nullable)
        for key in tag_keys {
            fields.push(Field::new(key, DataType::Utf8, true));
        }

        // Fields (typed, nullable)
        for (key, field_type) in field_keys {
            let data_type = match field_type {
                FieldType::Float => DataType::Float64,
                FieldType::Integer => DataType::Int64,
                FieldType::String => DataType::Utf8,
                FieldType::Boolean => DataType::Boolean,
            };
            fields.push(Field::new(key, data_type, true));
        }

        let schema = Arc::new(Schema::new(fields));
        cache.insert(cache_key, schema.clone());
        schema
    }

    /// Convert Points to RecordBatch with schema caching and pre-sized vectors
    fn points_to_record_batch(points: &[Point]) -> Result<RecordBatch, String> {
        if points.is_empty() {
            return Err("No points to convert".to_string());
        }

        let num_points = points.len();
        let first_point = &points[0];

        // Get sorted tag keys
        let mut tag_keys: Vec<_> = first_point.tags.keys().cloned().collect();
        tag_keys.sort();

        // Get sorted field keys with their types
        let mut field_keys: Vec<(String, FieldType)> = first_point
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), FieldType::from(v)))
            .collect();
        field_keys.sort_by(|a, b| a.0.cmp(&b.0));

        // Get or create cached schema
        let schema = Self::get_or_create_schema(&tag_keys, &field_keys);

        // Pre-size arrays vector
        let total_columns = 2 + tag_keys.len() + field_keys.len();
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(total_columns);

        // Timestamp array (pre-sized)
        let mut timestamps = Vec::with_capacity(num_points);
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        for p in points {
            timestamps.push(p.timestamp.unwrap_or(now));
        }
        arrays.push(Arc::new(TimestampNanosecondArray::from(timestamps)));

        // Measurement array (pre-sized)
        let mut measurements = Vec::with_capacity(num_points);
        for p in points {
            measurements.push(p.measurement.as_str());
        }
        arrays.push(Arc::new(StringArray::from(measurements)));

        // Tag arrays (pre-sized)
        for key in &tag_keys {
            let mut values = Vec::with_capacity(num_points);
            for p in points {
                values.push(p.tags.get(key).map(|s| s.as_str()));
            }
            arrays.push(Arc::new(StringArray::from(values)));
        }

        // Field arrays (typed, pre-sized)
        for (key, field_type) in &field_keys {
            match field_type {
                FieldType::Float => {
                    let mut values: Vec<Option<f64>> = Vec::with_capacity(num_points);
                    for p in points {
                        let v = p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::Float(f) => Some(*f),
                            FieldValue::Integer(i) => Some(*i as f64),
                            _ => None,
                        });
                        values.push(v);
                    }
                    arrays.push(Arc::new(Float64Array::from(values)));
                }
                FieldType::Integer => {
                    let mut values: Vec<Option<i64>> = Vec::with_capacity(num_points);
                    for p in points {
                        let v = p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::Integer(i) => Some(*i),
                            FieldValue::Float(f) => Some(*f as i64),
                            _ => None,
                        });
                        values.push(v);
                    }
                    arrays.push(Arc::new(Int64Array::from(values)));
                }
                FieldType::String => {
                    let mut values: Vec<Option<&str>> = Vec::with_capacity(num_points);
                    for p in points {
                        let v = p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::String(s) => Some(s.as_str()),
                            _ => None,
                        });
                        values.push(v);
                    }
                    arrays.push(Arc::new(StringArray::from(values)));
                }
                FieldType::Boolean => {
                    let mut values: Vec<Option<bool>> = Vec::with_capacity(num_points);
                    for p in points {
                        let v = p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::Boolean(b) => Some(*b),
                            _ => None,
                        });
                        values.push(v);
                    }
                    arrays.push(Arc::new(BooleanArray::from(values)));
                }
            }
        }

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))
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
            debug!(
                "Writing {} rows for {}.{}.{}/{}",
                total_rows, catalog, schema, table, date_partition
            );

            match self
                .writer
                .write_batches_partitioned_sync(&catalog, &schema, &table, date, batches)
            {
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
