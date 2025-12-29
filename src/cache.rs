use crate::line_protocol::{format_date32_as_partition, Point};
use crate::parquet_writer::ParquetWriter;
use crate::wal::Wal;
use arrow::array::{RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Notify};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// Cache key: (catalog/database, schema/service, table/measurement, date)
type CacheKey = (String, String, String, i32);

/// Default max cache size: 4GB (in MB for config compatibility)
const DEFAULT_MAX_SIZE_MB: u64 = 4096;

/// Flush threshold percentage (trigger flush at 80% capacity)
const FLUSH_THRESHOLD_PERCENT: usize = 80;

#[derive(Clone)]
pub struct DataCache {
    // Store RecordBatches instead of Points for better performance
    cache: Arc<Mutex<HashMap<CacheKey, Vec<RecordBatch>>>>,
    writer: Arc<ParquetWriter>,
    // Track current cache size in bytes (atomic for lock-free reads)
    cache_size_bytes: Arc<AtomicUsize>,
    // Maximum cache size in bytes
    max_size_bytes: usize,
    // Notify channel for size-based flush trigger
    flush_notify: Arc<Notify>,
    // WAL for durability (optional)
    wal: Option<Arc<Wal>>,
}

impl DataCache {
    pub fn with_max_size(data_dir: &str, max_size_mb: u64) -> Self {
        Self::with_max_size_and_wal(data_dir, max_size_mb, None)
    }

    pub fn with_max_size_and_wal(data_dir: &str, max_size_mb: u64, wal: Option<Arc<Wal>>) -> Self {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let writer = Arc::new(ParquetWriter::new(data_dir));
        let max_size_bytes = (max_size_mb as usize) * 1024 * 1024;

        DataCache {
            cache,
            writer,
            cache_size_bytes: Arc::new(AtomicUsize::new(0)),
            max_size_bytes,
            flush_notify: Arc::new(Notify::new()),
            wal,
        }
    }

    /// Get current cache size in bytes
    pub fn current_size_bytes(&self) -> usize {
        self.cache_size_bytes.load(Ordering::Relaxed)
    }

    /// Get maximum cache size in bytes
    pub fn max_size_bytes(&self) -> usize {
        self.max_size_bytes
    }

    /// Check if cache has capacity for additional bytes
    pub fn has_capacity(&self, additional_bytes: usize) -> bool {
        self.current_size_bytes() + additional_bytes <= self.max_size_bytes
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
        let current_size = self.cache_size_bytes.load(Ordering::Relaxed);

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

        // Add to cache
        {
            let mut cache = self.cache.lock().await;
            cache.entry(key).or_insert_with(Vec::new).push(batch);
        }

        // Update size counter
        let new_size = self.cache_size_bytes.fetch_add(batch_size, Ordering::Relaxed) + batch_size;

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

        debug!(
            "Cache: added {}KB, total {}MB/{}MB ({}%)",
            batch_size / 1024,
            new_size / 1024 / 1024,
            self.max_size_bytes / 1024 / 1024,
            new_size * 100 / self.max_size_bytes
        );

        Ok(())
    }

    /// Convert Points to RecordBatch
    fn points_to_record_batch(points: &[Point]) -> Result<RecordBatch, String> {
        if points.is_empty() {
            return Err("No points to convert".to_string());
        }

        let first_point = &points[0];

        // Build schema
        let mut fields = Vec::new();

        // Timestamp
        fields.push(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        // Measurement
        fields.push(Field::new("measurement", DataType::Utf8, false));

        // Tags
        let mut tag_keys: Vec<_> = first_point.tags.keys().cloned().collect();
        tag_keys.sort();
        for key in &tag_keys {
            fields.push(Field::new(key, DataType::Utf8, true));
        }

        let schema = Arc::new(Schema::new(fields));

        // Build arrays
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        // Timestamp array
        let timestamps: Vec<i64> = points
            .iter()
            .map(|p| {
                p.timestamp
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))
            })
            .collect();
        arrays.push(Arc::new(TimestampNanosecondArray::from(timestamps)));

        // Measurement array
        let measurements: Vec<&str> = points.iter().map(|p| p.measurement.as_str()).collect();
        arrays.push(Arc::new(StringArray::from(measurements)));

        // Tag arrays
        for key in &tag_keys {
            let values: Vec<Option<&str>> = points
                .iter()
                .map(|p| p.tags.get(key).map(|s| s.as_str()))
                .collect();
            arrays.push(Arc::new(StringArray::from(values)));
        }

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }

    /// Flush all cached data to disk
    pub async fn flush(&self) -> Result<usize, String> {
        let start = Instant::now();
        let mut cache = self.cache.lock().await;

        if cache.is_empty() {
            debug!("Flush: Cache is empty, nothing to write");
            return Ok(0);
        }

        let flushed_size = self.cache_size_bytes.load(Ordering::Relaxed);
        info!(
            "Flushing cache to disk: {}MB in {} table(s)...",
            flushed_size / 1024 / 1024,
            cache.len()
        );

        let mut files_written = 0;
        let mut errors = Vec::new();

        for ((catalog, schema, table, date), batches) in cache.drain() {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let date_partition = format_date32_as_partition(date);
            debug!(
                "Writing {} rows for {}.{}.{}/{}",
                total_rows, catalog, schema, table, date_partition
            );

            match self
                .writer
                .write_batches_partitioned(&catalog, &schema, &table, date, batches)
            {
                Ok(filename) => {
                    debug!("Written to: {}", filename);
                    files_written += 1;
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

        // Reset size counter after successful flush
        self.cache_size_bytes.store(0, Ordering::Relaxed);

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
            files_written,
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

        Ok(files_written)
    }

    /// Get current cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.lock().await;
        let total_rows: usize = cache
            .values()
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
            .sum();
        let num_measurements = cache.len();
        let size_bytes = self.cache_size_bytes.load(Ordering::Relaxed);

        CacheStats {
            total_rows,
            num_measurements,
            size_bytes,
            max_size_bytes: self.max_size_bytes,
        }
    }

    /// Start background flush task that runs every interval OR when size threshold is reached
    pub fn start_background_flush(self, flush_interval_secs: u64) {
        let flush_notify = self.flush_notify.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            info!(
                "Background flush started: every {}s or at {}% capacity ({}MB)",
                flush_interval_secs,
                FLUSH_THRESHOLD_PERCENT,
                self.max_size_bytes * FLUSH_THRESHOLD_PERCENT / 100 / 1024 / 1024
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

                if let Err(e) = self.flush().await {
                    error!("Background flush error: {}", e);
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
