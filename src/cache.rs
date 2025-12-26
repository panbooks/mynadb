use crate::line_protocol::{Point, format_date32_as_partition};
use crate::parquet_writer::ParquetWriter;
use crate::config::SchemaConfig;
use arrow::array::{
    RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use tracing::{info, error, debug};

/// Cache key: (catalog/database, schema/service, table/measurement)
type CacheKey = (String, String, String, i32);

#[derive(Clone)]
pub struct DataCache {
    // Store RecordBatches instead of Points for better performance
    cache: Arc<Mutex<HashMap<CacheKey, Vec<RecordBatch>>>>,
    writer: Arc<ParquetWriter>,
    schema_config: Arc<SchemaConfig>,
}

impl DataCache {
    pub fn new(data_dir: &str, schema_config: Arc<SchemaConfig>) -> Self {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let writer = Arc::new(ParquetWriter::new(data_dir));

        DataCache {
            cache,
            writer,
            schema_config,
        }
    }

    /// Add points to the in-memory cache as RecordBatch
    /// catalog = database, schema = service, table = measurement
    pub async fn add_points(&self, catalog: &str, schema: &str, table: &str, points: Vec<Point>) {
        if points.is_empty() {
            return;
        }

        let key = (catalog.to_string(), schema.to_string(), table.to_string(), points[0].date);

        // Convert points to RecordBatch
        match Self::points_to_record_batch(&points) {
            Ok(batch) => {
                let mut cache = self.cache.lock().await;
                cache.entry(key).or_insert_with(Vec::new).push(batch);

                let total_rows: usize = cache
                    .values()
                    .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
                    .sum();

                debug!("Cache: {} rows across {} table(s)", total_rows, cache.len());
            }
            Err(e) => {
                error!("Failed to convert points to RecordBatch: {}", e);
            }
        }
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

        info!("Flushing cache to disk...");
        let mut files_written = 0;
        let mut errors = Vec::new();

        for ((catalog, schema, table, date), batches) in cache.drain() {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let date_partition = format_date32_as_partition(date);
            debug!("Writing {} rows for {}.{}.{}/{}", total_rows, catalog, schema, table, date_partition);

            match self.writer.write_batches_partitioned(&catalog, &schema, &table, date, batches) {
                Ok(filename) => {
                    debug!("Written to: {}", filename);
                    files_written += 1;
                }
                Err(e) => {
                    error!("Error writing {}.{}.{}/{}: {}", catalog, schema, table, date_partition, e);
                    errors.push(format!("{}.{}.{}/{}: {}", catalog, schema, table, date_partition, e));
                }
            }
        }

        let elapsed = start.elapsed();

        if !errors.is_empty() {
            return Err(format!("Flush completed with {} error(s) in {:?}", errors.len(), elapsed));
        }

        info!("Flush complete: {} file(s) written in {:?}", files_written, elapsed);
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

        CacheStats {
            total_rows,
            num_measurements,
        }
    }

    /// Extract schema name from service tag, default to configured default
    /// Supports:
    /// - Multiple tag keys: tries each tag key in order until one is found
    /// - Simple values: "service" -> "service"
    /// - Numeric values: "123" -> "service_123" (if prefix_numeric=true)
    /// - Custom tag keys: hierarchy_2, hierarchy_1, etc.
    pub fn extract_schema(&self, tags: &HashMap<String, String>) -> String {
        // Try each tag key in order until we find one
        for tag_key in &self.schema_config.tag_keys {
            if let Some(value) = tags.get(tag_key) {
                // Prefix numeric schemas to avoid SQL parser issues
                return if self.schema_config.prefix_numeric && value.chars().all(|c| c.is_numeric()) {
                    format!("service_{}", value)
                } else {
                    value.clone()
                }
            }
        }

        // No matching tag found, use default
        self.schema_config.default_name.clone()
    }

    /// Start background flush task that runs every interval
    pub fn start_background_flush(self, flush_interval_secs: u64) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            info!("Background flush started: every {} seconds", flush_interval_secs);

            loop {
                ticker.tick().await;

                match self.flush().await {
                    Err(e) => {
                        error!("Background flush error: {}", e);
                    }
                    _ => {}
                }
            }
        });
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub total_rows: usize,
    pub num_measurements: usize,
}
