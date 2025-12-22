use crate::line_protocol::Point;
use crate::parquet_writer::ParquetWriter;
use arrow::array::{
    Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

/// Cache key: (database, measurement)
type CacheKey = (String, String);

#[derive(Clone)]
pub struct DataCache {
    // Store RecordBatches instead of Points for better performance
    cache: Arc<Mutex<HashMap<CacheKey, Vec<RecordBatch>>>>,
    writer: Arc<ParquetWriter>,
    data_dir: String,
}

impl DataCache {
    pub fn new(data_dir: &str) -> Self {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let writer = Arc::new(ParquetWriter::new(data_dir));

        DataCache {
            cache,
            writer,
            data_dir: data_dir.to_string(),
        }
    }

    /// Add points to the in-memory cache as RecordBatch
    pub async fn add_points(&self, db: &str, measurement: &str, points: Vec<Point>) {
        if points.is_empty() {
            return;
        }

        let key = (db.to_string(), measurement.to_string());

        // Convert points to RecordBatch
        match Self::points_to_record_batch(&points) {
            Ok(batch) => {
                let mut cache = self.cache.lock().await;
                cache.entry(key).or_insert_with(Vec::new).push(batch);

                let total_rows: usize = cache
                    .values()
                    .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
                    .sum();

                println!("📦 Cache: {} rows across {} measurement(s)", total_rows, cache.len());
            }
            Err(e) => {
                eprintln!("❌ Failed to convert points to RecordBatch: {}", e);
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

        // Fields
        let mut field_keys: Vec<_> = first_point.fields.keys().cloned().collect();
        field_keys.sort();
        for key in &field_keys {
            let data_type = match first_point.fields.get(key).unwrap() {
                crate::line_protocol::FieldValue::Float(_) => DataType::Float64,
                crate::line_protocol::FieldValue::Integer(_) => DataType::Int64,
                crate::line_protocol::FieldValue::String(_) => DataType::Utf8,
                crate::line_protocol::FieldValue::Boolean(_) => DataType::Boolean,
            };
            fields.push(Field::new(key, data_type, true));
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

        // Field arrays
        for key in &field_keys {
            let field_type = first_point.fields.get(key).unwrap();
            match field_type {
                crate::line_protocol::FieldValue::Float(_) => {
                    let values: Vec<Option<f64>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key).and_then(|v| match v {
                                crate::line_protocol::FieldValue::Float(f) => Some(*f),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(Float64Array::from(values)));
                }
                crate::line_protocol::FieldValue::Integer(_) => {
                    let values: Vec<Option<i64>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key).and_then(|v| match v {
                                crate::line_protocol::FieldValue::Integer(i) => Some(*i),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(Int64Array::from(values)));
                }
                crate::line_protocol::FieldValue::String(_) => {
                    let values: Vec<Option<&str>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key).and_then(|v| match v {
                                crate::line_protocol::FieldValue::String(s) => Some(s.as_str()),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(StringArray::from(values)));
                }
                crate::line_protocol::FieldValue::Boolean(_) => {
                    let values: Vec<Option<bool>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key).and_then(|v| match v {
                                crate::line_protocol::FieldValue::Boolean(b) => Some(*b),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(arrow::array::BooleanArray::from(values)));
                }
            }
        }

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }

    /// Flush all cached data to disk
    pub async fn flush(&self) -> Result<usize, String> {
        let mut cache = self.cache.lock().await;

        if cache.is_empty() {
            println!("💾 Flush: Cache is empty, nothing to write");
            return Ok(0);
        }

        println!("💾 Flushing cache to disk...");
        let mut files_written = 0;
        let mut errors = Vec::new();

        for ((db, measurement), batches) in cache.drain() {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("  Writing {} rows for {}/{}", total_rows, db, measurement);

            match self.writer.write_batches(&db, &measurement, batches) {
                Ok(filename) => {
                    println!("  ✅ Written to: {}", filename);
                    files_written += 1;
                }
                Err(e) => {
                    eprintln!("  ❌ Error writing {}/{}: {}", db, measurement, e);
                    errors.push(format!("{}/{}: {}", db, measurement, e));
                }
            }
        }

        if !errors.is_empty() {
            return Err(format!("Flush completed with {} error(s)", errors.len()));
        }

        println!("💾 Flush complete: {} file(s) written", files_written);
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

    /// Start background flush task that runs every interval
    pub fn start_background_flush(self, flush_interval_secs: u64) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            println!(
                "🔄 Background flush started: every {} seconds",
                flush_interval_secs
            );

            loop {
                ticker.tick().await;

                match self.flush().await {
                    Ok(count) => {
                        if count > 0 {
                            println!("🔄 Background flush: {} file(s) written", count);
                        }
                    }
                    Err(e) => {
                        eprintln!("🔄 Background flush error: {}", e);
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
}