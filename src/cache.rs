use crate::line_protocol::Point;
use crate::parquet_writer::ParquetWriter;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

/// Cache key: (database, measurement)
type CacheKey = (String, String);

#[derive(Clone)]
pub struct DataCache {
    cache: Arc<Mutex<HashMap<CacheKey, Vec<Point>>>>,
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

    /// Add points to the in-memory cache
    pub async fn add_points(&self, db: &str, measurement: &str, points: Vec<Point>) {
        let key = (db.to_string(), measurement.to_string());
        let mut cache = self.cache.lock().await;

        cache
            .entry(key)
            .or_insert_with(Vec::new)
            .extend(points);

        let total_points: usize = cache.values().map(|v| v.len()).sum();
        println!("📦 Cache: {} points across {} measurement(s)", total_points, cache.len());
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

        for ((db, measurement), points) in cache.drain() {
            println!(
                "  Writing {} points for {}/{}",
                points.len(),
                db,
                measurement
            );

            match self.writer.write_points(&db, &measurement, points) {
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
        let total_points: usize = cache.values().map(|v| v.len()).sum();
        let num_measurements = cache.len();

        CacheStats {
            total_points,
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
    pub total_points: usize,
    pub num_measurements: usize,
}