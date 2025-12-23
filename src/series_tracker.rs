use crate::line_protocol::Point;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

// System directory and file constants
const SYSTEM_DIR: &str = "_system";
const SERIES_STATS_DIR: &str = "series_stats";
const SERIES_STATS_FILE_EXT: &str = "parquet";
const MONITOR_SERVICE_TAG: &str = "service";
const DEFAULT_SERVICE_NAME: &str = "unknown";

/// Series record for batching
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct SeriesRecord {
    service: String,
    metric: String,
    hash: i64,
}

/// Series tracker for monitoring metric cardinality
/// Tracks unique series by service, metric, and tag combination hash
pub struct SeriesTracker {
    data_dir: String,
    /// In-memory cache: (service, metric, hash) -> exists
    cache: Arc<RwLock<HashSet<(String, String, i64)>>>,
    /// Pending writes buffer per database
    pending_writes: Arc<RwLock<HashMap<String, HashSet<SeriesRecord>>>>,
}

impl SeriesTracker {
    /// Get the series stats directory path
    fn get_stats_dir(&self) -> PathBuf {
        Path::new(&self.data_dir)
            .join(SYSTEM_DIR)
            .join(SERIES_STATS_DIR)
    }

    /// Get the series stats file path for a database
    fn get_stats_file_path(&self, db: &str) -> PathBuf {
        self.get_stats_dir()
            .join(format!("{}.{}", db, SERIES_STATS_FILE_EXT))
    }
}

impl SeriesTracker {
    pub fn new(data_dir: &str) -> Self {
        let tracker = SeriesTracker {
            data_dir: data_dir.to_string(),
            cache: Arc::new(RwLock::new(HashSet::new())),
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // Clean up any leftover temp files from previous crashes
        tracker.cleanup_temp_files();
        
        tracker
    }
    
    /// Clean up temporary files left from previous crashes
    fn cleanup_temp_files(&self) {
        let stats_dir = self.get_stats_dir();
        if let Ok(entries) = std::fs::read_dir(&stats_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".tmp") {
                        if let Err(e) = std::fs::remove_file(entry.path()) {
                            eprintln!("⚠️  Failed to cleanup temp file '{}': {}", entry.path().display(), e);
                        } else {
                            println!("🧹 Cleaned up leftover temp file: {}", name);
                        }
                    }
                }
            }
        }
    }
    
    /// Start background flush task
    pub fn start_background_flush(self: Arc<Self>, flush_interval_secs: u64) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            println!(
                "🔄 Series tracker background flush started: every {} seconds",
                flush_interval_secs
            );

            loop {
                ticker.tick().await;

                match self.flush_all().await {
                    Ok(count) => {
                        if count > 0 {
                            println!("🔄 Series tracker flush: {} database(s) updated", count);
                        }
                    }
                    Err(e) => {
                        eprintln!("🔄 Series tracker flush error: {}", e);
                    }
                }
            }
        });
    }

    /// Track multiple points in batch (more efficient than individual track_point calls)
    /// This method minimizes lock contention by batching cache and pending writes operations
    pub async fn track_points(&self, db: &str, points: &[Point]) {
        if points.is_empty() {
            return;
        }

        // Collect all new series records
        let mut new_records = Vec::new();
        
        // First pass: identify new series (minimizing write lock time)
        for point in points {
            let service = point
                .tags
                .iter()
                .find(|(k, _)| *k == MONITOR_SERVICE_TAG)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string());

            let hash = self.calculate_tag_hash(&point.tags);
            let key = (service.clone(), point.measurement.clone(), hash);

            // Check if already tracked
            {
                let cache = self.cache.read().await;
                if cache.contains(&key) {
                    continue;
                }
            }

            new_records.push((key, SeriesRecord {
                service,
                metric: point.measurement.clone(),
                hash,
            }));
        }

        if new_records.is_empty() {
            return;
        }

        // Batch insert into cache (single write lock)
        let mut actually_new = Vec::new();
        {
            let mut cache = self.cache.write().await;
            for (key, record) in new_records {
                if cache.insert(key) {
                    actually_new.push(record);
                }
            }
        }

        // Batch insert into pending writes (single write lock)
        if !actually_new.is_empty() {
            let mut pending = self.pending_writes.write().await;
            let db_pending = pending.entry(db.to_string()).or_insert_with(HashSet::new);
            for record in actually_new {
                db_pending.insert(record);
            }
        }
    }

    /// Calculate hash of tags excluding MONITOR.service
    fn calculate_tag_hash(&self, tags: &HashMap<String, String>) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Filter out MONITOR.service and sort the rest
        let mut filtered_tags: Vec<(&str, &str)> = tags
            .iter()
            .filter(|(k, _)| *k != MONITOR_SERVICE_TAG)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        
        filtered_tags.sort();

        // Create a stable hash from sorted tags
        let mut hasher = DefaultHasher::new();
        for (key, value) in filtered_tags {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        
        hasher.finish() as i64
    }

    /// Flush all pending writes for all databases
    pub async fn flush_all(&self) -> Result<usize, String> {
        let mut pending = self.pending_writes.write().await;
        
        if pending.is_empty() {
            return Ok(0);
        }
        
        let mut flushed_count = 0;
        for (db, records) in pending.drain() {
            if !records.is_empty() {
                println!("💾 Flushing {} series records for database '{}'", records.len(), db);
                match self.flush_database(&db, records).await {
                    Ok(()) => {
                        flushed_count += 1;
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to flush database '{}': {}", db, e);
                    }
                }
            }
        }
        
        Ok(flushed_count)
    }
    
    /// Flush pending writes for a specific database (batch write)
    async fn flush_database(&self, db: &str, new_records: HashSet<SeriesRecord>) -> Result<(), String> {
        let stats_dir = self.get_stats_dir();
        std::fs::create_dir_all(&stats_dir)
            .map_err(|e| format!("Failed to create stats directory: {}", e))?;

        let file_path = self.get_stats_file_path(db);
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("hash", DataType::Int64, false),
        ]));

        // Collect existing data if file exists
        let mut services = Vec::new();
        let mut metrics = Vec::new();
        let mut hashes = Vec::new();

        if file_path.exists() && std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) > 0 {
            use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            
            match File::open(&file_path) {
                Ok(read_file) => {
                    match ParquetRecordBatchReaderBuilder::try_new(read_file) {
                        Ok(builder) => {
                            match builder.build() {
                                Ok(mut reader) => {
                                    while let Some(Ok(batch)) = reader.next() {
                                        let service_col = batch
                                            .column(0)
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .unwrap();
                                        let metric_col = batch
                                            .column(1)
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .unwrap();
                                        let hash_col = batch
                                            .column(2)
                                            .as_any()
                                            .downcast_ref::<Int64Array>()
                                            .unwrap();

                                        for i in 0..batch.num_rows() {
                                            services.push(service_col.value(i).to_string());
                                            metrics.push(metric_col.value(i).to_string());
                                            hashes.push(hash_col.value(i));
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("⚠️  Corrupted Parquet file detected at '{}': {}", file_path.display(), e);
                                    eprintln!("   Deleting corrupted file and recreating...");
                                    if let Err(del_err) = std::fs::remove_file(&file_path) {
                                        eprintln!("   Failed to delete corrupted file: {}", del_err);
                                    } else {
                                        eprintln!("   ✓ Corrupted file deleted successfully");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("⚠️  Corrupted Parquet file detected at '{}': {}", file_path.display(), e);
                            eprintln!("   Deleting corrupted file and recreating...");
                            if let Err(del_err) = std::fs::remove_file(&file_path) {
                                eprintln!("   Failed to delete corrupted file: {}", del_err);
                            } else {
                                eprintln!("   ✓ Corrupted file deleted successfully");
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Failed to open existing file: {}", e));
                }
            }
        }

        // Add new records (batch)
        for record in new_records {
            services.push(record.service);
            metrics.push(record.metric);
            hashes.push(record.hash);
        }

        // Create arrays from all data
        let service_array = StringArray::from(services);
        let metric_array = StringArray::from(metrics);
        let hash_array = Int64Array::from(hashes);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(service_array),
                Arc::new(metric_array),
                Arc::new(hash_array),
            ],
        )
        .map_err(|e| format!("Failed to create record batch: {}", e))?;

        // Write to temporary file first for atomic operation
        let temp_file_path = file_path.parent()
            .ok_or("Invalid file path")?
            .join(format!("{}.tmp", file_path.file_name().unwrap().to_string_lossy()));
        
        // Ensure directory exists before creating temp file
        if let Some(parent) = temp_file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }
        
        let file = File::create(&temp_file_path)
            .map_err(|e| format!("Failed to create temp file '{}': {}", temp_file_path.display(), e))?;

        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        writer
            .write(&batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;
        
        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        // Atomic rename: only after successful write
        std::fs::rename(&temp_file_path, &file_path)
            .map_err(|e| format!("Failed to rename '{}' to '{}': {}", 
                temp_file_path.display(), file_path.display(), e))?;

        Ok(())
    }

    /// Get series count statistics grouped by service
    pub async fn get_series_stats(&self, db: &str) -> Result<HashMap<String, SeriesStats>, String> {
        let file_path = self.get_stats_file_path(db);

        if !file_path.exists() || std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) == 0 {
            return Ok(HashMap::new());
        }

        // Read the Parquet file
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(&file_path)
            .map_err(|e| format!("Failed to open stats file: {}", e))?;

        let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("⚠️  Corrupted stats file detected at '{}': {}", file_path.display(), e);
                eprintln!("   Deleting corrupted file and recreating...");
                if let Err(del_err) = std::fs::remove_file(&file_path) {
                    eprintln!("   Failed to delete corrupted file: {}", del_err);
                } else {
                    eprintln!("   ✓ Corrupted file deleted successfully");
                }
                return Ok(HashMap::new());
            }
        };
        
        let mut reader = match builder.build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("⚠️  Corrupted stats file detected at '{}': {}", file_path.display(), e);
                eprintln!("   Deleting corrupted file and recreating...");
                if let Err(del_err) = std::fs::remove_file(&file_path) {
                    eprintln!("   Failed to delete corrupted file: {}", del_err);
                } else {
                    eprintln!("   ✓ Corrupted file deleted successfully");
                }
                return Ok(HashMap::new());
            }
        };

        let mut stats: HashMap<String, HashMap<String, HashSet<i64>>> = HashMap::new();

        while let Some(Ok(batch)) = reader.next() {
            let service_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let metric_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hash_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let service = service_col.value(i).to_string();
                let metric = metric_col.value(i).to_string();
                let hash = hash_col.value(i);

                stats
                    .entry(service)
                    .or_insert_with(HashMap::new)
                    .entry(metric)
                    .or_insert_with(HashSet::new)
                    .insert(hash);
            }
        }

        // Convert to SeriesStats
        let result: HashMap<String, SeriesStats> = stats
            .into_iter()
            .map(|(service, metrics)| {
                let mut metric_stats: HashMap<String, usize> = HashMap::new();
                let mut total_series = 0;

                for (metric, hashes) in metrics {
                    let count = hashes.len();
                    metric_stats.insert(metric, count);
                    total_series += count;
                }

                (
                    service,
                    SeriesStats {
                        total_series,
                        metrics: metric_stats,
                    },
                )
            })
            .collect();

        Ok(result)
    }

    /// Load existing stats into cache on startup
    pub async fn load_cache(&self, db: &str) -> Result<(), String> {
        let file_path = self.get_stats_file_path(db);

        if !file_path.exists() || std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) == 0 {
            return Ok(());
        }

        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(&file_path)
            .map_err(|e| format!("Failed to open stats file: {}", e))?;

        let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("⚠️  Corrupted cache file detected at '{}': {}", file_path.display(), e);
                eprintln!("   Deleting corrupted file and recreating...");
                if let Err(del_err) = std::fs::remove_file(&file_path) {
                    eprintln!("   Failed to delete corrupted file: {}", del_err);
                } else {
                    eprintln!("   ✓ Corrupted file deleted successfully");
                }
                return Ok(());
            }
        };
        
        let mut reader = match builder.build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("⚠️  Corrupted cache file detected at '{}': {}", file_path.display(), e);
                eprintln!("   Deleting corrupted file and recreating...");
                if let Err(del_err) = std::fs::remove_file(&file_path) {
                    eprintln!("   Failed to delete corrupted file: {}", del_err);
                } else {
                    eprintln!("   ✓ Corrupted file deleted successfully");
                }
                return Ok(());
            }
        };

        let mut cache = self.cache.write().await;

        while let Some(Ok(batch)) = reader.next() {
            let service_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let metric_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hash_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let service = service_col.value(i).to_string();
                let metric = metric_col.value(i).to_string();
                let hash = hash_col.value(i);

                cache.insert((service, metric, hash));
            }
        }

        println!("📊 Loaded {} series into cache for database '{}'", cache.len(), db);
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SeriesStats {
    pub total_series: usize,
    pub metrics: HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_hash() {
        let tracker = SeriesTracker::new("./test_data");
        
        // Same tags in different order should produce same hash
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("type".to_string(), "http".to_string());
        tags1.insert(MONITOR_SERVICE_TAG.to_string(), "api".to_string());
        
        let mut tags2 = HashMap::new();
        tags2.insert("type".to_string(), "http".to_string());
        tags2.insert(MONITOR_SERVICE_TAG.to_string(), "api".to_string());
        tags2.insert("host".to_string(), "server1".to_string());
        
        let hash1 = tracker.calculate_tag_hash(&tags1);
        let hash2 = tracker.calculate_tag_hash(&tags2);
        
        assert_eq!(hash1, hash2, "Same tags should produce same hash");
    }

    #[test]
    fn test_tag_hash_excludes_monitor_service() {
        let tracker = SeriesTracker::new("./test_data");
        
        // Different MONITOR.service should produce same hash if other tags are same
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert(MONITOR_SERVICE_TAG.to_string(), "api".to_string());
        
        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server1".to_string());
        tags2.insert(MONITOR_SERVICE_TAG.to_string(), "web".to_string());
        
        let hash1 = tracker.calculate_tag_hash(&tags1);
        let hash2 = tracker.calculate_tag_hash(&tags2);
        
        assert_eq!(hash1, hash2, "MONITOR.service should not affect hash");
    }
}
