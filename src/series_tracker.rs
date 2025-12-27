use crate::config::SchemaConfig;
use crate::line_protocol::{format_date32_as_partition, Point};
use arrow::array::UInt64Array;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

// System catalog/schema constants (catalog.schema.table format)
const SYSTEM_CATALOG: &str = "_system";
const SERIES_STATS_SCHEMA: &str = "series_stats";

/// Series record for batching
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct SeriesRecord {
    service: String,
    metric: String,
    hash: u64,
    date: i32, // Date32: days since Unix epoch
}

/// Type alias for series cache: (service, metric, hash, date) -> exists
type SeriesCache = Arc<RwLock<HashSet<(String, String, u64, i32)>>>;

/// Type alias for pending writes buffer: (db, date) -> records
type PendingWrites = Arc<RwLock<HashMap<(String, i32), HashSet<SeriesRecord>>>>;

/// Series tracker for monitoring metric cardinality
/// Tracks unique series by service, metric, and tag combination hash
pub struct SeriesTracker {
    data_dir: String,
    /// In-memory cache: (service, metric, hash, date) -> exists
    cache: SeriesCache,
    /// Pending writes buffer per database and date partition: (db, date) -> records
    pending_writes: PendingWrites,
    /// Schema configuration for extracting service name
    schema_config: Arc<SchemaConfig>,
}

impl SeriesTracker {
    /// Get the series stats schema directory path (_system/series_stats/)
    fn get_stats_schema_dir(&self) -> PathBuf {
        Path::new(&self.data_dir)
            .join(SYSTEM_CATALOG)
            .join(SERIES_STATS_SCHEMA)
    }

    /// Get the table directory path for a database (_system/series_stats/{db}/)
    fn get_table_dir(&self, db: &str) -> PathBuf {
        self.get_stats_schema_dir().join(db)
    }

    /// Get the partition directory path for a database and date
    /// Format: _system/series_stats/{db}/date=YYYY-MM-DD/
    fn get_partition_dir(&self, db: &str, date: i32) -> PathBuf {
        let date_partition = format_date32_as_partition(date);
        self.get_table_dir(db).join(date_partition)
    }

    /// Get the parquet file path for a database table with date partition
    /// Format: _system/series_stats/{db}/date=YYYY-MM-DD/L0_{timestamp}.parquet
    fn get_parquet_file_path(&self, db: &str, date: i32) -> PathBuf {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        self.get_partition_dir(db, date)
            .join(format!("L0_{}.parquet", timestamp))
    }

    /// Get all parquet files for a database table (searches all date partitions)
    fn get_all_parquet_files(&self, db: &str) -> Result<Vec<PathBuf>, String> {
        let table_dir = self.get_table_dir(db);

        if !table_dir.exists() {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();

        // Iterate through date partition directories
        let entries = std::fs::read_dir(&table_dir)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        for entry in entries.flatten() {
            let path = entry.path();

            // Check for date partition directories (date=YYYY-MM-DD)
            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    if dir_name.starts_with("date=") {
                        // Read parquet files in partition directory
                        if let Ok(partition_entries) = std::fs::read_dir(&path) {
                            for pentry in partition_entries.flatten() {
                                let ppath = pentry.path();
                                if ppath.is_file()
                                    && ppath.extension().and_then(|s| s.to_str()) == Some("parquet")
                                {
                                    files.push(ppath);
                                }
                            }
                        }
                    }
                }
            }
            // Also check for legacy non-partitioned files directly in table dir
            else if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet")
            {
                files.push(path);
            }
        }

        files.sort();
        Ok(files)
    }
}

impl SeriesTracker {
    pub fn new(data_dir: &str, schema_config: Arc<SchemaConfig>) -> Self {
        let tracker = SeriesTracker {
            data_dir: data_dir.to_string(),
            cache: Arc::new(RwLock::new(HashSet::new())),
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            schema_config,
        };

        // Clean up any leftover temp files from previous crashes
        tracker.cleanup_temp_files();

        tracker
    }

    /// Clean up temporary files left from previous crashes
    fn cleanup_temp_files(&self) {
        let stats_schema_dir = self.get_stats_schema_dir();

        let Ok(table_dirs) = std::fs::read_dir(&stats_schema_dir) else {
            return;
        };

        for table in table_dirs.flatten() {
            let path = table.path();
            if !path.is_dir() {
                continue;
            }

            let Ok(files) = std::fs::read_dir(&path) else {
                continue;
            };

            for file in files.flatten() {
                let file_path = file.path();

                if file_path.extension().and_then(|ext| ext.to_str()) != Some("tmp") {
                    continue;
                }

                match std::fs::remove_file(&file_path) {
                    Ok(_) => {
                        info!("Cleaned up leftover temp file: {}", file_path.display());
                    }
                    Err(e) => {
                        warn!(
                            "Failed to cleanup temp file '{}': {}",
                            file_path.display(),
                            e
                        );
                    }
                }
            }
        }
    }

    /// Start background flush task
    pub fn start_background_flush(self: Arc<Self>, flush_interval_secs: u64) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));

            info!(
                "Series tracker background flush started: every {} seconds",
                flush_interval_secs
            );

            loop {
                ticker.tick().await;

                let start = Instant::now();
                match self.flush_all().await {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "Series tracker flush: {} database(s) updated in {:?}",
                                count,
                                start.elapsed()
                            );
                        }
                    }
                    Err(e) => {
                        error!("Series tracker flush error: {}", e);
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
            // Extract service name using schema config (same logic as extract_schema in cache.rs)
            let service = self.extract_service_name(&point.tags);

            let hash = self.calculate_tag_hash(&point.tags);
            let date = point.date;
            let key = (service.clone(), point.measurement.clone(), hash, date);

            // Check if already tracked
            {
                let cache = self.cache.read().await;
                if cache.contains(&key) {
                    continue;
                }
            }

            new_records.push((
                key,
                SeriesRecord {
                    service,
                    metric: point.measurement.clone(),
                    hash,
                    date,
                },
            ));
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

        // Batch insert into pending writes (single write lock), grouped by (db, date)
        if !actually_new.is_empty() {
            let mut pending = self.pending_writes.write().await;
            for record in actually_new {
                let key = (db.to_string(), record.date);
                pending
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .insert(record);
            }
        }
    }

    /// Extract service name from tags using schema config
    /// Iterates through tag_keys in order, returning the first match
    fn extract_service_name(&self, tags: &HashMap<String, String>) -> String {
        for tag_key in &self.schema_config.tag_keys {
            if let Some(value) = tags.get(tag_key) {
                return if self.schema_config.prefix_numeric && value.chars().all(|c| c.is_numeric())
                {
                    format!("service_{}", value)
                } else {
                    value.clone()
                };
            }
        }
        self.schema_config.default_name.clone()
    }

    fn extract_service_key(&self, tags: &HashMap<String, String>) -> Option<&str> {
        self.schema_config
            .tag_keys
            .iter()
            .find(|tag_key| tags.contains_key(*tag_key))
            .map(|s| s.as_str())
    }

    /// Calculate hash of tags excluding MONITOR.service
    fn calculate_tag_hash(&self, tags: &HashMap<String, String>) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let service_key = self.extract_service_key(tags);

        // Filter out MONITOR.service and sort the rest
        let mut filtered_tags: Vec<(&str, &str)> = tags
            .iter()
            .filter(|(key, _)| Some(key.as_str()) != service_key)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        filtered_tags.sort();

        // Create a stable hash from sorted tags
        let mut hasher = DefaultHasher::new();
        for (key, value) in filtered_tags {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Flush all pending writes for all databases
    pub async fn flush_all(&self) -> Result<usize, String> {
        let mut pending = self.pending_writes.write().await;

        if pending.is_empty() {
            return Ok(0);
        }

        let mut flushed_count = 0;
        for ((db, date), records) in pending.drain() {
            if !records.is_empty() {
                let date_partition = format_date32_as_partition(date);
                debug!(
                    "Flushing {} series records for database '{}' partition '{}'",
                    records.len(),
                    db,
                    date_partition
                );
                match self.flush_database_partition(&db, date, records).await {
                    Ok(()) => {
                        flushed_count += 1;
                    }
                    Err(e) => {
                        error!(
                            "Failed to flush database '{}' partition '{}': {}",
                            db, date_partition, e
                        );
                    }
                }
            }
        }

        Ok(flushed_count)
    }

    /// Flush pending writes for a specific database partition (append-only write to new parquet file)
    async fn flush_database_partition(
        &self,
        db: &str,
        date: i32,
        new_records: HashSet<SeriesRecord>,
    ) -> Result<(), String> {
        let partition_dir = self.get_partition_dir(db, date);
        std::fs::create_dir_all(&partition_dir)
            .map_err(|e| format!("Failed to create partition directory: {}", e))?;

        let file_path = self.get_parquet_file_path(db, date);

        // Note: date is NOT in schema - it's a Hive partition column (date=YYYY-MM-DD in path)
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("hash", DataType::UInt64, false),
        ]));

        // Convert new records directly to arrays (append-only, no merge)
        let mut services = Vec::new();
        let mut metrics = Vec::new();
        let mut hashes = Vec::new();

        for record in new_records {
            services.push(record.service);
            metrics.push(record.metric);
            hashes.push(record.hash);
        }

        // Create arrays from new data
        let service_array = StringArray::from(services);
        let metric_array = StringArray::from(metrics);
        let hash_array = UInt64Array::from(hashes);

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
        let temp_file_path = file_path.parent().ok_or("Invalid file path")?.join(format!(
            "{}.tmp",
            file_path.file_name().unwrap().to_string_lossy()
        ));

        let file = File::create(&temp_file_path).map_err(|e| {
            format!(
                "Failed to create temp file '{}': {}",
                temp_file_path.display(),
                e
            )
        })?;

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
        std::fs::rename(&temp_file_path, &file_path).map_err(|e| {
            format!(
                "Failed to rename '{}' to '{}': {}",
                temp_file_path.display(),
                file_path.display(),
                e
            )
        })?;

        Ok(())
    }

    /// Get series count statistics grouped by service
    pub async fn get_series_stats(&self, db: &str) -> Result<HashMap<String, SeriesStats>, String> {
        let parquet_files = self.get_all_parquet_files(db)?;

        if parquet_files.is_empty() {
            return Ok(HashMap::new());
        }

        // Read all Parquet files in the table directory
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let mut stats: HashMap<String, HashMap<String, HashSet<u64>>> = HashMap::new();

        for file_path in parquet_files {
            if std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) == 0 {
                continue;
            }

            let file =
                File::open(&file_path).map_err(|e| format!("Failed to open stats file: {}", e))?;

            let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        "Corrupted stats file detected at '{}': {}",
                        file_path.display(),
                        e
                    );
                    warn!("Deleting corrupted file...");
                    if let Err(del_err) = std::fs::remove_file(&file_path) {
                        error!("Failed to delete corrupted file: {}", del_err);
                    } else {
                        info!("Corrupted file deleted successfully");
                    }
                    continue;
                }
            };

            let mut reader = match builder.build() {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Corrupted stats file detected at '{}': {}",
                        file_path.display(),
                        e
                    );
                    warn!("Deleting corrupted file...");
                    if let Err(del_err) = std::fs::remove_file(&file_path) {
                        error!("Failed to delete corrupted file: {}", del_err);
                    } else {
                        info!("Corrupted file deleted successfully");
                    }
                    continue;
                }
            };

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
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let service = service_col.value(i).to_string();
                    let metric = metric_col.value(i).to_string();
                    let hash = hash_col.value(i);

                    stats
                        .entry(service)
                        .or_default()
                        .entry(metric)
                        .or_default()
                        .insert(hash);
                }
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
    #[allow(dead_code)]
    pub async fn load_cache(&self, db: &str) -> Result<(), String> {
        let parquet_files = self.get_all_parquet_files(db)?;

        if parquet_files.is_empty() {
            return Ok(());
        }

        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let mut cache = self.cache.write().await;
        let mut loaded_count = 0;

        for file_path in parquet_files {
            if std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) == 0 {
                continue;
            }

            let file =
                File::open(&file_path).map_err(|e| format!("Failed to open stats file: {}", e))?;

            let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        "Corrupted cache file detected at '{}': {}",
                        file_path.display(),
                        e
                    );
                    warn!("Deleting corrupted file...");
                    if let Err(del_err) = std::fs::remove_file(&file_path) {
                        error!("Failed to delete corrupted file: {}", del_err);
                    } else {
                        info!("Corrupted file deleted successfully");
                    }
                    continue;
                }
            };

            let mut reader = match builder.build() {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Corrupted cache file detected at '{}': {}",
                        file_path.display(),
                        e
                    );
                    warn!("Deleting corrupted file...");
                    if let Err(del_err) = std::fs::remove_file(&file_path) {
                        error!("Failed to delete corrupted file: {}", del_err);
                    } else {
                        info!("Corrupted file deleted successfully");
                    }
                    continue;
                }
            };

            // Extract date from partition path (date=YYYY-MM-DD)
            let date = Self::extract_date_from_path(&file_path);

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
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let service = service_col.value(i).to_string();
                    let metric = metric_col.value(i).to_string();
                    let hash = hash_col.value(i);

                    if cache.insert((service, metric, hash, date)) {
                        loaded_count += 1;
                    }
                }
            }
        }

        if loaded_count > 0 {
            info!(
                "Loaded {} series into cache for database '{}'",
                loaded_count, db
            );
        }
        Ok(())
    }

    /// Extract date (days since epoch) from partition path like ".../date=2024-01-15/..."
    fn extract_date_from_path(path: &Path) -> i32 {
        for ancestor in path.ancestors() {
            if let Some(dir_name) = ancestor.file_name().and_then(|s| s.to_str()) {
                if let Some(date_str) = dir_name.strip_prefix("date=") {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        return (date - epoch).num_days() as i32;
                    }
                }
            }
        }
        0
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
        use crate::config::SchemaConfig;
        let schema_config = Arc::new(SchemaConfig {
            tag_keys: vec!["service".to_string()],
            default_name: "default".to_string(),
            prefix_numeric: true,
        });
        let tracker = SeriesTracker::new("./test_data", schema_config);

        // Same tags in different order should produce same hash
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("type".to_string(), "http".to_string());
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("type".to_string(), "http".to_string());
        tags2.insert("service".to_string(), "api".to_string());
        tags2.insert("host".to_string(), "server1".to_string());

        let hash1 = tracker.calculate_tag_hash(&tags1);
        let hash2 = tracker.calculate_tag_hash(&tags2);

        assert_eq!(hash1, hash2, "Same tags should produce same hash");
    }

    #[test]
    fn test_tag_hash_excludes_service() {
        use crate::config::SchemaConfig;
        let schema_config = Arc::new(SchemaConfig {
            tag_keys: vec!["service".to_string()],
            default_name: "default".to_string(),
            prefix_numeric: true,
        });
        let tracker = SeriesTracker::new("./test_data", schema_config);

        // Different service should produce same hash if other tags are same
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server1".to_string());
        tags2.insert("service".to_string(), "web".to_string());

        let hash1 = tracker.calculate_tag_hash(&tags1);
        let hash2 = tracker.calculate_tag_hash(&tags2);

        assert_eq!(hash1, hash2, "service tag should not affect hash");
    }
}
