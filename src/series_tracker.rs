use crate::config::{SchemaConfig, SeriesTrackerConfig};
use crate::line_protocol::{format_date32_as_partition, Point};
use crate::series_dedup::{BloomConfig, StrategyManager};
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
const SERIES_STATS_SCHEMA: &str = "pmc";

/// Series record for batching
#[derive(Clone, Debug, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeriesRecord {
    pub service: String,
    pub metric: String,
    pub hash: u64,
    pub date: i32, // Date32: days since Unix epoch
}

/// Type alias for pending writes buffer: (db, date) -> records
type PendingWrites = Arc<RwLock<HashMap<(String, i32), HashSet<SeriesRecord>>>>;

/// Series tracker for monitoring metric cardinality
/// Tracks unique series by service, metric, and tag combination hash
pub struct SeriesTracker {
    data_dir: String,
    /// Strategy manager for per-database deduplication
    strategy_manager: Arc<StrategyManager>,
    /// Pending writes buffer per database and date partition: (db, date) -> records
    pending_writes: PendingWrites,
    /// Schema configuration for extracting service name
    schema_config: Arc<SchemaConfig>,
    /// Number of days to keep in cache
    cache_retention_days: u32,
}

impl SeriesTracker {
    /// Get the series stats schema directory path (_system/pmc/)
    fn get_stats_schema_dir(&self) -> PathBuf {
        Path::new(&self.data_dir)
            .join(SYSTEM_CATALOG)
            .join(SERIES_STATS_SCHEMA)
    }

    /// Get the table directory path for a database (_system/pmc/{db}/)
    fn get_table_dir(&self, db: &str) -> PathBuf {
        self.get_stats_schema_dir().join(db)
    }

    /// Get the partition directory path for a database and date
    /// Format: _system/pmc/{db}/date=YYYY-MM-DD/
    fn get_partition_dir(&self, db: &str, date: i32) -> PathBuf {
        let date_partition = format_date32_as_partition(date);
        self.get_table_dir(db).join(date_partition)
    }

    /// Get the parquet file path for a database table with date partition
    /// Format: _system/pmc/{db}/date=YYYY-MM-DD/L0_{timestamp}.parquet
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
    pub fn new(
        data_dir: &str,
        schema_config: Arc<SchemaConfig>,
        cache_retention_days: u32,
        tracker_config: &SeriesTrackerConfig,
    ) -> Self {
        // Create strategy manager with per-database strategies
        let bloom_custom = tracker_config
            .bloom_custom
            .iter()
            .map(|c| {
                (
                    c.database.clone(),
                    BloomConfig {
                        expected_series_per_day: c.expected_series_per_day,
                        false_positive_rate: c.false_positive_rate,
                    },
                )
            })
            .collect();

        let strategy_manager = Arc::new(StrategyManager::new(
            &tracker_config.default_strategy,
            tracker_config.bloom_databases.clone(),
            BloomConfig {
                expected_series_per_day: tracker_config.bloom.expected_series_per_day,
                false_positive_rate: tracker_config.bloom.false_positive_rate,
            },
            bloom_custom,
        ));

        info!(
            "SeriesTracker initialized (default strategy: {}, bloom databases: {})",
            tracker_config.default_strategy,
            tracker_config.bloom_databases.len()
        );

        let tracker = SeriesTracker {
            data_dir: data_dir.to_string(),
            strategy_manager,
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            schema_config,
            cache_retention_days,
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
                "Series tracker background flush started: every {} seconds (with cache retention)",
                flush_interval_secs
            );

            loop {
                ticker.tick().await;

                let start = Instant::now();

                // Flush pending writes
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

                // Evict old daily caches (keep latest 2 days)
                self.evict_old_daily_caches().await;
            }
        });
    }

    /// Calculate series hash for cache (without date - date is in cache key)
    fn calculate_series_hash(service: &str, metric: &str, tag_hash: u64) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        service.hash(&mut hasher);
        metric.hash(&mut hasher);
        tag_hash.hash(&mut hasher);
        hasher.finish()
    }

    /// Get the cutoff date for cache retention (days since Unix epoch)
    fn get_cutoff_date(&self) -> i32 {
        let today = chrono::Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        today - self.cache_retention_days as i32
    }

    /// Track multiple points in batch (more efficient than individual track_point calls)
    /// This method minimizes lock contention by batching strategy lookups and pending writes operations
    pub async fn track_points(&self, db: &str, points: &[Point]) {
        if points.is_empty() {
            return;
        }

        // Get or create strategy for this database
        let strategy = self.strategy_manager.get_or_create_strategy(db).await;

        // Group points by date for efficient strategy access
        let mut points_by_date: HashMap<i32, Vec<&Point>> = HashMap::new();
        for point in points {
            points_by_date
                .entry(point.date)
                .or_insert_with(Vec::new)
                .push(point);
        }

        // Process each date's points with strategy
        for (date, date_points) in points_by_date {
            let mut strategy_guard = strategy.write().await;
            let mut new_records = Vec::new();

            // Check each point against the strategy
            for point in date_points {
                let tag_hash = self.calculate_tag_hash(&point.tags);
                let series_hash = Self::calculate_series_hash(&point.service, &point.measurement, tag_hash);

                // Check if already tracked for this date and database
                if !strategy_guard.contains(db, date, series_hash) {
                    strategy_guard.insert(db, date, series_hash);
                    new_records.push(SeriesRecord {
                        service: point.service.clone(),
                        metric: point.measurement.clone(),
                        hash: tag_hash,
                        date,
                    });
                }
            }

            // Release strategy lock before acquiring pending writes lock
            drop(strategy_guard);

            // Batch insert into pending writes
            if !new_records.is_empty() {
                let mut pending = self.pending_writes.write().await;
                let key = (db.to_string(), date);
                pending
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .extend(new_records);
            }
        }
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

    /// Evict old daily caches (keep only latest N days based on config)
    async fn evict_old_daily_caches(&self) {
        let cutoff_date = self.get_cutoff_date();

        // Clear old data from all strategies
        self.strategy_manager.clear_before_date(cutoff_date).await;

        debug!(
            "Series cache retention: cleared data older than {} days (cutoff date: {})",
            self.cache_retention_days,
            cutoff_date
        );
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

    /// Load existing stats into cache on startup (only latest N days based on config)
    pub async fn load_cache(&self, db: &str) -> Result<(), String> {
        let parquet_files = self.get_all_parquet_files(db)?;

        if parquet_files.is_empty() {
            return Ok(());
        }

        // Calculate cutoff date - only load latest N days
        let cutoff_date = self.get_cutoff_date();

        // Get or create strategy for this database
        let strategy = self.strategy_manager.get_or_create_strategy(db).await;

        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let mut loaded_count = 0;
        let mut loaded_by_date: HashMap<i32, usize> = HashMap::new();
        let mut skipped_count = 0;

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

            // Skip files older than retention period
            if date < cutoff_date {
                skipped_count += 1;
                debug!(
                    "Skipping old partition {} (older than {} days)",
                    file_path.display(),
                    self.cache_retention_days
                );
                continue;
            }

            // Load data into strategy
            let mut strategy_guard = strategy.write().await;

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
                    let service = service_col.value(i);
                    let metric = metric_col.value(i);
                    let tag_hash = hash_col.value(i);

                    let series_hash = Self::calculate_series_hash(service, metric, tag_hash);

                    // Insert into strategy (strategy handles deduplication internally)
                    if !strategy_guard.contains(db, date, series_hash) {
                        strategy_guard.insert(db, date, series_hash);
                        loaded_count += 1;
                        *loaded_by_date.entry(date).or_insert(0) += 1;
                    }
                }
            }

            // Release lock after processing this file
            drop(strategy_guard);
        }

        if loaded_count > 0 || skipped_count > 0 {
            info!(
                "Loaded {} series into cache for database '{}' across {} day(s) (skipped {} old partition(s), keeping {} days)",
                loaded_count,
                db,
                loaded_by_date.len(),
                skipped_count,
                self.cache_retention_days
            );
            for (date, count) in loaded_by_date {
                debug!("  Date {}: {} series", date, count);
            }
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
        use crate::config::{BloomConfig as ConfigBloomConfig, BloomDatabaseConfig, SchemaConfig, SeriesTrackerConfig};
        let schema_config = Arc::new(SchemaConfig {
            tag_keys: vec!["service".to_string()],
            default_name: "default".to_string(),
            prefix_numeric: true,
        });
        let tracker_config = SeriesTrackerConfig {
            flush_interval_secs: 60,
            stats_dir: "pmc".to_string(),
            cache_retention_days: 2,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new("./test_data", schema_config, 2, &tracker_config);

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
        use crate::config::{BloomConfig as ConfigBloomConfig, BloomDatabaseConfig, SchemaConfig, SeriesTrackerConfig};
        let schema_config = Arc::new(SchemaConfig {
            tag_keys: vec!["service".to_string()],
            default_name: "default".to_string(),
            prefix_numeric: true,
        });
        let tracker_config = SeriesTrackerConfig {
            flush_interval_secs: 60,
            stats_dir: "pmc".to_string(),
            cache_retention_days: 2,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new("./test_data", schema_config, 2, &tracker_config);

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
