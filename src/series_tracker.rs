use crate::config::{SeriesTrackerConfig, StorageConfig};
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

/// PMC schema name: Physical Metric Count (includes all tags)
pub const SCHEMA_PMC: &str = "pmc";

/// UMC schema name: User Metric Count (excludes system tags)
pub const SCHEMA_UMC: &str = "umc";

/// Series record for batching
#[derive(Clone, Debug, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeriesRecord {
    pub service: String,
    pub metric: String,
    pub hash: u64,
    pub date: i32, // Date32: days since Unix epoch
}

/// Type alias for pending writes buffer: (schema, db, date) -> records
type PendingWrites = Arc<RwLock<HashMap<(String, String, i32), HashSet<SeriesRecord>>>>;

/// Series tracker for monitoring metric cardinality
/// Tracks unique series by service, metric, and tag combination hash
/// Supports dual tracking: PMC (physical) and UMC (user view without system tags)
pub struct SeriesTracker {
    data_dir: String,
    /// System catalog directory name (from config)
    system_dir: String,
    /// Strategy manager for per-database deduplication
    strategy_manager: Arc<StrategyManager>,
    /// Pending writes buffer per schema, database and date: (schema, db, date) -> records
    pending_writes: PendingWrites,
    /// Number of days to keep in cache
    cache_retention_days: u32,
    /// System tags to exclude for UMC calculation (from config)
    system_tags: Vec<String>,
}

impl SeriesTracker {
    /// Get the series stats schema directory path ({system_dir}/{schema}/)
    /// schema is SCHEMA_PMC or SCHEMA_UMC
    fn get_stats_schema_dir(&self, schema: &str) -> PathBuf {
        Path::new(&self.data_dir)
            .join(&self.system_dir)
            .join(schema)
    }

    /// Get the table directory path for a schema and database (_system/{schema}/{db}/)
    fn get_table_dir(&self, schema: &str, db: &str) -> PathBuf {
        self.get_stats_schema_dir(schema).join(db)
    }

    /// Get the partition directory path for a schema, database and date
    /// Format: _system/{schema}/{db}/date=YYYY-MM-DD/
    fn get_partition_dir(&self, schema: &str, db: &str, date: i32) -> PathBuf {
        let date_partition = format_date32_as_partition(date);
        self.get_table_dir(schema, db).join(date_partition)
    }

    /// Get the parquet file path for a schema and database table with date partition
    /// Format: _system/{schema}/{db}/date=YYYY-MM-DD/L0_{timestamp}.parquet
    fn get_parquet_file_path(&self, schema: &str, db: &str, date: i32) -> PathBuf {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        self.get_partition_dir(schema, db, date)
            .join(format!("L0_{}.parquet", timestamp))
    }

    /// Get all parquet files for a schema and database table (searches all date partitions)
    fn get_all_parquet_files(&self, schema: &str, db: &str) -> Result<Vec<PathBuf>, String> {
        let table_dir = self.get_table_dir(schema, db);

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
        storage_config: &StorageConfig,
        tracker_config: &SeriesTrackerConfig,
    ) -> Self {
        // Create strategy manager with per-database/schema strategies
        // Key is (database, schema) where schema can be empty string for database-wide config
        let bloom_custom = tracker_config
            .bloom_custom
            .iter()
            .map(|c| {
                let schema = c.schema.as_deref().unwrap_or("");
                (
                    (c.database.clone(), schema.to_string()),
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
            data_dir: storage_config.data_dir.clone(),
            system_dir: storage_config.system_dir.clone(),
            strategy_manager,
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            cache_retention_days: tracker_config.cache_retention_days,
            system_tags: tracker_config.system_tags.clone(),
        };

        // Clean up any leftover temp files from previous crashes
        tracker.cleanup_temp_files();

        tracker
    }

    /// Clean up temporary files left from previous crashes
    fn cleanup_temp_files(&self) {
        // Clean both PMC and UMC schemas
        for schema in [SCHEMA_PMC, SCHEMA_UMC] {
            let stats_schema_dir = self.get_stats_schema_dir(schema);

            let Ok(table_dirs) = std::fs::read_dir(&stats_schema_dir) else {
                continue;
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
    /// Tracks both PMC (physical metric count) and UMC (user metric count) independently
    pub async fn track_points(&self, db: &str, points: &[Point]) {
        if points.is_empty() {
            return;
        }

        // Get or create separate strategies for PMC and UMC (different cardinalities)
        let pmc_strategy = self.strategy_manager.get_or_create_strategy(db, SCHEMA_PMC).await;
        let umc_strategy = self.strategy_manager.get_or_create_strategy(db, SCHEMA_UMC).await;

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
            let mut pmc_strategy_guard = pmc_strategy.write().await;
            let mut umc_strategy_guard = umc_strategy.write().await;
            let mut pmc_new_records = Vec::new();
            let mut umc_new_records = Vec::new();

            // Check each point against the strategy for both PMC and UMC
            for point in date_points {
                // Track PMC (Physical Metric Count) - includes all tags
                let pmc_tag_hash = self.calculate_pmc_hash(&point.tags);
                let pmc_series_hash = Self::calculate_series_hash(&point.service, &point.measurement, pmc_tag_hash);

                if !pmc_strategy_guard.contains(SCHEMA_PMC, db, date, pmc_series_hash) {
                    pmc_strategy_guard.insert(SCHEMA_PMC, db, date, pmc_series_hash);
                    pmc_new_records.push(SeriesRecord {
                        service: point.service.clone(),
                        metric: point.measurement.clone(),
                        hash: pmc_tag_hash,
                        date,
                    });
                }

                // Track UMC (User Metric Count) - excludes system tags
                let umc_tag_hash = self.calculate_umc_hash(&point.tags);
                let umc_series_hash = Self::calculate_series_hash(&point.service, &point.measurement, umc_tag_hash);

                if !umc_strategy_guard.contains(SCHEMA_UMC, db, date, umc_series_hash) {
                    umc_strategy_guard.insert(SCHEMA_UMC, db, date, umc_series_hash);
                    umc_new_records.push(SeriesRecord {
                        service: point.service.clone(),
                        metric: point.measurement.clone(),
                        hash: umc_tag_hash,
                        date,
                    });
                }
            }

            // Release strategy locks before acquiring pending writes lock
            drop(pmc_strategy_guard);
            drop(umc_strategy_guard);

            // Batch insert into pending writes (separate keys for PMC and UMC)
            let mut pending = self.pending_writes.write().await;

            if !pmc_new_records.is_empty() {
                let key = (SCHEMA_PMC.to_string(), db.to_string(), date);
                pending
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .extend(pmc_new_records);
            }

            if !umc_new_records.is_empty() {
                let key = (SCHEMA_UMC.to_string(), db.to_string(), date);
                pending
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .extend(umc_new_records);
            }
        }
    }

    /// Calculate PMC (Physical Metric Count) hash: includes ALL tags
    /// This tracks ALL physical series including service tags and system tags
    fn calculate_pmc_hash(&self, tags: &HashMap<String, String>) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Include ALL tags - sort for stable hashing
        let mut filtered_tags: Vec<(&str, &str)> = tags
            .iter()
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

    /// Calculate UMC (User Metric Count) hash: excludes only system tags
    /// This tracks user-visible series cardinality (without system-added tags)
    fn calculate_umc_hash(&self, tags: &HashMap<String, String>) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Filter out system tags only, then sort
        let mut filtered_tags: Vec<(&str, &str)> = tags
            .iter()
            .filter(|(key, _)| {
                // Exclude system tags only
                !self.system_tags.contains(key)
            })
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

    /// Flush all pending writes for all databases (both PMC and UMC)
    pub async fn flush_all(&self) -> Result<usize, String> {
        let mut pending = self.pending_writes.write().await;

        if pending.is_empty() {
            return Ok(0);
        }

        let mut flushed_count = 0;
        for ((schema, db, date), records) in pending.drain() {
            if !records.is_empty() {
                let date_partition = format_date32_as_partition(date);
                debug!(
                    "Flushing {} series records for schema '{}', database '{}' partition '{}'",
                    records.len(),
                    schema,
                    db,
                    date_partition
                );
                match self.flush_database_partition(&schema, &db, date, records).await {
                    Ok(()) => {
                        flushed_count += 1;
                    }
                    Err(e) => {
                        error!(
                            "Failed to flush schema '{}', database '{}' partition '{}': {}",
                            schema, db, date_partition, e
                        );
                    }
                }
            }
        }

        Ok(flushed_count)
    }

    /// Flush pending writes for a specific schema and database partition (append-only write to new parquet file)
    async fn flush_database_partition(
        &self,
        schema: &str,
        db: &str,
        date: i32,
        new_records: HashSet<SeriesRecord>,
    ) -> Result<(), String> {
        let partition_dir = self.get_partition_dir(schema, db, date);
        std::fs::create_dir_all(&partition_dir)
            .map_err(|e| format!("Failed to create partition directory: {}", e))?;

        let file_path = self.get_parquet_file_path(schema, db, date);

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

    /// Get series count statistics grouped by service for a specific schema
    pub async fn get_series_stats(&self, schema: &str, db: &str) -> Result<HashMap<String, SeriesStats>, String> {
        let parquet_files = self.get_all_parquet_files(schema, db)?;

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
    pub async fn load_cache(&self, schema: &str, db: &str) -> Result<(), String> {
        let parquet_files = self.get_all_parquet_files(schema, db)?;

        if parquet_files.is_empty() {
            return Ok(());
        }

        // Calculate cutoff date - only load latest N days
        let cutoff_date = self.get_cutoff_date();

        // Get or create strategy for this database and schema
        let strategy = self.strategy_manager.get_or_create_strategy(db, schema).await;

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
                    if !strategy_guard.contains(schema, db, date, series_hash) {
                        strategy_guard.insert(schema, db, date, series_hash);
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
                "Loaded {} series into cache for schema '{}', database '{}' across {} day(s) (skipped {} old partition(s), keeping {} days)",
                loaded_count,
                schema,
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

    /// Get total memory usage of deduplication strategies
    pub async fn total_dedup_memory_bytes(&self) -> usize {
        self.strategy_manager.total_memory_bytes().await
    }

    /// Get detailed stats per database
    pub async fn get_dedup_stats(&self) -> HashMap<String, crate::series_dedup::StrategyStats> {
        self.strategy_manager.get_stats().await
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
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        let storage_config = StorageConfig {
            data_dir: "./test_data".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec![],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new(&storage_config, &tracker_config);

        // Same tags in different order should produce same hash
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("type".to_string(), "http".to_string());
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("type".to_string(), "http".to_string());
        tags2.insert("service".to_string(), "api".to_string());
        tags2.insert("host".to_string(), "server1".to_string());

        let hash1 = tracker.calculate_pmc_hash(&tags1);
        let hash2 = tracker.calculate_pmc_hash(&tags2);

        assert_eq!(hash1, hash2, "Same tags should produce same hash");
    }

    #[test]
    fn test_pmc_includes_all_tags() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        let storage_config = StorageConfig {
            data_dir: "./test_data".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec![],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new(&storage_config, &tracker_config);

        // PMC includes ALL tags - different service should produce different hash
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server1".to_string());
        tags2.insert("service".to_string(), "web".to_string());

        let hash1 = tracker.calculate_pmc_hash(&tags1);
        let hash2 = tracker.calculate_pmc_hash(&tags2);

        assert_ne!(hash1, hash2, "PMC should include all tags - different service produces different hash");
    }

    #[test]
    fn test_umc_excludes_system_tags() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        let storage_config = StorageConfig {
            data_dir: "./test_data".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string(), "MONITOR.hostName".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new(&storage_config, &tracker_config);

        // UMC should exclude system tags
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());
        tags1.insert("MONITOR.hostName".to_string(), "host1".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server1".to_string());
        tags2.insert("MONITOR.ip".to_string(), "192.168.1.2".to_string()); // Different IP
        tags2.insert("MONITOR.hostName".to_string(), "host2".to_string()); // Different hostname

        let umc_hash1 = tracker.calculate_umc_hash(&tags1);
        let umc_hash2 = tracker.calculate_umc_hash(&tags2);

        // UMC hashes should be the same (system tags excluded)
        assert_eq!(umc_hash1, umc_hash2, "UMC should exclude system tags - same user tags should produce same hash");

        // PMC hashes should be different (includes all tags)
        let pmc_hash1 = tracker.calculate_pmc_hash(&tags1);
        let pmc_hash2 = tracker.calculate_pmc_hash(&tags2);
        assert_ne!(pmc_hash1, pmc_hash2, "PMC should include all tags - different system tags should produce different hash");
    }

    #[test]
    fn test_umc_and_pmc_different_when_system_tags_present() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        let storage_config = StorageConfig {
            data_dir: "./test_data".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new(&storage_config, &tracker_config);

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());

        let umc_hash = tracker.calculate_umc_hash(&tags);
        let pmc_hash = tracker.calculate_pmc_hash(&tags);

        assert_ne!(umc_hash, pmc_hash, "UMC and PMC should produce different hashes when system tags are present");
    }

    #[test]
    fn test_umc_stable_hash_order() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        let storage_config = StorageConfig {
            data_dir: "./test_data".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = SeriesTracker::new(&storage_config, &tracker_config);

        // UMC hash should be stable regardless of tag insertion order
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("region".to_string(), "us-west".to_string());
        tags1.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("region".to_string(), "us-west".to_string());
        tags2.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());
        tags2.insert("host".to_string(), "server1".to_string());

        let hash1 = tracker.calculate_umc_hash(&tags1);
        let hash2 = tracker.calculate_umc_hash(&tags2);

        assert_eq!(hash1, hash2, "UMC hash should be stable regardless of tag order");
    }

    #[tokio::test]
    async fn test_track_points_basic() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;

        let storage_config = StorageConfig {
            data_dir: "./test_data_track".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());

        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                timestamp: Some(1234567890),
                date: today,
                service: "test_service".to_string(),
            },
        ];

        tracker.track_points("testdb", &points).await;

        // Verify pending writes exist
        let pending = tracker.pending_writes.read().await;
        assert!(pending.len() > 0, "Should have pending writes");

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_track");
    }

    #[tokio::test]
    async fn test_track_points_deduplication() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;

        let storage_config = StorageConfig {
            data_dir: "./test_data_dedup".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());

        // Track the same point twice
        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                timestamp: Some(1234567890),
                date: today,
                service: "test_service".to_string(),
            },
        ];

        tracker.track_points("testdb", &points).await;
        tracker.track_points("testdb", &points).await; // Duplicate

        // Check pending writes - should only have one unique record
        let pending = tracker.pending_writes.read().await;

        for ((schema, db, _date), records) in pending.iter() {
            if schema == "umc" && db == "testdb" {
                // Should have exactly 1 unique record despite 2 track_points calls
                assert_eq!(records.len(), 1, "Deduplication should prevent duplicate records");
            }
        }

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_dedup");
    }

    #[tokio::test]
    async fn test_track_points_pmc_umc_independence() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;

        let storage_config = StorageConfig {
            data_dir: "./test_data_independence".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        // Create two points with same user tags but different system tags
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server1".to_string());
        tags1.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server1".to_string());
        tags2.insert("MONITOR.ip".to_string(), "192.168.1.2".to_string()); // Different IP

        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: tags1,
                timestamp: Some(1234567890),
                date: today,
                service: "test_service".to_string(),
            },
            Point {
                measurement: "cpu".to_string(),
                tags: tags2,
                timestamp: Some(1234567891),
                date: today,
                service: "test_service".to_string(),
            },
        ];

        tracker.track_points("testdb", &points).await;

        // Check pending writes
        let pending = tracker.pending_writes.read().await;

        let mut pmc_count = 0;
        let mut umc_count = 0;

        for ((schema, db, _date), records) in pending.iter() {
            if db == "testdb" {
                if schema == "pmc" {
                    pmc_count = records.len();
                } else if schema == "umc" {
                    umc_count = records.len();
                }
            }
        }

        // PMC should have 2 records (different system tags)
        assert_eq!(pmc_count, 2, "PMC should track both series (different system tags)");

        // UMC should have 1 record (same user tags)
        assert_eq!(umc_count, 1, "UMC should deduplicate series with same user tags");

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_independence");
    }

    #[tokio::test]
    async fn test_concurrent_track_points() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;
        use tokio::task;

        let storage_config = StorageConfig {
            data_dir: "./test_data_concurrent".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        // Spawn 20 concurrent tasks that all track the SAME series
        let mut handles = vec![];

        for i in 0..20 {
            let tracker_clone = Arc::clone(&tracker);
            let handle = task::spawn(async move {
                let mut tags = HashMap::new();
                tags.insert("host".to_string(), "server1".to_string());
                tags.insert("MONITOR.ip".to_string(), format!("192.168.1.{}", i));

                let points = vec![
                    Point {
                        measurement: "cpu".to_string(),
                        tags,
                        timestamp: Some(1234567890 + i as i64),
                        date: today,
                        service: "test_service".to_string(),
                    },
                ];

                tracker_clone.track_points("concurrent_db", &points).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Check pending writes
        let pending = tracker.pending_writes.read().await;

        for ((schema, db, _date), records) in pending.iter() {
            if db == "concurrent_db" {
                if schema == "pmc" {
                    // PMC should have 20 records (different IPs)
                    assert_eq!(records.len(), 20, "PMC should track all 20 series with different IPs");
                } else if schema == "umc" {
                    // UMC should have 1 record (same host tag)
                    assert_eq!(records.len(), 1, "UMC should deduplicate to 1 series (same user tags)");
                }
            }
        }

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_concurrent");
    }

    #[tokio::test]
    async fn test_concurrent_same_series_deduplication() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;
        use tokio::task;

        let storage_config = StorageConfig {
            data_dir: "./test_data_same_series".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        // Spawn 50 concurrent tasks that all track the EXACT SAME series
        let mut handles = vec![];

        for _ in 0..50 {
            let tracker_clone = Arc::clone(&tracker);
            let handle = task::spawn(async move {
                let mut tags = HashMap::new();
                tags.insert("host".to_string(), "server1".to_string());
                tags.insert("MONITOR.ip".to_string(), "192.168.1.100".to_string());

                let points = vec![
                    Point {
                        measurement: "cpu".to_string(),
                        tags,
                        timestamp: Some(1234567890),
                        date: today,
                        service: "test_service".to_string(),
                    },
                ];

                tracker_clone.track_points("same_series_db", &points).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Check pending writes - should only have 1 unique record
        let pending = tracker.pending_writes.read().await;

        for ((schema, db, _date), records) in pending.iter() {
            if db == "same_series_db" {
                // Both PMC and UMC should have exactly 1 record (same series)
                assert_eq!(records.len(), 1, "Schema {} should have exactly 1 record despite 50 concurrent calls", schema);
            }
        }

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_same_series");
    }

    #[tokio::test]
    async fn test_multiple_databases_isolation() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;

        let storage_config = StorageConfig {
            data_dir: "./test_data_multi_db".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec![],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());

        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                timestamp: Some(1234567890),
                date: today,
                service: "test_service".to_string(),
            },
        ];

        // Track same series in different databases
        tracker.track_points("db1", &points).await;
        tracker.track_points("db2", &points).await;
        tracker.track_points("db3", &points).await;

        // Check pending writes - each database should have its own records
        let pending = tracker.pending_writes.read().await;

        let mut db_count = std::collections::HashSet::new();
        for ((schema, db, _date), _records) in pending.iter() {
            if schema == "pmc" {
                db_count.insert(db.clone());
            }
        }

        assert_eq!(db_count.len(), 3, "Should track series separately for each database");

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_multi_db");
    }

    #[tokio::test]
    async fn test_load_series_cache() {
        use crate::config::{BloomConfig as ConfigBloomConfig, SeriesTrackerConfig, StorageConfig};
        use chrono::Utc;

        let storage_config = StorageConfig {
            data_dir: "./test_data_load_cache".to_string(),
            system_dir: "_system".to_string(),
        };
        let tracker_config = SeriesTrackerConfig {
            track_only: false,
            flush_interval_secs: 60,
            system_tags: vec!["MONITOR.ip".to_string()],
            cache_retention_days: 2,
            max_concurrent_tables: 8,
            default_strategy: "hashset".to_string(),
            bloom_databases: vec![],
            bloom: ConfigBloomConfig {
                expected_series_per_day: 1_000_000_000,
                false_positive_rate: 0.01,
            },
            bloom_custom: vec![],
        };
        let tracker = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        let today = Utc::now()
            .date_naive()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;

        // Create test data
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("MONITOR.ip".to_string(), "192.168.1.1".to_string());

        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                timestamp: Some(1234567890),
                date: today,
                service: "test_service".to_string(),
            },
        ];

        // Track and flush
        tracker.track_points("testdb", &points).await;
        tracker.flush_all().await.unwrap();

        // Create a new tracker to simulate restart
        let tracker2 = Arc::new(SeriesTracker::new(&storage_config, &tracker_config));

        // Load cache
        let result = tracker2.load_cache(SCHEMA_PMC, "testdb").await;
        assert!(result.is_ok(), "Load cache should succeed");

        let result = tracker2.load_cache(SCHEMA_UMC, "testdb").await;
        assert!(result.is_ok(), "Load cache should succeed");

        // Verify data loaded: track same point again, should be deduplicated
        tracker2.track_points("testdb", &points).await;

        let pending = tracker2.pending_writes.read().await;

        // Should have no new pending writes because series was already in cache
        for ((schema, db, _date), records) in pending.iter() {
            if schema == SCHEMA_PMC && db == "testdb" {
                assert!(records.is_empty(), "Should have no new records after cache reload - deduplication works");
            }
        }

        // Clean up
        let _ = std::fs::remove_dir_all("./test_data_load_cache");
    }
}
