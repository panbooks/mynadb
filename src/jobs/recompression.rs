//! Background Recompression Job
//!
//! Periodically scans partitions and rewrites Parquet files with optimal
//! compression based on partition age. When using an adaptive compression
//! strategy, data that ages from "hot" to "warm" to "cold" benefits from
//! different codecs (e.g., Snappy for hot, ZSTD-9 for cold).
//!
//! The recompression job:
//! 1. Discovers all tables and partitions via storage
//! 2. Calculates each partition's age in days
//! 3. Reads existing Parquet files and rewrites them with the compression
//!    appropriate for their current age tier
//! 4. Tracks which partitions have been recompressed to avoid redundant work

use crate::jobs::database_manager::DatabaseManager;
use crate::storage::compression::CompressionStrategy;
use crate::storage::layout::STORE_DIR;
use crate::storage::storage::Storage;
use arrow::array::RecordBatch;
use bytes::Bytes;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

/// Row group size for recompressed files (same as compactor)
const ROW_GROUP_SIZE: usize = 1_000_000;

/// Maximum concurrent recompression operations
const DEFAULT_MAX_CONCURRENT: usize = 2;

/// Background recompression job
pub struct RecompressionJob {
    storage: Arc<Storage>,
    db_manager: Arc<DatabaseManager>,
    compression_strategy: CompressionStrategy,
    interval_secs: u64,
    max_concurrent: usize,
    /// Tracks partitions we've already recompressed at their current age tier
    /// Key: "catalog/schema/table/date=YYYY-MM-DD:tier" where tier is "hot"/"warm"/"cold"
    processed: Arc<RwLock<HashSet<String>>>,
}

impl RecompressionJob {
    /// Create a new recompression job
    pub fn new(
        storage: Arc<Storage>,
        db_manager: Arc<DatabaseManager>,
        compression_strategy: CompressionStrategy,
        interval_secs: u64,
    ) -> Self {
        Self {
            storage,
            db_manager,
            compression_strategy,
            interval_secs,
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            processed: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Start background recompression loop
    pub fn start_background(self: Arc<Self>) {
        if !self.compression_strategy.is_adaptive() {
            info!("Recompression: skipped (compression strategy is not adaptive)");
            return;
        }

        let interval_secs = self.interval_secs;
        info!(
            "Starting background recompression (interval: {}s)",
            interval_secs
        );

        tokio::spawn(async move {
            use tokio::time::{interval, Duration};
            let mut ticker = interval(Duration::from_secs(interval_secs));

            loop {
                ticker.tick().await;
                if let Err(e) = self.run_cycle().await {
                    error!("Recompression cycle error: {}", e);
                }
            }
        });
    }

    /// Run one recompression cycle
    async fn run_cycle(&self) -> Result<(), String> {
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
        let today = chrono::Utc::now().date_naive();

        // Discover all catalogs under store/
        let catalogs = self.storage.list_prefixes(STORE_DIR).await?;
        let mut total_recompressed = 0u64;
        let mut total_bytes_saved = 0i64;

        for catalog in &catalogs {
            if catalog.starts_with('.') || catalog.starts_with('_') {
                continue;
            }

            let catalog_prefix = format!("{}/{}", STORE_DIR, catalog);
            let schemas = match self.storage.list_prefixes(&catalog_prefix).await {
                Ok(s) => s,
                Err(_) => continue,
            };

            for schema in &schemas {
                if schema.starts_with('.') || schema.ends_with(".json") || schema.starts_with('_') {
                    continue;
                }

                let schema_prefix = format!("{}/{}/{}", STORE_DIR, catalog, schema);
                let tables = match self.storage.list_prefixes(&schema_prefix).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                for table in &tables {
                    let table_prefix = format!("{}/{}/{}/{}", STORE_DIR, catalog, schema, table);
                    let partition_dirs =
                        match self.storage.list_prefixes(&table_prefix).await {
                            Ok(p) => p,
                            Err(_) => continue,
                        };

                    for dir_name in &partition_dirs {
                        if !dir_name.starts_with("date=") {
                            continue;
                        }

                        let date_str = dir_name.strip_prefix("date=").unwrap_or(dir_name);
                        let partition_age = match partition_age_days(date_str, &today) {
                            Some(age) => age,
                            None => continue,
                        };

                        // Determine the age tier for this partition
                        let tier = self.age_tier(partition_age);

                        // Check if we've already recompressed at this tier
                        let tracking_key = format!(
                            "{}/{}/{}/{}:{}",
                            catalog, schema, table, dir_name, tier
                        );
                        {
                            let processed = self.processed.read().await;
                            if processed.contains(&tracking_key) {
                                continue;
                            }
                        }

                        // Skip "hot" tier — files are already written with hot compression
                        // Only recompress when data transitions to warm or cold
                        if tier == "hot" {
                            continue;
                        }

                        let partition_prefix = format!(
                            "{}/{}/{}/{}/{}",
                            STORE_DIR, catalog, schema, table, dir_name
                        );

                        // Acquire semaphore
                        let _permit = semaphore
                            .acquire()
                            .await
                            .map_err(|e| format!("Semaphore error: {}", e))?;

                        match self
                            .recompress_partition(&partition_prefix, partition_age)
                            .await
                        {
                            Ok(Some((files, bytes_saved))) => {
                                total_recompressed += files;
                                total_bytes_saved += bytes_saved;

                                // Mark as processed at this tier
                                let mut processed = self.processed.write().await;
                                processed.insert(tracking_key);
                            }
                            Ok(None) => {
                                // No work needed — mark as processed
                                let mut processed = self.processed.write().await;
                                processed.insert(tracking_key);
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to recompress {}: {}",
                                    partition_prefix, e
                                );
                            }
                        }
                    }
                }
            }
        }

        if total_recompressed > 0 {
            info!(
                "Recompression cycle complete: {} files recompressed, {} bytes saved",
                total_recompressed, total_bytes_saved
            );
        } else {
            debug!("Recompression cycle: no work needed");
        }

        Ok(())
    }

    /// Recompress all files in a partition with the optimal compression for its age
    ///
    /// Returns (files_recompressed, bytes_saved) or None if no work needed
    async fn recompress_partition(
        &self,
        partition_prefix: &str,
        partition_age_days: u32,
    ) -> Result<Option<(u64, i64)>, String> {
        // List all parquet files in this partition (including node_id= subdirs)
        let all_files = self.storage.list(partition_prefix).await?;
        let parquet_files: Vec<String> = all_files
            .into_iter()
            .filter(|f| f.ends_with(".parquet"))
            .collect();

        if parquet_files.is_empty() {
            return Ok(None);
        }

        let mut total_files = 0u64;
        let mut total_bytes_saved = 0i64;

        for file_path in &parquet_files {
            // Read the existing file
            let original_bytes = match self.storage.get(file_path).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("Failed to read {}: {}", file_path, e);
                    continue;
                }
            };

            let original_size = original_bytes.len() as i64;

            // Parse into record batches
            let reader = match ParquetRecordBatchReaderBuilder::try_new(original_bytes) {
                Ok(builder) => match builder.build() {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Failed to build reader for {}: {}", file_path, e);
                        continue;
                    }
                },
                Err(e) => {
                    warn!("Failed to open {}: {}", file_path, e);
                    continue;
                }
            };

            let mut batches: Vec<RecordBatch> = Vec::new();
            for batch_result in reader {
                match batch_result {
                    Ok(batch) => batches.push(batch),
                    Err(e) => {
                        warn!("Failed to read batch from {}: {}", file_path, e);
                        continue;
                    }
                }
            }

            if batches.is_empty() {
                continue;
            }

            let schema = batches[0].schema();

            // Build writer properties with compression for the partition's current age
            let props = self.compression_strategy.build_writer_properties(
                &schema,
                Some(partition_age_days),
                ROW_GROUP_SIZE,
            );

            // Write to buffer with new compression
            let mut buffer = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))
                .map_err(|e| format!("Failed to create writer: {}", e))?;

            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|e| format!("Failed to write batch: {}", e))?;
            }

            writer
                .close()
                .map_err(|e| format!("Failed to close writer: {}", e))?;

            let new_size = buffer.len() as i64;
            let bytes_saved = original_size - new_size;

            // Only write back if we actually improved compression (or it's close enough)
            // Allow up to 5% size increase (different codec overhead) to keep optimal codec
            let size_increase_threshold = (original_size as f64 * 0.05) as i64;
            if new_size > original_size + size_increase_threshold {
                debug!(
                    "Skipping {}: new size {} > original {} (would increase by {})",
                    file_path, new_size, original_size, -bytes_saved
                );
                continue;
            }

            // Write back to storage (atomic replace)
            self.storage
                .put(file_path, Bytes::from(buffer))
                .await
                .map_err(|e| format!("Failed to write recompressed file: {}", e))?;

            total_files += 1;
            total_bytes_saved += bytes_saved;

            debug!(
                "Recompressed {} (age {}d): {} → {} bytes (saved {})",
                file_path, partition_age_days, original_size, new_size, bytes_saved
            );
        }

        if total_files > 0 {
            info!(
                "Recompressed partition {}: {} files, {} bytes saved",
                partition_prefix, total_files, total_bytes_saved
            );
            Ok(Some((total_files, total_bytes_saved)))
        } else {
            Ok(None)
        }
    }

    /// Determine the age tier name for tracking purposes
    fn age_tier(&self, age_days: u32) -> &'static str {
        match &self.compression_strategy {
            CompressionStrategy::Adaptive(config) => {
                if age_days <= config.hot_days {
                    "hot"
                } else if age_days >= config.cold_days {
                    "cold"
                } else {
                    "warm"
                }
            }
            CompressionStrategy::Fixed(_) => "fixed",
        }
    }
}

/// Calculate partition age in days from date string
fn partition_age_days(date_str: &str, today: &NaiveDate) -> Option<u32> {
    let partition_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
    let age = (*today - partition_date).num_days();
    if age < 0 {
        None
    } else {
        Some(age as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_age_days() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 7).unwrap();
        assert_eq!(
            partition_age_days("2026-02-07", &today),
            Some(0)
        );
        assert_eq!(
            partition_age_days("2026-02-01", &today),
            Some(6)
        );
        assert_eq!(
            partition_age_days("2026-01-07", &today),
            Some(31)
        );
        assert_eq!(partition_age_days("2026-02-08", &today), None); // future
        assert_eq!(partition_age_days("invalid", &today), None);
    }

    #[test]
    fn test_age_tier() {
        use crate::storage::compression::AdaptiveConfig;
        use parquet::basic::{Compression, ZstdLevel};

        let strategy = CompressionStrategy::Adaptive(AdaptiveConfig {
            default: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_compression: Compression::SNAPPY,
            cold_compression: Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
            numeric_compression: Compression::LZ4,
            string_compression: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            hot_days: 7,
            cold_days: 30,
        });

        let storage = Arc::new(Storage::new_local("/tmp/test-recomp-tier").unwrap());
        let db_manager = Arc::new(DatabaseManager::with_storage("/tmp/test-recomp-tier", storage.clone()));
        let job = RecompressionJob::new(storage, db_manager, strategy, 3600);

        assert_eq!(job.age_tier(0), "hot");
        assert_eq!(job.age_tier(5), "hot");
        assert_eq!(job.age_tier(7), "hot");
        assert_eq!(job.age_tier(8), "warm");
        assert_eq!(job.age_tier(29), "warm");
        assert_eq!(job.age_tier(30), "cold");
        assert_eq!(job.age_tier(365), "cold");
    }

    #[test]
    fn test_age_tier_fixed() {
        let strategy = CompressionStrategy::default(); // Fixed ZSTD-3

        let storage = Arc::new(Storage::new_local("/tmp/test-recomp-fixed").unwrap());
        let db_manager = Arc::new(DatabaseManager::with_storage("/tmp/test-recomp-fixed", storage.clone()));
        let job = RecompressionJob::new(storage, db_manager, strategy, 3600);

        // Fixed strategy always returns "fixed"
        assert_eq!(job.age_tier(0), "fixed");
        assert_eq!(job.age_tier(100), "fixed");
    }
}
