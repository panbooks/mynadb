use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Timelike;
use futures::stream::{FuturesUnordered, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

use crate::query::catalog::{FileInfo, SharedMetadataManager};
use crate::services::rate_limiter::RateLimiter;
use crate::storage::storage::Storage;

// Compaction constants
const LEVEL_PREFIX: char = 'L';
const LEVEL_SEPARATOR: char = '_';
const PARQUET_EXT: &str = ".parquet";

// Default compaction thresholds
const DEFAULT_MIN_FILES: usize = 5;
const DEFAULT_MAJOR_MIN_FILES: usize = 4; // Minimum files for L1+ compaction
const DEFAULT_TARGET_FILE_SIZE_MB: u64 = 500;
const DEFAULT_MAX_LEVEL: u32 = 4; // L0, L1, L2, L3, L4
const MB_TO_BYTES: u64 = 1024 * 1024;

// Parquet optimization constants
const ROW_GROUP_SIZE: usize = 1_000_000; // 1M rows per row group
const BLOOM_FILTER_FPP: f64 = 0.01; // 1% false positive rate
const BLOOM_FILTER_NDV: u64 = 10_000; // Expected distinct values

/// Type alias for compaction locks map
/// Key: (catalog, schema, table, date_partition)
type CompactionLocks =
    Arc<RwLock<HashMap<(String, String, String, String), Arc<tokio::sync::Mutex<()>>>>>;

/// Compactor is responsible for merging small Parquet files into larger ones
/// using a leveled compaction strategy (similar to LSM-tree)
pub struct Compactor {
    data_dir: String,
    /// Storage backend (for future S3 support)
    #[allow(dead_code)]
    storage: Option<Arc<Storage>>,
    /// Lock to prevent concurrent compaction and read/write operations
    locks: CompactionLocks,
    /// Semaphore to limit concurrent LSM compaction operations
    compaction_semaphore: Option<Arc<Semaphore>>,
    /// Semaphore to limit concurrent full compaction operations
    full_compaction_semaphore: Option<Arc<Semaphore>>,
    /// Read rate limiter (None = unlimited)
    read_rate_limiter: Option<Arc<RateLimiter>>,
    /// Write rate limiter (None = unlimited)
    write_rate_limiter: Option<Arc<RateLimiter>>,
    /// Metadata manager for updating file registry after compaction
    metadata_manager: Option<SharedMetadataManager>,
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of L0 files to trigger minor compaction (L0 → L1)
    pub min_files: usize,
    /// Minimum number of files at L1+ to trigger major compaction (L1 → L2, L2 → L3, etc.)
    pub major_min_files: usize,
    /// Target file size after compaction (default: 500MB) for LSM compaction
    pub target_file_size: u64,
    /// Maximum compaction level (0-4: L0, L1, L2, L3, L4)
    pub max_level: u32,

    // Full compaction settings (date-based)
    /// Enable full compaction for yesterday's partitions
    pub full_compaction_enabled: bool,
    /// Hour to run full compaction (0-23, default: 1 for 01:00)
    pub full_compaction_hour: u32,
    /// Target file size for full compaction in bytes (default: 200MB)
    /// Partitions larger than this will be skipped
    pub full_compaction_target_size: u64,

    // Rate limiting settings
    /// Maximum concurrent LSM compaction operations (0 = unlimited)
    pub max_concurrent_compactions: usize,
    /// Maximum concurrent full compaction operations (0 = unlimited)
    pub full_compaction_max_concurrent: usize,
    /// Read rate limit in bytes/sec (0 = unlimited)
    pub read_rate_limit_bytes_per_sec: u64,
    /// Write rate limit in bytes/sec (0 = unlimited)
    pub write_rate_limit_bytes_per_sec: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            min_files: DEFAULT_MIN_FILES,
            major_min_files: DEFAULT_MAJOR_MIN_FILES,
            target_file_size: DEFAULT_TARGET_FILE_SIZE_MB * MB_TO_BYTES,
            max_level: DEFAULT_MAX_LEVEL,
            full_compaction_enabled: true,
            full_compaction_hour: 3,                        // 03:00
            full_compaction_target_size: 200 * MB_TO_BYTES, // 200MB
            max_concurrent_compactions: 8,
            full_compaction_max_concurrent: 2,
            read_rate_limit_bytes_per_sec: 0,  // Unlimited
            write_rate_limit_bytes_per_sec: 0, // Unlimited
        }
    }
}

impl From<&crate::config::CompactionConfig> for CompactionConfig {
    fn from(config: &crate::config::CompactionConfig) -> Self {
        CompactionConfig {
            min_files: config.min_files,
            major_min_files: config.major_min_files,
            target_file_size: config.target_file_size_mb * MB_TO_BYTES,
            max_level: config.max_level,
            full_compaction_enabled: config.full_compaction_enabled,
            full_compaction_hour: config.full_compaction_hour,
            full_compaction_target_size: config.full_compaction_target_size_mb * MB_TO_BYTES,
            max_concurrent_compactions: config.max_concurrent_compactions,
            full_compaction_max_concurrent: config.full_compaction_max_concurrent,
            read_rate_limit_bytes_per_sec: config.read_rate_limit_mb_per_sec * MB_TO_BYTES,
            write_rate_limit_bytes_per_sec: config.write_rate_limit_mb_per_sec * MB_TO_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct CompactionResult {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date_partition: String,
    pub l0_files_before: usize,
    pub l0_files_after: usize,
    pub files_before: usize,
    pub files_after: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub rows_compacted: usize,
}

#[derive(Debug, Clone)]
struct ParquetFileInfo {
    path: PathBuf,
    size: u64,
    level: u32,
    timestamp: i64,
}

impl ParquetFileInfo {
    fn from_path(path: PathBuf) -> Result<Self, String> {
        let metadata = fs::metadata(&path).map_err(|e| format!("Failed to get metadata: {}", e))?;

        let size = metadata.len();

        // Parse filename: L0_1703251234567.parquet or 1703251234567.parquet (legacy)
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("Invalid filename")?;

        let (level, timestamp) = if filename.starts_with(LEVEL_PREFIX) {
            // New format: L0_1703251234567.parquet
            let parts: Vec<&str> = filename.split(LEVEL_SEPARATOR).collect();
            if parts.len() >= 2 {
                let level = parts[0]
                    .trim_start_matches(LEVEL_PREFIX)
                    .parse::<u32>()
                    .map_err(|_| "Invalid level in filename")?;
                let timestamp = parts[1]
                    .trim_end_matches(PARQUET_EXT)
                    .parse::<i64>()
                    .map_err(|_| "Invalid timestamp in filename")?;
                (level, timestamp)
            } else {
                return Err("Invalid level format".into());
            }
        } else {
            // Legacy format: 1703251234567.parquet (treat as L0)
            let timestamp = filename
                .trim_end_matches(PARQUET_EXT)
                .parse::<i64>()
                .map_err(|_| "Invalid timestamp in legacy format")?;
            (0, timestamp)
        };

        Ok(ParquetFileInfo {
            path,
            size,
            level,
            timestamp,
        })
    }
}

impl Compactor {
    pub fn with_config(data_dir: &str, config: &CompactionConfig) -> Self {
        Self::with_storage_metadata_and_config(data_dir, None, config, None)
    }

    pub fn with_storage(data_dir: &str, storage: Arc<Storage>, config: &CompactionConfig) -> Self {
        Self::with_storage_metadata_and_config(data_dir, Some(storage), config, None)
    }

    pub fn with_storage_and_metadata(
        data_dir: &str,
        storage: Arc<Storage>,
        config: &CompactionConfig,
        metadata_manager: SharedMetadataManager,
    ) -> Self {
        Self::with_storage_metadata_and_config(
            data_dir,
            Some(storage),
            config,
            Some(metadata_manager),
        )
    }

    fn with_storage_metadata_and_config(
        data_dir: &str,
        storage: Option<Arc<Storage>>,
        config: &CompactionConfig,
        metadata_manager: Option<SharedMetadataManager>,
    ) -> Self {
        // Create semaphore if max_concurrent_compactions > 0
        let compaction_semaphore = if config.max_concurrent_compactions > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_compactions)))
        } else {
            None
        };

        // Create semaphore for full compaction if full_compaction_max_concurrent > 0
        let full_compaction_semaphore = if config.full_compaction_max_concurrent > 0 {
            Some(Arc::new(Semaphore::new(
                config.full_compaction_max_concurrent,
            )))
        } else {
            None
        };

        // Create rate limiters if configured
        let read_rate_limiter = if config.read_rate_limit_bytes_per_sec > 0 {
            Some(Arc::new(RateLimiter::new(
                config.read_rate_limit_bytes_per_sec,
            )))
        } else {
            None
        };

        let write_rate_limiter = if config.write_rate_limit_bytes_per_sec > 0 {
            Some(Arc::new(RateLimiter::new(
                config.write_rate_limit_bytes_per_sec,
            )))
        } else {
            None
        };

        Compactor {
            data_dir: data_dir.to_string(),
            storage,
            locks: Arc::new(RwLock::new(HashMap::new())),
            compaction_semaphore,
            full_compaction_semaphore,
            read_rate_limiter,
            write_rate_limiter,
            metadata_manager,
        }
    }

    /// Get or create a lock for a specific catalog/schema/table/partition tuple
    async fn get_lock(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_partition: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            date_partition.to_string(),
        );
        let mut locks = self.locks.write().await;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Create a FileInfo struct for registering with the metadata manager
    fn create_file_info(
        path: &str,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
        level: u8,
        row_count: u64,
        size_bytes: u64,
    ) -> FileInfo {
        let file_name = path.rsplit('/').next().unwrap_or(path).to_string();

        FileInfo {
            path: path.to_string(),
            file_name,
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            partition_key: partition_key.to_string(),
            size_bytes,
            row_count,
            level,
            min_timestamp: None,
            max_timestamp: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            row_group_count: 1,
            column_stats: None,
            etag: None,
        }
    }

    /// Compact all tables in a catalog
    /// Now runs tables in parallel (up to max_concurrent_compactions)
    pub async fn compact_catalog(
        &self,
        catalog: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog);

        if !Path::new(&catalog_path).exists() {
            return Err(format!("Catalog '{}' does not exist", catalog));
        }

        // Phase 1: Collect all tables that need compaction
        let mut tables_to_compact: Vec<(String, String)> = Vec::new();

        // List all schemas (subdirectories under catalog)
        let schema_entries = fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        for schema_entry in schema_entries {
            let schema_entry =
                schema_entry.map_err(|e| format!("Failed to read schema entry: {}", e))?;
            let schema_path = schema_entry.path();

            // Skip non-directories and config files
            if !schema_path.is_dir() {
                continue;
            }

            let schema_name = match schema_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // List all tables (subdirectories under schema)
            let table_entries = match fs::read_dir(&schema_path) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(
                        "Failed to read schema directory {}/{}: {}",
                        catalog, schema_name, e
                    );
                    continue;
                }
            };

            for table_entry in table_entries {
                let table_entry = match table_entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        warn!("Failed to read table entry: {}", e);
                        continue;
                    }
                };
                let table_path = table_entry.path();

                // Skip non-directories
                if !table_path.is_dir() {
                    continue;
                }

                if let Some(table_name) = table_path.file_name().and_then(|n| n.to_str()) {
                    tables_to_compact.push((schema_name.clone(), table_name.to_string()));
                }
            }
        }

        if tables_to_compact.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Catalog {}: found {} tables for compaction (max {} concurrent)",
            catalog,
            tables_to_compact.len(),
            config.max_concurrent_compactions
        );

        // Phase 2: Process tables concurrently using FuturesUnordered
        let mut tasks = FuturesUnordered::new();

        for (schema_name, table_name) in tables_to_compact {
            // Clone strings to move into the future
            let catalog_owned = catalog.to_string();
            let schema_owned = schema_name.clone();
            let table_owned = table_name.clone();
            let config_clone = config.clone();

            let future = async move {
                self.compact_table_all_partitions(
                    &catalog_owned,
                    &schema_owned,
                    &table_owned,
                    &config_clone,
                )
                .await
            };
            tasks.push(future);
        }

        // Collect results as they complete
        let mut results = Vec::new();
        while let Some(result) = tasks.next().await {
            match result {
                Ok(table_results) => results.extend(table_results),
                Err(e) => {
                    warn!("Table compaction failed: {}", e);
                }
            }
        }

        Ok(results)
    }

    /// Compact all partitions in a table (or the table itself if not partitioned)
    /// Runs sequentially within a table since most tables only have 1-2 active partitions
    pub async fn compact_table_all_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);

        if !Path::new(&table_path).exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Check if table has partitions or is a flat table
        let has_partitions = self.has_partitions(&table_path)?;

        if has_partitions {
            // Partitioned table: compact each partition separately (sequentially)
            // Most tables only have 1-2 active partitions (today + maybe yesterday)
            let entries = fs::read_dir(&table_path)
                .map_err(|e| format!("Failed to read table directory: {}", e))?;

            for entry in entries {
                let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
                let path = entry.path();

                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                        // Check for Hive-style partition format: date=YYYY-MM-DD
                        if dir_name.starts_with("date=") {
                            match self
                                .compact_table_partition(catalog, schema, table, dir_name, config)
                                .await
                            {
                                Ok(Some(result)) => results.push(result),
                                Ok(None) => {
                                    // No compaction needed for this partition
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to compact partition {}/{}/{}/{}: {}",
                                        catalog, schema, table, dir_name, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Non-partitioned table: compact directly in table directory
            debug!(
                "Compacting non-partitioned table: {}.{}.{}",
                catalog, schema, table
            );
            match self
                .compact_non_partitioned_table(catalog, schema, table, config)
                .await
            {
                Ok(Some(result)) => results.push(result),
                Ok(None) => {
                    // No compaction needed
                }
                Err(e) => {
                    warn!(
                        "Failed to compact non-partitioned table {}/{}/{}: {}",
                        catalog, schema, table, e
                    );
                }
            }
        }

        Ok(results)
    }

    /// Check if a table has partitions (contains subdirectories with date= prefix)
    fn has_partitions(&self, table_path: &str) -> Result<bool, String> {
        let entries = fs::read_dir(table_path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    if dir_name.starts_with("date=") {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Full compaction for yesterday's partitions (date-based, no metadata overhead)
    /// Now runs partitions in parallel (up to max_concurrent_compactions)
    pub async fn full_compact_yesterday(
        &self,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        if !config.full_compaction_enabled {
            return Ok(Vec::new());
        }

        let now = chrono::Local::now();
        let current_hour = now.hour();

        // Only run at configured hour (default: 01:00)
        if current_hour != config.full_compaction_hour {
            return Ok(Vec::new());
        }

        // Calculate yesterday's date
        let yesterday = (now - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        let yesterday_partition = format!("date={}", yesterday);

        info!(
            "Starting full compaction for yesterday: {} (hour {})",
            yesterday_partition, current_hour
        );

        // Phase 1: Collect all partitions that need compaction
        let mut partitions_to_compact: Vec<(String, String, String, PathBuf)> = Vec::new();

        // Scan all catalogs
        let catalog_entries =
            fs::read_dir(&self.data_dir).map_err(|e| format!("Failed to read data dir: {}", e))?;

        for catalog_entry in catalog_entries.flatten() {
            if !catalog_entry.path().is_dir() {
                continue;
            }

            let catalog = match catalog_entry.file_name().to_str() {
                Some(name) if !name.starts_with('.') => name.to_string(),
                _ => continue,
            };

            // Scan schemas
            let schema_entries = match fs::read_dir(&catalog_entry.path()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for schema_entry in schema_entries.flatten() {
                if !schema_entry.path().is_dir() {
                    continue;
                }

                let schema = match schema_entry.file_name().to_str() {
                    Some(name) if !name.starts_with('.') => name.to_string(),
                    _ => continue,
                };

                // Scan tables
                let table_entries = match fs::read_dir(&schema_entry.path()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                for table_entry in table_entries.flatten() {
                    if !table_entry.path().is_dir() {
                        continue;
                    }

                    let table = match table_entry.file_name().to_str() {
                        Some(name) => name.to_string(),
                        None => continue,
                    };

                    // Check if yesterday's partition exists
                    let partition_path = table_entry.path().join(&yesterday_partition);

                    if !partition_path.exists() {
                        continue;
                    }

                    // Add to compaction queue
                    partitions_to_compact.push((
                        catalog.clone(),
                        schema.clone(),
                        table.clone(),
                        partition_path,
                    ));
                }
            }
        }

        if partitions_to_compact.is_empty() {
            debug!(
                "No partitions found for full compaction on {}",
                yesterday_partition
            );
            return Ok(Vec::new());
        }

        info!(
            "Found {} partitions for full compaction (max {} concurrent)",
            partitions_to_compact.len(),
            config.full_compaction_max_concurrent
        );

        // Phase 2: Process partitions concurrently using FuturesUnordered
        let mut tasks = FuturesUnordered::new();

        for (catalog, schema, table, partition_path) in partitions_to_compact {
            // Clone strings to move into the future
            let catalog_owned = catalog.clone();
            let schema_owned = schema.clone();
            let table_owned = table.clone();
            let partition_name_owned = yesterday_partition.clone();
            let partition_path_owned = partition_path.clone();
            let config_clone = config.clone();

            let future = async move {
                self.full_compact_partition(
                    &catalog_owned,
                    &schema_owned,
                    &table_owned,
                    &partition_name_owned,
                    &partition_path_owned,
                    &config_clone,
                )
                .await
            };
            tasks.push(future);
        }

        // Collect results as they complete
        let mut results = Vec::new();
        let mut completed = 0;
        let total = tasks.len();

        while let Some(result) = tasks.next().await {
            completed += 1;
            match result {
                Ok(Some(r)) => {
                    info!(
                        "Full compaction [{}/{}]: {}.{}.{}/{} → {} files, {:.2} MB",
                        completed,
                        total,
                        r.catalog,
                        r.schema,
                        r.table,
                        r.date_partition,
                        r.files_after,
                        r.bytes_after as f64 / (1024.0 * 1024.0)
                    );
                    results.push(r);
                }
                Ok(None) => {
                    debug!(
                        "Full compaction [{}/{}]: skipped (no work needed)",
                        completed, total
                    );
                }
                Err(e) => {
                    warn!("Full compaction [{}/{}]: failed - {}", completed, total, e);
                }
            }
        }

        if !results.is_empty() {
            info!(
                "Full compaction complete for {}: {} partitions compacted",
                yesterday_partition,
                results.len()
            );
        }

        Ok(results)
    }

    /// Full compact a single partition
    /// Filters out files >= full_compaction_target_size to reduce I/O
    async fn full_compact_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_name: &str,
        partition_path: &Path,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        // Acquire full compaction semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.full_compaction_semaphore {
            Some(
                semaphore
                    .acquire()
                    .await
                    .map_err(|e| format!("Failed to acquire full compaction semaphore: {}", e))?,
            )
        } else {
            None
        };

        let start = Instant::now();

        // List all parquet files with their sizes
        let all_files: Vec<(PathBuf, u64)> = fs::read_dir(partition_path)
            .map_err(|e| format!("Failed to read partition: {}", e))?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path().is_file()
                    && e.path().extension().and_then(|s| s.to_str()) == Some("parquet")
            })
            .filter_map(|e| {
                let path = e.path();
                fs::metadata(&path).ok().map(|m| (path, m.len()))
            })
            .collect();

        if all_files.len() < 2 {
            debug!(
                "Partition {}/{}/{}/{} has {} file(s), skipping",
                catalog,
                schema,
                table,
                partition_name,
                all_files.len()
            );
            return Ok(None);
        }

        let files_before = all_files.len();
        let bytes_before: u64 = all_files.iter().map(|(_, size)| size).sum();

        // Filter out files >= full_compaction_target_size to reduce I/O
        // Already-large files don't need to be merged
        let compactable_files: Vec<(PathBuf, u64)> = all_files
            .iter()
            .filter(|(_, size)| *size < config.full_compaction_target_size)
            .cloned()
            .collect();

        let oversized_files: Vec<(PathBuf, u64)> = all_files
            .iter()
            .filter(|(_, size)| *size >= config.full_compaction_target_size)
            .cloned()
            .collect();

        let filtered_count = oversized_files.len();
        if filtered_count > 0 {
            let oversized_bytes: u64 = oversized_files.iter().map(|(_, size)| size).sum();
            debug!(
                "Filtered out {} oversized files ({:.2} MB) in partition {}/{}/{}/{}",
                filtered_count,
                oversized_bytes as f64 / MB_TO_BYTES as f64,
                catalog,
                schema,
                table,
                partition_name
            );
        }

        // If no compactable files or only 1 compactable file, skip
        if compactable_files.len() < 2 {
            debug!(
                "Partition {}/{}/{}/{}: {} compactable file(s) (< 2), skipping full compaction",
                catalog,
                schema,
                table,
                partition_name,
                compactable_files.len()
            );
            return Ok(None);
        }

        let compactable_bytes: u64 = compactable_files.iter().map(|(_, size)| size).sum();

        info!(
            "Full compaction: {}/{}/{}/{} ({} files, {:.2} MB total, compacting {} files/{:.2} MB, {} oversized files skipped)",
            catalog,
            schema,
            table,
            partition_name,
            files_before,
            bytes_before as f64 / MB_TO_BYTES as f64,
            compactable_files.len(),
            compactable_bytes as f64 / MB_TO_BYTES as f64,
            filtered_count
        );

        // Extract just the paths for reading
        let files_to_compact: Vec<PathBuf> =
            compactable_files.iter().map(|(p, _)| p.clone()).collect();

        // Read compactable files
        let batches = self.read_parquet_files(&files_to_compact).await?;
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        // Write consolidated file
        let timestamp = chrono::Utc::now().timestamp_millis();
        let output = partition_path.join(format!("FULL_{}.parquet", timestamp));

        self.write_compacted_file(output.to_str().unwrap(), batches)
            .await?;

        let output_size = fs::metadata(&output)
            .map(|m| m.len())
            .unwrap_or(compactable_bytes);

        info!(
            "Created: {} ({} rows, {:.2} MB from {:.2} MB)",
            output.display(),
            total_rows,
            output_size as f64 / MB_TO_BYTES as f64,
            compactable_bytes as f64 / MB_TO_BYTES as f64
        );

        // Register new FULL file with metadata manager
        if let Some(ref manager) = self.metadata_manager {
            let file_info = Self::create_file_info(
                output.to_str().unwrap_or_default(),
                catalog,
                schema,
                table,
                partition_name,
                255, // FULL files use level 255 to distinguish from LSM levels
                total_rows as u64,
                output_size,
            );
            if let Err(e) = manager.register_file(file_info).await {
                warn!(
                    "Failed to register compacted file {}: {}",
                    output.display(),
                    e
                );
            }
        }

        // Delete old compacted files (keep oversized files)
        let mut deleted_paths: Vec<String> = Vec::new();
        for (file, _) in &compactable_files {
            if let Err(e) = fs::remove_file(file) {
                warn!("Failed to delete {}: {}", file.display(), e);
            } else {
                debug!("Deleted: {}", file.display());
                deleted_paths.push(file.to_string_lossy().to_string());
            }
        }

        // Remove deleted files from metadata manager
        if let Some(ref manager) = self.metadata_manager {
            if !deleted_paths.is_empty() {
                if let Err(e) = manager.remove_files(deleted_paths).await {
                    warn!("Failed to remove files from metadata: {}", e);
                }
                // Save metadata after full compaction
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after full compaction: {}", e);
                }
            }
        }

        let files_after = 1 + filtered_count; // 1 new FULL file + oversized files remaining

        info!(
            "Full compaction complete: {}/{}/{}/{}: {} → {} files ({} compacted, {} oversized kept), {:.2} MB → {:.2} MB in {:?}",
            catalog,
            schema,
            table,
            partition_name,
            files_before,
            files_after,
            compactable_files.len(),
            filtered_count,
            bytes_before as f64 / MB_TO_BYTES as f64,
            (output_size + oversized_files.iter().map(|(_, s)| s).sum::<u64>()) as f64 / MB_TO_BYTES as f64,
            start.elapsed()
        );

        Ok(Some(CompactionResult {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date_partition: partition_name.to_string(),
            l0_files_before: files_before,
            l0_files_after: 0,
            files_before,
            files_after,
            bytes_before,
            bytes_after: output_size + oversized_files.iter().map(|(_, s)| s).sum::<u64>(),
            rows_compacted: total_rows,
        }))
    }

    /// Compact a non-partitioned table (e.g., pmc)
    async fn compact_non_partitioned_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
        self.compact_path_internal(
            catalog,
            schema,
            table,
            "",
            &table_path,
            "[non-partitioned]",
            config,
        )
        .await
    }

    /// Compact a specific partition within a table
    pub async fn compact_table_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_partition: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        let partition_path = format!(
            "{}/{}/{}/{}/{}",
            self.data_dir, catalog, schema, table, date_partition
        );
        self.compact_path_internal(
            catalog,
            schema,
            table,
            date_partition,
            &partition_path,
            date_partition,
            config,
        )
        .await
    }

    /// Internal compaction logic shared by partitioned and non-partitioned tables
    async fn compact_path_internal(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_id: &str,
        path: &str,
        display_partition: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        // Acquire semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.compaction_semaphore {
            Some(
                semaphore
                    .acquire()
                    .await
                    .map_err(|e| format!("Failed to acquire semaphore: {}", e))?,
            )
        } else {
            None
        };

        let start = Instant::now();
        let lock = self.get_lock(catalog, schema, table, partition_id).await;
        let _guard = lock.lock().await;

        if !Path::new(path).exists() {
            return Err(format!(
                "Path '{}.{}.{}/{}' does not exist",
                catalog, schema, table, display_partition
            ));
        }

        // List and parse all parquet files
        let mut file_infos = self.list_parquet_file_infos(path)?;

        if file_infos.is_empty() {
            return Ok(None);
        }

        // Sort by level (ascending) then timestamp (ascending)
        file_infos.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        let total_files_before = file_infos.len();
        let l0_files_before = file_infos.iter().filter(|f| f.level == 0).count();
        let total_bytes_before: u64 = file_infos.iter().map(|f| f.size).sum();

        debug!(
            "Compacting {}.{}.{}/{}: {} files ({} L0), {:.2} MB",
            catalog,
            schema,
            table,
            display_partition,
            total_files_before,
            l0_files_before,
            total_bytes_before as f64 / 1024.0 / 1024.0
        );

        // Perform multi-level compaction
        let mut total_rows = 0;
        let mut compaction_happened = false;

        // Step 1: Minor compaction (L0 → L1) - highest priority
        if l0_files_before >= config.min_files {
            let l0_files: Vec<_> = file_infos
                .iter()
                .filter(|f| f.level == 0)
                .cloned()
                .collect();

            info!("Minor compaction: {} L0 files → L1", l0_files.len());

            let rows = self
                .compact_level_files(
                    path,
                    &l0_files,
                    0,
                    config,
                    catalog,
                    schema,
                    table,
                    partition_id,
                )
                .await?;
            total_rows += rows;
            compaction_happened = true;
        }

        // Step 2: Major compactions (L1 → L2, L2 → L3, etc.)
        let file_infos_updated = self.list_parquet_file_infos(path)?;

        for current_level in 1..=config.max_level {
            let all_level_files: Vec<_> = file_infos_updated
                .iter()
                .filter(|f| f.level == current_level)
                .cloned()
                .collect();

            // Filter out files >= target_file_size to reduce I/O
            let level_files: Vec<_> = all_level_files
                .iter()
                .filter(|f| f.size < config.target_file_size)
                .cloned()
                .collect();

            let filtered_count = all_level_files.len() - level_files.len();
            if filtered_count > 0 {
                debug!(
                    "Filtered out {} oversized files at L{} (>= {:.2} MB)",
                    filtered_count,
                    current_level,
                    config.target_file_size as f64 / 1024.0 / 1024.0
                );
            }

            if level_files.len() >= config.major_min_files {
                if current_level < config.max_level {
                    info!(
                        "Major compaction: {} L{} files → L{} ({} oversized files skipped)",
                        level_files.len(),
                        current_level,
                        current_level + 1,
                        filtered_count
                    );
                } else {
                    info!(
                        "Max-level compaction: {} L{} files → L{} (same level, {} oversized files skipped)",
                        level_files.len(),
                        current_level,
                        current_level,
                        filtered_count
                    );
                }

                let rows = self
                    .compact_level_files(
                        path,
                        &level_files,
                        current_level,
                        config,
                        catalog,
                        schema,
                        table,
                        partition_id,
                    )
                    .await?;
                total_rows += rows;
                compaction_happened = true;
            }
        }

        // Save metadata after all compaction operations complete
        if compaction_happened {
            if let Some(ref manager) = self.metadata_manager {
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after LSM compaction: {}", e);
                }
            }
        }

        if !compaction_happened {
            return Ok(None);
        }

        // Recalculate files after compaction
        let file_infos_after = self.list_parquet_file_infos(path)?;
        let total_files_after = file_infos_after.len();
        let l0_files_after = file_infos_after.iter().filter(|f| f.level == 0).count();
        let total_bytes_after: u64 = file_infos_after.iter().map(|f| f.size).sum();

        info!(
            "Compaction complete: {}.{}.{}/{}: {} -> {} files, {:.2} MB -> {:.2} MB in {:?}",
            catalog,
            schema,
            table,
            display_partition,
            total_files_before,
            total_files_after,
            total_bytes_before as f64 / 1024.0 / 1024.0,
            total_bytes_after as f64 / 1024.0 / 1024.0,
            start.elapsed()
        );

        Ok(Some(CompactionResult {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date_partition: display_partition.to_string(),
            l0_files_before,
            l0_files_after,
            files_before: total_files_before,
            files_after: total_files_after,
            bytes_before: total_bytes_before,
            bytes_after: total_bytes_after,
            rows_compacted: total_rows,
        }))
    }

    /// Compact files at a specific level to the next level
    /// If at max_level, compacts within the same level to reduce file count
    /// Returns the total number of rows compacted
    async fn compact_level_files(
        &self,
        partition_path: &str,
        level_files: &[ParquetFileInfo],
        current_level: u32,
        config: &CompactionConfig,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> Result<usize, String> {
        if level_files.is_empty() {
            return Ok(0);
        }

        // At max_level, compact within same level; otherwise, compact to next level
        let target_level = if current_level >= config.max_level {
            current_level // Stay at max level
        } else {
            current_level + 1 // Promote to next level
        };

        let mut total_rows = 0;

        // Group files by size to avoid exceeding target file size
        let file_groups = self.group_files_by_size(level_files, config)?;

        for (group_idx, group) in file_groups.iter().enumerate() {
            // Need at least 2 files to compact (unless it's the only group)
            if group.len() < 2 && file_groups.len() > 1 {
                continue;
            }

            debug!(
                "Compacting L{} group {}: {} files",
                current_level,
                group_idx + 1,
                group.len()
            );

            let paths: Vec<PathBuf> = group.iter().map(|f| f.path.clone()).collect();
            let batches = self.read_parquet_files(&paths).await?;
            let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            total_rows += rows;

            // Create compacted file at target level
            let timestamp = chrono::Utc::now().timestamp_millis();
            let compacted_filename = format!(
                "{}/{}{}{}{}{}",
                partition_path, LEVEL_PREFIX, target_level, LEVEL_SEPARATOR, timestamp, PARQUET_EXT
            );

            self.write_compacted_file(&compacted_filename, batches)
                .await?;

            // Get the size of the newly written file for metadata
            let file_size = fs::metadata(&compacted_filename)
                .map(|m| m.len())
                .unwrap_or(0);

            // Register new file with metadata manager
            if let Some(ref manager) = self.metadata_manager {
                let file_info = Self::create_file_info(
                    &compacted_filename,
                    catalog,
                    schema,
                    table,
                    partition_key,
                    target_level as u8,
                    rows as u64,
                    file_size,
                );
                if let Err(e) = manager.register_file(file_info).await {
                    warn!(
                        "Failed to register compacted file {}: {}",
                        compacted_filename, e
                    );
                }
            }

            if target_level == current_level {
                info!(
                    "Created L{} file: {} ({} rows from {} L{} files) [max-level compaction]",
                    target_level,
                    Path::new(&compacted_filename)
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    rows,
                    group.len(),
                    current_level
                );
            } else {
                info!(
                    "Created L{} file: {} ({} rows from {} L{} files)",
                    target_level,
                    Path::new(&compacted_filename)
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    rows,
                    group.len(),
                    current_level
                );
            }

            // Delete old files after successful write
            let mut deleted_paths: Vec<String> = Vec::new();
            for file in &paths {
                if let Err(e) = fs::remove_file(file) {
                    warn!("Failed to delete {}: {}", file.display(), e);
                } else {
                    debug!("Deleted: {}", file.display());
                    deleted_paths.push(file.to_string_lossy().to_string());
                }
            }

            // Remove deleted files from metadata manager
            if let Some(ref manager) = self.metadata_manager {
                if !deleted_paths.is_empty() {
                    if let Err(e) = manager.remove_files(deleted_paths).await {
                        warn!("Failed to remove files from metadata: {}", e);
                    }
                }
            }
        }

        Ok(total_rows)
    }

    /// List all parquet files with level information
    fn list_parquet_file_infos(&self, dir: &str) -> Result<Vec<ParquetFileInfo>, String> {
        let entries = fs::read_dir(dir).map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut file_infos = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        if let Ok(info) = ParquetFileInfo::from_path(path) {
                            file_infos.push(info);
                        }
                    }
                }
            }
        }

        Ok(file_infos)
    }

    /// Group files by size to avoid exceeding target size
    fn group_files_by_size(
        &self,
        files: &[ParquetFileInfo],
        config: &CompactionConfig,
    ) -> Result<Vec<Vec<ParquetFileInfo>>, String> {
        let mut groups = Vec::new();
        let mut current_group = Vec::new();
        let mut current_size = 0u64;

        for file in files {
            // Start new group if adding this file would exceed target size
            if !current_group.is_empty() && current_size + file.size > config.target_file_size {
                groups.push(current_group.clone());
                current_group.clear();
                current_size = 0;
            }

            current_group.push(file.clone());
            current_size += file.size;
        }

        // Add remaining files
        if !current_group.is_empty() {
            groups.push(current_group);
        }

        Ok(groups)
    }

    /// Read all batches from multiple parquet files
    async fn read_parquet_files(&self, files: &[PathBuf]) -> Result<Vec<RecordBatch>, String> {
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        for file_path in files {
            let file = File::open(file_path)
                .map_err(|e| format!("Failed to open file {}: {}", file_path.display(), e))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| format!("Failed to create parquet reader: {}", e))?;

            let reader = builder
                .build()
                .map_err(|e| format!("Failed to build reader: {}", e))?;

            for batch_result in reader {
                let batch = batch_result.map_err(|e| format!("Failed to read batch: {}", e))?;

                // Calculate batch size (approximate)
                let batch_bytes = batch.get_array_memory_size() as u64;
                total_bytes_read += batch_bytes;

                // Apply read rate limiting
                if let Some(ref limiter) = self.read_rate_limiter {
                    limiter.wait_for_bytes(batch_bytes as usize).await;
                }

                all_batches.push(batch);
            }
        }

        debug!(
            "Read {} batches ({} MB)",
            all_batches.len(),
            total_bytes_read / (1024 * 1024)
        );

        // Merge schemas and align batches if there are schema conflicts
        if all_batches.len() > 1 {
            all_batches = self.align_batch_schemas(all_batches)?;
        }

        Ok(all_batches)
    }

    /// Align all batches to have a unified schema, handling type conflicts
    fn align_batch_schemas(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
        if batches.is_empty() {
            return Ok(batches);
        }

        // Fast path: check if all schemas are identical
        if batches.len() > 1 {
            let first_schema = batches[0].schema();
            let all_same = batches[1..]
                .iter()
                .all(|b| b.schema().as_ref() == first_schema.as_ref());

            if all_same {
                // All schemas are identical, no alignment needed
                return Ok(batches);
            }
        }

        // Step 1: Merge all schemas to create a unified schema
        let merged_schema = self.merge_schemas(&batches)?;

        // Step 2: Convert each batch to the merged schema
        let mut aligned_batches = Vec::new();
        for batch in batches {
            let aligned_batch = self.convert_batch_to_schema(&batch, &merged_schema)?;
            aligned_batches.push(aligned_batch);
        }

        Ok(aligned_batches)
    }

    /// Merge schemas from multiple batches, handling type conflicts
    fn merge_schemas(&self, batches: &[RecordBatch]) -> Result<Arc<Schema>, String> {
        let mut field_map: HashMap<String, DataType> = HashMap::new();
        let mut field_order: Vec<String> = Vec::new();

        // Collect all fields and their types
        for batch in batches {
            let schema = batch.schema();
            for field in schema.fields() {
                let field_name = field.name().clone();

                if let Some(existing_type) = field_map.get(&field_name) {
                    // Handle type conflicts
                    if existing_type != field.data_type() {
                        warn!("Schema conflict for field '{}': {:?} vs {:?}, using String as common type",
                              field_name, existing_type, field.data_type());
                        // Use String as the most flexible common type
                        field_map.insert(field_name.clone(), DataType::Utf8);
                    }
                } else {
                    field_map.insert(field_name.clone(), field.data_type().clone());
                    field_order.push(field_name);
                }
            }
        }

        // Build merged schema maintaining field order
        let fields: Vec<Field> = field_order
            .into_iter()
            .map(|name| {
                let data_type = field_map.get(&name).unwrap().clone();
                Field::new(name, data_type, true) // nullable=true for safety
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Convert a batch to match a target schema, casting types as needed
    fn convert_batch_to_schema(
        &self,
        batch: &RecordBatch,
        target_schema: &Arc<Schema>,
    ) -> Result<RecordBatch, String> {
        // Fast path: if schemas are identical, return batch as-is
        if batch.schema().as_ref() == target_schema.as_ref() {
            return Ok(batch.clone());
        }

        let mut new_columns: Vec<ArrayRef> = Vec::new();

        for target_field in target_schema.fields() {
            let field_name = target_field.name();

            // Find matching column in source batch
            if let Some((col_idx, source_field)) = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == field_name)
            {
                let source_array = batch.column(col_idx);

                // If types match, use as-is
                if source_field.data_type() == target_field.data_type() {
                    new_columns.push(source_array.clone());
                } else {
                    // Try to cast to target type
                    match cast(source_array, target_field.data_type()) {
                        Ok(casted_array) => {
                            new_columns.push(casted_array);
                        }
                        Err(_) => {
                            // Cast failed, convert to string as last resort
                            warn!(
                                "Failed to cast '{}' from {:?} to {:?}, converting to string",
                                field_name,
                                source_field.data_type(),
                                target_field.data_type()
                            );
                            let string_array = self.convert_array_to_string(source_array)?;
                            new_columns.push(Arc::new(string_array));
                        }
                    }
                }
            } else {
                // Field doesn't exist in source batch, add null array
                let null_array = new_null_array(target_field.data_type(), batch.num_rows());
                new_columns.push(null_array);
            }
        }

        RecordBatch::try_new(target_schema.clone(), new_columns)
            .map_err(|e| format!("Failed to create aligned batch: {}", e))
    }

    /// Convert any array type to StringArray
    fn convert_array_to_string(&self, array: &ArrayRef) -> Result<StringArray, String> {
        let num_rows = array.len();
        let mut string_values: Vec<Option<String>> = Vec::with_capacity(num_rows);

        match array.data_type() {
            DataType::Int64 => {
                let int_array = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("Failed to downcast to Int64Array")?;
                for i in 0..num_rows {
                    string_values.push(if int_array.is_null(i) {
                        None
                    } else {
                        Some(int_array.value(i).to_string())
                    });
                }
            }
            DataType::Float64 => {
                let float_array = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("Failed to downcast to Float64Array")?;
                for i in 0..num_rows {
                    string_values.push(if float_array.is_null(i) {
                        None
                    } else {
                        Some(float_array.value(i).to_string())
                    });
                }
            }
            DataType::Boolean => {
                let bool_array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or("Failed to downcast to BooleanArray")?;
                for i in 0..num_rows {
                    string_values.push(if bool_array.is_null(i) {
                        None
                    } else {
                        Some(bool_array.value(i).to_string())
                    });
                }
            }
            DataType::Utf8 => {
                let str_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("Failed to downcast to StringArray")?;
                for i in 0..num_rows {
                    string_values.push(if str_array.is_null(i) {
                        None
                    } else {
                        Some(str_array.value(i).to_string())
                    });
                }
            }
            _ => {
                // For unsupported types, use debug format
                for i in 0..num_rows {
                    string_values.push(if array.is_null(i) {
                        None
                    } else {
                        Some(format!("{:?}", array))
                    });
                }
            }
        }

        Ok(StringArray::from(string_values))
    }

    /// K-way merge sort for already-sorted batches (one batch per file)
    /// Much more efficient than full sort: O(N log K) vs O(N log N)
    fn merge_sorted_batches(
        batches: Vec<RecordBatch>,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, String> {
        if batches.is_empty() {
            return Err("No batches to merge".to_string());
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        let ts_idx = match schema.index_of("time") {
            Ok(idx) => idx,
            Err(_) => {
                // No time column, just concatenate
                debug!("No time column found, concatenating without sort");
                return concat_batches(schema, &batches)
                    .map_err(|e| format!("Failed to concat batches: {}", e));
            }
        };

        // Collect all rows with (time, batch_idx, row_idx)
        let mut indexed_rows: Vec<(i64, usize, usize)> = Vec::new();

        for (batch_idx, batch) in batches.iter().enumerate() {
            let ts_col = batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>();

            if let Some(ts_array) = ts_col {
                for row_idx in 0..batch.num_rows() {
                    let ts = ts_array.value(row_idx);
                    indexed_rows.push((ts, batch_idx, row_idx));
                }
            } else {
                // Fallback: try Int64 time
                let ts_col = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>();

                if let Some(ts_array) = ts_col {
                    for row_idx in 0..batch.num_rows() {
                        let ts = ts_array.value(row_idx);
                        indexed_rows.push((ts, batch_idx, row_idx));
                    }
                } else {
                    // Unknown time column type, just add rows in order
                    for row_idx in 0..batch.num_rows() {
                        indexed_rows.push((row_idx as i64, batch_idx, row_idx));
                    }
                }
            }
        }

        // Sort by time (stable sort preserves order for equal times)
        indexed_rows.sort_by_key(|(ts, _, _)| *ts);

        // Build sorted arrays
        let mut sorted_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let sorted_array =
                Self::build_sorted_column(&batches, col_idx, field.data_type(), &indexed_rows)?;
            sorted_columns.push(sorted_array);
        }

        RecordBatch::try_new(schema.clone(), sorted_columns)
            .map_err(|e| format!("Failed to create merged batch: {}", e))
    }

    /// Build a single sorted column from indexed rows
    fn build_sorted_column(
        batches: &[RecordBatch],
        col_idx: usize,
        data_type: &DataType,
        indexed_rows: &[(i64, usize, usize)],
    ) -> Result<ArrayRef, String> {
        use arrow::array::*;

        match data_type {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                let mut builder = TimestampNanosecondArray::builder(indexed_rows.len());
                for (_, batch_idx, row_idx) in indexed_rows {
                    let array = batches[*batch_idx]
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or("Failed to downcast timestamp array")?;
                    if array.is_null(*row_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(array.value(*row_idx));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(indexed_rows.len());
                for (_, batch_idx, row_idx) in indexed_rows {
                    let array = batches[*batch_idx]
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or("Failed to downcast Int64 array")?;
                    if array.is_null(*row_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(array.value(*row_idx));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(indexed_rows.len());
                for (_, batch_idx, row_idx) in indexed_rows {
                    let array = batches[*batch_idx]
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or("Failed to downcast Float64 array")?;
                    if array.is_null(*row_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(array.value(*row_idx));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder =
                    StringBuilder::with_capacity(indexed_rows.len(), indexed_rows.len() * 32);
                for (_, batch_idx, row_idx) in indexed_rows {
                    let array = batches[*batch_idx]
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("Failed to downcast String array")?;
                    if array.is_null(*row_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(array.value(*row_idx));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(indexed_rows.len());
                for (_, batch_idx, row_idx) in indexed_rows {
                    let array = batches[*batch_idx]
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or("Failed to downcast Boolean array")?;
                    if array.is_null(*row_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(array.value(*row_idx));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                // Fallback: use take with indices (less efficient but works for any type)
                // First concat all arrays, then take
                let arrays: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(col_idx).as_ref()).collect();

                let concatenated = arrow::compute::concat(&arrays)
                    .map_err(|e| format!("Failed to concat arrays: {}", e))?;

                // Build global indices
                let mut global_indices: Vec<u64> = Vec::with_capacity(indexed_rows.len());
                let mut offsets: Vec<usize> = vec![0];
                for batch in batches {
                    offsets.push(offsets.last().unwrap() + batch.num_rows());
                }

                for (_, batch_idx, row_idx) in indexed_rows {
                    let global_idx = offsets[*batch_idx] + row_idx;
                    global_indices.push(global_idx as u64);
                }

                let indices = UInt64Array::from(global_indices);
                take(&concatenated, &indices, None).map_err(|e| format!("Failed to take: {}", e))
            }
        }
    }

    /// Create optimized WriterProperties with Bloom filters and compression
    fn create_optimized_writer_props(schema: &Schema) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            // Row group size for good statistics granularity
            .set_max_row_group_size(ROW_GROUP_SIZE)
            // ZSTD compression (good balance of speed and ratio)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
            // Enable dictionary encoding for better compression of repeated values
            .set_dictionary_enabled(true)
            // Enable statistics for all columns
            .set_statistics_enabled(EnabledStatistics::Page);

        // Enable Bloom filters for string columns (tags)
        for field in schema.fields() {
            if field.data_type() == &DataType::Utf8 {
                let col_path: parquet::schema::types::ColumnPath = field.name().clone().into();
                builder = builder
                    .set_column_bloom_filter_enabled(col_path.clone(), true)
                    .set_column_bloom_filter_fpp(col_path.clone(), BLOOM_FILTER_FPP)
                    .set_column_bloom_filter_ndv(col_path, BLOOM_FILTER_NDV);
            }
        }

        builder.build()
    }

    /// Write compacted batches to a new file (merge-sorted by time, optimized)
    /// Uses k-way merge since each input file is already sorted
    async fn write_compacted_file(
        &self,
        filename: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        let schema = batches[0].schema();

        // K-way merge sort (efficient since each batch is already sorted)
        let sorted_batch = Self::merge_sorted_batches(batches, &schema)?;

        let total_rows = sorted_batch.num_rows();
        debug!(
            "Writing {} rows to {} (merge-sorted by time)",
            total_rows, filename
        );

        let file = File::create(filename).map_err(|e| format!("Failed to create file: {}", e))?;

        // Use optimized writer properties
        let props = Self::create_optimized_writer_props(&schema);

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        let batch_bytes = sorted_batch.get_array_memory_size() as u64;

        writer
            .write(&sorted_batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;

        // Apply write rate limiting
        if let Some(ref limiter) = self.write_rate_limiter {
            limiter.wait_for_bytes(batch_bytes as usize).await;
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        debug!(
            "Wrote {} rows, {} MB (merge-sorted, compressed)",
            total_rows,
            batch_bytes / (1024 * 1024)
        );

        Ok(())
    }

    /// Start background LSM compaction task for all catalogs
    /// This runs frequently (e.g., every 60s) to keep L0 files under control
    pub fn start_background_lsm_compaction(
        self: Arc<Self>,
        interval_secs: u64,
        config: CompactionConfig,
    ) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            info!(
                "Background LSM compaction started: every {} seconds",
                interval_secs
            );

            loop {
                ticker.tick().await;

                // LSM compaction for all catalogs (active partitions)
                if let Ok(entries) = fs::read_dir(&self.data_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(catalog_name) = path.file_name().and_then(|n| n.to_str()) {
                                // Skip hidden directories
                                if catalog_name.starts_with('.') {
                                    continue;
                                }

                                // LSM compaction for this catalog
                                match self.compact_catalog(catalog_name, &config).await {
                                    Ok(results) => {
                                        if !results.is_empty() {
                                            info!(
                                                "LSM compaction [{}]: {} partition(s)",
                                                catalog_name,
                                                results.len()
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "LSM compaction error for '{}': {}",
                                            catalog_name, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Start background full compaction task for yesterday's partitions
    /// This runs hourly but only acts at the configured hour (default: 01:00)
    pub fn start_background_full_compaction(
        self: Arc<Self>,
        interval_secs: u64,
        config: CompactionConfig,
    ) {
        if !config.full_compaction_enabled {
            info!("Full compaction is disabled");
            return;
        }

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            info!(
                "Background full compaction started: every {} seconds (acts at hour {})",
                interval_secs, config.full_compaction_hour
            );

            loop {
                ticker.tick().await;

                // Full compaction for yesterday (only acts at configured hour)
                match self.full_compact_yesterday(&config).await {
                    Ok(results) => {
                        if !results.is_empty() {
                            info!("Full compaction: {} partitions compacted", results.len());
                            for result in &results {
                                info!(
                                    "{}.{}.{}/{}: {} → {} files, {:.2} MB",
                                    result.catalog,
                                    result.schema,
                                    result.table,
                                    result.date_partition,
                                    result.files_before,
                                    result.files_after,
                                    result.bytes_before as f64 / 1024.0 / 1024.0
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!("Full compaction error: {}", e);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_config() -> CompactionConfig {
        CompactionConfig {
            min_files: 5,
            major_min_files: 3,
            target_file_size: 100 * 1024 * 1024, // 100MB
            max_level: 4,
            full_compaction_enabled: false,
            full_compaction_hour: 3,
            full_compaction_target_size: 200 * 1024 * 1024,
            max_concurrent_compactions: 4,
            full_compaction_max_concurrent: 2,
            read_rate_limit_bytes_per_sec: 0,
            write_rate_limit_bytes_per_sec: 0,
        }
    }

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.min_files, DEFAULT_MIN_FILES);
        assert_eq!(config.major_min_files, DEFAULT_MAJOR_MIN_FILES);
        assert_eq!(config.max_level, DEFAULT_MAX_LEVEL);
        assert_eq!(config.max_concurrent_compactions, 8);
        assert_eq!(config.full_compaction_max_concurrent, 2);
    }

    #[test]
    fn test_compaction_config_from_config() {
        let app_config = crate::config::CompactionConfig {
            min_files: 10,
            major_min_files: 5,
            target_file_size_mb: 500,
            max_level: 7,
            interval_secs: 120,
            full_compaction_enabled: true,
            full_compaction_hour: 5,
            full_compaction_interval_secs: 7200,
            full_compaction_target_size_mb: 300,
            max_concurrent_compactions: 16,
            full_compaction_max_concurrent: 4,
            read_rate_limit_mb_per_sec: 100,
            write_rate_limit_mb_per_sec: 50,
        };

        let config = CompactionConfig::from(&app_config);
        assert_eq!(config.min_files, 10);
        assert_eq!(config.major_min_files, 5);
        assert_eq!(config.target_file_size, 500 * 1024 * 1024);
        assert_eq!(config.max_level, 7);
        assert_eq!(config.full_compaction_enabled, true);
        assert_eq!(config.full_compaction_hour, 5);
        assert_eq!(config.full_compaction_target_size, 300 * 1024 * 1024);
        assert_eq!(config.max_concurrent_compactions, 16);
        assert_eq!(config.full_compaction_max_concurrent, 4);
        assert_eq!(config.read_rate_limit_bytes_per_sec, 100 * 1024 * 1024);
        assert_eq!(config.write_rate_limit_bytes_per_sec, 50 * 1024 * 1024);
    }

    #[test]
    fn test_parquet_file_info_from_path_l0() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("L0_1234567890123.parquet");
        fs::write(&file_path, vec![0u8; 1024 * 1024]).unwrap();

        let info = ParquetFileInfo::from_path(file_path).unwrap();
        assert_eq!(info.level, 0);
        assert_eq!(info.timestamp, 1234567890123);
        assert_eq!(info.size, 1024 * 1024);
    }

    #[test]
    fn test_parquet_file_info_from_path_l3() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("L3_9876543210987.parquet");
        fs::write(&file_path, vec![0u8; 5 * 1024 * 1024]).unwrap();

        let info = ParquetFileInfo::from_path(file_path).unwrap();
        assert_eq!(info.level, 3);
        assert_eq!(info.timestamp, 9876543210987);
        assert_eq!(info.size, 5 * 1024 * 1024);
    }

    #[test]
    fn test_parquet_file_info_from_path_legacy_format() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("1234567890123.parquet");
        fs::write(&file_path, vec![0u8; 2048]).unwrap();

        let info = ParquetFileInfo::from_path(file_path).unwrap();
        assert_eq!(info.level, 0); // Legacy files default to L0
        assert_eq!(info.timestamp, 1234567890123);
        assert_eq!(info.size, 2048);
    }

    #[test]
    fn test_parquet_file_info_sorting() {
        let mut files = vec![
            ParquetFileInfo {
                path: PathBuf::from("L1_3000.parquet"),
                level: 1,
                timestamp: 3000,
                size: 1024,
            },
            ParquetFileInfo {
                path: PathBuf::from("L0_2000.parquet"),
                level: 0,
                timestamp: 2000,
                size: 1024,
            },
            ParquetFileInfo {
                path: PathBuf::from("L0_1000.parquet"),
                level: 0,
                timestamp: 1000,
                size: 1024,
            },
            ParquetFileInfo {
                path: PathBuf::from("L1_4000.parquet"),
                level: 1,
                timestamp: 4000,
                size: 1024,
            },
        ];

        files.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        assert_eq!(files[0].level, 0);
        assert_eq!(files[0].timestamp, 1000);
        assert_eq!(files[1].level, 0);
        assert_eq!(files[1].timestamp, 2000);
        assert_eq!(files[2].level, 1);
        assert_eq!(files[2].timestamp, 3000);
        assert_eq!(files[3].level, 1);
        assert_eq!(files[3].timestamp, 4000);
    }

    #[test]
    fn test_compaction_result_calculation() {
        let result = CompactionResult {
            catalog: "test".to_string(),
            schema: "schema1".to_string(),
            table: "table1".to_string(),
            date_partition: "2024-01-01".to_string(),
            l0_files_before: 10,
            l0_files_after: 2,
            files_before: 15,
            files_after: 5,
            bytes_before: 150 * 1024 * 1024,
            bytes_after: 100 * 1024 * 1024,
            rows_compacted: 1000000,
        };

        assert_eq!(result.files_before, 15);
        assert_eq!(result.files_after, 5);
        assert_eq!(result.bytes_before, 150 * 1024 * 1024);
        assert_eq!(result.bytes_after, 100 * 1024 * 1024);
        assert_eq!(result.rows_compacted, 1000000);
    }

    #[test]
    fn test_compactor_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        assert_eq!(compactor.data_dir, temp_dir.path().to_str().unwrap());
    }

    #[test]
    fn test_compactor_with_semaphore() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig {
            max_concurrent_compactions: 4,
            full_compaction_max_concurrent: 2,
            ..create_test_config()
        };
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        assert!(compactor.compaction_semaphore.is_some());
        assert!(compactor.full_compaction_semaphore.is_some());
    }

    #[test]
    fn test_compactor_without_semaphore() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig {
            max_concurrent_compactions: 0,
            full_compaction_max_concurrent: 0,
            ..create_test_config()
        };
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        assert!(compactor.compaction_semaphore.is_none());
        assert!(compactor.full_compaction_semaphore.is_none());
    }

    #[test]
    fn test_compactor_with_rate_limiters() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig {
            read_rate_limit_bytes_per_sec: 100 * 1024 * 1024,
            write_rate_limit_bytes_per_sec: 50 * 1024 * 1024,
            ..create_test_config()
        };
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        assert!(compactor.read_rate_limiter.is_some());
        assert!(compactor.write_rate_limiter.is_some());
    }

    #[test]
    fn test_compactor_without_rate_limiters() {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig {
            read_rate_limit_bytes_per_sec: 0,
            write_rate_limit_bytes_per_sec: 0,
            ..create_test_config()
        };
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        assert!(compactor.read_rate_limiter.is_none());
        assert!(compactor.write_rate_limiter.is_none());
    }

    #[tokio::test]
    async fn test_get_lock_same_partition() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        let lock1 = compactor
            .get_lock("catalog", "schema", "table", "2024-01-01")
            .await;
        let lock2 = compactor
            .get_lock("catalog", "schema", "table", "2024-01-01")
            .await;

        // Should return the same lock instance
        assert!(Arc::ptr_eq(&lock1, &lock2));
    }

    #[tokio::test]
    async fn test_get_lock_different_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        let lock1 = compactor
            .get_lock("catalog", "schema", "table", "2024-01-01")
            .await;
        let lock2 = compactor
            .get_lock("catalog", "schema", "table", "2024-01-02")
            .await;

        // Should return different lock instances
        assert!(!Arc::ptr_eq(&lock1, &lock2));
    }

    #[tokio::test]
    async fn test_list_parquet_file_infos_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        let result = compactor.list_parquet_file_infos(temp_dir.path().to_str().unwrap());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_list_parquet_file_infos_with_files() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        // Create test parquet files
        fs::write(temp_dir.path().join("L0_1000000.parquet"), b"test data 1").unwrap();
        fs::write(temp_dir.path().join("L1_2000000.parquet"), b"test data 2").unwrap();
        fs::write(temp_dir.path().join("not_a_parquet.txt"), b"ignored").unwrap();

        let result = compactor.list_parquet_file_infos(temp_dir.path().to_str().unwrap());
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.level == 0 && f.timestamp == 1000000));
        assert!(files.iter().any(|f| f.level == 1 && f.timestamp == 2000000));
    }

    #[tokio::test]
    async fn test_list_parquet_file_infos_filters_invalid() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        // Create valid and invalid parquet files
        fs::write(temp_dir.path().join("L0_1000000.parquet"), b"test data").unwrap();
        fs::write(temp_dir.path().join("invalid_format.parquet"), b"test data").unwrap();
        fs::write(temp_dir.path().join("LX_1000000.parquet"), b"test data").unwrap();

        let result = compactor.list_parquet_file_infos(temp_dir.path().to_str().unwrap());
        assert!(result.is_ok());

        let files = result.unwrap();
        // Should only include the valid file
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].level, 0);
        assert_eq!(files[0].timestamp, 1000000);
    }

    #[test]
    fn test_file_filtering_by_size() {
        let config = create_test_config();
        let target_size = config.target_file_size;

        let files = vec![
            ParquetFileInfo {
                path: PathBuf::from("L1_1000.parquet"),
                level: 1,
                timestamp: 1000,
                size: 50 * 1024 * 1024, // 50MB - should be included
            },
            ParquetFileInfo {
                path: PathBuf::from("L1_2000.parquet"),
                level: 1,
                timestamp: 2000,
                size: 150 * 1024 * 1024, // 150MB - should be filtered out
            },
            ParquetFileInfo {
                path: PathBuf::from("L1_3000.parquet"),
                level: 1,
                timestamp: 3000,
                size: 80 * 1024 * 1024, // 80MB - should be included
            },
        ];

        let filtered: Vec<_> = files.iter().filter(|f| f.size < target_size).collect();

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].timestamp, 1000);
        assert_eq!(filtered[1].timestamp, 3000);
    }

    #[test]
    fn test_compaction_thresholds() {
        let config = create_test_config();

        // L0 files should trigger compaction
        let l0_count = config.min_files;
        assert!(l0_count >= config.min_files);

        // L1+ files should trigger compaction
        let l1_count = config.major_min_files;
        assert!(l1_count >= config.major_min_files);

        // Below threshold should not trigger
        assert!(config.min_files - 1 < config.min_files);
        assert!(config.major_min_files - 1 < config.major_min_files);
    }

    #[test]
    fn test_level_progression() {
        let config = create_test_config();

        // Test level progression
        for level in 0..config.max_level {
            let target_level = if level >= config.max_level {
                level // Stay at max
            } else {
                level + 1 // Promote to next
            };

            if level < config.max_level {
                assert_eq!(target_level, level + 1);
            } else {
                assert_eq!(target_level, level);
            }
        }
    }

    #[test]
    fn test_max_level_compaction() {
        let config = CompactionConfig {
            max_level: 4,
            ..create_test_config()
        };

        // At max level, should compact to same level
        let current_level = 4;
        let target_level = if current_level >= config.max_level {
            current_level
        } else {
            current_level + 1
        };

        assert_eq!(target_level, 4);
    }

    #[test]
    fn test_yesterday_partition_calculation() {
        use chrono::{Duration, Utc};

        let now = Utc::now();
        let yesterday = now - Duration::days(1);
        let partition = yesterday.format("date=%Y-%m-%d").to_string();

        assert!(partition.starts_with("date="));
        assert_eq!(partition.len(), 15); // "date=YYYY-MM-DD"
    }

    #[test]
    fn test_hour_check_for_full_compaction() {
        let config = CompactionConfig {
            full_compaction_hour: 3,
            ..create_test_config()
        };

        // Should only run at hour 3
        for hour in 0..24 {
            if hour == config.full_compaction_hour {
                assert_eq!(hour, 3);
            } else {
                assert_ne!(hour, 3);
            }
        }
    }

    #[tokio::test]
    async fn test_compact_non_existing_path() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let compactor = Compactor::with_config(temp_dir.path().to_str().unwrap(), &config);

        let result = compactor
            .compact_table_partition("catalog", "schema", "table", "2024-01-01", &config)
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }
}
