use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Timelike;
use futures::stream::{FuturesUnordered, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

use crate::rate_limiter::RateLimiter;

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

/// Type alias for compaction locks map
/// Key: (catalog, schema, table, date_partition)
type CompactionLocks =
    Arc<RwLock<HashMap<(String, String, String, String), Arc<tokio::sync::Mutex<()>>>>>;

/// Compactor is responsible for merging small Parquet files into larger ones
/// using a leveled compaction strategy (similar to LSM-tree)
pub struct Compactor {
    data_dir: String,
    /// Lock to prevent concurrent compaction and read/write operations
    locks: CompactionLocks,
    /// Semaphore to limit concurrent compaction operations
    compaction_semaphore: Option<Arc<Semaphore>>,
    /// Current memory usage in bytes (atomic for thread-safe updates)
    current_memory_bytes: Arc<AtomicU64>,
    /// Maximum memory budget in bytes (0 = unlimited)
    max_memory_bytes: u64,
    /// Read rate limiter (None = unlimited)
    read_rate_limiter: Option<Arc<RateLimiter>>,
    /// Write rate limiter (None = unlimited)
    write_rate_limiter: Option<Arc<RateLimiter>>,
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
    /// Maximum concurrent compaction operations (0 = unlimited)
    pub max_concurrent_compactions: usize,
    /// Maximum memory budget in bytes (0 = unlimited)
    pub max_memory_bytes: u64,
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
            full_compaction_hour: 1, // 01:00
            full_compaction_target_size: 200 * MB_TO_BYTES, // 200MB
            max_concurrent_compactions: 4,
            max_memory_bytes: 2048 * MB_TO_BYTES, // 2GB
            read_rate_limit_bytes_per_sec: 0, // Unlimited
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
            max_memory_bytes: config.max_memory_mb * MB_TO_BYTES,
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
                    .unwrap_or(0);
                let timestamp = parts[1]
                    .trim_end_matches(PARQUET_EXT)
                    .parse::<i64>()
                    .unwrap_or(0);
                (level, timestamp)
            } else {
                (0, 0)
            }
        } else {
            // Legacy format: 1703251234567.parquet (treat as L0)
            let timestamp = filename
                .trim_end_matches(PARQUET_EXT)
                .parse::<i64>()
                .unwrap_or(0);
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
    pub fn new(data_dir: &str) -> Self {
        Self::with_config(data_dir, &CompactionConfig::default())
    }

    pub fn with_config(data_dir: &str, config: &CompactionConfig) -> Self {
        // Create semaphore if max_concurrent_compactions > 0
        let compaction_semaphore = if config.max_concurrent_compactions > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_compactions)))
        } else {
            None
        };

        // Create rate limiters if configured
        let read_rate_limiter = if config.read_rate_limit_bytes_per_sec > 0 {
            Some(Arc::new(RateLimiter::new(config.read_rate_limit_bytes_per_sec)))
        } else {
            None
        };

        let write_rate_limiter = if config.write_rate_limit_bytes_per_sec > 0 {
            Some(Arc::new(RateLimiter::new(config.write_rate_limit_bytes_per_sec)))
        } else {
            None
        };

        Compactor {
            data_dir: data_dir.to_string(),
            locks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            compaction_semaphore,
            current_memory_bytes: Arc::new(AtomicU64::new(0)),
            max_memory_bytes: config.max_memory_bytes,
            read_rate_limiter,
            write_rate_limiter,
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
                self.compact_table_all_partitions(&catalog_owned, &schema_owned, &table_owned, &config_clone).await
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
        let catalog_entries = fs::read_dir(&self.data_dir)
            .map_err(|e| format!("Failed to read data dir: {}", e))?;

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
            debug!("No partitions found for full compaction on {}", yesterday_partition);
            return Ok(Vec::new());
        }

        info!(
            "Found {} partitions for full compaction (max {} concurrent)",
            partitions_to_compact.len(),
            config.max_concurrent_compactions
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
                ).await
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
                    debug!("Full compaction [{}/{}]: skipped (no work needed)", completed, total);
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
        // Acquire semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.compaction_semaphore {
            Some(semaphore.acquire().await.map_err(|e| format!("Failed to acquire semaphore: {}", e))?)
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
        let files_to_compact: Vec<PathBuf> = compactable_files.iter().map(|(p, _)| p.clone()).collect();

        // Read compactable files
        let batches = self.read_parquet_files(&files_to_compact).await?;
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        // Write consolidated file
        let timestamp = chrono::Utc::now().timestamp_millis();
        let output = partition_path.join(format!("FULL_{}.parquet", timestamp));

        self.write_compacted_file(output.to_str().unwrap(), batches).await?;

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

        // Delete old compacted files (keep oversized files)
        for (file, _) in &compactable_files {
            if let Err(e) = fs::remove_file(file) {
                warn!("Failed to delete {}: {}", file.display(), e);
            } else {
                debug!("Deleted: {}", file.display());
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
        // Acquire semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.compaction_semaphore {
            Some(semaphore.acquire().await.map_err(|e| format!("Failed to acquire semaphore: {}", e))?)
        } else {
            None
        };

        // Use empty string as partition identifier for non-partitioned tables
        let partition_id = "";
        let start = Instant::now();
        let lock = self.get_lock(catalog, schema, table, partition_id).await;
        let _guard = lock.lock().await;

        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);

        if !Path::new(&table_path).exists() {
            return Err(format!(
                "Table '{}.{}.{}' does not exist",
                catalog, schema, table
            ));
        }

        // List and parse all parquet files directly in the table directory
        let mut file_infos = self.list_parquet_file_infos(&table_path)?;

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
        let l0_files_before = file_infos
            .iter()
            .filter(|f| f.level == 0 && f.size < config.target_file_size)
            .count();

        // Calculate total size
        let total_bytes_before: u64 = file_infos.iter().map(|f| f.size).sum();

        debug!(
            "Compacting non-partitioned {}.{}.{}: {} files ({} L0), {:.2} MB",
            catalog,
            schema,
            table,
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
                .compact_level_files(&table_path, &l0_files, 0, config)
                .await?;
            total_rows += rows;
            compaction_happened = true;
        }

        // Step 2: Major compactions (L1 → L2, L2 → L3, L3 → L4, L4 → L4)
        // Refresh file list after L0 compaction
        let file_infos_updated = self.list_parquet_file_infos(&table_path)?;

        for current_level in 1..=config.max_level {
            // Get all files at this level
            let all_level_files: Vec<_> = file_infos_updated
                .iter()
                .filter(|f| f.level == current_level)
                .cloned()
                .collect();

            // Filter out files >= target_file_size to reduce I/O
            // Already-large files don't need recompaction
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

            // Allow compaction at all levels including max_level (to reduce file count)
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
                    .compact_level_files(&table_path, &level_files, current_level, config)
                    .await?;
                total_rows += rows;
                compaction_happened = true;
            }
        }

        // If no compaction happened, return None
        if !compaction_happened {
            return Ok(None);
        }

        // Recalculate files after compaction
        let file_infos_after = self.list_parquet_file_infos(&table_path)?;
        let total_files_after = file_infos_after.len();
        let l0_files_after = file_infos_after.iter().filter(|f| f.level == 0).count();
        let total_bytes_after: u64 = file_infos_after.iter().map(|f| f.size).sum();

        info!(
            "Compaction complete: {}.{}.{}: {} -> {} files, {:.2} MB -> {:.2} MB in {:?}",
            catalog,
            schema,
            table,
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
            date_partition: "[non-partitioned]".to_string(),
            l0_files_before,
            l0_files_after,
            files_before: total_files_before,
            files_after: total_files_after,
            bytes_before: total_bytes_before,
            bytes_after: total_bytes_after,
            rows_compacted: total_rows,
        }))
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
        // Acquire semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.compaction_semaphore {
            Some(semaphore.acquire().await.map_err(|e| format!("Failed to acquire semaphore: {}", e))?)
        } else {
            None
        };

        // Acquire lock to prevent concurrent operations on this partition
        let lock = self.get_lock(catalog, schema, table, date_partition).await;
        let _guard = lock.lock().await;
        let start = Instant::now();

        let partition_path = format!(
            "{}/{}/{}/{}/{}",
            self.data_dir, catalog, schema, table, date_partition
        );

        if !Path::new(&partition_path).exists() {
            return Err(format!(
                "Partition '{}.{}.{}/{}' does not exist",
                catalog, schema, table, date_partition
            ));
        }

        // List and parse all parquet files in this partition
        let mut file_infos = self.list_parquet_file_infos(&partition_path)?;

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

        // Calculate total size
        let total_bytes_before: u64 = file_infos.iter().map(|f| f.size).sum();

        debug!(
            "Compacting {}.{}.{}/{}: {} files ({} L0), {:.2} MB",
            catalog,
            schema,
            table,
            date_partition,
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
                .compact_level_files(&partition_path, &l0_files, 0, config)
                .await?;
            total_rows += rows;
            compaction_happened = true;
        }

        // Step 2: Major compactions (L1 → L2, L2 → L3, L3 → L4, L4 → L4)
        // Refresh file list after L0 compaction
        let file_infos_updated = self.list_parquet_file_infos(&partition_path)?;

        for current_level in 1..=config.max_level {
            // Get all files at this level
            let all_level_files: Vec<_> = file_infos_updated
                .iter()
                .filter(|f| f.level == current_level)
                .cloned()
                .collect();

            // Filter out files >= target_file_size to reduce I/O
            // Already-large files don't need recompaction
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

            // Allow compaction at all levels including max_level (to reduce file count)
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
                    .compact_level_files(&partition_path, &level_files, current_level, config)
                    .await?;
                total_rows += rows;
                compaction_happened = true;
            }
        }

        // If no compaction happened, return None
        if !compaction_happened {
            return Ok(None);
        }

        // Recalculate files after compaction
        let file_infos_after = self.list_parquet_file_infos(&partition_path)?;
        let total_files_after = file_infos_after.len();
        let l0_files_after = file_infos_after.iter().filter(|f| f.level == 0).count();
        let total_bytes_after: u64 = file_infos_after.iter().map(|f| f.size).sum();

        info!("Compaction complete: {}.{}.{}/{}: {} -> {} files, {:.2} MB -> {:.2} MB, time used: {:?}",
            catalog, schema, table, date_partition,
            total_files_before, total_files_after,
            total_bytes_before as f64 / 1024.0 / 1024.0,
            total_bytes_after as f64 / 1024.0 / 1024.0,
            start.elapsed());

        Ok(Some(CompactionResult {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date_partition: date_partition.to_string(),
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

            self.write_compacted_file(&compacted_filename, batches).await?;

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
            for file in &paths {
                if let Err(e) = fs::remove_file(file) {
                    warn!("Failed to delete {}: {}", file.display(), e);
                } else {
                    debug!("Deleted: {}", file.display());
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
            let file_size = fs::metadata(file_path)
                .map(|m| m.len())
                .unwrap_or(0);

            // Check memory budget before reading
            if self.max_memory_bytes > 0 {
                let current_memory = self.current_memory_bytes.load(Ordering::Relaxed);
                if current_memory + file_size > self.max_memory_bytes {
                    return Err(format!(
                        "Memory budget exceeded: current={} MB, file={} MB, max={} MB",
                        current_memory / (1024 * 1024),
                        file_size / (1024 * 1024),
                        self.max_memory_bytes / (1024 * 1024)
                    ));
                }
            }

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

                // Update memory tracking
                self.current_memory_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
                total_bytes_read += batch_bytes;

                // Apply read rate limiting
                if let Some(ref limiter) = self.read_rate_limiter {
                    limiter.wait_for_bytes(batch_bytes as usize).await;
                }

                all_batches.push(batch);
            }
        }

        debug!("Read {} batches ({} MB)", all_batches.len(), total_bytes_read / (1024 * 1024));

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

    /// Write compacted batches to a new file
    async fn write_compacted_file(
        &self,
        filename: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        let schema = batches[0].schema();
        let file = File::create(filename).map_err(|e| format!("Failed to create file: {}", e))?;

        let props = WriterProperties::builder().build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        let mut total_bytes_written = 0u64;
        let mut total_bytes_freed = 0u64;

        for batch in batches {
            // Calculate batch size before writing (for memory tracking)
            let batch_bytes = batch.get_array_memory_size() as u64;

            writer
                .write(&batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;

            total_bytes_written += batch_bytes;

            // Apply write rate limiting
            if let Some(ref limiter) = self.write_rate_limiter {
                limiter.wait_for_bytes(batch_bytes as usize).await;
            }

            // Free memory after writing (batch will be dropped)
            self.current_memory_bytes.fetch_sub(batch_bytes, Ordering::Relaxed);
            total_bytes_freed += batch_bytes;
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        debug!(
            "Wrote {} MB, freed {} MB memory",
            total_bytes_written / (1024 * 1024),
            total_bytes_freed / (1024 * 1024)
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
                            info!(
                                "Full compaction: {} partitions compacted",
                                results.len()
                            );
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
