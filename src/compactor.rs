use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// Compaction constants
const LEVEL_PREFIX: char = 'L';
const LEVEL_SEPARATOR: char = '_';
const PARQUET_EXT: &str = ".parquet";

// Default compaction thresholds
const DEFAULT_MIN_FILES: usize = 5;
const DEFAULT_MAJOR_MIN_FILES: usize = 4; // Minimum files for L1+ compaction
const DEFAULT_MAX_FILE_SIZE_MB: u64 = 100;
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
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of L0 files to trigger minor compaction (L0 → L1)
    pub min_files: usize,
    /// Minimum number of files at L1+ to trigger major compaction (L1 → L2, L2 → L3, etc.)
    pub major_min_files: usize,
    /// Maximum file size in bytes before compaction (default: 100MB)
    #[allow(dead_code)]
    pub max_file_size: u64,
    /// Target file size after compaction (default: 500MB)
    pub target_file_size: u64,
    /// Maximum compaction level (0-4: L0, L1, L2, L3, L4)
    pub max_level: u32,
    /// Age-based consolidation: merge all files if partition older than this (hours)
    pub stable_age_hours: u64,
    /// Minimum files required to trigger age-based merge
    pub min_files_for_age_merge: usize,
    /// Maximum single file size in MB (split if larger)
    pub max_single_file_size_mb: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            min_files: DEFAULT_MIN_FILES,
            major_min_files: DEFAULT_MAJOR_MIN_FILES,
            max_file_size: DEFAULT_MAX_FILE_SIZE_MB * MB_TO_BYTES,
            target_file_size: DEFAULT_TARGET_FILE_SIZE_MB * MB_TO_BYTES,
            max_level: DEFAULT_MAX_LEVEL,
            stable_age_hours: 12, // Default: 12 hours
            min_files_for_age_merge: 2,
            max_single_file_size_mb: 2048, // 2GB
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
    modified_time: std::time::SystemTime,
}

impl ParquetFileInfo {
    fn from_path(path: PathBuf) -> Result<Self, String> {
        let metadata = fs::metadata(&path).map_err(|e| format!("Failed to get metadata: {}", e))?;

        let size = metadata.len();
        let modified_time = metadata
            .modified()
            .map_err(|e| format!("Failed to get modified time: {}", e))?;

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
            modified_time,
        })
    }
}

impl Compactor {
    pub fn new(data_dir: &str) -> Self {
        Compactor {
            data_dir: data_dir.to_string(),
            locks: Arc::new(RwLock::new(std::collections::HashMap::new())),
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
    pub async fn compact_catalog(
        &self,
        catalog: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog);

        if !Path::new(&catalog_path).exists() {
            return Err(format!("Catalog '{}' does not exist", catalog));
        }

        let mut results = Vec::new();

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
                Some(name) => name,
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
                    match self
                        .compact_table_all_partitions(catalog, schema_name, table_name, config)
                        .await
                    {
                        Ok(table_results) => results.extend(table_results),
                        Err(e) => {
                            warn!(
                                "Failed to compact {}/{}/{}: {}",
                                catalog, schema_name, table_name, e
                            );
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Compact all partitions in a table (or the table itself if not partitioned)
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
            // Partitioned table: compact each partition separately
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

    /// Get partition age based on oldest file modification time
    fn get_partition_age(&self, files: &[ParquetFileInfo]) -> Result<std::time::Duration, String> {
        if files.is_empty() {
            return Err("No files in partition".to_string());
        }

        // Find the most recently modified file (newest file determines partition age)
        let newest_modified = files
            .iter()
            .map(|f| f.modified_time)
            .max()
            .ok_or("Failed to find newest file")?;

        std::time::SystemTime::now()
            .duration_since(newest_modified)
            .map_err(|e| format!("Failed to calculate partition age: {}", e))
    }

    /// Merge all files in a partition into a single file
    /// Returns CompactionResult if merge was performed, None if skipped
    async fn merge_all_to_one_file(
        &self,
        partition_path: &str,
        files: &[ParquetFileInfo],
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        if files.is_empty() {
            return Ok(None);
        }

        let total_size: u64 = files.iter().map(|f| f.size).sum();
        let max_size = config.max_single_file_size_mb * MB_TO_BYTES;

        info!(
            "Age-based consolidation: merging {} files ({}MB) into one file",
            files.len(),
            total_size / MB_TO_BYTES
        );

        // Read all files
        let paths: Vec<PathBuf> = files.iter().map(|f| f.path.clone()).collect();
        let batches = self.read_parquet_files(&paths)?;
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        // Check if result would be too large for a single file
        if total_size > max_size {
            warn!(
                "Total size {}MB exceeds max single file size {}MB, will split into multiple files",
                total_size / MB_TO_BYTES,
                config.max_single_file_size_mb
            );

            // Calculate number of output files needed
            let num_files = ((total_size as f64 / max_size as f64).ceil() as usize).max(1);
            let rows_per_file = (total_rows / num_files).max(1);

            // Split batches into chunks
            let mut current_rows = 0;
            let mut current_chunk = Vec::new();
            let mut file_idx = 0;

            for batch in batches {
                current_chunk.push(batch.clone());
                current_rows += batch.num_rows();

                if current_rows >= rows_per_file && file_idx < num_files - 1 {
                    // Write this chunk
                    let timestamp = chrono::Utc::now().timestamp_millis();
                    let output_filename = format!(
                        "{}/L0_FINAL_PART{}_{}.parquet",
                        partition_path, file_idx, timestamp
                    );
                    self.write_compacted_file(&output_filename, current_chunk.clone())?;
                    info!("Created consolidated file part {}: {}", file_idx, output_filename);

                    // Reset for next chunk
                    current_chunk.clear();
                    current_rows = 0;
                    file_idx += 1;
                }
            }

            // Write remaining rows
            if !current_chunk.is_empty() {
                let timestamp = chrono::Utc::now().timestamp_millis();
                let output_filename = format!(
                    "{}/L0_FINAL_PART{}_{}.parquet",
                    partition_path, file_idx, timestamp
                );
                self.write_compacted_file(&output_filename, current_chunk)?;
                info!("Created consolidated file part {}: {}", file_idx, output_filename);
            }
        } else {
            // Single file output
            let timestamp = chrono::Utc::now().timestamp_millis();
            let output_filename = format!("{}/L0_FINAL_{}.parquet", partition_path, timestamp);

            self.write_compacted_file(&output_filename, batches)?;

            info!(
                "Created consolidated file: {} ({} rows, {}MB)",
                output_filename,
                total_rows,
                total_size / MB_TO_BYTES
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

        Ok(Some(CompactionResult {
            catalog: String::new(), // Will be filled by caller
            schema: String::new(),
            table: String::new(),
            date_partition: String::new(),
            l0_files_before: files.iter().filter(|f| f.level == 0).count(),
            l0_files_after: 0, // Will be recalculated by caller if needed
            files_before: files.len(),
            files_after: 0, // Will be recalculated by caller if needed
            bytes_before: total_size,
            bytes_after: total_size, // Same data, just reorganized
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
        let l0_files_before = file_infos.iter().filter(|f| f.level == 0).count();

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

        // ========== AGE-BASED CONSOLIDATION ==========
        // Check table age and apply one-file policy if old enough
        match self.get_partition_age(&file_infos) {
            Ok(age) => {
                let age_threshold_secs = config.stable_age_hours * 3600;
                let age_secs = age.as_secs();

                if age_secs > age_threshold_secs
                    && file_infos.len() >= config.min_files_for_age_merge
                {
                    info!(
                        "Non-partitioned table {}.{}.{} is {}h old (threshold: {}h), applying ONE-FILE consolidation ({} files)",
                        catalog,
                        schema,
                        table,
                        age_secs / 3600,
                        config.stable_age_hours,
                        file_infos.len()
                    );

                    // Use age-based consolidation instead of traditional LSM
                    match self
                        .merge_all_to_one_file(&table_path, &file_infos, config)
                        .await
                    {
                        Ok(Some(mut result)) => {
                            // Fill in catalog/schema/table info
                            result.catalog = catalog.to_string();
                            result.schema = schema.to_string();
                            result.table = table.to_string();
                            result.date_partition = "[non-partitioned]".to_string();

                            // Recalculate after stats
                            let file_infos_after = self.list_parquet_file_infos(&table_path)?;
                            result.files_after = file_infos_after.len();
                            result.l0_files_after =
                                file_infos_after.iter().filter(|f| f.level == 0).count();
                            let total_bytes_after: u64 =
                                file_infos_after.iter().map(|f| f.size).sum();
                            result.bytes_after = total_bytes_after;

                            info!(
                                "Age-based consolidation complete: {}.{}.{}: {} → {} files, {:.2} MB → {:.2} MB in {:?}",
                                catalog,
                                schema,
                                table,
                                total_files_before,
                                result.files_after,
                                total_bytes_before as f64 / 1024.0 / 1024.0,
                                total_bytes_after as f64 / 1024.0 / 1024.0,
                                start.elapsed()
                            );

                            return Ok(Some(result));
                        }
                        Ok(None) => {
                            debug!("Age-based consolidation returned no result");
                        }
                        Err(e) => {
                            warn!("Age-based consolidation failed: {}, falling back to LSM", e);
                        }
                    }
                } else {
                    debug!(
                        "Table age {}h (threshold: {}h), files {} (min: {}), using traditional LSM",
                        age_secs / 3600,
                        config.stable_age_hours,
                        file_infos.len(),
                        config.min_files_for_age_merge
                    );
                }
            }
            Err(e) => {
                debug!("Could not determine table age: {}, using traditional LSM", e);
            }
        }

        // ========== TRADITIONAL LSM COMPACTION ==========
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

        // Step 2: Major compactions (L1 → L2, L2 → L3, L3 → L4)
        // Refresh file list after L0 compaction
        let file_infos_updated = self.list_parquet_file_infos(&table_path)?;

        for current_level in 1..=config.max_level {
            let level_files: Vec<_> = file_infos_updated
                .iter()
                .filter(|f| f.level == current_level)
                .cloned()
                .collect();

            if level_files.len() >= config.major_min_files && current_level < config.max_level {
                info!(
                    "Major compaction: {} L{} files → L{}",
                    level_files.len(),
                    current_level,
                    current_level + 1
                );

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

        // ========== AGE-BASED CONSOLIDATION ==========
        // Check partition age and apply one-file policy if old enough
        match self.get_partition_age(&file_infos) {
            Ok(age) => {
                let age_threshold_secs = config.stable_age_hours * 3600;
                let age_secs = age.as_secs();

                if age_secs > age_threshold_secs
                    && file_infos.len() >= config.min_files_for_age_merge
                {
                    info!(
                        "Partition {}.{}.{}/{} is {}h old (threshold: {}h), applying ONE-FILE consolidation ({} files)",
                        catalog,
                        schema,
                        table,
                        date_partition,
                        age_secs / 3600,
                        config.stable_age_hours,
                        file_infos.len()
                    );

                    // Use age-based consolidation instead of traditional LSM
                    match self
                        .merge_all_to_one_file(&partition_path, &file_infos, config)
                        .await
                    {
                        Ok(Some(mut result)) => {
                            // Fill in catalog/schema/table info
                            result.catalog = catalog.to_string();
                            result.schema = schema.to_string();
                            result.table = table.to_string();
                            result.date_partition = date_partition.to_string();

                            // Recalculate after stats
                            let file_infos_after = self.list_parquet_file_infos(&partition_path)?;
                            result.files_after = file_infos_after.len();
                            result.l0_files_after =
                                file_infos_after.iter().filter(|f| f.level == 0).count();
                            let total_bytes_after: u64 =
                                file_infos_after.iter().map(|f| f.size).sum();
                            result.bytes_after = total_bytes_after;

                            info!(
                                "Age-based consolidation complete: {}.{}.{}/{}: {} → {} files, {:.2} MB → {:.2} MB in {:?}",
                                catalog,
                                schema,
                                table,
                                date_partition,
                                total_files_before,
                                result.files_after,
                                total_bytes_before as f64 / 1024.0 / 1024.0,
                                total_bytes_after as f64 / 1024.0 / 1024.0,
                                start.elapsed()
                            );

                            return Ok(Some(result));
                        }
                        Ok(None) => {
                            debug!("Age-based consolidation returned no result");
                        }
                        Err(e) => {
                            warn!("Age-based consolidation failed: {}, falling back to LSM", e);
                        }
                    }
                } else {
                    debug!(
                        "Partition age {}h (threshold: {}h), files {} (min: {}), using traditional LSM",
                        age_secs / 3600,
                        config.stable_age_hours,
                        file_infos.len(),
                        config.min_files_for_age_merge
                    );
                }
            }
            Err(e) => {
                debug!("Could not determine partition age: {}, using traditional LSM", e);
            }
        }

        // ========== TRADITIONAL LSM COMPACTION ==========
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

        // Step 2: Major compactions (L1 → L2, L2 → L3, L3 → L4)
        // Refresh file list after L0 compaction
        let file_infos_updated = self.list_parquet_file_infos(&partition_path)?;

        for current_level in 1..=config.max_level {
            let level_files: Vec<_> = file_infos_updated
                .iter()
                .filter(|f| f.level == current_level)
                .cloned()
                .collect();

            if level_files.len() >= config.major_min_files && current_level < config.max_level {
                info!(
                    "Major compaction: {} L{} files → L{}",
                    level_files.len(),
                    current_level,
                    current_level + 1
                );

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

        let next_level = current_level + 1;
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
            let batches = self.read_parquet_files(&paths)?;
            let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            total_rows += rows;

            // Create compacted file at next level
            let timestamp = chrono::Utc::now().timestamp_millis();
            let compacted_filename = format!(
                "{}/{}{}{}{}{}",
                partition_path, LEVEL_PREFIX, next_level, LEVEL_SEPARATOR, timestamp, PARQUET_EXT
            );

            self.write_compacted_file(&compacted_filename, batches)?;

            info!(
                "Created L{} file: {} ({} rows from {} L{} files)",
                next_level,
                Path::new(&compacted_filename)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                rows,
                group.len(),
                current_level
            );

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
    fn read_parquet_files(&self, files: &[PathBuf]) -> Result<Vec<RecordBatch>, String> {
        let mut all_batches = Vec::new();

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
                all_batches.push(batch);
            }
        }

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
    fn write_compacted_file(
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

        for batch in batches {
            writer
                .write(&batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(())
    }

    /// Start background compaction task for all catalogs
    pub fn start_background_compaction(
        self: Arc<Self>,
        interval_secs: u64,
        config: CompactionConfig,
    ) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            info!(
                "Background compaction started: every {} seconds",
                interval_secs
            );

            loop {
                ticker.tick().await;

                // Get all catalogs in data directory
                if let Ok(entries) = fs::read_dir(&self.data_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(catalog_name) = path.file_name().and_then(|n| n.to_str()) {
                                // Skip hidden directories and series_stats
                                if catalog_name.starts_with('.') {
                                    continue;
                                }

                                // Compact this catalog
                                match self.compact_catalog(catalog_name, &config).await {
                                    Ok(results) => {
                                        if !results.is_empty() {
                                            info!("Background compaction [{}]: {} partition(s) compacted",
                                                catalog_name, results.len());
                                            for result in results {
                                                info!(
                                                    "{}.{}.{}/{}: L0 {} -> {}, Total {} -> {} files",
                                                    result.catalog, result.schema, result.table, result.date_partition,
                                                    result.l0_files_before, result.l0_files_after,
                                                    result.files_before, result.files_after
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Background compaction error for '{}': {}",
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
}
