use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug};

// Compaction constants
const LEVEL_PREFIX: char = 'L';
const LEVEL_SEPARATOR: char = '_';
const PARQUET_EXT: &str = ".parquet";
const CONFIG_FILENAME: &str = "config.json";

// Default compaction thresholds
const DEFAULT_MIN_FILES: usize = 5;
const DEFAULT_MAX_FILE_SIZE_MB: u64 = 100;
const DEFAULT_TARGET_FILE_SIZE_MB: u64 = 500;
const DEFAULT_MAX_LEVEL: u32 = 5;
const MB_TO_BYTES: u64 = 1024 * 1024;

/// Compactor is responsible for merging small Parquet files into larger ones
/// using a leveled compaction strategy (similar to LSM-tree)
pub struct Compactor {
    data_dir: String,
    /// Lock to prevent concurrent compaction and read/write operations
    /// Key: (catalog, schema, table)
    locks: Arc<RwLock<std::collections::HashMap<(String, String, String), Arc<tokio::sync::Mutex<()>>>>>,
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of files to trigger compaction
    pub min_files: usize,
    /// Maximum file size in bytes before compaction (default: 100MB)
    pub max_file_size: u64,
    /// Target file size after compaction (default: 500MB)
    pub target_file_size: u64,
    /// Maximum compaction level
    pub max_level: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            min_files: DEFAULT_MIN_FILES,
            max_file_size: DEFAULT_MAX_FILE_SIZE_MB * MB_TO_BYTES,
            target_file_size: DEFAULT_TARGET_FILE_SIZE_MB * MB_TO_BYTES,
            max_level: DEFAULT_MAX_LEVEL,
        }
    }
}

#[derive(Debug)]
pub struct CompactionResult {
    pub catalog: String,
    pub schema: String,
    pub table: String,
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
        let metadata = fs::metadata(&path)
            .map_err(|e| format!("Failed to get metadata: {}", e))?;

        let size = metadata.len();

        // Parse filename: L0_1703251234567.parquet or 1703251234567.parquet (legacy)
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .ok_or("Invalid filename")?;

        let (level, timestamp) = if filename.starts_with(LEVEL_PREFIX) {
            // New format: L0_1703251234567.parquet
            let parts: Vec<&str> = filename.split(LEVEL_SEPARATOR).collect();
            if parts.len() >= 2 {
                let level = parts[0].trim_start_matches(LEVEL_PREFIX)
                    .parse::<u32>()
                    .unwrap_or(0);
                let timestamp = parts[1].trim_end_matches(PARQUET_EXT)
                    .parse::<i64>()
                    .unwrap_or(0);
                (level, timestamp)
            } else {
                (0, 0)
            }
        } else {
            // Legacy format: 1703251234567.parquet (treat as L0)
            let timestamp = filename.trim_end_matches(PARQUET_EXT)
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
        Compactor {
            data_dir: data_dir.to_string(),
            locks: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get or create a lock for a specific catalog/schema/table tuple
    async fn get_lock(&self, catalog: &str, schema: &str, table: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
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
            let schema_entry = schema_entry.map_err(|e| format!("Failed to read schema entry: {}", e))?;
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
                    warn!("Failed to read schema directory {}/{}: {}", catalog, schema_name, e);
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
                    match self.compact_table(catalog, schema_name, table_name, config).await {
                        Ok(Some(result)) => results.push(result),
                        Ok(None) => {
                            // No compaction needed
                        }
                        Err(e) => {
                            warn!("Failed to compact {}/{}/{}: {}", catalog, schema_name, table_name, e);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Compact a specific table using leveled strategy
    pub async fn compact_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        // Acquire lock to prevent concurrent operations
        let lock = self.get_lock(catalog, schema, table).await;
        let _guard = lock.lock().await;

        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);

        if !Path::new(&table_path).exists() {
            return Err(format!("Table '{}.{}.{}' does not exist", catalog, schema, table));
        }

        // List and parse all parquet files
        let mut file_infos = self.list_parquet_file_infos(&table_path)?;

        if file_infos.is_empty() {
            return Ok(None);
        }

        // Sort by level (ascending) then timestamp (ascending)
        file_infos.sort_by(|a, b| {
            a.level.cmp(&b.level)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        let total_files_before = file_infos.len();
        let l0_files_before = file_infos.iter().filter(|f| f.level == 0).count();

        // Check if compaction is needed
        if l0_files_before < config.min_files {
            return Ok(None);
        }

        // Calculate total size
        let total_bytes_before: u64 = file_infos.iter().map(|f| f.size).sum();

        info!("Compacting {}.{}.{}: {} files ({} L0), {:.2} MB",
                 catalog, schema, table, total_files_before, l0_files_before,
                 total_bytes_before as f64 / 1024.0 / 1024.0);

        // Perform leveled compaction (prioritize L0 files)
        let mut total_rows = 0;
        let mut compacted_file_paths = Vec::new();

        // Step 1: Compact L0 files first (highest priority)
        let l0_files: Vec<_> = file_infos.iter()
            .filter(|f| f.level == 0)
            .cloned()
            .collect();

        if l0_files.len() >= config.min_files {
            info!("Compacting {} L0 files (priority)", l0_files.len());

            let file_groups = self.group_files_by_size(&l0_files, config)?;

            for (group_idx, group) in file_groups.iter().enumerate() {
                if group.len() < 2 {
                    continue;
                }

                debug!("Group {}: {} files", group_idx + 1, group.len());

                let paths: Vec<PathBuf> = group.iter().map(|f| f.path.clone()).collect();
                let batches = self.read_parquet_files(&paths)?;
                let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                total_rows += rows;

                // Determine next level (L0 → L1)
                let next_level = 1;
                let timestamp = chrono::Utc::now().timestamp_millis();
                let compacted_filename = format!(
                    "{}/{}{}{}{}{}",
                    table_path,
                    LEVEL_PREFIX,
                    next_level,
                    LEVEL_SEPARATOR,
                    timestamp,
                    PARQUET_EXT
                );

                self.write_compacted_file(&compacted_filename, batches)?;
                compacted_file_paths.push(compacted_filename.clone());

                info!("Created: {} ({} rows)", compacted_filename, rows);

                // Delete old files
                for file in &paths {
                    if let Err(e) = fs::remove_file(file) {
                        warn!("Failed to delete {}: {}", file.display(), e);
                    } else {
                        debug!("Deleted: {}", file.display());
                    }
                }
            }
        }

        // Step 2: Compact files within each higher level if needed
        for level in 1..config.max_level {
            let level_files: Vec<_> = file_infos.iter()
                .filter(|f| f.level == level)
                .cloned()
                .collect();

            if level_files.len() >= config.min_files {
                info!("Compacting {} L{} files", level_files.len(), level);

                let file_groups = self.group_files_by_size(&level_files, config)?;

                for (group_idx, group) in file_groups.iter().enumerate() {
                    if group.len() < 2 {
                        continue;
                    }

                    debug!("Group {}: {} files", group_idx + 1, group.len());

                    let paths: Vec<PathBuf> = group.iter().map(|f| f.path.clone()).collect();
                    let batches = self.read_parquet_files(&paths)?;
                    let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    total_rows += rows;

                    // Determine next level
                    let next_level = level + 1;
                    let timestamp = chrono::Utc::now().timestamp_millis();
                    let compacted_filename = format!(
                        "{}/{}{}{}{}{}",
                        table_path,
                        LEVEL_PREFIX,
                        next_level,
                        LEVEL_SEPARATOR,
                        timestamp,
                        PARQUET_EXT
                    );

                    self.write_compacted_file(&compacted_filename, batches)?;
                    compacted_file_paths.push(compacted_filename.clone());

                    info!("Created: {} ({} rows)", compacted_filename, rows);

                    // Delete old files
                    for file in &paths {
                        if let Err(e) = fs::remove_file(file) {
                            warn!("Failed to delete {}: {}", file.display(), e);
                        } else {
                            debug!("Deleted: {}", file.display());
                        }
                    }
                }
            }
        }

        // Calculate new stats
        let new_file_infos = self.list_parquet_file_infos(&table_path)?;
        let total_files_after = new_file_infos.len();
        let l0_files_after = new_file_infos.iter().filter(|f| f.level == 0).count();
        let total_bytes_after: u64 = new_file_infos.iter().map(|f| f.size).sum();

        info!("Compaction complete: L0 {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB)",
                 l0_files_before, l0_files_after,
                 total_files_before, total_files_after,
                 total_bytes_before as f64 / 1024.0 / 1024.0,
                 total_bytes_after as f64 / 1024.0 / 1024.0);

        Ok(Some(CompactionResult {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            l0_files_before,
            l0_files_after,
            files_before: total_files_before,
            files_after: total_files_after,
            bytes_before: total_bytes_before,
            bytes_after: total_bytes_after,
            rows_compacted: total_rows,
        }))
    }

    /// List all parquet files with level information
    fn list_parquet_file_infos(&self, dir: &str) -> Result<Vec<ParquetFileInfo>, String> {
        let entries = fs::read_dir(dir)
            .map_err(|e| format!("Failed to read directory: {}", e))?;

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

            let reader = builder.build()
                .map_err(|e| format!("Failed to build reader: {}", e))?;

            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| format!("Failed to read batch: {}", e))?;
                all_batches.push(batch);
            }
        }

        Ok(all_batches)
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
        let file = File::create(filename)
            .map_err(|e| format!("Failed to create file: {}", e))?;

        let props = WriterProperties::builder()
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        for batch in batches {
            writer.write(&batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
        }

        writer.close()
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

            info!("Background compaction started: every {} seconds", interval_secs);

            loop {
                ticker.tick().await;

                // Get all catalogs in data directory
                if let Ok(entries) = fs::read_dir(&self.data_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(catalog_name) = path.file_name().and_then(|n| n.to_str()) {
                                // Skip hidden directories and series_stats
                                if catalog_name.starts_with('.') || catalog_name == "series_stats" {
                                    continue;
                                }

                                // Compact this catalog
                                match self.compact_catalog(catalog_name, &config).await {
                                    Ok(results) => {
                                        if !results.is_empty() {
                                            info!("Background compaction [{}]: {} table(s) compacted",
                                                catalog_name, results.len());
                                            for result in results {
                                                info!(
                                                    "{}.{}.{}: L0 {} -> {}, Total {} -> {} files",
                                                    result.catalog, result.schema, result.table,
                                                    result.l0_files_before, result.l0_files_after,
                                                    result.files_before, result.files_after
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Background compaction error for '{}': {}", catalog_name, e);
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
