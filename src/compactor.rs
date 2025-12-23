use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Compactor is responsible for merging small Parquet files into larger ones
/// using a leveled compaction strategy (similar to LSM-tree)
pub struct Compactor {
    data_dir: String,
    /// Lock to prevent concurrent compaction and read/write operations
    /// Key: (db, measurement)
    locks: Arc<RwLock<std::collections::HashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>>,
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
            min_files: 5,
            max_file_size: 100 * 1024 * 1024,      // 100MB
            target_file_size: 500 * 1024 * 1024,   // 500MB
            max_level: 5,
        }
    }
}

#[derive(Debug)]
pub struct CompactionResult {
    pub db: String,
    pub measurement: String,
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
        
        let (level, timestamp) = if filename.starts_with('L') {
            // New format: L0_1703251234567.parquet
            let parts: Vec<&str> = filename.split('_').collect();
            if parts.len() >= 2 {
                let level = parts[0].trim_start_matches('L')
                    .parse::<u32>()
                    .unwrap_or(0);
                let timestamp = parts[1].trim_end_matches(".parquet")
                    .parse::<i64>()
                    .unwrap_or(0);
                (level, timestamp)
            } else {
                (0, 0)
            }
        } else {
            // Legacy format: 1703251234567.parquet (treat as L0)
            let timestamp = filename.trim_end_matches(".parquet")
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

    /// Get or create a lock for a specific db/measurement pair
    async fn get_lock(&self, db: &str, measurement: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = (db.to_string(), measurement.to_string());
        let mut locks = self.locks.write().await;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Compact all measurements in a database
    pub async fn compact_database(
        &self,
        db: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let db_path = format!("{}/{}", self.data_dir, db);
        
        if !Path::new(&db_path).exists() {
            return Err(format!("Database '{}' does not exist", db));
        }

        let mut results = Vec::new();
        
        // List all measurements (subdirectories)
        let entries = fs::read_dir(&db_path)
            .map_err(|e| format!("Failed to read database directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();
            
            // Skip non-directories and config.json
            if !path.is_dir() {
                continue;
            }

            if let Some(measurement) = path.file_name().and_then(|n| n.to_str()) {
                match self.compact_measurement(db, measurement, config).await {
                    Ok(Some(result)) => results.push(result),
                    Ok(None) => {
                        // No compaction needed
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to compact {}/{}: {}", db, measurement, e);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Compact a specific measurement using leveled strategy
    pub async fn compact_measurement(
        &self,
        db: &str,
        measurement: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        // Acquire lock to prevent concurrent operations
        let lock = self.get_lock(db, measurement).await;
        let _guard = lock.lock().await;

        let measurement_path = format!("{}/{}/{}", self.data_dir, db, measurement);
        
        if !Path::new(&measurement_path).exists() {
            return Err(format!("Measurement '{}' does not exist in database '{}'", measurement, db));
        }

        // List and parse all parquet files
        let mut file_infos = self.list_parquet_file_infos(&measurement_path)?;
        
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

        println!("🔧 Compacting {}/{}: {} files ({} L0), {:.2} MB", 
                 db, measurement, total_files_before, l0_files_before,
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
            println!("  📦 Compacting {} L0 files (priority)", l0_files.len());
            
            let file_groups = self.group_files_by_size(&l0_files, config)?;
            
            for (group_idx, group) in file_groups.iter().enumerate() {
                if group.len() < 2 {
                    continue;
                }
                
                println!("    Group {}: {} files", group_idx + 1, group.len());
                
                let paths: Vec<PathBuf> = group.iter().map(|f| f.path.clone()).collect();
                let batches = self.read_parquet_files(&paths)?;
                let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                total_rows += rows;
                
                // Determine next level (L0 → L1)
                let next_level = 1;
                let compacted_filename = format!(
                    "{}/L{}_{}.parquet",
                    measurement_path,
                    next_level,
                    chrono::Utc::now().timestamp_millis()
                );
                
                self.write_compacted_file(&compacted_filename, batches)?;
                compacted_file_paths.push(compacted_filename.clone());
                
                println!("    ✅ Created: {} ({} rows)", compacted_filename, rows);
                
                // Delete old files
                for file in &paths {
                    if let Err(e) = fs::remove_file(file) {
                        eprintln!("    ⚠️  Failed to delete {}: {}", file.display(), e);
                    } else {
                        println!("    🗑️  Deleted: {}", file.display());
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
                println!("  📦 Compacting {} L{} files", level_files.len(), level);
                
                let file_groups = self.group_files_by_size(&level_files, config)?;
                
                for (group_idx, group) in file_groups.iter().enumerate() {
                    if group.len() < 2 {
                        continue;
                    }
                    
                    println!("    Group {}: {} files", group_idx + 1, group.len());
                    
                    let paths: Vec<PathBuf> = group.iter().map(|f| f.path.clone()).collect();
                    let batches = self.read_parquet_files(&paths)?;
                    let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    total_rows += rows;
                    
                    // Determine next level
                    let next_level = level + 1;
                    let compacted_filename = format!(
                        "{}/L{}_{}.parquet",
                        measurement_path,
                        next_level,
                        chrono::Utc::now().timestamp_millis()
                    );
                    
                    self.write_compacted_file(&compacted_filename, batches)?;
                    compacted_file_paths.push(compacted_filename.clone());
                    
                    println!("    ✅ Created: {} ({} rows)", compacted_filename, rows);
                    
                    // Delete old files
                    for file in &paths {
                        if let Err(e) = fs::remove_file(file) {
                            eprintln!("    ⚠️  Failed to delete {}: {}", file.display(), e);
                        } else {
                            println!("    🗑️  Deleted: {}", file.display());
                        }
                    }
                }
            }
        }

        // Calculate new stats
        let new_file_infos = self.list_parquet_file_infos(&measurement_path)?;
        let total_files_after = new_file_infos.len();
        let l0_files_after = new_file_infos.iter().filter(|f| f.level == 0).count();
        let total_bytes_after: u64 = new_file_infos.iter().map(|f| f.size).sum();

        println!("✅ Compaction complete: L0 {} -> {}, Total {} -> {} files ({:.2} MB -> {:.2} MB)",
                 l0_files_before, l0_files_after,
                 total_files_before, total_files_after,
                 total_bytes_before as f64 / 1024.0 / 1024.0,
                 total_bytes_after as f64 / 1024.0 / 1024.0);

        Ok(Some(CompactionResult {
            db: db.to_string(),
            measurement: measurement.to_string(),
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

    /// Start background compaction task
    pub fn start_background_compaction(
        self: Arc<Self>,
        db: String,
        interval_secs: u64,
        config: CompactionConfig,
    ) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            println!(
                "🔧 Background compaction started for '{}': every {} seconds",
                db, interval_secs
            );

            loop {
                ticker.tick().await;

                match self.compact_database(&db, &config).await {
                    Ok(results) => {
                        if !results.is_empty() {
                            println!("🔄 Background compaction: {} measurement(s) compacted", results.len());
                            for result in results {
                                println!(
                                    "   {}/{}: L0 {} -> {}, Total {} -> {} files",
                                    result.db, result.measurement,
                                    result.l0_files_before, result.l0_files_after,
                                    result.files_before, result.files_after
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("🔄 Background compaction error: {}", e);
                    }
                }
            }
        });
    }
}
