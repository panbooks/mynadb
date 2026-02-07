use crate::storage::layout::STORE_DIR;
use crate::storage::storage::Storage;
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, read_to_string, write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Database configuration constants
const DB_CONFIG_FILENAME: &str = "config.json";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    pub partition_cols: Vec<String>,
}

pub struct DatabaseManager {
    data_dir: String,
    /// Storage backend (for S3/MinIO support)
    storage: Option<Arc<Storage>>,
    /// Node ID for cluster mode (used as prefix in storage paths)
    node_id: Option<String>,
}

impl DatabaseManager {
    pub fn new(data_dir: &str) -> Self {
        DatabaseManager {
            data_dir: data_dir.to_string(),
            storage: None,
            node_id: None,
        }
    }

    pub fn with_storage(data_dir: &str, storage: Arc<Storage>) -> Self {
        DatabaseManager {
            data_dir: data_dir.to_string(),
            storage: Some(storage),
            node_id: None,
        }
    }

    pub fn with_storage_and_node_id(data_dir: &str, storage: Arc<Storage>, node_id: &str) -> Self {
        DatabaseManager {
            data_dir: data_dir.to_string(),
            storage: Some(storage),
            node_id: Some(node_id.to_string()),
        }
    }

    /// Get the database directory path (under store/)
    fn get_database_path(&self, db_name: &str) -> PathBuf {
        Path::new(&self.data_dir).join(STORE_DIR).join(db_name)
    }

    /// Get the config file path for a database
    fn get_config_path(&self, db_name: &str) -> PathBuf {
        self.get_database_path(db_name).join(DB_CONFIG_FILENAME)
    }

    /// Create a new database
    pub fn create_database(&self, db_name: &str) -> Result<String, String> {
        let db_path = self.get_database_path(db_name);

        // Check if database already exists
        if db_path.exists() {
            return Err(format!("Database '{}' already exists", db_name));
        }

        // Create directory
        create_dir_all(&db_path)
            .map_err(|e| format!("Failed to create database directory: {}", e))?;

        // Create config file
        let config = DatabaseConfig {
            name: db_name.to_string(),
            tables: Vec::new(),
        };

        self.save_config(&config)?;

        Ok(format!(
            "Database '{}' created at {}",
            db_name,
            db_path.display()
        ))
    }

    /// Create a table with optional partitioning
    pub fn create_table(
        &self,
        db_name: &str,
        table_name: &str,
        partition_cols: Vec<String>,
    ) -> Result<String, String> {
        let db_path = self.get_database_path(db_name);

        // Check if database exists
        if !db_path.exists() {
            return Err(format!("Database '{}' does not exist", db_name));
        }

        // Load or create config
        let mut config = self
            .load_config(db_name)
            .unwrap_or_else(|_| DatabaseConfig {
                name: db_name.to_string(),
                tables: Vec::new(),
            });

        // Check if table already exists
        if config.tables.iter().any(|t| t.name == table_name) {
            return Err(format!("Table '{}' already exists", table_name));
        }

        // Add table config
        config.tables.push(TableConfig {
            name: table_name.to_string(),
            partition_cols: partition_cols.clone(),
        });

        self.save_config(&config)?;

        let partition_info = if partition_cols.is_empty() {
            "no partitioning".to_string()
        } else {
            format!("partitioned by: {}", partition_cols.join(", "))
        };

        Ok(format!(
            "Table '{}/{}' created ({})",
            db_name, table_name, partition_info
        ))
    }

    /// List all databases
    pub fn list_databases(&self) -> Result<Vec<String>, String> {
        let store_dir = Path::new(&self.data_dir).join(STORE_DIR);
        let entries = std::fs::read_dir(&store_dir)
            .map_err(|e| format!("Failed to read store directory: {}", e))?;

        let mut databases = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    databases.push(name.to_string());
                }
            }
        }

        databases.sort();
        Ok(databases)
    }

    /// Delete a database
    pub fn delete_database(&self, db_name: &str) -> Result<String, String> {
        let db_path = self.get_database_path(db_name);

        if !db_path.exists() {
            return Err(format!("Database '{}' does not exist", db_name));
        }

        std::fs::remove_dir_all(&db_path)
            .map_err(|e| format!("Failed to delete database: {}", e))?;

        Ok(format!("Database '{}' deleted", db_name))
    }

    /// Delete a table
    pub fn delete_table(&self, db_name: &str, table_name: &str) -> Result<String, String> {
        let mut config = self.load_config(db_name)?;

        let initial_len = config.tables.len();
        config.tables.retain(|t| t.name != table_name);

        if config.tables.len() == initial_len {
            return Err(format!("Table '{}' not found", table_name));
        }

        self.save_config(&config)?;

        Ok(format!(
            "Table '{}' deleted from database '{}'",
            table_name, db_name
        ))
    }

    /// Load database config
    fn load_config(&self, db_name: &str) -> Result<DatabaseConfig, String> {
        let config_path = self.get_config_path(db_name);

        if !config_path.exists() {
            return Err(format!("Database config not found for '{}'", db_name));
        }

        let content =
            read_to_string(&config_path).map_err(|e| format!("Failed to read config: {}", e))?;

        serde_json::from_str(&content).map_err(|e| format!("Failed to parse config: {}", e))
    }

    /// Save database config
    fn save_config(&self, config: &DatabaseConfig) -> Result<(), String> {
        let config_path = self.get_config_path(&config.name);

        let content = serde_json::to_string_pretty(config)
            .map_err(|e| format!("Failed to serialize config: {}", e))?;

        write(&config_path, content).map_err(|e| format!("Failed to write config: {}", e))?;

        Ok(())
    }

    /// Get database info
    pub fn get_database_info(&self, db_name: &str) -> Result<DatabaseConfig, String> {
        self.load_config(db_name)
    }

    /// Discover all date partitions for a table across all schemas
    pub fn discover_table_partitions(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<PartitionInfo>, String> {
        let table_path = self
            .get_database_path(catalog_name)
            .join(schema_name)
            .join(table_name);

        if !table_path.exists() {
            return Ok(Vec::new());
        }

        let mut partitions = Vec::new();

        let entries = std::fs::read_dir(&table_path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    // Check for Hive-style partition format: date=YYYY-MM-DD
                    if dir_name.starts_with("date=") {
                        let date_str = dir_name.strip_prefix("date=").unwrap_or(dir_name);

                        // Count files and calculate size
                        let (file_count, total_size) = self.count_partition_files(&path)?;

                        partitions.push(PartitionInfo {
                            catalog: catalog_name.to_string(),
                            schema: schema_name.to_string(),
                            table: table_name.to_string(),
                            partition: dir_name.to_string(),
                            date: date_str.to_string(),
                            file_count,
                            total_size_bytes: total_size,
                        });
                    }
                }
            }
        }

        partitions.sort_by(|a, b| b.date.cmp(&a.date)); // Sort by date, newest first
        Ok(partitions)
    }

    /// Discover all date partitions for a table using object storage
    ///
    /// Path structure (node_id as Hive partition key):
    ///   {catalog}/{schema}/{table}/date={YYYY-MM-DD}/node_id={node_id}/*.parquet
    pub async fn discover_table_partitions_async(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<PartitionInfo>, String> {
        // If storage is available, use it
        if let Some(storage) = &self.storage {
            // Path structure: store/catalog/schema/table (node_id is a subdirectory under date)
            let table_prefix = format!("{}/{}/{}/{}", STORE_DIR, catalog_name, schema_name, table_name);
            let partition_dirs = storage.list_prefixes(&table_prefix).await?;

            let mut partitions = Vec::new();
            for dir_name in partition_dirs {
                // Check for Hive-style partition format: date=YYYY-MM-DD
                if dir_name.starts_with("date=") {
                    let date_str = dir_name.strip_prefix("date=").unwrap_or(&dir_name);

                    // List files in partition (includes all node_id= subdirectories)
                    // Path: store/catalog/schema/table/date=YYYY-MM-DD/node_id=*/*.parquet
                    let partition_prefix = format!(
                        "{}/{}/{}/{}/{}",
                        STORE_DIR, catalog_name, schema_name, table_name, dir_name
                    );
                    let files = storage.list(&partition_prefix).await.unwrap_or_default();

                    let mut file_count = 0;
                    let mut total_size = 0u64;
                    for file in &files {
                        if file.ends_with(".parquet") {
                            file_count += 1;
                            if let Ok(size) = storage.size(file).await {
                                total_size += size;
                            }
                        }
                    }

                    partitions.push(PartitionInfo {
                        catalog: catalog_name.to_string(),
                        schema: schema_name.to_string(),
                        table: table_name.to_string(),
                        partition: dir_name.to_string(),
                        date: date_str.to_string(),
                        file_count,
                        total_size_bytes: total_size,
                    });
                }
            }

            partitions.sort_by(|a, b| b.date.cmp(&a.date)); // Sort by date, newest first
            return Ok(partitions);
        }

        // Fallback to local filesystem
        self.discover_table_partitions(catalog_name, schema_name, table_name)
    }

    /// Discover detailed partitions with (date, node_id) breakdown and individual file names.
    ///
    /// Path structure:
    ///   {catalog}/{schema}/{table}/date={YYYY-MM-DD}/node_id={node_id}/*.parquet
    pub async fn discover_table_partitions_detailed_async(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<PartitionDetailInfo>, String> {
        if let Some(storage) = &self.storage {
            let table_prefix = format!("{}/{}/{}/{}", STORE_DIR, catalog_name, schema_name, table_name);
            let date_dirs = storage.list_prefixes(&table_prefix).await?;

            let mut partitions = Vec::new();
            for date_dir in date_dirs {
                if !date_dir.starts_with("date=") {
                    continue;
                }
                let date_str = date_dir
                    .strip_prefix("date=")
                    .unwrap_or(&date_dir)
                    .to_string();

                let date_prefix = format!("{}/{}", table_prefix, date_dir);
                let node_dirs = storage.list_prefixes(&date_prefix).await.unwrap_or_default();

                if node_dirs.is_empty() {
                    // Legacy format: parquet files directly under date= dir (no node_id= level)
                    let files_list = storage.list(&date_prefix).await.unwrap_or_default();
                    let mut files = Vec::new();
                    let mut total_size = 0u64;
                    for file_path in &files_list {
                        if file_path.ends_with(".parquet") {
                            let size = storage.size(file_path).await.unwrap_or(0);
                            total_size += size;
                            let name = file_path
                                .rsplit('/')
                                .next()
                                .unwrap_or(file_path)
                                .to_string();
                            files.push(PartitionFileEntry {
                                path: file_path.clone(),
                                name,
                                size_bytes: size,
                            });
                        }
                    }
                    if !files.is_empty() {
                        partitions.push(PartitionDetailInfo {
                            date: date_str.clone(),
                            node_id: String::new(),
                            files,
                            total_size_bytes: total_size,
                        });
                    }
                } else {
                    for node_dir in node_dirs {
                        let node_id_str = if node_dir.starts_with("node_id=") {
                            node_dir
                                .strip_prefix("node_id=")
                                .unwrap_or(&node_dir)
                                .to_string()
                        } else {
                            node_dir.clone()
                        };

                        let node_prefix = format!("{}/{}", date_prefix, node_dir);
                        let files_list =
                            storage.list(&node_prefix).await.unwrap_or_default();

                        let mut files = Vec::new();
                        let mut total_size = 0u64;
                        for file_path in &files_list {
                            if file_path.ends_with(".parquet") {
                                let size = storage.size(file_path).await.unwrap_or(0);
                                total_size += size;
                                let name = file_path
                                    .rsplit('/')
                                    .next()
                                    .unwrap_or(file_path)
                                    .to_string();
                                files.push(PartitionFileEntry {
                                    path: file_path.clone(),
                                    name,
                                    size_bytes: size,
                                });
                            }
                        }

                        if !files.is_empty() {
                            partitions.push(PartitionDetailInfo {
                                date: date_str.clone(),
                                node_id: node_id_str,
                                files,
                                total_size_bytes: total_size,
                            });
                        }
                    }
                }
            }

            partitions.sort_by(|a, b| {
                b.date.cmp(&a.date).then_with(|| a.node_id.cmp(&b.node_id))
            });
            return Ok(partitions);
        }

        // Fallback to local filesystem
        self.discover_table_partitions_detailed_local(catalog_name, schema_name, table_name)
    }

    /// Local filesystem version of detailed partition discovery
    fn discover_table_partitions_detailed_local(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<PartitionDetailInfo>, String> {
        let table_path = self
            .get_database_path(catalog_name)
            .join(schema_name)
            .join(table_name);

        if !table_path.exists() {
            return Ok(Vec::new());
        }

        let mut partitions = Vec::new();

        let date_entries = std::fs::read_dir(&table_path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        for date_entry in date_entries {
            let date_entry = date_entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let date_path = date_entry.path();
            if !date_path.is_dir() {
                continue;
            }
            let date_dir_name = match date_path.file_name().and_then(|s| s.to_str()) {
                Some(n) if n.starts_with("date=") => n.to_string(),
                _ => continue,
            };
            let date_str = date_dir_name
                .strip_prefix("date=")
                .unwrap_or(&date_dir_name)
                .to_string();

            let sub_entries = std::fs::read_dir(&date_path)
                .map_err(|e| format!("Failed to read date directory: {}", e))?;

            let mut has_node_dirs = false;
            let mut direct_files = Vec::new();
            let mut direct_size = 0u64;

            for sub_entry in sub_entries {
                let sub_entry =
                    sub_entry.map_err(|e| format!("Failed to read entry: {}", e))?;
                let sub_path = sub_entry.path();

                if sub_path.is_dir() {
                    if let Some(dir_name) = sub_path.file_name().and_then(|s| s.to_str()) {
                        if dir_name.starts_with("node_id=") {
                            has_node_dirs = true;
                            let node_id_str = dir_name
                                .strip_prefix("node_id=")
                                .unwrap_or(dir_name)
                                .to_string();

                            let mut files = Vec::new();
                            let mut total_size = 0u64;
                            if let Ok(file_entries) = std::fs::read_dir(&sub_path) {
                                for file_entry in file_entries {
                                    if let Ok(fe) = file_entry {
                                        let fp = fe.path();
                                        if fp.is_file()
                                            && fp.extension().and_then(|s| s.to_str())
                                                == Some("parquet")
                                        {
                                            let size = std::fs::metadata(&fp)
                                                .map(|m| m.len())
                                                .unwrap_or(0);
                                            total_size += size;
                                            let name = fp
                                                .file_name()
                                                .and_then(|s| s.to_str())
                                                .unwrap_or("")
                                                .to_string();
                                            let rel_path = format!(
                                                "{}/{}/{}/{}/{}/{}",
                                                catalog_name,
                                                schema_name,
                                                table_name,
                                                date_dir_name,
                                                dir_name,
                                                name
                                            );
                                            files.push(PartitionFileEntry {
                                                path: rel_path,
                                                name,
                                                size_bytes: size,
                                            });
                                        }
                                    }
                                }
                            }

                            if !files.is_empty() {
                                partitions.push(PartitionDetailInfo {
                                    date: date_str.clone(),
                                    node_id: node_id_str,
                                    files,
                                    total_size_bytes: total_size,
                                });
                            }
                        }
                    }
                } else if sub_path.is_file()
                    && sub_path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    let size = std::fs::metadata(&sub_path)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    direct_size += size;
                    let name = sub_path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    let rel_path = format!(
                        "{}/{}/{}/{}/{}",
                        catalog_name, schema_name, table_name, date_dir_name, name
                    );
                    direct_files.push(PartitionFileEntry {
                        path: rel_path,
                        name,
                        size_bytes: size,
                    });
                }
            }

            // If no node_id= dirs found, use direct parquet files (legacy format)
            if !has_node_dirs && !direct_files.is_empty() {
                partitions.push(PartitionDetailInfo {
                    date: date_str,
                    node_id: String::new(),
                    files: direct_files,
                    total_size_bytes: direct_size,
                });
            }
        }

        partitions.sort_by(|a, b| {
            b.date.cmp(&a.date).then_with(|| a.node_id.cmp(&b.node_id))
        });
        Ok(partitions)
    }

    /// Count files and calculate total size in a partition directory
    ///
    /// Handles nested node_id= subdirectories (Hive partition structure):
    ///   date=YYYY-MM-DD/node_id=X/*.parquet
    ///   date=YYYY-MM-DD/node_id=Y/*.parquet
    fn count_partition_files(&self, partition_path: &Path) -> Result<(usize, u64), String> {
        let entries = std::fs::read_dir(partition_path)
            .map_err(|e| format!("Failed to read partition directory: {}", e))?;

        let mut file_count = 0;
        let mut total_size = 0u64;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                // Direct parquet file in partition (legacy format)
                file_count += 1;
                if let Ok(metadata) = std::fs::metadata(&path) {
                    total_size += metadata.len();
                }
            } else if path.is_dir() {
                // Check for node_id= subdirectory (new Hive partition format)
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    if dir_name.starts_with("node_id=") {
                        // Recurse into node_id= directory to count parquet files
                        let (sub_count, sub_size) = self.count_files_in_dir(&path)?;
                        file_count += sub_count;
                        total_size += sub_size;
                    }
                }
            }
        }

        Ok((file_count, total_size))
    }

    /// Count parquet files in a directory (non-recursive, single level)
    fn count_files_in_dir(&self, dir_path: &Path) -> Result<(usize, u64), String> {
        let entries = std::fs::read_dir(dir_path)
            .map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut file_count = 0;
        let mut total_size = 0u64;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                file_count += 1;
                if let Ok(metadata) = std::fs::metadata(&path) {
                    total_size += metadata.len();
                }
            }
        }

        Ok((file_count, total_size))
    }

    /// Delete a specific partition (date directory)
    pub fn delete_partition(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        partition: &str,
    ) -> Result<String, String> {
        // Ensure partition format: date=YYYY-MM-DD
        let partition_dir = if partition.starts_with("date=") {
            partition.to_string()
        } else {
            format!("date={}", partition)
        };

        let partition_path = self
            .get_database_path(catalog_name)
            .join(schema_name)
            .join(table_name)
            .join(&partition_dir);

        if !partition_path.exists() {
            return Err(format!(
                "Partition '{}' not found in {}.{}.{}",
                partition_dir, catalog_name, schema_name, table_name
            ));
        }

        // Get size before deletion
        let (file_count, total_size) = self.count_partition_files(&partition_path)?;

        std::fs::remove_dir_all(&partition_path)
            .map_err(|e| format!("Failed to delete partition: {}", e))?;

        Ok(format!(
            "Deleted partition {}.{}.{}/{} ({} files, {:.2} MB)",
            catalog_name,
            schema_name,
            table_name,
            partition_dir,
            file_count,
            total_size as f64 / 1024.0 / 1024.0
        ))
    }

    /// Delete partitions older than specified date
    pub fn delete_partitions_before(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        before_date: &str, // Format: YYYY-MM-DD
    ) -> Result<Vec<String>, String> {
        let partitions = self.discover_table_partitions(catalog_name, schema_name, table_name)?;

        let mut deleted = Vec::new();

        for partition in partitions {
            if partition.date.as_str() < before_date {
                match self.delete_partition(catalog_name, schema_name, table_name, &partition.date)
                {
                    Ok(msg) => deleted.push(msg),
                    Err(e) => {
                        return Err(format!(
                            "Failed to delete partition {}: {}",
                            partition.date, e
                        ));
                    }
                }
            }
        }

        Ok(deleted)
    }

    /// Delete partitions older than N days
    pub fn delete_partitions_older_than_days(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        days: u32,
    ) -> Result<Vec<String>, String> {
        use chrono::{Duration, Utc};

        let cutoff_date = Utc::now() - Duration::days(days as i64);
        let cutoff_str = cutoff_date.format("%Y-%m-%d").to_string();

        self.delete_partitions_before(catalog_name, schema_name, table_name, &cutoff_str)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PartitionInfo {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub partition: String,
    pub date: String,
    pub file_count: usize,
    pub total_size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PartitionDetailInfo {
    pub date: String,
    pub node_id: String,
    pub files: Vec<PartitionFileEntry>,
    pub total_size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PartitionFileEntry {
    pub path: String,
    pub name: String,
    pub size_bytes: u64,
}
