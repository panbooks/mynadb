//! Discovery Operations for QueryEngine
//!
//! Handles partition discovery, cache pre-warming, and auto-discovery of tables.

use crate::storage::layout::STORE_DIR;
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::QueryEngine;
use crate::query::cache::metadata_cache::TableParquetStats;

impl QueryEngine {
    // ========================================================================
    // Partition & File Discovery
    // ========================================================================

    /// Discover date partitions in a table directory (with caching)
    pub(crate) fn discover_table_partitions(
        &self,
        table_path: &str,
    ) -> Result<Vec<String>, String> {
        let path = std::path::Path::new(table_path);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let path_buf = path.to_path_buf();

        // Check file listing cache first
        if let Some(cached) = self.metadata_cache.get_file_listing(&path_buf) {
            // Extract partition names from cached file paths
            let partitions: Vec<String> = cached
                .files
                .iter()
                .filter_map(|p| {
                    p.file_name()
                        .and_then(|s| s.to_str())
                        .filter(|s| s.starts_with("date="))
                        .map(|s| s.to_string())
                })
                .collect();
            if !partitions.is_empty() {
                return Ok(partitions);
            }
        }

        let entries = std::fs::read_dir(path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        let mut partitions = Vec::new();
        let mut all_entries = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let entry_path = entry.path();
            all_entries.push(entry_path.clone());

            if entry_path.is_dir() {
                if let Some(dir_name) = entry_path.file_name().and_then(|s| s.to_str()) {
                    // Check for Hive-style partition format: date=YYYY-MM-DD
                    if dir_name.starts_with("date=") {
                        partitions.push(dir_name.to_string());
                    }
                }
            }
        }

        // Cache the directory listing
        self.metadata_cache.set_file_listing(path_buf, all_entries);

        partitions.sort();
        Ok(partitions)
    }

    /// Count Parquet files in a directory (with caching)
    pub(crate) fn count_parquet_files_cached(&self, dir_path: &str) -> usize {
        let path = PathBuf::from(dir_path);

        // Check cache first
        if let Some(cached) = self.metadata_cache.get_file_listing(&path) {
            return cached
                .files
                .iter()
                .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
                .count();
        }

        // Cache miss - scan and cache
        if let Ok(entries) = std::fs::read_dir(&path) {
            let files: Vec<PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();

            let parquet_count = files
                .iter()
                .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
                .count();

            self.metadata_cache.set_file_listing(path, files);
            parquet_count
        } else {
            0
        }
    }

    /// Read and cache Parquet metadata for a file
    /// Returns cached metadata if available, otherwise reads from disk and caches
    pub fn get_parquet_metadata_cached(&self, file_path: &PathBuf) -> Option<Arc<ParquetMetaData>> {
        // Check cache first
        if let Some(metadata) = self.metadata_cache.get_parquet_metadata(file_path) {
            return Some(metadata);
        }

        // Cache miss - read from disk
        match self.read_parquet_metadata(file_path) {
            Ok((metadata, file_size)) => {
                let arc_metadata = Arc::new(metadata);
                self.metadata_cache.set_parquet_metadata(
                    file_path.clone(),
                    arc_metadata.clone(),
                    file_size,
                );
                Some(arc_metadata)
            }
            Err(e) => {
                warn!("Failed to read Parquet metadata for {:?}: {}", file_path, e);
                None
            }
        }
    }

    /// Read Parquet metadata from disk
    fn read_parquet_metadata(&self, file_path: &PathBuf) -> Result<(ParquetMetaData, u64), String> {
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;
        use std::fs::File;

        let file = File::open(file_path).map_err(|e| format!("Failed to open file: {}", e))?;

        let file_size = file
            .metadata()
            .map_err(|e| format!("Failed to get file metadata: {}", e))?
            .len();

        let reader = SerializedFileReader::new(file)
            .map_err(|e| format!("Failed to create Parquet reader: {}", e))?;

        let metadata = reader.metadata().clone();
        Ok((metadata, file_size))
    }

    // ========================================================================
    // Cache Pre-warming
    // ========================================================================

    /// Pre-warm the metadata cache for a table
    /// This reads all Parquet file footers and caches them for faster query execution
    pub async fn prewarm_table_cache(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<usize, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
        let path = std::path::Path::new(&table_path);

        if !path.exists() {
            return Err(format!("Table path not found: {}", table_path));
        }

        let mut cached_count = 0;

        // Discover partitions
        let partitions = self.discover_table_partitions(&table_path)?;
        let has_partitions = !partitions.is_empty();

        if partitions.is_empty() {
            // Non-partitioned table - cache files directly
            cached_count += self.prewarm_directory_cache(&table_path)?;
        } else {
            // Partitioned table - cache files in each partition
            for partition in &partitions {
                let partition_path = format!("{}/{}", table_path, partition);
                cached_count += self.prewarm_directory_cache(&partition_path)?;
            }
        }

        // Also cache the table info
        self.metadata_cache
            .set_table_info(catalog, schema, table, has_partitions, partitions);

        info!(
            "Pre-warmed cache for {}.{}.{}: {} Parquet files cached",
            catalog, schema, table, cached_count
        );
        Ok(cached_count)
    }

    /// Pre-warm cache for all Parquet files in a directory
    fn prewarm_directory_cache(&self, dir_path: &str) -> Result<usize, String> {
        let path = PathBuf::from(dir_path);
        let entries =
            std::fs::read_dir(&path).map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut cached_count = 0;
        let mut all_files = Vec::new();

        for entry in entries.filter_map(|e| e.ok()) {
            let file_path = entry.path();
            all_files.push(file_path.clone());

            if file_path
                .extension()
                .map(|e| e == "parquet")
                .unwrap_or(false)
            {
                // Cache the Parquet metadata
                if self.get_parquet_metadata_cached(&file_path).is_some() {
                    cached_count += 1;
                }
            }
        }

        // Cache the file listing
        self.metadata_cache.set_file_listing(path, all_files);

        Ok(cached_count)
    }

    /// Pre-warm cache for an entire catalog (database)
    pub async fn prewarm_catalog_cache(&self, catalog: &str) -> Result<usize, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog);

        if !std::path::Path::new(&catalog_path).exists() {
            return Ok(0);
        }

        let mut total_cached = 0;

        // Iterate through schemas
        let schema_entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        for schema_entry in schema_entries.filter_map(|e| e.ok()) {
            let schema_path = schema_entry.path();
            if !schema_path.is_dir() {
                continue;
            }

            let schema_name = match schema_path.file_name().and_then(|s| s.to_str()) {
                Some(name) if !name.starts_with('.') => name,
                _ => continue,
            };

            // Iterate through tables
            let table_entries = match std::fs::read_dir(&schema_path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            for table_entry in table_entries.filter_map(|e| e.ok()) {
                let table_path = table_entry.path();
                if !table_path.is_dir() {
                    continue;
                }

                let table_name = match table_path.file_name().and_then(|s| s.to_str()) {
                    Some(name) if !name.starts_with('.') => name,
                    _ => continue,
                };

                match self
                    .prewarm_table_cache(catalog, schema_name, table_name)
                    .await
                {
                    Ok(count) => total_cached += count,
                    Err(e) => warn!(
                        "Failed to prewarm {}.{}.{}: {}",
                        catalog, schema_name, table_name, e
                    ),
                }
            }
        }

        info!(
            "Pre-warmed cache for catalog '{}': {} total Parquet files cached",
            catalog, total_cached
        );
        Ok(total_cached)
    }

    /// Get statistics about Parquet files in a table (using cached metadata)
    pub fn get_table_parquet_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableParquetStats, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
        let path = std::path::Path::new(&table_path);

        if !path.exists() {
            return Err(format!("Table path not found: {}", table_path));
        }

        let mut total_files = 0;
        let mut total_rows = 0u64;
        let mut total_size = 0u64;
        let mut total_row_groups = 0;

        let partitions = self.discover_table_partitions(&table_path)?;

        let dirs_to_scan: Vec<String> = if partitions.is_empty() {
            vec![table_path.clone()]
        } else {
            partitions
                .iter()
                .map(|p| format!("{}/{}", table_path, p))
                .collect()
        };

        for dir in dirs_to_scan {
            let path = PathBuf::from(&dir);
            if let Ok(entries) = std::fs::read_dir(&path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let file_path = entry.path();
                    if file_path
                        .extension()
                        .map(|e| e == "parquet")
                        .unwrap_or(false)
                    {
                        total_files += 1;

                        if let Ok(metadata) = std::fs::metadata(&file_path) {
                            total_size += metadata.len();
                        }

                        // Get row count from cached Parquet metadata
                        if let Some(pq_meta) = self.get_parquet_metadata_cached(&file_path) {
                            total_rows += pq_meta.file_metadata().num_rows() as u64;
                            total_row_groups += pq_meta.num_row_groups();
                        }
                    }
                }
            }
        }

        Ok(TableParquetStats {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            total_files,
            total_rows,
            total_size_bytes: total_size,
            total_row_groups,
            partition_count: partitions.len(),
        })
    }

    // ========================================================================
    // Auto-discovery
    // ========================================================================

    /// Auto-discover and register all tables in catalog.schema
    /// Works with both local filesystem and object storage (S3/MinIO)
    pub async fn discover_and_register(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<usize, String> {
        let mut tables_found: HashSet<String> = HashSet::new();

        // Try object storage first if configured
        if let Some(ref storage) = self.storage {
            let prefix = self.build_storage_prefix(catalog_name, schema_name);

            if let Ok(files) = storage.list(&prefix).await {
                for file in files {
                    let relative = file.strip_prefix(&prefix).unwrap_or(&file);
                    if let Some(table_name) = relative.split('/').next() {
                        if !table_name.is_empty()
                            && !table_name.starts_with('.')
                            && !table_name.starts_with("date=")
                        {
                            tables_found.insert(table_name.to_string());
                        }
                    }
                }
            }
        } else {
            // Fallback to local filesystem
            let schema_path = format!("{}/{}/{}", self.data_dir, catalog_name, schema_name);

            if std::path::Path::new(&schema_path).exists() {
                if let Ok(entries) = std::fs::read_dir(&schema_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(table_name) = path.file_name().and_then(|s| s.to_str()) {
                                if !table_name.starts_with('.') {
                                    tables_found.insert(table_name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Register all discovered tables
        let mut count = 0;
        for table_name in tables_found {
            if let Err(e) = self
                .register_table(catalog_name, schema_name, &table_name)
                .await
            {
                warn!(
                    "Failed to register {}.{}.{}: {}",
                    catalog_name, schema_name, table_name, e
                );
            } else {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Auto-discover all schemas and tables in a catalog
    /// Works with both local filesystem and object storage (S3/MinIO)
    /// Also includes tables from in-memory cache (write_cache)
    pub async fn discover_catalog(&self, catalog_name: &str) -> Result<usize, String> {
        let mut schemas_found: HashSet<String> = HashSet::new();
        let mut cached_tables: HashSet<(String, String)> = HashSet::new();

        // Get tables from in-memory cache first
        if let Some(ref cache) = self.write_cache {
            cached_tables = cache.list_cached_tables_in_catalog(catalog_name);
            for (schema, _table) in &cached_tables {
                schemas_found.insert(schema.clone());
            }
        }

        // Discover schemas from storage
        if let Some(ref storage) = self.storage {
            let prefix = self.build_catalog_prefix(catalog_name);

            if let Ok(files) = storage.list(&prefix).await {
                for file in files {
                    let relative = file.strip_prefix(&prefix).unwrap_or(&file);
                    if let Some(schema_name) = relative.split('/').next() {
                        if !schema_name.is_empty()
                            && !schema_name.starts_with('.')
                            && !schema_name.starts_with("date=")
                        {
                            schemas_found.insert(schema_name.to_string());
                        }
                    }
                }
            }
        } else {
            // Fallback to local filesystem
            let catalog_path = format!("{}/{}", self.data_dir, catalog_name);

            if std::path::Path::new(&catalog_path).exists() {
                if let Ok(entries) = std::fs::read_dir(&catalog_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(schema_name) = path.file_name().and_then(|s| s.to_str()) {
                                if !schema_name.starts_with('.') {
                                    schemas_found.insert(schema_name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Discover and register tables from each schema
        let mut total_tables = 0;
        for schema_name in &schemas_found {
            match self.discover_and_register(catalog_name, schema_name).await {
                Ok(count) => total_tables += count,
                Err(e) => warn!(
                    "Failed to discover schema {}.{}: {}",
                    catalog_name, schema_name, e
                ),
            }
        }

        // Register tables from in-memory cache that might not have parquet files yet
        for (schema_name, table_name) in cached_tables {
            if let Err(e) = self
                .register_table(catalog_name, &schema_name, &table_name)
                .await
            {
                // Don't warn if table already registered
                if !e.contains("already") {
                    warn!(
                        "Failed to register cached table {}.{}.{}: {}",
                        catalog_name, schema_name, table_name, e
                    );
                }
            } else {
                total_tables += 1;
            }
        }

        Ok(total_tables)
    }

    /// Ensure a schema is tracked in registered catalogs
    pub async fn ensure_schema(&self, catalog_name: &str, schema_name: &str) -> Result<(), String> {
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());
        Ok(())
    }

    /// Ensure all tables in a database are registered
    pub async fn ensure_database_registered(&self, db_name: &str) -> Result<(), String> {
        let count = self.discover_catalog(db_name).await?;
        if count > 0 {
            info!(
                "Auto-registered {} table(s) in catalog '{}'",
                count, db_name
            );
        }
        Ok(())
    }

    // ========================================================================
    // Storage Prefix Helpers
    // ========================================================================

    /// Build storage prefix for schema discovery
    /// Note: node_id is a Hive-style partition key (node_id=xxx) inside each table,
    /// NOT a top-level directory prefix. The storage path is: catalog/schema/table/date=.../node_id=.../
    pub(crate) fn build_storage_prefix(&self, catalog_name: &str, schema_name: &str) -> String {
        format!("{}/{}/{}/", STORE_DIR, catalog_name, schema_name)
    }

    /// Build catalog-level prefix
    /// Note: node_id is a Hive-style partition key inside each table, not a top-level prefix
    pub(crate) fn build_catalog_prefix(&self, catalog_name: &str) -> String {
        format!("{}/{}/", STORE_DIR, catalog_name)
    }
}
