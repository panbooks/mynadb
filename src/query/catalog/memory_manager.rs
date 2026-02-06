//! Memory-based MetadataManager implementation
//!
//! Stores all metadata in memory with periodic persistence to JSON files.

use super::manager::{GlobalStats, MetadataError, MetadataManager, MetadataResult};
use super::types::*;
use crate::meta::current_timestamp_ms;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// File name for the metadata snapshot
const SNAPSHOT_FILE: &str = "catalog_snapshot.json";

/// In-memory metadata storage
struct InMemoryStore {
    /// Catalogs: name -> CatalogInfo
    catalogs: HashMap<String, CatalogInfo>,
    /// Schemas: (catalog, schema) -> SchemaInfo
    schemas: HashMap<(String, String), SchemaInfo>,
    /// Tables: (catalog, schema, table) -> TableInfo
    tables: HashMap<(String, String, String), TableInfo>,
    /// Partitions: (catalog, schema, table, partition_key) -> PartitionInfo
    partitions: HashMap<(String, String, String, String), PartitionInfo>,
    /// Files: path -> FileInfo
    files: HashMap<String, FileInfo>,
    /// Reverse index: (catalog, schema, table, partition_key) -> Vec<file_path>
    partition_files: HashMap<(String, String, String, String), Vec<String>>,
}

impl InMemoryStore {
    fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
            schemas: HashMap::new(),
            tables: HashMap::new(),
            partitions: HashMap::new(),
            files: HashMap::new(),
            partition_files: HashMap::new(),
        }
    }

    /// Build from a snapshot
    fn from_snapshot(snapshot: CatalogSnapshot) -> Self {
        let mut store = Self::new();

        for catalog in snapshot.catalogs {
            store.catalogs.insert(catalog.name.clone(), catalog);
        }

        for schema in snapshot.schemas {
            store
                .schemas
                .insert((schema.catalog.clone(), schema.name.clone()), schema);
        }

        for table in snapshot.tables {
            store.tables.insert(
                (
                    table.catalog.clone(),
                    table.schema.clone(),
                    table.name.clone(),
                ),
                table,
            );
        }

        for partition in snapshot.partitions {
            store.partitions.insert(
                (
                    partition.catalog.clone(),
                    partition.schema.clone(),
                    partition.table.clone(),
                    partition.partition_key.clone(),
                ),
                partition,
            );
        }

        for file in snapshot.files {
            let key = (
                file.catalog.clone(),
                file.schema.clone(),
                file.table.clone(),
                file.partition_key.clone(),
            );
            store
                .partition_files
                .entry(key)
                .or_default()
                .push(file.path.clone());
            store.files.insert(file.path.clone(), file);
        }

        store
    }

    /// Export to snapshot
    fn to_snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            version: CatalogSnapshot::CURRENT_VERSION,
            snapshot_at: chrono::Utc::now().timestamp_millis(),
            catalogs: self.catalogs.values().cloned().collect(),
            schemas: self.schemas.values().cloned().collect(),
            tables: self.tables.values().cloned().collect(),
            partitions: self.partitions.values().cloned().collect(),
            files: self.files.values().cloned().collect(),
        }
    }
}

/// Memory-based metadata manager with JSON persistence
pub struct MemoryMetadataManager {
    /// In-memory store
    store: RwLock<InMemoryStore>,
    /// Path to metadata directory
    metadata_dir: PathBuf,
    /// Whether the store has been modified since last save
    dirty: AtomicBool,
    /// Modification counter
    version: AtomicU64,
}

impl MemoryMetadataManager {
    /// Create a new memory metadata manager
    pub fn new(metadata_dir: impl AsRef<Path>) -> Self {
        let metadata_dir = metadata_dir.as_ref().to_path_buf();
        Self {
            store: RwLock::new(InMemoryStore::new()),
            metadata_dir,
            dirty: AtomicBool::new(false),
            version: AtomicU64::new(0),
        }
    }

    /// Get the snapshot file path
    fn snapshot_path(&self) -> PathBuf {
        self.metadata_dir.join(SNAPSHOT_FILE)
    }

    /// Mark the store as dirty
    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if the store is dirty
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Get the current version
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Update aggregated statistics for a table
    fn update_table_stats(&self, catalog: &str, schema: &str, table: &str) {
        let mut store = self.store.write();

        let table_key = (catalog.to_string(), schema.to_string(), table.to_string());

        // Collect partition stats
        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut partition_count = 0;
        let mut file_count = 0;
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        for ((c, s, t, _), partition) in store.partitions.iter() {
            if c == catalog && s == schema && t == table {
                total_rows += partition.total_rows;
                total_bytes += partition.total_bytes;
                partition_count += 1;
                file_count += partition.file_count;

                if let Some(ts) = partition.min_timestamp {
                    min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
                }
                if let Some(ts) = partition.max_timestamp {
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                }
            }
        }

        if let Some(table_info) = store.tables.get_mut(&table_key) {
            table_info.total_rows = total_rows;
            table_info.total_bytes = total_bytes;
            table_info.partition_count = partition_count;
            table_info.file_count = file_count;
            table_info.min_timestamp = min_ts;
            table_info.max_timestamp = max_ts;
            table_info.updated_at = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Update aggregated statistics for a partition
    fn update_partition_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) {
        let mut store = self.store.write();

        let partition_key_tuple = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            partition_key.to_string(),
        );

        // Collect file stats
        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut file_count = 0;
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        if let Some(file_paths) = store.partition_files.get(&partition_key_tuple) {
            for path in file_paths {
                if let Some(file) = store.files.get(path) {
                    total_rows += file.row_count;
                    total_bytes += file.size_bytes;
                    file_count += 1;

                    if let Some(ts) = file.min_timestamp {
                        min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
                    }
                    if let Some(ts) = file.max_timestamp {
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    }
                }
            }
        }

        if let Some(partition) = store.partitions.get_mut(&partition_key_tuple) {
            partition.total_rows = total_rows;
            partition.total_bytes = total_bytes;
            partition.file_count = file_count;
            partition.min_timestamp = min_ts;
            partition.max_timestamp = max_ts;
            partition.updated_at = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Ensure catalog exists, create if not
    fn ensure_catalog(&self, catalog: &str) {
        let mut store = self.store.write();
        if !store.catalogs.contains_key(catalog) {
            let now = chrono::Utc::now().timestamp_millis();
            store.catalogs.insert(
                catalog.to_string(),
                CatalogInfo {
                    name: catalog.to_string(),
                    created_at: now,
                    updated_at: now,
                    schema_count: 0,
                    table_count: 0,
                    total_bytes: 0,
                },
            );
        }
    }

    /// Ensure schema exists, create if not
    fn ensure_schema(&self, catalog: &str, schema: &str) {
        self.ensure_catalog(catalog);
        let mut store = self.store.write();
        let key = (catalog.to_string(), schema.to_string());
        store.schemas.entry(key).or_insert_with(|| {
            let now = chrono::Utc::now().timestamp_millis();
            SchemaInfo {
                name: schema.to_string(),
                catalog: catalog.to_string(),
                created_at: now,
                updated_at: now,
                table_count: 0,
                total_bytes: 0,
            }
        });
    }

    /// Ensure table exists, create if not
    fn ensure_table(&self, catalog: &str, schema: &str, table: &str) {
        self.ensure_schema(catalog, schema);
        let mut store = self.store.write();
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        store.tables.entry(key).or_insert_with(|| {
            let now = chrono::Utc::now().timestamp_millis();
            TableInfo {
                name: table.to_string(),
                schema: schema.to_string(),
                catalog: catalog.to_string(),
                created_at: now,
                updated_at: now,
                total_rows: 0,
                total_bytes: 0,
                partition_count: 0,
                file_count: 0,
                min_timestamp: None,
                max_timestamp: None,
                columns: None,
            }
        });
    }

    /// Ensure partition exists, create if not
    fn ensure_partition(&self, catalog: &str, schema: &str, table: &str, partition_key: &str) {
        self.ensure_table(catalog, schema, table);
        let mut store = self.store.write();
        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            partition_key.to_string(),
        );
        if !store.partitions.contains_key(&key) {
            let now = chrono::Utc::now().timestamp_millis();
            // Parse partition value (days since epoch)
            let partition_value = parse_partition_date(partition_key).unwrap_or(0);
            store.partitions.insert(
                key.clone(),
                PartitionInfo {
                    partition_key: partition_key.to_string(),
                    partition_value,
                    table: table.to_string(),
                    schema: schema.to_string(),
                    catalog: catalog.to_string(),
                    created_at: now,
                    updated_at: now,
                    total_rows: 0,
                    total_bytes: 0,
                    file_count: 0,
                    min_timestamp: None,
                    max_timestamp: None,
                },
            );
            store.partition_files.insert(key, Vec::new());
        }
    }
}

/// Parse partition date string to days since epoch
fn parse_partition_date(date_str: &str) -> Option<i32> {
    use chrono::{Datelike, NaiveDate};
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .ok()
        .map(|d| d.num_days_from_ce() - 719163) // Days since 1970-01-01
}

/// Context for scanning a partition directory
struct PartitionScanContext<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
    partition_key: String,
    partition_dir: String,
}

/// Scan parquet files in a partition directory and register them
async fn scan_partition_files(
    manager: &MemoryMetadataManager,
    partition_path: &std::path::Path,
    ctx: &PartitionScanContext<'_>,
    node_prefix: Option<&str>,
) -> Result<(), MetadataError> {
    let mut files = tokio::fs::read_dir(partition_path)
        .await
        .map_err(|e| MetadataError::IoError(e.to_string()))?;

    while let Some(file_entry) = files
        .next_entry()
        .await
        .map_err(|e| MetadataError::IoError(e.to_string()))?
    {
        let file_name = file_entry.file_name().to_string_lossy().to_string();
        if !file_name.ends_with(".parquet") {
            continue;
        }

        let metadata = tokio::fs::metadata(file_entry.path())
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        let path = match node_prefix {
            Some(node_id) => format!(
                "{}/{}/{}/{}/{}/{}",
                node_id, ctx.catalog, ctx.schema, ctx.table, ctx.partition_dir, file_name
            ),
            None => file_entry.path().to_string_lossy().to_string(),
        };

        let level = FileInfo::parse_level(&file_name);

        let file_info = FileInfo {
            path,
            file_name,
            level,
            size_bytes: metadata.len(),
            row_count: 0,
            row_group_count: 0,
            partition_key: ctx.partition_key.clone(),
            table: ctx.table.to_string(),
            schema: ctx.schema.to_string(),
            catalog: ctx.catalog.to_string(),
            created_at: chrono::Utc::now().timestamp_millis(),
            min_timestamp: None,
            max_timestamp: None,
            etag: None,
            column_stats: None,
        };

        manager.register_file(file_info).await?;
    }

    Ok(())
}

#[async_trait]
impl MetadataManager for MemoryMetadataManager {
    // ==================== Lifecycle ====================

    async fn load(&self) -> MetadataResult<()> {
        let path = self.snapshot_path();

        if !path.exists() {
            info!("No metadata snapshot found at {:?}, starting fresh", path);
            return Ok(());
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        let snapshot: CatalogSnapshot = serde_json::from_str(&content)
            .map_err(|e| MetadataError::SerializationError(e.to_string()))?;

        if snapshot.version != CatalogSnapshot::CURRENT_VERSION {
            warn!(
                "Metadata snapshot version mismatch: {} vs {}",
                snapshot.version,
                CatalogSnapshot::CURRENT_VERSION
            );
        }

        let store = InMemoryStore::from_snapshot(snapshot);
        *self.store.write() = store;

        self.dirty.store(false, Ordering::Release);

        info!("Loaded metadata snapshot from {:?}", path);
        Ok(())
    }

    async fn save(&self) -> MetadataResult<()> {
        if !self.is_dirty() {
            debug!("Metadata not dirty, skipping save");
            return Ok(());
        }

        // Ensure directory exists
        tokio::fs::create_dir_all(&self.metadata_dir)
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        let snapshot = self.store.read().to_snapshot();

        let content = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| MetadataError::SerializationError(e.to_string()))?;

        let path = self.snapshot_path();
        let temp_path = path.with_extension("json.tmp");

        // Write to temp file first
        tokio::fs::write(&temp_path, &content)
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        // Atomic rename
        tokio::fs::rename(&temp_path, &path)
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        self.dirty.store(false, Ordering::Release);

        info!("Saved metadata snapshot to {:?}", path);
        Ok(())
    }

    async fn rescan(&self, data_dir: &str) -> MetadataResult<()> {
        info!("Starting full metadata rescan of {}", data_dir);

        let data_path = Path::new(data_dir);
        if !data_path.exists() {
            return Ok(());
        }

        // Clear existing data
        *self.store.write() = InMemoryStore::new();

        // Scan catalogs (top-level directories)
        let mut catalogs = tokio::fs::read_dir(data_path)
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        while let Some(catalog_entry) = catalogs
            .next_entry()
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?
        {
            let catalog_name = catalog_entry.file_name().to_string_lossy().to_string();
            if catalog_name.starts_with('.') || catalog_name.starts_with('_') {
                continue;
            }

            if !catalog_entry.path().is_dir() {
                continue;
            }

            self.ensure_catalog(&catalog_name);

            // Scan schemas
            let mut schemas = tokio::fs::read_dir(catalog_entry.path())
                .await
                .map_err(|e| MetadataError::IoError(e.to_string()))?;

            while let Some(schema_entry) = schemas
                .next_entry()
                .await
                .map_err(|e| MetadataError::IoError(e.to_string()))?
            {
                let schema_name = schema_entry.file_name().to_string_lossy().to_string();
                if schema_name.starts_with('.') || schema_name.starts_with('_') {
                    continue;
                }

                if !schema_entry.path().is_dir() {
                    continue;
                }

                self.ensure_schema(&catalog_name, &schema_name);

                // Scan tables
                let mut tables = tokio::fs::read_dir(schema_entry.path())
                    .await
                    .map_err(|e| MetadataError::IoError(e.to_string()))?;

                while let Some(table_entry) = tables
                    .next_entry()
                    .await
                    .map_err(|e| MetadataError::IoError(e.to_string()))?
                {
                    let table_name = table_entry.file_name().to_string_lossy().to_string();
                    if table_name.starts_with('.') || table_name.starts_with('_') {
                        continue;
                    }

                    if !table_entry.path().is_dir() {
                        continue;
                    }

                    self.ensure_table(&catalog_name, &schema_name, &table_name);

                    // Scan partitions
                    let mut partitions = tokio::fs::read_dir(table_entry.path())
                        .await
                        .map_err(|e| MetadataError::IoError(e.to_string()))?;

                    while let Some(partition_entry) = partitions
                        .next_entry()
                        .await
                        .map_err(|e| MetadataError::IoError(e.to_string()))?
                    {
                        let partition_dir =
                            partition_entry.file_name().to_string_lossy().to_string();

                        // Parse partition key (date=YYYY-MM-DD)
                        let partition_key = if let Some(key) = partition_dir.strip_prefix("date=") {
                            key.to_string()
                        } else {
                            continue;
                        };

                        if !partition_entry.path().is_dir() {
                            continue;
                        }

                        self.ensure_partition(
                            &catalog_name,
                            &schema_name,
                            &table_name,
                            &partition_key,
                        );

                        // Scan parquet files using helper
                        let ctx = PartitionScanContext {
                            catalog: &catalog_name,
                            schema: &schema_name,
                            table: &table_name,
                            partition_key: partition_key.clone(),
                            partition_dir: partition_dir.clone(),
                        };
                        scan_partition_files(self, &partition_entry.path(), &ctx, None).await?;

                        // Update partition stats
                        self.update_partition_stats(
                            &catalog_name,
                            &schema_name,
                            &table_name,
                            &partition_key,
                        );
                    }

                    // Update table stats
                    self.update_table_stats(&catalog_name, &schema_name, &table_name);
                }
            }
        }

        self.mark_dirty();
        info!("Metadata rescan complete");
        Ok(())
    }

    /// Rescan storage for a specific node (cluster mode)
    /// Scans: {data_dir}/{node_id}/{catalog}/{schema}/{table}/date=YYYY-MM-DD/*.parquet
    async fn rescan_node(&self, data_dir: &str, node_id: &str) -> MetadataResult<()> {
        // Build the node-specific data path
        let node_path = Path::new(data_dir).join(node_id);
        info!(
            "Starting metadata rescan for node {} at {:?}",
            node_id, node_path
        );

        if !node_path.exists() {
            info!("Node data directory does not exist yet: {:?}", node_path);
            return Ok(());
        }

        // Clear existing data
        *self.store.write() = InMemoryStore::new();

        // Scan catalogs under the node directory
        let mut catalogs = tokio::fs::read_dir(&node_path)
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?;

        while let Some(catalog_entry) = catalogs
            .next_entry()
            .await
            .map_err(|e| MetadataError::IoError(e.to_string()))?
        {
            let catalog_name = catalog_entry.file_name().to_string_lossy().to_string();
            if catalog_name.starts_with('.') || catalog_name.starts_with('_') {
                continue;
            }

            if !catalog_entry.path().is_dir() {
                continue;
            }

            self.ensure_catalog(&catalog_name);

            // Scan schemas
            let mut schemas = tokio::fs::read_dir(catalog_entry.path())
                .await
                .map_err(|e| MetadataError::IoError(e.to_string()))?;

            while let Some(schema_entry) = schemas
                .next_entry()
                .await
                .map_err(|e| MetadataError::IoError(e.to_string()))?
            {
                let schema_name = schema_entry.file_name().to_string_lossy().to_string();
                if schema_name.starts_with('.') || schema_name.starts_with('_') {
                    continue;
                }

                if !schema_entry.path().is_dir() {
                    continue;
                }

                self.ensure_schema(&catalog_name, &schema_name);

                // Scan tables
                let mut tables = tokio::fs::read_dir(schema_entry.path())
                    .await
                    .map_err(|e| MetadataError::IoError(e.to_string()))?;

                while let Some(table_entry) = tables
                    .next_entry()
                    .await
                    .map_err(|e| MetadataError::IoError(e.to_string()))?
                {
                    let table_name = table_entry.file_name().to_string_lossy().to_string();
                    if table_name.starts_with('.') || table_name.starts_with('_') {
                        continue;
                    }

                    if !table_entry.path().is_dir() {
                        continue;
                    }

                    self.ensure_table(&catalog_name, &schema_name, &table_name);

                    // Scan partitions
                    let mut partitions = tokio::fs::read_dir(table_entry.path())
                        .await
                        .map_err(|e| MetadataError::IoError(e.to_string()))?;

                    while let Some(partition_entry) = partitions
                        .next_entry()
                        .await
                        .map_err(|e| MetadataError::IoError(e.to_string()))?
                    {
                        let partition_dir =
                            partition_entry.file_name().to_string_lossy().to_string();

                        // Parse partition key (date=YYYY-MM-DD)
                        let partition_key = if let Some(key) = partition_dir.strip_prefix("date=") {
                            key.to_string()
                        } else {
                            continue;
                        };

                        if !partition_entry.path().is_dir() {
                            continue;
                        }

                        self.ensure_partition(
                            &catalog_name,
                            &schema_name,
                            &table_name,
                            &partition_key,
                        );

                        // Scan Parquet files using helper
                        let ctx = PartitionScanContext {
                            catalog: &catalog_name,
                            schema: &schema_name,
                            table: &table_name,
                            partition_key: partition_key.clone(),
                            partition_dir: partition_dir.clone(),
                        };
                        scan_partition_files(self, &partition_entry.path(), &ctx, Some(node_id)).await?;

                        // Update partition stats
                        self.update_partition_stats(
                            &catalog_name,
                            &schema_name,
                            &table_name,
                            &partition_key,
                        );
                    }

                    // Update table stats
                    self.update_table_stats(&catalog_name, &schema_name, &table_name);
                }
            }
        }

        self.mark_dirty();
        info!("Metadata rescan for node {} complete", node_id);
        Ok(())
    }

    // ==================== Catalog Operations ====================

    async fn list_catalogs(&self) -> MetadataResult<Vec<CatalogInfo>> {
        let store = self.store.read();
        Ok(store.catalogs.values().cloned().collect())
    }

    async fn get_catalog(&self, catalog: &str) -> MetadataResult<CatalogInfo> {
        let store = self.store.read();
        store
            .catalogs
            .get(catalog)
            .cloned()
            .ok_or_else(|| MetadataError::CatalogNotFound(catalog.to_string()))
    }

    async fn register_catalog(&self, info: CatalogInfo) -> MetadataResult<()> {
        let mut store = self.store.write();
        store.catalogs.insert(info.name.clone(), info);
        drop(store);
        self.mark_dirty();
        Ok(())
    }

    async fn remove_catalog(&self, catalog: &str) -> MetadataResult<()> {
        let mut store = self.store.write();

        // Remove all files in this catalog
        let files_to_remove: Vec<String> = store
            .files
            .iter()
            .filter(|(_, f)| f.catalog == catalog)
            .map(|(p, _)| p.clone())
            .collect();

        for path in files_to_remove {
            store.files.remove(&path);
        }

        // Remove partition_files entries
        store.partition_files.retain(|(c, _, _, _), _| c != catalog);

        // Remove partitions
        store.partitions.retain(|(c, _, _, _), _| c != catalog);

        // Remove tables
        store.tables.retain(|(c, _, _), _| c != catalog);

        // Remove schemas
        store.schemas.retain(|(c, _), _| c != catalog);

        // Remove catalog
        store.catalogs.remove(catalog);

        drop(store);
        self.mark_dirty();
        Ok(())
    }

    // ==================== Schema Operations ====================

    async fn list_schemas(&self, catalog: &str) -> MetadataResult<Vec<SchemaInfo>> {
        let store = self.store.read();
        Ok(store
            .schemas
            .iter()
            .filter(|((c, _), _)| c == catalog)
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn get_schema(&self, catalog: &str, schema: &str) -> MetadataResult<SchemaInfo> {
        let store = self.store.read();
        store
            .schemas
            .get(&(catalog.to_string(), schema.to_string()))
            .cloned()
            .ok_or_else(|| MetadataError::SchemaNotFound(catalog.to_string(), schema.to_string()))
    }

    async fn register_schema(&self, info: SchemaInfo) -> MetadataResult<()> {
        self.ensure_catalog(&info.catalog);
        let mut store = self.store.write();
        store
            .schemas
            .insert((info.catalog.clone(), info.name.clone()), info);
        drop(store);
        self.mark_dirty();
        Ok(())
    }

    async fn remove_schema(&self, catalog: &str, schema: &str) -> MetadataResult<()> {
        let mut store = self.store.write();

        // Remove all files
        let files_to_remove: Vec<String> = store
            .files
            .iter()
            .filter(|(_, f)| f.catalog == catalog && f.schema == schema)
            .map(|(p, _)| p.clone())
            .collect();

        for path in files_to_remove {
            store.files.remove(&path);
        }

        // Remove partition_files entries
        store
            .partition_files
            .retain(|(c, s, _, _), _| c != catalog || s != schema);

        // Remove partitions
        store
            .partitions
            .retain(|(c, s, _, _), _| c != catalog || s != schema);

        // Remove tables
        store
            .tables
            .retain(|(c, s, _), _| c != catalog || s != schema);

        // Remove schema
        store
            .schemas
            .remove(&(catalog.to_string(), schema.to_string()));

        drop(store);
        self.mark_dirty();
        Ok(())
    }

    // ==================== Table Operations ====================

    async fn list_tables(&self, catalog: &str, schema: &str) -> MetadataResult<Vec<TableInfo>> {
        let store = self.store.read();
        Ok(store
            .tables
            .iter()
            .filter(|((c, s, _), _)| c == catalog && s == schema)
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<TableInfo> {
        let store = self.store.read();
        store
            .tables
            .get(&(catalog.to_string(), schema.to_string(), table.to_string()))
            .cloned()
            .ok_or_else(|| {
                MetadataError::TableNotFound(
                    catalog.to_string(),
                    schema.to_string(),
                    table.to_string(),
                )
            })
    }

    async fn get_table_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<TableStats> {
        let store = self.store.read();

        let table_info = store
            .tables
            .get(&(catalog.to_string(), schema.to_string(), table.to_string()))
            .ok_or_else(|| {
                MetadataError::TableNotFound(
                    catalog.to_string(),
                    schema.to_string(),
                    table.to_string(),
                )
            })?;

        // Count files by level
        let mut files_by_level: HashMap<u8, usize> = HashMap::new();
        let mut oldest_partition: Option<String> = None;
        let mut newest_partition: Option<String> = None;

        for ((c, s, t, pk), _) in store.partitions.iter() {
            if c == catalog && s == schema && t == table {
                if oldest_partition.is_none() || pk < oldest_partition.as_ref().unwrap() {
                    oldest_partition = Some(pk.clone());
                }
                if newest_partition.is_none() || pk > newest_partition.as_ref().unwrap() {
                    newest_partition = Some(pk.clone());
                }
            }
        }

        for (_, file) in store.files.iter() {
            if file.catalog == catalog && file.schema == schema && file.table == table {
                *files_by_level.entry(file.level).or_insert(0) += 1;
            }
        }

        Ok(TableStats {
            table: table.to_string(),
            schema: schema.to_string(),
            catalog: catalog.to_string(),
            total_rows: table_info.total_rows,
            total_bytes: table_info.total_bytes,
            partition_count: table_info.partition_count,
            file_count: table_info.file_count,
            files_by_level,
            oldest_partition,
            newest_partition,
            min_timestamp: table_info.min_timestamp,
            max_timestamp: table_info.max_timestamp,
        })
    }

    async fn register_table(&self, info: TableInfo) -> MetadataResult<()> {
        self.ensure_schema(&info.catalog, &info.schema);
        let mut store = self.store.write();
        store.tables.insert(
            (info.catalog.clone(), info.schema.clone(), info.name.clone()),
            info,
        );
        drop(store);
        self.mark_dirty();
        Ok(())
    }

    async fn remove_table(&self, catalog: &str, schema: &str, table: &str) -> MetadataResult<()> {
        let mut store = self.store.write();

        // Remove all files
        let files_to_remove: Vec<String> = store
            .files
            .iter()
            .filter(|(_, f)| f.catalog == catalog && f.schema == schema && f.table == table)
            .map(|(p, _)| p.clone())
            .collect();

        for path in files_to_remove {
            store.files.remove(&path);
        }

        // Remove partition_files entries
        store
            .partition_files
            .retain(|(c, s, t, _), _| c != catalog || s != schema || t != table);

        // Remove partitions
        store
            .partitions
            .retain(|(c, s, t, _), _| c != catalog || s != schema || t != table);

        // Remove table
        store
            .tables
            .remove(&(catalog.to_string(), schema.to_string(), table.to_string()));

        drop(store);
        self.mark_dirty();
        Ok(())
    }

    // ==================== Partition Operations ====================

    async fn list_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<Vec<PartitionInfo>> {
        let store = self.store.read();
        let mut partitions: Vec<PartitionInfo> = store
            .partitions
            .iter()
            .filter(|((c, s, t, _), _)| c == catalog && s == schema && t == table)
            .map(|(_, v)| v.clone())
            .collect();

        partitions.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));
        Ok(partitions)
    }

    async fn list_partitions_in_range(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> MetadataResult<Vec<PartitionInfo>> {
        let store = self.store.read();
        let mut partitions: Vec<PartitionInfo> = store
            .partitions
            .iter()
            .filter(|((c, s, t, pk), _)| {
                if c != catalog || s != schema || t != table {
                    return false;
                }
                if let Some(start) = start_date {
                    if pk.as_str() < start {
                        return false;
                    }
                }
                if let Some(end) = end_date {
                    if pk.as_str() > end {
                        return false;
                    }
                }
                true
            })
            .map(|(_, v)| v.clone())
            .collect();

        partitions.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));
        Ok(partitions)
    }

    async fn get_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> MetadataResult<PartitionInfo> {
        let store = self.store.read();
        store
            .partitions
            .get(&(
                catalog.to_string(),
                schema.to_string(),
                table.to_string(),
                partition_key.to_string(),
            ))
            .cloned()
            .ok_or_else(|| {
                MetadataError::PartitionNotFound(
                    catalog.to_string(),
                    schema.to_string(),
                    table.to_string(),
                    partition_key.to_string(),
                )
            })
    }

    async fn register_partition(&self, info: PartitionInfo) -> MetadataResult<()> {
        self.ensure_table(&info.catalog, &info.schema, &info.table);
        let mut store = self.store.write();
        let key = (
            info.catalog.clone(),
            info.schema.clone(),
            info.table.clone(),
            info.partition_key.clone(),
        );
        store.partition_files.entry(key.clone()).or_default();
        store.partitions.insert(key, info);
        drop(store);
        self.mark_dirty();
        Ok(())
    }

    async fn remove_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> MetadataResult<()> {
        let mut store = self.store.write();

        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            partition_key.to_string(),
        );

        // Remove all files in this partition
        if let Some(file_paths) = store.partition_files.remove(&key) {
            for path in file_paths {
                store.files.remove(&path);
            }
        }

        // Remove partition
        store.partitions.remove(&key);

        drop(store);
        self.mark_dirty();

        // Update table stats
        self.update_table_stats(catalog, schema, table);

        Ok(())
    }

    // ==================== File Operations ====================

    async fn list_files(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> MetadataResult<Vec<FileInfo>> {
        let store = self.store.read();
        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            partition_key.to_string(),
        );

        let file_paths = store.partition_files.get(&key);
        match file_paths {
            Some(paths) => {
                let files: Vec<FileInfo> = paths
                    .iter()
                    .filter_map(|p| store.files.get(p).cloned())
                    .collect();
                Ok(files)
            }
            None => Ok(Vec::new()),
        }
    }

    async fn list_table_files(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<Vec<FileInfo>> {
        let store = self.store.read();
        Ok(store
            .files
            .values()
            .filter(|f| f.catalog == catalog && f.schema == schema && f.table == table)
            .cloned()
            .collect())
    }

    async fn list_files_by_level(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
        level: u8,
    ) -> MetadataResult<Vec<FileInfo>> {
        let files = self
            .list_files(catalog, schema, table, partition_key)
            .await?;
        Ok(files.into_iter().filter(|f| f.level == level).collect())
    }

    async fn get_file(&self, path: &str) -> MetadataResult<FileInfo> {
        let store = self.store.read();
        store
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| MetadataError::FileNotFound(path.to_string()))
    }

    async fn register_file(&self, info: FileInfo) -> MetadataResult<()> {
        self.ensure_partition(
            &info.catalog,
            &info.schema,
            &info.table,
            &info.partition_key,
        );

        let mut store = self.store.write();

        let key = (
            info.catalog.clone(),
            info.schema.clone(),
            info.table.clone(),
            info.partition_key.clone(),
        );

        // Add to partition_files index
        store
            .partition_files
            .entry(key)
            .or_default()
            .push(info.path.clone());

        // Add to files
        store.files.insert(info.path.clone(), info.clone());

        drop(store);
        self.mark_dirty();

        // Update stats
        self.update_partition_stats(
            &info.catalog,
            &info.schema,
            &info.table,
            &info.partition_key,
        );
        self.update_table_stats(&info.catalog, &info.schema, &info.table);

        Ok(())
    }

    async fn register_files(&self, files: Vec<FileInfo>) -> MetadataResult<()> {
        for file in files {
            self.register_file(file).await?;
        }
        Ok(())
    }

    async fn remove_file(&self, path: &str) -> MetadataResult<()> {
        let mut store = self.store.write();

        let file_info = store
            .files
            .remove(path)
            .ok_or_else(|| MetadataError::FileNotFound(path.to_string()))?;

        let key = (
            file_info.catalog.clone(),
            file_info.schema.clone(),
            file_info.table.clone(),
            file_info.partition_key.clone(),
        );

        // Remove from partition_files index
        if let Some(paths) = store.partition_files.get_mut(&key) {
            paths.retain(|p| p != path);
        }

        drop(store);
        self.mark_dirty();

        // Update stats
        self.update_partition_stats(
            &file_info.catalog,
            &file_info.schema,
            &file_info.table,
            &file_info.partition_key,
        );
        self.update_table_stats(&file_info.catalog, &file_info.schema, &file_info.table);

        Ok(())
    }

    async fn remove_files(&self, paths: Vec<String>) -> MetadataResult<()> {
        for path in paths {
            // Ignore errors for individual files
            let _ = self.remove_file(&path).await;
        }
        Ok(())
    }

    // ==================== Utility Operations ====================

    async fn catalog_exists(&self, catalog: &str) -> bool {
        self.store.read().catalogs.contains_key(catalog)
    }

    async fn schema_exists(&self, catalog: &str, schema: &str) -> bool {
        self.store
            .read()
            .schemas
            .contains_key(&(catalog.to_string(), schema.to_string()))
    }

    async fn table_exists(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.store.read().tables.contains_key(&(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
        ))
    }

    async fn partition_exists(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> bool {
        self.store.read().partitions.contains_key(&(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            partition_key.to_string(),
        ))
    }

    async fn get_global_stats(&self) -> MetadataResult<GlobalStats> {
        let store = self.store.read();

        let total_rows: u64 = store.tables.values().map(|t| t.total_rows).sum();
        let total_bytes: u64 = store.tables.values().map(|t| t.total_bytes).sum();

        Ok(GlobalStats {
            catalog_count: store.catalogs.len(),
            schema_count: store.schemas.len(),
            table_count: store.tables.len(),
            partition_count: store.partitions.len(),
            file_count: store.files.len(),
            total_rows,
            total_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_memory_manager_basic() {
        let temp_dir = TempDir::new().unwrap();
        let manager = MemoryMetadataManager::new(temp_dir.path());

        // Register a file
        let file_info = FileInfo {
            path: "/data/mydb/default/cpu/date=2024-01-15/L0_test.parquet".to_string(),
            file_name: "L0_test.parquet".to_string(),
            level: 0,
            size_bytes: 1024,
            row_count: 100,
            row_group_count: 1,
            partition_key: "2024-01-15".to_string(),
            table: "cpu".to_string(),
            schema: "default".to_string(),
            catalog: "mydb".to_string(),
            created_at: chrono::Utc::now().timestamp_millis(),
            min_timestamp: None,
            max_timestamp: None,
            etag: None,
            column_stats: None,
        };

        manager.register_file(file_info.clone()).await.unwrap();

        // Verify catalog was created
        assert!(manager.catalog_exists("mydb").await);

        // Verify schema was created
        assert!(manager.schema_exists("mydb", "default").await);

        // Verify table was created
        assert!(manager.table_exists("mydb", "default", "cpu").await);

        // Verify partition was created
        assert!(
            manager
                .partition_exists("mydb", "default", "cpu", "2024-01-15")
                .await
        );

        // Verify file can be retrieved
        let retrieved = manager.get_file(&file_info.path).await.unwrap();
        assert_eq!(retrieved.size_bytes, 1024);

        // List files
        let files = manager
            .list_files("mydb", "default", "cpu", "2024-01-15")
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let manager = MemoryMetadataManager::new(temp_dir.path());

        // Register a file
        let file_info = FileInfo {
            path: "/data/mydb/default/cpu/date=2024-01-15/L0_test.parquet".to_string(),
            file_name: "L0_test.parquet".to_string(),
            level: 0,
            size_bytes: 2048,
            row_count: 200,
            row_group_count: 2,
            partition_key: "2024-01-15".to_string(),
            table: "cpu".to_string(),
            schema: "default".to_string(),
            catalog: "mydb".to_string(),
            created_at: chrono::Utc::now().timestamp_millis(),
            min_timestamp: Some(1000),
            max_timestamp: Some(2000),
            etag: None,
            column_stats: None,
        };

        manager.register_file(file_info.clone()).await.unwrap();

        // Save
        manager.save().await.unwrap();

        // Create new manager and load
        let manager2 = MemoryMetadataManager::new(temp_dir.path());
        manager2.load().await.unwrap();

        // Verify data persisted
        assert!(manager2.catalog_exists("mydb").await);
        let retrieved = manager2.get_file(&file_info.path).await.unwrap();
        assert_eq!(retrieved.size_bytes, 2048);
        assert_eq!(retrieved.min_timestamp, Some(1000));
    }
}
