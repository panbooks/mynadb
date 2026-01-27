//! MetadataManager trait definition
//!
//! Defines the interface for catalog metadata management.

use super::types::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Result type for metadata operations
pub type MetadataResult<T> = Result<T, MetadataError>;

/// Errors that can occur during metadata operations
#[derive(Debug, Clone)]
pub enum MetadataError {
    /// Catalog not found
    CatalogNotFound(String),
    /// Schema not found
    SchemaNotFound(String, String),
    /// Table not found
    TableNotFound(String, String, String),
    /// Partition not found
    PartitionNotFound(String, String, String, String),
    /// File not found
    FileNotFound(String),
    /// IO error
    IoError(String),
    /// Serialization error
    SerializationError(String),
    /// Already exists
    AlreadyExists(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataError::CatalogNotFound(name) => write!(f, "Catalog not found: {}", name),
            MetadataError::SchemaNotFound(catalog, schema) => {
                write!(f, "Schema not found: {}.{}", catalog, schema)
            }
            MetadataError::TableNotFound(catalog, schema, table) => {
                write!(f, "Table not found: {}.{}.{}", catalog, schema, table)
            }
            MetadataError::PartitionNotFound(catalog, schema, table, partition) => {
                write!(
                    f,
                    "Partition not found: {}.{}.{}/{}",
                    catalog, schema, table, partition
                )
            }
            MetadataError::FileNotFound(path) => write!(f, "File not found: {}", path),
            MetadataError::IoError(msg) => write!(f, "IO error: {}", msg),
            MetadataError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            MetadataError::AlreadyExists(msg) => write!(f, "Already exists: {}", msg),
            MetadataError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for MetadataError {}

/// Trait for metadata management operations
///
/// Implementations must be thread-safe (Send + Sync).
#[async_trait]
pub trait MetadataManager: Send + Sync {
    // ==================== Lifecycle ====================

    /// Load metadata from persistent storage
    async fn load(&self) -> MetadataResult<()>;

    /// Save metadata to persistent storage
    async fn save(&self) -> MetadataResult<()>;

    /// Perform a full rescan of the storage to rebuild metadata
    async fn rescan(&self, data_dir: &str) -> MetadataResult<()>;

    /// Perform a full rescan of the storage for a specific node
    /// Scans the path: {data_dir}/{node_id}/{catalog}/{schema}/{table}/date=YYYY-MM-DD/*.parquet
    /// This is used in cluster mode where files are prefixed with node_id
    async fn rescan_node(&self, data_dir: &str, node_id: &str) -> MetadataResult<()>;

    // ==================== Catalog Operations ====================

    /// List all catalogs (databases)
    async fn list_catalogs(&self) -> MetadataResult<Vec<CatalogInfo>>;

    /// Get catalog info by name
    async fn get_catalog(&self, catalog: &str) -> MetadataResult<CatalogInfo>;

    /// Register or update a catalog
    async fn register_catalog(&self, info: CatalogInfo) -> MetadataResult<()>;

    /// Remove a catalog and all its contents
    async fn remove_catalog(&self, catalog: &str) -> MetadataResult<()>;

    // ==================== Schema Operations ====================

    /// List all schemas in a catalog
    async fn list_schemas(&self, catalog: &str) -> MetadataResult<Vec<SchemaInfo>>;

    /// Get schema info
    async fn get_schema(&self, catalog: &str, schema: &str) -> MetadataResult<SchemaInfo>;

    /// Register or update a schema
    async fn register_schema(&self, info: SchemaInfo) -> MetadataResult<()>;

    /// Remove a schema and all its contents
    async fn remove_schema(&self, catalog: &str, schema: &str) -> MetadataResult<()>;

    // ==================== Table Operations ====================

    /// List all tables in a schema
    async fn list_tables(&self, catalog: &str, schema: &str) -> MetadataResult<Vec<TableInfo>>;

    /// Get table info
    async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<TableInfo>;

    /// Get table statistics
    async fn get_table_stats(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<TableStats>;

    /// Register or update a table
    async fn register_table(&self, info: TableInfo) -> MetadataResult<()>;

    /// Remove a table and all its contents
    async fn remove_table(&self, catalog: &str, schema: &str, table: &str) -> MetadataResult<()>;

    // ==================== Partition Operations ====================

    /// List all partitions in a table
    async fn list_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<Vec<PartitionInfo>>;

    /// List partitions in a time range
    async fn list_partitions_in_range(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> MetadataResult<Vec<PartitionInfo>>;

    /// Get partition info
    async fn get_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> MetadataResult<PartitionInfo>;

    /// Register or update a partition
    async fn register_partition(&self, info: PartitionInfo) -> MetadataResult<()>;

    /// Remove a partition and all its files
    async fn remove_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> MetadataResult<()>;

    // ==================== File Operations ====================

    /// List all files in a partition
    async fn list_files(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> MetadataResult<Vec<FileInfo>>;

    /// List all files in a table (across all partitions)
    async fn list_table_files(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetadataResult<Vec<FileInfo>>;

    /// List files by compaction level
    async fn list_files_by_level(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
        level: u8,
    ) -> MetadataResult<Vec<FileInfo>>;

    /// Get file info by path
    async fn get_file(&self, path: &str) -> MetadataResult<FileInfo>;

    /// Register a new file
    async fn register_file(&self, info: FileInfo) -> MetadataResult<()>;

    /// Register multiple files atomically
    async fn register_files(&self, files: Vec<FileInfo>) -> MetadataResult<()>;

    /// Remove a file by path
    async fn remove_file(&self, path: &str) -> MetadataResult<()>;

    /// Remove multiple files atomically
    async fn remove_files(&self, paths: Vec<String>) -> MetadataResult<()>;

    // ==================== Utility Operations ====================

    /// Check if a catalog exists
    async fn catalog_exists(&self, catalog: &str) -> bool;

    /// Check if a schema exists
    async fn schema_exists(&self, catalog: &str, schema: &str) -> bool;

    /// Check if a table exists
    async fn table_exists(&self, catalog: &str, schema: &str, table: &str) -> bool;

    /// Check if a partition exists
    async fn partition_exists(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> bool;

    /// Get total statistics
    async fn get_global_stats(&self) -> MetadataResult<GlobalStats>;
}

/// Global statistics across all catalogs
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalStats {
    pub catalog_count: usize,
    pub schema_count: usize,
    pub table_count: usize,
    pub partition_count: usize,
    pub file_count: usize,
    pub total_rows: u64,
    pub total_bytes: u64,
}

/// Type alias for a shared MetadataManager
pub type SharedMetadataManager = Arc<dyn MetadataManager>;
