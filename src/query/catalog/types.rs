//! Catalog metadata types
//!
//! Data structures for representing database catalog metadata.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Information about a catalog (database)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogInfo {
    /// Catalog name (e.g., "mydb")
    pub name: String,
    /// Creation timestamp (Unix millis)
    pub created_at: i64,
    /// Last update timestamp (Unix millis)
    pub updated_at: i64,
    /// Total number of schemas
    pub schema_count: usize,
    /// Total number of tables across all schemas
    pub table_count: usize,
    /// Total size in bytes
    pub total_bytes: u64,
}

/// Information about a schema (service/namespace)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Schema name (e.g., "default", "service_1")
    pub name: String,
    /// Parent catalog name
    pub catalog: String,
    /// Creation timestamp (Unix millis)
    pub created_at: i64,
    /// Last update timestamp (Unix millis)
    pub updated_at: i64,
    /// Number of tables in this schema
    pub table_count: usize,
    /// Total size in bytes
    pub total_bytes: u64,
}

/// Information about a table (measurement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name (e.g., "cpu", "memory")
    pub name: String,
    /// Parent schema name
    pub schema: String,
    /// Parent catalog name
    pub catalog: String,
    /// Creation timestamp (Unix millis)
    pub created_at: i64,
    /// Last update timestamp (Unix millis)
    pub updated_at: i64,
    /// Total row count
    pub total_rows: u64,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Number of partitions
    pub partition_count: usize,
    /// Number of files
    pub file_count: usize,
    /// Minimum timestamp in data (nanos)
    pub min_timestamp: Option<i64>,
    /// Maximum timestamp in data (nanos)
    pub max_timestamp: Option<i64>,
    /// Column names and types (for schema inference)
    pub columns: Option<Vec<ColumnInfo>>,
}

/// Information about a column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column type (e.g., "Int64", "Float64", "Utf8")
    pub data_type: String,
    /// Whether the column is nullable
    pub nullable: bool,
}

/// Information about a partition (date=YYYY-MM-DD)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Partition key (e.g., "2024-01-15")
    pub partition_key: String,
    /// Partition value as days since epoch (for sorting)
    pub partition_value: i32,
    /// Parent table name
    pub table: String,
    /// Parent schema name
    pub schema: String,
    /// Parent catalog name
    pub catalog: String,
    /// Creation timestamp (Unix millis)
    pub created_at: i64,
    /// Last update timestamp (Unix millis)
    pub updated_at: i64,
    /// Total row count
    pub total_rows: u64,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Number of files
    pub file_count: usize,
    /// Minimum timestamp in data (nanos)
    pub min_timestamp: Option<i64>,
    /// Maximum timestamp in data (nanos)
    pub max_timestamp: Option<i64>,
}

/// Information about a Parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// Full path to the file
    pub path: String,
    /// File name only
    pub file_name: String,
    /// Compaction level (0=L0, 1=L1, etc.)
    pub level: u8,
    /// File size in bytes
    pub size_bytes: u64,
    /// Row count
    pub row_count: u64,
    /// Row group count
    pub row_group_count: usize,
    /// Parent partition key
    pub partition_key: String,
    /// Parent table name
    pub table: String,
    /// Parent schema name
    pub schema: String,
    /// Parent catalog name
    pub catalog: String,
    /// Creation timestamp (Unix millis)
    pub created_at: i64,
    /// Minimum timestamp in data (nanos)
    pub min_timestamp: Option<i64>,
    /// Maximum timestamp in data (nanos)
    pub max_timestamp: Option<i64>,
    /// ETag for cache invalidation (Object Store)
    pub etag: Option<String>,
    /// Column statistics for advanced pruning
    pub column_stats: Option<HashMap<String, ColumnStats>>,
}

/// Statistics for a column within a file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value (as string for portability)
    pub min: Option<String>,
    /// Maximum value (as string for portability)
    pub max: Option<String>,
    /// Null count
    pub null_count: u64,
    /// Distinct count (approximate)
    pub distinct_count: Option<u64>,
}

/// Table statistics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    /// Table name
    pub table: String,
    /// Schema name
    pub schema: String,
    /// Catalog name
    pub catalog: String,
    /// Total row count
    pub total_rows: u64,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Number of partitions
    pub partition_count: usize,
    /// Number of files
    pub file_count: usize,
    /// Files by level
    pub files_by_level: HashMap<u8, usize>,
    /// Oldest partition
    pub oldest_partition: Option<String>,
    /// Newest partition
    pub newest_partition: Option<String>,
    /// Minimum timestamp
    pub min_timestamp: Option<i64>,
    /// Maximum timestamp
    pub max_timestamp: Option<i64>,
}

/// Full catalog snapshot for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    /// Version of the snapshot format
    pub version: u32,
    /// Snapshot timestamp (Unix millis)
    pub snapshot_at: i64,
    /// All catalogs
    pub catalogs: Vec<CatalogInfo>,
    /// All schemas
    pub schemas: Vec<SchemaInfo>,
    /// All tables
    pub tables: Vec<TableInfo>,
    /// All partitions
    pub partitions: Vec<PartitionInfo>,
    /// All files
    pub files: Vec<FileInfo>,
}

impl CatalogSnapshot {
    /// Current snapshot format version
    pub const CURRENT_VERSION: u32 = 1;

    /// Create a new empty snapshot
    pub fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            snapshot_at: chrono::Utc::now().timestamp_millis(),
            catalogs: Vec::new(),
            schemas: Vec::new(),
            tables: Vec::new(),
            partitions: Vec::new(),
            files: Vec::new(),
        }
    }
}

impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl FileInfo {
    /// Extract compaction level from file name (e.g., "L0_xxx.parquet" -> 0)
    pub fn parse_level(file_name: &str) -> u8 {
        if file_name.starts_with("L") {
            if let Some(level_char) = file_name.chars().nth(1) {
                if let Some(level) = level_char.to_digit(10) {
                    return level as u8;
                }
            }
        }
        0 // Default to L0
    }
}
