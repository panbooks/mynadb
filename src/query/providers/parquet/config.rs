//! Configuration types for ParquetTableProvider

use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration for the Parquet table provider
///
/// All tables use Hive-style nested partitions:
///   - First level: date=YYYY-MM-DD (time-based partitioning)
///   - Second level: node_id={node_id} (writer identification for distributed queries)
///
/// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=XXX/file.parquet
#[derive(Debug, Clone)]
pub struct ParquetTableConfig {
    /// Enable filter pushdown to Parquet reader
    pub filter_pushdown: bool,
    /// Enable projection pushdown for column pruning
    pub projection_pushdown: bool,
    /// Table name for display purposes
    pub table_name: String,
    /// Batch size for reading
    pub batch_size: usize,
    /// First partition column name (default: "date")
    pub partition_column: String,
    /// Second partition column name (default: "node_id")
    /// Used for distributed query routing - each Data Node filters by its node_id
    pub node_id_column: String,
}

impl Default for ParquetTableConfig {
    fn default() -> Self {
        Self {
            filter_pushdown: true,
            projection_pushdown: true,
            table_name: "parquet_table".to_string(),
            batch_size: 8192,
            partition_column: "date".to_string(),
            node_id_column: "node_id".to_string(),
        }
    }
}

impl ParquetTableConfig {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            ..Default::default()
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// Information about a Parquet file to read
#[derive(Debug, Clone)]
pub struct ParquetFileInfo {
    /// Full path to the Parquet file (object store path, not filesystem path)
    pub path: String,
    /// Partition values (e.g., {"date": "2024-01-15", "node_id": "data-1"})
    pub partition_values: HashMap<String, String>,
    /// File size in bytes (for statistics)
    pub file_size: u64,
}

impl ParquetFileInfo {
    /// Get path as PathBuf for local file operations
    pub fn as_path(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }
}
