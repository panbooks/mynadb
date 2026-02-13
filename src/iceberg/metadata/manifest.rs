//! Iceberg Manifest Files
//!
//! A manifest file tracks a set of data files (Parquet files) along with
//! their partition values and column-level statistics. Written as Avro.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Content type of tracked files
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ManifestContent {
    Data = 0,
    Deletes = 1,
}

/// Status of a data file entry in a manifest
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FileStatus {
    Added = 1,
    Existing = 0,
    Deleted = 2,
}

/// A data file entry within a manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Content type: 0 = data, 1 = position deletes, 2 = equality deletes
    pub content: i32,
    /// Full URI of the data file
    pub file_path: String,
    /// File format (always "PARQUET" for MynaDB)
    pub file_format: String,
    /// Partition values as a map
    pub partition: HashMap<String, String>,
    /// Number of records in the file
    pub record_count: i64,
    /// File size in bytes
    pub file_size_in_bytes: i64,
    /// Column sizes by field ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Value counts by field ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_counts: Option<HashMap<i32, i64>>,
    /// Null value counts by field ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// Lower bounds by field ID (as bytes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Upper bounds by field ID (as bytes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}

/// A manifest file tracking data files for a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    /// Path of the manifest file itself
    pub manifest_path: String,
    /// Entries in this manifest
    pub entries: Vec<ManifestEntry>,
    /// Snapshot ID this manifest belongs to
    pub snapshot_id: i64,
    /// Schema ID used
    pub schema_id: i32,
    /// Partition spec ID
    pub partition_spec_id: i32,
    /// Content type (data or deletes)
    pub content: ManifestContent,
}

/// An entry in a manifest file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Status: 0 = existing, 1 = added, 2 = deleted
    pub status: i32,
    /// Snapshot ID when this entry was added
    pub snapshot_id: i64,
    /// The data file information
    pub data_file: DataFileEntry,
}

impl ManifestFile {
    /// Create a new manifest file
    pub fn new(manifest_path: String, snapshot_id: i64) -> Self {
        Self {
            manifest_path,
            entries: Vec::new(),
            snapshot_id,
            schema_id: 0,
            partition_spec_id: 0,
            content: ManifestContent::Data,
        }
    }

    /// Add a data file to this manifest
    pub fn add_data_file(
        &mut self,
        file_path: String,
        partition_date: &str,
        record_count: i64,
        file_size_bytes: i64,
    ) {
        let mut partition = HashMap::new();
        partition.insert("date".to_string(), partition_date.to_string());

        self.entries.push(ManifestEntry {
            status: FileStatus::Added as i32,
            snapshot_id: self.snapshot_id,
            data_file: DataFileEntry {
                content: 0, // data
                file_path,
                file_format: "PARQUET".to_string(),
                partition,
                record_count,
                file_size_in_bytes: file_size_bytes,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                lower_bounds: None,
                upper_bounds: None,
            },
        });
    }

    /// Add an existing data file (not new in this snapshot)
    pub fn add_existing_file(
        &mut self,
        file_path: String,
        partition_date: &str,
        record_count: i64,
        file_size_bytes: i64,
        added_snapshot_id: i64,
    ) {
        let mut partition = HashMap::new();
        partition.insert("date".to_string(), partition_date.to_string());

        self.entries.push(ManifestEntry {
            status: FileStatus::Existing as i32,
            snapshot_id: added_snapshot_id,
            data_file: DataFileEntry {
                content: 0,
                file_path,
                file_format: "PARQUET".to_string(),
                partition,
                record_count,
                file_size_in_bytes: file_size_bytes,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                lower_bounds: None,
                upper_bounds: None,
            },
        });
    }

    /// Write this manifest as a JSON file (simplified format)
    /// A production implementation would use Avro encoding per Iceberg spec
    pub fn write_json(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create manifest dir: {}", e))?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize manifest: {}", e))?;
        std::fs::write(path, json)
            .map_err(|e| format!("Failed to write manifest: {}", e))
    }

    /// Total record count across all entries
    pub fn total_records(&self) -> i64 {
        self.entries
            .iter()
            .filter(|e| e.status != FileStatus::Deleted as i32)
            .map(|e| e.data_file.record_count)
            .sum()
    }

    /// Total file size across all entries
    pub fn total_size_bytes(&self) -> i64 {
        self.entries
            .iter()
            .filter(|e| e.status != FileStatus::Deleted as i32)
            .map(|e| e.data_file.file_size_in_bytes)
            .sum()
    }

    /// Number of active data files
    pub fn active_file_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.status != FileStatus::Deleted as i32)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_creation() {
        let mut manifest = ManifestFile::new(
            "metadata/manifest-1.json".to_string(),
            12345,
        );

        manifest.add_data_file(
            "store/mydb/default/cpu/date=2024-01-15/L0_abc.parquet".to_string(),
            "2024-01-15",
            1000,
            1024 * 1024,
        );
        manifest.add_data_file(
            "store/mydb/default/cpu/date=2024-01-16/L0_def.parquet".to_string(),
            "2024-01-16",
            2000,
            2 * 1024 * 1024,
        );

        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(manifest.total_records(), 3000);
        assert_eq!(manifest.active_file_count(), 2);
    }
}
