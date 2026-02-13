//! Iceberg Manifest List
//!
//! A manifest list tracks all manifest files that belong to a snapshot.
//! Each entry points to a manifest file and includes summary statistics.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// An entry in a manifest list pointing to a manifest file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestListEntry {
    /// Path of the manifest file
    pub manifest_path: String,
    /// Length of the manifest file in bytes
    pub manifest_length: i64,
    /// Partition spec ID used by the manifest
    pub partition_spec_id: i32,
    /// Content type: 0 = data, 1 = deletes
    pub content: i32,
    /// Sequence number (for ordering)
    pub sequence_number: i64,
    /// Minimum sequence number of entries
    pub min_sequence_number: i64,
    /// Snapshot ID that added this manifest
    pub added_snapshot_id: i64,
    /// Number of entries with status ADDED
    pub added_data_files_count: i32,
    /// Number of entries with status EXISTING
    pub existing_data_files_count: i32,
    /// Number of entries with status DELETED
    pub deleted_data_files_count: i32,
    /// Total number of rows in added files
    pub added_rows_count: i64,
    /// Total number of rows in existing files
    pub existing_rows_count: i64,
    /// Total number of rows in deleted files
    pub deleted_rows_count: i64,
}

/// A manifest list file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestList {
    pub entries: Vec<ManifestListEntry>,
}

impl ManifestList {
    /// Create a new empty manifest list
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a manifest entry to the list
    pub fn add_manifest(
        &mut self,
        manifest_path: String,
        manifest_length: i64,
        snapshot_id: i64,
        added_files: i32,
        existing_files: i32,
        added_rows: i64,
        existing_rows: i64,
        sequence_number: i64,
    ) {
        self.entries.push(ManifestListEntry {
            manifest_path,
            manifest_length,
            partition_spec_id: 0,
            content: 0, // data
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id: snapshot_id,
            added_data_files_count: added_files,
            existing_data_files_count: existing_files,
            deleted_data_files_count: 0,
            added_rows_count: added_rows,
            existing_rows_count: existing_rows,
            deleted_rows_count: 0,
        });
    }

    /// Write this manifest list as a JSON file
    /// A production implementation would use Avro encoding per Iceberg spec
    pub fn write_json(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create manifest list dir: {}", e))?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize manifest list: {}", e))?;
        std::fs::write(path, json)
            .map_err(|e| format!("Failed to write manifest list: {}", e))
    }

    /// Total number of data files across all manifests
    pub fn total_data_files(&self) -> i32 {
        self.entries
            .iter()
            .map(|e| e.added_data_files_count + e.existing_data_files_count)
            .sum()
    }
}

impl Default for ManifestList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_list() {
        let mut list = ManifestList::new();
        list.add_manifest(
            "metadata/manifest-1.json".to_string(),
            4096,
            12345,
            10,
            0,
            1000,
            0,
            1,
        );

        assert_eq!(list.entries.len(), 1);
        assert_eq!(list.total_data_files(), 10);
    }
}
