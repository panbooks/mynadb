//! Iceberg Snapshot Management
//!
//! Each sync cycle creates a new snapshot capturing the current state
//! of all data files in a MynaDB table.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Iceberg Snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub operation: String,
}

impl Snapshot {
    /// Create a new snapshot for an append operation
    pub fn new_append(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        manifest_list_path: String,
        added_files: usize,
        added_rows: i64,
        total_files: usize,
        total_rows: i64,
        total_size_bytes: i64,
    ) -> Self {
        let mut summary = HashMap::new();
        summary.insert("operation".to_string(), "append".to_string());
        summary.insert(
            "added-data-files".to_string(),
            added_files.to_string(),
        );
        summary.insert("added-records".to_string(), added_rows.to_string());
        summary.insert(
            "total-data-files".to_string(),
            total_files.to_string(),
        );
        summary.insert("total-records".to_string(), total_rows.to_string());
        summary.insert(
            "total-files-size".to_string(),
            total_size_bytes.to_string(),
        );

        Self {
            snapshot_id,
            parent_snapshot_id,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            manifest_list: manifest_list_path,
            summary,
            schema_id: 0,
            operation: "append".to_string(),
        }
    }

    /// Create a new snapshot for an overwrite (compaction) operation
    pub fn new_overwrite(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        manifest_list_path: String,
        total_files: usize,
        total_rows: i64,
        total_size_bytes: i64,
    ) -> Self {
        let mut summary = HashMap::new();
        summary.insert("operation".to_string(), "overwrite".to_string());
        summary.insert(
            "total-data-files".to_string(),
            total_files.to_string(),
        );
        summary.insert("total-records".to_string(), total_rows.to_string());
        summary.insert(
            "total-files-size".to_string(),
            total_size_bytes.to_string(),
        );

        Self {
            snapshot_id,
            parent_snapshot_id,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            manifest_list: manifest_list_path,
            summary,
            schema_id: 0,
            operation: "overwrite".to_string(),
        }
    }
}

/// Snapshot log entry for the table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

/// Generate a unique snapshot ID based on timestamp
pub fn generate_snapshot_id() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_creation() {
        let snap = Snapshot::new_append(
            12345,
            None,
            "s3://bucket/metadata/snap-12345-manifest-list.avro".to_string(),
            10,
            1000,
            10,
            1000,
            1024 * 1024,
        );

        assert_eq!(snap.snapshot_id, 12345);
        assert!(snap.parent_snapshot_id.is_none());
        assert_eq!(snap.summary.get("operation").unwrap(), "append");
        assert_eq!(snap.summary.get("added-data-files").unwrap(), "10");
    }
}
