//! Iceberg v2 Table Metadata
//!
//! The top-level metadata file for an Iceberg table. Contains:
//! - Schema history
//! - Partition spec history
//! - Snapshot list and current snapshot
//! - Sort orders
//! - Table properties

use super::{partition_spec::PartitionSpec, schema::IcebergSchema, snapshot::{Snapshot, SnapshotLogEntry}};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Iceberg v2 Table Metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(rename = "format-version")]
    pub format_version: i32,
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    pub location: String,
    #[serde(rename = "last-sequence-number")]
    pub last_sequence_number: i64,
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    pub schemas: Vec<IcebergSchema>,
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<PartitionSpec>,
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    pub properties: HashMap<String, String>,
    #[serde(rename = "current-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<Snapshot>>,
    #[serde(rename = "snapshot-log", skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLogEntry>>,
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<SortOrder>,
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
}

/// Sort order specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

/// A field in a sort order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    pub transform: String,
    #[serde(rename = "source-id")]
    pub source_id: i32,
    pub direction: String,
    #[serde(rename = "null-order")]
    pub null_order: String,
}

impl TableMetadata {
    /// Create initial metadata for a new table
    pub fn new(
        table_uuid: String,
        location: String,
        schema: IcebergSchema,
        partition_spec: PartitionSpec,
    ) -> Self {
        let last_column_id = schema.fields.iter().map(|f| f.id).max().unwrap_or(0);
        let last_partition_id = partition_spec
            .fields
            .iter()
            .map(|f| f.field_id)
            .max()
            .unwrap_or(999);

        let mut properties = HashMap::new();
        properties.insert("write.format.default".to_string(), "parquet".to_string());
        properties.insert("created-by".to_string(), "mynadb".to_string());

        Self {
            format_version: 2,
            table_uuid,
            location,
            last_sequence_number: 0,
            last_updated_ms: chrono::Utc::now().timestamp_millis(),
            last_column_id,
            schemas: vec![schema],
            current_schema_id: 0,
            partition_specs: vec![partition_spec],
            default_spec_id: 0,
            last_partition_id,
            properties,
            current_snapshot_id: None,
            snapshots: None,
            snapshot_log: None,
            sort_orders: vec![SortOrder {
                order_id: 0,
                fields: vec![],
            }],
            default_sort_order_id: 0,
        }
    }

    /// Add a new snapshot to this table
    pub fn add_snapshot(&mut self, snapshot: Snapshot) {
        let snapshot_id = snapshot.snapshot_id;
        let timestamp_ms = snapshot.timestamp_ms;

        self.last_sequence_number += 1;
        self.last_updated_ms = timestamp_ms;
        self.current_snapshot_id = Some(snapshot_id);

        // Add to snapshots list
        let snapshots = self.snapshots.get_or_insert_with(Vec::new);
        snapshots.push(snapshot);

        // Add to snapshot log
        let log = self.snapshot_log.get_or_insert_with(Vec::new);
        log.push(SnapshotLogEntry {
            timestamp_ms,
            snapshot_id,
        });
    }

    /// Write metadata to a JSON file
    pub fn write_json(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create metadata dir: {}", e))?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize table metadata: {}", e))?;
        std::fs::write(path, json)
            .map_err(|e| format!("Failed to write table metadata: {}", e))
    }

    /// Load metadata from a JSON file
    pub fn load_json(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read table metadata: {}", e))?;
        serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse table metadata: {}", e))
    }

    /// Get the current schema
    pub fn current_schema(&self) -> Option<&IcebergSchema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
    }

    /// Get the current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let current_id = self.current_snapshot_id?;
        self.snapshots
            .as_ref()?
            .iter()
            .find(|s| s.snapshot_id == current_id)
    }

    /// Number of snapshots
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.as_ref().map_or(0, |s| s.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::metadata::schema::{IcebergField, IcebergType};

    fn test_schema() -> IcebergSchema {
        IcebergSchema {
            schema_type: "struct".to_string(),
            schema_id: 0,
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "time".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive("timestamptz".to_string()),
                    doc: None,
                },
                IcebergField {
                    id: 2,
                    name: "date".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive("date".to_string()),
                    doc: None,
                },
                IcebergField {
                    id: 3,
                    name: "value".to_string(),
                    required: false,
                    field_type: IcebergType::Primitive("double".to_string()),
                    doc: None,
                },
            ],
            identifier_field_ids: None,
        }
    }

    #[test]
    fn test_table_metadata_creation() {
        let schema = test_schema();
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![],
        };

        let metadata = TableMetadata::new(
            uuid::Uuid::new_v4().to_string(),
            "s3://bucket/mydb/default/cpu".to_string(),
            schema,
            spec,
        );

        assert_eq!(metadata.format_version, 2);
        assert_eq!(metadata.current_schema_id, 0);
        assert!(metadata.current_snapshot_id.is_none());
        assert_eq!(metadata.last_column_id, 3);
    }

    #[test]
    fn test_add_snapshot() {
        let schema = test_schema();
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![],
        };

        let mut metadata = TableMetadata::new(
            uuid::Uuid::new_v4().to_string(),
            "local://data".to_string(),
            schema,
            spec,
        );

        let snapshot = Snapshot::new_append(
            1,
            None,
            "metadata/manifest-list-1.json".to_string(),
            5,
            1000,
            5,
            1000,
            1024 * 1024,
        );

        metadata.add_snapshot(snapshot);

        assert_eq!(metadata.current_snapshot_id, Some(1));
        assert_eq!(metadata.snapshot_count(), 1);
        assert_eq!(metadata.last_sequence_number, 1);
    }

    #[test]
    fn test_metadata_serialization() {
        let schema = test_schema();
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![],
        };

        let metadata = TableMetadata::new(
            "test-uuid".to_string(),
            "local://data".to_string(),
            schema,
            spec,
        );

        let json = serde_json::to_string_pretty(&metadata).unwrap();
        assert!(json.contains("\"format-version\": 2"));
        assert!(json.contains("\"table-uuid\""));

        // Round-trip
        let parsed: TableMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.format_version, 2);
        assert_eq!(parsed.table_uuid, "test-uuid");
    }
}
