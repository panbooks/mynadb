//! Iceberg Partition Spec
//!
//! Maps MynaDB's `date=YYYY-MM-DD` partitioning to Iceberg's `day(time)` transform.

use serde::{Deserialize, Serialize};

/// Iceberg Partition Spec
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

/// A single partition field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    #[serde(rename = "source-id")]
    pub source_id: i32,
    #[serde(rename = "field-id")]
    pub field_id: i32,
    pub name: String,
    pub transform: String,
}

/// Create a partition spec using day(date) transform
/// This maps MynaDB's `date=YYYY-MM-DD` partitioning
pub fn create_date_partition_spec(date_field_id: i32) -> PartitionSpec {
    PartitionSpec {
        spec_id: 0,
        fields: vec![PartitionField {
            source_id: date_field_id,
            field_id: 1000, // Partition field IDs start at 1000 per spec
            name: "date".to_string(),
            transform: "identity".to_string(), // date column is already Date32
        }],
    }
}

/// Create a partition spec using day(time) transform
pub fn create_time_day_partition_spec(time_field_id: i32) -> PartitionSpec {
    PartitionSpec {
        spec_id: 0,
        fields: vec![PartitionField {
            source_id: time_field_id,
            field_id: 1000,
            name: "time_day".to_string(),
            transform: "day".to_string(),
        }],
    }
}

/// Create an unpartitioned spec
pub fn create_unpartitioned_spec() -> PartitionSpec {
    PartitionSpec {
        spec_id: 0,
        fields: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_partition_spec() {
        let spec = create_date_partition_spec(2);
        assert_eq!(spec.spec_id, 0);
        assert_eq!(spec.fields.len(), 1);
        assert_eq!(spec.fields[0].source_id, 2);
        assert_eq!(spec.fields[0].transform, "identity");
    }

    #[test]
    fn test_serialization() {
        let spec = create_date_partition_spec(2);
        let json = serde_json::to_string_pretty(&spec).unwrap();
        assert!(json.contains("\"spec-id\""));
        assert!(json.contains("\"source-id\""));
    }
}
