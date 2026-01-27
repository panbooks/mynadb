//! Batch Converter - Convert Points to RecordBatch
//!
//! This module provides the conversion logic from parsed Points to Arrow RecordBatch.
//! Used by both the standalone cache and the Router for cluster mode.

use super::line_protocol::{format_date32_as_string, FieldValue, Point};
use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Field type enumeration for schema generation
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub enum FieldType {
    Float,
    Integer,
    String,
    Boolean,
}

impl From<&FieldValue> for FieldType {
    fn from(v: &FieldValue) -> Self {
        match v {
            FieldValue::Float(_) => FieldType::Float,
            FieldValue::Integer(_) => FieldType::Integer,
            FieldValue::String(_) => FieldType::String,
            FieldValue::Boolean(_) => FieldType::Boolean,
        }
    }
}

/// Schema cache key includes tag keys and field keys with their types
#[derive(Hash, Eq, PartialEq, Clone)]
struct SchemaCacheKey {
    tag_keys: Vec<String>,
    field_keys: Vec<(String, FieldType)>,
}

/// Global schema cache to avoid rebuilding schemas for the same tag/field set
static SCHEMA_CACHE: std::sync::LazyLock<RwLock<HashMap<SchemaCacheKey, Arc<Schema>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::with_capacity(1024)));

/// Get or create schema from cache based on tag keys and field keys with types
fn get_or_create_schema(tag_keys: &[String], field_keys: &[(String, FieldType)]) -> Arc<Schema> {
    let cache_key = SchemaCacheKey {
        tag_keys: tag_keys.to_vec(),
        field_keys: field_keys.to_vec(),
    };

    // Try read lock first (fast path)
    {
        let cache = SCHEMA_CACHE.read();
        if let Some(schema) = cache.get(&cache_key) {
            return schema.clone();
        }
    }

    // Need to create schema - take write lock
    let mut cache = SCHEMA_CACHE.write();

    // Double-check after acquiring write lock
    if let Some(schema) = cache.get(&cache_key) {
        return schema.clone();
    }

    // Build schema: time + measurement + date + tags + fields
    let mut fields = Vec::with_capacity(3 + tag_keys.len() + field_keys.len());

    // Time column (InfluxQL compatible)
    fields.push(Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ));

    // Measurement
    fields.push(Field::new("measurement", DataType::Utf8, false));

    // Date partition column (YYYY-MM-DD string for Hive partitioning compatibility)
    fields.push(Field::new("date", DataType::Utf8, false));

    // Tags (all strings, nullable)
    for key in tag_keys {
        fields.push(Field::new(key, DataType::Utf8, true));
    }

    // Fields (typed, nullable)
    for (key, field_type) in field_keys {
        let data_type = match field_type {
            FieldType::Float => DataType::Float64,
            FieldType::Integer => DataType::Int64,
            FieldType::String => DataType::Utf8,
            FieldType::Boolean => DataType::Boolean,
        };
        fields.push(Field::new(key, data_type, true));
    }

    let schema = Arc::new(Schema::new(fields));
    cache.insert(cache_key, schema.clone());
    schema
}

/// Convert Points to RecordBatch with schema caching and pre-sized vectors
///
/// This is the primary conversion function used by both:
/// - Standalone mode: DataCache.add_points() converts Points internally
/// - Cluster mode: Router converts Points before sending to Data nodes
pub fn points_to_record_batch(points: &[Point]) -> Result<RecordBatch, String> {
    if points.is_empty() {
        return Err("No points to convert".to_string());
    }

    let num_points = points.len();
    let first_point = &points[0];

    // Get sorted tag keys
    let mut tag_keys: Vec<_> = first_point.tags.keys().cloned().collect();
    tag_keys.sort();

    // Get sorted field keys with their types
    let mut field_keys: Vec<(String, FieldType)> = first_point
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), FieldType::from(v)))
        .collect();
    field_keys.sort_by(|a, b| a.0.cmp(&b.0));

    // Get or create cached schema
    let schema = get_or_create_schema(&tag_keys, &field_keys);

    // Pre-size arrays vector: time + measurement + date + tags + fields
    let total_columns = 3 + tag_keys.len() + field_keys.len();
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(total_columns);

    // Timestamp array (pre-sized)
    let mut timestamps = Vec::with_capacity(num_points);
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    for p in points {
        timestamps.push(p.timestamp.unwrap_or(now));
    }
    arrays.push(Arc::new(TimestampNanosecondArray::from(timestamps)));

    // Measurement array (pre-sized)
    let mut measurements = Vec::with_capacity(num_points);
    for p in points {
        measurements.push(p.measurement.as_str());
    }
    arrays.push(Arc::new(StringArray::from(measurements)));

    // Date partition column (YYYY-MM-DD format)
    let mut dates = Vec::with_capacity(num_points);
    for p in points {
        dates.push(format_date32_as_string(p.date));
    }
    arrays.push(Arc::new(StringArray::from(dates)));

    // Tag arrays (pre-sized)
    for key in &tag_keys {
        let mut values = Vec::with_capacity(num_points);
        for p in points {
            values.push(p.tags.get(key).map(|s| s.as_str()));
        }
        arrays.push(Arc::new(StringArray::from(values)));
    }

    // Field arrays (typed, pre-sized)
    for (key, field_type) in &field_keys {
        match field_type {
            FieldType::Float => {
                let mut values: Vec<Option<f64>> = Vec::with_capacity(num_points);
                for p in points {
                    let v = p.fields.get(key).and_then(|fv| match fv {
                        FieldValue::Float(f) => Some(*f),
                        FieldValue::Integer(i) => Some(*i as f64),
                        _ => None,
                    });
                    values.push(v);
                }
                arrays.push(Arc::new(Float64Array::from(values)));
            }
            FieldType::Integer => {
                let mut values: Vec<Option<i64>> = Vec::with_capacity(num_points);
                for p in points {
                    let v = p.fields.get(key).and_then(|fv| match fv {
                        FieldValue::Integer(i) => Some(*i),
                        FieldValue::Float(f) => Some(*f as i64),
                        _ => None,
                    });
                    values.push(v);
                }
                arrays.push(Arc::new(Int64Array::from(values)));
            }
            FieldType::String => {
                let mut values: Vec<Option<&str>> = Vec::with_capacity(num_points);
                for p in points {
                    let v = p.fields.get(key).and_then(|fv| match fv {
                        FieldValue::String(s) => Some(s.as_str()),
                        _ => None,
                    });
                    values.push(v);
                }
                arrays.push(Arc::new(StringArray::from(values)));
            }
            FieldType::Boolean => {
                let mut values: Vec<Option<bool>> = Vec::with_capacity(num_points);
                for p in points {
                    let v = p.fields.get(key).and_then(|fv| match fv {
                        FieldValue::Boolean(b) => Some(*b),
                        _ => None,
                    });
                    values.push(v);
                }
                arrays.push(Arc::new(BooleanArray::from(values)));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| format!("Failed to create RecordBatch: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_points_to_record_batch() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server01".to_string());

        let mut fields = HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(45.5));
        fields.insert("mem".to_string(), FieldValue::Integer(8192));

        let point = Point {
            measurement: "metrics".to_string(),
            tags,
            fields,
            timestamp: Some(1704067200000000000),
            date: 19723, // 2024-01-01
            service: "default".to_string(),
        };

        let batch = points_to_record_batch(&[point]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // time + measurement + date + 1 tag + 2 fields = 6 columns
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn test_empty_points() {
        let result = points_to_record_batch(&[]);
        assert!(result.is_err());
    }
}
