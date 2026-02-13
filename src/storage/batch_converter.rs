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

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    // Merge points with the same key (timestamp, measurement, tags).
    // InfluxDB semantics: same series + timestamp = same point, last write wins.
    // Telegraf commonly sends different field subsets for the same measurement
    // at the same timestamp (e.g., swap: {free,total,used,used_percent} and {in,out}).
    // We merge them into a single point with all fields combined.
    let mut merged_points: Vec<MergedPoint> = Vec::with_capacity(points.len());
    let mut key_index: HashMap<PointKey, usize> = HashMap::with_capacity(points.len());

    for p in points {
        let ts = p.timestamp.unwrap_or(now);
        let key = PointKey {
            timestamp: ts,
            measurement: &p.measurement,
            tags: &p.tags,
        };

        if let Some(&idx) = key_index.get(&key) {
            // Same key — merge fields (last write wins)
            for (k, v) in &p.fields {
                merged_points[idx].fields.insert(k.clone(), v.clone());
            }
        } else {
            let idx = merged_points.len();
            key_index.insert(key, idx);
            merged_points.push(MergedPoint {
                timestamp: ts,
                measurement: &p.measurement,
                tags: &p.tags,
                fields: p.fields.clone(),
                date: p.date,
            });
        }
    }

    // Discover full tag and field sets from merged points
    let mut tag_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut field_map: std::collections::BTreeMap<String, FieldType> =
        std::collections::BTreeMap::new();
    for p in &merged_points {
        for key in p.tags.keys() {
            tag_set.insert(key.clone());
        }
        for (key, val) in &p.fields {
            field_map
                .entry(key.clone())
                .or_insert_with(|| FieldType::from(val));
        }
    }
    let tag_keys: Vec<String> = tag_set.into_iter().collect();
    let field_keys: Vec<(String, FieldType)> = field_map.into_iter().collect();

    // Get or create cached schema
    let schema = get_or_create_schema(&tag_keys, &field_keys);

    // Build arrays from merged points — one row per unique key, no NULL gaps
    let total_columns = 3 + tag_keys.len() + field_keys.len();
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(total_columns);

    // Timestamp array
    let timestamps: Vec<i64> = merged_points.iter().map(|p| p.timestamp).collect();
    arrays.push(Arc::new(TimestampNanosecondArray::from(timestamps)));

    // Measurement array
    let measurements: Vec<&str> = merged_points.iter().map(|p| p.measurement).collect();
    arrays.push(Arc::new(StringArray::from(measurements)));

    // Date partition column
    let dates: Vec<String> = merged_points
        .iter()
        .map(|p| format_date32_as_string(p.date))
        .collect();
    arrays.push(Arc::new(StringArray::from(
        dates.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )));

    // Tag arrays
    for key in &tag_keys {
        let values: Vec<Option<&str>> = merged_points
            .iter()
            .map(|p| p.tags.get(key).map(|s| s.as_str()))
            .collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }

    // Field arrays (typed)
    for (key, field_type) in &field_keys {
        match field_type {
            FieldType::Float => {
                let values: Vec<Option<f64>> = merged_points
                    .iter()
                    .map(|p| {
                        p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::Float(f) => Some(*f),
                            FieldValue::Integer(i) => Some(*i as f64),
                            _ => None,
                        })
                    })
                    .collect();
                arrays.push(Arc::new(Float64Array::from(values)));
            }
            FieldType::Integer => {
                let values: Vec<Option<i64>> = merged_points
                    .iter()
                    .map(|p| {
                        p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::Integer(i) => Some(*i),
                            FieldValue::Float(f) => Some(*f as i64),
                            _ => None,
                        })
                    })
                    .collect();
                arrays.push(Arc::new(Int64Array::from(values)));
            }
            FieldType::String => {
                let values: Vec<Option<&str>> = merged_points
                    .iter()
                    .map(|p| {
                        p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::String(s) => Some(s.as_str()),
                            _ => None,
                        })
                    })
                    .collect();
                arrays.push(Arc::new(StringArray::from(values)));
            }
            FieldType::Boolean => {
                let values: Vec<Option<bool>> = merged_points
                    .iter()
                    .map(|p| {
                        p.fields.get(key).and_then(|fv| match fv {
                            FieldValue::Boolean(b) => Some(*b),
                            _ => None,
                        })
                    })
                    .collect();
                arrays.push(Arc::new(BooleanArray::from(values)));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| format!("Failed to create RecordBatch: {}", e))
}

/// Key for identifying unique points: (timestamp, measurement, tags)
/// Used to merge points with the same series + timestamp (InfluxDB semantics).
#[derive(Eq)]
struct PointKey<'a> {
    timestamp: i64,
    measurement: &'a str,
    tags: &'a HashMap<String, String>,
}

impl<'a> PartialEq for PointKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
            && self.measurement == other.measurement
            && self.tags == other.tags
    }
}

impl<'a> std::hash::Hash for PointKey<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.measurement.hash(state);
        // Hash tags in sorted order for consistency
        let mut sorted_tags: Vec<_> = self.tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| k.as_str());
        for (k, v) in sorted_tags {
            k.hash(state);
            v.hash(state);
        }
    }
}

/// A merged point that borrows measurement/tags from the original Points
/// but owns its combined fields (since fields get overwritten by later points).
struct MergedPoint<'a> {
    timestamp: i64,
    measurement: &'a str,
    tags: &'a HashMap<String, String>,
    fields: HashMap<String, FieldValue>,
    date: i32,
}

/// Coalesce adjacent rows in a SORTED batch that share the same key columns.
///
/// Key columns are: "time" (Timestamp) + all Utf8/LargeUtf8/Dictionary columns (tags).
/// For field columns (numeric/boolean), takes the **last** non-NULL value in each group
/// (last-write-wins, matching InfluxDB semantics).
///
/// The batch MUST be sorted by key columns so that duplicate keys are adjacent.
/// Used by both the parquet writer (flush path) and memtable provider (read path)
/// as a safety net for cross-batch merges when the same point arrives in separate
/// HTTP requests.
pub fn coalesce_record_batch(batch: &RecordBatch) -> Result<RecordBatch, String> {
    use arrow::array::*;
    use arrow::compute::take;

    let schema = batch.schema();
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        return Ok(batch.clone());
    }

    // Identify key column indices: "time" + Utf8 columns (tags like host, measurement, date)
    let mut key_indices: Vec<usize> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name() == "time"
            || matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _)
            )
        {
            key_indices.push(i);
        }
    }

    if key_indices.is_empty() {
        return Ok(batch.clone());
    }

    // Pre-downcast key columns for efficient comparison
    enum KeyCol {
        Timestamp(TimestampNanosecondArray),
        Str(StringArray),
    }
    let key_columns: Vec<KeyCol> = key_indices
        .iter()
        .map(|&col_idx| {
            let col = batch.column(col_idx);
            match schema.field(col_idx).data_type() {
                DataType::Timestamp(_, _) => KeyCol::Timestamp(
                    col.as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .clone(),
                ),
                _ => KeyCol::Str(
                    arrow::compute::cast(col, &DataType::Utf8)
                        .ok()
                        .and_then(|a| a.as_any().downcast_ref::<StringArray>().cloned())
                        .unwrap_or_else(|| {
                            StringArray::from(vec![Option::<&str>::None; num_rows])
                        }),
                ),
            }
        })
        .collect();

    // Check if row i and row j have the same key
    let keys_equal = |i: usize, j: usize| -> bool {
        for kc in &key_columns {
            match kc {
                KeyCol::Timestamp(arr) => {
                    if arr.is_null(i) != arr.is_null(j) {
                        return false;
                    }
                    if !arr.is_null(i) && arr.value(i) != arr.value(j) {
                        return false;
                    }
                }
                KeyCol::Str(arr) => {
                    if arr.is_null(i) != arr.is_null(j) {
                        return false;
                    }
                    if !arr.is_null(i) && arr.value(i) != arr.value(j) {
                        return false;
                    }
                }
            }
        }
        true
    };

    // First pass: identify groups of adjacent rows with the same key
    let mut groups: Vec<(usize, usize)> = Vec::new(); // (start, end) exclusive
    let mut start = 0;
    for i in 1..num_rows {
        if !keys_equal(start, i) {
            groups.push((start, i));
            start = i;
        }
    }
    groups.push((start, num_rows));

    // If no duplicates found, return as-is
    if groups.len() == num_rows {
        return Ok(batch.clone());
    }

    tracing::debug!(
        "Coalescing {} rows → {} rows (merged {} duplicate keys)",
        num_rows,
        groups.len(),
        num_rows - groups.len()
    );

    // Second pass: build output arrays by coalescing each group
    let num_output = groups.len();
    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(col_idx);
        let is_key = key_indices.contains(&col_idx);

        if is_key {
            // For key columns, just take the first row of each group
            let indices: Vec<u64> = groups.iter().map(|(s, _)| *s as u64).collect();
            let idx_array = UInt64Array::from(indices);
            let taken = take(col.as_ref(), &idx_array, None)
                .map_err(|e| format!("Failed to take key column: {}", e))?;
            output_columns.push(taken);
        } else {
            // For field columns, take the LAST non-NULL value in the group
            // (last-write-wins, matching InfluxDB semantics)
            match field.data_type() {
                DataType::Float64 => {
                    let arr = col.as_any().downcast_ref::<Float64Array>()
                        .ok_or("Expected Float64Array")?;
                    let mut builder = Float64Array::builder(num_output);
                    for &(s, e) in &groups {
                        let mut found = false;
                        for row in (s..e).rev() {
                            if !arr.is_null(row) {
                                builder.append_value(arr.value(row));
                                found = true;
                                break;
                            }
                        }
                        if !found { builder.append_null(); }
                    }
                    output_columns.push(Arc::new(builder.finish()));
                }
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>()
                        .ok_or("Expected Int64Array")?;
                    let mut builder = Int64Array::builder(num_output);
                    for &(s, e) in &groups {
                        let mut found = false;
                        for row in (s..e).rev() {
                            if !arr.is_null(row) {
                                builder.append_value(arr.value(row));
                                found = true;
                                break;
                            }
                        }
                        if !found { builder.append_null(); }
                    }
                    output_columns.push(Arc::new(builder.finish()));
                }
                DataType::UInt64 => {
                    let arr = col.as_any().downcast_ref::<UInt64Array>()
                        .ok_or("Expected UInt64Array")?;
                    let mut builder = UInt64Array::builder(num_output);
                    for &(s, e) in &groups {
                        let mut found = false;
                        for row in (s..e).rev() {
                            if !arr.is_null(row) {
                                builder.append_value(arr.value(row));
                                found = true;
                                break;
                            }
                        }
                        if !found { builder.append_null(); }
                    }
                    output_columns.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let arr = col.as_any().downcast_ref::<BooleanArray>()
                        .ok_or("Expected BooleanArray")?;
                    let mut builder = BooleanArray::builder(num_output);
                    for &(s, e) in &groups {
                        let mut found = false;
                        for row in (s..e).rev() {
                            if !arr.is_null(row) {
                                builder.append_value(arr.value(row));
                                found = true;
                                break;
                            }
                        }
                        if !found { builder.append_null(); }
                    }
                    output_columns.push(Arc::new(builder.finish()));
                }
                _ => {
                    // For other types, take last row of group (last-write-wins)
                    let indices: Vec<u64> = groups.iter().map(|(_, e)| (*e - 1) as u64).collect();
                    let idx_array = UInt64Array::from(indices);
                    let taken = take(col.as_ref(), &idx_array, None)
                        .map_err(|e| format!("Failed to take column: {}", e))?;
                    output_columns.push(taken);
                }
            }
        }
    }

    RecordBatch::try_new(schema, output_columns)
        .map_err(|e| format!("Failed to create coalesced batch: {}", e))
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

    #[test]
    fn test_point_merge_heterogeneous_fields() {
        // Simulate Telegraf swap: two points at same timestamp with different fields
        let mut tags1 = HashMap::new();
        tags1.insert("host".to_string(), "server01".to_string());
        let mut fields1 = HashMap::new();
        fields1.insert("free".to_string(), FieldValue::Integer(1000));
        fields1.insert("total".to_string(), FieldValue::Integer(2000));

        let mut tags2 = HashMap::new();
        tags2.insert("host".to_string(), "server01".to_string());
        let mut fields2 = HashMap::new();
        fields2.insert("in".to_string(), FieldValue::Integer(100));
        fields2.insert("out".to_string(), FieldValue::Integer(200));

        let ts = 1704067200000000000;
        let points = vec![
            Point {
                measurement: "swap".to_string(),
                tags: tags1,
                fields: fields1,
                timestamp: Some(ts),
                date: 19723,
                service: "default".to_string(),
            },
            Point {
                measurement: "swap".to_string(),
                tags: tags2,
                fields: fields2,
                timestamp: Some(ts),
                date: 19723,
                service: "default".to_string(),
            },
        ];

        // Points are merged at write time — one row, all fields present
        let batch = points_to_record_batch(&points).unwrap();
        assert_eq!(batch.num_rows(), 1); // Merged into 1 row (same key)
        // time + measurement + date + host + free + in + out + total = 8
        assert_eq!(batch.num_columns(), 8);

        // Verify all field values are present (not NULL)
        for i in 0..batch.num_columns() {
            let col = batch.column(i);
            assert!(
                !col.is_null(0),
                "Column {} ('{}') should not be NULL",
                i,
                batch.schema().field(i).name()
            );
        }
    }

    #[test]
    fn test_point_merge_last_write_wins() {
        // Two points with same key but conflicting field value — last write wins
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server01".to_string());

        let mut fields1 = HashMap::new();
        fields1.insert("used".to_string(), FieldValue::Integer(300));
        fields1.insert("free".to_string(), FieldValue::Integer(1000));

        let mut fields2 = HashMap::new();
        fields2.insert("used".to_string(), FieldValue::Integer(999)); // conflict!
        fields2.insert("in".to_string(), FieldValue::Integer(100));

        let ts = 1704067200000000000;
        let points = vec![
            Point {
                measurement: "swap".to_string(),
                tags: tags.clone(),
                fields: fields1,
                timestamp: Some(ts),
                date: 19723,
                service: "default".to_string(),
            },
            Point {
                measurement: "swap".to_string(),
                tags,
                fields: fields2,
                timestamp: Some(ts),
                date: 19723,
                service: "default".to_string(),
            },
        ];

        let batch = points_to_record_batch(&points).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // "used" should be 999 (from second point, last write wins)
        let used_idx = batch.schema().index_of("used").unwrap();
        let used_arr = batch.column(used_idx).as_any()
            .downcast_ref::<Int64Array>().unwrap();
        assert_eq!(used_arr.value(0), 999);

        // "free" from first point should still be present
        let free_idx = batch.schema().index_of("free").unwrap();
        let free_arr = batch.column(free_idx).as_any()
            .downcast_ref::<Int64Array>().unwrap();
        assert_eq!(free_arr.value(0), 1000);

        // "in" from second point should be present
        let in_idx = batch.schema().index_of("in").unwrap();
        let in_arr = batch.column(in_idx).as_any()
            .downcast_ref::<Int64Array>().unwrap();
        assert_eq!(in_arr.value(0), 100);
    }

    #[test]
    fn test_point_merge_different_timestamps() {
        // Two points at different timestamps — should NOT merge
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server01".to_string());
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Float(1.0));

        let points = vec![
            Point {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: Some(1000),
                date: 19723,
                service: "default".to_string(),
            },
            Point {
                measurement: "cpu".to_string(),
                tags,
                fields,
                timestamp: Some(2000),
                date: 19723,
                service: "default".to_string(),
            },
        ];

        let batch = points_to_record_batch(&points).unwrap();
        assert_eq!(batch.num_rows(), 2); // Different timestamps — no merge
    }

    #[test]
    fn test_coalesce_last_write_wins() {
        // Test the coalesce safety net with conflicting values.
        // Build a batch manually with two rows at the same key, both having "value".
        use arrow::array::*;
        use arrow::datatypes::{Field, TimeUnit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("measurement", DataType::Utf8, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(TimestampNanosecondArray::from(vec![1000, 1000])),
            Arc::new(StringArray::from(vec!["cpu", "cpu"])),
            Arc::new(StringArray::from(vec!["h1", "h1"])),
            Arc::new(Int64Array::from(vec![Some(100), Some(999)])),
        ]).unwrap();

        let coalesced = coalesce_record_batch(&batch).unwrap();
        assert_eq!(coalesced.num_rows(), 1);

        // Last-write-wins: should be 999 (second row)
        let val = coalesced.column(3).as_any()
            .downcast_ref::<Int64Array>().unwrap();
        assert_eq!(val.value(0), 999);
    }
}
