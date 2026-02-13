//! InfluxDB Response Format
//!
//! Formats query results in InfluxDB-compatible JSON format.

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

/// InfluxDB query response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxResponse {
    pub results: Vec<StatementResult>,
}

/// Result of a single statement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResult {
    pub statement_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<Vec<Series>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A single series (measurement + tags combination)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::HashMap<String, String>>,
    pub columns: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

/// Error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl InfluxResponse {
    pub fn new() -> Self {
        Self { results: Vec::new() }
    }

    pub fn with_result(result: StatementResult) -> Self {
        Self {
            results: vec![result],
        }
    }

    pub fn error(statement_id: u32, message: impl Into<String>) -> Self {
        Self {
            results: vec![StatementResult {
                statement_id,
                series: None,
                error: Some(message.into()),
            }],
        }
    }
}

impl Default for InfluxResponse {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert Arrow RecordBatches to InfluxDB response format
pub fn batches_to_response(
    batches: &[RecordBatch],
    measurement: &str,
    statement_id: u32,
) -> InfluxResponse {
    batches_to_response_with_epoch(batches, measurement, statement_id, None)
}

/// Convert Arrow RecordBatches to InfluxDB response with optional epoch timestamp format
/// epoch: None = RFC3339, Some("ns") = nanoseconds, Some("ms") = milliseconds, etc.
pub fn batches_to_response_with_epoch(
    batches: &[RecordBatch],
    measurement: &str,
    statement_id: u32,
    epoch: Option<&str>,
) -> InfluxResponse {
    if batches.is_empty() {
        return InfluxResponse::with_result(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: None,
        });
    }

    // Get column names from schema
    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    // Convert all batches to values
    let mut all_values: Vec<Vec<serde_json::Value>> = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());

            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let col_name = columns[col_idx].as_str();
                let value = extract_value_with_epoch(col, row_idx, epoch, col_name);
                row.push(value);
            }

            all_values.push(row);
        }
    }

    let series = Series {
        name: measurement.to_string(),
        tags: None,
        columns,
        values: all_values,
    };

    InfluxResponse::with_result(StatementResult {
        statement_id,
        series: Some(vec![series]),
        error: None,
    })
}

/// Convert Arrow RecordBatches to InfluxDB response, grouping by tag columns
pub fn batches_to_grouped_response(
    batches: &[RecordBatch],
    measurement: &str,
    tag_columns: &[&str],
    statement_id: u32,
) -> InfluxResponse {
    batches_to_grouped_response_with_epoch(batches, measurement, tag_columns, statement_id, None)
}

/// Convert Arrow RecordBatches to InfluxDB response, grouping by tag columns, with epoch support
pub fn batches_to_grouped_response_with_epoch(
    batches: &[RecordBatch],
    measurement: &str,
    tag_columns: &[&str],
    statement_id: u32,
    epoch: Option<&str>,
) -> InfluxResponse {
    if batches.is_empty() {
        return InfluxResponse::with_result(StatementResult {
            statement_id,
            series: Some(vec![]),
            error: None,
        });
    }

    // Get schema
    let schema = batches[0].schema();

    // Identify tag column indices
    let tag_indices: Vec<usize> = tag_columns
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect();

    // Non-tag columns (for the series values)
    let value_columns: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| !tag_indices.contains(idx))
        .map(|(idx, f)| (idx, f.name().clone()))
        .collect();

    // Group rows by tag values
    let mut groups: std::collections::HashMap<Vec<String>, Vec<Vec<serde_json::Value>>> =
        std::collections::HashMap::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            // Extract tag values for grouping key
            let group_key: Vec<String> = tag_indices
                .iter()
                .map(|&idx| {
                    let col = batch.column(idx);
                    extract_string_value(col, row_idx)
                })
                .collect();

            // Extract value columns
            let row: Vec<serde_json::Value> = value_columns
                .iter()
                .map(|(idx, name)| extract_value_with_epoch(batch.column(*idx), row_idx, epoch, name))
                .collect();

            groups.entry(group_key).or_default().push(row);
        }
    }

    // Convert groups to series
    let series: Vec<Series> = groups
        .into_iter()
        .map(|(tag_values, values)| {
            let tags: std::collections::HashMap<String, String> = tag_columns
                .iter()
                .zip(tag_values.iter())
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();

            Series {
                name: measurement.to_string(),
                tags: if tags.is_empty() { None } else { Some(tags) },
                columns: value_columns.iter().map(|(_, n)| n.clone()).collect(),
                values,
            }
        })
        .collect();

    InfluxResponse::with_result(StatementResult {
        statement_id,
        series: Some(series),
        error: None,
    })
}

fn extract_value(array: &dyn Array, row_idx: usize) -> serde_json::Value {
    extract_value_with_epoch(array, row_idx, None, "")
}

/// Extract a value from an Arrow array, with optional epoch timestamp formatting.
/// epoch: None = RFC3339, "ns" = nanoseconds, "us"/"u" = microseconds,
///        "ms" = milliseconds, "s" = seconds
/// col_name: column name, used to identify Int64 time columns for epoch conversion
fn extract_value_with_epoch(
    array: &dyn Array,
    row_idx: usize,
    epoch: Option<&str>,
    col_name: &str,
) -> serde_json::Value {
    if array.is_null(row_idx) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row_idx))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>();
            if let Some(arr) = arr {
                let val = arr.value(row_idx);
                // time_bucket() returns Int64 nanoseconds — apply epoch conversion
                if col_name == "time" {
                    return convert_nanos_to_epoch(val, epoch);
                }
                serde_json::json!(val)
            } else {
                // Try other int types
                serde_json::Value::Null
            }
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            // Handle unsigned integers
            serde_json::Value::Null
        }
        DataType::Float32 | DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>();
            if let Some(arr) = arr {
                let val = arr.value(row_idx);
                if val.is_finite() {
                    serde_json::json!(val)
                } else {
                    serde_json::Value::Null
                }
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>();
            if let Some(arr) = arr {
                serde_json::Value::String(arr.value(row_idx).to_string())
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Timestamp(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>();
            if let Some(arr) = arr {
                let nanos = arr.value(row_idx);
                convert_nanos_to_epoch(nanos, epoch)
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Date32 => {
            // Days since epoch
            serde_json::Value::Null
        }
        _ => serde_json::Value::Null,
    }
}

/// Convert nanosecond timestamp to the requested epoch precision.
/// epoch: None = RFC3339 string, "ns" = nanos, "ms" = millis, "us" = micros, "s" = seconds
fn convert_nanos_to_epoch(nanos: i64, epoch: Option<&str>) -> serde_json::Value {
    match epoch {
        Some("ns" | "n") => serde_json::json!(nanos),
        Some("us" | "u") => serde_json::json!(nanos / 1_000),
        Some("ms") => serde_json::json!(nanos / 1_000_000),
        Some("s") => serde_json::json!(nanos / 1_000_000_000),
        _ => {
            // Default: RFC3339 string
            let secs = nanos / 1_000_000_000;
            let nsecs = (nanos % 1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                serde_json::Value::String(
                    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                )
            } else {
                serde_json::json!(nanos)
            }
        }
    }
}

fn extract_string_value(array: &dyn Array, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return String::new();
    }

    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>();
            arr.map(|a| a.value(row_idx).to_string())
                .unwrap_or_default()
        }
        _ => format!("{:?}", extract_value(array, row_idx)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_empty_response() {
        let response = batches_to_response(&[], "cpu", 0);
        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].series.as_ref().unwrap().is_empty());
    }

    #[test]
    fn test_simple_response() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1000000000, 2000000000])),
                Arc::new(Float64Array::from(vec![42.5, 43.1])),
            ],
        )
        .unwrap();

        let response = batches_to_response(&[batch], "cpu", 0);
        assert_eq!(response.results.len(), 1);

        let series = &response.results[0].series.as_ref().unwrap()[0];
        assert_eq!(series.name, "cpu");
        assert_eq!(series.columns, vec!["time", "value"]);
        assert_eq!(series.values.len(), 2);
    }

    #[test]
    fn test_error_response() {
        let response = InfluxResponse::error(0, "Query failed");
        assert_eq!(response.results[0].error, Some("Query failed".to_string()));
        assert!(response.results[0].series.is_none());
    }
}
