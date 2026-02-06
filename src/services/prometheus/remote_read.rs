//! Prometheus Remote Read Handler
//!
//! Handles POST /api/v1/read requests from Prometheus.
//! Data flow:
//! 1. Receive protobuf ReadRequest (optionally snappy-compressed)
//! 2. Parse and convert matchers to SQL
//! 3. Query MynaDB
//! 4. Convert results to TimeSeries
//! 5. Return snappy-compressed ReadResponse

use super::types::{
    Label, LabelMatcher, MatchType, Query, QueryResult, ReadRequest, ReadResponse, Sample,
    TimeSeries, METRIC_NAME_LABEL,
};
use crate::query::query_engine::QueryEngine;
use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// AppState subset needed for remote read
pub struct RemoteReadState {
    pub query_engine: Arc<QueryEngine>,
    pub default_schema: String,
}

/// Handle Prometheus remote read request
///
/// Expects:
/// - Content-Type: application/x-protobuf
/// - Body: ReadRequest protobuf (optionally snappy-compressed)
///
/// Returns:
/// - Content-Type: application/x-protobuf
/// - Content-Encoding: snappy
/// - Body: snappy-compressed ReadResponse
pub async fn remote_read_handler(
    Path(db): Path<String>,
    State(state): State<Arc<RemoteReadState>>,
    body: Bytes,
) -> impl IntoResponse {
    // Try to decompress (Prometheus may or may not compress read requests)
    let data = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(decompressed) => decompressed,
        Err(_) => body.to_vec(), // Not compressed, use as-is
    };

    // Parse protobuf
    let read_request = match ReadRequest::decode(&data[..]) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to parse ReadRequest protobuf: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse protobuf: {}", e),
            )
                .into_response();
        }
    };

    debug!(
        "Received Prometheus remote read: {} queries",
        read_request.queries.len()
    );

    // Process each query
    let mut results = Vec::new();

    for query in read_request.queries {
        match process_query(&state, &db, &query).await {
            Ok(result) => results.push(result),
            Err(e) => {
                error!("Query failed: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
            }
        }
    }

    // Build response
    let response = ReadResponse { results };

    // Encode to protobuf
    let mut proto_buf = Vec::new();
    if let Err(e) = response.encode(&mut proto_buf) {
        error!("Failed to encode ReadResponse: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to encode response: {}", e),
        )
            .into_response();
    }

    // Compress with snappy
    let compressed = match snap::raw::Encoder::new().compress_vec(&proto_buf) {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to compress response: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to compress response: {}", e),
            )
                .into_response();
        }
    };

    debug!(
        "Prometheus remote read complete: {} bytes response",
        compressed.len()
    );

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-protobuf"),
            (header::CONTENT_ENCODING, "snappy"),
        ],
        compressed,
    )
        .into_response()
}

/// Process a single Prometheus query
async fn process_query(
    state: &RemoteReadState,
    db: &str,
    query: &Query,
) -> Result<QueryResult, String> {
    // Extract metric name from matchers
    let metric_name = query
        .matchers
        .iter()
        .find(|m| m.name == METRIC_NAME_LABEL && m.r#type == MatchType::Eq as i32)
        .map(|m| m.value.as_str());

    let metric_name = match metric_name {
        Some(name) => name,
        None => {
            return Err(
                "Query must include __name__ matcher with equality match".to_string()
            );
        }
    };

    // Build SQL query
    let sql = build_sql_query(db, &state.default_schema, metric_name, query)?;
    debug!("Prometheus read SQL: {}", sql);

    // Execute query
    let batches = state
        .query_engine
        .execute_query_with_db(&sql, db)
        .await
        .map_err(|e| format!("Query execution failed: {}", e))?;

    // Convert results to TimeSeries
    let timeseries = batches_to_timeseries(&batches, metric_name)?;

    Ok(QueryResult { timeseries })
}

/// Build SQL query from Prometheus query
fn build_sql_query(
    db: &str,
    schema: &str,
    metric_name: &str,
    query: &Query,
) -> Result<String, String> {
    let table = format!("{}.{}.{}", db, schema, sanitize_metric_name(metric_name));

    // Build WHERE clause from matchers (excluding __name__)
    let mut conditions = Vec::new();

    // Time range
    let start_ns = query.start_timestamp_ms * 1_000_000;
    let end_ns = query.end_timestamp_ms * 1_000_000;
    conditions.push(format!("time >= {}", start_ns));
    conditions.push(format!("time <= {}", end_ns));

    // Label matchers
    for matcher in &query.matchers {
        if matcher.name == METRIC_NAME_LABEL {
            continue; // Already handled
        }

        let condition = match MatchType::from_i32(matcher.r#type) {
            Some(MatchType::Eq) => {
                format!("\"{}\" = '{}'", matcher.name, escape_sql_string(&matcher.value))
            }
            Some(MatchType::Neq) => {
                format!("\"{}\" != '{}'", matcher.name, escape_sql_string(&matcher.value))
            }
            Some(MatchType::Re) => {
                // DataFusion uses ~ for regex
                format!(
                    "\"{}\" ~ '{}'",
                    matcher.name,
                    escape_sql_string(&matcher.value)
                )
            }
            Some(MatchType::Nre) => {
                format!(
                    "NOT (\"{}\" ~ '{}')",
                    matcher.name,
                    escape_sql_string(&matcher.value)
                )
            }
            None => {
                return Err(format!("Unknown matcher type: {}", matcher.r#type));
            }
        };
        conditions.push(condition);
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    // Select all columns, order by time
    Ok(format!(
        "SELECT * FROM {}{}  ORDER BY time",
        table, where_clause
    ))
}

/// Convert Arrow RecordBatches to Prometheus TimeSeries
fn batches_to_timeseries(
    batches: &[RecordBatch],
    metric_name: &str,
) -> Result<Vec<TimeSeries>, String> {
    // Group samples by label combination
    let mut series_map: HashMap<Vec<(String, String)>, Vec<Sample>> = HashMap::new();

    for batch in batches {
        let schema = batch.schema();

        // Find time and value columns
        let time_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "time")
            .ok_or("Missing 'time' column")?;
        let value_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "value")
            .ok_or("Missing 'value' column")?;

        // Get arrays
        let time_array = batch.column(time_idx);
        let value_array = batch.column(value_idx);

        // Determine which columns are tags (not time, value, date, measurement)
        let tag_columns: Vec<(usize, &str)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                let name = f.name();
                name != "time" && name != "value" && name != "date" && name != "measurement"
            })
            .map(|(i, f)| (i, f.name().as_str()))
            .collect();

        // Process each row
        for row in 0..batch.num_rows() {
            // Extract timestamp (handle both i64 and timestamp types)
            let timestamp_ns = extract_timestamp(time_array, row)?;
            let timestamp_ms = timestamp_ns / 1_000_000;

            // Extract value
            let value = extract_value(value_array, row)?;

            // Extract tags
            let mut labels: Vec<(String, String)> = Vec::new();
            for (idx, name) in &tag_columns {
                if let Some(tag_value) = extract_string(batch.column(*idx), row) {
                    if !tag_value.is_empty() {
                        labels.push((name.to_string(), tag_value));
                    }
                }
            }
            labels.sort_by(|a, b| a.0.cmp(&b.0));

            // Add sample to series
            series_map
                .entry(labels)
                .or_default()
                .push(Sample::new(timestamp_ms, value));
        }
    }

    // Convert to TimeSeries
    let timeseries = series_map
        .into_iter()
        .map(|(labels, mut samples)| {
            // Sort samples by timestamp
            samples.sort_by_key(|s| s.timestamp);

            // Build labels including __name__
            let mut ts_labels = vec![Label::new(METRIC_NAME_LABEL, metric_name)];
            ts_labels.extend(labels.into_iter().map(|(k, v)| Label::new(k, v)));

            TimeSeries {
                labels: ts_labels,
                samples,
            }
        })
        .collect();

    Ok(timeseries)
}

/// Extract timestamp from array
fn extract_timestamp(array: &dyn Array, row: usize) -> Result<i64, String> {
    if let Some(ts_array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Ok(ts_array.value(row))
    } else if let Some(i64_array) = array.as_any().downcast_ref::<Int64Array>() {
        Ok(i64_array.value(row))
    } else {
        Err(format!("Unsupported time column type: {:?}", array.data_type()))
    }
}

/// Extract value from array
fn extract_value(array: &dyn Array, row: usize) -> Result<f64, String> {
    if let Some(f64_array) = array.as_any().downcast_ref::<Float64Array>() {
        Ok(f64_array.value(row))
    } else {
        Err(format!(
            "Unsupported value column type: {:?}",
            array.data_type()
        ))
    }
}

/// Extract string from array
fn extract_string(array: &dyn Array, row: usize) -> Option<String> {
    if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
        if str_array.is_null(row) {
            None
        } else {
            Some(str_array.value(row).to_string())
        }
    } else {
        None
    }
}

/// Sanitize metric name for table lookup
fn sanitize_metric_name(name: &str) -> String {
    name.replace(':', "_").replace('-', "_")
}

/// Escape single quotes in SQL strings
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_sql_query_basic() {
        let query = Query {
            start_timestamp_ms: 1704067200000,
            end_timestamp_ms: 1704153600000,
            matchers: vec![LabelMatcher {
                r#type: MatchType::Eq as i32,
                name: METRIC_NAME_LABEL.to_string(),
                value: "cpu_usage".to_string(),
            }],
        };

        let sql = build_sql_query("mydb", "default", "cpu_usage", &query).unwrap();
        assert!(sql.contains("mydb.default.cpu_usage"));
        assert!(sql.contains("time >= 1704067200000000000"));
        assert!(sql.contains("time <= 1704153600000000000"));
    }

    #[test]
    fn test_build_sql_query_with_labels() {
        let query = Query {
            start_timestamp_ms: 1704067200000,
            end_timestamp_ms: 1704153600000,
            matchers: vec![
                LabelMatcher {
                    r#type: MatchType::Eq as i32,
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "cpu_usage".to_string(),
                },
                LabelMatcher {
                    r#type: MatchType::Eq as i32,
                    name: "host".to_string(),
                    value: "server01".to_string(),
                },
                LabelMatcher {
                    r#type: MatchType::Re as i32,
                    name: "cpu".to_string(),
                    value: "[0-3]".to_string(),
                },
            ],
        };

        let sql = build_sql_query("mydb", "default", "cpu_usage", &query).unwrap();
        assert!(sql.contains("\"host\" = 'server01'"));
        assert!(sql.contains("\"cpu\" ~ '[0-3]'"));
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("'quoted'"), "''quoted''");
    }
}
