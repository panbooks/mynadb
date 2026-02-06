//! Grafana Datasource API
//!
//! Implements Grafana-specific endpoints for datasource plugin integration:
//! - /grafana/search - Metric/table discovery
//! - /grafana/query - Execute queries with Grafana response format
//! - /grafana/tag-keys - Template variable keys
//! - /grafana/tag-values - Template variable values
//! - /grafana/health - Health check

use axum::{
    extract::{Query as AxumQuery, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::query::query_engine::QueryEngine;
use crate::services::api::AppState;

// ===== Request/Response Types =====

#[derive(Deserialize)]
pub struct SearchRequest {
    #[serde(default)]
    pub target: Option<String>,
    #[serde(rename = "type", default)]
    pub search_type: Option<String>,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub text: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expandable: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct GrafanaQueryRequest {
    pub range: GrafanaTimeRange,
    pub targets: Vec<GrafanaTarget>,
    #[serde(rename = "maxDataPoints")]
    pub max_data_points: Option<i32>,
    pub interval: Option<String>,
    #[serde(rename = "intervalMs")]
    pub interval_ms: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct GrafanaTimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GrafanaTarget {
    #[serde(rename = "refId")]
    pub ref_id: String,
    #[serde(rename = "rawSql")]
    pub raw_sql: Option<String>,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub hide: bool,
}

fn default_format() -> String {
    "time_series".to_string()
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum GrafanaResponse {
    TimeSeries(TimeSeriesResponse),
    Table(TableResponse),
}

#[derive(Serialize)]
pub struct TimeSeriesResponse {
    pub target: String,
    pub datapoints: Vec<[serde_json::Value; 2]>,
}

#[derive(Serialize)]
pub struct TableResponse {
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(rename = "type")]
    pub response_type: String,
}

#[derive(Serialize)]
pub struct ColumnDef {
    pub text: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[derive(Serialize)]
pub struct TagKeyResult {
    pub text: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct TagValueResult {
    pub text: String,
}

// ===== Handlers =====

/// Search endpoint - returns available metrics/tables
pub async fn search_handler(
    AxumQuery(params): AxumQuery<SearchRequest>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let db = params.target.as_deref().unwrap_or("default");

    debug!("Grafana search: db={}, type={:?}", db, params.search_type);

    // List tables in catalog
    let sql = format!("SHOW TABLES FROM {}", db);

    let results = match state.query_engine.execute_query_json_with_db(&sql, db).await {
        Ok(json_str) => {
            // Parse JSON to extract table names
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(value) => {
                    let mut search_results = Vec::new();

                    if let Some(arr) = value.as_array() {
                        for row in arr {
                            if let Some(obj) = row.as_object() {
                                // Extract table name from first column
                                if let Some((_, table_value)) = obj.iter().next() {
                                    if let Some(table_name) = table_value.as_str() {
                                        search_results.push(SearchResult {
                                            text: table_name.to_string(),
                                            value: table_name.to_string(),
                                            expandable: Some(true),
                                        });
                                    }
                                }
                            }
                        }
                    }

                    search_results
                }
                Err(e) => {
                    error!("Failed to parse table list: {}", e);
                    vec![]
                }
            }
        }
        Err(e) => {
            error!("Failed to list tables: {}", e);
            vec![]
        }
    };

    Json(results)
}

/// Query endpoint - executes SQL and returns Grafana-formatted data
pub async fn query_handler(
    State(state): State<AppState>,
    Json(request): Json<GrafanaQueryRequest>,
) -> impl IntoResponse {
    debug!(
        "Grafana query: targets={}, range={:?} to {:?}",
        request.targets.len(),
        request.range.from,
        request.range.to
    );

    let mut responses = Vec::new();

    for target in &request.targets {
        if target.hide {
            continue;
        }

        if let Some(sql) = &target.raw_sql {
            // Replace Grafana variables
            let sql = replace_grafana_variables(sql, &request);

            debug!("Executing query: {}", sql);

            // Extract database from SQL or use default
            let db = extract_database_from_sql(&sql).unwrap_or("default");

            match state.query_engine.execute_query_with_db(&sql, db).await {
                Ok(batches) => {
                    if target.format == "table" {
                        responses.push(GrafanaResponse::Table(convert_to_table_format(
                            batches,
                        )));
                    } else {
                        // time_series format
                        responses.push(GrafanaResponse::TimeSeries(
                            convert_to_timeseries_format(batches, &target.ref_id),
                        ));
                    }
                }
                Err(e) => {
                    error!("Query error for {}: {}", target.ref_id, e);
                    // Return empty response
                    responses.push(GrafanaResponse::TimeSeries(TimeSeriesResponse {
                        target: target.ref_id.clone(),
                        datapoints: vec![],
                    }));
                }
            }
        }
    }

    Json(responses)
}

/// Tag keys endpoint - returns available tag keys for template variables
pub async fn tag_keys_handler(
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let db = params.get("db").map(|s| s.as_str()).unwrap_or("default");
    let table = params.get("table").map(|s| s.as_str());

    debug!("Grafana tag-keys: db={}, table={:?}", db, table);

    // If table specified, get its columns
    if let Some(table_name) = table {
        let sql = format!("SELECT * FROM {} LIMIT 0", table_name);

        match state.query_engine.execute_query_with_db(&sql, db).await {
            Ok(batches) => {
                if let Some(batch) = batches.first() {
                    let keys: Vec<TagKeyResult> = batch
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| {
                            // Filter out time column and numeric columns
                            !matches!(
                                f.name().as_str(),
                                "time" | "timestamp" | "date" | "value"
                            ) && matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8)
                        })
                        .map(|f| TagKeyResult {
                            text: f.name().clone(),
                            value: f.name().clone(),
                        })
                        .collect();

                    return Json(keys);
                }
            }
            Err(e) => {
                error!("Failed to get columns: {}", e);
            }
        }
    }

    // Default: return common tag keys
    Json(vec![
        TagKeyResult {
            text: "host".to_string(),
            value: "host".to_string(),
        },
        TagKeyResult {
            text: "service".to_string(),
            value: "service".to_string(),
        },
        TagKeyResult {
            text: "region".to_string(),
            value: "region".to_string(),
        },
    ])
}

/// Tag values endpoint - returns values for a given tag key
pub async fn tag_values_handler(
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let db = params.get("db").map(|s| s.as_str()).unwrap_or("default");
    let key = params.get("key").map(|s| s.as_str()).unwrap_or("");
    let table = params.get("table").map(|s| s.as_str());

    debug!(
        "Grafana tag-values: db={}, key={}, table={:?}",
        db, key, table
    );

    if key.is_empty() || table.is_none() {
        return Json(vec![]);
    }

    let table_name = table.unwrap();

    // Query distinct values for the tag key
    let sql = format!("SELECT DISTINCT {} FROM {} LIMIT 1000", key, table_name);

    match state.query_engine.execute_query_with_db(&sql, db).await {
        Ok(batches) => {
            let mut values = Vec::new();

            for batch in batches {
                if batch.num_columns() > 0 {
                    let col = batch.column(0);

                    if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
                        for i in 0..string_array.len() {
                            if string_array.is_valid(i) {
                                values.push(TagValueResult {
                                    text: string_array.value(i).to_string(),
                                });
                            }
                        }
                    }
                }
            }

            Json(values)
        }
        Err(e) => {
            error!("Failed to get tag values: {}", e);
            Json(vec![])
        }
    }
}

/// Health check endpoint
pub async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// ===== Helper Functions =====

/// Replace Grafana variables in SQL query
fn replace_grafana_variables(sql: &str, request: &GrafanaQueryRequest) -> String {
    let mut sql = sql.to_string();

    // Parse time range
    let from_ns = parse_grafana_time(&request.range.from);
    let to_ns = parse_grafana_time(&request.range.to);

    // Replace $__timeFilter(column)
    // Look for pattern: $__timeFilter(columnName)
    let time_filter_re = regex::Regex::new(r"\$__timeFilter\((\w+)\)").unwrap();
    sql = time_filter_re
        .replace_all(&sql, |caps: &regex::Captures| {
            let column = &caps[1];
            format!("{} >= {} AND {} <= {}", column, from_ns, column, to_ns)
        })
        .to_string();

    // Replace $__from and $__to
    sql = sql.replace("$__from", &from_ns.to_string());
    sql = sql.replace("$__to", &to_ns.to_string());

    // Replace $__interval
    if let Some(interval) = &request.interval {
        sql = sql.replace("$__interval", interval);
    }

    sql
}

/// Parse Grafana time string to nanoseconds
fn parse_grafana_time(time_str: &str) -> i64 {
    // Try parsing as epoch milliseconds first
    if let Ok(ms) = time_str.parse::<i64>() {
        return ms * 1_000_000; // Convert ms to ns
    }

    // Try parsing ISO8601
    if let Ok(dt) = DateTime::parse_from_rfc3339(time_str) {
        return dt.timestamp_nanos_opt().unwrap_or(0);
    }

    // Try parsing without timezone
    if let Ok(naive) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%dT%H:%M:%S%.f") {
        return naive.and_utc().timestamp_nanos_opt().unwrap_or(0);
    }

    // Fallback to current time
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Convert Arrow RecordBatch to Grafana time series format
fn convert_to_timeseries_format(batches: Vec<RecordBatch>, target: &str) -> TimeSeriesResponse {
    let mut datapoints = Vec::new();

    for batch in batches {
        if batch.num_columns() < 2 {
            continue;
        }

        // Assume schema: [time, value, ...]
        let time_col = batch.column(0);
        let value_col = batch.column(1);

        // Try different time column types
        let times = if let Some(int64_array) = time_col.as_any().downcast_ref::<Int64Array>() {
            int64_array
                .iter()
                .map(|t| t.map(|ts| ts / 1_000_000)) // ns to ms
                .collect::<Vec<_>>()
        } else {
            continue;
        };

        // Try different value column types
        let values = if let Some(float_array) = value_col.as_any().downcast_ref::<Float64Array>() {
            float_array.iter().collect::<Vec<_>>()
        } else if let Some(int_array) = value_col.as_any().downcast_ref::<Int64Array>() {
            int_array.iter().map(|v| v.map(|i| i as f64)).collect()
        } else {
            continue;
        };

        // Combine into datapoints
        for i in 0..batch.num_rows() {
            if let (Some(time), Some(value)) = (times.get(i), values.get(i)) {
                datapoints.push([
                    serde_json::json!(value),
                    serde_json::json!(time),
                ]);
            }
        }
    }

    TimeSeriesResponse {
        target: target.to_string(),
        datapoints,
    }
}

/// Convert Arrow RecordBatch to Grafana table format
fn convert_to_table_format(batches: Vec<RecordBatch>) -> TableResponse {
    if batches.is_empty() {
        return TableResponse {
            columns: vec![],
            rows: vec![],
            response_type: "table".to_string(),
        };
    }

    let schema = batches[0].schema();

    // Build column definitions
    let columns: Vec<ColumnDef> = schema
        .fields()
        .iter()
        .map(|field| ColumnDef {
            text: field.name().clone(),
            col_type: map_arrow_type_to_grafana(field.data_type()),
        })
        .collect();

    // Build rows
    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = extract_value_at_index(col, row_idx);
                row.push(value);
            }

            rows.push(row);
        }
    }

    TableResponse {
        columns,
        rows,
        response_type: "table".to_string(),
    }
}

/// Map Arrow data type to Grafana column type
fn map_arrow_type_to_grafana(data_type: &DataType) -> String {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "number".to_string(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            "number".to_string()
        }
        DataType::Float32 | DataType::Float64 => "number".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => "time".to_string(),
        _ => "string".to_string(),
    }
}

/// Extract value from Arrow array at index
fn extract_value_at_index(col: &dyn Array, idx: usize) -> serde_json::Value {
    if !col.is_valid(idx) {
        return serde_json::Value::Null;
    }

    // Try different array types
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return serde_json::json!(arr.value(idx));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return serde_json::json!(arr.value(idx));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return serde_json::json!(arr.value(idx));
    }

    serde_json::Value::Null
}

/// Extract database name from SQL query
fn extract_database_from_sql(sql: &str) -> Option<&str> {
    // Look for pattern: FROM catalog.schema.table or USE catalog
    let sql_lower = sql.to_lowercase();

    if sql_lower.contains("use ") {
        // USE database
        if let Some(start) = sql_lower.find("use ") {
            let rest = &sql[start + 4..];
            if let Some(end) = rest.find(|c: char| c.is_whitespace() || c == ';') {
                return Some(rest[..end].trim());
            }
        }
    }

    if sql_lower.contains("from ") {
        // FROM catalog.schema.table
        if let Some(start) = sql_lower.find("from ") {
            let rest = &sql[start + 5..];
            if let Some(first_word_end) = rest.find(|c: char| c.is_whitespace()) {
                let table_ref = rest[..first_word_end].trim();
                if let Some(dot_pos) = table_ref.find('.') {
                    return Some(&table_ref[..dot_pos]);
                }
            }
        }
    }

    None
}
