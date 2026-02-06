//! Query Console API handlers
//!
//! Forwards query requests to data nodes and provides saved queries / history.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

use super::state::AdminState;

// ============ Routes ============

pub fn routes() -> Router<AdminState> {
    Router::new()
        .route("/query/execute", post(execute_query))
        .route("/query/explain/:catalog", get(explain_query))
        .route("/query/analyze/:catalog", get(analyze_query))
        .route("/query/catalogs", get(list_catalogs))
        .route("/query/schemas/:catalog", get(list_schemas))
        .route(
            "/query/columns/:catalog/:schema/:table",
            get(get_columns),
        )
        .route("/query/saved", get(list_saved_queries).post(save_query))
        .route("/query/saved/:id", delete(delete_saved_query))
        .route(
            "/query/history",
            get(get_query_history).delete(clear_query_history),
        )
}

// ============ Response Types ============

#[derive(Serialize)]
struct ApiResponse<T> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

// ============ Query Execute ============

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    catalog: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Serialize)]
struct QueryResult {
    columns: Vec<ColumnInfo>,
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
    row_count: usize,
    execution_time_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    truncated: Option<bool>,
}

/// Infer a simple data type string from a JSON value
fn infer_json_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "Utf8",
        serde_json::Value::Bool(_) => "Boolean",
        serde_json::Value::Number(n) => {
            if n.is_f64() {
                "Float64"
            } else {
                "Int64"
            }
        }
        serde_json::Value::String(_) => "Utf8",
        serde_json::Value::Array(_) => "List",
        serde_json::Value::Object(_) => "Struct",
    }
}

/// Extract column info from query result rows
fn extract_columns(rows: &[serde_json::Value]) -> Vec<ColumnInfo> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Use the first row to determine column names and types
    // Check additional rows for better type inference
    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut col_types: HashMap<String, &'static str> = HashMap::new();
    let mut col_order: Vec<String> = Vec::new();

    for row in rows.iter().take(10) {
        if let Some(obj) = row.as_object() {
            for (key, value) in obj {
                if !col_types.contains_key(key) {
                    col_order.push(key.clone());
                }
                // Only override if current type is Null (unknown)
                let current = col_types.get(key.as_str()).copied().unwrap_or("Utf8");
                if current == "Utf8" || !value.is_null() {
                    col_types.insert(key.clone(), infer_json_type(value));
                }
            }
        }
    }

    for name in col_order {
        let data_type = col_types.get(&name).copied().unwrap_or("Utf8");
        columns.push(ColumnInfo {
            name,
            data_type: data_type.to_string(),
            nullable: true,
        });
    }

    columns
}

async fn execute_query(
    State(state): State<AdminState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<QueryResult>::error(e)),
            );
        }
    };

    let start = Instant::now();

    // Forward query to data node's POST /query/:catalog
    let url = format!("{}/query/{}", node_url, request.catalog);
    let response = match state.http_client.post(&url).body(request.sql.clone()).send().await {
        Ok(r) => r,
        Err(e) => {
            // Record failed query in history
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            state
                .add_query_history(super::state::QueryHistoryEntry {
                    id: uuid::Uuid::new_v4().to_string(),
                    sql: request.sql,
                    catalog: request.catalog,
                    executed_at: chrono::Utc::now().to_rfc3339(),
                    execution_time_ms: elapsed,
                    row_count: 0,
                    status: "error".to_string(),
                    error: Some(format!("Failed to reach data node: {}", e)),
                })
                .await;
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<QueryResult>::error(format!(
                    "Failed to reach data node: {}",
                    e
                ))),
            );
        }
    };

    let status = response.status();
    let body = match response.text().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<QueryResult>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    let elapsed = start.elapsed().as_secs_f64() * 1000.0;

    if !status.is_success() {
        // Record failed query
        state
            .add_query_history(super::state::QueryHistoryEntry {
                id: uuid::Uuid::new_v4().to_string(),
                sql: request.sql,
                catalog: request.catalog,
                executed_at: chrono::Utc::now().to_rfc3339(),
                execution_time_ms: elapsed,
                row_count: 0,
                status: "error".to_string(),
                error: Some(body.clone()),
            })
            .await;
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<QueryResult>::error(body)),
        );
    }

    // Parse the response - data node returns a JSON array of row objects
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<QueryResult>::error(format!(
                    "Failed to parse query response: {}",
                    e
                ))),
            );
        }
    };

    // Transform response into QueryResult format
    let result = match value {
        serde_json::Value::Array(rows) => {
            let columns = extract_columns(&rows);
            let limit = request.limit.unwrap_or(1000);
            let total_rows = rows.len();
            let truncated = total_rows > limit;
            let row_objects: Vec<serde_json::Map<String, serde_json::Value>> = rows
                .into_iter()
                .take(limit)
                .filter_map(|v| {
                    if let serde_json::Value::Object(map) = v {
                        Some(map)
                    } else {
                        None
                    }
                })
                .collect();

            QueryResult {
                row_count: total_rows,
                columns,
                rows: row_objects,
                execution_time_ms: elapsed,
                truncated: if truncated { Some(true) } else { None },
            }
        }
        other => {
            // If the response is a single object or other format, wrap it
            let row_count = if other.is_object() { 1 } else { 0 };
            let rows = if let serde_json::Value::Object(map) = other {
                vec![map]
            } else {
                vec![]
            };
            QueryResult {
                columns: Vec::new(),
                rows,
                row_count,
                execution_time_ms: elapsed,
                truncated: None,
            }
        }
    };

    // Record successful query in history
    state
        .add_query_history(super::state::QueryHistoryEntry {
            id: uuid::Uuid::new_v4().to_string(),
            sql: request.sql,
            catalog: request.catalog,
            executed_at: chrono::Utc::now().to_rfc3339(),
            execution_time_ms: elapsed,
            row_count: result.row_count,
            status: "success".to_string(),
            error: None,
        })
        .await;

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

// ============ Query Explain ============

#[derive(Deserialize)]
struct ExplainQuery {
    sql: String,
    #[serde(default)]
    verbose: Option<bool>,
    #[serde(default)]
    analyze: Option<bool>,
}

#[derive(Serialize)]
struct QueryExplain {
    logical_plan: String,
    physical_plan: String,
    optimizations: Vec<String>,
}

async fn explain_query(
    State(state): State<AdminState>,
    Path(catalog): Path<String>,
    Query(query): Query<ExplainQuery>,
) -> impl IntoResponse {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<QueryExplain>::error(e)),
            );
        }
    };

    // Forward to data node's GET /explain/:catalog
    let verbose = query.verbose.unwrap_or(true);
    let analyze = query.analyze.unwrap_or(false);
    let url = format!(
        "{}/explain/{}?sql={}&verbose={}&analyze={}",
        node_url,
        catalog,
        urlencoding::encode(&query.sql),
        verbose,
        analyze
    );

    let response = match state.http_client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<QueryExplain>::error(format!(
                    "Failed to reach data node: {}",
                    e
                ))),
            );
        }
    };

    let status = response.status();
    let body = match response.text().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<QueryExplain>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<QueryExplain>::error(body)),
        );
    }

    // Parse the explain response
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            // If not JSON, treat as plain text plan
            let result = QueryExplain {
                logical_plan: body.clone(),
                physical_plan: String::new(),
                optimizations: Vec::new(),
            };
            return (StatusCode::OK, Json(ApiResponse::ok(result)));
        }
    };

    // Extract fields from the response
    let logical_plan = value
        .get("logical_plan")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let physical_plan = value
        .get("physical_plan")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Convert optimizations object to list of enabled optimization names
    let optimizations = extract_optimization_names(&value);

    let result = QueryExplain {
        logical_plan,
        physical_plan,
        optimizations,
    };

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

/// Extract optimization names from the explain/analyze response
fn extract_optimization_names(value: &serde_json::Value) -> Vec<String> {
    let mut names = Vec::new();

    if let Some(opts) = value.get("optimizations") {
        if let Some(obj) = opts.as_object() {
            // Object format: { "name": true/false }
            for (key, val) in obj {
                if val.as_bool().unwrap_or(false) {
                    names.push(key.clone());
                }
            }
        } else if let Some(arr) = opts.as_array() {
            // Array format: ["name1", "name2"]
            for item in arr {
                if let Some(s) = item.as_str() {
                    names.push(s.to_string());
                }
            }
        }
    }

    names
}

// ============ Query Analyze ============

#[derive(Deserialize)]
struct AnalyzeQuery {
    sql: String,
}

#[derive(Serialize)]
struct QueryAnalysis {
    tables: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_range: Option<serde_json::Value>,
    partitions_to_scan: usize,
    files_to_scan: usize,
    optimizations: Vec<String>,
    plan_summary: String,
}

async fn analyze_query(
    State(state): State<AdminState>,
    Path(catalog): Path<String>,
    Query(query): Query<AnalyzeQuery>,
) -> impl IntoResponse {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<QueryAnalysis>::error(e)),
            );
        }
    };

    let url = format!(
        "{}/analyze/{}?sql={}",
        node_url,
        catalog,
        urlencoding::encode(&query.sql)
    );

    let response = match state.http_client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<QueryAnalysis>::error(format!(
                    "Failed to reach data node: {}",
                    e
                ))),
            );
        }
    };

    let status = response.status();
    let body = match response.text().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<QueryAnalysis>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<QueryAnalysis>::error(body)),
        );
    }

    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<QueryAnalysis>::error(format!(
                    "Failed to parse analyze response: {}",
                    e
                ))),
            );
        }
    };

    let tables = value
        .get("tables")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let time_range = value.get("time_range").cloned();

    let partitions_to_scan = value
        .get("partitions_to_scan")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    let files_to_scan = value
        .get("files_to_scan")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    let optimizations = extract_optimization_names(&value);

    let plan_summary = value
        .get("plan_summary")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let result = QueryAnalysis {
        tables,
        time_range,
        partitions_to_scan,
        files_to_scan,
        optimizations,
        plan_summary,
    };

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

// ============ Catalog/Schema/Column Listing ============

async fn list_catalogs(State(state): State<AdminState>) -> impl IntoResponse {
    // Get unique catalogs from MetaClient's table list
    let meta_client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<String>>::error(e)),
            );
        }
    };

    match meta_client.list_tables(None, None).await {
        Ok(tables) => {
            let mut catalogs: Vec<String> = tables
                .iter()
                .map(|t| t.catalog.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            catalogs.sort();
            (StatusCode::OK, Json(ApiResponse::ok(catalogs)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Vec<String>>::error(format!(
                "Failed to list catalogs: {}",
                e
            ))),
        ),
    }
}

async fn list_schemas(
    State(state): State<AdminState>,
    Path(catalog): Path<String>,
) -> impl IntoResponse {
    // Try forwarding to data node first for accurate schema list
    if let Ok(node_url) = state.get_data_node_url().await {
        let url = format!("{}/schemas/{}", node_url, catalog);
        if let Ok(response) = state.http_client.get(&url).send().await {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) {
                        if let Some(schemas) = value.get("schemas").and_then(|v| v.as_array()) {
                            let schema_list: Vec<String> = schemas
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                            return (StatusCode::OK, Json(ApiResponse::ok(schema_list)));
                        }
                    }
                }
            }
        }
    }

    // Fallback: derive from MetaClient tables
    let meta_client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<String>>::error(e)),
            );
        }
    };

    match meta_client.list_tables(Some(&catalog), None).await {
        Ok(tables) => {
            let mut schemas: Vec<String> = tables
                .iter()
                .map(|t| t.schema.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            schemas.sort();
            (StatusCode::OK, Json(ApiResponse::ok(schemas)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Vec<String>>::error(format!(
                "Failed to list schemas: {}",
                e
            ))),
        ),
    }
}

#[derive(Serialize)]
struct ColumnDetail {
    name: String,
    data_type: String,
}

async fn get_columns(
    State(state): State<AdminState>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<ColumnDetail>>::error(e)),
            );
        }
    };

    // Forward to data node's GET /schema/:db/:schema/:table
    let url = format!("{}/schema/{}/{}/{}", node_url, catalog, schema, table);
    let response = match state.http_client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<Vec<ColumnDetail>>::error(format!(
                    "Failed to reach data node: {}",
                    e
                ))),
            );
        }
    };

    let status = response.status();
    let body = match response.text().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<Vec<ColumnDetail>>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    if !status.is_success() || body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<Vec<ColumnDetail>>::error(if body.is_empty() {
                "Table not found or no schema available".to_string()
            } else {
                body
            })),
        );
    }

    // The data node returns the schema in Rust Debug format (DFSchema { ... })
    // Try JSON first, then fall back to parsing the debug text format
    let columns = if let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) {
        // JSON format
        if let Some(fields) = value.get("fields").and_then(|v| v.as_array()) {
            fields
                .iter()
                .filter_map(|f| {
                    let name = f.get("name")?.as_str()?.to_string();
                    let data_type = f
                        .get("data_type")
                        .or_else(|| f.get("type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown")
                        .to_string();
                    Some(ColumnDetail { name, data_type })
                })
                .collect()
        } else {
            Vec::new()
        }
    } else {
        // Parse Rust Debug format: extract Field { name: "...", data_type: ..., ... } blocks
        parse_debug_schema(&body)
    };

    (StatusCode::OK, Json(ApiResponse::ok(columns)))
}

/// Parse column info from Rust's Debug format of DFSchema/Arrow Schema.
/// Extracts Field { name: "col_name", data_type: TypeName, ... } blocks.
fn parse_debug_schema(text: &str) -> Vec<ColumnDetail> {
    let mut columns = Vec::new();
    let mut in_field = false;
    let mut current_name = String::new();
    let mut current_type = String::new();

    for line in text.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("Field {") || trimmed == "Field {" {
            in_field = true;
            current_name.clear();
            current_type.clear();
            continue;
        }

        if in_field {
            if trimmed.starts_with("name:") {
                // Extract name: "column_name",
                if let Some(start) = trimmed.find('"') {
                    if let Some(end) = trimmed[start + 1..].find('"') {
                        current_name = trimmed[start + 1..start + 1 + end].to_string();
                    }
                }
            } else if trimmed.starts_with("data_type:") {
                // Extract data_type: TypeName,
                current_type = trimmed
                    .trim_start_matches("data_type:")
                    .trim()
                    .trim_end_matches(',')
                    .to_string();
            } else if trimmed == "}," || trimmed == "}" {
                if !current_name.is_empty() {
                    columns.push(ColumnDetail {
                        name: current_name.clone(),
                        data_type: current_type.clone(),
                    });
                }
                in_field = false;
            }
        }
    }

    columns
}

// ============ Saved Queries ============

async fn list_saved_queries(State(state): State<AdminState>) -> impl IntoResponse {
    let queries = state.list_saved_queries().await;
    (StatusCode::OK, Json(ApiResponse::ok(queries)))
}

#[derive(Deserialize)]
struct SaveQueryRequest {
    name: String,
    sql: String,
    catalog: String,
}

async fn save_query(
    State(state): State<AdminState>,
    Json(request): Json<SaveQueryRequest>,
) -> impl IntoResponse {
    let query = state
        .save_query(request.name, request.sql, request.catalog)
        .await;
    (StatusCode::OK, Json(ApiResponse::ok(query)))
}

async fn delete_saved_query(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let deleted = state.delete_saved_query(&id).await;
    if deleted {
        (
            StatusCode::OK,
            Json(ApiResponse::ok(serde_json::json!({ "success": true }))),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<serde_json::Value>::error("Saved query not found")),
        )
    }
}

// ============ Query History ============

#[derive(Deserialize, Default)]
struct HistoryQuery {
    #[serde(default = "default_history_limit")]
    limit: usize,
}

fn default_history_limit() -> usize {
    100
}

async fn get_query_history(
    State(state): State<AdminState>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    let history = state.get_query_history(query.limit).await;
    (StatusCode::OK, Json(ApiResponse::ok(history)))
}

async fn clear_query_history(State(state): State<AdminState>) -> impl IntoResponse {
    state.clear_query_history().await;
    (
        StatusCode::OK,
        Json(ApiResponse::ok(serde_json::json!({ "success": true }))),
    )
}
