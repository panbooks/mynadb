//! Storage Browser API handlers
//!
//! Provides storage browsing, partition management, and compaction triggers.
//! Uses MetaClient for catalog/schema/table aggregation and forwards
//! partition/file operations to data nodes.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::state::AdminState;

// ============ Routes ============

pub fn routes() -> Router<AdminState> {
    Router::new()
        .route("/storage/info", get(storage_info))
        .route("/storage/catalogs", get(list_storage_catalogs))
        .route(
            "/storage/catalogs/:catalog/schemas",
            get(list_storage_schemas),
        )
        .route(
            "/storage/catalogs/:catalog/schemas/:schema/tables",
            get(list_storage_tables),
        )
        .route(
            "/storage/catalogs/:catalog/schemas/:schema/tables/:table",
            get(get_table_storage),
        )
        .route("/storage/file", get(get_parquet_details))
        .route(
            "/storage/catalogs/:catalog/schemas/:schema/tables/:table/partitions/:date",
            delete(delete_storage_partition),
        )
        .route("/storage/compact/:catalog", post(compact_catalog))
        .route(
            "/storage/compact/:catalog/:schema",
            post(compact_schema),
        )
        .route(
            "/storage/compact/:catalog/:schema/:table",
            post(compact_table),
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

// ============ Storage Info ============

#[derive(Serialize)]
struct StorageInfo {
    backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    total_size_bytes: u64,
    file_count: u64,
}

async fn storage_info(State(state): State<AdminState>) -> impl IntoResponse {
    // Aggregate storage info from MetaClient tables
    let meta_client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<StorageInfo>::error(e)),
            );
        }
    };

    let tables = match meta_client.list_tables(None, None).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<StorageInfo>::error(format!(
                    "Failed to list tables: {}",
                    e
                ))),
            );
        }
    };

    let total_size_bytes: u64 = tables.iter().map(|t| t.total_bytes).sum();
    let file_count: u64 = tables.iter().map(|t| t.partition_count as u64).sum();

    // Try to get storage backend info from a data node
    let mut backend = "unknown".to_string();
    if let Ok(node_url) = state.get_data_node_url().await {
        if let Ok(response) = state.http_client.get(format!("{}/health", node_url)).send().await {
            if response.status().is_success() {
                backend = "local".to_string(); // Default assumption; real info from config
            }
        }
    }

    let result = StorageInfo {
        backend,
        bucket: None,
        endpoint: None,
        total_size_bytes,
        file_count,
    };

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

// ============ Storage Catalogs ============

#[derive(Serialize)]
struct CatalogInfo {
    name: String,
    schemas: u32,
    tables: u32,
    partitions: u32,
    total_size_bytes: u64,
}

async fn list_storage_catalogs(State(state): State<AdminState>) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<CatalogInfo>>::error(e)),
            );
        }
    };

    let tables = match meta_client.list_tables(None, None).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Vec<CatalogInfo>>::error(format!(
                    "Failed to list tables: {}",
                    e
                ))),
            );
        }
    };

    // Group tables by catalog
    let mut catalog_map: HashMap<String, CatalogInfo> = HashMap::new();
    for table in &tables {
        let entry = catalog_map
            .entry(table.catalog.clone())
            .or_insert_with(|| CatalogInfo {
                name: table.catalog.clone(),
                schemas: 0,
                tables: 0,
                partitions: 0,
                total_size_bytes: 0,
            });
        entry.tables += 1;
        entry.partitions += table.partition_count;
        entry.total_size_bytes += table.total_bytes;
    }

    // Count unique schemas per catalog
    let mut schema_sets: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    for table in &tables {
        schema_sets
            .entry(table.catalog.clone())
            .or_default()
            .insert(table.schema.clone());
    }
    for (catalog, schemas) in schema_sets {
        if let Some(info) = catalog_map.get_mut(&catalog) {
            info.schemas = schemas.len() as u32;
        }
    }

    let mut catalogs: Vec<CatalogInfo> = catalog_map.into_values().collect();
    catalogs.sort_by(|a, b| a.name.cmp(&b.name));

    (StatusCode::OK, Json(ApiResponse::ok(catalogs)))
}

// ============ Storage Schemas ============

#[derive(Serialize)]
struct SchemaInfo {
    catalog: String,
    name: String,
    tables: u32,
    partitions: u32,
    total_size_bytes: u64,
}

async fn list_storage_schemas(
    State(state): State<AdminState>,
    Path(catalog): Path<String>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<SchemaInfo>>::error(e)),
            );
        }
    };

    let tables = match meta_client.list_tables(Some(&catalog), None).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Vec<SchemaInfo>>::error(format!(
                    "Failed to list tables: {}",
                    e
                ))),
            );
        }
    };

    // Group by schema
    let mut schema_map: HashMap<String, SchemaInfo> = HashMap::new();
    for table in &tables {
        let entry = schema_map
            .entry(table.schema.clone())
            .or_insert_with(|| SchemaInfo {
                catalog: catalog.clone(),
                name: table.schema.clone(),
                tables: 0,
                partitions: 0,
                total_size_bytes: 0,
            });
        entry.tables += 1;
        entry.partitions += table.partition_count;
        entry.total_size_bytes += table.total_bytes;
    }

    let mut schemas: Vec<SchemaInfo> = schema_map.into_values().collect();
    schemas.sort_by(|a, b| a.name.cmp(&b.name));

    (StatusCode::OK, Json(ApiResponse::ok(schemas)))
}

// ============ Tables in Schema ============

#[derive(Serialize)]
struct TableBasicInfo {
    name: String,
    partition_count: u32,
    total_size_bytes: u64,
    total_rows: u64,
}

async fn list_storage_tables(
    State(state): State<AdminState>,
    Path((catalog, schema)): Path<(String, String)>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<TableBasicInfo>>::error(e)),
            );
        }
    };

    let tables = match meta_client
        .list_tables(Some(&catalog), Some(&schema))
        .await
    {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Vec<TableBasicInfo>>::error(format!(
                    "Failed to list tables: {}",
                    e
                ))),
            );
        }
    };

    let mut result: Vec<TableBasicInfo> = tables
        .iter()
        .map(|t| TableBasicInfo {
            name: t.table.clone(),
            partition_count: t.partition_count,
            total_size_bytes: t.total_bytes,
            total_rows: t.total_rows,
        })
        .collect();

    result.sort_by(|a, b| a.name.cmp(&b.name));

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

// ============ Table Storage Details ============

#[derive(Serialize)]
struct TableStorageInfo {
    catalog: String,
    schema: String,
    name: String,
    partitions: Vec<PartitionInfo>,
    total_size_bytes: u64,
    total_rows: u64,
    file_count: u64,
}

#[derive(Serialize)]
struct PartitionInfo {
    date: String,
    files: Vec<FileInfo>,
    size_bytes: u64,
    row_count: u64,
}

#[derive(Serialize)]
struct FileInfo {
    path: String,
    name: String,
    level: u32,
    size_bytes: u64,
    row_count: u64,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_groups: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<String>,
}

/// Parse column info from the data node's schema response.
/// The response can be JSON or Rust Debug format (DFSchema { ... }).
fn parse_schema_columns(text: &str) -> Vec<ParquetColumnInfo> {
    // Try JSON first
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Some(fields) = value.get("fields").and_then(|v| v.as_array()) {
            return fields
                .iter()
                .filter_map(|f| {
                    let name = f.get("name")?.as_str()?.to_string();
                    let data_type = f
                        .get("data_type")
                        .or_else(|| f.get("type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown")
                        .to_string();
                    Some(ParquetColumnInfo {
                        name,
                        data_type,
                        nullable: true,
                    })
                })
                .collect();
        }
    }

    // Parse Rust Debug format
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
        } else if in_field {
            if trimmed.starts_with("name:") {
                if let Some(start) = trimmed.find('"') {
                    if let Some(end) = trimmed[start + 1..].find('"') {
                        current_name = trimmed[start + 1..start + 1 + end].to_string();
                    }
                }
            } else if trimmed.starts_with("data_type:") {
                current_type = trimmed
                    .trim_start_matches("data_type:")
                    .trim()
                    .trim_end_matches(',')
                    .to_string();
            } else if trimmed == "}," || trimmed == "}" {
                if !current_name.is_empty() {
                    columns.push(ParquetColumnInfo {
                        name: current_name.clone(),
                        data_type: current_type.clone(),
                        nullable: true,
                    });
                }
                in_field = false;
            }
        }
    }

    columns
}

/// Parse compaction level from file name (e.g., "L0_xxx.parquet" → 0)
fn parse_level(filename: &str) -> u32 {
    if let Some(rest) = filename.strip_prefix('L') {
        if let Some(level_str) = rest.split('_').next() {
            return level_str.parse().unwrap_or(0);
        }
    }
    0
}

async fn get_table_storage(
    State(state): State<AdminState>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<TableStorageInfo>::error(e)),
            );
        }
    };

    // Forward to data node's GET /partitions/:catalog/:schema/:table
    let url = format!(
        "{}/partitions/{}/{}/{}",
        node_url, catalog, schema, table
    );

    let response = match state.http_client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<TableStorageInfo>::error(format!(
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
                Json(ApiResponse::<TableStorageInfo>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<TableStorageInfo>::error(body)),
        );
    }

    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<TableStorageInfo>::error(format!(
                    "Failed to parse response: {}",
                    e
                ))),
            );
        }
    };

    // Transform the data node's partition response into our format
    // Data node returns: { partitions: [{partition, date, file_count, size_mb}], count, total_size_mb }
    let mut partitions = Vec::new();
    let mut total_size_bytes: u64 = 0;
    let mut total_rows: u64 = 0;
    let mut total_file_count: u64 = 0;

    if let Some(parts) = value.get("partitions").and_then(|v| v.as_array()) {
        for p in parts {
            let date = p
                .get("date")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let file_count = p
                .get("file_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let size_mb = p
                .get("size_mb")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let size_bytes = (size_mb * 1024.0 * 1024.0) as u64;

            total_size_bytes += size_bytes;
            total_file_count += file_count;

            // Create file entries - we don't have individual file details from the partition list,
            // so create summary entries per file count
            let files: Vec<FileInfo> = (0..file_count)
                .map(|i| FileInfo {
                    path: format!(
                        "{}/{}/{}/date={}/file_{}.parquet",
                        catalog, schema, table, date, i
                    ),
                    name: format!("file_{}.parquet", i),
                    level: 0,
                    size_bytes: if file_count > 0 {
                        size_bytes / file_count
                    } else {
                        0
                    },
                    row_count: 0,
                    created_at: String::new(),
                    row_groups: None,
                    compression: None,
                })
                .collect();

            partitions.push(PartitionInfo {
                date,
                files,
                size_bytes,
                row_count: 0,
            });
        }
    }

    // Fallback: if /partitions returned empty (common with object store backends),
    // query partition info via SQL
    if partitions.is_empty() {
        if let Ok(sql_partitions) =
            query_partitions_via_sql(&state, &node_url, &catalog, &schema, &table).await
        {
            partitions = sql_partitions.0;
            total_file_count = sql_partitions.1;
            total_rows = sql_partitions.2;
        }
    }

    // Try to get total_rows and total_size from meta
    if let Ok(meta_client) = state.get_meta_client().await {
        if let Ok(tables) = meta_client
            .list_tables(Some(&catalog), Some(&schema))
            .await
        {
            if let Some(t) = tables.iter().find(|t| t.table == table) {
                if total_rows == 0 {
                    total_rows = t.total_rows;
                }
                if total_size_bytes == 0 {
                    total_size_bytes = t.total_bytes;
                }
            }
        }
    }

    let result = TableStorageInfo {
        catalog,
        schema,
        name: table,
        partitions,
        total_size_bytes,
        total_rows,
        file_count: total_file_count,
    };

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

/// Query partition info via SQL when /partitions endpoint returns empty.
/// Returns (partitions, total_file_count, total_rows).
async fn query_partitions_via_sql(
    state: &AdminState,
    node_url: &str,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<(Vec<PartitionInfo>, u64, u64), String> {
    let sql = format!(
        "SELECT date, count(*) as row_count FROM {}.{} GROUP BY date ORDER BY date",
        schema, table
    );
    let encoded_sql: String = sql
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            b' ' => "+".to_string(),
            _ => format!("%{:02X}", b),
        })
        .collect();
    let url = format!("{}/query/{}?sql={}", node_url, catalog, encoded_sql);

    let response = state
        .http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("SQL query failed: {}", e))?;

    if !response.status().is_success() {
        return Err("SQL query returned error".to_string());
    }

    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read SQL response: {}", e))?;

    // Data node query returns an array of row objects: [{date, row_count}, ...]
    // or a wrapper: {success, data: [...]}
    let rows: Vec<serde_json::Value> = if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&body) {
        arr
    } else if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(&body) {
        wrapper
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default()
    } else {
        return Err("Failed to parse SQL response".to_string());
    };

    let mut partitions = Vec::new();
    let mut total_rows: u64 = 0;

    for row in &rows {
        let date = row
            .get("date")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let row_count = row
            .get("row_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        total_rows += row_count;

        partitions.push(PartitionInfo {
            date: date.clone(),
            files: vec![FileInfo {
                path: format!("{}/{}/{}/date={}", catalog, schema, table, date),
                name: "*.parquet".to_string(),
                level: 0,
                size_bytes: 0,
                row_count,
                created_at: String::new(),
                row_groups: None,
                compression: None,
            }],
            size_bytes: 0,
            row_count,
        });
    }

    let total_file_count = partitions.len() as u64;
    Ok((partitions, total_file_count, total_rows))
}

// ============ Parquet File Details ============

#[derive(Deserialize)]
struct FileQuery {
    path: String,
}

#[derive(Serialize)]
struct ParquetFileDetails {
    path: String,
    schema: Vec<ParquetColumnInfo>,
    row_count: u64,
    row_groups: u32,
    file_size_bytes: u64,
    compression: String,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    statistics: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct ParquetColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
}

async fn get_parquet_details(
    State(state): State<AdminState>,
    Query(query): Query<FileQuery>,
) -> impl IntoResponse {
    // Try to get file details from a data node
    // Note: The data node may not have a dedicated parquet details endpoint,
    // so we attempt to use the schema endpoint as a proxy
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ParquetFileDetails>::error(e)),
            );
        }
    };

    // Parse table info from the path: catalog/schema/table/date=.../file.parquet
    let path_parts: Vec<&str> = query.path.split('/').collect();
    let filename = path_parts.last().unwrap_or(&"");
    let _level = parse_level(filename);

    // Return basic info derived from the path
    let result = ParquetFileDetails {
        path: query.path.clone(),
        schema: Vec::new(),
        row_count: 0,
        row_groups: 0,
        file_size_bytes: 0,
        compression: "ZSTD".to_string(),
        created_at: String::new(),
        statistics: None,
    };

    // Try to get schema info if we can parse the table path
    if path_parts.len() >= 3 {
        let catalog = path_parts[0];
        let schema_name = path_parts[1];
        let table = path_parts[2];

        let url = format!("{}/schema/{}/{}/{}", node_url, catalog, schema_name, table);
        if let Ok(response) = state.http_client.get(&url).send().await {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    if !body.is_empty() {
                        let columns = parse_schema_columns(&body);
                        if !columns.is_empty() {
                            let enriched = ParquetFileDetails {
                                schema: columns,
                                ..result
                            };
                            return (StatusCode::OK, Json(ApiResponse::ok(enriched)));
                        }
                    }
                }
            }
        }
    }

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

// ============ Delete Partition ============

#[derive(Serialize)]
struct DeletePartitionResponse {
    success: bool,
    deleted_files: u64,
    freed_bytes: u64,
}

async fn delete_storage_partition(
    State(state): State<AdminState>,
    Path((catalog, schema, table, date)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<DeletePartitionResponse>::error(e)),
            );
        }
    };

    // Forward to data node's DELETE /partitions/:catalog/:schema/:table/:date
    let url = format!(
        "{}/partitions/{}/{}/{}/{}",
        node_url, catalog, schema, table, date
    );

    let response = match state.http_client.delete(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<DeletePartitionResponse>::error(format!(
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
                Json(ApiResponse::<DeletePartitionResponse>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<DeletePartitionResponse>::error(body)),
        );
    }

    let result = DeletePartitionResponse {
        success: true,
        deleted_files: 0,
        freed_bytes: 0,
    };

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}

// ============ Compaction ============

#[derive(Serialize)]
struct CompactionResponse {
    success: bool,
    message: String,
}

async fn compact_catalog(
    State(state): State<AdminState>,
    Path(catalog): Path<String>,
) -> impl IntoResponse {
    forward_compaction(&state, &format!("/compact/{}", catalog)).await
}

async fn compact_schema(
    State(state): State<AdminState>,
    Path((catalog, schema)): Path<(String, String)>,
) -> impl IntoResponse {
    forward_compaction(
        &state,
        &format!("/compact/{}/{}", catalog, schema),
    )
    .await
}

async fn compact_table(
    State(state): State<AdminState>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
) -> impl IntoResponse {
    forward_compaction(
        &state,
        &format!("/compact/{}/{}/{}", catalog, schema, table),
    )
    .await
}

async fn forward_compaction(
    state: &AdminState,
    path: &str,
) -> (StatusCode, Json<ApiResponse<CompactionResponse>>) {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<CompactionResponse>::error(e)),
            );
        }
    };

    let url = format!("{}{}", node_url, path);
    let response = match state.http_client.post(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ApiResponse::<CompactionResponse>::error(format!(
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
                Json(ApiResponse::<CompactionResponse>::error(format!(
                    "Failed to read response: {}",
                    e
                ))),
            );
        }
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<CompactionResponse>::error(body)),
        );
    }

    let result = CompactionResponse {
        success: true,
        message: body,
    };

    (StatusCode::OK, Json(ApiResponse::ok(result)))
}
