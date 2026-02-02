//! Router-specific HTTP API handlers
//!
//! Provides HTTP endpoints for the Router node that route requests
//! through WriteRouter and QueryCoordinator to Data nodes.

use super::state::RouterAppState;
use crate::utils::json::batches_to_json;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Request/Response Types
// ============================================================================

/// Query parameters for SQL queries (GET request)
#[derive(Debug, Deserialize)]
pub struct QueryParams {
    /// SQL query string
    #[serde(default)]
    pub sql: Option<String>,
    /// Query format (json, csv, etc.)
    #[serde(default)]
    pub format: Option<String>,
}

/// Query request body for POST requests
#[derive(Debug, Deserialize)]
pub struct QueryRequestBody {
    /// SQL query string
    pub sql: String,
}

/// Query response
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    pub rows: u64,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Write response
#[derive(Debug, Serialize)]
pub struct WriteResponse {
    pub success: bool,
    pub points_written: u64,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Router status response
#[derive(Debug, Serialize)]
pub struct RouterStatus {
    pub node_type: &'static str,
    pub node_id: String,
    pub active_data_nodes: usize,
    pub meta_connected: bool,
    pub http_port: u16,
    pub flight_port: u16,
    pub mysql_enabled: bool,
}

// ============================================================================
// Router Builder
// ============================================================================

/// Build Router node HTTP API
pub fn build_router(state: RouterAppState) -> Router {
    Router::new()
        // Basic endpoints
        .route("/", get(home))
        .route("/health", get(health))
        .route("/version", get(version_handler))
        // Write endpoint - routes to Data nodes
        .route("/write/:db", post(write_handler))
        .route("/write", post(write_handler_query_param))
        // Query endpoint - distributed query via Data nodes
        .route("/query/:db", get(query_handler))
        .route("/query/:db", post(query_handler_post))
        // Schema/Table endpoints (forwarded to Data nodes)
        .route("/schemas/:db", get(list_schemas_handler))
        .route("/tables/:db", get(list_tables_handler))
        .route("/tables/:db/:schema", get(list_schema_tables_handler))
        .route("/schema/:db/:schema/:table", get(schema_handler))
        // Partition endpoint (forwarded to Data nodes)
        .route(
            "/partitions/:catalog/:schema/:table",
            get(list_partitions_handler),
        )
        // Memory stats (aggregated from all Data nodes)
        .route("/memory", get(memory_handler))
        // Router-specific endpoints
        .route("/router/status", get(router_status_handler))
        .route("/router/nodes", get(list_data_nodes_handler))
        .route("/stats", get(stats_handler))
        .route("/flush", post(flush_handler))
        .with_state(state)
}

// ============================================================================
// Basic Handlers
// ============================================================================

/// Home page
pub async fn home() -> &'static str {
    "MynaDB Router Node\n\n\
     Basic Endpoints:\n\
       GET  /health              - Health check\n\
       GET  /version             - Version info\n\n\
     Write/Query Endpoints:\n\
       POST /write/:db           - Write data (InfluxDB line protocol)\n\
       GET/POST /query/:db       - Execute SQL query\n\n\
     Schema/Table Endpoints:\n\
       GET  /schemas/:db         - List schemas\n\
       GET  /tables/:db          - List all tables\n\
       GET  /tables/:db/:schema  - List tables in schema\n\
       GET  /schema/:db/:schema/:table - Get table schema\n\n\
     Partition Endpoints:\n\
       GET  /partitions/:catalog/:schema/:table - List partitions\n\n\
     Memory/Stats Endpoints:\n\
       GET  /memory              - Memory statistics (aggregated)\n\
       GET  /stats               - Router statistics\n\n\
     Management Endpoints:\n\
       POST /flush               - Flush all data nodes\n\
       GET  /router/status       - Router status\n\
       GET  /router/nodes        - List data nodes\n"
}

/// Health check
pub async fn health() -> &'static str {
    "OK"
}

/// Version information
pub async fn version_handler() -> impl IntoResponse {
    let version_info = serde_json::json!({
        "version": env!("GIT_VERSION"),
        "git_hash": env!("GIT_HASH"),
        "build_time": env!("BUILD_TIME"),
        "node_type": "router"
    });
    (StatusCode::OK, Json(version_info))
}

// ============================================================================
// Write Handlers
// ============================================================================

/// Write handler - routes writes to Data nodes
pub async fn write_handler(
    Path(db): Path<String>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    let result = state.write_router.route_write(&db, &body).await;

    let response = WriteResponse {
        success: result.success,
        points_written: result.points_written,
        elapsed_ms: result.elapsed_ms,
        error: result.error,
    };

    if result.success {
        (StatusCode::NO_CONTENT, Json(response))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

/// Write handler with database in query parameter
pub async fn write_handler_query_param(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    let db = params
        .get("db")
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    let result = state.write_router.route_write(&db, &body).await;

    let response = WriteResponse {
        success: result.success,
        points_written: result.points_written,
        elapsed_ms: result.elapsed_ms,
        error: result.error,
    };

    if result.success {
        (StatusCode::NO_CONTENT, Json(response))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

// ============================================================================
// Query Handlers
// ============================================================================

/// Query handler - distributed query across Data nodes (GET)
pub async fn query_handler(
    Path(db): Path<String>,
    Query(params): Query<QueryParams>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let sql = match params.sql {
        Some(sql) => sql,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(QueryResponse {
                    success: false,
                    data: None,
                    rows: 0,
                    elapsed_ms: 0,
                    error: Some("Missing 'sql' query parameter".to_string()),
                }),
            );
        }
    };

    execute_query(&state, &db, &sql).await
}

/// Query handler - distributed query across Data nodes (POST)
/// Accepts JSON body: {"sql": "SELECT ..."}
/// Or plain text body with just the SQL query
pub async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    // Try to parse as JSON first, then fall back to treating body as raw SQL
    let sql = if let Ok(request) = serde_json::from_str::<QueryRequestBody>(&body) {
        request.sql
    } else {
        // Treat body as raw SQL
        body
    };
    execute_query(&state, &db, &sql).await
}

/// Execute a distributed query
async fn execute_query(
    state: &RouterAppState,
    db: &str,
    sql: &str,
) -> (StatusCode, Json<QueryResponse>) {
    let result = state.query_coordinator.execute_query(db, sql).await;

    // Convert batches to JSON
    let data = if result.batches.is_empty() {
        Some(serde_json::json!([]))
    } else {
        match batches_to_json(&result.batches) {
            Ok(rows) => Some(serde_json::Value::Array(rows)),
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse {
                        success: false,
                        data: None,
                        rows: 0,
                        elapsed_ms: result.elapsed_ms,
                        error: Some(format!("JSON conversion error: {}", e)),
                    }),
                );
            }
        }
    };

    let response = QueryResponse {
        success: result.success,
        data,
        rows: result.total_rows,
        elapsed_ms: result.elapsed_ms,
        error: result.error,
    };

    if result.success {
        (StatusCode::OK, Json(response))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

// ============================================================================
// Router Status Handlers
// ============================================================================

/// Router status handler
pub async fn router_status_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    let status = RouterStatus {
        node_type: "router",
        node_id: state.router_config.node_id.clone(),
        active_data_nodes: state.active_data_node_count(),
        meta_connected: state.is_meta_connected(),
        http_port: state.router_config.http_port,
        flight_port: state.router_config.flight_port,
        mysql_enabled: state.router_config.mysql_enabled,
    };

    (StatusCode::OK, Json(status))
}

/// List active Primary Data nodes (only Primary nodes serve traffic)
pub async fn list_data_nodes_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    use super::routing::shard_cache::NodeState;
    use crate::meta::types::DataNodeMode;

    // Only show Primary nodes - Standby nodes are cold backups and don't serve traffic
    let nodes = state.shard_cache.get_active_primary_nodes();

    let node_list: Vec<_> = nodes
        .iter()
        .map(|n| {
            serde_json::json!({
                "node_id": n.node_id,
                "grpc_addr": n.grpc_addr,
                "state": match n.state {
                    NodeState::Active => "active",
                    NodeState::Draining => "draining",
                    NodeState::Unhealthy => "unhealthy",
                    NodeState::Suspect => "suspect",
                    NodeState::Retiring => "retiring",
                    NodeState::Drained => "drained",
                },
                "mode": match n.mode {
                    DataNodeMode::Primary => "primary",
                    DataNodeMode::Standby => "standby",
                },
            })
        })
        .collect();

    (StatusCode::OK, Json(node_list))
}

/// Stats handler - aggregate stats from data nodes
pub async fn stats_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    let primary_nodes = state.shard_cache.get_active_primary_nodes();
    let all_nodes = state.shard_cache.get_active_data_nodes();

    let stats = serde_json::json!({
        "node_type": "router",
        "node_id": state.router_config.node_id,
        "active_primary_nodes": primary_nodes.len(),
        "active_standby_nodes": all_nodes.len() - primary_nodes.len(),
        "active_data_nodes": all_nodes.len(),
        "meta_connected": state.is_meta_connected(),
        "queries_routed": 0,  // TODO: Add query counter
        "writes_routed": 0,   // TODO: Add write counter
    });

    (StatusCode::OK, Json(stats))
}

/// Flush handler - broadcast flush to all data nodes
pub async fn flush_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    // Broadcast flush to all data nodes via Flight
    let result = state.write_router.broadcast_flush().await;

    match result {
        Ok(count) => {
            let response = serde_json::json!({
                "success": true,
                "nodes_flushed": count,
            });
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            let response = serde_json::json!({
                "success": false,
                "error": e,
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        }
    }
}

// ============================================================================
// Schema/Table Handlers (forwarded to Data nodes)
// ============================================================================

/// List tables - broadcast to Primary nodes and combine results
pub async fn list_tables_handler(
    Path(db): Path<String>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_primary_nodes();
    if nodes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "No active data nodes"})),
        );
    }

    // Collect tables from all nodes and deduplicate
    let mut all_tables = std::collections::HashSet::new();
    let mut errors = Vec::new();

    for node in &nodes {
        match state.data_node_pool.list_tables(&node.node_id, &db).await {
            Ok(result) => {
                if let Some(tables) = result["tables"].as_array() {
                    for table in tables {
                        if let Some(t) = table.as_str() {
                            all_tables.insert(t.to_string());
                        }
                    }
                }
            }
            Err(e) => errors.push(format!("{}: {}", node.node_id, e)),
        }
    }

    let mut tables: Vec<String> = all_tables.into_iter().collect();
    tables.sort();

    let response = serde_json::json!({
        "database": db,
        "tables": tables,
        "count": tables.len(),
        "nodes_queried": nodes.len(),
        "errors": if errors.is_empty() { None } else { Some(errors) }
    });

    (StatusCode::OK, Json(response))
}

/// List schemas - broadcast to Primary nodes and combine results
pub async fn list_schemas_handler(
    Path(db): Path<String>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_primary_nodes();
    if nodes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "No active data nodes"})),
        );
    }

    // Collect schemas from all nodes and deduplicate
    let mut all_schemas = std::collections::HashSet::new();
    let mut errors = Vec::new();

    for node in &nodes {
        match state.data_node_pool.list_schemas(&node.node_id, &db).await {
            Ok(result) => {
                if let Some(schemas) = result["schemas"].as_array() {
                    for schema in schemas {
                        if let Some(s) = schema.as_str() {
                            all_schemas.insert(s.to_string());
                        }
                    }
                }
            }
            Err(e) => errors.push(format!("{}: {}", node.node_id, e)),
        }
    }

    let mut schemas: Vec<String> = all_schemas.into_iter().collect();
    schemas.sort();

    let response = serde_json::json!({
        "database": db,
        "schemas": schemas,
        "count": schemas.len(),
        "nodes_queried": nodes.len(),
        "errors": if errors.is_empty() { None } else { Some(errors) }
    });

    (StatusCode::OK, Json(response))
}

/// List tables in a specific schema - broadcast to Primary nodes and combine results
pub async fn list_schema_tables_handler(
    Path((db, schema)): Path<(String, String)>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_primary_nodes();
    if nodes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "No active data nodes"})),
        );
    }

    // Collect tables from all nodes and deduplicate
    let mut all_tables = std::collections::HashSet::new();
    let mut errors = Vec::new();

    for node in &nodes {
        match state
            .data_node_pool
            .list_schema_tables(&node.node_id, &db, &schema)
            .await
        {
            Ok(result) => {
                if let Some(tables) = result["tables"].as_array() {
                    for table in tables {
                        if let Some(t) = table.as_str() {
                            all_tables.insert(t.to_string());
                        }
                    }
                }
            }
            Err(e) => errors.push(format!("{}: {}", node.node_id, e)),
        }
    }

    let mut tables: Vec<String> = all_tables.into_iter().collect();
    tables.sort();

    let response = serde_json::json!({
        "database": db,
        "schema": schema,
        "tables": tables,
        "count": tables.len(),
        "nodes_queried": nodes.len(),
        "errors": if errors.is_empty() { None } else { Some(errors) }
    });

    (StatusCode::OK, Json(response))
}

/// Get table schema - try Primary nodes until one succeeds (not all nodes have all tables)
pub async fn schema_handler(
    Path((db, schema, table)): Path<(String, String, String)>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_primary_nodes();
    if nodes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "No active data nodes".to_string(),
        );
    }

    // Construct full table path: schema.table
    let full_table = format!("{}.{}", schema, table);

    // Try each node until one succeeds (table might only be on some nodes)
    let mut last_error = String::new();
    for node in &nodes {
        match state
            .data_node_pool
            .get_table_schema(&node.node_id, &db, &full_table)
            .await
        {
            Ok(schema) => return (StatusCode::OK, schema),
            Err(e) => last_error = e,
        }
    }

    (StatusCode::NOT_FOUND, last_error)
}

// ============================================================================
// Partition Handlers (forwarded to Data nodes)
// ============================================================================

/// List partitions - broadcast to Primary nodes and combine results
pub async fn list_partitions_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_primary_nodes();
    if nodes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "No active data nodes"})),
        );
    }

    // Collect partitions from all nodes and combine
    // Use a map to deduplicate by partition name while keeping the richest info
    let mut partition_map: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    let mut errors = Vec::new();

    for node in &nodes {
        match state
            .data_node_pool
            .list_partitions(&node.node_id, &catalog, &schema, &table)
            .await
        {
            Ok(result) => {
                if let Some(partitions) = result["partitions"].as_array() {
                    for partition in partitions {
                        if let Some(part_name) = partition["partition"].as_str() {
                            // If this partition already exists, merge file_count and size
                            if let Some(existing) = partition_map.get_mut(part_name) {
                                let existing_count =
                                    existing["file_count"].as_u64().unwrap_or(0);
                                let new_count = partition["file_count"].as_u64().unwrap_or(0);
                                let existing_size = existing["size_mb"].as_f64().unwrap_or(0.0);
                                let new_size = partition["size_mb"].as_f64().unwrap_or(0.0);
                                existing["file_count"] =
                                    serde_json::json!(existing_count + new_count);
                                existing["size_mb"] =
                                    serde_json::json!(existing_size + new_size);
                            } else {
                                partition_map.insert(part_name.to_string(), partition.clone());
                            }
                        }
                    }
                }
            }
            Err(e) => errors.push(format!("{}: {}", node.node_id, e)),
        }
    }

    let mut partitions: Vec<serde_json::Value> = partition_map.into_values().collect();
    partitions.sort_by(|a, b| {
        let a_part = a["partition"].as_str().unwrap_or("");
        let b_part = b["partition"].as_str().unwrap_or("");
        a_part.cmp(b_part)
    });

    let total_size: f64 = partitions
        .iter()
        .map(|p| p["size_mb"].as_f64().unwrap_or(0.0))
        .sum();

    let response = serde_json::json!({
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "partitions": partitions,
        "count": partitions.len(),
        "total_size_mb": total_size,
        "nodes_queried": nodes.len(),
        "errors": if errors.is_empty() { None } else { Some(errors) }
    });

    (StatusCode::OK, Json(response))
}

// ============================================================================
// Memory/Stats Handlers (aggregated from Data nodes)
// ============================================================================

/// Memory stats - aggregate from Primary data nodes (Standby nodes don't serve traffic)
pub async fn memory_handler(State(state): State<RouterAppState>) -> impl IntoResponse {
    let nodes = state.shard_cache.get_active_primary_nodes();
    if nodes.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "No active data nodes"})),
        );
    }

    let mut node_results = Vec::new();
    let mut total_size: u64 = 0;
    let mut total_max_size: u64 = 0;
    let mut total_rows: u64 = 0;
    let mut total_tables: u64 = 0;

    for node in &nodes {
        match state.data_node_pool.get_memory_stats(&node.node_id).await {
            Ok(stats) => {
                let size = stats["size_bytes"].as_u64().unwrap_or(0);
                let max_size = stats["max_size_bytes"].as_u64().unwrap_or(0);
                let rows = stats["total_rows"].as_u64().unwrap_or(0);
                let tables = stats["num_tables"].as_u64().unwrap_or(0);

                total_size += size;
                total_max_size += max_size;
                total_rows += rows;
                total_tables += tables;

                node_results.push(serde_json::json!({
                    "node_id": node.node_id,
                    "size_bytes": size,
                    "max_size_bytes": max_size,
                    "total_rows": rows,
                    "num_tables": tables
                }));
            }
            Err(e) => {
                node_results.push(serde_json::json!({
                    "node_id": node.node_id,
                    "error": e
                }));
            }
        }
    }

    let response = serde_json::json!({
        "aggregate": {
            "total_size_bytes": total_size,
            "total_max_size_bytes": total_max_size,
            "total_rows": total_rows,
            "total_tables": total_tables
        },
        "nodes": node_results
    });

    (StatusCode::OK, Json(response))
}
