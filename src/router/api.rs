//! Router-specific HTTP API handlers
//!
//! Provides HTTP endpoints for the Router node that route requests
//! through WriteRouter and QueryCoordinator to Data nodes.

use super::fan_out::fan_out;
use super::state::RouterAppState;
use crate::query::influxql::{self, InfluxResponse};
use crate::utils::json::batches_to_json;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

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

/// InfluxDB-compatible query parameters (used by Grafana InfluxDB datasource)
#[derive(Debug, Deserialize)]
pub struct InfluxQueryParams {
    /// The InfluxQL query
    #[serde(default)]
    pub q: Option<String>,
    /// Database name
    #[serde(default)]
    pub db: Option<String>,
    /// Epoch precision for timestamps (ns, us, ms, s)
    #[serde(default)]
    pub epoch: Option<String>,
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
        .route("/ping", get(influxdb_ping))
        .route("/version", get(version_handler))
        // Write endpoint - routes to Data nodes
        .route("/write/:db", post(write_handler))
        .route("/write", post(write_handler_query_param))
        // InfluxDB-compatible query endpoint (Grafana InfluxDB datasource)
        .route("/query", get(influxql_query_handler))
        .route("/query", post(influxql_query_handler_post))
        // SQL query endpoint - distributed query via Data nodes
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
       GET/POST /query/:db       - Execute SQL query\n\
       GET/POST /query?db=X&q=Y  - InfluxQL query (Grafana InfluxDB datasource)\n\
       GET  /ping                - InfluxDB connection test\n\n\
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
// InfluxDB-Compatible Handlers (Grafana InfluxDB datasource)
// ============================================================================

/// InfluxDB-compatible /ping endpoint — returns 204 with version header
pub async fn influxdb_ping() -> impl IntoResponse {
    (
        StatusCode::NO_CONTENT,
        [("X-Influxdb-Version", env!("GIT_VERSION"))],
    )
}

/// InfluxDB-compatible query handler (GET /query?db=X&q=Y&epoch=ms)
pub async fn influxql_query_handler(
    Query(params): Query<InfluxQueryParams>,
    State(state): State<RouterAppState>,
) -> impl IntoResponse {
    let q = match params.q {
        Some(q) if !q.is_empty() => q,
        _ => {
            let response = InfluxResponse::error(0, "missing required parameter \"q\"".to_string());
            return (StatusCode::BAD_REQUEST, Json(response));
        }
    };
    let db = params.db.unwrap_or_else(|| "default".to_string());
    debug!("InfluxQL Query (GET): db={}, q={}", db, q);
    execute_influxql_query(&state, db, q, params.epoch.as_deref()).await
}

/// InfluxDB-compatible query handler (POST /query?db=X)
pub async fn influxql_query_handler_post(
    Query(params): Query<InfluxQueryParams>,
    State(state): State<RouterAppState>,
    body: String,
) -> impl IntoResponse {
    let query = match influxql::extract_query_from_body(&body, params.q.as_deref()) {
        Some(q) => q,
        None => {
            let response = InfluxResponse::error(0, "missing required parameter \"q\"".to_string());
            return (StatusCode::BAD_REQUEST, Json(response));
        }
    };
    let db = params.db.unwrap_or_else(|| "default".to_string());
    debug!("InfluxQL Query (POST): db={}, q={}", db, query);
    execute_influxql_query(&state, db, query, params.epoch.as_deref()).await
}

/// Execute an InfluxQL query via the router's query coordinator
async fn execute_influxql_query(
    state: &RouterAppState,
    db: String,
    query: String,
    epoch: Option<&str>,
) -> (StatusCode, Json<InfluxResponse>) {
    use crate::query::influxql::Statement;

    // Handle metadata queries directly (without going through query coordinator)
    if let Ok(stmt) = influxql::parse(&query) {
        match stmt {
            Statement::ShowDatabases => {
                // Router doesn't have local filesystem; return the requested db
                let response = influxql::build_show_databases_response(vec![db]);
                return (StatusCode::OK, Json(response));
            }
            Statement::ShowMeasurements(ref s) => {
                // Collect tables from data nodes in parallel
                let target_db = s.database.as_deref().unwrap_or(&db);
                let nodes = state.shard_cache.get_active_primary_nodes();

                let futures: Vec<_> = nodes.iter().map(|node| {
                    let pool = Arc::clone(&state.data_node_pool);
                    let node_id = node.node_id.clone();
                    let target_db = target_db.to_string();
                    (node_id.clone(), Box::pin(async move {
                        pool.list_tables(&node_id, &target_db).await
                    }) as BoxFuture<'_, _>)
                }).collect();

                let (results, _errors) = fan_out::<serde_json::Value>(futures).await;

                let mut all_tables = std::collections::HashSet::new();
                for (_node_id, result) in &results {
                    if let Some(tables) = result["tables"].as_array() {
                        for table in tables {
                            if let Some(t) = table.as_str() {
                                all_tables.insert(t.to_string());
                            }
                        }
                    }
                }
                let mut tables: Vec<String> = all_tables.into_iter().collect();
                tables.sort();
                let response = influxql::build_show_measurements_response(tables);
                return (StatusCode::OK, Json(response));
            }
            Statement::ShowRetentionPolicies(_) => {
                let response = influxql::build_show_retention_policies_response();
                return (StatusCode::OK, Json(response));
            }
            _ => {}
        }
    }

    // Parse and transpile InfluxQL to SQL
    let prepared = match influxql::prepare_query(&query, &db, &state.config.schema.default_name) {
        Ok(p) => p,
        Err(response) => return (StatusCode::BAD_REQUEST, Json(response)),
    };

    debug!("InfluxQL transpiled to SQL: {}", prepared.sql);

    // Execute via query coordinator (distributes to data nodes)
    let result = state.query_coordinator.execute_query(&db, &prepared.sql).await;

    if let Some(ref error) = result.error {
        let response = InfluxResponse::error(0, format!("Query error: {}", error));
        return (StatusCode::BAD_REQUEST, Json(response));
    }

    let response = influxql::format_result(&result.batches, &prepared, epoch);
    (StatusCode::OK, Json(response))
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

    if result.success {
        (StatusCode::NO_CONTENT, String::new())
    } else {
        let response = WriteResponse {
            success: result.success,
            points_written: result.points_written,
            elapsed_ms: result.elapsed_ms,
            error: result.error,
        };
        (StatusCode::INTERNAL_SERVER_ERROR, serde_json::to_string(&response).unwrap_or_default())
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

    if result.success {
        (StatusCode::NO_CONTENT, String::new())
    } else {
        let response = WriteResponse {
            success: result.success,
            points_written: result.points_written,
            elapsed_ms: result.elapsed_ms,
            error: result.error,
        };
        (StatusCode::INTERNAL_SERVER_ERROR, serde_json::to_string(&response).unwrap_or_default())
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
    // Try to parse as JSON first, then form-encoded, then raw SQL
    let sql = if let Ok(request) = serde_json::from_str::<QueryRequestBody>(&body) {
        request.sql
    } else if let Some(sql_value) = body.strip_prefix("sql=") {
        // URL-encoded form body: sql=SELECT...
        urlencoding::decode(sql_value).unwrap_or(sql_value.into()).into_owned()
    } else {
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
    use super::shard_cache::NodeState;
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

    let futures: Vec<_> = nodes.iter().map(|node| {
        let pool = Arc::clone(&state.data_node_pool);
        let node_id = node.node_id.clone();
        let db = db.clone();
        (node_id.clone(), Box::pin(async move {
            pool.list_tables(&node_id, &db).await
        }) as BoxFuture<'_, _>)
    }).collect();

    let (results, errors) = fan_out::<serde_json::Value>(futures).await;

    let mut all_tables = std::collections::HashSet::new();
    for (_node_id, result) in &results {
        if let Some(tables) = result["tables"].as_array() {
            for table in tables {
                if let Some(t) = table.as_str() {
                    all_tables.insert(t.to_string());
                }
            }
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

    let futures: Vec<_> = nodes.iter().map(|node| {
        let pool = Arc::clone(&state.data_node_pool);
        let node_id = node.node_id.clone();
        let db = db.clone();
        (node_id.clone(), Box::pin(async move {
            pool.list_schemas(&node_id, &db).await
        }) as BoxFuture<'_, _>)
    }).collect();

    let (results, errors) = fan_out::<serde_json::Value>(futures).await;

    let mut all_schemas = std::collections::HashSet::new();
    for (_node_id, result) in &results {
        if let Some(schemas) = result["schemas"].as_array() {
            for schema in schemas {
                if let Some(s) = schema.as_str() {
                    all_schemas.insert(s.to_string());
                }
            }
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

    let futures: Vec<_> = nodes.iter().map(|node| {
        let pool = Arc::clone(&state.data_node_pool);
        let node_id = node.node_id.clone();
        let db = db.clone();
        let schema = schema.clone();
        (node_id.clone(), Box::pin(async move {
            pool.list_schema_tables(&node_id, &db, &schema).await
        }) as BoxFuture<'_, _>)
    }).collect();

    let (results, errors) = fan_out::<serde_json::Value>(futures).await;

    let mut all_tables = std::collections::HashSet::new();
    for (_node_id, result) in &results {
        if let Some(tables) = result["tables"].as_array() {
            for table in tables {
                if let Some(t) = table.as_str() {
                    all_tables.insert(t.to_string());
                }
            }
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

    // Try all nodes in parallel (table might only be on some nodes)
    let futures: Vec<_> = nodes.iter().map(|node| {
        let pool = Arc::clone(&state.data_node_pool);
        let node_id = node.node_id.clone();
        let db = db.clone();
        let full_table = full_table.clone();
        (node_id.clone(), Box::pin(async move {
            pool.get_table_schema(&node_id, &db, &full_table).await
        }) as BoxFuture<'_, _>)
    }).collect();

    let (results, errors) = fan_out::<String>(futures).await;

    // Return first successful result
    if let Some((_node_id, schema)) = results.into_iter().next() {
        return (StatusCode::OK, schema);
    }

    let last_error = errors.into_iter().last().unwrap_or_else(|| "No nodes responded".to_string());
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

    // Fan out partition requests to all nodes in parallel
    let futures: Vec<_> = nodes.iter().map(|node| {
        let pool = Arc::clone(&state.data_node_pool);
        let node_id = node.node_id.clone();
        let catalog = catalog.clone();
        let schema = schema.clone();
        let table = table.clone();
        (node_id.clone(), Box::pin(async move {
            pool.list_partitions(&node_id, &catalog, &schema, &table).await
        }) as BoxFuture<'_, _>)
    }).collect();

    let (results, errors) = fan_out::<serde_json::Value>(futures).await;

    // Deduplicate by partition name, merging file_count and size
    let mut partition_map: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();

    for (_node_id, result) in &results {
        if let Some(partitions) = result["partitions"].as_array() {
            for partition in partitions {
                if let Some(part_name) = partition["partition"].as_str() {
                    if let Some(existing) = partition_map.get_mut(part_name) {
                        let existing_count = existing["file_count"].as_u64().unwrap_or(0);
                        let new_count = partition["file_count"].as_u64().unwrap_or(0);
                        let existing_size = existing["size_mb"].as_f64().unwrap_or(0.0);
                        let new_size = partition["size_mb"].as_f64().unwrap_or(0.0);
                        existing["file_count"] =
                            serde_json::json!(existing_count + new_count);
                        existing["size_mb"] = serde_json::json!(existing_size + new_size);
                    } else {
                        partition_map.insert(part_name.to_string(), partition.clone());
                    }
                }
            }
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

    let futures: Vec<_> = nodes.iter().map(|node| {
        let pool = Arc::clone(&state.data_node_pool);
        let node_id = node.node_id.clone();
        (node_id.clone(), Box::pin(async move {
            pool.get_memory_stats(&node_id).await
        }) as BoxFuture<'_, _>)
    }).collect();

    let (results, errors) = fan_out::<serde_json::Value>(futures).await;

    let mut node_results = Vec::new();
    let mut total_size: u64 = 0;
    let mut total_max_size: u64 = 0;
    let mut total_rows: u64 = 0;
    let mut total_tables: u64 = 0;

    for (node_id, stats) in &results {
        let size = stats["size_bytes"].as_u64().unwrap_or(0);
        let max_size = stats["max_size_bytes"].as_u64().unwrap_or(0);
        let rows = stats["total_rows"].as_u64().unwrap_or(0);
        let tables = stats["num_tables"].as_u64().unwrap_or(0);

        total_size += size;
        total_max_size += max_size;
        total_rows += rows;
        total_tables += tables;

        node_results.push(serde_json::json!({
            "node_id": node_id,
            "size_bytes": size,
            "max_size_bytes": max_size,
            "total_rows": rows,
            "num_tables": tables
        }));
    }

    for error in &errors {
        // Error format is "node_id: error_message"
        let node_id = error.split(':').next().unwrap_or("unknown");
        let err_msg = error.splitn(2, ": ").nth(1).unwrap_or(error);
        node_results.push(serde_json::json!({
            "node_id": node_id,
            "error": err_msg
        }));
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
