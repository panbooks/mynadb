//! Admin API handlers
//!
//! All REST and WebSocket API routes for the admin console.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{state::AdminState, websocket};
use crate::meta::types::{DataNodeMode, NodeRole, NodeState};

// ============ Helpers ============

/// Convert Unix timestamp (milliseconds) to RFC3339 string
fn timestamp_to_rfc3339(ts_ms: i64) -> Option<String> {
    if ts_ms <= 0 {
        return None;
    }
    let secs = (ts_ms / 1000) as i64;
    let nsecs = ((ts_ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.to_rfc3339())
}

/// Build API routes (REST + WebSocket)
pub fn routes() -> Router<AdminState> {
    Router::new()
        // Cluster
        .route("/cluster/status", get(cluster_status))
        // Nodes
        .route("/nodes", get(list_nodes))
        .route("/nodes/:id", get(get_node))
        // Tables
        .route("/tables", get(list_tables))
        .route("/tables/:key/reassign", post(reassign_table))
        // Scale-in
        .route("/scale-in/recommend", get(scale_in_recommend))
        .route("/scale-in/plan", post(scale_in_plan))
        .route("/scale-in/execute", post(scale_in_execute))
        // Events
        .route("/events", get(list_events))
        .route("/events/sources", get(list_event_sources))
        .route("/events/stats", get(get_event_stats))
        .route("/events/ws", get(websocket::handler))
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

// ============ Cluster Endpoints ============

#[derive(Serialize)]
struct ClusterStatusResponse {
    healthy: bool,
    meta_leader: Option<String>,
    meta_term: Option<u64>,
    node_counts: NodeCounts,
    table_count: u32,
    partition_count: u32,
    warnings: Vec<String>,
}

#[derive(Serialize, Default)]
struct NodeCounts {
    meta: RoleCounts,
    router: RoleCounts,
    data: DataNodeCounts,
    compactor: RoleCounts,
}

#[derive(Serialize, Default)]
struct RoleCounts {
    total: u32,
    active: u32,
    inactive: u32,
}

#[derive(Serialize, Default)]
struct DataNodeCounts {
    total: u32,
    active: u32,
    inactive: u32,
    primary: u32,
    standby: u32,
    retiring: u32,
    drained: u32,
}

async fn cluster_status(State(state): State<AdminState>) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ClusterStatusResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    // Get cluster status from Meta (includes meta_leader)
    let cluster_status = match meta_client.get_cluster_status().await {
        Ok(status) => status,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<ClusterStatusResponse>::error(format!(
                    "Failed to get cluster status: {}",
                    e
                ))),
            );
        }
    };

    // Count nodes by role and state
    let nodes = match meta_client.list_nodes(None, None).await {
        Ok(n) => n,
        Err(_) => Vec::new(),
    };

    let mut node_counts = NodeCounts::default();
    for node in &nodes {
        match node.role {
            NodeRole::Meta => {
                node_counts.meta.total += 1;
                if node.state == NodeState::Active {
                    node_counts.meta.active += 1;
                } else {
                    node_counts.meta.inactive += 1;
                }
            }
            NodeRole::Router => {
                node_counts.router.total += 1;
                if node.state == NodeState::Active {
                    node_counts.router.active += 1;
                } else {
                    node_counts.router.inactive += 1;
                }
            }
            NodeRole::Data => {
                node_counts.data.total += 1;
                match node.state {
                    NodeState::Active => {
                        node_counts.data.active += 1;
                        if node.mode == Some(DataNodeMode::Primary) {
                            node_counts.data.primary += 1;
                        } else if node.mode == Some(DataNodeMode::Standby) {
                            node_counts.data.standby += 1;
                        }
                    }
                    NodeState::Retiring => node_counts.data.retiring += 1,
                    NodeState::Drained => node_counts.data.drained += 1,
                    _ => node_counts.data.inactive += 1,
                }
            }
            NodeRole::Compactor => {
                node_counts.compactor.total += 1;
                if node.state == NodeState::Active {
                    node_counts.compactor.active += 1;
                } else {
                    node_counts.compactor.inactive += 1;
                }
            }
            NodeRole::Unknown => {}
        }
    }

    // Count tables
    let tables = match meta_client.list_tables(None, None).await {
        Ok(t) => t,
        Err(_) => Vec::new(),
    };

    let table_count = tables.len() as u32;
    let partition_count: u32 = tables.iter().map(|t| t.partition_count).sum();

    // Check for warnings
    let mut warnings = Vec::new();
    if node_counts.data.retiring > 0 {
        warnings.push(format!(
            "{} data node(s) are retiring",
            node_counts.data.retiring
        ));
    }
    if node_counts.data.active == 0 && node_counts.data.total > 0 {
        warnings.push("No active data nodes!".to_string());
    }
    if cluster_status.meta_leader.is_empty() {
        warnings.push("No Meta leader elected".to_string());
    }

    let meta_leader = if cluster_status.meta_leader.is_empty() {
        None
    } else {
        Some(cluster_status.meta_leader)
    };

    let healthy = meta_leader.is_some()
        && node_counts.data.active > 0
        && node_counts.data.retiring == 0;

    let response = ClusterStatusResponse {
        healthy,
        meta_leader,
        meta_term: None, // Not currently available from MetaClient
        node_counts,
        table_count,
        partition_count,
        warnings,
    };

    (StatusCode::OK, Json(ApiResponse::ok(response)))
}

// ============ Node Endpoints ============

#[derive(Deserialize)]
struct ListNodesQuery {
    role: Option<String>,
    state: Option<String>,
    mode: Option<String>,
}

#[derive(Serialize)]
struct NodeSummary {
    id: String,
    role: String,
    state: String,
    mode: Option<String>,
    advertise_addr: String,
    zone: Option<String>,
    tables_owned: Option<u32>,
    last_heartbeat: Option<String>,
}

async fn list_nodes(
    State(state): State<AdminState>,
    Query(query): Query<ListNodesQuery>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<NodeSummary>>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    // Parse filters
    let role_filter = query.role.as_ref().and_then(|r| match r.to_lowercase().as_str() {
        "meta" => Some(NodeRole::Meta),
        "router" => Some(NodeRole::Router),
        "data" => Some(NodeRole::Data),
        "compactor" => Some(NodeRole::Compactor),
        _ => None,
    });

    let state_filter = query.state.as_ref().and_then(|s| match s.to_lowercase().as_str() {
        "active" => Some(NodeState::Active),
        "starting" => Some(NodeState::Starting),
        "retiring" => Some(NodeState::Retiring),
        "drained" => Some(NodeState::Drained),
        "draining" => Some(NodeState::Draining),
        "inactive" => Some(NodeState::Inactive),
        _ => None,
    });

    match meta_client.list_nodes(role_filter, state_filter).await {
        Ok(nodes) => {
            let mut summaries: Vec<NodeSummary> = nodes
                .into_iter()
                .filter(|n| {
                    // Additional mode filter
                    if let Some(ref mode_str) = query.mode {
                        let mode_match = match mode_str.to_lowercase().as_str() {
                            "primary" => n.mode == Some(DataNodeMode::Primary),
                            "standby" => n.mode == Some(DataNodeMode::Standby),
                            _ => true,
                        };
                        if !mode_match {
                            return false;
                        }
                    }
                    true
                })
                .map(|n| {
                    let zone = n.zone().map(|s| s.to_string());
                    let last_heartbeat = timestamp_to_rfc3339(n.last_heartbeat);
                    NodeSummary {
                        id: n.node_id,
                        role: format!("{:?}", n.role),
                        state: format!("{:?}", n.state),
                        mode: n.mode.map(|m| format!("{:?}", m)),
                        advertise_addr: n.advertise_addr,
                        zone,
                        tables_owned: None,
                        last_heartbeat,
                    }
                })
                .collect();

            summaries.sort_by(|a, b| a.id.cmp(&b.id));

            (StatusCode::OK, Json(ApiResponse::ok(summaries)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Vec<NodeSummary>>::error(format!(
                "Failed to list nodes: {}",
                e
            ))),
        ),
    }
}

/// Node detail response - matches frontend NodeDetail type
#[derive(Serialize)]
struct NodeDetailResponse {
    id: String,
    role: String,
    state: String,
    mode: Option<String>,
    advertise_addr: String,
    http_addr: Option<String>,
    flight_addr: Option<String>,
    labels: std::collections::HashMap<String, String>,
    zone: Option<String>,
    paired_node_id: Option<String>,
    last_heartbeat: Option<String>,
    tables_owned: Option<u32>,
    registered_at: Option<String>,
    health: Option<NodeHealthInfo>,
    tables: Option<Vec<TableInfo>>,
    stats: Option<NodeStatsInfo>,
}

#[derive(Serialize)]
struct NodeHealthInfo {
    is_healthy: bool,
    last_heartbeat: String,
    heartbeat_age_ms: i64,
    message: Option<String>,
}

#[derive(Serialize)]
struct TableInfo {
    key: String,
    catalog: String,
    schema: String,
    table: String,
}

#[derive(Serialize)]
struct NodeStatsInfo {
    memory_used_bytes: Option<u64>,
    memory_total_bytes: Option<u64>,
    cache_hits: Option<u64>,
    cache_misses: Option<u64>,
    active_queries: Option<u32>,
    compaction_status: Option<String>,
}

async fn get_node(
    State(state): State<AdminState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<NodeDetailResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    match meta_client.list_nodes(None, None).await {
        Ok(nodes) => {
            if let Some(node) = nodes.into_iter().find(|n| n.node_id == node_id) {
                // Get tables owned by this node
                let tables_meta = meta_client
                    .list_tables_for_node(&node_id)
                    .await
                    .unwrap_or_default();

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let heartbeat_age_ms = now_ms - node.last_heartbeat;
                let last_heartbeat_str = timestamp_to_rfc3339(node.last_heartbeat);

                let tables: Vec<TableInfo> = tables_meta
                    .iter()
                    .map(|t| TableInfo {
                        key: t.table_key(),
                        catalog: t.catalog.clone(),
                        schema: t.schema.clone(),
                        table: t.table.clone(),
                    })
                    .collect();

                let health = last_heartbeat_str.as_ref().map(|hb| NodeHealthInfo {
                    is_healthy: heartbeat_age_ms < 30_000,
                    last_heartbeat: hb.clone(),
                    heartbeat_age_ms,
                    message: None,
                });

                let zone = node.zone().map(|s| s.to_string());
                let registered_at = timestamp_to_rfc3339(node.registered_at);
                let tables_owned = Some(tables_meta.len() as u32);

                let response = NodeDetailResponse {
                    id: node.node_id,
                    role: format!("{:?}", node.role),
                    state: format!("{:?}", node.state),
                    mode: node.mode.map(|m| format!("{:?}", m)),
                    advertise_addr: node.advertise_addr,
                    http_addr: node.http_addr,
                    flight_addr: node.flight_addr,
                    labels: node.labels,
                    zone,
                    paired_node_id: node.backing_standby,
                    last_heartbeat: last_heartbeat_str,
                    tables_owned,
                    registered_at,
                    health,
                    tables: Some(tables),
                    stats: None,
                };

                (StatusCode::OK, Json(ApiResponse::ok(response)))
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(ApiResponse::<NodeDetailResponse>::error("Node not found")),
                )
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<NodeDetailResponse>::error(format!(
                "Failed to get node: {}",
                e
            ))),
        ),
    }
}

// ============ Table Endpoints ============

#[derive(Deserialize, Default)]
struct ListTablesQuery {
    catalog: Option<String>,
    schema: Option<String>,
    owner: Option<String>,
    search: Option<String>,
    #[serde(default = "default_page")]
    page: u32,
    #[serde(default = "default_page_size")]
    page_size: u32,
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    20
}

/// Table summary - matches frontend TableSummary type
#[derive(Serialize)]
struct TableSummary {
    key: String,
    catalog: String,
    schema: String,
    table: String,
    owner_node_id: Option<String>,
    partition_count: u32,
    total_bytes: u64,
    total_rows: u64,
}

/// Paginated response - matches frontend PaginatedResponse type
#[derive(Serialize)]
struct ListTablesResponse {
    items: Vec<TableSummary>,
    total: usize,
    page: u32,
    page_size: u32,
    total_pages: u32,
}

async fn list_tables(
    State(state): State<AdminState>,
    Query(query): Query<ListTablesQuery>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ListTablesResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    match meta_client
        .list_tables(query.catalog.as_deref(), query.schema.as_deref())
        .await
    {
        Ok(tables) => {
            let mut summaries: Vec<TableSummary> = tables
                .into_iter()
                .filter(|t| {
                    // Owner filter
                    if let Some(ref owner) = query.owner {
                        if !t.owner_node_ids.contains(owner) {
                            return false;
                        }
                    }
                    // Search filter
                    if let Some(ref search) = query.search {
                        let search_lower = search.to_lowercase();
                        let matches = t.table.to_lowercase().contains(&search_lower)
                            || t.schema.to_lowercase().contains(&search_lower)
                            || t.catalog.to_lowercase().contains(&search_lower);
                        if !matches {
                            return false;
                        }
                    }
                    true
                })
                .map(|t| {
                    let owner_node_id = t.primary_owner().map(|s| s.to_string());
                    TableSummary {
                        key: t.table_key(),
                        catalog: t.catalog,
                        schema: t.schema,
                        table: t.table,
                        owner_node_id,
                        partition_count: t.partition_count,
                        total_bytes: t.total_bytes,
                        total_rows: t.total_rows,
                    }
                })
                .collect();

            let total = summaries.len();

            // Apply pagination (page is 1-based)
            let page = query.page.max(1);
            let page_size = query.page_size;
            let offset = ((page - 1) * page_size) as usize;
            let total_pages = ((total as u32) + page_size - 1) / page_size;

            summaries = summaries.into_iter().skip(offset).take(page_size as usize).collect();

            (
                StatusCode::OK,
                Json(ApiResponse::ok(ListTablesResponse {
                    items: summaries,
                    total,
                    page,
                    page_size,
                    total_pages,
                })),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<ListTablesResponse>::error(format!(
                "Failed to list tables: {}",
                e
            ))),
        ),
    }
}

#[derive(Deserialize)]
struct ReassignTableRequest {
    new_owner_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    force: bool,
}

#[derive(Serialize)]
struct ReassignTableResponse {
    table_key: String,
    new_owner: String,
}

async fn reassign_table(
    State(state): State<AdminState>,
    Path(table_key): Path<String>,
    Json(request): Json<ReassignTableRequest>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ReassignTableResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    // Parse table key (catalog/schema/table)
    let parts: Vec<&str> = table_key.split('/').collect();
    if parts.len() != 3 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<ReassignTableResponse>::error(
                "Invalid table key format. Expected: catalog/schema/table",
            )),
        );
    }

    let (catalog, schema, table) = (parts[0], parts[1], parts[2]);

    match meta_client
        .reassign_table(catalog, schema, table, &request.new_owner_id)
        .await
    {
        Ok(_) => {
            let response = ReassignTableResponse {
                table_key,
                new_owner: request.new_owner_id,
            };
            (StatusCode::OK, Json(ApiResponse::ok(response)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<ReassignTableResponse>::error(format!(
                "Failed to reassign table: {}",
                e
            ))),
        ),
    }
}

// ============ Scale-In Endpoints ============

#[derive(Deserialize, Default)]
struct ScaleInRecommendQuery {
    #[serde(default)]
    primaries: usize,
    #[serde(default)]
    standbys: usize,
}

#[derive(Serialize)]
struct ScaleInRecommendResponse {
    recommended_primaries: Vec<String>,
    recommended_standbys: Vec<String>,
    reasons: HashMap<String, String>,
    warnings: Vec<String>,
}

async fn scale_in_recommend(
    State(state): State<AdminState>,
    Query(query): Query<ScaleInRecommendQuery>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ScaleInRecommendResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    match meta_client
        .recommend_scale_in(query.primaries, query.standbys)
        .await
    {
        Ok(recommendation) => {
            let response = ScaleInRecommendResponse {
                recommended_primaries: recommendation.recommended_primaries,
                recommended_standbys: recommendation.recommended_standbys,
                reasons: recommendation.reasons,
                warnings: recommendation.warnings,
            };
            (StatusCode::OK, Json(ApiResponse::ok(response)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<ScaleInRecommendResponse>::error(format!(
                "Failed to get recommendations: {}",
                e
            ))),
        ),
    }
}

#[derive(Deserialize)]
struct ScaleInPlanRequest {
    #[serde(default)]
    node_ids: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    dry_run: bool,
}

#[derive(Serialize)]
struct ScaleInPlanResponse {
    node_ids: Vec<String>,
    tables_to_reassign: u32,
    estimated_duration_secs: u32,
    warnings: Vec<String>,
}

async fn scale_in_plan(
    State(state): State<AdminState>,
    Json(request): Json<ScaleInPlanRequest>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ScaleInPlanResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    // For now, we just return what the user requested with some validation
    // Full implementation would call MetaClient methods when available
    let mut warnings = Vec::new();
    let mut tables_to_reassign = 0u32;

    for node_id in &request.node_ids {
        // Check if node exists and count its tables
        match meta_client.list_tables_for_node(node_id).await {
            Ok(tables) => {
                tables_to_reassign += tables.len() as u32;
            }
            Err(_) => {
                warnings.push(format!("Node {} not found or has no tables", node_id));
            }
        }
    }

    let response = ScaleInPlanResponse {
        node_ids: request.node_ids,
        tables_to_reassign,
        estimated_duration_secs: tables_to_reassign * 10, // Rough estimate
        warnings,
    };

    (StatusCode::OK, Json(ApiResponse::ok(response)))
}

#[derive(Serialize)]
struct ScaleInExecuteResponse {
    started: bool,
    message: String,
    nodes_to_retire: Vec<String>,
}

async fn scale_in_execute(
    State(state): State<AdminState>,
    Json(request): Json<ScaleInPlanRequest>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ScaleInExecuteResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    // Start retirement for each node
    let mut started_nodes = Vec::new();
    let mut errors = Vec::new();

    for node_id in &request.node_ids {
        match meta_client.start_retire(node_id).await {
            Ok(tables) => {
                started_nodes.push(node_id.clone());
                tracing::info!("Started retiring node {} with {} tables", node_id, tables);
            }
            Err(e) => {
                errors.push(format!("Failed to start retire for {}: {}", node_id, e));
            }
        }
    }

    if started_nodes.is_empty() && !errors.is_empty() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<ScaleInExecuteResponse>::error(errors.join("; "))),
        );
    }

    let response = ScaleInExecuteResponse {
        started: !started_nodes.is_empty(),
        message: if errors.is_empty() {
            format!("Started retiring {} nodes", started_nodes.len())
        } else {
            format!(
                "Started retiring {} nodes, errors: {}",
                started_nodes.len(),
                errors.join("; ")
            )
        },
        nodes_to_retire: started_nodes,
    };

    (StatusCode::OK, Json(ApiResponse::ok(response)))
}

// ============ Events Endpoints ============

#[derive(Deserialize)]
struct ListEventsQuery {
    /// Filter by event type (can be comma-separated for multiple)
    #[serde(rename = "type")]
    event_type: Option<String>,
    /// Filter by source node
    source: Option<String>,
    /// Filter events since (ISO timestamp or relative like "1h", "24h", "7d")
    since: Option<String>,
    /// Full-text search
    search: Option<String>,
    /// Maximum events to return
    #[serde(default = "default_events_limit")]
    limit: usize,
}

fn default_events_limit() -> usize {
    100
}

/// Parse relative time string (1h, 24h, 7d) or ISO timestamp to milliseconds
fn parse_since(since: &str) -> Option<i64> {
    let now = chrono::Utc::now();

    // Try relative time first
    if let Some(hours) = since.strip_suffix('h').and_then(|s| s.parse::<i64>().ok()) {
        return Some((now - chrono::Duration::hours(hours)).timestamp_millis());
    }
    if let Some(days) = since.strip_suffix('d').and_then(|s| s.parse::<i64>().ok()) {
        return Some((now - chrono::Duration::days(days)).timestamp_millis());
    }
    if let Some(minutes) = since.strip_suffix('m').and_then(|s| s.parse::<i64>().ok()) {
        return Some((now - chrono::Duration::minutes(minutes)).timestamp_millis());
    }

    // Try ISO timestamp
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(since) {
        return Some(dt.timestamp_millis());
    }

    // Try Unix timestamp in milliseconds
    if let Ok(ts) = since.parse::<i64>() {
        return Some(ts);
    }

    None
}

#[derive(Serialize)]
struct ListEventsResponse {
    events: Vec<super::events::AdminEventDetail>,
    total: usize,
    cache_size: usize,
    cache_max: usize,
}

async fn list_events(
    State(state): State<AdminState>,
    Query(query): Query<ListEventsQuery>,
) -> impl IntoResponse {
    use super::state::EventFilter;

    // Parse event types (comma-separated)
    let event_types = query.event_type.map(|t| {
        t.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
    });

    // Parse since time
    let since_ms = query.since.as_deref().and_then(parse_since);

    let filter = EventFilter {
        event_types,
        source: query.source,
        since_ms,
        search: query.search,
        limit: query.limit,
    };

    // Get events from cache
    let events = state.get_cached_events(filter).await;
    let (cache_size, cache_max) = state.event_cache_stats().await;

    (
        StatusCode::OK,
        Json(ApiResponse::ok(ListEventsResponse {
            total: events.len(),
            events,
            cache_size,
            cache_max,
        })),
    )
}

/// Get unique source nodes
async fn list_event_sources(State(state): State<AdminState>) -> impl IntoResponse {
    let sources = state.get_event_sources().await;
    (StatusCode::OK, Json(ApiResponse::ok(sources)))
}

/// Get event statistics
async fn get_event_stats(State(state): State<AdminState>) -> impl IntoResponse {
    let stats = state.get_event_stats().await;
    (StatusCode::OK, Json(ApiResponse::ok(stats)))
}
