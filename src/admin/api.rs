//! Admin API handlers
//!
//! All REST and WebSocket API routes for the admin console.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{query_api, response::ApiResponse, state::AdminState, storage_api, websocket};
use crate::meta::types::{DataNodeMode, NodeRole, NodeState};

use super::response::timestamp_to_rfc3339;

/// Build API routes (REST + WebSocket)
pub fn routes() -> Router<AdminState> {
    Router::new()
        // Health
        .route("/health", get(health))
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
        // Meta operations
        .route("/distribution", get(distribution).post(set_distribution))
        .route("/meta/status", get(meta_status))
        // Debug Tools
        .route("/debug/heap", get(debug_heap_profile))
        .route("/debug/cpu", get(debug_cpu_profile))
        .route("/debug/generate-data", post(generate_test_data))
        .route("/cache/prewarm/:catalog", post(cache_prewarm))
        .route("/cache/clear", post(cache_clear))
        // Elections
        .route("/elections", get(list_elections))
        .route("/elections/:election/campaign", post(force_campaign))
        .route("/elections/:election/resign", post(force_resign))
        // Configuration
        .route("/config", get(get_config))
        // Query Console
        .merge(query_api::routes())
        // Storage Browser
        .merge(storage_api::routes())
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

    // Probe meta nodes directly (they don't self-register)
    {
        let futures: Vec<_> = state
            .meta_endpoints
            .iter()
            .map(|ep| probe_meta_health(&state.http_client, ep))
            .collect();
        let results = futures::future::join_all(futures).await;
        for peer in results.into_iter().flatten() {
            node_counts.meta.total += 1;
            if peer.is_healthy {
                node_counts.meta.active += 1;
            } else {
                node_counts.meta.inactive += 1;
            }
        }
    }

    for node in &nodes {
        match node.role {
            NodeRole::Meta => {
                // Skip — already counted via HTTP probing above
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

    // Fetch registered nodes (data nodes primarily) from meta registry
    let registered_nodes = match meta_client.list_nodes(role_filter, state_filter).await {
        Ok(nodes) => nodes,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Vec<NodeSummary>>::error(format!(
                    "Failed to list nodes: {}",
                    e
                ))),
            );
        }
    };

    // Build table ownership counts from meta
    let table_counts: HashMap<String, u32> = if let Ok(tables) = meta_client.list_tables(None, None).await {
        let mut counts: HashMap<String, u32> = HashMap::new();
        for table in &tables {
            for owner in &table.owners {
                *counts.entry(owner.node_id.clone()).or_insert(0) += 1;
            }
        }
        counts
    } else {
        HashMap::new()
    };

    let mut summaries: Vec<NodeSummary> = registered_nodes
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
            let tables_owned = table_counts.get(&n.node_id).copied();
            NodeSummary {
                id: n.node_id,
                role: format!("{:?}", n.role),
                state: format!("{:?}", n.state),
                mode: n.mode.map(|m| format!("{:?}", m)),
                advertise_addr: n.advertise_addr,
                zone,
                tables_owned,
                last_heartbeat,
            }
        })
        .collect();

    // Include synthetic meta nodes by probing HTTP endpoints
    // (meta nodes never self-register in the node registry)
    let include_meta = role_filter.is_none() || role_filter == Some(NodeRole::Meta);
    if include_meta {
        let futures: Vec<_> = state
            .meta_endpoints
            .iter()
            .map(|ep| probe_meta_health(&state.http_client, ep))
            .collect();
        let results = futures::future::join_all(futures).await;

        for peer in results.into_iter().flatten() {
            summaries.push(NodeSummary {
                id: if peer.id.is_empty() {
                    format!("meta-{}", peer.address)
                } else {
                    format!("meta-{}", peer.id)
                },
                role: "Meta".to_string(),
                state: if peer.is_healthy {
                    "Active".to_string()
                } else {
                    "Inactive".to_string()
                },
                mode: if peer.is_leader {
                    Some("Leader".to_string())
                } else {
                    Some("Follower".to_string())
                },
                advertise_addr: peer.address,
                zone: None,
                tables_owned: None,
                last_heartbeat: peer.last_heartbeat,
            });
        }
    }

    summaries.sort_by(|a, b| a.id.cmp(&b.id));

    (StatusCode::OK, Json(ApiResponse::ok(summaries)))
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

/// Discover tables from data nodes by querying each node's /tables/:db endpoint.
/// Returns deduplicated TableSummary entries with owner_node_id set.
async fn discover_tables_from_data_nodes(
    state: &AdminState,
) -> Vec<TableSummary> {
    let node_urls = match state.get_all_data_node_urls().await {
        Ok(urls) => urls,
        Err(_) => return Vec::new(),
    };

    // Also get node info for mapping URLs to node IDs
    let meta_client = state.get_meta_client().await.ok();
    let nodes = if let Some(ref client) = meta_client {
        client
            .list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    // Query all data nodes in parallel for their tables
    let futures: Vec<_> = node_urls
        .iter()
        .enumerate()
        .map(|(idx, url)| {
            let client = state.http_client.clone();
            let url = url.clone();
            let node_id = nodes.get(idx).map(|n| n.node_id.clone());
            async move {
                // Try common catalog names: telegraf, testdb, default
                let mut tables = Vec::new();
                // First discover catalogs by trying the node's tables endpoint
                // Data nodes respond to /tables/:db with tables in write cache
                let catalogs = discover_catalogs_from_node(&client, &url).await;
                for catalog in catalogs {
                    if let Ok(resp) = client
                        .get(format!("{}/tables/{}", url, catalog))
                        .timeout(std::time::Duration::from_secs(5))
                        .send()
                        .await
                    {
                        if let Ok(body) = resp.json::<serde_json::Value>().await {
                            if let Some(table_list) = body.get("tables").and_then(|t| t.as_array()) {
                                for t in table_list {
                                    if let Some(full_name) = t.as_str() {
                                        // Format: "catalog.schema.table"
                                        let parts: Vec<&str> = full_name.splitn(3, '.').collect();
                                        if parts.len() == 3 {
                                            tables.push((
                                                parts[0].to_string(),
                                                parts[1].to_string(),
                                                parts[2].to_string(),
                                                node_id.clone(),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                tables
            }
        })
        .collect();

    let all_results = futures::future::join_all(futures).await;

    // Deduplicate and merge (same table may appear on multiple nodes)
    let mut table_map: HashMap<String, TableSummary> = HashMap::new();
    for node_tables in all_results {
        for (catalog, schema, table, node_id) in node_tables {
            let key = format!("{}/{}/{}", catalog, schema, table);
            table_map.entry(key.clone()).or_insert_with(|| TableSummary {
                key,
                catalog,
                schema,
                table,
                owner_node_id: node_id,
                partition_count: 0,
                total_bytes: 0,
                total_rows: 0,
            });
        }
    }

    let mut result: Vec<TableSummary> = table_map.into_values().collect();
    result.sort_by(|a, b| a.key.cmp(&b.key));
    result
}

/// Discover catalog names from a data node by trying its /schemas endpoint
pub(crate) async fn discover_catalogs_from_node(
    client: &reqwest::Client,
    node_url: &str,
) -> Vec<String> {
    // Try common catalog names - data nodes don't have a "list all catalogs" endpoint
    // but /schemas/:db will return schemas if the catalog exists
    let candidates = ["telegraf", "testdb", "default", "mynadb", "metrics"];
    let mut found = Vec::new();

    for catalog in &candidates {
        if let Ok(resp) = client
            .get(format!("{}/schemas/{}", node_url, catalog))
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
        {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(count) = body.get("count").and_then(|c| c.as_u64()) {
                        if count > 0 {
                            found.push(catalog.to_string());
                        }
                    }
                }
            }
        }
    }
    found
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

    // Get tables from meta registry (has ownership, partition counts, sizes)
    let meta_tables = meta_client
        .list_tables(query.catalog.as_deref(), query.schema.as_deref())
        .await
        .unwrap_or_default();

    let mut summaries: Vec<TableSummary> = meta_tables
        .into_iter()
        .filter(|t| {
            if let Some(ref owner) = query.owner {
                if !t.all_owners().contains(&owner.to_string()) {
                    return false;
                }
            }
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

    // Also discover tables from data nodes and merge (catches tables not in meta)
    let discovered = discover_tables_from_data_nodes(&state).await;
    let meta_keys: std::collections::HashSet<String> =
        summaries.iter().map(|t| t.key.clone()).collect();
    for disc in discovered {
        if meta_keys.contains(&disc.key) {
            continue;
        }
        // Apply filters
        if let Some(ref catalog) = query.catalog {
            if disc.catalog != *catalog {
                continue;
            }
        }
        if let Some(ref schema) = query.schema {
            if disc.schema != *schema {
                continue;
            }
        }
        if let Some(ref owner) = query.owner {
            if disc.owner_node_id.as_deref() != Some(owner.as_str()) {
                continue;
            }
        }
        if let Some(ref search) = query.search {
            let search_lower = search.to_lowercase();
            let matches = disc.table.to_lowercase().contains(&search_lower)
                || disc.schema.to_lowercase().contains(&search_lower)
                || disc.catalog.to_lowercase().contains(&search_lower);
            if !matches {
                continue;
            }
        }
        summaries.push(disc);
    }
    summaries.sort_by(|a, b| a.key.cmp(&b.key));

    let total = summaries.len();

    // Apply pagination (page is 1-based)
    let page = query.page.max(1);
    let page_size = query.page_size;
    let offset = ((page - 1) * page_size) as usize;
    let total_pages = if total == 0 {
        0
    } else {
        ((total as u32) + page_size - 1) / page_size
    };

    summaries = summaries
        .into_iter()
        .skip(offset)
        .take(page_size as usize)
        .collect();

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

#[derive(Deserialize)]
struct ReassignTableRequest {
    new_owner_id: String,
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

// ============ Meta Endpoints ============

/// Health check endpoint
async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({
        "status": "ok",
        "service": "admin"
    })))
}

/// Distribution config response matching frontend DistributionConfig type
#[derive(Serialize)]
struct DistributionConfigResponse {
    scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    replication_factor: u8,
    placement_strategy: String,
}

/// Get distribution configuration
async fn distribution(State(state): State<AdminState>) -> impl IntoResponse {
    let client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Vec<DistributionConfigResponse>>::error(format!("Failed to connect to meta: {}", e)))
            )
        }
    };

    match client.list_distribution_configs(None).await {
        Ok(configs) => {
            let result: Vec<DistributionConfigResponse> = configs.iter().map(|c| {
                use crate::meta::types::DistributionScope;
                let scope = match c.scope {
                    DistributionScope::Catalog => "Catalog",
                    DistributionScope::Schema => "Schema",
                    DistributionScope::Table => "Table",
                };
                DistributionConfigResponse {
                    scope: scope.to_string(),
                    catalog: if c.catalog.is_empty() { None } else { Some(c.catalog.clone()) },
                    schema: if c.schema.is_empty() { None } else { Some(c.schema.clone()) },
                    table: if c.table.is_empty() { None } else { Some(c.table.clone()) },
                    // Map percent to a replication-like factor:
                    // percent represents distribution spread; expose as replication_factor
                    replication_factor: if c.percent == 0 { 1 } else { (c.percent / 25).max(1).min(5) },
                    placement_strategy: "RoundRobin".to_string(),
                }
            }).collect();
            (StatusCode::OK, Json(ApiResponse::ok(result)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Vec<DistributionConfigResponse>>::error(format!("Failed to get distribution: {}", e)))
        ),
    }
}

/// Set distribution config request from frontend
#[derive(Deserialize)]
struct SetDistributionRequest {
    scope: String,
    catalog: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    replication_factor: u8,
}

/// Set distribution configuration
async fn set_distribution(
    State(state): State<AdminState>,
    Json(body): Json<SetDistributionRequest>,
) -> impl IntoResponse {
    let client = match state.get_meta_client().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<serde_json::Value>::error(format!("Failed to connect to meta: {}", e)))
            )
        }
    };

    let catalog = body.catalog.unwrap_or_default();
    let schema = body.schema.unwrap_or_default();
    let table = body.table.unwrap_or_default();
    // Map replication_factor back to percent (1=0%, 2=50%, 3=75%, 4=100%, 5=100%)
    let percent = match body.replication_factor {
        0 | 1 => 0,
        2 => 50,
        3 => 75,
        4 | 5 => 100,
        _ => 100,
    };

    match client.set_distribution_config(&catalog, &schema, &table, percent).await {
        Ok(_version) => (
            StatusCode::OK,
            Json(ApiResponse::ok(serde_json::json!({"success": true})))
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<serde_json::Value>::error(format!("Failed to set distribution config: {}", e)))
        ),
    }
}

#[derive(Serialize)]
struct MetaStatusResponse {
    leader_id: Option<String>,
    term: u64,
    commit_index: u64,
    is_healthy: bool,
    peers: Vec<MetaPeerInfo>,
}

#[derive(Serialize)]
struct MetaPeerInfo {
    id: String,
    address: String,
    is_leader: bool,
    is_healthy: bool,
    last_heartbeat: Option<String>,
}

/// Derive HTTP URL from a meta gRPC endpoint address.
/// Convention: meta HTTP port = gRPC port - 1 (e.g., gRPC 6001 → HTTP 6000).
fn derive_meta_http_url(grpc_endpoint: &str) -> Option<String> {
    // endpoint is "host:port" (gRPC)
    let parts: Vec<&str> = grpc_endpoint.rsplitn(2, ':').collect();
    if parts.len() != 2 {
        return None;
    }
    let port: u16 = parts[0].parse().ok()?;
    let host = parts[1];
    let http_port = port.checked_sub(1)?;
    Some(format!("http://{}:{}", host, http_port))
}

/// Probe a single meta node's /health endpoint and return peer info.
async fn probe_meta_health(
    http_client: &reqwest::Client,
    grpc_endpoint: &str,
) -> Option<MetaPeerInfo> {
    let http_url = derive_meta_http_url(grpc_endpoint)?;
    let url = format!("{}/health", http_url);

    let resp = http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return Some(MetaPeerInfo {
            id: String::new(),
            address: grpc_endpoint.to_string(),
            is_leader: false,
            is_healthy: false,
            last_heartbeat: None,
        });
    }

    let body: serde_json::Value = resp.json().await.ok()?;

    let node_id = body
        .get("node_id")
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let is_leader = body.get("is_leader").and_then(|v| v.as_bool()).unwrap_or(false);
    let is_healthy = body
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s == "healthy")
        .unwrap_or(true); // If no status field, assume healthy since endpoint responded

    Some(MetaPeerInfo {
        id: node_id,
        address: grpc_endpoint.to_string(),
        is_leader,
        is_healthy,
        last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
    })
}

/// Get meta node status by probing each meta endpoint's HTTP /health
async fn meta_status(State(state): State<AdminState>) -> impl IntoResponse {
    // Get cluster status from gRPC for leader/health info
    let cluster_status = match state.get_meta_client().await {
        Ok(client) => client.get_cluster_status().await.ok(),
        Err(_) => None,
    };

    // Probe each meta endpoint's HTTP health in parallel
    let futures: Vec<_> = state
        .meta_endpoints
        .iter()
        .map(|ep| probe_meta_health(&state.http_client, ep))
        .collect();
    let results = futures::future::join_all(futures).await;

    let mut peers: Vec<MetaPeerInfo> = results.into_iter().flatten().collect();

    // If we couldn't reach any meta node via HTTP, add entries showing them as unhealthy
    if peers.is_empty() {
        for ep in &state.meta_endpoints {
            peers.push(MetaPeerInfo {
                id: String::new(),
                address: ep.clone(),
                is_leader: false,
                is_healthy: false,
                last_heartbeat: None,
            });
        }
    }

    let (leader_id, is_healthy) = match &cluster_status {
        Some(cs) => (
            if cs.meta_leader.is_empty() {
                None
            } else {
                Some(cs.meta_leader.clone())
            },
            cs.healthy,
        ),
        None => (None, false),
    };

    let response = MetaStatusResponse {
        leader_id,
        term: 0,
        commit_index: 0,
        is_healthy,
        peers,
    };
    (StatusCode::OK, Json(ApiResponse::ok(response)))
}

// ============ Debug Tools Endpoints ============

#[derive(Deserialize)]
struct DebugProfileQuery {
    duration: Option<u64>,
    format: Option<String>,
}

/// Forward heap profile request to data node and stream the response
async fn debug_heap_profile(
    State(state): State<AdminState>,
    Query(query): Query<DebugProfileQuery>,
) -> Response<Body> {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&ApiResponse::<()>::error(e)).unwrap(),
                ))
                .unwrap();
        }
    };

    let mut params = Vec::new();
    if let Some(duration) = query.duration {
        params.push(format!("duration={}", duration));
    }
    if let Some(ref format) = query.format {
        params.push(format!("format={}", format));
    }
    let query_string = if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    };

    let url = format!("{}/debug/heap{}", node_url, query_string);
    forward_debug_profile(&state, &url).await
}

/// Forward CPU profile request to data node and stream the response
async fn debug_cpu_profile(
    State(state): State<AdminState>,
    Query(query): Query<DebugProfileQuery>,
) -> Response<Body> {
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&ApiResponse::<()>::error(e)).unwrap(),
                ))
                .unwrap();
        }
    };

    let mut params = Vec::new();
    if let Some(duration) = query.duration {
        params.push(format!("duration={}", duration));
    }
    if let Some(ref format) = query.format {
        params.push(format!("format={}", format));
    }
    let query_string = if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    };

    let url = format!("{}/debug/cpu{}", node_url, query_string);
    forward_debug_profile(&state, &url).await
}

/// Forward a debug profile request and stream the raw response back
async fn forward_debug_profile(state: &AdminState, url: &str) -> Response<Body> {
    let response = match state.http_client.get(url).send().await {
        Ok(r) => r,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&ApiResponse::<()>::error(format!(
                        "Failed to reach data node: {}",
                        e
                    )))
                    .unwrap(),
                ))
                .unwrap();
        }
    };

    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let content_disposition = response
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let bytes = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&ApiResponse::<()>::error(format!(
                        "Failed to read response: {}",
                        e
                    )))
                    .unwrap(),
                ))
                .unwrap();
        }
    };

    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type);

    if let Some(disposition) = content_disposition {
        builder = builder.header(header::CONTENT_DISPOSITION, disposition);
    }

    builder.body(Body::from(bytes)).unwrap()
}

#[derive(Serialize)]
struct CacheActionResponse {
    success: bool,
    message: String,
}

/// Prewarm cache for a catalog by forwarding to data node
async fn cache_prewarm(
    State(state): State<AdminState>,
    Path(catalog): Path<String>,
) -> impl IntoResponse {
    let path = format!("/cache/prewarm/{}", catalog);
    let (status, body) = match state.data_node_post(&path, "").await {
        Ok(r) => r,
        Err((code, msg)) => return (code, Json(ApiResponse::<CacheActionResponse>::error(msg))),
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<CacheActionResponse>::error(body)),
        );
    }

    (
        StatusCode::OK,
        Json(ApiResponse::ok(CacheActionResponse {
            success: true,
            message: format!("Cache prewarm triggered for catalog '{}'", catalog),
        })),
    )
}

/// Clear all caches by forwarding to data node
async fn cache_clear(State(state): State<AdminState>) -> impl IntoResponse {
    let (status, body) = match state.data_node_post("/cache/clear", "").await {
        Ok(r) => r,
        Err((code, msg)) => return (code, Json(ApiResponse::<CacheActionResponse>::error(msg))),
    };

    if !status.is_success() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<CacheActionResponse>::error(body)),
        );
    }

    (
        StatusCode::OK,
        Json(ApiResponse::ok(CacheActionResponse {
            success: true,
            message: "All caches cleared".to_string(),
        })),
    )
}

// ============ Election Endpoints ============

#[derive(Serialize)]
struct ElectionInfo {
    name: String,
    leader: Option<String>,
    term: u64,
    has_leader: bool,
}

/// Known election names in the cluster
const KNOWN_ELECTIONS: &[&str] = &["compactor", "retention"];

/// List all known elections and their current leaders
async fn list_elections(State(state): State<AdminState>) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<Vec<ElectionInfo>>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    let mut elections = Vec::new();
    for &election in KNOWN_ELECTIONS {
        let (leader, term, has_leader) = match meta_client.get_leader(election).await {
            Ok(Some((leader_id, leader_term))) => (Some(leader_id), leader_term, true),
            Ok(None) => (None, 0, false),
            Err(_) => (None, 0, false),
        };

        elections.push(ElectionInfo {
            name: election.to_string(),
            leader,
            term,
            has_leader,
        });
    }

    (StatusCode::OK, Json(ApiResponse::ok(elections)))
}

#[derive(Deserialize)]
struct CampaignRequest {
    #[serde(default = "default_campaign_ttl")]
    ttl_ms: u64,
}

fn default_campaign_ttl() -> u64 {
    30_000
}

#[derive(Serialize)]
struct CampaignResponse {
    elected: bool,
    term: u64,
    election: String,
}

/// Force campaign: make the admin node the leader for an election
async fn force_campaign(
    State(state): State<AdminState>,
    Path(election): Path<String>,
    Json(request): Json<CampaignRequest>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<CampaignResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    match meta_client.campaign(&election, request.ttl_ms).await {
        Ok(Some(term)) => (
            StatusCode::OK,
            Json(ApiResponse::ok(CampaignResponse {
                elected: true,
                term,
                election,
            })),
        ),
        Ok(None) => (
            StatusCode::OK,
            Json(ApiResponse::ok(CampaignResponse {
                elected: false,
                term: 0,
                election,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<CampaignResponse>::error(format!(
                "Campaign failed: {}",
                e
            ))),
        ),
    }
}

#[derive(Serialize)]
struct ResignResponse {
    success: bool,
    election: String,
}

/// Force resign: resign the current leader for an election
async fn force_resign(
    State(state): State<AdminState>,
    Path(election): Path<String>,
) -> impl IntoResponse {
    let meta_client = match state.get_meta_client().await {
        Ok(client) => client,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ResignResponse>::error(format!(
                    "Failed to connect to Meta: {}",
                    e
                ))),
            );
        }
    };

    // Get current leader and term
    let term = match meta_client.get_leader(&election).await {
        Ok(Some((_, t))) => t,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<ResignResponse>::error(format!(
                    "No leader for election '{}'",
                    election
                ))),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<ResignResponse>::error(format!(
                    "Failed to get leader: {}",
                    e
                ))),
            );
        }
    };

    match meta_client.resign(&election, term).await {
        Ok(()) => (
            StatusCode::OK,
            Json(ApiResponse::ok(ResignResponse {
                success: true,
                election,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<ResignResponse>::error(format!(
                "Resign failed: {}",
                e
            ))),
        ),
    }
}

// ============ Configuration Endpoint ============

#[derive(Serialize)]
struct ClusterConfigResponse {
    admin: AdminConfigInfo,
    node_counts: NodeCountInfo,
}

#[derive(Serialize)]
struct AdminConfigInfo {
    http_host: String,
    http_port: u16,
    meta_endpoints: Vec<String>,
}

#[derive(Serialize)]
struct NodeCountInfo {
    meta: u32,
    router: u32,
    data: u32,
    compactor: u32,
}

/// Get cluster configuration
async fn get_config(State(state): State<AdminState>) -> impl IntoResponse {
    let admin_info = AdminConfigInfo {
        http_host: state.config.http_host.clone(),
        http_port: state.config.http_port,
        meta_endpoints: state.meta_endpoints.clone(),
    };

    // Get node counts from meta
    let node_counts = if let Ok(meta_client) = state.get_meta_client().await {
        if let Ok(nodes) = meta_client.list_nodes(None, None).await {
            let mut counts = NodeCountInfo {
                meta: 0,
                router: 0,
                data: 0,
                compactor: 0,
            };
            for node in &nodes {
                match node.role {
                    NodeRole::Meta => counts.meta += 1,
                    NodeRole::Router => counts.router += 1,
                    NodeRole::Data => counts.data += 1,
                    NodeRole::Compactor => counts.compactor += 1,
                    NodeRole::Unknown => {}
                }
            }
            counts
        } else {
            NodeCountInfo {
                meta: 0,
                router: 0,
                data: 0,
                compactor: 0,
            }
        }
    } else {
        NodeCountInfo {
            meta: 0,
            router: 0,
            data: 0,
            compactor: 0,
        }
    };

    (
        StatusCode::OK,
        Json(ApiResponse::ok(ClusterConfigResponse {
            admin: admin_info,
            node_counts,
        })),
    )
}

// ============ Test Data Generator ============

#[derive(Deserialize)]
struct GenerateDataRequest {
    #[serde(default = "default_data_types")]
    types: Vec<String>,
    #[serde(default = "default_points")]
    points_per_source: u32,
    #[serde(default)]
    date: Option<String>,
    #[serde(default = "default_database")]
    database: String,
}

fn default_data_types() -> Vec<String> {
    vec!["cpu".into()]
}

fn default_points() -> u32 {
    100
}

fn default_database() -> String {
    "testdb".into()
}

#[derive(Serialize)]
struct GenerateDataResponse {
    database: String,
    date: String,
    results: Vec<GenerateTypeResult>,
    total_points: u32,
}

#[derive(Serialize)]
struct GenerateTypeResult {
    #[serde(rename = "type")]
    data_type: String,
    sources: u32,
    points_written: u32,
}

/// Generate and write test data to a data node
async fn generate_test_data(
    State(state): State<AdminState>,
    Json(request): Json<GenerateDataRequest>,
) -> impl IntoResponse {
    // Resolve date
    let date = request.date.unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());

    // Parse date to get timestamp range
    let day_start = match chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d") {
        Ok(d) => d
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<GenerateDataResponse>::error(format!(
                    "Invalid date format: {}. Use YYYY-MM-DD",
                    date
                ))),
            );
        }
    };

    let points = request.points_per_source.min(10000).max(1);
    let interval_sec = if points > 1 { 86399 / (points as i64 - 1) } else { 0 };

    // Get data node URL to write to
    let node_url = match state.get_data_node_url().await {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<GenerateDataResponse>::error(format!(
                    "No data node available: {}",
                    e
                ))),
            );
        }
    };

    let write_url = format!("{}/write/{}", node_url, request.database);
    let mut results = Vec::new();
    let mut total_points = 0u32;

    for data_type in &request.types {
        let (lines, sources) = match data_type.as_str() {
            "cpu" => (generate_cpu_data(day_start, interval_sec, points), 5u32),
            "memory" => (generate_memory_data(day_start, interval_sec, points), 5),
            "disk" => (generate_disk_data(day_start, interval_sec, points), 10),
            "http" => (generate_http_data(day_start, interval_sec, points), 3),
            "iot" => (generate_iot_data(day_start, interval_sec, points), 8),
            "app" => (generate_app_data(day_start, interval_sec, points), 3),
            other => {
                results.push(GenerateTypeResult {
                    data_type: other.to_string(),
                    sources: 0,
                    points_written: 0,
                });
                continue;
            }
        };

        let line_count = lines.len() as u32;

        // Write in batches
        let mut written = 0u32;
        for chunk in lines.chunks(500) {
            let body = chunk.join("\n");
            match state
                .http_client
                .post(&write_url)
                .body(body)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    written += chunk.len() as u32;
                }
                Ok(resp) => {
                    let status = resp.status();
                    let msg = resp.text().await.unwrap_or_default();
                    tracing::warn!(
                        "Write batch failed for {}: HTTP {} - {}",
                        data_type,
                        status,
                        msg
                    );
                }
                Err(e) => {
                    tracing::warn!("Write batch failed for {}: {}", data_type, e);
                }
            }
        }

        total_points += written;
        results.push(GenerateTypeResult {
            data_type: data_type.clone(),
            sources,
            points_written: written,
        });

        tracing::info!(
            "Generated {} data: {}/{} points written to {}",
            data_type,
            written,
            line_count,
            request.database
        );
    }

    let response = GenerateDataResponse {
        database: request.database,
        date,
        results,
        total_points,
    };

    (StatusCode::OK, Json(ApiResponse::ok(response)))
}

// --- Data generators ---
// Each returns a Vec<String> of InfluxDB line protocol lines.

use rand::Rng;

fn generate_cpu_data(day_start: i64, interval_sec: i64, points: u32) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let hosts = [
        ("web-1", "us-east"),
        ("web-2", "us-west"),
        ("web-3", "eu-west"),
        ("api-1", "us-east"),
        ("db-1", "us-west"),
    ];
    let mut lines = Vec::with_capacity(hosts.len() * points as usize);

    for (i, (host, region)) in hosts.iter().enumerate() {
        let base_user = 10 + i * 8;
        let base_sys = 3 + i * 2;
        for p in 0..points {
            let ts = (day_start + p as i64 * interval_sec) * 1_000_000_000;
            let usage_user = (base_user as i32 + rng.gen_range(-5..=15)).clamp(1, 95) as u32;
            let usage_system = (base_sys as i32 + rng.gen_range(-3..=5)).clamp(0, 20) as u32;
            let usage_idle = 100u32.saturating_sub(usage_user + usage_system);
            let load_1m: f32 = rng.gen_range(0.1..8.0);
            let load_5m: f32 = rng.gen_range(0.1..6.0);
            lines.push(format!(
                "cpu,host={},region={},service=monitoring usage_user={:.1},usage_system={:.1},usage_idle={:.1},load_1m={:.2},load_5m={:.2},cores=8i {}",
                host, region, usage_user as f64, usage_system as f64, usage_idle as f64, load_1m, load_5m, ts
            ));
        }
    }
    lines
}

fn generate_memory_data(day_start: i64, interval_sec: i64, points: u32) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let hosts = [
        ("web-1", "us-east", 16384u64),
        ("web-2", "us-west", 16384),
        ("web-3", "eu-west", 16384),
        ("api-1", "us-east", 32768),
        ("db-1", "us-west", 65536),
    ];
    let mut lines = Vec::with_capacity(hosts.len() * points as usize);

    for (i, (host, region, total_mb)) in hosts.iter().enumerate() {
        let base_pct = 50 + i * 8;
        for p in 0..points {
            let ts = (day_start + p as i64 * interval_sec) * 1_000_000_000;
            let pct = (base_pct as i32 + rng.gen_range(-10..=10)).clamp(10, 95) as u64;
            let used_mb = total_mb * pct / 100;
            let available_mb = total_mb - used_mb;
            let swap_used: u64 = rng.gen_range(0..512);
            lines.push(format!(
                "memory,host={},region={},service=monitoring total_mb={}i,used_mb={}i,available_mb={}i,usage_percent={:.1},swap_used_mb={}i {}",
                host, region, total_mb, used_mb, available_mb, pct as f64, swap_used, ts
            ));
        }
    }
    lines
}

fn generate_disk_data(day_start: i64, interval_sec: i64, points: u32) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let hosts = [
        ("web-1", "us-east"),
        ("web-2", "us-west"),
        ("web-3", "eu-west"),
        ("api-1", "us-east"),
        ("db-1", "us-west"),
    ];
    let devices = ["sda", "sdb"];
    let mut lines = Vec::with_capacity(hosts.len() * devices.len() * points as usize);

    for (host, region) in &hosts {
        for device in &devices {
            for p in 0..points {
                let ts = (day_start + p as i64 * interval_sec) * 1_000_000_000;
                let read_bytes: u64 = rng.gen_range(1024..33_554_432);
                let write_bytes: u64 = rng.gen_range(512..16_777_216);
                let read_ops: u32 = rng.gen_range(10..500);
                let write_ops: u32 = rng.gen_range(5..300);
                let io_util: f32 = rng.gen_range(0.0..95.0);
                lines.push(format!(
                    "disk_io,host={},region={},device={},service=monitoring read_bytes={}i,write_bytes={}i,read_ops={}i,write_ops={}i,io_util={:.1} {}",
                    host, region, device, read_bytes, write_bytes, read_ops, write_ops, io_util, ts
                ));
            }
        }
    }
    lines
}

fn generate_http_data(day_start: i64, interval_sec: i64, points: u32) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let services = [
        ("frontend", &["/index.html", "/dashboard", "/login", "/settings", "/search", "/profile"][..]),
        ("api-gateway", &["/v1/users", "/v1/orders", "/v1/products", "/v1/search", "/v1/health"][..]),
        ("auth-service", &["/auth/login", "/auth/token", "/auth/refresh", "/auth/logout"][..]),
    ];
    let methods = ["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"];
    let statuses = ["200", "200", "200", "200", "200", "201", "204", "301", "400", "401", "404", "500"];
    let mut lines = Vec::with_capacity(services.len() * points as usize);

    for (service, endpoints) in &services {
        for p in 0..points {
            let ts = (day_start + p as i64 * interval_sec) * 1_000_000_000;
            let method = methods[rng.gen_range(0..methods.len())];
            let endpoint = endpoints[rng.gen_range(0..endpoints.len())];
            let status = statuses[rng.gen_range(0..statuses.len())];
            // 90% fast, 5% moderate, 5% slow
            let latency: f32 = match rng.gen_range(0..100) {
                0..=89 => rng.gen_range(1.0..50.0),
                90..=94 => rng.gen_range(50.0..200.0),
                _ => rng.gen_range(200.0..2000.0),
            };
            let req_bytes: u32 = rng.gen_range(64..4096);
            let resp_bytes: u32 = rng.gen_range(128..65536);
            lines.push(format!(
                "http_requests,service={},method={},endpoint={},status={} latency_ms={:.1},request_bytes={}i,response_bytes={}i {}",
                service, method, endpoint, status, latency, req_bytes, resp_bytes, ts
            ));
        }
    }
    lines
}

fn generate_iot_data(day_start: i64, interval_sec: i64, points: u32) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let sensors = [
        ("sensor-01", "warehouse-a", "environmental", 22.0f32),
        ("sensor-02", "warehouse-a", "environmental", 22.0),
        ("sensor-03", "warehouse-b", "environmental", 21.0),
        ("sensor-04", "warehouse-b", "environmental", 21.0),
        ("sensor-05", "office-1", "environmental", 24.0),
        ("sensor-06", "office-1", "environmental", 24.0),
        ("sensor-07", "outdoor", "weather", 18.0),
        ("sensor-08", "outdoor", "weather", 18.0),
    ];
    let mut lines = Vec::with_capacity(sensors.len() * points as usize);

    for (sid, location, stype, base_temp) in &sensors {
        for p in 0..points {
            let ts = (day_start + p as i64 * interval_sec) * 1_000_000_000;
            // Daily temperature cycle
            let hour = (p as f32 / points as f32) * 24.0;
            let temp_offset = if hour >= 6.0 && hour <= 14.0 {
                (hour - 6.0) * 0.8
            } else if hour > 14.0 && hour <= 22.0 {
                (22.0 - hour) * 0.8
            } else {
                -2.0
            };
            let temperature = base_temp + temp_offset + rng.gen_range(-1.0..1.0);
            let humidity: f32 = rng.gen_range(30.0..80.0);
            let pressure: f32 = rng.gen_range(1010.0..1025.0);
            let battery = (100.0 - (p as f32 / points as f32) * 15.0 + rng.gen_range(-1.0..1.0))
                .clamp(5.0, 100.0);
            lines.push(format!(
                "sensor_data,sensor_id={},location={},type={},service=iot_gateway temperature={:.1},humidity={:.1},pressure={:.1},battery_pct={:.0} {}",
                sid, location, stype, temperature, humidity, pressure, battery, ts
            ));
        }
    }
    lines
}

fn generate_app_data(day_start: i64, interval_sec: i64, points: u32) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let services = [
        ("order-service", "app-order-1"),
        ("payment-service", "app-payment-1"),
        ("notification-service", "app-notify-1"),
    ];
    let levels = ["info", "info", "info", "info", "info", "warn", "error"];
    let mut lines = Vec::with_capacity(services.len() * points as usize);

    for (service, host) in &services {
        for p in 0..points {
            let ts = (day_start + p as i64 * interval_sec) * 1_000_000_000;
            let level = levels[rng.gen_range(0..levels.len())];
            let response_time: f32 = if level == "error" {
                rng.gen_range(500.0..5000.0)
            } else {
                rng.gen_range(5.0..200.0)
            };
            let queue_depth: u32 = rng.gen_range(0..50);
            let error_count: u32 = match level {
                "error" => rng.gen_range(1..=5),
                "warn" => rng.gen_range(0..=1),
                _ => 0,
            };
            // Active users: higher during business hours
            let hour = (p as f32 / points as f32) * 24.0;
            let active_users: u32 = if hour >= 9.0 && hour <= 17.0 {
                rng.gen_range(200..700)
            } else {
                rng.gen_range(10..100)
            };
            lines.push(format!(
                "app_events,service={},host={},level={} response_time_ms={:.1},queue_depth={}i,error_count={}i,active_users={}i {}",
                service, host, level, response_time, queue_depth, error_count, active_users, ts
            ));
        }
    }
    lines
}
