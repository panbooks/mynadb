//! HTTP API client for admin backend

use admin_types::*;
use gloo_net::http::Request;
use serde::de::DeserializeOwned;

/// Base URL for API requests (same origin)
fn api_base() -> String {
    // In development, this will be proxied by trunk
    // In production, the admin backend serves both static files and API
    "/admin/api/v1".to_string()
}

/// Generic API error
#[derive(Debug, Clone)]
pub struct ApiError {
    pub message: String,
    pub status: Option<u16>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(status) = self.status {
            write!(f, "[{}] {}", status, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl From<gloo_net::Error> for ApiError {
    fn from(err: gloo_net::Error) -> Self {
        Self {
            message: err.to_string(),
            status: None,
        }
    }
}

/// Make a GET request and deserialize the response
async fn get<T: DeserializeOwned>(path: &str) -> Result<T, ApiError> {
    let url = format!("{}{}", api_base(), path);
    let response = Request::get(&url)
        .header("Accept", "application/json")
        .send()
        .await?;

    if !response.ok() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(ApiError {
            message: text,
            status: Some(status),
        });
    }

    response.json::<T>().await.map_err(|e| ApiError {
        message: format!("Failed to parse response: {}", e),
        status: None,
    })
}

/// Make a POST request with JSON body
async fn post<T: DeserializeOwned, B: serde::Serialize>(path: &str, body: &B) -> Result<T, ApiError> {
    let url = format!("{}{}", api_base(), path);
    let response = Request::post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(body)?
        .send()
        .await?;

    if !response.ok() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(ApiError {
            message: text,
            status: Some(status),
        });
    }

    response.json::<T>().await.map_err(|e| ApiError {
        message: format!("Failed to parse response: {}", e),
        status: None,
    })
}

// ============ Cluster API ============

/// Fetch cluster status
pub async fn fetch_cluster_status() -> Result<ClusterStatus, ApiError> {
    get("/cluster/status").await
}

// ============ Nodes API ============

/// Fetch list of nodes
pub async fn fetch_nodes(query: &ListNodesQuery) -> Result<ListNodesResponse, ApiError> {
    let mut path = "/nodes".to_string();
    let mut params = Vec::new();

    if let Some(ref role) = query.role {
        params.push(format!("role={}", role));
    }
    if let Some(ref state) = query.state {
        params.push(format!("state={}", state));
    }
    if let Some(ref mode) = query.mode {
        params.push(format!("mode={}", mode));
    }

    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }

    get(&path).await
}

/// Fetch node details
pub async fn fetch_node(node_id: &str) -> Result<NodeDetailResponse, ApiError> {
    get(&format!("/nodes/{}", node_id)).await
}

// ============ Tables API ============

/// Fetch list of tables
pub async fn fetch_tables(query: &ListTablesQuery) -> Result<ListTablesResponse, ApiError> {
    let mut path = "/tables".to_string();
    let mut params = Vec::new();

    if let Some(ref catalog) = query.catalog {
        params.push(format!("catalog={}", catalog));
    }
    if let Some(ref schema) = query.schema {
        params.push(format!("schema={}", schema));
    }
    if let Some(ref owner) = query.owner {
        params.push(format!("owner={}", owner));
    }
    if let Some(ref search) = query.search {
        params.push(format!("search={}", search));
    }
    if query.offset > 0 {
        params.push(format!("offset={}", query.offset));
    }
    if query.limit != 100 {
        params.push(format!("limit={}", query.limit));
    }

    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }

    get(&path).await
}

/// Reassign table to new owner
pub async fn reassign_table(table_key: &str, request: &ReassignTableRequest) -> Result<ReassignTableResponse, ApiError> {
    post(&format!("/tables/{}/reassign", table_key), request).await
}

// ============ Scale-In API ============

/// Get scale-in recommendations
pub async fn fetch_scale_in_recommend(query: &ScaleInRecommendQuery) -> Result<ScaleInRecommendation, ApiError> {
    let mut path = "/scale-in/recommend".to_string();
    let mut params = Vec::new();

    if query.primaries > 0 {
        params.push(format!("primaries={}", query.primaries));
    }
    if query.standbys > 0 {
        params.push(format!("standbys={}", query.standbys));
    }

    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }

    get(&path).await
}

/// Generate scale-in plan
pub async fn create_scale_in_plan(request: &ScaleInPlanRequest) -> Result<ScaleInPlan, ApiError> {
    post("/scale-in/plan", request).await
}

/// Execute scale-in plan
pub async fn execute_scale_in(request: &ScaleInPlanRequest) -> Result<ScaleInResult, ApiError> {
    post("/scale-in/execute", request).await
}

/// Get scale-in status
pub async fn fetch_scale_in_status() -> Result<Option<ScaleInStatus>, ApiError> {
    get("/scale-in/status").await
}

/// Cancel scale-in operation
pub async fn cancel_scale_in() -> Result<(), ApiError> {
    post::<serde_json::Value, _>("/scale-in/cancel", &()).await?;
    Ok(())
}

// ============ Events API ============

/// Fetch recent events
pub async fn fetch_events(query: &ListEventsQuery) -> Result<ListEventsResponse, ApiError> {
    let mut path = "/events".to_string();
    let mut params = Vec::new();

    if let Some(ref event_type) = query.event_type {
        params.push(format!("event_type={}", event_type));
    }
    if let Some(ref source_node) = query.source_node {
        params.push(format!("source_node={}", source_node));
    }
    if let Some(start_sequence) = query.start_sequence {
        params.push(format!("start_sequence={}", start_sequence));
    }
    if query.limit > 0 {
        params.push(format!("limit={}", query.limit));
    }

    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }

    get(&path).await
}
