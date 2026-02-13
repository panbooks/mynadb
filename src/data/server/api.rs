//! Data Node HTTP API handlers
//!
//! Provides HTTP endpoints for monitoring, administration, and data operations.
//! Uses shared handlers from services/shared.rs to avoid code duplication.

use axum::{
    extract::{Path, Query as AxumQuery, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

use crate::config;
use crate::jobs::compactor::Compactor;
use crate::jobs::database_manager::DatabaseManager;
use crate::meta::client::MetaClient;
use crate::query::query_engine::QueryEngine;
use crate::services::shared;
use crate::storage::cache::DataCache;
use crate::storage::wal::Wal;

// ============================================================================
// State and Types
// ============================================================================

/// Data node HTTP state
#[derive(Clone)]
pub struct DataNodeState {
    pub node_id: String,
    pub start_time: Instant,
    pub cache: Arc<DataCache>,
    pub meta_client: Option<Arc<MetaClient>>,
    pub is_retiring: Arc<AtomicBool>,
    pub retire_status: Arc<RwLock<Option<RetireStatusDetail>>>,
    pub query_engine: Option<Arc<QueryEngine>>,
    pub config: Option<Arc<config::Config>>,
    pub wal: Option<Arc<Wal>>,
    pub compactor: Option<Arc<Compactor>>,
    pub db_manager: Option<Arc<DatabaseManager>>,
    /// Counter for in-flight queries (shared with Flight service)
    pub in_flight_queries: Option<Arc<std::sync::atomic::AtomicU64>>,
}

/// Detailed retirement status
#[derive(Clone, Serialize)]
pub struct RetireStatusDetail {
    pub state: String,
    pub started_at: i64,
    pub partitions_to_reassign: usize,
    pub memtable_rows_remaining: u64,
}

// Response types
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    node_id: String,
    node_type: &'static str,
}

#[derive(Serialize)]
struct StatsResponse {
    node_id: String,
    node_type: &'static str,
    uptime_secs: u64,
    cache_size_bytes: usize,
    cache_entries: usize,
    total_rows: usize,
}

#[derive(Serialize)]
struct VersionResponse {
    version: &'static str,
    git_hash: &'static str,
}

// Request types
#[derive(Deserialize)]
pub struct QueryParams {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct WriteQueryParams {
    pub db: String,
}

#[derive(Deserialize)]
pub struct RetentionRequest {
    #[serde(default)]
    pub before_date: Option<String>,
    #[serde(default)]
    pub older_than_days: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ExplainParams {
    pub sql: String,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub analyze: bool,
}

#[derive(Deserialize, Default)]
pub struct RetireRequest {
    /// If true, skip the automatic flush on retire (default: false).
    /// By default, retire triggers an immediate flush to accelerate draining.
    #[serde(default)]
    pub defer_flush: bool,
}

#[derive(Serialize)]
pub struct RetireResponse {
    pub success: bool,
    pub status: String,
    pub partitions_to_reassign: usize,
    /// True if an immediate flush was triggered to accelerate draining
    #[serde(default)]
    pub flush_triggered: bool,
    /// Number of memtable rows being flushed (0 if no flush or already empty)
    #[serde(default)]
    pub memtable_rows_flushing: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Default retirement timeout in seconds (5 minutes)
const DEFAULT_RETIRE_TIMEOUT_SECS: u64 = 300;

#[derive(Serialize)]
pub struct RetireStatusResponse {
    pub state: String,
    pub is_retiring: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_to_reassign: Option<usize>,
    pub memtable_rows: u64,
    pub in_flight_queries: u64,
    /// True when node is ready for shutdown (retiring, no in-flight queries, memtable drained)
    pub ready_for_shutdown: bool,
    /// Seconds elapsed since retirement started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_secs: Option<u64>,
    /// True when retirement has exceeded timeout and force shutdown is recommended
    pub timeout_exceeded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_completion_secs: Option<u64>,
}

// ============================================================================
// Router
// ============================================================================

/// Build the Data node HTTP API router
pub fn build_router(state: DataNodeState) -> Router {
    Router::new()
        // Basic endpoints
        .route("/", get(home))
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/stats", get(stats))
        .route("/metrics", get(metrics_handler))
        // Retirement endpoints
        .route("/retire", post(retire))
        .route("/retire/status", get(retire_status))
        .route("/retire/force", post(force_retire))
        .route("/retire/cancel", post(cancel_retire))
        // Query endpoints
        .route("/query/:db", get(query_handler))
        .route("/query/:db", post(query_handler_post))
        .route("/explain/:db", get(explain_handler))
        .route("/explain/:db", post(explain_handler_post))
        .route("/analyze/:db", get(analyze_handler))
        .route("/analyze/:db", post(analyze_handler_post))
        // Write endpoints
        .route("/write/:db", post(write_handler))
        .route("/write", post(write_handler_query_param))
        // Schema/Table endpoints
        .route("/schemas/:db", get(list_schemas_handler))
        .route("/tables/:db", get(list_tables_handler))
        .route("/tables/:db/:schema", get(list_schema_tables_handler))
        .route("/schema/:db/:schema/:table", get(schema_handler))
        // Cache management
        .route("/flush", post(flush_handler))
        .route("/memory", get(memory_handler))
        .route("/cache/stats", get(query_cache_stats_handler))
        .route("/cache/clear", post(clear_cache_handler))
        .route("/cache/prewarm/:catalog", post(prewarm_cache_handler))
        .route(
            "/cache/prewarm/:catalog/:schema/:table",
            post(prewarm_table_cache_handler),
        )
        // Compaction endpoints
        .route("/compact/:catalog", post(compact_handler))
        .route(
            "/compact/:catalog/:schema/:table",
            post(compact_table_handler),
        )
        .route(
            "/compact/:catalog/:schema/:table/:date",
            post(compact_partition_handler),
        )
        // Partition management
        .route(
            "/partitions/:catalog/:schema/:table",
            get(list_partitions_handler),
        )
        .route(
            "/partitions/:catalog/:schema/:table/:date",
            delete(delete_partition_handler),
        )
        .route(
            "/retention/:catalog/:schema/:table",
            post(delete_old_partitions_handler),
        )
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::max(250 * 1024 * 1024)) // 250MB
}

// ============================================================================
// Basic Handlers
// ============================================================================

async fn home() -> &'static str {
    "MynaDB Data Node\n\n\
     Basic Endpoints:\n\
       GET  /health              - Health check\n\
       GET  /version             - Version info\n\
       GET  /stats               - Node statistics\n\
       GET  /metrics             - Prometheus metrics\n\n\
     Scale-In Endpoints:\n\
       POST /retire              - Start graceful retirement\n\
       GET  /retire/status       - Get retirement progress\n\
       POST /retire/force        - Force shutdown (after timeout)\n\
       POST /retire/cancel       - Cancel retirement\n\n\
     Query Endpoints:\n\
       GET  /query/:db?sql=      - Execute SQL query (GET)\n\
       POST /query/:db           - Execute SQL query (POST body)\n\
       GET  /explain/:db?sql=    - Get query execution plan\n\
       POST /explain/:db         - Get query execution plan (POST)\n\
       GET  /analyze/:db?sql=    - Analyze query with statistics\n\
       POST /analyze/:db         - Analyze query (POST)\n\n\
     Write Endpoints:\n\
       POST /write/:db           - Write data (line protocol)\n\
       POST /write?db=:db        - Write data (InfluxDB-compatible)\n\n\
     Schema/Table Endpoints:\n\
       GET  /schemas/:db         - List schemas\n\
       GET  /tables/:db          - List all tables\n\
       GET  /tables/:db/:schema  - List tables in schema\n\
       GET  /schema/:db/:schema/:table - Get table schema\n\n\
     Cache Management:\n\
       GET  /memory              - Memory usage stats\n\
       POST /flush               - Flush cache to disk\n\
       GET  /cache/stats         - Cache statistics\n\
       POST /cache/clear         - Clear all caches\n\
       POST /cache/prewarm/:catalog - Pre-warm cache\n\
       POST /cache/prewarm/:catalog/:schema/:table - Pre-warm table cache\n\n\
     Compaction:\n\
       POST /compact/:catalog    - Compact all tables in catalog\n\
       POST /compact/:catalog/:schema/:table - Compact table\n\
       POST /compact/:catalog/:schema/:table/:date - Compact partition\n\n\
     Partition Management:\n\
       GET    /partitions/:catalog/:schema/:table - List partitions\n\
       DELETE /partitions/:catalog/:schema/:table/:date - Delete partition\n\
       POST   /retention/:catalog/:schema/:table - Delete old partitions\n\n\
     Primary protocol: Arrow Flight\n"
}

async fn health(State(state): State<DataNodeState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "healthy",
            node_id: state.node_id.clone(),
            node_type: "data",
        }),
    )
}

async fn version() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(VersionResponse {
            version: env!("GIT_VERSION"),
            git_hash: env!("GIT_HASH"),
        }),
    )
}

async fn stats(State(state): State<DataNodeState>) -> impl IntoResponse {
    let cache_stats = state.cache.stats().await;
    (
        StatusCode::OK,
        Json(StatsResponse {
            node_id: state.node_id.clone(),
            node_type: "data",
            uptime_secs: state.start_time.elapsed().as_secs(),
            cache_size_bytes: cache_stats.size_bytes,
            cache_entries: cache_stats.num_measurements,
            total_rows: cache_stats.total_rows,
        }),
    )
}

async fn metrics_handler(State(state): State<DataNodeState>) -> impl IntoResponse {
    let cache_stats = state.cache.stats().await;
    let output = shared::generate_metrics(&cache_stats, "data");
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

// ============================================================================
// Retirement Handlers
// ============================================================================

async fn retire(
    State(state): State<DataNodeState>,
    Json(request): Json<RetireRequest>,
) -> impl IntoResponse {
    if state.is_retiring.load(Ordering::Relaxed) {
        return (
            StatusCode::OK,
            Json(RetireResponse {
                success: true,
                status: "already_retiring".to_string(),
                partitions_to_reassign: 0,
                flush_triggered: false,
                memtable_rows_flushing: 0,
                error: None,
            }),
        );
    }

    let meta_client = match &state.meta_client {
        Some(mc) => mc,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(RetireResponse {
                    success: false,
                    status: "error".to_string(),
                    partitions_to_reassign: 0,
                    flush_triggered: false,
                    memtable_rows_flushing: 0,
                    error: Some(
                        "Meta client not available - node running in standalone mode".to_string(),
                    ),
                }),
            );
        }
    };

    match meta_client.start_retire(&state.node_id).await {
        Ok(partitions_to_reassign) => {
            state.is_retiring.store(true, Ordering::SeqCst);
            let memtable_rows = state.cache.stats().await.total_rows as u64;
            *state.retire_status.write() = Some(RetireStatusDetail {
                state: "retiring".to_string(),
                started_at: chrono::Utc::now().timestamp_millis(),
                partitions_to_reassign,
                memtable_rows_remaining: memtable_rows,
            });

            // By default, trigger immediate flush to accelerate retire draining
            // This avoids waiting 0-60 seconds for the background flush interval
            let (flush_triggered, memtable_rows_flushing) =
                if !request.defer_flush && memtable_rows > 0 {
                    info!(
                    "Node {} retire: triggering immediate flush of {} rows to accelerate draining",
                    state.node_id, memtable_rows
                );
                    if let Err(e) = state.cache.flush().await {
                        tracing::warn!("Error during retire flush: {}", e);
                    }
                    (true, memtable_rows)
                } else if request.defer_flush && memtable_rows > 0 {
                    debug!(
                        "Node {} retire: deferring flush (defer_flush=true), {} rows in memtable",
                        state.node_id, memtable_rows
                    );
                    (false, 0)
                } else {
                    debug!(
                        "Node {} retire: memtable empty, no flush needed",
                        state.node_id
                    );
                    (false, 0)
                };

            info!(
                "Node {} started retirement: {} partitions to reassign, flush_triggered={}, memtable_rows={}",
                state.node_id, partitions_to_reassign, flush_triggered, memtable_rows_flushing
            );

            (
                StatusCode::OK,
                Json(RetireResponse {
                    success: true,
                    status: "retiring".to_string(),
                    partitions_to_reassign,
                    flush_triggered,
                    memtable_rows_flushing,
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RetireResponse {
                success: false,
                status: "error".to_string(),
                partitions_to_reassign: 0,
                flush_triggered: false,
                memtable_rows_flushing: 0,
                error: Some(e),
            }),
        ),
    }
}

async fn retire_status(State(state): State<DataNodeState>) -> impl IntoResponse {
    let is_retiring = state.is_retiring.load(Ordering::Relaxed);
    let cache_stats = state.cache.stats().await;
    let in_flight = state
        .in_flight_queries
        .as_ref()
        .map(|c| c.load(Ordering::Relaxed))
        .unwrap_or(0);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    if let Some(status) = state.retire_status.read().as_ref() {
        let current_state = if cache_stats.total_rows == 0 && in_flight == 0 {
            "drained"
        } else if cache_stats.total_rows == 0 {
            "draining_queries"
        } else {
            &status.state
        };

        let estimated_secs = if cache_stats.total_rows > 0 {
            Some((cache_stats.total_rows as u64 / 10000).max(1) * 60)
        } else {
            Some(0)
        };

        // Calculate elapsed time since retirement started
        let elapsed_secs = ((now_ms - status.started_at) / 1000).max(0) as u64;
        let timeout_exceeded = elapsed_secs > DEFAULT_RETIRE_TIMEOUT_SECS;

        // Ready for shutdown when retiring, memtable empty, and no in-flight queries
        let ready_for_shutdown = is_retiring && cache_stats.total_rows == 0 && in_flight == 0;

        (
            StatusCode::OK,
            Json(RetireStatusResponse {
                state: current_state.to_string(),
                is_retiring,
                started_at: Some(status.started_at),
                partitions_to_reassign: Some(status.partitions_to_reassign),
                memtable_rows: cache_stats.total_rows as u64,
                in_flight_queries: in_flight,
                ready_for_shutdown,
                elapsed_secs: Some(elapsed_secs),
                timeout_exceeded,
                estimated_completion_secs: estimated_secs,
            }),
        )
    } else {
        (
            StatusCode::OK,
            Json(RetireStatusResponse {
                state: "active".to_string(),
                is_retiring: false,
                started_at: None,
                partitions_to_reassign: None,
                memtable_rows: cache_stats.total_rows as u64,
                in_flight_queries: in_flight,
                ready_for_shutdown: false,
                elapsed_secs: None,
                timeout_exceeded: false,
                estimated_completion_secs: None,
            }),
        )
    }
}

async fn cancel_retire(State(state): State<DataNodeState>) -> impl IntoResponse {
    if !state.is_retiring.load(Ordering::Relaxed) {
        return (
            StatusCode::OK,
            Json(RetireResponse {
                success: true,
                status: "not_retiring".to_string(),
                partitions_to_reassign: 0,
                flush_triggered: false,
                memtable_rows_flushing: 0,
                error: None,
            }),
        );
    }

    let meta_client = match &state.meta_client {
        Some(mc) => mc,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(RetireResponse {
                    success: false,
                    status: "error".to_string(),
                    partitions_to_reassign: 0,
                    flush_triggered: false,
                    memtable_rows_flushing: 0,
                    error: Some("Meta client not available".to_string()),
                }),
            );
        }
    };

    match meta_client.cancel_retire(&state.node_id).await {
        Ok(()) => {
            state.is_retiring.store(false, Ordering::SeqCst);
            *state.retire_status.write() = None;
            info!("Node {} retirement cancelled", state.node_id);
            (
                StatusCode::OK,
                Json(RetireResponse {
                    success: true,
                    status: "active".to_string(),
                    partitions_to_reassign: 0,
                    flush_triggered: false,
                    memtable_rows_flushing: 0,
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RetireResponse {
                success: false,
                status: "error".to_string(),
                partitions_to_reassign: 0,
                flush_triggered: false,
                memtable_rows_flushing: 0,
                error: Some(e),
            }),
        ),
    }
}

/// Force retirement response
#[derive(Serialize)]
struct ForceRetireResponse {
    success: bool,
    message: String,
    in_flight_queries_cancelled: u64,
    memtable_rows_lost: u64,
}

/// Force retirement - immediately marks node as ready for shutdown
/// WARNING: This may cause data loss if memtable has unflushed data
/// and query failures for in-flight queries
async fn force_retire(State(state): State<DataNodeState>) -> impl IntoResponse {
    let is_retiring = state.is_retiring.load(Ordering::Relaxed);

    if !is_retiring {
        return (
            StatusCode::BAD_REQUEST,
            Json(ForceRetireResponse {
                success: false,
                message: "Node is not retiring. Call POST /retire first.".to_string(),
                in_flight_queries_cancelled: 0,
                memtable_rows_lost: 0,
            }),
        );
    }

    let cache_stats = state.cache.stats().await;
    let in_flight = state
        .in_flight_queries
        .as_ref()
        .map(|c| c.load(Ordering::Relaxed))
        .unwrap_or(0);

    // Log warning about potential data loss
    if cache_stats.total_rows > 0 || in_flight > 0 {
        tracing::warn!(
            "Force retire requested: {} memtable rows may be lost, {} queries may fail",
            cache_stats.total_rows,
            in_flight
        );
    }

    // Trigger emergency flush if there's data in memtable
    if cache_stats.total_rows > 0 {
        info!(
            "Force retire: triggering emergency flush of {} rows",
            cache_stats.total_rows
        );
        if let Err(e) = state.cache.flush().await {
            tracing::warn!("Emergency flush failed during force retire: {}", e);
        }
    }

    // Update status to indicate forced shutdown
    if let Some(status) = state.retire_status.write().as_mut() {
        status.state = "force_drained".to_string();
    }

    info!(
        "Node {} force retire complete: {} queries cancelled, {} rows flushed",
        state.node_id, in_flight, cache_stats.total_rows
    );

    (
        StatusCode::OK,
        Json(ForceRetireResponse {
            success: true,
            message: "Node marked for forced shutdown. Emergency flush triggered.".to_string(),
            in_flight_queries_cancelled: in_flight,
            memtable_rows_lost: 0, // We attempted flush, so hopefully 0
        }),
    )
}

// ============================================================================
// Query Handlers (using shared)
// ============================================================================

async fn query_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    debug!("Query Request (GET): db={}, sql={}", db, params.sql);
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::execute_query_impl(qe, &db, &params.sql).await {
        Ok(json) => (StatusCode::OK, json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

async fn query_handler_post(
    Path(db): Path<String>,
    State(state): State<DataNodeState>,
    body: String,
) -> impl IntoResponse {
    debug!("Query Request (POST): db={}, sql={}", db, body);
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::execute_query_impl(qe, &db, &body).await {
        Ok(json) => (StatusCode::OK, json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

async fn explain_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<ExplainParams>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::explain_query_impl(qe, &db, &params.sql, params.verbose, params.analyze).await {
        Ok(json) => (StatusCode::OK, json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

async fn explain_handler_post(
    Path(db): Path<String>,
    State(state): State<DataNodeState>,
    Json(params): Json<ExplainParams>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::explain_query_impl(qe, &db, &params.sql, params.verbose, params.analyze).await {
        Ok(json) => (StatusCode::OK, json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

async fn analyze_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::analyze_query_impl(qe, &db, &params.sql).await {
        Ok(json) => (StatusCode::OK, json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

async fn analyze_handler_post(
    Path(db): Path<String>,
    State(state): State<DataNodeState>,
    body: String,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::analyze_query_impl(qe, &db, &body).await {
        Ok(json) => (StatusCode::OK, json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

// ============================================================================
// Write Handlers (using shared)
// ============================================================================

async fn write_handler(
    Path(db): Path<String>,
    State(state): State<DataNodeState>,
    req: Request,
) -> impl IntoResponse {
    debug!("Write Request: db={}", db);
    let config = match &state.config {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Config not available".to_string(),
            )
        }
    };

    let data = match extract_write_body(req).await {
        Ok(s) => s,
        Err(e) => return e,
    };

    match shared::process_write_impl(&state.cache, config, state.wal.as_ref(), &db, &data).await {
        Ok(_result) => (StatusCode::NO_CONTENT, "".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

async fn write_handler_query_param(
    AxumQuery(params): AxumQuery<WriteQueryParams>,
    State(state): State<DataNodeState>,
    req: Request,
) -> impl IntoResponse {
    debug!("Write Request (InfluxDB-compatible): db={}", params.db);
    let config = match &state.config {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Config not available".to_string(),
            )
        }
    };

    let data = match extract_write_body(req).await {
        Ok(s) => s,
        Err(e) => return e,
    };

    match shared::process_write_impl(&state.cache, config, state.wal.as_ref(), &params.db, &data)
        .await
    {
        Ok(_result) => (StatusCode::NO_CONTENT, "".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

/// Extract write body from request, handling gzip Content-Encoding.
async fn extract_write_body(req: Request) -> Result<String, (StatusCode, String)> {
    let is_gzip = req
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("gzip"))
        .unwrap_or(false);

    let body = axum::body::to_bytes(req.into_body(), 250 * 1024 * 1024)
        .await
        .map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("Error reading body: {}", e))
        })?;

    let bytes = if is_gzip {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(&body[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Gzip decompression failed: {}", e),
            )
        })?;
        decompressed
    } else {
        body.to_vec()
    };

    String::from_utf8(bytes).map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("Invalid UTF-8: {}", e))
    })
}

// ============================================================================
// Schema/Table Handlers (using shared)
// ============================================================================

async fn list_tables_handler(
    Path(db): Path<String>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::list_tables_impl(qe, &db).await {
        Ok(json) => (StatusCode::OK, serde_json::to_string_pretty(&json).unwrap()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn list_schemas_handler(
    Path(db): Path<String>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::list_schemas_impl(qe, &db).await {
        Ok(json) => (StatusCode::OK, serde_json::to_string_pretty(&json).unwrap()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn list_schema_tables_handler(
    Path((db, schema)): Path<(String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    match shared::list_schema_tables_impl(qe, &db, &schema).await {
        Ok(json) => (StatusCode::OK, serde_json::to_string_pretty(&json).unwrap()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn schema_handler(
    Path((db, schema, table)): Path<(String, String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Query engine not available".to_string(),
            )
        }
    };
    // Construct full table path: schema.table
    let full_table = format!("{}.{}", schema, table);
    match shared::get_table_schema_impl(qe, &db, &full_table).await {
        Ok(schema) => (StatusCode::OK, schema),
        Err(e) => (StatusCode::NOT_FOUND, e),
    }
}

// ============================================================================
// Cache Handlers (using shared)
// ============================================================================

async fn memory_handler(State(state): State<DataNodeState>) -> impl IntoResponse {
    let cache_stats = state.cache.stats().await;
    (StatusCode::OK, shared::get_memory_stats(&cache_stats))
}

async fn flush_handler(State(state): State<DataNodeState>) -> impl IntoResponse {
    info!("Manual Flush Request");
    match shared::flush_cache_impl(&state.cache, state.query_engine.as_ref()).await {
        Ok(count) => (StatusCode::OK, format!("Flushed {} file(s) to disk", count)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn query_cache_stats_handler(State(state): State<DataNodeState>) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Query engine not available"})),
            )
        }
    };
    (StatusCode::OK, Json(shared::get_query_cache_stats_impl(qe)))
}

async fn clear_cache_handler(State(state): State<DataNodeState>) -> impl IntoResponse {
    info!("Clearing all caches");
    if let Some(ref qe) = state.query_engine {
        qe.clear_all_caches();
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "success", "message": "All caches cleared"})),
    )
}

async fn prewarm_cache_handler(
    Path(catalog): Path<String>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Query engine not available"})),
            )
        }
    };
    match shared::prewarm_cache_impl(qe, &catalog).await {
        Ok(count) => (
            StatusCode::OK,
            Json(
                serde_json::json!({"status": "success", "catalog": catalog, "files_cached": count}),
            ),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"status": "error", "message": e})),
        ),
    }
}

async fn prewarm_table_cache_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let qe = match &state.query_engine {
        Some(qe) => qe,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Query engine not available"})),
            )
        }
    };
    match shared::prewarm_table_cache_impl(qe, &catalog, &schema, &table).await {
        Ok(count) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "success", "catalog": catalog, "schema": schema, "table": table, "files_cached": count
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"status": "error", "message": e})),
        ),
    }
}

// ============================================================================
// Compaction Handlers (using shared)
// ============================================================================

async fn compact_handler(
    Path(catalog): Path<String>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let compactor = match &state.compactor {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Compactor not available".to_string(),
            )
        }
    };
    let config = match &state.config {
        Some(c) => crate::jobs::compactor::CompactionConfig::from(&c.compaction),
        None => crate::jobs::compactor::CompactionConfig::default(),
    };
    match shared::compact_catalog_impl(compactor, &catalog, &config).await {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn compact_table_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let compactor = match &state.compactor {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Compactor not available".to_string(),
            )
        }
    };
    let config = match &state.config {
        Some(c) => crate::jobs::compactor::CompactionConfig::from(&c.compaction),
        None => crate::jobs::compactor::CompactionConfig::default(),
    };
    match shared::compact_table_impl(compactor, &catalog, &schema, &table, &config).await {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn compact_partition_handler(
    Path((catalog, schema, table, date)): Path<(String, String, String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let compactor = match &state.compactor {
        Some(c) => c,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Compactor not available".to_string(),
            )
        }
    };
    let config = match &state.config {
        Some(c) => crate::jobs::compactor::CompactionConfig::from(&c.compaction),
        None => crate::jobs::compactor::CompactionConfig::default(),
    };
    match shared::compact_partition_impl(compactor, &catalog, &schema, &table, &date, &config).await
    {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

// ============================================================================
// Partition Handlers (using shared)
// ============================================================================

async fn list_partitions_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let dm = match &state.db_manager {
        Some(dm) => dm,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Database manager not available".to_string(),
            )
        }
    };
    match shared::list_partitions_detailed_impl_async(dm, &catalog, &schema, &table).await {
        Ok(json) => (StatusCode::OK, serde_json::to_string_pretty(&json).unwrap()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn delete_partition_handler(
    Path((catalog, schema, table, date)): Path<(String, String, String, String)>,
    State(state): State<DataNodeState>,
) -> impl IntoResponse {
    let dm = match &state.db_manager {
        Some(dm) => dm,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Database manager not available".to_string(),
            )
        }
    };
    match shared::delete_partition_impl(dm, &catalog, &schema, &table, &date) {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => (StatusCode::NOT_FOUND, e),
    }
}

async fn delete_old_partitions_handler(
    Path((catalog, schema, table)): Path<(String, String, String)>,
    State(state): State<DataNodeState>,
    Json(req): Json<RetentionRequest>,
) -> impl IntoResponse {
    let dm = match &state.db_manager {
        Some(dm) => dm,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Database manager not available".to_string(),
            )
        }
    };
    match shared::delete_old_partitions_impl(
        dm,
        &catalog,
        &schema,
        &table,
        req.older_than_days,
        req.before_date.as_deref(),
    ) {
        Ok(msg) => (StatusCode::OK, msg),
        Err(e) => {
            if e.contains("Must specify") {
                (StatusCode::BAD_REQUEST, e)
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, e)
            }
        }
    }
}

// ============================================================================
// Server
// ============================================================================

/// Start HTTP server for data node
pub async fn start_http_server(
    host: &str,
    port: u16,
    state: DataNodeState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = build_router(state);
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Data node HTTP server started on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}
