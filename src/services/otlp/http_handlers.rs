//! OTLP HTTP Handlers
//!
//! Handles POST /v1/metrics, /v1/logs, /v1/traces on the main HTTP server.
//! Supports both protobuf (application/x-protobuf) and JSON content types.

use super::{logs_converter, metrics_converter, traces_converter};
use crate::config::OtlpConfig;
use crate::storage::cache::DataCache;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Shared state for OTLP HTTP handlers
pub struct OtlpHttpState {
    pub cache: DataCache,
    pub config: OtlpConfig,
}

/// Handle OTLP metrics HTTP endpoint
/// POST /v1/metrics
pub async fn metrics_http_handler(
    State(state): State<Arc<OtlpHttpState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !state.config.enabled {
        return (StatusCode::SERVICE_UNAVAILABLE, "OTLP ingestion is disabled".to_string());
    }

    let request: ExportMetricsServiceRequest = match parse_otlp_request(&headers, &body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Failed to parse request: {}", e)),
    };

    let points_by_table = metrics_converter::convert_metrics(
        &request,
        &state.config.database,
        &state.config.schema,
        state.config.resource_as_tags,
        state.config.flatten_attributes,
    );

    let mut total = 0;
    let mut errors = Vec::new();
    for (table, points) in points_by_table {
        total += points.len();
        if let Err(e) = state
            .cache
            .add_points(
                &state.config.database,
                &state.config.schema,
                &table,
                points,
            )
            .await
        {
            error!("OTLP HTTP metrics write failed for {}: {:?}", table, e);
            errors.push(format!("{}: {:?}", table, e));
        }
    }

    if errors.is_empty() {
        debug!("OTLP HTTP: ingested {} metric points", total);
        (StatusCode::OK, "{}".to_string())
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Partial failure: {}", errors.join(", ")),
        )
    }
}

/// Handle OTLP logs HTTP endpoint
/// POST /v1/logs
pub async fn logs_http_handler(
    State(state): State<Arc<OtlpHttpState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !state.config.enabled {
        return (StatusCode::SERVICE_UNAVAILABLE, "OTLP ingestion is disabled".to_string());
    }

    let request: ExportLogsServiceRequest = match parse_otlp_request(&headers, &body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Failed to parse request: {}", e)),
    };

    let points = logs_converter::convert_logs(
        &request,
        &state.config.schema,
        state.config.resource_as_tags,
        state.config.flatten_attributes,
    );

    let count = points.len();
    if !points.is_empty() {
        if let Err(e) = state
            .cache
            .add_points(
                &state.config.database,
                &state.config.schema,
                logs_converter::LOGS_TABLE,
                points,
            )
            .await
        {
            error!("OTLP HTTP logs write failed: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Write failed: {:?}", e),
            );
        }
    }

    debug!("OTLP HTTP: ingested {} log records", count);
    (StatusCode::OK, "{}".to_string())
}

/// Handle OTLP traces HTTP endpoint
/// POST /v1/traces
pub async fn traces_http_handler(
    State(state): State<Arc<OtlpHttpState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !state.config.enabled {
        return (StatusCode::SERVICE_UNAVAILABLE, "OTLP ingestion is disabled".to_string());
    }

    let request: ExportTraceServiceRequest = match parse_otlp_request(&headers, &body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("Failed to parse request: {}", e)),
    };

    let result = traces_converter::convert_traces(
        &request,
        &state.config.schema,
        state.config.resource_as_tags,
        state.config.flatten_attributes,
    );

    let db = &state.config.database;
    let schema = &state.config.schema;
    let total = result.spans.len() + result.events.len() + result.links.len();
    let mut errors = Vec::new();

    if !result.spans.is_empty() {
        if let Err(e) = state
            .cache
            .add_points(db, schema, traces_converter::TRACES_TABLE, result.spans)
            .await
        {
            errors.push(format!("spans: {:?}", e));
        }
    }

    if !result.events.is_empty() {
        if let Err(e) = state
            .cache
            .add_points(db, schema, traces_converter::TRACE_EVENTS_TABLE, result.events)
            .await
        {
            errors.push(format!("events: {:?}", e));
        }
    }

    if !result.links.is_empty() {
        if let Err(e) = state
            .cache
            .add_points(db, schema, traces_converter::TRACE_LINKS_TABLE, result.links)
            .await
        {
            errors.push(format!("links: {:?}", e));
        }
    }

    if errors.is_empty() {
        debug!("OTLP HTTP: ingested {} trace points", total);
        (StatusCode::OK, "{}".to_string())
    } else {
        error!("OTLP HTTP traces write errors: {:?}", errors);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Partial failure: {}", errors.join(", ")),
        )
    }
}

/// Parse an OTLP request from either protobuf or JSON content type
fn parse_otlp_request<T: Message + Default + serde::de::DeserializeOwned>(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<T, String> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    if content_type.contains("json") {
        serde_json::from_slice(body).map_err(|e| format!("JSON parse error: {}", e))
    } else {
        T::decode(body.as_ref()).map_err(|e| format!("Protobuf decode error: {}", e))
    }
}
