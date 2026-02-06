//! Prometheus Remote Write Handler
//!
//! Handles POST /api/v1/write requests from Prometheus.
//! Data flow:
//! 1. Receive snappy-compressed protobuf WriteRequest
//! 2. Decompress and parse
//! 3. Convert TimeSeries to MynaDB Point format
//! 4. Write to MynaDB storage

use super::types::{WriteRequest, METRIC_NAME_LABEL};
use crate::storage::cache::DataCache;
use crate::storage::line_protocol::{FieldValue, Point};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// AppState subset needed for remote write
pub struct RemoteWriteState {
    pub cache: DataCache,
    pub default_schema: String,
}

/// Handle Prometheus remote write request
///
/// Expects:
/// - Content-Type: application/x-protobuf
/// - Content-Encoding: snappy
/// - Body: snappy-compressed WriteRequest protobuf
pub async fn remote_write_handler(
    Path(db): Path<String>,
    State(state): State<Arc<RemoteWriteState>>,
    body: Bytes,
) -> impl IntoResponse {
    // Decompress snappy
    let decompressed = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to decompress snappy: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to decompress: {}", e),
            );
        }
    };

    // Parse protobuf
    let write_request = match WriteRequest::decode(&decompressed[..]) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to parse WriteRequest protobuf: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse protobuf: {}", e),
            );
        }
    };

    debug!(
        "Received Prometheus remote write: {} timeseries",
        write_request.timeseries.len()
    );

    // Convert and store
    let mut total_samples = 0;
    let mut errors = Vec::new();

    // Group points by table (metric name)
    let mut points_by_table: HashMap<String, Vec<Point>> = HashMap::new();

    for ts in write_request.timeseries {
        // Get metric name
        let metric_name = match ts.metric_name() {
            Some(name) => sanitize_metric_name(name),
            None => {
                warn!("TimeSeries missing __name__ label, skipping");
                continue;
            }
        };

        // Convert labels to tags
        let tags: HashMap<String, String> = ts
            .tags()
            .map(|l| (sanitize_label_name(&l.name), l.value.clone()))
            .collect();

        // Convert samples to points
        for sample in &ts.samples {
            // Convert timestamp from ms to ns
            let timestamp_ns = sample.timestamp * 1_000_000;
            let date = compute_date_from_timestamp(Some(timestamp_ns));

            let mut fields = HashMap::new();
            fields.insert("value".to_string(), FieldValue::Float(sample.value));

            let point = Point {
                measurement: metric_name.clone(),
                tags: tags.clone(),
                fields,
                timestamp: Some(timestamp_ns),
                date,
                service: state.default_schema.clone(),
            };

            points_by_table
                .entry(metric_name.clone())
                .or_default()
                .push(point);
            total_samples += 1;
        }
    }

    // Write points to cache
    for (table, points) in points_by_table {
        if let Err(e) = state
            .cache
            .add_points(&db, &state.default_schema, &table, points)
            .await
        {
            error!("Failed to write points for {}: {:?}", table, e);
            errors.push(format!("{}: {:?}", table, e));
        }
    }

    if errors.is_empty() {
        debug!(
            "Prometheus remote write complete: {} samples written to {}",
            total_samples, db
        );
        (StatusCode::NO_CONTENT, String::new())
    } else {
        error!("Prometheus remote write had errors: {:?}", errors);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Partial write failure: {}", errors.join(", ")),
        )
    }
}

/// Compute date (Date32: days since Unix epoch) from nanosecond timestamp
fn compute_date_from_timestamp(timestamp: Option<i64>) -> i32 {
    let dt = if let Some(ts_nanos) = timestamp {
        DateTime::from_timestamp(ts_nanos / 1_000_000_000, (ts_nanos % 1_000_000_000) as u32)
            .unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    };

    // Calculate days since Unix epoch (1970-01-01)
    let epoch = DateTime::from_timestamp(0, 0).unwrap();
    let duration = dt.signed_duration_since(epoch);
    duration.num_days() as i32
}

/// Sanitize metric name for MynaDB table name
/// Prometheus allows [a-zA-Z_:][a-zA-Z0-9_:]* but MynaDB is more restrictive
fn sanitize_metric_name(name: &str) -> String {
    name.replace(':', "_").replace('-', "_")
}

/// Sanitize label name for MynaDB tag name
fn sanitize_label_name(name: &str) -> String {
    // Remove leading underscores (reserved in Prometheus)
    let name = name.trim_start_matches('_');
    name.replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::prometheus::types::{Label, Sample, TimeSeries};

    #[test]
    fn test_sanitize_metric_name() {
        assert_eq!(
            sanitize_metric_name("http_requests_total"),
            "http_requests_total"
        );
        assert_eq!(
            sanitize_metric_name("job:requests:rate5m"),
            "job_requests_rate5m"
        );
        assert_eq!(sanitize_metric_name("my-metric"), "my_metric");
    }

    #[test]
    fn test_sanitize_label_name() {
        assert_eq!(sanitize_label_name("instance"), "instance");
        assert_eq!(sanitize_label_name("__name__"), "name__");
        assert_eq!(sanitize_label_name("my-label"), "my_label");
    }

    #[test]
    fn test_compute_date_from_timestamp() {
        // 2024-01-01 00:00:00 UTC in nanoseconds
        let ts_ns = 1704067200_i64 * 1_000_000_000;
        let date = compute_date_from_timestamp(Some(ts_ns));
        // 2024-01-01 is 19723 days since 1970-01-01
        assert_eq!(date, 19723);
    }

    #[test]
    fn test_timeseries_to_points() {
        let ts = TimeSeries {
            labels: vec![
                Label::new(METRIC_NAME_LABEL, "cpu_usage"),
                Label::new("host", "server01"),
                Label::new("cpu", "0"),
            ],
            samples: vec![
                Sample::new(1704067200000, 42.5),
                Sample::new(1704067260000, 43.1),
            ],
        };

        let metric_name = ts.metric_name().unwrap();
        assert_eq!(metric_name, "cpu_usage");

        let tags: HashMap<String, String> = ts
            .tags()
            .map(|l| (l.name.clone(), l.value.clone()))
            .collect();

        assert_eq!(tags.get("host"), Some(&"server01".to_string()));
        assert_eq!(tags.get("cpu"), Some(&"0".to_string()));
        assert!(!tags.contains_key(METRIC_NAME_LABEL));
    }
}
