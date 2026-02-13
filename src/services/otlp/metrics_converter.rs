//! OTLP Metrics -> MynaDB Point Converter
//!
//! Converts ExportMetricsServiceRequest into Vec<Point>, one table per metric name.
//! Supports: Gauge, Sum, Histogram, Summary, ExponentialHistogram.

use super::converter::{
    attributes_to_tags, bytes_to_hex, resource_attrs_to_tags, sanitize_tag_name,
    timestamp_nanos_to_date,
};
use crate::storage::line_protocol::{FieldValue, Point};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
use std::collections::HashMap;

/// Convert OTLP metrics request to MynaDB Points, grouped by table name
pub fn convert_metrics(
    request: &ExportMetricsServiceRequest,
    _database: &str,
    schema: &str,
    resource_as_tags: bool,
    flatten_attributes: bool,
) -> HashMap<String, Vec<Point>> {
    let mut points_by_table: HashMap<String, Vec<Point>> = HashMap::new();

    for resource_metrics in &request.resource_metrics {
        // Extract resource attributes (service.name, host.name, etc.)
        let resource_tags = if resource_as_tags {
            resource_metrics
                .resource
                .as_ref()
                .map(|r| {
                    resource_attrs_to_tags(
                        &r.attributes,
                        if flatten_attributes { "resource" } else { "" },
                        flatten_attributes,
                    )
                })
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        for scope_metrics in &resource_metrics.scope_metrics {
            // Extract instrumentation scope as tags
            let mut scope_tags = HashMap::new();
            if let Some(ref scope) = scope_metrics.scope {
                if !scope.name.is_empty() {
                    scope_tags.insert("otel.scope.name".to_string(), scope.name.clone());
                }
                if !scope.version.is_empty() {
                    scope_tags.insert("otel.scope.version".to_string(), scope.version.clone());
                }
            }

            for metric in &scope_metrics.metrics {
                let table_name = sanitize_metric_name(&metric.name);

                let metric_points = convert_metric_data(
                    metric,
                    &table_name,
                    schema,
                    &resource_tags,
                    &scope_tags,
                    flatten_attributes,
                );

                points_by_table
                    .entry(table_name)
                    .or_default()
                    .extend(metric_points);
            }
        }
    }

    points_by_table
}

/// Convert NumberDataPoint entries (shared by Gauge and Sum metric types).
fn convert_number_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
    table_name: &str,
    schema: &str,
    resource_tags: &HashMap<String, String>,
    scope_tags: &HashMap<String, String>,
    flatten_attributes: bool,
    extra_tags: &[(&str, String)],
) -> Vec<Point> {
    let mut points = Vec::new();
    for dp in data_points {
        let mut tags = resource_tags.clone();
        tags.extend(scope_tags.clone());
        tags.extend(attributes_to_tags(
            &dp.attributes,
            if flatten_attributes { "attrs" } else { "" },
            flatten_attributes,
        ));
        for (k, v) in extra_tags {
            tags.insert(k.to_string(), v.clone());
        }

        let timestamp = dp.time_unix_nano;
        let date = timestamp_nanos_to_date(timestamp);

        let mut fields = HashMap::new();
        match &dp.value {
            Some(number_data_point::Value::AsDouble(v)) => {
                fields.insert("value".to_string(), FieldValue::Float(*v));
            }
            Some(number_data_point::Value::AsInt(v)) => {
                fields.insert("value".to_string(), FieldValue::Float(*v as f64));
            }
            None => continue,
        }

        points.push(Point {
            measurement: table_name.to_string(),
            tags,
            fields,
            timestamp: Some(timestamp as i64),
            date,
            service: schema.to_string(),
        });
    }
    points
}

fn convert_metric_data(
    metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
    table_name: &str,
    schema: &str,
    resource_tags: &HashMap<String, String>,
    scope_tags: &HashMap<String, String>,
    flatten_attributes: bool,
) -> Vec<Point> {
    let mut points = Vec::new();

    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            return convert_number_data_points(
                &gauge.data_points,
                table_name,
                schema,
                resource_tags,
                scope_tags,
                flatten_attributes,
                &[],
            );
        }
        Some(metric::Data::Sum(sum)) => {
            return convert_number_data_points(
                &sum.data_points,
                table_name,
                schema,
                resource_tags,
                scope_tags,
                flatten_attributes,
                &[
                    ("otel.aggregation_temporality", sum.aggregation_temporality.to_string()),
                    ("otel.is_monotonic", sum.is_monotonic.to_string()),
                ],
            );
        }
        Some(metric::Data::Histogram(histogram)) => {
            for dp in &histogram.data_points {
                let mut base_tags = resource_tags.clone();
                base_tags.extend(scope_tags.clone());
                base_tags.extend(attributes_to_tags(
                    &dp.attributes,
                    if flatten_attributes { "attrs" } else { "" },
                    flatten_attributes,
                ));

                let timestamp = dp.time_unix_nano;
                let date = timestamp_nanos_to_date(timestamp);

                // Summary point with count and sum
                let mut fields = HashMap::new();
                fields.insert("count".to_string(), FieldValue::Integer(dp.count as i64));
                if let Some(sum) = dp.sum {
                    fields.insert("sum".to_string(), FieldValue::Float(sum));
                }
                if let Some(min) = dp.min {
                    fields.insert("min".to_string(), FieldValue::Float(min));
                }
                if let Some(max) = dp.max {
                    fields.insert("max".to_string(), FieldValue::Float(max));
                }

                points.push(Point {
                    measurement: table_name.to_string(),
                    tags: base_tags.clone(),
                    fields,
                    timestamp: Some(timestamp as i64),
                    date,
                    service: schema.to_string(),
                });

                // Individual bucket points
                for (i, count) in dp.bucket_counts.iter().enumerate() {
                    let le = if i < dp.explicit_bounds.len() {
                        dp.explicit_bounds[i]
                    } else {
                        f64::INFINITY
                    };

                    let mut bucket_tags = base_tags.clone();
                    bucket_tags.insert("le".to_string(), format_le(le));

                    let mut fields = HashMap::new();
                    fields.insert("bucket_count".to_string(), FieldValue::Integer(*count as i64));
                    fields.insert("bucket_le".to_string(), FieldValue::Float(le));

                    points.push(Point {
                        measurement: table_name.to_string(),
                        tags: bucket_tags,
                        fields,
                        timestamp: Some(timestamp as i64),
                        date,
                        service: schema.to_string(),
                    });
                }
            }
        }
        Some(metric::Data::Summary(summary)) => {
            for dp in &summary.data_points {
                let mut base_tags = resource_tags.clone();
                base_tags.extend(scope_tags.clone());
                base_tags.extend(attributes_to_tags(
                    &dp.attributes,
                    if flatten_attributes { "attrs" } else { "" },
                    flatten_attributes,
                ));

                let timestamp = dp.time_unix_nano;
                let date = timestamp_nanos_to_date(timestamp);

                // Summary point
                let mut fields = HashMap::new();
                fields.insert("count".to_string(), FieldValue::Integer(dp.count as i64));
                fields.insert("sum".to_string(), FieldValue::Float(dp.sum));

                points.push(Point {
                    measurement: table_name.to_string(),
                    tags: base_tags.clone(),
                    fields,
                    timestamp: Some(timestamp as i64),
                    date,
                    service: schema.to_string(),
                });

                // Quantile points
                for qv in &dp.quantile_values {
                    let mut q_tags = base_tags.clone();
                    q_tags.insert("quantile".to_string(), qv.quantile.to_string());

                    let mut fields = HashMap::new();
                    fields.insert("quantile".to_string(), FieldValue::Float(qv.quantile));
                    fields.insert("quantile_value".to_string(), FieldValue::Float(qv.value));

                    points.push(Point {
                        measurement: table_name.to_string(),
                        tags: q_tags,
                        fields,
                        timestamp: Some(timestamp as i64),
                        date,
                        service: schema.to_string(),
                    });
                }
            }
        }
        Some(metric::Data::ExponentialHistogram(exp_hist)) => {
            for dp in &exp_hist.data_points {
                let mut tags = resource_tags.clone();
                tags.extend(scope_tags.clone());
                tags.extend(attributes_to_tags(
                    &dp.attributes,
                    if flatten_attributes { "attrs" } else { "" },
                    flatten_attributes,
                ));

                let timestamp = dp.time_unix_nano;
                let date = timestamp_nanos_to_date(timestamp);

                let mut fields = HashMap::new();
                fields.insert("count".to_string(), FieldValue::Integer(dp.count as i64));
                if let Some(sum) = dp.sum {
                    fields.insert("sum".to_string(), FieldValue::Float(sum));
                }
                if let Some(min) = dp.min {
                    fields.insert("min".to_string(), FieldValue::Float(min));
                }
                if let Some(max) = dp.max {
                    fields.insert("max".to_string(), FieldValue::Float(max));
                }
                fields.insert("scale".to_string(), FieldValue::Integer(dp.scale as i64));
                fields.insert(
                    "zero_count".to_string(),
                    FieldValue::Integer(dp.zero_count as i64),
                );

                points.push(Point {
                    measurement: table_name.to_string(),
                    tags,
                    fields,
                    timestamp: Some(timestamp as i64),
                    date,
                    service: schema.to_string(),
                });
            }
        }
        None => {}
    }

    points
}

/// Format histogram bucket boundary for consistent string representation
fn format_le(le: f64) -> String {
    if le.is_infinite() {
        "+Inf".to_string()
    } else {
        le.to_string()
    }
}

/// Sanitize metric name for use as MynaDB table name
fn sanitize_metric_name(name: &str) -> String {
    name.replace('.', "_")
        .replace('-', "_")
        .replace('/', "_")
        .replace(':', "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::*;
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn make_kv(key: &str, val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.to_string())),
            }),
        }
    }

    #[test]
    fn test_gauge_conversion() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "myapp")],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu.usage".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![make_kv("host", "server01")],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1704067200_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(42.5)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let result = convert_metrics(&request, "otel", "default", true, true);
        assert_eq!(result.len(), 1);
        let points = result.get("cpu_usage").unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].tags.get("resource.service.name"), Some(&"myapp".to_string()));
        assert_eq!(points[0].tags.get("attrs.host"), Some(&"server01".to_string()));
        match points[0].fields.get("value") {
            Some(FieldValue::Float(v)) => assert!((v - 42.5).abs() < f64::EPSILON),
            _ => panic!("Expected float value"),
        }
    }

    #[test]
    fn test_sanitize_metric_name() {
        assert_eq!(sanitize_metric_name("http.server.request.duration"), "http_server_request_duration");
        assert_eq!(sanitize_metric_name("my-metric"), "my_metric");
    }
}
