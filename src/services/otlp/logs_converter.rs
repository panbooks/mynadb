//! OTLP Logs -> MynaDB Point Converter
//!
//! Converts ExportLogsServiceRequest into Vec<Point>.
//! All logs go into a single table: `otel_logs`.

use super::converter::{
    any_value_to_string, attributes_to_tags, bytes_to_hex, resource_attrs_to_tags,
    timestamp_nanos_to_date,
};
use crate::storage::line_protocol::{FieldValue, Point};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use std::collections::HashMap;

/// Table name for all OTLP logs
pub const LOGS_TABLE: &str = "otel_logs";

/// Convert OTLP logs request to MynaDB Points
pub fn convert_logs(
    request: &ExportLogsServiceRequest,
    schema: &str,
    resource_as_tags: bool,
    flatten_attributes: bool,
) -> Vec<Point> {
    let mut points = Vec::new();

    for resource_logs in &request.resource_logs {
        let resource_tags = if resource_as_tags {
            resource_logs
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

        for scope_logs in &resource_logs.scope_logs {
            let mut scope_tags = HashMap::new();
            if let Some(ref scope) = scope_logs.scope {
                if !scope.name.is_empty() {
                    scope_tags.insert("otel.scope.name".to_string(), scope.name.clone());
                }
                if !scope.version.is_empty() {
                    scope_tags.insert("otel.scope.version".to_string(), scope.version.clone());
                }
            }

            for log_record in &scope_logs.log_records {
                let mut tags = resource_tags.clone();
                tags.extend(scope_tags.clone());
                tags.extend(attributes_to_tags(
                    &log_record.attributes,
                    if flatten_attributes { "attrs" } else { "" },
                    flatten_attributes,
                ));

                let timestamp = if log_record.time_unix_nano > 0 {
                    log_record.time_unix_nano
                } else {
                    log_record.observed_time_unix_nano
                };
                let date = timestamp_nanos_to_date(timestamp);

                let mut fields = HashMap::new();

                // Severity
                fields.insert(
                    "severity_number".to_string(),
                    FieldValue::Integer(log_record.severity_number as i64),
                );
                if !log_record.severity_text.is_empty() {
                    fields.insert(
                        "severity_text".to_string(),
                        FieldValue::String(log_record.severity_text.clone()),
                    );
                }

                // Body
                if let Some(ref body) = log_record.body {
                    if let Some(s) = any_value_to_string(body) {
                        fields.insert("body".to_string(), FieldValue::String(s));
                    }
                }

                // Trace context
                if !log_record.trace_id.is_empty() {
                    let trace_id = bytes_to_hex(&log_record.trace_id);
                    fields.insert(
                        "trace_id".to_string(),
                        FieldValue::String(trace_id),
                    );
                }
                if !log_record.span_id.is_empty() {
                    let span_id = bytes_to_hex(&log_record.span_id);
                    fields.insert(
                        "span_id".to_string(),
                        FieldValue::String(span_id),
                    );
                }

                // Observed timestamp as field (if different from time)
                if log_record.observed_time_unix_nano > 0
                    && log_record.observed_time_unix_nano != timestamp
                {
                    fields.insert(
                        "observed_time".to_string(),
                        FieldValue::Integer(log_record.observed_time_unix_nano as i64),
                    );
                }

                points.push(Point {
                    measurement: LOGS_TABLE.to_string(),
                    tags,
                    fields,
                    timestamp: Some(timestamp as i64),
                    date,
                    service: schema.to_string(),
                });
            }
        }
    }

    points
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::*;
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
    fn test_log_conversion() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "myapp")],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1704067200_000_000_000,
                        observed_time_unix_nano: 1704067200_000_000_000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "Application started".to_string(),
                            )),
                        }),
                        attributes: vec![make_kv("component", "main")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90],
                        span_id: vec![0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef],
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let points = convert_logs(&request, "default", true, true);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].measurement, "otel_logs");

        match points[0].fields.get("body") {
            Some(FieldValue::String(s)) => assert_eq!(s, "Application started"),
            _ => panic!("Expected string body"),
        }
        match points[0].fields.get("severity_text") {
            Some(FieldValue::String(s)) => assert_eq!(s, "INFO"),
            _ => panic!("Expected severity_text"),
        }
        match points[0].fields.get("trace_id") {
            Some(FieldValue::String(s)) => assert_eq!(s, "abcdef1234567890abcdef1234567890"),
            _ => panic!("Expected trace_id"),
        }
    }
}
