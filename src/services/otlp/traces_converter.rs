//! OTLP Traces -> MynaDB Point Converter
//!
//! Converts ExportTraceServiceRequest into Vec<Point>.
//! - Spans go into `otel_traces` table
//! - Span events go into `otel_trace_events` table
//! - Span links go into `otel_trace_links` table

use super::converter::{
    any_value_to_string, attributes_to_tags, bytes_to_hex, resource_attrs_to_tags,
    timestamp_nanos_to_date,
};
use crate::storage::line_protocol::{FieldValue, Point};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{span, Status};
use std::collections::HashMap;

/// Table name for trace spans
pub const TRACES_TABLE: &str = "otel_traces";
/// Table name for span events
pub const TRACE_EVENTS_TABLE: &str = "otel_trace_events";
/// Table name for span links
pub const TRACE_LINKS_TABLE: &str = "otel_trace_links";

/// Result of trace conversion with points for each table
pub struct TraceConversionResult {
    pub spans: Vec<Point>,
    pub events: Vec<Point>,
    pub links: Vec<Point>,
}

/// Convert OTLP traces request to MynaDB Points
pub fn convert_traces(
    request: &ExportTraceServiceRequest,
    schema: &str,
    resource_as_tags: bool,
    flatten_attributes: bool,
) -> TraceConversionResult {
    let mut spans = Vec::new();
    let mut events = Vec::new();
    let mut links = Vec::new();

    for resource_spans in &request.resource_spans {
        let resource_tags = if resource_as_tags {
            resource_spans
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

        for scope_spans in &resource_spans.scope_spans {
            let mut scope_tags = HashMap::new();
            if let Some(ref scope) = scope_spans.scope {
                if !scope.name.is_empty() {
                    scope_tags.insert("otel.scope.name".to_string(), scope.name.clone());
                }
                if !scope.version.is_empty() {
                    scope_tags.insert("otel.scope.version".to_string(), scope.version.clone());
                }
            }

            for otel_span in &scope_spans.spans {
                let trace_id = bytes_to_hex(&otel_span.trace_id);
                let span_id = bytes_to_hex(&otel_span.span_id);
                let parent_span_id = if otel_span.parent_span_id.is_empty() {
                    String::new()
                } else {
                    bytes_to_hex(&otel_span.parent_span_id)
                };

                let mut tags = resource_tags.clone();
                tags.extend(scope_tags.clone());
                tags.extend(attributes_to_tags(
                    &otel_span.attributes,
                    if flatten_attributes { "attrs" } else { "" },
                    flatten_attributes,
                ));

                let timestamp = otel_span.start_time_unix_nano;
                let date = timestamp_nanos_to_date(timestamp);
                let duration_ns = otel_span.end_time_unix_nano.saturating_sub(timestamp);

                let mut fields = HashMap::new();
                fields.insert(
                    "trace_id".to_string(),
                    FieldValue::String(trace_id.clone()),
                );
                fields.insert(
                    "span_id".to_string(),
                    FieldValue::String(span_id.clone()),
                );
                if !parent_span_id.is_empty() {
                    fields.insert(
                        "parent_span_id".to_string(),
                        FieldValue::String(parent_span_id.clone()),
                    );
                }
                fields.insert(
                    "span_name".to_string(),
                    FieldValue::String(otel_span.name.clone()),
                );
                fields.insert(
                    "span_kind".to_string(),
                    FieldValue::String(span_kind_to_string(otel_span.kind)),
                );
                fields.insert(
                    "end_time".to_string(),
                    FieldValue::Integer(otel_span.end_time_unix_nano as i64),
                );
                fields.insert(
                    "duration_ns".to_string(),
                    FieldValue::Integer(duration_ns as i64),
                );

                // Status
                if let Some(ref status) = otel_span.status {
                    fields.insert(
                        "status_code".to_string(),
                        FieldValue::String(status_code_to_string(status)),
                    );
                    if !status.message.is_empty() {
                        fields.insert(
                            "status_message".to_string(),
                            FieldValue::String(status.message.clone()),
                        );
                    }
                }

                spans.push(Point {
                    measurement: TRACES_TABLE.to_string(),
                    tags: tags.clone(),
                    fields,
                    timestamp: Some(timestamp as i64),
                    date,
                    service: schema.to_string(),
                });

                // Convert span events
                for event in &otel_span.events {
                    let mut event_tags = tags.clone();
                    event_tags.extend(attributes_to_tags(
                        &event.attributes,
                        if flatten_attributes { "event" } else { "" },
                        flatten_attributes,
                    ));

                    let event_ts = event.time_unix_nano;
                    let event_date = timestamp_nanos_to_date(event_ts);

                    let mut event_fields = HashMap::new();
                    event_fields.insert(
                        "trace_id".to_string(),
                        FieldValue::String(trace_id.clone()),
                    );
                    event_fields.insert(
                        "span_id".to_string(),
                        FieldValue::String(span_id.clone()),
                    );
                    event_fields.insert(
                        "event_name".to_string(),
                        FieldValue::String(event.name.clone()),
                    );

                    events.push(Point {
                        measurement: TRACE_EVENTS_TABLE.to_string(),
                        tags: event_tags,
                        fields: event_fields,
                        timestamp: Some(event_ts as i64),
                        date: event_date,
                        service: schema.to_string(),
                    });
                }

                // Convert span links
                for link in &otel_span.links {
                    let mut link_tags = tags.clone();
                    link_tags.extend(attributes_to_tags(
                        &link.attributes,
                        if flatten_attributes { "link" } else { "" },
                        flatten_attributes,
                    ));

                    let mut link_fields = HashMap::new();
                    link_fields.insert(
                        "trace_id".to_string(),
                        FieldValue::String(trace_id.clone()),
                    );
                    link_fields.insert(
                        "span_id".to_string(),
                        FieldValue::String(span_id.clone()),
                    );
                    link_fields.insert(
                        "linked_trace_id".to_string(),
                        FieldValue::String(bytes_to_hex(&link.trace_id)),
                    );
                    link_fields.insert(
                        "linked_span_id".to_string(),
                        FieldValue::String(bytes_to_hex(&link.span_id)),
                    );

                    links.push(Point {
                        measurement: TRACE_LINKS_TABLE.to_string(),
                        tags: link_tags,
                        fields: link_fields,
                        timestamp: Some(timestamp as i64),
                        date,
                        service: schema.to_string(),
                    });
                }
            }
        }
    }

    TraceConversionResult {
        spans,
        events,
        links,
    }
}

fn span_kind_to_string(kind: i32) -> String {
    match span::SpanKind::try_from(kind) {
        Ok(span::SpanKind::Internal) => "INTERNAL",
        Ok(span::SpanKind::Server) => "SERVER",
        Ok(span::SpanKind::Client) => "CLIENT",
        Ok(span::SpanKind::Producer) => "PRODUCER",
        Ok(span::SpanKind::Consumer) => "CONSUMER",
        _ => "UNSPECIFIED",
    }
    .to_string()
}

fn status_code_to_string(status: &Status) -> String {
    match status.code {
        0 => "UNSET",
        1 => "OK",
        2 => "ERROR",
        _ => "UNKNOWN",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::*;

    fn make_kv(key: &str, val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.to_string())),
            }),
        }
    }

    #[test]
    fn test_span_conversion() {
        let trace_id = vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        let span_id = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "myapp")],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: trace_id.clone(),
                        span_id: span_id.clone(),
                        parent_span_id: vec![],
                        name: "GET /api/users".to_string(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: 1704067200_000_000_000,
                        end_time_unix_nano: 1704067200_050_000_000,
                        attributes: vec![make_kv("http.method", "GET")],
                        events: vec![span::Event {
                            time_unix_nano: 1704067200_010_000_000,
                            name: "log".to_string(),
                            attributes: vec![make_kv("message", "query executed")],
                            dropped_attributes_count: 0,
                        }],
                        links: vec![],
                        status: Some(Status {
                            code: 1,
                            message: String::new(),
                        }),
                        dropped_attributes_count: 0,
                        dropped_events_count: 0,
                        dropped_links_count: 0,
                        trace_state: String::new(),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let result = convert_traces(&request, "default", true, true);
        assert_eq!(result.spans.len(), 1);
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.links.len(), 0);

        let span = &result.spans[0];
        assert_eq!(span.measurement, "otel_traces");
        match span.fields.get("span_name") {
            Some(FieldValue::String(s)) => assert_eq!(s, "GET /api/users"),
            _ => panic!("Expected span_name"),
        }
        match span.fields.get("span_kind") {
            Some(FieldValue::String(s)) => assert_eq!(s, "SERVER"),
            _ => panic!("Expected span_kind"),
        }
        match span.fields.get("duration_ns") {
            Some(FieldValue::Integer(d)) => assert_eq!(*d, 50_000_000),
            _ => panic!("Expected duration_ns"),
        }

        let event = &result.events[0];
        assert_eq!(event.measurement, "otel_trace_events");
    }
}
