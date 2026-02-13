//! Shared conversion utilities for OTLP -> MynaDB Point
//!
//! Handles resource attribute extraction, timestamp conversion,
//! attribute flattening, and hex encoding for trace/span IDs.

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use std::collections::HashMap;

/// Convert nanosecond timestamp to Date32 (days since Unix epoch)
pub fn timestamp_nanos_to_date(timestamp_nanos: u64) -> i32 {
    let dt = DateTime::from_timestamp(
        (timestamp_nanos / 1_000_000_000) as i64,
        (timestamp_nanos % 1_000_000_000) as u32,
    )
    .unwrap_or_else(Utc::now);

    let epoch = DateTime::from_timestamp(0, 0).unwrap();
    dt.signed_duration_since(epoch).num_days() as i32
}

/// Extract tags from resource attributes
pub fn resource_attrs_to_tags(
    attributes: &[KeyValue],
    prefix: &str,
    flatten: bool,
) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for kv in attributes {
        if let Some(ref value) = kv.value {
            let key = if prefix.is_empty() {
                sanitize_tag_name(&kv.key)
            } else if flatten {
                format!("{}.{}", prefix, sanitize_tag_name(&kv.key))
            } else {
                sanitize_tag_name(&kv.key)
            };
            if let Some(s) = any_value_to_string(value) {
                tags.insert(key, s);
            }
        }
    }
    tags
}

/// Extract tags from KeyValue attributes
pub fn attributes_to_tags(
    attributes: &[KeyValue],
    prefix: &str,
    flatten: bool,
) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for kv in attributes {
        if let Some(ref value) = kv.value {
            let key = if prefix.is_empty() || !flatten {
                sanitize_tag_name(&kv.key)
            } else {
                format!("{}.{}", prefix, sanitize_tag_name(&kv.key))
            };
            if let Some(s) = any_value_to_string(value) {
                tags.insert(key, s);
            }
        }
    }
    tags
}

/// Convert AnyValue to String representation
pub fn any_value_to_string(value: &AnyValue) -> Option<String> {
    match &value.value {
        Some(any_value::Value::StringValue(s)) => Some(s.clone()),
        Some(any_value::Value::BoolValue(b)) => Some(b.to_string()),
        Some(any_value::Value::IntValue(i)) => Some(i.to_string()),
        Some(any_value::Value::DoubleValue(d)) => Some(d.to_string()),
        Some(any_value::Value::BytesValue(b)) => Some(hex::encode(b)),
        Some(any_value::Value::ArrayValue(arr)) => {
            let items: Vec<String> = arr
                .values
                .iter()
                .filter_map(any_value_to_string)
                .collect();
            Some(format!("[{}]", items.join(",")))
        }
        Some(any_value::Value::KvlistValue(kvlist)) => {
            let items: Vec<String> = kvlist
                .values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .and_then(any_value_to_string)
                        .map(|v| format!("{}={}", kv.key, v))
                })
                .collect();
            Some(format!("{{{}}}", items.join(",")))
        }
        None => None,
    }
}

/// Sanitize tag name for MynaDB compatibility
pub fn sanitize_tag_name(name: &str) -> String {
    name.replace('-', "_")
        .replace(' ', "_")
        .replace('/', ".")
}

/// Encode bytes as hex string (for trace_id, span_id)
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Simple hex encoder (avoids extra dependency)
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_conversion() {
        // 2024-01-01 00:00:00 UTC
        let ts = 1704067200_u64 * 1_000_000_000;
        let date = timestamp_nanos_to_date(ts);
        assert_eq!(date, 19723);
    }

    #[test]
    fn test_sanitize_tag_name() {
        assert_eq!(sanitize_tag_name("service.name"), "service.name");
        assert_eq!(sanitize_tag_name("host-name"), "host_name");
        assert_eq!(sanitize_tag_name("http/method"), "http.method");
    }

    #[test]
    fn test_bytes_to_hex() {
        assert_eq!(bytes_to_hex(&[0xab, 0xcd, 0xef]), "abcdef");
        assert_eq!(bytes_to_hex(&[0x00, 0xff]), "00ff");
    }

    #[test]
    fn test_any_value_to_string() {
        let val = AnyValue {
            value: Some(any_value::Value::StringValue("hello".to_string())),
        };
        assert_eq!(any_value_to_string(&val), Some("hello".to_string()));

        let val = AnyValue {
            value: Some(any_value::Value::IntValue(42)),
        };
        assert_eq!(any_value_to_string(&val), Some("42".to_string()));

        let val = AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        };
        assert_eq!(any_value_to_string(&val), Some("true".to_string()));
    }
}
