//! Event handling for Admin console
//!
//! This module bridges Meta service events to WebSocket clients and provides
//! event formatting/conversion utilities.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::meta::client::{EventSubscriber, MetaEvent, SubscriberConfig};
use crate::meta::types::EventType;

use super::state::AdminEvent;

/// AdminEvent with full details for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminEventDetail {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub timestamp: String,
    pub source: Option<String>,
    pub payload: Value,
    pub message: String,
}

/// Convert a MetaEvent to AdminEvent for broadcasting
pub fn meta_event_to_admin_event(meta_event: &MetaEvent) -> AdminEvent {
    let event_type = format_event_type(meta_event.event_type);
    let payload = parse_payload(&meta_event.payload);
    let message = generate_message(meta_event.event_type, &payload, &meta_event.source_node);

    AdminEvent {
        event_type,
        payload: json!({
            "id": format!("evt-{}", meta_event.sequence),
            "sequence": meta_event.sequence,
            "timestamp": meta_event.timestamp,
            "source": meta_event.source_node,
            "topic": meta_event.topic,
            "message": message,
            "data": payload,
        }),
    }
}

/// Convert a MetaEvent to AdminEventDetail for API responses
pub fn meta_event_to_detail(meta_event: &MetaEvent) -> AdminEventDetail {
    let event_type = format_event_type(meta_event.event_type);
    let payload = parse_payload(&meta_event.payload);
    let message = generate_message(meta_event.event_type, &payload, &meta_event.source_node);

    // Convert timestamp from milliseconds to RFC3339
    let timestamp = super::response::timestamp_to_rfc3339(meta_event.timestamp)
        .unwrap_or_default();

    AdminEventDetail {
        id: format!("evt-{}", meta_event.sequence),
        event_type,
        timestamp,
        source: Some(meta_event.source_node.clone()),
        payload,
        message,
    }
}

/// Format EventType to string for frontend
fn format_event_type(event_type: EventType) -> String {
    match event_type {
        EventType::NodeJoined => "NodeJoined".to_string(),
        EventType::NodeLeft => "NodeLeft".to_string(),
        EventType::NodeStateChanged => "NodeStateChanged".to_string(),
        EventType::LeaderChanged => "LeaderChanged".to_string(),
        EventType::CacheInvalidate => "CacheInvalidate".to_string(),
        EventType::SchemaChanged => "SchemaChanged".to_string(),
        EventType::FlushForRecovery => "FlushForRecovery".to_string(),
        EventType::TableOwnerAdded => "TableOwnerAdded".to_string(),
        EventType::TableOwnerRemoved => "TableOwnerRemoved".to_string(),
        EventType::TableReassigned => "TableReassigned".to_string(),
        EventType::DistributionConfigChanged => "DistributionConfigChanged".to_string(),
        EventType::TablesCacheRefresh => "TablesCacheRefresh".to_string(),
        EventType::TableOwnerExpired => "TableOwnerExpired".to_string(),
        EventType::Unknown => "Unknown".to_string(),
    }
}

/// Parse event payload from bytes to JSON
fn parse_payload(payload: &[u8]) -> Value {
    if payload.is_empty() {
        return json!({});
    }

    // Try to parse as JSON first
    if let Ok(value) = serde_json::from_slice::<Value>(payload) {
        return value;
    }

    // If not JSON, return as base64 string
    json!({
        "raw": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload)
    })
}

/// Generate a human-readable message for an event
fn generate_message(event_type: EventType, payload: &Value, source_node: &str) -> String {
    match event_type {
        EventType::NodeJoined => {
            let node_id = payload.get("node_id").and_then(|v| v.as_str()).unwrap_or(source_node);
            let role = payload.get("role").and_then(|v| v.as_str()).unwrap_or("unknown");
            format!("Node {} ({}) joined the cluster", node_id, role)
        }
        EventType::NodeLeft => {
            let node_id = payload.get("node_id").and_then(|v| v.as_str()).unwrap_or(source_node);
            format!("Node {} left the cluster", node_id)
        }
        EventType::NodeStateChanged => {
            let node_id = payload.get("node_id").and_then(|v| v.as_str()).unwrap_or(source_node);
            let old_state = payload.get("old_state").and_then(|v| v.as_str()).unwrap_or("unknown");
            let new_state = payload.get("new_state").and_then(|v| v.as_str()).unwrap_or("unknown");
            format!("Node {} state changed: {} → {}", node_id, old_state, new_state)
        }
        EventType::LeaderChanged => {
            let old_leader = payload.get("old_leader").and_then(|v| v.as_str()).unwrap_or("none");
            let new_leader = payload.get("new_leader").and_then(|v| v.as_str()).unwrap_or("unknown");
            let election = payload.get("election").and_then(|v| v.as_str()).unwrap_or("meta");
            format!("{} leader changed: {} → {}", election, old_leader, new_leader)
        }
        EventType::TableReassigned => {
            let table = format_table_name(payload);
            let old_owner = payload.get("old_primary").and_then(|v| v.as_str())
                .or_else(|| payload.get("old_owner").and_then(|v| v.as_str()))
                .unwrap_or("unknown");
            let new_owner = payload.get("new_primary").and_then(|v| v.as_str())
                .or_else(|| payload.get("new_owner").and_then(|v| v.as_str()))
                .unwrap_or("unknown");
            format!("Table {} reassigned: {} → {}", table, old_owner, new_owner)
        }
        EventType::TableOwnerAdded => {
            let table = format_table_name(payload);
            let added_owner = payload.get("added_owner").and_then(|v| v.as_str()).unwrap_or("unknown");
            format!("Owner {} added to table {}", added_owner, table)
        }
        EventType::TableOwnerRemoved => {
            let table = format_table_name(payload);
            let removed_owner = payload.get("removed_owner").and_then(|v| v.as_str()).unwrap_or("unknown");
            format!("Owner {} removed from table {}", removed_owner, table)
        }
        EventType::TableOwnerExpired => {
            let table = format_table_name(payload);
            let expired_owner = payload.get("expired_owner").and_then(|v| v.as_str()).unwrap_or("unknown");
            format!("Owner {} expired on table {}", expired_owner, table)
        }
        EventType::FlushForRecovery => {
            let standby_id = payload.get("standby_id").and_then(|v| v.as_str()).unwrap_or("unknown");
            let crashed_primary = payload.get("crashed_primary_id").and_then(|v| v.as_str()).unwrap_or("unknown");
            format!("Recovery: Standby {} flushing data for crashed Primary {}", standby_id, crashed_primary)
        }
        EventType::CacheInvalidate => {
            let table = format_table_name(payload);
            if table.is_empty() {
                "Cache invalidation triggered".to_string()
            } else {
                format!("Cache invalidated for table {}", table)
            }
        }
        EventType::SchemaChanged => {
            let table = format_table_name(payload);
            if table.is_empty() {
                "Schema changed".to_string()
            } else {
                format!("Schema changed for table {}", table)
            }
        }
        EventType::DistributionConfigChanged => {
            let table = format_table_name(payload);
            let percent = payload.get("percent").and_then(|v| v.as_u64()).unwrap_or(0);
            let deleted = payload.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false);
            if deleted {
                format!("Distribution config deleted for {}", table)
            } else {
                format!("Distribution config set to {}% for {}", percent, table)
            }
        }
        EventType::TablesCacheRefresh => {
            let evicted = payload.get("evicted_count").and_then(|v| v.as_u64()).unwrap_or(0);
            format!("Tables cache refreshed, {} entries evicted", evicted)
        }
        EventType::Unknown => {
            format!("Unknown event from {}", source_node)
        }
    }
}

/// Format table name from payload
fn format_table_name(payload: &Value) -> String {
    let catalog = payload.get("catalog").and_then(|v| v.as_str()).unwrap_or("");
    let schema = payload.get("schema").and_then(|v| v.as_str()).unwrap_or("");
    let table = payload.get("table").and_then(|v| v.as_str()).unwrap_or("");

    if catalog.is_empty() && schema.is_empty() && table.is_empty() {
        return String::new();
    }

    if !catalog.is_empty() && !schema.is_empty() && !table.is_empty() {
        format!("{}.{}.{}", catalog, schema, table)
    } else if !schema.is_empty() && !table.is_empty() {
        format!("{}.{}", schema, table)
    } else {
        table.to_string()
    }
}

/// Callback for caching events
pub type EventCacheCallback = Box<dyn Fn(AdminEventDetail) + Send + Sync>;

/// Start the event bridge that subscribes to Meta events and broadcasts to WebSocket clients
pub async fn start_event_bridge(
    meta_endpoints: Vec<String>,
    event_tx: tokio::sync::broadcast::Sender<AdminEvent>,
) {
    start_event_bridge_with_cache(meta_endpoints, event_tx, None).await
}

/// Start the event bridge with optional caching callback
pub async fn start_event_bridge_with_cache(
    meta_endpoints: Vec<String>,
    event_tx: tokio::sync::broadcast::Sender<AdminEvent>,
    cache_callback: Option<EventCacheCallback>,
) {
    tracing::info!("Starting event bridge to Meta endpoints: {:?}", meta_endpoints);

    let config = SubscriberConfig {
        endpoints: meta_endpoints,
        topics: vec![], // Subscribe to all topics
        reconnect_delay_ms: 2000,
        connect_timeout_ms: 5000,
    };

    let mut subscriber = EventSubscriber::new(config);
    let (mut event_rx, _handle) = subscriber.start(0); // Start from sequence 0

    // Process events and broadcast to WebSocket clients
    while let Some(meta_event) = event_rx.recv().await {
        let admin_event = meta_event_to_admin_event(&meta_event);
        let admin_event_detail = meta_event_to_detail(&meta_event);

        tracing::debug!(
            "Broadcasting event: type={}, seq={}",
            admin_event.event_type,
            meta_event.sequence
        );

        // Cache the event if callback is provided
        if let Some(ref callback) = cache_callback {
            callback(admin_event_detail);
        }

        // Broadcast to WebSocket clients (ignore send errors - no subscribers is OK)
        let _ = event_tx.send(admin_event);
    }

    tracing::warn!("Event bridge stopped - no more events from Meta");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_event_type() {
        assert_eq!(format_event_type(EventType::NodeJoined), "NodeJoined");
        assert_eq!(format_event_type(EventType::TableReassigned), "TableReassigned");
    }

    #[test]
    fn test_generate_message() {
        let payload = json!({
            "node_id": "node-1",
            "role": "Data"
        });
        let msg = generate_message(EventType::NodeJoined, &payload, "node-1");
        assert!(msg.contains("node-1"));
        assert!(msg.contains("Data"));
    }

    #[test]
    fn test_format_table_name() {
        let payload = json!({
            "catalog": "mydb",
            "schema": "default",
            "table": "cpu"
        });
        assert_eq!(format_table_name(&payload), "mydb.default.cpu");

        let empty = json!({});
        assert_eq!(format_table_name(&empty), "");
    }
}
