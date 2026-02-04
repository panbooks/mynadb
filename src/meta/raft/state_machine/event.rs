//! Event and Lease Operations for MetaStateMachine
//!
//! Handles event publishing, subscription, and lease expiration.

use crate::meta::types::{current_timestamp_ms, keys, Event, EventType, LeaseInfo, MetaResponse};
use tokio::sync::broadcast;

use super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Event Operations
    // ========================================================================

    pub(crate) fn publish_event(
        &self,
        topic: &str,
        event_type: EventType,
        payload: Vec<u8>,
        source_node: &str,
    ) -> MetaResponse {
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            event_type,
            topic.to_string(),
            payload,
            source_node.to_string(),
        );

        // Persist to sled for durability and catch-up queries
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        // Broadcast to subscribers for instant notification
        // Ignore send errors (no subscribers or channel full)
        let _ = self.event_broadcast.send(event);

        MetaResponse::EventPublished { sequence: seq }
    }

    /// Subscribe to real-time event notifications
    ///
    /// Returns a broadcast receiver that will receive events as they are published.
    /// If the receiver falls behind (more than 1000 events), it will receive a
    /// `RecvError::Lagged` and should use `get_events_from()` to catch up.
    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.event_broadcast.subscribe()
    }

    // ========================================================================
    // Lease Management
    // ========================================================================

    pub(crate) fn expire_leases(&self) -> MetaResponse {
        let now = current_timestamp_ms();
        let prefix = keys::PREFIX_LEASE;
        let mut expired_nodes = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((k, v)) = item {
                if let Ok(lease) = bincode::deserialize::<LeaseInfo>(&v) {
                    if lease.expires_at < now {
                        let _ = self.db.remove(&k);
                        expired_nodes.push(lease.node_id);
                    }
                }
            }
        }

        // Deregister expired nodes
        for node_id in &expired_nodes {
            self.deregister_node(node_id);
        }

        // Return expired nodes so recovery can be triggered
        MetaResponse::LeasesExpired { expired_nodes }
    }
}
