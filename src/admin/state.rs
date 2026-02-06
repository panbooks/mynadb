//! Admin server state

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::config::AdminConfig;
use crate::meta::client::MetaClient;

use serde::Serialize;

use super::events::AdminEventDetail;

/// Admin event for broadcasting to WebSocket clients
#[derive(Clone, Debug)]
pub struct AdminEvent {
    pub event_type: String,
    pub payload: serde_json::Value,
}

/// Filter options for querying events
#[derive(Debug, Default)]
pub struct EventFilter {
    /// Filter by event types (empty = all)
    pub event_types: Option<Vec<String>>,
    /// Filter by source node (substring match)
    pub source: Option<String>,
    /// Filter events since this timestamp (milliseconds)
    pub since_ms: Option<i64>,
    /// Full-text search in message/payload
    pub search: Option<String>,
    /// Maximum number of events to return
    pub limit: usize,
}

/// Event statistics
#[derive(Debug, Clone, Serialize)]
pub struct EventStats {
    pub total: usize,
    pub events_last_hour: usize,
    pub by_type: std::collections::HashMap<String, usize>,
}

/// Maximum number of events to keep in the cache
const MAX_EVENT_CACHE_SIZE: usize = 1000;

/// Shared admin server state
#[derive(Clone)]
pub struct AdminState {
    pub config: AdminConfig,
    pub meta_endpoints: Vec<String>,
    pub meta_client: Arc<RwLock<Option<Arc<MetaClient>>>>,
    pub event_tx: broadcast::Sender<AdminEvent>,
    /// Cache of recent events for historical queries
    event_cache: Arc<RwLock<VecDeque<AdminEventDetail>>>,
}

impl AdminState {
    pub fn new(config: AdminConfig, meta_endpoints: Vec<String>) -> Self {
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            config,
            meta_endpoints,
            meta_client: Arc::new(RwLock::new(None)),
            event_tx,
            event_cache: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_EVENT_CACHE_SIZE))),
        }
    }

    /// Get or create a MetaClient connection
    pub async fn get_meta_client(&self) -> Result<Arc<MetaClient>, String> {
        // Check if we have a cached client
        {
            let guard = self.meta_client.read().await;
            if let Some(ref client) = *guard {
                return Ok(Arc::clone(client));
            }
        }

        // Create a new client
        let client = Arc::new(MetaClient::connect(&self.meta_endpoints).await?);

        // Cache it
        {
            let mut guard = self.meta_client.write().await;
            *guard = Some(Arc::clone(&client));
        }

        Ok(client)
    }

    /// Broadcast an event to all connected WebSocket clients
    pub fn broadcast_event(&self, event: AdminEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<AdminEvent> {
        self.event_tx.subscribe()
    }

    /// Add an event to the cache
    pub async fn cache_event(&self, event: AdminEventDetail) {
        let mut cache = self.event_cache.write().await;
        if cache.len() >= MAX_EVENT_CACHE_SIZE {
            cache.pop_front();
        }
        cache.push_back(event);
    }

    /// Get cached events with advanced filtering
    pub async fn get_cached_events(
        &self,
        filter: EventFilter,
    ) -> Vec<AdminEventDetail> {
        let cache = self.event_cache.read().await;

        let filtered: Vec<AdminEventDetail> = cache
            .iter()
            .rev() // Most recent first
            .filter(|e| {
                // Event type filter
                if let Some(ref types) = filter.event_types {
                    if !types.is_empty() && !types.iter().any(|t| t == &e.event_type) {
                        return false;
                    }
                }

                // Source node filter
                if let Some(ref source) = filter.source {
                    if let Some(ref event_source) = e.source {
                        if !event_source.contains(source) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }

                // Time range filter (since)
                if let Some(since_ms) = filter.since_ms {
                    if let Ok(event_time) = chrono::DateTime::parse_from_rfc3339(&e.timestamp) {
                        if event_time.timestamp_millis() < since_ms {
                            return false;
                        }
                    }
                }

                // Text search filter
                if let Some(ref search) = filter.search {
                    let search_lower = search.to_lowercase();
                    let message_match = e.message.to_lowercase().contains(&search_lower);
                    let type_match = e.event_type.to_lowercase().contains(&search_lower);
                    let payload_match = e.payload.to_string().to_lowercase().contains(&search_lower);
                    if !message_match && !type_match && !payload_match {
                        return false;
                    }
                }

                true
            })
            .take(filter.limit)
            .cloned()
            .collect();

        filtered
    }

    /// Get unique source nodes from cached events
    pub async fn get_event_sources(&self) -> Vec<String> {
        let cache = self.event_cache.read().await;
        let mut sources: std::collections::HashSet<String> = std::collections::HashSet::new();

        for event in cache.iter() {
            if let Some(ref source) = event.source {
                sources.insert(source.clone());
            }
        }

        let mut result: Vec<String> = sources.into_iter().collect();
        result.sort();
        result
    }

    /// Get event statistics
    pub async fn get_event_stats(&self) -> EventStats {
        let cache = self.event_cache.read().await;
        let mut type_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        let now = chrono::Utc::now();
        let one_hour_ago = now - chrono::Duration::hours(1);
        let mut events_last_hour = 0usize;

        for event in cache.iter() {
            *type_counts.entry(event.event_type.clone()).or_insert(0) += 1;

            if let Ok(event_time) = chrono::DateTime::parse_from_rfc3339(&event.timestamp) {
                if event_time > one_hour_ago {
                    events_last_hour += 1;
                }
            }
        }

        EventStats {
            total: cache.len(),
            events_last_hour,
            by_type: type_counts,
        }
    }

    /// Get event cache statistics
    pub async fn event_cache_stats(&self) -> (usize, usize) {
        let cache = self.event_cache.read().await;
        (cache.len(), MAX_EVENT_CACHE_SIZE)
    }
}
