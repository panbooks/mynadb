//! Admin server state

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::config::AdminConfig;
use crate::meta::client::MetaClient;

use super::events::AdminEventDetail;

/// Admin event for broadcasting to WebSocket clients
#[derive(Clone, Debug)]
pub struct AdminEvent {
    pub event_type: String,
    pub payload: serde_json::Value,
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

    /// Get cached events, optionally filtered
    pub async fn get_cached_events(
        &self,
        event_type: Option<&str>,
        limit: usize,
    ) -> Vec<AdminEventDetail> {
        let cache = self.event_cache.read().await;

        let filtered: Vec<AdminEventDetail> = cache
            .iter()
            .rev() // Most recent first
            .filter(|e| event_type.is_none() || Some(e.event_type.as_str()) == event_type)
            .take(limit)
            .cloned()
            .collect();

        filtered
    }

    /// Get event cache statistics
    pub async fn event_cache_stats(&self) -> (usize, usize) {
        let cache = self.event_cache.read().await;
        (cache.len(), MAX_EVENT_CACHE_SIZE)
    }
}
