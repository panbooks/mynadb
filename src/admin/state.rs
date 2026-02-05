//! Admin server state

use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::config::AdminConfig;
use crate::meta::client::MetaClient;

/// Admin event for broadcasting to WebSocket clients
#[derive(Clone, Debug)]
pub struct AdminEvent {
    pub event_type: String,
    pub payload: serde_json::Value,
}

/// Shared admin server state
#[derive(Clone)]
pub struct AdminState {
    pub config: AdminConfig,
    pub meta_endpoints: Vec<String>,
    pub meta_client: Arc<RwLock<Option<Arc<MetaClient>>>>,
    pub event_tx: broadcast::Sender<AdminEvent>,
}

impl AdminState {
    pub fn new(config: AdminConfig, meta_endpoints: Vec<String>) -> Self {
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            config,
            meta_endpoints,
            meta_client: Arc::new(RwLock::new(None)),
            event_tx,
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
}
