//! Cache Invalidation Handler
//!
//! Subscribes to Meta service events and updates the Router's ShardCache accordingly.

use super::shard_cache::{DataNodeInfo, NodeState, PartitionKey, ShardCache};
use crate::meta::client::{EventSubscriber, MetaEvent, SubscriberConfig};
use crate::meta::types::EventType;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Cache invalidation handler
pub struct CacheInvalidationHandler {
    /// Shard cache to update
    shard_cache: Arc<ShardCache>,
    /// Event receiver
    event_rx: Option<mpsc::Receiver<MetaEvent>>,
    /// Event subscriber
    subscriber: Option<EventSubscriber>,
}

impl CacheInvalidationHandler {
    pub fn new(shard_cache: Arc<ShardCache>) -> Self {
        Self {
            shard_cache,
            event_rx: None,
            subscriber: None,
        }
    }

    /// Start the cache invalidation handler
    pub fn start(&mut self, config: SubscriberConfig) {
        let mut subscriber = EventSubscriber::new(config);
        let initial_seq = self.shard_cache.get_last_event_seq();
        let (event_rx, _handle) = subscriber.start(initial_seq);

        self.event_rx = Some(event_rx);
        self.subscriber = Some(subscriber);

        // Spawn the event processing loop
        let shard_cache = Arc::clone(&self.shard_cache);
        if let Some(rx) = self.event_rx.take() {
            tokio::spawn(async move {
                Self::process_events(rx, shard_cache).await;
            });
        }
    }

    /// Process events from the Meta service
    async fn process_events(
        mut event_rx: mpsc::Receiver<MetaEvent>,
        shard_cache: Arc<ShardCache>,
    ) {
        info!("Starting cache invalidation event processor");

        while let Some(event) = event_rx.recv().await {
            debug!(
                "Processing event: seq={}, type={:?}, topic={}",
                event.sequence, event.event_type, event.topic
            );

            // Update last seen sequence
            shard_cache.set_last_event_seq(event.sequence + 1);

            // Handle event based on type
            match event.event_type {
                EventType::NodeJoined => {
                    Self::handle_node_joined(&shard_cache, &event);
                }
                EventType::NodeLeft => {
                    Self::handle_node_left(&shard_cache, &event);
                }
                EventType::NodeStateChanged => {
                    Self::handle_node_updated(&shard_cache, &event);
                }
                EventType::FilesRegistered => {
                    Self::handle_files_registered(&shard_cache, &event);
                }
                EventType::FilesRemoved => {
                    Self::handle_files_removed(&shard_cache, &event);
                }
                EventType::LeaderChanged => {
                    info!("Leader changed event received");
                }
                EventType::CacheInvalidate => {
                    debug!("Cache invalidate event received");
                }
                EventType::SchemaChanged => {
                    debug!("Schema changed event received");
                }
                EventType::Unknown => {
                    debug!("Unknown event type");
                }
            }
        }

        info!("Cache invalidation event processor stopped");
    }

    /// Handle node joined event
    fn handle_node_joined(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();
            let grpc_addr = payload["grpc_addr"].as_str().unwrap_or_default();

            if !node_id.is_empty() && !grpc_addr.is_empty() {
                info!("Node joined: {} at {}", node_id, grpc_addr);
                shard_cache.upsert_data_node(DataNodeInfo {
                    node_id: node_id.to_string(),
                    grpc_addr: grpc_addr.to_string(),
                    state: NodeState::Active,
                    load_score: 0.5,
                });
            }
        }
    }

    /// Handle node left event
    fn handle_node_left(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();

            if !node_id.is_empty() {
                info!("Node left: {}", node_id);
                shard_cache.remove_data_node(node_id);

                // Clean up partition ownership to prevent memory growth
                // Note: This is optional since lookup will fail gracefully anyway,
                // but it keeps the cache clean
                shard_cache.remove_partitions_for_node(node_id);
            }
        }
    }

    /// Handle node updated event
    ///
    /// Note: When Meta sends a state update, it overrides local state (including Suspect).
    /// This allows nodes to recover from local Suspect state when Meta confirms they're healthy.
    fn handle_node_updated(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();
            let state_str = payload["state"].as_str().unwrap_or("active");
            let grpc_addr = payload["grpc_addr"].as_str().unwrap_or_default();

            if !node_id.is_empty() {
                let state = match state_str {
                    "active" => NodeState::Active,
                    "draining" => NodeState::Draining,
                    "suspect" => NodeState::Suspect,
                    _ => NodeState::Unhealthy,
                };

                debug!("Node updated: {} -> {:?}", node_id, state);
                shard_cache.upsert_data_node(DataNodeInfo {
                    node_id: node_id.to_string(),
                    grpc_addr: grpc_addr.to_string(),
                    state,
                    load_score: 0.5,
                });
            }
        }
    }

    /// Handle files registered event
    fn handle_files_registered(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            if let Some(files) = payload["files"].as_array() {
                for file in files {
                    let catalog = file["catalog"].as_str().unwrap_or_default();
                    let schema = file["schema"].as_str().unwrap_or_default();
                    let table = file["table"].as_str().unwrap_or_default();
                    let partition = file["partition"].as_str().unwrap_or_default();
                    let created_by = file["created_by"].as_str().unwrap_or_default();

                    if !catalog.is_empty() && !partition.is_empty() && !created_by.is_empty() {
                        // Extract date from partition (format: "date=YYYY-MM-DD")
                        let date = partition.strip_prefix("date=").unwrap_or(partition);

                        let key = PartitionKey::new(catalog, schema, table, date);
                        debug!("File registered for partition: {}", key.to_path());

                        // Update partition owner (just the node_id reference)
                        // Full node info is looked up from data_nodes when routing
                        shard_cache.set_partition_owner(&key, created_by.to_string());
                    }
                }
            }
        }
    }

    /// Handle files removed event
    fn handle_files_removed(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            if let Some(paths) = payload["paths"].as_array() {
                for path in paths {
                    if let Some(path_str) = path.as_str() {
                        // Try to parse partition key from path
                        if let Some(key) = PartitionKey::from_path(path_str) {
                            debug!("File removed from partition: {}", key.to_path());
                            // Note: We don't remove partition owners on file removal
                            // since there might be other files in the partition
                        }
                    }
                }
            }
        }
    }
}
