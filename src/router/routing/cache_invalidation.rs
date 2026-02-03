//! Cache Invalidation Handler
//!
//! Subscribes to Meta service events and updates the Router's ShardCache accordingly.

use super::shard_cache::{DataNodeInfo, NodeState, ShardCache, TableKey};
use crate::meta::client::{EventSubscriber, MetaClient, MetaEvent, SubscriberConfig};
use crate::meta::types::{DataNodeMode, EventType, NodeRole, NodeState as MetaNodeState};
use crate::router::client::DataNodePool;
use dashmap::DashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Cache invalidation handler
pub struct CacheInvalidationHandler {
    /// Shard cache to update (for routing decisions)
    shard_cache: Arc<ShardCache>,
    /// Data node pool to update (for connection addresses)
    data_node_pool: Arc<DataNodePool>,
    /// Event receiver
    event_rx: Option<mpsc::Receiver<MetaEvent>>,
    /// Event subscriber
    subscriber: Option<EventSubscriber>,
    /// Nodes that were active when init_from_meta was called.
    /// Used to filter out stale NodeLeft events that would incorrectly
    /// remove nodes that are currently active according to Meta.
    initial_node_ids: Arc<DashSet<String>>,
}

impl CacheInvalidationHandler {
    pub fn new(shard_cache: Arc<ShardCache>, data_node_pool: Arc<DataNodePool>) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            event_rx: None,
            subscriber: None,
            initial_node_ids: Arc::new(DashSet::new()),
        }
    }

    /// Initialize cache from Meta's current state with retry
    ///
    /// Fetches all registered data nodes from Meta to populate:
    /// 1. ShardCache - for routing decisions
    /// 2. DataNodePool - for connection addresses
    ///
    /// Retries with exponential backoff if no nodes are found (race condition
    /// when router and data nodes start simultaneously).
    ///
    /// Must be called BEFORE start() to avoid a window where the cache is empty.
    pub async fn init_from_meta(&self, meta_client: &MetaClient) -> Result<(), String> {
        const MAX_RETRIES: u32 = 10;
        const INITIAL_DELAY_MS: u64 = 500;

        for attempt in 0..MAX_RETRIES {
            // Fetch all active data nodes
            let nodes = meta_client
                .list_nodes(Some(NodeRole::Data), Some(MetaNodeState::Active))
                .await?;

            if !nodes.is_empty() {
                // Convert to DataNodeInfo and populate shard cache
                let data_nodes: Vec<DataNodeInfo> = nodes
                    .iter()
                    .map(|n| DataNodeInfo {
                        node_id: n.node_id.clone(),
                        grpc_addr: n.advertise_addr.clone(),
                        state: NodeState::Active,
                        mode: n.mode.unwrap_or_default(),
                    })
                    .collect();

                let count = data_nodes.len();

                // Update shard cache for routing
                self.shard_cache.update_data_nodes(data_nodes);

                // Also add nodes to connection pool and record initial node IDs
                for node in &nodes {
                    self.data_node_pool
                        .add_node(&node.node_id, &node.advertise_addr);
                    // Record this node as initially active.
                    self.initial_node_ids.insert(node.node_id.clone());
                }

                // Skip historical events by setting a high sequence number.
                // This prevents replaying old NodeLeft events. The race condition
                // where a node registers AFTER this init is handled by:
                // 1. The node will send NodeJoined events which we'll process
                // 2. We do a re-sync in start() to catch any missed nodes
                self.shard_cache.set_last_event_seq(u64::MAX / 2);

                info!(
                    "Initialized shard cache and connection pool with {} data nodes from Meta (attempt {})",
                    count,
                    attempt + 1
                );
                return Ok(());
            }

            // No nodes found, wait and retry
            if attempt < MAX_RETRIES - 1 {
                let delay = INITIAL_DELAY_MS * (1 << attempt.min(4)); // Cap at 8 seconds
                info!(
                    "No data nodes found, retrying in {}ms (attempt {}/{})",
                    delay,
                    attempt + 1,
                    MAX_RETRIES
                );
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }

        warn!(
            "No data nodes found after {} attempts, will rely on events",
            MAX_RETRIES
        );
        Ok(())
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
        let data_node_pool = Arc::clone(&self.data_node_pool);
        let initial_node_ids = Arc::clone(&self.initial_node_ids);
        if let Some(rx) = self.event_rx.take() {
            tokio::spawn(async move {
                Self::process_events(rx, shard_cache, data_node_pool, initial_node_ids).await;
            });
        }
    }

    /// Re-sync nodes from Meta to handle race conditions
    ///
    /// This should be called after start() with a short delay to catch any nodes
    /// that registered between init_from_meta() and start(). It's safe to call
    /// this multiple times - it will only ADD nodes, not remove them.
    pub async fn resync_from_meta(&self, meta_client: &MetaClient) -> Result<(), String> {
        let nodes = meta_client
            .list_nodes(Some(NodeRole::Data), Some(MetaNodeState::Active))
            .await?;

        if nodes.is_empty() {
            return Ok(());
        }

        let mut added = 0;
        for node in &nodes {
            // Check if node is already in shard cache
            if self.shard_cache.get_data_node(&node.node_id).is_none() {
                // Add missing node
                self.shard_cache.upsert_data_node(DataNodeInfo {
                    node_id: node.node_id.clone(),
                    grpc_addr: node.advertise_addr.clone(),
                    state: NodeState::Active,
                    mode: node.mode.unwrap_or_default(),
                });
                self.data_node_pool
                    .add_node(&node.node_id, &node.advertise_addr);
                self.initial_node_ids.insert(node.node_id.clone());
                added += 1;
            }
        }

        if added > 0 {
            info!(
                "Resync added {} missing data nodes from Meta (total now: {})",
                added,
                self.shard_cache.get_active_data_nodes().len()
            );
        }

        Ok(())
    }

    /// Process events from the Meta service
    async fn process_events(
        mut event_rx: mpsc::Receiver<MetaEvent>,
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        initial_node_ids: Arc<DashSet<String>>,
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
                    Self::handle_node_joined(
                        &shard_cache,
                        &data_node_pool,
                        &initial_node_ids,
                        &event,
                    );
                }
                EventType::NodeLeft => {
                    Self::handle_node_left(
                        &shard_cache,
                        &data_node_pool,
                        &initial_node_ids,
                        &event,
                    );
                }
                EventType::NodeStateChanged => {
                    Self::handle_node_updated(&shard_cache, &data_node_pool, &event);
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
                EventType::DistributionConfigChanged => {
                    Self::handle_distribution_config_changed(&shard_cache, &event);
                }
                _ => {
                    // FlushForRecovery handled by Data nodes directly
                    // Other events not relevant to Router
                }
            }
        }

        info!("Cache invalidation event processor stopped");
    }

    /// Handle node joined event
    fn handle_node_joined(
        shard_cache: &ShardCache,
        data_node_pool: &DataNodePool,
        initial_node_ids: &DashSet<String>,
        event: &MetaEvent,
    ) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();
            let grpc_addr = payload["grpc_addr"].as_str().unwrap_or_default();

            if !node_id.is_empty() && !grpc_addr.is_empty() {
                // Extract mode from payload if present
                let mode = match payload["mode"].as_str() {
                    Some("standby") | Some("Standby") => DataNodeMode::Standby,
                    _ => DataNodeMode::Primary, // Default to Primary
                };

                info!(
                    "Node joined: {} at {} (mode: {:?})",
                    node_id, grpc_addr, mode
                );

                // Update shard cache for routing
                shard_cache.upsert_data_node(DataNodeInfo {
                    node_id: node_id.to_string(),
                    grpc_addr: grpc_addr.to_string(),
                    state: NodeState::Active,
                    mode,
                });

                // Also add to connection pool
                data_node_pool.add_node(node_id, grpc_addr);

                // Add to initial set if not already there - this ensures we track ALL nodes
                // that have been seen as active, protecting them from stale NodeLeft events
                initial_node_ids.insert(node_id.to_string());
            }
        }
    }

    /// Handle node left event
    fn handle_node_left(
        shard_cache: &ShardCache,
        data_node_pool: &DataNodePool,
        initial_node_ids: &DashSet<String>,
        event: &MetaEvent,
    ) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();

            if !node_id.is_empty() {
                // Check if this is a stale event for a node that was active when we initialized.
                // If so, skip it - the node is still active according to Meta.
                if initial_node_ids.contains(node_id) {
                    debug!(
                        "Ignoring stale NodeLeft for {} (node was active during init)",
                        node_id
                    );
                    return;
                }

                info!("Node left: {}", node_id);

                // Remove from shard cache
                shard_cache.remove_data_node(node_id);

                // Remove from connection pool
                data_node_pool.remove_node(node_id);

                // Clean up table ownership to prevent memory growth
                shard_cache.remove_tables_for_node(node_id);
            }
        }
    }

    /// Handle node updated event
    ///
    /// Note: When Meta sends a state update, it overrides local state (including Suspect).
    /// This allows nodes to recover from local Suspect state when Meta confirms they're healthy.
    fn handle_node_updated(
        shard_cache: &ShardCache,
        data_node_pool: &DataNodePool,
        event: &MetaEvent,
    ) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();
            // Support both "state" and "new_state" fields (NodeStateChanged uses "new_state")
            let state_str = payload["new_state"]
                .as_str()
                .or_else(|| payload["state"].as_str())
                .unwrap_or("active");
            let grpc_addr_from_event = payload["grpc_addr"].as_str().unwrap_or_default();

            if !node_id.is_empty() {
                let state = match state_str.to_lowercase().as_str() {
                    "active" => NodeState::Active,
                    "draining" => NodeState::Draining,
                    "suspect" => NodeState::Suspect,
                    "retiring" => NodeState::Retiring,
                    "drained" => NodeState::Drained,
                    _ => NodeState::Unhealthy,
                };

                // For retiring/drained nodes, log a special message
                if state == NodeState::Retiring {
                    info!(
                        "Node {} is retiring (scale-in): routing will stop sending new writes",
                        node_id
                    );
                } else if state == NodeState::Drained {
                    info!(
                        "Node {} is drained (scale-in complete): safe to stop",
                        node_id
                    );
                } else {
                    debug!("Node updated: {} -> {:?}", node_id, state);
                }

                // Get existing node info to preserve grpc_addr and mode if not in event
                let existing_node = shard_cache.get_data_node(node_id);

                // Preserve existing grpc_addr if event doesn't include it
                let grpc_addr = if !grpc_addr_from_event.is_empty() {
                    grpc_addr_from_event.to_string()
                } else {
                    existing_node
                        .as_ref()
                        .map(|n| n.grpc_addr.clone())
                        .unwrap_or_default()
                };

                // Preserve existing mode or extract from payload
                let mode = existing_node.as_ref().map(|n| n.mode).unwrap_or_else(|| {
                    // Try to get mode from payload
                    match payload["mode"].as_str() {
                        Some("standby") | Some("Standby") => DataNodeMode::Standby,
                        _ => DataNodeMode::Primary,
                    }
                });

                // Only update if we have a valid grpc_addr (either from event or existing)
                if !grpc_addr.is_empty() {
                    shard_cache.upsert_data_node(DataNodeInfo {
                        node_id: node_id.to_string(),
                        grpc_addr: grpc_addr.clone(),
                        state,
                        mode,
                    });

                    // Also update in connection pool (if address changed)
                    if !grpc_addr_from_event.is_empty() {
                        data_node_pool.add_node(node_id, &grpc_addr);
                    }
                } else {
                    warn!(
                        "Cannot update node {} state to {:?}: no grpc_addr available",
                        node_id, state
                    );
                }
            }
        }
    }

    /// Handle table registered event (table-level routing)
    ///
    /// Updates the shard cache with table ownership information.
    fn handle_files_registered(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            // Handle table registration events
            let catalog = payload["catalog"].as_str().unwrap_or_default();
            let schema = payload["schema"].as_str().unwrap_or_default();
            let table = payload["table"].as_str().unwrap_or_default();
            let owner_node_id = payload["owner_node_id"]
                .as_str()
                .or_else(|| payload["created_by"].as_str())
                .unwrap_or_default();
            let version = payload["version"].as_u64().unwrap_or(1);

            if !catalog.is_empty() && !table.is_empty() && !owner_node_id.is_empty() {
                let key = TableKey::new(catalog, schema, table);
                debug!(
                    "Table registered: {} -> {} (v{})",
                    key.to_path(),
                    owner_node_id,
                    version
                );

                // Update table owner for routing
                shard_cache.set_table_owner(&key, owner_node_id.to_string(), version);
            }
        }
    }

    /// Handle files removed event (no-op for table-level routing)
    fn handle_files_removed(_shard_cache: &ShardCache, event: &MetaEvent) {
        // With table-level routing, file removal doesn't affect routing
        // Tables are only removed when explicitly dropped
        debug!(
            "Files removed event (no-op for table routing): seq={}",
            event.sequence
        );
    }

    /// Handle distribution config changed event
    ///
    /// Updates the local distribution config cache for fast routing lookups.
    /// This avoids querying Meta on every write for distribution decisions.
    fn handle_distribution_config_changed(shard_cache: &ShardCache, event: &MetaEvent) {
        use crate::meta::types::{DistributionConfig, DistributionScope};

        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let catalog = payload["catalog"].as_str().unwrap_or_default();
            let schema = payload["schema"].as_str().unwrap_or_default();
            let table = payload["table"].as_str().unwrap_or_default();
            let percent = payload["percent"].as_u64().unwrap_or(0) as u8;
            let deleted = payload["deleted"].as_bool().unwrap_or(false);

            if catalog.is_empty() {
                warn!("Distribution config event missing catalog");
                return;
            }

            if deleted {
                // Remove config from cache
                info!(
                    "Distribution config deleted: {}/{}/{} - removing from cache",
                    catalog,
                    if schema.is_empty() { "*" } else { schema },
                    if table.is_empty() { "*" } else { table }
                );
                shard_cache.remove_distribution_config(catalog, schema, table);
            } else {
                // Determine scope based on which fields are set
                let scope = if !table.is_empty() {
                    DistributionScope::Table
                } else if !schema.is_empty() {
                    DistributionScope::Schema
                } else {
                    DistributionScope::Catalog
                };

                info!(
                    "Distribution config changed: {}/{}/{} -> {}% ({:?})",
                    catalog,
                    if schema.is_empty() { "*" } else { schema },
                    if table.is_empty() { "*" } else { table },
                    percent,
                    scope
                );

                // Create config and add to cache
                let config = DistributionConfig::new(
                    catalog.to_string(),
                    schema.to_string(),
                    table.to_string(),
                    percent,
                );
                shard_cache.set_distribution_config(&config);
            }
        }
    }
}
