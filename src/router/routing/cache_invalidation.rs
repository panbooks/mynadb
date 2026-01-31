//! Cache Invalidation Handler
//!
//! Subscribes to Meta service events and updates the Router's ShardCache accordingly.

use super::shard_cache::{DataNodeInfo, DataNodeMode, NodeState, PartitionKey, ShardCache};
use crate::meta::client::{EventSubscriber, MetaClient, MetaEvent, SubscriberConfig};
use crate::meta::types::{
    DataNodeMode as MetaDataNodeMode, EventType, NodeRole, NodeState as MetaNodeState,
};
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
                        load_score: 0.5,
                        mode: convert_node_mode(n.mode),
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
                    load_score: 0.5,
                    mode: convert_node_mode(node.mode),
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
                    Self::handle_node_joined(&shard_cache, &data_node_pool, &initial_node_ids, &event);
                }
                EventType::NodeLeft => {
                    Self::handle_node_left(&shard_cache, &data_node_pool, &initial_node_ids, &event);
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
                EventType::ReplicaAssigned => {
                    Self::handle_replica_assigned(&shard_cache, &event);
                }
                EventType::ReplicaPromoted => {
                    Self::handle_replica_promoted(&shard_cache, &event);
                }
                EventType::ReplicaStateChanged => {
                    Self::handle_replica_state_changed(&shard_cache, &event);
                }
                EventType::DataNodeModeChanged => {
                    Self::handle_data_node_mode_changed(&shard_cache, &event);
                }
                EventType::Unknown => {
                    debug!("Unknown event type");
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
                    load_score: 0.5,
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

                // Clean up partition ownership to prevent memory growth
                shard_cache.remove_partitions_for_node(node_id);
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
            let grpc_addr = payload["grpc_addr"].as_str().unwrap_or_default();

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

                // Preserve existing mode or extract from payload
                let existing_mode = shard_cache
                    .get_data_node(node_id)
                    .map(|n| n.mode)
                    .unwrap_or_else(|| {
                        // Try to get mode from payload
                        match payload["mode"].as_str() {
                            Some("standby") | Some("Standby") => DataNodeMode::Standby,
                            _ => DataNodeMode::Primary,
                        }
                    });

                // Update shard cache
                shard_cache.upsert_data_node(DataNodeInfo {
                    node_id: node_id.to_string(),
                    grpc_addr: grpc_addr.to_string(),
                    state,
                    load_score: 0.5,
                    mode: existing_mode,
                });

                // Also update in connection pool (address may have changed)
                if !grpc_addr.is_empty() {
                    data_node_pool.add_node(node_id, grpc_addr);
                }
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

    /// Handle replica assigned event (warm standby replication)
    ///
    /// Updates the shard cache with partition replica information for instant failover.
    fn handle_replica_assigned(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let partition_key = payload["partition_key"].as_str().unwrap_or_default();
            let leader_node_id = payload["leader_node_id"].as_str().unwrap_or_default();
            let standby_node_ids: Vec<String> = payload["standby_node_ids"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            if !partition_key.is_empty() && !leader_node_id.is_empty() {
                info!(
                    "Replica assigned: {} -> leader={}, standbys={:?}",
                    partition_key, leader_node_id, standby_node_ids
                );

                // Update shard cache with replica information
                shard_cache.set_partition_replica(
                    partition_key,
                    leader_node_id.to_string(),
                    standby_node_ids,
                );
            }
        }
    }

    /// Handle replica promoted event (standby becomes leader)
    ///
    /// This is the critical event for zero-downtime failover. When a leader fails,
    /// Meta promotes a standby and publishes this event. Router immediately updates
    /// its shard cache so subsequent writes go to the new leader.
    fn handle_replica_promoted(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let partition_key = payload["partition_key"].as_str().unwrap_or_default();
            let new_leader_id = payload["new_leader_id"].as_str().unwrap_or_default();
            let old_leader_id = payload["old_leader_id"].as_str().unwrap_or_default();

            if !partition_key.is_empty() && !new_leader_id.is_empty() {
                info!(
                    "Replica promoted: {} -> new_leader={}, old_leader={}",
                    partition_key, new_leader_id, old_leader_id
                );

                // Update shard cache: promote standby to leader
                shard_cache.promote_partition_leader(partition_key, new_leader_id.to_string());

                // Mark old leader as suspect (if it's still in the cache)
                if !old_leader_id.is_empty() {
                    shard_cache.mark_node_suspect(old_leader_id);
                }
            }
        }
    }

    /// Handle replica state changed event
    fn handle_replica_state_changed(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let partition_key = payload["partition_key"].as_str().unwrap_or_default();
            let state = payload["state"].as_str().unwrap_or("offline");

            if !partition_key.is_empty() {
                debug!("Replica state changed: {} -> {}", partition_key, state);
                // State changes are informational for now
                // Router uses node health for routing decisions
            }
        }
    }

    /// Handle data node mode changed event (Cold Standby Architecture)
    ///
    /// When a Standby node is promoted to Primary, or a node's mode changes,
    /// this event is published. Router updates its shard cache to include/exclude
    /// the node from query routing.
    fn handle_data_node_mode_changed(shard_cache: &ShardCache, event: &MetaEvent) {
        if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
            let node_id = payload["node_id"].as_str().unwrap_or_default();
            let mode_str = payload["mode"].as_str().unwrap_or("primary");

            if !node_id.is_empty() {
                let mode = match mode_str {
                    "standby" => DataNodeMode::Standby,
                    _ => DataNodeMode::Primary,
                };

                info!("Data node mode changed: {} -> {:?}", node_id, mode);

                // Update node mode in shard cache
                shard_cache.update_node_mode(node_id, mode);
            }
        }
    }
}

/// Convert meta's DataNodeMode to router's DataNodeMode
fn convert_node_mode(mode: Option<MetaDataNodeMode>) -> DataNodeMode {
    match mode {
        Some(MetaDataNodeMode::Standby) => DataNodeMode::Standby,
        Some(MetaDataNodeMode::Primary) | None => DataNodeMode::Primary,
    }
}
