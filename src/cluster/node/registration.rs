//! Node registration with the cluster coordinator
//!
//! Handles registering and deregistering nodes from the cluster.

use super::identity::{NodeId, NodeInfo, NodeState};
use crate::cluster::coordinator::Coordinator;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Node registration manager
pub struct NodeRegistration {
    /// The coordinator
    coordinator: Arc<dyn Coordinator>,
    /// This node's info
    node_info: Arc<RwLock<NodeInfo>>,
    /// Lease ID from registration
    lease_id: Arc<RwLock<Option<i64>>>,
    /// Heartbeat interval
    heartbeat_interval: Duration,
    /// Heartbeat task handle
    heartbeat_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl NodeRegistration {
    /// Create a new node registration manager
    pub fn new(
        coordinator: Arc<dyn Coordinator>,
        node_info: NodeInfo,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            coordinator,
            node_info: Arc::new(RwLock::new(node_info)),
            lease_id: Arc::new(RwLock::new(None)),
            heartbeat_interval,
            heartbeat_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_info.read().node_id.clone()
    }

    /// Get a copy of the current node info
    pub fn node_info(&self) -> NodeInfo {
        self.node_info.read().clone()
    }

    /// Update node info
    pub fn update_node_info<F>(&self, f: F)
    where
        F: FnOnce(&mut NodeInfo),
    {
        f(&mut self.node_info.write());
    }

    /// Register this node with the cluster
    pub async fn register(&self) -> Result<(), String> {
        let mut node_info = self.node_info.write();
        node_info.set_state(NodeState::Starting);

        let key = node_info.etcd_key();
        let value = node_info.to_bytes()?;

        // Register with a TTL so the key expires if the node dies
        let ttl = self.heartbeat_interval * 3; // 3x heartbeat interval
        let lease_id = self.coordinator.put_with_ttl(&key, &value, ttl).await?;

        *self.lease_id.write() = Some(lease_id);

        info!(
            "Node registered: node_id={}, key={}, lease_id={}",
            node_info.node_id, key, lease_id
        );

        Ok(())
    }

    /// Mark the node as active and start heartbeat
    pub async fn activate(&self) -> Result<(), String> {
        {
            let mut node_info = self.node_info.write();
            node_info.set_state(NodeState::Active);
        }

        // Update registration with new state
        self.update_registration().await?;

        // Start heartbeat
        self.start_heartbeat();

        info!("Node activated: node_id={}", self.node_id());
        Ok(())
    }

    /// Start draining the node (prepare to leave)
    pub async fn drain(&self) -> Result<(), String> {
        {
            let mut node_info = self.node_info.write();
            node_info.set_state(NodeState::Draining);
        }

        self.update_registration().await?;

        info!("Node draining: node_id={}", self.node_id());
        Ok(())
    }

    /// Deregister this node from the cluster
    pub async fn deregister(&self) -> Result<(), String> {
        // Stop heartbeat
        self.stop_heartbeat();

        let key = self.node_info.read().etcd_key();

        // Delete the node key
        self.coordinator.delete(&key).await?;

        // Revoke the lease if we have one
        if let Some(lease_id) = self.lease_id.read().as_ref() {
            // Note: The coordinator doesn't expose lease revocation directly,
            // but the key will be deleted and the lease will expire
            debug!("Lease {} will expire", lease_id);
        }

        {
            let mut node_info = self.node_info.write();
            node_info.set_state(NodeState::Left);
        }

        info!("Node deregistered: node_id={}", self.node_id());
        Ok(())
    }

    /// Update the registration (after state change)
    async fn update_registration(&self) -> Result<(), String> {
        // Extract values before await to avoid holding lock across await point
        let (key, value) = {
            let node_info = self.node_info.read();
            let key = node_info.etcd_key();
            let value = node_info.to_bytes()?;
            (key, value)
        };

        self.coordinator.put(&key, &value).await
    }

    /// Start the heartbeat task
    fn start_heartbeat(&self) {
        let coordinator = Arc::clone(&self.coordinator);
        let node_info = Arc::clone(&self.node_info);
        let lease_id = Arc::clone(&self.lease_id);
        let interval = self.heartbeat_interval;

        let handle = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);

            loop {
                timer.tick().await;

                // Update heartbeat timestamp
                {
                    node_info.write().heartbeat();
                }

                // Get lease_id before awaiting
                let lid = { *lease_id.read() };

                // Keep the lease alive
                if let Some(lid) = lid {
                    if let Err(e) = coordinator.keep_alive(lid).await {
                        warn!("Heartbeat keep-alive failed: {}", e);
                    }
                }

                // Get node info before awaiting
                let (key, value_result, node_id) = {
                    let info = node_info.read();
                    (info.etcd_key(), info.to_bytes(), info.node_id.clone())
                };

                // Update the node info in etcd
                match value_result {
                    Ok(value) => {
                        if let Err(e) = coordinator.put(&key, &value).await {
                            warn!("Heartbeat update failed: {}", e);
                        } else {
                            debug!("Heartbeat sent: node_id={}", node_id);
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize node info: {}", e);
                    }
                }
            }
        });

        *self.heartbeat_handle.lock() = Some(handle);
    }

    /// Stop the heartbeat task
    fn stop_heartbeat(&self) {
        if let Some(handle) = self.heartbeat_handle.lock().take() {
            handle.abort();
        }
    }

    /// List all registered nodes in the cluster
    pub async fn list_nodes(&self) -> Result<Vec<NodeInfo>, String> {
        let prefix = "/mynadb/nodes/";
        let entries = self.coordinator.get_prefix(prefix).await?;

        let mut nodes = Vec::new();
        for (key, value) in entries {
            match NodeInfo::from_bytes(&value) {
                Ok(info) => nodes.push(info),
                Err(e) => {
                    warn!("Failed to parse node info for {}: {}", key, e);
                }
            }
        }

        Ok(nodes)
    }

    /// Get info for a specific node
    pub async fn get_node(&self, node_id: &str) -> Result<Option<NodeInfo>, String> {
        let key = format!("/mynadb/nodes/{}", node_id);
        match self.coordinator.get(&key).await? {
            Some(value) => {
                let info = NodeInfo::from_bytes(&value)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }
}

impl Drop for NodeRegistration {
    fn drop(&mut self) {
        self.stop_heartbeat();
    }
}
