//! Leader election for background tasks
//!
//! Ensures that only one node in the cluster runs certain background tasks
//! (like compaction and retention) at a time.

use crate::cluster::coordinator::Coordinator;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Leader election state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderState {
    /// Not participating in election
    NotStarted,
    /// Campaigning for leadership
    Campaigning,
    /// Currently the leader
    Leader,
    /// A follower (not the leader)
    Follower,
    /// Election failed
    Failed,
}

impl std::fmt::Display for LeaderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaderState::NotStarted => write!(f, "not_started"),
            LeaderState::Campaigning => write!(f, "campaigning"),
            LeaderState::Leader => write!(f, "leader"),
            LeaderState::Follower => write!(f, "follower"),
            LeaderState::Failed => write!(f, "failed"),
        }
    }
}

/// Callback type for leadership changes
pub type LeadershipCallback = Box<dyn Fn(bool) + Send + Sync>;

/// Leader election manager
pub struct LeaderElection {
    /// The coordinator
    coordinator: Arc<dyn Coordinator>,
    /// This node's ID
    node_id: String,
    /// Election key
    election_key: String,
    /// Lease TTL
    lease_ttl: Duration,
    /// Current state
    state: Arc<RwLock<LeaderState>>,
    /// Current lease ID (if leader)
    lease_id: Arc<RwLock<Option<i64>>>,
    /// Callbacks for leadership changes
    callbacks: Arc<RwLock<Vec<LeadershipCallback>>>,
    /// Election task handle
    election_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// Keep-alive task handle
    keepalive_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl LeaderElection {
    /// Create a new leader election manager
    pub fn new(
        coordinator: Arc<dyn Coordinator>,
        node_id: String,
        election_key: String,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            coordinator,
            node_id,
            election_key,
            lease_ttl,
            state: Arc::new(RwLock::new(LeaderState::NotStarted)),
            lease_id: Arc::new(RwLock::new(None)),
            callbacks: Arc::new(RwLock::new(Vec::new())),
            election_handle: parking_lot::Mutex::new(None),
            keepalive_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Get the current election state
    pub fn state(&self) -> LeaderState {
        *self.state.read()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        *self.state.read() == LeaderState::Leader
    }

    /// Get the current leader's node ID
    pub async fn current_leader(&self) -> Result<Option<String>, String> {
        self.coordinator.get_leader(&self.election_key).await
    }

    /// Register a callback for leadership changes
    pub fn on_leadership_change<F>(&self, callback: F)
    where
        F: Fn(bool) + Send + Sync + 'static,
    {
        self.callbacks.write().push(Box::new(callback));
    }

    /// Start campaigning for leadership
    pub async fn start(&self) -> Result<(), String> {
        if *self.state.read() != LeaderState::NotStarted {
            return Err("Election already started".to_string());
        }

        *self.state.write() = LeaderState::Campaigning;
        info!(
            "Starting leader election: node_id={}, key={}",
            self.node_id, self.election_key
        );

        self.start_election_loop();
        Ok(())
    }

    /// Stop participating in election
    pub async fn stop(&self) -> Result<(), String> {
        // Stop the election loop
        if let Some(handle) = self.election_handle.lock().take() {
            handle.abort();
        }

        // Stop keep-alive
        if let Some(handle) = self.keepalive_handle.lock().take() {
            handle.abort();
        }

        // Resign if we're the leader
        if self.is_leader() {
            if let Some(lease_id) = *self.lease_id.read() {
                self.coordinator
                    .resign(&self.election_key, lease_id)
                    .await?;
            }
        }

        *self.state.write() = LeaderState::NotStarted;
        *self.lease_id.write() = None;

        info!("Stopped leader election: node_id={}", self.node_id);
        Ok(())
    }

    /// Start the election loop
    fn start_election_loop(&self) {
        let coordinator = Arc::clone(&self.coordinator);
        let node_id = self.node_id.clone();
        let election_key = self.election_key.clone();
        let lease_ttl = self.lease_ttl;
        let state = Arc::clone(&self.state);
        let lease_id_holder = Arc::clone(&self.lease_id);
        let callbacks = Arc::clone(&self.callbacks);

        let handle = tokio::spawn(async move {
            loop {
                let current_state = *state.read();

                match current_state {
                    LeaderState::NotStarted | LeaderState::Failed => {
                        break;
                    }
                    LeaderState::Campaigning | LeaderState::Follower => {
                        // Try to become leader
                        match coordinator
                            .campaign(&election_key, &node_id, lease_ttl)
                            .await
                        {
                            Ok(lid) => {
                                info!("Became leader: node_id={}, lease_id={}", node_id, lid);
                                *state.write() = LeaderState::Leader;
                                *lease_id_holder.write() = Some(lid);

                                // Notify callbacks
                                for callback in callbacks.read().iter() {
                                    callback(true);
                                }

                                // Start keep-alive
                                Self::start_keepalive_static(
                                    Arc::clone(&coordinator),
                                    lid,
                                    lease_ttl / 3,
                                    Arc::clone(&state),
                                    Arc::clone(&lease_id_holder),
                                    Arc::clone(&callbacks),
                                );
                            }
                            Err(e) => {
                                warn!("Failed to campaign: {}", e);
                                *state.write() = LeaderState::Follower;
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                    LeaderState::Leader => {
                        // Already leader, wait and check
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        *self.election_handle.lock() = Some(handle);
    }

    /// Start keep-alive for the lease (static version for use in spawned task)
    fn start_keepalive_static(
        coordinator: Arc<dyn Coordinator>,
        lease_id: i64,
        interval: Duration,
        state: Arc<RwLock<LeaderState>>,
        lease_id_holder: Arc<RwLock<Option<i64>>>,
        callbacks: Arc<RwLock<Vec<LeadershipCallback>>>,
    ) {
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);

            loop {
                timer.tick().await;

                if *state.read() != LeaderState::Leader {
                    debug!("No longer leader, stopping keep-alive");
                    break;
                }

                match coordinator.keep_alive(lease_id).await {
                    Ok(_) => {
                        debug!("Leadership lease renewed: lease_id={}", lease_id);
                    }
                    Err(e) => {
                        error!("Failed to renew leadership lease: {}", e);
                        *state.write() = LeaderState::Follower;
                        *lease_id_holder.write() = None;

                        // Notify callbacks
                        for callback in callbacks.read().iter() {
                            callback(false);
                        }

                        break;
                    }
                }
            }
        });
    }
}

impl Drop for LeaderElection {
    fn drop(&mut self) {
        if let Some(handle) = self.election_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.keepalive_handle.lock().take() {
            handle.abort();
        }
    }
}

/// Helper to run a task only when this node is the leader
pub struct LeaderOnlyTask {
    /// The leader election
    election: Arc<LeaderElection>,
    /// Task name for logging
    name: String,
    /// The task handle
    handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl LeaderOnlyTask {
    /// Create a new leader-only task
    pub fn new(election: Arc<LeaderElection>, name: impl Into<String>) -> Self {
        Self {
            election,
            name: name.into(),
            handle: parking_lot::Mutex::new(None),
        }
    }

    /// Run a task only when this node is the leader
    pub fn run<F, Fut>(&self, task: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let election = Arc::clone(&self.election);
        let name = self.name.clone();

        let handle = tokio::spawn(async move {
            loop {
                if election.is_leader() {
                    debug!("Running leader-only task: {}", name);
                    task().await;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        *self.handle.lock() = Some(handle);
    }

    /// Stop the task
    pub fn stop(&self) {
        if let Some(handle) = self.handle.lock().take() {
            handle.abort();
        }
    }
}

impl Drop for LeaderOnlyTask {
    fn drop(&mut self) {
        self.stop();
    }
}
