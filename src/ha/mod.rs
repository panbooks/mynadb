//! High Availability module
//!
//! Provides Master-Slave replication for high availability.

pub mod api;
pub mod failover;
pub mod replication;
pub mod state;

#[cfg(test)]
mod tests;

pub use failover::{FailoverManager, PromotionResult, PromotionStateInfo, Promoter};
pub use replication::{ReplicatedWalEntry, ReplicationManager, ReplicationStats};
pub use state::{AtomicRole, AtomicState, HaState};

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::{HaConfig, HaRole};

/// High Availability Manager
pub struct HaManager {
    /// Configuration
    config: HaConfig,
    /// Current role
    role: Arc<AtomicRole>,
    /// Current state
    state: Arc<AtomicState>,
    /// Replication manager
    replication: Option<ReplicationManager>,
    /// Failover manager
    failover: Option<FailoverManager>,
    /// Channel to receive entries for replication (master)
    entry_rx: Option<mpsc::Receiver<ReplicatedWalEntry>>,
}

impl HaManager {
    /// Create a new HA manager
    pub fn new(config: &HaConfig) -> Result<Self, String> {
        if !config.enabled {
            return Ok(Self {
                config: config.clone(),
                role: Arc::new(AtomicRole::new(HaRole::Master)),
                state: Arc::new(AtomicState::new(HaState::Running)),
                replication: None,
                failover: None,
                entry_rx: None,
            });
        }

        let role = Arc::new(AtomicRole::new(config.role));
        let state = Arc::new(AtomicState::new(HaState::Initializing));

        let mut replication = ReplicationManager::new(config)?;
        let entry_rx = replication.take_entry_receiver();

        let receiver = replication.receiver();
        let failover = FailoverManager::new(config, role.clone(), state.clone(), receiver);

        Ok(Self {
            config: config.clone(),
            role,
            state,
            replication: Some(replication),
            failover: Some(failover),
            entry_rx,
        })
    }

    /// Check if HA is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get current role
    pub fn role(&self) -> HaRole {
        self.role.load()
    }

    /// Check if this node is master
    pub fn is_master(&self) -> bool {
        self.role.is_master()
    }

    /// Check if this node is slave
    pub fn is_slave(&self) -> bool {
        self.role.is_slave()
    }

    /// Get current state
    pub fn state(&self) -> HaState {
        self.state.load()
    }

    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get replication manager
    pub fn replication(&self) -> Option<&ReplicationManager> {
        self.replication.as_ref()
    }

    /// Get failover manager
    pub fn failover(&self) -> Option<&FailoverManager> {
        self.failover.as_ref()
    }

    /// Take the entry receiver channel (slave only, for WAL replay)
    pub fn take_entry_receiver(&mut self) -> Option<mpsc::Receiver<ReplicatedWalEntry>> {
        self.entry_rx.take()
    }

    /// Queue a write for replication (master only)
    pub async fn replicate_write(
        &self,
        db_name: &str,
        data: &[u8],
        entry_type: u8,
    ) -> Result<u64, String> {
        if !self.is_master() {
            return Err("Cannot replicate: not master".to_string());
        }

        let replication = match &self.replication {
            Some(r) => r,
            None => return Err("Replication not enabled".to_string()),
        };

        let sender = match replication.sender() {
            Some(s) => s,
            None => return Err("No sender available".to_string()),
        };

        let sequence = sender.next_sequence();
        let timestamp_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let mut entry = ReplicatedWalEntry {
            sequence,
            timestamp_nanos,
            entry_type,
            db_name: db_name.to_string(),
            payload: data.to_vec(),
            checksum: 0,
        };
        entry.checksum = entry.calculate_checksum();

        sender.replicate(entry).await?;

        Ok(sequence)
    }

    /// Start HA services
    pub async fn start(&self) -> Result<(), String> {
        if !self.config.enabled {
            info!("HA is disabled, skipping start");
            return Ok(());
        }

        info!(
            "Starting HA manager: role={:?}, node_id={}",
            self.config.role, self.config.node_id
        );

        self.state.store(HaState::Running);

        // Start replication
        if let Some(replication) = &self.replication {
            let repl = replication.clone();
            tokio::spawn(async move {
                if let Err(e) = repl.start().await {
                    error!("Replication error: {}", e);
                }
            });
        }

        // Start failover monitoring (slave only)
        if let Some(failover) = &self.failover {
            if self.is_slave() {
                let fm = failover.clone();
                tokio::spawn(async move {
                    if let Err(e) = fm.start().await {
                        error!("Failover error: {}", e);
                    }
                });
            }
        }

        Ok(())
    }

    /// Stop HA services
    pub fn stop(&self) {
        if let Some(replication) = &self.replication {
            replication.stop();
        }
        if let Some(failover) = &self.failover {
            failover.stop();
        }
    }

    /// Manually promote to master
    pub async fn promote(&self) -> PromotionResult {
        match &self.failover {
            Some(fm) => fm.manual_promote().await,
            None => PromotionResult::NotAllowed("Failover not configured".to_string()),
        }
    }

    /// Get HA status
    pub async fn status(&self) -> HaStatus {
        let replication_stats = if let Some(repl) = &self.replication {
            Some(repl.stats().await)
        } else {
            None
        };

        let promotion_info = if let Some(fm) = &self.failover {
            Some(fm.promoter().state_info())
        } else {
            None
        };

        HaStatus {
            enabled: self.config.enabled,
            node_id: self.config.node_id.clone(),
            role: self.role.load(),
            state: self.state.load(),
            replication: replication_stats,
            promotion: promotion_info,
        }
    }
}

// Implement Clone for FailoverManager and ReplicationManager for spawning
impl Clone for FailoverManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            detector: self.detector.clone(),
            promoter: self.promoter.clone(),
            master_down_rx: self.master_down_rx.clone(),
            promoted_rx: self.promoted_rx.clone(),
        }
    }
}

impl Clone for ReplicationManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            entry_rx: None, // Cannot clone receiver
        }
    }
}

/// HA Status for API response
#[derive(Debug, Clone, Serialize)]
pub struct HaStatus {
    /// Whether HA is enabled
    pub enabled: bool,
    /// Node ID
    pub node_id: String,
    /// Current role
    pub role: HaRole,
    /// Current state
    pub state: HaState,
    /// Replication statistics
    pub replication: Option<ReplicationStats>,
    /// Promotion state info
    pub promotion: Option<PromotionStateInfo>,
}

// Implement Serialize for types used in API
impl Serialize for HaState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            HaState::Initializing => "initializing",
            HaState::Running => "running",
            HaState::Syncing => "syncing",
            HaState::Promoting => "promoting",
            HaState::Demoting => "demoting",
            HaState::Failed => "failed",
        };
        serializer.serialize_str(s)
    }
}

impl Serialize for ReplicationStats {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ReplicationStats", 6)?;
        state.serialize_field("role", &self.role)?;
        state.serialize_field("current_sequence", &self.current_sequence)?;
        state.serialize_field("slave_count", &self.slave_count)?;
        state.serialize_field("connected", &self.connected)?;
        state.serialize_field("last_sequence", &self.last_sequence)?;
        state.serialize_field("seconds_since_heartbeat", &self.seconds_since_heartbeat)?;
        state.end()
    }
}

impl Serialize for PromotionStateInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PromotionStateInfo", 4)?;
        state.serialize_field("role", &self.role)?;
        state.serialize_field("state", &self.state)?;
        state.serialize_field("can_promote", &self.can_promote)?;
        state.serialize_field(
            "failover_mode",
            match self.failover_mode {
                crate::config::FailoverMode::Automatic => "automatic",
                crate::config::FailoverMode::Manual => "manual",
                crate::config::FailoverMode::Disabled => "disabled",
            },
        )?;
        state.end()
    }
}
