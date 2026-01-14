//! Slave promotion logic
//!
//! Handles promoting a slave to master role.

use std::sync::Arc;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::{FailoverMode, HaConfig, HaRole};
use crate::ha::state::{AtomicRole, AtomicState, HaState};

/// Promotion result
#[derive(Debug, Clone)]
pub enum PromotionResult {
    /// Successfully promoted to master
    Success,
    /// Promotion in progress
    InProgress,
    /// Promotion failed
    Failed(String),
    /// Already master
    AlreadyMaster,
    /// Promotion not allowed (wrong mode or state)
    NotAllowed(String),
}

/// Slave promoter
pub struct Promoter {
    /// Configuration
    config: HaConfig,
    /// Current role
    role: Arc<AtomicRole>,
    /// Current state
    state: Arc<AtomicState>,
    /// Promotion complete notification
    promoted_tx: watch::Sender<bool>,
}

impl Promoter {
    /// Create a new promoter
    pub fn new(
        config: &HaConfig,
        role: Arc<AtomicRole>,
        state: Arc<AtomicState>,
    ) -> (Self, watch::Receiver<bool>) {
        let (promoted_tx, promoted_rx) = watch::channel(false);

        (
            Self {
                config: config.clone(),
                role,
                state,
                promoted_tx,
            },
            promoted_rx,
        )
    }

    /// Check if promotion is allowed
    pub fn can_promote(&self) -> bool {
        // Must be slave
        if self.role.is_master() {
            return false;
        }

        // Must be in Running or Syncing state
        let state = self.state.load();
        matches!(state, HaState::Running | HaState::Syncing)
    }

    /// Promote this node to master
    pub async fn promote(&self) -> PromotionResult {
        info!("Starting promotion to master");

        // Check if already master
        if self.role.is_master() {
            warn!("Already master, skipping promotion");
            return PromotionResult::AlreadyMaster;
        }

        // Check state
        let current_state = self.state.load();
        if !matches!(current_state, HaState::Running | HaState::Syncing) {
            return PromotionResult::NotAllowed(format!(
                "Cannot promote in state {:?}",
                current_state
            ));
        }

        // Set state to Promoting
        self.state.store(HaState::Promoting);

        // Promotion steps:
        // 1. Stop receiving from master
        // 2. Flush any pending writes
        // 3. Switch role to master
        // 4. Start accepting writes
        // 5. Optionally start replication server for new slaves

        // In a real implementation, we would:
        // - Ensure all received WAL entries are applied
        // - Close connection to old master
        // - Update configuration
        // - Restart replication as master

        // For now, just switch the role
        self.role.store(HaRole::Master);
        self.state.store(HaState::Running);

        info!("Promotion complete, now running as master");

        // Notify listeners
        let _ = self.promoted_tx.send(true);

        PromotionResult::Success
    }

    /// Demote this node to slave (for manual failback)
    pub async fn demote(&self, master_addr: &str) -> Result<(), String> {
        info!("Starting demotion to slave, new master: {}", master_addr);

        // Check if already slave
        if self.role.is_slave() {
            warn!("Already slave, skipping demotion");
            return Ok(());
        }

        // Set state to Demoting
        self.state.store(HaState::Demoting);

        // Demotion steps:
        // 1. Stop accepting writes
        // 2. Flush pending data
        // 3. Switch role to slave
        // 4. Connect to new master

        self.role.store(HaRole::Slave);
        self.state.store(HaState::Syncing);

        info!("Demotion complete, now syncing as slave from {}", master_addr);

        Ok(())
    }

    /// Get promotion state info
    pub fn state_info(&self) -> PromotionStateInfo {
        PromotionStateInfo {
            role: self.role.load(),
            state: self.state.load(),
            can_promote: self.can_promote(),
            failover_mode: self.config.failover.mode,
        }
    }
}

/// Promotion state information
#[derive(Debug, Clone)]
pub struct PromotionStateInfo {
    /// Current role
    pub role: HaRole,
    /// Current HA state
    pub state: HaState,
    /// Whether promotion is allowed
    pub can_promote: bool,
    /// Configured failover mode
    pub failover_mode: FailoverMode,
}
