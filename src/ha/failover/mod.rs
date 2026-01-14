//! Failover module for HA
//!
//! Handles failure detection and promotion logic.

pub mod detector;
pub mod promoter;

pub use detector::{DetectorState, FailureDetector};
pub use promoter::{PromotionResult, PromotionStateInfo, Promoter};

use std::sync::Arc;

use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{FailoverMode, HaConfig, HaRole};
use crate::ha::replication::ReplicationReceiver;
use crate::ha::state::{AtomicRole, AtomicState};

/// Failover manager that coordinates detection and promotion
pub struct FailoverManager {
    /// Configuration
    pub(crate) config: HaConfig,
    /// Failure detector (slave only)
    pub(crate) detector: Option<Arc<FailureDetector>>,
    /// Promoter
    pub(crate) promoter: Arc<Promoter>,
    /// Master down notification receiver
    pub(crate) master_down_rx: Option<watch::Receiver<bool>>,
    /// Promoted notification receiver
    pub(crate) promoted_rx: watch::Receiver<bool>,
}

impl FailoverManager {
    /// Create a new failover manager
    pub fn new(
        config: &HaConfig,
        role: Arc<AtomicRole>,
        state: Arc<AtomicState>,
        receiver: Option<Arc<ReplicationReceiver>>,
    ) -> Self {
        let (detector, master_down_rx) = if let Some(recv) = receiver {
            let (det, rx) = FailureDetector::new(config, recv);
            (Some(Arc::new(det)), Some(rx))
        } else {
            (None, None)
        };

        let (promoter, promoted_rx) = Promoter::new(config, role, state);

        Self {
            config: config.clone(),
            detector,
            promoter: Arc::new(promoter),
            master_down_rx,
            promoted_rx,
        }
    }

    /// Get the promoter
    pub fn promoter(&self) -> Arc<Promoter> {
        self.promoter.clone()
    }

    /// Get the detector
    pub fn detector(&self) -> Option<Arc<FailureDetector>> {
        self.detector.clone()
    }

    /// Start failover monitoring (slave only, automatic mode)
    pub async fn start(&self) -> Result<(), String> {
        // Only run on slave with automatic failover
        if self.config.role != HaRole::Slave {
            info!("Failover manager not starting: not a slave");
            return Ok(());
        }

        if self.config.failover.mode != FailoverMode::Automatic {
            info!(
                "Failover manager not starting automatic failover: mode={:?}",
                self.config.failover.mode
            );
            return Ok(());
        }

        let detector = match &self.detector {
            Some(d) => d.clone(),
            None => return Err("No detector configured".to_string()),
        };

        let mut master_down_rx = match &self.master_down_rx {
            Some(rx) => rx.clone(),
            None => return Err("No master_down receiver".to_string()),
        };

        let promoter = self.promoter.clone();

        // Spawn automatic failover task
        tokio::spawn(async move {
            loop {
                if master_down_rx.changed().await.is_err() {
                    break;
                }

                if *master_down_rx.borrow() {
                    info!("Master down detected, initiating automatic failover");

                    match promoter.promote().await {
                        PromotionResult::Success => {
                            info!("Automatic failover complete: promoted to master");
                            break; // Exit failover loop after promotion
                        }
                        PromotionResult::AlreadyMaster => {
                            warn!("Already master during failover");
                            break;
                        }
                        PromotionResult::InProgress => {
                            info!("Promotion already in progress");
                        }
                        PromotionResult::Failed(e) => {
                            error!("Automatic failover failed: {}", e);
                        }
                        PromotionResult::NotAllowed(reason) => {
                            warn!("Automatic failover not allowed: {}", reason);
                        }
                    }
                }
            }
        });

        // Start failure detector
        detector.start().await
    }

    /// Manually trigger promotion
    pub async fn manual_promote(&self) -> PromotionResult {
        if self.config.failover.mode == FailoverMode::Disabled {
            return PromotionResult::NotAllowed("Failover is disabled".to_string());
        }

        self.promoter.promote().await
    }

    /// Stop failover monitoring
    pub fn stop(&self) {
        if let Some(detector) = &self.detector {
            detector.stop();
        }
    }

    /// Get subscription for promotion events
    pub fn promoted_receiver(&self) -> watch::Receiver<bool> {
        self.promoted_rx.clone()
    }
}
