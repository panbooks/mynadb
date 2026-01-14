//! Heartbeat-based failure detection
//!
//! Monitors master health and triggers failover when master is unreachable.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::HaConfig;
use crate::ha::replication::ReplicationReceiver;

/// Failure detector state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectorState {
    /// Monitoring master health
    Monitoring,
    /// Master suspected to be down
    Suspected,
    /// Master confirmed down (threshold exceeded)
    MasterDown,
    /// Stopped
    Stopped,
}

/// Heartbeat-based failure detector
pub struct FailureDetector {
    /// Configuration
    config: HaConfig,
    /// Reference to replication receiver
    receiver: Arc<ReplicationReceiver>,
    /// Current consecutive failures
    failure_count: AtomicU32,
    /// Whether master is considered down
    master_down: AtomicBool,
    /// Shutdown signal
    shutdown_tx: watch::Sender<bool>,
    /// Callback channel for master down events
    master_down_tx: watch::Sender<bool>,
}

impl FailureDetector {
    /// Create a new failure detector
    pub fn new(
        config: &HaConfig,
        receiver: Arc<ReplicationReceiver>,
    ) -> (Self, watch::Receiver<bool>) {
        let (shutdown_tx, _) = watch::channel(false);
        let (master_down_tx, master_down_rx) = watch::channel(false);

        (
            Self {
                config: config.clone(),
                receiver,
                failure_count: AtomicU32::new(0),
                master_down: AtomicBool::new(false),
                shutdown_tx,
                master_down_tx,
            },
            master_down_rx,
        )
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::SeqCst)
    }

    /// Check if master is considered down
    pub fn is_master_down(&self) -> bool {
        self.master_down.load(Ordering::SeqCst)
    }

    /// Reset failure count (e.g., after successful reconnection)
    pub fn reset(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        self.master_down.store(false, Ordering::SeqCst);
        let _ = self.master_down_tx.send(false);
    }

    /// Start the failure detector
    pub async fn start(&self) -> Result<(), String> {
        let check_interval = Duration::from_secs(self.config.heartbeat.interval_secs);
        let timeout = Duration::from_secs(self.config.heartbeat.timeout_secs);
        let threshold = self.config.failover.failure_threshold;

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        info!(
            "Starting failure detector: interval={}s, timeout={}s, threshold={}",
            check_interval.as_secs(),
            timeout.as_secs(),
            threshold
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {
                    self.check_master_health(timeout.as_secs(), threshold).await;
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Failure detector shutdown requested");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Check master health based on heartbeat
    async fn check_master_health(&self, timeout_secs: u64, threshold: u32) {
        let seconds_since_hb = self.receiver.seconds_since_heartbeat();

        if seconds_since_hb > timeout_secs {
            let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
            warn!(
                "Master heartbeat timeout: {}s since last heartbeat, failure_count={}",
                seconds_since_hb, count
            );

            if count >= threshold && !self.is_master_down() {
                error!(
                    "Master failure threshold exceeded ({}/{}), triggering failover",
                    count, threshold
                );
                self.master_down.store(true, Ordering::SeqCst);
                let _ = self.master_down_tx.send(true);
            }
        } else {
            // Reset failure count on successful heartbeat
            let prev_count = self.failure_count.swap(0, Ordering::SeqCst);
            if prev_count > 0 {
                info!(
                    "Master heartbeat restored, reset failure count from {}",
                    prev_count
                );
            }

            // Reset master down if it was set
            if self.is_master_down() {
                info!("Master reconnected, clearing master_down flag");
                self.master_down.store(false, Ordering::SeqCst);
                let _ = self.master_down_tx.send(false);
            }

            debug!(
                "Master healthy: {}s since last heartbeat",
                seconds_since_hb
            );
        }
    }

    /// Stop the detector
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}
