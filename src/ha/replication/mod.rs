//! Replication module for HA
//!
//! Handles WAL replication between master and slave nodes.

pub mod protocol;
pub mod receiver;
pub mod sender;

pub use protocol::*;
pub use receiver::{ReceiverState, ReplicationReceiver};
pub use sender::ReplicationSender;

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::config::{HaConfig, HaRole};

/// Replication manager that handles both sending (master) and receiving (slave)
pub struct ReplicationManager {
    /// Configuration
    pub(crate) config: HaConfig,
    /// Sender for master role
    pub(crate) sender: Option<Arc<ReplicationSender>>,
    /// Receiver for slave role
    pub(crate) receiver: Option<Arc<ReplicationReceiver>>,
    /// Channel for received entries (slave side)
    pub(crate) entry_rx: Option<mpsc::Receiver<ReplicatedWalEntry>>,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(config: &HaConfig) -> Result<Self, String> {
        let (sender, receiver, entry_rx) = match config.role {
            HaRole::Master => {
                let sender = ReplicationSender::new(config);
                (Some(Arc::new(sender)), None, None)
            }
            HaRole::Slave => {
                let (tx, rx) = mpsc::channel(10000);
                let receiver = ReplicationReceiver::new(config, tx)?;
                (None, Some(Arc::new(receiver)), Some(rx))
            }
        };

        Ok(Self {
            config: config.clone(),
            sender,
            receiver,
            entry_rx,
        })
    }

    /// Get the role
    pub fn role(&self) -> HaRole {
        self.config.role
    }

    /// Check if this is master
    pub fn is_master(&self) -> bool {
        self.config.role == HaRole::Master
    }

    /// Check if this is slave
    pub fn is_slave(&self) -> bool {
        self.config.role == HaRole::Slave
    }

    /// Get sender (master only)
    pub fn sender(&self) -> Option<Arc<ReplicationSender>> {
        self.sender.clone()
    }

    /// Get receiver (slave only)
    pub fn receiver(&self) -> Option<Arc<ReplicationReceiver>> {
        self.receiver.clone()
    }

    /// Take the entry receiver channel (slave only, can only be called once)
    pub fn take_entry_receiver(&mut self) -> Option<mpsc::Receiver<ReplicatedWalEntry>> {
        self.entry_rx.take()
    }

    /// Start replication
    pub async fn start(&self) -> Result<(), String> {
        match self.config.role {
            HaRole::Master => {
                if let Some(sender) = &self.sender {
                    let listen_addr = self.config.replication.listen_addr.clone();
                    sender.serve(&listen_addr).await?;
                }
            }
            HaRole::Slave => {
                if let Some(receiver) = &self.receiver {
                    receiver.start().await?;
                }
            }
        }
        Ok(())
    }

    /// Stop replication
    pub fn stop(&self) {
        if let Some(receiver) = &self.receiver {
            receiver.stop();
        }
        // Sender stops when connections are dropped
    }

    /// Get replication statistics
    pub async fn stats(&self) -> ReplicationStats {
        match self.config.role {
            HaRole::Master => {
                if let Some(sender) = &self.sender {
                    ReplicationStats {
                        role: HaRole::Master,
                        current_sequence: sender.current_sequence(),
                        slave_count: Some(sender.slave_count().await),
                        connected: true,
                        last_sequence: sender.current_sequence(),
                        seconds_since_heartbeat: None,
                    }
                } else {
                    ReplicationStats::default()
                }
            }
            HaRole::Slave => {
                if let Some(receiver) = &self.receiver {
                    let state = receiver.state().await;
                    ReplicationStats {
                        role: HaRole::Slave,
                        current_sequence: receiver.last_sequence(),
                        slave_count: None,
                        connected: state == ReceiverState::Connected,
                        last_sequence: receiver.last_sequence(),
                        seconds_since_heartbeat: Some(receiver.seconds_since_heartbeat()),
                    }
                } else {
                    ReplicationStats::default()
                }
            }
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone, Default)]
pub struct ReplicationStats {
    /// Current role
    pub role: HaRole,
    /// Current sequence number
    pub current_sequence: u64,
    /// Number of connected slaves (master only)
    pub slave_count: Option<usize>,
    /// Whether connected to master (slave) or has slaves (master)
    pub connected: bool,
    /// Last sequence number
    pub last_sequence: u64,
    /// Seconds since last heartbeat (slave only)
    pub seconds_since_heartbeat: Option<u64>,
}

