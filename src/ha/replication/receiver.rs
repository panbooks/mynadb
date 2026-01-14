//! Replication receiver (slave side)
//!
//! Handles connecting to master and receiving WAL entries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, error, info, warn};

use crate::config::HaConfig;

use super::protocol::*;

/// Replication receiver state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiverState {
    /// Not connected
    Disconnected,
    /// Connecting to master
    Connecting,
    /// Connected and receiving
    Connected,
    /// Connection lost, will retry
    Reconnecting,
    /// Stopped
    Stopped,
}

/// Replication receiver for slave node
pub struct ReplicationReceiver {
    /// Configuration
    config: HaConfig,
    /// Node ID
    node_id: String,
    /// Master address
    master_addr: String,
    /// Last received sequence number
    last_sequence: AtomicU64,
    /// Current state
    state: Arc<RwLock<ReceiverState>>,
    /// Channel to send received entries for processing
    entry_tx: mpsc::Sender<ReplicatedWalEntry>,
    /// Shutdown signal
    shutdown_tx: watch::Sender<bool>,
    /// Last heartbeat timestamp
    last_heartbeat: Arc<AtomicU64>,
}

impl ReplicationReceiver {
    /// Create a new replication receiver
    pub fn new(
        config: &HaConfig,
        entry_tx: mpsc::Sender<ReplicatedWalEntry>,
    ) -> Result<Self, String> {
        let master_addr = config
            .replication
            .master_addr
            .clone()
            .ok_or_else(|| "Master address not configured for slave".to_string())?;

        let (shutdown_tx, _) = watch::channel(false);

        Ok(Self {
            config: config.clone(),
            node_id: config.node_id.clone(),
            master_addr,
            last_sequence: AtomicU64::new(0),
            state: Arc::new(RwLock::new(ReceiverState::Disconnected)),
            entry_tx,
            shutdown_tx,
            last_heartbeat: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Get current state
    pub async fn state(&self) -> ReceiverState {
        *self.state.read().await
    }

    /// Get last received sequence
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    /// Set initial sequence (for recovery)
    pub fn set_initial_sequence(&self, seq: u64) {
        self.last_sequence.store(seq, Ordering::SeqCst);
    }

    /// Get seconds since last heartbeat
    pub fn seconds_since_heartbeat(&self) -> u64 {
        let last = self.last_heartbeat.load(Ordering::SeqCst);
        if last == 0 {
            return u64::MAX;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(last)
    }

    /// Start receiving from master
    pub async fn start(&self) -> Result<(), String> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let reconnect_delay = Duration::from_secs(self.config.replication.reconnect_interval_secs);

        loop {
            // Check for shutdown
            if *shutdown_rx.borrow() {
                info!("Receiver shutdown requested");
                break;
            }

            // Update state
            {
                let mut state = self.state.write().await;
                *state = ReceiverState::Connecting;
            }

            info!("Connecting to master at {}", self.master_addr);

            match self.connect_and_receive().await {
                Ok(()) => {
                    info!("Connection closed normally");
                }
                Err(e) => {
                    error!("Connection error: {}", e);
                }
            }

            // Check for shutdown again
            if *shutdown_rx.borrow() {
                break;
            }

            // Update state and wait before reconnecting
            {
                let mut state = self.state.write().await;
                *state = ReceiverState::Reconnecting;
            }

            info!("Reconnecting in {} seconds...", reconnect_delay.as_secs());

            tokio::select! {
                _ = tokio::time::sleep(reconnect_delay) => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }

        {
            let mut state = self.state.write().await;
            *state = ReceiverState::Stopped;
        }

        Ok(())
    }

    /// Connect to master and receive entries
    async fn connect_and_receive(&self) -> Result<(), String> {
        // Connect to master
        let mut socket = TcpStream::connect(&self.master_addr)
            .await
            .map_err(|e| format!("Failed to connect to master: {}", e))?;

        info!("Connected to master at {}", self.master_addr);

        // Send handshake
        let handshake = HandshakeRequest {
            node_id: self.node_id.clone(),
            last_sequence: self.last_sequence.load(Ordering::SeqCst),
            protocol_version: PROTOCOL_VERSION,
        };
        write_message(&mut socket, MessageType::Handshake, &handshake).await?;

        // Read handshake response
        let response: HandshakeResponse = read_message(&mut socket).await?;

        if !response.accepted {
            return Err(format!(
                "Handshake rejected: {}",
                response.error.unwrap_or_else(|| "Unknown error".to_string())
            ));
        }

        info!(
            "Handshake accepted by master {}, current_sequence={}",
            response.node_id, response.current_sequence
        );

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ReceiverState::Connected;
        }

        // Update heartbeat timestamp
        self.update_heartbeat();

        // Split socket for concurrent read/write
        let (mut reader, mut writer) = socket.into_split();

        // Spawn ACK sender task
        let (ack_tx, mut ack_rx) = mpsc::channel::<AckMessage>(1000);
        let ack_handle = tokio::spawn(async move {
            while let Some(ack) = ack_rx.recv().await {
                if let Err(e) = write_message_to_writer(&mut writer, MessageType::Ack, &ack).await {
                    error!("Failed to send ACK: {}", e);
                    break;
                }
            }
        });

        // Read messages from master
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        loop {
            tokio::select! {
                result = read_wire_message(&mut reader) => {
                    match result {
                        Ok(wire) => {
                            if let Err(e) = self.handle_message(wire, &ack_tx).await {
                                error!("Error handling message: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Error reading message: {}", e);
                            break;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received");
                        break;
                    }
                }
            }
        }

        // Cleanup
        drop(ack_tx);
        let _ = ack_handle.await;

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ReceiverState::Disconnected;
        }

        Ok(())
    }

    /// Handle a received message
    async fn handle_message(
        &self,
        wire: WireMessage,
        ack_tx: &mpsc::Sender<AckMessage>,
    ) -> Result<(), String> {
        match wire.msg_type {
            MessageType::WalEntry => {
                let entry: ReplicatedWalEntry = wire.decode_payload()?;
                debug!(
                    "Received WAL entry: sequence={}, db={}",
                    entry.sequence, entry.db_name
                );

                // Verify checksum
                if !entry.verify_checksum() {
                    error!("Checksum mismatch for sequence {}", entry.sequence);
                    // Send NACK
                    let ack = AckMessage {
                        sequence: entry.sequence,
                        node_id: self.node_id.clone(),
                        success: false,
                        error: Some("Checksum mismatch".to_string()),
                    };
                    let _ = ack_tx.send(ack).await;
                    return Ok(());
                }

                let sequence = entry.sequence;

                // Send entry for processing
                if let Err(e) = self.entry_tx.send(entry).await {
                    error!("Failed to queue entry for processing: {}", e);
                    // Send NACK
                    let ack = AckMessage {
                        sequence,
                        node_id: self.node_id.clone(),
                        success: false,
                        error: Some(format!("Failed to queue: {}", e)),
                    };
                    let _ = ack_tx.send(ack).await;
                    return Ok(());
                }

                // Update last sequence
                self.last_sequence.store(sequence, Ordering::SeqCst);

                // Send ACK
                let ack = AckMessage {
                    sequence,
                    node_id: self.node_id.clone(),
                    success: true,
                    error: None,
                };
                let _ = ack_tx.send(ack).await;
            }
            MessageType::Heartbeat => {
                let heartbeat: HeartbeatMessage = wire.decode_payload()?;
                debug!(
                    "Received heartbeat from master {}, sequence={}",
                    heartbeat.node_id, heartbeat.last_sequence
                );
                self.update_heartbeat();
            }
            MessageType::FileNotify => {
                let notify: FileNotifyMessage = wire.decode_payload()?;
                info!(
                    "Received file notification: path={}, size={}",
                    notify.path, notify.size
                );
                // TODO: Implement file sync for parquet files
            }
            _ => {
                warn!("Unexpected message type: {:?}", wire.msg_type);
            }
        }

        Ok(())
    }

    /// Update heartbeat timestamp
    fn update_heartbeat(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_heartbeat.store(now, Ordering::SeqCst);
    }

    /// Stop the receiver
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Read a framed message from socket
async fn read_message<T: for<'de> serde::Deserialize<'de>>(
    socket: &mut TcpStream,
) -> Result<T, String> {
    let mut len_buf = [0u8; 4];
    socket
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("Failed to read message length: {}", e))?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(format!("Message too large: {} bytes", len));
    }

    let mut buf = vec![0u8; len];
    socket
        .read_exact(&mut buf)
        .await
        .map_err(|e| format!("Failed to read message body: {}", e))?;

    let wire = WireMessage::decode(&buf)?;
    wire.decode_payload()
}

/// Read a wire message from reader half
async fn read_wire_message(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<WireMessage, String> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("Failed to read message length: {}", e))?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(format!("Message too large: {} bytes", len));
    }

    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| format!("Failed to read message body: {}", e))?;

    WireMessage::decode(&buf)
}

/// Write a framed message to socket
async fn write_message<T: serde::Serialize>(
    socket: &mut TcpStream,
    msg_type: MessageType,
    data: &T,
) -> Result<(), String> {
    let wire = WireMessage::new(msg_type, data)?;
    let bytes = wire.encode()?;

    let len = bytes.len() as u32;
    socket
        .write_all(&len.to_le_bytes())
        .await
        .map_err(|e| format!("Failed to write message length: {}", e))?;

    socket
        .write_all(&bytes)
        .await
        .map_err(|e| format!("Failed to write message body: {}", e))?;

    Ok(())
}

/// Write a message to writer half
async fn write_message_to_writer<T: serde::Serialize>(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    msg_type: MessageType,
    data: &T,
) -> Result<(), String> {
    let wire = WireMessage::new(msg_type, data)?;
    let bytes = wire.encode()?;

    let len = bytes.len() as u32;
    writer
        .write_all(&len.to_le_bytes())
        .await
        .map_err(|e| format!("Failed to write message length: {}", e))?;

    writer
        .write_all(&bytes)
        .await
        .map_err(|e| format!("Failed to write message body: {}", e))?;

    Ok(())
}
