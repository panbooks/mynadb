//! Replication sender (master side)
//!
//! Handles accepting slave connections and streaming WAL entries.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::config::HaConfig;

use super::protocol::*;

/// Connected slave information
#[derive(Debug)]
struct SlaveConnection {
    /// Slave node ID
    node_id: String,
    /// Last acknowledged sequence
    last_ack_sequence: AtomicU64,
    /// Sender channel to the slave handler task
    tx: mpsc::Sender<ReplicatedWalEntry>,
}

/// Replication sender for master node
pub struct ReplicationSender {
    /// Configuration
    config: HaConfig,
    /// Node ID
    node_id: String,
    /// Current sequence number
    sequence: Arc<AtomicU64>,
    /// Connected slaves
    slaves: Arc<RwLock<HashMap<String, Arc<SlaveConnection>>>>,
    /// Pending ACKs for sync replication (sequence -> oneshot sender)
    pending_acks: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    /// Channel to receive entries to replicate
    entry_rx: Arc<Mutex<Option<mpsc::Receiver<ReplicatedWalEntry>>>>,
    /// Channel to send entries for replication
    entry_tx: mpsc::Sender<ReplicatedWalEntry>,
}

impl ReplicationSender {
    /// Create a new replication sender
    pub fn new(config: &HaConfig) -> Self {
        let (entry_tx, entry_rx) = mpsc::channel(10000);

        Self {
            config: config.clone(),
            node_id: config.node_id.clone(),
            sequence: Arc::new(AtomicU64::new(0)),
            slaves: Arc::new(RwLock::new(HashMap::new())),
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            entry_rx: Arc::new(Mutex::new(Some(entry_rx))),
            entry_tx,
        }
    }

    /// Get the next sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get channel to send entries for replication
    pub fn get_entry_sender(&self) -> mpsc::Sender<ReplicatedWalEntry> {
        self.entry_tx.clone()
    }

    /// Queue an entry for replication
    pub async fn replicate(&self, entry: ReplicatedWalEntry) -> Result<(), String> {
        self.entry_tx
            .send(entry)
            .await
            .map_err(|e| format!("Failed to queue entry for replication: {}", e))
    }

    /// Queue an entry and wait for ACK (sync mode)
    pub async fn replicate_and_wait(
        &self,
        entry: ReplicatedWalEntry,
        timeout: Duration,
    ) -> Result<(), String> {
        let sequence = entry.sequence;

        // Create oneshot channel for ACK
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_acks.lock().await;
            pending.insert(sequence, tx);
        }

        // Send entry
        self.replicate(entry).await?;

        // Wait for ACK with timeout
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err("Slave reported error applying entry".to_string()),
            Ok(Err(_)) => Err("ACK channel closed".to_string()),
            Err(_) => {
                // Remove pending ACK on timeout
                let mut pending = self.pending_acks.lock().await;
                pending.remove(&sequence);
                Err("Replication timeout".to_string())
            }
        }
    }

    /// Start the replication server
    pub async fn serve(&self, addr: &str) -> Result<(), String> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("Failed to bind replication server: {}", e))?;

        info!("Replication server listening on {}", addr);

        // Start entry broadcaster task
        let slaves = self.slaves.clone();
        let entry_rx = self.entry_rx.lock().await.take();
        if let Some(mut rx) = entry_rx {
            tokio::spawn(async move {
                while let Some(entry) = rx.recv().await {
                    let slaves = slaves.read().await;
                    for (node_id, slave) in slaves.iter() {
                        if let Err(e) = slave.tx.send(entry.clone()).await {
                            warn!("Failed to send entry to slave {}: {}", node_id, e);
                        }
                    }
                }
            });
        }

        // Start heartbeat task
        let slaves_hb = self.slaves.clone();
        let node_id_hb = self.node_id.clone();
        let sequence_hb = self.sequence.clone();
        let interval = Duration::from_secs(self.config.heartbeat.interval_secs);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let heartbeat = HeartbeatMessage {
                    node_id: node_id_hb.clone(),
                    last_sequence: sequence_hb.load(Ordering::SeqCst),
                    timestamp_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                };

                // Heartbeats are sent through a separate mechanism
                // The slave handler tasks will periodically check for heartbeats
                debug!(
                    "Heartbeat: sequence={}, slaves={}",
                    heartbeat.last_sequence,
                    slaves_hb.read().await.len()
                );
            }
        });

        // Accept connections
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("New slave connection from {}", addr);
                    let slaves = self.slaves.clone();
                    let pending_acks = self.pending_acks.clone();
                    let node_id = self.node_id.clone();
                    let current_seq = self.current_sequence();
                    let heartbeat_interval = Duration::from_secs(self.config.heartbeat.interval_secs);

                    tokio::spawn(async move {
                        if let Err(e) = handle_slave_connection(
                            socket,
                            slaves,
                            pending_acks,
                            node_id,
                            current_seq,
                            heartbeat_interval,
                        )
                        .await
                        {
                            error!("Slave connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Get connected slaves count
    pub async fn slave_count(&self) -> usize {
        self.slaves.read().await.len()
    }

    /// Check if any slave is connected
    pub async fn has_slaves(&self) -> bool {
        !self.slaves.read().await.is_empty()
    }

    /// Get replication lag for a slave (in sequence numbers)
    pub async fn get_slave_lag(&self, node_id: &str) -> Option<u64> {
        let slaves = self.slaves.read().await;
        slaves.get(node_id).map(|slave| {
            let current = self.current_sequence();
            let acked = slave.last_ack_sequence.load(Ordering::SeqCst);
            current.saturating_sub(acked)
        })
    }
}

/// Handle a slave connection
async fn handle_slave_connection(
    mut socket: TcpStream,
    slaves: Arc<RwLock<HashMap<String, Arc<SlaveConnection>>>>,
    pending_acks: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    master_node_id: String,
    current_sequence: u64,
    heartbeat_interval: Duration,
) -> Result<(), String> {
    // Read handshake
    let handshake: HandshakeRequest = read_message(&mut socket).await?;
    info!(
        "Slave {} requesting connection, last_sequence={}",
        handshake.node_id, handshake.last_sequence
    );

    // Send handshake response
    let response = HandshakeResponse {
        node_id: master_node_id.clone(),
        accepted: true,
        current_sequence,
        error: None,
    };
    write_message(&mut socket, MessageType::HandshakeResponse, &response).await?;

    // Create channel for this slave
    let (tx, mut rx) = mpsc::channel::<ReplicatedWalEntry>(1000);

    let slave_conn = Arc::new(SlaveConnection {
        node_id: handshake.node_id.clone(),
        last_ack_sequence: AtomicU64::new(handshake.last_sequence),
        tx,
    });

    // Register slave
    {
        let mut slaves_guard = slaves.write().await;
        slaves_guard.insert(handshake.node_id.clone(), slave_conn.clone());
    }

    let slave_node_id = handshake.node_id.clone();
    info!("Slave {} connected and registered", slave_node_id);

    // Split socket for concurrent read/write
    let (mut reader, mut writer) = socket.into_split();

    // Spawn writer task
    let writer_slave_id = slave_node_id.clone();
    let writer_master_id = master_node_id.clone();
    let writer_handle = tokio::spawn(async move {
        let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);

        loop {
            tokio::select! {
                // Send WAL entries
                entry = rx.recv() => {
                    match entry {
                        Some(entry) => {
                            if let Err(e) = write_message_to_writer(&mut writer, MessageType::WalEntry, &entry).await {
                                error!("Failed to send entry to slave {}: {}", writer_slave_id, e);
                                break;
                            }
                        }
                        None => {
                            info!("Entry channel closed for slave {}", writer_slave_id);
                            break;
                        }
                    }
                }
                // Send heartbeats
                _ = heartbeat_ticker.tick() => {
                    let heartbeat = HeartbeatMessage {
                        node_id: writer_master_id.clone(),
                        last_sequence: 0, // Will be filled by actual state
                        timestamp_nanos: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64,
                    };
                    if let Err(e) = write_message_to_writer(&mut writer, MessageType::Heartbeat, &heartbeat).await {
                        error!("Failed to send heartbeat to slave {}: {}", writer_slave_id, e);
                        break;
                    }
                }
            }
        }
    });

    // Reader task - handle ACKs
    let reader_slave_id = slave_node_id.clone();
    let reader_handle = tokio::spawn(async move {
        loop {
            match read_message_from_reader::<AckMessage>(&mut reader).await {
                Ok(ack) => {
                    debug!(
                        "Received ACK from slave {} for sequence {}",
                        reader_slave_id, ack.sequence
                    );

                    // Update last ack sequence
                    slave_conn
                        .last_ack_sequence
                        .store(ack.sequence, Ordering::SeqCst);

                    // Notify pending sync waiter
                    let mut pending = pending_acks.lock().await;
                    if let Some(tx) = pending.remove(&ack.sequence) {
                        let _ = tx.send(ack.success);
                    }
                }
                Err(e) => {
                    error!("Failed to read from slave {}: {}", reader_slave_id, e);
                    break;
                }
            }
        }
    });

    // Wait for either task to complete (connection closed)
    tokio::select! {
        _ = writer_handle => {}
        _ = reader_handle => {}
    }

    // Unregister slave
    {
        let mut slaves_guard = slaves.write().await;
        slaves_guard.remove(&slave_node_id);
    }
    info!("Slave {} disconnected", slave_node_id);

    Ok(())
}

/// Read a framed message from socket
async fn read_message<T: for<'de> serde::Deserialize<'de>>(
    socket: &mut TcpStream,
) -> Result<T, String> {
    // Read length prefix (4 bytes)
    let mut len_buf = [0u8; 4];
    socket
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("Failed to read message length: {}", e))?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(format!("Message too large: {} bytes", len));
    }

    // Read message body
    let mut buf = vec![0u8; len];
    socket
        .read_exact(&mut buf)
        .await
        .map_err(|e| format!("Failed to read message body: {}", e))?;

    // Decode wire message
    let wire = WireMessage::decode(&buf)?;
    wire.decode_payload()
}

/// Read a message from reader half
async fn read_message_from_reader<T: for<'de> serde::Deserialize<'de>>(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<T, String> {
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

    let wire = WireMessage::decode(&buf)?;
    wire.decode_payload()
}

/// Write a framed message to socket
async fn write_message<T: serde::Serialize>(
    socket: &mut TcpStream,
    msg_type: MessageType,
    data: &T,
) -> Result<(), String> {
    let wire = WireMessage::new(msg_type, data)?;
    let bytes = wire.encode()?;

    // Write length prefix
    let len = bytes.len() as u32;
    socket
        .write_all(&len.to_le_bytes())
        .await
        .map_err(|e| format!("Failed to write message length: {}", e))?;

    // Write message body
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
