//! Replication protocol messages
//!
//! Binary protocol for master-slave communication using bincode serialization.

use serde::{Deserialize, Serialize};

/// Message type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    /// Handshake request from slave
    Handshake = 0,
    /// Handshake response from master
    HandshakeResponse = 1,
    /// WAL entry from master to slave
    WalEntry = 2,
    /// Acknowledgment from slave
    Ack = 3,
    /// Heartbeat from master
    Heartbeat = 4,
    /// Heartbeat response from slave
    HeartbeatResponse = 5,
    /// File notification (new parquet file)
    FileNotify = 6,
    /// Request file content
    FileRequest = 7,
    /// File content chunk
    FileChunk = 8,
}

/// Handshake request from slave to master
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    /// Slave node ID
    pub node_id: String,
    /// Last received sequence number (for resuming)
    pub last_sequence: u64,
    /// Protocol version
    pub protocol_version: u32,
}

/// Handshake response from master to slave
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    /// Master node ID
    pub node_id: String,
    /// Whether handshake was accepted
    pub accepted: bool,
    /// Current sequence number on master
    pub current_sequence: u64,
    /// Error message if not accepted
    pub error: Option<String>,
}

/// WAL entry for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedWalEntry {
    /// Global sequence number for ordering
    pub sequence: u64,
    /// Timestamp in nanoseconds
    pub timestamp_nanos: u64,
    /// Entry type (0 = DataWrite, 1 = SeriesTrack)
    pub entry_type: u8,
    /// Database name
    pub db_name: String,
    /// Payload (line protocol data)
    pub payload: Vec<u8>,
    /// CRC32 checksum
    pub checksum: u32,
}

impl ReplicatedWalEntry {
    /// Calculate CRC32 checksum for the entry
    pub fn calculate_checksum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.sequence.to_le_bytes());
        hasher.update(&self.timestamp_nanos.to_le_bytes());
        hasher.update(&[self.entry_type]);
        hasher.update(self.db_name.as_bytes());
        hasher.update(&self.payload);
        hasher.finalize()
    }

    /// Verify checksum
    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

/// Acknowledgment from slave
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMessage {
    /// Acknowledged sequence number
    pub sequence: u64,
    /// Node ID of the acknowledging slave
    pub node_id: String,
    /// Whether the entry was successfully applied
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}

/// Heartbeat from master
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// Master node ID
    pub node_id: String,
    /// Current sequence number
    pub last_sequence: u64,
    /// Timestamp in nanoseconds
    pub timestamp_nanos: u64,
}

/// Heartbeat response from slave
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponseMessage {
    /// Slave node ID
    pub node_id: String,
    /// Last received sequence number
    pub last_sequence: u64,
    /// Timestamp in nanoseconds
    pub timestamp_nanos: u64,
}

/// File notification (new parquet file created)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNotifyMessage {
    /// Relative path to the file
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// CRC32 checksum of the file
    pub checksum: u32,
}

/// Request file content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRequestMessage {
    /// Relative path to the file
    pub path: String,
}

/// File content chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChunkMessage {
    /// Relative path to the file
    pub path: String,
    /// Offset in the file
    pub offset: u64,
    /// Chunk data
    pub data: Vec<u8>,
    /// Whether this is the last chunk
    pub is_last: bool,
    /// CRC32 checksum (only on last chunk)
    pub checksum: Option<u32>,
}

/// Wire message format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    /// Message type
    pub msg_type: MessageType,
    /// Serialized payload
    pub payload: Vec<u8>,
}

impl WireMessage {
    /// Create a new wire message
    pub fn new<T: Serialize>(msg_type: MessageType, data: &T) -> Result<Self, String> {
        let payload =
            bincode::serialize(data).map_err(|e| format!("Failed to serialize: {}", e))?;
        Ok(Self { msg_type, payload })
    }

    /// Encode to bytes for transmission
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(self).map_err(|e| format!("Failed to encode wire message: {}", e))
    }

    /// Decode from bytes
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        bincode::deserialize(data).map_err(|e| format!("Failed to decode wire message: {}", e))
    }

    /// Decode the payload to a specific type
    pub fn decode_payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T, String> {
        bincode::deserialize(&self.payload)
            .map_err(|e| format!("Failed to decode payload: {}", e))
    }
}

/// Protocol version
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum message size (16 MB)
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Frame header size (4 bytes for length)
pub const FRAME_HEADER_SIZE: usize = 4;
