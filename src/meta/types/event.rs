//! Event types for the Meta service

use serde::{Deserialize, Serialize};

use super::{current_timestamp_ms, keys};

/// Event types for the event log
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventType {
    Unknown = 0,
    NodeJoined = 1,
    NodeLeft = 2,
    NodeStateChanged = 3,
    // Note: IDs 4-5 were previously used for file-level events (FilesRegistered, FilesRemoved)
    // Note: IDs 9-12 were previously used for partition-level replica events
    // (ReplicaAssigned, ReplicaPromoted, ReplicaStateChanged, DataNodeModeChanged)
    // These have been removed as part of the table-level routing migration.
    LeaderChanged = 6,
    CacheInvalidate = 7,
    SchemaChanged = 8,
    /// Standby needs to flush data for crashed Primary (Recovery)
    ///
    /// Published when a Primary crashes and its Standby needs to flush replicated data.
    /// Payload: {"standby_id": "...", "crashed_primary_id": "..."}
    FlushForRecovery = 13,

    /// Table ownership added (for rerouted writes)
    ///
    /// Published when a secondary owner is added to a table.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "added_owner": "...", "all_owners": [...]}
    TableOwnerAdded = 14,

    /// Table ownership removed
    ///
    /// Published when an owner is removed from a table.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "removed_owner": "...", "all_owners": [...]}
    TableOwnerRemoved = 15,

    /// Table primary owner reassigned
    ///
    /// Published when a table's primary owner changes.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "old_primary": "...", "new_primary": "...", "all_owners": [...]}
    TableReassigned = 16,

    /// Distribution config changed
    ///
    /// Published when write distribution config is set or deleted.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "percent": N, "deleted": bool}
    DistributionConfigChanged = 17,

    /// Legacy prefix added to table during scale-in
    ///
    /// Published when a node is retired and its prefix is recorded in TableMeta.
    /// This allows queries to continue finding data under the old prefix.
    /// Payload: {"catalog": "...", "schema": "...", "table": "...", "legacy_prefix": "..."}
    TableLegacyPrefixAdded = 18,
}

impl From<i32> for EventType {
    fn from(v: i32) -> Self {
        match v {
            1 => EventType::NodeJoined,
            2 => EventType::NodeLeft,
            3 => EventType::NodeStateChanged,
            // 4-5 reserved (formerly file events, now removed)
            6 => EventType::LeaderChanged,
            7 => EventType::CacheInvalidate,
            8 => EventType::SchemaChanged,
            // 9-12 reserved (formerly replica events, now removed)
            13 => EventType::FlushForRecovery,
            14 => EventType::TableOwnerAdded,
            15 => EventType::TableOwnerRemoved,
            16 => EventType::TableReassigned,
            17 => EventType::DistributionConfigChanged,
            18 => EventType::TableLegacyPrefixAdded,
            _ => EventType::Unknown,
        }
    }
}

impl From<EventType> for i32 {
    fn from(et: EventType) -> Self {
        et as i32
    }
}

/// Event in the event log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub sequence: u64,
    pub event_type: EventType,
    pub topic: String,
    pub payload: Vec<u8>,
    pub timestamp: i64,
    pub source_node: String,
}

impl Event {
    pub fn new(
        sequence: u64,
        event_type: EventType,
        topic: String,
        payload: Vec<u8>,
        source_node: String,
    ) -> Self {
        Self {
            sequence,
            event_type,
            topic,
            payload,
            timestamp: current_timestamp_ms(),
            source_node,
        }
    }

    pub fn storage_key(&self) -> String {
        // Use zero-padded sequence for proper ordering
        format!("{}{:020}", keys::PREFIX_EVENT, self.sequence)
    }
}
