//! State machine implementation using sled
//!
//! This module implements the Raft state machine for the Meta service using OpenRaft.
//! It manages all metadata (nodes, files, locks, leaders, events) and supports
//! snapshotting for log compaction.

use super::{Entry, LogId, NodeId, SnapshotMeta, StoredMembership, TypeConfig};
use crate::meta::types::*;
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{AnyError, OptionalSend, StorageError, StorageIOError};
use parking_lot::RwLock;
use sled::Db;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// Storage keys for Raft state
mod raft_keys {
    pub const LAST_APPLIED: &[u8] = b"/raft/sm/last_applied";
    pub const LAST_MEMBERSHIP: &[u8] = b"/raft/sm/membership";
    pub const SNAPSHOT_META: &[u8] = b"/raft/sm/snapshot_meta";
}

/// Operations module - contains impl blocks for MetaStateMachine operations
mod ops;

/// State machine backed by sled embedded database
pub struct MetaStateMachine {
    /// Sled database instance
    db: Db,

    /// Global sequence counter for events
    event_sequence: AtomicU64,

    /// Global fencing token counter for locks
    fencing_counter: AtomicU64,

    /// Global lease ID counter
    lease_counter: AtomicU64,

    /// Last applied log ID (for Raft)
    last_applied: RwLock<Option<LogId>>,

    /// Current membership configuration (for Raft)
    last_membership: RwLock<StoredMembership>,

    /// Broadcast channel for real-time event notifications
    /// Subscribers receive events instantly without polling
    event_broadcast: broadcast::Sender<Event>,

    /// Cold Standby configuration for Primary/Standby mode assignment
    standby_config: crate::meta::config::StandbyConfig,
}

/// Snapshot data for the Meta state machine
///
/// Contains a serialized representation of all state that needs
/// to be transferred when a new node joins or a node falls behind.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetaSnapshot {
    /// The log ID at which this snapshot was taken
    pub last_applied: Option<LogId>,

    /// The membership configuration at snapshot time
    pub membership: StoredMembership,

    /// Serialized state data (nodes, files, locks, etc.)
    pub data: Vec<u8>,

    /// Counter values to restore
    pub event_sequence: u64,
    pub fencing_counter: u64,
}

/// Builder for creating state machine snapshots
pub struct MetaSnapshotBuilder {
    /// Reference to the state machine
    db: Db,
    last_applied: Option<LogId>,
    membership: StoredMembership,
    event_sequence: u64,
    fencing_counter: u64,
}

impl MetaStateMachine {
    /// Create a new state machine with the given sled database and standby config
    pub fn new(db: Db, standby_config: crate::meta::config::StandbyConfig) -> Self {
        // Load counters from database
        let event_sequence = db
            .get(keys::META_SEQUENCE)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or(0u64);

        let fencing_counter = db
            .get(keys::META_FENCING)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or(0u64);

        // Load Raft state
        let last_applied = db
            .get(raft_keys::LAST_APPLIED)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok());

        let last_membership = db
            .get(raft_keys::LAST_MEMBERSHIP)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_else(StoredMembership::default);

        debug!(
            "MetaStateMachine initialized: last_applied={:?}, membership={:?}, standby_enabled={}",
            last_applied, last_membership, standby_config.enabled
        );

        // Create broadcast channel with capacity for 1000 events
        // Slow subscribers will miss events (lagging), which is acceptable
        // since they can catch up via get_events_from()
        let (event_broadcast, _) = broadcast::channel(1000);

        Self {
            db,
            event_sequence: AtomicU64::new(event_sequence),
            fencing_counter: AtomicU64::new(fencing_counter),
            lease_counter: AtomicU64::new(0),
            last_applied: RwLock::new(last_applied),
            last_membership: RwLock::new(last_membership),
            event_broadcast,
            standby_config,
        }
    }

    /// Open or create a sled database at the given path (uses default standby config)
    pub fn open(path: &std::path::Path) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self::new(db, crate::meta::config::StandbyConfig::default()))
    }

    /// Get a reference to the underlying sled database
    /// Used for direct reads that bypass the state machine
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Get the next event sequence number
    fn next_sequence(&self) -> u64 {
        let seq = self.event_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self
            .db
            .insert(keys::META_SEQUENCE, bincode::serialize(&seq).unwrap());
        seq
    }

    /// Get the next fencing token
    fn next_fencing_token(&self) -> u64 {
        let token = self.fencing_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self
            .db
            .insert(keys::META_FENCING, bincode::serialize(&token).unwrap());
        token
    }

    /// Get the next lease ID
    fn next_lease_id(&self) -> u64 {
        self.lease_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Apply a command to the state machine
    pub fn apply_command(&self, cmd: MetaCommand) -> MetaResponse {
        match cmd {
            MetaCommand::RegisterNode {
                info,
                lease_id: _,
                lease_ttl_ms,
            } => self.register_node(info, lease_ttl_ms),

            MetaCommand::Heartbeat {
                node_id,
                lease_id,
                timestamp,
            } => self.heartbeat(&node_id, lease_id, timestamp),

            MetaCommand::DeregisterNode { node_id } => self.deregister_node(&node_id),

            MetaCommand::UpdateNodeState { node_id, state } => {
                self.update_node_state(&node_id, state)
            }

            // ================================================================
            // Table-Level Operations (Recommended for routing)
            // ================================================================
            MetaCommand::RegisterTable {
                catalog,
                schema,
                table,
                owner_node_id,
            } => self.register_table(&catalog, &schema, &table, &owner_node_id),

            MetaCommand::AddTableOwner {
                catalog,
                schema,
                table,
                owner_node_id,
            } => self.add_table_owner(&catalog, &schema, &table, &owner_node_id),

            MetaCommand::RemoveTableOwner {
                catalog,
                schema,
                table,
                owner_node_id,
            } => self.remove_table_owner(&catalog, &schema, &table, &owner_node_id),

            MetaCommand::UpdateTableStats {
                catalog,
                schema,
                table,
                delta,
            } => self.update_table_stats(&catalog, &schema, &table, delta),

            MetaCommand::ReassignTable {
                catalog,
                schema,
                table,
                new_owner_node_id,
            } => self.reassign_table(&catalog, &schema, &table, &new_owner_node_id),

            MetaCommand::RemoveTable {
                catalog,
                schema,
                table,
            } => self.remove_table(&catalog, &schema, &table),

            // Distribution config operations
            MetaCommand::SetDistributionConfig {
                catalog,
                schema,
                table,
                percent,
            } => self.set_distribution_config(&catalog, &schema, &table, percent),

            MetaCommand::DeleteDistributionConfig {
                catalog,
                schema,
                table,
            } => self.delete_distribution_config(&catalog, &schema, &table),

            MetaCommand::AcquireLock {
                key,
                holder,
                ttl_ms,
            } => self.acquire_lock(&key, &holder, ttl_ms),

            MetaCommand::ReleaseLock {
                key,
                holder,
                fencing_token,
            } => self.release_lock(&key, &holder, fencing_token),

            MetaCommand::RenewLock {
                key,
                holder,
                fencing_token,
                ttl_ms,
            } => self.renew_lock(&key, &holder, fencing_token, ttl_ms),

            MetaCommand::Campaign {
                election,
                node_id,
                ttl_ms,
            } => self.campaign(&election, &node_id, ttl_ms),

            MetaCommand::Resign {
                election,
                node_id,
                term,
            } => self.resign(&election, &node_id, term),

            MetaCommand::PublishEvent {
                topic,
                event_type,
                payload,
                source_node,
            } => self.publish_event(&topic, event_type, payload, &source_node),

            MetaCommand::ExpireLeases => self.expire_leases(),

            // Cold Standby Architecture: Data node mode management
            // PromoteDataNode removed - using No-Promotion architecture
            // UpdateDataNodeMode removed - No-Promotion architecture (mode set at registration)

            // Scale-in (retirement) operations
            MetaCommand::StartRetire { node_id } => self.start_retire(&node_id),

            MetaCommand::CompleteRetire { node_id } => self.complete_retire(&node_id),

            MetaCommand::CancelRetire { node_id } => self.cancel_retire(&node_id),

            MetaCommand::UpdateRetireStatus {
                node_id,
                tables_reassigned,
                memtable_rows_remaining,
            } => self.update_retire_status(&node_id, tables_reassigned, memtable_rows_remaining),

            // Recovery operations (failover without promotion)
            MetaCommand::TriggerStandbyFlush {
                standby_id,
                crashed_primary_id,
            } => self.trigger_standby_flush(&standby_id, &crashed_primary_id),

            MetaCommand::AssignStandbyToPrimaries {
                standby_id,
                primary_ids,
            } => self.assign_standby_to_primaries(&standby_id, &primary_ids),

            MetaCommand::MarkPrimaryUnprotected { primary_id, reason } => {
                self.mark_primary_unprotected(&primary_id, &reason)
            }

            MetaCommand::RedistributeTables {
                crashed_primary_id,
                target_primaries,
            } => self.redistribute_partitions(&crashed_primary_id, &target_primaries),
        }
    }

    // NOTE: Operation implementations have been moved to the ops/ submodule:
    // - ops/node.rs: Node operations (register_node, heartbeat, deregister_node, update_node_state)
    // - ops/lock.rs: Lock operations (acquire_lock, release_lock, renew_lock)
    // - ops/election.rs: Election operations (campaign, resign)
    // - ops/event.rs: Event operations (publish_event, subscribe_events, expire_leases)
    // - ops/standby.rs: Standby pairing operations
    // - ops/scale_in.rs: Scale-in/retirement operations
    // - ops/recovery.rs: Recovery operations (failover without promotion)
    // - ops/query.rs: Query operations (list_nodes, get_node, etc.)

    // ========================================================================
    // Raft State Machine Helpers
    // ========================================================================

    /// Serialize all state data for snapshot
    fn serialize_state(&self) -> Result<Vec<u8>, String> {
        // Collect all data from the database that should be in the snapshot
        // This includes everything except Raft log entries
        let mut state_data = Vec::new();

        // Iterate over all keys and collect them
        for item in self.db.iter() {
            if let Ok((key, value)) = item {
                // Skip Raft log entries (they have their own persistence)
                if key.starts_with(b"/raft/log/") {
                    continue;
                }
                // Skip Raft vote/committed state
                if key.starts_with(b"/raft/vote")
                    || key.starts_with(b"/raft/committed")
                    || key.starts_with(b"/raft/last_purged")
                {
                    continue;
                }
                state_data.push((key.to_vec(), value.to_vec()));
            }
        }

        bincode::serialize(&state_data).map_err(|e| format!("Failed to serialize state: {}", e))
    }

    /// Restore state from snapshot data
    fn restore_state(&self, data: &[u8]) -> Result<(), String> {
        let state_data: Vec<(Vec<u8>, Vec<u8>)> = bincode::deserialize(data)
            .map_err(|e| format!("Failed to deserialize state: {}", e))?;

        // Clear existing state (except Raft log entries)
        for item in self.db.iter() {
            if let Ok((key, _)) = item {
                if key.starts_with(b"/raft/log/")
                    || key.starts_with(b"/raft/vote")
                    || key.starts_with(b"/raft/committed")
                    || key.starts_with(b"/raft/last_purged")
                {
                    continue;
                }
                let _ = self.db.remove(&key);
            }
        }

        // Insert new state
        for (key, value) in state_data {
            self.db
                .insert(key, value)
                .map_err(|e| format!("Failed to insert state: {}", e))?;
        }

        self.db
            .flush()
            .map_err(|e| format!("Failed to flush state: {}", e))?;

        Ok(())
    }

    /// Update last applied log ID
    fn update_last_applied(&self, log_id: LogId) {
        *self.last_applied.write() = Some(log_id);
        let _ = self.db.insert(
            raft_keys::LAST_APPLIED,
            bincode::serialize(&log_id).unwrap(),
        );
    }

    /// Update membership configuration
    fn update_membership(&self, membership: StoredMembership) {
        *self.last_membership.write() = membership.clone();
        let _ = self.db.insert(
            raft_keys::LAST_MEMBERSHIP,
            bincode::serialize(&membership).unwrap(),
        );
    }

    /// Helper to create a storage error
    fn storage_error(e: impl std::fmt::Display, context: &str) -> StorageError<NodeId> {
        let msg = format!("{}: {}", context, e);
        error!("{}", msg);
        StorageError::IO {
            source: StorageIOError::write(AnyError::error(msg)),
        }
    }
}

impl Clone for MetaStateMachine {
    fn clone(&self) -> Self {
        // Create a new broadcast channel for the cloned state machine
        // Clone of sender shares the same channel, allowing all clones to broadcast
        let (event_broadcast, _) = broadcast::channel(1000);
        Self {
            db: self.db.clone(),
            event_sequence: AtomicU64::new(self.event_sequence.load(Ordering::SeqCst)),
            fencing_counter: AtomicU64::new(self.fencing_counter.load(Ordering::SeqCst)),
            lease_counter: AtomicU64::new(0),
            last_applied: RwLock::new(self.last_applied.read().clone()),
            last_membership: RwLock::new(self.last_membership.read().clone()),
            event_broadcast,
            standby_config: self.standby_config.clone(),
        }
    }
}

// ============================================================================
// RaftStateMachine Implementation
// ============================================================================

impl RaftStateMachine<TypeConfig> for MetaStateMachine {
    type SnapshotBuilder = MetaSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), StorageError<NodeId>> {
        let last_applied = self.last_applied.read().clone();
        let membership = self.last_membership.read().clone();
        debug!(
            "applied_state: last_applied={:?}, membership={:?}",
            last_applied, membership
        );
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<crate::meta::types::MetaResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            debug!("Applying log entry: {:?}", log_id);

            // Handle the entry based on its type
            match entry.payload {
                openraft::EntryPayload::Blank => {
                    // No-op entry
                    responses.push(MetaResponse::Success);
                }
                openraft::EntryPayload::Normal(cmd) => {
                    // Apply the command to our state
                    let response = self.apply_command(cmd);
                    responses.push(response);
                }
                openraft::EntryPayload::Membership(m) => {
                    // Update membership configuration
                    let membership = StoredMembership::new(Some(log_id), m);
                    self.update_membership(membership);
                    responses.push(MetaResponse::Success);
                }
            }

            // Update last applied
            self.update_last_applied(log_id);
        }

        // Flush to disk
        self.db
            .flush()
            .map_err(|e| Self::storage_error(e, "flush after apply"))?;

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaSnapshotBuilder {
            db: self.db.clone(),
            last_applied: self.last_applied.read().clone(),
            membership: self.last_membership.read().clone(),
            event_sequence: self.event_sequence.load(Ordering::SeqCst),
            fencing_counter: self.fencing_counter.load(Ordering::SeqCst),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        info!("Beginning to receive snapshot");
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        info!(
            "Installing snapshot: last_log_id={:?}, last_membership={:?}",
            meta.last_log_id, meta.last_membership
        );

        // Deserialize the snapshot
        let data = snapshot.into_inner();
        let snap: MetaSnapshot = bincode::deserialize(&data)
            .map_err(|e| Self::storage_error(e, "deserialize snapshot"))?;

        // Restore the state
        self.restore_state(&snap.data).map_err(|e| {
            let msg = format!("restore state: {}", e);
            error!("{}", msg);
            StorageError::IO {
                source: StorageIOError::write(AnyError::error(msg)),
            }
        })?;

        // Update counters
        self.event_sequence
            .store(snap.event_sequence, Ordering::SeqCst);
        self.fencing_counter
            .store(snap.fencing_counter, Ordering::SeqCst);

        // Update Raft state
        if let Some(log_id) = meta.last_log_id {
            self.update_last_applied(log_id);
        }
        self.update_membership(meta.last_membership.clone());

        // Save snapshot metadata
        let _ = self
            .db
            .insert(raft_keys::SNAPSHOT_META, bincode::serialize(meta).unwrap());

        self.db
            .flush()
            .map_err(|e| Self::storage_error(e, "flush after install snapshot"))?;

        info!("Snapshot installed successfully");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // Load snapshot metadata if it exists
        let meta: Option<SnapshotMeta> = self
            .db
            .get(raft_keys::SNAPSHOT_META)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok());

        match meta {
            Some(meta) => {
                // Build the snapshot data
                let state_data = self.serialize_state().map_err(|e| {
                    let msg = format!("serialize state: {}", e);
                    error!("{}", msg);
                    StorageError::IO {
                        source: StorageIOError::read(AnyError::error(msg)),
                    }
                })?;

                let snap = MetaSnapshot {
                    last_applied: self.last_applied.read().clone(),
                    membership: self.last_membership.read().clone(),
                    data: state_data,
                    event_sequence: self.event_sequence.load(Ordering::SeqCst),
                    fencing_counter: self.fencing_counter.load(Ordering::SeqCst),
                };

                let snap_data = bincode::serialize(&snap)
                    .map_err(|e| Self::storage_error(e, "serialize snapshot"))?;

                debug!(
                    "Returning current snapshot: meta={:?}, size={}",
                    meta,
                    snap_data.len()
                );

                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(snap_data)),
                }))
            }
            None => {
                debug!("No current snapshot available");
                Ok(None)
            }
        }
    }
}

// ============================================================================
// MetaSnapshotBuilder Implementation
// ============================================================================

impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for MetaSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        info!("Building snapshot: last_applied={:?}", self.last_applied);

        // Collect all state data
        let mut state_data = Vec::new();
        for item in self.db.iter() {
            if let Ok((key, value)) = item {
                // Skip Raft log entries
                if key.starts_with(b"/raft/log/")
                    || key.starts_with(b"/raft/vote")
                    || key.starts_with(b"/raft/committed")
                    || key.starts_with(b"/raft/last_purged")
                {
                    continue;
                }
                state_data.push((key.to_vec(), value.to_vec()));
            }
        }

        let data = bincode::serialize(&state_data).map_err(|e| {
            let msg = format!("serialize state: {}", e);
            error!("{}", msg);
            StorageError::IO {
                source: StorageIOError::write(AnyError::error(msg)),
            }
        })?;

        let snap = MetaSnapshot {
            last_applied: self.last_applied.clone(),
            membership: self.membership.clone(),
            data,
            event_sequence: self.event_sequence,
            fencing_counter: self.fencing_counter,
        };

        let snap_data = bincode::serialize(&snap).map_err(|e| {
            let msg = format!("serialize snapshot: {}", e);
            error!("{}", msg);
            StorageError::IO {
                source: StorageIOError::write(AnyError::error(msg)),
            }
        })?;

        // Generate snapshot ID
        let snapshot_id = format!(
            "{}-{}",
            self.last_applied
                .map(|l| l.index.to_string())
                .unwrap_or_else(|| "0".to_string()),
            chrono::Utc::now().timestamp_millis()
        );

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.membership.clone(),
            snapshot_id,
        };

        // Save snapshot metadata to db
        let _ = self
            .db
            .insert(raft_keys::SNAPSHOT_META, bincode::serialize(&meta).unwrap());

        info!(
            "Snapshot built: id={}, size={}",
            meta.snapshot_id,
            snap_data.len()
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snap_data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::*;
    use tempfile::tempdir;

    /// Helper to create a state machine for testing
    fn create_test_state_machine() -> MetaStateMachine {
        let dir = tempdir().unwrap();
        let db = sled::open(dir.path()).unwrap();
        MetaStateMachine::new(db, crate::meta::config::StandbyConfig::default())
    }

    // ============================================================================
    // Serialization/Deserialization Tests
    // ============================================================================

    /// Helper to create a test NodeInfo
    fn create_test_node_info(node_id: &str, role: NodeRole) -> NodeInfo {
        NodeInfo {
            node_id: node_id.to_string(),
            role,
            state: NodeState::Active,
            advertise_addr: format!("127.0.0.1:5000"),
            http_addr: Some("127.0.0.1:5001".to_string()),
            flight_addr: Some("127.0.0.1:5002".to_string()),
            registered_at: 1234567890,
            last_heartbeat: 1234567890,
            labels: std::collections::HashMap::new(),
            mode: None,
            backing_standby: None,
            backed_primaries: vec![],
        }
    }

    #[test]
    fn test_meta_command_serialization_register_node() {
        let info = create_test_node_info("test-node", NodeRole::Data);
        let cmd = MetaCommand::RegisterNode {
            info,
            lease_id: 123,
            lease_ttl_ms: 30000,
        };

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: MetaCommand = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            MetaCommand::RegisterNode {
                info,
                lease_id,
                lease_ttl_ms,
            } => {
                assert_eq!(info.node_id, "test-node");
                assert_eq!(lease_id, 123);
                assert_eq!(lease_ttl_ms, 30000);
            }
            _ => panic!("Expected RegisterNode command"),
        }
    }

    #[test]
    fn test_meta_command_serialization_heartbeat() {
        let cmd = MetaCommand::Heartbeat {
            node_id: "test-node".to_string(),
            lease_id: 456,
            timestamp: 9876543210,
        };

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: MetaCommand = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            MetaCommand::Heartbeat {
                node_id,
                lease_id,
                timestamp,
            } => {
                assert_eq!(node_id, "test-node");
                assert_eq!(lease_id, 456);
                assert_eq!(timestamp, 9876543210);
            }
            _ => panic!("Expected Heartbeat command"),
        }
    }

    #[test]
    fn test_meta_command_serialization_acquire_lock() {
        let cmd = MetaCommand::AcquireLock {
            key: "partition/db/schema/table/2024-01-15".to_string(),
            holder: "node-1".to_string(),
            ttl_ms: 10000,
        };

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: MetaCommand = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            MetaCommand::AcquireLock {
                key,
                holder,
                ttl_ms,
            } => {
                assert_eq!(key, "partition/db/schema/table/2024-01-15");
                assert_eq!(holder, "node-1");
                assert_eq!(ttl_ms, 10000);
            }
            _ => panic!("Expected AcquireLock command"),
        }
    }

    #[test]
    fn test_meta_response_serialization() {
        let responses = vec![
            MetaResponse::Success,
            MetaResponse::NodeRegistered { lease_id: 123 },
            MetaResponse::LockAcquired { fencing_token: 456 },
            MetaResponse::LockNotAcquired {
                current_holder: "other-node".to_string(),
                expires_at: 9999999999,
            },
            MetaResponse::Error {
                message: "Test error".to_string(),
            },
        ];

        for response in responses {
            let serialized = bincode::serialize(&response).unwrap();
            let deserialized: MetaResponse = bincode::deserialize(&serialized).unwrap();
            assert_eq!(format!("{:?}", response), format!("{:?}", deserialized));
        }
    }

    #[test]
    fn test_entry_serialization_with_blank_payload() {
        use super::Entry;
        use openraft::EntryPayload;

        let entry = Entry {
            log_id: LogId::new(openraft::LeaderId::new(1, 1), 5),
            payload: EntryPayload::Blank,
        };

        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: Entry = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.log_id.index, 5);
        assert!(matches!(deserialized.payload, EntryPayload::Blank));
    }

    #[test]
    fn test_entry_serialization_with_normal_payload() {
        use super::Entry;
        use openraft::EntryPayload;

        let cmd = MetaCommand::DeregisterNode {
            node_id: "test-node".to_string(),
        };
        let entry = Entry {
            log_id: LogId::new(openraft::LeaderId::new(2, 3), 10),
            payload: EntryPayload::Normal(cmd),
        };

        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: Entry = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.log_id.index, 10);
        assert_eq!(deserialized.log_id.leader_id.term, 2);

        match deserialized.payload {
            EntryPayload::Normal(MetaCommand::DeregisterNode { node_id }) => {
                assert_eq!(node_id, "test-node");
            }
            _ => panic!("Expected Normal payload with DeregisterNode"),
        }
    }

    #[test]
    fn test_entry_serialization_with_membership_payload() {
        use super::Entry;
        use openraft::{EntryPayload, Membership};
        use std::collections::BTreeMap;

        let mut nodes: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
        nodes.insert(
            1,
            openraft::BasicNode {
                addr: "127.0.0.1:6000".to_string(),
            },
        );
        nodes.insert(
            2,
            openraft::BasicNode {
                addr: "127.0.0.1:6100".to_string(),
            },
        );
        nodes.insert(
            3,
            openraft::BasicNode {
                addr: "127.0.0.1:6200".to_string(),
            },
        );

        let membership = Membership::new(vec![vec![1, 2, 3].into_iter().collect()], nodes);

        let entry = Entry {
            log_id: LogId::new(openraft::LeaderId::new(1, 1), 1),
            payload: EntryPayload::Membership(membership),
        };

        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: Entry = bincode::deserialize(&serialized).unwrap();

        match deserialized.payload {
            EntryPayload::Membership(m) => {
                assert_eq!(m.nodes().count(), 3);
            }
            _ => panic!("Expected Membership payload"),
        }
    }

    // ============================================================================
    // State Machine Operation Tests
    // ============================================================================

    #[test]
    fn test_state_machine_register_node() {
        let sm = create_test_state_machine();

        let info = create_test_node_info("data-1", NodeRole::Data);

        let response = sm.apply_command(MetaCommand::RegisterNode {
            info: info.clone(),
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        match response {
            MetaResponse::NodeRegistered { lease_id } => {
                assert!(lease_id > 0);
            }
            _ => panic!("Expected NodeRegistered response"),
        }

        // Verify node is stored
        let stored = sm.get_node("data-1");
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().advertise_addr, "127.0.0.1:5000");
    }

    #[test]
    fn test_state_machine_reregister_preserves_mode() {
        use crate::meta::config::StandbyConfig;
        use crate::meta::types::DataNodeMode;

        // Create state machine with standby enabled
        let standby_config = StandbyConfig {
            enabled: true,
            primary_standby_ratio: 2,
            min_primary_nodes: 2,
            ..Default::default()
        };
        let dir = tempdir().unwrap();
        let db = sled::open(dir.path()).unwrap();
        let sm = MetaStateMachine::new(db, standby_config);

        // Register first two nodes as Primary (min_primary_nodes=2)
        for i in 1..=2 {
            let info = create_test_node_info(&format!("data-{}", i), NodeRole::Data);
            sm.apply_command(MetaCommand::RegisterNode {
                info,
                lease_id: 0,
                lease_ttl_ms: 30000,
            });
        }

        // Third node should be Standby
        let info3 = create_test_node_info("data-3", NodeRole::Data);
        sm.apply_command(MetaCommand::RegisterNode {
            info: info3,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        let stored = sm.get_node("data-3").unwrap();
        assert_eq!(stored.mode, Some(DataNodeMode::Standby));

        // Now re-register data-3 (simulating restart)
        // Even though cluster state might suggest Primary, mode should be preserved
        let info3_restart = create_test_node_info("data-3", NodeRole::Data);
        sm.apply_command(MetaCommand::RegisterNode {
            info: info3_restart,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        // Mode should still be Standby (preserved from original registration)
        let stored_after = sm.get_node("data-3").unwrap();
        assert_eq!(
            stored_after.mode,
            Some(DataNodeMode::Standby),
            "Mode should be preserved on re-registration"
        );
    }

    #[test]
    fn test_state_machine_heartbeat() {
        let sm = create_test_state_machine();

        // First register a node
        let info = create_test_node_info("data-1", NodeRole::Data);

        let reg_response = sm.apply_command(MetaCommand::RegisterNode {
            info,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        let lease_id = match reg_response {
            MetaResponse::NodeRegistered { lease_id } => lease_id,
            _ => panic!("Expected NodeRegistered"),
        };

        // Send heartbeat
        let hb_response = sm.apply_command(MetaCommand::Heartbeat {
            node_id: "data-1".to_string(),
            lease_id,
            timestamp: 1234567890,
        });

        assert!(matches!(hb_response, MetaResponse::Success));

        // Verify node state changed to Active
        let stored = sm.get_node("data-1").unwrap();
        assert_eq!(stored.state, NodeState::Active);
        assert_eq!(stored.last_heartbeat, 1234567890);
    }

    #[test]
    fn test_state_machine_deregister_node() {
        let sm = create_test_state_machine();

        // Register a node
        let info = create_test_node_info("data-1", NodeRole::Data);

        sm.apply_command(MetaCommand::RegisterNode {
            info,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        // Verify node exists and is Active
        let node = sm.get_node("data-1");
        assert!(node.is_some());
        assert_eq!(node.unwrap().state, NodeState::Active);

        // Deregister
        sm.apply_command(MetaCommand::DeregisterNode {
            node_id: "data-1".to_string(),
        });

        // Verify node is marked as Drained (not removed)
        // Node stays in db for background cleanup job to process
        let node = sm.get_node("data-1");
        assert!(node.is_some());
        assert_eq!(node.unwrap().state, NodeState::Drained);
    }

    #[test]
    fn test_state_machine_acquire_release_lock() {
        let sm = create_test_state_machine();
        let lock_key = "partition/db/default/cpu/2024-01-15".to_string();

        // Acquire lock
        let acquire_response = sm.apply_command(MetaCommand::AcquireLock {
            key: lock_key.clone(),
            holder: "node-1".to_string(),
            ttl_ms: 30000,
        });

        let fencing_token = match acquire_response {
            MetaResponse::LockAcquired { fencing_token } => fencing_token,
            _ => panic!("Expected LockAcquired"),
        };
        assert!(fencing_token > 0);

        // Try to acquire same lock from another node (should fail)
        let second_acquire = sm.apply_command(MetaCommand::AcquireLock {
            key: lock_key.clone(),
            holder: "node-2".to_string(),
            ttl_ms: 30000,
        });

        match second_acquire {
            MetaResponse::LockNotAcquired { current_holder, .. } => {
                assert_eq!(current_holder, "node-1");
            }
            _ => panic!("Expected LockNotAcquired"),
        }

        // Release lock
        let release_response = sm.apply_command(MetaCommand::ReleaseLock {
            key: lock_key.clone(),
            holder: "node-1".to_string(),
            fencing_token,
        });

        assert!(matches!(release_response, MetaResponse::LockReleased));

        // Now node-2 can acquire the lock
        let third_acquire = sm.apply_command(MetaCommand::AcquireLock {
            key: lock_key,
            holder: "node-2".to_string(),
            ttl_ms: 30000,
        });

        assert!(matches!(third_acquire, MetaResponse::LockAcquired { .. }));
    }

    #[test]
    fn test_state_machine_campaign_resign() {
        let sm = create_test_state_machine();
        let election = "compactor".to_string();

        // Campaign for leadership
        let campaign_response = sm.apply_command(MetaCommand::Campaign {
            election: election.clone(),
            node_id: "node-1".to_string(),
            ttl_ms: 30000,
        });

        match campaign_response {
            MetaResponse::LeaderElected { term } => {
                assert_eq!(term, 1);
            }
            _ => panic!("Expected LeaderElected"),
        }

        // Verify leader is stored
        let leader = sm.get_leader(&election);
        assert!(leader.is_some());
        assert_eq!(leader.unwrap().leader, "node-1");

        // Another node tries to campaign (should fail)
        let second_campaign = sm.apply_command(MetaCommand::Campaign {
            election: election.clone(),
            node_id: "node-2".to_string(),
            ttl_ms: 30000,
        });

        match second_campaign {
            MetaResponse::LeaderExists { current_leader, .. } => {
                assert_eq!(current_leader, "node-1");
            }
            _ => panic!("Expected LeaderExists"),
        }

        // Resign
        let resign_response = sm.apply_command(MetaCommand::Resign {
            election: election.clone(),
            node_id: "node-1".to_string(),
            term: 1,
        });

        assert!(matches!(resign_response, MetaResponse::Success));

        // Now node-2 can campaign
        let third_campaign = sm.apply_command(MetaCommand::Campaign {
            election,
            node_id: "node-2".to_string(),
            ttl_ms: 30000,
        });

        match third_campaign {
            MetaResponse::LeaderElected { term } => {
                assert_eq!(term, 2);
            }
            _ => panic!("Expected LeaderElected"),
        }
    }

    #[test]
    fn test_state_machine_list_nodes_filter() {
        let sm = create_test_state_machine();

        // Register multiple nodes
        for (id, role) in &[
            ("data-1", NodeRole::Data),
            ("data-2", NodeRole::Data),
            ("meta-1", NodeRole::Meta),
            ("router-1", NodeRole::Router),
        ] {
            let info = create_test_node_info(id, *role);
            sm.apply_command(MetaCommand::RegisterNode {
                info,
                lease_id: 0,
                lease_ttl_ms: 30000,
            });
        }

        // List all nodes
        let all_nodes = sm.list_nodes(None, None);
        assert_eq!(all_nodes.len(), 4);

        // List only data nodes
        let data_nodes = sm.list_nodes(Some(NodeRole::Data), None);
        assert_eq!(data_nodes.len(), 2);

        // List only active meta nodes
        let meta_nodes = sm.list_nodes(Some(NodeRole::Meta), Some(NodeState::Active));
        assert_eq!(meta_nodes.len(), 1);
    }

    // ============================================================================
    // Snapshot Tests
    // ============================================================================

    #[tokio::test]
    async fn test_state_machine_snapshot_and_restore() {
        use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

        // Create state machine and populate with data
        let mut sm1 = create_test_state_machine();

        // Register some nodes
        for i in 1..=3 {
            let info = create_test_node_info(&format!("node-{}", i), NodeRole::Data);
            sm1.apply_command(MetaCommand::RegisterNode {
                info,
                lease_id: 0,
                lease_ttl_ms: 30000,
            });
        }

        // Acquire a lock
        sm1.apply_command(MetaCommand::AcquireLock {
            key: "test-lock".to_string(),
            holder: "node-1".to_string(),
            ttl_ms: 300000,
        });

        // Build snapshot
        let mut builder = sm1.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        // Create new state machine and restore
        let mut sm2 = create_test_state_machine();
        sm2.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        // Verify nodes were restored
        let nodes = sm2.list_nodes(None, None);
        assert_eq!(nodes.len(), 3);

        // Verify lock state - lock should exist after restore
        let lock = sm2.get_lock("test-lock");
        assert!(lock.is_some(), "Lock should exist after snapshot restore");
    }

    // ============================================================================
    // Edge Case Tests
    // ============================================================================

    #[test]
    fn test_state_machine_renew_lock_wrong_token() {
        let sm = create_test_state_machine();
        let lock_key = "test-lock".to_string();

        // Acquire lock
        let acquire_response = sm.apply_command(MetaCommand::AcquireLock {
            key: lock_key.clone(),
            holder: "node-1".to_string(),
            ttl_ms: 30000,
        });

        let fencing_token = match acquire_response {
            MetaResponse::LockAcquired { fencing_token } => fencing_token,
            _ => panic!("Expected LockAcquired"),
        };

        // Try to renew with wrong fencing token
        let renew_response = sm.apply_command(MetaCommand::RenewLock {
            key: lock_key,
            holder: "node-1".to_string(),
            fencing_token: fencing_token + 999,
            ttl_ms: 30000,
        });

        match renew_response {
            MetaResponse::Error { message } => {
                assert!(
                    message.contains("not found") || message.contains("mismatch"),
                    "Expected 'not found' or 'mismatch' error, got: {}",
                    message
                );
            }
            MetaResponse::LockRenewed { .. } => {
                panic!("Should not renew with wrong token");
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_state_machine_release_lock_wrong_holder() {
        let sm = create_test_state_machine();
        let lock_key = "test-lock".to_string();

        // Acquire lock
        let acquire_response = sm.apply_command(MetaCommand::AcquireLock {
            key: lock_key.clone(),
            holder: "node-1".to_string(),
            ttl_ms: 30000,
        });

        let fencing_token = match acquire_response {
            MetaResponse::LockAcquired { fencing_token } => fencing_token,
            _ => panic!("Expected LockAcquired"),
        };

        // Try to release with wrong holder
        let release_response = sm.apply_command(MetaCommand::ReleaseLock {
            key: lock_key,
            holder: "node-2".to_string(),
            fencing_token,
        });

        // Should not be LockReleased
        match release_response {
            MetaResponse::LockReleased => panic!("Should not release lock with wrong holder"),
            _ => {} // Any other response is acceptable
        }
    }

    #[test]
    fn test_state_machine_heartbeat_unknown_node() {
        let sm = create_test_state_machine();

        // Send heartbeat for non-existent node
        let response = sm.apply_command(MetaCommand::Heartbeat {
            node_id: "unknown-node".to_string(),
            lease_id: 999,
            timestamp: 1234567890,
        });

        // Should still succeed (idempotent)
        assert!(matches!(response, MetaResponse::Success));
    }

    #[test]
    fn test_state_machine_update_node_state() {
        let sm = create_test_state_machine();

        // Register a node
        let info = create_test_node_info("data-1", NodeRole::Data);
        sm.apply_command(MetaCommand::RegisterNode {
            info,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        // Update state to Inactive
        sm.apply_command(MetaCommand::UpdateNodeState {
            node_id: "data-1".to_string(),
            state: NodeState::Inactive,
        });

        // Verify state changed
        let stored = sm.get_node("data-1").unwrap();
        assert_eq!(stored.state, NodeState::Inactive);
    }

    #[test]
    fn test_state_machine_publish_event() {
        let sm = create_test_state_machine();

        // Publish an event
        let payload = serde_json::to_vec(&serde_json::json!({"node_id": "test-node"})).unwrap();
        let response = sm.apply_command(MetaCommand::PublishEvent {
            topic: "test".to_string(),
            event_type: EventType::NodeJoined,
            payload,
            source_node: "node-1".to_string(),
        });

        match response {
            MetaResponse::EventPublished { sequence } => {
                assert!(sequence > 0);
            }
            _ => panic!("Expected EventPublished"),
        }

        // Get events
        let events = sm.get_events_from(0, 10);
        assert!(!events.is_empty());
    }

    #[test]
    fn test_get_table_owner_filters_drained_nodes() {
        let sm = create_test_state_machine();

        // Register two data nodes
        let info1 = create_test_node_info("data-1", NodeRole::Data);
        let info2 = create_test_node_info("data-2", NodeRole::Data);

        sm.apply_command(MetaCommand::RegisterNode {
            info: info1,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });
        sm.apply_command(MetaCommand::RegisterNode {
            info: info2,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        // Register table with data-1 as owner
        sm.register_table("mydb", "default", "cpu", "data-1");

        // Verify data-1 is the owner
        assert_eq!(
            sm.get_table_owner("mydb", "default", "cpu"),
            Some("data-1".to_string())
        );

        // Deregister data-1 (marks as Drained)
        sm.apply_command(MetaCommand::DeregisterNode {
            node_id: "data-1".to_string(),
        });

        // Verify data-1 is Drained
        let node = sm.get_node("data-1").unwrap();
        assert_eq!(node.state, NodeState::Drained);

        // get_table_owner should now return None (data-1 is Drained, no other owners)
        assert_eq!(sm.get_table_owner("mydb", "default", "cpu"), None);

        // Add data-2 as owner
        sm.add_table_owner("mydb", "default", "cpu", "data-2");

        // Now get_table_owner should return data-2 (skipping Drained data-1)
        assert_eq!(
            sm.get_table_owner("mydb", "default", "cpu"),
            Some("data-2".to_string())
        );

        // get_table_all_owners should only return data-2
        let all_owners = sm.get_table_all_owners("mydb", "default", "cpu");
        assert_eq!(all_owners, vec!["data-2".to_string()]);
    }

    #[test]
    fn test_drained_node_cleanup_logic() {
        let sm = create_test_state_machine();

        // Register nodes
        let info1 = create_test_node_info("data-1", NodeRole::Data);
        let info2 = create_test_node_info("data-2", NodeRole::Data);

        sm.apply_command(MetaCommand::RegisterNode {
            info: info1,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });
        sm.apply_command(MetaCommand::RegisterNode {
            info: info2,
            lease_id: 0,
            lease_ttl_ms: 30000,
        });

        // Register tables
        sm.register_table("mydb", "default", "cpu", "data-1");
        sm.register_table("mydb", "default", "memory", "data-2");

        // Deregister data-1 (marks as Drained)
        sm.apply_command(MetaCommand::DeregisterNode {
            node_id: "data-1".to_string(),
        });

        // Simulate cleanup logic: build set of nodes with tables (single scan)
        let all_tables = sm.list_all_tables();
        let nodes_with_tables: std::collections::HashSet<String> = all_tables
            .iter()
            .flat_map(|t| t.owner_node_ids.iter().cloned())
            .collect();

        // data-1 should still be in the set (table ownership not yet cleaned)
        assert!(nodes_with_tables.contains("data-1"));
        assert!(nodes_with_tables.contains("data-2"));

        // Get drained nodes
        let drained = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Drained));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].node_id, "data-1");

        // data-1 is drained but still owns table, should NOT be removed yet
        assert!(nodes_with_tables.contains(&drained[0].node_id));

        // Now remove data-1 from table ownership (simulating rebalancer)
        sm.remove_table("mydb", "default", "cpu");

        // Rebuild the set
        let all_tables = sm.list_all_tables();
        let nodes_with_tables: std::collections::HashSet<String> = all_tables
            .iter()
            .flat_map(|t| t.owner_node_ids.iter().cloned())
            .collect();

        // Now data-1 is not in the set (no tables), safe to remove
        assert!(!nodes_with_tables.contains("data-1"));
        assert!(nodes_with_tables.contains("data-2"));
    }
}
