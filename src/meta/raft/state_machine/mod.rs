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

// Operation modules - contain impl blocks for MetaStateMachine
mod distribution;
mod election;
mod event;
mod lock;
mod node;

#[cfg(test)]
mod tests;
mod query;
mod recovery;
mod scale_in;
mod standby;
mod table;

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
            .unwrap_or_default();

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
                ttl_ms,
            } => self.add_table_owner(&catalog, &schema, &table, &owner_node_id, ttl_ms),

            MetaCommand::RefreshOwnerTTL {
                catalog,
                schema,
                table,
                owner_node_id,
                ttl_ms,
            } => self.refresh_owner_ttl(&catalog, &schema, &table, &owner_node_id, ttl_ms),

            MetaCommand::CleanupExpiredOwners => self.cleanup_expired_owners(),

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
        for (key, value) in self.db.iter().flatten() {
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

        bincode::serialize(&state_data).map_err(|e| format!("Failed to serialize state: {}", e))
    }

    /// Restore state from snapshot data
    fn restore_state(&self, data: &[u8]) -> Result<(), String> {
        let state_data: Vec<(Vec<u8>, Vec<u8>)> = bincode::deserialize(data)
            .map_err(|e| format!("Failed to deserialize state: {}", e))?;

        // Clear existing state (except Raft log entries)
        for (key, _) in self.db.iter().flatten() {
            if key.starts_with(b"/raft/log/")
                || key.starts_with(b"/raft/vote")
                || key.starts_with(b"/raft/committed")
                || key.starts_with(b"/raft/last_purged")
            {
                continue;
            }
            let _ = self.db.remove(&key);
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
            last_applied: RwLock::new(*self.last_applied.read()),
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
        let last_applied = *self.last_applied.read();
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
            last_applied: *self.last_applied.read(),
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
                    last_applied: *self.last_applied.read(),
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
        for (key, value) in self.db.iter().flatten() {
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

        let data = bincode::serialize(&state_data).map_err(|e| {
            let msg = format!("serialize state: {}", e);
            error!("{}", msg);
            StorageError::IO {
                source: StorageIOError::write(AnyError::error(msg)),
            }
        })?;

        let snap = MetaSnapshot {
            last_applied: self.last_applied,
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

