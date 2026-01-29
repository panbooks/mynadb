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
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

/// Storage keys for Raft state
mod raft_keys {
    pub const LAST_APPLIED: &[u8] = b"/raft/sm/last_applied";
    pub const LAST_MEMBERSHIP: &[u8] = b"/raft/sm/membership";
    pub const SNAPSHOT_META: &[u8] = b"/raft/sm/snapshot_meta";
}

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
    /// Create a new state machine with the given sled database
    pub fn new(db: Db) -> Self {
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
            "MetaStateMachine initialized: last_applied={:?}, membership={:?}",
            last_applied, last_membership
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
        }
    }

    /// Open or create a sled database at the given path
    pub fn open(path: &std::path::Path) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self::new(db))
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

            MetaCommand::RegisterFiles { files } => self.register_files(files),

            MetaCommand::RemoveFiles { paths } => self.remove_files(paths),

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

            // Partition replica operations (warm standby)
            MetaCommand::AssignReplica {
                partition_key,
                leader_node_id,
                standby_node_ids,
                replication_factor,
            } => self.assign_replica(&partition_key, &leader_node_id, standby_node_ids, replication_factor),

            MetaCommand::PromoteStandby { partition_key } => self.promote_standby(&partition_key),

            MetaCommand::UpdateReplicaState {
                partition_key,
                state,
                last_replicated_seq,
                replication_lag_ms,
            } => self.update_replica_state(&partition_key, state, last_replicated_seq, replication_lag_ms),

            MetaCommand::RemoveReplica { partition_key } => self.remove_replica(&partition_key),

            MetaCommand::AutoAssignReplica { partition_key } => self.auto_assign_replica(&partition_key),

            MetaCommand::RebalanceOnNodeJoin { node_id } => self.rebalance_on_node_join(&node_id),

            // Cold Standby Architecture: Data node mode management
            MetaCommand::PromoteDataNode {
                standby_node_id,
                failed_primary_id,
            } => self.promote_data_node(&standby_node_id, failed_primary_id.as_deref()),

            MetaCommand::UpdateDataNodeMode { node_id, mode } => {
                self.update_data_node_mode(&node_id, mode)
            }

            // Scale-in (retirement) operations
            MetaCommand::StartRetire { node_id } => self.start_retire(&node_id),

            MetaCommand::CompleteRetire { node_id } => self.complete_retire(&node_id),

            MetaCommand::ReassignPartitionForRetire {
                partition_key,
                from_node_id,
                to_node_id,
            } => self.reassign_partition_for_retire(&partition_key, &from_node_id, &to_node_id),

            MetaCommand::CancelRetire { node_id } => self.cancel_retire(&node_id),

            MetaCommand::UpdateRetireStatus {
                node_id,
                partitions_reassigned,
                memtable_rows_remaining,
            } => self.update_retire_status(&node_id, partitions_reassigned, memtable_rows_remaining),

            MetaCommand::RemoveStandbyFromReplicas { standby_node_id } => {
                self.remove_standby_from_replicas(&standby_node_id)
            }
        }
    }

    // ========================================================================
    // Node Operations
    // ========================================================================

    fn register_node(&self, mut info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, info.node_id);
        let lease_id = self.next_lease_id();

        tracing::info!(
            "register_node: registering node_id={}, key={}, role={:?}, state={:?}",
            info.node_id, key, info.role, info.state
        );

        // Cold Standby Architecture: Auto-assign mode for Data nodes
        if info.role == NodeRole::Data && info.mode.is_none() {
            let standby_config = crate::meta::config::StandbyConfig::default();
            if standby_config.enabled && standby_config.auto_assign_mode {
                // Count current Primary and Standby nodes
                let (primary_count, standby_count) = self.count_data_nodes_by_mode();

                // Determine mode based on ratio
                let assigned_mode = standby_config.mode_for_new_node(primary_count, standby_count);
                info.mode = Some(assigned_mode);

                info!(
                    "Auto-assigned mode {} to Data node {} (primaries={}, standbys={})",
                    assigned_mode, info.node_id, primary_count, standby_count
                );
            } else {
                // Default to Primary if standby architecture is disabled
                info.mode = Some(DataNodeMode::Primary);
            }
        }

        // Create lease
        let lease = LeaseInfo::new(lease_id, info.node_id.clone(), lease_ttl_ms);
        let _ = self.db.insert(
            lease.storage_key().as_bytes(),
            bincode::serialize(&lease).unwrap(),
        );

        // Store node info
        let serialized = bincode::serialize(&info).unwrap();
        let insert_result = self.db.insert(key.as_bytes(), serialized.clone());

        // Verify the insert worked
        match insert_result {
            Ok(_) => {
                // Flush to ensure durability and visibility across clones
                let _ = self.db.flush();

                // Try to read it back immediately
                match self.db.get(key.as_bytes()) {
                    Ok(Some(data)) => {
                        tracing::info!(
                            "register_node: INSERT OK - key={}, wrote {} bytes, read back {} bytes",
                            key, serialized.len(), data.len()
                        );
                    }
                    Ok(None) => {
                        tracing::error!("register_node: INSERT FAILED - key={} not found after insert!", key);
                    }
                    Err(e) => {
                        tracing::error!("register_node: GET FAILED after insert - key={}, error={:?}", key, e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("register_node: INSERT FAILED - key={}, error={:?}", key, e);
            }
        }

        // Publish event
        let event_payload = serde_json::to_vec(&info).unwrap_or_default();
        self.publish_event("nodes", EventType::NodeJoined, event_payload, &info.node_id);

        MetaResponse::NodeRegistered { lease_id }
    }

    fn heartbeat(&self, node_id: &str, lease_id: u64, timestamp: i64) -> MetaResponse {
        // Update lease
        let lease_key = format!("{}{}", keys::PREFIX_LEASE, lease_id);
        if let Ok(Some(data)) = self.db.get(lease_key.as_bytes()) {
            if let Ok(mut lease) = bincode::deserialize::<LeaseInfo>(&data) {
                if lease.node_id == node_id {
                    lease.renew();
                    let _ = self
                        .db
                        .insert(lease_key.as_bytes(), bincode::serialize(&lease).unwrap());
                }
            }
        }

        // Update node heartbeat timestamp
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);
        if let Ok(Some(data)) = self.db.get(node_key.as_bytes()) {
            if let Ok(mut info) = bincode::deserialize::<NodeInfo>(&data) {
                info.last_heartbeat = timestamp;
                if info.state == NodeState::Starting {
                    info.state = NodeState::Active;
                }
                let _ = self
                    .db
                    .insert(node_key.as_bytes(), bincode::serialize(&info).unwrap());
            }
        }

        MetaResponse::Success
    }

    fn deregister_node(&self, node_id: &str) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get node info for event
        let info = self
            .db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize::<NodeInfo>(&data).ok());

        // Remove node
        let _ = self.db.remove(key.as_bytes());

        // Remove any leases for this node
        let prefix = keys::PREFIX_LEASE;
        for item in self.db.scan_prefix(prefix) {
            if let Ok((k, v)) = item {
                if let Ok(lease) = bincode::deserialize::<LeaseInfo>(&v) {
                    if lease.node_id == node_id {
                        let _ = self.db.remove(k);
                    }
                }
            }
        }

        // Cold Standby Architecture: If a PRIMARY Data node left, promote a STANDBY node
        let standby_config = crate::meta::config::StandbyConfig::default();
        if standby_config.enabled && standby_config.auto_promote {
            if let Some(ref node_info) = info {
                if node_info.role == NodeRole::Data && node_info.mode == Some(DataNodeMode::Primary) {
                    // Find an available Standby node to promote
                    if let Some(standby) = self.select_standby_to_promote(node_id) {
                        info!(
                            "Cold Standby: Primary node {} left, promoting Standby {} to Primary",
                            node_id, standby.node_id
                        );
                        self.promote_data_node(&standby.node_id, Some(node_id));
                    } else {
                        info!(
                            "Cold Standby: Primary node {} left, no Standby available to promote",
                            node_id
                        );
                    }
                }
            }
        }

        // Automatic failover: Promote standbys for partitions where this node was leader
        // This enables zero-downtime failover when combined with Router's instant standby routing
        let replicas_to_failover = self.get_replicas_by_leader(node_id);
        for replica in replicas_to_failover {
            info!(
                "Auto-failover: Node {} was leader for partition {}, promoting standby",
                node_id, replica.partition_key
            );
            // Promote the first available standby
            self.promote_standby(&replica.partition_key);
        }

        // Publish event
        if let Some(info) = info {
            let event_payload = serde_json::to_vec(&info).unwrap_or_default();
            self.publish_event("nodes", EventType::NodeLeft, event_payload, node_id);
        }

        MetaResponse::Success
    }

    fn update_node_state(&self, node_id: &str, state: NodeState) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, node_id);

        if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(mut info) = bincode::deserialize::<NodeInfo>(&data) {
                let old_state = info.state;
                info.state = state;
                let _ = self
                    .db
                    .insert(key.as_bytes(), bincode::serialize(&info).unwrap());

                // Publish event
                let payload = serde_json::json!({
                    "node_id": node_id,
                    "old_state": old_state as u8,
                    "new_state": state as u8
                });
                self.publish_event(
                    "nodes",
                    EventType::NodeStateChanged,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    node_id,
                );
            }
        }

        MetaResponse::Success
    }

    // ========================================================================
    // File Operations
    // ========================================================================

    fn register_files(&self, files: Vec<FileInfo>) -> MetaResponse {
        let mut registered = Vec::new();

        for file in files {
            let key = file.storage_key();
            let _ = self
                .db
                .insert(key.as_bytes(), bincode::serialize(&file).unwrap());
            registered.push(file.path.clone());
        }

        // Publish event
        let payload = serde_json::to_vec(&registered).unwrap_or_default();
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::FilesRegistered,
            "files".to_string(),
            payload,
            "system".to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::EventPublished { sequence: seq }
    }

    fn remove_files(&self, paths: Vec<String>) -> MetaResponse {
        for path in &paths {
            // Find and remove the file entry
            let prefix = keys::PREFIX_FILE;
            for item in self.db.scan_prefix(prefix) {
                if let Ok((k, v)) = item {
                    if let Ok(file) = bincode::deserialize::<FileInfo>(&v) {
                        if file.path == *path {
                            let _ = self.db.remove(k);
                            break;
                        }
                    }
                }
            }
        }

        // Publish event
        let payload = serde_json::to_vec(&paths).unwrap_or_default();
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::FilesRemoved,
            "files".to_string(),
            payload,
            "system".to_string(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::EventPublished { sequence: seq }
    }

    // ========================================================================
    // Lock Operations
    // ========================================================================

    fn acquire_lock(&self, key: &str, holder: &str, ttl_ms: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);

        // Check if lock exists and is not expired
        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LockInfo>(&data) {
                if !existing.is_expired() && existing.holder != holder {
                    return MetaResponse::LockNotAcquired {
                        current_holder: existing.holder,
                        expires_at: existing.expires_at,
                    };
                }
            }
        }

        // Acquire the lock
        let fencing_token = self.next_fencing_token();
        let lock = LockInfo::new(key.to_string(), holder.to_string(), fencing_token, ttl_ms);
        let _ = self
            .db
            .insert(storage_key.as_bytes(), bincode::serialize(&lock).unwrap());

        MetaResponse::LockAcquired { fencing_token }
    }

    fn release_lock(&self, key: &str, holder: &str, fencing_token: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LockInfo>(&data) {
                // Validate holder and fencing token
                if existing.holder == holder && existing.fencing_token == fencing_token {
                    let _ = self.db.remove(storage_key.as_bytes());
                    return MetaResponse::LockReleased;
                }
            }
        }

        MetaResponse::Error {
            message: "Lock not held or fencing token mismatch".to_string(),
        }
    }

    fn renew_lock(&self, key: &str, holder: &str, fencing_token: u64, ttl_ms: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(mut existing) = bincode::deserialize::<LockInfo>(&data) {
                if existing.holder == holder && existing.fencing_token == fencing_token {
                    let now = current_timestamp_ms();
                    existing.expires_at = now + ttl_ms as i64;
                    let _ = self.db.insert(
                        storage_key.as_bytes(),
                        bincode::serialize(&existing).unwrap(),
                    );
                    return MetaResponse::LockRenewed {
                        expires_at: existing.expires_at,
                    };
                }
            }
        }

        MetaResponse::Error {
            message: "Lock not held or fencing token mismatch".to_string(),
        }
    }

    // ========================================================================
    // Leader Election
    // ========================================================================

    fn campaign(&self, election: &str, node_id: &str, ttl_ms: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LEADER, election);

        // Check if there's an existing leader
        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LeaderInfo>(&data) {
                if !existing.is_expired() && existing.leader != node_id {
                    return MetaResponse::LeaderExists {
                        current_leader: existing.leader,
                        term: existing.term,
                    };
                }
            }
        }

        // Become leader
        let term = self.next_fencing_token();
        let leader = LeaderInfo::new(election.to_string(), node_id.to_string(), term, ttl_ms);
        let _ = self
            .db
            .insert(storage_key.as_bytes(), bincode::serialize(&leader).unwrap());

        // Publish event
        let payload = serde_json::json!({
            "election": election,
            "leader": node_id,
            "term": term
        });
        self.publish_event(
            "leaders",
            EventType::LeaderChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::LeaderElected { term }
    }

    fn resign(&self, election: &str, node_id: &str, term: u64) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_LEADER, election);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<LeaderInfo>(&data) {
                if existing.leader == node_id && existing.term == term {
                    let _ = self.db.remove(storage_key.as_bytes());

                    // Publish event
                    let payload = serde_json::json!({
                        "election": election,
                        "resigned": node_id,
                        "term": term
                    });
                    self.publish_event(
                        "leaders",
                        EventType::LeaderChanged,
                        serde_json::to_vec(&payload).unwrap_or_default(),
                        node_id,
                    );

                    return MetaResponse::Success;
                }
            }
        }

        MetaResponse::Error {
            message: "Not the current leader or term mismatch".to_string(),
        }
    }

    // ========================================================================
    // Event Operations
    // ========================================================================

    fn publish_event(
        &self,
        topic: &str,
        event_type: EventType,
        payload: Vec<u8>,
        source_node: &str,
    ) -> MetaResponse {
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            event_type,
            topic.to_string(),
            payload,
            source_node.to_string(),
        );

        // Persist to sled for durability and catch-up queries
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        // Broadcast to subscribers for instant notification
        // Ignore send errors (no subscribers or channel full)
        let _ = self.event_broadcast.send(event);

        MetaResponse::EventPublished { sequence: seq }
    }

    /// Subscribe to real-time event notifications
    ///
    /// Returns a broadcast receiver that will receive events as they are published.
    /// If the receiver falls behind (more than 1000 events), it will receive a
    /// `RecvError::Lagged` and should use `get_events_from()` to catch up.
    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.event_broadcast.subscribe()
    }

    // ========================================================================
    // Lease Management
    // ========================================================================

    fn expire_leases(&self) -> MetaResponse {
        let now = current_timestamp_ms();
        let prefix = keys::PREFIX_LEASE;
        let mut expired_nodes = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((k, v)) = item {
                if let Ok(lease) = bincode::deserialize::<LeaseInfo>(&v) {
                    if lease.expires_at < now {
                        let _ = self.db.remove(&k);
                        expired_nodes.push(lease.node_id);
                    }
                }
            }
        }

        // Deregister expired nodes
        for node_id in expired_nodes {
            self.deregister_node(&node_id);
        }

        MetaResponse::Success
    }

    // ========================================================================
    // Partition Replica Operations (Warm Standby)
    // ========================================================================

    /// Assign a partition replica (leader + standbys)
    fn assign_replica(
        &self,
        partition_key: &str,
        leader_node_id: &str,
        standby_node_ids: Vec<String>,
        replication_factor: u8,
    ) -> MetaResponse {
        let replica = PartitionReplica::new(
            partition_key.to_string(),
            leader_node_id.to_string(),
            standby_node_ids.clone(),
            replication_factor,
        );

        let storage_key = replica.storage_key();
        let _ = self.db.insert(
            storage_key.as_bytes(),
            bincode::serialize(&replica).unwrap(),
        );

        // Publish event for Router cache invalidation
        let payload = serde_json::json!({
            "partition_key": partition_key,
            "leader_node_id": leader_node_id,
            "standby_node_ids": standby_node_ids,
            "replication_factor": replication_factor,
        });
        self.publish_event(
            "replicas",
            EventType::ReplicaAssigned,
            serde_json::to_vec(&payload).unwrap_or_default(),
            leader_node_id,
        );

        MetaResponse::ReplicaAssigned {
            partition_key: partition_key.to_string(),
            leader_node_id: leader_node_id.to_string(),
        }
    }

    /// Promote standby to leader (failover)
    fn promote_standby(&self, partition_key: &str) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(mut replica) = bincode::deserialize::<PartitionReplica>(&data) {
                let old_leader = replica.leader_node_id.clone();

                if let Some(new_leader) = replica.promote_standby() {
                    // Update storage
                    let _ = self.db.insert(
                        storage_key.as_bytes(),
                        bincode::serialize(&replica).unwrap(),
                    );

                    // Publish event for Router cache invalidation
                    let payload = serde_json::json!({
                        "partition_key": partition_key,
                        "new_leader_id": &new_leader,
                        "old_leader_id": &old_leader,
                    });
                    self.publish_event(
                        "replicas",
                        EventType::ReplicaPromoted,
                        serde_json::to_vec(&payload).unwrap_or_default(),
                        &new_leader,
                    );

                    return MetaResponse::StandbyPromoted {
                        partition_key: partition_key.to_string(),
                        new_leader_id: new_leader,
                        old_leader_id: old_leader,
                    };
                } else {
                    return MetaResponse::NoStandbyAvailable {
                        partition_key: partition_key.to_string(),
                    };
                }
            }
        }

        MetaResponse::ReplicaNotFound {
            partition_key: partition_key.to_string(),
        }
    }

    /// Update replica state (replication progress)
    fn update_replica_state(
        &self,
        partition_key: &str,
        state: ReplicaState,
        last_replicated_seq: u64,
        replication_lag_ms: u64,
    ) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        if let Ok(Some(data)) = self.db.get(storage_key.as_bytes()) {
            if let Ok(mut replica) = bincode::deserialize::<PartitionReplica>(&data) {
                replica.state = state;
                replica.last_replicated_seq = last_replicated_seq;
                replica.replication_lag_ms = replication_lag_ms;
                replica.leader_last_seen = current_timestamp_ms();

                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&replica).unwrap(),
                );

                // Publish event
                let payload = serde_json::json!({
                    "partition_key": partition_key,
                    "state": format!("{:?}", state),
                    "last_replicated_seq": last_replicated_seq,
                    "replication_lag_ms": replication_lag_ms,
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaStateChanged,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    &replica.leader_node_id,
                );

                return MetaResponse::ReplicaUpdated {
                    partition_key: partition_key.to_string(),
                };
            }
        }

        MetaResponse::ReplicaNotFound {
            partition_key: partition_key.to_string(),
        }
    }

    /// Remove a partition replica
    fn remove_replica(&self, partition_key: &str) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        if self.db.remove(storage_key.as_bytes()).is_ok() {
            return MetaResponse::ReplicaRemoved {
                partition_key: partition_key.to_string(),
            };
        }

        MetaResponse::ReplicaNotFound {
            partition_key: partition_key.to_string(),
        }
    }

    /// Auto-assign a replica for a new partition using ReplicaAssigner
    ///
    /// Uses the configured load balancing strategy and zone-awareness
    /// to select leader and standbys.
    fn auto_assign_replica(&self, partition_key: &str) -> MetaResponse {
        // Check if replica already exists
        if self.get_replica(partition_key).is_some() {
            return MetaResponse::ReplicaAlreadyExists {
                partition_key: partition_key.to_string(),
            };
        }

        // Get active Data nodes
        let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        if nodes.is_empty() {
            return MetaResponse::NoDataNodesAvailable;
        }

        // Build partition counts for load balancing
        let partition_counts = self.get_partition_counts_per_node();

        // Use default config for auto-assignment
        // In production, this would come from MetaConfig
        let config = crate::meta::config::ReplicationAssignmentConfig::default();
        let assigner = crate::meta::replica_assigner::ReplicaAssigner::new(config);

        match assigner.assign_new_partition(partition_key, &nodes, &partition_counts) {
            Some(replica) => {
                let leader_node_id = replica.leader_node_id.clone();
                let standby_node_ids = replica.standby_node_ids.clone();
                let replication_factor = replica.replication_factor;

                // Store the replica
                let storage_key = replica.storage_key();
                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&replica).unwrap(),
                );

                // Publish event
                let payload = serde_json::json!({
                    "partition_key": partition_key,
                    "leader_node_id": &leader_node_id,
                    "standby_node_ids": &standby_node_ids,
                    "replication_factor": replication_factor,
                    "auto_assigned": true,
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaAssigned,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    "meta",
                );

                info!(
                    "Auto-assigned replica for partition {}: leader={}, standbys={:?}, rf={}",
                    partition_key, leader_node_id, standby_node_ids, replication_factor
                );

                MetaResponse::ReplicaAutoAssigned {
                    partition_key: partition_key.to_string(),
                    leader_node_id,
                    standby_node_ids,
                    replication_factor,
                }
            }
            None => MetaResponse::NoDataNodesAvailable,
        }
    }

    /// Rebalance replicas when a new node joins
    ///
    /// Adds missing standbys to under-replicated partitions.
    fn rebalance_on_node_join(&self, node_id: &str) -> MetaResponse {
        let node = match self.get_node(node_id) {
            Some(n) => n,
            None => return MetaResponse::Error {
                message: format!("Node {} not found", node_id),
            },
        };

        if node.role != NodeRole::Data || node.state != NodeState::Active {
            return MetaResponse::RebalanceCompleted { updated_count: 0 };
        }

        let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let replicas = self.list_replicas();

        let config = crate::meta::config::ReplicationAssignmentConfig::default();
        let assigner = crate::meta::replica_assigner::ReplicaAssigner::new(config);

        let updates = assigner.rebalance_on_node_join(&node, &replicas, &nodes);
        let updated_count = updates.len();

        for updated in updates {
            let storage_key = updated.storage_key();
            let _ = self.db.insert(
                storage_key.as_bytes(),
                bincode::serialize(&updated).unwrap(),
            );

            // Publish event
            let payload = serde_json::json!({
                "partition_key": &updated.partition_key,
                "leader_node_id": &updated.leader_node_id,
                "standby_node_ids": &updated.standby_node_ids,
                "rebalance_reason": "node_join",
                "new_node": node_id,
            });
            self.publish_event(
                "replicas",
                EventType::ReplicaAssigned,
                serde_json::to_vec(&payload).unwrap_or_default(),
                "meta",
            );

            info!(
                "Rebalanced replica {} after node {} join: added standby",
                updated.partition_key, node_id
            );
        }

        MetaResponse::RebalanceCompleted { updated_count }
    }

    // ========================================================================
    // Cold Standby Architecture Operations
    // ========================================================================

    /// Count Data nodes by mode (Primary vs Standby)
    fn count_data_nodes_by_mode(&self) -> (usize, usize) {
        let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let primary_count = nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary))
            .count();
        let standby_count = nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .count();
        (primary_count, standby_count)
    }

    /// Select a Standby node to promote when a Primary fails
    ///
    /// Selection criteria:
    /// 1. Must be in Active state
    /// 2. Must be in Standby mode
    /// 3. Prefer nodes in different zones for fault tolerance
    fn select_standby_to_promote(&self, failed_primary_id: &str) -> Option<NodeInfo> {
        // Get the failed primary's info for zone comparison
        let failed_primary = self.get_node(failed_primary_id);

        // Get all active Standby nodes
        let standbys: Vec<NodeInfo> = self
            .list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
            .into_iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        if standbys.is_empty() {
            return None;
        }

        // If we have zone info for the failed primary, prefer standbys in different zones
        if let Some(ref failed) = failed_primary {
            // First, try to find a standby in a different zone
            let different_zone = standbys
                .iter()
                .find(|s| s.is_different_zone(failed));
            if let Some(standby) = different_zone {
                return Some(standby.clone());
            }
        }

        // Fall back to the first available standby
        standbys.into_iter().next()
    }

    /// Promote a Standby data node to Primary
    ///
    /// Called when a Primary node fails and we need a replacement.
    /// The promoted Standby:
    /// 1. Changes mode from Standby to Primary
    /// 2. Takes over partition leadership from the failed Primary
    /// 3. Starts serving queries and flushing to Parquet
    fn promote_data_node(
        &self,
        standby_node_id: &str,
        failed_primary_id: Option<&str>,
    ) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, standby_node_id);

        // Get the current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: standby_node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Verify it's a Standby data node
        if node.role != NodeRole::Data {
            return MetaResponse::Error {
                message: format!("Node {} is not a Data node (role={:?})", standby_node_id, node.role),
            };
        }

        let old_mode = node.mode.unwrap_or(DataNodeMode::Primary);
        if old_mode != DataNodeMode::Standby {
            return MetaResponse::Error {
                message: format!(
                    "Node {} is not a Standby (mode={:?})",
                    standby_node_id, old_mode
                ),
            };
        }

        // Promote to Primary
        node.mode = Some(DataNodeMode::Primary);
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Promoted Data node {} from Standby to Primary (failed_primary={:?})",
            standby_node_id, failed_primary_id
        );

        // If a failed Primary was specified, transfer its partition leadership
        if let Some(failed_id) = failed_primary_id {
            let replicas_to_transfer = self.get_replicas_by_leader(failed_id);
            for replica in replicas_to_transfer {
                // Re-assign leader to the promoted node
                let mut updated = replica.clone();
                updated.leader_node_id = standby_node_id.to_string();
                // Remove the promoted node from standbys if it was there
                updated.standby_node_ids.retain(|id| id != standby_node_id);
                // Add the old leader to standbys (it may recover)
                if !updated.standby_node_ids.contains(&failed_id.to_string()) {
                    updated.standby_node_ids.push(failed_id.to_string());
                }
                updated.leader_last_seen = current_timestamp_ms();

                let storage_key = updated.storage_key();
                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&updated).unwrap(),
                );

                info!(
                    "Transferred partition {} leadership from {} to {}",
                    updated.partition_key, failed_id, standby_node_id
                );
            }
        }

        // Publish mode change event
        let payload = serde_json::json!({
            "node_id": standby_node_id,
            "old_mode": format!("{:?}", old_mode),
            "new_mode": "Primary",
            "failed_primary_id": failed_primary_id,
        });
        self.publish_event(
            "nodes",
            EventType::DataNodeModeChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            standby_node_id,
        );

        MetaResponse::DataNodePromoted {
            node_id: standby_node_id.to_string(),
            old_mode,
            new_mode: DataNodeMode::Primary,
        }
    }

    /// Update a data node's mode (Primary/Standby)
    fn update_data_node_mode(&self, node_id: &str, mode: DataNodeMode) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get the current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Verify it's a Data node
        if node.role != NodeRole::Data {
            return MetaResponse::Error {
                message: format!("Node {} is not a Data node", node_id),
            };
        }

        let old_mode = node.mode;
        node.mode = Some(mode);
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Updated Data node {} mode: {:?} -> {:?}",
            node_id, old_mode, mode
        );

        // Publish mode change event
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_mode": old_mode.map(|m| format!("{:?}", m)),
            "new_mode": format!("{:?}", mode),
        });
        self.publish_event(
            "nodes",
            EventType::DataNodeModeChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::DataNodeModeUpdated {
            node_id: node_id.to_string(),
            mode,
        }
    }

    // ========================================================================
    // Scale-In (Retirement) Operations
    // ========================================================================

    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins partition reassignment to other nodes
    /// - Node continues serving existing queries until Drained
    fn start_retire(&self, node_id: &str) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Check if node is in a state that can be retired
        if node.state != NodeState::Active {
            return MetaResponse::NodeNotRetirable {
                node_id: node_id.to_string(),
                current_state: node.state,
            };
        }

        // Count partitions this node leads (for Primary) or replicates (for Standby)
        let partitions_to_reassign = if node.mode == Some(DataNodeMode::Primary) {
            self.get_replicas_by_leader(node_id).len()
        } else {
            // For Standby, count partitions where this node is a standby
            self.list_replicas()
                .iter()
                .filter(|r| r.standby_node_ids.contains(&node_id.to_string()))
                .count()
        };

        // Check if there are other nodes to take over
        let active_nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let other_primaries: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.node_id != node_id && n.mode == Some(DataNodeMode::Primary))
            .collect();

        if node.mode == Some(DataNodeMode::Primary) && other_primaries.is_empty() {
            return MetaResponse::CannotRetireNoAlternative {
                node_id: node_id.to_string(),
                reason: "No other Primary nodes available to take over partitions".to_string(),
            };
        }

        // Update node state to Retiring
        node.state = NodeState::Retiring;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Starting retirement of node {} ({:?}): {} partitions to reassign",
            node_id,
            node.mode,
            partitions_to_reassign
        );

        // Publish event for Router to stop routing to this node
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Active",
            "new_state": "Retiring",
            "partitions_to_reassign": partitions_to_reassign,
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireStarted {
            node_id: node_id.to_string(),
            partitions_to_reassign,
        }
    }

    /// Mark a node as fully drained (scale-in complete)
    fn complete_retire(&self, node_id: &str) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Can only complete retirement if currently Retiring
        if node.state != NodeState::Retiring {
            return MetaResponse::Error {
                message: format!(
                    "Node {} is not in Retiring state (current: {:?})",
                    node_id, node.state
                ),
            };
        }

        // Update node state to Drained
        node.state = NodeState::Drained;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!(
            "Retirement complete for node {}: now Drained, safe to stop",
            node_id
        );

        // Publish event
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Retiring",
            "new_state": "Drained",
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireCompleted {
            node_id: node_id.to_string(),
        }
    }

    /// Reassign a partition from a retiring node to another Primary
    ///
    /// For Object Store clusters:
    /// - New leader takes over writes for this partition
    /// - Old data remains in S3 at the retiring node's prefix
    /// - New leader can read old data from S3 (any node can read any prefix)
    fn reassign_partition_for_retire(
        &self,
        partition_key: &str,
        from_node_id: &str,
        to_node_id: &str,
    ) -> MetaResponse {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);

        // Get current replica info
        let replica_data = match self.db.get(storage_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::ReplicaNotFound {
                    partition_key: partition_key.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut replica: PartitionReplica = match bincode::deserialize(&replica_data) {
            Ok(r) => r,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Verify the current leader is the retiring node
        if replica.leader_node_id != from_node_id {
            return MetaResponse::Error {
                message: format!(
                    "Partition {} leader is {}, not {}",
                    partition_key, replica.leader_node_id, from_node_id
                ),
            };
        }

        // Verify target node exists and is a Primary
        let target_node = match self.get_node(to_node_id) {
            Some(n) => n,
            None => {
                return MetaResponse::DataNodeNotFound {
                    node_id: to_node_id.to_string(),
                }
            }
        };

        if target_node.mode != Some(DataNodeMode::Primary) {
            return MetaResponse::Error {
                message: format!("Target node {} is not a Primary", to_node_id),
            };
        }

        if target_node.state != NodeState::Active {
            return MetaResponse::Error {
                message: format!("Target node {} is not Active", to_node_id),
            };
        }

        // Update leader
        let old_leader = replica.leader_node_id.clone();
        replica.leader_node_id = to_node_id.to_string();

        // Remove target from standbys if it was there
        replica.standby_node_ids.retain(|id| id != to_node_id);

        // Don't add the retiring node as standby (it's going away)
        // Just remove it completely
        replica.standby_node_ids.retain(|id| id != from_node_id);

        replica.leader_last_seen = current_timestamp_ms();

        // Save updated replica
        let _ = self.db.insert(
            storage_key.as_bytes(),
            bincode::serialize(&replica).unwrap(),
        );

        info!(
            "Reassigned partition {} from retiring node {} to {}",
            partition_key, from_node_id, to_node_id
        );

        // Publish event for Router cache invalidation
        let payload = serde_json::json!({
            "partition_key": partition_key,
            "old_leader_id": old_leader,
            "new_leader_id": to_node_id,
            "reason": "retire",
        });
        self.publish_event(
            "replicas",
            EventType::ReplicaPromoted,
            serde_json::to_vec(&payload).unwrap_or_default(),
            to_node_id,
        );

        MetaResponse::PartitionReassigned {
            partition_key: partition_key.to_string(),
            from_node_id: from_node_id.to_string(),
            to_node_id: to_node_id.to_string(),
        }
    }

    /// Cancel a node retirement (scale-in abort)
    fn cancel_retire(&self, node_id: &str) -> MetaResponse {
        let node_key = format!("{}{}", keys::PREFIX_NODE, node_id);

        // Get current node info
        let node_data = match self.db.get(node_key.as_bytes()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                return MetaResponse::DataNodeNotFound {
                    node_id: node_id.to_string(),
                }
            }
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Database error: {}", e),
                }
            }
        };

        let mut node: NodeInfo = match bincode::deserialize(&node_data) {
            Ok(n) => n,
            Err(e) => {
                return MetaResponse::Error {
                    message: format!("Deserialize error: {}", e),
                }
            }
        };

        // Can only cancel if currently Retiring (not yet Drained)
        if node.state != NodeState::Retiring {
            return MetaResponse::Error {
                message: format!(
                    "Cannot cancel retirement: node {} is {:?}, not Retiring",
                    node_id, node.state
                ),
            };
        }

        // Restore to Active state
        node.state = NodeState::Active;
        let _ = self
            .db
            .insert(node_key.as_bytes(), bincode::serialize(&node).unwrap());

        info!("Cancelled retirement of node {}: restored to Active", node_id);

        // Publish event
        let payload = serde_json::json!({
            "node_id": node_id,
            "old_state": "Retiring",
            "new_state": "Active",
            "reason": "cancelled",
        });
        self.publish_event(
            "nodes",
            EventType::NodeStateChanged,
            serde_json::to_vec(&payload).unwrap_or_default(),
            node_id,
        );

        MetaResponse::RetireCancelled {
            node_id: node_id.to_string(),
        }
    }

    /// Update retirement status (progress tracking)
    fn update_retire_status(
        &self,
        node_id: &str,
        partitions_reassigned: usize,
        memtable_rows_remaining: u64,
    ) -> MetaResponse {
        // For now, just log the progress
        // A more complete implementation would store RetireStatus separately
        info!(
            "Retirement progress for {}: partitions_reassigned={}, memtable_rows={}",
            node_id, partitions_reassigned, memtable_rows_remaining
        );

        // Calculate progress (approximate)
        let replicas = self.get_replicas_by_leader(node_id);
        let total = replicas.len() + partitions_reassigned;
        let progress = if total > 0 {
            ((partitions_reassigned as f64 / total as f64) * 100.0) as u8
        } else {
            100
        };

        MetaResponse::RetireStatusUpdated {
            node_id: node_id.to_string(),
            progress_percent: progress,
        }
    }

    /// Remove a standby from all partition replicas (for Standby scale-in)
    fn remove_standby_from_replicas(&self, standby_node_id: &str) -> MetaResponse {
        let replicas = self.list_replicas();
        let mut affected_count = 0;

        for replica in replicas {
            if replica.standby_node_ids.contains(&standby_node_id.to_string()) {
                let storage_key = replica.storage_key();

                let mut updated = replica.clone();
                updated.standby_node_ids.retain(|id| id != standby_node_id);

                let _ = self.db.insert(
                    storage_key.as_bytes(),
                    bincode::serialize(&updated).unwrap(),
                );

                affected_count += 1;

                // Publish event
                let payload = serde_json::json!({
                    "partition_key": updated.partition_key,
                    "removed_standby": standby_node_id,
                    "remaining_standbys": updated.standby_node_ids,
                });
                self.publish_event(
                    "replicas",
                    EventType::ReplicaStateChanged,
                    serde_json::to_vec(&payload).unwrap_or_default(),
                    "meta",
                );
            }
        }

        info!(
            "Removed standby {} from {} partition replicas",
            standby_node_id, affected_count
        );

        MetaResponse::StandbyRemovedFromReplicas {
            standby_node_id: standby_node_id.to_string(),
            partitions_affected: affected_count,
        }
    }

    /// Recommend nodes for scale-in based on load and redundancy
    ///
    /// Selection criteria for Primaries:
    /// - Prefer nodes with fewer partitions (least disruption)
    /// - Ensure at least one Primary remains
    ///
    /// Selection criteria for Standbys:
    /// - Prefer nodes with most redundancy (other standbys cover same partitions)
    /// - Ensure minimum standby coverage
    pub fn recommend_scale_in(
        &self,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> ScaleInRecommendation {
        let mut recommendation = ScaleInRecommendation::new();

        let active_nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let partition_counts = self.get_partition_counts_per_node();

        // Collect Primary and Standby nodes
        let mut primaries: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary))
            .collect();
        let mut standbys: Vec<_> = active_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        // Sort Primaries by partition count (ascending - prefer least-loaded)
        primaries.sort_by_key(|n| partition_counts.get(&n.node_id).unwrap_or(&0));

        // For Standbys, we'd ideally sort by redundancy, but for now sort by partition count
        standbys.sort_by_key(|n| partition_counts.get(&n.node_id).unwrap_or(&0));

        // Recommend Primaries to retire
        let primaries_available = primaries.len();
        if reduce_primaries >= primaries_available {
            recommendation.add_warning(format!(
                "Cannot reduce {} Primaries - only {} available (need at least 1)",
                reduce_primaries, primaries_available
            ));
        } else {
            for node in primaries.iter().take(reduce_primaries) {
                let count = partition_counts.get(&node.node_id).unwrap_or(&0);
                recommendation.add_primary(
                    node.node_id.clone(),
                    format!("lowest_partition_count ({} partitions)", count),
                );
            }
        }

        // Recommend Standbys to retire
        let standbys_available = standbys.len();
        if reduce_standbys > standbys_available {
            recommendation.add_warning(format!(
                "Cannot reduce {} Standbys - only {} available",
                reduce_standbys, standbys_available
            ));
        }
        for node in standbys.iter().take(reduce_standbys.min(standbys_available)) {
            let count = partition_counts.get(&node.node_id).unwrap_or(&0);
            recommendation.add_standby(
                node.node_id.clone(),
                format!("standby_with_coverage ({} partitions)", count),
            );
        }

        recommendation
    }

    /// Get partition count per node for load balancing
    fn get_partition_counts_per_node(&self) -> std::collections::HashMap<String, usize> {
        let replicas = self.list_replicas();
        let mut counts = std::collections::HashMap::new();

        for replica in replicas {
            // Count leader
            *counts.entry(replica.leader_node_id.clone()).or_insert(0) += 1;
            // Count standbys
            for standby_id in &replica.standby_node_ids {
                *counts.entry(standby_id.clone()).or_insert(0) += 1;
            }
        }

        counts
    }

    /// Get partition replica info
    pub fn get_replica(&self, partition_key: &str) -> Option<PartitionReplica> {
        let storage_key = format!("{}{}", keys::PREFIX_REPLICA, partition_key);
        self.db
            .get(storage_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// List all partition replicas
    pub fn list_replicas(&self) -> Vec<PartitionReplica> {
        let prefix = keys::PREFIX_REPLICA;
        let mut replicas = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(replica) = bincode::deserialize::<PartitionReplica>(&v) {
                    replicas.push(replica);
                }
            }
        }

        replicas
    }

    /// Get replicas by leader node ID
    pub fn get_replicas_by_leader(&self, leader_node_id: &str) -> Vec<PartitionReplica> {
        self.list_replicas()
            .into_iter()
            .filter(|r| r.leader_node_id == leader_node_id)
            .collect()
    }

    // ========================================================================
    // Query Operations (read-only)
    // ========================================================================

    /// List all nodes (optionally filtered by role and state)
    pub fn list_nodes(&self, role: Option<NodeRole>, state: Option<NodeState>) -> Vec<NodeInfo> {
        let prefix = keys::PREFIX_NODE;
        let mut nodes = Vec::new();
        let mut scanned = 0;
        let mut deserialized = 0;
        let mut matched = 0;

        // Force sync to see latest writes from other handles
        let _ = self.db.flush();

        for item in self.db.scan_prefix(prefix) {
            scanned += 1;
            if let Ok((k, v)) = item {
                match bincode::deserialize::<NodeInfo>(&v) {
                    Ok(info) => {
                        deserialized += 1;
                        let role_match = role.map_or(true, |r| info.role == r);
                        let state_match = state.map_or(true, |s| info.state == s);
                        if role_match && state_match {
                            matched += 1;
                            nodes.push(info);
                        }
                    }
                    Err(e) => {
                        let key_str = String::from_utf8_lossy(&k);
                        tracing::warn!(
                            "list_nodes: failed to deserialize node key={}, len={}, error={:?}",
                            key_str, v.len(), e
                        );
                    }
                }
            }
        }

        tracing::info!(
            "list_nodes: role={:?}, state={:?}, scanned={}, deserialized={}, matched={}",
            role, state, scanned, deserialized, matched
        );

        nodes
    }

    /// Get a specific node by ID
    pub fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        let key = format!("{}{}", keys::PREFIX_NODE, node_id);
        self.db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// List files for a partition
    pub fn list_files_for_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition: &str,
    ) -> Vec<FileInfo> {
        let prefix = FileInfo::partition_prefix(catalog, schema, table, partition);
        let mut files = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            if let Ok((_, v)) = item {
                if let Ok(file) = bincode::deserialize::<FileInfo>(&v) {
                    files.push(file);
                }
            }
        }

        files
    }

    /// List files for a table
    pub fn list_files_for_table(&self, catalog: &str, schema: &str, table: &str) -> Vec<FileInfo> {
        let prefix = FileInfo::table_prefix(catalog, schema, table);
        let mut files = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            if let Ok((_, v)) = item {
                if let Ok(file) = bincode::deserialize::<FileInfo>(&v) {
                    files.push(file);
                }
            }
        }

        files
    }

    /// Get lock info
    pub fn get_lock(&self, key: &str) -> Option<LockInfo> {
        let storage_key = format!("{}{}", keys::PREFIX_LOCK, key);
        self.db
            .get(storage_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// Get leader info
    pub fn get_leader(&self, election: &str) -> Option<LeaderInfo> {
        let storage_key = format!("{}{}", keys::PREFIX_LEADER, election);
        self.db
            .get(storage_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
            .filter(|l: &LeaderInfo| !l.is_expired())
    }

    /// Get events from a starting sequence
    pub fn get_events_from(&self, start_seq: u64, limit: usize) -> Vec<Event> {
        let start_key = format!("{}{:020}", keys::PREFIX_EVENT, start_seq);
        let mut events = Vec::new();

        for item in self.db.range(start_key.as_bytes()..) {
            if let Ok((k, v)) = item {
                // Check if still in events prefix
                if !k.starts_with(keys::PREFIX_EVENT.as_bytes()) {
                    break;
                }
                if let Ok(event) = bincode::deserialize::<Event>(&v) {
                    events.push(event);
                    if events.len() >= limit {
                        break;
                    }
                }
            }
        }

        events
    }

    /// Get current event sequence
    pub fn current_sequence(&self) -> u64 {
        self.event_sequence.load(Ordering::SeqCst)
    }

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
