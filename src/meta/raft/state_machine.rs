//! State machine implementation using sled
//!
//! This is a simplified implementation that provides the core metadata storage
//! functionality. Full Raft replication will be added in a later iteration.

use crate::meta::types::*;
use sled::Db;
use std::sync::atomic::{AtomicU64, Ordering};

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

        Self {
            db,
            event_sequence: AtomicU64::new(event_sequence),
            fencing_counter: AtomicU64::new(fencing_counter),
            lease_counter: AtomicU64::new(0),
        }
    }

    /// Open or create a sled database at the given path
    pub fn open(path: &std::path::Path) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self::new(db))
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

            MetaCommand::AcquireLock { key, holder, ttl_ms } => {
                self.acquire_lock(&key, &holder, ttl_ms)
            }

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
        }
    }

    // ========================================================================
    // Node Operations
    // ========================================================================

    fn register_node(&self, info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
        let key = format!("{}{}", keys::PREFIX_NODE, info.node_id);
        let lease_id = self.next_lease_id();

        // Create lease
        let lease = LeaseInfo::new(lease_id, info.node_id.clone(), lease_ttl_ms);
        let _ = self.db.insert(
            lease.storage_key().as_bytes(),
            bincode::serialize(&lease).unwrap(),
        );

        // Store node info
        let _ = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&info).unwrap());

        // Publish event
        let event_payload = serde_json::to_vec(&info).unwrap_or_default();
        self.publish_event(
            "nodes",
            EventType::NodeJoined,
            event_payload,
            &info.node_id,
        );

        MetaResponse::NodeRegistered { lease_id }
    }

    fn heartbeat(&self, node_id: &str, lease_id: u64, timestamp: i64) -> MetaResponse {
        // Update lease
        let lease_key = format!("{}{}", keys::PREFIX_LEASE, lease_id);
        if let Ok(Some(data)) = self.db.get(lease_key.as_bytes()) {
            if let Ok(mut lease) = bincode::deserialize::<LeaseInfo>(&data) {
                if lease.node_id == node_id {
                    lease.renew();
                    let _ = self.db.insert(
                        lease_key.as_bytes(),
                        bincode::serialize(&lease).unwrap(),
                    );
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
        let _ = self.db.insert(
            storage_key.as_bytes(),
            bincode::serialize(&lock).unwrap(),
        );

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
        let _ = self.db.insert(
            storage_key.as_bytes(),
            bincode::serialize(&leader).unwrap(),
        );

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

        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );

        MetaResponse::EventPublished { sequence: seq }
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
    // Query Operations (read-only)
    // ========================================================================

    /// List all nodes (optionally filtered by role and state)
    pub fn list_nodes(&self, role: Option<NodeRole>, state: Option<NodeState>) -> Vec<NodeInfo> {
        let prefix = keys::PREFIX_NODE;
        let mut nodes = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            if let Ok((_, v)) = item {
                if let Ok(info) = bincode::deserialize::<NodeInfo>(&v) {
                    let role_match = role.map_or(true, |r| info.role == r);
                    let state_match = state.map_or(true, |s| info.state == s);
                    if role_match && state_match {
                        nodes.push(info);
                    }
                }
            }
        }

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
    pub fn list_files_for_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Vec<FileInfo> {
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
}
