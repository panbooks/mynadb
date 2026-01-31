//! Raft log store implementation using sled
//!
//! This module provides persistent storage for Raft log entries, vote state,
//! and committed log ID using the sled embedded database.

use super::{Entry, LogId, NodeId, TypeConfig, Vote};
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{AnyError, OptionalSend, StorageError, StorageIOError};
use parking_lot::RwLock;
use sled::Db;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;
use tracing::{debug, error};

const KEY_VOTE: &[u8] = b"/raft/vote";
const KEY_COMMITTED: &[u8] = b"/raft/committed";
const KEY_LAST_PURGED: &[u8] = b"/raft/last_purged";
const PREFIX_LOG: &str = "/raft/log/";

/// Shared state for the log store that must be consistent across clones
struct SledLogStoreInner {
    /// Sled database instance
    db: Db,
    /// Cached vote state
    vote: RwLock<Option<Vote>>,
    /// Cached committed log ID
    committed: RwLock<Option<LogId>>,
    /// Cached last purged log ID
    last_purged: RwLock<Option<LogId>>,
}

/// Raft log store backed by sled embedded database
///
/// This store persists:
/// - Log entries (indexed by log index)
/// - Vote state (current voted-for candidate)
/// - Committed log ID
/// - Last purged log ID (for log compaction)
///
/// Note: Uses Arc internally to ensure all clones share the same state.
/// This is critical for openraft's defensive checks to work correctly.
#[derive(Clone)]
pub struct SledLogStore {
    inner: Arc<SledLogStoreInner>,
}

impl SledLogStore {
    /// Create a new log store with the given sled database
    pub fn new(db: Db) -> Self {
        // Load persisted state with error logging
        let vote = match db.get(KEY_VOTE) {
            Ok(Some(v)) => match bincode::deserialize(&v) {
                Ok(vote) => Some(vote),
                Err(e) => {
                    error!("Failed to deserialize vote ({} bytes): {:?}", v.len(), e);
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                error!("Failed to read vote from sled: {:?}", e);
                None
            }
        };

        let committed = match db.get(KEY_COMMITTED) {
            Ok(Some(v)) => match bincode::deserialize(&v) {
                Ok(c) => Some(c),
                Err(e) => {
                    error!(
                        "Failed to deserialize committed ({} bytes): {:?}",
                        v.len(),
                        e
                    );
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                error!("Failed to read committed from sled: {:?}", e);
                None
            }
        };

        let last_purged = match db.get(KEY_LAST_PURGED) {
            Ok(Some(v)) => match bincode::deserialize(&v) {
                Ok(lp) => Some(lp),
                Err(e) => {
                    error!(
                        "Failed to deserialize last_purged ({} bytes): {:?}",
                        v.len(),
                        e
                    );
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                error!("Failed to read last_purged from sled: {:?}", e);
                None
            }
        };

        // Count existing log entries
        let log_count = db.scan_prefix(PREFIX_LOG).count();

        debug!(
            "SledLogStore initialized: vote={:?}, committed={:?}, last_purged={:?}, log_entries={}",
            vote, committed, last_purged, log_count
        );

        Self {
            inner: Arc::new(SledLogStoreInner {
                db,
                vote: RwLock::new(vote),
                committed: RwLock::new(committed),
                last_purged: RwLock::new(last_purged),
            }),
        }
    }

    /// Open or create a sled database at the given path
    pub fn open(path: &std::path::Path) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self::new(db))
    }

    /// Generate the storage key for a log entry
    fn log_key(index: u64) -> String {
        format!("{}{:020}", PREFIX_LOG, index)
    }

    /// Get the first log index
    fn first_log_index(&self) -> Option<u64> {
        self.inner
            .db
            .scan_prefix(PREFIX_LOG)
            .next()
            .and_then(|r| r.ok())
            .and_then(|(k, _)| {
                std::str::from_utf8(&k)
                    .ok()
                    .and_then(|s| s.strip_prefix(PREFIX_LOG))
                    .and_then(|s| s.parse().ok())
            })
    }

    /// Get the last log index
    fn last_log_index(&self) -> Option<u64> {
        self.inner
            .db
            .scan_prefix(PREFIX_LOG)
            .last()
            .and_then(|r| r.ok())
            .and_then(|(k, _)| {
                std::str::from_utf8(&k)
                    .ok()
                    .and_then(|s| s.strip_prefix(PREFIX_LOG))
                    .and_then(|s| s.parse().ok())
            })
    }

    /// Get the last log entry
    fn last_log_entry(&self) -> Option<Entry> {
        if let Some(index) = self.last_log_index() {
            let key = Self::log_key(index);
            self.inner
                .db
                .get(key.as_bytes())
                .ok()
                .flatten()
                .and_then(|v| bincode::deserialize(&v).ok())
        } else {
            None
        }
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

// Clone is derived on SledLogStore, sharing the inner Arc

impl RaftLogStorage<TypeConfig> for SledLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged_log_id = self.inner.last_purged.read().clone();

        let last_log_id = self.last_log_entry().map(|e| e.log_id);

        // Also get actual indices for debugging
        let first_idx = self.first_log_index();
        let last_idx = self.last_log_index();

        tracing::info!(
            "get_log_state: last_purged={:?}, last_log={:?}, actual_first={:?}, actual_last={:?}",
            last_purged_log_id,
            last_log_id,
            first_idx,
            last_idx
        );

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError<NodeId>> {
        let data =
            bincode::serialize(vote).map_err(|e| Self::storage_error(e, "serialize vote"))?;

        self.inner
            .db
            .insert(KEY_VOTE, data)
            .map_err(|e| Self::storage_error(e, "insert vote"))?;

        self.inner
            .db
            .flush()
            .map_err(|e| Self::storage_error(e, "flush vote"))?;

        *self.inner.vote.write() = Some(vote.clone());
        debug!("Saved vote: {:?}", vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(ref c) = committed {
            let data =
                bincode::serialize(c).map_err(|e| Self::storage_error(e, "serialize committed"))?;

            self.inner
                .db
                .insert(KEY_COMMITTED, data)
                .map_err(|e| Self::storage_error(e, "insert committed"))?;
        }

        *self.inner.committed.write() = committed;
        debug!("Saved committed: {:?}", committed);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError<NodeId>> {
        Ok(self.inner.vote.read().clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        let mut count = 0;
        let mut indices = Vec::new();
        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            indices.push(entry.log_id.index);

            let data = bincode::serialize(&entry)
                .map_err(|e| Self::storage_error(e, "serialize entry"))?;

            self.inner
                .db
                .insert(key.as_bytes(), data)
                .map_err(|e| Self::storage_error(e, "insert entry"))?;
            count += 1;
        }

        self.inner
            .db
            .flush()
            .map_err(|e| Self::storage_error(e, "flush entries"))?;

        debug!("Appended {} entries at indices {:?}", count, indices);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
        // Remove all entries from log_id.index onwards
        let start_key = Self::log_key(log_id.index);
        let mut keys_to_remove = Vec::new();

        for item in self.inner.db.range(start_key.as_bytes()..) {
            if let Ok((k, _)) = item {
                if !k.starts_with(PREFIX_LOG.as_bytes()) {
                    break;
                }
                keys_to_remove.push(k);
            }
        }

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.inner
                .db
                .remove(key)
                .map_err(|e| Self::storage_error(e, "remove entry"))?;
        }

        debug!("Truncated {} entries from index {}", count, log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
        // Log current state before purge
        let first_before = self.first_log_index();
        let last_before = self.last_log_index();
        tracing::info!(
            "PURGE: BEFORE - first={:?}, last={:?}, purging up to {:?}",
            first_before,
            last_before,
            log_id
        );

        // Remove all entries up to and including log_id.index
        let end_key = Self::log_key(log_id.index + 1);
        let mut keys_to_remove = Vec::new();

        for item in self.inner.db.scan_prefix(PREFIX_LOG) {
            if let Ok((k, _)) = item {
                if k.as_ref() >= end_key.as_bytes() {
                    break;
                }
                keys_to_remove.push(k);
            }
        }

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.inner
                .db
                .remove(key)
                .map_err(|e| Self::storage_error(e, "purge entry"))?;
        }

        // Update last purged
        let data = bincode::serialize(&log_id)
            .map_err(|e| Self::storage_error(e, "serialize last_purged"))?;
        self.inner
            .db
            .insert(KEY_LAST_PURGED, data)
            .map_err(|e| Self::storage_error(e, "insert last_purged"))?;

        *self.inner.last_purged.write() = Some(log_id);

        // Log state after purge
        let first_after = self.first_log_index();
        let last_after = self.last_log_index();
        tracing::info!(
            "PURGE: AFTER - first={:?}, last={:?}, purged {} entries up to index {}",
            first_after,
            last_after,
            count,
            log_id.index
        );
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for SledLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => Some(n + 1),
            std::ops::Bound::Excluded(&n) => Some(n),
            std::ops::Bound::Unbounded => None,
        };

        let mut entries = Vec::new();

        // Use scan_prefix to iterate through log entries
        for item in self.inner.db.scan_prefix(PREFIX_LOG) {
            if let Ok((k, v)) = item {
                // Parse the index from the key
                let key_str = std::str::from_utf8(&k).unwrap_or("?");
                let idx: u64 = key_str
                    .strip_prefix(PREFIX_LOG)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(u64::MAX);

                // Skip entries before our start
                if idx < start {
                    continue;
                }

                // Stop at end
                if let Some(end_idx) = end {
                    if idx >= end_idx {
                        break;
                    }
                }

                // Deserialize Entry using bincode
                match bincode::deserialize::<Entry>(&v) {
                    Ok(entry) => {
                        entries.push(entry);
                    }
                    Err(e) => {
                        error!("Failed to deserialize entry at idx={}: {:?}", idx, e);
                    }
                }
            }
        }

        debug!("Read {} entries for range {:?}", entries.len(), range);
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::MetaCommand;
    use openraft::EntryPayload;
    use tempfile::tempdir;

    /// Create a test entry with the given index and term
    fn create_test_entry(index: u64, term: u64) -> Entry {
        Entry {
            log_id: LogId::new(openraft::LeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    /// Create a test entry with a Normal payload
    fn create_normal_entry(index: u64, term: u64, cmd: MetaCommand) -> Entry {
        Entry {
            log_id: LogId::new(openraft::LeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    impl SledLogStore {
        /// Test helper: append entries directly to storage without callback
        #[cfg(test)]
        async fn test_append(&mut self, entries: Vec<Entry>) -> Result<(), StorageError<NodeId>> {
            let mut count = 0;
            for entry in entries {
                let key = Self::log_key(entry.log_id.index);
                let data = bincode::serialize(&entry)
                    .map_err(|e| Self::storage_error(e, "serialize entry"))?;
                self.inner
                    .db
                    .insert(key.as_bytes(), data)
                    .map_err(|e| Self::storage_error(e, "insert entry"))?;
                count += 1;
            }
            self.inner
                .db
                .flush()
                .map_err(|e| Self::storage_error(e, "flush entries"))?;
            debug!("Test appended {} entries", count);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_log_store_vote() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Initially no vote
        assert!(store.read_vote().await.unwrap().is_none());

        // Save a vote
        let vote = openraft::Vote::new(1, 2);
        store.save_vote(&vote).await.unwrap();

        // Read it back
        let read_vote = store.read_vote().await.unwrap();
        assert_eq!(read_vote, Some(vote));
    }

    #[tokio::test]
    async fn test_log_store_vote_persistence() {
        let dir = tempdir().unwrap();

        // First: save a vote
        {
            let mut store = SledLogStore::open(dir.path()).unwrap();
            let vote = openraft::Vote::new(5, 10);
            store.save_vote(&vote).await.unwrap();
        }

        // Second: reopen and verify vote persisted
        {
            let mut store = SledLogStore::open(dir.path()).unwrap();
            let read_vote = store.read_vote().await.unwrap();
            assert!(read_vote.is_some());
            let vote = read_vote.unwrap();
            assert_eq!(vote.leader_id().term, 5);
            assert_eq!(vote.leader_id().node_id, 10);
        }
    }

    #[tokio::test]
    async fn test_log_store_append_entries() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Initially empty
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());

        // Append some entries
        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 1),
        ];
        store.test_append(entries).await.unwrap();

        // Verify state
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_some());
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_log_store_append_normal_entries() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Append entries with actual commands
        let entries = vec![
            create_normal_entry(
                1,
                1,
                MetaCommand::RegisterFiles { files: vec![] },
            ),
            create_normal_entry(
                2,
                1,
                MetaCommand::DeregisterNode {
                    node_id: "test-node".to_string(),
                },
            ),
        ];
        store.test_append(entries).await.unwrap();

        // Read back
        let reader = store.get_log_reader().await;
        let read_entries = reader.clone().try_get_log_entries(1..3).await.unwrap();
        assert_eq!(read_entries.len(), 2);
        assert_eq!(read_entries[0].log_id.index, 1);
        assert_eq!(read_entries[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_log_store_read_entries_range() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Append entries 1-10
        let entries: Vec<_> = (1..=10).map(|i| create_test_entry(i, 1)).collect();
        store.test_append(entries).await.unwrap();

        // Test various ranges
        let mut reader = store.get_log_reader().await;

        // Read all
        let all = reader.try_get_log_entries(1..11).await.unwrap();
        assert_eq!(all.len(), 10);

        // Read subset (3..7)
        let subset = reader.try_get_log_entries(3..7).await.unwrap();
        assert_eq!(subset.len(), 4);
        assert_eq!(subset[0].log_id.index, 3);
        assert_eq!(subset[3].log_id.index, 6);

        // Read single entry
        let single = reader.try_get_log_entries(5..6).await.unwrap();
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].log_id.index, 5);

        // Read empty range
        let empty = reader.try_get_log_entries(100..101).await.unwrap();
        assert_eq!(empty.len(), 0);
    }

    #[tokio::test]
    async fn test_log_store_truncate() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Append entries 1-10
        let entries: Vec<_> = (1..=10).map(|i| create_test_entry(i, 1)).collect();
        store.test_append(entries).await.unwrap();

        // Truncate from index 6
        let truncate_log_id = LogId::new(openraft::LeaderId::new(1, 1), 6);
        store.truncate(truncate_log_id).await.unwrap();

        // Verify only entries 1-5 remain
        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..11).await.unwrap();
        assert_eq!(remaining.len(), 5);
        assert_eq!(remaining.last().unwrap().log_id.index, 5);
    }

    #[tokio::test]
    async fn test_log_store_purge() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Append entries 1-10
        let entries: Vec<_> = (1..=10).map(|i| create_test_entry(i, 1)).collect();
        store.test_append(entries).await.unwrap();

        // Purge up to index 5
        let purge_log_id = LogId::new(openraft::LeaderId::new(1, 1), 5);
        store.purge(purge_log_id).await.unwrap();

        // Verify state
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_some());
        assert_eq!(state.last_purged_log_id.unwrap().index, 5);

        // Verify only entries 6-10 remain
        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..11).await.unwrap();
        assert_eq!(remaining.len(), 5);
        assert_eq!(remaining.first().unwrap().log_id.index, 6);
    }

    #[tokio::test]
    async fn test_log_store_committed() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Initially no committed
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_none());

        // Save committed
        let committed_id = LogId::new(openraft::LeaderId::new(1, 1), 5);
        store.save_committed(Some(committed_id)).await.unwrap();

        // Verify cached value (committed is cached, not stored in log_state)
        let cached = store.inner.committed.read().clone();
        assert_eq!(cached, Some(committed_id));
    }

    #[tokio::test]
    async fn test_log_store_persistence_entries() {
        let dir = tempdir().unwrap();

        // First: append entries
        {
            let mut store = SledLogStore::open(dir.path()).unwrap();
            let entries: Vec<_> = (1..=5).map(|i| create_test_entry(i, 1)).collect();
            store.test_append(entries).await.unwrap();
        }

        // Second: reopen and verify entries persisted
        {
            let mut store = SledLogStore::open(dir.path()).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_some());
            assert_eq!(state.last_log_id.unwrap().index, 5);

            let mut reader = store.get_log_reader().await;
            let entries = reader.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(entries.len(), 5);
        }
    }

    #[tokio::test]
    async fn test_log_store_empty_range() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Empty store - reading should return empty
        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..10).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_log_store_overwrite_vote() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Save first vote
        let vote1 = openraft::Vote::new(1, 2);
        store.save_vote(&vote1).await.unwrap();

        // Overwrite with second vote
        let vote2 = openraft::Vote::new(2, 3);
        store.save_vote(&vote2).await.unwrap();

        // Verify second vote is stored
        let read_vote = store.read_vote().await.unwrap();
        assert_eq!(read_vote, Some(vote2));
    }

    #[tokio::test]
    async fn test_log_store_append_after_truncate() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Append entries 1-10
        let entries: Vec<_> = (1..=10).map(|i| create_test_entry(i, 1)).collect();
        store.test_append(entries).await.unwrap();

        // Truncate from index 5
        let truncate_log_id = LogId::new(openraft::LeaderId::new(1, 1), 5);
        store.truncate(truncate_log_id).await.unwrap();

        // Append new entries 5-8 with term 2 (simulating new leader)
        let new_entries: Vec<_> = (5..=8).map(|i| create_test_entry(i, 2)).collect();
        store.test_append(new_entries).await.unwrap();

        // Verify final state
        let mut reader = store.get_log_reader().await;
        let all = reader.try_get_log_entries(1..20).await.unwrap();
        assert_eq!(all.len(), 8); // 1-4 (term 1) + 5-8 (term 2)

        // Verify entry 5 has term 2
        let entry5 = all.iter().find(|e| e.log_id.index == 5).unwrap();
        assert_eq!(entry5.log_id.leader_id.term, 2);
    }

    #[tokio::test]
    async fn test_log_store_large_index_numbers() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Use large index numbers to test zero-padding in keys
        let entries = vec![
            create_test_entry(1_000_000, 1),
            create_test_entry(1_000_001, 1),
            create_test_entry(10_000_000, 1),
        ];
        store.test_append(entries).await.unwrap();

        // Read back
        let mut reader = store.get_log_reader().await;

        // Read specific range
        let subset = reader.try_get_log_entries(1_000_000..1_000_002).await.unwrap();
        assert_eq!(subset.len(), 2);

        // Read all
        let all = reader.try_get_log_entries(0..u64::MAX).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_log_store_membership_entry() {
        use openraft::Membership;
        use std::collections::BTreeMap;

        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Create a membership entry
        let mut nodes: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
        nodes.insert(1, openraft::BasicNode { addr: "127.0.0.1:6000".to_string() });
        nodes.insert(2, openraft::BasicNode { addr: "127.0.0.1:6100".to_string() });

        let membership = Membership::new(vec![vec![1, 2].into_iter().collect()], nodes);

        let entry = Entry {
            log_id: LogId::new(openraft::LeaderId::new(1, 1), 1),
            payload: EntryPayload::Membership(membership),
        };

        store.test_append(vec![entry]).await.unwrap();

        // Read back and verify
        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..2).await.unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0].payload {
            EntryPayload::Membership(m) => {
                // Check that we have nodes in the membership
                let nodes = m.nodes();
                assert_eq!(nodes.count(), 2);
            }
            _ => panic!("Expected membership entry"),
        }
    }

    #[tokio::test]
    async fn test_log_store_purge_all() {
        let dir = tempdir().unwrap();
        let mut store = SledLogStore::open(dir.path()).unwrap();

        // Append entries 1-5
        let entries: Vec<_> = (1..=5).map(|i| create_test_entry(i, 1)).collect();
        store.test_append(entries).await.unwrap();

        // Purge all entries
        let purge_log_id = LogId::new(openraft::LeaderId::new(1, 1), 5);
        store.purge(purge_log_id).await.unwrap();

        // Verify no entries remain
        let mut reader = store.get_log_reader().await;
        let remaining = reader.try_get_log_entries(1..10).await.unwrap();
        assert!(remaining.is_empty());

        // But last_purged should be set
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(purge_log_id));
    }

    #[tokio::test]
    async fn test_log_store_clone_shares_state() {
        let dir = tempdir().unwrap();
        let mut store1 = SledLogStore::open(dir.path()).unwrap();

        // Append using first handle
        let entries: Vec<_> = (1..=3).map(|i| create_test_entry(i, 1)).collect();
        store1.test_append(entries).await.unwrap();

        // Clone and verify the clone sees the same data
        let mut store2 = store1.clone();
        let state = store2.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);

        // Append more using clone
        let more_entries: Vec<_> = (4..=5).map(|i| create_test_entry(i, 1)).collect();
        store2.test_append(more_entries).await.unwrap();

        // Verify original sees the new entries
        let state = store1.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 5);
    }
}
