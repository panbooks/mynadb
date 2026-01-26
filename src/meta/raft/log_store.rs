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
use tracing::{debug, error};

const KEY_VOTE: &[u8] = b"/raft/vote";
const KEY_COMMITTED: &[u8] = b"/raft/committed";
const KEY_LAST_PURGED: &[u8] = b"/raft/last_purged";
const PREFIX_LOG: &str = "/raft/log/";

/// Raft log store backed by sled embedded database
///
/// This store persists:
/// - Log entries (indexed by log index)
/// - Vote state (current voted-for candidate)
/// - Committed log ID
/// - Last purged log ID (for log compaction)
pub struct SledLogStore {
    /// Sled database instance
    db: Db,
    /// Cached vote state
    vote: RwLock<Option<Vote>>,
    /// Cached committed log ID
    committed: RwLock<Option<LogId>>,
    /// Cached last purged log ID
    last_purged: RwLock<Option<LogId>>,
}

impl SledLogStore {
    /// Create a new log store with the given sled database
    pub fn new(db: Db) -> Self {
        // Load persisted state
        let vote = db
            .get(KEY_VOTE)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok());

        let committed = db
            .get(KEY_COMMITTED)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok());

        let last_purged = db
            .get(KEY_LAST_PURGED)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok());

        debug!(
            "SledLogStore initialized: vote={:?}, committed={:?}, last_purged={:?}",
            vote, committed, last_purged
        );

        Self {
            db,
            vote: RwLock::new(vote),
            committed: RwLock::new(committed),
            last_purged: RwLock::new(last_purged),
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
        self.db
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
        self.db
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
            self.db
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

impl Clone for SledLogStore {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            vote: RwLock::new(self.vote.read().clone()),
            committed: RwLock::new(self.committed.read().clone()),
            last_purged: RwLock::new(self.last_purged.read().clone()),
        }
    }
}

impl RaftLogStorage<TypeConfig> for SledLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged_log_id = self.last_purged.read().clone();

        let last_log_id = self.last_log_entry().map(|e| e.log_id);

        debug!(
            "get_log_state: last_purged={:?}, last_log={:?}",
            last_purged_log_id, last_log_id
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

        self.db
            .insert(KEY_VOTE, data)
            .map_err(|e| Self::storage_error(e, "insert vote"))?;

        self.db
            .flush()
            .map_err(|e| Self::storage_error(e, "flush vote"))?;

        *self.vote.write() = Some(vote.clone());
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

            self.db
                .insert(KEY_COMMITTED, data)
                .map_err(|e| Self::storage_error(e, "insert committed"))?;
        }

        *self.committed.write() = committed;
        debug!("Saved committed: {:?}", committed);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError<NodeId>> {
        Ok(self.vote.read().clone())
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
        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            let data = bincode::serialize(&entry)
                .map_err(|e| Self::storage_error(e, "serialize entry"))?;

            self.db
                .insert(key.as_bytes(), data)
                .map_err(|e| Self::storage_error(e, "insert entry"))?;
            count += 1;
        }

        self.db
            .flush()
            .map_err(|e| Self::storage_error(e, "flush entries"))?;

        debug!("Appended {} log entries", count);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
        // Remove all entries from log_id.index onwards
        let start_key = Self::log_key(log_id.index);
        let mut keys_to_remove = Vec::new();

        for item in self.db.range(start_key.as_bytes()..) {
            if let Ok((k, _)) = item {
                if !k.starts_with(PREFIX_LOG.as_bytes()) {
                    break;
                }
                keys_to_remove.push(k);
            }
        }

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.db
                .remove(key)
                .map_err(|e| Self::storage_error(e, "remove entry"))?;
        }

        debug!("Truncated {} entries from index {}", count, log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
        // Remove all entries up to and including log_id.index
        let end_key = Self::log_key(log_id.index + 1);
        let mut keys_to_remove = Vec::new();

        for item in self.db.scan_prefix(PREFIX_LOG) {
            if let Ok((k, _)) = item {
                if k.as_ref() >= end_key.as_bytes() {
                    break;
                }
                keys_to_remove.push(k);
            }
        }

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.db
                .remove(key)
                .map_err(|e| Self::storage_error(e, "purge entry"))?;
        }

        // Update last purged
        let data = bincode::serialize(&log_id)
            .map_err(|e| Self::storage_error(e, "serialize last_purged"))?;
        self.db
            .insert(KEY_LAST_PURGED, data)
            .map_err(|e| Self::storage_error(e, "insert last_purged"))?;

        *self.last_purged.write() = Some(log_id);

        debug!(
            "Purged {} entries up to index {}",
            count, log_id.index
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

        let start_key = Self::log_key(start);
        let mut entries = Vec::new();

        for item in self.db.range(start_key.as_bytes()..) {
            if let Ok((k, v)) = item {
                if !k.starts_with(PREFIX_LOG.as_bytes()) {
                    break;
                }

                if let Some(end_idx) = end {
                    let idx: u64 = std::str::from_utf8(&k)
                        .ok()
                        .and_then(|s| s.strip_prefix(PREFIX_LOG))
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(u64::MAX);

                    if idx >= end_idx {
                        break;
                    }
                }

                if let Ok(entry) = bincode::deserialize::<Entry>(&v) {
                    entries.push(entry);
                }
            }
        }

        debug!(
            "Read {} log entries for range {:?}",
            entries.len(),
            range
        );
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
}
