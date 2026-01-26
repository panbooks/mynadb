//! Raft log store implementation using sled

use crate::meta::raft::{NodeConfig, NodeId, TypeConfig};
use crate::meta::types::MetaCommand;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StoredMembership, Vote};
use parking_lot::RwLock;
use sled::Db;
use std::fmt::Debug;
use std::ops::RangeBounds;

const KEY_VOTE: &[u8] = b"/raft/vote";
const KEY_COMMITTED: &[u8] = b"/raft/committed";
const PREFIX_LOG: &str = "/raft/log/";

/// Raft log store backed by sled
pub struct SledLogStore {
    db: Db,
    vote: RwLock<Option<Vote<NodeId>>>,
    committed: RwLock<Option<LogId<NodeId>>>,
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

        Self {
            db,
            vote: RwLock::new(vote),
            committed: RwLock::new(committed),
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
}

impl RaftLogStorage<TypeConfig> for SledLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, openraft::StorageError<NodeId>> {
        let last = self.last_log_index();

        let last_log_id = if let Some(index) = last {
            let key = Self::log_key(index);
            self.db
                .get(key.as_bytes())
                .ok()
                .flatten()
                .and_then(|v| bincode::deserialize::<Entry<TypeConfig>>(&v).ok())
                .map(|e| e.log_id)
        } else {
            None
        };

        let last_purged_log_id = None; // We don't track purged logs separately

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Clone the database handle for reading
        Self {
            db: self.db.clone(),
            vote: RwLock::new(self.vote.read().clone()),
            committed: RwLock::new(self.committed.read().clone()),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), openraft::StorageError<NodeId>> {
        let data = bincode::serialize(vote).map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::write_vote(&e),
            }
        })?;

        self.db.insert(KEY_VOTE, data).map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::write_vote(&e),
            }
        })?;

        self.db.flush().map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::write_vote(&e),
            }
        })?;

        *self.vote.write() = Some(vote.clone());
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), openraft::StorageError<NodeId>> {
        if let Some(ref c) = committed {
            let data = bincode::serialize(c).map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::write_logs(&e),
                }
            })?;

            self.db.insert(KEY_COMMITTED, data).map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::write_logs(&e),
                }
            })?;
        }

        *self.committed.write() = committed;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, openraft::StorageError<NodeId>> {
        Ok(self.vote.read().clone())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, openraft::StorageError<NodeId>> {
        Ok(self.committed.read().clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), openraft::StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            let data = bincode::serialize(&entry).map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::write_logs(&e),
                }
            })?;

            self.db.insert(key.as_bytes(), data).map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::write_logs(&e),
                }
            })?;
        }

        self.db.flush().map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::write_logs(&e),
            }
        })?;

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), openraft::StorageError<NodeId>> {
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

        for key in keys_to_remove {
            self.db.remove(key).map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::write_logs(&e),
                }
            })?;
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), openraft::StorageError<NodeId>> {
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

        for key in keys_to_remove {
            self.db.remove(key).map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::write_logs(&e),
                }
            })?;
        }

        Ok(())
    }
}

// Implement LogReader trait
impl openraft::storage::RaftLogReader<TypeConfig> for SledLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, openraft::StorageError<NodeId>> {
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

                if let Ok(entry) = bincode::deserialize::<Entry<TypeConfig>>(&v) {
                    entries.push(entry);
                }
            }
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, openraft::StorageError<NodeId>> {
        Ok(self.vote.read().clone())
    }
}
