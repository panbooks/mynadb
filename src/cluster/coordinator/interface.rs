//! Coordinator trait interface
//!
//! Defines the abstract interface for distributed coordination,
//! allowing different backends (etcd, Redis) to be used interchangeably.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;

/// Type of coordinator backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorType {
    /// etcd-based coordination
    Etcd,
    /// Redis-based coordination (future)
    Redis,
    /// No coordination (standalone mode)
    None,
}

impl std::fmt::Display for CoordinatorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinatorType::Etcd => write!(f, "etcd"),
            CoordinatorType::Redis => write!(f, "redis"),
            CoordinatorType::None => write!(f, "none"),
        }
    }
}

/// Watch event types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventType {
    /// Key was created or updated
    Put,
    /// Key was deleted
    Delete,
}

/// Watch event from coordinator
#[derive(Debug, Clone)]
pub struct WatchEvent {
    /// Event type
    pub event_type: WatchEventType,
    /// Key that changed
    pub key: String,
    /// Value (if Put event)
    pub value: Option<Vec<u8>>,
    /// Previous value (if available)
    pub prev_value: Option<Vec<u8>>,
    /// Modification revision
    pub mod_revision: i64,
}

/// Subscription to a watch or pub/sub channel
pub struct Subscription {
    /// Receiver for events
    pub receiver: mpsc::Receiver<WatchEvent>,
    /// Internal handle for cleanup
    pub(crate) _handle: Option<tokio::task::JoinHandle<()>>,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(
        receiver: mpsc::Receiver<WatchEvent>,
        handle: Option<tokio::task::JoinHandle<()>>,
    ) -> Self {
        Self {
            receiver,
            _handle: handle,
        }
    }

    /// Receive the next event
    pub async fn recv(&mut self) -> Option<WatchEvent> {
        self.receiver.recv().await
    }
}

/// Lock guard that releases the lock when dropped
pub struct LockGuard {
    /// The key this lock is held on
    pub key: String,
    /// The holder ID
    pub holder: String,
    /// Internal release function
    release_fn: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl LockGuard {
    /// Create a new lock guard
    pub fn new<F>(key: String, holder: String, release_fn: F) -> Self
    where
        F: FnOnce() + Send + Sync + 'static,
    {
        Self {
            key,
            holder,
            release_fn: Some(Box::new(release_fn)),
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(release_fn) = self.release_fn.take() {
            release_fn();
        }
    }
}

/// Coordinator trait for distributed coordination
///
/// This trait abstracts the coordination backend, allowing different
/// implementations (etcd, Redis) to be used interchangeably.
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Get the coordinator type
    fn coordinator_type(&self) -> CoordinatorType;

    /// Check if the coordinator is connected
    async fn is_connected(&self) -> bool;

    // ==================== Key-Value Operations ====================

    /// Get a value by key
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;

    /// Get all key-value pairs with a given prefix
    async fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String>;

    /// Put a value
    async fn put(&self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Put a value with TTL (lease)
    async fn put_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<i64, String>;

    /// Delete a key
    async fn delete(&self, key: &str) -> Result<bool, String>;

    /// Delete all keys with a given prefix
    async fn delete_prefix(&self, prefix: &str) -> Result<i64, String>;

    // ==================== Distributed Locks ====================

    /// Try to acquire a lock (non-blocking)
    ///
    /// Returns true if lock was acquired, false if already held by another holder.
    /// The lock will automatically expire after `ttl` duration.
    async fn try_lock(&self, key: &str, holder: &str, ttl: Duration) -> Result<bool, String>;

    /// Acquire a lock (blocking with timeout)
    ///
    /// Waits up to `timeout` for the lock to become available.
    async fn lock(
        &self,
        key: &str,
        holder: &str,
        ttl: Duration,
        timeout: Duration,
    ) -> Result<bool, String>;

    /// Release a lock
    ///
    /// Returns true if the lock was released, false if it was not held.
    async fn unlock(&self, key: &str, holder: &str) -> Result<bool, String>;

    /// Renew a lock's TTL
    ///
    /// Extends the lock's expiration time. Returns false if lock is not held.
    async fn renew_lock(&self, key: &str, holder: &str, ttl: Duration) -> Result<bool, String>;

    /// Check if a lock is held by a specific holder
    async fn is_lock_held(&self, key: &str, holder: &str) -> Result<bool, String>;

    // ==================== Watch ====================

    /// Watch a key for changes
    async fn watch(&self, key: &str) -> Result<Subscription, String>;

    /// Watch all keys with a given prefix
    async fn watch_prefix(&self, prefix: &str) -> Result<Subscription, String>;

    // ==================== Pub/Sub ====================

    /// Publish a message to a channel
    async fn publish(&self, channel: &str, message: &[u8]) -> Result<(), String>;

    /// Subscribe to a channel
    async fn subscribe(&self, channel: &str) -> Result<Subscription, String>;

    // ==================== Leader Election ====================

    /// Campaign for leadership
    ///
    /// Blocks until leadership is acquired or an error occurs.
    /// Returns a lease ID that must be kept alive.
    async fn campaign(
        &self,
        election_key: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<i64, String>;

    /// Resign from leadership
    async fn resign(&self, election_key: &str, lease_id: i64) -> Result<(), String>;

    /// Get the current leader
    async fn get_leader(&self, election_key: &str) -> Result<Option<String>, String>;

    /// Check if this node is the leader
    async fn is_leader(&self, election_key: &str, node_id: &str) -> Result<bool, String>;

    /// Keep a lease alive (heartbeat)
    async fn keep_alive(&self, lease_id: i64) -> Result<(), String>;
}

/// Extension trait for common coordinator operations
#[async_trait]
pub trait CoordinatorExt: Coordinator {
    /// Get a string value
    async fn get_string(&self, key: &str) -> Result<Option<String>, String> {
        match self.get(key).await? {
            Some(bytes) => {
                let s = String::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8: {}", e))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Put a string value
    async fn put_string(&self, key: &str, value: &str) -> Result<(), String> {
        self.put(key, value.as_bytes()).await
    }

    /// Get a JSON value
    async fn get_json<T: for<'de> Deserialize<'de> + Send>(
        &self,
        key: &str,
    ) -> Result<Option<T>, String> {
        match self.get(key).await? {
            Some(bytes) => {
                let value: T = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("JSON parse error: {}", e))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Put a JSON value
    async fn put_json<T: Serialize + Sync>(&self, key: &str, value: &T) -> Result<(), String> {
        let bytes =
            serde_json::to_vec(value).map_err(|e| format!("JSON serialize error: {}", e))?;
        self.put(key, &bytes).await
    }
}

// Blanket implementation for all Coordinators
impl<T: Coordinator + ?Sized> CoordinatorExt for T {}
