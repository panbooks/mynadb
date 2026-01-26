//! No-op coordinator for standalone mode
//!
//! This coordinator does nothing - it's used when cluster mode is disabled.
//! All operations succeed immediately (locks are always acquired, etc.)

use super::interface::{Coordinator, CoordinatorType, Subscription, WatchEvent};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::mpsc;

/// No-op coordinator for standalone mode
pub struct NoopCoordinator;

impl NoopCoordinator {
    /// Create a new no-op coordinator
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Coordinator for NoopCoordinator {
    fn coordinator_type(&self) -> CoordinatorType {
        CoordinatorType::None
    }

    async fn is_connected(&self) -> bool {
        true // Always "connected" in standalone mode
    }

    // ==================== Key-Value Operations ====================

    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    async fn get_prefix(&self, _prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        Ok(vec![])
    }

    async fn put(&self, _key: &str, _value: &[u8]) -> Result<(), String> {
        Ok(())
    }

    async fn put_with_ttl(&self, _key: &str, _value: &[u8], _ttl: Duration) -> Result<i64, String> {
        Ok(0) // Fake lease ID
    }

    async fn delete(&self, _key: &str) -> Result<bool, String> {
        Ok(true)
    }

    async fn delete_prefix(&self, _prefix: &str) -> Result<i64, String> {
        Ok(0)
    }

    // ==================== Distributed Locks ====================

    async fn try_lock(&self, _key: &str, _holder: &str, _ttl: Duration) -> Result<bool, String> {
        Ok(true) // Always succeed in standalone mode
    }

    async fn lock(
        &self,
        _key: &str,
        _holder: &str,
        _ttl: Duration,
        _timeout: Duration,
    ) -> Result<bool, String> {
        Ok(true) // Always succeed in standalone mode
    }

    async fn unlock(&self, _key: &str, _holder: &str) -> Result<bool, String> {
        Ok(true)
    }

    async fn renew_lock(&self, _key: &str, _holder: &str, _ttl: Duration) -> Result<bool, String> {
        Ok(true)
    }

    async fn is_lock_held(&self, _key: &str, _holder: &str) -> Result<bool, String> {
        Ok(true) // Pretend we hold all locks
    }

    // ==================== Watch ====================

    async fn watch(&self, _key: &str) -> Result<Subscription, String> {
        // Return an empty channel that never produces events
        let (_tx, rx) = mpsc::channel(1);
        Ok(Subscription::new(rx, None))
    }

    async fn watch_prefix(&self, _prefix: &str) -> Result<Subscription, String> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Subscription::new(rx, None))
    }

    // ==================== Pub/Sub ====================

    async fn publish(&self, _channel: &str, _message: &[u8]) -> Result<(), String> {
        Ok(()) // Messages go nowhere
    }

    async fn subscribe(&self, _channel: &str) -> Result<Subscription, String> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Subscription::new(rx, None))
    }

    // ==================== Leader Election ====================

    async fn campaign(
        &self,
        _election_key: &str,
        _node_id: &str,
        _ttl: Duration,
    ) -> Result<i64, String> {
        Ok(0) // Always become leader immediately
    }

    async fn resign(&self, _election_key: &str, _lease_id: i64) -> Result<(), String> {
        Ok(())
    }

    async fn get_leader(&self, _election_key: &str) -> Result<Option<String>, String> {
        Ok(None) // No leader tracking in standalone mode
    }

    async fn is_leader(&self, _election_key: &str, _node_id: &str) -> Result<bool, String> {
        Ok(true) // Always the leader in standalone mode
    }

    async fn keep_alive(&self, _lease_id: i64) -> Result<(), String> {
        Ok(())
    }
}
