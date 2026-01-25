//! Distributed lock helpers
//!
//! Provides convenient abstractions for working with distributed locks.

use super::interface::Coordinator;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Options for acquiring a distributed lock
#[derive(Debug, Clone)]
pub struct LockOptions {
    /// Lock TTL (how long the lock is held if not renewed)
    pub ttl: Duration,
    /// Timeout for acquiring the lock
    pub timeout: Duration,
    /// Whether to automatically renew the lock
    pub auto_renew: bool,
    /// Renewal interval (should be less than TTL)
    pub renew_interval: Duration,
}

impl Default for LockOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            timeout: Duration::from_secs(10),
            auto_renew: true,
            renew_interval: Duration::from_secs(10),
        }
    }
}

impl LockOptions {
    /// Create new lock options with the given TTL
    pub fn with_ttl(ttl: Duration) -> Self {
        let renew_interval = Duration::from_secs(ttl.as_secs() / 3);
        Self {
            ttl,
            renew_interval,
            ..Default::default()
        }
    }

    /// Set the timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Disable auto-renewal
    pub fn no_auto_renew(mut self) -> Self {
        self.auto_renew = false;
        self
    }
}

/// A distributed lock that can be held across cluster nodes
pub struct DistributedLock {
    /// The coordinator
    coordinator: Arc<dyn Coordinator>,
    /// The lock key
    key: String,
    /// The holder ID (usually node_id)
    holder: String,
    /// Lock options
    options: LockOptions,
    /// Whether the lock is currently held
    held: std::sync::atomic::AtomicBool,
    /// Background renewal task handle
    renewal_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DistributedLock {
    /// Create a new distributed lock
    pub fn new(
        coordinator: Arc<dyn Coordinator>,
        key: impl Into<String>,
        holder: impl Into<String>,
    ) -> Self {
        Self {
            coordinator,
            key: key.into(),
            holder: holder.into(),
            options: LockOptions::default(),
            held: std::sync::atomic::AtomicBool::new(false),
            renewal_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Create a new distributed lock with options
    pub fn with_options(
        coordinator: Arc<dyn Coordinator>,
        key: impl Into<String>,
        holder: impl Into<String>,
        options: LockOptions,
    ) -> Self {
        Self {
            coordinator,
            key: key.into(),
            holder: holder.into(),
            options,
            held: std::sync::atomic::AtomicBool::new(false),
            renewal_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Try to acquire the lock (non-blocking)
    pub async fn try_acquire(&self) -> Result<bool, String> {
        if self.is_held() {
            return Ok(true);
        }

        let acquired = self.coordinator
            .try_lock(&self.key, &self.holder, self.options.ttl)
            .await?;

        if acquired {
            self.held.store(true, std::sync::atomic::Ordering::SeqCst);

            if self.options.auto_renew {
                self.start_renewal();
            }
        }

        Ok(acquired)
    }

    /// Acquire the lock (blocking with timeout)
    pub async fn acquire(&self) -> Result<bool, String> {
        if self.is_held() {
            return Ok(true);
        }

        let acquired = self.coordinator
            .lock(&self.key, &self.holder, self.options.ttl, self.options.timeout)
            .await?;

        if acquired {
            self.held.store(true, std::sync::atomic::Ordering::SeqCst);

            if self.options.auto_renew {
                self.start_renewal();
            }
        }

        Ok(acquired)
    }

    /// Release the lock
    pub async fn release(&self) -> Result<bool, String> {
        if !self.is_held() {
            return Ok(false);
        }

        // Stop renewal task
        self.stop_renewal();

        let released = self.coordinator
            .unlock(&self.key, &self.holder)
            .await?;

        if released {
            self.held.store(false, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(released)
    }

    /// Check if the lock is currently held by this instance
    pub fn is_held(&self) -> bool {
        self.held.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the lock key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the holder ID
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Start the background renewal task
    fn start_renewal(&self) {
        let coordinator = Arc::clone(&self.coordinator);
        let key = self.key.clone();
        let holder = self.holder.clone();
        let ttl = self.options.ttl;
        let interval = self.options.renew_interval;

        let handle = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            loop {
                timer.tick().await;
                match coordinator.renew_lock(&key, &holder, ttl).await {
                    Ok(true) => {
                        debug!("Lock renewed: key={}", key);
                    }
                    Ok(false) => {
                        warn!("Lock renewal failed (not held): key={}", key);
                        break;
                    }
                    Err(e) => {
                        warn!("Lock renewal error: key={}, error={}", key, e);
                        break;
                    }
                }
            }
        });

        *self.renewal_handle.lock() = Some(handle);
    }

    /// Stop the background renewal task
    fn stop_renewal(&self) {
        if let Some(handle) = self.renewal_handle.lock().take() {
            handle.abort();
        }
    }
}

impl Drop for DistributedLock {
    fn drop(&mut self) {
        self.stop_renewal();
        // Note: We can't release the lock here because drop is not async
        // The lock will expire via TTL
    }
}

/// RAII guard for a distributed lock
pub struct LockGuard<'a> {
    lock: &'a DistributedLock,
}

impl<'a> LockGuard<'a> {
    /// Create a guard by acquiring the lock
    pub async fn acquire(lock: &'a DistributedLock) -> Result<Option<Self>, String> {
        if lock.acquire().await? {
            Ok(Some(LockGuard { lock }))
        } else {
            Ok(None)
        }
    }
}

impl<'a> Drop for LockGuard<'a> {
    fn drop(&mut self) {
        // Spawn a task to release the lock
        let coordinator = Arc::clone(&self.lock.coordinator);
        let key = self.lock.key.clone();
        let holder = self.lock.holder.clone();

        tokio::spawn(async move {
            if let Err(e) = coordinator.unlock(&key, &holder).await {
                warn!("Failed to release lock on drop: {}", e);
            }
        });
    }
}

/// Helper macro for executing code with a distributed lock
#[macro_export]
macro_rules! with_lock {
    ($lock:expr, $code:block) => {{
        if $lock.acquire().await? {
            let result = $code;
            $lock.release().await?;
            Ok(result)
        } else {
            Err("Failed to acquire lock".to_string())
        }
    }};
}

/// Format a partition lock key
pub fn partition_lock_key(catalog: &str, schema: &str, table: &str, date: &str) -> String {
    format!("write/{}/{}/{}/{}", catalog, schema, table, date)
}

/// Format a compaction lock key
pub fn compaction_lock_key(catalog: &str, schema: &str, table: &str, date: &str) -> String {
    format!("compaction/{}/{}/{}/{}", catalog, schema, table, date)
}

/// Format a metadata lock key
pub fn metadata_lock_key(catalog: &str, schema: &str, table: &str) -> String {
    format!("metadata/{}/{}/{}", catalog, schema, table)
}
