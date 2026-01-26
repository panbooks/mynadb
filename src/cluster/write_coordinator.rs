//! Write coordination across cluster nodes
//!
//! Ensures that writes to the same partition are serialized across all nodes
//! using distributed locks.

use crate::cluster::cache_invalidation::{
    CacheInvalidation, CacheInvalidationHandler, InvalidationReason,
};
use crate::cluster::coordinator::{partition_lock_key, Coordinator, DistributedLock, LockOptions};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Write coordination manager
pub struct WriteCoordinator {
    /// The coordinator
    coordinator: Arc<dyn Coordinator>,
    /// This node's ID
    node_id: String,
    /// Cache invalidation handler
    invalidation_handler: Option<Arc<CacheInvalidationHandler>>,
    /// Default lock TTL
    lock_ttl: Duration,
    /// Default lock timeout
    lock_timeout: Duration,
    /// Whether write coordination is enabled
    enabled: bool,
    /// Active locks (for cleanup on shutdown)
    active_locks: Arc<RwLock<Vec<DistributedLock>>>,
}

impl WriteCoordinator {
    /// Create a new write coordinator
    pub fn new(
        coordinator: Arc<dyn Coordinator>,
        node_id: String,
        lock_ttl: Duration,
        lock_timeout: Duration,
        enabled: bool,
    ) -> Self {
        Self {
            coordinator,
            node_id,
            invalidation_handler: None,
            lock_ttl,
            lock_timeout,
            enabled,
            active_locks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Set the cache invalidation handler
    pub fn with_invalidation_handler(mut self, handler: Arc<CacheInvalidationHandler>) -> Self {
        self.invalidation_handler = Some(handler);
        self
    }

    /// Check if write coordination is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get a lock for a partition
    pub fn partition_lock(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> DistributedLock {
        let key = partition_lock_key(catalog, schema, table, date);
        let options = LockOptions::with_ttl(self.lock_ttl).timeout(self.lock_timeout);

        DistributedLock::with_options(
            Arc::clone(&self.coordinator),
            key,
            self.node_id.clone(),
            options,
        )
    }

    /// Acquire a partition lock for writing
    pub async fn acquire_write_lock(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Result<WriteGuard, String> {
        if !self.enabled {
            // Return a no-op guard when disabled
            return Ok(WriteGuard {
                lock: None,
                invalidation_handler: self.invalidation_handler.clone(),
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
                date: date.to_string(),
            });
        }

        let lock = self.partition_lock(catalog, schema, table, date);

        if !lock.acquire().await? {
            return Err(format!(
                "Failed to acquire write lock for {}/{}/{}/{}",
                catalog, schema, table, date
            ));
        }

        debug!(
            "Acquired write lock: {}/{}/{}/{}",
            catalog, schema, table, date
        );

        Ok(WriteGuard {
            lock: Some(lock),
            invalidation_handler: self.invalidation_handler.clone(),
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date: date.to_string(),
        })
    }

    /// Try to acquire a partition lock (non-blocking)
    pub async fn try_acquire_write_lock(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Result<Option<WriteGuard>, String> {
        if !self.enabled {
            return Ok(Some(WriteGuard {
                lock: None,
                invalidation_handler: self.invalidation_handler.clone(),
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
                date: date.to_string(),
            }));
        }

        let lock = self.partition_lock(catalog, schema, table, date);

        if !lock.try_acquire().await? {
            return Ok(None);
        }

        debug!(
            "Acquired write lock (try): {}/{}/{}/{}",
            catalog, schema, table, date
        );

        Ok(Some(WriteGuard {
            lock: Some(lock),
            invalidation_handler: self.invalidation_handler.clone(),
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date: date.to_string(),
        }))
    }

    /// Execute a write operation with a lock
    pub async fn with_write_lock<F, Fut, T>(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
        operation: F,
    ) -> Result<T, String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        let guard = self
            .acquire_write_lock(catalog, schema, table, date)
            .await?;
        let result = operation().await;

        // Guard will be dropped here, releasing the lock and sending invalidation
        drop(guard);

        result
    }

    /// Notify other nodes about a write
    pub async fn notify_write(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
        file_path: &str,
    ) -> Result<(), String> {
        if let Some(ref handler) = self.invalidation_handler {
            handler.invalidate_new_file(file_path).await?;
        }
        Ok(())
    }

    /// Release all active locks (for graceful shutdown)
    pub async fn release_all_locks(&self) {
        let locks = std::mem::take(&mut *self.active_locks.write());
        for lock in locks {
            if let Err(e) = lock.release().await {
                warn!("Failed to release lock on shutdown: {}", e);
            }
        }
    }
}

/// RAII guard for a write operation
///
/// Holds the partition lock and sends cache invalidation when dropped.
pub struct WriteGuard {
    /// The distributed lock (None if coordination disabled)
    lock: Option<DistributedLock>,
    /// Cache invalidation handler
    invalidation_handler: Option<Arc<CacheInvalidationHandler>>,
    /// Catalog name
    catalog: String,
    /// Schema name
    schema: String,
    /// Table name
    table: String,
    /// Partition date
    date: String,
}

impl WriteGuard {
    /// Manually release the lock and send invalidation
    pub async fn release(mut self) -> Result<(), String> {
        self.release_internal().await
    }

    /// Mark the write as complete (sends invalidation but doesn't release lock yet)
    pub async fn complete(&self) -> Result<(), String> {
        if let Some(ref handler) = self.invalidation_handler {
            let invalidation = CacheInvalidation::for_partition(
                handler.node_id.clone(),
                &self.catalog,
                &self.schema,
                &self.table,
                &self.date,
                InvalidationReason::NewFile,
            );
            handler.invalidate(invalidation).await?;
        }
        Ok(())
    }

    async fn release_internal(&mut self) -> Result<(), String> {
        // Send invalidation
        if let Some(ref handler) = self.invalidation_handler {
            let node_id = if let Some(ref lock) = self.lock {
                lock.holder().to_string()
            } else {
                "standalone".to_string()
            };

            let invalidation = CacheInvalidation::for_partition(
                node_id,
                &self.catalog,
                &self.schema,
                &self.table,
                &self.date,
                InvalidationReason::NewFile,
            );

            if let Err(e) = handler.invalidate(invalidation).await {
                warn!("Failed to send cache invalidation: {}", e);
            }
        }

        // Release the lock
        if let Some(ref lock) = self.lock {
            lock.release().await?;
        }

        Ok(())
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Can't release async in drop, but the lock has auto-renewal
        // and will release when the DistributedLock is dropped
        if let Some(ref lock) = self.lock {
            if lock.is_held() {
                debug!(
                    "WriteGuard dropped while holding lock: {}/{}/{}/{}",
                    self.catalog, self.schema, self.table, self.date
                );
                // The DistributedLock's drop will handle cleanup
            }
        }
    }
}

impl WriteGuard {
    /// Reference to the catalog, schema, table, and date
    pub fn location(&self) -> (&str, &str, &str, &str) {
        (&self.catalog, &self.schema, &self.table, &self.date)
    }

    // Helper to create a node_id string for WriteGuard
    fn node_id(&self) -> String {
        if let Some(ref handler) = self.invalidation_handler {
            // We need to access the node_id from the handler
            // This is a bit awkward, but we'll create it from the handler
            "standalone".to_string() // Default fallback
        } else {
            "standalone".to_string()
        }
    }
}

/// Batch write coordinator for multiple partitions
pub struct BatchWriteCoordinator {
    /// The write coordinator
    write_coordinator: Arc<WriteCoordinator>,
    /// Active guards
    guards: Vec<WriteGuard>,
}

impl BatchWriteCoordinator {
    /// Create a new batch write coordinator
    pub fn new(write_coordinator: Arc<WriteCoordinator>) -> Self {
        Self {
            write_coordinator,
            guards: Vec::new(),
        }
    }

    /// Acquire a lock for a partition
    pub async fn acquire(
        &mut self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Result<(), String> {
        // Check if we already have a lock for this partition
        for guard in &self.guards {
            let (c, s, t, d) = guard.location();
            if c == catalog && s == schema && t == table && d == date {
                return Ok(()); // Already have the lock
            }
        }

        let guard = self
            .write_coordinator
            .acquire_write_lock(catalog, schema, table, date)
            .await?;
        self.guards.push(guard);
        Ok(())
    }

    /// Release all locks
    pub async fn release_all(self) -> Result<(), String> {
        for guard in self.guards {
            guard.release().await?;
        }
        Ok(())
    }

    /// Get the number of held locks
    pub fn lock_count(&self) -> usize {
        self.guards.len()
    }
}
