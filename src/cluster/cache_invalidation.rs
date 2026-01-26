//! Cache invalidation across cluster nodes
//!
//! Provides pub/sub-based cache invalidation to keep local caches
//! consistent across all nodes in the cluster.

use crate::cluster::coordinator::Coordinator;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Reason for cache invalidation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationReason {
    /// New file was written
    NewFile,
    /// File was deleted
    FileDeleted,
    /// Files were compacted
    Compacted,
    /// Metadata changed
    MetadataChanged,
    /// Table schema changed
    SchemaChanged,
    /// Partition deleted
    PartitionDeleted,
    /// Full cache clear requested
    CacheClear,
}

impl std::fmt::Display for InvalidationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidationReason::NewFile => write!(f, "new_file"),
            InvalidationReason::FileDeleted => write!(f, "file_deleted"),
            InvalidationReason::Compacted => write!(f, "compacted"),
            InvalidationReason::MetadataChanged => write!(f, "metadata_changed"),
            InvalidationReason::SchemaChanged => write!(f, "schema_changed"),
            InvalidationReason::PartitionDeleted => write!(f, "partition_deleted"),
            InvalidationReason::CacheClear => write!(f, "cache_clear"),
        }
    }
}

/// Cache invalidation message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInvalidation {
    /// Affected paths (glob patterns supported)
    pub paths: Vec<String>,
    /// Reason for invalidation
    pub reason: InvalidationReason,
    /// Source node that triggered the invalidation
    pub source_node: String,
    /// Timestamp when the invalidation was created
    pub timestamp: i64,
    /// Optional catalog filter
    pub catalog: Option<String>,
    /// Optional schema filter
    pub schema: Option<String>,
    /// Optional table filter
    pub table: Option<String>,
    /// Optional partition date filter
    pub partition_date: Option<String>,
}

impl CacheInvalidation {
    /// Create a new cache invalidation message
    pub fn new(source_node: String, reason: InvalidationReason) -> Self {
        Self {
            paths: Vec::new(),
            reason,
            source_node,
            timestamp: Utc::now().timestamp_millis(),
            catalog: None,
            schema: None,
            table: None,
            partition_date: None,
        }
    }

    /// Add a path to invalidate
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.paths.push(path.into());
        self
    }

    /// Add multiple paths to invalidate
    pub fn with_paths(mut self, paths: Vec<String>) -> Self {
        self.paths.extend(paths);
        self
    }

    /// Set the catalog filter
    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    /// Set the schema filter
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the table filter
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Set the partition date filter
    pub fn with_partition(mut self, date: impl Into<String>) -> Self {
        self.partition_date = Some(date.into());
        self
    }

    /// Create a full table invalidation
    pub fn for_table(
        source_node: String,
        catalog: &str,
        schema: &str,
        table: &str,
        reason: InvalidationReason,
    ) -> Self {
        Self::new(source_node, reason)
            .with_catalog(catalog)
            .with_schema(schema)
            .with_table(table)
            .with_path(format!("{}/{}/{}", catalog, schema, table))
    }

    /// Create a partition invalidation
    pub fn for_partition(
        source_node: String,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
        reason: InvalidationReason,
    ) -> Self {
        Self::new(source_node, reason)
            .with_catalog(catalog)
            .with_schema(schema)
            .with_table(table)
            .with_partition(date)
            .with_path(format!("{}/{}/{}/date={}", catalog, schema, table, date))
    }

    /// Create a file invalidation
    pub fn for_file(source_node: String, file_path: &str, reason: InvalidationReason) -> Self {
        Self::new(source_node, reason).with_path(file_path)
    }

    /// Serialize to JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self)
            .map_err(|e| format!("Failed to serialize CacheInvalidation: {}", e))
    }

    /// Deserialize from JSON bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes)
            .map_err(|e| format!("Failed to deserialize CacheInvalidation: {}", e))
    }

    /// Check if a path matches this invalidation
    pub fn matches_path(&self, path: &str) -> bool {
        if self.paths.is_empty() {
            // No specific paths, match based on filters
            return self.matches_filters(path);
        }

        for pattern in &self.paths {
            if pattern.contains('*') {
                // Simple glob matching
                if wildmatch::WildMatch::new(pattern).matches(path) {
                    return true;
                }
            } else if path.starts_with(pattern) || path == pattern {
                return true;
            }
        }

        false
    }

    /// Check if a path matches the catalog/schema/table filters
    fn matches_filters(&self, path: &str) -> bool {
        let parts: Vec<&str> = path.split('/').collect();

        if let Some(ref catalog) = self.catalog {
            if parts.first() != Some(&catalog.as_str()) {
                return false;
            }
        }

        if let Some(ref schema) = self.schema {
            if parts.get(1) != Some(&schema.as_str()) {
                return false;
            }
        }

        if let Some(ref table) = self.table {
            if parts.get(2) != Some(&table.as_str()) {
                return false;
            }
        }

        if let Some(ref date) = self.partition_date {
            let date_prefix = format!("date={}", date);
            if !parts
                .get(3)
                .map(|p| p.starts_with(&date_prefix))
                .unwrap_or(false)
            {
                return false;
            }
        }

        true
    }
}

/// Cache invalidation handler
///
/// Listens for cache invalidation messages and notifies local caches.
pub struct CacheInvalidationHandler {
    /// The coordinator
    coordinator: Arc<dyn Coordinator>,
    /// This node's ID
    pub node_id: String,
    /// Invalidation channel name
    channel: String,
    /// Local broadcast sender for invalidation events
    sender: broadcast::Sender<CacheInvalidation>,
    /// Subscriber task handle
    subscriber_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl CacheInvalidationHandler {
    /// Create a new cache invalidation handler
    pub fn new(coordinator: Arc<dyn Coordinator>, node_id: String, channel: String) -> Self {
        let (sender, _) = broadcast::channel(1000);

        Self {
            coordinator,
            node_id,
            channel,
            sender,
            subscriber_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Get a receiver for invalidation events
    pub fn subscribe(&self) -> broadcast::Receiver<CacheInvalidation> {
        self.sender.subscribe()
    }

    /// Publish a cache invalidation to all nodes
    pub async fn invalidate(&self, invalidation: CacheInvalidation) -> Result<(), String> {
        let bytes = invalidation.to_bytes()?;
        self.coordinator.publish(&self.channel, &bytes).await?;

        debug!(
            "Published cache invalidation: reason={}, paths={:?}",
            invalidation.reason, invalidation.paths
        );

        Ok(())
    }

    /// Invalidate for a new file
    pub async fn invalidate_new_file(&self, path: &str) -> Result<(), String> {
        let invalidation =
            CacheInvalidation::for_file(self.node_id.clone(), path, InvalidationReason::NewFile);
        self.invalidate(invalidation).await
    }

    /// Invalidate for compaction
    pub async fn invalidate_compaction(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Result<(), String> {
        let invalidation = CacheInvalidation::for_partition(
            self.node_id.clone(),
            catalog,
            schema,
            table,
            date,
            InvalidationReason::Compacted,
        );
        self.invalidate(invalidation).await
    }

    /// Invalidate for partition deletion
    pub async fn invalidate_partition_deleted(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Result<(), String> {
        let invalidation = CacheInvalidation::for_partition(
            self.node_id.clone(),
            catalog,
            schema,
            table,
            date,
            InvalidationReason::PartitionDeleted,
        );
        self.invalidate(invalidation).await
    }

    /// Invalidate entire cache
    pub async fn invalidate_all(&self) -> Result<(), String> {
        let invalidation =
            CacheInvalidation::new(self.node_id.clone(), InvalidationReason::CacheClear)
                .with_path("*");
        self.invalidate(invalidation).await
    }

    /// Start listening for invalidation messages
    pub async fn start(&self) -> Result<(), String> {
        let mut subscription = self.coordinator.subscribe(&self.channel).await?;
        let sender = self.sender.clone();
        let node_id = self.node_id.clone();

        let handle = tokio::spawn(async move {
            info!("Cache invalidation listener started");

            while let Some(event) = subscription.recv().await {
                if let Some(value) = event.value {
                    match CacheInvalidation::from_bytes(&value) {
                        Ok(invalidation) => {
                            // Skip our own messages
                            if invalidation.source_node == node_id {
                                debug!("Skipping own invalidation message");
                                continue;
                            }

                            debug!(
                                "Received cache invalidation from {}: reason={}, paths={:?}",
                                invalidation.source_node, invalidation.reason, invalidation.paths
                            );

                            // Broadcast to local subscribers
                            if let Err(e) = sender.send(invalidation) {
                                warn!("Failed to broadcast invalidation: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse invalidation message: {}", e);
                        }
                    }
                }
            }

            warn!("Cache invalidation listener stopped");
        });

        *self.subscriber_handle.lock() = Some(handle);
        Ok(())
    }

    /// Stop listening for invalidation messages
    pub fn stop(&self) {
        if let Some(handle) = self.subscriber_handle.lock().take() {
            handle.abort();
        }
    }
}

impl Drop for CacheInvalidationHandler {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Wrapper for integrating with existing cache implementations
pub struct CacheInvalidationIntegration {
    /// Receiver for invalidation events
    receiver: broadcast::Receiver<CacheInvalidation>,
    /// Callback to handle invalidations
    on_invalidate: Box<dyn Fn(&CacheInvalidation) + Send + Sync>,
}

impl CacheInvalidationIntegration {
    /// Create a new integration
    pub fn new<F>(handler: &CacheInvalidationHandler, on_invalidate: F) -> Self
    where
        F: Fn(&CacheInvalidation) + Send + Sync + 'static,
    {
        Self {
            receiver: handler.subscribe(),
            on_invalidate: Box::new(on_invalidate),
        }
    }

    /// Start processing invalidation events
    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Ok(invalidation) = self.receiver.recv().await {
                (self.on_invalidate)(&invalidation);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidation_serialization() {
        let inv = CacheInvalidation::for_partition(
            "node-1".to_string(),
            "mydb",
            "default",
            "cpu",
            "2024-01-15",
            InvalidationReason::NewFile,
        );

        let bytes = inv.to_bytes().unwrap();
        let restored = CacheInvalidation::from_bytes(&bytes).unwrap();

        assert_eq!(inv.source_node, restored.source_node);
        assert_eq!(inv.reason, restored.reason);
        assert_eq!(inv.catalog, restored.catalog);
    }

    #[test]
    fn test_path_matching() {
        let inv = CacheInvalidation::for_table(
            "node-1".to_string(),
            "mydb",
            "default",
            "cpu",
            InvalidationReason::MetadataChanged,
        );

        assert!(inv.matches_path("mydb/default/cpu/date=2024-01-15/L0_123.parquet"));
        assert!(!inv.matches_path("otherdb/default/cpu/date=2024-01-15/L0_123.parquet"));
    }

    #[test]
    fn test_glob_matching() {
        let inv = CacheInvalidation::new("node-1".to_string(), InvalidationReason::CacheClear)
            .with_path("mydb/*/cpu/*");

        assert!(inv.matches_path("mydb/default/cpu/file.parquet"));
        assert!(inv.matches_path("mydb/schema1/cpu/file.parquet"));
        assert!(!inv.matches_path("mydb/default/memory/file.parquet"));
    }
}
