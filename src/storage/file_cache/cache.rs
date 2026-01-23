//! FileCache trait definition

use super::types::*;
use crate::storage::storage::Storage;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

/// Trait for file caching operations
///
/// Implementations must be thread-safe (Send + Sync).
#[async_trait]
pub trait FileCache: Send + Sync {
    // ==================== Core Operations ====================

    /// Get a file from cache, fetching from source if not cached
    ///
    /// Returns the file contents as bytes.
    async fn get(&self, source_path: &str, storage: &Arc<Storage>) -> CacheResult<Bytes>;

    /// Get a file from cache only (no fetch on miss)
    ///
    /// Returns None if not in cache.
    async fn get_if_cached(&self, source_path: &str) -> CacheResult<Option<Bytes>>;

    /// Put a file into the cache
    ///
    /// The file is written to local storage and indexed.
    async fn put(
        &self,
        source_path: &str,
        data: Bytes,
        etag: Option<String>,
        partition_date: Option<String>,
    ) -> CacheResult<()>;

    /// Check if a file is in the cache
    async fn contains(&self, source_path: &str) -> bool;

    /// Invalidate a cache entry
    async fn invalidate(&self, source_path: &str) -> CacheResult<()>;

    /// Invalidate all cache entries matching a prefix
    async fn invalidate_prefix(&self, prefix: &str) -> CacheResult<usize>;

    /// Clear the entire cache
    async fn clear(&self) -> CacheResult<()>;

    // ==================== Batch Operations ====================

    /// Get multiple files, fetching missing ones from source
    async fn get_many(
        &self,
        source_paths: &[String],
        storage: &Arc<Storage>,
    ) -> CacheResult<Vec<(String, Bytes)>>;

    /// Pre-fetch files into cache (background)
    async fn prefetch(&self, source_paths: Vec<String>, storage: Arc<Storage>);

    // ==================== Statistics ====================

    /// Get cache statistics
    async fn stats(&self) -> CacheStats;

    /// Get cache entry metadata
    async fn get_entry_meta(&self, source_path: &str) -> CacheResult<CacheEntryMeta>;

    /// List all cached files
    async fn list_entries(&self) -> CacheResult<Vec<CacheEntryMeta>>;

    // ==================== Maintenance ====================

    /// Run cache eviction to free space
    ///
    /// Returns the number of entries evicted and bytes freed.
    async fn evict(&self, target_bytes: u64) -> CacheResult<(usize, u64)>;

    /// Persist cache index to disk
    async fn save_index(&self) -> CacheResult<()>;

    /// Load cache index from disk
    async fn load_index(&self) -> CacheResult<()>;

    /// Verify cache integrity (check local files match index)
    async fn verify(&self) -> CacheResult<Vec<String>>;
}

/// Type alias for shared FileCache
pub type SharedFileCache = Arc<dyn FileCache>;
