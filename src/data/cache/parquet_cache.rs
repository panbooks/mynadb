//! Local disk cache for Parquet files from object storage
//!
//! This cache stores Parquet files on local disk for fast repeated access.
//!
//! Key design decisions:
//! - Only caches files owned by this node (path starts with node_id)
//! - LRU eviction when cache exceeds max_size_bytes
//! - Concurrent access safe with file-level tracking
//! - Integrates with Storage abstraction for seamless remote access

use crate::storage::storage::Storage;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::fs;
use tracing::{debug, info, warn};

/// Local disk cache for Parquet files
///
/// Provides transparent caching of Parquet files from object storage.
/// When a file is requested:
/// 1. If cached locally, return the local path
/// 2. If not cached, download from object storage and cache
/// 3. If cache is full, evict LRU files to make space
pub struct ParquetLocalCache {
    /// Local cache directory
    cache_dir: PathBuf,

    /// Maximum cache size in bytes
    max_size_bytes: u64,

    /// Current cache size in bytes
    current_size: AtomicU64,

    /// Node ID for affinity checking
    node_id: String,

    /// Track file access times for LRU eviction: path → (last_access, size_bytes)
    access_times: RwLock<HashMap<String, (Instant, u64)>>,

    /// Storage backend for fetching from object store
    storage: Option<std::sync::Arc<Storage>>,
}

impl ParquetLocalCache {
    /// Create a new ParquetLocalCache
    ///
    /// # Arguments
    /// * `cache_dir` - Local directory to store cached files
    /// * `max_size_mb` - Maximum cache size in megabytes
    /// * `node_id` - Node ID for affinity checking (only cache files we own)
    pub fn new(cache_dir: PathBuf, max_size_mb: usize, node_id: String) -> Self {
        // Create cache directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&cache_dir) {
            warn!("Failed to create cache directory {:?}: {}", cache_dir, e);
        }

        info!(
            "Parquet local cache initialized: dir={:?}, max_size={}MB, node_id={}",
            cache_dir, max_size_mb, node_id
        );

        Self {
            cache_dir,
            max_size_bytes: (max_size_mb as u64) * 1024 * 1024,
            current_size: AtomicU64::new(0),
            node_id,
            access_times: RwLock::new(HashMap::new()),
            storage: None,
        }
    }

    /// Set the storage backend for remote file access
    pub fn with_storage(mut self, storage: std::sync::Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Get the local path for a cached file, downloading if necessary
    ///
    /// # Arguments
    /// * `path` - Remote file path (e.g., "data-1/mydb/default/cpu/date=2024-01-15/L0_xxx.parquet")
    ///
    /// # Returns
    /// Local path to the cached file, or the original path if caching is not possible
    pub async fn get_or_fetch(&self, path: &str) -> Result<PathBuf, String> {
        // Only cache files owned by this node
        if !self.is_owned_by_this_node(path) {
            debug!(
                "Not caching file {} (not owned by node {})",
                path, self.node_id
            );
            // Return the original path - storage layer will handle it
            return Ok(PathBuf::from(path));
        }

        let local_path = self.local_path(path);

        // Check if already cached
        if local_path.exists() {
            self.update_access_time(path);
            debug!("Cache HIT: {}", path);
            return Ok(local_path);
        }

        // Cache miss - need to download
        debug!("Cache MISS: {}", path);
        self.download_and_cache(path).await
    }

    /// Check if this file is owned by this node (path starts with node_id/)
    fn is_owned_by_this_node(&self, path: &str) -> bool {
        path.starts_with(&format!("{}/", self.node_id))
    }

    /// Get the local cache path for a remote file
    fn local_path(&self, path: &str) -> PathBuf {
        self.cache_dir.join(path)
    }

    /// Update access time for LRU tracking
    fn update_access_time(&self, path: &str) {
        let mut times = self.access_times.write();
        if let Some((ref mut instant, _)) = times.get_mut(path) {
            *instant = Instant::now();
        }
    }

    /// Download a file from storage and cache it locally
    async fn download_and_cache(&self, path: &str) -> Result<PathBuf, String> {
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| "Storage not configured for cache".to_string())?;

        // Get file from storage
        let data = storage
            .get(path)
            .await
            .map_err(|e| format!("Failed to fetch {}: {}", path, e))?;

        let file_size = data.len() as u64;

        // Ensure we have space
        self.ensure_space_available(file_size).await?;

        // Create local path
        let local_path = self.local_path(path);

        // Create parent directories
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create cache dir: {}", e))?;
        }

        // Write to local cache
        fs::write(&local_path, &data)
            .await
            .map_err(|e| format!("Failed to write cache file: {}", e))?;

        // Update tracking
        self.current_size.fetch_add(file_size, Ordering::SeqCst);
        {
            let mut times = self.access_times.write();
            times.insert(path.to_string(), (Instant::now(), file_size));
        }

        debug!("Cached {} ({} bytes)", path, file_size);
        Ok(local_path)
    }

    /// Ensure we have enough space for a new file, evicting LRU files if needed
    async fn ensure_space_available(&self, needed_bytes: u64) -> Result<(), String> {
        let current = self.current_size.load(Ordering::SeqCst);
        if current + needed_bytes <= self.max_size_bytes {
            return Ok(());
        }

        // Need to evict files
        let to_free = (current + needed_bytes).saturating_sub(self.max_size_bytes);
        let mut freed = 0u64;

        // Get files sorted by last access time (oldest first)
        let files_to_evict: Vec<(String, u64)> = {
            let times = self.access_times.read();
            let mut files: Vec<_> = times
                .iter()
                .map(|(path, (instant, size))| (path.clone(), *instant, *size))
                .collect();
            files.sort_by_key(|(_, instant, _)| *instant);
            files
                .into_iter()
                .map(|(path, _, size)| (path, size))
                .collect()
        };

        for (path, size) in files_to_evict {
            if freed >= to_free {
                break;
            }

            if let Err(e) = self.evict_file(&path).await {
                warn!("Failed to evict {}: {}", path, e);
                continue;
            }

            freed += size;
        }

        if freed < to_free {
            return Err(format!(
                "Could not free enough space: needed {} bytes, freed {} bytes",
                to_free, freed
            ));
        }

        debug!("Evicted {} bytes from cache to make room", freed);
        Ok(())
    }

    /// Evict a single file from the cache
    async fn evict_file(&self, path: &str) -> Result<(), String> {
        let local_path = self.local_path(path);

        if !local_path.exists() {
            // Already gone
            let mut times = self.access_times.write();
            times.remove(path);
            return Ok(());
        }

        // Get size before deleting
        let size = fs::metadata(&local_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        // Delete file
        fs::remove_file(&local_path)
            .await
            .map_err(|e| format!("Failed to remove {}: {}", path, e))?;

        // Update tracking
        self.current_size.fetch_sub(size, Ordering::SeqCst);
        {
            let mut times = self.access_times.write();
            times.remove(path);
        }

        debug!("Evicted {} ({} bytes)", path, size);
        Ok(())
    }

    /// Invalidate a cache entry (e.g., when file is deleted or compacted)
    pub async fn invalidate(&self, path: &str) -> Result<(), String> {
        self.evict_file(path).await
    }

    /// Get current cache statistics
    pub fn stats(&self) -> CacheStats {
        let times = self.access_times.read();
        CacheStats {
            current_size_bytes: self.current_size.load(Ordering::SeqCst),
            max_size_bytes: self.max_size_bytes,
            file_count: times.len(),
            node_id: self.node_id.clone(),
        }
    }

    /// Scan existing cache directory and rebuild tracking
    ///
    /// Call this at startup to account for files already in the cache directory.
    pub async fn scan_existing(&self) -> Result<usize, String> {
        let mut count = 0;
        let mut total_size = 0u64;

        // Walk the cache directory
        if let Ok(mut entries) = fs::read_dir(&self.cache_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    if metadata.is_file() {
                        let size = metadata.len();
                        let path = entry
                            .path()
                            .strip_prefix(&self.cache_dir)
                            .map(|p| p.to_string_lossy().to_string())
                            .unwrap_or_default();

                        if !path.is_empty() {
                            let mut times = self.access_times.write();
                            times.insert(path, (Instant::now(), size));
                            total_size += size;
                            count += 1;
                        }
                    }
                }
            }
        }

        self.current_size.store(total_size, Ordering::SeqCst);
        info!(
            "Scanned cache directory: {} files, {} bytes",
            count, total_size
        );

        Ok(count)
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub current_size_bytes: u64,
    pub max_size_bytes: u64,
    pub file_count: usize,
    pub node_id: String,
}

impl CacheStats {
    /// Get cache utilization as a percentage
    pub fn utilization_percent(&self) -> f64 {
        if self.max_size_bytes == 0 {
            0.0
        } else {
            (self.current_size_bytes as f64 / self.max_size_bytes as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_is_owned_by_node() {
        let temp_dir = TempDir::new().unwrap();
        let cache =
            ParquetLocalCache::new(temp_dir.path().to_path_buf(), 100, "data-1".to_string());

        // Owned by this node
        assert!(
            cache.is_owned_by_this_node("data-1/mydb/default/cpu/date=2024-01-15/L0_xxx.parquet")
        );

        // Not owned by this node
        assert!(
            !cache.is_owned_by_this_node("data-2/mydb/default/cpu/date=2024-01-15/L0_xxx.parquet")
        );
        assert!(!cache.is_owned_by_this_node("mydb/default/cpu/date=2024-01-15/L0_xxx.parquet"));
    }

    #[tokio::test]
    async fn test_local_path() {
        let temp_dir = TempDir::new().unwrap();
        let cache =
            ParquetLocalCache::new(temp_dir.path().to_path_buf(), 100, "data-1".to_string());

        let path = "data-1/mydb/default/cpu/date=2024-01-15/L0_xxx.parquet";
        let local = cache.local_path(path);
        assert!(local.starts_with(temp_dir.path()));
        assert!(local.ends_with("L0_xxx.parquet"));
    }

    #[test]
    fn test_cache_stats() {
        let stats = CacheStats {
            current_size_bytes: 50 * 1024 * 1024,
            max_size_bytes: 100 * 1024 * 1024,
            file_count: 10,
            node_id: "data-1".to_string(),
        };

        assert!((stats.utilization_percent() - 50.0).abs() < 0.1);
    }
}
