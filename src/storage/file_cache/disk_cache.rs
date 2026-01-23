//! Disk-based file cache implementation
//!
//! Caches Parquet files on local SSD to reduce Object Store latency.

use super::cache::FileCache;
use super::types::*;
use crate::storage::storage::Storage;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for disk cache
#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: u64,
    /// Cache directory path
    pub cache_dir: PathBuf,
    /// Index file name
    pub index_file: String,
    /// Auto-save interval in seconds (0 = disabled)
    pub auto_save_interval_secs: u64,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            cache_dir: PathBuf::from("/var/cache/mynadb/parquet"),
            index_file: "cache_index.json".to_string(),
            auto_save_interval_secs: 300, // 5 minutes
        }
    }
}

/// In-memory state for cache
struct CacheState {
    /// Cached entries indexed by source path
    entries: HashMap<String, CacheEntry>,
    /// Total size of cached data
    total_size: u64,
    /// Statistics
    stats: CacheStats,
}

impl CacheState {
    fn new(max_size: u64) -> Self {
        Self {
            entries: HashMap::new(),
            total_size: 0,
            stats: CacheStats {
                max_size_bytes: max_size,
                ..Default::default()
            },
        }
    }
}

/// Disk-based file cache implementation
pub struct DiskFileCache {
    /// Configuration
    config: DiskCacheConfig,
    /// In-memory state
    state: RwLock<CacheState>,
}

impl DiskFileCache {
    /// Create a new disk cache
    pub async fn new(config: DiskCacheConfig) -> CacheResult<Self> {
        // Create cache directory if it doesn't exist
        fs::create_dir_all(&config.cache_dir)
            .await
            .map_err(|e| CacheError::IoError(format!("Failed to create cache dir: {}", e)))?;

        let cache = Self {
            state: RwLock::new(CacheState::new(config.max_size_bytes)),
            config,
        };

        // Try to load existing index
        if let Err(e) = cache.load_index().await {
            warn!("Failed to load cache index (starting fresh): {}", e);
        }

        Ok(cache)
    }

    /// Create with default config at specified directory
    pub async fn new_at(cache_dir: impl Into<PathBuf>, max_size_bytes: u64) -> CacheResult<Self> {
        let config = DiskCacheConfig {
            cache_dir: cache_dir.into(),
            max_size_bytes,
            ..Default::default()
        };
        Self::new(config).await
    }

    /// Get the cache file path for a source path
    fn cache_path_for(&self, source_path: &str) -> PathBuf {
        let key = CacheKey::file(source_path);
        self.config.cache_dir.join(key.to_cache_filename())
    }

    /// Get the index file path
    fn index_path(&self) -> PathBuf {
        self.config.cache_dir.join(&self.config.index_file)
    }

    /// Read file from local cache
    async fn read_cached_file(&self, cache_path: &Path) -> CacheResult<Bytes> {
        let data = fs::read(cache_path)
            .await
            .map_err(|e| CacheError::IoError(format!("Failed to read cache file: {}", e)))?;
        Ok(Bytes::from(data))
    }

    /// Write file to local cache
    async fn write_cached_file(&self, cache_path: &Path, data: &Bytes) -> CacheResult<()> {
        fs::write(cache_path, data)
            .await
            .map_err(|e| CacheError::IoError(format!("Failed to write cache file: {}", e)))
    }

    /// Delete file from local cache
    async fn delete_cached_file(&self, cache_path: &Path) -> CacheResult<()> {
        if cache_path.exists() {
            fs::remove_file(cache_path)
                .await
                .map_err(|e| CacheError::IoError(format!("Failed to delete cache file: {}", e)))?;
        }
        Ok(())
    }

    /// Fetch file from storage (object store)
    async fn fetch_from_source(
        &self,
        source_path: &str,
        storage: &Arc<Storage>,
    ) -> CacheResult<Bytes> {
        storage
            .get(source_path)
            .await
            .map_err(|e| CacheError::FetchError(format!("Failed to fetch from source: {}", e)))
    }

    /// Evict entries to make room for new data
    async fn make_room(&self, needed_bytes: u64) -> CacheResult<()> {
        let state = self.state.read().await;

        // Check if we need to evict
        let available = state.stats.max_size_bytes.saturating_sub(state.total_size);
        if available >= needed_bytes {
            return Ok(());
        }

        let bytes_to_free = needed_bytes - available;
        drop(state); // Release lock before calling evict

        self.evict(bytes_to_free).await?;
        Ok(())
    }
}

#[async_trait]
impl FileCache for DiskFileCache {
    async fn get(&self, source_path: &str, storage: &Arc<Storage>) -> CacheResult<Bytes> {
        // First, check if in cache
        {
            let mut state = self.state.write().await;
            if let Some(entry) = state.entries.get_mut(source_path) {
                // Update access time
                entry.touch();
                state.stats.hits += 1;
                let cache_path = self.cache_path_for(source_path);

                // Read from disk
                match self.read_cached_file(&cache_path).await {
                    Ok(data) => {
                        state.stats.bytes_read_from_cache += data.len() as u64;
                        return Ok(data);
                    }
                    Err(e) => {
                        // Cache file missing, remove from index
                        warn!("Cache file missing for {}: {}", source_path, e);
                        state.entries.remove(source_path);
                    }
                }
            }
            state.stats.misses += 1;
        }

        // Cache miss - fetch from source
        debug!("Cache miss for {}, fetching from source", source_path);
        let data = self.fetch_from_source(source_path, storage).await?;

        // Store in cache
        self.put(source_path, data.clone(), None, None).await?;

        // Update stats
        {
            let mut state = self.state.write().await;
            state.stats.bytes_fetched_from_source += data.len() as u64;
        }

        Ok(data)
    }

    async fn get_if_cached(&self, source_path: &str) -> CacheResult<Option<Bytes>> {
        let mut state = self.state.write().await;
        if let Some(entry) = state.entries.get_mut(source_path) {
            entry.touch();
            let cache_path = self.cache_path_for(source_path);
            drop(state); // Release lock before I/O

            match self.read_cached_file(&cache_path).await {
                Ok(data) => {
                    let mut state = self.state.write().await;
                    state.stats.hits += 1;
                    state.stats.bytes_read_from_cache += data.len() as u64;
                    Ok(Some(data))
                }
                Err(_) => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn put(
        &self,
        source_path: &str,
        data: Bytes,
        etag: Option<String>,
        partition_date: Option<String>,
    ) -> CacheResult<()> {
        let size = data.len() as u64;
        let cache_path = self.cache_path_for(source_path);

        // Make room if needed
        self.make_room(size).await?;

        // Write to disk
        self.write_cached_file(&cache_path, &data).await?;

        // Update index
        let now = chrono::Utc::now().timestamp_millis();
        let meta = CacheEntryMeta {
            source_path: source_path.to_string(),
            cache_path: cache_path.to_string_lossy().to_string(),
            size_bytes: size,
            etag,
            cached_at: now,
            last_accessed: now,
            access_count: 1,
            partition_date,
        };

        let mut state = self.state.write().await;

        // Remove old entry if exists
        if let Some(old) = state.entries.remove(source_path) {
            state.total_size -= old.meta.size_bytes;
        }

        state.total_size += size;
        state
            .entries
            .insert(source_path.to_string(), CacheEntry::new(meta));
        state.stats.file_count = state.entries.len();
        state.stats.total_size_bytes = state.total_size;

        debug!(
            "Cached {} ({} bytes), total size: {} bytes",
            source_path, size, state.total_size
        );

        Ok(())
    }

    async fn contains(&self, source_path: &str) -> bool {
        let state = self.state.read().await;
        state.entries.contains_key(source_path)
    }

    async fn invalidate(&self, source_path: &str) -> CacheResult<()> {
        let cache_path = self.cache_path_for(source_path);

        // Delete file
        self.delete_cached_file(&cache_path).await?;

        // Update index
        let mut state = self.state.write().await;
        if let Some(entry) = state.entries.remove(source_path) {
            state.total_size -= entry.meta.size_bytes;
            state.stats.file_count = state.entries.len();
            state.stats.total_size_bytes = state.total_size;
        }

        debug!("Invalidated cache entry: {}", source_path);
        Ok(())
    }

    async fn invalidate_prefix(&self, prefix: &str) -> CacheResult<usize> {
        let mut state = self.state.write().await;

        let to_remove: Vec<String> = state
            .entries
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        let count = to_remove.len();

        for path in &to_remove {
            if let Some(entry) = state.entries.remove(path) {
                state.total_size -= entry.meta.size_bytes;
                let cache_path = self.cache_path_for(path);
                drop(state); // Release lock for I/O
                let _ = self.delete_cached_file(&cache_path).await;
                state = self.state.write().await;
            }
        }

        state.stats.file_count = state.entries.len();
        state.stats.total_size_bytes = state.total_size;

        info!(
            "Invalidated {} cache entries with prefix: {}",
            count, prefix
        );
        Ok(count)
    }

    async fn clear(&self) -> CacheResult<()> {
        let mut state = self.state.write().await;

        // Collect all cache paths
        let paths: Vec<PathBuf> = state
            .entries
            .keys()
            .map(|p| self.cache_path_for(p))
            .collect();

        // Clear in-memory state
        state.entries.clear();
        state.total_size = 0;
        state.stats.file_count = 0;
        state.stats.total_size_bytes = 0;

        drop(state); // Release lock for I/O

        // Delete files
        for path in paths {
            let _ = self.delete_cached_file(&path).await;
        }

        info!("Cleared all cache entries");
        Ok(())
    }

    async fn get_many(
        &self,
        source_paths: &[String],
        storage: &Arc<Storage>,
    ) -> CacheResult<Vec<(String, Bytes)>> {
        let mut results = Vec::with_capacity(source_paths.len());

        for path in source_paths {
            let data = self.get(path, storage).await?;
            results.push((path.clone(), data));
        }

        Ok(results)
    }

    async fn prefetch(&self, source_paths: Vec<String>, storage: Arc<Storage>) {
        for path in source_paths {
            if !self.contains(&path).await {
                if let Err(e) = self.get(&path, &storage).await {
                    warn!("Failed to prefetch {}: {}", path, e);
                }
            }
        }
    }

    async fn stats(&self) -> CacheStats {
        self.state.read().await.stats.clone()
    }

    async fn get_entry_meta(&self, source_path: &str) -> CacheResult<CacheEntryMeta> {
        let state = self.state.read().await;
        state
            .entries
            .get(source_path)
            .map(|e| e.meta.clone())
            .ok_or_else(|| CacheError::NotFound(source_path.to_string()))
    }

    async fn list_entries(&self) -> CacheResult<Vec<CacheEntryMeta>> {
        let state = self.state.read().await;
        Ok(state.entries.values().map(|e| e.meta.clone()).collect())
    }

    async fn evict(&self, target_bytes: u64) -> CacheResult<(usize, u64)> {
        let mut state = self.state.write().await;

        // Score all entries (lower score = evict first)
        let mut scored: Vec<(String, f64)> = state
            .entries
            .iter()
            .map(|(path, entry)| (path.clone(), entry.priority_score()))
            .collect();

        // Sort by score ascending (lowest first)
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut evicted_count = 0;
        let mut evicted_bytes = 0u64;

        for (path, _score) in scored {
            if evicted_bytes >= target_bytes {
                break;
            }

            if let Some(entry) = state.entries.remove(&path) {
                let size = entry.meta.size_bytes;
                state.total_size -= size;
                evicted_bytes += size;
                evicted_count += 1;

                let cache_path = self.cache_path_for(&path);
                drop(state); // Release lock for I/O
                let _ = self.delete_cached_file(&cache_path).await;
                state = self.state.write().await;
            }
        }

        state.stats.evictions += evicted_count as u64;
        state.stats.file_count = state.entries.len();
        state.stats.total_size_bytes = state.total_size;

        info!(
            "Evicted {} entries ({} bytes) from cache",
            evicted_count, evicted_bytes
        );

        Ok((evicted_count, evicted_bytes))
    }

    async fn save_index(&self) -> CacheResult<()> {
        let state = self.state.read().await;

        let index = CacheIndex {
            version: CacheIndex::CURRENT_VERSION,
            indexed_at: chrono::Utc::now().timestamp_millis(),
            entries: state.entries.values().map(|e| e.meta.clone()).collect(),
        };

        drop(state); // Release lock before I/O

        let json = serde_json::to_string_pretty(&index)
            .map_err(|e| CacheError::SerializationError(format!("Failed to serialize: {}", e)))?;

        fs::write(self.index_path(), json)
            .await
            .map_err(|e| CacheError::IoError(format!("Failed to write index: {}", e)))?;

        debug!("Saved cache index with {} entries", index.entries.len());
        Ok(())
    }

    async fn load_index(&self) -> CacheResult<()> {
        let index_path = self.index_path();
        if !index_path.exists() {
            return Ok(());
        }

        let json = fs::read_to_string(&index_path)
            .await
            .map_err(|e| CacheError::IoError(format!("Failed to read index: {}", e)))?;

        let index: CacheIndex = serde_json::from_str(&json)
            .map_err(|e| CacheError::SerializationError(format!("Failed to parse: {}", e)))?;

        if index.version != CacheIndex::CURRENT_VERSION {
            warn!(
                "Cache index version mismatch ({} vs {}), starting fresh",
                index.version,
                CacheIndex::CURRENT_VERSION
            );
            return Ok(());
        }

        let mut state = self.state.write().await;
        let mut loaded = 0;
        let mut missing = 0;

        for meta in index.entries {
            let cache_path = PathBuf::from(&meta.cache_path);
            if cache_path.exists() {
                state.total_size += meta.size_bytes;
                state
                    .entries
                    .insert(meta.source_path.clone(), CacheEntry::new(meta));
                loaded += 1;
            } else {
                missing += 1;
            }
        }

        state.stats.file_count = state.entries.len();
        state.stats.total_size_bytes = state.total_size;

        info!(
            "Loaded cache index: {} entries loaded, {} missing",
            loaded, missing
        );

        Ok(())
    }

    async fn verify(&self) -> CacheResult<Vec<String>> {
        let state = self.state.read().await;
        let mut missing = Vec::new();

        for (source_path, entry) in &state.entries {
            let cache_path = PathBuf::from(&entry.meta.cache_path);
            if !cache_path.exists() {
                missing.push(source_path.clone());
            }
        }

        if !missing.is_empty() {
            warn!("{} cache entries have missing files", missing.len());
        }

        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_cache() -> (DiskFileCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            max_size_bytes: 10 * 1024 * 1024, // 10 MB
            ..Default::default()
        };
        let cache = DiskFileCache::new(config).await.unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get_if_cached() {
        let (cache, _temp) = create_test_cache().await;

        let data = Bytes::from("test data content");
        cache
            .put("s3://bucket/file.parquet", data.clone(), None, None)
            .await
            .unwrap();

        let cached = cache
            .get_if_cached("s3://bucket/file.parquet")
            .await
            .unwrap();
        assert!(cached.is_some());
        assert_eq!(cached.unwrap(), data);
    }

    #[tokio::test]
    async fn test_contains() {
        let (cache, _temp) = create_test_cache().await;

        assert!(!cache.contains("s3://bucket/file.parquet").await);

        cache
            .put("s3://bucket/file.parquet", Bytes::from("data"), None, None)
            .await
            .unwrap();

        assert!(cache.contains("s3://bucket/file.parquet").await);
    }

    #[tokio::test]
    async fn test_invalidate() {
        let (cache, _temp) = create_test_cache().await;

        cache
            .put("s3://bucket/file.parquet", Bytes::from("data"), None, None)
            .await
            .unwrap();
        assert!(cache.contains("s3://bucket/file.parquet").await);

        cache.invalidate("s3://bucket/file.parquet").await.unwrap();
        assert!(!cache.contains("s3://bucket/file.parquet").await);
    }

    #[tokio::test]
    async fn test_stats() {
        let (cache, _temp) = create_test_cache().await;

        let stats = cache.stats().await;
        assert_eq!(stats.file_count, 0);
        assert_eq!(stats.total_size_bytes, 0);

        cache
            .put(
                "s3://bucket/file.parquet",
                Bytes::from("some data"),
                None,
                None,
            )
            .await
            .unwrap();

        let stats = cache.stats().await;
        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.total_size_bytes, 9); // "some data" = 9 bytes
    }

    #[tokio::test]
    async fn test_save_and_load_index() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            max_size_bytes: 10 * 1024 * 1024,
            ..Default::default()
        };

        // Create cache and add entries
        {
            let cache = DiskFileCache::new(config.clone()).await.unwrap();
            cache
                .put(
                    "s3://bucket/file1.parquet",
                    Bytes::from("data1"),
                    None,
                    None,
                )
                .await
                .unwrap();
            cache
                .put(
                    "s3://bucket/file2.parquet",
                    Bytes::from("data2"),
                    None,
                    None,
                )
                .await
                .unwrap();
            cache.save_index().await.unwrap();
        }

        // Create new cache and load index
        {
            let cache = DiskFileCache::new(config).await.unwrap();
            let stats = cache.stats().await;
            assert_eq!(stats.file_count, 2);

            let data1 = cache
                .get_if_cached("s3://bucket/file1.parquet")
                .await
                .unwrap();
            assert!(data1.is_some());
        }
    }

    #[tokio::test]
    async fn test_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            max_size_bytes: 100, // Very small cache
            ..Default::default()
        };
        let cache = DiskFileCache::new(config).await.unwrap();

        // Add entries that exceed max size
        cache
            .put(
                "s3://bucket/file1.parquet",
                Bytes::from(vec![0u8; 50]),
                None,
                None,
            )
            .await
            .unwrap();
        cache
            .put(
                "s3://bucket/file2.parquet",
                Bytes::from(vec![0u8; 50]),
                None,
                None,
            )
            .await
            .unwrap();

        // This should trigger eviction
        cache
            .put(
                "s3://bucket/file3.parquet",
                Bytes::from(vec![0u8; 50]),
                None,
                None,
            )
            .await
            .unwrap();

        let stats = cache.stats().await;
        assert!(stats.total_size_bytes <= 100);
        assert!(stats.evictions > 0);
    }
}
