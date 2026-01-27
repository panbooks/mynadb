//! Metadata Cache for Query Engine
//!
//! Provides caching for table metadata, file listings, and Parquet file metadata
//! to avoid repeated filesystem scans during query execution.

use parking_lot::RwLock as ParkingRwLock;
use parquet::file::metadata::ParquetMetaData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Cache TTL for table metadata (5 minutes)
const TABLE_CACHE_TTL_SECS: u64 = 300;

/// Cache TTL for file listings (60 seconds)
const FILE_LISTING_CACHE_TTL_SECS: u64 = 60;

/// Maximum entries in Parquet metadata cache
const PARQUET_METADATA_CACHE_MAX_ENTRIES: usize = 10000;

/// Cached table information
#[derive(Debug, Clone)]
pub struct CachedTableInfo {
    /// When this entry was cached
    pub cached_at: Instant,
    /// Table version (incremented on writes)
    pub version: u64,
    /// Whether the table has partitions
    pub has_partitions: bool,
    /// List of partition directories (date=YYYY-MM-DD)
    pub partitions: Vec<String>,
}

/// Cached file listing for a directory
#[derive(Debug, Clone)]
pub struct CachedFileListing {
    /// When this listing was cached
    pub cached_at: Instant,
    /// List of Parquet files
    pub files: Vec<PathBuf>,
    /// Total file count
    pub file_count: usize,
}

/// Parquet metadata cache entry
#[derive(Debug)]
struct CachedParquetMetadata {
    /// The cached metadata
    metadata: Arc<ParquetMetaData>,
    /// When this was cached
    cached_at: Instant,
    /// File size at cache time (for invalidation)
    file_size: u64,
}

/// Cache statistics for monitoring
#[derive(Debug, Default)]
struct CacheStats {
    table_hits: AtomicU64,
    table_misses: AtomicU64,
    file_listing_hits: AtomicU64,
    file_listing_misses: AtomicU64,
    parquet_metadata_hits: AtomicU64,
    parquet_metadata_misses: AtomicU64,
}

/// Table metadata cache for fast lookups
pub struct TableMetadataCache {
    /// Table info cache: (catalog, schema, table) -> CachedTableInfo
    tables: ParkingRwLock<HashMap<(String, String, String), CachedTableInfo>>,
    /// File listing cache: directory path -> CachedFileListing
    file_listings: ParkingRwLock<HashMap<PathBuf, CachedFileListing>>,
    /// Parquet metadata cache: file path -> CachedParquetMetadata
    parquet_metadata: ParkingRwLock<HashMap<PathBuf, CachedParquetMetadata>>,
    /// Global version counter (incremented on any write)
    global_version: AtomicU64,
    /// Cache statistics
    stats: CacheStats,
}

impl TableMetadataCache {
    pub fn new() -> Self {
        Self {
            tables: ParkingRwLock::new(HashMap::new()),
            file_listings: ParkingRwLock::new(HashMap::new()),
            parquet_metadata: ParkingRwLock::new(HashMap::new()),
            global_version: AtomicU64::new(0),
            stats: CacheStats::default(),
        }
    }

    /// Get cached table info if valid
    pub fn get_table_info(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<CachedTableInfo> {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        let tables = self.tables.read();

        if let Some(info) = tables.get(&key) {
            // Check TTL
            if info.cached_at.elapsed() < Duration::from_secs(TABLE_CACHE_TTL_SECS) {
                self.stats.table_hits.fetch_add(1, Ordering::Relaxed);
                return Some(info.clone());
            }
        }
        self.stats.table_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache table info
    pub fn set_table_info(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        has_partitions: bool,
        partitions: Vec<String>,
    ) {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        let info = CachedTableInfo {
            cached_at: Instant::now(),
            version: self.global_version.load(Ordering::Relaxed),
            has_partitions,
            partitions,
        };
        self.tables.write().insert(key, info);
    }

    /// Get cached file listing if valid
    pub fn get_file_listing(&self, dir: &PathBuf) -> Option<CachedFileListing> {
        let listings = self.file_listings.read();

        if let Some(listing) = listings.get(dir) {
            if listing.cached_at.elapsed() < Duration::from_secs(FILE_LISTING_CACHE_TTL_SECS) {
                self.stats.file_listing_hits.fetch_add(1, Ordering::Relaxed);
                return Some(listing.clone());
            }
        }
        self.stats
            .file_listing_misses
            .fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache file listing
    pub fn set_file_listing(&self, dir: PathBuf, files: Vec<PathBuf>) {
        let listing = CachedFileListing {
            cached_at: Instant::now(),
            file_count: files.len(),
            files,
        };
        self.file_listings.write().insert(dir, listing);
    }

    /// Get cached Parquet metadata if valid
    pub fn get_parquet_metadata(&self, file: &PathBuf) -> Option<Arc<ParquetMetaData>> {
        let cache = self.parquet_metadata.read();

        if let Some(entry) = cache.get(file) {
            // Check if file size changed (quick invalidation check)
            if let Ok(metadata) = std::fs::metadata(file) {
                if metadata.len() == entry.file_size {
                    self.stats
                        .parquet_metadata_hits
                        .fetch_add(1, Ordering::Relaxed);
                    return Some(entry.metadata.clone());
                }
            }
        }
        self.stats
            .parquet_metadata_misses
            .fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache Parquet metadata
    pub fn set_parquet_metadata(
        &self,
        file: PathBuf,
        metadata: Arc<ParquetMetaData>,
        file_size: u64,
    ) {
        let mut cache = self.parquet_metadata.write();

        // Evict old entries if cache is too large
        if cache.len() >= PARQUET_METADATA_CACHE_MAX_ENTRIES {
            // Remove oldest 10% of entries
            let to_remove: Vec<PathBuf> = cache
                .iter()
                .map(|(k, v)| (k.clone(), v.cached_at))
                .collect::<Vec<_>>()
                .into_iter()
                .take(PARQUET_METADATA_CACHE_MAX_ENTRIES / 10)
                .map(|(k, _)| k)
                .collect();

            for key in to_remove {
                cache.remove(&key);
            }
        }

        cache.insert(
            file,
            CachedParquetMetadata {
                metadata,
                cached_at: Instant::now(),
                file_size,
            },
        );
    }

    /// Invalidate cache for a specific table (called after writes)
    pub fn invalidate_table(&self, catalog: &str, schema: &str, table: &str) {
        let key = (catalog.to_string(), schema.to_string(), table.to_string());
        self.tables.write().remove(&key);

        // Also invalidate file listings for this table's directory
        // This ensures new partitions/files are discovered after flush
        let table_path_suffix = format!("{}/{}/{}", catalog, schema, table);
        self.file_listings
            .write()
            .retain(|path, _| !path.to_string_lossy().contains(&table_path_suffix));

        // Increment global version
        self.global_version.fetch_add(1, Ordering::Relaxed);

        debug!(
            "Invalidated cache for table: {}.{}.{}",
            catalog, schema, table
        );
    }

    /// Invalidate all cache entries for a catalog (database)
    pub fn invalidate_catalog(&self, catalog: &str) {
        let mut tables = self.tables.write();
        tables.retain(|(c, _, _), _| c != catalog);

        // Also invalidate file listings for this catalog's directories
        let catalog_path_component = format!("/{}/", catalog);
        self.file_listings
            .write()
            .retain(|path, _| !path.to_string_lossy().contains(&catalog_path_component));

        self.global_version.fetch_add(1, Ordering::Relaxed);
        debug!("Invalidated cache for catalog: {}", catalog);
    }

    /// Invalidate file listing cache for a directory
    pub fn invalidate_file_listing(&self, dir: &PathBuf) {
        self.file_listings.write().remove(dir);
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStatistics {
        CacheStatistics {
            table_cache_size: self.tables.read().len(),
            file_listing_cache_size: self.file_listings.read().len(),
            parquet_metadata_cache_size: self.parquet_metadata.read().len(),
            table_hits: self.stats.table_hits.load(Ordering::Relaxed),
            table_misses: self.stats.table_misses.load(Ordering::Relaxed),
            file_listing_hits: self.stats.file_listing_hits.load(Ordering::Relaxed),
            file_listing_misses: self.stats.file_listing_misses.load(Ordering::Relaxed),
            parquet_metadata_hits: self.stats.parquet_metadata_hits.load(Ordering::Relaxed),
            parquet_metadata_misses: self.stats.parquet_metadata_misses.load(Ordering::Relaxed),
        }
    }

    /// Clear all caches
    pub fn clear(&self) {
        self.tables.write().clear();
        self.file_listings.write().clear();
        self.parquet_metadata.write().clear();
        self.global_version.fetch_add(1, Ordering::Relaxed);
        info!("All query caches cleared");
    }
}

impl Default for TableMetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatistics {
    pub table_cache_size: usize,
    pub file_listing_cache_size: usize,
    pub parquet_metadata_cache_size: usize,
    pub table_hits: u64,
    pub table_misses: u64,
    pub file_listing_hits: u64,
    pub file_listing_misses: u64,
    pub parquet_metadata_hits: u64,
    pub parquet_metadata_misses: u64,
}

impl CacheStatistics {
    pub fn table_hit_rate(&self) -> f64 {
        let total = self.table_hits + self.table_misses;
        if total == 0 {
            0.0
        } else {
            self.table_hits as f64 / total as f64 * 100.0
        }
    }

    pub fn parquet_hit_rate(&self) -> f64 {
        let total = self.parquet_metadata_hits + self.parquet_metadata_misses;
        if total == 0 {
            0.0
        } else {
            self.parquet_metadata_hits as f64 / total as f64 * 100.0
        }
    }
}

/// Statistics about Parquet files in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableParquetStats {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub total_files: usize,
    pub total_rows: u64,
    pub total_size_bytes: u64,
    pub total_row_groups: usize,
    pub partition_count: usize,
}
