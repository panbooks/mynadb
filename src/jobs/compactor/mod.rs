//! Compactor Module
//!
//! Responsible for merging small Parquet files into larger ones using a leveled
//! compaction strategy (similar to LSM-tree).

use arrow::datatypes::Schema;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tracing::debug;

use crate::meta::client::MetaClient;
use crate::query::catalog::SharedMetadataManager;
use crate::services::rate_limiter::RateLimiter;
use crate::storage::storage::Storage;

// Sub-modules containing impl blocks for Compactor
mod background;
mod catalog;
mod full;
mod parquet_io;
mod partition;
mod schema;
mod sorting;

// Re-export public items
pub use background::*;
pub use catalog::*;
pub use full::*;
pub use partition::*;

// Compaction constants
pub(crate) const LEVEL_PREFIX: char = 'L';
pub(crate) const LEVEL_SEPARATOR: char = '_';
pub(crate) const PARQUET_EXT: &str = ".parquet";

// Default compaction thresholds
const DEFAULT_MIN_FILES: usize = 5;
const DEFAULT_MAJOR_MIN_FILES: usize = 4; // Minimum files for L1+ compaction
const DEFAULT_TARGET_FILE_SIZE_MB: u64 = 500;
const DEFAULT_MAX_LEVEL: u32 = 4; // L0, L1, L2, L3, L4
pub(crate) const MB_TO_BYTES: u64 = 1024 * 1024;

// Parquet optimization constants
pub(crate) const ROW_GROUP_SIZE: usize = 1_000_000; // 1M rows per row group
pub(crate) const BLOOM_FILTER_FPP: f64 = 0.01; // 1% false positive rate
pub(crate) const BLOOM_FILTER_NDV: u64 = 10_000; // Expected distinct values

// Cardinality-based sorting constants
/// Maximum rows to sample for cardinality estimation (performance optimization)
pub(crate) const CARDINALITY_SAMPLE_SIZE: usize = 100_000;
/// Threshold for "high cardinality" - tags with more distinct values are sorted last
pub(crate) const HIGH_CARDINALITY_THRESHOLD: usize = 1000;

// Distributed lock constants
/// TTL for compaction locks in milliseconds (5 minutes)
/// Compaction can take a while for large partitions, so we use a longer TTL.
/// Lock is released immediately on completion/failure via RAII guard.
pub(crate) const COMPACTION_LOCK_TTL_MS: u64 = 300_000;

/// Type alias for compaction locks map
/// Key: (catalog, schema, table, date_partition)
pub(crate) type CompactionLocks =
    Arc<RwLock<HashMap<(String, String, String, String), Arc<tokio::sync::Mutex<()>>>>>;

/// RAII guard for distributed compaction locks - releases lock on drop
///
/// In cluster mode, this prevents multiple nodes from compacting the same partition
/// simultaneously. The lock is released when compaction completes (success or failure).
pub(crate) struct DistributedLockGuard {
    pub(crate) meta_client: Option<Arc<MetaClient>>,
    pub(crate) lock_key: String,
    pub(crate) fencing_token: u64,
}

impl Drop for DistributedLockGuard {
    fn drop(&mut self) {
        if let Some(ref mc) = self.meta_client {
            let mc = mc.clone();
            let lock_key = self.lock_key.clone();
            let token = self.fencing_token;
            // Spawn async task to release lock (can't await in drop)
            tokio::spawn(async move {
                if let Err(e) = mc.unlock(&lock_key, token).await {
                    // Not critical - lock will expire via TTL
                    debug!("Failed to release compaction lock {}: {}", lock_key, e);
                } else {
                    debug!("Released compaction lock {}", lock_key);
                }
            });
        }
    }
}

/// Compactor is responsible for merging small Parquet files into larger ones
/// using a leveled compaction strategy (similar to LSM-tree)
///
/// Supports both standalone (local filesystem) and cluster (S3/MinIO) modes.
/// All storage operations use the Object Store abstraction.
///
/// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/*.parquet
pub struct Compactor {
    /// Storage backend for Object Store operations
    pub(crate) storage: Arc<Storage>,
    /// Lock to prevent concurrent compaction and read/write operations
    pub(crate) locks: CompactionLocks,
    /// Semaphore to limit concurrent LSM compaction operations
    pub(crate) compaction_semaphore: Option<Arc<Semaphore>>,
    /// Semaphore to limit concurrent full compaction operations
    pub(crate) full_compaction_semaphore: Option<Arc<Semaphore>>,
    /// Read rate limiter (None = unlimited)
    pub(crate) read_rate_limiter: Option<Arc<RateLimiter>>,
    /// Write rate limiter (None = unlimited)
    pub(crate) write_rate_limiter: Option<Arc<RateLimiter>>,
    /// Metadata manager for updating file registry after compaction
    pub(crate) metadata_manager: Option<SharedMetadataManager>,
    /// Meta client for distributed locks (cluster mode only)
    /// When set, compaction acquires a distributed lock to prevent multiple nodes
    /// from compacting the same partition simultaneously.
    pub(crate) meta_client: Option<Arc<MetaClient>>,
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of L0 files to trigger minor compaction (L0 → L1)
    pub min_files: usize,
    /// Minimum number of files at L1+ to trigger major compaction (L1 → L2, L2 → L3, etc.)
    pub major_min_files: usize,
    /// Target file size after compaction (default: 500MB) for LSM compaction
    pub target_file_size: u64,
    /// Maximum compaction level (0-4: L0, L1, L2, L3, L4)
    pub max_level: u32,

    // Full compaction settings (date-based)
    /// Enable full compaction for yesterday's partitions
    pub full_compaction_enabled: bool,
    /// Hour to run full compaction (0-23, default: 1 for 01:00)
    pub full_compaction_hour: u32,
    /// Target file size for full compaction in bytes (default: 200MB)
    /// Partitions larger than this will be skipped
    pub full_compaction_target_size: u64,

    // Rate limiting settings
    /// Maximum concurrent LSM compaction operations (0 = unlimited)
    pub max_concurrent_compactions: usize,
    /// Maximum concurrent full compaction operations (0 = unlimited)
    pub full_compaction_max_concurrent: usize,
    /// Read rate limit in bytes/sec (0 = unlimited)
    pub read_rate_limit_bytes_per_sec: u64,
    /// Write rate limit in bytes/sec (0 = unlimited)
    pub write_rate_limit_bytes_per_sec: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            min_files: DEFAULT_MIN_FILES,
            major_min_files: DEFAULT_MAJOR_MIN_FILES,
            target_file_size: DEFAULT_TARGET_FILE_SIZE_MB * MB_TO_BYTES,
            max_level: DEFAULT_MAX_LEVEL,
            full_compaction_enabled: true,
            full_compaction_hour: 3,                        // 03:00
            full_compaction_target_size: 200 * MB_TO_BYTES, // 200MB
            max_concurrent_compactions: 8,
            full_compaction_max_concurrent: 2,
            read_rate_limit_bytes_per_sec: 0,  // Unlimited
            write_rate_limit_bytes_per_sec: 0, // Unlimited
        }
    }
}

impl From<&crate::config::CompactionConfig> for CompactionConfig {
    fn from(config: &crate::config::CompactionConfig) -> Self {
        CompactionConfig {
            min_files: config.min_files,
            major_min_files: config.major_min_files,
            target_file_size: config.target_file_size_mb * MB_TO_BYTES,
            max_level: config.max_level,
            full_compaction_enabled: config.full_compaction_enabled,
            full_compaction_hour: config.full_compaction_hour,
            full_compaction_target_size: config.full_compaction_target_size_mb * MB_TO_BYTES,
            max_concurrent_compactions: config.max_concurrent_compactions,
            full_compaction_max_concurrent: config.full_compaction_max_concurrent,
            read_rate_limit_bytes_per_sec: config.read_rate_limit_mb_per_sec * MB_TO_BYTES,
            write_rate_limit_bytes_per_sec: config.write_rate_limit_mb_per_sec * MB_TO_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct CompactionResult {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date_partition: String,
    pub l0_files_before: usize,
    pub l0_files_after: usize,
    pub files_before: usize,
    pub files_after: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub rows_compacted: usize,
}

/// Information about a Parquet file for compaction
///
/// Uses String path for Object Store compatibility (works with both local and S3).
#[derive(Debug, Clone)]
pub(crate) struct ParquetFileInfo {
    /// Object Store path (relative to storage base)
    pub(crate) path: String,
    /// File size in bytes
    pub(crate) size: u64,
    /// Compaction level (0 = L0, 1 = L1, etc.)
    pub(crate) level: u32,
    /// Timestamp from filename
    pub(crate) timestamp: i64,
}

impl ParquetFileInfo {
    /// Parse file info from an Object Store path and size
    ///
    /// Extracts level and timestamp from filename patterns:
    /// - L0_1703251234567.parquet (new format)
    /// - 1703251234567.parquet (legacy, treated as L0)
    /// - FULL_1703251234567.parquet (full compaction output)
    pub(crate) fn from_path_and_size(path: String, size: u64) -> Result<Self, String> {
        // Extract filename from path
        let filename = path
            .rsplit('/')
            .next()
            .ok_or("Invalid path: no filename")?;

        let (level, timestamp) = if filename.starts_with("FULL_") {
            // Full compaction output: FULL_1703251234567.parquet (level 255)
            let timestamp = filename
                .trim_start_matches("FULL_")
                .trim_end_matches(PARQUET_EXT)
                .parse::<i64>()
                .map_err(|_| "Invalid timestamp in FULL filename")?;
            (255, timestamp)
        } else if filename.starts_with(LEVEL_PREFIX) {
            // New format: L0_1703251234567.parquet
            let parts: Vec<&str> = filename.split(LEVEL_SEPARATOR).collect();
            if parts.len() >= 2 {
                let level = parts[0]
                    .trim_start_matches(LEVEL_PREFIX)
                    .parse::<u32>()
                    .map_err(|_| "Invalid level in filename")?;
                let timestamp = parts[1]
                    .trim_end_matches(PARQUET_EXT)
                    .parse::<i64>()
                    .map_err(|_| "Invalid timestamp in filename")?;
                (level, timestamp)
            } else {
                return Err("Invalid level format".into());
            }
        } else {
            // Legacy format: 1703251234567.parquet (treat as L0)
            let timestamp = filename
                .trim_end_matches(PARQUET_EXT)
                .parse::<i64>()
                .map_err(|_| "Invalid timestamp in legacy format")?;
            (0, timestamp)
        };

        Ok(ParquetFileInfo {
            path,
            size,
            level,
            timestamp,
        })
    }
}

impl Compactor {
    /// Create a new Compactor with storage backend and configuration
    pub fn new(storage: Arc<Storage>, config: &CompactionConfig) -> Self {
        Self::with_metadata(storage, config, None)
    }

    /// Create a new Compactor with storage, config, and metadata manager
    pub fn with_metadata(
        storage: Arc<Storage>,
        config: &CompactionConfig,
        metadata_manager: Option<SharedMetadataManager>,
    ) -> Self {
        // Create semaphore if max_concurrent_compactions > 0
        let compaction_semaphore = if config.max_concurrent_compactions > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_compactions)))
        } else {
            None
        };

        // Create semaphore for full compaction if full_compaction_max_concurrent > 0
        let full_compaction_semaphore = if config.full_compaction_max_concurrent > 0 {
            Some(Arc::new(Semaphore::new(
                config.full_compaction_max_concurrent,
            )))
        } else {
            None
        };

        // Create rate limiters if configured
        let read_rate_limiter = if config.read_rate_limit_bytes_per_sec > 0 {
            Some(Arc::new(RateLimiter::new(
                config.read_rate_limit_bytes_per_sec,
            )))
        } else {
            None
        };

        let write_rate_limiter = if config.write_rate_limit_bytes_per_sec > 0 {
            Some(Arc::new(RateLimiter::new(
                config.write_rate_limit_bytes_per_sec,
            )))
        } else {
            None
        };

        Compactor {
            storage,
            locks: Arc::new(RwLock::new(HashMap::new())),
            compaction_semaphore,
            full_compaction_semaphore,
            read_rate_limiter,
            write_rate_limiter,
            metadata_manager,
            meta_client: None,
        }
    }

    /// Set the Meta client for distributed locking in cluster mode
    ///
    /// When MetaClient is set, the compactor will acquire a distributed lock
    /// before compacting each partition to prevent multiple nodes from
    /// compacting the same partition simultaneously.
    pub fn with_meta_client(mut self, meta_client: Arc<MetaClient>) -> Self {
        self.meta_client = Some(meta_client);
        self
    }

    /// Get or create a lock for a specific catalog/schema/table/partition tuple
    pub(crate) async fn get_lock(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_partition: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let key = (
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            date_partition.to_string(),
        );
        let mut locks = self.locks.write().await;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Create optimized WriterProperties with Bloom filters and compression
    pub(crate) fn create_optimized_writer_props(schema: &Schema) -> WriterProperties {
        use arrow::datatypes::DataType;

        let mut builder = WriterProperties::builder()
            // Row group size for good statistics granularity
            .set_max_row_group_size(ROW_GROUP_SIZE)
            // ZSTD compression (good balance of speed and ratio)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
            // Enable dictionary encoding for better compression of repeated values
            .set_dictionary_enabled(true)
            // Enable statistics for all columns
            .set_statistics_enabled(EnabledStatistics::Page);

        // Enable Bloom filters for string columns (tags)
        for field in schema.fields() {
            if field.data_type() == &DataType::Utf8 {
                let col_path: parquet::schema::types::ColumnPath = field.name().clone().into();
                builder = builder
                    .set_column_bloom_filter_enabled(col_path.clone(), true)
                    .set_column_bloom_filter_fpp(col_path.clone(), BLOOM_FILTER_FPP)
                    .set_column_bloom_filter_ndv(col_path, BLOOM_FILTER_NDV);
            }
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests;
