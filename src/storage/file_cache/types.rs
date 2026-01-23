//! File cache types

use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Cache entry metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntryMeta {
    /// Original source path (e.g., "s3://bucket/path/file.parquet")
    pub source_path: String,
    /// Local cache path
    pub cache_path: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// ETag or version for invalidation
    pub etag: Option<String>,
    /// When the file was cached (Unix millis)
    pub cached_at: i64,
    /// Last access time (Unix millis)
    pub last_accessed: i64,
    /// Access count
    pub access_count: u64,
    /// Partition date (for priority scoring)
    pub partition_date: Option<String>,
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Total cache size in bytes
    pub total_size_bytes: u64,
    /// Maximum cache size in bytes
    pub max_size_bytes: u64,
    /// Number of cached files
    pub file_count: usize,
    /// Cache hit count
    pub hits: u64,
    /// Cache miss count
    pub misses: u64,
    /// Eviction count
    pub evictions: u64,
    /// Bytes read from cache
    pub bytes_read_from_cache: u64,
    /// Bytes fetched from source
    pub bytes_fetched_from_source: u64,
}

impl CacheStats {
    /// Calculate hit rate
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Calculate fill percentage
    pub fn fill_percentage(&self) -> f64 {
        if self.max_size_bytes == 0 {
            0.0
        } else {
            (self.total_size_bytes as f64 / self.max_size_bytes as f64) * 100.0
        }
    }
}

/// Cache entry with runtime info
#[derive(Debug)]
pub struct CacheEntry {
    /// Entry metadata
    pub meta: CacheEntryMeta,
    /// In-memory last access (for LRU)
    pub last_access: Instant,
}

impl CacheEntry {
    /// Create a new cache entry
    pub fn new(meta: CacheEntryMeta) -> Self {
        Self {
            meta,
            last_access: Instant::now(),
        }
    }

    /// Update access time
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        self.meta.last_accessed = chrono::Utc::now().timestamp_millis();
        self.meta.access_count += 1;
    }

    /// Calculate priority score for eviction (lower = evict first)
    pub fn priority_score(&self) -> f64 {
        let now = Instant::now();
        let age_secs = now.duration_since(self.last_access).as_secs_f64();

        // Base score from recency (higher is better)
        let recency_score = 1.0 / (1.0 + age_secs / 3600.0); // Decay over hours

        // Frequency bonus (more accesses = higher score)
        let frequency_score = (self.meta.access_count as f64).ln().max(1.0);

        // Partition date bonus (recent partitions = higher score)
        let partition_score = if let Some(ref date) = self.meta.partition_date {
            // Parse date and calculate days ago
            if let Ok(partition_date) = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
                let today = chrono::Utc::now().date_naive();
                let days_ago = (today - partition_date).num_days();
                match days_ago {
                    0 => 10.0,     // Today
                    1 => 8.0,      // Yesterday
                    2..=7 => 5.0,  // This week
                    8..=30 => 2.0, // This month
                    _ => 1.0,      // Older
                }
            } else {
                1.0
            }
        } else {
            1.0
        };

        recency_score * frequency_score * partition_score
    }
}

/// Result type for cache operations
pub type CacheResult<T> = Result<T, CacheError>;

/// Cache errors
#[derive(Debug, Clone)]
pub enum CacheError {
    /// IO error
    IoError(String),
    /// File not found
    NotFound(String),
    /// Cache is full
    CacheFull,
    /// Serialization error
    SerializationError(String),
    /// Source fetch error
    FetchError(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::IoError(msg) => write!(f, "IO error: {}", msg),
            CacheError::NotFound(path) => write!(f, "Not found: {}", path),
            CacheError::CacheFull => write!(f, "Cache is full"),
            CacheError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            CacheError::FetchError(msg) => write!(f, "Fetch error: {}", msg),
        }
    }
}

impl std::error::Error for CacheError {}

/// Cache key for file identification
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    /// Source path
    pub path: String,
    /// Optional row group index (for row-group level caching)
    pub row_group: Option<usize>,
}

impl CacheKey {
    /// Create a key for a whole file
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            row_group: None,
        }
    }

    /// Create a key for a specific row group
    pub fn row_group(path: impl Into<String>, row_group: usize) -> Self {
        Self {
            path: path.into(),
            row_group: Some(row_group),
        }
    }

    /// Generate a hash-based file name for local storage
    pub fn to_cache_filename(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash = hasher.finish();

        match self.row_group {
            Some(rg) => format!("{:016x}_rg{}.parquet", hash, rg),
            None => format!("{:016x}.parquet", hash),
        }
    }
}

/// Index file for cache persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheIndex {
    /// Version of the index format
    pub version: u32,
    /// Index timestamp
    pub indexed_at: i64,
    /// All cache entries
    pub entries: Vec<CacheEntryMeta>,
}

impl CacheIndex {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            indexed_at: chrono::Utc::now().timestamp_millis(),
            entries: Vec::new(),
        }
    }
}

impl Default for CacheIndex {
    fn default() -> Self {
        Self::new()
    }
}
