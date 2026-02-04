//! Cache statistics and error types

#[derive(Debug)]
pub struct CacheStats {
    pub total_rows: usize,
    pub num_measurements: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
}

impl CacheStats {
    /// Get cache usage as percentage (0-100)
    pub fn usage_percent(&self) -> usize {
        if self.max_size_bytes == 0 {
            0
        } else {
            self.size_bytes * 100 / self.max_size_bytes
        }
    }
}

/// Error returned when cache operations fail
#[derive(Debug)]
pub enum CacheFullError {
    /// Cache is at capacity, cannot accept more data
    CacheFull {
        current_bytes: usize,
        max_bytes: usize,
        batch_bytes: usize,
    },
    /// Error converting points to RecordBatch
    ConversionError(String),
}

impl std::fmt::Display for CacheFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheFullError::CacheFull {
                current_bytes,
                max_bytes,
                batch_bytes,
            } => {
                write!(
                    f,
                    "Cache full: {}MB used of {}MB max, cannot add {}KB batch",
                    current_bytes / 1024 / 1024,
                    max_bytes / 1024 / 1024,
                    batch_bytes / 1024
                )
            }
            CacheFullError::ConversionError(e) => {
                write!(f, "Failed to convert points: {}", e)
            }
        }
    }
}

impl std::error::Error for CacheFullError {}

#[derive(Debug, Clone)]
pub struct TableCacheStats {
    pub total_rows: usize,
    pub total_batches: usize,
    pub total_size_bytes: usize,
    pub partition_count: usize,
}
