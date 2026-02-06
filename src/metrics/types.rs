use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Percentile statistics for latency measurements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentileStats {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub count: u64,
}

impl Default for PercentileStats {
    fn default() -> Self {
        Self {
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            count: 0,
        }
    }
}

/// Write throughput metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteMetrics {
    pub lines_per_sec: f64,
    pub bytes_per_sec: f64,
    pub total_lines: u64,
    pub total_bytes: u64,
    pub total_writes: u64,
    pub failed_writes: u64,
    pub write_latency_ms: PercentileStats,
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self {
            lines_per_sec: 0.0,
            bytes_per_sec: 0.0,
            total_lines: 0,
            total_bytes: 0,
            total_writes: 0,
            failed_writes: 0,
            write_latency_ms: PercentileStats::default(),
        }
    }
}

/// Query performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetrics {
    pub active_queries: u64,
    pub total_queries: u64,
    pub failed_queries: u64,
    pub latency_ms: PercentileStats,
    pub queries_per_sec: f64,
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self {
            active_queries: 0,
            total_queries: 0,
            failed_queries: 0,
            latency_ms: PercentileStats::default(),
            queries_per_sec: 0.0,
        }
    }
}

/// Cache performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    pub metadata_hit_rate: f64,
    pub metadata_hits: u64,
    pub metadata_misses: u64,
    pub query_cache_hit_rate: f64,
    pub query_cache_hits: u64,
    pub query_cache_misses: u64,
    pub metadata_cache_size_bytes: u64,
    pub query_cache_size_bytes: u64,
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self {
            metadata_hit_rate: 0.0,
            metadata_hits: 0,
            metadata_misses: 0,
            query_cache_hit_rate: 0.0,
            query_cache_hits: 0,
            query_cache_misses: 0,
            metadata_cache_size_bytes: 0,
            query_cache_size_bytes: 0,
        }
    }
}

/// Storage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub wal_size_bytes: u64,
    pub wal_flush_rate_per_sec: f64,
    pub wal_flushes: u64,
    pub parquet_files_l0: u64,
    pub parquet_files_l1: u64,
    pub parquet_files_l2: u64,
    pub parquet_files_l3: u64,
    pub parquet_files_l4: u64,
    pub parquet_files_l5: u64,
    pub parquet_total_size_bytes: u64,
    pub compactions_running: u64,
    pub compactions_total: u64,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            wal_size_bytes: 0,
            wal_flush_rate_per_sec: 0.0,
            wal_flushes: 0,
            parquet_files_l0: 0,
            parquet_files_l1: 0,
            parquet_files_l2: 0,
            parquet_files_l3: 0,
            parquet_files_l4: 0,
            parquet_files_l5: 0,
            parquet_total_size_bytes: 0,
            compactions_running: 0,
            compactions_total: 0,
        }
    }
}

/// Memory usage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub rss_bytes: u64,
    pub cache_bytes: u64,
    pub wal_bytes: u64,
    pub query_engine_bytes: u64,
    pub total_allocated_bytes: u64,
}

impl Default for MemoryMetrics {
    fn default() -> Self {
        Self {
            rss_bytes: 0,
            cache_bytes: 0,
            wal_bytes: 0,
            query_engine_bytes: 0,
            total_allocated_bytes: 0,
        }
    }
}

/// Complete metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp: i64,
    pub uptime_secs: u64,
    pub write: WriteMetrics,
    pub query: QueryMetrics,
    pub cache: CacheMetrics,
    pub storage: StorageMetrics,
    pub memory: MemoryMetrics,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp(),
            uptime_secs: 0,
            write: WriteMetrics::default(),
            query: QueryMetrics::default(),
            cache: CacheMetrics::default(),
            storage: StorageMetrics::default(),
            memory: MemoryMetrics::default(),
        }
    }
}

/// Latency measurement helper
pub struct LatencyMeasurement {
    pub start: std::time::Instant,
}

impl LatencyMeasurement {
    pub fn start() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_micros() as f64 / 1000.0
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}
