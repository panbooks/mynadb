use super::types::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

/// Thread-safe metrics collector
pub struct MetricsCollector {
    start_time: Instant,
    write_metrics: Arc<RwLock<WriteMetricsCollector>>,
    query_metrics: Arc<RwLock<QueryMetricsCollector>>,
    cache_metrics: Arc<RwLock<CacheMetricsCollector>>,
    storage_metrics: Arc<RwLock<StorageMetricsCollector>>,
    memory_metrics: Arc<RwLock<MemoryMetricsCollector>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            write_metrics: Arc::new(RwLock::new(WriteMetricsCollector::new())),
            query_metrics: Arc::new(RwLock::new(QueryMetricsCollector::new())),
            cache_metrics: Arc::new(RwLock::new(CacheMetricsCollector::new())),
            storage_metrics: Arc::new(RwLock::new(StorageMetricsCollector::new())),
            memory_metrics: Arc::new(RwLock::new(MemoryMetricsCollector::new())),
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            timestamp: chrono::Utc::now().timestamp(),
            uptime_secs: self.start_time.elapsed().as_secs(),
            write: self.write_metrics.read().snapshot(),
            query: self.query_metrics.read().snapshot(),
            cache: self.cache_metrics.read().snapshot(),
            storage: self.storage_metrics.read().snapshot(),
            memory: self.memory_metrics.read().snapshot(),
        }
    }

    // Write metrics accessors
    pub fn record_write(&self, lines: u64, bytes: u64, latency_ms: f64) {
        self.write_metrics.write().record_write(lines, bytes, latency_ms);
    }

    pub fn record_write_failure(&self) {
        self.write_metrics.write().record_failure();
    }

    // Query metrics accessors
    pub fn record_query_start(&self) -> QueryHandle {
        self.query_metrics.write().record_query_start();
        QueryHandle {
            collector: Arc::clone(&self.query_metrics),
            start: Instant::now(),
        }
    }

    // Cache metrics accessors
    pub fn record_metadata_hit(&self) {
        self.cache_metrics.write().record_metadata_hit();
    }

    pub fn record_metadata_miss(&self) {
        self.cache_metrics.write().record_metadata_miss();
    }

    pub fn record_query_cache_hit(&self) {
        self.cache_metrics.write().record_query_cache_hit();
    }

    pub fn record_query_cache_miss(&self) {
        self.cache_metrics.write().record_query_cache_miss();
    }

    pub fn set_cache_sizes(&self, metadata_bytes: u64, query_cache_bytes: u64) {
        self.cache_metrics.write().set_cache_sizes(metadata_bytes, query_cache_bytes);
    }

    // Storage metrics accessors
    pub fn set_wal_size(&self, bytes: u64) {
        self.storage_metrics.write().set_wal_size(bytes);
    }

    pub fn record_wal_flush(&self) {
        self.storage_metrics.write().record_wal_flush();
    }

    pub fn set_parquet_files(&self, l0: u64, l1: u64, l2: u64, l3: u64, l4: u64, l5: u64, total_size: u64) {
        self.storage_metrics.write().set_parquet_files(l0, l1, l2, l3, l4, l5, total_size);
    }

    pub fn record_compaction_start(&self) {
        self.storage_metrics.write().record_compaction_start();
    }

    pub fn record_compaction_end(&self) {
        self.storage_metrics.write().record_compaction_end();
    }

    // Memory metrics accessors
    pub fn set_memory_usage(&self, rss: u64, cache: u64, wal: u64, query_engine: u64) {
        self.memory_metrics.write().set_memory_usage(rss, cache, wal, query_engine);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for tracking query lifecycle
pub struct QueryHandle {
    collector: Arc<RwLock<QueryMetricsCollector>>,
    start: Instant,
}

impl QueryHandle {
    pub fn finish(self, success: bool) {
        let latency_ms = self.start.elapsed().as_micros() as f64 / 1000.0;
        self.collector.write().record_query_end(latency_ms, success);
    }
}

impl Drop for QueryHandle {
    fn drop(&mut self) {
        // Auto-complete if not explicitly finished
        let latency_ms = self.start.elapsed().as_micros() as f64 / 1000.0;
        self.collector.write().record_query_end(latency_ms, true);
    }
}

// Internal collectors
struct WriteMetricsCollector {
    start_time: Instant,
    total_lines: u64,
    total_bytes: u64,
    total_writes: u64,
    failed_writes: u64,
    latencies: Vec<f64>,
}

impl WriteMetricsCollector {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_lines: 0,
            total_bytes: 0,
            total_writes: 0,
            failed_writes: 0,
            latencies: Vec::new(),
        }
    }

    fn record_write(&mut self, lines: u64, bytes: u64, latency_ms: f64) {
        self.total_lines += lines;
        self.total_bytes += bytes;
        self.total_writes += 1;
        self.latencies.push(latency_ms);

        // Keep only last 10000 latencies to prevent unbounded growth
        if self.latencies.len() > 10000 {
            self.latencies.drain(0..5000);
        }
    }

    fn record_failure(&mut self) {
        self.failed_writes += 1;
    }

    fn snapshot(&self) -> WriteMetrics {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let lines_per_sec = if elapsed_secs > 0.0 {
            self.total_lines as f64 / elapsed_secs
        } else {
            0.0
        };
        let bytes_per_sec = if elapsed_secs > 0.0 {
            self.total_bytes as f64 / elapsed_secs
        } else {
            0.0
        };

        WriteMetrics {
            lines_per_sec,
            bytes_per_sec,
            total_lines: self.total_lines,
            total_bytes: self.total_bytes,
            total_writes: self.total_writes,
            failed_writes: self.failed_writes,
            write_latency_ms: calculate_percentiles(&self.latencies),
        }
    }
}

struct QueryMetricsCollector {
    start_time: Instant,
    active_queries: u64,
    total_queries: u64,
    failed_queries: u64,
    latencies: Vec<f64>,
}

impl QueryMetricsCollector {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            active_queries: 0,
            total_queries: 0,
            failed_queries: 0,
            latencies: Vec::new(),
        }
    }

    fn record_query_start(&mut self) {
        self.active_queries += 1;
        self.total_queries += 1;
    }

    fn record_query_end(&mut self, latency_ms: f64, success: bool) {
        if self.active_queries > 0 {
            self.active_queries -= 1;
        }
        if !success {
            self.failed_queries += 1;
        }
        self.latencies.push(latency_ms);

        // Keep only last 10000 latencies
        if self.latencies.len() > 10000 {
            self.latencies.drain(0..5000);
        }
    }

    fn snapshot(&self) -> QueryMetrics {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let queries_per_sec = if elapsed_secs > 0.0 {
            self.total_queries as f64 / elapsed_secs
        } else {
            0.0
        };

        QueryMetrics {
            active_queries: self.active_queries,
            total_queries: self.total_queries,
            failed_queries: self.failed_queries,
            latency_ms: calculate_percentiles(&self.latencies),
            queries_per_sec,
        }
    }
}

struct CacheMetricsCollector {
    metadata_hits: u64,
    metadata_misses: u64,
    query_cache_hits: u64,
    query_cache_misses: u64,
    metadata_cache_size_bytes: u64,
    query_cache_size_bytes: u64,
}

impl CacheMetricsCollector {
    fn new() -> Self {
        Self {
            metadata_hits: 0,
            metadata_misses: 0,
            query_cache_hits: 0,
            query_cache_misses: 0,
            metadata_cache_size_bytes: 0,
            query_cache_size_bytes: 0,
        }
    }

    fn record_metadata_hit(&mut self) {
        self.metadata_hits += 1;
    }

    fn record_metadata_miss(&mut self) {
        self.metadata_misses += 1;
    }

    fn record_query_cache_hit(&mut self) {
        self.query_cache_hits += 1;
    }

    fn record_query_cache_miss(&mut self) {
        self.query_cache_misses += 1;
    }

    fn set_cache_sizes(&mut self, metadata_bytes: u64, query_cache_bytes: u64) {
        self.metadata_cache_size_bytes = metadata_bytes;
        self.query_cache_size_bytes = query_cache_bytes;
    }

    fn snapshot(&self) -> CacheMetrics {
        let metadata_total = self.metadata_hits + self.metadata_misses;
        let metadata_hit_rate = if metadata_total > 0 {
            self.metadata_hits as f64 / metadata_total as f64
        } else {
            0.0
        };

        let query_cache_total = self.query_cache_hits + self.query_cache_misses;
        let query_cache_hit_rate = if query_cache_total > 0 {
            self.query_cache_hits as f64 / query_cache_total as f64
        } else {
            0.0
        };

        CacheMetrics {
            metadata_hit_rate,
            metadata_hits: self.metadata_hits,
            metadata_misses: self.metadata_misses,
            query_cache_hit_rate,
            query_cache_hits: self.query_cache_hits,
            query_cache_misses: self.query_cache_misses,
            metadata_cache_size_bytes: self.metadata_cache_size_bytes,
            query_cache_size_bytes: self.query_cache_size_bytes,
        }
    }
}

struct StorageMetricsCollector {
    start_time: Instant,
    wal_size_bytes: u64,
    wal_flushes: u64,
    parquet_files_l0: u64,
    parquet_files_l1: u64,
    parquet_files_l2: u64,
    parquet_files_l3: u64,
    parquet_files_l4: u64,
    parquet_files_l5: u64,
    parquet_total_size_bytes: u64,
    compactions_running: u64,
    compactions_total: u64,
}

impl StorageMetricsCollector {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            wal_size_bytes: 0,
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

    fn set_wal_size(&mut self, bytes: u64) {
        self.wal_size_bytes = bytes;
    }

    fn record_wal_flush(&mut self) {
        self.wal_flushes += 1;
    }

    fn set_parquet_files(&mut self, l0: u64, l1: u64, l2: u64, l3: u64, l4: u64, l5: u64, total_size: u64) {
        self.parquet_files_l0 = l0;
        self.parquet_files_l1 = l1;
        self.parquet_files_l2 = l2;
        self.parquet_files_l3 = l3;
        self.parquet_files_l4 = l4;
        self.parquet_files_l5 = l5;
        self.parquet_total_size_bytes = total_size;
    }

    fn record_compaction_start(&mut self) {
        self.compactions_running += 1;
        self.compactions_total += 1;
    }

    fn record_compaction_end(&mut self) {
        if self.compactions_running > 0 {
            self.compactions_running -= 1;
        }
    }

    fn snapshot(&self) -> StorageMetrics {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let wal_flush_rate_per_sec = if elapsed_secs > 0.0 {
            self.wal_flushes as f64 / elapsed_secs
        } else {
            0.0
        };

        StorageMetrics {
            wal_size_bytes: self.wal_size_bytes,
            wal_flush_rate_per_sec,
            wal_flushes: self.wal_flushes,
            parquet_files_l0: self.parquet_files_l0,
            parquet_files_l1: self.parquet_files_l1,
            parquet_files_l2: self.parquet_files_l2,
            parquet_files_l3: self.parquet_files_l3,
            parquet_files_l4: self.parquet_files_l4,
            parquet_files_l5: self.parquet_files_l5,
            parquet_total_size_bytes: self.parquet_total_size_bytes,
            compactions_running: self.compactions_running,
            compactions_total: self.compactions_total,
        }
    }
}

struct MemoryMetricsCollector {
    rss_bytes: u64,
    cache_bytes: u64,
    wal_bytes: u64,
    query_engine_bytes: u64,
}

impl MemoryMetricsCollector {
    fn new() -> Self {
        Self {
            rss_bytes: 0,
            cache_bytes: 0,
            wal_bytes: 0,
            query_engine_bytes: 0,
        }
    }

    fn set_memory_usage(&mut self, rss: u64, cache: u64, wal: u64, query_engine: u64) {
        self.rss_bytes = rss;
        self.cache_bytes = cache;
        self.wal_bytes = wal;
        self.query_engine_bytes = query_engine;
    }

    fn snapshot(&self) -> MemoryMetrics {
        MemoryMetrics {
            rss_bytes: self.rss_bytes,
            cache_bytes: self.cache_bytes,
            wal_bytes: self.wal_bytes,
            query_engine_bytes: self.query_engine_bytes,
            total_allocated_bytes: self.cache_bytes + self.wal_bytes + self.query_engine_bytes,
        }
    }
}

/// Calculate percentiles from a list of values
fn calculate_percentiles(values: &[f64]) -> PercentileStats {
    if values.is_empty() {
        return PercentileStats::default();
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let count = sorted.len();
    let p50_idx = (count as f64 * 0.50) as usize;
    let p95_idx = (count as f64 * 0.95) as usize;
    let p99_idx = (count as f64 * 0.99) as usize;

    let sum: f64 = sorted.iter().sum();
    let mean = sum / count as f64;

    PercentileStats {
        p50: sorted[p50_idx.min(count - 1)],
        p95: sorted[p95_idx.min(count - 1)],
        p99: sorted[p99_idx.min(count - 1)],
        min: sorted[0],
        max: sorted[count - 1],
        mean,
        count: count as u64,
    }
}

// Global metrics instance
pub static METRICS: std::sync::LazyLock<MetricsCollector> =
    std::sync::LazyLock::new(|| MetricsCollector::new());
