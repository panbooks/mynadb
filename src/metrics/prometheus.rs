use super::types::*;
use std::fmt::Write;

/// Convert metrics snapshot to Prometheus text format
pub fn to_prometheus_format(snapshot: &MetricsSnapshot) -> String {
    let mut output = String::new();

    // System metrics
    writeln!(&mut output, "# HELP mynadb_uptime_seconds Server uptime in seconds").unwrap();
    writeln!(&mut output, "# TYPE mynadb_uptime_seconds counter").unwrap();
    writeln!(&mut output, "mynadb_uptime_seconds {}", snapshot.uptime_secs).unwrap();
    writeln!(&mut output).unwrap();

    // Write metrics
    write_prometheus_section(&mut output, "write", &snapshot.write);

    // Query metrics
    write_prometheus_section(&mut output, "query", &snapshot.query);

    // Cache metrics
    write_prometheus_section(&mut output, "cache", &snapshot.cache);

    // Storage metrics
    write_prometheus_section(&mut output, "storage", &snapshot.storage);

    // Memory metrics
    write_prometheus_section(&mut output, "memory", &snapshot.memory);

    output
}

fn write_prometheus_section(output: &mut String, prefix: &str, metrics: &dyn PrometheusExport) {
    metrics.export_prometheus(output, prefix);
    writeln!(output).unwrap();
}

pub trait PrometheusExport {
    fn export_prometheus(&self, output: &mut String, prefix: &str);
}

impl PrometheusExport for WriteMetrics {
    fn export_prometheus(&self, output: &mut String, prefix: &str) {
        // Throughput
        writeln!(output, "# HELP mynadb_{}_lines_per_second Write throughput in lines per second", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_lines_per_second gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_lines_per_second {}", prefix, self.lines_per_sec).unwrap();

        writeln!(output, "# HELP mynadb_{}_bytes_per_second Write throughput in bytes per second", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_bytes_per_second gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_bytes_per_second {}", prefix, self.bytes_per_sec).unwrap();

        // Totals
        writeln!(output, "# HELP mynadb_{}_lines_total Total lines written", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_lines_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_lines_total {}", prefix, self.total_lines).unwrap();

        writeln!(output, "# HELP mynadb_{}_bytes_total Total bytes written", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_bytes_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_bytes_total {}", prefix, self.total_bytes).unwrap();

        writeln!(output, "# HELP mynadb_{}_requests_total Total write requests", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_requests_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_requests_total {}", prefix, self.total_writes).unwrap();

        writeln!(output, "# HELP mynadb_{}_requests_failed_total Total failed write requests", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_requests_failed_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_requests_failed_total {}", prefix, self.failed_writes).unwrap();

        // Latency percentiles
        export_percentiles(output, &format!("mynadb_{}_latency_milliseconds", prefix), &self.write_latency_ms);
    }
}

impl PrometheusExport for QueryMetrics {
    fn export_prometheus(&self, output: &mut String, prefix: &str) {
        writeln!(output, "# HELP mynadb_{}_active_queries Number of currently active queries", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_active_queries gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_active_queries {}", prefix, self.active_queries).unwrap();

        writeln!(output, "# HELP mynadb_{}_queries_total Total queries executed", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_queries_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_queries_total {}", prefix, self.total_queries).unwrap();

        writeln!(output, "# HELP mynadb_{}_queries_failed_total Total failed queries", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_queries_failed_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_queries_failed_total {}", prefix, self.failed_queries).unwrap();

        writeln!(output, "# HELP mynadb_{}_queries_per_second Query rate per second", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_queries_per_second gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_queries_per_second {}", prefix, self.queries_per_sec).unwrap();

        export_percentiles(output, &format!("mynadb_{}_latency_milliseconds", prefix), &self.latency_ms);
    }
}

impl PrometheusExport for CacheMetrics {
    fn export_prometheus(&self, output: &mut String, prefix: &str) {
        // Metadata cache
        writeln!(output, "# HELP mynadb_{}_metadata_hit_rate Metadata cache hit rate", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_metadata_hit_rate gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_metadata_hit_rate {}", prefix, self.metadata_hit_rate).unwrap();

        writeln!(output, "# HELP mynadb_{}_metadata_hits_total Metadata cache hits", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_metadata_hits_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_metadata_hits_total {}", prefix, self.metadata_hits).unwrap();

        writeln!(output, "# HELP mynadb_{}_metadata_misses_total Metadata cache misses", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_metadata_misses_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_metadata_misses_total {}", prefix, self.metadata_misses).unwrap();

        writeln!(output, "# HELP mynadb_{}_metadata_size_bytes Metadata cache size in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_metadata_size_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_metadata_size_bytes {}", prefix, self.metadata_cache_size_bytes).unwrap();

        // Query cache
        writeln!(output, "# HELP mynadb_{}_query_cache_hit_rate Query cache hit rate", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_query_cache_hit_rate gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_query_cache_hit_rate {}", prefix, self.query_cache_hit_rate).unwrap();

        writeln!(output, "# HELP mynadb_{}_query_cache_hits_total Query cache hits", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_query_cache_hits_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_query_cache_hits_total {}", prefix, self.query_cache_hits).unwrap();

        writeln!(output, "# HELP mynadb_{}_query_cache_misses_total Query cache misses", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_query_cache_misses_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_query_cache_misses_total {}", prefix, self.query_cache_misses).unwrap();

        writeln!(output, "# HELP mynadb_{}_query_cache_size_bytes Query cache size in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_query_cache_size_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_query_cache_size_bytes {}", prefix, self.query_cache_size_bytes).unwrap();
    }
}

impl PrometheusExport for StorageMetrics {
    fn export_prometheus(&self, output: &mut String, prefix: &str) {
        // WAL
        writeln!(output, "# HELP mynadb_{}_wal_size_bytes WAL size in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_wal_size_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_wal_size_bytes {}", prefix, self.wal_size_bytes).unwrap();

        writeln!(output, "# HELP mynadb_{}_wal_flush_rate_per_second WAL flush rate per second", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_wal_flush_rate_per_second gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_wal_flush_rate_per_second {}", prefix, self.wal_flush_rate_per_sec).unwrap();

        writeln!(output, "# HELP mynadb_{}_wal_flushes_total Total WAL flushes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_wal_flushes_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_wal_flushes_total {}", prefix, self.wal_flushes).unwrap();

        // Parquet files by level
        writeln!(output, "# HELP mynadb_{}_parquet_files Parquet files by level", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_parquet_files gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_parquet_files{{level=\"L0\"}} {}", prefix, self.parquet_files_l0).unwrap();
        writeln!(output, "mynadb_{}_parquet_files{{level=\"L1\"}} {}", prefix, self.parquet_files_l1).unwrap();
        writeln!(output, "mynadb_{}_parquet_files{{level=\"L2\"}} {}", prefix, self.parquet_files_l2).unwrap();
        writeln!(output, "mynadb_{}_parquet_files{{level=\"L3\"}} {}", prefix, self.parquet_files_l3).unwrap();
        writeln!(output, "mynadb_{}_parquet_files{{level=\"L4\"}} {}", prefix, self.parquet_files_l4).unwrap();
        writeln!(output, "mynadb_{}_parquet_files{{level=\"L5\"}} {}", prefix, self.parquet_files_l5).unwrap();

        writeln!(output, "# HELP mynadb_{}_parquet_total_size_bytes Total size of parquet files", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_parquet_total_size_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_parquet_total_size_bytes {}", prefix, self.parquet_total_size_bytes).unwrap();

        // Compactions
        writeln!(output, "# HELP mynadb_{}_compactions_running Currently running compactions", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_compactions_running gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_compactions_running {}", prefix, self.compactions_running).unwrap();

        writeln!(output, "# HELP mynadb_{}_compactions_total Total compactions", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_compactions_total counter", prefix).unwrap();
        writeln!(output, "mynadb_{}_compactions_total {}", prefix, self.compactions_total).unwrap();
    }
}

impl PrometheusExport for MemoryMetrics {
    fn export_prometheus(&self, output: &mut String, prefix: &str) {
        writeln!(output, "# HELP mynadb_{}_rss_bytes Resident set size in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_rss_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_rss_bytes {}", prefix, self.rss_bytes).unwrap();

        writeln!(output, "# HELP mynadb_{}_cache_bytes Cache memory in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_cache_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_cache_bytes {}", prefix, self.cache_bytes).unwrap();

        writeln!(output, "# HELP mynadb_{}_wal_bytes WAL memory in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_wal_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_wal_bytes {}", prefix, self.wal_bytes).unwrap();

        writeln!(output, "# HELP mynadb_{}_query_engine_bytes Query engine memory in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_query_engine_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_query_engine_bytes {}", prefix, self.query_engine_bytes).unwrap();

        writeln!(output, "# HELP mynadb_{}_total_allocated_bytes Total allocated memory in bytes", prefix).unwrap();
        writeln!(output, "# TYPE mynadb_{}_total_allocated_bytes gauge", prefix).unwrap();
        writeln!(output, "mynadb_{}_total_allocated_bytes {}", prefix, self.total_allocated_bytes).unwrap();
    }
}

fn export_percentiles(output: &mut String, metric_name: &str, stats: &PercentileStats) {
    writeln!(output, "# HELP {} Latency in milliseconds", metric_name).unwrap();
    writeln!(output, "# TYPE {} summary", metric_name).unwrap();
    writeln!(output, "{}{{quantile=\"0.5\"}} {}", metric_name, stats.p50).unwrap();
    writeln!(output, "{}{{quantile=\"0.95\"}} {}", metric_name, stats.p95).unwrap();
    writeln!(output, "{}{{quantile=\"0.99\"}} {}", metric_name, stats.p99).unwrap();
    writeln!(output, "{}_sum {}", metric_name, stats.mean * stats.count as f64).unwrap();
    writeln!(output, "{}_count {}", metric_name, stats.count).unwrap();
}
