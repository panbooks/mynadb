//! Prometheus metrics for cluster monitoring
//!
//! Exports metrics about cluster health, coordination, and performance.

use parking_lot::RwLock;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::Arc;

/// Cluster metrics collection
pub struct ClusterMetrics {
    /// Registry for all metrics
    registry: Registry,

    // Node metrics
    /// Total number of nodes in the cluster
    pub nodes_total: IntGauge,
    /// Number of healthy nodes
    pub nodes_healthy: IntGauge,
    /// Whether this node is the leader
    pub is_leader: IntGauge,

    // Election metrics
    /// Total leader elections
    pub leader_elections_total: IntCounter,
    /// Time spent as leader (seconds)
    pub leader_time_seconds: Counter,

    // Lock metrics
    /// Lock acquisitions by result (success/failure/timeout)
    pub lock_acquisitions_total: IntCounterVec,
    /// Lock wait time histogram
    pub lock_wait_seconds: HistogramVec,
    /// Currently held locks
    pub locks_held: IntGauge,

    // Cache invalidation metrics
    /// Cache invalidations sent
    pub cache_invalidations_sent_total: IntCounter,
    /// Cache invalidations received
    pub cache_invalidations_received_total: IntCounter,
    /// Cache invalidations by reason
    pub cache_invalidations_by_reason: IntCounterVec,

    // Coordinator metrics
    /// Coordinator connection status
    pub coordinator_connected: IntGauge,
    /// Coordinator operation latency
    pub coordinator_operation_seconds: HistogramVec,
    /// Coordinator errors
    pub coordinator_errors_total: IntCounterVec,

    // Write coordination metrics
    /// Write operations coordinated
    pub writes_coordinated_total: IntCounter,
    /// Write lock contention (how often we had to wait)
    pub write_lock_contention_total: IntCounter,

    // Health check metrics
    /// Health check results
    pub health_checks_total: IntCounterVec,
    /// Last health check timestamp
    pub last_health_check_timestamp: IntGauge,
}

impl ClusterMetrics {
    /// Create a new cluster metrics collection
    pub fn new() -> Self {
        let registry = Registry::new();

        // Node metrics
        let nodes_total = IntGauge::new(
            "mynadb_cluster_nodes_total",
            "Total number of nodes registered in the cluster",
        )
        .unwrap();

        let nodes_healthy = IntGauge::new(
            "mynadb_cluster_nodes_healthy",
            "Number of healthy nodes in the cluster",
        )
        .unwrap();

        let is_leader = IntGauge::new(
            "mynadb_cluster_is_leader",
            "Whether this node is the compaction leader (1 = leader, 0 = follower)",
        )
        .unwrap();

        // Election metrics
        let leader_elections_total = IntCounter::new(
            "mynadb_cluster_leader_elections_total",
            "Total number of leader elections this node has won",
        )
        .unwrap();

        let leader_time_seconds = Counter::new(
            "mynadb_cluster_leader_time_seconds",
            "Total time this node has been the leader (seconds)",
        )
        .unwrap();

        // Lock metrics
        let lock_acquisitions_total = IntCounterVec::new(
            Opts::new(
                "mynadb_cluster_lock_acquisitions_total",
                "Total lock acquisition attempts by result",
            ),
            &["result"], // success, failure, timeout
        )
        .unwrap();

        let lock_wait_seconds = HistogramVec::new(
            HistogramOpts::new(
                "mynadb_cluster_lock_wait_seconds",
                "Time spent waiting for locks",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["lock_type"], // write, compaction, metadata
        )
        .unwrap();

        let locks_held = IntGauge::new(
            "mynadb_cluster_locks_held",
            "Number of distributed locks currently held by this node",
        )
        .unwrap();

        // Cache invalidation metrics
        let cache_invalidations_sent_total = IntCounter::new(
            "mynadb_cluster_cache_invalidations_sent_total",
            "Total cache invalidation messages sent",
        )
        .unwrap();

        let cache_invalidations_received_total = IntCounter::new(
            "mynadb_cluster_cache_invalidations_received_total",
            "Total cache invalidation messages received",
        )
        .unwrap();

        let cache_invalidations_by_reason = IntCounterVec::new(
            Opts::new(
                "mynadb_cluster_cache_invalidations_by_reason_total",
                "Cache invalidations by reason",
            ),
            &["reason"], // new_file, file_deleted, compacted, etc.
        )
        .unwrap();

        // Coordinator metrics
        let coordinator_connected = IntGauge::new(
            "mynadb_cluster_coordinator_connected",
            "Whether the coordinator (etcd) is connected (1 = connected, 0 = disconnected)",
        )
        .unwrap();

        let coordinator_operation_seconds = HistogramVec::new(
            HistogramOpts::new(
                "mynadb_cluster_coordinator_operation_seconds",
                "Coordinator operation latency",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
            &["operation"], // get, put, delete, lock, unlock, watch
        )
        .unwrap();

        let coordinator_errors_total = IntCounterVec::new(
            Opts::new(
                "mynadb_cluster_coordinator_errors_total",
                "Total coordinator errors by operation",
            ),
            &["operation"],
        )
        .unwrap();

        // Write coordination metrics
        let writes_coordinated_total = IntCounter::new(
            "mynadb_cluster_writes_coordinated_total",
            "Total write operations coordinated with locks",
        )
        .unwrap();

        let write_lock_contention_total = IntCounter::new(
            "mynadb_cluster_write_lock_contention_total",
            "Number of times a write had to wait for a lock",
        )
        .unwrap();

        // Health check metrics
        let health_checks_total = IntCounterVec::new(
            Opts::new(
                "mynadb_cluster_health_checks_total",
                "Total health checks by result",
            ),
            &["status"], // healthy, degraded, unhealthy
        )
        .unwrap();

        let last_health_check_timestamp = IntGauge::new(
            "mynadb_cluster_last_health_check_timestamp",
            "Unix timestamp of the last health check",
        )
        .unwrap();

        // Register all metrics
        let metrics = Self {
            registry,
            nodes_total,
            nodes_healthy,
            is_leader,
            leader_elections_total,
            leader_time_seconds,
            lock_acquisitions_total,
            lock_wait_seconds,
            locks_held,
            cache_invalidations_sent_total,
            cache_invalidations_received_total,
            cache_invalidations_by_reason,
            coordinator_connected,
            coordinator_operation_seconds,
            coordinator_errors_total,
            writes_coordinated_total,
            write_lock_contention_total,
            health_checks_total,
            last_health_check_timestamp,
        };

        metrics.register_all();
        metrics
    }

    /// Register all metrics with the registry
    fn register_all(&self) {
        // Ignore errors if already registered
        let _ = self.registry.register(Box::new(self.nodes_total.clone()));
        let _ = self.registry.register(Box::new(self.nodes_healthy.clone()));
        let _ = self.registry.register(Box::new(self.is_leader.clone()));
        let _ = self
            .registry
            .register(Box::new(self.leader_elections_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.leader_time_seconds.clone()));
        let _ = self
            .registry
            .register(Box::new(self.lock_acquisitions_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.lock_wait_seconds.clone()));
        let _ = self.registry.register(Box::new(self.locks_held.clone()));
        let _ = self
            .registry
            .register(Box::new(self.cache_invalidations_sent_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.cache_invalidations_received_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.cache_invalidations_by_reason.clone()));
        let _ = self
            .registry
            .register(Box::new(self.coordinator_connected.clone()));
        let _ = self
            .registry
            .register(Box::new(self.coordinator_operation_seconds.clone()));
        let _ = self
            .registry
            .register(Box::new(self.coordinator_errors_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.writes_coordinated_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.write_lock_contention_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.health_checks_total.clone()));
        let _ = self
            .registry
            .register(Box::new(self.last_health_check_timestamp.clone()));
    }

    /// Get the registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Gather all metrics as text for Prometheus scraping
    pub fn gather(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Record a lock acquisition attempt
    pub fn record_lock_acquisition(&self, success: bool, wait_time_secs: f64, lock_type: &str) {
        let result = if success { "success" } else { "failure" };
        self.lock_acquisitions_total
            .with_label_values(&[result])
            .inc();
        self.lock_wait_seconds
            .with_label_values(&[lock_type])
            .observe(wait_time_secs);
    }

    /// Record a cache invalidation
    pub fn record_cache_invalidation(&self, reason: &str, sent: bool) {
        if sent {
            self.cache_invalidations_sent_total.inc();
        } else {
            self.cache_invalidations_received_total.inc();
        }
        self.cache_invalidations_by_reason
            .with_label_values(&[reason])
            .inc();
    }

    /// Record a coordinator operation
    pub fn record_coordinator_operation(&self, operation: &str, duration_secs: f64, success: bool) {
        self.coordinator_operation_seconds
            .with_label_values(&[operation])
            .observe(duration_secs);
        if !success {
            self.coordinator_errors_total
                .with_label_values(&[operation])
                .inc();
        }
    }

    /// Record a health check
    pub fn record_health_check(&self, status: &str) {
        self.health_checks_total.with_label_values(&[status]).inc();
        self.last_health_check_timestamp
            .set(chrono::Utc::now().timestamp());
    }

    /// Update leader status
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.set(if is_leader { 1 } else { 0 });
        if is_leader {
            self.leader_elections_total.inc();
        }
    }

    /// Update node counts
    pub fn set_node_counts(&self, total: i64, healthy: i64) {
        self.nodes_total.set(total);
        self.nodes_healthy.set(healthy);
    }

    /// Update coordinator connection status
    pub fn set_coordinator_connected(&self, connected: bool) {
        self.coordinator_connected
            .set(if connected { 1 } else { 0 });
    }
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global cluster metrics instance
static CLUSTER_METRICS: std::sync::OnceLock<ClusterMetrics> = std::sync::OnceLock::new();

/// Get the global cluster metrics instance
pub fn cluster_metrics() -> &'static ClusterMetrics {
    CLUSTER_METRICS.get_or_init(ClusterMetrics::new)
}

/// Initialize cluster metrics with a custom instance
pub fn init_cluster_metrics(metrics: ClusterMetrics) {
    let _ = CLUSTER_METRICS.set(metrics);
}
