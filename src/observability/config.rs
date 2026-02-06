use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable OpenTelemetry distributed tracing
    #[serde(default)]
    pub enabled: bool,

    /// OTLP endpoint (e.g., "http://localhost:4317")
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// Service name for tracing
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Slow query threshold in milliseconds
    #[serde(default = "default_slow_query_threshold_ms")]
    pub slow_query_threshold_ms: u64,

    /// Enable slow query logging
    #[serde(default = "default_true")]
    pub slow_query_logging: bool,

    /// Enable query timeline tracking
    #[serde(default = "default_true")]
    pub timeline_enabled: bool,

    /// Maximum number of query timelines to keep in memory
    #[serde(default = "default_max_timelines")]
    pub max_timelines: usize,

    /// Sample rate for tracing (0.0 to 1.0)
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,
}

fn default_service_name() -> String {
    "mynadb".to_string()
}

fn default_slow_query_threshold_ms() -> u64 {
    1000 // 1 second
}

fn default_true() -> bool {
    true
}

fn default_max_timelines() -> usize {
    1000
}

fn default_sample_rate() -> f64 {
    1.0 // Sample all queries by default
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: None,
            service_name: default_service_name(),
            slow_query_threshold_ms: default_slow_query_threshold_ms(),
            slow_query_logging: true,
            timeline_enabled: true,
            max_timelines: default_max_timelines(),
            sample_rate: default_sample_rate(),
        }
    }
}
