//! Data Node configuration

use serde::{Deserialize, Serialize};

/// Data Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataNodeConfig {
    /// Unique node identifier
    ///
    /// For Kubernetes deployments, this should be derived from the pod name
    /// (e.g., via MYNADB_NODE_ID or HOSTNAME environment variable).
    /// StatefulSet pods have stable names like "mynadb-data-0", "mynadb-data-1".
    pub node_id: String,

    /// Advertised address for Router nodes to connect
    ///
    /// For Kubernetes with headless services, use DNS pattern:
    /// `${HOSTNAME}.mynadb-data-headless.${NAMESPACE}.svc.cluster.local:7200`
    pub advertise_addr: String,

    /// gRPC port for internal communication
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,

    /// HTTP port for health checks and monitoring (0 = disabled)
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// Meta service endpoints
    ///
    /// For Kubernetes, use headless service DNS:
    /// `mynadb-meta-0.mynadb-meta-headless:7100,mynadb-meta-1.mynadb-meta-headless:7100,...`
    #[serde(default)]
    pub meta_endpoints: Vec<String>,

    /// Meta client connection timeout
    #[serde(default = "default_meta_connect_timeout_ms")]
    pub meta_connect_timeout_ms: u64,

    /// Meta client request timeout
    #[serde(default = "default_meta_request_timeout_ms")]
    pub meta_request_timeout_ms: u64,

    /// Replication configuration for warm standby
    #[serde(default)]
    pub replication: ReplicationConfig,

    /// Node labels for placement decisions
    ///
    /// Common labels for Kubernetes:
    /// - `topology.kubernetes.io/zone` - Availability zone
    /// - `topology.kubernetes.io/region` - Region
    /// - `mynadb.io/rack` - Rack identifier
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

/// Replication configuration for warm standby
///
/// When enabled, the leader node streams writes to standby nodes for
/// zero-downtime failover. Standbys maintain an in-memory copy but
/// don't flush to Parquet (only the leader does).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Whether replication is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Replication mode: "async" or "sync"
    /// - async: ACK immediately after local write (default)
    /// - sync: Wait for at least one standby ACK
    #[serde(default = "default_replication_mode")]
    pub mode: String,

    /// How often to batch replication entries (ms)
    #[serde(default = "default_batch_interval_ms")]
    pub batch_interval_ms: u64,

    /// Maximum entries per batch
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    /// Timeout for standby ACK (ms)
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,

    /// Alert if standby replication lag exceeds this (ms)
    #[serde(default = "default_max_lag_alert_ms")]
    pub max_lag_alert_ms: u64,
}

fn default_replication_mode() -> String {
    "async".to_string()
}

fn default_batch_interval_ms() -> u64 {
    100
}

fn default_max_batch_size() -> usize {
    1000
}

fn default_ack_timeout_ms() -> u64 {
    5000
}

fn default_max_lag_alert_ms() -> u64 {
    10000
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: default_replication_mode(),
            batch_interval_ms: default_batch_interval_ms(),
            max_batch_size: default_max_batch_size(),
            ack_timeout_ms: default_ack_timeout_ms(),
            max_lag_alert_ms: default_max_lag_alert_ms(),
        }
    }
}

fn default_grpc_port() -> u16 {
    7200
}

fn default_http_port() -> u16 {
    7201
}

fn default_meta_connect_timeout_ms() -> u64 {
    5000
}

fn default_meta_request_timeout_ms() -> u64 {
    30000
}

impl Default for DataNodeConfig {
    fn default() -> Self {
        Self {
            node_id: Self::derive_stable_node_id(),
            advertise_addr: "127.0.0.1:7200".to_string(),
            grpc_port: default_grpc_port(),
            http_port: default_http_port(),
            meta_endpoints: Self::derive_meta_endpoints(),
            meta_connect_timeout_ms: default_meta_connect_timeout_ms(),
            meta_request_timeout_ms: default_meta_request_timeout_ms(),
            replication: ReplicationConfig::default(),
            labels: Self::derive_node_labels(),
        }
    }
}

impl DataNodeConfig {
    /// Derive stable node ID from environment
    ///
    /// Priority: MYNADB_NODE_ID > HOSTNAME > generated UUID
    ///
    /// In Kubernetes StatefulSet, HOSTNAME is the pod name (e.g., mynadb-data-0).
    pub fn derive_stable_node_id() -> String {
        crate::config::get_stable_node_id("data")
    }

    /// Derive meta endpoints from environment
    ///
    /// Checks MYNADB_META_ENDPOINTS for comma-separated list.
    /// Falls back to localhost for development.
    pub fn derive_meta_endpoints() -> Vec<String> {
        if let Ok(endpoints) = std::env::var("MYNADB_META_ENDPOINTS") {
            endpoints
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec!["127.0.0.1:7100".to_string()]
        }
    }

    /// Derive node labels from environment
    ///
    /// Checks common K8s topology labels:
    /// - MYNADB_ZONE -> topology.kubernetes.io/zone
    /// - MYNADB_REGION -> topology.kubernetes.io/region
    /// - MYNADB_RACK -> mynadb.io/rack
    pub fn derive_node_labels() -> std::collections::HashMap<String, String> {
        let mut labels = std::collections::HashMap::new();

        if let Ok(zone) = std::env::var("MYNADB_ZONE") {
            labels.insert("topology.kubernetes.io/zone".to_string(), zone);
        }
        if let Ok(region) = std::env::var("MYNADB_REGION") {
            labels.insert("topology.kubernetes.io/region".to_string(), region);
        }
        if let Ok(rack) = std::env::var("MYNADB_RACK") {
            labels.insert("mynadb.io/rack".to_string(), rack);
        }

        labels
    }

    /// Derive advertise address for Kubernetes
    ///
    /// Uses pattern: `{hostname}.{service}.{namespace}.svc.cluster.local:{port}`
    ///
    /// Environment variables:
    /// - MYNADB_ADVERTISE_ADDR - explicit override
    /// - HOSTNAME - pod name (from StatefulSet)
    /// - MYNADB_HEADLESS_SERVICE - headless service name (default: mynadb-data-headless)
    /// - MYNADB_NAMESPACE - namespace (default: from /var/run/secrets/.../namespace or "default")
    pub fn derive_k8s_advertise_addr(port: u16) -> String {
        // Explicit override takes precedence
        if let Ok(addr) = std::env::var("MYNADB_ADVERTISE_ADDR") {
            return addr;
        }

        // Try to construct K8s DNS name
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());

        // Check if we're in K8s (pod name typically contains hyphens and ordinals)
        if hostname.contains('-') && hostname.chars().last().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            let service = std::env::var("MYNADB_HEADLESS_SERVICE")
                .unwrap_or_else(|_| "mynadb-data-headless".to_string());
            let namespace = std::env::var("MYNADB_NAMESPACE")
                .or_else(|_| {
                    // Try to read namespace from K8s service account
                    std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                        .map(|s| s.trim().to_string())
                })
                .unwrap_or_else(|_| "default".to_string());

            format!("{}.{}.{}.svc.cluster.local:{}", hostname, service, namespace, port)
        } else {
            // Not in K8s, use localhost
            format!("127.0.0.1:{}", port)
        }
    }

    /// Get the zone label if present
    pub fn zone(&self) -> Option<&str> {
        self.labels.get("topology.kubernetes.io/zone").map(|s| s.as_str())
    }

    /// Get the region label if present
    pub fn region(&self) -> Option<&str> {
        self.labels.get("topology.kubernetes.io/region").map(|s| s.as_str())
    }

    /// Get the rack label if present
    pub fn rack(&self) -> Option<&str> {
        self.labels.get("mynadb.io/rack").map(|s| s.as_str())
    }

    /// Check if this node is in a different zone than another node's labels
    pub fn is_different_zone(&self, other_labels: &std::collections::HashMap<String, String>) -> bool {
        match (self.zone(), other_labels.get("topology.kubernetes.io/zone")) {
            (Some(z1), Some(z2)) => z1 != z2,
            _ => false, // If either doesn't have zone, we can't determine
        }
    }
}
