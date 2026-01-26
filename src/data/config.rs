//! Data Node configuration

use serde::{Deserialize, Serialize};

/// Data Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataNodeConfig {
    /// Unique node identifier
    pub node_id: String,

    /// Advertised address for Router nodes to connect
    pub advertise_addr: String,

    /// gRPC port for internal communication
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,

    /// Meta service endpoints
    #[serde(default)]
    pub meta_endpoints: Vec<String>,

    /// Meta client connection timeout
    #[serde(default = "default_meta_connect_timeout_ms")]
    pub meta_connect_timeout_ms: u64,

    /// Meta client request timeout
    #[serde(default = "default_meta_request_timeout_ms")]
    pub meta_request_timeout_ms: u64,
}

fn default_grpc_port() -> u16 {
    7200
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
            node_id: format!("data-{}", uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("unknown")),
            advertise_addr: "127.0.0.1:7200".to_string(),
            grpc_port: default_grpc_port(),
            meta_endpoints: vec!["127.0.0.1:7100".to_string()],
            meta_connect_timeout_ms: default_meta_connect_timeout_ms(),
            meta_request_timeout_ms: default_meta_request_timeout_ms(),
        }
    }
}
