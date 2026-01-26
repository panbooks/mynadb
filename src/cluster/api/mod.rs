//! Cluster API endpoints
//!
//! HTTP endpoints for cluster status, node management, and operations.

pub mod management;
pub mod status;

pub use status::{
    cluster_health_handler, cluster_leader_handler, cluster_nodes_handler, cluster_status_handler,
    ClusterState,
};

pub use management::{drain_node_handler, remove_node_handler};
