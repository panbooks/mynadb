//! Cluster API endpoints
//!
//! HTTP endpoints for cluster status, node management, and operations.

pub mod status;
pub mod management;

pub use status::{
    ClusterState,
    cluster_status_handler,
    cluster_nodes_handler,
    cluster_leader_handler,
    cluster_health_handler,
};

pub use management::{
    drain_node_handler,
    remove_node_handler,
};
