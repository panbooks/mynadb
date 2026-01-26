//! Cluster module for MynaDB Enterprise Cluster Edition
//!
//! This module provides distributed coordination capabilities for running
//! MynaDB in a clustered configuration with shared storage (S3/MinIO/OSS).
//!
//! # Architecture
//!
//! The cluster uses a shared-storage architecture where:
//! - All nodes share the same object storage (S3/MinIO/OSS)
//! - Coordination is handled via etcd (or Redis)
//! - Nodes are stateless and can be added/removed freely
//!
//! # Components
//!
//! - `config`: Cluster configuration structs
//! - `coordinator`: Distributed coordination (locks, pub/sub)
//! - `node`: Node identity and registration
//! - `cache_invalidation`: Cross-node cache invalidation
//! - `election`: Leader election for background tasks
//! - `metrics`: Prometheus metrics for cluster monitoring
//! - `api`: Cluster management API endpoints

pub mod api;
pub mod cache_invalidation;
pub mod config;
pub mod coordinator;
pub mod election;
pub mod metrics;
pub mod node;
pub mod write_coordinator;

pub use cache_invalidation::{CacheInvalidation, InvalidationReason};
pub use config::ClusterConfig;
pub use coordinator::{Coordinator, CoordinatorType};
pub use election::LeaderElection;
pub use metrics::ClusterMetrics;
pub use node::{NodeInfo, NodeRole, NodeState};
pub use write_coordinator::WriteCoordinator;
