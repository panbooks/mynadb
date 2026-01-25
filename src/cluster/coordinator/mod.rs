//! Coordinator module for distributed coordination
//!
//! Provides distributed locks, leader election, and pub/sub functionality
//! using etcd or Redis as the coordination backend.

mod interface;
mod etcd;
mod locks;
mod noop;

pub use interface::{
    Coordinator, CoordinatorType, WatchEvent, WatchEventType, Subscription, LockGuard,
};
pub use etcd::EtcdCoordinator;
pub use locks::{DistributedLock, LockOptions, partition_lock_key, compaction_lock_key, metadata_lock_key};
pub use noop::NoopCoordinator;

use super::config::CoordinatorConfig;
use std::sync::Arc;

/// Create a coordinator based on configuration
pub async fn create_coordinator(config: &CoordinatorConfig) -> Result<Arc<dyn Coordinator>, String> {
    match config.coordinator_type.as_str() {
        "etcd" => {
            let coordinator = EtcdCoordinator::new(config).await?;
            Ok(Arc::new(coordinator))
        }
        "none" | "" => {
            Ok(Arc::new(NoopCoordinator::new()))
        }
        other => {
            Err(format!("Unsupported coordinator type: {}", other))
        }
    }
}
