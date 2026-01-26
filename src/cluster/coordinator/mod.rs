//! Coordinator module for distributed coordination
//!
//! Provides distributed locks, leader election, and pub/sub functionality
//! using etcd or Redis as the coordination backend.

mod etcd;
mod interface;
mod locks;
mod noop;

pub use etcd::EtcdCoordinator;
pub use interface::{
    Coordinator, CoordinatorType, LockGuard, Subscription, WatchEvent, WatchEventType,
};
pub use locks::{
    compaction_lock_key, metadata_lock_key, partition_lock_key, DistributedLock, LockOptions,
};
pub use noop::NoopCoordinator;

use super::config::CoordinatorConfig;
use std::sync::Arc;

/// Create a coordinator based on configuration
pub async fn create_coordinator(
    config: &CoordinatorConfig,
) -> Result<Arc<dyn Coordinator>, String> {
    match config.coordinator_type.as_str() {
        "etcd" => {
            let coordinator = EtcdCoordinator::new(config).await?;
            Ok(Arc::new(coordinator))
        }
        "none" | "" => Ok(Arc::new(NoopCoordinator::new())),
        other => Err(format!("Unsupported coordinator type: {}", other)),
    }
}
