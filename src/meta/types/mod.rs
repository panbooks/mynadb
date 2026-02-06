//! Core types for the Meta service

use std::time::{SystemTime, UNIX_EPOCH};

mod command;
mod distribution;
mod event;
mod lock;
mod node;
mod scale_in;
mod table;

// Re-export all types
pub use command::{MetaCommand, MetaResponse};
pub use distribution::{DistributionConfig, DistributionScope};
pub use event::{Event, EventType};
pub use lock::{LeaderInfo, LeaseInfo, LockInfo};
pub use node::{node_labels, DataNodeMode, NodeInfo, NodeRole, NodeState};
pub use scale_in::{RetireStatus, ScaleInRecommendation, ScaleInRequest};
pub use table::{TableMeta, TableOwnerEntry, TableStatsDelta, DEFAULT_OWNER_TTL_MS};

/// Key prefixes for sled storage
pub mod keys {
    pub const PREFIX_NODE: &str = "/nodes/";
    pub const PREFIX_SCHEMA: &str = "/schemas/";
    pub const PREFIX_TABLE: &str = "/tables/";
    pub const PREFIX_LOCK: &str = "/locks/";
    pub const PREFIX_LEADER: &str = "/leaders/";
    pub const PREFIX_EVENT: &str = "/events/";
    pub const PREFIX_LEASE: &str = "/leases/";
    pub const PREFIX_DISTRIBUTION: &str = "/distribution/";
    pub const META_SEQUENCE: &str = "/meta/sequence";
    pub const META_FENCING: &str = "/meta/fencing";
}

/// Get current timestamp in milliseconds
pub fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
