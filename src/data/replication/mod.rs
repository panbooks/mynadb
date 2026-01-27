//! Warm Standby Replication Module
//!
//! This module implements warm standby replication for zero-downtime failover.
//!
//! ## Architecture
//!
//! ```text
//! Leader Node                         Standby Node
//! ┌───────────────┐                  ┌───────────────┐
//! │   Write Path  │                  │   StandbyRecv │
//! │       │       │                  │       │       │
//! │       ▼       │                  │       ▼       │
//! │   MemTable    │──────────────────▶   MemTable    │
//! │       │       │   ReplicationStream  (no flush)  │
//! │       ▼       │                  │               │
//! │   Parquet     │                  │               │
//! └───────────────┘                  └───────────────┘
//! ```
//!
//! ## Key Components
//!
//! - [`ReplicationStream`]: Leader sends data to standbys
//! - [`StandbyReceiver`]: Standby receives and applies data
//! - [`ReplicationManager`]: Manages multiple streams
//!
//! ## Failover Flow
//!
//! 1. Leader crashes
//! 2. Router detects failure (write timeout)
//! 3. Router marks leader as Suspect
//! 4. Router routes to standby (instant failover via ShardCache)
//! 5. Standby promotes to leader
//! 6. Standby starts flushing to Parquet
//!
//! ## Configuration
//!
//! ```toml
//! [cluster.replication]
//! enabled = true
//! mode = "async"           # "async" or "sync"
//! batch_interval_ms = 100  # How often to batch entries
//! max_batch_size = 1000    # Max entries per batch
//! ack_timeout_ms = 5000    # Timeout for standby ACK
//! max_lag_alert_ms = 10000 # Alert if lag exceeds this
//! ```

pub mod receiver;
pub mod stream;
pub mod types;

pub use receiver::{StandbyReceiver, StandbyStats};
pub use stream::{ReplicationManager, ReplicationStats, ReplicationStream};
pub use types::{
    ReplicateRequest, ReplicateResponse, ReplicationConfig, ReplicationEntry, ReplicationMode,
};
