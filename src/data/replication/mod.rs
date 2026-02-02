//! Warm Standby Replication Module
//!
//! This module implements warm standby replication for zero-downtime failover.
//!
//! Enable via `[data.replication] enabled = true` in config.
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
//! ## Failover Flow (No-Promotion Architecture)
//!
//! 1. Primary crashes
//! 2. Meta detects failure (heartbeat timeout)
//! 3. Meta sends FlushForRecovery to Standby
//! 4. Standby flushes cached data to S3 (preserving unflushed writes)
//! 5. K8s replaces the Primary pod
//! 6. New Primary starts, loads state from S3
//!
//! Note: Standby NEVER promotes to Primary - this eliminates split-brain risk
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

pub use receiver::StandbyReceiver;
pub use stream::ReplicationManager;
pub use types::{ReplicateRequest, ReplicateResponse, ReplicationConfig, ReplicationMode};
