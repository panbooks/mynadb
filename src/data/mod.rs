//! Data Node module for MynaDB cluster mode
//!
//! The Data node is responsible for:
//! - Handling write requests from Router nodes
//! - Managing local WAL and DataCache
//! - Flushing data to object storage
//! - Executing queries locally with DataFusion
//! - Registering files with Meta service
//! - Warm standby replication for zero-downtime failover

pub mod config;
pub mod replication;
pub mod server;

pub use config::{DataNodeConfig, ReplicationConfig as DataReplicationConfig};
pub use replication::{
    ReplicateRequest, ReplicateResponse, ReplicationConfig, ReplicationManager, ReplicationMode,
    StandbyReceiver,
};

// Re-export existing storage components
pub use crate::storage::cache::DataCache;
pub use crate::storage::parquet_writer::ParquetWriter;
pub use crate::storage::wal::Wal;
