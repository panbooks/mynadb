//! Data Node module for MynaDB cluster mode
//!
//! The Data node is responsible for:
//! - Handling write requests from Router nodes
//! - Managing local WAL and DataCache
//! - Flushing data to object storage
//! - Executing queries locally with DataFusion
//! - Registering files with Meta service

pub mod config;
pub mod server;

pub use config::DataNodeConfig;

// Re-export existing storage components
pub use crate::storage::cache::DataCache;
pub use crate::storage::parquet_writer::ParquetWriter;
pub use crate::storage::wal::Wal;
