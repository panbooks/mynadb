//! MynaDB Library
//!
//! This module exposes internal components for testing (fuzz, property, integration tests).
//! The main binary uses these modules via `mod` declarations in main.rs.

// Internal modules
pub mod admin;
pub mod aggregates;
pub mod config;
pub mod data;
pub mod jobs;
pub mod meta;
pub mod node;
pub mod query;
pub mod router;
pub mod services;
pub mod storage;
pub mod utils;

// Re-export at crate root for internal `crate::` imports to work
pub use storage::cache;
pub use storage::file_cache;
pub use storage::line_protocol;
pub use storage::parquet_writer;
pub use storage::storage as storage_mod;
pub use storage::wal;

pub use query::catalog;
pub use query::query_engine;
pub use query::timeseries;

// Re-export jobs modules
pub use jobs::compactor;
pub use jobs::database_manager;
pub use jobs::profiling;
