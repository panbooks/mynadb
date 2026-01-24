//! MynaDB Library
//!
//! This module exposes internal components for testing (fuzz, property, integration tests).
//! The main binary uses these modules via `mod` declarations in main.rs.

// Internal modules
pub mod config;
pub mod storage;
pub mod query;

// Re-export at crate root for internal `crate::` imports to work
pub use storage::line_protocol;
pub use storage::parquet_writer;
pub use storage::wal;
pub use storage::cache;
pub use storage::storage as storage_mod;
pub use storage::file_cache;

pub use query::query_engine;
pub use query::catalog;
pub use query::timeseries;
