//! Local disk cache for Parquet files
//!
//! This module provides a local disk cache for Parquet files from object storage.
//! Each Data node caches files it owns (path starts with node_id) for fast repeated access.

mod parquet_cache;

pub use parquet_cache::ParquetLocalCache;
