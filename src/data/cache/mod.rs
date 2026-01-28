//! Local disk cache for Parquet files
//!
//! This module provides a local disk cache for Parquet files from object storage.
//! Each Data node caches files it owns (path starts with node_id) for fast repeated access.
//!
//! **Note**: This is a planned feature for cluster mode that is not yet integrated
//! into the query pipeline. Uses node_id affinity for optimal cache locality.

#![allow(dead_code)]

mod parquet_cache;

pub use parquet_cache::ParquetLocalCache;
