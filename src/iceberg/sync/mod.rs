//! Iceberg Metadata Sync
//!
//! Background job that scans MynaDB Parquet files and generates
//! Iceberg metadata (snapshots, manifests, table metadata).

pub mod metadata_sync;
pub mod snapshot_builder;
