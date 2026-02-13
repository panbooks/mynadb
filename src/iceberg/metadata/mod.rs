//! Iceberg v2 Metadata Generation
//!
//! Generates Iceberg-compatible metadata from MynaDB's Parquet files:
//! - TableMetadata (v2 JSON)
//! - Snapshots
//! - ManifestList (Avro)
//! - Manifest files (Avro)
//! - Schema conversion (Arrow <-> Iceberg)
//! - Partition specs

pub mod manifest;
pub mod manifest_list;
pub mod partition_spec;
pub mod schema;
pub mod snapshot;
pub mod table_metadata;
