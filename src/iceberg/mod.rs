//! Apache Iceberg Table Format Integration
//!
//! Exposes MynaDB data as Iceberg v2 tables, enabling Spark, Trino, DuckDB,
//! and Flink to query MynaDB data directly via the Iceberg REST Catalog API.
//!
//! ## Architecture
//!
//! ```text
//! MynaDB Parquet files
//!         |
//!         v
//! Iceberg Metadata Sync Job (background)
//!         |
//!         v
//! metadata.json + manifest-list + manifest files
//! (generated under {data_dir}/iceberg/{catalog}/{schema}/{table}/metadata/)
//!         |
//!         v
//! REST Catalog API (/iceberg/v1/*)  <--- Spark / Trino / DuckDB
//! ```

pub mod catalog;
pub mod metadata;
pub mod sync;
