//! Caching components for the query engine
//!
//! This module contains caching implementations for query results,
//! table metadata, hot partition data, and partition file listings.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Query Engine Caches                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  MetadataCache        - Table/schema metadata                    │
//! │  QueryCache           - Query results                            │
//! │  HotPartitionCache    - Recent partition data                    │
//! │  PartitionFilesCache  - S3 file listings (NEW)                  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Partition Files Cache
//!
//! The `PartitionFilesCache` is used in table-level routing architecture:
//! - Router sends queries to the Data node that owns the table
//! - Data node lists parquet files from S3 (cached for 30s)
//! - No file listings stored in Meta service (scalability)
//!
//! See `docs/architecture-meta-routing.md` for details.

pub mod hot_partition_cache;
pub mod metadata_cache;
pub mod partition_files_cache;
pub mod query_cache;

pub use partition_files_cache::{PartitionFilesCache, SharedPartitionFilesCache};
