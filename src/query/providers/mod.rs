//! TableProvider implementations for the query engine
//!
//! This module contains various TableProvider implementations:
//! - `ParquetTableProvider` - Reads from Parquet files on disk
//! - `MemtableProvider` - Reads from in-memory cached data
//! - `UnionTableProvider` - Combines multiple providers

pub mod memtable_provider;
pub mod table_provider;
pub mod union_provider;

pub use memtable_provider::MemtableProvider;
pub use table_provider::{ParquetTableConfig, ParquetTableProvider};
pub use union_provider::UnionTableProvider;
