//! TableProvider implementations for the query engine
//!
//! This module contains various TableProvider implementations:
//! - `ParquetTableProvider` - Reads from Parquet files via ObjectStore
//! - `MemtableProvider` - Reads from in-memory cached data
//! - `UnionTableProvider` - Combines two providers (memtable + parquet)
//! - `MultiUnionTableProvider` - Combines N providers (multiple storage prefixes)

pub mod memtable_provider;
pub mod multi_union_provider;
pub mod table_provider;
pub mod union_provider;

pub use memtable_provider::MemtableProvider;
pub use multi_union_provider::MultiUnionTableProvider;
pub use table_provider::{ParquetExecPlan, ParquetFileInfo, ParquetTableConfig, ParquetTableProvider};
pub use union_provider::UnionTableProvider;
