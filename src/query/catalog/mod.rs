//! Catalog Management Module
//!
//! Provides centralized metadata management for databases, schemas, tables,
//! partitions, and files. Eliminates the need for directory scanning during queries.
//!
//! ## Architecture
//!
//! ```text
//! MetadataManager (trait)
//!     │
//!     ├── MemoryMetadataManager (in-memory with JSON persistence)
//!     │
//!     └── SqliteMetadataManager (future: SQLite-based)
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! let manager = MemoryMetadataManager::new("/data/_metadata");
//! manager.load().await?;
//!
//! // Register a new file
//! manager.register_file(FileInfo { ... }).await?;
//!
//! // Query metadata
//! let tables = manager.list_tables("mydb", "default").await?;
//! let files = manager.list_files("mydb", "default", "cpu", "2024-01-15").await?;
//! ```

pub mod manager;
pub mod memory_manager;
pub mod types;

pub use manager::MetadataManager;
pub use memory_manager::MemoryMetadataManager;
pub use types::*;
