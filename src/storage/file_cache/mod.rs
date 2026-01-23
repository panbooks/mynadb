//! Parquet File Cache Module
//!
//! Provides a tiered caching system for Parquet files to reduce Object Store latency.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    FileCache (trait)                         │
//! │                                                              │
//! │  L1: Memory Cache (hot)     ← Fast, limited size            │
//! │      │                                                       │
//! │      ▼ miss                                                  │
//! │  L2: Disk Cache (warm)      ← Larger, SSD-backed            │
//! │      │                                                       │
//! │      ▼ miss                                                  │
//! │  L3: Object Store (cold)    ← S3, MinIO, etc.               │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! let cache = DiskFileCache::new("/var/cache/mynadb", 100 * 1024 * 1024 * 1024);
//!
//! // Get file (from cache or fetch from source)
//! let data = cache.get("s3://bucket/path/file.parquet", &storage).await?;
//!
//! // Invalidate cache entry
//! cache.invalidate("s3://bucket/path/file.parquet").await?;
//! ```

pub mod cache;
pub mod disk_cache;
pub mod types;

pub use cache::FileCache;
pub use disk_cache::DiskFileCache;
pub use types::*;
