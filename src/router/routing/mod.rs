//! Routing module for the Router node
//!
//! Handles write dispatch and query coordination across Data nodes.

pub mod cache_invalidation;
pub mod shard_cache;
pub mod write_router;
pub mod query_coordinator;

pub use cache_invalidation::CacheInvalidationHandler;
pub use shard_cache::ShardCache;
pub use write_router::WriteRouter;
pub use query_coordinator::QueryCoordinator;
