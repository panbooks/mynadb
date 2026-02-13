//! Router Node module for MynaDB cluster mode
//!
//! The Router node is responsible for:
//! - Handling client protocols (HTTP, Arrow Flight, MySQL wire)
//! - Routing writes to appropriate Data nodes based on partition affinity
//! - Coordinating distributed queries across Data nodes
//! - Caching partition→node mappings from Meta service

pub mod api;
pub mod cache_invalidation;
pub mod client;
pub mod fan_out;
pub mod config;
pub mod flight;
pub mod health_checker;
pub mod query_coordinator;
pub mod shard_cache;
pub mod state;
pub mod write_router;

// Re-exports for convenience
pub use cache_invalidation::CacheInvalidationHandler;
pub use query_coordinator::{DistributedQueryPlan, QueryCoordinator, QueryResult, TableRef, TimeRange};
pub use shard_cache::ShardCache;
pub use write_router::WriteRouter;
