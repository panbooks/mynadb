//! Automated Downsampling
//!
//! Policy-based downsampling that automatically aggregates old high-resolution
//! data into lower-resolution tiers (e.g., 1s → 1m → 1h → 1d), reducing
//! storage costs 10-100x while preserving query capability.
//!
//! # Architecture
//!
//! - **Policies** define per-table downsampling rules with ordered tiers
//! - **Background job** periodically discovers eligible partitions and executes aggregation
//! - **Query rewriter** transparently redirects queries to tier tables when appropriate
//! - **Storage** persists policies and status as JSON files in `{data_dir}/_downsampling/`

pub mod job;
pub mod query_builder;
pub mod storage;
pub mod types;

pub use job::{CycleStats, DownsamplingJob};
pub use storage::DownsamplingStorage;
pub use types::{
    AggregateFn, AggregateSpec, DownsamplingPolicy, DownsamplingTier, PolicyStatus, TierStatus,
};
