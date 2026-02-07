//! Custom Query Execution Pipeline
//!
//! This module provides custom query optimization using DataFusion's native traits:
//! - `AnalyzerRule` - Semantic analysis (e.g., timestamp to date conversion)
//! - `OptimizerRule` - Logical optimization (e.g., partition filter pushdown)
//! - `PhysicalOptimizerRule` - Physical optimization (e.g., scan configuration)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::query::{CustomQueryExecutor, create_custom_session_state};
//!
//! // Option 1: Use CustomQueryExecutor
//! let ctx = SessionContext::new();
//! let executor = CustomQueryExecutor::new(ctx);
//! let results = executor.execute_sql("SELECT * FROM cpu").await?;
//!
//! // Option 2: Create custom SessionState for existing context
//! let ctx = SessionContext::new();
//! let custom_state = create_custom_session_state(ctx.state());
//! let custom_ctx = SessionContext::new_with_state(custom_state);
//! ```
//!
//! ## Pipeline Flow
//!
//! ```text
//! SQL String
//!     → Parse (SQL to LogicalPlan)
//!     → Analyze (custom: TimestampToDate + builtin rules)
//!     → Logical Optimize (custom: PartitionFilterPushdown + builtin rules)
//!     → Physical Plan (LogicalPlan to ExecutionPlan)
//!     → Physical Optimize (custom: ScanConfig + builtin rules)
//!     → Execute (stream results)
//! ```

pub mod analyzers;
pub mod cache;
pub mod catalog;
pub mod executor;
pub mod functions;
pub mod influxql;
pub mod logical_optimizers;
pub mod physical_optimizers;
pub mod providers;
pub mod query_engine;
pub mod sql_utils;
pub mod streaming;
pub mod timeseries;
pub mod types;

// Re-exports for backward compatibility
pub use executor::create_custom_session_state;
