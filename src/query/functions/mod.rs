//! Custom User-Defined Functions (UDFs)
//!
//! This module provides custom functions for the query engine:
//! - `time_bucket` - Time bucketing for GROUP BY time() queries

pub mod time_bucket;

pub use time_bucket::create_time_bucket_udf;
