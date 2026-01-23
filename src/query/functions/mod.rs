//! Custom User-Defined Functions (UDFs) and Aggregates (UDAFs)
//!
//! This module provides custom functions for the query engine:
//! - `time_bucket` - Time bucketing for GROUP BY time() queries (UDF)
//! - `first_value` - Returns value with minimum timestamp (UDAF)
//! - `last_value` - Returns value with maximum timestamp (UDAF)

pub mod first_last;
pub mod time_bucket;

pub use first_last::{create_first_value_udaf, create_last_value_udaf};
pub use time_bucket::create_time_bucket_udf;
