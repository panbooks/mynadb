//! Time series processing utilities
//!
//! This module contains utilities for time series queries:
//! - `TimeInterval` - Duration parsing for InfluxDB-style intervals
//! - `FillOption` - Gap-filling strategies for GROUP BY time()
//! - `GroupByTimeTransformer` - SQL transformer for GROUP BY time() syntax
//! - `FillProcessor` - Post-processing for gap-filling query results

pub mod fill_processor;
pub mod group_by_time;
pub mod time_interval;

pub use fill_processor::FillProcessor;
pub use group_by_time::GroupByTimeTransformer;
pub use time_interval::FillOption;
