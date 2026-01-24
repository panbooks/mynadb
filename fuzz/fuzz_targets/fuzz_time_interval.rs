//! Fuzz target for time interval parser
//!
//! Tests the REAL TimeInterval and FillOption parsers from
//! src/query/timeseries/time_interval.rs to find edge cases and potential panics.

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::str::FromStr;

// Import the REAL parsers from the mynadb library
use mynadb::query::timeseries::time_interval::{TimeInterval, FillOption};

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string
    if let Ok(input) = std::str::from_utf8(data) {
        // Test TimeInterval parsing with the REAL function - should not panic
        let _ = TimeInterval::from_str(input);

        // Test FillOption parsing with the REAL function - should not panic
        let _ = FillOption::from_str(input);

        // Test with trimmed input
        let trimmed = input.trim();
        if !trimmed.is_empty() {
            let _ = TimeInterval::from_str(trimmed);
            let _ = FillOption::from_str(trimmed);
        }

        // Test with lowercase
        let lower = input.to_lowercase();
        let _ = TimeInterval::from_str(&lower);
        let _ = FillOption::from_str(&lower);
    }
});
