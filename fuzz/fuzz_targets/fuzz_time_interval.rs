//! Fuzz target for time interval parser
//!
//! Tests that the parser handles arbitrary duration strings without panicking.
//! Valid formats: 1s, 5m, 1h, 7d, 2w, 100ms, 1000us, 1000000ns

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::str::FromStr;

/// Nanoseconds per time unit
const NANOS_PER_MICROSECOND: i64 = 1_000;
const NANOS_PER_MILLISECOND: i64 = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MINUTE: i64 = NANOS_PER_SECOND * 60;
const NANOS_PER_HOUR: i64 = NANOS_PER_MINUTE * 60;
const NANOS_PER_DAY: i64 = NANOS_PER_HOUR * 24;
const NANOS_PER_WEEK: i64 = NANOS_PER_DAY * 7;

/// Error type for time interval parsing
#[derive(Debug, Clone, PartialEq)]
pub enum ParseTimeIntervalError {
    Empty,
    MissingUnit,
    InvalidNumber(String),
    InvalidUnit(String),
    NonPositive,
    Overflow,
}

/// Represents a time interval in nanoseconds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeInterval {
    pub nanos: i64,
}

impl FromStr for TimeInterval {
    type Err = ParseTimeIntervalError;

    /// Parse an InfluxDB-style duration literal
    ///
    /// Supported formats:
    /// - `<n>ns` - nanoseconds
    /// - `<n>us` or `<n>µs` - microseconds
    /// - `<n>ms` - milliseconds
    /// - `<n>s` - seconds
    /// - `<n>m` - minutes
    /// - `<n>h` - hours
    /// - `<n>d` - days
    /// - `<n>w` - weeks
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseTimeIntervalError::Empty);
        }

        // Find where the number ends and the unit begins
        let unit_start = s
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .ok_or(ParseTimeIntervalError::MissingUnit)?;

        if unit_start == 0 {
            return Err(ParseTimeIntervalError::InvalidNumber(s.to_string()));
        }

        let (num_str, unit) = s.split_at(unit_start);
        let value: i64 = num_str
            .parse()
            .map_err(|_| ParseTimeIntervalError::InvalidNumber(num_str.to_string()))?;

        if value <= 0 {
            return Err(ParseTimeIntervalError::NonPositive);
        }

        let multiplier = match unit.to_lowercase().as_str() {
            "ns" => 1i64,
            "us" | "µs" => NANOS_PER_MICROSECOND,
            "ms" => NANOS_PER_MILLISECOND,
            "s" => NANOS_PER_SECOND,
            "m" => NANOS_PER_MINUTE,
            "h" => NANOS_PER_HOUR,
            "d" => NANOS_PER_DAY,
            "w" => NANOS_PER_WEEK,
            _ => return Err(ParseTimeIntervalError::InvalidUnit(unit.to_string())),
        };

        // Check for overflow
        let nanos = value.checked_mul(multiplier).ok_or(ParseTimeIntervalError::Overflow)?;

        Ok(Self { nanos })
    }
}

/// Fill option for GROUP BY time queries
#[derive(Debug, Clone, PartialEq)]
pub enum FillOption {
    Null,
    None,
    Previous,
    Linear,
    Value(f64),
}

/// Error type for fill option parsing
#[derive(Debug, Clone, PartialEq)]
pub enum ParseFillOptionError {
    Empty,
    InvalidFormat(String),
    InvalidNumber(String),
}

impl FromStr for FillOption {
    type Err = ParseFillOptionError;

    /// Parse a fill option string
    ///
    /// Supported formats:
    /// - `null` - fill with null values
    /// - `none` - no fill (skip gaps)
    /// - `previous` - fill with previous value
    /// - `linear` - linear interpolation
    /// - `<number>` - fill with specific value
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if s.is_empty() {
            return Err(ParseFillOptionError::Empty);
        }

        match s.as_str() {
            "null" => Ok(FillOption::Null),
            "none" => Ok(FillOption::None),
            "previous" => Ok(FillOption::Previous),
            "linear" => Ok(FillOption::Linear),
            _ => {
                // Try to parse as a number
                s.parse::<f64>()
                    .map(FillOption::Value)
                    .map_err(|_| ParseFillOptionError::InvalidNumber(s))
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string
    if let Ok(input) = std::str::from_utf8(data) {
        // Test time interval parsing - should not panic
        let _ = TimeInterval::from_str(input);

        // Test fill option parsing - should not panic
        let _ = FillOption::from_str(input);

        // Test with trimmed input
        let trimmed = input.trim();
        let _ = TimeInterval::from_str(trimmed);
        let _ = FillOption::from_str(trimmed);
    }
});
