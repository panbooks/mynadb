//! Time Interval Parser
//!
//! Parses InfluxDB-style duration literals (e.g., `1s`, `5m`, `1h`, `1d`, `1w`)
//! and fill options for GROUP BY time() queries.

use std::fmt;
use std::str::FromStr;

/// Nanoseconds per time unit
const NANOS_PER_MICROSECOND: i64 = 1_000;
const NANOS_PER_MILLISECOND: i64 = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MINUTE: i64 = NANOS_PER_SECOND * 60;
const NANOS_PER_HOUR: i64 = NANOS_PER_MINUTE * 60;
const NANOS_PER_DAY: i64 = NANOS_PER_HOUR * 24;
const NANOS_PER_WEEK: i64 = NANOS_PER_DAY * 7;

/// Represents a time interval in nanoseconds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeInterval {
    /// Duration in nanoseconds
    pub nanos: i64,
}

impl TimeInterval {
    /// Create a new TimeInterval from nanoseconds
    pub fn from_nanos(nanos: i64) -> Self {
        Self { nanos }
    }

    /// Create a new TimeInterval from seconds
    pub fn from_secs(secs: i64) -> Self {
        Self {
            nanos: secs * NANOS_PER_SECOND,
        }
    }

    /// Create a new TimeInterval from minutes
    pub fn from_mins(mins: i64) -> Self {
        Self {
            nanos: mins * NANOS_PER_MINUTE,
        }
    }

    /// Create a new TimeInterval from hours
    pub fn from_hours(hours: i64) -> Self {
        Self {
            nanos: hours * NANOS_PER_HOUR,
        }
    }

    /// Create a new TimeInterval from days
    pub fn from_days(days: i64) -> Self {
        Self {
            nanos: days * NANOS_PER_DAY,
        }
    }

    /// Get the interval in nanoseconds
    pub fn as_nanos(&self) -> i64 {
        self.nanos
    }

    /// Get the interval in milliseconds
    pub fn as_millis(&self) -> i64 {
        self.nanos / NANOS_PER_MILLISECOND
    }

    /// Get the interval in seconds
    pub fn as_secs(&self) -> i64 {
        self.nanos / NANOS_PER_SECOND
    }

    /// Get the interval in minutes
    pub fn as_mins(&self) -> i64 {
        self.nanos / NANOS_PER_MINUTE
    }
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
    ///
    /// Examples: `1s`, `5m`, `1h`, `7d`, `2w`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseTimeIntervalError::Empty);
        }

        // Find where the number ends and the unit begins
        let unit_start = s
            .find(|c: char| !c.is_ascii_digit())
            .ok_or(ParseTimeIntervalError::MissingUnit)?;

        let (num_str, unit) = s.split_at(unit_start);
        let value: i64 = num_str
            .parse()
            .map_err(|_| ParseTimeIntervalError::InvalidNumber(num_str.to_string()))?;

        if value <= 0 {
            return Err(ParseTimeIntervalError::NonPositive);
        }

        let nanos = match unit.to_lowercase().as_str() {
            "ns" => value,
            "us" | "µs" => value * NANOS_PER_MICROSECOND,
            "ms" => value * NANOS_PER_MILLISECOND,
            "s" => value * NANOS_PER_SECOND,
            "m" => value * NANOS_PER_MINUTE,
            "h" => value * NANOS_PER_HOUR,
            "d" => value * NANOS_PER_DAY,
            "w" => value * NANOS_PER_WEEK,
            _ => return Err(ParseTimeIntervalError::InvalidUnit(unit.to_string())),
        };

        Ok(Self { nanos })
    }
}

impl fmt::Display for TimeInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display in the most appropriate unit
        let nanos = self.nanos;
        if nanos % NANOS_PER_WEEK == 0 {
            write!(f, "{}w", nanos / NANOS_PER_WEEK)
        } else if nanos % NANOS_PER_DAY == 0 {
            write!(f, "{}d", nanos / NANOS_PER_DAY)
        } else if nanos % NANOS_PER_HOUR == 0 {
            write!(f, "{}h", nanos / NANOS_PER_HOUR)
        } else if nanos % NANOS_PER_MINUTE == 0 {
            write!(f, "{}m", nanos / NANOS_PER_MINUTE)
        } else if nanos % NANOS_PER_SECOND == 0 {
            write!(f, "{}s", nanos / NANOS_PER_SECOND)
        } else if nanos % NANOS_PER_MILLISECOND == 0 {
            write!(f, "{}ms", nanos / NANOS_PER_MILLISECOND)
        } else if nanos % NANOS_PER_MICROSECOND == 0 {
            write!(f, "{}us", nanos / NANOS_PER_MICROSECOND)
        } else {
            write!(f, "{}ns", nanos)
        }
    }
}

/// Errors that can occur when parsing a time interval
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseTimeIntervalError {
    Empty,
    MissingUnit,
    InvalidNumber(String),
    InvalidUnit(String),
    NonPositive,
}

impl fmt::Display for ParseTimeIntervalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "empty time interval"),
            Self::MissingUnit => write!(f, "missing time unit"),
            Self::InvalidNumber(s) => write!(f, "invalid number: '{}'", s),
            Self::InvalidUnit(s) => write!(
                f,
                "invalid time unit: '{}' (expected ns, us, ms, s, m, h, d, or w)",
                s
            ),
            Self::NonPositive => write!(f, "time interval must be positive"),
        }
    }
}

impl std::error::Error for ParseTimeIntervalError {}

/// Fill option for missing time buckets in GROUP BY time() queries
#[derive(Debug, Clone, PartialEq)]
pub enum FillOption {
    /// Insert null values for missing time buckets (default InfluxDB behavior)
    Null,
    /// Do not fill missing buckets - omit rows for empty intervals
    None,
    /// Forward-fill: use the previous non-null value
    Previous,
    /// Linear interpolation between adjacent values
    Linear,
    /// Fill with a constant numeric value
    Value(f64),
}

impl Default for FillOption {
    fn default() -> Self {
        Self::None
    }
}

impl FromStr for FillOption {
    type Err = ParseFillOptionError;

    /// Parse a fill option from a string
    ///
    /// Supported formats:
    /// - `null` - fill with null values
    /// - `none` - no filling (default)
    /// - `previous` - forward fill
    /// - `linear` - linear interpolation
    /// - `<number>` - fill with constant value (e.g., `0`, `100.5`)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        match s.as_str() {
            "null" => Ok(Self::Null),
            "none" => Ok(Self::None),
            "previous" => Ok(Self::Previous),
            "linear" => Ok(Self::Linear),
            _ => {
                // Try to parse as a number
                s.parse::<f64>()
                    .map(Self::Value)
                    .map_err(|_| ParseFillOptionError::Invalid(s))
            }
        }
    }
}

impl fmt::Display for FillOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::None => write!(f, "none"),
            Self::Previous => write!(f, "previous"),
            Self::Linear => write!(f, "linear"),
            Self::Value(v) => write!(f, "{}", v),
        }
    }
}

/// Errors that can occur when parsing a fill option
#[derive(Debug, Clone, PartialEq)]
pub enum ParseFillOptionError {
    Invalid(String),
}

impl fmt::Display for ParseFillOptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) => write!(
                f,
                "invalid fill option: '{}' (expected null, none, previous, linear, or a number)",
                s
            ),
        }
    }
}

impl std::error::Error for ParseFillOptionError {}

/// Parsed GROUP BY time() clause information
#[derive(Debug, Clone)]
pub struct GroupByTimeClause {
    /// Time bucket interval
    pub interval: TimeInterval,
    /// Optional offset for bucket alignment
    pub offset: Option<TimeInterval>,
    /// Fill option for missing buckets
    pub fill: FillOption,
}

impl GroupByTimeClause {
    /// Create a new GROUP BY time() clause with defaults
    pub fn new(interval: TimeInterval) -> Self {
        Self {
            interval,
            offset: None,
            fill: FillOption::default(),
        }
    }

    /// Set the offset
    pub fn with_offset(mut self, offset: TimeInterval) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set the fill option
    pub fn with_fill(mut self, fill: FillOption) -> Self {
        self.fill = fill;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval_seconds() {
        let interval: TimeInterval = "1s".parse().unwrap();
        assert_eq!(interval.nanos, 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_minutes() {
        let interval: TimeInterval = "5m".parse().unwrap();
        assert_eq!(interval.nanos, 5 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_hours() {
        let interval: TimeInterval = "1h".parse().unwrap();
        assert_eq!(interval.nanos, 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_days() {
        let interval: TimeInterval = "7d".parse().unwrap();
        assert_eq!(interval.nanos, 7 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_weeks() {
        let interval: TimeInterval = "2w".parse().unwrap();
        assert_eq!(interval.nanos, 2 * 7 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_milliseconds() {
        let interval: TimeInterval = "100ms".parse().unwrap();
        assert_eq!(interval.nanos, 100_000_000);
    }

    #[test]
    fn test_parse_interval_microseconds() {
        let interval: TimeInterval = "500us".parse().unwrap();
        assert_eq!(interval.nanos, 500_000);
    }

    #[test]
    fn test_parse_interval_nanoseconds() {
        let interval: TimeInterval = "1000ns".parse().unwrap();
        assert_eq!(interval.nanos, 1000);
    }

    #[test]
    fn test_parse_interval_case_insensitive() {
        let upper: TimeInterval = "5M".parse().unwrap();
        let lower: TimeInterval = "5m".parse().unwrap();
        assert_eq!(upper.nanos, lower.nanos);
    }

    #[test]
    fn test_parse_interval_invalid_unit() {
        let result: Result<TimeInterval, _> = "5x".parse();
        assert!(matches!(result, Err(ParseTimeIntervalError::InvalidUnit(_))));
    }

    #[test]
    fn test_parse_interval_missing_unit() {
        let result: Result<TimeInterval, _> = "5".parse();
        assert!(matches!(result, Err(ParseTimeIntervalError::MissingUnit)));
    }

    #[test]
    fn test_parse_interval_empty() {
        let result: Result<TimeInterval, _> = "".parse();
        assert!(matches!(result, Err(ParseTimeIntervalError::Empty)));
    }

    #[test]
    fn test_parse_interval_non_positive() {
        let result: Result<TimeInterval, _> = "0m".parse();
        assert!(matches!(result, Err(ParseTimeIntervalError::NonPositive)));
    }

    #[test]
    fn test_interval_display() {
        assert_eq!(TimeInterval::from_mins(5).to_string(), "5m");
        assert_eq!(TimeInterval::from_hours(1).to_string(), "1h");
        assert_eq!(TimeInterval::from_days(1).to_string(), "1d");
        assert_eq!(TimeInterval::from_secs(90).to_string(), "90s");
    }

    #[test]
    fn test_parse_fill_null() {
        let fill: FillOption = "null".parse().unwrap();
        assert_eq!(fill, FillOption::Null);
    }

    #[test]
    fn test_parse_fill_none() {
        let fill: FillOption = "none".parse().unwrap();
        assert_eq!(fill, FillOption::None);
    }

    #[test]
    fn test_parse_fill_previous() {
        let fill: FillOption = "previous".parse().unwrap();
        assert_eq!(fill, FillOption::Previous);
    }

    #[test]
    fn test_parse_fill_linear() {
        let fill: FillOption = "linear".parse().unwrap();
        assert_eq!(fill, FillOption::Linear);
    }

    #[test]
    fn test_parse_fill_value() {
        let fill: FillOption = "0".parse().unwrap();
        assert_eq!(fill, FillOption::Value(0.0));

        let fill: FillOption = "100.5".parse().unwrap();
        assert_eq!(fill, FillOption::Value(100.5));
    }

    #[test]
    fn test_parse_fill_case_insensitive() {
        let upper: FillOption = "NULL".parse().unwrap();
        let lower: FillOption = "null".parse().unwrap();
        assert_eq!(upper, lower);
    }

    #[test]
    fn test_group_by_time_clause() {
        let clause = GroupByTimeClause::new(TimeInterval::from_mins(5))
            .with_offset(TimeInterval::from_secs(30))
            .with_fill(FillOption::Previous);

        assert_eq!(clause.interval.as_mins(), 5);
        assert_eq!(clause.offset.unwrap().as_secs(), 30);
        assert_eq!(clause.fill, FillOption::Previous);
    }
}
