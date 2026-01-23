//! GROUP BY time() SQL Transformer
//!
//! Transforms InfluxQL-style GROUP BY time(interval) queries into standard SQL
//! using the time_bucket UDF.
//!
//! Example transformation:
//!
//! Input:
//! ```sql
//! SELECT mean(usage) FROM cpu
//! WHERE timestamp >= 1704067200000000000
//! GROUP BY time(5m), host
//! fill(previous)
//! ```
//!
//! Output:
//! ```sql
//! SELECT time_bucket(timestamp, 300000000000, 0) as time, host, avg(usage) as mean_usage
//! FROM cpu
//! WHERE timestamp >= 1704067200000000000
//! GROUP BY time_bucket(timestamp, 300000000000, 0), host
//! ORDER BY time
//! ```

use crate::query::time_interval::{FillOption, TimeInterval};
use regex::Regex;
use std::collections::HashMap;

/// Result of transforming a GROUP BY time() query
#[derive(Debug, Clone)]
pub struct TransformedQuery {
    /// The transformed SQL query
    pub sql: String,
    /// Fill option extracted from the query (if any)
    pub fill_option: FillOption,
    /// Time bucket interval in nanoseconds (for fill processing)
    pub interval_nanos: Option<i64>,
    /// Time bucket offset in nanoseconds
    pub offset_nanos: i64,
    /// Whether the query was transformed
    pub was_transformed: bool,
    /// The time column name used in the query
    pub time_column: String,
    /// Additional GROUP BY columns (excluding time)
    pub group_columns: Vec<String>,
}

impl Default for TransformedQuery {
    fn default() -> Self {
        Self {
            sql: String::new(),
            fill_option: FillOption::default(),
            interval_nanos: None,
            offset_nanos: 0,
            was_transformed: false,
            time_column: "timestamp".to_string(),
            group_columns: Vec::new(),
        }
    }
}

/// InfluxDB to DataFusion function mapping
fn get_function_mapping() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    map.insert("mean", "avg");
    map.insert("sum", "sum");
    map.insert("count", "count");
    map.insert("min", "min");
    map.insert("max", "max");
    map.insert("stddev", "stddev");
    map.insert("median", "approx_median");
    // first/last would need custom UDFs
    // spread would need transformation to max() - min()
    map
}

/// SQL Transformer for GROUP BY time() queries
pub struct GroupByTimeTransformer {
    /// The default timestamp column name
    timestamp_column: String,
}

impl Default for GroupByTimeTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupByTimeTransformer {
    /// Create a new transformer with default settings
    pub fn new() -> Self {
        Self {
            timestamp_column: "timestamp".to_string(),
        }
    }

    /// Create a transformer with a custom timestamp column
    pub fn with_timestamp_column(mut self, column: &str) -> Self {
        self.timestamp_column = column.to_string();
        self
    }

    /// Transform a SQL query with GROUP BY time() syntax
    ///
    /// This detects InfluxQL-style GROUP BY time(interval) syntax and transforms it
    /// to use the time_bucket UDF.
    pub fn transform(&self, sql: &str) -> Result<TransformedQuery, TransformError> {
        // Check if this query contains GROUP BY time()
        if !self.has_group_by_time(sql) {
            return Ok(TransformedQuery {
                sql: sql.to_string(),
                was_transformed: false,
                ..Default::default()
            });
        }

        // Extract and remove fill() clause first
        let (sql_without_fill, fill_option) = self.extract_fill_clause(sql)?;

        // Parse the GROUP BY time() clause
        let (interval, offset) = self.parse_group_by_time(&sql_without_fill)?;

        // Extract other GROUP BY columns
        let group_columns = self.extract_group_columns(&sql_without_fill)?;

        // Transform the query
        let transformed_sql = self.transform_query(&sql_without_fill, &interval, offset)?;

        Ok(TransformedQuery {
            sql: transformed_sql,
            fill_option,
            interval_nanos: Some(interval.nanos),
            offset_nanos: offset.map_or(0, |o| o.nanos),
            was_transformed: true,
            time_column: self.timestamp_column.clone(),
            group_columns,
        })
    }

    /// Check if the query contains GROUP BY time()
    fn has_group_by_time(&self, sql: &str) -> bool {
        // Look for GROUP BY ... time( with case-insensitive matching
        let group_by_pattern = Regex::new(r"(?i)GROUP\s+BY\s+.*\bTIME\s*\(").unwrap();
        group_by_pattern.is_match(sql)
    }

    /// Extract and remove the fill() clause from the query
    fn extract_fill_clause(&self, sql: &str) -> Result<(String, FillOption), TransformError> {
        // Match fill(option) at the end of the query
        let fill_pattern = Regex::new(r"(?i)\s+fill\s*\(\s*([^)]+)\s*\)\s*$").unwrap();

        if let Some(caps) = fill_pattern.captures(sql) {
            let fill_str = caps.get(1).unwrap().as_str().trim();
            let fill_option = fill_str
                .parse::<FillOption>()
                .map_err(|e| TransformError::InvalidFillOption(e.to_string()))?;

            let sql_without_fill = fill_pattern.replace(sql, "").to_string();
            Ok((sql_without_fill, fill_option))
        } else {
            Ok((sql.to_string(), FillOption::default()))
        }
    }

    /// Parse the GROUP BY time(interval[, offset]) clause
    fn parse_group_by_time(
        &self,
        sql: &str,
    ) -> Result<(TimeInterval, Option<TimeInterval>), TransformError> {
        // Match time(interval) or time(interval, offset)
        let time_pattern =
            Regex::new(r"(?i)\btime\s*\(\s*([^,)]+)(?:\s*,\s*([^)]+))?\s*\)").unwrap();

        if let Some(caps) = time_pattern.captures(sql) {
            let interval_str = caps.get(1).unwrap().as_str().trim();
            let interval = interval_str
                .parse::<TimeInterval>()
                .map_err(|e| TransformError::InvalidInterval(e.to_string()))?;

            let offset = if let Some(offset_match) = caps.get(2) {
                let offset_str = offset_match.as_str().trim();
                Some(
                    offset_str
                        .parse::<TimeInterval>()
                        .map_err(|e| TransformError::InvalidOffset(e.to_string()))?,
                )
            } else {
                None
            };

            Ok((interval, offset))
        } else {
            Err(TransformError::NoGroupByTime)
        }
    }

    /// Extract GROUP BY columns other than time()
    fn extract_group_columns(&self, sql: &str) -> Result<Vec<String>, TransformError> {
        let group_by_pattern = Regex::new(r"(?i)GROUP\s+BY\s+(.+?)(?:ORDER|LIMIT|$)").unwrap();

        if let Some(caps) = group_by_pattern.captures(sql) {
            let group_clause = caps.get(1).unwrap().as_str();

            // Split by comma, filter out time()
            let columns: Vec<String> = group_clause
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| {
                    let upper = s.to_uppercase();
                    !upper.starts_with("TIME(") && !upper.starts_with("TIME (")
                })
                .collect();

            Ok(columns)
        } else {
            Ok(Vec::new())
        }
    }

    /// Transform the full query
    fn transform_query(
        &self,
        sql: &str,
        interval: &TimeInterval,
        offset: Option<TimeInterval>,
    ) -> Result<String, TransformError> {
        let offset_nanos = offset.map_or(0, |o| o.nanos);

        // Build the time_bucket expression
        let time_bucket_expr = format!(
            "time_bucket({}, {}, {})",
            self.timestamp_column, interval.nanos, offset_nanos
        );

        // Transform SELECT clause - map InfluxDB functions and add time bucket
        let transformed_select = self.transform_select_clause(sql, &time_bucket_expr)?;

        // Transform GROUP BY clause
        let transformed = self.transform_group_by_clause(&transformed_select, &time_bucket_expr)?;

        // Add ORDER BY time if not present
        let transformed = self.ensure_order_by(&transformed)?;

        Ok(transformed)
    }

    /// Transform the SELECT clause
    fn transform_select_clause(
        &self,
        sql: &str,
        time_bucket_expr: &str,
    ) -> Result<String, TransformError> {
        // Extract SELECT ... FROM portion
        let select_pattern = Regex::new(r"(?i)^(\s*SELECT\s+)(.*?)(\s+FROM\s+.*)$").unwrap();

        if let Some(caps) = select_pattern.captures(sql) {
            let select_keyword = caps.get(1).unwrap().as_str();
            let select_columns = caps.get(2).unwrap().as_str().trim();
            let from_onwards = caps.get(3).unwrap().as_str();

            // Check for SELECT * which is not supported with GROUP BY time()
            // because aggregation requires explicit columns
            if select_columns == "*"
                || select_columns.starts_with("*, ")
                || select_columns.contains(", *")
            {
                return Err(TransformError::InvalidSqlSyntax(
                    "SELECT * is not supported with GROUP BY time(). Please specify columns explicitly, e.g., SELECT mean(column_name) FROM table GROUP BY time(5m)".to_string()
                ));
            }

            // Transform aggregate functions
            let transformed_columns = self.transform_aggregates(select_columns)?;

            // Add time bucket as first column if not already present
            let final_columns = if !select_columns.to_uppercase().contains("TIME_BUCKET") {
                format!("{} as time, {}", time_bucket_expr, transformed_columns)
            } else {
                transformed_columns
            };

            Ok(format!(
                "{}{}{}",
                select_keyword, final_columns, from_onwards
            ))
        } else {
            Err(TransformError::InvalidSqlSyntax(
                "Could not parse SELECT clause".to_string(),
            ))
        }
    }

    /// Transform InfluxDB aggregate functions to DataFusion equivalents
    fn transform_aggregates(&self, select_columns: &str) -> Result<String, TransformError> {
        let mut result = select_columns.to_string();
        let func_map = get_function_mapping();

        // Transform each function
        for (influx_fn, datafusion_fn) in &func_map {
            // Match function call pattern: func(column)
            let pattern = Regex::new(&format!(r"(?i)\b{}\s*\(\s*([^)]+)\s*\)", influx_fn)).unwrap();

            result = pattern
                .replace_all(&result, |caps: &regex::Captures| {
                    let arg = caps.get(1).unwrap().as_str();
                    format!("{}({}) as {}_{}", datafusion_fn, arg, influx_fn, arg)
                })
                .to_string();
        }

        // Handle spread() -> max() - min()
        let spread_pattern = Regex::new(r"(?i)\bspread\s*\(\s*([^)]+)\s*\)").unwrap();
        result = spread_pattern
            .replace_all(&result, |caps: &regex::Captures| {
                let arg = caps.get(1).unwrap().as_str();
                format!("(max({}) - min({})) as spread_{}", arg, arg, arg)
            })
            .to_string();

        Ok(result)
    }

    /// Transform the GROUP BY clause
    fn transform_group_by_clause(
        &self,
        sql: &str,
        time_bucket_expr: &str,
    ) -> Result<String, TransformError> {
        // Match GROUP BY time(...), other_columns
        let group_by_pattern =
            Regex::new(r"(?i)(GROUP\s+BY\s+)time\s*\([^)]+\)(\s*,\s*)?(.*)").unwrap();

        if let Some(caps) = group_by_pattern.captures(sql) {
            let group_by_keyword = caps.get(1).unwrap().as_str();
            let other_columns = caps.get(3).map_or("", |m| m.as_str()).trim();

            let new_group_by = if other_columns.is_empty() {
                format!("{}{}", group_by_keyword, time_bucket_expr)
            } else {
                format!(
                    "{}{}, {}",
                    group_by_keyword, time_bucket_expr, other_columns
                )
            };

            Ok(group_by_pattern
                .replace(sql, new_group_by.as_str())
                .to_string())
        } else {
            Ok(sql.to_string())
        }
    }

    /// Ensure ORDER BY time is present
    fn ensure_order_by(&self, sql: &str) -> Result<String, TransformError> {
        let sql_upper = sql.to_uppercase();

        // Check if ORDER BY already exists
        if sql_upper.contains("ORDER BY") {
            return Ok(sql.to_string());
        }

        // Add ORDER BY time at the end
        // Handle LIMIT clause if present
        let limit_pattern = Regex::new(r"(?i)(\s+LIMIT\s+\d+.*)$").unwrap();

        if let Some(caps) = limit_pattern.captures(sql) {
            let limit_clause = caps.get(1).unwrap().as_str();
            let sql_without_limit = limit_pattern.replace(sql, "").to_string();
            Ok(format!(
                "{} ORDER BY time{}",
                sql_without_limit, limit_clause
            ))
        } else {
            Ok(format!("{} ORDER BY time", sql.trim_end()))
        }
    }
}

/// Errors that can occur during query transformation
#[derive(Debug, Clone)]
pub enum TransformError {
    NoGroupByTime,
    InvalidInterval(String),
    InvalidOffset(String),
    InvalidFillOption(String),
    InvalidSqlSyntax(String),
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoGroupByTime => write!(f, "No GROUP BY time() clause found"),
            Self::InvalidInterval(e) => write!(f, "Invalid time interval: {}", e),
            Self::InvalidOffset(e) => write!(f, "Invalid time offset: {}", e),
            Self::InvalidFillOption(e) => write!(f, "Invalid fill option: {}", e),
            Self::InvalidSqlSyntax(e) => write!(f, "Invalid SQL syntax: {}", e),
        }
    }
}

impl std::error::Error for TransformError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_group_by_time() {
        let transformer = GroupByTimeTransformer::new();

        assert!(transformer.has_group_by_time("SELECT mean(usage) FROM cpu GROUP BY time(5m)"));
        assert!(
            transformer.has_group_by_time("SELECT mean(usage) FROM cpu GROUP BY time(5m), host")
        );
        assert!(transformer.has_group_by_time("SELECT * FROM cpu GROUP BY TIME(1h)"));
        assert!(!transformer.has_group_by_time("SELECT * FROM cpu GROUP BY host"));
        assert!(!transformer.has_group_by_time("SELECT * FROM cpu"));
    }

    #[test]
    fn test_extract_fill_clause() {
        let transformer = GroupByTimeTransformer::new();

        let (sql, fill) = transformer
            .extract_fill_clause("SELECT mean(usage) FROM cpu GROUP BY time(5m) fill(previous)")
            .unwrap();
        assert_eq!(fill, FillOption::Previous);
        assert!(!sql.contains("fill"));

        let (sql, fill) = transformer
            .extract_fill_clause("SELECT mean(usage) FROM cpu GROUP BY time(5m) fill(0)")
            .unwrap();
        assert_eq!(fill, FillOption::Value(0.0));
        assert!(!sql.contains("fill"));

        let (sql, fill) = transformer
            .extract_fill_clause("SELECT mean(usage) FROM cpu GROUP BY time(5m)")
            .unwrap();
        assert_eq!(fill, FillOption::None);
        assert_eq!(sql, "SELECT mean(usage) FROM cpu GROUP BY time(5m)");
    }

    #[test]
    fn test_parse_group_by_time() {
        let transformer = GroupByTimeTransformer::new();

        let (interval, offset) = transformer
            .parse_group_by_time("SELECT mean(usage) FROM cpu GROUP BY time(5m)")
            .unwrap();
        assert_eq!(interval.as_mins(), 5);
        assert!(offset.is_none());

        let (interval, offset) = transformer
            .parse_group_by_time("SELECT mean(usage) FROM cpu GROUP BY time(1h, 15m)")
            .unwrap();
        assert_eq!(interval.as_mins(), 60);
        assert_eq!(offset.unwrap().as_mins(), 15);
    }

    #[test]
    fn test_extract_group_columns() {
        let transformer = GroupByTimeTransformer::new();

        let columns = transformer
            .extract_group_columns("SELECT mean(usage) FROM cpu GROUP BY time(5m), host, region")
            .unwrap();
        assert_eq!(columns, vec!["host", "region"]);

        let columns = transformer
            .extract_group_columns("SELECT mean(usage) FROM cpu GROUP BY time(5m)")
            .unwrap();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_transform_aggregates() {
        let transformer = GroupByTimeTransformer::new();

        let result = transformer.transform_aggregates("mean(usage)").unwrap();
        assert!(result.contains("avg(usage)"));
        assert!(result.contains("as mean_usage"));

        let result = transformer
            .transform_aggregates("sum(value), count(*)")
            .unwrap();
        assert!(result.contains("sum(value)"));
        assert!(result.contains("count(*)"));

        let result = transformer
            .transform_aggregates("spread(temperature)")
            .unwrap();
        assert!(result.contains("max(temperature) - min(temperature)"));
    }

    #[test]
    fn test_transform_basic_query() {
        let transformer = GroupByTimeTransformer::new();

        let result = transformer
            .transform("SELECT mean(usage) FROM cpu WHERE timestamp >= 1704067200000000000 GROUP BY time(5m)")
            .unwrap();

        assert!(result.was_transformed);
        assert!(result.sql.contains("time_bucket(timestamp,"));
        assert!(result.sql.contains("300000000000")); // 5 minutes in nanos
        assert!(result.sql.contains("avg(usage)"));
        assert!(result.sql.contains("ORDER BY time"));
        assert_eq!(result.fill_option, FillOption::None);
    }

    #[test]
    fn test_transform_with_offset() {
        let transformer = GroupByTimeTransformer::new();

        let result = transformer
            .transform("SELECT mean(usage) FROM cpu GROUP BY time(1h, 15m) fill(previous)")
            .unwrap();

        assert!(result.was_transformed);
        assert!(result.sql.contains("time_bucket(timestamp,"));
        assert_eq!(result.fill_option, FillOption::Previous);
        assert_eq!(result.offset_nanos, 15 * 60 * 1_000_000_000); // 15 minutes
    }

    #[test]
    fn test_transform_with_multiple_group_columns() {
        let transformer = GroupByTimeTransformer::new();

        let result = transformer
            .transform("SELECT mean(usage) FROM cpu GROUP BY time(5m), host, region")
            .unwrap();

        assert!(result.was_transformed);
        assert!(result.sql.contains("time_bucket(timestamp,"));
        assert!(result.sql.contains("host"));
        assert!(result.sql.contains("region"));
        assert_eq!(result.group_columns, vec!["host", "region"]);
    }

    #[test]
    fn test_no_transformation_needed() {
        let transformer = GroupByTimeTransformer::new();

        let result = transformer
            .transform("SELECT * FROM cpu WHERE timestamp > 1000")
            .unwrap();

        assert!(!result.was_transformed);
        assert_eq!(result.sql, "SELECT * FROM cpu WHERE timestamp > 1000");
    }

    #[test]
    fn test_transform_preserves_limit() {
        let transformer = GroupByTimeTransformer::new();

        let result = transformer
            .transform("SELECT mean(usage) FROM cpu GROUP BY time(5m) LIMIT 100")
            .unwrap();

        assert!(result.was_transformed);
        assert!(result.sql.contains("ORDER BY time"));
        assert!(result.sql.contains("LIMIT 100"));
        // ORDER BY should come before LIMIT
        let order_pos = result.sql.find("ORDER BY").unwrap();
        let limit_pos = result.sql.find("LIMIT").unwrap();
        assert!(order_pos < limit_pos);
    }
}
