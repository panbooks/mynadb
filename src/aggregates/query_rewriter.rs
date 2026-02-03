//! Query Rewriter for Continuous Aggregates
//!
//! This module implements automatic query rewriting to use pre-aggregated data
//! when possible. It analyzes incoming queries and rewrites them to query the
//! aggregate table instead of the raw data table.
//!
//! # How It Works
//!
//! 1. Parse the incoming SQL to extract:
//!    - Source tables
//!    - Time bucket (if any)
//!    - Aggregate functions
//!    - Group by columns
//!    - Time range predicates
//!
//! 2. Find matching aggregates:
//!    - Same source table
//!    - Compatible time bucket (query bucket >= aggregate bucket)
//!    - Required aggregate columns exist
//!    - Required group by columns exist
//!
//! 3. Rewrite query:
//!    - Replace source table with aggregate table
//!    - Map aggregate functions to pre-computed columns
//!    - Optionally add real-time data via UNION
//!
//! # Example
//!
//! Original:
//! ```sql
//! SELECT time_bucket(time, 3600000000000, 0) as hour, host, avg(cpu_usage)
//! FROM mydb.default.cpu
//! WHERE time >= now() - interval '7 days'
//! GROUP BY hour, host
//! ```
//!
//! Rewritten:
//! ```sql
//! SELECT hour, host, avg_cpu
//! FROM mydb.default._ca_hourly_cpu
//! WHERE hour >= now() - interval '7 days'
//! ```

use super::definition::{AggregateDefinition, AggregateFunction, GroupByColumn};
use super::storage::AggregateStorage;
use super::types::TimeBucketGranularity;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};

/// Query rewriter for continuous aggregates
pub struct ContinuousAggregateRewriter {
    storage: Arc<AggregateStorage>,
}

impl ContinuousAggregateRewriter {
    /// Create a new rewriter with the given storage backend
    pub fn new(storage: Arc<AggregateStorage>) -> Self {
        Self { storage }
    }

    /// Check if a query can be rewritten to use aggregates
    ///
    /// Returns the rewritten SQL and the aggregate used, or None if rewriting is not possible.
    pub async fn try_rewrite(&self, sql: &str) -> Option<RewriteResult> {
        // Parse the query to extract components
        let query_info = self.parse_query(sql)?;

        trace!("Parsed query: {:?}", query_info);

        // Find matching aggregates
        let matches = self
            .storage
            .find_matching_aggregates(&query_info.source_table, query_info.time_bucket_nanos)
            .await
            .ok()?;

        if matches.is_empty() {
            debug!(
                "No matching aggregates found for {}",
                query_info.source_table
            );
            return None;
        }

        // Find the best match (finest granularity that can answer the query)
        let best_match = self.select_best_aggregate(&query_info, &matches)?;

        debug!(
            "Selected aggregate {} for query on {}",
            best_match.name, query_info.source_table
        );

        // Rewrite the query
        let rewritten = self.rewrite_query(&query_info, best_match)?;

        Some(RewriteResult {
            original_sql: sql.to_string(),
            rewritten_sql: rewritten,
            aggregate_name: best_match.name.clone(),
            aggregate_table: best_match.target_table.clone(),
            watermark_ns: best_match.watermark.high_watermark_ns,
        })
    }

    /// Parse a SQL query to extract relevant information
    fn parse_query(&self, sql: &str) -> Option<QueryInfo> {
        let sql_lower = sql.to_lowercase();

        // Basic validation - must be a SELECT
        if !sql_lower.trim().starts_with("select") {
            return None;
        }

        // Extract source table (simple heuristic - look for FROM clause)
        let source_table = self.extract_source_table(sql)?;

        // Extract time bucket if present
        let time_bucket_nanos = self.extract_time_bucket(sql);

        // Extract aggregate functions
        let aggregates = self.extract_aggregates(sql);

        // Extract group by columns
        let group_by_columns = self.extract_group_by(sql);

        // Extract time range predicates
        let time_range = self.extract_time_range(sql);

        Some(QueryInfo {
            source_table,
            time_bucket_nanos,
            aggregates,
            group_by_columns,
            time_range,
            original_sql: sql.to_string(),
        })
    }

    /// Extract the source table from a SQL query
    fn extract_source_table(&self, sql: &str) -> Option<String> {
        let sql_lower = sql.to_lowercase();

        // Find FROM clause
        let from_pos = sql_lower.find(" from ")?;
        let after_from = &sql[from_pos + 6..];

        // Get the table name (up to next keyword or end)
        let table_end = after_from
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after_from.len());

        let table = after_from[..table_end].trim();

        // Remove any trailing keywords that might have been captured
        let table = table
            .split_whitespace()
            .next()
            .unwrap_or(table)
            .trim_end_matches(',');

        if table.is_empty() {
            None
        } else {
            Some(table.to_string())
        }
    }

    /// Extract time bucket from a SQL query
    fn extract_time_bucket(&self, sql: &str) -> Option<i64> {
        let sql_lower = sql.to_lowercase();

        // Look for time_bucket function call
        if let Some(pos) = sql_lower.find("time_bucket(") {
            let after_func = &sql[pos + 12..];
            // Find the parameters
            if let Some(paren_end) = after_func.find(')') {
                let params = &after_func[..paren_end];
                let parts: Vec<&str> = params.split(',').collect();
                if parts.len() >= 2 {
                    // Second parameter is the interval in nanos
                    if let Ok(nanos) = parts[1].trim().parse::<i64>() {
                        return Some(nanos);
                    }
                }
            }
        }

        None
    }

    /// Extract aggregate functions from a SQL query
    fn extract_aggregates(&self, sql: &str) -> Vec<(AggregateFunction, String)> {
        let mut aggregates = Vec::new();
        let sql_lower = sql.to_lowercase();

        // Look for common aggregate functions
        for func_name in &[
            "avg",
            "sum",
            "count",
            "min",
            "max",
            "first_value",
            "last_value",
        ] {
            let pattern = format!("{}(", func_name);
            let mut search_pos = 0;

            while let Some(pos) = sql_lower[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos;
                let after_func = &sql[abs_pos + pattern.len()..];

                if let Some(paren_end) = after_func.find(')') {
                    let column = after_func[..paren_end].trim().to_string();
                    if let Some(func) = AggregateFunction::from_sql(func_name) {
                        aggregates.push((func, column));
                    }
                }

                search_pos = abs_pos + pattern.len();
            }
        }

        aggregates
    }

    /// Extract GROUP BY columns from a SQL query
    fn extract_group_by(&self, sql: &str) -> Vec<String> {
        let sql_lower = sql.to_lowercase();

        if let Some(pos) = sql_lower.find("group by") {
            let after_group = &sql[pos + 8..];

            // Find end of GROUP BY clause (next keyword or end)
            let end_pos = after_group
                .to_lowercase()
                .find("having")
                .or_else(|| after_group.to_lowercase().find("order by"))
                .or_else(|| after_group.to_lowercase().find("limit"))
                .unwrap_or(after_group.len());

            let group_by_clause = &after_group[..end_pos];

            return group_by_clause
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        Vec::new()
    }

    /// Extract time range predicates from a SQL query
    fn extract_time_range(&self, sql: &str) -> Option<(i64, i64)> {
        // This is a simplified implementation
        // A full implementation would parse the WHERE clause properly

        let sql_lower = sql.to_lowercase();

        // Look for time >= X patterns
        let mut start = None;
        let mut end = None;

        if let Some(pos) = sql_lower.find("time >=") {
            let after = &sql[pos + 7..];
            if let Some(value) = self.extract_timestamp_value(after) {
                start = Some(value);
            }
        }

        if let Some(pos) = sql_lower.find("time >") {
            if !sql_lower[pos..].starts_with("time >=") {
                let after = &sql[pos + 6..];
                if let Some(value) = self.extract_timestamp_value(after) {
                    start = Some(value);
                }
            }
        }

        if let Some(pos) = sql_lower.find("time <=") {
            let after = &sql[pos + 7..];
            if let Some(value) = self.extract_timestamp_value(after) {
                end = Some(value);
            }
        }

        if let Some(pos) = sql_lower.find("time <") {
            if !sql_lower[pos..].starts_with("time <=") {
                let after = &sql[pos + 6..];
                if let Some(value) = self.extract_timestamp_value(after) {
                    end = Some(value);
                }
            }
        }

        match (start, end) {
            (Some(s), Some(e)) => Some((s, e)),
            (Some(s), None) => Some((s, i64::MAX)),
            (None, Some(e)) => Some((0, e)),
            (None, None) => None,
        }
    }

    /// Extract a timestamp value from a string
    fn extract_timestamp_value(&self, s: &str) -> Option<i64> {
        let s = s.trim();

        // Try to parse as integer (nanoseconds)
        let num_end = s
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .unwrap_or(s.len());

        if num_end > 0 {
            if let Ok(value) = s[..num_end].parse::<i64>() {
                return Some(value);
            }
        }

        None
    }

    /// Select the best aggregate from a list of matches
    fn select_best_aggregate<'a>(
        &self,
        query_info: &QueryInfo,
        matches: &'a [AggregateDefinition],
    ) -> Option<&'a AggregateDefinition> {
        if matches.is_empty() {
            return None;
        }

        // If query has no time bucket, use the finest granularity aggregate
        let query_bucket = query_info.time_bucket_nanos.unwrap_or(0);

        // Find aggregates that can answer the query
        let mut candidates: Vec<_> = matches
            .iter()
            .filter(|def| {
                // Check if aggregate has required aggregates
                for (func, col) in &query_info.aggregates {
                    let has_agg = def.aggregate_columns.iter().any(|ac| {
                        ac.function == *func
                            && (ac.source_column == *col || col == "*" || col == "1")
                    });
                    if !has_agg {
                        return false;
                    }
                }

                // Check if aggregate can answer query bucket
                if query_bucket > 0 {
                    if let Some(agg_bucket) = def.time_bucket_granularity() {
                        if query_bucket < agg_bucket.to_nanos() {
                            return false;
                        }
                    }
                }

                true
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Sort by granularity (finest first) - prefer finest granularity that works
        candidates.sort_by_key(|def| {
            def.time_bucket_granularity()
                .map(|g| g.to_nanos())
                .unwrap_or(i64::MAX)
        });

        candidates.first().copied()
    }

    /// Rewrite a query to use an aggregate
    fn rewrite_query(
        &self,
        query_info: &QueryInfo,
        aggregate: &AggregateDefinition,
    ) -> Option<String> {
        let original = &query_info.original_sql;

        // Simple rewrite: replace source table with aggregate table
        // A full implementation would do proper SQL rewriting

        let mut rewritten = original.clone();

        // Replace table name
        rewritten = rewritten.replace(&query_info.source_table, &aggregate.target_table);

        // Replace aggregate function calls with pre-computed column names
        for (func, col) in &query_info.aggregates {
            // Find the matching aggregate column
            if let Some(agg_col) = aggregate.aggregate_columns.iter().find(|ac| {
                ac.function == *func && (ac.source_column == *col || col == "*" || col == "1")
            }) {
                // Replace avg(col) with alias
                let func_call = format!("{}({})", func.sql_name(), col);
                rewritten = rewritten.replace(&func_call, &agg_col.alias);

                // Also try uppercase version
                let func_call_upper = format!("{}({})", func.sql_name().to_uppercase(), col);
                rewritten = rewritten.replace(&func_call_upper, &agg_col.alias);
            }
        }

        // Replace time_bucket with the aggregate's time column
        if let Some(time_col) = aggregate.group_by_columns.iter().find_map(|gc| {
            if let GroupByColumn::TimeBucket { alias, .. } = gc {
                Some(alias.clone())
            } else {
                None
            }
        }) {
            // Replace time_bucket function call with the alias
            if query_info.time_bucket_nanos.is_some() {
                // Find and replace the time_bucket(...) call
                let sql_lower = rewritten.to_lowercase();
                if let Some(pos) = sql_lower.find("time_bucket(") {
                    if let Some(end) = rewritten[pos..].find(')') {
                        let before = &rewritten[..pos];
                        let after = &rewritten[pos + end + 1..];
                        rewritten = format!("{}{}{}", before, time_col, after);
                    }
                }
            }
        }

        // Remove GROUP BY if we're just selecting pre-aggregated columns
        // (This is a simplification - full implementation would be smarter)

        Some(rewritten)
    }
}

/// Information extracted from a query
#[derive(Debug)]
struct QueryInfo {
    /// Source table (fully qualified)
    source_table: String,
    /// Time bucket in nanoseconds (if present)
    time_bucket_nanos: Option<i64>,
    /// Aggregate functions and their columns
    aggregates: Vec<(AggregateFunction, String)>,
    /// Group by columns
    group_by_columns: Vec<String>,
    /// Time range (start, end) in nanoseconds
    time_range: Option<(i64, i64)>,
    /// Original SQL
    original_sql: String,
}

/// Result of a query rewrite
#[derive(Debug, Clone)]
pub struct RewriteResult {
    /// Original SQL
    pub original_sql: String,
    /// Rewritten SQL
    pub rewritten_sql: String,
    /// Name of the aggregate used
    pub aggregate_name: String,
    /// Target aggregate table
    pub aggregate_table: String,
    /// Current watermark (data up to this time is in the aggregate)
    pub watermark_ns: i64,
}

impl RewriteResult {
    /// Check if real-time data needs to be unioned
    ///
    /// Returns true if the query's time range extends beyond the watermark.
    pub fn needs_realtime_union(&self, query_end_time: Option<i64>) -> bool {
        if let Some(end) = query_end_time {
            end > self.watermark_ns
        } else {
            // No end time specified, assume query wants current data
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_storage() -> Arc<AggregateStorage> {
        let dir = tempdir().unwrap();
        Arc::new(AggregateStorage::with_dir(dir.path().to_path_buf()))
    }

    #[test]
    fn test_extract_source_table() {
        let rewriter = ContinuousAggregateRewriter::new(create_test_storage());

        let sql = "SELECT * FROM mydb.default.cpu WHERE time > 0";
        let table = rewriter.extract_source_table(sql);
        assert_eq!(table, Some("mydb.default.cpu".to_string()));

        let sql = "SELECT avg(value) FROM metrics GROUP BY host";
        let table = rewriter.extract_source_table(sql);
        assert_eq!(table, Some("metrics".to_string()));
    }

    #[test]
    fn test_extract_time_bucket() {
        let rewriter = ContinuousAggregateRewriter::new(create_test_storage());

        let sql = "SELECT time_bucket(time, 3600000000000, 0) as hour FROM cpu";
        let bucket = rewriter.extract_time_bucket(sql);
        assert_eq!(bucket, Some(3600000000000));

        let sql = "SELECT * FROM cpu";
        let bucket = rewriter.extract_time_bucket(sql);
        assert_eq!(bucket, None);
    }

    #[test]
    fn test_extract_aggregates() {
        let rewriter = ContinuousAggregateRewriter::new(create_test_storage());

        let sql = "SELECT avg(cpu), max(memory), count(*) FROM metrics";
        let aggs = rewriter.extract_aggregates(sql);

        assert_eq!(aggs.len(), 3);
        assert!(aggs
            .iter()
            .any(|(f, c)| *f == AggregateFunction::Avg && c == "cpu"));
        assert!(aggs
            .iter()
            .any(|(f, c)| *f == AggregateFunction::Max && c == "memory"));
        assert!(aggs
            .iter()
            .any(|(f, c)| *f == AggregateFunction::Count && c == "*"));
    }

    #[test]
    fn test_extract_group_by() {
        let rewriter = ContinuousAggregateRewriter::new(create_test_storage());

        let sql = "SELECT * FROM cpu GROUP BY host, region ORDER BY time";
        let groups = rewriter.extract_group_by(sql);

        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"host".to_string()));
        assert!(groups.contains(&"region".to_string()));
    }

    #[tokio::test]
    async fn test_try_rewrite_no_match() {
        let storage = create_test_storage();
        let rewriter = ContinuousAggregateRewriter::new(storage);

        let sql = "SELECT * FROM unknown_table";
        let result = rewriter.try_rewrite(sql).await;

        assert!(result.is_none());
    }
}
