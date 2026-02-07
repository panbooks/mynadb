//! Query builder for downsampling aggregation SQL
//!
//! Generates SQL queries that aggregate data using time_bucket and the
//! configured aggregate functions for each tier.

use super::types::{AggregateFn, AggregateSpec, DownsamplingTier};

/// Build a downsampling aggregation query for a specific partition date.
///
/// Produces SQL like:
/// ```sql
/// SELECT time_bucket('1 minute', time) AS time,
///        host,
///        min(usage) AS min_usage, max(usage) AS max_usage, avg(usage) AS avg_usage
/// FROM mydb.default.cpu
/// WHERE date = '2026-01-15'
/// GROUP BY time_bucket('1 minute', time), host
/// ORDER BY time
/// ```
pub fn build_downsample_query(
    source_table: &str,
    tier: &DownsamplingTier,
    partition_date: &str,
    tag_columns: &[String],
) -> String {
    let mut select_parts = Vec::new();
    let mut group_by_parts = Vec::new();

    // Time bucket as first column
    let time_bucket_expr = format!("time_bucket('{}', time)", tier.interval);
    select_parts.push(format!("{} AS time", time_bucket_expr));
    group_by_parts.push(time_bucket_expr);

    // Tag columns (GROUP BY dimensions)
    for tag in tag_columns {
        select_parts.push(tag.clone());
        group_by_parts.push(tag.clone());
    }

    // Aggregate expressions
    for agg in &tier.aggregates {
        let expr = format_aggregate(agg);
        select_parts.push(expr);
    }

    format!(
        "SELECT {} FROM {} WHERE date = '{}' GROUP BY {} ORDER BY time",
        select_parts.join(", "),
        source_table,
        partition_date,
        group_by_parts.join(", "),
    )
}

/// Build a query to discover tag columns (non-numeric, non-time columns)
/// for a given table.
pub fn build_tag_discovery_query(source_table: &str) -> String {
    format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = '{}' \
         AND data_type IN ('Utf8', 'LargeUtf8', 'Dictionary(Int32, Utf8)') \
         AND column_name != 'time' AND column_name != 'date'",
        source_table.rsplit('.').next().unwrap_or(source_table)
    )
}

/// Format a single aggregate expression with alias
fn format_aggregate(agg: &AggregateSpec) -> String {
    let alias = agg.function.output_alias(&agg.column);
    match agg.function {
        AggregateFn::Count => format!("count(*) AS {}", alias),
        _ => format!(
            "{}({}) AS {}",
            agg.function.sql_name(),
            agg.column,
            alias
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_downsample_query_basic() {
        let tier = DownsamplingTier {
            name: "1m".to_string(),
            after_days: 7,
            interval: "1 minute".to_string(),
            aggregates: vec![
                AggregateSpec {
                    function: AggregateFn::Min,
                    column: "usage".to_string(),
                },
                AggregateSpec {
                    function: AggregateFn::Max,
                    column: "usage".to_string(),
                },
                AggregateSpec {
                    function: AggregateFn::Avg,
                    column: "usage".to_string(),
                },
            ],
            retention_days: None,
        };

        let sql = build_downsample_query(
            "mydb.default.cpu",
            &tier,
            "2026-01-15",
            &["host".to_string()],
        );

        assert!(sql.contains("time_bucket('1 minute', time) AS time"));
        assert!(sql.contains("host"));
        assert!(sql.contains("min(usage) AS min_usage"));
        assert!(sql.contains("max(usage) AS max_usage"));
        assert!(sql.contains("avg(usage) AS avg_usage"));
        assert!(sql.contains("FROM mydb.default.cpu"));
        assert!(sql.contains("WHERE date = '2026-01-15'"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("ORDER BY time"));
    }

    #[test]
    fn test_build_downsample_query_no_tags() {
        let tier = DownsamplingTier {
            name: "1h".to_string(),
            after_days: 30,
            interval: "1 hour".to_string(),
            aggregates: vec![AggregateSpec {
                function: AggregateFn::Avg,
                column: "value".to_string(),
            }],
            retention_days: Some(365),
        };

        let sql = build_downsample_query("mydb.default.temp", &tier, "2026-01-01", &[]);

        assert!(sql.contains("time_bucket('1 hour', time) AS time"));
        assert!(sql.contains("avg(value) AS avg_value"));
        assert!(sql.contains("FROM mydb.default.temp"));
        // GROUP BY should only have time_bucket
        assert!(sql.contains("GROUP BY time_bucket('1 hour', time)"));
    }

    #[test]
    fn test_build_downsample_query_with_count() {
        let tier = DownsamplingTier {
            name: "1d".to_string(),
            after_days: 90,
            interval: "1 day".to_string(),
            aggregates: vec![
                AggregateSpec {
                    function: AggregateFn::Avg,
                    column: "usage".to_string(),
                },
                AggregateSpec {
                    function: AggregateFn::Count,
                    column: "*".to_string(),
                },
            ],
            retention_days: None,
        };

        let sql = build_downsample_query("mydb.default.cpu", &tier, "2025-10-01", &[]);

        assert!(sql.contains("count(*) AS count"));
        assert!(sql.contains("avg(usage) AS avg_usage"));
    }

    #[test]
    fn test_build_downsample_query_multiple_tags() {
        let tier = DownsamplingTier {
            name: "1m".to_string(),
            after_days: 7,
            interval: "1 minute".to_string(),
            aggregates: vec![AggregateSpec {
                function: AggregateFn::Avg,
                column: "value".to_string(),
            }],
            retention_days: None,
        };

        let sql = build_downsample_query(
            "mydb.default.metrics",
            &tier,
            "2026-01-15",
            &["host".to_string(), "region".to_string(), "service".to_string()],
        );

        assert!(sql.contains("host, region, service"));
        assert!(sql.contains("GROUP BY time_bucket('1 minute', time), host, region, service"));
    }

    #[test]
    fn test_format_aggregate() {
        let agg = AggregateSpec {
            function: AggregateFn::Min,
            column: "cpu".to_string(),
        };
        assert_eq!(format_aggregate(&agg), "min(cpu) AS min_cpu");

        let agg = AggregateSpec {
            function: AggregateFn::Count,
            column: "*".to_string(),
        };
        assert_eq!(format_aggregate(&agg), "count(*) AS count");

        let agg = AggregateSpec {
            function: AggregateFn::First,
            column: "name".to_string(),
        };
        assert_eq!(format_aggregate(&agg), "first_value(name) AS first_value_name");
    }
}
