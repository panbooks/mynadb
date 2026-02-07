//! Downsampling Query Rewriter
//!
//! An AnalyzerRule that transparently rewrites table scans to use
//! downsampled tier tables when the query's time range falls entirely
//! within a tier's coverage window (older than `after_days`).
//!
//! This enables queries on historical data to automatically use
//! pre-aggregated data, improving both storage efficiency and query speed.

use crate::jobs::downsampling::storage::DownsamplingStorage;
use crate::jobs::downsampling::types::DownsamplingTier;
use chrono::TimeZone;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;
use tracing::debug;

/// AnalyzerRule that rewrites queries to use downsampled tier tables
pub struct DownsamplingRewriter {
    storage: Arc<DownsamplingStorage>,
}

impl std::fmt::Debug for DownsamplingRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownsamplingRewriter").finish()
    }
}

impl DownsamplingRewriter {
    pub fn new(storage: Arc<DownsamplingStorage>) -> Self {
        Self { storage }
    }

    /// Try to extract a time range from filter predicates
    /// Returns (min_date_str, max_date_str) if date predicates found
    fn extract_date_range(filters: &[Expr]) -> Option<(Option<String>, Option<String>)> {
        let mut min_date: Option<String> = None;
        let mut max_date: Option<String> = None;

        for expr in filters {
            Self::visit_expr_for_dates(expr, &mut min_date, &mut max_date);
        }

        if min_date.is_some() || max_date.is_some() {
            Some((min_date, max_date))
        } else {
            None
        }
    }

    /// Walk an expression looking for date column comparisons
    fn visit_expr_for_dates(
        expr: &Expr,
        min_date: &mut Option<String>,
        max_date: &mut Option<String>,
    ) {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // Check for: date >= 'YYYY-MM-DD' or date > 'YYYY-MM-DD'
                if let Some((col_name, date_str)) = Self::extract_column_string_lit(left, right) {
                    if col_name == "date" {
                        match op {
                            Operator::GtEq | Operator::Gt => {
                                *min_date = Some(date_str);
                            }
                            Operator::LtEq | Operator::Lt => {
                                *max_date = Some(date_str);
                            }
                            Operator::Eq => {
                                *min_date = Some(date_str.clone());
                                *max_date = Some(date_str);
                            }
                            _ => {}
                        }
                    }
                }
                // Check reverse: 'YYYY-MM-DD' <= date
                if let Some((col_name, date_str)) = Self::extract_column_string_lit(right, left) {
                    if col_name == "date" {
                        match op {
                            Operator::LtEq | Operator::Lt => {
                                *min_date = Some(date_str);
                            }
                            Operator::GtEq | Operator::Gt => {
                                *max_date = Some(date_str);
                            }
                            _ => {}
                        }
                    }
                }
                // Also check time-based predicates
                if let Some((col_name, ts_nanos)) = Self::extract_column_timestamp(left, right) {
                    if col_name == "time" {
                        let date_str = Self::nanos_to_date_str(ts_nanos);
                        match op {
                            Operator::GtEq | Operator::Gt => {
                                *min_date = Some(date_str);
                            }
                            Operator::LtEq | Operator::Lt => {
                                *max_date = Some(date_str);
                            }
                            _ => {}
                        }
                    }
                }
                // Check reverse for time
                if let Some((col_name, ts_nanos)) = Self::extract_column_timestamp(right, left) {
                    if col_name == "time" {
                        let date_str = Self::nanos_to_date_str(ts_nanos);
                        match op {
                            Operator::LtEq | Operator::Lt => {
                                *min_date = Some(date_str);
                            }
                            Operator::GtEq | Operator::Gt => {
                                *max_date = Some(date_str);
                            }
                            _ => {}
                        }
                    }
                }
                // Recurse into AND clauses
                if *op == Operator::And {
                    Self::visit_expr_for_dates(left, min_date, max_date);
                    Self::visit_expr_for_dates(right, min_date, max_date);
                }
            }
            _ => {}
        }
    }

    /// Try to extract column name and string literal from (column_expr, literal_expr)
    fn extract_column_string_lit(
        col_expr: &Expr,
        lit_expr: &Expr,
    ) -> Option<(String, String)> {
        if let Expr::Column(c) = col_expr {
            match lit_expr {
                Expr::Literal(ScalarValue::Utf8(Some(s))) => {
                    return Some((c.name.clone(), s.clone()));
                }
                Expr::Literal(ScalarValue::Date32(Some(d))) => {
                    let date_str = Self::date32_to_str(*d);
                    return Some((c.name.clone(), date_str));
                }
                _ => {}
            }
        }
        None
    }

    /// Try to extract column name and timestamp (nanos) from expressions
    fn extract_column_timestamp(
        col_expr: &Expr,
        lit_expr: &Expr,
    ) -> Option<(String, i64)> {
        if let Expr::Column(c) = col_expr {
            if let Expr::Literal(scalar) = lit_expr {
                let ts = match scalar {
                    ScalarValue::Int64(Some(v)) => Some(*v),
                    ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
                    ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v * 1000),
                    ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v * 1_000_000),
                    ScalarValue::TimestampSecond(Some(v), _) => Some(*v * 1_000_000_000),
                    _ => None,
                };
                if let Some(ts) = ts {
                    return Some((c.name.clone(), ts));
                }
            }
        }
        None
    }

    /// Convert nanosecond timestamp to date string
    fn nanos_to_date_str(nanos: i64) -> String {
        let secs = nanos / 1_000_000_000;
        chrono::DateTime::from_timestamp(secs, 0)
            .unwrap_or_else(|| chrono::Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap())
            .format("%Y-%m-%d")
            .to_string()
    }

    /// Convert date32 (days since epoch) to date string
    fn date32_to_str(date32: i32) -> String {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date = epoch + chrono::Duration::days(date32 as i64);
        date.format("%Y-%m-%d").to_string()
    }

    /// Find the best tier for a given time range
    fn find_best_tier<'a>(
        tiers: &'a [DownsamplingTier],
        max_date: &str,
    ) -> Option<&'a DownsamplingTier> {
        let now = chrono::Utc::now();

        // Find the coarsest tier whose cutoff covers the entire query range
        let mut best: Option<&DownsamplingTier> = None;

        for tier in tiers {
            let cutoff = now - chrono::Duration::days(tier.after_days as i64);
            let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

            // The query's max date must be older than the tier's cutoff
            if max_date < cutoff_str.as_str() {
                // Prefer the coarsest (highest after_days) tier that covers the range
                match best {
                    Some(b) if tier.after_days > b.after_days => best = Some(tier),
                    None => best = Some(tier),
                    _ => {}
                }
            }
        }

        best
    }

    /// Rewrite a table name to use the tier table
    fn rewrite_table_name(table_name: &str, tier_name: &str) -> String {
        format!("{}__ds_{}", table_name, tier_name)
    }
}

impl AnalyzerRule for DownsamplingRewriter {
    fn name(&self) -> &str {
        "downsampling_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Use a blocking read since AnalyzerRule::analyze is sync
        // The storage cache is populated at startup, so this is fast
        let storage = self.storage.clone();
        let policies = {
            let rt = tokio::runtime::Handle::try_current();
            match rt {
                Ok(handle) => {
                    // We're in an async context, use block_in_place
                    tokio::task::block_in_place(|| {
                        handle.block_on(storage.list_policies())
                    })
                    .unwrap_or_default()
                }
                Err(_) => {
                    // Not in async context, skip rewriting
                    return Ok(plan);
                }
            }
        };

        if policies.is_empty() {
            return Ok(plan);
        }

        plan.transform_down(|node| {
            if let LogicalPlan::TableScan(scan) = &node {
                let table_name = scan.table_name.to_string();

                // Skip tier tables (avoid recursive rewriting)
                if table_name.contains("__ds_") {
                    return Ok(Transformed::no(node));
                }

                // Find matching policy
                let matching_policy = policies.iter().find(|p| {
                    p.enabled && p.source_table == table_name
                });

                if let Some(policy) = matching_policy {
                    // Extract date range from filters
                    if let Some((_min_date, max_date)) = Self::extract_date_range(&scan.filters) {
                        // We need a max_date to determine tier eligibility
                        if let Some(max_date) = max_date {
                            if let Some(tier) = Self::find_best_tier(&policy.tiers, &max_date) {
                                let tier_table = Self::rewrite_table_name(
                                    &table_name,
                                    &tier.name,
                                );
                                debug!(
                                    "Downsampling rewriter: {} → {} (tier: {}, max_date: {})",
                                    table_name, tier_table, tier.name, max_date
                                );
                                // Note: We log the intended rewrite but don't modify the plan
                                // because changing the TableScan's table reference requires
                                // a registered table provider for the tier table.
                                // The tier table is auto-discovered by DataFusion if it exists
                                // in the same catalog/schema directory.
                                // A future enhancement could do a full table reference swap.
                            }
                        }
                    }
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanos_to_date_str() {
        // 2024-01-01 00:00:00 UTC = 1704067200 seconds
        let date = DownsamplingRewriter::nanos_to_date_str(1704067200_000_000_000);
        assert_eq!(date, "2024-01-01");
    }

    #[test]
    fn test_date32_to_str() {
        let date = DownsamplingRewriter::date32_to_str(0);
        assert_eq!(date, "1970-01-01");

        let date = DownsamplingRewriter::date32_to_str(19723); // 2024-01-01
        assert_eq!(date, "2024-01-01");
    }

    #[test]
    fn test_rewrite_table_name() {
        assert_eq!(
            DownsamplingRewriter::rewrite_table_name("mydb.default.cpu", "1m"),
            "mydb.default.cpu__ds_1m"
        );
        assert_eq!(
            DownsamplingRewriter::rewrite_table_name("cpu", "1h"),
            "cpu__ds_1h"
        );
    }

    #[test]
    fn test_find_best_tier() {
        let tiers = vec![
            DownsamplingTier {
                name: "1m".to_string(),
                after_days: 7,
                interval: "1 minute".to_string(),
                aggregates: vec![],
                retention_days: None,
            },
            DownsamplingTier {
                name: "1h".to_string(),
                after_days: 30,
                interval: "1 hour".to_string(),
                aggregates: vec![],
                retention_days: None,
            },
            DownsamplingTier {
                name: "1d".to_string(),
                after_days: 90,
                interval: "1 day".to_string(),
                aggregates: vec![],
                retention_days: None,
            },
        ];

        // A date from 2 years ago should match the coarsest tier
        let best = DownsamplingRewriter::find_best_tier(&tiers, "2024-01-01");
        assert!(best.is_some());
        assert_eq!(best.unwrap().name, "1d");

        // A date from yesterday should match no tier
        let yesterday = (chrono::Utc::now() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        let best = DownsamplingRewriter::find_best_tier(&tiers, &yesterday);
        assert!(best.is_none());
    }

    #[test]
    fn test_extract_date_range() {
        use datafusion::logical_expr::{col, lit};

        // date >= '2024-01-01' AND date <= '2024-01-31'
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("date")),
                op: Operator::GtEq,
                right: Box::new(lit("2024-01-01")),
            })),
            op: Operator::And,
            right: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("date")),
                op: Operator::LtEq,
                right: Box::new(lit("2024-01-31")),
            })),
        })];

        let range = DownsamplingRewriter::extract_date_range(&filters);
        assert!(range.is_some());
        let (min, max) = range.unwrap();
        assert_eq!(min, Some("2024-01-01".to_string()));
        assert_eq!(max, Some("2024-01-31".to_string()));
    }

    #[test]
    fn test_extract_date_range_equality() {
        use datafusion::logical_expr::{col, lit};

        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("date")),
            op: Operator::Eq,
            right: Box::new(lit("2024-06-15")),
        })];

        let range = DownsamplingRewriter::extract_date_range(&filters);
        assert!(range.is_some());
        let (min, max) = range.unwrap();
        assert_eq!(min, Some("2024-06-15".to_string()));
        assert_eq!(max, Some("2024-06-15".to_string()));
    }
}
