//! Timestamp to Date Analyzer Rule
//!
//! This analyzer detects timestamp predicates in WHERE clauses and adds
//! corresponding date predicates for partition pruning.
//!
//! Implements DataFusion's `AnalyzerRule` trait for native integration.

use chrono::{DateTime, TimeZone, Utc};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{col, lit, BinaryExpr, Expr, Filter, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

/// Nanoseconds per second
const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Analyzer rule that converts timestamp predicates to date predicates for partition pruning
#[derive(Debug, Default)]
pub struct TimestampToDateAnalyzer {
    /// Column name for timestamp
    timestamp_column: String,
    /// Column name for date partition
    date_column: String,
}

impl TimestampToDateAnalyzer {
    pub fn new() -> Self {
        Self {
            timestamp_column: "timestamp".to_string(),
            date_column: "date".to_string(),
        }
    }

    pub fn with_columns(timestamp_column: &str, date_column: &str) -> Self {
        Self {
            timestamp_column: timestamp_column.to_string(),
            date_column: date_column.to_string(),
        }
    }

    /// Convert nanosecond timestamp to date string
    fn timestamp_to_date(ts_nanos: i64) -> String {
        let secs = ts_nanos / NANOS_PER_SEC;
        DateTime::from_timestamp(secs, 0)
            .unwrap_or_else(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap())
            .format("%Y-%m-%d")
            .to_string()
    }

    /// Try to extract i64 from a ScalarValue
    fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
        match scalar {
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::Int32(Some(v)) => Some(*v as i64),
            ScalarValue::UInt64(Some(v)) => Some(*v as i64),
            ScalarValue::UInt32(Some(v)) => Some(*v as i64),
            ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
            ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v * 1000),
            ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v * 1_000_000),
            ScalarValue::TimestampSecond(Some(v), _) => Some(*v * 1_000_000_000),
            _ => None,
        }
    }

    /// Check if an expression is a timestamp comparison and extract the value
    fn extract_timestamp_comparison(&self, expr: &Expr) -> Option<(Operator, i64)> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
            // Check for: timestamp >= value or timestamp > value
            if let Expr::Column(c) = left.as_ref() {
                if c.name == self.timestamp_column {
                    if let Expr::Literal(scalar) = right.as_ref() {
                        if let Some(val) = Self::scalar_to_i64(scalar) {
                            return Some((*op, val));
                        }
                    }
                }
            }
            // Check for: value <= timestamp or value < timestamp
            if let Expr::Column(c) = right.as_ref() {
                if c.name == self.timestamp_column {
                    if let Expr::Literal(scalar) = left.as_ref() {
                        if let Some(val) = Self::scalar_to_i64(scalar) {
                            let flipped_op = match op {
                                Operator::Lt => Operator::Gt,
                                Operator::LtEq => Operator::GtEq,
                                Operator::Gt => Operator::Lt,
                                Operator::GtEq => Operator::LtEq,
                                _ => return None,
                            };
                            return Some((flipped_op, val));
                        }
                    }
                }
            }
        }
        None
    }

    /// Check if the plan already has a date predicate
    fn has_date_predicate(&self, expr: &Expr) -> bool {
        let mut has_date = false;
        let _ = expr.clone().transform(|e| {
            if let Expr::Column(c) = &e {
                if c.name == self.date_column {
                    has_date = true;
                }
            }
            Ok(Transformed::no(e))
        });
        has_date
    }

    /// Create a date predicate from timestamp comparison
    fn create_date_predicate(&self, op: Operator, ts_value: i64) -> Option<Expr> {
        let date_str = Self::timestamp_to_date(ts_value);

        match op {
            Operator::Gt | Operator::GtEq => Some(col(&self.date_column).gt_eq(lit(date_str))),
            Operator::Lt | Operator::LtEq => Some(col(&self.date_column).lt_eq(lit(date_str))),
            Operator::Eq => Some(col(&self.date_column).eq(lit(date_str))),
            _ => None,
        }
    }

    /// Analyze a filter expression and add date predicates if needed
    fn analyze_filter_expr(&self, expr: &Expr) -> Option<Expr> {
        if self.has_date_predicate(expr) {
            return None;
        }

        let mut date_predicates = Vec::new();

        let _ = expr.clone().transform(|e| {
            if let Some((op, ts_value)) = self.extract_timestamp_comparison(&e) {
                if let Some(date_pred) = self.create_date_predicate(op, ts_value) {
                    date_predicates.push(date_pred);
                }
            }
            Ok(Transformed::no(e))
        });

        if date_predicates.is_empty() {
            return None;
        }

        let combined_date_pred = date_predicates
            .into_iter()
            .reduce(|a, b| a.and(b))
            .unwrap();

        Some(combined_date_pred.and(expr.clone()))
    }

    /// Transform the logical plan
    fn transform_plan(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform(|node| {
            if let LogicalPlan::Filter(filter) = &node {
                if let Some(new_predicate) = self.analyze_filter_expr(&filter.predicate) {
                    let new_filter = Filter::try_new(new_predicate, filter.input.clone())?;
                    return Ok(Transformed::yes(LogicalPlan::Filter(new_filter)));
                }
            }
            Ok(Transformed::no(node))
        })
    }
}

impl AnalyzerRule for TimestampToDateAnalyzer {
    fn name(&self) -> &str {
        "timestamp_to_date"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        self.transform_plan(plan).map(|t| t.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_to_date() {
        let ts = 1704067200000000000i64;
        let date = TimestampToDateAnalyzer::timestamp_to_date(ts);
        assert_eq!(date, "2024-01-01");
    }

    #[test]
    fn test_analyzer_name() {
        let analyzer = TimestampToDateAnalyzer::new();
        assert_eq!(analyzer.name(), "timestamp_to_date");
    }
}
