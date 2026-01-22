//! Partition Filter Pushdown Optimizer Rule
//!
//! This optimizer ensures partition filters (like date predicates) are pushed
//! down as close to the table scan as possible, enabling partition pruning.
//!
//! Implements DataFusion's `OptimizerRule` trait for native integration.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{BinaryExpr, Expr, Filter, LogicalPlan, Operator, TableScan};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::OptimizerRule;

/// Logical optimizer that pushes partition filters down to table scans
#[derive(Debug, Default)]
pub struct PartitionFilterPushdown {
    /// Partition column names to look for
    partition_columns: Vec<String>,
}

impl PartitionFilterPushdown {
    pub fn new() -> Self {
        Self {
            partition_columns: vec!["date".to_string()],
        }
    }

    pub fn with_columns(columns: Vec<String>) -> Self {
        Self {
            partition_columns: columns,
        }
    }

    /// Check if an expression references a partition column
    fn is_partition_filter(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    self.expr_references_partition_column(left)
                        || self.expr_references_partition_column(right)
                }
                Operator::And => self.is_partition_filter(left) || self.is_partition_filter(right),
                Operator::Or => self.is_partition_filter(left) && self.is_partition_filter(right),
                _ => false,
            },
            Expr::Between(between) => self.expr_references_partition_column(&between.expr),
            Expr::InList(in_list) => self.expr_references_partition_column(&in_list.expr),
            _ => false,
        }
    }

    fn expr_references_partition_column(&self, expr: &Expr) -> bool {
        if let Expr::Column(col) = expr {
            return self.partition_columns.contains(&col.name);
        }
        false
    }

    fn extract_partition_filters(&self, expr: &Expr) -> (Option<Expr>, Option<Expr>) {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                let (left_part, left_non_part) = self.extract_partition_filters(left);
                let (right_part, right_non_part) = self.extract_partition_filters(right);

                let partition_filter = match (left_part, right_part) {
                    (Some(l), Some(r)) => Some(l.and(r)),
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                };

                let non_partition_filter = match (left_non_part, right_non_part) {
                    (Some(l), Some(r)) => Some(l.and(r)),
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                };

                (partition_filter, non_partition_filter)
            }
            _ => {
                if self.is_partition_filter(expr) {
                    (Some(expr.clone()), None)
                } else {
                    (None, Some(expr.clone()))
                }
            }
        }
    }

    fn push_to_table_scan(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        if let LogicalPlan::Filter(filter) = plan {
            if let LogicalPlan::TableScan(scan) = filter.input.as_ref() {
                let (partition_filters, non_partition_filters) =
                    self.extract_partition_filters(&filter.predicate);

                if let Some(part_filter) = partition_filters {
                    let new_scan_filters = match &scan.filters {
                        filters if !filters.is_empty() => {
                            let mut new_filters = filters.clone();
                            new_filters.push(part_filter);
                            new_filters
                        }
                        _ => vec![part_filter],
                    };

                    let new_scan = TableScan {
                        table_name: scan.table_name.clone(),
                        source: scan.source.clone(),
                        projection: scan.projection.clone(),
                        projected_schema: scan.projected_schema.clone(),
                        filters: new_scan_filters,
                        fetch: scan.fetch,
                    };

                    let new_scan_plan = LogicalPlan::TableScan(new_scan);

                    if let Some(remaining) = non_partition_filters {
                        if let Ok(new_filter) =
                            Filter::try_new(remaining, std::sync::Arc::new(new_scan_plan))
                        {
                            return Some(LogicalPlan::Filter(new_filter));
                        }
                    } else {
                        return Some(new_scan_plan);
                    }
                }
            }
        }
        None
    }
}

impl OptimizerRule for PartitionFilterPushdown {
    fn name(&self) -> &str {
        "partition_filter_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let Some(optimized) = self.push_to_table_scan(&plan) {
            tracing::debug!("Pushed partition filter to table scan");
            return Ok(Transformed::yes(optimized));
        }
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn test_is_partition_filter() {
        let optimizer = PartitionFilterPushdown::new();
        let expr = col("date").eq(lit("2024-01-01"));
        assert!(optimizer.is_partition_filter(&expr));

        let expr = col("host").eq(lit("server01"));
        assert!(!optimizer.is_partition_filter(&expr));
    }

    #[test]
    fn test_optimizer_name() {
        let optimizer = PartitionFilterPushdown::new();
        assert_eq!(optimizer.name(), "partition_filter_pushdown");
    }
}
