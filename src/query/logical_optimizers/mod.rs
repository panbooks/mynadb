//! Custom Logical Optimizer Rules
//!
//! Implements DataFusion's `OptimizerRule` trait for logical plan optimization.
//! These rules run after analysis and before physical planning.

mod partition_filter_pushdown;

pub use partition_filter_pushdown::PartitionFilterPushdown;

use datafusion::optimizer::OptimizerRule;
use std::sync::Arc;

/// Get default custom logical optimizer rules
pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![Arc::new(PartitionFilterPushdown::new())]
}
