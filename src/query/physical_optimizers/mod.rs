//! Custom Physical Optimizer Rules
//!
//! Implements DataFusion's `PhysicalOptimizerRule` trait for execution plan optimization.
//! These rules run after physical planning and before execution.

mod scan_config;

pub use scan_config::ScanConfigOptimizer;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use std::sync::Arc;

/// Get default custom physical optimizer rules
pub fn default_physical_optimizer_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![Arc::new(ScanConfigOptimizer::new())]
}
