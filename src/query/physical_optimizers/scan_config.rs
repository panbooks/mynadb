//! Scan Configuration Physical Optimizer
//!
//! This physical optimizer logs and validates scan configurations.
//! Implements DataFusion's `PhysicalOptimizerRule` trait.

use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Physical optimizer that validates and logs scan configurations
#[derive(Debug, Default)]
pub struct ScanConfigOptimizer {
    log_scans: bool,
}

impl ScanConfigOptimizer {
    pub fn new() -> Self {
        Self { log_scans: true }
    }

    pub fn with_logging(log_scans: bool) -> Self {
        Self { log_scans }
    }

    fn analyze_plan(&self, plan: &dyn ExecutionPlan, depth: usize) {
        let indent = "  ".repeat(depth);
        let name = plan.name();

        if name.contains("Parquet") || name.contains("Scan") {
            let props = plan.properties();
            let partitions = props.partitioning.partition_count();
            tracing::debug!(
                "{}[{}] partitions={}, schema_fields={}",
                indent,
                name,
                partitions,
                plan.schema().fields().len()
            );
            tracing::debug!("{}  output_ordering: {:?}", indent, props.output_ordering());
        }

        for child in plan.children() {
            self.analyze_plan(child.as_ref(), depth + 1);
        }
    }
}

impl PhysicalOptimizerRule for ScanConfigOptimizer {
    fn name(&self) -> &str {
        "scan_config_optimizer"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.log_scans {
            tracing::debug!("Physical plan structure:");
            self.analyze_plan(plan.as_ref(), 0);
        }
        Ok(plan)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_creation() {
        let opt = ScanConfigOptimizer::new();
        assert_eq!(opt.name(), "scan_config_optimizer");
    }
}
