//! Custom Analyzer Rules
//!
//! Implements DataFusion's `AnalyzerRule` trait for semantic analysis.
//! These rules run after parsing and before optimization.

mod timestamp_to_date;

pub use timestamp_to_date::TimestampToDateAnalyzer;

use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

/// Get default custom analyzer rules
pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![Arc::new(TimestampToDateAnalyzer::new())]
}
