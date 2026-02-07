//! Custom Analyzer Rules
//!
//! Implements DataFusion's `AnalyzerRule` trait for semantic analysis.
//! These rules run after parsing and before optimization.

pub mod downsampling_rewriter;
mod timestamp_to_date;

pub use downsampling_rewriter::DownsamplingRewriter;
pub use timestamp_to_date::TimestampToDateAnalyzer;

use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

/// Get default custom analyzer rules
pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![Arc::new(TimestampToDateAnalyzer::new())]
}

/// Get custom analyzer rules with downsampling rewriter
pub fn analyzer_rules_with_downsampling(
    downsampling_storage: Arc<crate::jobs::downsampling::DownsamplingStorage>,
) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![
        Arc::new(TimestampToDateAnalyzer::new()),
        Arc::new(DownsamplingRewriter::new(downsampling_storage)),
    ]
}
