//! Custom Query Executor
//!
//! Provides a custom query execution pipeline using DataFusion's native traits:
//! - `AnalyzerRule` for semantic analysis
//! - `OptimizerRule` for logical optimization
//! - `PhysicalOptimizerRule` for physical optimization
//!
//! This integrates with the main query path via SessionState.

use crate::query::analyzers::default_analyzer_rules;
use crate::query::logical_optimizers::default_optimizer_rules;
use crate::query::physical_optimizers::default_physical_optimizer_rules;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

/// Create a SessionState with custom analyzer and optimizer rules
pub fn create_custom_session_state(base_state: SessionState) -> SessionState {
    // Get custom rules
    let custom_analyzer_rules = default_analyzer_rules();
    let custom_optimizer_rules = default_optimizer_rules();
    let custom_physical_rules = default_physical_optimizer_rules();

    // Build new session state with custom rules prepended to existing rules
    let builder = SessionStateBuilder::new_from_existing(base_state);

    // Add custom analyzer rules
    let builder = custom_analyzer_rules
        .into_iter()
        .fold(builder, |b, rule| b.with_analyzer_rule(rule));

    // Add custom optimizer rules
    let builder = custom_optimizer_rules
        .into_iter()
        .fold(builder, |b, rule| b.with_optimizer_rule(rule));

    // Add custom physical optimizer rules
    let builder = custom_physical_rules
        .into_iter()
        .fold(builder, |b, rule| b.with_physical_optimizer_rule(rule));

    builder.build()
}

/// Custom query executor with integrated DataFusion pipeline
pub struct CustomQueryExecutor {
    ctx: SessionContext,
}

impl CustomQueryExecutor {
    /// Create a new executor with custom rules integrated into SessionContext
    pub fn new(base_ctx: SessionContext) -> Self {
        let custom_state = create_custom_session_state(base_ctx.state());
        let ctx = SessionContext::new_with_state(custom_state);
        Self { ctx }
    }

    /// Create from an existing SessionContext without modifying it
    pub fn from_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    /// Get the underlying session context
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Execute SQL query and collect all results
    ///
    /// This uses the full DataFusion pipeline with custom rules:
    /// 1. Parse SQL
    /// 2. Run custom + builtin analyzer rules
    /// 3. Run custom + builtin logical optimizer rules
    /// 4. Create physical plan
    /// 5. Run custom + builtin physical optimizer rules
    /// 6. Execute and collect results
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        tracing::debug!("Executing SQL: {}", sql);

        // Use DataFrame API which runs through full optimization pipeline
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;

        tracing::debug!("Query complete, {} batches returned", batches.len());
        Ok(batches)
    }

    /// Execute SQL and return a stream (for large result sets)
    pub async fn execute_sql_stream(
        &self,
        sql: &str,
    ) -> Result<impl futures::Stream<Item = Result<RecordBatch>>> {
        let df = self.ctx.sql(sql).await?;
        df.execute_stream().await
    }

    /// Get the execution plan for a query (for EXPLAIN)
    pub async fn explain(&self, sql: &str, verbose: bool) -> Result<String> {
        let df = self.ctx.sql(sql).await?;
        let logical_plan = df.logical_plan().clone();
        let physical_plan = df.create_physical_plan().await?;

        if verbose {
            Ok(format!(
                "Logical Plan:\n{}\n\nPhysical Plan:\n{}",
                logical_plan.display_indent(),
                datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
            ))
        } else {
            Ok(format!(
                "{}",
                datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
            ))
        }
    }

    /// Execute with explicit state control (advanced usage)
    pub async fn execute_with_state(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let state = self.ctx.state();

        // Parse
        let logical_plan = state.create_logical_plan(sql).await?;
        tracing::debug!("Parsed logical plan");

        // Analyze (custom + builtin rules)
        let analyzed_plan = state.analyzer().execute_and_check(
            logical_plan,
            state.config_options(),
            |_plan, rule| {
                tracing::trace!("Analyzer rule '{}' applied", rule.name());
            },
        )?;
        tracing::debug!("Analysis complete");

        // Logical optimize (custom + builtin rules)
        let optimized_plan = state
            .optimizer()
            .optimize(analyzed_plan, &state, |_plan, rule| {
                tracing::trace!("Optimizer rule '{}' applied", rule.name());
            })?;
        tracing::debug!("Logical optimization complete");

        // Physical plan
        let physical_plan = state.create_physical_plan(&optimized_plan).await?;
        tracing::debug!("Physical plan created");

        // Physical optimize (custom + builtin rules)
        let physical_plan =
            state
                .physical_optimizers()
                .iter()
                .try_fold(physical_plan, |plan, optimizer| {
                    let result = optimizer.optimize(plan, state.config_options())?;
                    tracing::trace!("Physical optimizer '{}' applied", optimizer.name());
                    Ok(result) as Result<_>
                })?;
        tracing::debug!("Physical optimization complete");

        // Execute
        let task_ctx = state.task_ctx();
        let mut stream = execute_stream(physical_plan, task_ctx)?;

        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result?);
        }

        tracing::debug!("Execution complete, {} batches", batches.len());
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_executor_creation() {
        let ctx = SessionContext::new();
        let executor = CustomQueryExecutor::new(ctx);
        // Just verify it creates without error
        assert!(
            executor
                .context()
                .state()
                .config_options()
                .execution
                .batch_size
                > 0
        );
    }

    #[tokio::test]
    async fn test_from_context() {
        let ctx = SessionContext::new();
        let executor = CustomQueryExecutor::from_context(ctx);
        assert!(
            executor
                .context()
                .state()
                .config_options()
                .execution
                .batch_size
                > 0
        );
    }
}
