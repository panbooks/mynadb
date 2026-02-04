//! Query result aggregation
//!
//! Handles aggregation of partial results from distributed query execution.
//!
//! For distributed queries with GROUP BY, ORDER BY, or LIMIT, we need to
//! re-aggregate the partial results from each Data node.

use super::types::DistributedQueryPlan;
use super::QueryCoordinator;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::{debug, info, warn};

impl QueryCoordinator {
    /// Aggregate partial results if needed
    ///
    /// For distributed queries with GROUP BY, ORDER BY, or LIMIT, we need to
    /// re-aggregate the partial results from each Data node.
    pub(super) async fn aggregate_results(
        &self,
        plan: &DistributedQueryPlan,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, String> {
        if batches.is_empty() {
            return Ok(batches);
        }

        // If no aggregation needed, just return concatenated results
        if !plan.needs_aggregation {
            return Ok(batches);
        }

        // Get the schema from the first batch
        let schema = batches[0].schema();
        let batch_count = batches.len();

        // Create a temporary in-memory table with the partial results
        let mem_table = MemTable::try_new(schema.clone(), vec![batches])
            .map_err(|e| format!("Failed to create memory table: {}", e))?;

        // Create a session context for the final aggregation
        let ctx = SessionContext::new();
        ctx.register_table("partial_results", Arc::new(mem_table))
            .map_err(|e| format!("Failed to register table: {}", e))?;

        // Build the final aggregation query based on the original SQL
        let final_sql = self.build_final_aggregation_sql(&plan.sql);

        debug!("Executing final aggregation: {}", final_sql);

        // Execute the final aggregation
        let df = ctx
            .sql(&final_sql)
            .await
            .map_err(|e| format!("Failed to parse final aggregation SQL: {}", e))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| format!("Failed to execute final aggregation: {}", e))?;

        info!(
            "Aggregated {} partial batches into {} result batches",
            batch_count,
            result_batches.len()
        );

        Ok(result_batches)
    }

    /// Build SQL for final aggregation of partial results
    ///
    /// This transforms the original SQL to work on the "partial_results" table.
    /// For simple ORDER BY/LIMIT, we just apply them to the partial results.
    /// For GROUP BY with aggregates, we re-aggregate the partial results.
    pub(super) fn build_final_aggregation_sql(&self, original_sql: &str) -> String {
        let sql_upper = original_sql.to_uppercase();

        // Check if this is a simple ORDER BY / LIMIT query (no aggregation)
        let has_group_by = sql_upper.contains("GROUP BY");
        let has_order_by = sql_upper.contains("ORDER BY");
        let has_limit = sql_upper.contains("LIMIT");

        if !has_group_by && (has_order_by || has_limit) {
            // Simple case: just apply ORDER BY and/or LIMIT to partial results
            let mut final_sql = "SELECT * FROM partial_results".to_string();

            // Extract and append ORDER BY clause
            if let Some(order_pos) = sql_upper.find("ORDER BY") {
                let order_clause = &original_sql[order_pos..];
                // Find where ORDER BY ends (at LIMIT or end of string)
                let end_pos = order_clause
                    .to_uppercase()
                    .find("LIMIT")
                    .unwrap_or(order_clause.len());
                final_sql.push(' ');
                final_sql.push_str(&order_clause[..end_pos].trim());
            }

            // Extract and append LIMIT clause
            if let Some(limit_pos) = sql_upper.find("LIMIT") {
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }

            return final_sql;
        }

        // For GROUP BY queries, we need to re-aggregate
        // This is a simplified approach - a full implementation would parse
        // the SELECT clause and rebuild proper aggregations
        if has_group_by {
            // For now, just apply ORDER BY and LIMIT if present
            // The partial results already have the GROUP BY applied
            let mut final_sql = "SELECT * FROM partial_results".to_string();

            if let Some(order_pos) = sql_upper.find("ORDER BY") {
                let order_clause = &original_sql[order_pos..];
                let end_pos = order_clause
                    .to_uppercase()
                    .find("LIMIT")
                    .unwrap_or(order_clause.len());
                final_sql.push(' ');
                final_sql.push_str(&order_clause[..end_pos].trim());
            }

            if let Some(limit_pos) = sql_upper.find("LIMIT") {
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }

            warn!(
                "GROUP BY re-aggregation not fully implemented. \
                 Results may be incorrect for queries with SUM/COUNT/AVG across partitions."
            );

            return final_sql;
        }

        // Default: just select all from partial results
        "SELECT * FROM partial_results".to_string()
    }
}
