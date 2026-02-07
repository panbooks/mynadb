//! Query result aggregation
//!
//! Handles aggregation of partial results from distributed query execution.
//!
//! For distributed queries with GROUP BY, ORDER BY, or LIMIT, we need to
//! re-aggregate the partial results from each Data node.
//!
//! ## Global Aggregates (COUNT, SUM, AVG without GROUP BY)
//!
//! When each node returns partial aggregates, we need to re-aggregate:
//! - COUNT(*) → SUM(cnt) - sum all partial counts
//! - SUM(col) → SUM(sum_col) - sum all partial sums
//! - MIN(col) → MIN(min_col) - minimum of all partial mins
//! - MAX(col) → MAX(max_col) - maximum of all partial maxes
//! - AVG is tricky: need SUM/COUNT, not AVG of AVGs

use super::types::DistributedQueryPlan;
use super::QueryCoordinator;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::{debug, info};

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

        // Build the final aggregation query based on the original SQL and partial results schema
        let final_sql = self.build_final_aggregation_sql(&plan.sql, &schema);

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
    /// For global aggregates (COUNT/SUM without GROUP BY), we re-aggregate properly.
    pub(super) fn build_final_aggregation_sql(&self, original_sql: &str, schema: &SchemaRef) -> String {
        let sql_upper = original_sql.to_uppercase();

        // Check query characteristics
        let has_group_by = sql_upper.contains("GROUP BY");
        let has_order_by = sql_upper.contains("ORDER BY");
        let has_limit = sql_upper.contains("LIMIT");
        let has_global_aggregate = self.has_global_aggregate(&sql_upper);

        // Handle global aggregates (COUNT, SUM, etc. without GROUP BY)
        // Each node returns partial aggregates that need to be combined
        if has_global_aggregate && !has_group_by {
            return self.build_global_aggregate_sql(&sql_upper, original_sql, schema);
        }

        // Simple case: ORDER BY / LIMIT without aggregation
        if !has_group_by && (has_order_by || has_limit) {
            let mut final_sql = "SELECT * FROM partial_results".to_string();

            // Extract and append ORDER BY clause
            if let Some(order_pos) = sql_upper.find("ORDER BY") {
                let order_clause = &original_sql[order_pos..];
                let end_pos = order_clause
                    .to_uppercase()
                    .find("LIMIT")
                    .unwrap_or(order_clause.len());
                final_sql.push(' ');
                final_sql.push_str(order_clause[..end_pos].trim());
            }

            // Extract and append LIMIT clause
            if let Some(limit_pos) = sql_upper.find("LIMIT") {
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }

            return final_sql;
        }

        // For GROUP BY queries, re-aggregate partial results
        if has_group_by {
            return self.build_group_by_reaggregate_sql(&sql_upper, original_sql, schema);
        }

        // Default: just select all from partial results
        "SELECT * FROM partial_results".to_string()
    }

    /// Check if SQL has global aggregates (COUNT/SUM/AVG/MIN/MAX without GROUP BY)
    fn has_global_aggregate(&self, sql_upper: &str) -> bool {
        // Simple pattern matching for aggregate functions
        let aggregate_patterns = [
            " COUNT(", "COUNT(*", ",COUNT(",
            " SUM(", ",SUM(",
            " AVG(", ",AVG(",
            " MIN(", ",MIN(",
            " MAX(", ",MAX(",
        ];

        aggregate_patterns.iter().any(|p| sql_upper.contains(p))
    }

    /// Build re-aggregation SQL for global aggregates (no GROUP BY)
    ///
    /// Uses the actual schema from partial results to determine column names
    /// and appropriate re-aggregation functions. This handles cases like
    /// `SELECT COUNT(*) FROM cpu` where DataFusion names the column `count(*)`.
    fn build_global_aggregate_sql(&self, _sql_upper: &str, _original_sql: &str, schema: &SchemaRef) -> String {
        let mut select_parts = Vec::new();

        for field in schema.fields() {
            let col_name = field.name();
            let col_lower = col_name.to_lowercase();

            // Determine re-aggregation function based on column name
            let reagg_func = if col_lower.contains("count") {
                "SUM" // SUM of partial counts
            } else if col_lower.contains("sum") {
                "SUM" // SUM of partial sums
            } else if col_lower.contains("min") {
                "MIN" // MIN of partial mins
            } else if col_lower.contains("max") {
                "MAX" // MAX of partial maxes
            } else if col_lower.contains("avg") {
                "AVG" // AVG of AVGs (approximation)
            } else {
                "SUM" // Safe default for aggregates
            };

            // Quote column names to handle names like "count(*)" with special chars
            select_parts.push(format!("{}(\"{}\") as \"{}\"", reagg_func, col_name, col_name));
        }

        if select_parts.is_empty() {
            return "SELECT * FROM partial_results".to_string();
        }

        format!("SELECT {} FROM partial_results", select_parts.join(", "))
    }

    /// Build re-aggregation SQL for GROUP BY queries
    ///
    /// For GROUP BY queries, each node returns partial aggregates per group.
    /// We need to re-aggregate across nodes for each group.
    fn build_group_by_reaggregate_sql(&self, sql_upper: &str, original_sql: &str, schema: &SchemaRef) -> String {
        // Extract GROUP BY columns
        let group_by_cols = self.extract_group_by_columns(sql_upper, original_sql);

        // Extract aggregate columns from schema (excludes GROUP BY columns)
        let agg_cols = self.extract_aggregate_columns_from_schema(schema, &group_by_cols);

        if group_by_cols.is_empty() || agg_cols.is_empty() {
            // Fallback: just concatenate with ORDER BY/LIMIT
            let mut final_sql = "SELECT * FROM partial_results".to_string();

            if let Some(order_pos) = sql_upper.find("ORDER BY") {
                let order_clause = &original_sql[order_pos..];
                let end_pos = order_clause
                    .to_uppercase()
                    .find("LIMIT")
                    .unwrap_or(order_clause.len());
                final_sql.push(' ');
                final_sql.push_str(order_clause[..end_pos].trim());
            }

            if let Some(limit_pos) = sql_upper.find("LIMIT") {
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }

            return final_sql;
        }

        // Build SELECT clause with proper re-aggregation
        let mut select_parts: Vec<String> = group_by_cols.iter().map(|c| c.to_string()).collect();

        for (col_name, reagg_func) in &agg_cols {
            select_parts.push(format!("{}(\"{}\") as \"{}\"", reagg_func, col_name, col_name));
        }

        let mut final_sql = format!(
            "SELECT {} FROM partial_results GROUP BY {}",
            select_parts.join(", "),
            group_by_cols.join(", ")
        );

        // Add ORDER BY if present
        if let Some(order_pos) = sql_upper.find("ORDER BY") {
            let order_clause = &original_sql[order_pos..];
            let end_pos = order_clause
                .to_uppercase()
                .find("LIMIT")
                .unwrap_or(order_clause.len());
            final_sql.push(' ');
            final_sql.push_str(order_clause[..end_pos].trim());
        }

        // Add LIMIT if present
        if let Some(limit_pos) = sql_upper.find("LIMIT") {
            let limit_clause = &original_sql[limit_pos..];
            final_sql.push(' ');
            final_sql.push_str(limit_clause.trim());
        }

        debug!("GROUP BY re-aggregation SQL: {}", final_sql);
        final_sql
    }

    /// Extract GROUP BY column names from SQL
    fn extract_group_by_columns(&self, sql_upper: &str, original_sql: &str) -> Vec<String> {
        if let Some(group_by_pos) = sql_upper.find("GROUP BY") {
            let after_group_by = &original_sql[group_by_pos + 9..]; // Skip "GROUP BY "

            // Find where GROUP BY clause ends
            let end_markers = ["ORDER BY", "LIMIT", "HAVING", ";"];
            let mut end_pos = after_group_by.len();
            for marker in &end_markers {
                if let Some(pos) = after_group_by.to_uppercase().find(marker) {
                    end_pos = end_pos.min(pos);
                }
            }

            let group_cols = &after_group_by[..end_pos];
            return group_cols
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        Vec::new()
    }

    /// Extract aggregate columns from schema, excluding GROUP BY columns
    /// Returns: Vec<(column_name, reagg_function)>
    fn extract_aggregate_columns_from_schema(&self, schema: &SchemaRef, group_by_cols: &[String]) -> Vec<(String, String)> {
        let mut results = Vec::new();

        // Normalize group by column names for comparison
        let group_cols_lower: Vec<String> = group_by_cols.iter()
            .map(|c| c.trim_matches('"').to_lowercase())
            .collect();

        for field in schema.fields() {
            let col_name = field.name();
            let col_lower = col_name.to_lowercase();

            // Skip GROUP BY columns
            if group_cols_lower.contains(&col_lower) {
                continue;
            }

            // Determine re-aggregation function based on column name
            let reagg_func = if col_lower.contains("count") {
                "SUM"
            } else if col_lower.contains("sum") {
                "SUM"
            } else if col_lower.contains("min") {
                "MIN"
            } else if col_lower.contains("max") {
                "MAX"
            } else if col_lower.contains("avg") {
                "AVG"
            } else {
                "SUM"
            };

            results.push((col_name.clone(), reagg_func.to_string()));
        }

        results
    }
}
