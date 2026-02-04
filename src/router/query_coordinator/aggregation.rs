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
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use regex::Regex;
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
    /// For global aggregates (COUNT/SUM without GROUP BY), we re-aggregate properly.
    pub(super) fn build_final_aggregation_sql(&self, original_sql: &str) -> String {
        let sql_upper = original_sql.to_uppercase();

        // Check query characteristics
        let has_group_by = sql_upper.contains("GROUP BY");
        let has_order_by = sql_upper.contains("ORDER BY");
        let has_limit = sql_upper.contains("LIMIT");
        let has_global_aggregate = self.has_global_aggregate(&sql_upper);

        // Handle global aggregates (COUNT, SUM, etc. without GROUP BY)
        // Each node returns partial aggregates that need to be combined
        if has_global_aggregate && !has_group_by {
            return self.build_global_aggregate_sql(&sql_upper, original_sql);
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
            return self.build_group_by_reaggregate_sql(&sql_upper, original_sql);
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
    /// For partial results like:
    ///   | cnt |     from node-1
    ///   | 50  |
    ///   | cnt |     from node-2
    ///   | 30  |
    ///
    /// We build: SELECT SUM(cnt) as cnt FROM partial_results
    fn build_global_aggregate_sql(&self, sql_upper: &str, _original_sql: &str) -> String {
        // Extract column names from partial results schema
        // For global aggregates, we need to re-aggregate each column appropriately
        //
        // Strategy:
        // - COUNT columns → SUM
        // - SUM columns → SUM
        // - MIN columns → MIN
        // - MAX columns → MAX
        // - AVG columns → tricky (would need sum/count), just use AVG of partial AVGs as approximation

        let mut select_parts = Vec::new();

        // Parse SELECT clause to find aggregate columns and their aliases
        // Pattern: SELECT COUNT(*) as cnt, SUM(value) as total FROM ...
        if let Some(select_end) = sql_upper.find(" FROM ") {
            let select_clause = &sql_upper[7..select_end]; // Skip "SELECT "

            // Use regex to find aggregate patterns
            // Match: COUNT(*) as alias, SUM(col) as alias, etc.
            let agg_re = Regex::new(
                r"(?i)(COUNT|SUM|AVG|MIN|MAX)\s*\([^)]*\)\s*(?:AS\s+)?(\w+)"
            ).unwrap();

            for cap in agg_re.captures_iter(select_clause) {
                let func = cap.get(1).map(|m| m.as_str().to_uppercase()).unwrap_or_default();
                let alias = cap.get(2).map(|m| m.as_str()).unwrap_or("col");

                // Choose re-aggregation function
                let reagg_func = match func.as_str() {
                    "COUNT" => "SUM",  // SUM of partial counts
                    "SUM" => "SUM",    // SUM of partial sums
                    "MIN" => "MIN",    // MIN of partial mins
                    "MAX" => "MAX",    // MAX of partial maxes
                    "AVG" => "AVG",    // AVG of AVGs (approximation)
                    _ => "SUM",
                };

                select_parts.push(format!("{}({}) as {}", reagg_func, alias, alias));
            }
        }

        if select_parts.is_empty() {
            // Fallback: if we couldn't parse, try simple column-based approach
            // Assume columns named like "cnt", "count", "sum", etc.
            return "SELECT SUM(cnt) as cnt FROM partial_results".to_string();
        }

        format!("SELECT {} FROM partial_results", select_parts.join(", "))
    }

    /// Build re-aggregation SQL for GROUP BY queries
    ///
    /// For GROUP BY queries, each node returns partial aggregates per group.
    /// We need to re-aggregate across nodes for each group.
    fn build_group_by_reaggregate_sql(&self, sql_upper: &str, original_sql: &str) -> String {
        // Extract GROUP BY columns
        let group_by_cols = self.extract_group_by_columns(sql_upper, original_sql);

        // Extract aggregate columns and their types
        let agg_cols = self.extract_aggregate_columns(sql_upper);

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

        for (alias, func) in &agg_cols {
            let reagg_func = match func.as_str() {
                "COUNT" => "SUM",
                "SUM" => "SUM",
                "MIN" => "MIN",
                "MAX" => "MAX",
                "AVG" => "AVG",  // Approximation
                _ => "SUM",
            };
            select_parts.push(format!("{}({}) as {}", reagg_func, alias, alias));
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

    /// Extract aggregate columns and their functions from SQL
    /// Returns: Vec<(alias, function)>
    fn extract_aggregate_columns(&self, sql_upper: &str) -> Vec<(String, String)> {
        let mut results = Vec::new();

        let agg_re = Regex::new(
            r"(?i)(COUNT|SUM|AVG|MIN|MAX)\s*\([^)]*\)\s*(?:AS\s+)?(\w+)"
        ).unwrap();

        if let Some(from_pos) = sql_upper.find(" FROM ") {
            let select_clause = &sql_upper[7..from_pos]; // Skip "SELECT "

            for cap in agg_re.captures_iter(select_clause) {
                let func = cap.get(1).map(|m| m.as_str().to_uppercase()).unwrap_or_default();
                let alias = cap.get(2).map(|m| m.as_str().to_string()).unwrap_or_default();
                if !alias.is_empty() {
                    results.push((alias, func));
                }
            }
        }

        results
    }
}
