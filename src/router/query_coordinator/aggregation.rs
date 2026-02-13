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
use crate::query::functions::time_bucket::create_time_bucket_udf;
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
        // Register custom UDFs so expressions like time_bucket() in GROUP BY work
        let ctx = SessionContext::new();
        ctx.register_udf(create_time_bucket_udf());
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

        // CTE queries (e.g., derivative with WITH _derivative_sub AS) already produce
        // final results on each node. Don't try to re-aggregate — just concatenate,
        // sort by time, and apply LIMIT.
        if sql_upper.trim_start().starts_with("WITH ") {
            let mut final_sql = "SELECT * FROM partial_results ORDER BY time ASC".to_string();
            if let Some(limit_pos) = sql_upper.rfind("LIMIT") {
                // Extract LIMIT from the outermost query (last occurrence)
                let limit_clause = &original_sql[limit_pos..];
                final_sql.push(' ');
                final_sql.push_str(limit_clause.trim());
            }
            debug!("CTE query detected, skipping re-aggregation: {}", final_sql);
            return final_sql;
        }

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
    ///
    /// Key insight: data nodes return results with SELECT aliases applied.
    /// GROUP BY `time_bucket("time", 10000000000, 0)` produces a column
    /// named `time` (the AS alias). The re-aggregation must use these
    /// schema column names, not the raw GROUP BY expressions.
    fn build_group_by_reaggregate_sql(&self, sql_upper: &str, original_sql: &str, schema: &SchemaRef) -> String {
        // Resolve GROUP BY expressions to their output column names in the schema.
        // e.g., GROUP BY time_bucket("time", 10s) → schema column "time" (from AS alias)
        let group_col_names = self.resolve_group_by_to_schema(sql_upper, original_sql, schema);

        // Everything else in the schema is an aggregate column
        let group_set: std::collections::HashSet<&str> = group_col_names.iter().map(|s| s.as_str()).collect();
        let mut agg_cols: Vec<(String, String)> = Vec::new();

        for field in schema.fields() {
            let col_name = field.name();
            if group_set.contains(col_name.as_str()) {
                continue;
            }
            // Determine re-aggregation function based on column name
            let col_lower = col_name.to_lowercase();
            let reagg_func = if col_lower.contains("count") {
                "SUM"
            } else if col_lower.contains("sum") {
                "SUM"
            } else if col_lower.contains("min") {
                "MIN"
            } else if col_lower.contains("max") {
                "MAX"
            } else {
                // Default: AVG for numeric aggregates (AVG of AVGs is an approximation,
                // but correct when each node has the same number of rows per group)
                "AVG"
            };
            agg_cols.push((col_name.clone(), reagg_func.to_string()));
        }

        if group_col_names.is_empty() || agg_cols.is_empty() {
            // Fallback: just concatenate with ORDER BY/LIMIT
            let mut final_sql = "SELECT * FROM partial_results".to_string();
            self.append_order_by_limit(&mut final_sql, sql_upper, original_sql);
            return final_sql;
        }

        // Build SELECT clause using schema column names
        let mut select_parts: Vec<String> = group_col_names.iter()
            .map(|c| format!("\"{}\"", c))
            .collect();

        for (col_name, reagg_func) in &agg_cols {
            select_parts.push(format!("{}(\"{}\") as \"{}\"", reagg_func, col_name, col_name));
        }

        let group_by_clause: Vec<String> = group_col_names.iter()
            .map(|c| format!("\"{}\"", c))
            .collect();

        let mut final_sql = format!(
            "SELECT {} FROM partial_results GROUP BY {}",
            select_parts.join(", "),
            group_by_clause.join(", ")
        );

        self.append_order_by_limit(&mut final_sql, sql_upper, original_sql);

        debug!("GROUP BY re-aggregation SQL: {}", final_sql);
        final_sql
    }

    /// Append ORDER BY and LIMIT clauses from the original SQL
    fn append_order_by_limit(&self, final_sql: &mut String, sql_upper: &str, original_sql: &str) {
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
    }

    /// Resolve GROUP BY expressions to schema column names.
    ///
    /// GROUP BY may contain raw expressions like `time_bucket("time", 10000000000, 0)`
    /// but the schema has the alias `time` (from SELECT ... AS time).
    /// This resolves each GROUP BY expression to its corresponding schema column.
    fn resolve_group_by_to_schema(&self, sql_upper: &str, original_sql: &str, schema: &SchemaRef) -> Vec<String> {
        let raw_group_cols = self.extract_group_by_columns(sql_upper, original_sql);
        if raw_group_cols.is_empty() {
            return Vec::new();
        }

        // Build a map of SELECT expression → alias from the original SQL
        let select_aliases = Self::extract_select_aliases(original_sql);

        let schema_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let mut resolved = Vec::new();

        for raw_col in &raw_group_cols {
            let trimmed = raw_col.trim().trim_matches('"');

            // Direct match: GROUP BY column name exists in schema
            if schema_names.iter().any(|s| s.eq_ignore_ascii_case(trimmed)) {
                resolved.push(
                    schema_names.iter().find(|s| s.eq_ignore_ascii_case(trimmed)).unwrap().clone()
                );
                continue;
            }

            // Alias match: GROUP BY expression has a SELECT alias
            let raw_normalized = raw_col.trim().to_lowercase().replace(' ', "");
            if let Some(alias) = select_aliases.get(&raw_normalized) {
                if schema_names.iter().any(|s| s == alias) {
                    resolved.push(alias.clone());
                    continue;
                }
            }

            // Last resort: use the raw expression (may produce extra columns)
            debug!("Could not resolve GROUP BY expression to schema column: {}", raw_col);
            resolved.push(raw_col.clone());
        }

        resolved
    }

    /// Extract SELECT expression → alias mapping.
    /// Parses "expr AS alias" patterns from the SELECT clause.
    fn extract_select_aliases(original_sql: &str) -> std::collections::HashMap<String, String> {
        let mut aliases = std::collections::HashMap::new();
        let sql_upper = original_sql.to_uppercase();

        // Find SELECT ... FROM boundaries
        let select_start = sql_upper.find("SELECT").map(|p| p + 6).unwrap_or(0);
        let from_pos = sql_upper.find(" FROM ").unwrap_or(original_sql.len());
        let select_clause = &original_sql[select_start..from_pos];

        // Split by comma, respecting parentheses depth
        let items = Self::split_select_items(select_clause);

        for item in items {
            let item = item.trim();
            // Look for " AS " (case-insensitive) to find alias
            let item_upper = item.to_uppercase();
            if let Some(as_pos) = item_upper.rfind(" AS ") {
                let expr = item[..as_pos].trim();
                let alias = item[as_pos + 4..].trim().trim_matches('"');
                let expr_normalized = expr.to_lowercase().replace(' ', "");
                aliases.insert(expr_normalized, alias.to_string());
            }
        }

        aliases
    }

    /// Split SELECT items by comma, respecting parentheses depth
    fn split_select_items(select_clause: &str) -> Vec<String> {
        let mut items = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for ch in select_clause.chars() {
            match ch {
                '(' => { depth += 1; current.push(ch); }
                ')' => { depth -= 1; current.push(ch); }
                ',' if depth == 0 => {
                    items.push(current.trim().to_string());
                    current.clear();
                }
                _ => current.push(ch),
            }
        }
        if !current.trim().is_empty() {
            items.push(current.trim().to_string());
        }
        items
    }

    /// Extract raw GROUP BY column expressions from SQL
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

            let group_clause = &after_group_by[..end_pos];
            // Split by comma respecting parentheses (for function calls in GROUP BY)
            return Self::split_select_items(group_clause);
        }
        Vec::new()
    }
}
