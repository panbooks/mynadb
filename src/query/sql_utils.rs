//! SQL Parsing and Transformation Utilities
//!
//! Provides utilities for parsing SQL queries, extracting table references,
//! qualifying table names, and injecting date predicates for partition pruning.

use crate::query::types::{QueryOptimizations, TimeRange};
use tracing::debug;

/// Default schema name when not specified
pub const DEFAULT_SCHEMA: &str = "default";

/// Parse a table reference considering quoted identifiers
/// Returns (parts, original_string)
pub fn parse_table_reference(table_ref: &str) -> (Vec<String>, String) {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = ' ';
    let chars: Vec<char> = table_ref.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        if !in_quotes && (ch == '"' || ch == '`') {
            in_quotes = true;
            quote_char = ch;
            i += 1;
            continue;
        }

        if in_quotes && ch == quote_char {
            in_quotes = false;
            // Add the current quoted part
            if !current.is_empty() {
                parts.push(current.clone());
                current.clear();
            }
            i += 1;
            // Skip the dot after closing quote if present
            if i < chars.len() && chars[i] == '.' {
                i += 1;
            }
            continue;
        }

        if !in_quotes && ch == '.' {
            if !current.is_empty() {
                parts.push(current.clone());
                current.clear();
            }
            i += 1;
            continue;
        }

        current.push(ch);
        i += 1;
    }

    if !current.is_empty() {
        parts.push(current);
    }

    (parts, table_ref.to_string())
}

/// Quote an identifier if it contains special characters or mixed case
/// DataFusion requires quotes for:
/// - Case-sensitive identifiers (any uppercase letters)
/// - Identifiers with special characters (dots, hyphens, spaces)
pub fn quote_identifier(ident: &str) -> String {
    let needs_quoting = ident.contains('.')
        || ident.contains('-')
        || ident.contains(' ')
        || ident.chars().any(|c| c.is_uppercase()); // Preserve case-sensitive names

    if needs_quoting {
        format!("\"{}\"", ident)
    } else {
        ident.to_string()
    }
}

/// Extract table references from SQL query
/// Returns Vec<(catalog, schema, table)> with fully resolved names
pub fn extract_table_references(
    sql: &str,
    catalog_name: &str,
) -> Vec<(String, String, String)> {
    let sql_upper = sql.to_uppercase();
    let mut table_refs = Vec::new();

    let keywords = [
        "FROM",
        "JOIN",
        "INNER JOIN",
        "LEFT JOIN",
        "RIGHT JOIN",
        "FULL JOIN",
    ];

    for keyword in &keywords {
        let pattern = format!("{} ", keyword);
        let mut search_pos = 0;

        while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
            let abs_pos = search_pos + pos + pattern.len();
            let remaining = &sql[abs_pos..];

            // Extract all comma-separated table references
            let mut table_list_end = 0;
            let mut in_quotes = false;
            let mut quote_char = ' ';
            let mut paren_depth = 0;

            // Find the end of the table list (stops at WHERE, ORDER BY, GROUP BY, etc.)
            for (i, ch) in remaining.chars().enumerate() {
                if !in_quotes && ch == '(' {
                    paren_depth += 1;
                } else if !in_quotes && ch == ')' {
                    paren_depth -= 1;
                    if paren_depth < 0 {
                        table_list_end = i;
                        break;
                    }
                } else if !in_quotes && (ch == '"' || ch == '`') {
                    in_quotes = true;
                    quote_char = ch;
                } else if in_quotes && ch == quote_char {
                    in_quotes = false;
                } else if !in_quotes && paren_depth == 0 {
                    // Check for SQL keywords that end the table list
                    let remaining_upper = &sql_upper[abs_pos + i..];
                    if remaining_upper.starts_with("WHERE ")
                        || remaining_upper.starts_with("GROUP ")
                        || remaining_upper.starts_with("ORDER ")
                        || remaining_upper.starts_with("LIMIT ")
                        || remaining_upper.starts_with("HAVING ")
                        || remaining_upper.starts_with("UNION ")
                        || remaining_upper.starts_with("EXCEPT ")
                        || remaining_upper.starts_with("INTERSECT ")
                        || ch == ';'
                    {
                        table_list_end = i;
                        break;
                    }
                }
            }

            if table_list_end == 0 {
                table_list_end = remaining.len();
            }

            let table_list = &remaining[..table_list_end];

            // Split by commas and process each table reference
            let mut current_table = String::new();
            let mut in_quotes = false;
            let mut quote_char = ' ';

            for ch in table_list.chars() {
                if !in_quotes && (ch == '"' || ch == '`') {
                    in_quotes = true;
                    quote_char = ch;
                    current_table.push(ch);
                } else if in_quotes && ch == quote_char {
                    in_quotes = false;
                    current_table.push(ch);
                } else if !in_quotes && ch == ',' {
                    // Process the current table
                    process_table_ref(&current_table, catalog_name, &mut table_refs);
                    current_table.clear();
                } else {
                    current_table.push(ch);
                }
            }

            // Process the last table
            if !current_table.is_empty() {
                process_table_ref(&current_table, catalog_name, &mut table_refs);
            }

            search_pos = abs_pos + table_list_end;
        }
    }

    // Deduplicate
    table_refs.sort();
    table_refs.dedup();
    table_refs
}

/// Helper to process a single table reference and add to the list
fn process_table_ref(
    table_ref: &str,
    catalog_name: &str,
    table_refs: &mut Vec<(String, String, String)>,
) {
    let table_ref = table_ref.trim();

    // Skip empty refs, subqueries, or aliases (words after whitespace)
    if table_ref.is_empty() || table_ref.starts_with('(') {
        return;
    }

    // Remove alias if present (e.g., "cpu c" -> "cpu")
    let table_ref = table_ref.split_whitespace().next().unwrap_or(table_ref);

    // Parse the table reference considering quotes
    let (parts, _) = parse_table_reference(table_ref);

    // Determine catalog, schema, table based on number of parts
    let (catalog, schema, table) = match parts.len() {
        0 => return,
        1 => {
            // Simple name: cpu -> catalog.default.cpu
            (catalog_name.to_string(), DEFAULT_SCHEMA.to_string(), parts[0].clone())
        }
        2 => {
            // Partially qualified: schema.table -> catalog.schema.table
            (catalog_name.to_string(), parts[0].clone(), parts[1].clone())
        }
        _ => {
            // Fully qualified: catalog.schema.table
            (parts[0].clone(), parts[1].clone(), parts[2].clone())
        }
    };

    table_refs.push((catalog, schema, table));
}

/// Qualify unqualified or partially qualified table names with catalog.schema
///
/// Examples (with catalog_name = "testdb"):
/// - "cpu" -> "testdb.default.cpu"
/// - "webapp.cpu" -> "testdb.webapp.cpu"
/// - "testdb.webapp.cpu" -> "testdb.webapp.cpu" (already fully qualified)
/// - "otherdb.schema.table" -> "otherdb.schema.table" (different catalog, unchanged)
/// - "default.\"cat.transaction\"" -> "testdb.default.\"cat.transaction\""
pub fn qualify_table_names(sql: &str, catalog_name: &str) -> String {
    let sql_upper = sql.to_uppercase();
    let mut result = sql.to_string();
    let mut offset: i64 = 0; // Track the offset due to replacements

    let keywords = [
        "FROM",
        "JOIN",
        "INNER JOIN",
        "LEFT JOIN",
        "RIGHT JOIN",
        "FULL JOIN",
    ];

    for keyword in &keywords {
        let pattern = format!("{} ", keyword);
        let mut search_pos = 0;

        while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
            let abs_pos = search_pos + pos + pattern.len();
            let remaining = &sql[abs_pos..];

            // Find end of table reference, considering quotes
            let mut end_pos = 0;
            let mut in_quotes = false;
            let mut quote_char = ' ';

            for (i, ch) in remaining.chars().enumerate() {
                if !in_quotes && (ch == '"' || ch == '`') {
                    in_quotes = true;
                    quote_char = ch;
                } else if in_quotes && ch == quote_char {
                    in_quotes = false;
                } else if !in_quotes
                    && (ch.is_whitespace() || ch == ',' || ch == ';' || ch == ')')
                {
                    end_pos = i;
                    break;
                }
            }

            if end_pos == 0 {
                end_pos = remaining.len();
            }

            let table_ref = remaining[..end_pos].trim();

            // Skip empty refs or subqueries
            if table_ref.is_empty() || table_ref.starts_with('(') {
                search_pos = abs_pos + end_pos;
                continue;
            }

            // Parse the table reference considering quotes
            let (parts, _) = parse_table_reference(table_ref);

            // Determine qualification based on number of parts
            let qualified = match parts.len() {
                0 => {
                    search_pos = abs_pos + end_pos;
                    continue;
                }
                1 => {
                    // Simple name: cpu -> catalog.default.cpu
                    format!(
                        "{}.{}.{}",
                        catalog_name,
                        DEFAULT_SCHEMA,
                        quote_identifier(&parts[0])
                    )
                }
                2 => {
                    // Partially qualified: schema.table -> catalog.schema.table
                    format!(
                        "{}.{}.{}",
                        catalog_name,
                        quote_identifier(&parts[0]),
                        quote_identifier(&parts[1])
                    )
                }
                _ => {
                    // Already fully qualified (catalog.schema.table), don't modify
                    // But reconstruct it with proper quoting
                    format!(
                        "{}.{}.{}",
                        quote_identifier(&parts[0]),
                        quote_identifier(&parts[1]),
                        quote_identifier(&parts[2])
                    )
                }
            };

            // Only replace if we actually changed something
            if qualified != table_ref {
                // Calculate the actual position in the result string with offset
                let result_pos = (abs_pos as i64 + offset) as usize;
                let before_part = &result[..result_pos];
                let after_part = &result[result_pos + table_ref.len()..];

                result = format!("{}{}{}", before_part, qualified, after_part);

                // Update offset
                offset += qualified.len() as i64 - table_ref.len() as i64;
            }

            search_pos = abs_pos + end_pos;
        }
    }

    result
}

/// Extract time range from WHERE clause for partition pruning hints
/// Returns (start_timestamp, end_timestamp) if found
pub fn extract_time_range(sql: &str) -> Option<TimeRange> {
    let sql_upper = sql.to_uppercase();

    // Look for timestamp conditions in WHERE clause
    if let Some(where_pos) = sql_upper.find("WHERE") {
        let where_clause = &sql[where_pos + 5..];

        // Common patterns:
        // timestamp > X, timestamp >= X, timestamp < X, timestamp <= X
        // timestamp BETWEEN X AND Y
        // time > X, time >= X (common alias)

        let mut start_ts: Option<i64> = None;
        let mut end_ts: Option<i64> = None;
        let mut start_date: Option<String> = None;
        let mut end_date: Option<String> = None;

        // Look for timestamp/time comparisons
        let time_patterns = ["TIMESTAMP", "TIME", "TS"];
        for pattern in time_patterns {
            // Check for >= or >
            if let Some(pos) = where_clause.to_uppercase().find(&format!("{} >", pattern)) {
                let after = &where_clause[pos + pattern.len() + 2..];
                if let Some(val) = extract_numeric_value(after) {
                    start_ts = Some(val);
                }
            }
            if let Some(pos) = where_clause.to_uppercase().find(&format!("{} >=", pattern)) {
                let after = &where_clause[pos + pattern.len() + 3..];
                if let Some(val) = extract_numeric_value(after) {
                    start_ts = Some(val);
                }
            }
            // Check for <= or <
            if let Some(pos) = where_clause.to_uppercase().find(&format!("{} <", pattern)) {
                let after = &where_clause[pos + pattern.len() + 2..];
                if let Some(val) = extract_numeric_value(after) {
                    end_ts = Some(val);
                }
            }
            if let Some(pos) = where_clause.to_uppercase().find(&format!("{} <=", pattern)) {
                let after = &where_clause[pos + pattern.len() + 3..];
                if let Some(val) = extract_numeric_value(after) {
                    end_ts = Some(val);
                }
            }
        }

        // Look for date partition filter (date = 'YYYY-MM-DD')
        if let Some(pos) = where_clause.to_uppercase().find("DATE =") {
            let after = &where_clause[pos + 6..];
            if let Some(date) = extract_date_value(after) {
                start_date = Some(date.clone());
                end_date = Some(date);
            }
        }
        if let Some(pos) = where_clause.to_uppercase().find("DATE >=") {
            let after = &where_clause[pos + 7..];
            if let Some(date) = extract_date_value(after) {
                start_date = Some(date);
            }
        }
        if let Some(pos) = where_clause.to_uppercase().find("DATE <=") {
            let after = &where_clause[pos + 7..];
            if let Some(date) = extract_date_value(after) {
                end_date = Some(date);
            }
        }

        if start_ts.is_some() || end_ts.is_some() || start_date.is_some() || end_date.is_some() {
            return Some(TimeRange {
                start_timestamp: start_ts,
                end_timestamp: end_ts,
                start_date,
                end_date,
            });
        }
    }

    None
}

/// Extract numeric value from SQL fragment
fn extract_numeric_value(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    let mut num_str = String::new();

    for ch in trimmed.chars() {
        if ch.is_ascii_digit() || ch == '-' {
            num_str.push(ch);
        } else if !num_str.is_empty() {
            break;
        }
    }

    num_str.parse().ok()
}

/// Extract date value from SQL fragment (e.g., '2024-01-15')
fn extract_date_value(s: &str) -> Option<String> {
    let trimmed = s.trim();

    // Look for quoted date string
    if let Some(start) = trimmed.find('\'') {
        if let Some(end) = trimmed[start + 1..].find('\'') {
            let date = &trimmed[start + 1..start + 1 + end];
            // Validate date format (YYYY-MM-DD)
            if date.len() == 10 && date.chars().nth(4) == Some('-') && date.chars().nth(7) == Some('-') {
                return Some(date.to_string());
            }
        }
    }

    None
}

/// Inject date predicates into SQL based on timestamp filters for smart partition pruning
///
/// This converts timestamp predicates to date predicates, enabling Hive-style partition pruning.
/// Example:
///   Input:  SELECT * FROM cpu WHERE timestamp >= 1704067200000000000
///   Output: SELECT * FROM cpu WHERE timestamp >= 1704067200000000000 AND date >= '2024-01-01'
///
/// This allows DataFusion to prune partitions based on the date column.
pub fn inject_date_predicates(sql: &str) -> String {
    // Extract time range from the SQL
    let time_range = match extract_time_range(sql) {
        Some(tr) => tr,
        None => return sql.to_string(), // No time range found, return original SQL
    };

    // Get derived date range from timestamps
    let (start_date, end_date) = time_range.get_date_range();

    // If no dates can be derived, return original SQL
    if start_date.is_none() && end_date.is_none() {
        return sql.to_string();
    }

    // Check if date predicates already exist in the query
    let sql_upper = sql.to_uppercase();
    if sql_upper.contains("DATE =")
        || sql_upper.contains("DATE >=")
        || sql_upper.contains("DATE <=")
        || sql_upper.contains("DATE >")
        || sql_upper.contains("DATE <")
        || sql_upper.contains("DATE BETWEEN")
    {
        // Date predicates already exist, don't inject duplicates
        debug!("Date predicates already present in query, skipping injection");
        return sql.to_string();
    }

    // Build date predicate clause
    let date_predicates = match (&start_date, &end_date) {
        (Some(start), Some(end)) if start == end => {
            format!("date = '{}'", start)
        }
        (Some(start), Some(end)) => {
            format!("date >= '{}' AND date <= '{}'", start, end)
        }
        (Some(start), None) => {
            format!("date >= '{}'", start)
        }
        (None, Some(end)) => {
            format!("date <= '{}'", end)
        }
        (None, None) => {
            return sql.to_string();
        }
    };

    // Find WHERE clause position to inject the date predicates
    if let Some(where_pos) = sql_upper.find("WHERE") {
        // Insert date predicates after WHERE keyword
        let where_end = where_pos + 5; // "WHERE" is 5 characters
        let before = &sql[..where_end];
        let after = &sql[where_end..];

        let result = format!("{} {} AND{}", before, date_predicates, after);
        debug!("Injected date predicates: {} -> {}", sql, result);
        result
    } else {
        // No WHERE clause - we shouldn't inject date predicates without other conditions
        // This could cause issues with queries that have no filtering
        debug!("No WHERE clause found, cannot inject date predicates safely");
        sql.to_string()
    }
}

/// Extract optimization information from the physical plan
pub fn extract_optimization_info(plan: &str) -> QueryOptimizations {
    let plan_lower = plan.to_lowercase();

    QueryOptimizations {
        predicate_pushdown: plan_lower.contains("pushdown")
            || plan_lower.contains("predicate=")
            || plan_lower.contains("filter:"),
        partition_pruning: plan_lower.contains("partition")
            || plan_lower.contains("pruning")
            || plan_lower.contains("date="),
        row_group_pruning: plan_lower.contains("row_group")
            || plan_lower.contains("statistics")
            || plan_lower.contains("skip"),
        projection_pushdown: plan_lower.contains("projection=")
            || plan_lower.contains("project:"),
        limit_pushdown: plan_lower.contains("limit")
            || plan_lower.contains("topk")
            || plan_lower.contains("fetch="),
    }
}

/// Create a brief summary of the physical plan
pub fn summarize_plan(plan: &str) -> String {
    let mut summary = Vec::new();

    if plan.contains("ParquetExec") || plan.contains("ParquetScan") {
        summary.push("Parquet scan");
    }
    if plan.contains("FilterExec") || plan.contains("filter=") {
        summary.push("Filter applied");
    }
    if plan.contains("ProjectionExec") || plan.contains("projection=") {
        summary.push("Projection pushdown");
    }
    if plan.contains("AggregateExec") || plan.contains("aggregate=") {
        summary.push("Aggregation");
    }
    if plan.contains("SortExec") || plan.contains("sort=") {
        summary.push("Sort");
    }
    if plan.contains("HashJoin") || plan.contains("MergeJoin") {
        summary.push("Join");
    }
    if plan.contains("GlobalLimitExec") || plan.contains("LocalLimitExec") {
        summary.push("Limit");
    }

    if summary.is_empty() {
        "Simple scan".to_string()
    } else {
        summary.join(" → ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qualify_table_names_unqualified() {
        // Unqualified table name: cpu -> testdb.default.cpu
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let result = qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu WHERE usage > 50");
    }

    #[test]
    fn test_qualify_table_names_partially_qualified() {
        // Partially qualified: webapp.cpu -> testdb.webapp.cpu
        let sql = "SELECT * FROM webapp.cpu";
        let result = qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.webapp.cpu");
    }

    #[test]
    fn test_qualify_table_names_fully_qualified() {
        // Fully qualified stays the same
        let sql = "SELECT * FROM testdb.default.cpu";
        let result = qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu");
    }

    #[test]
    fn test_extract_table_references() {
        let sql = "SELECT * FROM cpu, memory WHERE cpu.usage > 50";
        let refs = extract_table_references(sql, "testdb");
        assert!(refs.contains(&("testdb".to_string(), "default".to_string(), "cpu".to_string())));
        assert!(refs.contains(&("testdb".to_string(), "default".to_string(), "memory".to_string())));
    }

    #[test]
    fn test_parse_table_reference_simple() {
        let (parts, _) = parse_table_reference("cpu");
        assert_eq!(parts, vec!["cpu"]);
    }

    #[test]
    fn test_parse_table_reference_qualified() {
        let (parts, _) = parse_table_reference("catalog.schema.table");
        assert_eq!(parts, vec!["catalog", "schema", "table"]);
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "simple");
        assert_eq!(quote_identifier("MixedCase"), "\"MixedCase\"");
        assert_eq!(quote_identifier("has-dash"), "\"has-dash\"");
        assert_eq!(quote_identifier("has.dot"), "\"has.dot\"");
    }
}
