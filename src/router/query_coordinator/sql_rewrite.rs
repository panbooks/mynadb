//! SQL Rewriting for Partition-Level Routing
//!
//! This module handles rewriting SQL queries to add node_id partition filters,
//! enabling partition-level routing with cache affinity.

/// Rewrite SQL to add node_id partition filter
///
/// This is crucial for partition-level routing with cache affinity:
/// - Each target node receives SQL filtered to only its assigned partitions
/// - Prevents duplicate data (each partition queried by exactly one node)
/// - Enables cache affinity (nodes query partitions they wrote)
///
/// ## Examples
///
/// Original: `SELECT * FROM cpu WHERE date='2024-01-15'`
/// Rewritten: `SELECT * FROM cpu WHERE date='2024-01-15' AND node_id IN ('data-1', 'data-3')`
///
/// Original: `SELECT * FROM cpu`
/// Rewritten: `SELECT * FROM cpu WHERE node_id IN ('data-1')`
pub(crate) fn rewrite_sql_with_node_filter(sql: &str, assigned_partitions: &[String]) -> String {
    if assigned_partitions.is_empty() {
        return sql.to_string();
    }

    // Build the node_id IN clause
    let node_ids: Vec<String> = assigned_partitions
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "''"))) // Escape single quotes
        .collect();
    let node_filter = format!("node_id IN ({})", node_ids.join(", "));

    // Find the best place to inject the filter
    let sql_upper = sql.to_uppercase();

    // Strategy: Find WHERE clause and append, or add WHERE before GROUP BY/ORDER BY/LIMIT
    if let Some(where_pos) = find_where_clause_end(&sql_upper) {
        // Has WHERE clause - append with AND
        let (before, after) = sql.split_at(where_pos);
        format!("{} AND {}{}", before, node_filter, after)
    } else if let Some(clause_pos) = find_clause_position(&sql_upper) {
        // No WHERE but has GROUP BY/ORDER BY/LIMIT - insert WHERE before it
        let (before, after) = sql.split_at(clause_pos);
        format!("{} WHERE {}{}", before.trim_end(), node_filter, after)
    } else {
        // No WHERE and no trailing clauses - append WHERE at end
        format!("{} WHERE {}", sql.trim_end(), node_filter)
    }
}

/// Find the end position of the WHERE clause (before GROUP BY/ORDER BY/LIMIT/;)
fn find_where_clause_end(sql_upper: &str) -> Option<usize> {
    let where_pos = sql_upper.find(" WHERE ")?;
    let where_end = where_pos + 7; // Past " WHERE "

    // Find where the WHERE clause ends
    let terminators = [" GROUP BY", " ORDER BY", " LIMIT ", " HAVING ", ";"];
    let mut end_pos = sql_upper.len();

    for terminator in &terminators {
        if let Some(pos) = sql_upper[where_end..].find(terminator) {
            end_pos = end_pos.min(where_end + pos);
        }
    }

    Some(end_pos)
}

/// Find position of GROUP BY/ORDER BY/LIMIT clause (for inserting WHERE before)
fn find_clause_position(sql_upper: &str) -> Option<usize> {
    let clauses = [" GROUP BY", " ORDER BY", " LIMIT ", " HAVING "];
    let mut earliest = None;

    for clause in &clauses {
        if let Some(pos) = sql_upper.find(clause) {
            earliest = Some(earliest.map_or(pos, |e: usize| e.min(pos)));
        }
    }

    earliest
}
