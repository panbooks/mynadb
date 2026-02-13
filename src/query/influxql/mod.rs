//! InfluxQL Query Language Support
//!
//! This module provides InfluxQL parsing and transpilation to SQL.
//!
//! # Architecture
//!
//! ```text
//! InfluxQL Query
//!       │
//!       ▼
//!   ┌────────┐
//!   │ Lexer  │  Tokenizes input string
//!   └────────┘
//!       │
//!       ▼
//!   ┌────────┐
//!   │ Parser │  Builds AST from tokens
//!   └────────┘
//!       │
//!       ▼
//!   ┌────────────┐
//!   │ Transpiler │  Converts AST to SQL
//!   └────────────┘
//!       │
//!       ▼
//!   SQL Query → DataFusion
//! ```
//!
//! # Supported Statements
//!
//! ## SELECT
//! ```text
//! SELECT mean(value) FROM cpu WHERE host = 'server01' GROUP BY time(5m), host
//! SELECT * FROM cpu WHERE time > now() - 1h ORDER BY time DESC LIMIT 100
//! ```
//!
//! ## SHOW
//! ```text
//! SHOW MEASUREMENTS
//! SHOW DATABASES
//! SHOW TAG KEYS FROM cpu
//! SHOW TAG VALUES FROM cpu WITH KEY = "host"
//! SHOW FIELD KEYS FROM cpu
//! SHOW RETENTION POLICIES
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use mynadb::query::influxql::{parse, transpile, TranspileContext};
//!
//! let stmt = parse("SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)")?;
//! let ctx = TranspileContext {
//!     database: "mydb".to_string(),
//!     schema: "default".to_string(),
//! };
//! let sql = transpile(&stmt, &ctx)?;
//! // Execute SQL via DataFusion...
//! ```

mod ast;
mod lexer;
mod parser;
mod response;
mod transpiler;

// Re-export main types
pub use ast::*;
pub use lexer::LexerError;
pub use parser::{parse, ParseError};
pub use response::{
    batches_to_grouped_response, batches_to_grouped_response_with_epoch, batches_to_response,
    batches_to_response_with_epoch, ErrorResponse, InfluxResponse, Series, StatementResult,
};
pub use transpiler::{extract_filter_columns, transpile, TranspileContext, TranspileError};

/// Combined error type for InfluxQL processing
#[derive(Debug)]
pub enum InfluxQLError {
    Lexer(LexerError),
    Parse(ParseError),
    Transpile(TranspileError),
    Execution(String),
}

impl std::fmt::Display for InfluxQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InfluxQLError::Lexer(e) => write!(f, "Lexer error: {}", e),
            InfluxQLError::Parse(e) => write!(f, "Parse error: {}", e),
            InfluxQLError::Transpile(e) => write!(f, "Transpile error: {}", e),
            InfluxQLError::Execution(e) => write!(f, "Execution error: {}", e),
        }
    }
}

impl std::error::Error for InfluxQLError {}

impl From<LexerError> for InfluxQLError {
    fn from(e: LexerError) -> Self {
        InfluxQLError::Lexer(e)
    }
}

impl From<ParseError> for InfluxQLError {
    fn from(e: ParseError) -> Self {
        InfluxQLError::Parse(e)
    }
}

impl From<TranspileError> for InfluxQLError {
    fn from(e: TranspileError) -> Self {
        InfluxQLError::Transpile(e)
    }
}

/// Parse and transpile InfluxQL to SQL in one step
pub fn influxql_to_sql(query: &str, ctx: &TranspileContext) -> Result<String, InfluxQLError> {
    let stmt = parse(query)?;
    let sql = transpile(&stmt, ctx)?;
    Ok(sql)
}

// ============================================================================
// Shared execution helpers (used by standalone API and router API)
// ============================================================================

use arrow::record_batch::RecordBatch;

/// Result of preparing an InfluxQL query for execution
pub struct PreparedInfluxQL {
    /// The transpiled SQL query
    pub sql: String,
    /// Measurement name for the response
    pub measurement: String,
    /// Tag columns for GROUP BY grouping (None = no tag grouping)
    pub tag_columns: Option<Vec<String>>,
}

/// Parse and transpile an InfluxQL query, returning the SQL and response metadata.
/// On error, returns a ready-to-send InfluxResponse.
pub fn prepare_query(
    query: &str,
    db: &str,
    schema_default: &str,
) -> Result<PreparedInfluxQL, InfluxResponse> {
    let stmt = match parse(query) {
        Ok(s) => s,
        Err(e) => return Err(InfluxResponse::error(0, format!("Parse error: {}", e))),
    };

    let measurement = get_measurement_name(&stmt).unwrap_or_else(|| "result".to_string());
    let tag_columns = has_tag_grouping(&stmt);

    let ctx = TranspileContext {
        database: db.to_string(),
        schema: schema_default.to_string(),
    };

    let sql = match influxql_to_sql(query, &ctx) {
        Ok(s) => s,
        Err(e) => return Err(InfluxResponse::error(0, format!("Transpile error: {}", e))),
    };

    Ok(PreparedInfluxQL {
        sql,
        measurement,
        tag_columns,
    })
}

/// Format Arrow RecordBatches into an InfluxDB-compatible response.
pub fn format_result(
    batches: &[RecordBatch],
    prepared: &PreparedInfluxQL,
    epoch: Option<&str>,
) -> InfluxResponse {
    if let Some(ref tags) = prepared.tag_columns {
        let tag_refs: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
        batches_to_grouped_response_with_epoch(batches, &prepared.measurement, &tag_refs, 0, epoch)
    } else {
        batches_to_response_with_epoch(batches, &prepared.measurement, 0, epoch)
    }
}

/// Extract the InfluxQL query string from a POST body.
/// Handles url-encoded form (q=...) and plain text bodies.
pub fn extract_query_from_body(body: &str, fallback_q: Option<&str>) -> Option<String> {
    if body.starts_with("q=") {
        body.strip_prefix("q=")
            .map(|s| {
                // Form-urlencoded uses + for spaces; replace before percent-decoding
                let with_spaces = s.replace('+', " ");
                urlencoding::decode(&with_spaces).unwrap_or_default().into_owned()
            })
    } else if !body.is_empty() {
        Some(body.to_string())
    } else {
        fallback_q.map(|s| s.to_string())
    }
}

/// Extract measurement name from a parsed statement
pub fn get_measurement_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Select(s) => match &s.from.name {
            MeasurementName::Name(n) => Some(n.clone()),
            MeasurementName::Regex(_) => None,
        },
        Statement::ShowTagKeys(s) => s.measurement.as_ref().and_then(|m| match &m.name {
            MeasurementName::Name(n) => Some(n.clone()),
            MeasurementName::Regex(_) => None,
        }),
        Statement::ShowTagValues(s) => s.measurement.as_ref().and_then(|m| match &m.name {
            MeasurementName::Name(n) => Some(n.clone()),
            MeasurementName::Regex(_) => None,
        }),
        Statement::ShowFieldKeys(s) => s.measurement.as_ref().and_then(|m| match &m.name {
            MeasurementName::Name(n) => Some(n.clone()),
            MeasurementName::Regex(_) => None,
        }),
        _ => None,
    }
}

/// Check if a statement has GROUP BY with tags (requires grouping in response)
pub fn has_tag_grouping(stmt: &Statement) -> Option<Vec<String>> {
    if let Statement::Select(s) = stmt {
        if let Some(ref gb) = s.group_by {
            if gb.tags.is_empty() {
                return None;
            }
            // Expand GROUP BY * to concrete tag names from WHERE clause
            let mut tags = Vec::new();
            for tag in &gb.tags {
                if tag == "*" {
                    if let Some(ref cond) = s.condition {
                        for col in extract_filter_columns(cond) {
                            if !tags.contains(&col) {
                                tags.push(col);
                            }
                        }
                    }
                } else {
                    if !tags.contains(tag) {
                        tags.push(tag.clone());
                    }
                }
            }
            if !tags.is_empty() {
                return Some(tags);
            }
        }
    }
    None
}

/// Build an InfluxDB-compatible response for SHOW DATABASES
pub fn build_show_databases_response(databases: Vec<String>) -> InfluxResponse {
    let values: Vec<Vec<serde_json::Value>> = databases
        .into_iter()
        .map(|db| vec![serde_json::Value::String(db)])
        .collect();

    InfluxResponse::with_result(StatementResult {
        statement_id: 0,
        series: Some(vec![Series {
            name: "databases".to_string(),
            tags: None,
            columns: vec!["name".to_string()],
            values,
        }]),
        error: None,
    })
}

/// Build an InfluxDB-compatible response for SHOW MEASUREMENTS
pub fn build_show_measurements_response(measurements: Vec<String>) -> InfluxResponse {
    if measurements.is_empty() {
        return InfluxResponse::with_result(StatementResult {
            statement_id: 0,
            series: None,
            error: None,
        });
    }

    let values: Vec<Vec<serde_json::Value>> = measurements
        .into_iter()
        .map(|m| vec![serde_json::Value::String(m)])
        .collect();

    InfluxResponse::with_result(StatementResult {
        statement_id: 0,
        series: Some(vec![Series {
            name: "measurements".to_string(),
            tags: None,
            columns: vec!["name".to_string()],
            values,
        }]),
        error: None,
    })
}

/// Build an InfluxDB-compatible response for SHOW RETENTION POLICIES
/// Returns a synthetic "autogen" retention policy (MynaDB doesn't have InfluxDB-style RPs)
pub fn build_show_retention_policies_response() -> InfluxResponse {
    InfluxResponse::with_result(StatementResult {
        statement_id: 0,
        series: Some(vec![Series {
            name: "results".to_string(),
            tags: None,
            columns: vec![
                "name".to_string(),
                "duration".to_string(),
                "shardGroupDuration".to_string(),
                "replicaN".to_string(),
                "default".to_string(),
            ],
            values: vec![vec![
                serde_json::Value::String("autogen".to_string()),
                serde_json::Value::String("0s".to_string()),
                serde_json::Value::String("168h0m0s".to_string()),
                serde_json::json!(1),
                serde_json::Value::Bool(true),
            ]],
        }]),
        error: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_influxql_to_sql() {
        let ctx = TranspileContext {
            database: "mydb".to_string(),
            schema: "default".to_string(),
        };

        let sql = influxql_to_sql("SELECT * FROM cpu", &ctx).unwrap();
        assert!(sql.contains("SELECT *"), "SQL was: {}", sql);
        assert!(sql.contains(r#""mydb"."default"."cpu""#), "SQL was: {}", sql);
    }

    #[test]
    fn test_influxql_to_sql_with_aggregation() {
        let ctx = TranspileContext {
            database: "mydb".to_string(),
            schema: "default".to_string(),
        };

        let sql = influxql_to_sql("SELECT mean(value) FROM cpu GROUP BY time(5m)", &ctx).unwrap();
        assert!(sql.contains(r#"AVG("value")"#), "SQL was: {}", sql);
        assert!(sql.contains("time_bucket"), "SQL was: {}", sql);
    }

    #[test]
    fn test_get_measurement_name() {
        let stmt = parse("SELECT * FROM cpu").unwrap();
        assert_eq!(get_measurement_name(&stmt), Some("cpu".to_string()));
    }

    #[test]
    fn test_has_tag_grouping() {
        let stmt = parse("SELECT mean(value) FROM cpu GROUP BY host").unwrap();
        let tags = has_tag_grouping(&stmt);
        assert!(tags.is_some());
        assert!(tags.unwrap().contains(&"host".to_string()));
    }

    #[test]
    fn test_parse_errors() {
        assert!(parse("SELEC * FROM cpu").is_err());
        assert!(parse("SELECT FROM cpu").is_err());
    }
}
