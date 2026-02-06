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
    batches_to_grouped_response, batches_to_response, ErrorResponse, InfluxResponse, Series,
    StatementResult,
};
pub use transpiler::{transpile, TranspileContext, TranspileError};

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
            if !gb.tags.is_empty() && gb.tags[0] != "*" {
                return Some(gb.tags.clone());
            }
        }
    }
    None
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
        assert!(sql.contains("SELECT *"));
        assert!(sql.contains("mydb.default.cpu"));
    }

    #[test]
    fn test_influxql_to_sql_with_aggregation() {
        let ctx = TranspileContext {
            database: "mydb".to_string(),
            schema: "default".to_string(),
        };

        let sql = influxql_to_sql("SELECT mean(value) FROM cpu GROUP BY time(5m)", &ctx).unwrap();
        assert!(sql.contains("AVG(value)"));
        assert!(sql.contains("time_bucket"));
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
