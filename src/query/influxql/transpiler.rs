//! InfluxQL to SQL Transpiler
//!
//! Converts InfluxQL AST to SQL strings for execution via DataFusion.

use super::ast::*;

/// Transpiler error
#[derive(Debug, Clone)]
pub struct TranspileError {
    pub message: String,
}

impl std::fmt::Display for TranspileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transpile error: {}", self.message)
    }
}

impl std::error::Error for TranspileError {}

/// Transpiler context
pub struct TranspileContext {
    /// Default database if not specified in query
    pub database: String,
    /// Default schema/retention policy
    pub schema: String,
}

impl Default for TranspileContext {
    fn default() -> Self {
        Self {
            database: "default".to_string(),
            schema: "default".to_string(),
        }
    }
}

/// Transpile an InfluxQL statement to SQL
pub fn transpile(stmt: &Statement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    match stmt {
        Statement::Select(s) => transpile_select(s, ctx),
        Statement::ShowMeasurements(s) => transpile_show_measurements(s, ctx),
        Statement::ShowTagKeys(s) => transpile_show_tag_keys(s, ctx),
        Statement::ShowTagValues(s) => transpile_show_tag_values(s, ctx),
        Statement::ShowFieldKeys(s) => transpile_show_field_keys(s, ctx),
        Statement::ShowDatabases => Ok("SHOW DATABASES".to_string()),
        Statement::ShowRetentionPolicies(db) => {
            let db = db.as_ref().unwrap_or(&ctx.database);
            Ok(format!("SHOW SCHEMAS FROM {}", db))
        }
    }
}

fn transpile_select(stmt: &SelectStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    let mut sql = String::new();

    // SELECT clause
    sql.push_str("SELECT ");

    // Handle GROUP BY time() with aggregations
    let has_time_group = stmt.group_by.as_ref().map(|g| g.time.is_some()).unwrap_or(false);
    let _has_aggregates = stmt.fields.iter().any(|f| matches!(f.expr, Expr::Function(_)));

    let fields: Vec<String> = stmt
        .fields
        .iter()
        .map(|f| transpile_field(f, has_time_group))
        .collect::<Result<Vec<_>, _>>()?;
    sql.push_str(&fields.join(", "));

    // Add time bucket if GROUP BY time()
    if has_time_group {
        if let Some(ref gb) = stmt.group_by {
            if let Some(ref duration) = gb.time {
                sql.push_str(&format!(
                    ", time_bucket('{}', time) AS time",
                    duration.to_interval_string()
                ));
            }
        }
    }

    // FROM clause
    sql.push_str(" FROM ");
    sql.push_str(&transpile_measurement(&stmt.from, ctx));

    // WHERE clause
    if let Some(ref cond) = stmt.condition {
        sql.push_str(" WHERE ");
        sql.push_str(&transpile_expr(cond)?);
    }

    // GROUP BY clause
    if let Some(ref gb) = stmt.group_by {
        let mut group_parts = Vec::new();

        // Time bucket
        if gb.time.is_some() {
            group_parts.push("time_bucket".to_string());
        }

        // Tags
        for tag in &gb.tags {
            if tag == "*" {
                // GROUP BY * means group by all tags - this needs special handling
                // For now, we'll skip this
            } else {
                group_parts.push(quote_ident(tag));
            }
        }

        if !group_parts.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&group_parts.join(", "));
        }
    }

    // ORDER BY clause
    if let Some(ref ob) = stmt.order_by {
        sql.push_str(" ORDER BY time");
        if ob.time_desc {
            sql.push_str(" DESC");
        } else {
            sql.push_str(" ASC");
        }
    } else if has_time_group {
        // Default order by time for grouped queries
        sql.push_str(" ORDER BY time_bucket ASC");
    }

    // LIMIT/OFFSET
    if let Some(limit) = stmt.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = stmt.offset {
        sql.push_str(&format!(" OFFSET {}", offset));
    }

    Ok(sql)
}

fn transpile_field(field: &Field, _has_time_group: bool) -> Result<String, TranspileError> {
    let expr_sql = transpile_expr(&field.expr)?;

    if let Some(ref alias) = field.alias {
        Ok(format!("{} AS {}", expr_sql, quote_ident(alias)))
    } else {
        Ok(expr_sql)
    }
}

fn transpile_expr(expr: &Expr) -> Result<String, TranspileError> {
    match expr {
        Expr::Wildcard => Ok("*".to_string()),
        Expr::Identifier(name) => Ok(quote_ident(name)),
        Expr::Literal(lit) => transpile_literal(lit),
        Expr::Function(func) => transpile_function(func),
        Expr::BinaryOp { left, op, right } => {
            let left_sql = transpile_expr(left)?;
            let right_sql = transpile_expr(right)?;
            Ok(format!("({} {} {})", left_sql, transpile_binary_op(op), right_sql))
        }
        Expr::UnaryOp { op, expr } => {
            let expr_sql = transpile_expr(expr)?;
            match op {
                UnaryOperator::Minus => Ok(format!("(-{})", expr_sql)),
                UnaryOperator::Not => Ok(format!("(NOT {})", expr_sql)),
            }
        }
        Expr::Regex(pattern) => {
            // Convert to SQL LIKE or regexp function
            Ok(format!("'{}'", escape_string(pattern)))
        }
        Expr::Duration(d) => {
            // Convert to interval
            Ok(format!("INTERVAL '{}'", d.to_interval_string()))
        }
        Expr::Now => Ok("now()".to_string()),
    }
}

fn transpile_literal(lit: &Literal) -> Result<String, TranspileError> {
    match lit {
        Literal::Integer(n) => Ok(n.to_string()),
        Literal::Float(f) => Ok(f.to_string()),
        Literal::String(s) => Ok(format!("'{}'", escape_string(s))),
        Literal::Boolean(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
    }
}

fn transpile_function(func: &FunctionCall) -> Result<String, TranspileError> {
    let name = map_function_name(&func.name);

    let args: Vec<String> = func
        .args
        .iter()
        .map(transpile_expr)
        .collect::<Result<Vec<_>, _>>()?;

    // Handle special cases
    match func.name.to_lowercase().as_str() {
        "count" if args.is_empty() || args[0] == "*" => {
            Ok("COUNT(*)".to_string())
        }
        "first" | "last" => {
            // first(value) -> first_value(value, time)
            // These need our custom UDFs
            if args.is_empty() {
                return Err(TranspileError {
                    message: format!("{} requires an argument", func.name),
                });
            }
            Ok(format!("{}_value({}, time)", func.name.to_lowercase(), args[0]))
        }
        "derivative" | "non_negative_derivative" => {
            // These need window functions - simplified for now
            if args.is_empty() {
                return Err(TranspileError {
                    message: "derivative requires an argument".to_string(),
                });
            }
            // Approximate with difference
            Ok(format!(
                "({} - LAG({}) OVER (ORDER BY time))",
                args[0], args[0]
            ))
        }
        "difference" | "non_negative_difference" => {
            if args.is_empty() {
                return Err(TranspileError {
                    message: "difference requires an argument".to_string(),
                });
            }
            Ok(format!(
                "({} - LAG({}) OVER (ORDER BY time))",
                args[0], args[0]
            ))
        }
        "moving_average" => {
            if args.len() < 2 {
                return Err(TranspileError {
                    message: "moving_average requires field and window size".to_string(),
                });
            }
            Ok(format!(
                "AVG({}) OVER (ORDER BY time ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                args[0], args[1]
            ))
        }
        "cumulative_sum" => {
            if args.is_empty() {
                return Err(TranspileError {
                    message: "cumulative_sum requires an argument".to_string(),
                });
            }
            Ok(format!(
                "SUM({}) OVER (ORDER BY time ROWS UNBOUNDED PRECEDING)",
                args[0]
            ))
        }
        "elapsed" => {
            // elapsed(field, unit) - time since last point
            let unit_ns = if args.len() > 1 {
                // Parse unit from args[1] - simplified
                1_000_000_000i64 // Default to seconds
            } else {
                1_000_000_000i64
            };
            Ok(format!(
                "(time - LAG(time) OVER (ORDER BY time)) / {}",
                unit_ns
            ))
        }
        "spread" => {
            // spread(field) = max - min
            if args.is_empty() {
                return Err(TranspileError {
                    message: "spread requires an argument".to_string(),
                });
            }
            Ok(format!("(MAX({}) - MIN({}))", args[0], args[0]))
        }
        "percentile" => {
            if args.len() < 2 {
                return Err(TranspileError {
                    message: "percentile requires field and percentile value".to_string(),
                });
            }
            // Use approx_percentile_cont
            Ok(format!("approx_percentile_cont({}, {})", args[0], args[1]))
        }
        "sample" => {
            // sample(field, n) - random sample
            // Not directly supported, use LIMIT with random order
            Ok(format!("{}", args[0]))
        }
        "top" | "bottom" => {
            // These need subqueries - simplified
            Ok(format!("{}", args[0]))
        }
        _ => {
            // Standard function
            Ok(format!("{}({})", name, args.join(", ")))
        }
    }
}

fn map_function_name(name: &str) -> &str {
    match name.to_lowercase().as_str() {
        "mean" => "AVG",
        "stddev" => "STDDEV",
        "median" => "MEDIAN",
        "mode" => "MODE",
        "distinct" => "DISTINCT",
        _ => name,
    }
}

fn transpile_binary_op(op: &BinaryOperator) -> &'static str {
    match op {
        BinaryOperator::Add => "+",
        BinaryOperator::Sub => "-",
        BinaryOperator::Mul => "*",
        BinaryOperator::Div => "/",
        BinaryOperator::Mod => "%",
        BinaryOperator::Eq => "=",
        BinaryOperator::Ne => "<>",
        BinaryOperator::Lt => "<",
        BinaryOperator::Le => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::Ge => ">=",
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        BinaryOperator::RegexMatch => "~",     // PostgreSQL regex
        BinaryOperator::RegexNotMatch => "!~", // PostgreSQL not regex
    }
}

fn transpile_measurement(m: &Measurement, ctx: &TranspileContext) -> String {
    let db = m.database.as_ref().unwrap_or(&ctx.database);
    let schema = m.retention_policy.as_ref().unwrap_or(&ctx.schema);

    match &m.name {
        MeasurementName::Name(name) => {
            format!("{}.{}.{}", quote_ident(db), quote_ident(schema), quote_ident(name))
        }
        MeasurementName::Regex(pattern) => {
            // Regex measurement names need special handling
            // For now, just use the pattern as-is
            format!("{}.{}.{}", quote_ident(db), quote_ident(schema), pattern)
        }
    }
}

fn transpile_show_measurements(stmt: &ShowMeasurementsStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    let db = stmt.database.as_ref().unwrap_or(&ctx.database);

    // Use information_schema or custom query
    let mut sql = format!(
        "SELECT table_name AS name FROM information_schema.tables WHERE table_catalog = '{}' AND table_schema = '{}'",
        db, ctx.schema
    );

    if let Some(ref cond) = stmt.condition {
        // Translate condition on measurement name
        sql.push_str(" AND ");
        sql.push_str(&transpile_show_condition(cond, "table_name")?);
    }

    sql.push_str(" ORDER BY name");

    if let Some(limit) = stmt.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = stmt.offset {
        sql.push_str(&format!(" OFFSET {}", offset));
    }

    Ok(sql)
}

fn transpile_show_tag_keys(stmt: &ShowTagKeysStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    let db = stmt.database.as_ref().unwrap_or(&ctx.database);

    // Get column names that are tags (not 'time', 'value', or fields)
    // This is a simplified version - in practice we'd query metadata
    let table = if let Some(ref m) = stmt.measurement {
        match &m.name {
            MeasurementName::Name(n) => n.clone(),
            MeasurementName::Regex(_) => return Err(TranspileError {
                message: "Regex measurement not supported for SHOW TAG KEYS".to_string(),
            }),
        }
    } else {
        return Err(TranspileError {
            message: "SHOW TAG KEYS requires FROM measurement".to_string(),
        });
    };

    let sql = format!(
        "SELECT column_name AS tagKey FROM information_schema.columns \
         WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}' \
         AND column_name NOT IN ('time', 'date', 'value') \
         AND data_type = 'Utf8' \
         ORDER BY column_name",
        db, ctx.schema, table
    );

    Ok(sql)
}

fn transpile_show_tag_values(stmt: &ShowTagValuesStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    let db = stmt.database.as_ref().unwrap_or(&ctx.database);

    let table = if let Some(ref m) = stmt.measurement {
        match &m.name {
            MeasurementName::Name(n) => n.clone(),
            MeasurementName::Regex(_) => return Err(TranspileError {
                message: "Regex measurement not supported for SHOW TAG VALUES".to_string(),
            }),
        }
    } else {
        return Err(TranspileError {
            message: "SHOW TAG VALUES requires FROM measurement".to_string(),
        });
    };

    let tag_col = match &stmt.tag_key {
        TagKeySpec::Single(key) => key.clone(),
        TagKeySpec::List(keys) => keys.first().cloned().unwrap_or_default(),
        TagKeySpec::Regex(_) => return Err(TranspileError {
            message: "Regex tag key not supported".to_string(),
        }),
    };

    let mut sql = format!(
        "SELECT '{}' AS key, {} AS value FROM {}.{}.{} GROUP BY {}",
        tag_col,
        quote_ident(&tag_col),
        quote_ident(db),
        quote_ident(&ctx.schema),
        quote_ident(&table),
        quote_ident(&tag_col)
    );

    if let Some(ref cond) = stmt.condition {
        sql.push_str(" HAVING ");
        sql.push_str(&transpile_expr(cond)?);
    }

    sql.push_str(" ORDER BY value");

    if let Some(limit) = stmt.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }

    Ok(sql)
}

fn transpile_show_field_keys(stmt: &ShowFieldKeysStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    let db = stmt.database.as_ref().unwrap_or(&ctx.database);

    let table = if let Some(ref m) = stmt.measurement {
        match &m.name {
            MeasurementName::Name(n) => n.clone(),
            MeasurementName::Regex(_) => return Err(TranspileError {
                message: "Regex measurement not supported for SHOW FIELD KEYS".to_string(),
            }),
        }
    } else {
        return Err(TranspileError {
            message: "SHOW FIELD KEYS requires FROM measurement".to_string(),
        });
    };

    let sql = format!(
        "SELECT column_name AS fieldKey, data_type AS fieldType FROM information_schema.columns \
         WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}' \
         AND column_name NOT IN ('time', 'date') \
         AND data_type != 'Utf8' \
         ORDER BY column_name",
        db, ctx.schema, table
    );

    Ok(sql)
}

fn transpile_show_condition(expr: &Expr, column: &str) -> Result<String, TranspileError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Handle measurement name conditions
            if let Expr::Identifier(ref name) = **left {
                if name == "name" || name == "measurement" {
                    let right_sql = transpile_expr(right)?;
                    return Ok(format!("{} {} {}", column, transpile_binary_op(op), right_sql));
                }
            }
            // Otherwise recurse
            let left_sql = transpile_show_condition(left, column)?;
            let right_sql = transpile_show_condition(right, column)?;
            Ok(format!("({} {} {})", left_sql, transpile_binary_op(op), right_sql))
        }
        _ => transpile_expr(expr),
    }
}

fn quote_ident(name: &str) -> String {
    // Quote if contains special chars or is a keyword
    if name.contains(' ') || name.contains('-') || name.contains('.') || is_reserved_word(name) {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn is_reserved_word(word: &str) -> bool {
    matches!(
        word.to_uppercase().as_str(),
        "SELECT" | "FROM" | "WHERE" | "GROUP" | "ORDER" | "BY" | "LIMIT" | "OFFSET" |
        "AND" | "OR" | "NOT" | "AS" | "TRUE" | "FALSE" | "NULL"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::influxql::parser::parse;

    fn ctx() -> TranspileContext {
        TranspileContext {
            database: "testdb".to_string(),
            schema: "default".to_string(),
        }
    }

    #[test]
    fn test_simple_select() {
        let stmt = parse("SELECT * FROM cpu").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("SELECT *"));
        assert!(sql.contains("FROM testdb.default.cpu"));
    }

    #[test]
    fn test_select_with_where() {
        let stmt = parse("SELECT value FROM cpu WHERE host = 'server01'").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("host"));
        assert!(sql.contains("'server01'"));
    }

    #[test]
    fn test_select_with_function() {
        let stmt = parse("SELECT mean(value) FROM cpu").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        println!("SQL: {}", sql);
        assert!(sql.contains("AVG(value)"), "SQL was: {}", sql);
    }

    #[test]
    fn test_select_with_group_by_time() {
        let stmt = parse("SELECT mean(value) FROM cpu GROUP BY time(5m)").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("time_bucket"));
        assert!(sql.contains("5 minute"));
        assert!(sql.contains("GROUP BY"));
    }

    #[test]
    fn test_select_with_limit() {
        let stmt = parse("SELECT * FROM cpu LIMIT 100 OFFSET 50").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("LIMIT 100"));
        assert!(sql.contains("OFFSET 50"));
    }

    #[test]
    fn test_show_measurements() {
        let stmt = parse("SHOW MEASUREMENTS").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("information_schema.tables"));
    }

    #[test]
    fn test_qualified_table() {
        let stmt = parse("SELECT * FROM mydb.myrp.cpu").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("mydb.myrp.cpu"));
    }
}
