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
    // Check if any field uses derivative(aggregate(...)) which needs a CTE
    if needs_cte(stmt) {
        return transpile_select_with_cte(stmt, ctx);
    }

    let mut sql = String::new();

    // SELECT clause
    sql.push_str("SELECT ");

    // Handle GROUP BY time() with aggregations
    let has_time_group = stmt.group_by.as_ref().map(|g| g.time.is_some()).unwrap_or(false);

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

/// Check if a SELECT statement needs a CTE (derivative wrapping an aggregate)
fn needs_cte(stmt: &SelectStatement) -> bool {
    stmt.fields.iter().any(|f| expr_has_derivative_of_aggregate(&f.expr))
}

/// Recursively check if an expression contains derivative(aggregate(...))
fn expr_has_derivative_of_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_lowercase();
            if (name == "derivative" || name == "non_negative_derivative") && !func.args.is_empty() {
                // Check if the first argument is an aggregate function
                if let Expr::Function(inner) = &func.args[0] {
                    return is_aggregate_function(&inner.name);
                }
            }
            // Recurse into function args
            func.args.iter().any(expr_has_derivative_of_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_derivative_of_aggregate(left) || expr_has_derivative_of_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_derivative_of_aggregate(expr),
        _ => false,
    }
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "mean" | "sum" | "count" | "min" | "max" | "first" | "last" | "median"
            | "mode" | "stddev" | "spread" | "percentile"
    )
}

/// Information about a derivative field extracted for CTE generation
struct DerivativeField {
    /// Index in the fields list
    idx: usize,
    /// The derivative function name (derivative or non_negative_derivative)
    func_name: String,
    /// The inner aggregate expression (e.g., mean("bytes_recv"))
    inner_agg: Expr,
    /// The interval argument (if any), in seconds
    interval_secs: Option<f64>,
    /// Outer expression wrapping the derivative (for arithmetic like * 8)
    /// None if the derivative is the top-level expression
    outer_wrapper: Option<Box<Expr>>,
    /// Alias for the field
    alias: Option<String>,
}

/// Extract derivative info from an expression, returning the DerivativeField info
/// and a placeholder expression. Returns None if the expression doesn't contain
/// a derivative-of-aggregate.
fn extract_derivative_info(expr: &Expr, idx: usize) -> Option<DerivativeField> {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_lowercase();
            if (name == "derivative" || name == "non_negative_derivative") && !func.args.is_empty() {
                if let Expr::Function(inner) = &func.args[0] {
                    if is_aggregate_function(&inner.name) {
                        let interval_secs = if func.args.len() > 1 {
                            match &func.args[1] {
                                Expr::Duration(d) => Some(d.to_nanoseconds() as f64 / 1_000_000_000.0),
                                _ => None,
                            }
                        } else {
                            None
                        };
                        return Some(DerivativeField {
                            idx,
                            func_name: name,
                            inner_agg: Expr::Function(inner.clone()),
                            interval_secs,
                            outer_wrapper: None,
                            alias: None,
                        });
                    }
                }
            }
            None
        }
        Expr::BinaryOp { left, op, right } => {
            // Check if left side has derivative (e.g., non_negative_derivative(...) * 8)
            if let Some(mut info) = extract_derivative_info(left, idx) {
                info.outer_wrapper = Some(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("__PLACEHOLDER__".to_string())),
                    op: *op,
                    right: right.clone(),
                }));
                return Some(info);
            }
            // Check if right side has derivative (unlikely but handle it)
            if let Some(mut info) = extract_derivative_info(right, idx) {
                info.outer_wrapper = Some(Box::new(Expr::BinaryOp {
                    left: left.clone(),
                    op: *op,
                    right: Box::new(Expr::Identifier("__PLACEHOLDER__".to_string())),
                }));
                return Some(info);
            }
            None
        }
        Expr::UnaryOp { op, expr: inner } => {
            if let Some(mut info) = extract_derivative_info(inner, idx) {
                info.outer_wrapper = Some(Box::new(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(Expr::Identifier("__PLACEHOLDER__".to_string())),
                }));
                return Some(info);
            }
            None
        }
        _ => None,
    }
}

/// Transpile a SELECT with derivative(aggregate()) using a CTE approach
fn transpile_select_with_cte(stmt: &SelectStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    let has_time_group = stmt.group_by.as_ref().map(|g| g.time.is_some()).unwrap_or(false);
    let time_duration = stmt.group_by.as_ref().and_then(|gb| gb.time.as_ref());

    // Collect GROUP BY tags (excluding * and time)
    let group_tags: Vec<String> = stmt.group_by.as_ref()
        .map(|gb| gb.tags.iter().filter(|t| *t != "*").cloned().collect())
        .unwrap_or_default();

    // Extract derivative fields
    let mut deriv_fields = Vec::new();
    for (i, field) in stmt.fields.iter().enumerate() {
        if let Some(mut info) = extract_derivative_info(&field.expr, i) {
            info.alias = field.alias.clone();
            deriv_fields.push(info);
        }
    }

    if deriv_fields.is_empty() {
        // Shouldn't happen since needs_cte returned true, but fall back
        return transpile_select_simple(stmt, ctx);
    }

    // Calculate the time bucket interval in seconds for derivative calculation
    let bucket_secs = time_duration
        .map(|d| d.to_nanoseconds() as f64 / 1_000_000_000.0)
        .unwrap_or(1.0);

    // --- Build the CTE (inner subquery) ---
    let mut cte = String::new();
    cte.push_str("WITH _derivative_sub AS (\n    SELECT ");

    // Time bucket column
    if has_time_group {
        if let Some(ref duration) = time_duration {
            cte.push_str(&format!(
                "time_bucket('{}', time) AS time",
                duration.to_interval_string()
            ));
        }
    } else {
        cte.push_str("time");
    }

    // Inner aggregate columns
    for (i, df) in deriv_fields.iter().enumerate() {
        let agg_sql = transpile_expr(&df.inner_agg)?;
        cte.push_str(&format!(",\n           {} AS \"_deriv_val_{}\"", agg_sql, i));
    }

    // Group tags
    for tag in &group_tags {
        cte.push_str(&format!(",\n           {}", quote_ident(tag)));
    }

    // FROM clause
    cte.push_str(&format!("\n    FROM {}", transpile_measurement(&stmt.from, ctx)));

    // WHERE clause
    if let Some(ref cond) = stmt.condition {
        cte.push_str(&format!("\n    WHERE {}", transpile_expr(cond)?));
    }

    // GROUP BY clause
    if has_time_group || !group_tags.is_empty() {
        let mut parts = Vec::new();
        if has_time_group {
            parts.push("time_bucket".to_string());
        }
        for tag in &group_tags {
            parts.push(quote_ident(tag));
        }
        cte.push_str(&format!("\n    GROUP BY {}", parts.join(", ")));
    }

    // ORDER BY
    if has_time_group {
        cte.push_str("\n    ORDER BY time ASC");
    }

    cte.push_str("\n)\n");

    // --- Build the outer SELECT ---
    let mut outer = String::new();
    outer.push_str("SELECT\n    ");

    let mut select_parts = Vec::new();

    for (i, df) in deriv_fields.iter().enumerate() {
        let val_col = format!("\"_deriv_val_{}\"", i);

        // Build PARTITION BY clause for window function
        let partition_clause = if !group_tags.is_empty() {
            let parts: Vec<String> = group_tags.iter().map(|t| quote_ident(t)).collect();
            format!("PARTITION BY {} ", parts.join(", "))
        } else {
            String::new()
        };

        // Calculate the divisor: derivative normalizes by interval_secs / bucket_secs
        // If interval_secs is specified, divide the difference by (bucket_secs / interval_secs)
        // derivative formula: (current - prev) / (bucket_secs / interval_secs)
        let divisor = if let Some(interval_secs) = df.interval_secs {
            bucket_secs / interval_secs
        } else {
            1.0
        };

        // Build the derivative expression
        let deriv_expr = if divisor == 1.0 {
            format!(
                "({val_col} - LAG({val_col}) OVER ({partition}ORDER BY time))",
                val_col = val_col,
                partition = partition_clause,
            )
        } else {
            format!(
                "({val_col} - LAG({val_col}) OVER ({partition}ORDER BY time)) / {divisor}",
                val_col = val_col,
                partition = partition_clause,
                divisor = format_float(divisor),
            )
        };

        // Apply non-negative clamp
        let clamped = if df.func_name == "non_negative_derivative" {
            format!("GREATEST(0, {})", deriv_expr)
        } else {
            deriv_expr
        };

        // Apply outer wrapper (e.g., * 8)
        let final_expr = if let Some(ref wrapper) = df.outer_wrapper {
            let wrapper_sql = transpile_expr(wrapper)?;
            wrapper_sql.replace("\"__PLACEHOLDER__\"", &clamped)
        } else {
            clamped
        };

        // Alias
        let alias = df.alias.as_ref().map(|a| a.as_str()).unwrap_or(&df.func_name);
        select_parts.push(format!("{} AS \"{}\"", final_expr, alias.replace('"', "\"\"")));
    }

    // Add time column
    select_parts.push("time".to_string());

    // Add group tags
    for tag in &group_tags {
        select_parts.push(quote_ident(tag));
    }

    outer.push_str(&select_parts.join(",\n    "));
    outer.push_str("\nFROM _derivative_sub");

    // ORDER BY
    outer.push_str("\nORDER BY time ASC");

    // LIMIT/OFFSET
    if let Some(limit) = stmt.limit {
        outer.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = stmt.offset {
        outer.push_str(&format!(" OFFSET {}", offset));
    }

    Ok(format!("{}{}", cte, outer))
}

/// Simple transpile (same as transpile_select but callable from CTE fallback)
fn transpile_select_simple(stmt: &SelectStatement, ctx: &TranspileContext) -> Result<String, TranspileError> {
    // This is identical to the non-CTE path in transpile_select
    let mut sql = String::new();
    sql.push_str("SELECT ");
    let has_time_group = stmt.group_by.as_ref().map(|g| g.time.is_some()).unwrap_or(false);
    let fields: Vec<String> = stmt.fields.iter()
        .map(|f| transpile_field(f, has_time_group))
        .collect::<Result<Vec<_>, _>>()?;
    sql.push_str(&fields.join(", "));
    if has_time_group {
        if let Some(ref gb) = stmt.group_by {
            if let Some(ref duration) = gb.time {
                sql.push_str(&format!(", time_bucket('{}', time) AS time", duration.to_interval_string()));
            }
        }
    }
    sql.push_str(" FROM ");
    sql.push_str(&transpile_measurement(&stmt.from, ctx));
    if let Some(ref cond) = stmt.condition {
        sql.push_str(" WHERE ");
        sql.push_str(&transpile_expr(cond)?);
    }
    if let Some(ref gb) = stmt.group_by {
        let mut group_parts = Vec::new();
        if gb.time.is_some() { group_parts.push("time_bucket".to_string()); }
        for tag in &gb.tags {
            if tag != "*" { group_parts.push(quote_ident(tag)); }
        }
        if !group_parts.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&group_parts.join(", "));
        }
    }
    if let Some(ref ob) = stmt.order_by {
        sql.push_str(" ORDER BY time");
        if ob.time_desc { sql.push_str(" DESC"); } else { sql.push_str(" ASC"); }
    } else if has_time_group {
        sql.push_str(" ORDER BY time_bucket ASC");
    }
    if let Some(limit) = stmt.limit { sql.push_str(&format!(" LIMIT {}", limit)); }
    if let Some(offset) = stmt.offset { sql.push_str(&format!(" OFFSET {}", offset)); }
    Ok(sql)
}

/// Format a float value cleanly (avoid unnecessary decimal points)
fn format_float(v: f64) -> String {
    if v == v.floor() {
        format!("{:.1}", v)
    } else {
        format!("{}", v)
    }
}

fn transpile_field(field: &Field, _has_time_group: bool) -> Result<String, TranspileError> {
    let expr_sql = transpile_expr(&field.expr)?;

    if let Some(ref alias) = field.alias {
        Ok(format!("{} AS \"{}\"", expr_sql, alias.replace('"', "\"\"")))
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
    // Always double-quote identifiers to avoid conflicts with SQL reserved words
    // (e.g., "user", "system", "time", "host")
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
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
        assert!(sql.contains("SELECT *"), "SQL was: {}", sql);
        assert!(sql.contains(r#""testdb"."default"."cpu""#), "SQL was: {}", sql);
    }

    #[test]
    fn test_select_with_where() {
        let stmt = parse("SELECT value FROM cpu WHERE host = 'server01'").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("WHERE"), "SQL was: {}", sql);
        assert!(sql.contains(r#""host""#), "SQL was: {}", sql);
        assert!(sql.contains("'server01'"), "SQL was: {}", sql);
    }

    #[test]
    fn test_select_with_function() {
        let stmt = parse("SELECT mean(value) FROM cpu").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains(r#"AVG("value")"#), "SQL was: {}", sql);
    }

    #[test]
    fn test_select_with_group_by_time() {
        let stmt = parse("SELECT mean(value) FROM cpu GROUP BY time(5m)").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("time_bucket"), "SQL was: {}", sql);
        assert!(sql.contains("5 minute"), "SQL was: {}", sql);
        assert!(sql.contains("GROUP BY"), "SQL was: {}", sql);
    }

    #[test]
    fn test_select_with_limit() {
        let stmt = parse("SELECT * FROM cpu LIMIT 100 OFFSET 50").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("LIMIT 100"), "SQL was: {}", sql);
        assert!(sql.contains("OFFSET 50"), "SQL was: {}", sql);
    }

    #[test]
    fn test_show_measurements() {
        let stmt = parse("SHOW MEASUREMENTS").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("information_schema.tables"), "SQL was: {}", sql);
    }

    #[test]
    fn test_qualified_table() {
        let stmt = parse("SELECT * FROM mydb.myrp.cpu").unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains(r#""mydb"."myrp"."cpu""#), "SQL was: {}", sql);
    }

    #[test]
    fn test_double_quoted_identifiers() {
        // Double-quoted identifiers in InfluxQL are column references, not strings
        let stmt = parse(r#"SELECT mean("usage_user") FROM "cpu" WHERE "host" = 'server01'"#).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains(r#"AVG("usage_user")"#), "SQL was: {}", sql);
        assert!(sql.contains(r#""host""#), "SQL was: {}", sql);
        assert!(sql.contains("'server01'"), "SQL was: {}", sql);
    }

    #[test]
    fn test_alias_with_reserved_word() {
        // Aliases like "user", "system" are SQL reserved words
        let stmt = parse(r#"SELECT mean("usage_user") AS user FROM "cpu""#).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains(r#"AS "user""#), "SQL was: {}", sql);
    }

    #[test]
    fn test_alias_quoting() {
        let stmt = parse(r#"SELECT mean("usage_system") AS system FROM "cpu""#).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains(r#"AS "system""#), "SQL was: {}", sql);
    }

    #[test]
    fn test_non_negative_derivative_mean() {
        // non_negative_derivative(mean("bytes_recv"), 1s) generates CTE
        let stmt = parse(
            r#"SELECT non_negative_derivative(mean("bytes_recv"), 1s) FROM "net" GROUP BY time(5m)"#
        ).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("WITH _derivative_sub AS"), "SQL was: {}", sql);
        assert!(sql.contains(r#"AVG("bytes_recv")"#), "SQL was: {}", sql);
        assert!(sql.contains("LAG"), "SQL was: {}", sql);
        assert!(sql.contains("GREATEST(0,"), "SQL was: {}", sql);
        assert!(sql.contains("/ 300.0"), "SQL was: {}", sql); // 5m bucket / 1s interval = 300
    }

    #[test]
    fn test_non_negative_derivative_with_arithmetic() {
        // non_negative_derivative(mean("bytes_recv"), 1s) * 8
        let stmt = parse(
            r#"SELECT non_negative_derivative(mean("bytes_recv"), 1s) * 8 FROM "net" GROUP BY time(5m)"#
        ).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("WITH _derivative_sub AS"), "SQL was: {}", sql);
        assert!(sql.contains("* 8"), "SQL was: {}", sql);
        assert!(sql.contains("GREATEST(0,"), "SQL was: {}", sql);
    }

    #[test]
    fn test_derivative_with_group_by_tags() {
        // Derivative with GROUP BY time and tag should include PARTITION BY
        let stmt = parse(
            r#"SELECT non_negative_derivative(mean("bytes_recv"), 1s) FROM "net" GROUP BY time(5m), "interface""#
        ).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("WITH _derivative_sub AS"), "SQL was: {}", sql);
        assert!(sql.contains(r#"PARTITION BY "interface""#), "SQL was: {}", sql);
        assert!(sql.contains(r#""interface""#), "SQL was: {}", sql);
    }

    #[test]
    fn test_plain_derivative() {
        // Plain derivative (without non_negative) should NOT wrap in GREATEST
        let stmt = parse(
            r#"SELECT derivative(mean("value"), 1s) FROM "cpu" GROUP BY time(1m)"#
        ).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("WITH _derivative_sub AS"), "SQL was: {}", sql);
        assert!(sql.contains("LAG"), "SQL was: {}", sql);
        assert!(!sql.contains("GREATEST"), "SQL was: {}", sql);
    }

    #[test]
    fn test_regex_where_clause() {
        // Regex patterns like /^server1$/ should parse correctly
        let stmt = parse(r#"SELECT * FROM "cpu" WHERE "host" =~ /^server1$/"#).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("~"), "SQL was: {}", sql);
        assert!(sql.contains("^server1$"), "SQL was: {}", sql);
    }

    #[test]
    fn test_group_by_star_ignored() {
        // GROUP BY * should not break the query
        let stmt = parse(r#"SELECT mean("value") FROM "cpu" GROUP BY time(5m), *"#).unwrap();
        let sql = transpile(&stmt, &ctx()).unwrap();
        assert!(sql.contains("GROUP BY time_bucket"), "SQL was: {}", sql);
        // * should be silently skipped
        assert!(!sql.contains("GROUP BY time_bucket, *"), "SQL was: {}", sql);
    }
}
