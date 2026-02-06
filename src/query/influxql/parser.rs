//! InfluxQL Parser
//!
//! Parses token stream into AST.

use super::ast::*;
use super::lexer::{scan_regex, Lexer, Token};

/// Parser error
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parse error: {}", self.message)
    }
}

impl std::error::Error for ParseError {}

/// InfluxQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    input: String,
}

impl Parser {
    pub fn new(input: &str) -> Result<Self, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().map_err(|e| ParseError {
            message: e.message,
            position: e.position,
        })?;
        Ok(Self {
            tokens,
            position: 0,
            input: input.to_string(),
        })
    }

    /// Parse the input into a statement
    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        let stmt = self.parse_statement()?;

        // Consume optional semicolon
        if self.check(&Token::Semicolon) {
            self.advance();
        }

        // Ensure we've consumed all tokens
        if !self.check(&Token::Eof) {
            return Err(self.error(&format!(
                "Unexpected token: {:?}",
                self.current()
            )));
        }

        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.current() {
            Token::Select => self.parse_select(),
            Token::Show => self.parse_show(),
            _ => Err(self.error("Expected SELECT or SHOW")),
        }
    }

    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Select)?;

        // Parse fields
        let fields = self.parse_fields()?;

        // Parse FROM
        self.expect(&Token::From)?;
        let from = self.parse_measurement()?;

        // Parse optional clauses
        let condition = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.check(&Token::GroupBy) {
            self.advance();
            self.expect_ident("BY")?;
            Some(self.parse_group_by()?)
        } else {
            None
        };

        let fill = if self.check(&Token::Fill) {
            self.advance();
            self.expect(&Token::LParen)?;
            let fill = self.parse_fill_option()?;
            self.expect(&Token::RParen)?;
            Some(fill)
        } else {
            None
        };

        let order_by = if self.check(&Token::OrderBy) {
            self.advance();
            self.expect_ident("BY")?;
            Some(self.parse_order_by()?)
        } else {
            None
        };

        let limit = self.parse_optional_limit()?;
        let offset = self.parse_optional_offset()?;
        let slimit = self.parse_optional_slimit()?;
        let soffset = self.parse_optional_soffset()?;

        let timezone = if self.check(&Token::Tz) {
            self.advance();
            self.expect(&Token::LParen)?;
            let tz = self.expect_string()?;
            self.expect(&Token::RParen)?;
            Some(tz)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            fields,
            from,
            condition,
            group_by,
            order_by,
            limit,
            offset,
            slimit,
            soffset,
            fill,
            timezone,
        }))
    }

    fn parse_fields(&mut self) -> Result<Vec<Field>, ParseError> {
        let mut fields = Vec::new();

        loop {
            let expr = self.parse_field_expr()?;

            let alias = if self.check(&Token::As) {
                self.advance();
                Some(self.expect_ident_any()?)
            } else {
                None
            };

            fields.push(Field { expr, alias });

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(fields)
    }

    fn parse_field_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_additive()
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative()?;

        loop {
            let op = match self.current() {
                Token::Plus => BinaryOperator::Add,
                Token::Minus => BinaryOperator::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;

        loop {
            let op = match self.current() {
                Token::Star => BinaryOperator::Mul,
                Token::Slash => BinaryOperator::Div,
                Token::Percent => BinaryOperator::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        match self.current() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.current().clone() {
            Token::Star => {
                self.advance();
                Ok(Expr::Wildcard)
            }
            Token::Now => {
                self.advance();
                if self.check(&Token::LParen) {
                    self.advance();
                    self.expect(&Token::RParen)?;
                }
                Ok(Expr::Now)
            }
            Token::Integer(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(f)))
            }
            Token::String(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Duration(value, unit) => {
                self.advance();
                let duration_unit = parse_duration_unit(&unit)?;
                Ok(Expr::Duration(Duration {
                    value,
                    unit: duration_unit,
                }))
            }
            Token::Ident(name) => {
                self.advance();
                if self.check(&Token::LParen) {
                    // Function call
                    self.advance();
                    let args = self.parse_function_args()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::Function(FunctionCall { name, args }))
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            Token::Time => {
                self.advance();
                Ok(Expr::Identifier("time".to_string()))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            _ => Err(self.error(&format!("Unexpected token in expression: {:?}", self.current()))),
        }
    }

    fn parse_function_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();

        if self.check(&Token::RParen) {
            return Ok(args);
        }

        loop {
            args.push(self.parse_field_expr()?);
            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(args)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;

        while self.check(&Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;

        while self.check(&Token::And) {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_additive()?;

        let op = match self.current() {
            Token::Eq => BinaryOperator::Eq,
            Token::Ne => BinaryOperator::Ne,
            Token::Lt => BinaryOperator::Lt,
            Token::Le => BinaryOperator::Le,
            Token::Gt => BinaryOperator::Gt,
            Token::Ge => BinaryOperator::Ge,
            Token::RegexMatch => BinaryOperator::RegexMatch,
            Token::RegexNotMatch => BinaryOperator::RegexNotMatch,
            _ => return Ok(left),
        };
        self.advance();

        // Handle regex patterns for =~ and !~
        if op == BinaryOperator::RegexMatch || op == BinaryOperator::RegexNotMatch {
            let pattern = self.parse_regex_pattern()?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(Expr::Regex(pattern)),
            });
        }

        let right = self.parse_additive()?;
        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_regex_pattern(&mut self) -> Result<String, ParseError> {
        // The regex should be in the format /pattern/
        // Simplified approach - parse tokens between slashes
        if let Token::Slash = self.current() {
            self.advance();
            // Collect until next /
            let mut pattern = String::new();
            loop {
                match self.current() {
                    Token::Slash => {
                        self.advance();
                        return Ok(pattern);
                    }
                    Token::Ident(s) => {
                        pattern.push_str(&s);
                        self.advance();
                    }
                    Token::Star => {
                        pattern.push('*');
                        self.advance();
                    }
                    Token::Dot => {
                        pattern.push('.');
                        self.advance();
                    }
                    Token::Eof => {
                        return Err(self.error("Unterminated regex"));
                    }
                    _ => {
                        return Err(self.error(&format!("Unexpected token in regex: {:?}", self.current())));
                    }
                }
            }
        }

        // If not a slash-delimited regex, try string
        if let Token::String(s) = self.current().clone() {
            self.advance();
            return Ok(s);
        }

        Err(self.error("Expected regex pattern"))
    }

    fn parse_measurement(&mut self) -> Result<Measurement, ParseError> {
        let mut database = None;
        let mut retention_policy = None;

        // Parse measurement name (possibly qualified)
        let name = self.expect_ident_any()?;

        // Check for qualified names: db.rp.measurement or db..measurement
        if self.check(&Token::Dot) {
            self.advance();

            if self.check(&Token::Dot) {
                // db..measurement
                self.advance();
                database = Some(name);
                let measurement_name = self.expect_ident_any()?;
                return Ok(Measurement {
                    database,
                    retention_policy,
                    name: MeasurementName::Name(measurement_name),
                });
            } else {
                // Could be db.rp.measurement or db.measurement
                let second = self.expect_ident_any()?;

                if self.check(&Token::Dot) {
                    self.advance();
                    database = Some(name);
                    retention_policy = Some(second);
                    let measurement_name = self.expect_ident_any()?;
                    return Ok(Measurement {
                        database,
                        retention_policy,
                        name: MeasurementName::Name(measurement_name),
                    });
                } else {
                    // db.measurement
                    database = Some(name);
                    return Ok(Measurement {
                        database,
                        retention_policy,
                        name: MeasurementName::Name(second),
                    });
                }
            }
        }

        Ok(Measurement {
            database,
            retention_policy,
            name: MeasurementName::Name(name),
        })
    }

    fn parse_group_by(&mut self) -> Result<GroupBy, ParseError> {
        let mut time = None;
        let mut tags = Vec::new();
        let mut fill = None;

        loop {
            if self.check(&Token::Time) {
                self.advance();
                self.expect(&Token::LParen)?;
                time = Some(self.parse_duration_value()?);
                self.expect(&Token::RParen)?;

                // Check for fill within GROUP BY
                if self.check(&Token::Fill) {
                    self.advance();
                    self.expect(&Token::LParen)?;
                    fill = Some(self.parse_fill_option()?);
                    self.expect(&Token::RParen)?;
                }
            } else if let Token::Ident(name) = self.current().clone() {
                self.advance();
                tags.push(name);
            } else if self.check(&Token::Star) {
                self.advance();
                tags.push("*".to_string());
            } else {
                break;
            }

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(GroupBy { time, tags, fill })
    }

    fn parse_duration_value(&mut self) -> Result<Duration, ParseError> {
        if let Token::Duration(value, unit) = self.current().clone() {
            self.advance();
            let duration_unit = parse_duration_unit(&unit)?;
            Ok(Duration {
                value,
                unit: duration_unit,
            })
        } else {
            Err(self.error("Expected duration"))
        }
    }

    fn parse_fill_option(&mut self) -> Result<FillOption, ParseError> {
        match self.current().clone() {
            Token::None => {
                self.advance();
                Ok(FillOption::None)
            }
            Token::Null => {
                self.advance();
                Ok(FillOption::Null)
            }
            Token::Previous => {
                self.advance();
                Ok(FillOption::Previous)
            }
            Token::Linear => {
                self.advance();
                Ok(FillOption::Linear)
            }
            Token::Integer(n) => {
                self.advance();
                Ok(FillOption::Value(n as f64))
            }
            Token::Float(f) => {
                self.advance();
                Ok(FillOption::Value(f))
            }
            _ => Err(self.error("Expected fill option")),
        }
    }

    fn parse_order_by(&mut self) -> Result<OrderBy, ParseError> {
        // ORDER BY time [ASC|DESC]
        self.expect(&Token::Time)?;

        let time_desc = if self.check(&Token::Desc) {
            self.advance();
            true
        } else if self.check(&Token::Asc) {
            self.advance();
            false
        } else {
            false // Default to ASC
        };

        Ok(OrderBy { time_desc })
    }

    fn parse_optional_limit(&mut self) -> Result<Option<u64>, ParseError> {
        if self.check(&Token::Limit) {
            self.advance();
            let n = self.expect_integer()?;
            Ok(Some(n as u64))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_offset(&mut self) -> Result<Option<u64>, ParseError> {
        if self.check(&Token::Offset) {
            self.advance();
            let n = self.expect_integer()?;
            Ok(Some(n as u64))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_slimit(&mut self) -> Result<Option<u64>, ParseError> {
        if self.check(&Token::SLimit) {
            self.advance();
            let n = self.expect_integer()?;
            Ok(Some(n as u64))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_soffset(&mut self) -> Result<Option<u64>, ParseError> {
        if self.check(&Token::SOffset) {
            self.advance();
            let n = self.expect_integer()?;
            Ok(Some(n as u64))
        } else {
            Ok(None)
        }
    }

    fn parse_show(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Show)?;

        match self.current() {
            Token::Measurements => {
                self.advance();
                self.parse_show_measurements()
            }
            Token::Databases => {
                self.advance();
                Ok(Statement::ShowDatabases)
            }
            Token::Tag => {
                self.advance();
                match self.current() {
                    Token::Keys => {
                        self.advance();
                        self.parse_show_tag_keys()
                    }
                    Token::Values => {
                        self.advance();
                        self.parse_show_tag_values()
                    }
                    _ => Err(self.error("Expected KEYS or VALUES after TAG")),
                }
            }
            Token::Field => {
                self.advance();
                self.expect(&Token::Keys)?;
                self.parse_show_field_keys()
            }
            Token::RetentionPolicies => {
                self.advance();
                // SHOW RETENTION POLICIES [ON database]
                if self.check(&Token::Ident(String::new())) {
                    if let Token::Ident(s) = self.current() {
                        if s.to_uppercase() == "POLICIES" {
                            self.advance();
                        }
                    }
                }
                let db = if self.check(&Token::On) {
                    self.advance();
                    Some(self.expect_ident_any()?)
                } else {
                    None
                };
                Ok(Statement::ShowRetentionPolicies(db))
            }
            _ => Err(self.error("Expected MEASUREMENTS, DATABASES, TAG, FIELD, or RETENTION")),
        }
    }

    fn parse_show_measurements(&mut self) -> Result<Statement, ParseError> {
        let database = if self.check(&Token::On) {
            self.advance();
            Some(self.expect_ident_any()?)
        } else {
            None
        };

        let condition = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let limit = self.parse_optional_limit()?;
        let offset = self.parse_optional_offset()?;

        Ok(Statement::ShowMeasurements(ShowMeasurementsStatement {
            database,
            condition,
            limit,
            offset,
        }))
    }

    fn parse_show_tag_keys(&mut self) -> Result<Statement, ParseError> {
        let database = if self.check(&Token::On) {
            self.advance();
            Some(self.expect_ident_any()?)
        } else {
            None
        };

        let measurement = if self.check(&Token::From) {
            self.advance();
            Some(self.parse_measurement()?)
        } else {
            None
        };

        let limit = self.parse_optional_limit()?;
        let offset = self.parse_optional_offset()?;

        Ok(Statement::ShowTagKeys(ShowTagKeysStatement {
            database,
            measurement,
            limit,
            offset,
        }))
    }

    fn parse_show_tag_values(&mut self) -> Result<Statement, ParseError> {
        let database = if self.check(&Token::On) {
            self.advance();
            Some(self.expect_ident_any()?)
        } else {
            None
        };

        let measurement = if self.check(&Token::From) {
            self.advance();
            Some(self.parse_measurement()?)
        } else {
            None
        };

        // WITH KEY = "tag" or WITH KEY IN ("tag1", "tag2")
        self.expect(&Token::With)?;
        self.expect(&Token::Key)?;

        let tag_key = if self.check(&Token::Eq) {
            self.advance();
            TagKeySpec::Single(self.expect_string_or_ident()?)
        } else if self.check(&Token::In) {
            self.advance();
            self.expect(&Token::LParen)?;
            let mut keys = Vec::new();
            loop {
                keys.push(self.expect_string_or_ident()?);
                if !self.check(&Token::Comma) {
                    break;
                }
                self.advance();
            }
            self.expect(&Token::RParen)?;
            TagKeySpec::List(keys)
        } else if self.check(&Token::RegexMatch) {
            self.advance();
            let pattern = self.parse_regex_pattern()?;
            TagKeySpec::Regex(pattern)
        } else {
            return Err(self.error("Expected =, IN, or =~ after KEY"));
        };

        let condition = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let limit = self.parse_optional_limit()?;
        let offset = self.parse_optional_offset()?;

        Ok(Statement::ShowTagValues(ShowTagValuesStatement {
            database,
            measurement,
            tag_key,
            condition,
            limit,
            offset,
        }))
    }

    fn parse_show_field_keys(&mut self) -> Result<Statement, ParseError> {
        let database = if self.check(&Token::On) {
            self.advance();
            Some(self.expect_ident_any()?)
        } else {
            None
        };

        let measurement = if self.check(&Token::From) {
            self.advance();
            Some(self.parse_measurement()?)
        } else {
            None
        };

        Ok(Statement::ShowFieldKeys(ShowFieldKeysStatement {
            database,
            measurement,
        }))
    }

    // Helper methods

    fn current(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        self.position += 1;
        self.tokens.get(self.position - 1).unwrap_or(&Token::Eof)
    }

    fn check(&self, expected: &Token) -> bool {
        std::mem::discriminant(self.current()) == std::mem::discriminant(expected)
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        if self.check(expected) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("Expected {:?}, got {:?}", expected, self.current())))
        }
    }

    fn expect_ident(&mut self, name: &str) -> Result<(), ParseError> {
        if let Token::Ident(s) = self.current() {
            if s.to_uppercase() == name.to_uppercase() {
                self.advance();
                return Ok(());
            }
        }
        Err(self.error(&format!("Expected '{}'", name)))
    }

    fn expect_ident_any(&mut self) -> Result<String, ParseError> {
        match self.current().clone() {
            Token::Ident(s) => {
                self.advance();
                Ok(s)
            }
            Token::String(s) => {
                self.advance();
                Ok(s)
            }
            // Allow keywords as identifiers in some contexts
            Token::Time => {
                self.advance();
                Ok("time".to_string())
            }
            _ => Err(self.error(&format!("Expected identifier, got {:?}", self.current()))),
        }
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        if let Token::String(s) = self.current().clone() {
            self.advance();
            Ok(s)
        } else {
            Err(self.error("Expected string"))
        }
    }

    fn expect_string_or_ident(&mut self) -> Result<String, ParseError> {
        match self.current().clone() {
            Token::String(s) => {
                self.advance();
                Ok(s)
            }
            Token::Ident(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(self.error("Expected string or identifier")),
        }
    }

    fn expect_integer(&mut self) -> Result<i64, ParseError> {
        if let Token::Integer(n) = self.current() {
            let n = *n;
            self.advance();
            Ok(n)
        } else {
            Err(self.error("Expected integer"))
        }
    }

    fn error(&self, message: &str) -> ParseError {
        ParseError {
            message: message.to_string(),
            position: self.position,
        }
    }
}

fn parse_duration_unit(unit: &str) -> Result<DurationUnit, ParseError> {
    match unit {
        "ns" => Ok(DurationUnit::Nanosecond),
        "u" | "µ" | "us" => Ok(DurationUnit::Microsecond),
        "ms" => Ok(DurationUnit::Millisecond),
        "s" => Ok(DurationUnit::Second),
        "m" => Ok(DurationUnit::Minute),
        "h" => Ok(DurationUnit::Hour),
        "d" => Ok(DurationUnit::Day),
        "w" => Ok(DurationUnit::Week),
        _ => Err(ParseError {
            message: format!("Unknown duration unit: {}", unit),
            position: 0,
        }),
    }
}

/// Parse an InfluxQL query string into an AST
pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let mut parser = Parser::new(input)?;
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let stmt = parse("SELECT * FROM cpu").unwrap();
        if let Statement::Select(s) = stmt {
            assert_eq!(s.fields.len(), 1);
            assert!(matches!(s.fields[0].expr, Expr::Wildcard));
            assert!(matches!(s.from.name, MeasurementName::Name(ref n) if n == "cpu"));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_fields() {
        let stmt = parse("SELECT value, usage FROM cpu").unwrap();
        if let Statement::Select(s) = stmt {
            assert_eq!(s.fields.len(), 2);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_function() {
        let stmt = parse("SELECT mean(value) FROM cpu").unwrap();
        if let Statement::Select(s) = stmt {
            assert_eq!(s.fields.len(), 1);
            if let Expr::Function(f) = &s.fields[0].expr {
                assert_eq!(f.name, "mean");
                assert_eq!(f.args.len(), 1);
            } else {
                panic!("Expected function");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_where() {
        let stmt = parse("SELECT * FROM cpu WHERE host = 'server01'").unwrap();
        if let Statement::Select(s) = stmt {
            assert!(s.condition.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_time_condition() {
        let stmt = parse("SELECT * FROM cpu WHERE time > now() - 1h").unwrap();
        if let Statement::Select(s) = stmt {
            assert!(s.condition.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_group_by_time() {
        let stmt = parse("SELECT mean(value) FROM cpu GROUP BY time(5m)").unwrap();
        if let Statement::Select(s) = stmt {
            assert!(s.group_by.is_some());
            let gb = s.group_by.unwrap();
            assert!(gb.time.is_some());
            let time = gb.time.unwrap();
            assert_eq!(time.value, 5);
            assert_eq!(time.unit, DurationUnit::Minute);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_group_by_tag() {
        let stmt = parse("SELECT mean(value) FROM cpu GROUP BY host").unwrap();
        if let Statement::Select(s) = stmt {
            let gb = s.group_by.unwrap();
            assert!(gb.tags.contains(&"host".to_string()));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_fill() {
        let stmt = parse("SELECT mean(value) FROM cpu GROUP BY time(5m) fill(previous)").unwrap();
        if let Statement::Select(s) = stmt {
            let gb = s.group_by.unwrap();
            assert!(matches!(gb.fill, Some(FillOption::Previous)));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_with_limit() {
        let stmt = parse("SELECT * FROM cpu LIMIT 100").unwrap();
        if let Statement::Select(s) = stmt {
            assert_eq!(s.limit, Some(100));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_show_measurements() {
        let stmt = parse("SHOW MEASUREMENTS").unwrap();
        assert!(matches!(stmt, Statement::ShowMeasurements(_)));
    }

    #[test]
    fn test_show_tag_keys() {
        let stmt = parse("SHOW TAG KEYS FROM cpu").unwrap();
        if let Statement::ShowTagKeys(s) = stmt {
            assert!(s.measurement.is_some());
        } else {
            panic!("Expected SHOW TAG KEYS statement");
        }
    }

    #[test]
    fn test_show_databases() {
        let stmt = parse("SHOW DATABASES").unwrap();
        assert!(matches!(stmt, Statement::ShowDatabases));
    }

    #[test]
    fn test_qualified_measurement() {
        let stmt = parse("SELECT * FROM mydb.myrp.cpu").unwrap();
        if let Statement::Select(s) = stmt {
            assert_eq!(s.from.database, Some("mydb".to_string()));
            assert_eq!(s.from.retention_policy, Some("myrp".to_string()));
            assert!(matches!(s.from.name, MeasurementName::Name(ref n) if n == "cpu"));
        } else {
            panic!("Expected SELECT statement");
        }
    }
}
