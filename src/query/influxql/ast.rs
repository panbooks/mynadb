//! InfluxQL Abstract Syntax Tree
//!
//! Defines the AST nodes for InfluxQL queries.

use std::fmt;

/// Root AST node - a complete InfluxQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    ShowMeasurements(ShowMeasurementsStatement),
    ShowTagKeys(ShowTagKeysStatement),
    ShowTagValues(ShowTagValuesStatement),
    ShowFieldKeys(ShowFieldKeysStatement),
    ShowDatabases,
    ShowRetentionPolicies(Option<String>),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub fields: Vec<Field>,
    pub from: Measurement,
    pub condition: Option<Expr>,
    pub group_by: Option<GroupBy>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub slimit: Option<u64>,
    pub soffset: Option<u64>,
    pub fill: Option<FillOption>,
    pub timezone: Option<String>,
}

/// A field in SELECT clause
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// Expression types
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Wildcard: *
    Wildcard,
    /// Column/field reference
    Identifier(String),
    /// Literal value
    Literal(Literal),
    /// Function call: mean(field), sum(field), etc.
    Function(FunctionCall),
    /// Binary operation: a + b, a > b, etc.
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation: -a, NOT a
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// Regex match: field =~ /pattern/
    Regex(String),
    /// Duration: 1h, 5m, 30s
    Duration(Duration),
    /// now() function
    Now,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

/// Function call
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // Regex
    RegexMatch,    // =~
    RegexNotMatch, // !~
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Minus,
    Not,
}

/// Duration value
#[derive(Debug, Clone, PartialEq)]
pub struct Duration {
    pub value: i64,
    pub unit: DurationUnit,
}

/// Duration units
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl DurationUnit {
    pub fn to_nanoseconds(&self, value: i64) -> i64 {
        match self {
            DurationUnit::Nanosecond => value,
            DurationUnit::Microsecond => value * 1_000,
            DurationUnit::Millisecond => value * 1_000_000,
            DurationUnit::Second => value * 1_000_000_000,
            DurationUnit::Minute => value * 60 * 1_000_000_000,
            DurationUnit::Hour => value * 3600 * 1_000_000_000,
            DurationUnit::Day => value * 86400 * 1_000_000_000,
            DurationUnit::Week => value * 7 * 86400 * 1_000_000_000,
        }
    }

    pub fn to_interval_string(&self) -> &'static str {
        match self {
            DurationUnit::Nanosecond => "nanosecond",
            DurationUnit::Microsecond => "microsecond",
            DurationUnit::Millisecond => "millisecond",
            DurationUnit::Second => "second",
            DurationUnit::Minute => "minute",
            DurationUnit::Hour => "hour",
            DurationUnit::Day => "day",
            DurationUnit::Week => "week",
        }
    }
}

impl Duration {
    pub fn to_nanoseconds(&self) -> i64 {
        self.unit.to_nanoseconds(self.value)
    }

    pub fn to_interval_string(&self) -> String {
        format!("{} {}", self.value, self.unit.to_interval_string())
    }
}

/// Measurement (table) reference
#[derive(Debug, Clone, PartialEq)]
pub struct Measurement {
    pub database: Option<String>,
    pub retention_policy: Option<String>,
    pub name: MeasurementName,
}

/// Measurement name - can be identifier or regex
#[derive(Debug, Clone, PartialEq)]
pub enum MeasurementName {
    Name(String),
    Regex(String),
}

/// GROUP BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct GroupBy {
    pub time: Option<Duration>,
    pub tags: Vec<String>,
    pub fill: Option<FillOption>,
}

/// FILL options for GROUP BY time()
#[derive(Debug, Clone, PartialEq)]
pub enum FillOption {
    None,
    Null,
    Previous,
    Linear,
    Value(f64),
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub time_desc: bool,
}

/// SHOW MEASUREMENTS statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowMeasurementsStatement {
    pub database: Option<String>,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// SHOW TAG KEYS statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTagKeysStatement {
    pub database: Option<String>,
    pub measurement: Option<Measurement>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// SHOW TAG VALUES statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTagValuesStatement {
    pub database: Option<String>,
    pub measurement: Option<Measurement>,
    pub tag_key: TagKeySpec,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// Tag key specification for SHOW TAG VALUES
#[derive(Debug, Clone, PartialEq)]
pub enum TagKeySpec {
    Single(String),
    List(Vec<String>),
    Regex(String),
}

/// SHOW FIELD KEYS statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowFieldKeysStatement {
    pub database: Option<String>,
    pub measurement: Option<Measurement>,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Add => write!(f, "+"),
            BinaryOperator::Sub => write!(f, "-"),
            BinaryOperator::Mul => write!(f, "*"),
            BinaryOperator::Div => write!(f, "/"),
            BinaryOperator::Mod => write!(f, "%"),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::Ne => write!(f, "<>"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::Le => write!(f, "<="),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Ge => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
            BinaryOperator::RegexMatch => write!(f, "=~"),
            BinaryOperator::RegexNotMatch => write!(f, "!~"),
        }
    }
}
