//! Aggregate Definition Types
//!
//! Defines the structure of a continuous aggregate, including:
//! - Source table and target aggregate table
//! - Aggregation functions (avg, sum, count, min, max, first, last)
//! - Group by columns (including time bucket)
//! - Refresh policy and retention settings

use super::types::{AggregateState, TimeBucketGranularity, WatermarkInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Aggregate function type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    /// Average value
    Avg,
    /// Sum of values
    Sum,
    /// Count of rows
    Count,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// First value (by time)
    First,
    /// Last value (by time)
    Last,
    /// Count of distinct values
    CountDistinct,
    /// Standard deviation
    Stddev,
    /// Variance
    Variance,
    /// Percentile (with parameter)
    Percentile(u8), // e.g., 95 for p95
}

impl AggregateFunction {
    /// Get the SQL function name
    pub fn sql_name(&self) -> String {
        match self {
            Self::Avg => "avg".to_string(),
            Self::Sum => "sum".to_string(),
            Self::Count => "count".to_string(),
            Self::Min => "min".to_string(),
            Self::Max => "max".to_string(),
            Self::First => "first_value".to_string(),
            Self::Last => "last_value".to_string(),
            Self::CountDistinct => "count_distinct".to_string(),
            Self::Stddev => "stddev".to_string(),
            Self::Variance => "variance".to_string(),
            Self::Percentile(p) => format!("percentile_cont({})", *p as f64 / 100.0),
        }
    }

    /// Parse from SQL function name
    pub fn from_sql(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "avg" => Some(Self::Avg),
            "sum" => Some(Self::Sum),
            "count" => Some(Self::Count),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "first" | "first_value" => Some(Self::First),
            "last" | "last_value" => Some(Self::Last),
            "count_distinct" => Some(Self::CountDistinct),
            "stddev" | "stddev_pop" => Some(Self::Stddev),
            "variance" | "var_pop" => Some(Self::Variance),
            _ => None,
        }
    }

    /// Check if this function can be re-aggregated (for hierarchical aggregates)
    ///
    /// For example:
    /// - avg cannot be re-aggregated (must keep sum and count separately)
    /// - sum, count, min, max can be re-aggregated
    pub fn is_reaggregatable(&self) -> bool {
        matches!(
            self,
            Self::Sum | Self::Count | Self::Min | Self::Max | Self::First | Self::Last
        )
    }
}

impl std::fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql_name())
    }
}

/// Column to aggregate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateColumn {
    /// Source column name
    pub source_column: String,

    /// Aggregate function to apply
    pub function: AggregateFunction,

    /// Alias for the result column
    pub alias: String,

    /// For avg: we also store sum and count for re-aggregation
    #[serde(default)]
    pub helper_columns: Vec<String>,
}

impl AggregateColumn {
    pub fn new(source: &str, function: AggregateFunction, alias: &str) -> Self {
        let helper_columns = if matches!(function, AggregateFunction::Avg) {
            // For avg, we need sum and count for re-aggregation
            vec![
                format!("{}_sum", alias),
                format!("{}_count", alias),
            ]
        } else {
            vec![]
        };

        Self {
            source_column: source.to_string(),
            function,
            alias: alias.to_string(),
            helper_columns,
        }
    }

    /// Generate SQL for this aggregate column
    pub fn to_sql(&self) -> String {
        format!(
            "{}({}) AS {}",
            self.function.sql_name(),
            self.source_column,
            self.alias
        )
    }

    /// Generate SQL including helper columns (for re-aggregation support)
    pub fn to_sql_with_helpers(&self) -> Vec<String> {
        let mut result = vec![self.to_sql()];

        // For avg, also generate sum and count columns
        if matches!(self.function, AggregateFunction::Avg) {
            result.push(format!(
                "sum({}) AS {}",
                self.source_column,
                self.helper_columns.get(0).unwrap_or(&format!("{}_sum", self.alias))
            ));
            result.push(format!(
                "count({}) AS {}",
                self.source_column,
                self.helper_columns.get(1).unwrap_or(&format!("{}_count", self.alias))
            ));
        }

        result
    }
}

/// Column to group by
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupByColumn {
    /// Time bucket grouping
    TimeBucket {
        /// Source time column (usually "time")
        source_column: String,
        /// Bucket granularity
        granularity: TimeBucketGranularity,
        /// Alias for the result column
        alias: String,
    },
    /// Regular column grouping
    Column {
        /// Column name
        name: String,
        /// Optional alias
        alias: Option<String>,
    },
}

impl GroupByColumn {
    /// Create a time bucket group by
    pub fn time_bucket(source: &str, granularity: TimeBucketGranularity, alias: &str) -> Self {
        Self::TimeBucket {
            source_column: source.to_string(),
            granularity,
            alias: alias.to_string(),
        }
    }

    /// Create a regular column group by
    pub fn column(name: &str) -> Self {
        Self::Column {
            name: name.to_string(),
            alias: None,
        }
    }

    /// Create a regular column group by with alias
    pub fn column_with_alias(name: &str, alias: &str) -> Self {
        Self::Column {
            name: name.to_string(),
            alias: Some(alias.to_string()),
        }
    }

    /// Get the output column name
    pub fn output_name(&self) -> &str {
        match self {
            Self::TimeBucket { alias, .. } => alias,
            Self::Column { alias, name } => alias.as_deref().unwrap_or(name),
        }
    }

    /// Generate SQL for the SELECT clause
    pub fn to_select_sql(&self) -> String {
        match self {
            Self::TimeBucket {
                source_column,
                granularity,
                alias,
            } => {
                format!(
                    "time_bucket({}, {}, 0) AS {}",
                    source_column,
                    granularity.to_nanos(),
                    alias
                )
            }
            Self::Column { name, alias } => {
                if let Some(a) = alias {
                    format!("{} AS {}", name, a)
                } else {
                    name.clone()
                }
            }
        }
    }

    /// Generate SQL for the GROUP BY clause
    pub fn to_group_by_sql(&self) -> String {
        match self {
            Self::TimeBucket {
                source_column,
                granularity,
                ..
            } => {
                format!(
                    "time_bucket({}, {}, 0)",
                    source_column,
                    granularity.to_nanos()
                )
            }
            Self::Column { name, .. } => name.clone(),
        }
    }
}

/// Refresh policy for the aggregate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshPolicy {
    /// Refresh interval in seconds
    /// The aggregate will be refreshed at most this often
    pub interval_secs: u64,

    /// Start offset in seconds
    /// Don't refresh data newer than this (allows for late data)
    pub start_offset_secs: u64,

    /// End offset in seconds
    /// Don't refresh data older than this (for bounded refresh windows)
    pub end_offset_secs: Option<u64>,

    /// Maximum rows to process per refresh
    /// Limits memory usage for large tables
    pub max_rows_per_refresh: Option<u64>,

    /// Whether to do a full refresh (recompute all) vs incremental
    #[serde(default)]
    pub full_refresh: bool,
}

impl Default for RefreshPolicy {
    fn default() -> Self {
        Self {
            interval_secs: 300,      // 5 minutes
            start_offset_secs: 60,   // Don't refresh last 1 minute (late data buffer)
            end_offset_secs: None,   // No end limit
            max_rows_per_refresh: Some(10_000_000), // 10M rows max per refresh
            full_refresh: false,
        }
    }
}

/// Complete continuous aggregate definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateDefinition {
    /// Unique name of this aggregate
    pub name: String,

    /// Source table (fully qualified: catalog.schema.table)
    pub source_table: String,

    /// Target aggregate table (fully qualified: catalog.schema.table)
    /// By convention: catalog.schema._ca_{name}
    pub target_table: String,

    /// Columns to aggregate
    pub aggregate_columns: Vec<AggregateColumn>,

    /// Columns to group by (including time bucket)
    pub group_by_columns: Vec<GroupByColumn>,

    /// Optional WHERE clause for filtering source data
    pub filter: Option<String>,

    /// Refresh policy
    pub refresh_policy: RefreshPolicy,

    /// Retention days for aggregate data (None = keep forever)
    pub retention_days: Option<u32>,

    /// Current state
    pub state: AggregateState,

    /// Watermark information
    pub watermark: WatermarkInfo,

    /// Created at timestamp (nanoseconds)
    pub created_at: i64,

    /// Last modified timestamp (nanoseconds)
    pub modified_at: i64,

    /// Optional description
    pub description: Option<String>,

    /// Custom metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl AggregateDefinition {
    /// Create a new aggregate definition builder
    pub fn builder(name: &str, source_table: &str) -> AggregateDefinitionBuilder {
        AggregateDefinitionBuilder::new(name, source_table)
    }

    /// Get the time bucket granularity (if any)
    pub fn time_bucket_granularity(&self) -> Option<TimeBucketGranularity> {
        for col in &self.group_by_columns {
            if let GroupByColumn::TimeBucket { granularity, .. } = col {
                return Some(*granularity);
            }
        }
        None
    }

    /// Generate the CREATE TABLE SQL for the aggregate table
    pub fn to_create_sql(&self) -> String {
        let mut columns = Vec::new();

        // Add group by columns
        for gb in &self.group_by_columns {
            columns.push(gb.to_select_sql());
        }

        // Add aggregate columns (with helpers for re-aggregation)
        for agg in &self.aggregate_columns {
            columns.extend(agg.to_sql_with_helpers());
        }

        let group_by: Vec<String> = self
            .group_by_columns
            .iter()
            .map(|gb| gb.to_group_by_sql())
            .collect();

        let mut sql = format!(
            "SELECT {} FROM {} ",
            columns.join(", "),
            self.source_table
        );

        if let Some(filter) = &self.filter {
            sql.push_str(&format!("WHERE {} ", filter));
        }

        sql.push_str(&format!("GROUP BY {}", group_by.join(", ")));

        sql
    }

    /// Generate the incremental refresh query
    /// This selects new data since the last watermark
    pub fn to_incremental_refresh_sql(&self, from_watermark: i64, to_watermark: i64) -> String {
        let base_sql = self.to_create_sql();

        // Find the time column from group by
        let time_column = self
            .group_by_columns
            .iter()
            .find_map(|gb| {
                if let GroupByColumn::TimeBucket { source_column, .. } = gb {
                    Some(source_column.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "time".to_string());

        // Add time range filter
        let time_filter = format!(
            "{} >= {} AND {} < {}",
            time_column, from_watermark, time_column, to_watermark
        );

        if self.filter.is_some() {
            // Insert before GROUP BY, after existing WHERE
            base_sql.replace(
                "GROUP BY",
                &format!("AND {} GROUP BY", time_filter),
            )
        } else {
            // Insert WHERE clause
            base_sql.replace(
                "GROUP BY",
                &format!("WHERE {} GROUP BY", time_filter),
            )
        }
    }

    /// Check if this aggregate can answer a query with the given time bucket
    pub fn can_answer_query(&self, query_bucket_nanos: i64) -> bool {
        if let Some(agg_granularity) = self.time_bucket_granularity() {
            // Query bucket must be >= aggregate bucket (can't get finer granularity)
            query_bucket_nanos >= agg_granularity.to_nanos()
        } else {
            false
        }
    }
}

/// Builder for AggregateDefinition
pub struct AggregateDefinitionBuilder {
    name: String,
    source_table: String,
    target_table: Option<String>,
    aggregate_columns: Vec<AggregateColumn>,
    group_by_columns: Vec<GroupByColumn>,
    filter: Option<String>,
    refresh_policy: RefreshPolicy,
    retention_days: Option<u32>,
    description: Option<String>,
}

impl AggregateDefinitionBuilder {
    pub fn new(name: &str, source_table: &str) -> Self {
        Self {
            name: name.to_string(),
            source_table: source_table.to_string(),
            target_table: None,
            aggregate_columns: Vec::new(),
            group_by_columns: Vec::new(),
            filter: None,
            refresh_policy: RefreshPolicy::default(),
            retention_days: None,
            description: None,
        }
    }

    /// Set the target table name (default: source_schema._ca_{name})
    pub fn target_table(mut self, table: &str) -> Self {
        self.target_table = Some(table.to_string());
        self
    }

    /// Add an aggregate column
    pub fn aggregate(mut self, source: &str, function: AggregateFunction, alias: &str) -> Self {
        self.aggregate_columns
            .push(AggregateColumn::new(source, function, alias));
        self
    }

    /// Add avg aggregate (convenience method)
    pub fn avg(self, source: &str, alias: &str) -> Self {
        self.aggregate(source, AggregateFunction::Avg, alias)
    }

    /// Add sum aggregate (convenience method)
    pub fn sum(self, source: &str, alias: &str) -> Self {
        self.aggregate(source, AggregateFunction::Sum, alias)
    }

    /// Add count aggregate (convenience method)
    pub fn count(self, alias: &str) -> Self {
        self.aggregate("*", AggregateFunction::Count, alias)
    }

    /// Add min aggregate (convenience method)
    pub fn min(self, source: &str, alias: &str) -> Self {
        self.aggregate(source, AggregateFunction::Min, alias)
    }

    /// Add max aggregate (convenience method)
    pub fn max(self, source: &str, alias: &str) -> Self {
        self.aggregate(source, AggregateFunction::Max, alias)
    }

    /// Add time bucket group by
    pub fn time_bucket(mut self, source: &str, granularity: &str, alias: &str) -> Result<Self, String> {
        let gran = TimeBucketGranularity::from_str(granularity)?;
        self.group_by_columns
            .push(GroupByColumn::time_bucket(source, gran, alias));
        Ok(self)
    }

    /// Add column group by
    pub fn group_by(mut self, column: &str) -> Self {
        self.group_by_columns.push(GroupByColumn::column(column));
        self
    }

    /// Set filter clause
    pub fn filter(mut self, filter: &str) -> Self {
        self.filter = Some(filter.to_string());
        self
    }

    /// Set refresh interval
    pub fn refresh_interval(mut self, secs: u64) -> Self {
        self.refresh_policy.interval_secs = secs;
        self
    }

    /// Set retention days
    pub fn retention(mut self, days: u32) -> Self {
        self.retention_days = Some(days);
        self
    }

    /// Set description
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Build the definition
    pub fn build(self) -> Result<AggregateDefinition, String> {
        if self.aggregate_columns.is_empty() {
            return Err("At least one aggregate column is required".to_string());
        }

        if self.group_by_columns.is_empty() {
            return Err("At least one group by column is required".to_string());
        }

        // Check for time bucket
        let has_time_bucket = self
            .group_by_columns
            .iter()
            .any(|gb| matches!(gb, GroupByColumn::TimeBucket { .. }));

        if !has_time_bucket {
            return Err("Time bucket is required for continuous aggregates".to_string());
        }

        // Generate target table name if not specified
        let target_table = self.target_table.unwrap_or_else(|| {
            // Parse source table to get catalog.schema
            let parts: Vec<&str> = self.source_table.split('.').collect();
            if parts.len() >= 2 {
                format!("{}.{}._ca_{}", parts[0], parts[1], self.name)
            } else {
                format!("_ca_{}", self.name)
            }
        });

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        Ok(AggregateDefinition {
            name: self.name.clone(),
            source_table: self.source_table,
            target_table,
            aggregate_columns: self.aggregate_columns,
            group_by_columns: self.group_by_columns,
            filter: self.filter,
            refresh_policy: self.refresh_policy,
            retention_days: self.retention_days,
            state: AggregateState::Creating,
            watermark: WatermarkInfo::new(&self.name),
            created_at: now,
            modified_at: now,
            description: self.description,
            metadata: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_column_sql() {
        let col = AggregateColumn::new("cpu_usage", AggregateFunction::Avg, "avg_cpu");
        assert_eq!(col.to_sql(), "avg(cpu_usage) AS avg_cpu");

        // Check helper columns for avg
        let helpers = col.to_sql_with_helpers();
        assert_eq!(helpers.len(), 3);
        assert!(helpers[1].contains("sum(cpu_usage)"));
        assert!(helpers[2].contains("count(cpu_usage)"));
    }

    #[test]
    fn test_group_by_column_sql() {
        let gb = GroupByColumn::time_bucket("time", TimeBucketGranularity::Hour, "hour");
        assert!(gb.to_select_sql().contains("time_bucket"));
        assert!(gb.to_select_sql().contains("3600000000000")); // 1 hour in nanos

        let gb = GroupByColumn::column("host");
        assert_eq!(gb.to_select_sql(), "host");
    }

    #[test]
    fn test_builder() {
        let def = AggregateDefinition::builder("hourly_cpu", "mydb.default.cpu")
            .time_bucket("time", "1h", "hour")
            .unwrap()
            .group_by("host")
            .avg("cpu_usage", "avg_cpu")
            .max("cpu_usage", "max_cpu")
            .count("sample_count")
            .refresh_interval(300)
            .retention(365)
            .build()
            .unwrap();

        assert_eq!(def.name, "hourly_cpu");
        assert_eq!(def.source_table, "mydb.default.cpu");
        assert_eq!(def.target_table, "mydb.default._ca_hourly_cpu");
        assert_eq!(def.aggregate_columns.len(), 3);
        assert_eq!(def.group_by_columns.len(), 2);
        assert_eq!(def.retention_days, Some(365));
    }

    #[test]
    fn test_create_sql() {
        let def = AggregateDefinition::builder("hourly_cpu", "mydb.default.cpu")
            .time_bucket("time", "1h", "hour")
            .unwrap()
            .group_by("host")
            .avg("cpu_usage", "avg_cpu")
            .build()
            .unwrap();

        let sql = def.to_create_sql();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("time_bucket(time, 3600000000000, 0) AS hour"));
        assert!(sql.contains("avg(cpu_usage) AS avg_cpu"));
        assert!(sql.contains("GROUP BY"));
    }

    #[test]
    fn test_can_answer_query() {
        let def = AggregateDefinition::builder("hourly_cpu", "mydb.default.cpu")
            .time_bucket("time", "1h", "hour")
            .unwrap()
            .avg("cpu_usage", "avg_cpu")
            .build()
            .unwrap();

        // Can answer hourly or coarser queries
        assert!(def.can_answer_query(TimeBucketGranularity::Hour.to_nanos()));
        assert!(def.can_answer_query(TimeBucketGranularity::Day.to_nanos()));

        // Cannot answer finer queries
        assert!(!def.can_answer_query(TimeBucketGranularity::Minute.to_nanos()));
    }
}
