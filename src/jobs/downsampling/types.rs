//! Types for automated downsampling
//!
//! Defines policies, tiers, aggregate functions, and status tracking.

use serde::{Deserialize, Serialize};

/// A downsampling policy for a source table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsamplingPolicy {
    /// Unique policy name
    pub name: String,
    /// Source table in catalog.schema.table format
    pub source_table: String,
    /// Ordered list of downsampling tiers (finest to coarsest)
    pub tiers: Vec<DownsamplingTier>,
    /// Whether this policy is active
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Creation timestamp (epoch nanos)
    #[serde(default)]
    pub created_at: i64,
    /// Last modification timestamp (epoch nanos)
    #[serde(default)]
    pub modified_at: i64,
}

fn default_true() -> bool {
    true
}

/// A single downsampling tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsamplingTier {
    /// Tier name (e.g., "1m", "1h", "1d")
    pub name: String,
    /// Downsample data older than this many days
    pub after_days: u32,
    /// Time bucket interval string (e.g., "1 minute", "1 hour", "1 day")
    pub interval: String,
    /// Aggregate specifications to compute
    pub aggregates: Vec<AggregateSpec>,
    /// Optional retention for tier data (days). None = keep forever.
    #[serde(default)]
    pub retention_days: Option<u32>,
}

/// An aggregate specification (function + column)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateSpec {
    /// The aggregate function to apply
    pub function: AggregateFn,
    /// The source column name
    pub column: String,
}

/// Supported aggregate functions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AggregateFn {
    Min,
    Max,
    Avg,
    Sum,
    Count,
    First,
    Last,
}

impl AggregateFn {
    /// Get the SQL function name
    pub fn sql_name(&self) -> &str {
        match self {
            AggregateFn::Min => "min",
            AggregateFn::Max => "max",
            AggregateFn::Avg => "avg",
            AggregateFn::Sum => "sum",
            AggregateFn::Count => "count",
            AggregateFn::First => "first_value",
            AggregateFn::Last => "last_value",
        }
    }

    /// Get the output column alias for this aggregate on a given column
    pub fn output_alias(&self, column: &str) -> String {
        match self {
            AggregateFn::Count => "count".to_string(),
            _ => format!("{}_{}", self.sql_name(), column),
        }
    }
}

/// Status tracking for a tier's processing progress
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TierStatus {
    /// Tier name
    pub tier_name: String,
    /// Last successfully processed partition date (YYYY-MM-DD)
    pub last_processed_date: Option<String>,
    /// Total rows written to this tier
    pub total_rows_written: u64,
    /// Total storage bytes for this tier
    pub total_bytes_written: u64,
    /// Number of partitions processed
    pub partitions_processed: u64,
    /// Last run timestamp (epoch millis)
    pub last_run_at: i64,
}

/// Status for a complete policy
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PolicyStatus {
    /// Policy name
    pub policy_name: String,
    /// Per-tier status
    pub tiers: Vec<TierStatus>,
}

impl DownsamplingPolicy {
    /// Parse the source_table into (catalog, schema, table)
    pub fn parse_source_table(&self) -> Result<(String, String, String), String> {
        let parts: Vec<&str> = self.source_table.split('.').collect();
        if parts.len() != 3 {
            return Err(format!(
                "Invalid source_table format '{}': expected catalog.schema.table",
                self.source_table
            ));
        }
        Ok((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ))
    }

    /// Get the tier target table name for a given tier
    /// e.g., source "cpu" + tier "1m" → "cpu__ds_1m"
    pub fn tier_table_name(&self, tier: &DownsamplingTier) -> Result<String, String> {
        let (catalog, schema, table) = self.parse_source_table()?;
        Ok(format!("{}.{}.{}__ds_{}", catalog, schema, table, tier.name))
    }

    /// Get just the table part of the tier table name (without catalog.schema)
    pub fn tier_table_short_name(&self, tier: &DownsamplingTier) -> Result<String, String> {
        let (_, _, table) = self.parse_source_table()?;
        Ok(format!("{}__ds_{}", table, tier.name))
    }
}

/// Parse an aggregate string like "min(usage)" into an AggregateSpec
pub fn parse_aggregate_str(s: &str) -> Result<AggregateSpec, String> {
    let s = s.trim();
    if let Some(paren_start) = s.find('(') {
        if let Some(paren_end) = s.find(')') {
            let func_name = s[..paren_start].trim();
            let column = s[paren_start + 1..paren_end].trim().to_string();
            let function = match func_name.to_lowercase().as_str() {
                "min" => AggregateFn::Min,
                "max" => AggregateFn::Max,
                "avg" => AggregateFn::Avg,
                "sum" => AggregateFn::Sum,
                "count" => AggregateFn::Count,
                "first" | "first_value" => AggregateFn::First,
                "last" | "last_value" => AggregateFn::Last,
                _ => return Err(format!("Unknown aggregate function: {}", func_name)),
            };
            return Ok(AggregateSpec { function, column });
        }
    }
    Err(format!(
        "Invalid aggregate format '{}': expected 'function(column)'",
        s
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_fn_sql_name() {
        assert_eq!(AggregateFn::Min.sql_name(), "min");
        assert_eq!(AggregateFn::Avg.sql_name(), "avg");
        assert_eq!(AggregateFn::Count.sql_name(), "count");
        assert_eq!(AggregateFn::First.sql_name(), "first_value");
    }

    #[test]
    fn test_aggregate_fn_output_alias() {
        assert_eq!(AggregateFn::Min.output_alias("usage"), "min_usage");
        assert_eq!(AggregateFn::Avg.output_alias("value"), "avg_value");
        assert_eq!(AggregateFn::Count.output_alias("*"), "count");
    }

    #[test]
    fn test_parse_aggregate_str() {
        let spec = parse_aggregate_str("min(usage)").unwrap();
        assert_eq!(spec.function, AggregateFn::Min);
        assert_eq!(spec.column, "usage");

        let spec = parse_aggregate_str("avg( value )").unwrap();
        assert_eq!(spec.function, AggregateFn::Avg);
        assert_eq!(spec.column, "value");

        let spec = parse_aggregate_str("count(*)").unwrap();
        assert_eq!(spec.function, AggregateFn::Count);
        assert_eq!(spec.column, "*");

        assert!(parse_aggregate_str("invalid").is_err());
        assert!(parse_aggregate_str("unknown(col)").is_err());
    }

    #[test]
    fn test_policy_parse_source_table() {
        let policy = DownsamplingPolicy {
            name: "test".to_string(),
            source_table: "mydb.default.cpu".to_string(),
            tiers: vec![],
            enabled: true,
            created_at: 0,
            modified_at: 0,
        };
        let (c, s, t) = policy.parse_source_table().unwrap();
        assert_eq!(c, "mydb");
        assert_eq!(s, "default");
        assert_eq!(t, "cpu");
    }

    #[test]
    fn test_tier_table_name() {
        let tier = DownsamplingTier {
            name: "1m".to_string(),
            after_days: 7,
            interval: "1 minute".to_string(),
            aggregates: vec![],
            retention_days: None,
        };
        let policy = DownsamplingPolicy {
            name: "test".to_string(),
            source_table: "mydb.default.cpu".to_string(),
            tiers: vec![tier.clone()],
            enabled: true,
            created_at: 0,
            modified_at: 0,
        };
        assert_eq!(
            policy.tier_table_name(&tier).unwrap(),
            "mydb.default.cpu__ds_1m"
        );
        assert_eq!(
            policy.tier_table_short_name(&tier).unwrap(),
            "cpu__ds_1m"
        );
    }

    #[test]
    fn test_policy_serialization() {
        let policy = DownsamplingPolicy {
            name: "cpu_ds".to_string(),
            source_table: "mydb.default.cpu".to_string(),
            tiers: vec![DownsamplingTier {
                name: "1m".to_string(),
                after_days: 7,
                interval: "1 minute".to_string(),
                aggregates: vec![
                    AggregateSpec {
                        function: AggregateFn::Min,
                        column: "usage".to_string(),
                    },
                    AggregateSpec {
                        function: AggregateFn::Max,
                        column: "usage".to_string(),
                    },
                ],
                retention_days: Some(90),
            }],
            enabled: true,
            created_at: 0,
            modified_at: 0,
        };

        let json = serde_json::to_string_pretty(&policy).unwrap();
        let parsed: DownsamplingPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "cpu_ds");
        assert_eq!(parsed.tiers.len(), 1);
        assert_eq!(parsed.tiers[0].aggregates.len(), 2);
        assert_eq!(parsed.tiers[0].retention_days, Some(90));
    }

    #[test]
    fn test_tier_status_default() {
        let status = TierStatus::default();
        assert!(status.last_processed_date.is_none());
        assert_eq!(status.total_rows_written, 0);
    }
}
