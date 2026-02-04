//! Write distribution configuration types

use serde::{Deserialize, Serialize};

use super::{current_timestamp_ms, keys};

/// Scope level for distribution configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DistributionScope {
    /// Apply to all tables in a catalog
    Catalog = 1,
    /// Apply to all tables in a schema
    Schema = 2,
    /// Apply to a specific table
    Table = 3,
}

impl From<i32> for DistributionScope {
    fn from(v: i32) -> Self {
        match v {
            1 => DistributionScope::Catalog,
            2 => DistributionScope::Schema,
            3 => DistributionScope::Table,
            _ => DistributionScope::Table,
        }
    }
}

impl From<DistributionScope> for i32 {
    fn from(scope: DistributionScope) -> Self {
        scope as i32
    }
}

/// Write distribution configuration
///
/// Allows distributing writes for a table across multiple primary nodes
/// for better load balancing on high-throughput tables.
///
/// Configuration is hierarchical: table > schema > catalog
/// The most specific matching config wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionConfig {
    /// Catalog name (required)
    pub catalog: String,
    /// Schema name (optional - if empty, applies to all schemas in catalog)
    pub schema: String,
    /// Table name (optional - if empty, applies to all tables in schema)
    pub table: String,
    /// Scope level (derived from catalog/schema/table presence)
    pub scope: DistributionScope,
    /// Distribution percentage [0-100]
    /// - 0: No distribution, single node (default behavior)
    /// - 50: Distribute across 50% of primary nodes
    /// - 100: Distribute across ALL primary nodes
    pub percent: u8,
    /// When config was created
    pub created_at: i64,
    /// When config was last modified
    pub last_modified: i64,
    /// Version for cache invalidation
    pub version: u64,
}

impl DistributionConfig {
    /// Create a new distribution config
    pub fn new(catalog: String, schema: String, table: String, percent: u8) -> Self {
        let scope = if !table.is_empty() {
            DistributionScope::Table
        } else if !schema.is_empty() {
            DistributionScope::Schema
        } else {
            DistributionScope::Catalog
        };
        let now = current_timestamp_ms();
        Self {
            catalog,
            schema,
            table,
            scope,
            percent: percent.min(100),
            created_at: now,
            last_modified: now,
            version: 1,
        }
    }

    /// Generate storage key for this config
    pub fn storage_key(&self) -> String {
        Self::key_for(&self.catalog, &self.schema, &self.table)
    }

    /// Generate storage key for a distribution config
    pub fn key_for(catalog: &str, schema: &str, table: &str) -> String {
        if !table.is_empty() {
            format!(
                "{}{}/{}/{}",
                keys::PREFIX_DISTRIBUTION,
                catalog,
                schema,
                table
            )
        } else if !schema.is_empty() {
            format!("{}{}/{}", keys::PREFIX_DISTRIBUTION, catalog, schema)
        } else {
            format!("{}{}", keys::PREFIX_DISTRIBUTION, catalog)
        }
    }

    /// Generate prefix for listing all distribution configs
    pub fn all_prefix() -> String {
        keys::PREFIX_DISTRIBUTION.to_string()
    }

    /// Check if this config is more specific than another
    pub fn is_more_specific_than(&self, other: &Self) -> bool {
        (self.scope as u8) > (other.scope as u8)
    }

    /// Check if this config applies to the given table
    pub fn applies_to(&self, catalog: &str, schema: &str, table: &str) -> bool {
        if self.catalog != catalog {
            return false;
        }
        match self.scope {
            DistributionScope::Catalog => true,
            DistributionScope::Schema => self.schema == schema,
            DistributionScope::Table => self.schema == schema && self.table == table,
        }
    }

    /// Calculate number of nodes to use based on percentage and total primary nodes
    pub fn nodes_to_use(&self, total_primary_nodes: usize) -> usize {
        if self.percent == 0 || total_primary_nodes == 0 {
            1 // Single node (default)
        } else if self.percent >= 100 {
            total_primary_nodes
        } else {
            let count = (total_primary_nodes as u64 * self.percent as u64 / 100).max(1) as usize;
            count.min(total_primary_nodes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_config_new_catalog_scope() {
        let config =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);

        assert_eq!(config.catalog, "mydb");
        assert_eq!(config.schema, "");
        assert_eq!(config.table, "");
        assert_eq!(config.scope, DistributionScope::Catalog);
        assert_eq!(config.percent, 100);
    }

    #[test]
    fn test_distribution_config_new_schema_scope() {
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );

        assert_eq!(config.catalog, "mydb");
        assert_eq!(config.schema, "metrics");
        assert_eq!(config.table, "");
        assert_eq!(config.scope, DistributionScope::Schema);
        assert_eq!(config.percent, 50);
    }

    #[test]
    fn test_distribution_config_new_table_scope() {
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            0,
        );

        assert_eq!(config.catalog, "mydb");
        assert_eq!(config.schema, "metrics");
        assert_eq!(config.table, "cpu");
        assert_eq!(config.scope, DistributionScope::Table);
        assert_eq!(config.percent, 0);
    }

    #[test]
    fn test_distribution_config_storage_key_catalog() {
        let config =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);

        // Catalog-level: /distribution/catalog
        assert_eq!(config.storage_key(), "/distribution/mydb");
    }

    #[test]
    fn test_distribution_config_storage_key_schema() {
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );

        // Schema-level: /distribution/catalog/schema
        assert_eq!(config.storage_key(), "/distribution/mydb/metrics");
    }

    #[test]
    fn test_distribution_config_storage_key_table() {
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            100,
        );

        // Table-level: /distribution/catalog/schema/table
        assert_eq!(config.storage_key(), "/distribution/mydb/metrics/cpu");
    }

    #[test]
    fn test_distribution_config_key_for() {
        // key_for generates the same keys as storage_key
        assert_eq!(
            DistributionConfig::key_for("mydb", "", ""),
            "/distribution/mydb"
        );
        assert_eq!(
            DistributionConfig::key_for("mydb", "metrics", ""),
            "/distribution/mydb/metrics"
        );
        assert_eq!(
            DistributionConfig::key_for("mydb", "metrics", "cpu"),
            "/distribution/mydb/metrics/cpu"
        );
    }

    #[test]
    fn test_distribution_config_all_prefix() {
        assert_eq!(DistributionConfig::all_prefix(), "/distribution/");
    }

    #[test]
    fn test_distribution_scope_ordering() {
        // Catalog < Schema < Table (more specific is higher)
        assert!((DistributionScope::Catalog as u8) < (DistributionScope::Schema as u8));
        assert!((DistributionScope::Schema as u8) < (DistributionScope::Table as u8));
    }

    #[test]
    fn test_distribution_config_serialization() {
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            75,
        );

        // Test bincode serialization (used in sled storage)
        let encoded = bincode::serialize(&config).unwrap();
        let decoded: DistributionConfig = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded.catalog, config.catalog);
        assert_eq!(decoded.schema, config.schema);
        assert_eq!(decoded.table, config.table);
        assert_eq!(decoded.scope, config.scope);
        assert_eq!(decoded.percent, config.percent);
    }

    #[test]
    fn test_distribution_config_percent_boundaries() {
        // Test 0% (single node)
        let config_0 =
            DistributionConfig::new("db".to_string(), "".to_string(), "".to_string(), 0);
        assert_eq!(config_0.percent, 0);

        // Test 100% (all nodes)
        let config_100 =
            DistributionConfig::new("db".to_string(), "".to_string(), "".to_string(), 100);
        assert_eq!(config_100.percent, 100);

        // Test mid-range
        let config_50 =
            DistributionConfig::new("db".to_string(), "".to_string(), "".to_string(), 50);
        assert_eq!(config_50.percent, 50);
    }
}
