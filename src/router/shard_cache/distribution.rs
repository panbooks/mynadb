//! Distribution config cache methods for ShardCache
//!
//! Handles hierarchical distribution config caching for write routing:
//! - Catalog-level configs
//! - Schema-level configs
//! - Table-level configs (highest priority)

use super::types::CachedDistributionConfig;
use super::ShardCache;
use crate::meta::types::{DistributionConfig, DistributionScope};
use tracing::{debug, info};

impl ShardCache {
    // ========================================================================
    // Distribution Config Cache
    // ========================================================================

    /// Get the distribution percent for a table (hierarchical lookup)
    ///
    /// Lookup order: table > schema > catalog
    /// Returns 0 (single-node routing) if no config is found.
    ///
    /// This is the hot-path method used by WriteRouter to determine
    /// if writes should be distributed across multiple nodes.
    pub fn get_distribution_percent(&self, catalog: &str, schema: &str, table: &str) -> u8 {
        // Try table-level first (most specific)
        let table_key = format!("{}/{}/{}", catalog, schema, table);
        if let Some(config) = self.distribution_configs.get(&table_key) {
            return config.percent;
        }

        // Try schema-level
        let schema_key = format!("{}/{}", catalog, schema);
        if let Some(config) = self.distribution_configs.get(&schema_key) {
            return config.percent;
        }

        // Try catalog-level
        if let Some(config) = self.distribution_configs.get(catalog) {
            return config.percent;
        }

        // Default: single-node routing
        0
    }

    /// Check if writes should be distributed for a table
    ///
    /// Returns true if percent > 0 (i.e., writes should go to multiple nodes)
    pub fn is_distributed(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.get_distribution_percent(catalog, schema, table) > 0
    }

    /// Set distribution config in cache
    ///
    /// The key format depends on scope:
    /// - Catalog: "catalog"
    /// - Schema: "catalog/schema"
    /// - Table: "catalog/schema/table"
    pub fn set_distribution_config(&self, config: &DistributionConfig) {
        let key = match config.scope {
            DistributionScope::Catalog => config.catalog.clone(),
            DistributionScope::Schema => format!("{}/{}", config.catalog, config.schema),
            DistributionScope::Table => {
                format!("{}/{}/{}", config.catalog, config.schema, config.table)
            }
        };

        debug!(
            "Caching distribution config: {} -> {}% (v{})",
            key, config.percent, config.version
        );

        self.distribution_configs.insert(
            key,
            CachedDistributionConfig {
                percent: config.percent,
                version: config.version,
                cached_at: chrono::Utc::now().timestamp_millis(),
            },
        );
    }

    /// Remove distribution config from cache
    pub fn remove_distribution_config(&self, catalog: &str, schema: &str, table: &str) {
        let key = if !table.is_empty() {
            format!("{}/{}/{}", catalog, schema, table)
        } else if !schema.is_empty() {
            format!("{}/{}", catalog, schema)
        } else {
            catalog.to_string()
        };

        debug!("Removing distribution config from cache: {}", key);
        self.distribution_configs.remove(&key);
    }

    /// Invalidate all distribution configs for a catalog
    ///
    /// Used when a catalog-level config changes, which affects all
    /// tables in that catalog.
    pub fn invalidate_distribution_configs_for_catalog(&self, catalog: &str) {
        let prefix = format!("{}/", catalog);
        let keys_to_remove: Vec<String> = self
            .distribution_configs
            .iter()
            .filter(|entry| entry.key() == catalog || entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.distribution_configs.remove(&key);
        }
        debug!("Invalidated distribution configs for catalog: {}", catalog);
    }

    /// Clear all distribution configs from cache
    pub fn clear_distribution_configs(&self) {
        self.distribution_configs.clear();
        info!("Cleared all distribution configs from cache");
    }

    /// Get all cached distribution configs (for debugging/stats)
    pub fn list_distribution_configs(&self) -> Vec<(String, u8, u64)> {
        self.distribution_configs
            .iter()
            .map(|entry| (entry.key().clone(), entry.percent, entry.version))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cache() -> ShardCache {
        ShardCache::new()
    }

    #[test]
    fn test_get_distribution_percent_default() {
        let cache = create_test_cache();

        // No config set, should return 0 (single node routing)
        let percent = cache.get_distribution_percent("mydb", "metrics", "cpu");
        assert_eq!(percent, 0);
    }

    #[test]
    fn test_set_and_get_distribution_config_catalog() {
        let cache = create_test_cache();

        // Set catalog-level config
        let config =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
        cache.set_distribution_config(&config);

        // Should inherit catalog-level config
        assert_eq!(
            cache.get_distribution_percent("mydb", "metrics", "cpu"),
            100
        );
        assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 100);
        assert_eq!(cache.get_distribution_percent("mydb", "", ""), 100);

        // Different catalog should not be affected
        assert_eq!(cache.get_distribution_percent("other", "metrics", "cpu"), 0);
    }

    #[test]
    fn test_set_and_get_distribution_config_schema() {
        let cache = create_test_cache();

        // Set schema-level config
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );
        cache.set_distribution_config(&config);

        // Should inherit schema-level config
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 50);
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 50);

        // Different schema should not be affected
        assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 0);
    }

    #[test]
    fn test_set_and_get_distribution_config_table() {
        let cache = create_test_cache();

        // Set table-level config
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            75,
        );
        cache.set_distribution_config(&config);

        // Should match table-level config
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 75);

        // Different table should not be affected
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 0);
    }

    #[test]
    fn test_hierarchical_override_table_over_schema() {
        let cache = create_test_cache();

        // Set schema-level config: 50%
        let schema_config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );
        cache.set_distribution_config(&schema_config);

        // Set table-level config: 0% (override)
        let table_config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            0,
        );
        cache.set_distribution_config(&table_config);

        // cpu table should use table-level config (0%)
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);

        // Other tables in same schema should use schema-level config (50%)
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 50);
    }

    #[test]
    fn test_hierarchical_override_schema_over_catalog() {
        let cache = create_test_cache();

        // Set catalog-level config: 100%
        let catalog_config =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
        cache.set_distribution_config(&catalog_config);

        // Set schema-level config: 25% (override)
        let schema_config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            25,
        );
        cache.set_distribution_config(&schema_config);

        // metrics schema should use schema-level config (25%)
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 25);

        // Other schemas should use catalog-level config (100%)
        assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 100);
    }

    #[test]
    fn test_hierarchical_override_all_levels() {
        let cache = create_test_cache();

        // Set all three levels
        let catalog_config =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
        cache.set_distribution_config(&catalog_config);

        let schema_config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );
        cache.set_distribution_config(&schema_config);

        let table_config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            0,
        );
        cache.set_distribution_config(&table_config);

        // Table level wins
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);

        // Schema level for other tables in metrics
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "mem"), 50);

        // Catalog level for other schemas
        assert_eq!(cache.get_distribution_percent("mydb", "logs", "app"), 100);
    }

    #[test]
    fn test_is_distributed() {
        let cache = create_test_cache();

        // No config = not distributed
        assert!(!cache.is_distributed("mydb", "metrics", "cpu"));

        // Set 0% = not distributed
        let config_0 = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            0,
        );
        cache.set_distribution_config(&config_0);
        assert!(!cache.is_distributed("mydb", "metrics", "cpu"));

        // Set 1% = distributed
        let config_1 = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "mem".to_string(),
            1,
        );
        cache.set_distribution_config(&config_1);
        assert!(cache.is_distributed("mydb", "metrics", "mem"));

        // Set 100% = distributed
        let config_100 = DistributionConfig::new(
            "mydb".to_string(),
            "logs".to_string(),
            "".to_string(),
            100,
        );
        cache.set_distribution_config(&config_100);
        assert!(cache.is_distributed("mydb", "logs", "app"));
    }

    #[test]
    fn test_remove_distribution_config() {
        let cache = create_test_cache();

        // Set config
        let config = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            100,
        );
        cache.set_distribution_config(&config);
        assert_eq!(
            cache.get_distribution_percent("mydb", "metrics", "cpu"),
            100
        );

        // Remove config
        cache.remove_distribution_config("mydb", "metrics", "cpu");
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);
    }

    #[test]
    fn test_clear_distribution_configs() {
        let cache = create_test_cache();

        // Set multiple configs
        let config1 =
            DistributionConfig::new("db1".to_string(), "".to_string(), "".to_string(), 100);
        let config2 =
            DistributionConfig::new("db2".to_string(), "s".to_string(), "".to_string(), 50);
        let config3 =
            DistributionConfig::new("db3".to_string(), "s".to_string(), "t".to_string(), 25);

        cache.set_distribution_config(&config1);
        cache.set_distribution_config(&config2);
        cache.set_distribution_config(&config3);

        assert_eq!(cache.stats().distribution_config_count, 3);

        // Clear all
        cache.clear_distribution_configs();

        assert_eq!(cache.stats().distribution_config_count, 0);
        assert_eq!(cache.get_distribution_percent("db1", "", ""), 0);
        assert_eq!(cache.get_distribution_percent("db2", "s", ""), 0);
        assert_eq!(cache.get_distribution_percent("db3", "s", "t"), 0);
    }

    #[test]
    fn test_list_distribution_configs() {
        let cache = create_test_cache();

        let config1 =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
        let config2 = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );

        cache.set_distribution_config(&config1);
        cache.set_distribution_config(&config2);

        let configs = cache.list_distribution_configs();
        assert_eq!(configs.len(), 2);

        // Check that both configs are present
        let keys: Vec<String> = configs.iter().map(|(k, _, _)| k.clone()).collect();
        assert!(keys.contains(&"mydb".to_string()));
        assert!(keys.contains(&"mydb/metrics".to_string()));
    }

    #[test]
    fn test_invalidate_distribution_configs_for_catalog() {
        let cache = create_test_cache();

        // Set configs for multiple catalogs
        let config1 =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
        let config2 = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "".to_string(),
            50,
        );
        let config3 = DistributionConfig::new(
            "mydb".to_string(),
            "metrics".to_string(),
            "cpu".to_string(),
            25,
        );
        let config4 =
            DistributionConfig::new("other".to_string(), "".to_string(), "".to_string(), 75);

        cache.set_distribution_config(&config1);
        cache.set_distribution_config(&config2);
        cache.set_distribution_config(&config3);
        cache.set_distribution_config(&config4);

        assert_eq!(cache.stats().distribution_config_count, 4);

        // Invalidate only mydb catalog
        cache.invalidate_distribution_configs_for_catalog("mydb");

        // Only "other" catalog should remain
        assert_eq!(cache.stats().distribution_config_count, 1);
        assert_eq!(cache.get_distribution_percent("mydb", "", ""), 0);
        assert_eq!(cache.get_distribution_percent("mydb", "metrics", "cpu"), 0);
        assert_eq!(cache.get_distribution_percent("other", "", ""), 75);
    }

    #[test]
    fn test_stats_includes_distribution_count() {
        let cache = create_test_cache();

        let stats = cache.stats();
        assert_eq!(stats.distribution_config_count, 0);

        let config =
            DistributionConfig::new("mydb".to_string(), "".to_string(), "".to_string(), 100);
        cache.set_distribution_config(&config);

        let stats = cache.stats();
        assert_eq!(stats.distribution_config_count, 1);
    }
}
