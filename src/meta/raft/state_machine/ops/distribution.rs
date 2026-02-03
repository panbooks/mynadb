//! Distribution Config Operations for MetaStateMachine
//!
//! Handles write distribution configuration for load balancing across nodes.

use crate::meta::types::{
    current_timestamp_ms, DistributionConfig, DistributionScope, Event, EventType, MetaResponse,
};
use tracing::{debug, info};

use super::super::MetaStateMachine;

impl MetaStateMachine {
    // ========================================================================
    // Distribution Config Operations
    // ========================================================================

    /// Set distribution config for catalog/schema/table
    pub(crate) fn set_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        percent: u8,
    ) -> MetaResponse {
        let config = DistributionConfig::new(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            percent,
        );
        let key = config.storage_key();

        // Check if config already exists (for version increment)
        let version = if let Ok(Some(data)) = self.db.get(key.as_bytes()) {
            if let Ok(existing) = bincode::deserialize::<DistributionConfig>(&data) {
                existing.version + 1
            } else {
                1
            }
        } else {
            1
        };

        let mut config = config;
        config.version = version;

        // Store config
        if let Err(e) = self
            .db
            .insert(key.as_bytes(), bincode::serialize(&config).unwrap())
        {
            return MetaResponse::Error {
                message: format!("Failed to set distribution config: {}", e),
            };
        }

        info!(
            "Distribution config set: {}/{}/{} -> {}% (v{})",
            catalog,
            if schema.is_empty() { "*" } else { schema },
            if table.is_empty() { "*" } else { table },
            percent,
            version
        );

        // Publish event for cache invalidation
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "percent": percent,
            "deleted": false,
        });
        self.publish_distribution_event(&config, &payload);

        MetaResponse::DistributionConfigSet {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            percent,
            version,
        }
    }

    /// Delete distribution config
    pub(crate) fn delete_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> MetaResponse {
        let key = DistributionConfig::key_for(catalog, schema, table);

        // Check if exists
        if self.db.get(key.as_bytes()).ok().flatten().is_none() {
            return MetaResponse::DistributionConfigNotFound {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            };
        }

        // Delete
        if let Err(e) = self.db.remove(key.as_bytes()) {
            return MetaResponse::Error {
                message: format!("Failed to delete distribution config: {}", e),
            };
        }

        info!(
            "Distribution config deleted: {}/{}/{}",
            catalog,
            if schema.is_empty() { "*" } else { schema },
            if table.is_empty() { "*" } else { table },
        );

        // Publish event for cache invalidation
        let config = DistributionConfig::new(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            0,
        );
        let payload = serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "percent": 0,
            "deleted": true,
        });
        self.publish_distribution_event(&config, &payload);

        MetaResponse::DistributionConfigDeleted {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    /// Get distribution config for a specific key
    pub fn get_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<DistributionConfig> {
        let key = DistributionConfig::key_for(catalog, schema, table);
        self.db
            .get(key.as_bytes())
            .ok()
            .flatten()
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    /// Get effective distribution config for a table (hierarchical lookup)
    ///
    /// Looks up config in order: table > schema > catalog
    /// Returns the most specific matching config, or None if no config exists.
    pub fn get_effective_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<DistributionConfig> {
        // Try table-level first (most specific)
        if let Some(config) = self.get_distribution_config(catalog, schema, table) {
            debug!(
                "Found table-level distribution config for {}/{}/{}: {}%",
                catalog, schema, table, config.percent
            );
            return Some(config);
        }

        // Try schema-level
        if let Some(config) = self.get_distribution_config(catalog, schema, "") {
            debug!(
                "Found schema-level distribution config for {}/{}: {}%",
                catalog, schema, config.percent
            );
            return Some(config);
        }

        // Try catalog-level
        if let Some(config) = self.get_distribution_config(catalog, "", "") {
            debug!(
                "Found catalog-level distribution config for {}: {}%",
                catalog, config.percent
            );
            return Some(config);
        }

        None
    }

    /// List all distribution configs
    pub fn list_distribution_configs(&self) -> Vec<DistributionConfig> {
        let prefix = DistributionConfig::all_prefix();
        self.db
            .scan_prefix(prefix.as_bytes())
            .filter_map(|result| result.ok().and_then(|(_, v)| bincode::deserialize(&v).ok()))
            .collect()
    }

    /// List distribution configs for a catalog
    pub fn list_distribution_configs_for_catalog(&self, catalog: &str) -> Vec<DistributionConfig> {
        let prefix = format!(
            "{}{}",
            crate::meta::types::keys::PREFIX_DISTRIBUTION,
            catalog
        );
        self.db
            .scan_prefix(prefix.as_bytes())
            .filter_map(|result| result.ok().and_then(|(_, v)| bincode::deserialize(&v).ok()))
            .collect()
    }

    /// Helper to publish distribution config change event
    fn publish_distribution_event(&self, config: &DistributionConfig, payload: &serde_json::Value) {
        let key = match config.scope {
            DistributionScope::Catalog => config.catalog.clone(),
            DistributionScope::Schema => format!("{}/{}", config.catalog, config.schema),
            DistributionScope::Table => {
                format!("{}/{}/{}", config.catalog, config.schema, config.table)
            }
        };
        let seq = self.next_sequence();
        let event = Event::new(
            seq,
            EventType::DistributionConfigChanged,
            key,
            serde_json::to_vec(payload).unwrap_or_default(),
            String::new(),
        );
        let _ = self.db.insert(
            event.storage_key().as_bytes(),
            bincode::serialize(&event).unwrap(),
        );
    }
}
