//! Distribution config operations

use super::MetaNode;
use crate::meta::types::{DistributionConfig, MetaCommand, MetaResponse};

impl MetaNode {
    /// Set distribution config for catalog/schema/table
    pub async fn set_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        percent: u8,
    ) -> Result<MetaResponse, String> {
        let cmd = MetaCommand::SetDistributionConfig {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            percent,
        };
        self.apply_command(cmd).await
    }

    /// Delete distribution config
    pub async fn delete_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<MetaResponse, String> {
        let cmd = MetaCommand::DeleteDistributionConfig {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        };
        self.apply_command(cmd).await
    }

    /// Get distribution config for a specific key (exact match)
    pub async fn get_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<DistributionConfig> {
        self.state_machine
            .get_distribution_config(catalog, schema, table)
    }

    /// Get the best matching distribution config
    /// Matches in order: exact > schema wildcard > catalog wildcard > default
    pub async fn get_effective_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<DistributionConfig> {
        self.state_machine
            .get_effective_distribution_config(catalog, schema, table)
    }

    /// List distribution configs, optionally filtered by catalog
    pub async fn list_distribution_configs(
        &self,
        catalog: Option<&str>,
    ) -> Vec<DistributionConfig> {
        let configs = self.state_machine.list_distribution_configs();
        match catalog {
            Some(cat) => configs.into_iter().filter(|c| c.catalog == cat).collect(),
            None => configs,
        }
    }
}
