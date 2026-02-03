//! Distribution config operations

use super::MetaNode;
use crate::meta::types::{DistributionConfig, MetaCommand, MetaResponse};
use openraft::error::{ClientWriteError, RaftError};
use tracing::debug;

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

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => Ok(response.data),
            Err(e) => {
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding set_distribution_config to leader {}", leader_id);
                        return self.forward_command_to_leader_direct(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
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

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => Ok(response.data),
            Err(e) => {
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!(
                            "Forwarding delete_distribution_config to leader {}",
                            leader_id
                        );
                        return self.forward_command_to_leader_direct(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
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

    /// Get effective distribution config (hierarchical lookup: table > schema > catalog)
    pub async fn get_effective_distribution_config(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<DistributionConfig> {
        self.state_machine
            .get_effective_distribution_config(catalog, schema, table)
    }

    /// List all distribution configs (optionally filtered by catalog)
    pub async fn list_distribution_configs(&self, catalog: Option<&str>) -> Vec<DistributionConfig> {
        if let Some(cat) = catalog {
            self.state_machine.list_distribution_configs_for_catalog(cat)
        } else {
            self.state_machine.list_distribution_configs()
        }
    }
}
