//! Node registration and heartbeat operations

use super::MetaNode;
use crate::meta::types::{MetaCommand, MetaResponse, NodeInfo};

impl MetaNode {
    /// Register a node
    pub async fn register_node(&self, info: NodeInfo, lease_ttl_ms: u64) -> Result<u64, String> {
        let cmd = MetaCommand::RegisterNode {
            info,
            lease_id: 0,
            lease_ttl_ms,
        };
        match self.apply_command(cmd).await? {
            MetaResponse::NodeRegistered { lease_id } => Ok(lease_id),
            MetaResponse::Error { message } => Err(message),
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }

    /// Send heartbeat for a node
    pub async fn heartbeat(
        &self,
        node_id: &str,
        lease_id: u64,
        timestamp: i64,
    ) -> Result<(), String> {
        let cmd = MetaCommand::Heartbeat {
            node_id: node_id.to_string(),
            lease_id,
            timestamp,
        };
        self.apply_command(cmd).await?;
        Ok(())
    }

    /// Deregister a node
    pub async fn deregister_node(&self, node_id: &str) -> Result<(), String> {
        let cmd = MetaCommand::DeregisterNode {
            node_id: node_id.to_string(),
        };
        self.apply_command(cmd).await?;
        Ok(())
    }
}
