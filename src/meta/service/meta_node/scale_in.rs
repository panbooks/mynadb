//! Scale-in (retirement) operations
//!
//! Uses table-level operations for routing and node-level pairing for standby management.

use super::MetaNode;
use crate::meta::proto;
use crate::meta::raft::NodeId;
use crate::meta::types::{
    MetaCommand, MetaResponse, NodeState, RetireStatus, ScaleInRecommendation,
};
use openraft::error::{ClientWriteError, RaftError};
use tracing::debug;

impl MetaNode {
    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins table reassignment to other nodes
    /// - Node continues serving existing queries until Drained
    ///
    /// Returns the number of tables that need to be reassigned.
    pub async fn start_retire(&self, node_id: &str) -> Result<usize, String> {
        let cmd = MetaCommand::StartRetire {
            node_id: node_id.to_string(),
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::RetireStarted {
                    tables_to_reassign, ..
                } => Ok(tables_to_reassign),
                MetaResponse::DataNodeNotFound { node_id } => {
                    Err(format!("Node {} not found", node_id))
                }
                MetaResponse::NodeNotRetirable {
                    node_id,
                    current_state,
                } => Err(format!(
                    "Node {} cannot be retired (state: {:?})",
                    node_id, current_state
                )),
                MetaResponse::CannotRetireNoAlternative { node_id, reason } => {
                    Err(format!("Cannot retire {}: {}", node_id, reason))
                }
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response from start_retire".to_string()),
            },
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding start_retire to leader node {}", leader_id);
                        return self.forward_start_retire_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward start_retire to leader
    async fn forward_start_retire_to_leader(
        &self,
        leader_id: NodeId,
        cmd: MetaCommand,
    ) -> Result<usize, String> {
        let channel = self
            .network
            .get_channel_to(leader_id)
            .await
            .map_err(|e| format!("Failed to connect to leader {}: {}", leader_id, e))?;

        let mut client = proto::raft_internal_client::RaftInternalClient::new(channel);

        let command_data =
            bincode::serialize(&cmd).map_err(|e| format!("Failed to serialize command: {}", e))?;

        let request = tonic::Request::new(proto::ForwardCommandRequest { command_data });

        let response = client
            .forward_command(request)
            .await
            .map_err(|e| format!("Forward to leader failed: {}", e))?
            .into_inner();

        if response.success {
            if !response.response_data.is_empty() {
                let meta_resp: MetaResponse = bincode::deserialize(&response.response_data)
                    .map_err(|e| format!("Failed to deserialize response: {}", e))?;
                match meta_resp {
                    MetaResponse::RetireStarted {
                        tables_to_reassign, ..
                    } => Ok(tables_to_reassign),
                    MetaResponse::Error { message } => Err(message),
                    _ => Err("Unexpected response".to_string()),
                }
            } else {
                Ok(response.result as usize)
            }
        } else {
            Err(response.error)
        }
    }

    /// Mark a node as fully drained (scale-in complete)
    pub async fn complete_retire(&self, node_id: &str) -> Result<(), String> {
        let cmd = MetaCommand::CompleteRetire {
            node_id: node_id.to_string(),
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Cancel a node retirement (restore to Active)
    pub async fn cancel_retire(&self, node_id: &str) -> Result<(), String> {
        let cmd = MetaCommand::CancelRetire {
            node_id: node_id.to_string(),
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Get retirement status for a node
    pub async fn get_retire_status(&self, node_id: &str) -> Result<RetireStatus, String> {
        let node = self
            .state_machine
            .get_node(node_id)
            .ok_or_else(|| format!("Node {} not found", node_id))?;

        // Only Retiring and Drained states have retirement status
        if node.state != NodeState::Retiring && node.state != NodeState::Drained {
            return Err(format!(
                "Node {} is not retiring (state: {:?})",
                node_id, node.state
            ));
        }

        // Count tables owned by this node
        let tables = self.state_machine.list_tables_for_node(node_id);
        let tables_total = tables.len();

        // For a retiring node, tables_reassigned = tables that were transferred
        // Since we're counting remaining, reassigned = 0 for now (simplified)
        Ok(RetireStatus {
            node_id: node_id.to_string(),
            state: node.state,
            started_at: node.last_heartbeat, // Approximate with last_heartbeat for now
            tables_total,
            tables_reassigned: 0,       // Would need additional tracking
            memtable_rows_remaining: 0, // Would need to query data node
            estimated_completion_secs: 0,
        })
    }

    /// Get recommendations for which nodes to scale in
    ///
    /// Returns a ScaleInRecommendation with suggested nodes to retire.
    pub async fn recommend_scale_in(
        &self,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> Result<ScaleInRecommendation, String> {
        // This is a read-only operation, so we can query the state machine directly
        Ok(self
            .state_machine
            .recommend_scale_in(reduce_primaries, reduce_standbys))
    }
}
