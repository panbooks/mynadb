//! Command application and forwarding helpers

use super::MetaNode;
use crate::meta::proto;
use crate::meta::raft::NodeId;
use crate::meta::types::{MetaCommand, MetaResponse};
use openraft::error::{ClientWriteError, RaftError};
use tracing::debug;

impl MetaNode {
    /// Apply a command through Raft consensus
    ///
    /// Returns the full MetaResponse from the state machine.
    /// If this node is not the leader, transparently forwards to the leader.
    pub async fn apply_command(&self, cmd: MetaCommand) -> Result<MetaResponse, String> {
        let result = self.raft.client_write(cmd.clone()).await;

        match result {
            Ok(response) => Ok(response.data),
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!(
                            "Forwarding command to leader node {} (this node: {})",
                            leader_id, self.node_id
                        );
                        return self.forward_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward a command to the leader node
    pub(crate) async fn forward_to_leader(
        &self,
        leader_id: NodeId,
        cmd: MetaCommand,
    ) -> Result<MetaResponse, String> {
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
                bincode::deserialize(&response.response_data)
                    .map_err(|e| format!("Failed to deserialize response: {}", e))
            } else {
                Ok(MetaResponse::Success)
            }
        } else {
            Err(response.error)
        }
    }

    /// Execute a forwarded command (called on the leader)
    /// This is used by the gRPC handler when receiving ForwardCommand requests
    pub async fn execute_forwarded_command(
        &self,
        command_data: Vec<u8>,
    ) -> Result<MetaResponse, String> {
        let cmd: MetaCommand = bincode::deserialize(&command_data)
            .map_err(|e| format!("Failed to deserialize command: {}", e))?;

        let result = self.raft.client_write(cmd).await;

        match result {
            Ok(response) => Ok(response.data),
            Err(e) => Err(format!("Raft error on leader: {}", e)),
        }
    }

}
