//! Command application and forwarding helpers

use super::MetaNode;
use crate::meta::proto;
use crate::meta::raft::NodeId;
use crate::meta::types::{MetaCommand, MetaResponse};
use openraft::error::{ClientWriteError, RaftError};
use tracing::debug;

impl MetaNode {
    /// Apply a command through Raft consensus
    /// If this node is not the leader, transparently forwards to the leader
    pub(crate) async fn apply_command(&self, cmd: MetaCommand) -> Result<u64, String> {
        // Route through Raft for consensus
        let result = self.raft.client_write(cmd.clone()).await;

        match result {
            Ok(response) => {
                // Extract result from the MetaResponse
                match response.data {
                    MetaResponse::Success => Ok(0),
                    MetaResponse::NodeRegistered { lease_id } => Ok(lease_id),
                    MetaResponse::EventPublished { sequence } => Ok(sequence),
                    MetaResponse::LockAcquired { fencing_token } => Ok(fencing_token),
                    MetaResponse::LockReleased => Ok(0),
                    MetaResponse::LeaderElected { term } => Ok(term),
                    MetaResponse::Error { message } => Err(message),
                    _ => Ok(0),
                }
            }
            Err(e) => {
                // Handle Raft errors
                match e {
                    RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                        if let Some(leader_id) = forward.leader_id {
                            // Forward the command to the leader transparently
                            debug!(
                                "Forwarding command to leader node {} (this node: {})",
                                leader_id, self.node_id
                            );
                            self.forward_to_leader(leader_id, cmd).await
                        } else {
                            Err("No leader available".to_string())
                        }
                    }
                    _ => Err(format!("Raft error: {}", e)),
                }
            }
        }
    }

    /// Forward a command to the leader node
    pub(crate) async fn forward_to_leader(&self, leader_id: NodeId, cmd: MetaCommand) -> Result<u64, String> {
        // Get a gRPC channel to the leader
        let channel = self
            .network
            .get_channel_to(leader_id)
            .await
            .map_err(|e| format!("Failed to connect to leader {}: {}", leader_id, e))?;

        // Create the RaftInternal client (we use the internal service for forwarding)
        let mut client = proto::raft_internal_client::RaftInternalClient::new(channel);

        // Serialize the command
        let command_data =
            bincode::serialize(&cmd).map_err(|e| format!("Failed to serialize command: {}", e))?;

        let request = tonic::Request::new(proto::ForwardCommandRequest { command_data });

        // Forward to leader
        let response = client
            .forward_command(request)
            .await
            .map_err(|e| format!("Forward to leader failed: {}", e))?
            .into_inner();

        if response.success {
            Ok(response.result)
        } else {
            Err(response.error)
        }
    }

    /// Execute a forwarded command (called on the leader)
    /// This is used by the gRPC handler when receiving ForwardCommand requests
    pub async fn execute_forwarded_command(
        &self,
        command_data: Vec<u8>,
    ) -> Result<(u64, Option<MetaResponse>), String> {
        // Deserialize the command
        let cmd: MetaCommand = bincode::deserialize(&command_data)
            .map_err(|e| format!("Failed to deserialize command: {}", e))?;

        // Apply through Raft (we're the leader, so this should succeed)
        let result = self.raft.client_write(cmd).await;

        match result {
            Ok(response) => {
                let (result_value, meta_response) = match &response.data {
                    MetaResponse::Success => (0, None),
                    MetaResponse::NodeRegistered { lease_id } => (*lease_id, None),
                    MetaResponse::EventPublished { sequence } => (*sequence, None),
                    MetaResponse::LockAcquired { fencing_token } => {
                        // Return full response for lock operations
                        (*fencing_token, Some(response.data.clone()))
                    }
                    MetaResponse::LockNotAcquired { .. } => {
                        // Return full response so client can see it wasn't acquired
                        (0, Some(response.data.clone()))
                    }
                    MetaResponse::LockReleased => (0, None),
                    MetaResponse::LockRenewed { expires_at } => {
                        (*expires_at as u64, Some(response.data.clone()))
                    }
                    MetaResponse::LeaderElected { term } => (*term, Some(response.data.clone())),
                    MetaResponse::LeaderExists { .. } => {
                        // Return full response so client can see leader exists
                        (0, Some(response.data.clone()))
                    }
                    MetaResponse::Error { message } => return Err(message.clone()),
                    // For any other responses, return the full response
                    other => (0, Some(other.clone())),
                };
                Ok((result_value, meta_response))
            }
            Err(e) => Err(format!("Raft error on leader: {}", e)),
        }
    }

    /// Apply a command directly to state machine (bypassing Raft)
    /// Use only for read operations or internal state queries
    #[allow(dead_code)]
    pub(crate) fn apply_command_local(&self, cmd: MetaCommand) -> MetaResponse {
        self.state_machine.apply_command(cmd)
    }

    /// Apply a command through Raft and return the MetaResponse
    ///
    /// This is a public wrapper for internal modules that need direct
    /// access to the MetaResponse (e.g., scale_in executor).
    pub async fn apply_command_direct(&self, cmd: MetaCommand) -> Result<MetaResponse, String> {
        let result = self.raft.client_write(cmd.clone()).await;

        match result {
            Ok(response) => Ok(response.data),
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding command to leader node {}", leader_id);
                        return self.forward_command_to_leader_direct(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward a command to leader and return the direct response
    pub(crate) async fn forward_command_to_leader_direct(
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
}
