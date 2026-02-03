//! Election and campaign operations

use super::MetaNode;
use crate::meta::proto;
use crate::meta::raft::NodeId;
use crate::meta::types::{EventType, MetaCommand, MetaResponse};
use openraft::error::{ClientWriteError, RaftError};
use tracing::debug;

impl MetaNode {
    /// Campaign for leadership in an election
    pub async fn campaign(
        &self,
        election: &str,
        node_id: &str,
        ttl_ms: u64,
    ) -> Result<(bool, u64), String> {
        let cmd = MetaCommand::Campaign {
            election: election.to_string(),
            node_id: node_id.to_string(),
            ttl_ms,
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::LeaderElected { term } => Ok((true, term)),
                MetaResponse::LeaderExists { .. } => Ok((false, 0)),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response".to_string()),
            },
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!("Forwarding campaign to leader node {}", leader_id);
                        return self.forward_campaign_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward campaign to leader
    async fn forward_campaign_to_leader(
        &self,
        leader_id: NodeId,
        cmd: MetaCommand,
    ) -> Result<(bool, u64), String> {
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
                    MetaResponse::LeaderElected { term } => Ok((true, term)),
                    MetaResponse::LeaderExists { .. } => Ok((false, 0)),
                    MetaResponse::Error { message } => Err(message),
                    _ => Err("Unexpected response".to_string()),
                }
            } else {
                // result contains term
                Ok((true, response.result))
            }
        } else {
            Err(response.error)
        }
    }

    /// Resign from leadership
    pub async fn resign(&self, election: &str, node_id: &str, term: u64) -> Result<(), String> {
        let cmd = MetaCommand::Resign {
            election: election.to_string(),
            node_id: node_id.to_string(),
            term,
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Publish an event
    pub async fn publish_event(
        &self,
        topic: &str,
        event_type: EventType,
        payload: Vec<u8>,
        source_node: &str,
    ) -> Result<u64, String> {
        let cmd = MetaCommand::PublishEvent {
            topic: topic.to_string(),
            event_type,
            payload,
            source_node: source_node.to_string(),
        };
        self.apply_command(cmd).await
    }
}
