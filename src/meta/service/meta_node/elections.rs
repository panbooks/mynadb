//! Election and campaign operations

use super::MetaNode;
use crate::meta::types::{EventType, MetaCommand, MetaResponse};

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

        match self.apply_command(cmd).await? {
            MetaResponse::LeaderElected { term } => Ok((true, term)),
            MetaResponse::LeaderExists { .. } => Ok((false, 0)),
            MetaResponse::Error { message } => Err(message),
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }

    /// Resign from leadership
    pub async fn resign(&self, election: &str, node_id: &str, term: u64) -> Result<(), String> {
        let cmd = MetaCommand::Resign {
            election: election.to_string(),
            node_id: node_id.to_string(),
            term,
        };
        self.apply_command(cmd).await?;
        Ok(())
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
        match self.apply_command(cmd).await? {
            MetaResponse::EventPublished { sequence } => Ok(sequence),
            MetaResponse::Error { message } => Err(message),
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }
}
