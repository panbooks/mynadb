//! Scale-in (retirement) operations

use super::MetaNode;
use crate::meta::proto;
use crate::meta::raft::NodeId;
use crate::meta::types::{MetaCommand, MetaResponse, NodeState, RetireStatus, ScaleInRecommendation};
use openraft::error::{ClientWriteError, RaftError};
use tracing::debug;

impl MetaNode {
    /// Start retiring a node (scale-in)
    ///
    /// Marks the node as Retiring:
    /// - Router stops sending new writes to this node
    /// - Begins partition reassignment to other nodes
    /// - Node continues serving existing queries until Drained
    ///
    /// Returns the number of partitions that need to be reassigned.
    pub async fn start_retire(&self, node_id: &str) -> Result<usize, String> {
        let cmd = MetaCommand::StartRetire {
            node_id: node_id.to_string(),
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::RetireStarted {
                    partitions_to_reassign,
                    ..
                } => Ok(partitions_to_reassign),
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
                        partitions_to_reassign,
                        ..
                    } => Ok(partitions_to_reassign),
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

        // Count partitions
        let replicas_as_leader = self.state_machine.get_replicas_by_leader(node_id);
        let partitions_total = replicas_as_leader.len();

        // For a retiring node, partitions_reassigned = partitions that were transferred
        // Since we're counting remaining, reassigned = 0 for now (simplified)
        Ok(RetireStatus {
            node_id: node_id.to_string(),
            state: node.state,
            started_at: node.last_heartbeat, // Approximate with last_heartbeat for now
            partitions_total,
            partitions_reassigned: 0,   // Would need additional tracking
            memtable_rows_remaining: 0, // Would need to query data node
            estimated_completion_secs: 0,
        })
    }

    /// Reassign a partition from a retiring node to another Primary
    pub async fn reassign_partition_for_retire(
        &self,
        partition_key: &str,
        from_node_id: &str,
        to_node_id: &str,
    ) -> Result<(), String> {
        let cmd = MetaCommand::ReassignPartitionForRetire {
            partition_key: partition_key.to_string(),
            from_node_id: from_node_id.to_string(),
            to_node_id: to_node_id.to_string(),
        };
        match self.apply_command(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Remove a standby from all partition replicas (for Standby scale-in)
    pub async fn remove_standby_from_replicas(
        &self,
        standby_node_id: &str,
    ) -> Result<usize, String> {
        let cmd = MetaCommand::RemoveStandbyFromReplicas {
            standby_node_id: standby_node_id.to_string(),
        };

        let result = self.raft.client_write(cmd.clone()).await;
        match result {
            Ok(response) => match response.data {
                MetaResponse::StandbyRemovedFromReplicas {
                    partitions_affected,
                    ..
                } => Ok(partitions_affected),
                MetaResponse::Error { message } => Err(message),
                _ => Err("Unexpected response from remove_standby_from_replicas".to_string()),
            },
            Err(e) => {
                // Handle ForwardToLeader
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id {
                        debug!(
                            "Forwarding remove_standby_from_replicas to leader node {}",
                            leader_id
                        );
                        return self.forward_remove_standby_to_leader(leader_id, cmd).await;
                    }
                }
                Err(format!("Raft error: {}", e))
            }
        }
    }

    /// Forward remove_standby_from_replicas to leader
    async fn forward_remove_standby_to_leader(
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
                    MetaResponse::StandbyRemovedFromReplicas {
                        partitions_affected,
                        ..
                    } => Ok(partitions_affected),
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
