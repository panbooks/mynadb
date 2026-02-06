//! Scale-in (retirement) operations
//!
//! Uses table-level operations for routing and node-level pairing for standby management.

use super::MetaNode;
use crate::meta::types::{
    MetaCommand, MetaResponse, NodeState, RetireStatus, ScaleInRecommendation,
};

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

        match self.apply_command(cmd).await? {
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
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }

    /// Mark a node as fully drained (scale-in complete)
    pub async fn complete_retire(&self, node_id: &str) -> Result<(), String> {
        let cmd = MetaCommand::CompleteRetire {
            node_id: node_id.to_string(),
        };
        self.apply_command(cmd).await?;
        Ok(())
    }

    /// Cancel a node retirement (restore to Active)
    pub async fn cancel_retire(&self, node_id: &str) -> Result<(), String> {
        let cmd = MetaCommand::CancelRetire {
            node_id: node_id.to_string(),
        };
        self.apply_command(cmd).await?;
        Ok(())
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
