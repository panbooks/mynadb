//! Cluster management operations

use super::MetaNode;
use crate::meta::raft::NodeId;
use openraft::BasicNode;
use std::collections::BTreeMap;
use tracing::info;

impl MetaNode {
    /// Initialize the Raft cluster with this node as the first member
    ///
    /// This should only be called on the first node in a new cluster.
    /// For joining existing clusters, use `add_learner` and `change_membership`.
    pub async fn initialize_cluster(&self) -> Result<(), String> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.config.advertise_addr.clone(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| format!("Failed to initialize cluster: {}", e))?;

        info!(
            "Cluster initialized with node {} as first member",
            self.node_id
        );
        Ok(())
    }

    /// Add a learner node to the cluster
    ///
    /// Learner nodes receive log replication but don't vote in elections.
    /// Use `change_membership` to promote a learner to a voting member.
    pub async fn add_learner(&self, learner_id: NodeId, addr: String) -> Result<(), String> {
        let node = BasicNode { addr };
        self.raft
            .add_learner(learner_id, node, true)
            .await
            .map_err(|e| format!("Failed to add learner: {}", e))?;

        info!("Added learner node {} to cluster", learner_id);
        Ok(())
    }

    /// Change cluster membership (add or remove voting members)
    ///
    /// The new membership must include all nodes that should be voting members.
    pub async fn change_membership(
        &self,
        members: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), String> {
        self.raft
            .change_membership(members, false)
            .await
            .map_err(|e| format!("Failed to change membership: {}", e))?;

        info!("Cluster membership changed");
        Ok(())
    }

    /// Check if the cluster is initialized
    pub async fn is_initialized(&self) -> bool {
        self.raft.is_initialized().await.unwrap_or(false)
    }
}
