//! Scale-In Module for Zero-Downtime Cluster Shrinking
//!
//! This module implements safe, zero-downtime scale-in operations for MynaDB clusters
//! using the Cold Standby architecture (2:1 Primary:Standby ratio).
//!
//! # Golden Rule
//!
//! The scale-in process follows two critical rules to ensure zero data loss:
//!
//! 1. **NEVER retire a Standby while it still backs up active Primaries**
//!    - If a Standby is retired and its Primary crashes, data loss occurs
//!
//! 2. **NEVER retire a Primary while it still owns tables**
//!    - Tables must be migrated before a Primary can be safely removed
//!
//! # Three-Phase Execution
//!
//! ```text
//! Phase 1: Table Migration
//! ├── Move tables from target Primaries to remaining Primaries
//! ├── Standbys continue backing up all Primaries (HA maintained)
//! └── Target Primaries become empty (no tables)
//!
//! Phase 2: Retire Empty Primaries
//! ├── Mark empty Primaries as Retiring → Drained
//! ├── Standbys still exist as safety net
//! └── If any remaining Primary crashes, Standby can still take over
//!
//! Phase 3: Retire Orphaned Standbys
//! ├── Only retire Standbys whose Primaries are already removed
//! └── No risk: these Standbys have no Primaries to back up
//! ```
//!
//! # HA Unit Concept
//!
//! With 2:1 ratio, every 2 Primaries share 1 Standby:
//!
//! ```text
//! HA Unit:  [Primary A] ←→ [Standby X] ←→ [Primary B]
//!                            (backs up both)
//! ```
//!
//! Scale-in by HA units ensures the ratio is maintained.

pub mod executor;
pub mod planner;
pub mod types;
pub mod validator;

pub use executor::ScaleInExecutor;
pub use planner::ScaleInPlanner;
pub use types::*;
pub use validator::ScaleInValidator;

use crate::meta::service::MetaNode;
use crate::meta::types::{DataNodeMode, NodeInfo, NodeRole, NodeState, ScaleInRecommendation};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Manager for orchestrating scale-in operations
///
/// Coordinates the planner, validator, and executor to safely reduce cluster size.
pub struct ScaleInManager {
    /// Reference to the MetaNode for cluster operations
    meta_node: Arc<MetaNode>,
    /// Configuration for scale-in behavior
    config: ScaleInConfig,
}

impl ScaleInManager {
    /// Create a new scale-in manager
    pub fn new(meta_node: Arc<MetaNode>, config: ScaleInConfig) -> Self {
        Self { meta_node, config }
    }

    /// Generate a scale-in plan
    ///
    /// Returns a detailed plan showing what will happen during scale-in.
    /// Use `dry_run=true` to preview without executing.
    pub async fn plan(&self, request: ScaleInPlanRequest) -> Result<ScaleInPlan, ScaleInError> {
        info!(
            "Planning scale-in: {} primaries, {} standbys requested",
            request.reduce_primaries, request.reduce_standbys
        );

        // Get current cluster state
        let cluster_state = self.get_cluster_state().await?;

        // Pre-flight check (skip for dry-run since we're just previewing)
        if self.config.pre_flight_check_enabled && !request.dry_run {
            self.pre_flight_check(&cluster_state).await?;
        }

        // Generate plan
        let planner = ScaleInPlanner::new(self.config.clone());
        let plan = planner.generate_plan(&cluster_state, &request)?;

        // Validate plan
        let validator = ScaleInValidator::new(self.config.clone());
        validator.validate(&plan, &cluster_state)?;

        info!(
            "Scale-in plan generated: {} phases, {} primaries, {} standbys to retire",
            plan.phases.len(),
            plan.primaries_to_retire.len(),
            plan.standbys_to_retire.len()
        );

        Ok(plan)
    }

    /// Pre-flight health check before scale-in
    ///
    /// Verifies:
    /// 1. All nodes are healthy (recent heartbeat)
    /// 2. All Standbys are reachable
    /// 3. No nodes are already in Retiring/Drained state
    /// 4. Minimum redundancy is met
    async fn pre_flight_check(&self, cluster_state: &ClusterState) -> Result<(), ScaleInError> {
        info!("Running pre-flight health check...");

        let mut unhealthy_nodes = Vec::new();
        let sm = self.meta_node.state_machine();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Check 1: Verify all Primaries are healthy
        for primary in &cluster_state.primaries {
            if let Some(node) = sm.get_node(&primary.node_id) {
                let heartbeat_age = now - node.last_heartbeat;
                if heartbeat_age > self.config.max_heartbeat_age_secs as i64 {
                    warn!(
                        "Primary {} heartbeat is stale: {}s old (max: {}s)",
                        primary.node_id, heartbeat_age, self.config.max_heartbeat_age_secs
                    );
                    unhealthy_nodes.push(format!(
                        "Primary {} (heartbeat {}s old)",
                        primary.node_id, heartbeat_age
                    ));
                }
            } else {
                unhealthy_nodes.push(format!("Primary {} (not found)", primary.node_id));
            }
        }

        // Check 2: Verify all Standbys are healthy (CRITICAL for scale-in safety)
        for standby in &cluster_state.standbys {
            if let Some(node) = sm.get_node(&standby.node_id) {
                let heartbeat_age = now - node.last_heartbeat;
                if heartbeat_age > self.config.max_heartbeat_age_secs as i64 {
                    warn!(
                        "Standby {} heartbeat is stale: {}s old (max: {}s)",
                        standby.node_id, heartbeat_age, self.config.max_heartbeat_age_secs
                    );
                    unhealthy_nodes.push(format!(
                        "CRITICAL: Standby {} (heartbeat {}s old)",
                        standby.node_id, heartbeat_age
                    ));
                }
            } else {
                unhealthy_nodes.push(format!("CRITICAL: Standby {} (not found)", standby.node_id));
            }
        }

        // Check 3: No nodes already retiring
        if !cluster_state.retiring.is_empty() {
            let retiring_ids: Vec<_> = cluster_state.retiring.iter().map(|n| n.node_id.clone()).collect();
            return Err(ScaleInError::PreFlightCheckFailed {
                reason: format!(
                    "Cannot start new scale-in while {} nodes are already retiring",
                    retiring_ids.len()
                ),
                unhealthy_nodes: retiring_ids,
            });
        }

        // Check 4: Minimum cluster size
        if cluster_state.primaries.is_empty() {
            return Err(ScaleInError::PreFlightCheckFailed {
                reason: "No active Primaries in cluster".to_string(),
                unhealthy_nodes: vec![],
            });
        }

        // Fail if any nodes are unhealthy
        if !unhealthy_nodes.is_empty() {
            error!(
                "Pre-flight check failed: {} unhealthy nodes detected",
                unhealthy_nodes.len()
            );
            return Err(ScaleInError::PreFlightCheckFailed {
                reason: format!(
                    "{} nodes are unhealthy. Scale-in requires all nodes to be healthy.",
                    unhealthy_nodes.len()
                ),
                unhealthy_nodes,
            });
        }

        info!(
            "Pre-flight check passed: {} Primaries, {} Standbys all healthy",
            cluster_state.primaries.len(),
            cluster_state.standbys.len()
        );
        Ok(())
    }

    /// Execute a scale-in plan
    ///
    /// This performs the actual scale-in operation in the correct phase order.
    pub async fn execute(&self, plan: &ScaleInPlan) -> Result<ScaleInResult, ScaleInError> {
        info!("Executing scale-in plan: {}", plan.plan_id);

        let executor = ScaleInExecutor::new(self.meta_node.clone(), self.config.clone());
        let result = executor.execute(plan).await?;

        info!(
            "Scale-in completed: {} primaries retired, {} standbys retired",
            result.primaries_retired.len(),
            result.standbys_retired.len()
        );

        Ok(result)
    }

    /// Perform a complete scale-in operation (plan + execute)
    ///
    /// Convenience method that plans and executes in one call.
    pub async fn scale_in(
        &self,
        request: ScaleInPlanRequest,
    ) -> Result<ScaleInResult, ScaleInError> {
        // Plan
        let plan = self.plan(request).await?;

        // Execute
        self.execute(&plan).await
    }

    /// Get current cluster state for planning
    async fn get_cluster_state(&self) -> Result<ClusterState, ScaleInError> {
        let sm = self.meta_node.state_machine();

        // Get all active data nodes
        let all_nodes = sm.list_nodes(Some(NodeRole::Data), None);

        let primaries: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary) && n.state == NodeState::Active)
            .cloned()
            .collect();

        let standbys: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby) && n.state == NodeState::Active)
            .cloned()
            .collect();

        let retiring: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.state == NodeState::Retiring)
            .cloned()
            .collect();

        // Get table assignments
        let tables = sm.list_all_tables();
        let mut table_assignments = std::collections::HashMap::new();

        for table in &tables {
            let table_key = table.table_key();
            table_assignments.insert(table_key, table.clone());
        }

        // Count tables per node (by primary owner)
        let mut tables_per_node: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for table in &tables {
            if let Some(primary) = table.primary_owner() {
                *tables_per_node
                    .entry(primary.to_string())
                    .or_insert(0) += 1;
            }
        }

        Ok(ClusterState {
            primaries,
            standbys,
            retiring,
            table_assignments,
            tables_per_node,
            primary_standby_ratio: self.config.primary_standby_ratio,
        })
    }

    /// Get a recommendation for scale-in (alias for compatibility)
    pub async fn recommend(
        &self,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> Result<ScaleInRecommendation, ScaleInError> {
        self.meta_node
            .recommend_scale_in(reduce_primaries, reduce_standbys)
            .await
            .map_err(ScaleInError::MetaError)
    }

    /// Get the status of an ongoing scale-in operation
    pub async fn get_status(&self, plan_id: &str) -> Result<ScaleInStatus, ScaleInError> {
        // For now, return a basic status based on cluster state
        let sm = self.meta_node.state_machine();
        let retiring_nodes = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Retiring));
        let drained_nodes = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Drained));

        let phase = if retiring_nodes.is_empty() && drained_nodes.is_empty() {
            ScaleInPhaseType::Completed
        } else if !retiring_nodes.is_empty() {
            ScaleInPhaseType::RetirePrimaries
        } else {
            ScaleInPhaseType::Completed
        };

        Ok(ScaleInStatus {
            plan_id: plan_id.to_string(),
            phase,
            progress_percent: if retiring_nodes.is_empty() { 100 } else { 50 },
            nodes_remaining: retiring_nodes.len(),
            estimated_completion_secs: 0,
            errors: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scale_in_config_default() {
        let config = ScaleInConfig::default();
        assert_eq!(config.primary_standby_ratio, 2);
        assert_eq!(config.min_primaries, 1);
    }
}
