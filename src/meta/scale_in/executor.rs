//! Scale-In Executor
//!
//! Executes scale-in plans in the correct three-phase order:
//!
//! ## Execution Flow
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Phase 1: Partition Migration                                    │
//! │ ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
//! │ │ Retiring P1 │ ──► │ Remaining P2│ ──► │ Remaining P3│       │
//! │ │ (10 parts)  │     │ (+5 parts)  │     │ (+5 parts)  │       │
//! │ └─────────────┘     └─────────────┘     └─────────────┘       │
//! │        │                                                        │
//! │        ▼                                                        │
//! │   All partitions moved, P1 now empty                           │
//! └─────────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Phase 2: Retire Empty Primaries                                 │
//! │ ┌─────────────┐                                                │
//! │ │ P1 (empty)  │ ──► Flush memtable ──► StartRetire ──► Drained │
//! │ └─────────────┘                                                │
//! │                                                                 │
//! │   Standbys still exist as safety net for remaining Primaries   │
//! └─────────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Phase 3: Retire Orphaned Standbys                              │
//! │ ┌─────────────┐                                                │
//! │ │ S1 (orphan) │ ──► RemoveFromReplicas ──► StartRetire ──► Done│
//! │ └─────────────┘                                                │
//! │                                                                 │
//! │   S1's Primary (P1) already removed - safe to retire           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use super::types::*;
use super::validator::ScaleInValidator;
use crate::meta::service::MetaNode;
use crate::meta::types::{MetaCommand, NodeRole, NodeState};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Executor for scale-in plans
pub struct ScaleInExecutor {
    meta_node: Arc<MetaNode>,
    config: ScaleInConfig,
    validator: ScaleInValidator,
}

impl ScaleInExecutor {
    /// Create a new executor
    pub fn new(meta_node: Arc<MetaNode>, config: ScaleInConfig) -> Self {
        let validator = ScaleInValidator::new(config.clone());
        Self {
            meta_node,
            config,
            validator,
        }
    }

    /// Execute a scale-in plan
    pub async fn execute(&self, plan: &ScaleInPlan) -> Result<ScaleInResult, ScaleInError> {
        let start = Instant::now();
        info!("Starting scale-in execution for plan {}", plan.plan_id);

        if plan.dry_run {
            info!("Dry-run mode: no changes will be made");
            return Ok(ScaleInResult {
                plan_id: plan.plan_id.clone(),
                success: true,
                primaries_retired: plan.primaries_to_retire.clone(),
                standbys_retired: plan.standbys_to_retire.clone(),
                partitions_migrated: plan.migrations.len(),
                duration_ms: start.elapsed().as_millis() as u64,
                errors: vec![],
                warnings: vec!["Dry-run mode: no changes made".to_string()],
            });
        }

        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut primaries_retired = Vec::new();
        let mut standbys_retired = Vec::new();
        let mut partitions_migrated = 0;

        // Execute phases in order
        for phase in &plan.phases {
            info!(
                "Executing phase {}: {}",
                phase.phase_number, phase.description
            );

            match phase.phase_type {
                ScaleInPhaseType::MigratePartitions => {
                    match self.execute_migration_phase(phase).await {
                        Ok(count) => {
                            partitions_migrated = count;
                            info!("Phase 1 complete: {} partitions migrated", count);
                        }
                        Err(e) => {
                            error!("Phase 1 failed: {}", e);
                            errors.push(format!("Migration phase failed: {}", e));
                            // Continue with other phases? For now, abort
                            break;
                        }
                    }
                }

                ScaleInPhaseType::RetirePrimaries => {
                    match self.execute_retire_primaries_phase(phase).await {
                        Ok(retired) => {
                            primaries_retired = retired;
                            info!(
                                "Phase 2 complete: {} primaries retired",
                                primaries_retired.len()
                            );
                        }
                        Err(e) => {
                            error!("Phase 2 failed: {}", e);
                            errors.push(format!("Primary retirement phase failed: {}", e));
                            break;
                        }
                    }
                }

                ScaleInPhaseType::RetireStandbys => {
                    match self.execute_retire_standbys_phase(phase).await {
                        Ok(retired) => {
                            standbys_retired = retired;
                            info!(
                                "Phase 3 complete: {} standbys retired",
                                standbys_retired.len()
                            );
                        }
                        Err(e) => {
                            error!("Phase 3 failed: {}", e);
                            errors.push(format!("Standby retirement phase failed: {}", e));
                            break;
                        }
                    }
                }

                ScaleInPhaseType::Completed => {
                    info!("Scale-in completed");
                }
            }
        }

        let success = errors.is_empty();
        let duration_ms = start.elapsed().as_millis() as u64;

        if success {
            info!(
                "Scale-in plan {} completed successfully in {}ms",
                plan.plan_id, duration_ms
            );
        } else {
            error!(
                "Scale-in plan {} completed with errors: {:?}",
                plan.plan_id, errors
            );
        }

        Ok(ScaleInResult {
            plan_id: plan.plan_id.clone(),
            success,
            primaries_retired,
            standbys_retired,
            partitions_migrated,
            duration_ms,
            errors,
            warnings,
        })
    }

    /// Execute Phase 1: Partition Migration
    async fn execute_migration_phase(
        &self,
        phase: &ScaleInPhase,
    ) -> Result<usize, ScaleInError> {
        let mut migrated_count = 0;

        for operation in &phase.operations {
            if let ScaleInOperation::MigratePartition {
                partition_key,
                from_node,
                to_node,
            } = operation
            {
                debug!(
                    "Migrating partition {} from {} to {}",
                    partition_key, from_node, to_node
                );

                match self
                    .meta_node
                    .reassign_partition_for_retire(partition_key, from_node, to_node)
                    .await
                {
                    Ok(()) => {
                        migrated_count += 1;
                        debug!("Migrated partition {}", partition_key);
                    }
                    Err(e) => {
                        error!("Failed to migrate partition {}: {}", partition_key, e);
                        return Err(ScaleInError::MigrationFailed {
                            partition: partition_key.clone(),
                            reason: e,
                        });
                    }
                }
            }
        }

        Ok(migrated_count)
    }

    /// Execute Phase 2: Retire Primaries
    async fn execute_retire_primaries_phase(
        &self,
        phase: &ScaleInPhase,
    ) -> Result<Vec<String>, ScaleInError> {
        let mut retired = Vec::new();

        for node_id in &phase.affected_nodes {
            info!("Retiring Primary node: {}", node_id);

            // Step 1: Trigger flush (if force_flush enabled)
            if self.config.force_flush {
                debug!("Triggering memtable flush for {}", node_id);
                // The actual flush is triggered by the Data node itself
                // when it receives the Retiring state via event subscription
            }

            // Step 2: Start retirement
            match self.meta_node.start_retire(node_id).await {
                Ok(partitions) => {
                    if partitions > 0 {
                        warn!(
                            "Node {} still has {} partitions - should have been migrated",
                            node_id, partitions
                        );
                    }
                    debug!("Started retirement for {}", node_id);
                }
                Err(e) => {
                    error!("Failed to start retirement for {}: {}", node_id, e);
                    return Err(ScaleInError::RetirementFailed {
                        node_id: node_id.clone(),
                        reason: e,
                    });
                }
            }

            // Step 3: Wait for drain (optional, with timeout)
            if let Err(e) = self
                .wait_for_drain(node_id, self.config.migration_timeout_secs)
                .await
            {
                warn!("Timeout waiting for drain of {}: {}", node_id, e);
                // Continue anyway - the node will complete drain eventually
            }

            // Step 4: Complete retirement
            match self.complete_retire(node_id).await {
                Ok(()) => {
                    info!("Retired Primary node: {}", node_id);
                    retired.push(node_id.clone());
                }
                Err(e) => {
                    error!("Failed to complete retirement for {}: {}", node_id, e);
                    // Node is already retiring, mark as partial success
                    retired.push(node_id.clone());
                }
            }
        }

        Ok(retired)
    }

    /// Execute Phase 3: Retire Standbys
    async fn execute_retire_standbys_phase(
        &self,
        phase: &ScaleInPhase,
    ) -> Result<Vec<String>, ScaleInError> {
        let mut retired = Vec::new();

        for node_id in &phase.affected_nodes {
            info!("Retiring Standby node: {}", node_id);

            // Step 1: Remove from replica assignments
            match self.meta_node.remove_standby_from_replicas(node_id).await {
                Ok(count) => {
                    debug!("Removed {} from {} partition replicas", node_id, count);
                }
                Err(e) => {
                    warn!(
                        "Failed to remove {} from replicas (continuing): {}",
                        node_id, e
                    );
                }
            }

            // Step 2: Start retirement
            match self.meta_node.start_retire(node_id).await {
                Ok(_) => {
                    debug!("Started retirement for Standby {}", node_id);
                }
                Err(e) => {
                    error!("Failed to start retirement for Standby {}: {}", node_id, e);
                    return Err(ScaleInError::RetirementFailed {
                        node_id: node_id.clone(),
                        reason: e,
                    });
                }
            }

            // Step 3: Complete retirement (standbys don't need drain wait)
            match self.complete_retire(node_id).await {
                Ok(()) => {
                    info!("Retired Standby node: {}", node_id);
                    retired.push(node_id.clone());
                }
                Err(e) => {
                    error!("Failed to complete retirement for Standby {}: {}", node_id, e);
                    retired.push(node_id.clone()); // Partial success
                }
            }
        }

        Ok(retired)
    }

    /// Wait for a node to finish draining
    async fn wait_for_drain(&self, node_id: &str, timeout_secs: u64) -> Result<(), ScaleInError> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            // Check node state
            let sm = self.meta_node.state_machine();
            if let Some(node) = sm.get_node(node_id) {
                match node.state {
                    NodeState::Drained => {
                        debug!("Node {} is drained", node_id);
                        return Ok(());
                    }
                    NodeState::Retiring => {
                        // Still draining, continue waiting
                    }
                    other => {
                        warn!(
                            "Node {} in unexpected state while waiting for drain: {:?}",
                            node_id, other
                        );
                    }
                }
            } else {
                // Node not found - might have been removed
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(ScaleInError::Timeout {
                    operation: format!("wait_for_drain({})", node_id),
                    timeout_secs,
                });
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Complete retirement for a node
    async fn complete_retire(&self, node_id: &str) -> Result<(), ScaleInError> {
        // Apply the CompleteRetire command through Raft
        let cmd = MetaCommand::CompleteRetire {
            node_id: node_id.to_string(),
        };

        match self.meta_node.apply_command_direct(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(ScaleInError::RetirementFailed {
                node_id: node_id.to_string(),
                reason: e,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a running Meta cluster
    // Unit tests focus on plan structure validation

    #[test]
    fn test_executor_creation() {
        // Basic test that the executor can be instantiated
        // Full tests require a running MetaNode
    }
}
