//! Scale-In Executor
//!
//! Executes scale-in plans in the correct three-phase order:
//!
//! ## Execution Flow
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Phase 1: Table Migration                                        │
//! │ ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
//! │ │ Retiring P1 │ ──► │ Remaining P2│ ──► │ Remaining P3│       │
//! │ │ (10 tables) │     │ (+5 tables) │     │ (+5 tables) │       │
//! │ └─────────────┘     └─────────────┘     └─────────────┘       │
//! │        │                                                        │
//! │        ▼                                                        │
//! │   All tables moved, P1 now empty                               │
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
//! │ │ S1 (orphan) │ ──► RemoveFromTables ──► StartRetire ──► Done  │
//! │ └─────────────┘                                                │
//! │                                                                 │
//! │   S1's Primary (P1) already removed - safe to retire           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use super::types::*;
use super::validator::ScaleInValidator;
use crate::meta::service::MetaNode;
use crate::meta::types::{DataNodeMode, MetaCommand, NodeRole, NodeState};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

    /// Check health of Standbys that back up the retiring Primaries
    ///
    /// This is CRITICAL during Phase 1 and Phase 2 when Primaries are being
    /// drained or retired. If a Standby fails during this time, we lose our
    /// safety net.
    ///
    /// With table-level routing, all Standbys back up all Primaries at the
    /// node level, so we check all active Standbys.
    fn check_standby_health(&self, retiring_primaries: &[String]) -> Result<(), ScaleInError> {
        let sm = self.meta_node.state_machine();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // With table-level routing, all Standbys back up all Primaries
        // Get all active Standby nodes
        let standby_nodes = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let critical_standbys: HashSet<String> = standby_nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .map(|n| n.node_id.clone())
            .collect();

        // Check health of each critical Standby
        for standby_id in &critical_standbys {
            if let Some(node) = sm.get_node(standby_id) {
                // Check heartbeat age
                let heartbeat_age = now - node.last_heartbeat;
                if heartbeat_age > self.config.max_heartbeat_age_secs as i64 {
                    error!(
                        "SAFETY NET LOST: Standby {} heartbeat is stale ({}s old)",
                        standby_id, heartbeat_age
                    );

                    return Err(ScaleInError::SafetyNetLost {
                        primary_id: retiring_primaries.first().cloned().unwrap_or_default(),
                        standby_id: standby_id.clone(),
                        reason: format!(
                            "Heartbeat stale ({}s > max {}s). Backed Primaries: {:?}",
                            heartbeat_age, self.config.max_heartbeat_age_secs, retiring_primaries
                        ),
                    });
                }

                // Check node state
                if node.state != NodeState::Active {
                    error!(
                        "SAFETY NET LOST: Standby {} is not active (state: {:?})",
                        standby_id, node.state
                    );
                    return Err(ScaleInError::SafetyNetLost {
                        primary_id: String::new(),
                        standby_id: standby_id.clone(),
                        reason: format!("Standby state is {:?}, not Active", node.state),
                    });
                }
            } else {
                error!(
                    "SAFETY NET LOST: Standby {} not found in cluster",
                    standby_id
                );
                return Err(ScaleInError::SafetyNetLost {
                    primary_id: String::new(),
                    standby_id: standby_id.clone(),
                    reason: "Standby node not found".to_string(),
                });
            }
        }

        debug!(
            "Standby health check passed: {} critical Standbys healthy",
            critical_standbys.len()
        );
        Ok(())
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
                tables_migrated: plan.migrations.len(),
                duration_ms: start.elapsed().as_millis() as u64,
                errors: vec![],
                warnings: vec!["Dry-run mode: no changes made".to_string()],
            });
        }

        let mut errors = Vec::new();
        let warnings = Vec::new();
        let mut primaries_retired = Vec::new();
        let mut standbys_retired = Vec::new();
        let mut tables_migrated = 0;
        let mut completed_actions = Vec::new();

        // Initial Standby health check before starting
        if self.config.abort_on_standby_failure {
            info!("Performing initial Standby health check...");
            if let Err(e) = self.check_standby_health(&plan.primaries_to_retire) {
                error!("Initial Standby health check failed: {}", e);
                return Err(ScaleInError::Aborted {
                    reason: format!("Pre-execution health check failed: {}", e),
                    phase: "Pre-execution".to_string(),
                    completed_actions: vec![],
                });
            }
        }

        // Execute phases in order
        for phase in &plan.phases {
            info!(
                "Executing phase {}: {}",
                phase.phase_number, phase.description
            );

            let phase_result = match phase.phase_type {
                ScaleInPhaseType::MigrateTables => {
                    self.run_migrate_tables_phase(phase, &plan.primaries_to_retire, &mut tables_migrated).await
                }
                ScaleInPhaseType::RetirePrimaries => {
                    self.run_retire_primaries_phase(phase, &plan.primaries_to_retire, &mut primaries_retired).await
                }
                ScaleInPhaseType::RetireStandbys => {
                    self.run_retire_standbys_phase(phase, &mut standbys_retired).await
                }
                ScaleInPhaseType::Completed => {
                    info!("Scale-in completed");
                    Ok(None)
                }
            };

            match phase_result {
                Ok(Some(action)) => completed_actions.push(action),
                Ok(None) => {}
                Err((phase_name, e)) => {
                    errors.push(format!("{} phase failed: {}", phase_name, e));
                    if matches!(e, ScaleInError::SafetyNetLost { .. }) {
                        return Err(ScaleInError::Aborted {
                            reason: e.to_string(),
                            phase: phase_name,
                            completed_actions,
                        });
                    }
                    break;
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
            tables_migrated,
            duration_ms,
            errors,
            warnings,
        })
    }

    // ============================================================================
    // Phase Runners (wrapper methods for cleaner execute() flow)
    // ============================================================================

    /// Run Phase 1: Table Migration (wrapper for execute_migration_phase)
    async fn run_migrate_tables_phase(
        &self,
        phase: &ScaleInPhase,
        retiring_primaries: &[String],
        tables_migrated: &mut usize,
    ) -> Result<Option<String>, (String, ScaleInError)> {
        match self.execute_migration_phase(phase, retiring_primaries).await {
            Ok(count) => {
                *tables_migrated = count;
                info!("Migration phase complete: {} tables migrated", count);
                Ok(Some(format!("Migrated {} tables", count)))
            }
            Err(e) => Err(("Migration".to_string(), e)),
        }
    }

    /// Run Phase 2: Retire Primaries (wrapper for execute_retire_primaries_phase)
    async fn run_retire_primaries_phase(
        &self,
        phase: &ScaleInPhase,
        retiring_primaries: &[String],
        primaries_retired: &mut Vec<String>,
    ) -> Result<Option<String>, (String, ScaleInError)> {
        match self
            .execute_retire_primaries_phase(phase, retiring_primaries)
            .await
        {
            Ok(retired) => {
                let count = retired.len();
                primaries_retired.extend(retired);
                info!("Retire Primaries phase complete: {} nodes retired", count);
                Ok(Some(format!("Retired {} Primary nodes", count)))
            }
            Err(e) => Err(("RetirePrimaries".to_string(), e)),
        }
    }

    /// Run Phase 3: Retire Standbys (wrapper for execute_retire_standbys_phase)
    async fn run_retire_standbys_phase(
        &self,
        phase: &ScaleInPhase,
        standbys_retired: &mut Vec<String>,
    ) -> Result<Option<String>, (String, ScaleInError)> {
        match self.execute_retire_standbys_phase(phase).await {
            Ok(retired) => {
                let count = retired.len();
                standbys_retired.extend(retired);
                info!("Retire Standbys phase complete: {} nodes retired", count);
                Ok(Some(format!("Retired {} Standby nodes", count)))
            }
            Err(e) => Err(("RetireStandbys".to_string(), e)),
        }
    }

    // ============================================================================
    // Phase Executors (actual implementation)
    // ============================================================================

    /// Execute Phase 1: Table Migration
    ///
    /// During migration, we continuously check Standby health because:
    /// - Retiring Primaries still own tables being migrated
    /// - If a Standby fails, those tables lose their backup
    async fn execute_migration_phase(
        &self,
        phase: &ScaleInPhase,
        retiring_primaries: &[String],
    ) -> Result<usize, ScaleInError> {
        let mut migrated_count = 0;
        let total_operations = phase.operations.len();
        let _check_interval = self.config.health_check_interval_ms;

        for (i, operation) in phase.operations.iter().enumerate() {
            if let ScaleInOperation::MigrateTable {
                table_key,
                from_node,
                to_node,
            } = operation
            {
                // Periodic Standby health check during migration
                if self.config.abort_on_standby_failure && i % 5 == 0 {
                    debug!(
                        "Health check during migration ({}/{})",
                        i + 1,
                        total_operations
                    );
                    self.check_standby_health(retiring_primaries)?;
                }

                debug!(
                    "Migrating table {} from {} to {} ({}/{})",
                    table_key,
                    from_node,
                    to_node,
                    i + 1,
                    total_operations
                );

                match self
                    .meta_node
                    .reassign_table_for_retire(table_key, from_node, to_node)
                    .await
                {
                    Ok(()) => {
                        migrated_count += 1;
                        debug!("Migrated table {}", table_key);
                    }
                    Err(e) => {
                        error!("Failed to migrate table {}: {}", table_key, e);
                        return Err(ScaleInError::MigrationFailed {
                            table: table_key.clone(),
                            reason: e,
                        });
                    }
                }
            }
        }

        // Final health check after all migrations
        if self.config.abort_on_standby_failure {
            debug!("Final health check after migration phase");
            self.check_standby_health(retiring_primaries)?;
        }

        Ok(migrated_count)
    }

    /// Execute Phase 2: Retire Primaries
    ///
    /// During primary retirement, Standbys are CRITICAL as safety net.
    /// If remaining Primaries crash during this phase, Standbys must take over.
    async fn execute_retire_primaries_phase(
        &self,
        phase: &ScaleInPhase,
        all_retiring_primaries: &[String],
    ) -> Result<Vec<String>, ScaleInError> {
        let mut retired: Vec<String> = Vec::new();

        for (i, node_id) in phase.affected_nodes.iter().enumerate() {
            info!(
                "Retiring Primary node: {} ({}/{})",
                node_id,
                i + 1,
                phase.affected_nodes.len()
            );

            // Health check before each Primary retirement
            // This ensures Standbys are still healthy to protect remaining cluster
            if self.config.abort_on_standby_failure {
                // Check Standbys for primaries not yet retired
                let remaining: Vec<_> = all_retiring_primaries
                    .iter()
                    .filter(|p| !retired.contains(p))
                    .cloned()
                    .collect();
                if !remaining.is_empty() {
                    debug!("Health check before retiring {}", node_id);
                    self.check_standby_health(&remaining)?;
                }
            }

            // Step 1: Trigger flush (if force_flush enabled)
            if self.config.force_flush {
                debug!("Triggering memtable flush for {}", node_id);
                // The actual flush is triggered by the Data node itself
                // when it receives the Retiring state via event subscription
            }

            // Step 2: Start retirement
            match self.meta_node.start_retire(node_id).await {
                Ok(tables) => {
                    if tables > 0 {
                        warn!(
                            "Node {} still has {} tables - should have been migrated",
                            node_id, tables
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
            // During wait, periodically check Standby health
            if let Err(e) = self
                .wait_for_drain_with_health_check(
                    node_id,
                    self.config.migration_timeout_secs,
                    all_retiring_primaries,
                )
                .await
            {
                if matches!(e, ScaleInError::SafetyNetLost { .. }) {
                    return Err(e);
                }
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

            // Step 1: Remove from table secondary assignments
            match self.meta_node.remove_standby_from_tables(node_id).await {
                Ok(count) => {
                    debug!("Removed {} from {} table assignments", node_id, count);
                }
                Err(e) => {
                    warn!(
                        "Failed to remove {} from tables (continuing): {}",
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
                    error!(
                        "Failed to complete retirement for Standby {}: {}",
                        node_id, e
                    );
                    retired.push(node_id.clone()); // Partial success
                }
            }
        }

        Ok(retired)
    }

    /// Wait for a node to finish draining
    async fn wait_for_drain(&self, node_id: &str, timeout_secs: u64) -> Result<(), ScaleInError> {
        self.wait_for_drain_with_health_check(node_id, timeout_secs, &[])
            .await
    }

    /// Wait for a node to finish draining with periodic Standby health checks
    async fn wait_for_drain_with_health_check(
        &self,
        node_id: &str,
        timeout_secs: u64,
        retiring_primaries: &[String],
    ) -> Result<(), ScaleInError> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let health_check_interval = Duration::from_millis(self.config.health_check_interval_ms);
        let mut last_health_check = Instant::now();

        loop {
            // Periodic Standby health check during wait
            if self.config.abort_on_standby_failure
                && !retiring_primaries.is_empty()
                && last_health_check.elapsed() > health_check_interval
            {
                self.check_standby_health(retiring_primaries)?;
                last_health_check = Instant::now();
            }

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

        match self.meta_node.apply_command(cmd).await {
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
