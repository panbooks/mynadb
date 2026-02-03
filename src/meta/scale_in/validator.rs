//! Scale-In Validator
//!
//! Enforces the Golden Rule to ensure zero-downtime scale-in:
//!
//! ## Golden Rule
//!
//! 1. **NEVER retire a Standby while it still backs up active Primaries**
//!    - Before retiring a Standby, all its backed Primaries must be retired first
//!    - This prevents data loss if a Primary crashes after its Standby is removed
//!
//! 2. **NEVER retire a Primary while it still owns tables**
//!    - All tables must be migrated to other Primaries first
//!    - This ensures no writes are lost during scale-in
//!
//! ## Validation Checks
//!
//! | Rule | Description | Phase Enforced |
//! |------|-------------|----------------|
//! | GR-1 | Standbys retired only after their Primaries | Phase 3 |
//! | GR-2 | Primaries retired only after table drain | Phase 2 |
//! | MIN-P | Minimum Primary count maintained | Plan validation |
//! | MIN-S | Minimum Standby count maintained | Plan validation |
//! | HA-C | HA coverage for remaining cluster | Plan validation |

use super::types::*;
use std::collections::HashSet;
use tracing::{debug, error, info, warn};

/// Validator for scale-in plans
pub struct ScaleInValidator {
    config: ScaleInConfig,
}

impl ScaleInValidator {
    /// Create a new validator
    pub fn new(config: ScaleInConfig) -> Self {
        Self { config }
    }

    /// Validate a scale-in plan
    ///
    /// Returns Ok(()) if the plan is valid, Err with details if it violates any rules.
    pub fn validate(
        &self,
        plan: &ScaleInPlan,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        info!("Validating scale-in plan {}", plan.plan_id);

        // Run all validation rules
        self.validate_golden_rule_1(plan, cluster_state)?;
        self.validate_golden_rule_2(plan, cluster_state)?;
        self.validate_minimum_nodes(plan, cluster_state)?;
        self.validate_ha_coverage(plan, cluster_state)?;
        self.validate_phase_order(plan)?;
        self.validate_no_orphan_tables(plan, cluster_state)?;

        info!("Scale-in plan {} validated successfully", plan.plan_id);
        Ok(())
    }

    /// Golden Rule 1: Never retire a Standby while it backs up active Primaries
    ///
    /// For each standby being retired, verify that ALL primaries it backs up
    /// are also being retired (and will be retired BEFORE the standby).
    fn validate_golden_rule_1(
        &self,
        plan: &ScaleInPlan,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        let primaries_retiring: HashSet<_> = plan.primaries_to_retire.iter().collect();

        for standby_id in &plan.standbys_to_retire {
            // Get all primaries this standby backs up
            let backed_primaries = cluster_state.primaries_backed_by_standby(standby_id);

            // Check if any backed primary is NOT being retired
            let remaining_backed: Vec<_> = backed_primaries
                .iter()
                .filter(|p| !primaries_retiring.contains(p))
                .collect();

            if !remaining_backed.is_empty() {
                error!(
                    "GR-1 violation: Standby {} backs up Primaries {:?} which are NOT being retired",
                    standby_id, remaining_backed
                );

                return Err(ScaleInError::ValidationFailed(ValidationError {
                    rule: "GR-1: Standby Protection".to_string(),
                    description: format!(
                        "Standby {} still backs up active Primaries: {:?}. \
                         Retiring this Standby would leave these Primaries unprotected. \
                         Either retire those Primaries first, or don't retire this Standby.",
                        standby_id, remaining_backed
                    ),
                    nodes: std::iter::once(standby_id.clone())
                        .chain(remaining_backed.into_iter().cloned())
                        .collect(),
                }));
            }
        }

        debug!("Golden Rule 1 validated: All retiring Standbys have retiring Primaries");
        Ok(())
    }

    /// Golden Rule 2: Never retire a Primary while it owns tables
    ///
    /// Verify that the plan includes table migrations for ALL tables
    /// owned by retiring Primaries.
    fn validate_golden_rule_2(
        &self,
        plan: &ScaleInPlan,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        for primary_id in &plan.primaries_to_retire {
            let owned_tables = cluster_state.tables_for_primary(primary_id);
            let table_count = owned_tables.len();

            if table_count == 0 {
                // No tables - safe to retire directly
                continue;
            }

            // Check that all tables have migration planned
            let migrations_for_primary: Vec<_> = plan
                .migrations
                .iter()
                .filter(|m| m.from_node == *primary_id)
                .collect();

            let migrated_keys: HashSet<_> = migrations_for_primary
                .iter()
                .map(|m| m.table_key.as_str())
                .collect();

            let unmigrated: Vec<_> = owned_tables
                .iter()
                .filter(|t| !migrated_keys.contains(t.table_key().as_str()))
                .map(|t| t.table_key())
                .collect();

            if !unmigrated.is_empty() {
                error!(
                    "GR-2 violation: Primary {} owns {} tables without migration plan: {:?}",
                    primary_id,
                    unmigrated.len(),
                    unmigrated
                );

                return Err(ScaleInError::ValidationFailed(ValidationError {
                    rule: "GR-2: Table Drain".to_string(),
                    description: format!(
                        "Primary {} owns {} tables without migration plan. \
                         All tables must be migrated before retirement. \
                         Missing migrations: {:?}",
                        primary_id,
                        unmigrated.len(),
                        unmigrated
                    ),
                    nodes: vec![primary_id.clone()],
                }));
            }

            debug!(
                "Primary {} has {} tables with migration plans",
                primary_id, table_count
            );
        }

        debug!("Golden Rule 2 validated: All retiring Primaries have table migrations");
        Ok(())
    }

    /// Validate minimum node counts
    fn validate_minimum_nodes(
        &self,
        plan: &ScaleInPlan,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        let remaining_primaries = cluster_state.primaries.len() - plan.primaries_to_retire.len();
        let remaining_standbys = cluster_state.standbys.len() - plan.standbys_to_retire.len();

        // Check minimum primaries
        if remaining_primaries < self.config.min_primaries {
            return Err(ScaleInError::InsufficientNodes {
                requested: plan.primaries_to_retire.len(),
                available: cluster_state.primaries.len(),
                min_required: self.config.min_primaries,
            });
        }

        // Check minimum standbys
        if remaining_standbys < self.config.min_standbys {
            return Err(ScaleInError::InsufficientNodes {
                requested: plan.standbys_to_retire.len(),
                available: cluster_state.standbys.len(),
                min_required: self.config.min_standbys,
            });
        }

        debug!(
            "Minimum nodes validated: {} primaries, {} standbys will remain",
            remaining_primaries, remaining_standbys
        );
        Ok(())
    }

    /// Validate HA coverage for remaining cluster
    ///
    /// After scale-in, remaining tables should still have:
    /// - A primary owner assigned
    /// - Standby coverage if standbys exist
    fn validate_ha_coverage(
        &self,
        plan: &ScaleInPlan,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        let primaries_retiring: HashSet<_> = plan.primaries_to_retire.iter().cloned().collect();
        let standbys_retiring: HashSet<_> = plan.standbys_to_retire.iter().cloned().collect();

        // Get remaining nodes
        let remaining_primaries: HashSet<_> = cluster_state
            .primaries
            .iter()
            .map(|n| n.node_id.clone())
            .filter(|id| !primaries_retiring.contains(id))
            .collect();

        let remaining_standbys: HashSet<_> = cluster_state
            .standbys
            .iter()
            .map(|n| n.node_id.clone())
            .filter(|id| !standbys_retiring.contains(id))
            .collect();

        // Build expected state after migrations
        let mut post_migration_owners: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // Start with current assignments
        for (key, table) in &cluster_state.table_assignments {
            if let Some(primary) = table.primary_owner() {
                post_migration_owners.insert(key.clone(), primary.to_string());
            }
        }

        // Apply migrations
        for migration in &plan.migrations {
            post_migration_owners.insert(migration.table_key.clone(), migration.to_node.clone());
        }

        // Check all tables have valid owners
        for (table_key, owner) in &post_migration_owners {
            if !remaining_primaries.contains(owner) && !primaries_retiring.contains(owner) {
                return Err(ScaleInError::HAViolation(format!(
                    "Table {} would have no valid owner after scale-in (assigned to {})",
                    table_key, owner
                )));
            }
        }

        // Warn (but don't fail) if standbys don't cover all primaries
        if !remaining_standbys.is_empty() && remaining_primaries.len() > remaining_standbys.len() * 2 {
            warn!(
                "HA coverage may be reduced: {} primaries, {} standbys (ratio > 2:1)",
                remaining_primaries.len(),
                remaining_standbys.len()
            );
        }

        debug!(
            "HA coverage validated: {} primaries, {} standbys, {} tables",
            remaining_primaries.len(),
            remaining_standbys.len(),
            post_migration_owners.len()
        );
        Ok(())
    }

    /// Validate phase order follows Golden Rule
    ///
    /// Required order:
    /// 1. Table migration (if needed)
    /// 2. Primary retirement (after draining)
    /// 3. Standby retirement (after Primaries)
    fn validate_phase_order(&self, plan: &ScaleInPlan) -> Result<(), ScaleInError> {
        let mut seen_migrate = false;
        let mut seen_retire_primary = false;
        let mut seen_retire_standby = false;

        for phase in &plan.phases {
            match phase.phase_type {
                ScaleInPhaseType::MigrateTables => {
                    if seen_retire_primary || seen_retire_standby {
                        return Err(ScaleInError::ValidationFailed(ValidationError {
                            rule: "Phase Order".to_string(),
                            description: "Table migration must happen before any retirements"
                                .to_string(),
                            nodes: vec![],
                        }));
                    }
                    seen_migrate = true;
                }
                ScaleInPhaseType::RetirePrimaries => {
                    if seen_retire_standby {
                        return Err(ScaleInError::ValidationFailed(ValidationError {
                            rule: "Phase Order (Golden Rule)".to_string(),
                            description: "Primary retirement must happen BEFORE Standby retirement. \
                                         Standbys protect Primaries until the Primaries are retired."
                                .to_string(),
                            nodes: vec![],
                        }));
                    }
                    seen_retire_primary = true;
                }
                ScaleInPhaseType::RetireStandbys => {
                    // Standbys can only be retired after Primaries
                    // (This is the Golden Rule enforcement)
                    if !plan.primaries_to_retire.is_empty() && !seen_retire_primary {
                        return Err(ScaleInError::ValidationFailed(ValidationError {
                            rule: "Golden Rule Phase Order".to_string(),
                            description: "Standby retirement phase found before Primary retirement. \
                                         Primaries must be retired FIRST so Standbys can protect them until then."
                                .to_string(),
                            nodes: plan.standbys_to_retire.clone(),
                        }));
                    }
                    seen_retire_standby = true;
                }
                ScaleInPhaseType::Completed => {}
            }
        }

        debug!("Phase order validated: Migration → Primaries → Standbys");
        Ok(())
    }

    /// Validate no tables become orphaned
    ///
    /// After all migrations and retirements, every table must have
    /// a valid owner from the remaining primaries.
    fn validate_no_orphan_tables(
        &self,
        plan: &ScaleInPlan,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        let primaries_retiring: HashSet<_> = plan.primaries_to_retire.iter().collect();

        // Track table assignments after migrations
        let mut final_assignments: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // Start with current assignments
        for (key, table) in &cluster_state.table_assignments {
            if let Some(primary) = table.primary_owner() {
                final_assignments.insert(key.clone(), primary.to_string());
            }
        }

        // Apply migrations
        for migration in &plan.migrations {
            final_assignments.insert(migration.table_key.clone(), migration.to_node.clone());
        }

        // Check for orphans (tables assigned to retiring nodes without migration)
        let mut orphans = Vec::new();
        for (table_key, owner) in &final_assignments {
            if primaries_retiring.contains(owner) {
                orphans.push((table_key.clone(), owner.clone()));
            }
        }

        if !orphans.is_empty() {
            let orphan_info: Vec<_> = orphans
                .iter()
                .take(5) // Limit error message size
                .map(|(t, o)| format!("{} (on {})", t, o))
                .collect();

            return Err(ScaleInError::ValidationFailed(ValidationError {
                rule: "No Orphan Tables".to_string(),
                description: format!(
                    "{} table(s) would be orphaned (assigned to retiring nodes): {}{}",
                    orphans.len(),
                    orphan_info.join(", "),
                    if orphans.len() > 5 { "..." } else { "" }
                ),
                nodes: orphans.iter().map(|(_, o)| o.clone()).collect(),
            }));
        }

        debug!(
            "No orphan tables: {} tables have valid owners",
            final_assignments.len()
        );
        Ok(())
    }

    /// Validate that a specific operation is safe to execute
    ///
    /// Called during execution to double-check before each operation.
    pub fn validate_operation(
        &self,
        operation: &ScaleInOperation,
        cluster_state: &ClusterState,
    ) -> Result<(), ScaleInError> {
        match operation {
            ScaleInOperation::StartRetire { node_id } => {
                // Verify node exists and is active
                let is_primary = cluster_state.primaries.iter().any(|n| &n.node_id == node_id);
                let is_standby = cluster_state.standbys.iter().any(|n| &n.node_id == node_id);

                if !is_primary && !is_standby {
                    return Err(ScaleInError::RetirementFailed {
                        node_id: node_id.clone(),
                        reason: "Node not found or not active".to_string(),
                    });
                }

                // If it's a Primary, verify it has no tables (already drained)
                if is_primary && cluster_state.node_has_tables(node_id) {
                    return Err(ScaleInError::ValidationFailed(ValidationError {
                        rule: "GR-2: Table Drain".to_string(),
                        description: format!(
                            "Cannot retire Primary {} - still has tables",
                            node_id
                        ),
                        nodes: vec![node_id.clone()],
                    }));
                }

                // If it's a Standby, verify it doesn't back up any active Primaries
                if is_standby {
                    let backed = cluster_state.primaries_backed_by_standby(node_id);
                    if !backed.is_empty() {
                        return Err(ScaleInError::ValidationFailed(ValidationError {
                            rule: "GR-1: Standby Protection".to_string(),
                            description: format!(
                                "Cannot retire Standby {} - still backs up Primaries: {:?}",
                                node_id, backed
                            ),
                            nodes: std::iter::once(node_id.clone())
                                .chain(backed)
                                .collect(),
                        }));
                    }
                }
            }

            ScaleInOperation::MigrateTable {
                table_key,
                to_node,
                ..
            } => {
                // Verify target node is a valid Primary
                let target_is_primary = cluster_state
                    .primaries
                    .iter()
                    .any(|n| &n.node_id == to_node);

                if !target_is_primary {
                    return Err(ScaleInError::MigrationFailed {
                        table: table_key.clone(),
                        reason: format!("Target node {} is not an active Primary", to_node),
                    });
                }
            }

            _ => {
                // Other operations don't need pre-validation
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::{DataNodeMode, NodeInfo, NodeRole, NodeState, TableMeta};

    fn make_node(id: &str, mode: DataNodeMode) -> NodeInfo {
        let mut node = NodeInfo::new(id.to_string(), NodeRole::Data, format!("{}:7000", id));
        node.mode = Some(mode);
        node
    }

    fn make_table(catalog: &str, schema: &str, table: &str, primary: &str) -> TableMeta {
        TableMeta::new(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
            primary.to_string(),
        )
    }

    #[test]
    fn test_golden_rule_1_violation() {
        let config = ScaleInConfig::default();
        let validator = ScaleInValidator::new(config);

        // Cluster with 2 primaries and 1 standby
        let table = make_table("db", "default", "test", "p1");
        let mut table_assignments = std::collections::HashMap::new();
        table_assignments.insert(table.table_key(), table);

        let cluster_state = ClusterState {
            primaries: vec![
                make_node("p1", DataNodeMode::Primary),
                make_node("p2", DataNodeMode::Primary),
            ],
            standbys: vec![make_node("s1", DataNodeMode::Standby)],
            retiring: vec![],
            table_assignments,
            tables_per_node: [("p1".to_string(), 1)].into_iter().collect(),
            primary_standby_ratio: 2,
        };

        // Plan that tries to retire s1 while p1 is still active
        let plan = ScaleInPlan {
            plan_id: "test".to_string(),
            created_at: 0,
            dry_run: false,
            phases: vec![ScaleInPhase {
                phase_number: 1,
                phase_type: ScaleInPhaseType::RetireStandbys,
                description: "Bad plan".to_string(),
                affected_nodes: vec!["s1".to_string()],
                operations: vec![],
            }],
            primaries_to_retire: vec![], // p1 NOT being retired!
            standbys_to_retire: vec!["s1".to_string()],
            migrations: vec![],
            warnings: vec![],
            final_state: ClusterStatePreview {
                primary_count: 2,
                standby_count: 0,
                ratio_maintained: false,
                total_tables: 1,
            },
        };

        let result = validator.validate_golden_rule_1(&plan, &cluster_state);
        assert!(result.is_err());

        if let Err(ScaleInError::ValidationFailed(err)) = result {
            assert!(err.rule.contains("GR-1"));
        }
    }

    #[test]
    fn test_golden_rule_2_violation() {
        let config = ScaleInConfig::default();
        let validator = ScaleInValidator::new(config);

        // Cluster with table
        let table = make_table("db", "default", "test", "p1");
        let mut table_assignments = std::collections::HashMap::new();
        table_assignments.insert(table.table_key(), table);

        let cluster_state = ClusterState {
            primaries: vec![
                make_node("p1", DataNodeMode::Primary),
                make_node("p2", DataNodeMode::Primary),
            ],
            standbys: vec![],
            retiring: vec![],
            table_assignments,
            tables_per_node: [("p1".to_string(), 1)].into_iter().collect(),
            primary_standby_ratio: 2,
        };

        // Plan that tries to retire p1 without migrating its table
        let plan = ScaleInPlan {
            plan_id: "test".to_string(),
            created_at: 0,
            dry_run: false,
            phases: vec![],
            primaries_to_retire: vec!["p1".to_string()],
            standbys_to_retire: vec![],
            migrations: vec![], // No migration for table!
            warnings: vec![],
            final_state: ClusterStatePreview {
                primary_count: 1,
                standby_count: 0,
                ratio_maintained: true,
                total_tables: 1,
            },
        };

        let result = validator.validate_golden_rule_2(&plan, &cluster_state);
        assert!(result.is_err());

        if let Err(ScaleInError::ValidationFailed(err)) = result {
            assert!(err.rule.contains("GR-2"));
        }
    }

    #[test]
    fn test_phase_order_violation() {
        let config = ScaleInConfig::default();
        let validator = ScaleInValidator::new(config);

        // Plan with standbys retired before primaries
        let plan = ScaleInPlan {
            plan_id: "test".to_string(),
            created_at: 0,
            dry_run: false,
            phases: vec![
                ScaleInPhase {
                    phase_number: 1,
                    phase_type: ScaleInPhaseType::RetireStandbys, // Wrong order!
                    description: "".to_string(),
                    affected_nodes: vec![],
                    operations: vec![],
                },
                ScaleInPhase {
                    phase_number: 2,
                    phase_type: ScaleInPhaseType::RetirePrimaries,
                    description: "".to_string(),
                    affected_nodes: vec![],
                    operations: vec![],
                },
            ],
            primaries_to_retire: vec!["p1".to_string()],
            standbys_to_retire: vec!["s1".to_string()],
            migrations: vec![],
            warnings: vec![],
            final_state: ClusterStatePreview {
                primary_count: 1,
                standby_count: 0,
                ratio_maintained: true,
                total_tables: 0,
            },
        };

        let result = validator.validate_phase_order(&plan);
        assert!(result.is_err());
    }
}
