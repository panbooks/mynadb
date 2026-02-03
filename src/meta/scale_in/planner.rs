//! Scale-In Planner
//!
//! Generates scale-in plans that follow the Golden Rule:
//! 1. Never retire a Standby while it still backs up active Primaries
//! 2. Never retire a Primary while it still owns tables
//!
//! The planner creates a three-phase plan:
//! - Phase 1: Migrate tables away from target Primaries
//! - Phase 2: Retire empty Primaries (Standbys still protect remaining Primaries)
//! - Phase 3: Retire orphaned Standbys (no Primaries to back up)

use super::types::*;
use crate::meta::types::{DataNodeMode, NodeInfo};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Planner for generating scale-in plans
pub struct ScaleInPlanner {
    config: ScaleInConfig,
}

impl ScaleInPlanner {
    /// Create a new planner
    pub fn new(config: ScaleInConfig) -> Self {
        Self { config }
    }

    /// Generate a scale-in plan
    pub fn generate_plan(
        &self,
        cluster_state: &ClusterState,
        request: &ScaleInPlanRequest,
    ) -> Result<ScaleInPlan, ScaleInError> {
        let plan_id = Uuid::new_v4().to_string()[..8].to_string();
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        info!(
            "Generating scale-in plan {} for {} primaries, {} standbys",
            plan_id, request.reduce_primaries, request.reduce_standbys
        );

        // Select nodes to retire based on strategy
        let (primaries_to_retire, standbys_to_retire) =
            self.select_nodes_to_retire(cluster_state, request)?;

        // Validate we have enough nodes
        self.validate_node_counts(cluster_state, &primaries_to_retire, &standbys_to_retire)?;

        // Generate table migrations
        let migrations = self.plan_migrations(cluster_state, &primaries_to_retire)?;

        // Generate phases
        let phases = self.generate_phases(&primaries_to_retire, &standbys_to_retire, &migrations);

        // Calculate final state preview
        let final_state = self.preview_final_state(
            cluster_state,
            primaries_to_retire.len(),
            standbys_to_retire.len(),
        );

        // Generate warnings
        let warnings = self.generate_warnings(cluster_state, &primaries_to_retire, &final_state);

        Ok(ScaleInPlan {
            plan_id,
            created_at,
            dry_run: request.dry_run,
            phases,
            primaries_to_retire,
            standbys_to_retire,
            migrations,
            warnings,
            final_state,
        })
    }

    /// Select nodes to retire based on strategy
    fn select_nodes_to_retire(
        &self,
        cluster_state: &ClusterState,
        request: &ScaleInPlanRequest,
    ) -> Result<(Vec<String>, Vec<String>), ScaleInError> {
        // Calculate counts (handles reduce_num vs reduce_primaries/reduce_standbys priority)
        // Pass current cluster state to calculate based on FINAL desired ratio
        let (reduce_primaries, reduce_standbys) = request.calculate_counts(
            cluster_state.primary_standby_ratio,
            cluster_state.primaries.len(),
            cluster_state.standbys.len(),
        );

        debug!(
            "Calculated scale-in counts: {} primaries, {} standbys (from reduce_num={}, reduce_primaries={}, reduce_standbys={}, ratio={})",
            reduce_primaries, reduce_standbys,
            request.reduce_num, request.reduce_primaries, request.reduce_standbys,
            cluster_state.primary_standby_ratio
        );

        match request.strategy {
            ScaleInStrategy::LeastLoaded => {
                self.select_least_loaded(cluster_state, reduce_primaries, reduce_standbys)
            }
            ScaleInStrategy::FirstIn => {
                self.select_first_in(cluster_state, reduce_primaries, reduce_standbys)
            }
            ScaleInStrategy::LastIn => {
                self.select_last_in(cluster_state, reduce_primaries, reduce_standbys)
            }
            ScaleInStrategy::HAUnit => {
                self.select_by_ha_unit(cluster_state, reduce_primaries, reduce_standbys)
            }
        }
    }

    /// Select least-loaded nodes (fewest tables)
    fn select_least_loaded(
        &self,
        cluster_state: &ClusterState,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> Result<(Vec<String>, Vec<String>), ScaleInError> {
        // Sort primaries by table count (ascending)
        let mut primaries: Vec<_> = cluster_state.primaries.iter().collect();
        primaries.sort_by_key(|n| {
            cluster_state
                .tables_per_node
                .get(&n.node_id)
                .copied()
                .unwrap_or(0)
        });

        let primaries_to_retire: Vec<_> = primaries
            .into_iter()
            .take(reduce_primaries)
            .map(|n| n.node_id.clone())
            .collect();

        // For standbys, prefer those that back up retiring primaries
        let standbys_to_retire = self.select_standbys_for_primaries(
            cluster_state,
            &primaries_to_retire,
            reduce_standbys,
        );

        Ok((primaries_to_retire, standbys_to_retire))
    }

    /// Select oldest nodes first (FIFO - First In, First Out)
    fn select_first_in(
        &self,
        cluster_state: &ClusterState,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> Result<(Vec<String>, Vec<String>), ScaleInError> {
        // Sort primaries by registration time (ascending = oldest first)
        let mut primaries: Vec<_> = cluster_state.primaries.iter().collect();
        primaries.sort_by(|a, b| a.registered_at.cmp(&b.registered_at));

        let primaries_to_retire: Vec<_> = primaries
            .into_iter()
            .take(reduce_primaries)
            .map(|n| n.node_id.clone())
            .collect();

        let standbys_to_retire = self.select_standbys_for_primaries(
            cluster_state,
            &primaries_to_retire,
            reduce_standbys,
        );

        Ok((primaries_to_retire, standbys_to_retire))
    }

    /// Select most recently added nodes (LIFO - Last In, First Out)
    fn select_last_in(
        &self,
        cluster_state: &ClusterState,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> Result<(Vec<String>, Vec<String>), ScaleInError> {
        // Sort primaries by registration time (descending = most recent first)
        let mut primaries: Vec<_> = cluster_state.primaries.iter().collect();
        primaries.sort_by(|a, b| b.registered_at.cmp(&a.registered_at));

        let primaries_to_retire: Vec<_> = primaries
            .into_iter()
            .take(reduce_primaries)
            .map(|n| n.node_id.clone())
            .collect();

        let standbys_to_retire = self.select_standbys_for_primaries(
            cluster_state,
            &primaries_to_retire,
            reduce_standbys,
        );

        Ok((primaries_to_retire, standbys_to_retire))
    }

    /// Select by HA unit to maintain ratio
    fn select_by_ha_unit(
        &self,
        cluster_state: &ClusterState,
        reduce_primaries: usize,
        reduce_standbys: usize,
    ) -> Result<(Vec<String>, Vec<String>), ScaleInError> {
        let ratio = cluster_state.primary_standby_ratio as usize;

        // Calculate how many complete HA units we're removing
        // 1 HA unit = ratio primaries + 1 standby
        let ha_units_to_remove = reduce_primaries / ratio;
        let extra_primaries = reduce_primaries % ratio;

        if extra_primaries > 0 && reduce_standbys == 0 {
            warn!(
                "Removing {} primaries without matching standbys may break ratio",
                extra_primaries
            );
        }

        // Build HA units from current cluster
        let ha_units = self.build_ha_units(cluster_state);

        // Select units to remove (prefer least loaded)
        let mut units_sorted = ha_units.clone();
        units_sorted.sort_by_key(|unit| {
            unit.primary_ids
                .iter()
                .map(|id| cluster_state.tables_per_node.get(id).copied().unwrap_or(0))
                .sum::<usize>()
        });

        let mut primaries_to_retire = Vec::new();
        let mut standbys_to_retire = Vec::new();

        for unit in units_sorted.into_iter().take(ha_units_to_remove) {
            primaries_to_retire.extend(unit.primary_ids);
            if let Some(standby_id) = unit.standby_id {
                standbys_to_retire.push(standby_id);
            }
        }

        // Add extra primaries if needed
        if extra_primaries > 0 {
            let remaining_primaries: Vec<_> = cluster_state
                .primaries
                .iter()
                .filter(|p| !primaries_to_retire.contains(&p.node_id))
                .collect();

            let mut sorted = remaining_primaries;
            sorted.sort_by_key(|n| {
                cluster_state
                    .tables_per_node
                    .get(&n.node_id)
                    .copied()
                    .unwrap_or(0)
            });

            for node in sorted.into_iter().take(extra_primaries) {
                primaries_to_retire.push(node.node_id.clone());
            }
        }

        // Add extra standbys if needed
        let extra_standbys = reduce_standbys.saturating_sub(standbys_to_retire.len());
        if extra_standbys > 0 {
            let additional = self.select_standbys_for_primaries(
                cluster_state,
                &primaries_to_retire,
                extra_standbys,
            );
            standbys_to_retire.extend(additional);
        }

        Ok((primaries_to_retire, standbys_to_retire))
    }

    /// Build HA units from cluster state
    fn build_ha_units(&self, cluster_state: &ClusterState) -> Vec<HAUnit> {
        let ratio = cluster_state.primary_standby_ratio as usize;
        let mut units = Vec::new();
        let mut unit_counter = 0;

        // Group primaries into units of `ratio` size
        let primaries: Vec<_> = cluster_state
            .primaries
            .iter()
            .map(|n| n.node_id.clone())
            .collect();

        for chunk in primaries.chunks(ratio) {
            let unit_id = format!("ha-unit-{}", unit_counter);
            unit_counter += 1;

            // Find standby that backs up these primaries
            let standby_id = self.find_standby_for_primaries(cluster_state, chunk);

            units.push(HAUnit {
                unit_id,
                primary_ids: chunk.to_vec(),
                standby_id,
            });
        }

        units
    }

    /// Find a standby that backs up the given primaries
    ///
    /// Note: With table-level ownership, standbys back up Primary nodes at the
    /// node level (not per-table). This function looks for standbys that
    /// are configured to back up the given primaries.
    fn find_standby_for_primaries(
        &self,
        cluster_state: &ClusterState,
        primary_ids: &[String],
    ) -> Option<String> {
        // Find standby nodes that are backing up any of these primaries
        // Note: With table-level routing, standbys back up entire Primary nodes
        // rather than individual tables. We pick any standby.
        if cluster_state.standbys.is_empty() {
            return None;
        }

        // Return the first available standby
        cluster_state.standbys.first().map(|s| s.node_id.clone())
    }

    /// Select standbys that back up the retiring primaries
    fn select_standbys_for_primaries(
        &self,
        cluster_state: &ClusterState,
        retiring_primaries: &[String],
        max_standbys: usize,
    ) -> Vec<String> {
        if max_standbys == 0 {
            return Vec::new();
        }

        // Find standbys that back up ONLY retiring primaries
        // These are "orphaned" after the primaries are retired
        let mut orphan_candidates: Vec<(String, usize)> = Vec::new();

        for standby in &cluster_state.standbys {
            let backed_primaries = cluster_state.primaries_backed_by_standby(&standby.node_id);

            // Check if ALL backed primaries are being retired
            let all_retiring = backed_primaries
                .iter()
                .all(|p| retiring_primaries.contains(p));

            if all_retiring && !backed_primaries.is_empty() {
                orphan_candidates.push((standby.node_id.clone(), backed_primaries.len()));
            }
        }

        // Sort by number of backed primaries (prefer standbys backing more retiring primaries)
        orphan_candidates.sort_by(|a, b| b.1.cmp(&a.1));

        orphan_candidates
            .into_iter()
            .take(max_standbys)
            .map(|(id, _)| id)
            .collect()
    }

    /// Validate node counts before generating plan
    fn validate_node_counts(
        &self,
        cluster_state: &ClusterState,
        primaries_to_retire: &[String],
        standbys_to_retire: &[String],
    ) -> Result<(), ScaleInError> {
        let remaining_primaries = cluster_state.primaries.len() - primaries_to_retire.len();
        let remaining_standbys = cluster_state.standbys.len() - standbys_to_retire.len();

        // Check minimum primaries
        if remaining_primaries < self.config.min_primaries {
            return Err(ScaleInError::InsufficientNodes {
                requested: primaries_to_retire.len(),
                available: cluster_state.primaries.len(),
                min_required: self.config.min_primaries,
            });
        }

        // Check minimum standbys
        if remaining_standbys < self.config.min_standbys {
            return Err(ScaleInError::InsufficientNodes {
                requested: standbys_to_retire.len(),
                available: cluster_state.standbys.len(),
                min_required: self.config.min_standbys,
            });
        }

        // Check that we can migrate tables
        if remaining_primaries == 0 && cluster_state.total_tables() > 0 {
            return Err(ScaleInError::HAViolation(
                "Cannot remove all primaries when tables exist".to_string(),
            ));
        }

        // Ratio-Preserving Guarantee: actual_ratio <= config_ratio
        // Each Standby backs AT MOST `ratio` Primaries (prevents overload/OOM)
        if remaining_standbys > 0 {
            let ratio = self.config.primary_standby_ratio as usize;
            let actual_ratio = remaining_primaries / remaining_standbys;

            if actual_ratio > ratio {
                return Err(ScaleInError::HAViolation(format!(
                    "Ratio violation: final state {}P + {}S (ratio {:.1}:1) exceeds config ratio {}:1. \
                     Each Standby would back up {} Primaries, max allowed is {}. \
                     Remove fewer Standbys or more Primaries.",
                    remaining_primaries,
                    remaining_standbys,
                    remaining_primaries as f64 / remaining_standbys as f64,
                    ratio,
                    actual_ratio,
                    ratio
                )));
            }
        }

        Ok(())
    }

    /// Plan table migrations
    fn plan_migrations(
        &self,
        cluster_state: &ClusterState,
        primaries_to_retire: &[String],
    ) -> Result<Vec<TableMigration>, ScaleInError> {
        let mut migrations = Vec::new();

        // Get remaining primaries
        let remaining_primaries: Vec<_> = cluster_state
            .primaries
            .iter()
            .filter(|p| !primaries_to_retire.contains(&p.node_id))
            .map(|p| p.node_id.clone())
            .collect();

        if remaining_primaries.is_empty() && !primaries_to_retire.is_empty() {
            // This shouldn't happen due to validation, but double-check
            let total_tables = primaries_to_retire
                .iter()
                .filter_map(|id| cluster_state.tables_per_node.get(id))
                .sum::<usize>();

            if total_tables > 0 {
                return Err(ScaleInError::HAViolation(
                    "No remaining primaries to receive migrated tables".to_string(),
                ));
            }
        }

        // Track how many tables each remaining primary will receive
        let mut target_table_counts: HashMap<String, usize> = remaining_primaries
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    cluster_state
                        .tables_per_node
                        .get(id)
                        .copied()
                        .unwrap_or(0),
                )
            })
            .collect();

        // For each retiring primary, migrate its tables
        for retiring_id in primaries_to_retire {
            let tables = cluster_state.tables_for_primary(retiring_id);

            for table in tables {
                // Select least-loaded remaining primary as target
                let target_id = target_table_counts
                    .iter()
                    .min_by_key(|(_, count)| *count)
                    .map(|(id, _)| id.clone());

                if let Some(target) = target_id {
                    migrations.push(TableMigration {
                        table_key: table.table_key(),
                        from_node: retiring_id.clone(),
                        to_node: target.clone(),
                        reason: format!("Retiring node {}", retiring_id),
                    });

                    // Update target count
                    *target_table_counts.get_mut(&target).unwrap() += 1;
                }
            }
        }

        debug!("Planned {} table migrations", migrations.len());
        Ok(migrations)
    }

    /// Generate the three phases
    fn generate_phases(
        &self,
        primaries_to_retire: &[String],
        standbys_to_retire: &[String],
        migrations: &[TableMigration],
    ) -> Vec<ScaleInPhase> {
        let mut phases = Vec::new();

        // Phase 1: Migrate Tables
        if !migrations.is_empty() {
            let operations: Vec<_> = migrations
                .iter()
                .map(|m| ScaleInOperation::MigrateTable {
                    table_key: m.table_key.clone(),
                    from_node: m.from_node.clone(),
                    to_node: m.to_node.clone(),
                })
                .collect();

            phases.push(ScaleInPhase {
                phase_number: 1,
                phase_type: ScaleInPhaseType::MigrateTables,
                description: format!(
                    "Migrate {} tables away from retiring Primaries",
                    migrations.len()
                ),
                affected_nodes: primaries_to_retire.to_vec(),
                operations,
            });
        }

        // Phase 2: Retire Primaries
        if !primaries_to_retire.is_empty() {
            let mut operations = Vec::new();

            for node_id in primaries_to_retire {
                // Flush memtable first
                if self.config.force_flush {
                    operations.push(ScaleInOperation::FlushMemtable {
                        node_id: node_id.clone(),
                    });
                }

                // Start retirement
                operations.push(ScaleInOperation::StartRetire {
                    node_id: node_id.clone(),
                });

                // Wait for drain
                operations.push(ScaleInOperation::WaitForDrain {
                    node_id: node_id.clone(),
                    timeout_secs: self.config.migration_timeout_secs,
                });

                // Complete retirement
                operations.push(ScaleInOperation::CompleteRetire {
                    node_id: node_id.clone(),
                });
            }

            phases.push(ScaleInPhase {
                phase_number: 2,
                phase_type: ScaleInPhaseType::RetirePrimaries,
                description: format!(
                    "Retire {} empty Primary nodes (Standbys still protect remaining cluster)",
                    primaries_to_retire.len()
                ),
                affected_nodes: primaries_to_retire.to_vec(),
                operations,
            });
        }

        // Phase 3: Retire Standbys
        if !standbys_to_retire.is_empty() {
            let mut operations = Vec::new();

            for node_id in standbys_to_retire {
                // Remove from table secondary assignments first
                operations.push(ScaleInOperation::RemoveStandbyFromTables {
                    standby_node_id: node_id.clone(),
                });

                // Start retirement
                operations.push(ScaleInOperation::StartRetire {
                    node_id: node_id.clone(),
                });

                // Complete retirement (standbys don't need drain wait)
                operations.push(ScaleInOperation::CompleteRetire {
                    node_id: node_id.clone(),
                });
            }

            phases.push(ScaleInPhase {
                phase_number: 3,
                phase_type: ScaleInPhaseType::RetireStandbys,
                description: format!(
                    "Retire {} orphaned Standby nodes (their Primaries are already removed)",
                    standbys_to_retire.len()
                ),
                affected_nodes: standbys_to_retire.to_vec(),
                operations,
            });
        }

        phases
    }

    /// Preview the final cluster state
    fn preview_final_state(
        &self,
        cluster_state: &ClusterState,
        retire_primaries: usize,
        retire_standbys: usize,
    ) -> ClusterStatePreview {
        let primary_count = cluster_state.primaries.len() - retire_primaries;
        let standby_count = cluster_state.standbys.len() - retire_standbys;

        // Check if ratio is maintained
        let expected_standbys = if cluster_state.primary_standby_ratio > 0 {
            primary_count / (cluster_state.primary_standby_ratio as usize)
        } else {
            0
        };
        let ratio_maintained = standby_count >= expected_standbys;

        ClusterStatePreview {
            primary_count,
            standby_count,
            ratio_maintained,
            total_tables: cluster_state.total_tables(),
        }
    }

    /// Generate warnings about the plan
    fn generate_warnings(
        &self,
        cluster_state: &ClusterState,
        primaries_to_retire: &[String],
        final_state: &ClusterStatePreview,
    ) -> Vec<String> {
        let mut warnings = Vec::new();

        // Warn if ratio won't be maintained
        if !final_state.ratio_maintained {
            warnings.push(format!(
                "Primary:Standby ratio will not be maintained after scale-in ({} primaries, {} standbys)",
                final_state.primary_count, final_state.standby_count
            ));
        }

        // Warn about heavy migration load
        let total_tables_to_migrate: usize = primaries_to_retire
            .iter()
            .filter_map(|id| cluster_state.tables_per_node.get(id))
            .sum();

        if total_tables_to_migrate > 100 {
            warnings.push(format!(
                "Large migration: {} tables will be moved (may take time)",
                total_tables_to_migrate
            ));
        }

        // Warn if only one primary remains
        if final_state.primary_count == 1 {
            warnings.push("Only 1 Primary will remain - no redundancy for tables".to_string());
        }

        // Warn if no standbys remain
        if final_state.standby_count == 0 && cluster_state.standbys.len() > 0 {
            warnings.push("No Standbys will remain - no failover protection".to_string());
        }

        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::{DataNodeMode, NodeInfo, NodeRole, NodeState};

    fn make_node(id: &str, mode: DataNodeMode) -> NodeInfo {
        let mut node = NodeInfo::new(id.to_string(), NodeRole::Data, format!("{}:7000", id));
        node.mode = Some(mode);
        node
    }

    #[test]
    fn test_generate_plan_empty_cluster() {
        let config = ScaleInConfig::default();
        let planner = ScaleInPlanner::new(config);

        let cluster_state = ClusterState {
            primaries: vec![],
            standbys: vec![],
            retiring: vec![],
            table_assignments: HashMap::new(),
            tables_per_node: HashMap::new(),
            primary_standby_ratio: 2,
        };

        let request = ScaleInPlanRequest {
            reduce_primaries: 1,
            ..Default::default()
        };

        let result = planner.generate_plan(&cluster_state, &request);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_least_loaded() {
        let config = ScaleInConfig::default();
        let planner = ScaleInPlanner::new(config);

        let cluster_state = ClusterState {
            primaries: vec![
                make_node("p1", DataNodeMode::Primary),
                make_node("p2", DataNodeMode::Primary),
                make_node("p3", DataNodeMode::Primary),
            ],
            standbys: vec![make_node("s1", DataNodeMode::Standby)],
            retiring: vec![],
            table_assignments: HashMap::new(),
            tables_per_node: [
                ("p1".to_string(), 10),
                ("p2".to_string(), 5),
                ("p3".to_string(), 20),
            ]
            .into_iter()
            .collect(),
            primary_standby_ratio: 2,
        };

        let result = planner.select_least_loaded(&cluster_state, 1, 0);
        assert!(result.is_ok());
        let (primaries, _) = result.unwrap();
        assert_eq!(primaries, vec!["p2".to_string()]); // p2 has fewest tables
    }
}
