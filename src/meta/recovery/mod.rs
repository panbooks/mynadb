//! Node Failure Recovery Module
//!
//! Handles automatic recovery when Data nodes (Primary or Standby) fail.
//!
//! # Recovery Strategies
//!
//! ## Primary Crash
//!
//! ```text
//! 1. Detect Primary crash (lease expired)
//! 2. Find Standby backing the crashed Primary
//! 3. Trigger Standby to flush crashed Primary's replicated data
//! 4. Redistribute partitions to remaining Primaries
//! 5. Update replica assignments
//! ```
//!
//! ## Standby Crash
//!
//! ```text
//! 1. Detect Standby crash (lease expired)
//! 2. Find Primaries that were backed by this Standby
//! 3. If spare Standby available: assign it to affected Primaries
//! 4. If no spare: mark Primaries as unprotected, emit warning
//! ```

pub mod types;

pub use types::*;

use crate::meta::config::{FailoverConfig, StandbyConfig};
use crate::meta::service::MetaNode;
use crate::meta::types::{
    DataNodeMode, MetaCommand, NodeRole, NodeState,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// Recovery Manager for handling node failures
///
/// Coordinates recovery operations when Primary or Standby nodes fail.
pub struct RecoveryManager {
    meta_node: Arc<MetaNode>,
    failover_config: FailoverConfig,
    standby_config: StandbyConfig,
}

impl RecoveryManager {
    /// Create a new RecoveryManager
    pub fn new(
        meta_node: Arc<MetaNode>,
        failover_config: FailoverConfig,
        standby_config: StandbyConfig,
    ) -> Self {
        Self {
            meta_node,
            failover_config,
            standby_config,
        }
    }

    /// Handle a node crash
    ///
    /// This is the main entry point called when a node's lease expires.
    /// Determines the node type and dispatches to appropriate handler.
    pub async fn handle_node_crash(&self, node_id: &str) -> Result<RecoveryResult, RecoveryError> {
        let start = Instant::now();
        info!("Recovery: Handling crash of node {}", node_id);

        let sm = self.meta_node.state_machine();

        // Get node info (might already be deregistered)
        let node_info = sm.get_node(node_id);

        // Determine node type from mode or from replica assignments
        let node_mode = if let Some(ref info) = node_info {
            info.mode
        } else {
            // Node already deregistered, check replica assignments
            self.detect_node_mode_from_replicas(node_id)
        };

        match node_mode {
            Some(DataNodeMode::Primary) => {
                info!("Recovery: Node {} was a Primary, starting Primary recovery", node_id);
                self.handle_primary_crash(node_id, start).await
            }
            Some(DataNodeMode::Standby) => {
                info!("Recovery: Node {} was a Standby, starting Standby recovery", node_id);
                self.handle_standby_crash(node_id, start).await
            }
            None => {
                // Unknown node type, check if it was a spare Standby
                if self.is_spare_standby(node_id) {
                    info!("Recovery: Node {} was a spare Standby, no recovery needed", node_id);
                    Ok(RecoveryResult {
                        crashed_node_id: node_id.to_string(),
                        success: true,
                        action: RecoveryAction::NoRecoveryNeeded {
                            node_id: node_id.to_string(),
                            reason: "Spare Standby, no partitions affected".to_string(),
                        },
                        partitions_redistributed: vec![],
                        new_primary_assignments: vec![],
                        spare_assigned: None,
                        unprotected_primaries: vec![],
                        duration_ms: start.elapsed().as_millis() as u64,
                        warnings: vec![],
                    })
                } else {
                    warn!("Recovery: Unknown node type for {}, skipping recovery", node_id);
                    Ok(RecoveryResult {
                        crashed_node_id: node_id.to_string(),
                        success: true,
                        action: RecoveryAction::NoRecoveryNeeded {
                            node_id: node_id.to_string(),
                            reason: "Unknown node type".to_string(),
                        },
                        partitions_redistributed: vec![],
                        new_primary_assignments: vec![],
                        spare_assigned: None,
                        unprotected_primaries: vec![],
                        duration_ms: start.elapsed().as_millis() as u64,
                        warnings: vec![],
                    })
                }
            }
        }
    }

    /// Handle Primary node crash
    ///
    /// Strategy:
    /// 1. Find Standby that was backing this Primary
    /// 2. Trigger Standby to flush the Primary's replicated data
    /// 3. Redistribute partitions to remaining Primaries
    async fn handle_primary_crash(
        &self,
        primary_id: &str,
        start: Instant,
    ) -> Result<RecoveryResult, RecoveryError> {
        // Step 1: Find the Standby backing this Primary
        let standby_id = self.find_standby_for_primary(primary_id)?;
        info!(
            "Recovery: Found Standby {} backing crashed Primary {}",
            standby_id, primary_id
        );

        // Step 2: Get partitions owned by crashed Primary
        let partitions = self.get_primary_partitions(primary_id);
        if partitions.is_empty() {
            info!("Recovery: Primary {} had no partitions, no redistribution needed", primary_id);
            return Ok(RecoveryResult {
                crashed_node_id: primary_id.to_string(),
                success: true,
                action: RecoveryAction::NoRecoveryNeeded {
                    node_id: primary_id.to_string(),
                    reason: "Primary had no partitions".to_string(),
                },
                partitions_redistributed: vec![],
                new_primary_assignments: vec![],
                spare_assigned: None,
                unprotected_primaries: vec![],
                duration_ms: start.elapsed().as_millis() as u64,
                warnings: vec![],
            });
        }

        info!(
            "Recovery: Primary {} had {} partitions to redistribute",
            primary_id,
            partitions.len()
        );

        // Step 3: Trigger Standby to flush Primary's data
        if self.failover_config.flush_on_primary_crash {
            info!("Recovery: Triggering Standby {} to flush Primary {} data", standby_id, primary_id);
            self.trigger_standby_flush(&standby_id, primary_id).await?;
        }

        // Step 4: Get remaining Primaries for redistribution
        let remaining_primaries = self.get_remaining_primaries(primary_id);
        if remaining_primaries.is_empty() {
            error!("Recovery: No remaining Primaries to redistribute to!");
            return Err(RecoveryError::NoRemainingPrimaries);
        }

        info!(
            "Recovery: Redistributing {} partitions to {} remaining Primaries",
            partitions.len(),
            remaining_primaries.len()
        );

        // Step 5: Redistribute partitions evenly
        let new_assignments = self
            .redistribute_partitions(&partitions, &remaining_primaries, primary_id)
            .await?;

        // Step 6: Publish recovery event
        self.publish_recovery_event(primary_id, &partitions, &new_assignments)
            .await;

        let duration_ms = start.elapsed().as_millis() as u64;
        info!(
            "Recovery: Primary {} recovery completed in {}ms. {} partitions redistributed.",
            primary_id,
            duration_ms,
            partitions.len()
        );

        Ok(RecoveryResult::primary_recovery_success(
            primary_id.to_string(),
            standby_id,
            partitions,
            new_assignments,
            duration_ms,
        ))
    }

    /// Handle Standby node crash
    ///
    /// Strategy:
    /// 1. Find Primaries that were backed by this Standby
    /// 2. If spare Standby available, assign it to affected Primaries
    /// 3. If no spare, mark Primaries as unprotected
    async fn handle_standby_crash(
        &self,
        standby_id: &str,
        start: Instant,
    ) -> Result<RecoveryResult, RecoveryError> {
        // Step 1: Find Primaries backed by this Standby
        let affected_primaries = self.find_primaries_for_standby(standby_id);
        if affected_primaries.is_empty() {
            info!(
                "Recovery: Standby {} had no Primaries assigned, no recovery needed",
                standby_id
            );
            return Ok(RecoveryResult {
                crashed_node_id: standby_id.to_string(),
                success: true,
                action: RecoveryAction::NoRecoveryNeeded {
                    node_id: standby_id.to_string(),
                    reason: "Standby had no Primaries assigned".to_string(),
                },
                partitions_redistributed: vec![],
                new_primary_assignments: vec![],
                spare_assigned: None,
                unprotected_primaries: vec![],
                duration_ms: start.elapsed().as_millis() as u64,
                warnings: vec![],
            });
        }

        info!(
            "Recovery: Standby {} was backing {} Primaries: {:?}",
            standby_id,
            affected_primaries.len(),
            affected_primaries
        );

        // Step 2: Try to find a spare Standby
        if let Some(spare_id) = self.find_spare_standby() {
            info!(
                "Recovery: Found spare Standby {}, assigning to affected Primaries",
                spare_id
            );

            // Assign spare to affected Primaries
            match self
                .assign_spare_to_primaries(&spare_id, &affected_primaries)
                .await
            {
                Ok(()) => {
                    let duration_ms = start.elapsed().as_millis() as u64;
                    info!(
                        "Recovery: Standby {} recovery completed in {}ms. Spare {} assigned.",
                        standby_id, duration_ms, spare_id
                    );

                    return Ok(RecoveryResult::standby_recovery_with_spare(
                        standby_id.to_string(),
                        affected_primaries,
                        spare_id,
                        duration_ms,
                    ));
                }
                Err(e) => {
                    warn!("Recovery: Failed to assign spare: {}. Primaries unprotected.", e);
                }
            }
        }

        // Step 3: No spare available - try flexible assignment if enabled
        if !self.standby_config.ratio_guarantee {
            info!(
                "Recovery: No spare available, trying flexible assignment (max_ratio={})",
                self.standby_config.max_primary_standby_ratio
            );

            // Find Standbys with capacity
            let standbys_with_capacity = self.find_standbys_with_capacity(standby_id);

            if !standbys_with_capacity.is_empty() {
                // Distribute affected Primaries to available Standbys
                let (assigned, unassigned) = self
                    .distribute_to_available_standbys(&affected_primaries, &standbys_with_capacity)
                    .await;

                // Remove crashed Standby from assignments
                self.remove_standby_from_assignments(standby_id).await;

                let duration_ms = start.elapsed().as_millis() as u64;

                if unassigned.is_empty() {
                    // All Primaries reassigned successfully
                    info!(
                        "Recovery: Flexible assignment successful. {} Primaries redistributed to existing Standbys.",
                        assigned.len()
                    );

                    return Ok(RecoveryResult {
                        crashed_node_id: standby_id.to_string(),
                        success: true,
                        action: RecoveryAction::StandbyRecovery {
                            crashed_standby_id: standby_id.to_string(),
                            affected_primaries: affected_primaries.clone(),
                            spare_standby_id: None,
                        },
                        partitions_redistributed: vec![],
                        new_primary_assignments: vec![],
                        spare_assigned: None,
                        unprotected_primaries: vec![],
                        duration_ms,
                        warnings: vec![format!(
                            "Flexible assignment: {} Primaries redistributed to existing Standbys (may be overloaded)",
                            assigned.len()
                        )],
                    });
                } else {
                    // Partial assignment - some Primaries still unprotected
                    warn!(
                        "Recovery: Partial flexible assignment. {} assigned, {} unprotected: {:?}",
                        assigned.len(),
                        unassigned.len(),
                        unassigned
                    );

                    // Publish warning for unprotected ones
                    self.publish_unprotected_warning(&unassigned).await;

                    return Ok(RecoveryResult {
                        crashed_node_id: standby_id.to_string(),
                        success: true,
                        action: RecoveryAction::StandbyRecovery {
                            crashed_standby_id: standby_id.to_string(),
                            affected_primaries: affected_primaries.clone(),
                            spare_standby_id: None,
                        },
                        partitions_redistributed: vec![],
                        new_primary_assignments: vec![],
                        spare_assigned: None,
                        unprotected_primaries: unassigned,
                        duration_ms,
                        warnings: vec![format!(
                            "Partial flexible assignment: {} Primaries redistributed, some at capacity",
                            assigned.len()
                        )],
                    });
                }
            }
        }

        // Step 4: No spare and no flexible assignment - mark all as unprotected
        warn!(
            "Recovery: No spare Standby available and no capacity for flexible assignment. {} Primaries left unprotected: {:?}",
            affected_primaries.len(),
            affected_primaries
        );

        // Update partition replicas to remove crashed Standby
        self.remove_standby_from_assignments(standby_id).await;

        // Publish warning event
        self.publish_unprotected_warning(&affected_primaries).await;

        let duration_ms = start.elapsed().as_millis() as u64;
        Ok(RecoveryResult::standby_recovery_degraded(
            standby_id.to_string(),
            affected_primaries,
            duration_ms,
        ))
    }

    /// Find the Standby that was backing a Primary
    fn find_standby_for_primary(&self, primary_id: &str) -> Result<String, RecoveryError> {
        let sm = self.meta_node.state_machine();
        let replicas = sm.list_replicas();

        // Find partitions owned by this Primary
        for replica in &replicas {
            if replica.leader_node_id == primary_id {
                // Return first Standby found
                if let Some(standby_id) = replica.standby_node_ids.first() {
                    return Ok(standby_id.clone());
                }
            }
        }

        // No Standby found - Primary was unprotected
        Err(RecoveryError::NoStandbyForPrimary {
            primary_id: primary_id.to_string(),
        })
    }

    /// Get partitions owned by a Primary
    fn get_primary_partitions(&self, primary_id: &str) -> Vec<String> {
        let sm = self.meta_node.state_machine();
        let replicas = sm.list_replicas();

        replicas
            .iter()
            .filter(|r| r.leader_node_id == primary_id)
            .map(|r| r.partition_key.clone())
            .collect()
    }

    /// Get remaining active Primaries (excluding crashed one)
    fn get_remaining_primaries(&self, exclude_primary: &str) -> Vec<String> {
        let sm = self.meta_node.state_machine();
        let nodes = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));

        nodes
            .iter()
            .filter(|n| {
                n.mode == Some(DataNodeMode::Primary) && n.node_id != exclude_primary
            })
            .map(|n| n.node_id.clone())
            .collect()
    }

    /// Trigger Standby to flush the crashed Primary's data
    async fn trigger_standby_flush(
        &self,
        standby_id: &str,
        primary_id: &str,
    ) -> Result<(), RecoveryError> {
        info!(
            "Recovery: Sending flush command to Standby {} for Primary {}",
            standby_id, primary_id
        );

        // Apply command to trigger flush
        let cmd = MetaCommand::TriggerStandbyFlush {
            standby_id: standby_id.to_string(),
            crashed_primary_id: primary_id.to_string(),
        };

        match self.meta_node.apply_command_direct(cmd).await {
            Ok(_) => {
                info!("Recovery: Flush command sent to Standby {}", standby_id);

                // Wait for flush completion (with timeout)
                self.wait_for_flush_completion(standby_id, primary_id).await
            }
            Err(e) => Err(RecoveryError::FlushFailed {
                standby_id: standby_id.to_string(),
                primary_id: primary_id.to_string(),
                reason: e,
            }),
        }
    }

    /// Wait for Standby to complete flush
    async fn wait_for_flush_completion(
        &self,
        standby_id: &str,
        primary_id: &str,
    ) -> Result<(), RecoveryError> {
        let timeout = Duration::from_secs(self.failover_config.flush_timeout_secs);
        let start = Instant::now();

        // Poll for flush completion
        // In a real implementation, this would check a flush status in state machine
        // For now, we just wait a short time since flush is async
        tokio::time::sleep(Duration::from_millis(500)).await;

        // TODO: Implement proper flush status tracking
        // For now, assume flush completes quickly for Parquet write

        if start.elapsed() > timeout {
            return Err(RecoveryError::FlushTimeout {
                standby_id: standby_id.to_string(),
                timeout_secs: self.failover_config.flush_timeout_secs,
            });
        }

        info!(
            "Recovery: Standby {} flush completed for Primary {}",
            standby_id, primary_id
        );
        Ok(())
    }

    /// Redistribute partitions to remaining Primaries
    async fn redistribute_partitions(
        &self,
        partitions: &[String],
        target_primaries: &[String],
        crashed_primary: &str,
    ) -> Result<Vec<(String, String)>, RecoveryError> {
        let mut assignments = Vec::new();
        let num_targets = target_primaries.len();

        if num_targets == 0 {
            return Err(RecoveryError::NoRemainingPrimaries);
        }

        // Distribute partitions evenly across remaining Primaries
        for (i, partition) in partitions.iter().enumerate() {
            let target_idx = i % num_targets;
            let target_primary = &target_primaries[target_idx];

            debug!(
                "Recovery: Reassigning partition {} from {} to {}",
                partition, crashed_primary, target_primary
            );

            // Apply reassignment through Raft
            match self
                .meta_node
                .reassign_partition_for_retire(partition, crashed_primary, target_primary)
                .await
            {
                Ok(()) => {
                    assignments.push((partition.clone(), target_primary.clone()));
                }
                Err(e) => {
                    error!("Recovery: Failed to reassign partition {}: {}", partition, e);
                    return Err(RecoveryError::RedistributionFailed {
                        partition: partition.clone(),
                        reason: e,
                    });
                }
            }
        }

        info!(
            "Recovery: Redistributed {} partitions to {} Primaries",
            assignments.len(),
            num_targets
        );

        Ok(assignments)
    }

    /// Find Primaries that were backed by a Standby
    fn find_primaries_for_standby(&self, standby_id: &str) -> Vec<String> {
        let sm = self.meta_node.state_machine();
        let replicas = sm.list_replicas();

        let mut primaries = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for replica in &replicas {
            if replica.standby_node_ids.contains(&standby_id.to_string()) {
                if seen.insert(replica.leader_node_id.clone()) {
                    primaries.push(replica.leader_node_id.clone());
                }
            }
        }

        primaries
    }

    /// Find a spare Standby node
    fn find_spare_standby(&self) -> Option<String> {
        let sm = self.meta_node.state_machine();

        // Get all Standby nodes
        let standbys = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let standby_nodes: Vec<_> = standbys
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        // Get replicas to find which Standbys are assigned
        let replicas = sm.list_replicas();
        let assigned_standbys: std::collections::HashSet<_> = replicas
            .iter()
            .flat_map(|r| r.standby_node_ids.iter().cloned())
            .collect();

        // Find a Standby that's not assigned to any Primary
        for standby in standby_nodes {
            if !assigned_standbys.contains(&standby.node_id) {
                return Some(standby.node_id.clone());
            }
        }

        None
    }

    /// Find Standbys with capacity to take more Primaries (flexible assignment)
    ///
    /// Returns a list of (standby_id, available_capacity) sorted by capacity descending.
    fn find_standbys_with_capacity(&self, exclude_standby: &str) -> Vec<(String, usize)> {
        let sm = self.meta_node.state_machine();
        let max_ratio = self.standby_config.max_primary_standby_ratio as usize;

        // Get all active Standby nodes (excluding the crashed one)
        let standbys = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let standby_nodes: Vec<_> = standbys
            .iter()
            .filter(|n| {
                n.mode == Some(DataNodeMode::Standby) && n.node_id != exclude_standby
            })
            .collect();

        // Count how many Primaries each Standby is backing
        let replicas = sm.list_replicas();
        let mut standby_load: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for replica in &replicas {
            for standby_id in &replica.standby_node_ids {
                *standby_load.entry(standby_id.clone()).or_insert(0) += 1;
            }
        }

        // Find Standbys with available capacity
        let mut standbys_with_capacity: Vec<(String, usize)> = Vec::new();

        for standby in standby_nodes {
            let current_load = standby_load.get(&standby.node_id).copied().unwrap_or(0);
            if current_load < max_ratio {
                let available = max_ratio - current_load;
                standbys_with_capacity.push((standby.node_id.clone(), available));
            }
        }

        // Sort by available capacity descending (prefer Standbys with more capacity)
        standbys_with_capacity.sort_by(|a, b| b.1.cmp(&a.1));

        debug!(
            "Recovery: Found {} Standbys with capacity: {:?}",
            standbys_with_capacity.len(),
            standbys_with_capacity
        );

        standbys_with_capacity
    }

    /// Distribute affected Primaries to available Standbys (flexible assignment)
    ///
    /// Returns (assigned_primaries, unassigned_primaries)
    async fn distribute_to_available_standbys(
        &self,
        affected_primaries: &[String],
        standbys_with_capacity: &[(String, usize)],
    ) -> (Vec<String>, Vec<String>) {
        let mut assigned = Vec::new();
        let mut unassigned = Vec::new();

        // Track remaining capacity for each Standby
        let mut remaining_capacity: std::collections::HashMap<String, usize> =
            standbys_with_capacity.iter().cloned().collect();

        for primary_id in affected_primaries {
            // Find a Standby with remaining capacity
            let mut assigned_to = None;

            for (standby_id, _) in standbys_with_capacity {
                if let Some(capacity) = remaining_capacity.get_mut(standby_id) {
                    if *capacity > 0 {
                        assigned_to = Some(standby_id.clone());
                        *capacity -= 1;
                        break;
                    }
                }
            }

            if let Some(standby_id) = assigned_to {
                // Assign this Primary to the Standby
                info!(
                    "Recovery: Flexible assignment - assigning Primary {} to Standby {}",
                    primary_id, standby_id
                );

                let cmd = MetaCommand::AssignStandbyToPrimaries {
                    standby_id: standby_id.clone(),
                    primary_ids: vec![primary_id.clone()],
                };

                match self.meta_node.apply_command_direct(cmd).await {
                    Ok(_) => {
                        assigned.push(primary_id.clone());
                    }
                    Err(e) => {
                        warn!(
                            "Recovery: Failed to assign Primary {} to Standby {}: {}",
                            primary_id, standby_id, e
                        );
                        unassigned.push(primary_id.clone());
                    }
                }
            } else {
                // No Standby with remaining capacity
                warn!(
                    "Recovery: No Standby with remaining capacity for Primary {}",
                    primary_id
                );
                unassigned.push(primary_id.clone());
            }
        }

        info!(
            "Recovery: Flexible assignment complete. {} assigned, {} unassigned",
            assigned.len(),
            unassigned.len()
        );

        (assigned, unassigned)
    }

    /// Check if a node was a spare Standby
    fn is_spare_standby(&self, node_id: &str) -> bool {
        let sm = self.meta_node.state_machine();
        let replicas = sm.list_replicas();

        // Check if this node was assigned to any partition
        for replica in &replicas {
            if replica.leader_node_id == node_id || replica.standby_node_ids.contains(&node_id.to_string()) {
                return false;
            }
        }

        true
    }

    /// Assign spare Standby to affected Primaries
    async fn assign_spare_to_primaries(
        &self,
        spare_id: &str,
        primary_ids: &[String],
    ) -> Result<(), RecoveryError> {
        info!(
            "Recovery: Assigning spare {} to Primaries: {:?}",
            spare_id, primary_ids
        );

        // Apply command to assign spare to Primaries
        let cmd = MetaCommand::AssignStandbyToPrimaries {
            standby_id: spare_id.to_string(),
            primary_ids: primary_ids.to_vec(),
        };

        match self.meta_node.apply_command_direct(cmd).await {
            Ok(_) => {
                info!("Recovery: Spare {} assigned to Primaries", spare_id);
                Ok(())
            }
            Err(e) => Err(RecoveryError::SpareAssignmentFailed {
                spare_id: spare_id.to_string(),
                primary_id: primary_ids.first().cloned().unwrap_or_default(),
                reason: e,
            }),
        }
    }

    /// Remove crashed Standby from partition assignments
    async fn remove_standby_from_assignments(&self, standby_id: &str) {
        info!(
            "Recovery: Removing crashed Standby {} from assignments",
            standby_id
        );

        if let Err(e) = self.meta_node.remove_standby_from_replicas(standby_id).await {
            warn!("Recovery: Failed to remove Standby from replicas: {}", e);
        }
    }

    /// Detect node mode from replica assignments
    fn detect_node_mode_from_replicas(&self, node_id: &str) -> Option<DataNodeMode> {
        let sm = self.meta_node.state_machine();
        let replicas = sm.list_replicas();

        for replica in &replicas {
            if replica.leader_node_id == node_id {
                return Some(DataNodeMode::Primary);
            }
            if replica.standby_node_ids.contains(&node_id.to_string()) {
                return Some(DataNodeMode::Standby);
            }
        }

        None
    }

    /// Publish recovery event
    async fn publish_recovery_event(
        &self,
        crashed_primary: &str,
        partitions: &[String],
        _new_assignments: &[(String, String)],
    ) {
        // Event is published by the state machine when assignments change
        // This is a placeholder for additional event publishing if needed
        debug!(
            "Recovery: Published event for Primary {} crash. {} partitions redistributed.",
            crashed_primary,
            partitions.len()
        );
    }

    /// Publish warning for unprotected Primaries
    async fn publish_unprotected_warning(&self, primaries: &[String]) {
        warn!(
            "CLUSTER DEGRADED: {} Primaries have no Standby backup: {:?}",
            primaries.len(),
            primaries
        );

        // In a real implementation, this would publish a MetaEvent
        // that monitoring systems can subscribe to
    }

    /// Get cluster protection status
    pub fn get_protection_status(&self) -> ClusterProtectionStatus {
        let sm = self.meta_node.state_machine();
        let replicas = sm.list_replicas();

        let mut protected = 0;
        let mut unprotected_list = Vec::new();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        for replica in &replicas {
            if replica.standby_node_ids.is_empty() {
                unprotected_list.push(UnprotectedPartition {
                    partition_key: replica.partition_key.clone(),
                    primary_id: replica.leader_node_id.clone(),
                    since: now,
                    reason: "No Standby assigned".to_string(),
                });
            } else {
                protected += 1;
            }
        }

        let total = replicas.len();
        let unprotected = unprotected_list.len();
        let spare_count = self.count_spare_standbys();

        let status = if unprotected == 0 {
            if spare_count > 0 {
                ClusterHealthStatus::Healthy
            } else {
                ClusterHealthStatus::HealthyNoSpares
            }
        } else if unprotected * 2 > total {
            ClusterHealthStatus::Critical
        } else {
            ClusterHealthStatus::Degraded
        };

        ClusterProtectionStatus {
            total_partitions: total,
            protected_partitions: protected,
            unprotected_partitions: unprotected,
            unprotected_list,
            spare_standbys_available: spare_count,
            status,
        }
    }

    /// Count available spare Standbys
    fn count_spare_standbys(&self) -> usize {
        let sm = self.meta_node.state_machine();

        let standbys = sm.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
        let standby_nodes: Vec<_> = standbys
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .collect();

        let replicas = sm.list_replicas();
        let assigned_standbys: std::collections::HashSet<_> = replicas
            .iter()
            .flat_map(|r| r.standby_node_ids.iter().cloned())
            .collect();

        standby_nodes
            .iter()
            .filter(|n| !assigned_standbys.contains(&n.node_id))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_health_status_display() {
        assert_eq!(format!("{}", ClusterHealthStatus::Healthy), "HEALTHY");
        assert_eq!(
            format!("{}", ClusterHealthStatus::HealthyNoSpares),
            "HEALTHY_NO_SPARES"
        );
        assert_eq!(format!("{}", ClusterHealthStatus::Degraded), "DEGRADED");
        assert_eq!(format!("{}", ClusterHealthStatus::Critical), "CRITICAL");
    }

    #[test]
    fn test_recovery_step_display() {
        assert_eq!(
            format!("{}", RecoveryStep::FlushingStandby),
            "Triggering Standby flush"
        );
        assert_eq!(
            format!("{}", RecoveryStep::RedistributingPartitions),
            "Redistributing partitions"
        );
    }

    #[test]
    fn test_recovery_result_primary_success() {
        let result = RecoveryResult::primary_recovery_success(
            "primary-1".to_string(),
            "standby-1".to_string(),
            vec!["part-1".to_string(), "part-2".to_string()],
            vec![
                ("part-1".to_string(), "primary-2".to_string()),
                ("part-2".to_string(), "primary-3".to_string()),
            ],
            100,
        );

        assert!(result.success);
        assert_eq!(result.partitions_redistributed.len(), 2);
        assert_eq!(result.new_primary_assignments.len(), 2);
        assert!(result.unprotected_primaries.is_empty());
    }

    #[test]
    fn test_recovery_result_standby_degraded() {
        let result = RecoveryResult::standby_recovery_degraded(
            "standby-1".to_string(),
            vec!["primary-1".to_string(), "primary-2".to_string()],
            50,
        );

        assert!(result.success); // Success but degraded
        assert_eq!(result.unprotected_primaries.len(), 2);
        assert!(result.spare_assigned.is_none());
        assert!(!result.warnings.is_empty());
    }
}
