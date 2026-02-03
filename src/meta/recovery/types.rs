//! Recovery Types
//!
//! Types and errors for node failure recovery operations.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Recovery action to perform when a node fails
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryAction {
    /// Primary crashed: Standby flushes data, tables redistributed
    PrimaryRecovery {
        crashed_primary_id: String,
        standby_id: String,
        tables_to_redistribute: Vec<String>,
    },

    /// Standby crashed: Assign spare or mark Primary as unprotected
    StandbyRecovery {
        crashed_standby_id: String,
        affected_primaries: Vec<String>,
        spare_standby_id: Option<String>,
    },

    /// No recovery needed (node had no tables or was spare)
    NoRecoveryNeeded { node_id: String, reason: String },
}

/// Status of a recovery operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStatus {
    /// Recovery in progress
    InProgress {
        action: RecoveryAction,
        started_at: i64,
        current_step: RecoveryStep,
    },

    /// Recovery completed successfully
    Completed {
        action: RecoveryAction,
        completed_at: i64,
        duration_ms: u64,
    },

    /// Recovery failed
    Failed {
        action: RecoveryAction,
        failed_at: i64,
        error: String,
    },
}

/// Current step in recovery process
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStep {
    /// Detecting failure type
    Detecting,

    /// Triggering Standby to flush Primary's data
    FlushingStandby,

    /// Waiting for flush to complete
    WaitingForFlush,

    /// Redistributing tables to remaining Primaries
    RedistributingTables,

    /// Assigning spare Standby
    AssigningSpare,

    /// Updating table ownership
    UpdatingOwnership,

    /// Marking Primaries as unprotected
    MarkingUnprotected,

    /// Finalizing recovery
    Finalizing,
}

impl fmt::Display for RecoveryStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryStep::Detecting => write!(f, "Detecting failure type"),
            RecoveryStep::FlushingStandby => write!(f, "Triggering Standby flush"),
            RecoveryStep::WaitingForFlush => write!(f, "Waiting for flush"),
            RecoveryStep::RedistributingTables => write!(f, "Redistributing tables"),
            RecoveryStep::AssigningSpare => write!(f, "Assigning spare Standby"),
            RecoveryStep::UpdatingOwnership => write!(f, "Updating table ownership"),
            RecoveryStep::MarkingUnprotected => write!(f, "Marking unprotected"),
            RecoveryStep::Finalizing => write!(f, "Finalizing"),
        }
    }
}

/// Error types for recovery operations
#[derive(Debug, Clone)]
pub enum RecoveryError {
    /// No Standby found for crashed Primary
    NoStandbyForPrimary { primary_id: String },

    /// Standby flush failed
    FlushFailed {
        standby_id: String,
        primary_id: String,
        reason: String,
    },

    /// Flush timed out
    FlushTimeout {
        standby_id: String,
        timeout_secs: u64,
    },

    /// No remaining Primaries to redistribute to
    NoRemainingPrimaries,

    /// Table redistribution failed
    RedistributionFailed { table_key: String, reason: String },

    /// No spare Standby available
    NoSpareAvailable { affected_primaries: Vec<String> },

    /// Spare assignment failed
    SpareAssignmentFailed {
        spare_id: String,
        primary_id: String,
        reason: String,
    },

    /// Meta operation failed
    MetaError(String),

    /// Recovery was aborted
    Aborted { reason: String },
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryError::NoStandbyForPrimary { primary_id } => {
                write!(f, "No Standby found for crashed Primary {}", primary_id)
            }
            RecoveryError::FlushFailed {
                standby_id,
                primary_id,
                reason,
            } => {
                write!(
                    f,
                    "Standby {} failed to flush Primary {} data: {}",
                    standby_id, primary_id, reason
                )
            }
            RecoveryError::FlushTimeout {
                standby_id,
                timeout_secs,
            } => {
                write!(
                    f,
                    "Standby {} flush timed out after {}s",
                    standby_id, timeout_secs
                )
            }
            RecoveryError::NoRemainingPrimaries => {
                write!(f, "No remaining Primaries to redistribute tables to")
            }
            RecoveryError::RedistributionFailed { table_key, reason } => {
                write!(f, "Failed to redistribute table {}: {}", table_key, reason)
            }
            RecoveryError::NoSpareAvailable { affected_primaries } => {
                write!(
                    f,
                    "No spare Standby available. Primaries without backup: {:?}",
                    affected_primaries
                )
            }
            RecoveryError::SpareAssignmentFailed {
                spare_id,
                primary_id,
                reason,
            } => {
                write!(
                    f,
                    "Failed to assign spare {} to Primary {}: {}",
                    spare_id, primary_id, reason
                )
            }
            RecoveryError::MetaError(e) => write!(f, "Meta operation failed: {}", e),
            RecoveryError::Aborted { reason } => write!(f, "Recovery aborted: {}", reason),
        }
    }
}

impl std::error::Error for RecoveryError {}

/// Result of a recovery operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryResult {
    /// The crashed node ID
    pub crashed_node_id: String,

    /// Whether recovery was successful
    pub success: bool,

    /// Action that was taken
    pub action: RecoveryAction,

    /// Tables that were redistributed (for Primary crash)
    pub tables_redistributed: Vec<String>,

    /// New Primary assignments (table_key -> new_primary_id)
    pub new_primary_assignments: Vec<(String, String)>,

    /// Spare Standby that was assigned (for Standby crash)
    pub spare_assigned: Option<String>,

    /// Primaries left unprotected (no Standby)
    pub unprotected_primaries: Vec<String>,

    /// Duration of recovery operation
    pub duration_ms: u64,

    /// Warnings during recovery
    pub warnings: Vec<String>,
}

impl RecoveryResult {
    /// Create a successful Primary recovery result
    pub fn primary_recovery_success(
        crashed_primary_id: String,
        standby_id: String,
        tables: Vec<String>,
        new_assignments: Vec<(String, String)>,
        duration_ms: u64,
    ) -> Self {
        Self {
            crashed_node_id: crashed_primary_id.clone(),
            success: true,
            action: RecoveryAction::PrimaryRecovery {
                crashed_primary_id,
                standby_id,
                tables_to_redistribute: tables.clone(),
            },
            tables_redistributed: tables,
            new_primary_assignments: new_assignments,
            spare_assigned: None,
            unprotected_primaries: vec![],
            duration_ms,
            warnings: vec![],
        }
    }

    /// Create a successful Standby recovery result with spare assignment
    pub fn standby_recovery_with_spare(
        crashed_standby_id: String,
        affected_primaries: Vec<String>,
        spare_id: String,
        duration_ms: u64,
    ) -> Self {
        Self {
            crashed_node_id: crashed_standby_id.clone(),
            success: true,
            action: RecoveryAction::StandbyRecovery {
                crashed_standby_id,
                affected_primaries,
                spare_standby_id: Some(spare_id.clone()),
            },
            tables_redistributed: vec![],
            new_primary_assignments: vec![],
            spare_assigned: Some(spare_id),
            unprotected_primaries: vec![],
            duration_ms,
            warnings: vec![],
        }
    }

    /// Create a Standby recovery result without spare (degraded state)
    pub fn standby_recovery_degraded(
        crashed_standby_id: String,
        affected_primaries: Vec<String>,
        duration_ms: u64,
    ) -> Self {
        Self {
            crashed_node_id: crashed_standby_id.clone(),
            success: true, // Recovery "succeeded" but cluster is degraded
            action: RecoveryAction::StandbyRecovery {
                crashed_standby_id,
                affected_primaries: affected_primaries.clone(),
                spare_standby_id: None,
            },
            tables_redistributed: vec![],
            new_primary_assignments: vec![],
            spare_assigned: None,
            unprotected_primaries: affected_primaries,
            duration_ms,
            warnings: vec!["No spare Standby available. Primaries left unprotected.".to_string()],
        }
    }

    /// Create a Standby recovery result with flexible assignment
    ///
    /// Some Primaries may be assigned to existing Standbys with capacity,
    /// while others may be left unprotected if no capacity available.
    pub fn standby_recovery_flexible(
        crashed_standby_id: String,
        affected_primaries: Vec<String>,
        assigned_primaries: Vec<String>,
        unprotected_primaries: Vec<String>,
        duration_ms: u64,
    ) -> Self {
        let warnings = if unprotected_primaries.is_empty() {
            vec![]
        } else {
            vec![format!(
                "Flexible assignment: {} Primaries assigned to existing Standbys, {} left unprotected",
                assigned_primaries.len(),
                unprotected_primaries.len()
            )]
        };

        Self {
            crashed_node_id: crashed_standby_id.clone(),
            success: true,
            action: RecoveryAction::StandbyRecovery {
                crashed_standby_id,
                affected_primaries,
                spare_standby_id: None, // No single spare, using flexible assignment
            },
            tables_redistributed: vec![],
            new_primary_assignments: vec![],
            spare_assigned: None,
            unprotected_primaries,
            duration_ms,
            warnings,
        }
    }
}

/// Information about an unprotected Primary (no Standby backup)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnprotectedPrimary {
    pub primary_id: String,
    pub since: i64, // Timestamp when became unprotected
    pub reason: String,
}

/// Cluster protection status (node-level)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterProtectionStatus {
    /// Total number of Primary nodes
    pub total_primaries: usize,

    /// Number of protected Primaries (have Standby backup)
    pub protected_primaries: usize,

    /// Number of unprotected Primaries
    pub unprotected_primaries: usize,

    /// List of unprotected Primaries
    pub unprotected_list: Vec<UnprotectedPrimary>,

    /// Number of spare Standbys available
    pub spare_standbys_available: usize,

    /// Overall cluster health
    pub status: ClusterHealthStatus,
}

/// Overall cluster health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterHealthStatus {
    /// All Primaries protected, spares available
    Healthy,

    /// All Primaries protected, no spares
    HealthyNoSpares,

    /// Some Primaries unprotected
    Degraded,

    /// Many Primaries unprotected (>50%)
    Critical,
}

impl fmt::Display for ClusterHealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterHealthStatus::Healthy => write!(f, "HEALTHY"),
            ClusterHealthStatus::HealthyNoSpares => write!(f, "HEALTHY_NO_SPARES"),
            ClusterHealthStatus::Degraded => write!(f, "DEGRADED"),
            ClusterHealthStatus::Critical => write!(f, "CRITICAL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_result_flexible_all_assigned() {
        // All affected Primaries were successfully assigned to existing Standbys
        let result = RecoveryResult::standby_recovery_flexible(
            "standby-1".to_string(),
            vec!["primary-1".to_string(), "primary-2".to_string()],
            vec!["primary-1".to_string(), "primary-2".to_string()],
            vec![], // No unprotected
            100,
        );

        assert!(result.success);
        assert!(result.unprotected_primaries.is_empty());
        assert!(result.warnings.is_empty()); // No warnings if all assigned
        assert!(result.spare_assigned.is_none()); // Flexible uses existing Standbys
    }

    #[test]
    fn test_recovery_result_flexible_partial() {
        // Some Primaries assigned, some left unprotected
        let result = RecoveryResult::standby_recovery_flexible(
            "standby-1".to_string(),
            vec![
                "primary-1".to_string(),
                "primary-2".to_string(),
                "primary-3".to_string(),
            ],
            vec!["primary-1".to_string(), "primary-2".to_string()],
            vec!["primary-3".to_string()],
            150,
        );

        assert!(result.success);
        assert_eq!(result.unprotected_primaries.len(), 1);
        assert_eq!(result.unprotected_primaries[0], "primary-3");
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("2 Primaries assigned"));
        assert!(result.warnings[0].contains("1 left unprotected"));
    }

    #[test]
    fn test_recovery_result_flexible_none_assigned() {
        // No Standbys had capacity, all left unprotected
        let result = RecoveryResult::standby_recovery_flexible(
            "standby-1".to_string(),
            vec!["primary-1".to_string(), "primary-2".to_string()],
            vec![], // None assigned
            vec!["primary-1".to_string(), "primary-2".to_string()],
            50,
        );

        assert!(result.success); // Still succeeds but degraded
        assert_eq!(result.unprotected_primaries.len(), 2);
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("0 Primaries assigned"));
        assert!(result.warnings[0].contains("2 left unprotected"));
    }

    #[test]
    fn test_recovery_error_display() {
        let err = RecoveryError::NoSpareAvailable {
            affected_primaries: vec!["p1".to_string(), "p2".to_string()],
        };
        assert!(err.to_string().contains("p1"));
        assert!(err.to_string().contains("p2"));

        let err = RecoveryError::FlushTimeout {
            standby_id: "s1".to_string(),
            timeout_secs: 60,
        };
        assert!(err.to_string().contains("s1"));
        assert!(err.to_string().contains("60s"));
    }

    #[test]
    fn test_cluster_protection_status_init() {
        let status = ClusterProtectionStatus {
            total_primaries: 10,
            protected_primaries: 8,
            unprotected_primaries: 2,
            unprotected_list: vec![UnprotectedPrimary {
                primary_id: "primary-1".to_string(),
                since: 1704067200,
                reason: "Standby crashed".to_string(),
            }],
            spare_standbys_available: 0,
            status: ClusterHealthStatus::Degraded,
        };

        assert_eq!(status.protected_primaries, 8);
        assert_eq!(status.unprotected_primaries, 2);
        assert_eq!(status.unprotected_list.len(), 1);
        assert_eq!(format!("{}", status.status), "DEGRADED");
    }
}
