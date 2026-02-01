//! Recovery Types
//!
//! Types and errors for node failure recovery operations.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Recovery action to perform when a node fails
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryAction {
    /// Primary crashed: Standby flushes data, partitions redistributed
    PrimaryRecovery {
        crashed_primary_id: String,
        standby_id: String,
        partitions_to_redistribute: Vec<String>,
    },

    /// Standby crashed: Assign spare or mark Primary as unprotected
    StandbyRecovery {
        crashed_standby_id: String,
        affected_primaries: Vec<String>,
        spare_standby_id: Option<String>,
    },

    /// No recovery needed (node had no partitions or was spare)
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

    /// Redistributing partitions to remaining Primaries
    RedistributingPartitions,

    /// Assigning spare Standby
    AssigningSpare,

    /// Updating replica assignments
    UpdatingReplicas,

    /// Marking partitions as unprotected
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
            RecoveryStep::RedistributingPartitions => write!(f, "Redistributing partitions"),
            RecoveryStep::AssigningSpare => write!(f, "Assigning spare Standby"),
            RecoveryStep::UpdatingReplicas => write!(f, "Updating replica assignments"),
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

    /// Partition redistribution failed
    RedistributionFailed { partition: String, reason: String },

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
                write!(f, "No remaining Primaries to redistribute partitions to")
            }
            RecoveryError::RedistributionFailed { partition, reason } => {
                write!(f, "Failed to redistribute partition {}: {}", partition, reason)
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

    /// Partitions that were redistributed (for Primary crash)
    pub partitions_redistributed: Vec<String>,

    /// New Primary assignments (partition_key -> new_primary_id)
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
        partitions: Vec<String>,
        new_assignments: Vec<(String, String)>,
        duration_ms: u64,
    ) -> Self {
        Self {
            crashed_node_id: crashed_primary_id.clone(),
            success: true,
            action: RecoveryAction::PrimaryRecovery {
                crashed_primary_id,
                standby_id,
                partitions_to_redistribute: partitions.clone(),
            },
            partitions_redistributed: partitions,
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
            partitions_redistributed: vec![],
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
            partitions_redistributed: vec![],
            new_primary_assignments: vec![],
            spare_assigned: None,
            unprotected_primaries: affected_primaries,
            duration_ms,
            warnings: vec!["No spare Standby available. Primaries left unprotected.".to_string()],
        }
    }
}

/// Information about an unprotected partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnprotectedPartition {
    pub partition_key: String,
    pub primary_id: String,
    pub since: i64, // Timestamp when became unprotected
    pub reason: String,
}

/// Cluster protection status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterProtectionStatus {
    /// Total number of partitions
    pub total_partitions: usize,

    /// Number of protected partitions (have Standby backup)
    pub protected_partitions: usize,

    /// Number of unprotected partitions
    pub unprotected_partitions: usize,

    /// List of unprotected partitions
    pub unprotected_list: Vec<UnprotectedPartition>,

    /// Number of spare Standbys available
    pub spare_standbys_available: usize,

    /// Overall cluster health
    pub status: ClusterHealthStatus,
}

/// Overall cluster health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterHealthStatus {
    /// All partitions protected, spares available
    Healthy,

    /// All partitions protected, no spares
    HealthyNoSpares,

    /// Some partitions unprotected
    Degraded,

    /// Many partitions unprotected (>50%)
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
