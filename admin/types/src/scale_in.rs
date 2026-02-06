//! Scale-In types for Admin Console

use serde::{Deserialize, Serialize};

/// Strategy for selecting which nodes to retire
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScaleInStrategy {
    /// Retire least-loaded nodes first (fewest tables)
    #[default]
    LeastLoaded,
    /// Retire oldest nodes first (FIFO)
    FirstIn,
    /// Retire most-recently-added nodes first (LIFO)
    LastIn,
    /// Retire by HA units (maintains Primary:Standby ratio)
    HaUnit,
}

impl std::fmt::Display for ScaleInStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeastLoaded => write!(f, "Least Loaded"),
            Self::FirstIn => write!(f, "First In (FIFO)"),
            Self::LastIn => write!(f, "Last In (LIFO)"),
            Self::HaUnit => write!(f, "HA Unit"),
        }
    }
}

/// Request to generate a scale-in plan
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScaleInPlanRequest {
    /// Total number of nodes to remove
    #[serde(default)]
    pub reduce_num: usize,
    /// Number of Primary nodes to remove (takes priority)
    #[serde(default)]
    pub reduce_primaries: usize,
    /// Number of Standby nodes to remove (takes priority)
    #[serde(default)]
    pub reduce_standbys: usize,
    /// Strategy for node selection
    #[serde(default)]
    pub strategy: ScaleInStrategy,
    /// Whether this is a dry-run (no actual changes)
    #[serde(default)]
    pub dry_run: bool,
    /// Force flush memtables before retirement
    #[serde(default = "default_true")]
    pub force_flush: bool,
}

fn default_true() -> bool {
    true
}

/// A complete scale-in plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInPlan {
    pub plan_id: String,
    pub created_at: i64,
    pub dry_run: bool,
    pub phases: Vec<ScaleInPhase>,
    pub primaries_to_retire: Vec<String>,
    pub standbys_to_retire: Vec<String>,
    pub migrations: Vec<TableMigration>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub final_state: ClusterStatePreview,
}

/// A phase in the scale-in process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInPhase {
    pub phase_number: u8,
    pub phase_type: ScaleInPhaseType,
    pub description: String,
    pub affected_nodes: Vec<String>,
    pub operations: Vec<ScaleInOperation>,
}

/// Type of scale-in phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScaleInPhaseType {
    MigrateTables,
    RetirePrimaries,
    RetireStandbys,
    Completed,
}

impl std::fmt::Display for ScaleInPhaseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MigrateTables => write!(f, "Migrate Tables"),
            Self::RetirePrimaries => write!(f, "Retire Primaries"),
            Self::RetireStandbys => write!(f, "Retire Standbys"),
            Self::Completed => write!(f, "Completed"),
        }
    }
}

/// An operation within a scale-in phase
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScaleInOperation {
    MigrateTable {
        table_key: String,
        from_node: String,
        to_node: String,
    },
    FlushMemtable {
        node_id: String,
    },
    StartRetire {
        node_id: String,
    },
    CompleteRetire {
        node_id: String,
    },
    RemoveStandbyFromTables {
        standby_node_id: String,
    },
    WaitForDrain {
        node_id: String,
        timeout_secs: u64,
    },
}

/// A table migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMigration {
    pub table_key: String,
    pub from_node: String,
    pub to_node: String,
    pub reason: String,
}

/// Preview of cluster state after scale-in
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatePreview {
    pub primary_count: usize,
    pub standby_count: usize,
    pub ratio_maintained: bool,
    pub total_tables: usize,
}

/// Result of a scale-in execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInResult {
    pub plan_id: String,
    pub success: bool,
    pub primaries_retired: Vec<String>,
    pub standbys_retired: Vec<String>,
    pub tables_migrated: usize,
    pub duration_ms: u64,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Status of an ongoing scale-in operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInStatus {
    pub plan_id: String,
    pub phase: ScaleInPhaseType,
    pub progress_percent: u8,
    pub nodes_remaining: usize,
    pub estimated_completion_secs: u64,
    #[serde(default)]
    pub errors: Vec<String>,
}

/// Recommendation for scale-in
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleInRecommendation {
    pub recommended_nodes: Vec<RecommendedNode>,
    pub reasons: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// A recommended node for scale-in
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendedNode {
    pub node_id: String,
    pub role: String,
    pub mode: Option<String>,
    pub tables_owned: usize,
    pub reason: String,
}

/// Query parameters for scale-in recommendation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScaleInRecommendQuery {
    #[serde(default)]
    pub primaries: usize,
    #[serde(default)]
    pub standbys: usize,
}
