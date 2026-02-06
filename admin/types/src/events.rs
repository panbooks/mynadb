//! Event types for Admin Console

use serde::{Deserialize, Serialize};

/// Event types from the Meta service
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Unknown,
    NodeJoined,
    NodeLeft,
    NodeStateChanged,
    LeaderChanged,
    CacheInvalidate,
    SchemaChanged,
    FlushForRecovery,
    TableOwnerAdded,
    TableOwnerRemoved,
    TableReassigned,
    DistributionConfigChanged,
    TablesCacheRefresh,
}

impl Default for EventType {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::NodeJoined => write!(f, "Node Joined"),
            Self::NodeLeft => write!(f, "Node Left"),
            Self::NodeStateChanged => write!(f, "Node State Changed"),
            Self::LeaderChanged => write!(f, "Leader Changed"),
            Self::CacheInvalidate => write!(f, "Cache Invalidate"),
            Self::SchemaChanged => write!(f, "Schema Changed"),
            Self::FlushForRecovery => write!(f, "Flush for Recovery"),
            Self::TableOwnerAdded => write!(f, "Table Owner Added"),
            Self::TableOwnerRemoved => write!(f, "Table Owner Removed"),
            Self::TableReassigned => write!(f, "Table Reassigned"),
            Self::DistributionConfigChanged => write!(f, "Distribution Config Changed"),
            Self::TablesCacheRefresh => write!(f, "Tables Cache Refresh"),
        }
    }
}

impl From<i32> for EventType {
    fn from(v: i32) -> Self {
        match v {
            1 => EventType::NodeJoined,
            2 => EventType::NodeLeft,
            3 => EventType::NodeStateChanged,
            6 => EventType::LeaderChanged,
            7 => EventType::CacheInvalidate,
            8 => EventType::SchemaChanged,
            13 => EventType::FlushForRecovery,
            14 => EventType::TableOwnerAdded,
            15 => EventType::TableOwnerRemoved,
            16 => EventType::TableReassigned,
            17 => EventType::DistributionConfigChanged,
            18 => EventType::TablesCacheRefresh,
            _ => EventType::Unknown,
        }
    }
}

/// Event from the Meta service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub sequence: u64,
    pub event_type: EventType,
    pub topic: String,
    pub payload: serde_json::Value,
    pub timestamp: i64,
    pub source_node: String,
}

/// Admin-specific WebSocket event wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdminEvent {
    /// Node joined the cluster
    NodeJoined {
        node_id: String,
        role: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
    },
    /// Node left the cluster
    NodeLeft { node_id: String },
    /// Node state changed
    NodeStateChanged {
        node_id: String,
        old_state: String,
        new_state: String,
    },
    /// Scale-in progress update
    ScaleInProgress {
        plan_id: String,
        phase: String,
        progress: u8,
    },
    /// Scale-in completed
    ScaleInCompleted {
        plan_id: String,
        success: bool,
        nodes_retired: usize,
    },
    /// Table reassigned
    TableReassigned {
        table_key: String,
        old_owner: Option<String>,
        new_owner: String,
    },
    /// Cluster status changed
    ClusterStatusChanged { healthy: bool },
    /// Heartbeat (keep-alive)
    Heartbeat { timestamp: i64 },
    /// Error event
    Error { message: String },
}

/// Query parameters for listing events
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListEventsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_sequence: Option<u64>,
    #[serde(default)]
    pub limit: u32,
}

/// Response for list events endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEventsResponse {
    pub events: Vec<Event>,
    pub total: usize,
    pub last_sequence: u64,
}
