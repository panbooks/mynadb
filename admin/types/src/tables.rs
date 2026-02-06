//! Table types for Admin Console

use serde::{Deserialize, Serialize};

/// Table metadata for routing and statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub owner_node_ids: Vec<String>,
    pub partition_count: u32,
    pub total_bytes: u64,
    pub total_rows: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_partition: Option<String>,
    pub min_time: i64,
    pub max_time: i64,
    pub created_at: i64,
    pub last_modified: i64,
    pub version: u64,
}

impl TableMeta {
    /// Get the primary owner node (first in the list)
    pub fn primary_owner(&self) -> Option<&str> {
        self.owner_node_ids.first().map(|s| s.as_str())
    }

    /// Get the table key in format "catalog/schema/table"
    pub fn table_key(&self) -> String {
        format!("{}/{}/{}", self.catalog, self.schema, self.table)
    }

    /// Check if table has multiple owners
    pub fn is_multi_owner(&self) -> bool {
        self.owner_node_ids.len() > 1
    }

    /// Format bytes as human-readable string
    pub fn formatted_size(&self) -> String {
        format_bytes(self.total_bytes)
    }
}

/// Summary of table for list views
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub primary_owner: Option<String>,
    pub owner_count: u32,
    pub partition_count: u32,
    pub total_bytes: u64,
    pub total_rows: u64,
}

/// Query parameters for listing tables
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListTablesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
    #[serde(default)]
    pub offset: u32,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

/// Response for list tables endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableSummary>,
    pub total: usize,
    pub offset: u32,
    pub limit: u32,
}

/// Request to reassign table ownership
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReassignTableRequest {
    pub new_owner_id: String,
    #[serde(default)]
    pub force: bool,
}

/// Response for table reassignment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReassignTableResponse {
    pub table_key: String,
    pub old_owner: Option<String>,
    pub new_owner: String,
}

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
