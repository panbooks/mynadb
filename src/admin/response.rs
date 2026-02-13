//! Shared response types and helpers for admin API handlers.

use serde::Serialize;

/// Standard API response wrapper used by all admin endpoints.
#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

/// Convert Unix timestamp (milliseconds) to RFC3339 string.
pub fn timestamp_to_rfc3339(ts_ms: i64) -> Option<String> {
    if ts_ms <= 0 {
        return None;
    }
    let secs = ts_ms / 1000;
    let nsecs = ((ts_ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.to_rfc3339())
}

/// A column parsed from Rust Debug-format schema output.
pub struct ParsedColumn {
    pub name: String,
    pub data_type: String,
}

/// Parse column info from Rust's Debug format of DFSchema/Arrow Schema.
/// Extracts `Field { name: "col_name", data_type: TypeName, ... }` blocks.
pub fn parse_schema_fields(text: &str) -> Vec<ParsedColumn> {
    let mut columns = Vec::new();
    let mut in_field = false;
    let mut current_name = String::new();
    let mut current_type = String::new();

    for line in text.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("Field {") || trimmed == "Field {" {
            in_field = true;
            current_name.clear();
            current_type.clear();
            continue;
        }

        if in_field {
            if trimmed.starts_with("name:") {
                if let Some(start) = trimmed.find('"') {
                    if let Some(end) = trimmed[start + 1..].find('"') {
                        current_name = trimmed[start + 1..start + 1 + end].to_string();
                    }
                }
            } else if trimmed.starts_with("data_type:") {
                current_type = trimmed
                    .trim_start_matches("data_type:")
                    .trim()
                    .trim_end_matches(',')
                    .to_string();
            } else if trimmed == "}," || trimmed == "}" {
                if !current_name.is_empty() {
                    columns.push(ParsedColumn {
                        name: current_name.clone(),
                        data_type: current_type.clone(),
                    });
                }
                in_field = false;
            }
        }
    }

    columns
}
