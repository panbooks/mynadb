use crate::config::SchemaConfig;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub timestamp: Option<i64>,
    pub date: i32, // Date32 type: days since Unix epoch (1970-01-01)
    pub service: String, // Extracted schema/service name
}

/// Parse InfluxDB line protocol with schema extraction
/// Format: measurement[,tag=value...] field=value[,field=value...] [timestamp]
/// Example: cpu,host=server01,region=us-west usage=23.5,temp=45i 1672531200000000000
pub fn parse_line_protocol(line: &str, schema_config: &SchemaConfig) -> Result<Point, String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err("Empty or comment line".to_string());
    }

    // Split by space: measurement+tags fields timestamp
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("Invalid line protocol format".to_string());
    }

    // Parse measurement and tags
    let measurement_part = parts[0];
    let (measurement, tags) = parse_measurement_and_tags(measurement_part)?;

    // Parse fields
    // let fields = parse_fields(parts[1])?;

    // Parse timestamp (optional)
    let timestamp = if parts.len() > 2 {
        parts[2].parse::<i64>().ok()
    } else {
        None
    };

    // Compute date from timestamp (Date32: days since Unix epoch)
    let date = compute_date_from_timestamp(timestamp);

    // Extract schema (service name) from tags
    let service = extract_service_from_tags(&tags, schema_config);

    Ok(Point {
        measurement,
        tags,
        timestamp,
        date,
        service,
    })
}

/// Extract schema name from tags using schema configuration
/// Iterates through tag_keys in order, returning the first match
fn extract_service_from_tags(tags: &HashMap<String, String>, schema_config: &SchemaConfig) -> String {
    for tag_key in &schema_config.tag_keys {
        if let Some(value) = tags.get(tag_key) {
            // Prefix numeric values if configured
            return if schema_config.prefix_numeric && value.chars().all(|c| c.is_numeric()) {
                format!("service_{}", value)
            } else {
                value.clone()
            };
        }
    }
    schema_config.default_name.clone()
}

fn parse_measurement_and_tags(part: &str) -> Result<(String, HashMap<String, String>), String> {
    let mut tags = HashMap::new();
    let parts: Vec<&str> = part.split(',').collect();

    if parts.is_empty() {
        return Err("No measurement found".to_string());
    }

    let measurement = parts[0].to_string();

    // Parse tags
    for tag_part in &parts[1..] {
        if let Some((key, value)) = tag_part.split_once('=') {
            tags.insert(key.to_string(), value.to_string());
        }
    }

    Ok((measurement, tags))
}

/// Convert timestamp (nanoseconds) to Date32 (days since Unix epoch)
fn compute_date_from_timestamp(timestamp: Option<i64>) -> i32 {
    let dt = if let Some(ts_nanos) = timestamp {
        DateTime::from_timestamp(ts_nanos / 1_000_000_000, (ts_nanos % 1_000_000_000) as u32)
            .unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    };

    // Calculate days since Unix epoch (1970-01-01)
    let epoch = DateTime::from_timestamp(0, 0).unwrap();
    let duration = dt.signed_duration_since(epoch);
    duration.num_days() as i32
}

/// Format Date32 value as YYYY-MM-DD string for directory names
pub fn format_date32_as_string(date32: i32) -> String {
    let epoch = DateTime::from_timestamp(0, 0).unwrap();
    let date = epoch + chrono::Duration::days(date32 as i64);
    date.format("%Y-%m-%d").to_string()
}

/// Format Date32 value as Hive partition format: date=YYYY-MM-DD
pub fn format_date32_as_partition(date32: i32) -> String {
    format!("date={}", format_date32_as_string(date32))
}

pub fn parse_multiple_lines(data: &str, schema_config: &SchemaConfig) -> Vec<Point> {
    data.lines()
        .filter_map(|line| parse_line_protocol(line, schema_config).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::config::SchemaTableStrategy;
    use super::*;

    fn get_test_schema_config() -> SchemaConfig {
        SchemaConfig {
            strategy: SchemaTableStrategy::ServiceAsSchema,
            tag_keys: vec!["service".to_string()],
            default_name: "default".to_string(),
            prefix_numeric: true,
        }
    }

    #[test]
    fn test_parse_simple() {
        let line = "cpu usage=23.5 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.service, "default"); // No service tag -> default
    }

    #[test]
    fn test_parse_with_tags() {
        let line = "cpu,host=server01,region=us-west usage=23.5,temp=45i";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.tags.get("host"), Some(&"server01".to_string()));
        assert_eq!(point.service, "default"); // No service tag -> default
    }

    #[test]
    fn test_parse_with_service_numeric() {
        let line = "cpu,service=1,host=server01 usage=23.5";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert_eq!(point.service, "service_1"); // Numeric service -> service_1
    }

    #[test]
    fn test_parse_with_service_text() {
        let line = "cpu,service=production,host=server01 usage=23.5";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert_eq!(point.service, "production"); // Text service -> as-is
    }
}
