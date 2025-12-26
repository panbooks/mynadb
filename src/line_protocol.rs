use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub timestamp: Option<i64>,
    pub date: i32,  // Date32 type: days since Unix epoch (1970-01-01)
}

/// Parse InfluxDB line protocol
/// Format: measurement[,tag=value...] field=value[,field=value...] [timestamp]
/// Example: cpu,host=server01,region=us-west usage=23.5,temp=45i 1672531200000000000
pub fn parse_line_protocol(line: &str) -> Result<Point, String> {
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

    Ok(Point {
        measurement,
        tags,
        timestamp,
        date,
    })
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
            .unwrap_or_else(|| Utc::now())
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

pub fn parse_multiple_lines(data: &str) -> Vec<Point> {
    data.lines()
        .filter_map(|line| parse_line_protocol(line).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let line = "cpu usage=23.5 1672531200000000000";
        let point = parse_line_protocol(line).unwrap();
        assert_eq!(point.measurement, "cpu");
    }

    #[test]
    fn test_parse_with_tags() {
        let line = "cpu,host=server01,region=us-west usage=23.5,temp=45i";
        let point = parse_line_protocol(line).unwrap();
        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.tags.get("host"), Some(&"server01".to_string()));
    }
}