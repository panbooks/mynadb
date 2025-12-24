use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: Option<i64>,
    pub date: i32,  // Date32 type: days since Unix epoch (1970-01-01)
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
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
    let fields = parse_fields(parts[1])?;

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
        fields,
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

fn parse_fields(part: &str) -> Result<HashMap<String, FieldValue>, String> {
    let mut fields = HashMap::new();

    for field_part in part.split(',') {
        if let Some((key, value)) = field_part.split_once('=') {
            let field_value = parse_field_value(value)?;
            fields.insert(key.to_string(), field_value);
        }
    }

    if fields.is_empty() {
        return Err("No fields found".to_string());
    }

    Ok(fields)
}

fn parse_field_value(value: &str) -> Result<FieldValue, String> {
    // Boolean
    if value == "true" || value == "t" || value == "True" || value == "TRUE" {
        return Ok(FieldValue::Boolean(true));
    }
    if value == "false" || value == "f" || value == "False" || value == "FALSE" {
        return Ok(FieldValue::Boolean(false));
    }

    // String (quoted)
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\'')) {
        let s = &value[1..value.len() - 1];
        return Ok(FieldValue::String(s.to_string()));
    }

    // Integer (ends with 'i')
    if value.ends_with('i') {
        let num_str = &value[..value.len() - 1];
        return num_str
            .parse::<i64>()
            .map(FieldValue::Integer)
            .map_err(|e| format!("Invalid integer: {}", e));
    }

    // Float
    value
        .parse::<f64>()
        .map(FieldValue::Float)
        .map_err(|e| format!("Invalid field value: {}", e))
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
        assert_eq!(point.fields.len(), 1);
    }

    #[test]
    fn test_parse_with_tags() {
        let line = "cpu,host=server01,region=us-west usage=23.5,temp=45i";
        let point = parse_line_protocol(line).unwrap();
        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.tags.get("host"), Some(&"server01".to_string()));
        assert_eq!(point.fields.len(), 2);
    }
}