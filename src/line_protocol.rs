use crate::config::SchemaConfig;
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use std::collections::HashMap;

/// Minimum lines to trigger parallel parsing (below this, sequential is faster)
const PARALLEL_THRESHOLD: usize = 1000;

/// Field value types supported by InfluxDB line protocol
#[derive(Debug, Clone)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
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
    let fields = parse_fields(parts[1])?;

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
        fields,
        timestamp,
        date,
        service,
    })
}

/// Parse field set from line protocol
/// Format: field=value[,field=value...]
/// Values can be: float (1.5), integer (1i), string ("hello"), boolean (true/false)
fn parse_fields(field_str: &str) -> Result<HashMap<String, FieldValue>, String> {
    let parts: Vec<&str> = field_str.split(',').collect();
    let mut fields = HashMap::with_capacity(parts.len());

    for part in parts {
        if let Some((key, value)) = part.split_once('=') {
            let field_value = parse_field_value(value)?;
            fields.insert(key.to_string(), field_value);
        }
    }

    if fields.is_empty() {
        return Err("No fields found".to_string());
    }

    Ok(fields)
}

/// Parse a single field value
fn parse_field_value(value: &str) -> Result<FieldValue, String> {
    let value = value.trim();

    // Boolean: true, false, t, f, TRUE, FALSE, T, F
    match value.to_lowercase().as_str() {
        "true" | "t" => return Ok(FieldValue::Boolean(true)),
        "false" | "f" => return Ok(FieldValue::Boolean(false)),
        _ => {}
    }

    // String: "..." or '...'
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        let s = &value[1..value.len() - 1];
        return Ok(FieldValue::String(s.to_string()));
    }

    // Integer: ends with 'i' (e.g., 42i)
    if value.ends_with('i') {
        let num_str = &value[..value.len() - 1];
        if let Ok(n) = num_str.parse::<i64>() {
            return Ok(FieldValue::Integer(n));
        }
    }

    // Float: default numeric type
    if let Ok(f) = value.parse::<f64>() {
        return Ok(FieldValue::Float(f));
    }

    // Fallback: treat as string
    Ok(FieldValue::String(value.to_string()))
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
    let parts: Vec<&str> = part.split(',').collect();

    if parts.is_empty() {
        return Err("No measurement found".to_string());
    }

    let measurement = parts[0].to_string();

    // Pre-size HashMap based on tag count
    let tag_count = parts.len().saturating_sub(1);
    let mut tags = HashMap::with_capacity(tag_count);

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

/// Parse multiple lines sequentially (for small batches)
fn parse_multiple_lines_sequential(data: &str, schema_config: &SchemaConfig) -> Vec<Point> {
    let estimated_lines = (data.len() / 500).max(16);
    let mut points = Vec::with_capacity(estimated_lines);

    for line in data.lines() {
        if let Ok(point) = parse_line_protocol(line, schema_config) {
            points.push(point);
        }
    }

    points
}

/// Parse multiple lines in parallel using rayon (for large batches)
fn parse_multiple_lines_parallel(data: &str, schema_config: &SchemaConfig) -> Vec<Point> {
    let lines: Vec<&str> = data.lines().collect();

    // Process lines in parallel chunks for better cache locality
    lines
        .par_chunks(1000)
        .flat_map(|chunk| {
            chunk
                .iter()
                .filter_map(|line| parse_line_protocol(line, schema_config).ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Parse multiple lines with automatic parallel/sequential selection
/// Uses parallel parsing for batches > PARALLEL_THRESHOLD lines
pub fn parse_multiple_lines(data: &str, schema_config: &SchemaConfig) -> Vec<Point> {
    // Estimate line count from data size (assume ~100 bytes per line average)
    let estimated_lines = data.len() / 100;

    if estimated_lines >= PARALLEL_THRESHOLD {
        parse_multiple_lines_parallel(data, schema_config)
    } else {
        parse_multiple_lines_sequential(data, schema_config)
    }
}

/// Force parallel parsing regardless of batch size (for benchmarking)
pub fn parse_multiple_lines_force_parallel(data: &str, schema_config: &SchemaConfig) -> Vec<Point> {
    parse_multiple_lines_parallel(data, schema_config)
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

    #[test]
    fn test_parse_field_float() {
        let line = "cpu usage=23.5 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert!(matches!(point.fields.get("usage"), Some(FieldValue::Float(f)) if (*f - 23.5).abs() < 0.001));
    }

    #[test]
    fn test_parse_field_integer() {
        let line = "cpu temp=45i 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert!(matches!(point.fields.get("temp"), Some(FieldValue::Integer(45))));
    }

    #[test]
    fn test_parse_field_boolean() {
        let line = "system active=true,enabled=false 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert!(matches!(point.fields.get("active"), Some(FieldValue::Boolean(true))));
        assert!(matches!(point.fields.get("enabled"), Some(FieldValue::Boolean(false))));
    }

    #[test]
    fn test_parse_field_string() {
        // Note: strings with spaces would break whitespace-based parsing
        // Using string without spaces for this test
        let line = "log message=\"hello\" 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert!(matches!(point.fields.get("message"), Some(FieldValue::String(s)) if s == "hello"));
    }

    #[test]
    fn test_parse_multiple_fields() {
        let line = "cpu,host=server01 usage=23.5,temp=45i,active=true,name=\"cpu0\" 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();

        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.tags.get("host"), Some(&"server01".to_string()));
        assert_eq!(point.fields.len(), 4);
        assert!(matches!(point.fields.get("usage"), Some(FieldValue::Float(_))));
        assert!(matches!(point.fields.get("temp"), Some(FieldValue::Integer(45))));
        assert!(matches!(point.fields.get("active"), Some(FieldValue::Boolean(true))));
        assert!(matches!(point.fields.get("name"), Some(FieldValue::String(s)) if s == "cpu0"));
    }

    #[test]
    fn test_parse_field_boolean_variants() {
        // Test various boolean representations
        let cases = vec![
            ("m f1=true", true),
            ("m f1=TRUE", true),
            ("m f1=t", true),
            ("m f1=T", true),
            ("m f1=false", false),
            ("m f1=FALSE", false),
            ("m f1=f", false),
            ("m f1=F", false),
        ];

        let config = get_test_schema_config();
        for (line, expected) in cases {
            let point = parse_line_protocol(line, &config).unwrap();
            assert!(
                matches!(point.fields.get("f1"), Some(FieldValue::Boolean(b)) if *b == expected),
                "Failed for line: {}",
                line
            );
        }
    }

    #[test]
    fn test_parse_field_scientific_notation() {
        let line = "cpu value=1.5e10 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert!(matches!(point.fields.get("value"), Some(FieldValue::Float(f)) if (*f - 1.5e10).abs() < 1e5));
    }

    #[test]
    fn test_parse_field_negative_values() {
        let line = "temp value=-10.5,count=-100i 1672531200000000000";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert!(matches!(point.fields.get("value"), Some(FieldValue::Float(f)) if (*f - (-10.5)).abs() < 0.001));
        assert!(matches!(point.fields.get("count"), Some(FieldValue::Integer(-100))));
    }

    #[test]
    fn test_parse_no_timestamp() {
        let line = "cpu,host=server01 usage=50.0";
        let config = get_test_schema_config();
        let point = parse_line_protocol(line, &config).unwrap();
        assert_eq!(point.measurement, "cpu");
        assert!(point.timestamp.is_none());
        assert!(matches!(point.fields.get("usage"), Some(FieldValue::Float(_))));
    }

    #[test]
    fn test_parse_multiple_lines_fn() {
        let data = "cpu,host=a usage=10.0 1000000000\ncpu,host=b usage=20.0 2000000000\n# comment\n\nmem,host=a free=1024i 3000000000";
        let config = get_test_schema_config();
        let points = parse_multiple_lines(data, &config);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].measurement, "cpu");
        assert_eq!(points[1].measurement, "cpu");
        assert_eq!(points[2].measurement, "mem");
    }

    #[test]
    fn test_parse_multiple_lines_parallel() {
        // Generate a large batch to trigger parallel parsing
        let config = get_test_schema_config();
        let mut lines = Vec::with_capacity(2000);
        for i in 0..2000 {
            let ts: i64 = 1700000000000000000 + (i as i64 * 1000000);
            lines.push(format!("cpu,host=server{} usage={}.0 {}", i % 100, i, ts));
        }
        let data = lines.join("\n");

        let points = parse_multiple_lines(&data, &config);
        assert_eq!(points.len(), 2000);

        // Verify some points
        assert_eq!(points[0].measurement, "cpu");
        assert!(points.iter().all(|p| p.measurement == "cpu"));
    }

    #[test]
    fn test_parse_force_parallel() {
        let data = "cpu,host=a usage=10.0 1000000000\ncpu,host=b usage=20.0 2000000000";
        let config = get_test_schema_config();
        let points = parse_multiple_lines_force_parallel(data, &config);
        assert_eq!(points.len(), 2);
    }
}
