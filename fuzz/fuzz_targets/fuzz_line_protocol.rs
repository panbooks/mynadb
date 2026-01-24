//! Fuzz target for InfluxDB line protocol parser
//!
//! Tests that the parser handles arbitrary input without panicking.
//! The parser should gracefully return errors for invalid input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;

/// Field value types supported by InfluxDB line protocol
#[derive(Debug, Clone)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

/// A parsed point from line protocol
#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: Option<i64>,
}

/// Parse a single field value
fn parse_field_value(value: &str) -> Result<FieldValue, &'static str> {
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
        if value.len() >= 2 {
            let s = &value[1..value.len() - 1];
            return Ok(FieldValue::String(s.to_string()));
        }
    }

    // Integer: ends with 'i' (e.g., 42i)
    if value.ends_with('i') && value.len() > 1 {
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

/// Parse field set from line protocol
fn parse_fields(field_str: &str) -> Result<HashMap<String, FieldValue>, &'static str> {
    let parts: Vec<&str> = field_str.split(',').collect();
    let mut fields = HashMap::with_capacity(parts.len());

    for part in parts {
        if let Some((key, value)) = part.split_once('=') {
            if !key.is_empty() {
                let field_value = parse_field_value(value)?;
                fields.insert(key.to_string(), field_value);
            }
        }
    }

    if fields.is_empty() {
        return Err("No fields found");
    }

    Ok(fields)
}

/// Parse measurement and tags from the first part of line protocol
fn parse_measurement_and_tags(part: &str) -> Result<(String, HashMap<String, String>), &'static str> {
    let parts: Vec<&str> = part.split(',').collect();

    if parts.is_empty() || parts[0].is_empty() {
        return Err("No measurement found");
    }

    let measurement = parts[0].to_string();
    let mut tags = HashMap::with_capacity(parts.len().saturating_sub(1));

    for tag_part in &parts[1..] {
        if let Some((key, value)) = tag_part.split_once('=') {
            if !key.is_empty() {
                tags.insert(key.to_string(), value.to_string());
            }
        }
    }

    Ok((measurement, tags))
}

/// Parse InfluxDB line protocol
/// Format: measurement[,tag=value...] field=value[,field=value...] [timestamp]
fn parse_line_protocol(line: &str) -> Result<Point, &'static str> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err("Empty or comment line");
    }

    // Split by space: measurement+tags fields timestamp
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("Invalid line protocol format");
    }

    // Parse measurement and tags
    let (measurement, tags) = parse_measurement_and_tags(parts[0])?;

    // Parse fields
    let fields = parse_fields(parts[1])?;

    // Parse timestamp (optional)
    let timestamp = if parts.len() > 2 {
        parts[2].parse::<i64>().ok()
    } else {
        None
    };

    Ok(Point {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

/// Parse multiple lines of line protocol
fn parse_multiple_lines(data: &str) -> Vec<Point> {
    data.lines()
        .filter_map(|line| parse_line_protocol(line).ok())
        .collect()
}

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string
    if let Ok(input) = std::str::from_utf8(data) {
        // Test single line parsing - should not panic
        let _ = parse_line_protocol(input);

        // Test multi-line parsing - should not panic
        let _ = parse_multiple_lines(input);
    }
});
