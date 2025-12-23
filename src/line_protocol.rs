use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: Option<i64>,
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

    Ok(Point {
        measurement,
        tags,
        fields,
        timestamp,
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