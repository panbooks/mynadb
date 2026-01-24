//! Fuzz target for field value parser
//!
//! Tests that the field value parser handles arbitrary input without panicking.
//! Field values can be: float (1.5), integer (1i), string ("hello"), boolean (true/false)

#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

/// Field value types supported by InfluxDB line protocol
#[derive(Debug, Clone)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

/// Parse a single field value from InfluxDB line protocol
///
/// Supported formats:
/// - Boolean: true, false, t, f, TRUE, FALSE, T, F
/// - String: "..." or '...'
/// - Integer: ends with 'i' (e.g., 42i, -100i)
/// - Float: any valid floating point number (e.g., 1.5, -3.14, 1e10)
fn parse_field_value(value: &str) -> Result<FieldValue, &'static str> {
    let value = value.trim();

    if value.is_empty() {
        return Err("Empty value");
    }

    // Boolean: true, false, t, f, TRUE, FALSE, T, F
    match value.to_lowercase().as_str() {
        "true" | "t" => return Ok(FieldValue::Boolean(true)),
        "false" | "f" => return Ok(FieldValue::Boolean(false)),
        _ => {}
    }

    // String: "..." or '...'
    if value.len() >= 2 {
        if (value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\''))
        {
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
        // If parsing failed, fall through to try as float or string
    }

    // Float: default numeric type
    if let Ok(f) = value.parse::<f64>() {
        // Check for special float values
        if f.is_nan() || f.is_infinite() {
            return Ok(FieldValue::Float(f));
        }
        return Ok(FieldValue::Float(f));
    }

    // Fallback: treat as unquoted string
    Ok(FieldValue::String(value.to_string()))
}

/// Parse a key=value pair
fn parse_field_pair(pair: &str) -> Result<(String, FieldValue), &'static str> {
    let (key, value) = pair.split_once('=').ok_or("No equals sign")?;

    if key.is_empty() {
        return Err("Empty key");
    }

    let field_value = parse_field_value(value)?;
    Ok((key.to_string(), field_value))
}

/// Structured input for more targeted fuzzing
#[derive(Debug, Arbitrary)]
struct FieldInput {
    /// The value part of a field
    value: String,
    /// Whether to add integer suffix 'i'
    add_i_suffix: bool,
    /// Whether to quote the string
    quote_style: u8, // 0=none, 1=double, 2=single
}

impl FieldInput {
    fn to_string(&self) -> String {
        let mut result = self.value.clone();

        if self.add_i_suffix {
            result.push('i');
        }

        match self.quote_style % 3 {
            1 => format!("\"{}\"", result),
            2 => format!("'{}'", result),
            _ => result,
        }
    }
}

fuzz_target!(|data: &[u8]| {
    // Test 1: Raw byte input as string
    if let Ok(input) = std::str::from_utf8(data) {
        // Test field value parsing - should not panic
        let _ = parse_field_value(input);

        // Test with trimmed input
        let _ = parse_field_value(input.trim());

        // Test field pair parsing
        let _ = parse_field_pair(input);
    }

    // Test 2: Structured input for more targeted fuzzing
    if let Ok(structured) = FieldInput::arbitrary(&mut arbitrary::Unstructured::new(data)) {
        let value_str = structured.to_string();
        let _ = parse_field_value(&value_str);
    }
});
