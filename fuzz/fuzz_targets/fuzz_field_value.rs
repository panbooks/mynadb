//! Fuzz target for field value parser
//!
//! Tests the REAL parse_field_value and parse_fields functions from
//! src/storage/line_protocol.rs to find edge cases and potential panics.

#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

// Import the REAL parsers from the mynadb library
use mynadb::storage::line_protocol::{parse_field_value, parse_fields};

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
    fn to_field_string(&self) -> String {
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
    // Try to parse as UTF-8 string
    if let Ok(input) = std::str::from_utf8(data) {
        // Test field value parsing with the REAL function - should not panic
        let _ = parse_field_value(input);

        // Test with trimmed input
        let trimmed = input.trim();
        if !trimmed.is_empty() {
            let _ = parse_field_value(trimmed);
        }

        // Test field set parsing (comma-separated key=value pairs)
        let _ = parse_fields(input);

        // Test with lowercase
        let lower = input.to_lowercase();
        let _ = parse_field_value(&lower);
    }

    // Test structured input for more targeted fuzzing
    if let Ok(structured) = FieldInput::arbitrary(&mut arbitrary::Unstructured::new(data)) {
        let value_str = structured.to_field_string();
        let _ = parse_field_value(&value_str);

        // Also test as a field set: key=value
        let field_set = format!("field={}", value_str);
        let _ = parse_fields(&field_set);
    }
});
