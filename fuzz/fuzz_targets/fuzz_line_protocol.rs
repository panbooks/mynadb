//! Fuzz target for InfluxDB line protocol parser
//!
//! Tests the REAL parse_line_protocol function from src/storage/line_protocol.rs
//! to find edge cases and potential panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

// Import the REAL parser from the mynadb library
use mynadb::config::{SchemaConfig, SchemaTableStrategy};
use mynadb::storage::line_protocol::{parse_line_protocol, parse_multiple_lines};

/// Create a default schema config for testing
fn default_schema_config() -> SchemaConfig {
    SchemaConfig {
        strategy: SchemaTableStrategy::ServiceAsSchema,
        tag_keys: vec!["service".to_string(), "svc".to_string()],
        default_name: "default".to_string(),
        prefix_numeric: true,
    }
}

fuzz_target!(|data: &[u8]| {
    // Try to parse as UTF-8 string
    if let Ok(input) = std::str::from_utf8(data) {
        let config = default_schema_config();

        // Test single line parsing with the REAL function - should not panic
        let _ = parse_line_protocol(input, &config);

        // Test multi-line parsing with the REAL function - should not panic
        let _ = parse_multiple_lines(input, &config);

        // Also test with trimmed input
        let trimmed = input.trim();
        if !trimmed.is_empty() {
            let _ = parse_line_protocol(trimmed, &config);
        }
    }
});
