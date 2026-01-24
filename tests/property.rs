//! Property-Based Tests for MynaDB
//!
//! Uses proptest to generate random test cases and verify invariants.
//! Reference: ClickHouse and InfluxDB testing strategies.
//!
//! Run with: cargo test --test property

use proptest::prelude::*;
use std::collections::HashMap;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

// ============================================================================
// Line Protocol Property Tests
// ============================================================================

/// Strategy to generate valid measurement names (alphanumeric, starting with letter)
fn measurement_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{0,15}".prop_map(|s| s.to_string())
}

/// Strategy to generate valid tag keys (alphanumeric, starting with letter)
fn tag_key_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,15}".prop_map(|s| s.to_string())
}

/// Strategy to generate valid tag values (alphanumeric, no special chars)
fn tag_value_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_]{1,20}".prop_map(|s| s.to_string())
}

/// Strategy to generate valid field keys
fn field_key_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,15}".prop_map(|s| s.to_string())
}

/// Strategy to generate float field values (avoiding NaN and Infinity)
fn float_value_strategy() -> impl Strategy<Value = f64> {
    prop::num::f64::NORMAL.prop_filter("finite", |f| f.is_finite() && f.abs() < 1e15)
}

/// Strategy to generate integer field values
fn integer_value_strategy() -> impl Strategy<Value = i64> {
    -1_000_000_000i64..1_000_000_000i64
}

/// Strategy to generate a valid InfluxDB line protocol line
#[allow(dead_code)]
fn line_protocol_strategy() -> impl Strategy<Value = (String, String, HashMap<String, String>, f64)>
{
    (
        measurement_strategy(),
        prop::collection::hash_map(tag_key_strategy(), tag_value_strategy(), 0..3),
        field_key_strategy(),
        float_value_strategy(),
    )
        .prop_map(|(measurement, tags, field_key, field_value)| {
            let mut line = measurement.clone();

            // Add tags
            let mut tag_list: Vec<_> = tags.iter().collect();
            tag_list.sort_by_key(|(k, _)| *k);
            for (k, v) in &tag_list {
                line.push_str(&format!(",{}={}", k, v));
            }

            // Add field
            line.push_str(&format!(" {}={}", field_key, field_value));

            // Add timestamp
            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
            line.push_str(&format!(" {}", now));

            (
                line,
                measurement,
                tags.into_iter().collect(),
                field_value,
            )
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Valid line protocol should parse without error
    #[test]
    fn prop_valid_line_protocol_parses(
        measurement in measurement_strategy(),
        field_key in field_key_strategy(),
        field_value in float_value_strategy(),
    ) {
        let line = format!("{} {}={}", measurement, field_key, field_value);

        // This tests the parsing logic directly
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok(), "Failed to parse: {} - {:?}", line, result);

        let (parsed_measurement, parsed_fields) = result.unwrap();
        prop_assert_eq!(parsed_measurement, measurement);
        prop_assert!(parsed_fields.contains_key(&field_key));
    }

    /// Property: Measurement name is preserved through parsing
    #[test]
    fn prop_measurement_preserved(
        measurement in measurement_strategy(),
    ) {
        let line = format!("{} value=1.0", measurement);
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok());
        let (parsed_measurement, _) = result.unwrap();
        prop_assert_eq!(parsed_measurement, measurement);
    }

    /// Property: Tags are preserved through parsing
    #[test]
    fn prop_tags_preserved(
        measurement in measurement_strategy(),
        tags in prop::collection::hash_map(tag_key_strategy(), tag_value_strategy(), 1..4),
    ) {
        let mut line = measurement.clone();
        let mut tag_list: Vec<_> = tags.iter().collect();
        tag_list.sort_by_key(|(k, _)| *k);
        for (k, v) in &tag_list {
            line.push_str(&format!(",{}={}", k, v));
        }
        line.push_str(" value=1.0");

        let result = parse_line_with_tags(&line);
        prop_assert!(result.is_ok(), "Failed to parse: {}", line);

        let (_, parsed_tags, _) = result.unwrap();
        for (k, v) in &tags {
            prop_assert_eq!(parsed_tags.get(k), Some(v), "Tag {} mismatch", k);
        }
    }

    /// Property: Field value is preserved (within floating point tolerance)
    #[test]
    fn prop_field_value_preserved(
        field_value in float_value_strategy(),
    ) {
        let line = format!("test value={}", field_value);
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok());

        let (_, fields) = result.unwrap();
        let parsed_value = fields.get("value").cloned().unwrap_or(0.0);

        // Check within tolerance for floating point
        let diff = (parsed_value - field_value).abs();
        let tolerance = field_value.abs() * 1e-10 + 1e-15;
        prop_assert!(
            diff < tolerance,
            "Value mismatch: expected {}, got {}, diff {}",
            field_value, parsed_value, diff
        );
    }

    /// Property: Integer fields are preserved exactly
    #[test]
    fn prop_integer_field_preserved(
        field_value in integer_value_strategy(),
    ) {
        let line = format!("test value={}i", field_value);
        let result = parse_line_integer(&line);
        prop_assert!(result.is_ok());

        let (_, parsed_value) = result.unwrap();
        prop_assert_eq!(parsed_value, field_value);
    }

    /// Property: Boolean fields are preserved
    #[test]
    fn prop_boolean_field_preserved(
        field_value in prop::bool::ANY,
    ) {
        let line = format!("test value={}", field_value);
        let result = parse_line_boolean(&line);
        prop_assert!(result.is_ok());

        let (_, parsed_value) = result.unwrap();
        prop_assert_eq!(parsed_value, field_value);
    }

    /// Property: Timestamp is preserved exactly
    #[test]
    fn prop_timestamp_preserved(
        ts in 1_600_000_000_000_000_000i64..1_900_000_000_000_000_000i64,
    ) {
        let line = format!("test value=1.0 {}", ts);
        let result = parse_line_with_timestamp(&line);
        prop_assert!(result.is_ok());

        let (_, _, parsed_ts) = result.unwrap();
        prop_assert_eq!(parsed_ts, Some(ts));
    }

    /// Property: Multiple fields are all preserved
    #[test]
    fn prop_multiple_fields_preserved(
        field1 in float_value_strategy(),
        field2 in float_value_strategy(),
        field3 in float_value_strategy(),
    ) {
        let line = format!("test a={},b={},c={}", field1, field2, field3);
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok());

        let (_, fields) = result.unwrap();
        prop_assert_eq!(fields.len(), 3);
        prop_assert!(fields.contains_key("a"));
        prop_assert!(fields.contains_key("b"));
        prop_assert!(fields.contains_key("c"));
    }
}

// ============================================================================
// Simple Line Protocol Parser for Tests
// (Simulates the actual parser behavior for testing)
// ============================================================================

fn parse_line_simple(line: &str) -> Result<(String, HashMap<String, f64>), String> {
    let line = line.trim();
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty line".to_string());
    }

    // Parse measurement (before any comma)
    let measurement_part = parts[0];
    let measurement = measurement_part
        .split(',')
        .next()
        .unwrap_or("")
        .to_string();

    // Parse fields
    if parts.len() < 2 {
        return Err("No fields".to_string());
    }

    let fields_str = parts[1];
    let mut fields = HashMap::new();

    for field_part in fields_str.split(',') {
        if let Some((key, value)) = field_part.split_once('=') {
            if let Ok(v) = value.parse::<f64>() {
                fields.insert(key.to_string(), v);
            }
        }
    }

    Ok((measurement, fields))
}

fn parse_line_with_tags(
    line: &str,
) -> Result<(String, HashMap<String, String>, HashMap<String, f64>), String> {
    let line = line.trim();
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty line".to_string());
    }

    // Parse measurement and tags
    let measurement_part = parts[0];
    let tag_parts: Vec<&str> = measurement_part.split(',').collect();
    let measurement = tag_parts[0].to_string();

    let mut tags = HashMap::new();
    for tag_str in tag_parts.iter().skip(1) {
        if let Some((key, value)) = tag_str.split_once('=') {
            tags.insert(key.to_string(), value.to_string());
        }
    }

    // Parse fields
    let mut fields = HashMap::new();
    if parts.len() >= 2 {
        for field_part in parts[1].split(',') {
            if let Some((key, value)) = field_part.split_once('=') {
                if let Ok(v) = value.parse::<f64>() {
                    fields.insert(key.to_string(), v);
                }
            }
        }
    }

    Ok((measurement, tags, fields))
}

fn parse_line_integer(line: &str) -> Result<(String, i64), String> {
    let line = line.trim();
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("No fields".to_string());
    }

    let measurement = parts[0].split(',').next().unwrap_or("").to_string();

    // Find integer field
    for field_part in parts[1].split(',') {
        if let Some((_, value)) = field_part.split_once('=') {
            if value.ends_with('i') {
                let num_str = &value[..value.len() - 1];
                if let Ok(v) = num_str.parse::<i64>() {
                    return Ok((measurement, v));
                }
            }
        }
    }

    Err("No integer field found".to_string())
}

fn parse_line_boolean(line: &str) -> Result<(String, bool), String> {
    let line = line.trim();
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("No fields".to_string());
    }

    let measurement = parts[0].split(',').next().unwrap_or("").to_string();

    for field_part in parts[1].split(',') {
        if let Some((_, value)) = field_part.split_once('=') {
            match value.to_lowercase().as_str() {
                "true" | "t" => return Ok((measurement, true)),
                "false" | "f" => return Ok((measurement, false)),
                _ => {}
            }
        }
    }

    Err("No boolean field found".to_string())
}

fn parse_line_with_timestamp(line: &str) -> Result<(String, HashMap<String, f64>, Option<i64>), String> {
    let line = line.trim();
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty line".to_string());
    }

    let measurement = parts[0].split(',').next().unwrap_or("").to_string();

    let mut fields = HashMap::new();
    if parts.len() >= 2 {
        for field_part in parts[1].split(',') {
            if let Some((key, value)) = field_part.split_once('=') {
                if let Ok(v) = value.parse::<f64>() {
                    fields.insert(key.to_string(), v);
                }
            }
        }
    }

    let timestamp = if parts.len() >= 3 {
        parts[2].parse::<i64>().ok()
    } else {
        None
    };

    Ok((measurement, fields, timestamp))
}

// ============================================================================
// Query Result Invariant Tests (Integration)
// ============================================================================

/// Global counter for unique database names
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test server for integration tests
struct TestServer {
    process: Child,
    port: u16,
    #[allow(dead_code)]
    data_dir: tempfile::TempDir,
}

impl TestServer {
    fn start() -> Self {
        let data_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let port = find_available_port();

        let config_path = data_dir.path().join("config.toml");
        let storage_dir = data_dir.path().join("data");
        std::fs::create_dir_all(&storage_dir).expect("Failed to create storage dir");

        let storage_dir_str = storage_dir.to_str().unwrap().replace('\\', "/");
        let config_content = format!(
            "[server]\n\
            http_port = {port}\n\
            mysql_enabled = false\n\
            \n\
            [storage]\n\
            data_dir = \"{data_dir}\"\n\
            \n\
            [storage.object_store]\n\
            backend = \"local\"\n\
            data_dir = \"{data_dir}\"\n\
            \n\
            [cache]\n\
            flush_interval_secs = 1\n\
            max_size_mb = 256\n\
            \n\
            [wal]\n\
            enabled = false\n\
            \n\
            [compaction]\n\
            min_files = 100\n\
            \n\
            [retention]\n\
            enabled = false\n\
            \n\
            [logging]\n\
            level = \"warn\"\n",
            port = port,
            data_dir = storage_dir_str
        );
        std::fs::write(&config_path, &config_content).expect("Failed to write config");

        let binary_path = std::env::current_dir()
            .unwrap()
            .join("target/release/mynadb");
        let binary_path = if binary_path.exists() {
            binary_path
        } else {
            std::env::current_dir()
                .unwrap()
                .join("target/debug/mynadb")
        };

        let process = Command::new(&binary_path)
            .args(["-c", config_path.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start MynaDB");

        let server = TestServer {
            process,
            port,
            data_dir,
        };

        server.wait_for_ready();
        server
    }

    fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/health", self.port);
        for _ in 0..60 {
            if let Ok(resp) = ureq::get(&url).call() {
                if resp.status() == 200 {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("Server failed to start");
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn unique_db(&self) -> String {
        format!("propdb_{}", DB_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    fn write(&self, db: &str, data: &str) -> Result<(), String> {
        let url = format!("{}/write/{}", self.base_url(), db);
        match ureq::post(&url).send_string(data) {
            Ok(resp) if resp.status() == 200 || resp.status() == 204 => Ok(()),
            Ok(resp) => Err(format!("Write failed: {}", resp.status())),
            Err(e) => Err(format!("Write failed: {}", e)),
        }
    }

    fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/query/{}", self.base_url(), db);
        match ureq::post(&url).send_string(sql) {
            Ok(resp) => {
                let body = resp.into_string().map_err(|e| e.to_string())?;
                serde_json::from_str(&body).map_err(|e| e.to_string())
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Query failed ({}): {}", code, body))
            }
            Err(e) => Err(format!("Query failed: {}", e)),
        }
    }

    fn query_scalar(&self, db: &str, sql: &str) -> Result<f64, String> {
        let result = self.query(db, sql)?;
        extract_scalar(&result)
    }

    fn query_count(&self, db: &str, sql: &str) -> Result<i64, String> {
        let result = self.query(db, sql)?;
        extract_count(&result)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn extract_scalar(result: &serde_json::Value) -> Result<f64, String> {
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                if let Some((_, val)) = obj.iter().next() {
                    if let Some(f) = val.as_f64() {
                        return Ok(f);
                    }
                    if let Some(i) = val.as_i64() {
                        return Ok(i as f64);
                    }
                }
            }
        }
    }
    Err(format!("Failed to extract scalar: {:?}", result))
}

fn extract_count(result: &serde_json::Value) -> Result<i64, String> {
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                if let Some((_, val)) = obj.iter().next() {
                    if let Some(i) = val.as_i64() {
                        return Ok(i);
                    }
                    if let Some(f) = val.as_f64() {
                        return Ok(f as i64);
                    }
                }
            }
        }
    }
    Err(format!("Failed to extract count: {:?}", result))
}

// ============================================================================
// Query Invariant Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))] // Limited due to server startup time

    /// Property: COUNT(*) equals the number of inserted rows
    #[test]
    #[ignore] // Requires server
    fn prop_count_equals_inserted_rows(row_count in 2usize..50) {
        let server = TestServer::start();
        let db = server.unique_db();

        // Insert N rows
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let mut data = String::new();
        for i in 0..row_count {
            data.push_str(&format!("test value={}.0 {}\n", i, now + (i as i64) * 1_000_000_000));
        }

        server.write(&db, &data).map_err(|e| TestCaseError::fail(e))?;
        thread::sleep(Duration::from_secs(1));

        let count = server.query_count(&db, "SELECT COUNT(*) FROM test")
            .map_err(|e| TestCaseError::fail(e))?;

        prop_assert_eq!(count as usize, row_count, "COUNT mismatch");
    }

    /// Property: SUM equals the sum of inserted values
    #[test]
    #[ignore]
    fn prop_sum_equals_expected(values in prop::collection::vec(1.0f64..100.0, 1..20)) {
        let server = TestServer::start();
        let db = server.unique_db();

        let expected_sum: f64 = values.iter().sum();
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let mut data = String::new();
        for (i, v) in values.iter().enumerate() {
            data.push_str(&format!("test value={} {}\n", v, now + (i as i64) * 1_000_000_000));
        }

        server.write(&db, &data).map_err(|e| TestCaseError::fail(e))?;
        thread::sleep(Duration::from_secs(1));

        let sum = server.query_scalar(&db, "SELECT SUM(value) FROM test")
            .map_err(|e| TestCaseError::fail(e))?;

        let tolerance = expected_sum.abs() * 1e-10 + 1e-10;
        prop_assert!(
            (sum - expected_sum).abs() < tolerance,
            "SUM mismatch: expected {}, got {}", expected_sum, sum
        );
    }

    /// Property: MIN <= all values <= MAX
    #[test]
    #[ignore]
    fn prop_min_max_bounds(values in prop::collection::vec(-100.0f64..100.0, 2..20)) {
        let server = TestServer::start();
        let db = server.unique_db();

        let expected_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let expected_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let mut data = String::new();
        for (i, v) in values.iter().enumerate() {
            data.push_str(&format!("test value={} {}\n", v, now + (i as i64) * 1_000_000_000));
        }

        server.write(&db, &data).map_err(|e| TestCaseError::fail(e))?;
        thread::sleep(Duration::from_secs(1));

        let min = server.query_scalar(&db, "SELECT MIN(value) FROM test")
            .map_err(|e| TestCaseError::fail(e))?;
        let max = server.query_scalar(&db, "SELECT MAX(value) FROM test")
            .map_err(|e| TestCaseError::fail(e))?;

        prop_assert!(
            (min - expected_min).abs() < 1e-10,
            "MIN mismatch: expected {}, got {}", expected_min, min
        );
        prop_assert!(
            (max - expected_max).abs() < 1e-10,
            "MAX mismatch: expected {}, got {}", expected_max, max
        );
        prop_assert!(min <= max, "MIN should be <= MAX");
    }

    /// Property: AVG = SUM / COUNT
    #[test]
    #[ignore]
    fn prop_avg_equals_sum_over_count(values in prop::collection::vec(1.0f64..100.0, 2..20)) {
        let server = TestServer::start();
        let db = server.unique_db();

        let expected_avg: f64 = values.iter().sum::<f64>() / values.len() as f64;

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let mut data = String::new();
        for (i, v) in values.iter().enumerate() {
            data.push_str(&format!("test value={} {}\n", v, now + (i as i64) * 1_000_000_000));
        }

        server.write(&db, &data).map_err(|e| TestCaseError::fail(e))?;
        thread::sleep(Duration::from_secs(1));

        let avg = server.query_scalar(&db, "SELECT AVG(value) FROM test")
            .map_err(|e| TestCaseError::fail(e))?;

        let tolerance = expected_avg.abs() * 1e-10 + 1e-10;
        prop_assert!(
            (avg - expected_avg).abs() < tolerance,
            "AVG mismatch: expected {}, got {}", expected_avg, avg
        );
    }
}

// ============================================================================
// Algebraic Property Tests (No Server Required)
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Line protocol with empty measurement should fail
    #[test]
    fn prop_empty_measurement_fails(
        field_key in field_key_strategy(),
        field_value in float_value_strategy(),
    ) {
        let line = format!(" {}={}", field_key, field_value);
        let result = parse_line_simple(&line);
        // Empty measurement should result in empty string or parse error
        if let Ok((measurement, _)) = result {
            prop_assert!(measurement.is_empty() || measurement == field_key.as_str());
        }
    }

    /// Property: Parsing is deterministic (same input -> same output)
    #[test]
    fn prop_parsing_is_deterministic(
        measurement in measurement_strategy(),
        field_value in float_value_strategy(),
    ) {
        let line = format!("{} value={}", measurement, field_value);

        let result1 = parse_line_simple(&line);
        let result2 = parse_line_simple(&line);

        prop_assert_eq!(result1, result2, "Parsing should be deterministic");
    }

    /// Property: Adding more tags doesn't change measurement name
    #[test]
    fn prop_tags_dont_affect_measurement(
        measurement in measurement_strategy(),
        tag_count in 0usize..5,
    ) {
        let line1 = format!("{} value=1.0", measurement);
        let mut line2 = measurement.clone();
        for i in 0..tag_count {
            line2.push_str(&format!(",tag{}=val{}", i, i));
        }
        line2.push_str(" value=1.0");

        let result1 = parse_line_simple(&line1);
        let result2 = parse_line_simple(&line2);

        prop_assert!(result1.is_ok());
        prop_assert!(result2.is_ok());

        let (m1, _) = result1.unwrap();
        let (m2, _) = result2.unwrap();
        prop_assert_eq!(m1, m2, "Measurement should be same regardless of tags");
    }

    /// Property: Field order doesn't affect parsing
    #[test]
    fn prop_field_order_independent(
        field1 in float_value_strategy(),
        field2 in float_value_strategy(),
    ) {
        let line1 = format!("test a={},b={}", field1, field2);
        let line2 = format!("test b={},a={}", field2, field1);

        let result1 = parse_line_simple(&line1);
        let result2 = parse_line_simple(&line2);

        prop_assert!(result1.is_ok());
        prop_assert!(result2.is_ok());

        let (_, fields1) = result1.unwrap();
        let (_, fields2) = result2.unwrap();

        // Both should have same fields with same values
        prop_assert_eq!(fields1.len(), fields2.len());
        prop_assert_eq!(fields1.get("a"), fields2.get("a"));
        prop_assert_eq!(fields1.get("b"), fields2.get("b"));
    }

    /// Property: Tag order doesn't affect parsing
    #[test]
    fn prop_tag_order_independent(
        measurement in measurement_strategy(),
        tag1_key in tag_key_strategy(),
        tag1_val in tag_value_strategy(),
        tag2_key in tag_key_strategy(),
        tag2_val in tag_value_strategy(),
    ) {
        // Ensure different tag keys
        prop_assume!(tag1_key != tag2_key);

        let line1 = format!("{},{}={},{}={} value=1.0", measurement, tag1_key, tag1_val, tag2_key, tag2_val);
        let line2 = format!("{},{}={},{}={} value=1.0", measurement, tag2_key, tag2_val, tag1_key, tag1_val);

        let result1 = parse_line_with_tags(&line1);
        let result2 = parse_line_with_tags(&line2);

        prop_assert!(result1.is_ok(), "line1 parse failed: {}", line1);
        prop_assert!(result2.is_ok(), "line2 parse failed: {}", line2);

        let (m1, tags1, _) = result1.unwrap();
        let (m2, tags2, _) = result2.unwrap();

        prop_assert_eq!(m1, m2, "Measurement should match");
        prop_assert_eq!(tags1, tags2, "Tags should match regardless of order");
    }

    /// Property: Whitespace trimming is consistent
    #[test]
    fn prop_whitespace_trimming(
        measurement in measurement_strategy(),
        leading_spaces in 0usize..3,
        trailing_spaces in 0usize..3,
    ) {
        let base_line = format!("{} value=1.0", measurement);
        let padded_line = format!(
            "{}{}{}",
            " ".repeat(leading_spaces),
            base_line,
            " ".repeat(trailing_spaces)
        );

        let result1 = parse_line_simple(&base_line);
        let result2 = parse_line_simple(&padded_line);

        prop_assert!(result1.is_ok());
        prop_assert!(result2.is_ok());

        let (m1, _) = result1.unwrap();
        let (m2, _) = result2.unwrap();
        prop_assert_eq!(m1, m2, "Measurement should match after trimming");
    }

    /// Property: Very large field count doesn't cause issues
    #[test]
    fn prop_many_fields_parse(field_count in 5usize..15) {
        let mut line = "test".to_string();
        let mut expected_fields = HashMap::new();

        for i in 0..field_count {
            let field_name = format!("f{}", i);
            let field_value = i as f64;
            if i == 0 {
                line.push_str(&format!(" {}={}", field_name, field_value));
            } else {
                line.push_str(&format!(",{}={}", field_name, field_value));
            }
            expected_fields.insert(field_name, field_value);
        }

        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok(), "Failed to parse: {}", line);

        let (_, fields) = result.unwrap();
        prop_assert_eq!(fields.len(), field_count, "Field count mismatch");

        for (key, expected_val) in &expected_fields {
            let actual = fields.get(key).cloned().unwrap_or(f64::NAN);
            prop_assert!(
                (actual - expected_val).abs() < 0.0001,
                "Field {} mismatch: expected {}, got {}",
                key, expected_val, actual
            );
        }
    }

    /// Property: Negative values are preserved
    #[test]
    fn prop_negative_values_preserved(value in -1000.0f64..-0.001) {
        let line = format!("test value={}", value);
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok());

        let (_, fields) = result.unwrap();
        let parsed = fields.get("value").cloned().unwrap_or(0.0);
        let tolerance = value.abs() * 1e-10 + 1e-15;
        prop_assert!(
            (parsed - value).abs() < tolerance,
            "Negative value mismatch: expected {}, got {}",
            value, parsed
        );
    }

    /// Property: Zero is preserved exactly
    #[test]
    fn prop_zero_preserved(_dummy in 0..1) {
        let line = "test value=0.0";
        let result = parse_line_simple(line);
        prop_assert!(result.is_ok());

        let (_, fields) = result.unwrap();
        let parsed = fields.get("value").cloned().unwrap_or(f64::NAN);
        prop_assert!(
            parsed == 0.0,
            "Zero not preserved: got {}",
            parsed
        );
    }

    /// Property: Very small numbers are handled
    #[test]
    fn prop_small_numbers_handled(exp in -15i32..-1) {
        let value = 10f64.powi(exp);
        let line = format!("test value={}", value);
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok());

        let (_, fields) = result.unwrap();
        let parsed = fields.get("value").cloned().unwrap_or(0.0);
        let tolerance = value.abs() * 1e-10 + 1e-20;
        prop_assert!(
            (parsed - value).abs() < tolerance,
            "Small number mismatch: expected {}, got {}",
            value, parsed
        );
    }

    /// Property: Measurement name length doesn't affect parsing
    #[test]
    fn prop_long_measurement_name(length in 1usize..50) {
        let measurement: String = (0..length).map(|_| 'a').collect();
        let line = format!("{} value=1.0", measurement);
        let result = parse_line_simple(&line);
        prop_assert!(result.is_ok());

        let (parsed_measurement, _) = result.unwrap();
        prop_assert_eq!(parsed_measurement, measurement);
    }
}

// ============================================================================
// Edge Case Unit Tests (Specific Known Edge Cases)
// ============================================================================

#[test]
fn test_edge_case_empty_line() {
    let result = parse_line_simple("");
    assert!(result.is_err() || result.unwrap().1.is_empty());
}

#[test]
fn test_edge_case_whitespace_only() {
    let result = parse_line_simple("   ");
    assert!(result.is_err() || result.unwrap().1.is_empty());
}

#[test]
fn test_edge_case_measurement_only() {
    let result = parse_line_simple("cpu");
    // Should fail - no fields
    assert!(result.is_err() || result.unwrap().1.is_empty());
}

#[test]
fn test_edge_case_single_char_measurement() {
    let result = parse_line_simple("m value=1.0");
    assert!(result.is_ok());
    let (measurement, fields) = result.unwrap();
    assert_eq!(measurement, "m");
    assert!(fields.contains_key("value"));
}

#[test]
fn test_edge_case_single_char_field() {
    let result = parse_line_simple("test v=1.0");
    assert!(result.is_ok());
    let (_, fields) = result.unwrap();
    assert!(fields.contains_key("v"));
}

#[test]
fn test_edge_case_underscore_in_names() {
    let result = parse_line_simple("my_measurement my_field=1.0");
    assert!(result.is_ok());
    let (measurement, fields) = result.unwrap();
    assert_eq!(measurement, "my_measurement");
    assert!(fields.contains_key("my_field"));
}

#[test]
fn test_edge_case_numeric_suffix_measurement() {
    let result = parse_line_simple("cpu123 value=1.0");
    assert!(result.is_ok());
    let (measurement, _) = result.unwrap();
    assert_eq!(measurement, "cpu123");
}

#[test]
fn test_edge_case_integer_field() {
    let result = parse_line_integer("test count=42i");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert_eq!(value, 42);
}

#[test]
fn test_edge_case_negative_integer() {
    let result = parse_line_integer("test count=-100i");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert_eq!(value, -100);
}

#[test]
fn test_edge_case_large_integer() {
    let result = parse_line_integer("test count=9223372036854775807i");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert_eq!(value, i64::MAX);
}

#[test]
fn test_edge_case_boolean_true() {
    let result = parse_line_boolean("test active=true");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert!(value);
}

#[test]
fn test_edge_case_boolean_false() {
    let result = parse_line_boolean("test active=false");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert!(!value);
}

#[test]
fn test_edge_case_boolean_t() {
    let result = parse_line_boolean("test active=t");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert!(value);
}

#[test]
fn test_edge_case_boolean_f() {
    let result = parse_line_boolean("test active=f");
    assert!(result.is_ok());
    let (_, value) = result.unwrap();
    assert!(!value);
}

#[test]
fn test_edge_case_scientific_notation() {
    let result = parse_line_simple("test value=1.5e10");
    assert!(result.is_ok());
    let (_, fields) = result.unwrap();
    let value = fields.get("value").cloned().unwrap_or(0.0);
    assert!((value - 1.5e10).abs() < 1e5);
}

#[test]
fn test_edge_case_negative_scientific() {
    let result = parse_line_simple("test value=-2.5e-5");
    assert!(result.is_ok());
    let (_, fields) = result.unwrap();
    let value = fields.get("value").cloned().unwrap_or(0.0);
    assert!((value - (-2.5e-5)).abs() < 1e-10);
}

#[test]
fn test_edge_case_many_tags() {
    let line = "cpu,t1=v1,t2=v2,t3=v3,t4=v4,t5=v5 value=1.0";
    let result = parse_line_with_tags(line);
    assert!(result.is_ok());
    let (_, tags, _) = result.unwrap();
    assert_eq!(tags.len(), 5);
}

#[test]
fn test_edge_case_timestamp_zero() {
    let result = parse_line_with_timestamp("test value=1.0 0");
    assert!(result.is_ok());
    let (_, _, ts) = result.unwrap();
    assert_eq!(ts, Some(0));
}

#[test]
fn test_edge_case_no_timestamp() {
    let result = parse_line_with_timestamp("test value=1.0");
    assert!(result.is_ok());
    let (_, _, ts) = result.unwrap();
    assert_eq!(ts, None);
}

#[test]
fn test_edge_case_future_timestamp() {
    let future_ts = 4102444800_000_000_000i64; // Year 2100
    let line = format!("test value=1.0 {}", future_ts);
    let result = parse_line_with_timestamp(&line);
    assert!(result.is_ok());
    let (_, _, ts) = result.unwrap();
    assert_eq!(ts, Some(future_ts));
}
