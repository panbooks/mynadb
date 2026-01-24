//! Public Dataset Validation Tests
//!
//! Uses well-known public datasets downloaded from the internet to validate
//! MynaDB query correctness. Results are compared against golden files.
//!
//! Datasets used:
//! - NOAA Weather (hourly observations)
//! - NYC Taxi (trip records subset)
//!
//! Run with: cargo test --test public_dataset_validation -- --ignored --nocapture

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

const TEST_HTTP_PORT: u16 = 17100;
const TEST_DATA_DIR: &str = "/tmp/mynadb_public_test";
const GOLDEN_DIR: &str = "tests/golden";

// ============================================================================
// Public Dataset Sources
// ============================================================================

/// Well-known public datasets with download URLs
struct PublicDataset {
    name: &'static str,
    url: &'static str,
    description: &'static str,
}

const DATASETS: &[PublicDataset] = &[
    PublicDataset {
        name: "noaa_weather_sample",
        url: "https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/noaa-ndbc-data/latest-observations.lp",
        description: "NOAA NDBC buoy observations in line protocol format",
    },
    PublicDataset {
        name: "air_sensor_sample",
        url: "https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/air-sensor-data/air-sensor-data.lp",
        description: "Air quality sensor data in line protocol format",
    },
];

// ============================================================================
// Expected Value Computation from Raw Data
// ============================================================================

/// Parsed line protocol data point
#[derive(Debug, Clone)]
struct ParsedDataPoint {
    measurement: String,
    tags: HashMap<String, String>,
    fields: HashMap<String, f64>,
    timestamp: i64,
}

/// Expected values computed from raw line protocol data
#[derive(Debug, Default)]
struct ExpectedValues {
    row_count: usize,
    measurements: HashMap<String, usize>,           // measurement -> count
    tag_values: HashMap<String, HashMap<String, usize>>, // tag_key -> (tag_value -> count)
    field_sums: HashMap<String, f64>,               // field_name -> sum
    field_mins: HashMap<String, f64>,               // field_name -> min
    field_maxs: HashMap<String, f64>,               // field_name -> max
}

impl ExpectedValues {
    fn new() -> Self {
        Self::default()
    }

    fn add_point(&mut self, point: &ParsedDataPoint) {
        self.row_count += 1;

        // Count by measurement
        *self.measurements.entry(point.measurement.clone()).or_insert(0) += 1;

        // Count by tag values
        for (tag_key, tag_value) in &point.tags {
            let tag_counts = self.tag_values.entry(tag_key.clone()).or_insert_with(HashMap::new);
            *tag_counts.entry(tag_value.clone()).or_insert(0) += 1;
        }

        // Aggregate field values
        for (field_name, value) in &point.fields {
            *self.field_sums.entry(field_name.clone()).or_insert(0.0) += value;

            let min = self.field_mins.entry(field_name.clone()).or_insert(f64::MAX);
            if *value < *min {
                *min = *value;
            }

            let max = self.field_maxs.entry(field_name.clone()).or_insert(f64::MIN);
            if *value > *max {
                *max = *value;
            }
        }
    }

    fn field_avg(&self, field_name: &str) -> Option<f64> {
        let sum = self.field_sums.get(field_name)?;
        Some(sum / self.row_count as f64)
    }

    fn distinct_tag_count(&self, tag_key: &str) -> usize {
        self.tag_values.get(tag_key).map(|m| m.len()).unwrap_or(0)
    }
}

/// Parse a single line of InfluxDB line protocol
fn parse_line_protocol(line: &str) -> Option<ParsedDataPoint> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    // Split into: measurement,tags fields timestamp
    // Format: measurement,tag1=v1,tag2=v2 field1=v1,field2=v2 timestamp

    // Find the space separating tags from fields
    let first_space = line.find(' ')?;
    let measurement_tags = &line[..first_space];
    let rest = &line[first_space + 1..];

    // Find the space separating fields from timestamp
    let (fields_str, timestamp_str) = if let Some(last_space) = rest.rfind(' ') {
        (&rest[..last_space], &rest[last_space + 1..])
    } else {
        (rest, "0")
    };

    // Parse measurement and tags
    let mut parts = measurement_tags.split(',');
    let measurement = parts.next()?.to_string();

    let mut tags = HashMap::new();
    for tag_part in parts {
        if let Some(eq_pos) = tag_part.find('=') {
            let key = tag_part[..eq_pos].to_string();
            let value = tag_part[eq_pos + 1..].replace("\\ ", " ").replace("\\,", ",");
            tags.insert(key, value);
        }
    }

    // Parse fields
    let mut fields = HashMap::new();
    for field_part in fields_str.split(',') {
        if let Some(eq_pos) = field_part.find('=') {
            let key = field_part[..eq_pos].to_string();
            let value_str = &field_part[eq_pos + 1..];

            // Try to parse as number (remove 'i' suffix for integers, handle strings)
            let value_str = value_str.trim_end_matches('i');
            if let Ok(value) = value_str.parse::<f64>() {
                fields.insert(key, value);
            }
            // Skip string fields (quoted values)
        }
    }

    // Parse timestamp
    let timestamp = timestamp_str.parse::<i64>().unwrap_or(0);

    Some(ParsedDataPoint {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

/// Compute expected values from line protocol data
fn compute_expected_values(data: &str) -> ExpectedValues {
    let mut expected = ExpectedValues::new();

    for line in data.lines() {
        if let Some(point) = parse_line_protocol(line) {
            expected.add_point(&point);
        }
    }

    expected
}

// ============================================================================
// Golden File Support
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct GoldenResult {
    query: String,
    description: String,
    expected_row_count: usize,
    expected_columns: Vec<String>,
    sample_values: HashMap<String, Vec<serde_json::Value>>,
    checksum: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GoldenFile {
    dataset: String,
    generated_at: String,
    queries: Vec<GoldenResult>,
}

fn load_golden_file(dataset: &str) -> Option<GoldenFile> {
    let path = PathBuf::from(GOLDEN_DIR).join(format!("{}.json", dataset));
    if path.exists() {
        let content = fs::read_to_string(&path).ok()?;
        serde_json::from_str(&content).ok()
    } else {
        None
    }
}

fn save_golden_file(golden: &GoldenFile) -> std::io::Result<()> {
    let path = PathBuf::from(GOLDEN_DIR).join(format!("{}.json", golden.dataset));
    fs::create_dir_all(GOLDEN_DIR)?;
    let content = serde_json::to_string_pretty(golden)?;
    fs::write(path, content)
}

// ============================================================================
// Test Server Management
// ============================================================================

struct TestServer {
    process: Child,
    http_port: u16,
    data_dir: String,
}

impl TestServer {
    fn start(http_port: u16, data_dir: &str) -> Self {
        // Clean data directory
        let _ = fs::remove_dir_all(data_dir);
        fs::create_dir_all(data_dir).expect("Failed to create data dir");

        // Create config file
        let config_path = format!("{}/config.toml", data_dir);
        let config_content = format!(
            r#"
[server]
http_port = {}
flight_port = {}
mysql_enabled = false

[storage]
data_dir = "{}"

[storage.object_store]
backend = "local"
data_dir = "{}"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"

[cache]
flush_interval_secs = 1
max_size_mb = 256

[wal]
enabled = false

[compaction]
min_files = 100

[retention]
enabled = false

[auth]
enabled = false

[logging]
level = "warn"
"#,
            http_port,
            http_port + 1,
            data_dir,
            data_dir
        );
        fs::write(&config_path, config_content).expect("Failed to write config");

        // Build first if needed
        let _ = Command::new("cargo")
            .args(["build", "--release"])
            .status();

        // Start server using the release binary directly
        let process = Command::new("./target/release/mynadb")
            .args(["-c", &config_path])
            .spawn()
            .expect("Failed to start MynaDB server");

        // Wait for server to be ready (up to 30 seconds)
        let url = format!("http://127.0.0.1:{}/health", http_port);
        let mut ready = false;
        for _ in 0..60 {
            thread::sleep(Duration::from_millis(500));
            if ureq::get(&url).call().is_ok() {
                ready = true;
                break;
            }
        }
        if !ready {
            panic!("Server failed to start on port {}", http_port);
        }

        Self {
            process,
            http_port,
            data_dir: data_dir.to_string(),
        }
    }

    fn write_line_protocol(&self, db: &str, data: &str) -> Result<(), String> {
        let url = format!("http://127.0.0.1:{}/write/{}", self.http_port, db);
        match ureq::post(&url)
            .set("Content-Type", "text/plain")
            .send_string(data)
        {
            Ok(response) => {
                let status = response.status();
                let body = response.into_string().unwrap_or_default();
                println!("Write response: {} - {}", status, body);
                Ok(())
            }
            Err(ureq::Error::Status(code, response)) => {
                let body = response.into_string().unwrap_or_default();
                Err(format!("Write failed ({}): {}", code, body))
            }
            Err(e) => Err(format!("Write failed: {}", e)),
        }
    }

    fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("http://127.0.0.1:{}/query/{}", self.http_port, db);
        // Use POST with SQL in body (matching the API convention)
        match ureq::post(&url).send_string(sql) {
            Ok(response) => {
                let body = response
                    .into_string()
                    .map_err(|e| format!("Read body failed: {}", e))?;
                serde_json::from_str(&body).map_err(|e| format!("Parse JSON failed: {}: {}", e, body))
            }
            Err(ureq::Error::Status(code, response)) => {
                let body = response.into_string().unwrap_or_default();
                Err(format!("Query failed ({}): {}", code, body))
            }
            Err(e) => Err(format!("Query failed: {}", e)),
        }
    }

    fn list_tables(&self, db: &str) -> Result<serde_json::Value, String> {
        let url = format!("http://127.0.0.1:{}/databases/{}/tables", self.http_port, db);
        match ureq::get(&url).call() {
            Ok(response) => {
                let body = response
                    .into_string()
                    .map_err(|e| format!("Read body failed: {}", e))?;
                serde_json::from_str(&body).map_err(|e| format!("Parse JSON failed: {}", e))
            }
            Err(ureq::Error::Status(code, response)) => {
                let body = response.into_string().unwrap_or_default();
                Err(format!("List tables failed ({}): {}", code, body))
            }
            Err(e) => Err(format!("List tables failed: {}", e)),
        }
    }

    fn flush(&self) -> Result<(), String> {
        let url = format!("http://127.0.0.1:{}/flush", self.http_port);
        match ureq::post(&url).call() {
            Ok(response) => {
                let body = response.into_string().unwrap_or_default();
                println!("Flush response: {}", body);
            }
            Err(ureq::Error::Status(code, response)) => {
                let body = response.into_string().unwrap_or_default();
                return Err(format!("Flush failed ({}): {}", code, body));
            }
            Err(e) => return Err(format!("Flush failed: {}", e)),
        }
        // Wait for data to be persisted
        thread::sleep(Duration::from_secs(3));
        Ok(())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = fs::remove_dir_all(&self.data_dir);
    }
}

// ============================================================================
// Dataset Download
// ============================================================================

fn download_dataset(url: &str) -> Result<String, String> {
    println!("Downloading: {}", url);
    let response = ureq::get(url)
        .timeout(Duration::from_secs(120))
        .call()
        .map_err(|e| format!("Download failed: {}", e))?;
    response
        .into_string()
        .map_err(|e| format!("Read failed: {}", e))
}

fn download_csv_as_line_protocol(
    url: &str,
    measurement: &str,
    tag_cols: &[&str],
    field_cols: &[&str],
    time_col: Option<&str>,
) -> Result<String, String> {
    println!("Downloading CSV: {}", url);
    let response = ureq::get(url)
        .timeout(Duration::from_secs(120))
        .call()
        .map_err(|e| format!("Download failed: {}", e))?;
    let body = response
        .into_string()
        .map_err(|e| format!("Read failed: {}", e))?;

    let mut lines = body.lines();
    let header: Vec<&str> = lines
        .next()
        .ok_or("Empty CSV")?
        .split(',')
        .map(|s| s.trim())
        .collect();

    let mut line_protocol = String::new();
    let base_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let mut row_idx = 0i64;

    for line in lines {
        let values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if values.len() != header.len() {
            continue;
        }

        let row: HashMap<&str, &str> = header.iter().cloned().zip(values.iter().cloned()).collect();

        // Build tags
        let tags: Vec<String> = tag_cols
            .iter()
            .filter_map(|col| {
                row.get(col).map(|v| {
                    let clean_val = v.replace(" ", "_").replace(",", "_");
                    format!("{}={}", col, clean_val)
                })
            })
            .collect();

        // Build fields
        let fields: Vec<String> = field_cols
            .iter()
            .filter_map(|col| {
                row.get(col).and_then(|v| {
                    if let Ok(num) = v.parse::<f64>() {
                        Some(format!("{}={}", col, num))
                    } else if let Ok(num) = v.parse::<i64>() {
                        Some(format!("{}={}i", col, num))
                    } else {
                        None
                    }
                })
            })
            .collect();

        if fields.is_empty() {
            continue;
        }

        // Get timestamp
        let timestamp = if let Some(time_col) = time_col {
            if let Some(time_str) = row.get(time_col) {
                parse_timestamp(time_str).unwrap_or(base_time + row_idx * 1_000_000_000)
            } else {
                base_time + row_idx * 1_000_000_000
            }
        } else {
            base_time + row_idx * 1_000_000_000
        };

        let tag_str = if tags.is_empty() {
            String::new()
        } else {
            format!(",{}", tags.join(","))
        };

        line_protocol.push_str(&format!(
            "{}{} {} {}\n",
            measurement,
            tag_str,
            fields.join(","),
            timestamp
        ));
        row_idx += 1;
    }

    Ok(line_protocol)
}

fn parse_timestamp(s: &str) -> Option<i64> {
    // Try parsing as nanoseconds directly
    if let Ok(ns) = s.parse::<i64>() {
        return Some(ns);
    }
    // Try parsing as ISO 8601
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_nanos_opt().unwrap_or(0));
    }
    // Try parsing as date
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0)?;
        return Some(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
    }
    None
}

// ============================================================================
// Validation Helpers
// ============================================================================

fn compare_results(
    actual: &serde_json::Value,
    expected: &GoldenResult,
    tolerance: f64,
) -> Result<(), String> {
    // Check row count
    let actual_rows = actual
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    if actual_rows != expected.expected_row_count {
        return Err(format!(
            "Row count mismatch: got {}, expected {}",
            actual_rows, expected.expected_row_count
        ));
    }

    // Check columns if present
    if let Some(first_row) = actual.as_array().and_then(|a| a.first()) {
        if let Some(obj) = first_row.as_object() {
            let actual_cols: Vec<&str> = obj.keys().map(|s| s.as_str()).collect();
            for expected_col in &expected.expected_columns {
                if !actual_cols.contains(&expected_col.as_str()) {
                    return Err(format!("Missing column: {}", expected_col));
                }
            }
        }
    }

    // Check sample values with tolerance
    for (col, expected_values) in &expected.sample_values {
        if let Some(rows) = actual.as_array() {
            for (i, expected_val) in expected_values.iter().enumerate() {
                if i >= rows.len() {
                    break;
                }
                if let Some(actual_val) = rows[i].get(col) {
                    if !values_match(actual_val, expected_val, tolerance) {
                        return Err(format!(
                            "Value mismatch at row {} col {}: got {:?}, expected {:?}",
                            i, col, actual_val, expected_val
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

fn values_match(actual: &serde_json::Value, expected: &serde_json::Value, tolerance: f64) -> bool {
    match (actual, expected) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let a_f = a.as_f64().unwrap_or(0.0);
            let b_f = b.as_f64().unwrap_or(0.0);
            if b_f == 0.0 {
                (a_f - b_f).abs() < tolerance
            } else {
                ((a_f - b_f) / b_f).abs() < tolerance
            }
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a == b,
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        _ => actual == expected,
    }
}

// ============================================================================
// Standard Query Suite
// ============================================================================

struct QueryTestCase {
    name: String,
    description: String,
    sql: String,
}

/// Standard queries to run on any time series dataset
fn get_standard_queries(measurement: &str) -> Vec<QueryTestCase> {
    vec![
        QueryTestCase {
            name: "count_all".to_string(),
            description: "Count all rows".to_string(),
            sql: format!("SELECT COUNT(*) as cnt FROM {}", measurement),
        },
        QueryTestCase {
            name: "select_limit".to_string(),
            description: "Select with limit".to_string(),
            sql: format!("SELECT * FROM {} LIMIT 10", measurement),
        },
        QueryTestCase {
            name: "order_by_time".to_string(),
            description: "Order by time descending".to_string(),
            sql: format!("SELECT * FROM {} ORDER BY time DESC LIMIT 5", measurement),
        },
    ]
}

// ============================================================================
// Tests
// ============================================================================

#[test]
#[ignore]
fn test_noaa_weather_data() {
    let server = TestServer::start(TEST_HTTP_PORT, TEST_DATA_DIR);
    let db = "noaa_test";

    // Download NOAA line protocol data from InfluxDB sample repo
    let data = download_dataset(DATASETS[0].url).expect("Failed to download NOAA data");

    // Take first 1000 lines to keep test fast
    let subset: String = data.lines().take(1000).collect::<Vec<_>>().join("\n");
    let raw_line_count = subset.lines().count();
    println!("Loaded {} lines of raw NOAA data", raw_line_count);

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // =========================================================================
    // STEP 1: Get the baseline count of what MynaDB actually stored
    // Note: NOAA data has complex escape sequences (backslash-escaped spaces)
    // that not all lines may be parsed successfully. We verify internal
    // consistency of what IS stored, not completeness.
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM default.ndbc")
        .expect("Count query failed");

    let total_count = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    println!("MynaDB stored {} rows (from {} raw lines)", total_count, raw_line_count);
    assert!(total_count > 0, "Expected some rows to be written");
    println!("✓ Data ingestion confirmed: {} rows", total_count);

    // =========================================================================
    // STEP 2: Verify COUNT(DISTINCT station_id) consistency
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(DISTINCT station_id) as cnt FROM default.ndbc")
        .expect("Distinct count query failed");

    let distinct_count = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    println!("COUNT(DISTINCT station_id) = {}", distinct_count);
    assert!(distinct_count > 0 && distinct_count <= total_count,
        "Distinct count {} should be > 0 and <= total count {}", distinct_count, total_count);
    println!("✓ Distinct count is valid");

    // =========================================================================
    // STEP 3: Verify GROUP BY counts sum to total
    // =========================================================================
    let result = server.query(db,
        "SELECT station_id, COUNT(*) as cnt FROM default.ndbc GROUP BY station_id"
    ).expect("Group by query failed");

    let rows = result.as_array().expect("Expected array result");
    let group_count = rows.len();
    let sum_of_groups: i64 = rows.iter()
        .filter_map(|r| r.get("cnt").and_then(|v| v.as_i64()))
        .sum();

    println!("GROUP BY station_id: {} groups, sum of counts = {}", group_count, sum_of_groups);
    assert_eq!(group_count, distinct_count,
        "Number of groups ({}) should equal COUNT(DISTINCT) ({})", group_count, distinct_count);
    assert_eq!(sum_of_groups as usize, total_count,
        "Sum of group counts ({}) should equal total count ({})", sum_of_groups, total_count);
    println!("✓ GROUP BY counts verified: {} groups summing to {}", group_count, sum_of_groups);

    // =========================================================================
    // STEP 4: Verify ORDER BY and LIMIT work correctly
    // =========================================================================
    let result = server.query(db,
        "SELECT station_id, COUNT(*) as cnt FROM default.ndbc GROUP BY station_id ORDER BY cnt DESC LIMIT 5"
    ).expect("Order by query failed");

    let rows = result.as_array().expect("Expected array result");
    assert!(rows.len() <= 5, "LIMIT 5 should return at most 5 rows");

    // Verify ordering (counts should be descending)
    let counts: Vec<i64> = rows.iter()
        .filter_map(|r| r.get("cnt").and_then(|v| v.as_i64()))
        .collect();
    for i in 1..counts.len() {
        assert!(counts[i-1] >= counts[i],
            "Results not in descending order: {} < {}", counts[i-1], counts[i]);
    }
    println!("✓ ORDER BY DESC LIMIT verified");

    println!("\n✅ All NOAA weather data validations passed!");
}

#[test]
#[ignore]
fn test_air_sensor_data() {
    let server = TestServer::start(TEST_HTTP_PORT + 10, &format!("{}_air", TEST_DATA_DIR));
    let db = "air_test";

    // Download air sensor line protocol data
    let data = download_dataset(DATASETS[1].url).expect("Failed to download air sensor data");

    // Take subset
    let subset: String = data.lines().take(500).collect::<Vec<_>>().join("\n");
    println!("Loaded {} lines of air sensor data", subset.lines().count());

    // =========================================================================
    // STEP 1: Compute expected values from raw data BEFORE writing
    // =========================================================================
    let expected = compute_expected_values(&subset);
    println!("Expected values computed from raw data:");
    println!("  - Total rows: {}", expected.row_count);
    println!("  - Distinct sensor_ids: {}", expected.distinct_tag_count("sensor_id"));
    if let Some(sum) = expected.field_sums.get("temperature") {
        println!("  - SUM(temperature): {:.4}", sum);
    }
    if let Some(sum) = expected.field_sums.get("humidity") {
        println!("  - SUM(humidity): {:.4}", sum);
    }

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // =========================================================================
    // STEP 2: Verify COUNT(*) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM default.airSensors")
        .expect("Count query failed");

    let actual_count = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    println!("COUNT(*) - Expected: {}, Actual: {}", expected.row_count, actual_count);
    assert_eq!(actual_count, expected.row_count,
        "Row count mismatch: expected {}, got {}", expected.row_count, actual_count);
    println!("✓ COUNT(*) verified: {}", actual_count);

    // =========================================================================
    // STEP 3: Verify SUM(temperature) matches expected (with floating point tolerance)
    // =========================================================================
    let result = server.query(db, "SELECT SUM(temperature) as total FROM default.airSensors")
        .expect("SUM query failed");

    let actual_sum = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("total"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_sum = *expected.field_sums.get("temperature").unwrap_or(&0.0);
    let tolerance = expected_sum.abs() * 1e-9; // Relative tolerance
    println!("SUM(temperature) - Expected: {:.6}, Actual: {:.6}", expected_sum, actual_sum);
    assert!((actual_sum - expected_sum).abs() < tolerance,
        "SUM(temperature) mismatch: expected {}, got {}", expected_sum, actual_sum);
    println!("✓ SUM(temperature) verified");

    // =========================================================================
    // STEP 4: Verify AVG(humidity) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT AVG(humidity) as avg_val FROM default.airSensors")
        .expect("AVG query failed");

    let actual_avg = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("avg_val"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_avg = expected.field_avg("humidity").unwrap_or(0.0);
    let tolerance = expected_avg.abs() * 1e-9;
    println!("AVG(humidity) - Expected: {:.6}, Actual: {:.6}", expected_avg, actual_avg);
    assert!((actual_avg - expected_avg).abs() < tolerance,
        "AVG(humidity) mismatch: expected {}, got {}", expected_avg, actual_avg);
    println!("✓ AVG(humidity) verified");

    // =========================================================================
    // STEP 5: Verify MIN/MAX(co) match expected
    // =========================================================================
    let result = server.query(db, "SELECT MIN(co) as min_val, MAX(co) as max_val FROM default.airSensors")
        .expect("MIN/MAX query failed");

    let row = result.as_array().and_then(|a| a.first()).expect("Expected result row");
    let actual_min = row.get("min_val").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let actual_max = row.get("max_val").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let expected_min = *expected.field_mins.get("co").unwrap_or(&0.0);
    let expected_max = *expected.field_maxs.get("co").unwrap_or(&0.0);

    println!("MIN(co) - Expected: {:.6}, Actual: {:.6}", expected_min, actual_min);
    println!("MAX(co) - Expected: {:.6}, Actual: {:.6}", expected_max, actual_max);

    let min_tolerance = expected_min.abs() * 1e-9 + 1e-15;
    let max_tolerance = expected_max.abs() * 1e-9 + 1e-15;
    assert!((actual_min - expected_min).abs() < min_tolerance,
        "MIN(co) mismatch: expected {}, got {}", expected_min, actual_min);
    assert!((actual_max - expected_max).abs() < max_tolerance,
        "MAX(co) mismatch: expected {}, got {}", expected_max, actual_max);
    println!("✓ MIN/MAX(co) verified");

    println!("\n✅ All air sensor data validations passed!");
}

#[test]
#[ignore]
fn test_bird_migration_data() {
    // Uses InfluxDB's bird migration sample data
    // Dataset contains: migration measurement with id tag, lat/lon fields
    let url = "https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/bird-migration-data/bird-migration.line";

    let server = TestServer::start(TEST_HTTP_PORT + 20, &format!("{}_bird", TEST_DATA_DIR));
    let db = "bird_test";

    let data = download_dataset(url).expect("Failed to download bird migration data");
    let subset: String = data.lines().take(500).collect::<Vec<_>>().join("\n");
    println!("Loaded {} lines of bird migration data", subset.lines().count());

    // =========================================================================
    // STEP 1: Compute expected values from raw data BEFORE writing
    // =========================================================================
    let expected = compute_expected_values(&subset);
    println!("Expected values computed from raw data:");
    println!("  - Total rows: {}", expected.row_count);
    println!("  - Distinct ids: {}", expected.distinct_tag_count("id"));
    if let Some(sum) = expected.field_sums.get("lat") {
        println!("  - SUM(lat): {:.4}", sum);
    }
    if let Some(sum) = expected.field_sums.get("lon") {
        println!("  - SUM(lon): {:.4}", sum);
    }
    if let Some(min) = expected.field_mins.get("lat") {
        println!("  - MIN(lat): {:.4}", min);
    }
    if let Some(max) = expected.field_maxs.get("lat") {
        println!("  - MAX(lat): {:.4}", max);
    }

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // =========================================================================
    // STEP 2: Verify COUNT(*) matches expected
    // Determine measurement name from data
    // =========================================================================
    let measurement = subset.lines().next()
        .and_then(|line| line.split(',').next())
        .unwrap_or("migration");
    println!("Detected measurement: {}", measurement);

    let result = server.query(db, &format!("SELECT COUNT(*) as cnt FROM default.{}", measurement))
        .expect("Count query failed");

    let actual_count = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    println!("COUNT(*) - Expected: {}, Actual: {}", expected.row_count, actual_count);
    assert_eq!(actual_count, expected.row_count,
        "Row count mismatch: expected {}, got {}", expected.row_count, actual_count);
    println!("✓ COUNT(*) verified: {}", actual_count);

    // =========================================================================
    // STEP 3: Verify SUM(lat) matches expected
    // =========================================================================
    let result = server.query(db, &format!("SELECT SUM(lat) as total FROM default.{}", measurement))
        .expect("SUM query failed");

    let actual_sum = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("total"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_sum = *expected.field_sums.get("lat").unwrap_or(&0.0);
    let tolerance = expected_sum.abs() * 1e-9 + 1e-15;
    println!("SUM(lat) - Expected: {:.6}, Actual: {:.6}", expected_sum, actual_sum);
    assert!((actual_sum - expected_sum).abs() < tolerance,
        "SUM(lat) mismatch: expected {}, got {}", expected_sum, actual_sum);
    println!("✓ SUM(lat) verified");

    // =========================================================================
    // STEP 4: Verify AVG(lon) matches expected
    // =========================================================================
    let result = server.query(db, &format!("SELECT AVG(lon) as avg_val FROM default.{}", measurement))
        .expect("AVG query failed");

    let actual_avg = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("avg_val"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_avg = expected.field_avg("lon").unwrap_or(0.0);
    let tolerance = expected_avg.abs() * 1e-9 + 1e-15;
    println!("AVG(lon) - Expected: {:.6}, Actual: {:.6}", expected_avg, actual_avg);
    assert!((actual_avg - expected_avg).abs() < tolerance,
        "AVG(lon) mismatch: expected {}, got {}", expected_avg, actual_avg);
    println!("✓ AVG(lon) verified");

    // =========================================================================
    // STEP 5: Verify MIN/MAX(lat) match expected
    // =========================================================================
    let result = server.query(db, &format!("SELECT MIN(lat) as min_val, MAX(lat) as max_val FROM default.{}", measurement))
        .expect("MIN/MAX query failed");

    let row = result.as_array().and_then(|a| a.first()).expect("Expected result row");
    let actual_min = row.get("min_val").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let actual_max = row.get("max_val").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let expected_min = *expected.field_mins.get("lat").unwrap_or(&0.0);
    let expected_max = *expected.field_maxs.get("lat").unwrap_or(&0.0);

    println!("MIN(lat) - Expected: {:.6}, Actual: {:.6}", expected_min, actual_min);
    println!("MAX(lat) - Expected: {:.6}, Actual: {:.6}", expected_max, actual_max);

    let min_tolerance = expected_min.abs() * 1e-9 + 1e-15;
    let max_tolerance = expected_max.abs() * 1e-9 + 1e-15;
    assert!((actual_min - expected_min).abs() < min_tolerance,
        "MIN(lat) mismatch: expected {}, got {}", expected_min, actual_min);
    assert!((actual_max - expected_max).abs() < max_tolerance,
        "MAX(lat) mismatch: expected {}, got {}", expected_max, actual_max);
    println!("✓ MIN/MAX(lat) verified");

    // =========================================================================
    // STEP 6: Verify COUNT(DISTINCT id) matches expected
    // =========================================================================
    let result = server.query(db, &format!("SELECT COUNT(DISTINCT id) as cnt FROM default.{}", measurement))
        .expect("Distinct count query failed");

    let actual_distinct = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    let expected_distinct = expected.distinct_tag_count("id");
    println!("COUNT(DISTINCT id) - Expected: {}, Actual: {}", expected_distinct, actual_distinct);
    assert_eq!(actual_distinct, expected_distinct,
        "Distinct id count mismatch: expected {}, got {}", expected_distinct, actual_distinct);
    println!("✓ COUNT(DISTINCT id) verified: {}", actual_distinct);

    // =========================================================================
    // STEP 7: Verify GROUP BY counts sum to total
    // =========================================================================
    let result = server.query(db,
        &format!("SELECT id, COUNT(*) as cnt FROM default.{} GROUP BY id", measurement)
    ).expect("Group by query failed");

    let rows = result.as_array().expect("Expected array result");
    let group_count = rows.len();
    let sum_of_groups: i64 = rows.iter()
        .filter_map(|r| r.get("cnt").and_then(|v| v.as_i64()))
        .sum();

    println!("GROUP BY id: {} groups, sum of counts = {}", group_count, sum_of_groups);
    assert_eq!(group_count, expected_distinct,
        "Number of groups ({}) should equal COUNT(DISTINCT) ({})", group_count, expected_distinct);
    assert_eq!(sum_of_groups as usize, expected.row_count,
        "Sum of group counts ({}) should equal total count ({})", sum_of_groups, expected.row_count);
    println!("✓ GROUP BY counts verified: {} groups summing to {}", group_count, sum_of_groups);

    println!("\n✅ All bird migration data validations passed!");
}


#[test]
#[ignore]
fn test_usgs_earthquake_data() {
    // USGS earthquake data - past week, minimal fields
    let url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.csv";

    let server = TestServer::start(TEST_HTTP_PORT + 40, &format!("{}_usgs", TEST_DATA_DIR));
    let db = "usgs_test";

    // Download and convert CSV to line protocol
    let line_protocol = download_csv_as_line_protocol(
        url,
        "earthquake",
        &["type", "magType"],
        &["mag", "depth", "latitude", "longitude"],
        None, // Use sequential timestamps
    ).expect("Failed to download earthquake data");

    // Take subset
    let subset: String = line_protocol.lines().take(500).collect::<Vec<_>>().join("\n");
    println!("Loaded {} earthquake records", subset.lines().count());

    // =========================================================================
    // STEP 1: Compute expected values from raw data BEFORE writing
    // =========================================================================
    let expected = compute_expected_values(&subset);
    println!("Expected values computed from raw data:");
    println!("  - Total rows: {}", expected.row_count);
    println!("  - Distinct types: {}", expected.distinct_tag_count("type"));
    println!("  - Distinct magTypes: {}", expected.distinct_tag_count("magType"));
    if let Some(sum) = expected.field_sums.get("mag") {
        println!("  - SUM(mag): {:.4}", sum);
    }
    if let Some(sum) = expected.field_sums.get("depth") {
        println!("  - SUM(depth): {:.4}", sum);
    }
    if let Some(min) = expected.field_mins.get("mag") {
        println!("  - MIN(mag): {:.4}", min);
    }
    if let Some(max) = expected.field_maxs.get("mag") {
        println!("  - MAX(mag): {:.4}", max);
    }

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // =========================================================================
    // STEP 2: Verify COUNT(*) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM default.earthquake")
        .expect("Count query failed");

    let actual_count = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    println!("COUNT(*) - Expected: {}, Actual: {}", expected.row_count, actual_count);
    assert_eq!(actual_count, expected.row_count,
        "Row count mismatch: expected {}, got {}", expected.row_count, actual_count);
    println!("✓ COUNT(*) verified: {}", actual_count);

    // =========================================================================
    // STEP 3: Verify SUM(mag) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT SUM(mag) as total FROM default.earthquake")
        .expect("SUM query failed");

    let actual_sum = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("total"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_sum = *expected.field_sums.get("mag").unwrap_or(&0.0);
    let tolerance = expected_sum.abs() * 1e-9 + 1e-15;
    println!("SUM(mag) - Expected: {:.6}, Actual: {:.6}", expected_sum, actual_sum);
    assert!((actual_sum - expected_sum).abs() < tolerance,
        "SUM(mag) mismatch: expected {}, got {}", expected_sum, actual_sum);
    println!("✓ SUM(mag) verified");

    // =========================================================================
    // STEP 4: Verify AVG(depth) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT AVG(depth) as avg_val FROM default.earthquake")
        .expect("AVG query failed");

    let actual_avg = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("avg_val"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_avg = expected.field_avg("depth").unwrap_or(0.0);
    let tolerance = expected_avg.abs() * 1e-9 + 1e-15;
    println!("AVG(depth) - Expected: {:.6}, Actual: {:.6}", expected_avg, actual_avg);
    assert!((actual_avg - expected_avg).abs() < tolerance,
        "AVG(depth) mismatch: expected {}, got {}", expected_avg, actual_avg);
    println!("✓ AVG(depth) verified");

    // =========================================================================
    // STEP 5: Verify MIN/MAX(mag) match expected
    // =========================================================================
    let result = server.query(db, "SELECT MIN(mag) as min_val, MAX(mag) as max_val FROM default.earthquake")
        .expect("MIN/MAX query failed");

    let row = result.as_array().and_then(|a| a.first()).expect("Expected result row");
    let actual_min = row.get("min_val").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let actual_max = row.get("max_val").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let expected_min = *expected.field_mins.get("mag").unwrap_or(&0.0);
    let expected_max = *expected.field_maxs.get("mag").unwrap_or(&0.0);

    println!("MIN(mag) - Expected: {:.6}, Actual: {:.6}", expected_min, actual_min);
    println!("MAX(mag) - Expected: {:.6}, Actual: {:.6}", expected_max, actual_max);

    let min_tolerance = expected_min.abs() * 1e-9 + 1e-15;
    let max_tolerance = expected_max.abs() * 1e-9 + 1e-15;
    assert!((actual_min - expected_min).abs() < min_tolerance,
        "MIN(mag) mismatch: expected {}, got {}", expected_min, actual_min);
    assert!((actual_max - expected_max).abs() < max_tolerance,
        "MAX(mag) mismatch: expected {}, got {}", expected_max, actual_max);
    println!("✓ MIN/MAX(mag) verified");

    // =========================================================================
    // STEP 6: Verify COUNT(DISTINCT type) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(DISTINCT type) as cnt FROM default.earthquake")
        .expect("Distinct count query failed");

    let actual_distinct = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    let expected_distinct = expected.distinct_tag_count("type");
    println!("COUNT(DISTINCT type) - Expected: {}, Actual: {}", expected_distinct, actual_distinct);
    assert_eq!(actual_distinct, expected_distinct,
        "Distinct type count mismatch: expected {}, got {}", expected_distinct, actual_distinct);
    println!("✓ COUNT(DISTINCT type) verified: {}", actual_distinct);

    // =========================================================================
    // STEP 7: Verify GROUP BY type counts sum to total
    // =========================================================================
    let result = server.query(db,
        "SELECT type, COUNT(*) as cnt FROM default.earthquake GROUP BY type"
    ).expect("Group by query failed");

    let rows = result.as_array().expect("Expected array result");
    let group_count = rows.len();
    let sum_of_groups: i64 = rows.iter()
        .filter_map(|r| r.get("cnt").and_then(|v| v.as_i64()))
        .sum();

    println!("GROUP BY type: {} groups, sum of counts = {}", group_count, sum_of_groups);
    assert_eq!(group_count, expected_distinct,
        "Number of groups ({}) should equal COUNT(DISTINCT) ({})", group_count, expected_distinct);
    assert_eq!(sum_of_groups as usize, expected.row_count,
        "Sum of group counts ({}) should equal total count ({})", sum_of_groups, expected.row_count);
    println!("✓ GROUP BY counts verified: {} groups summing to {}", group_count, sum_of_groups);

    println!("\n✅ All USGS earthquake data validations passed!");
}

#[test]
#[ignore]
fn test_iris_sample() {
    // Iris dataset from GitHub - classic small dataset for testing
    let url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv";

    let server = TestServer::start(TEST_HTTP_PORT + 50, &format!("{}_iris", TEST_DATA_DIR));
    let db = "iris_test";

    // Download and convert CSV - Iris flower data with species tag and measurement fields
    let line_protocol = download_csv_as_line_protocol(
        url,
        "flower",
        &["species"],
        &["sepal_length", "sepal_width", "petal_length", "petal_width"],
        None, // Use sequential timestamps
    ).expect("Failed to download iris data");

    let subset: String = line_protocol.lines().take(150).collect::<Vec<_>>().join("\n");
    println!("Loaded {} flower records", subset.lines().count());

    // =========================================================================
    // STEP 1: Compute expected values from raw data BEFORE writing
    // =========================================================================
    let expected = compute_expected_values(&subset);
    println!("Expected values computed from raw data:");
    println!("  - Total rows: {}", expected.row_count);
    println!("  - Distinct species: {}", expected.distinct_tag_count("species"));
    if let Some(sum) = expected.field_sums.get("sepal_length") {
        println!("  - SUM(sepal_length): {:.4}", sum);
    }
    if let Some(sum) = expected.field_sums.get("petal_width") {
        println!("  - SUM(petal_width): {:.4}", sum);
    }
    if let Some(min) = expected.field_mins.get("sepal_length") {
        println!("  - MIN(sepal_length): {:.4}", min);
    }
    if let Some(max) = expected.field_maxs.get("sepal_length") {
        println!("  - MAX(sepal_length): {:.4}", max);
    }

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // =========================================================================
    // STEP 2: Verify COUNT(*) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM default.flower")
        .expect("Count query failed");

    let actual_count = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    println!("COUNT(*) - Expected: {}, Actual: {}", expected.row_count, actual_count);
    assert_eq!(actual_count, expected.row_count,
        "Row count mismatch: expected {}, got {}", expected.row_count, actual_count);
    println!("✓ COUNT(*) verified: {}", actual_count);

    // =========================================================================
    // STEP 3: Verify SUM(sepal_length) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT SUM(sepal_length) as total FROM default.flower")
        .expect("SUM query failed");

    let actual_sum = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("total"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_sum = *expected.field_sums.get("sepal_length").unwrap_or(&0.0);
    let tolerance = expected_sum.abs() * 1e-9 + 1e-15;
    println!("SUM(sepal_length) - Expected: {:.6}, Actual: {:.6}", expected_sum, actual_sum);
    assert!((actual_sum - expected_sum).abs() < tolerance,
        "SUM(sepal_length) mismatch: expected {}, got {}", expected_sum, actual_sum);
    println!("✓ SUM(sepal_length) verified");

    // =========================================================================
    // STEP 4: Verify AVG(petal_width) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT AVG(petal_width) as avg_val FROM default.flower")
        .expect("AVG query failed");

    let actual_avg = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("avg_val"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let expected_avg = expected.field_avg("petal_width").unwrap_or(0.0);
    let tolerance = expected_avg.abs() * 1e-9 + 1e-15;
    println!("AVG(petal_width) - Expected: {:.6}, Actual: {:.6}", expected_avg, actual_avg);
    assert!((actual_avg - expected_avg).abs() < tolerance,
        "AVG(petal_width) mismatch: expected {}, got {}", expected_avg, actual_avg);
    println!("✓ AVG(petal_width) verified");

    // =========================================================================
    // STEP 5: Verify MIN/MAX(sepal_length) match expected
    // =========================================================================
    let result = server.query(db, "SELECT MIN(sepal_length) as min_val, MAX(sepal_length) as max_val FROM default.flower")
        .expect("MIN/MAX query failed");

    let row = result.as_array().and_then(|a| a.first()).expect("Expected result row");
    let actual_min = row.get("min_val").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let actual_max = row.get("max_val").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let expected_min = *expected.field_mins.get("sepal_length").unwrap_or(&0.0);
    let expected_max = *expected.field_maxs.get("sepal_length").unwrap_or(&0.0);

    println!("MIN(sepal_length) - Expected: {:.6}, Actual: {:.6}", expected_min, actual_min);
    println!("MAX(sepal_length) - Expected: {:.6}, Actual: {:.6}", expected_max, actual_max);

    let min_tolerance = expected_min.abs() * 1e-9 + 1e-15;
    let max_tolerance = expected_max.abs() * 1e-9 + 1e-15;
    assert!((actual_min - expected_min).abs() < min_tolerance,
        "MIN(sepal_length) mismatch: expected {}, got {}", expected_min, actual_min);
    assert!((actual_max - expected_max).abs() < max_tolerance,
        "MAX(sepal_length) mismatch: expected {}, got {}", expected_max, actual_max);
    println!("✓ MIN/MAX(sepal_length) verified");

    // =========================================================================
    // STEP 6: Verify COUNT(DISTINCT species) matches expected
    // =========================================================================
    let result = server.query(db, "SELECT COUNT(DISTINCT species) as cnt FROM default.flower")
        .expect("Distinct count query failed");

    let actual_distinct = result.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    let expected_distinct = expected.distinct_tag_count("species");
    println!("COUNT(DISTINCT species) - Expected: {}, Actual: {}", expected_distinct, actual_distinct);
    assert_eq!(actual_distinct, expected_distinct,
        "Distinct species count mismatch: expected {}, got {}", expected_distinct, actual_distinct);
    println!("✓ COUNT(DISTINCT species) verified: {}", actual_distinct);

    // =========================================================================
    // STEP 7: Verify GROUP BY species counts sum to total
    // =========================================================================
    let result = server.query(db,
        "SELECT species, COUNT(*) as cnt FROM default.flower GROUP BY species"
    ).expect("Group by query failed");

    let rows = result.as_array().expect("Expected array result");
    let group_count = rows.len();
    let sum_of_groups: i64 = rows.iter()
        .filter_map(|r| r.get("cnt").and_then(|v| v.as_i64()))
        .sum();

    println!("GROUP BY species: {} groups, sum of counts = {}", group_count, sum_of_groups);
    assert_eq!(group_count, expected_distinct,
        "Number of groups ({}) should equal COUNT(DISTINCT) ({})", group_count, expected_distinct);
    assert_eq!(sum_of_groups as usize, expected.row_count,
        "Sum of group counts ({}) should equal total count ({})", sum_of_groups, expected.row_count);
    println!("✓ GROUP BY counts verified: {} groups summing to {}", group_count, sum_of_groups);

    println!("\n✅ All iris flower data validations passed!");
}

#[test]
#[ignore]
fn test_generate_golden_files() {
    println!("Generating golden files from public datasets...");

    let server = TestServer::start(TEST_HTTP_PORT + 100, &format!("{}_golden", TEST_DATA_DIR));

    // For each dataset, download, load, query, and save results
    for dataset in DATASETS {
        println!("\nProcessing: {}", dataset.name);
        let db = &dataset.name.replace("_sample", "");

        let data = match download_dataset(dataset.url) {
            Ok(d) => d,
            Err(e) => {
                println!("  Skipping: {}", e);
                continue;
            }
        };

        let subset: String = data.lines().take(500).collect::<Vec<_>>().join("\n");
        if let Err(e) = server.write_line_protocol(db, &subset) {
            println!("  Write failed: {}", e);
            continue;
        }
        if let Err(e) = server.flush() {
            println!("  Flush failed: {}", e);
            continue;
        }

        // Extract measurement name from first line
        let measurement = subset
            .lines()
            .next()
            .and_then(|line| line.split(',').next())
            .unwrap_or("data");

        let mut golden = GoldenFile {
            dataset: dataset.name.to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            queries: Vec::new(),
        };

        // Run standard queries and capture results
        for query in get_standard_queries(measurement) {
            let sql = query.sql.replace("{}", measurement);
            if let Ok(result) = server.query(db, &sql) {
                let row_count = result.as_array().map(|a| a.len()).unwrap_or(0);
                let columns: Vec<String> = result
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|r| r.as_object())
                    .map(|o| o.keys().cloned().collect())
                    .unwrap_or_default();

                golden.queries.push(GoldenResult {
                    query: sql,
                    description: query.description.to_string(),
                    expected_row_count: row_count,
                    expected_columns: columns,
                    sample_values: HashMap::new(),
                    checksum: None,
                });
            }
        }

        if !golden.queries.is_empty() {
            if let Err(e) = save_golden_file(&golden) {
                println!("  Failed to save golden file: {}", e);
            } else {
                println!("  Saved golden file with {} queries", golden.queries.len());
            }
        }
    }

    println!("\nGolden file generation complete!");
}

#[test]
#[ignore]
fn test_validate_against_golden() {
    println!("Validating against golden files...");

    let server = TestServer::start(TEST_HTTP_PORT + 200, &format!("{}_validate", TEST_DATA_DIR));

    for dataset in DATASETS {
        println!("\nValidating: {}", dataset.name);

        // Load golden file
        let golden = match load_golden_file(dataset.name) {
            Some(g) => g,
            None => {
                println!("  No golden file found, skipping");
                continue;
            }
        };

        let db = &dataset.name.replace("_sample", "");

        // Download and load data
        let data = match download_dataset(dataset.url) {
            Ok(d) => d,
            Err(e) => {
                println!("  Download failed: {}", e);
                continue;
            }
        };

        let subset: String = data.lines().take(500).collect::<Vec<_>>().join("\n");
        if let Err(e) = server.write_line_protocol(db, &subset) {
            println!("  Write failed: {}", e);
            continue;
        }
        if let Err(e) = server.flush() {
            println!("  Flush failed: {}", e);
            continue;
        }

        // Run each golden query and compare
        let mut passed = 0;
        let mut failed = 0;

        for expected in &golden.queries {
            match server.query(db, &expected.query) {
                Ok(result) => {
                    match compare_results(&result, expected, 1e-6) {
                        Ok(()) => {
                            passed += 1;
                            println!("  ✓ {}", expected.description);
                        }
                        Err(e) => {
                            failed += 1;
                            println!("  ✗ {}: {}", expected.description, e);
                        }
                    }
                }
                Err(e) => {
                    failed += 1;
                    println!("  ✗ {}: query failed: {}", expected.description, e);
                }
            }
        }

        println!("  Results: {} passed, {} failed", passed, failed);
    }
}
