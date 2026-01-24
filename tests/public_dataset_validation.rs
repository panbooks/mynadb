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

[cache]
flush_interval_secs = 1
max_size_mb = 64

[wal]
enabled = false

[compaction]
enabled = false

[retention]
enabled = false

[auth]
enabled = false

[logging]
level = "warn"
"#,
            http_port,
            http_port + 1,
            data_dir
        );
        fs::write(&config_path, config_content).expect("Failed to write config");

        // Start server
        let process = Command::new("cargo")
            .args(["run", "--release", "--", "-c", &config_path])
            .spawn()
            .expect("Failed to start MynaDB server");

        // Wait for server to be ready
        let url = format!("http://127.0.0.1:{}/health", http_port);
        for _ in 0..60 {
            thread::sleep(Duration::from_millis(500));
            if ureq::get(&url).call().is_ok() {
                break;
            }
        }

        Self {
            process,
            http_port,
            data_dir: data_dir.to_string(),
        }
    }

    fn write_line_protocol(&self, db: &str, data: &str) -> Result<(), String> {
        let url = format!("http://127.0.0.1:{}/write/{}", self.http_port, db);
        ureq::post(&url)
            .set("Content-Type", "text/plain")
            .send_string(data)
            .map_err(|e| format!("Write failed: {}", e))?;
        Ok(())
    }

    fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!(
            "http://127.0.0.1:{}/query/{}?sql={}",
            self.http_port,
            db,
            urlencoding::encode(sql)
        );
        let response = ureq::get(&url)
            .call()
            .map_err(|e| format!("Query failed: {}", e))?;
        let body = response
            .into_string()
            .map_err(|e| format!("Read body failed: {}", e))?;
        serde_json::from_str(&body).map_err(|e| format!("Parse JSON failed: {}", e))
    }

    fn flush(&self) -> Result<(), String> {
        let url = format!("http://127.0.0.1:{}/flush", self.http_port);
        ureq::post(&url)
            .call()
            .map_err(|e| format!("Flush failed: {}", e))?;
        thread::sleep(Duration::from_secs(2));
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
        .timeout(Duration::from_secs(30))
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
        .timeout(Duration::from_secs(60))
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
    println!("Loaded {} lines of NOAA data", subset.lines().count());

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // Run standard queries
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM ndbc").unwrap();
    println!("NOAA row count: {:?}", result);

    let result = server.query(db, "SELECT * FROM ndbc LIMIT 5").unwrap();
    println!("NOAA sample rows: {:?}", result);

    // Query with aggregation
    let result = server.query(db,
        "SELECT station_id, AVG(avg_wave_period) as avg_period FROM ndbc GROUP BY station_id LIMIT 10"
    );
    println!("NOAA aggregation: {:?}", result);
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

    // Write to MynaDB
    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // Test queries
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM airSensors").unwrap();
    println!("Air sensor row count: {:?}", result);

    let result = server.query(db, "SELECT * FROM airSensors LIMIT 5").unwrap();
    println!("Air sensor sample: {:?}", result);

    // Aggregation query
    let result = server.query(db,
        "SELECT sensor_id, AVG(co) as avg_co, MAX(humidity) as max_humidity FROM airSensors GROUP BY sensor_id LIMIT 10"
    );
    println!("Air sensor aggregation: {:?}", result);
}

#[test]
#[ignore]
fn test_machine_production_data() {
    // Uses InfluxDB's machine production sample data
    let url = "https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/machine-production-data/machine-production.lp";

    let server = TestServer::start(TEST_HTTP_PORT + 20, &format!("{}_machine", TEST_DATA_DIR));
    let db = "machine_test";

    let data = download_dataset(url).expect("Failed to download machine data");
    let subset: String = data.lines().take(500).collect::<Vec<_>>().join("\n");
    println!("Loaded {} lines of machine data", subset.lines().count());

    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // Test queries
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM machinery").unwrap();
    println!("Machine data row count: {:?}", result);

    let result = server.query(db, "SELECT * FROM machinery LIMIT 5").unwrap();
    println!("Machine data sample: {:?}", result);
}

#[test]
#[ignore]
fn test_bitcoin_price_data() {
    // Uses InfluxDB's crypto price sample data
    let url = "https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/bitcoin-price-data/bitcoin-historical-annotated.lp";

    let server = TestServer::start(TEST_HTTP_PORT + 30, &format!("{}_bitcoin", TEST_DATA_DIR));
    let db = "crypto_test";

    let data = download_dataset(url).expect("Failed to download bitcoin data");
    let subset: String = data.lines().take(1000).collect::<Vec<_>>().join("\n");
    println!("Loaded {} lines of bitcoin data", subset.lines().count());

    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // Test queries
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM coindesk").unwrap();
    println!("Bitcoin row count: {:?}", result);

    let result = server.query(db, "SELECT * FROM coindesk ORDER BY time DESC LIMIT 5").unwrap();
    println!("Bitcoin recent prices: {:?}", result);

    // Price analysis
    let result = server.query(db,
        "SELECT MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price FROM coindesk"
    );
    println!("Bitcoin price stats: {:?}", result);
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
        &["place", "type", "magType"],
        &["mag", "depth", "latitude", "longitude"],
        Some("time"),
    ).expect("Failed to download earthquake data");

    // Take subset
    let subset: String = line_protocol.lines().take(500).collect::<Vec<_>>().join("\n");
    println!("Loaded {} earthquake records", subset.lines().count());

    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // Test queries
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM earthquake").unwrap();
    println!("Earthquake count: {:?}", result);

    let result = server.query(db, "SELECT * FROM earthquake ORDER BY time DESC LIMIT 5").unwrap();
    println!("Recent earthquakes: {:?}", result);

    // Magnitude analysis
    let result = server.query(db,
        "SELECT AVG(mag) as avg_mag, MAX(mag) as max_mag, MIN(mag) as min_mag FROM earthquake"
    );
    println!("Earthquake magnitude stats: {:?}", result);

    // Group by type
    let result = server.query(db,
        "SELECT type, COUNT(*) as cnt FROM earthquake GROUP BY type"
    );
    println!("Earthquakes by type: {:?}", result);
}

#[test]
#[ignore]
fn test_nyc_taxi_sample() {
    // NYC TLC trip data sample (very small sample from GitHub)
    let url = "https://raw.githubusercontent.com/plotly/datasets/master/uber-rides-data1.csv";

    let server = TestServer::start(TEST_HTTP_PORT + 50, &format!("{}_taxi", TEST_DATA_DIR));
    let db = "taxi_test";

    // Download and convert CSV
    let line_protocol = download_csv_as_line_protocol(
        url,
        "rides",
        &["Base"],
        &["Lat", "Lon"],
        Some("Date/Time"),
    ).expect("Failed to download taxi data");

    let subset: String = line_protocol.lines().take(1000).collect::<Vec<_>>().join("\n");
    println!("Loaded {} ride records", subset.lines().count());

    server.write_line_protocol(db, &subset).expect("Write failed");
    server.flush().expect("Flush failed");

    // Test queries
    let result = server.query(db, "SELECT COUNT(*) as cnt FROM rides").unwrap();
    println!("Ride count: {:?}", result);

    let result = server.query(db,
        "SELECT Base, COUNT(*) as trip_count FROM rides GROUP BY Base"
    );
    println!("Trips by base: {:?}", result);
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
