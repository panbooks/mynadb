//! Correctness Tests for MynaDB
//!
//! These tests verify that MynaDB produces correct results for various queries.
//! Based on testing strategies from InfluxDB, ClickHouse, and TimescaleDB.
//!
//! Run with: cargo test --test correctness

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

/// Global counter for unique database names
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test harness for running MynaDB integration tests
struct TestServer {
    process: Child,
    port: u16,
    #[allow(dead_code)] // Kept alive to persist temp directory
    data_dir: tempfile::TempDir,
}

impl TestServer {
    /// Start a new MynaDB test server on an available port
    fn start() -> Self {
        let data_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let port = find_available_port();

        // Create minimal config
        let config_path = data_dir.path().join("config.toml");
        let storage_dir = data_dir.path().join("data");
        std::fs::create_dir_all(&storage_dir).expect("Failed to create storage dir");

        // Use minimal valid config - use just the data_dir without escaping issues
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

        // Get the path to the binary - try release first, then debug
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

        // Start server directly and capture output for debugging
        let process = Command::new(&binary_path)
            .args(["-c", config_path.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect(&format!("Failed to start MynaDB from {:?}", binary_path));

        let server = TestServer {
            process,
            port,
            data_dir,
        };

        // Wait for server to be ready
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
        panic!("Server failed to start within 30 seconds");
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Generate a unique database name for test isolation
    fn unique_db(&self) -> String {
        format!("testdb_{}", DB_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    /// Write data using InfluxDB line protocol
    fn write(&self, db: &str, data: &str) -> Result<(), String> {
        let url = format!("{}/write/{}", self.base_url(), db);
        match ureq::post(&url).send_string(data) {
            Ok(resp) => {
                let status = resp.status();
                if status != 200 && status != 204 {
                    return Err(format!("Write failed with status: {}", status));
                }
                Ok(())
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Write failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("Write failed: {}", e)),
        }
    }

    /// Flush data to Parquet
    fn flush(&self) -> Result<(), String> {
        let url = format!("{}/flush", self.base_url());
        match ureq::post(&url).call() {
            Ok(resp) => {
                if resp.status() != 200 {
                    return Err(format!("Flush failed with status: {}", resp.status()));
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                return Err(format!("Flush failed with status {}: {}", code, body));
            }
            Err(e) => return Err(format!("Flush failed: {}", e)),
        }
        // Wait for flush to complete
        thread::sleep(Duration::from_secs(2));
        Ok(())
    }

    /// Execute SQL query and return results as JSON
    fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/query/{}", self.base_url(), db);
        // Use POST with SQL in body (matching the curl commands in test scripts)
        let resp = ureq::post(&url).send_string(sql);

        match resp {
            Ok(resp) => {
                let body = resp
                    .into_string()
                    .map_err(|e| format!("Failed to read response: {}", e))?;
                serde_json::from_str(&body)
                    .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Query failed with status {}: {}", code, body))
            }
            Err(e) => Err(format!("Query failed: {}", e)),
        }
    }

    /// Query and extract a single numeric value
    fn query_scalar(&self, db: &str, sql: &str) -> Result<f64, String> {
        let result = self.query(db, sql)?;
        extract_scalar(&result)
    }

    /// Query and extract count as integer
    fn query_count(&self, db: &str, sql: &str) -> Result<i64, String> {
        let result = self.query(db, sql)?;
        extract_count(&result)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Kill the server process
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

/// Extract a scalar numeric value from query result
/// Handles format: [{"column_name": value}] or {"results": [[value]]}
fn extract_scalar(result: &serde_json::Value) -> Result<f64, String> {
    // Try format: [{"column_name": value}]
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                // Get the first value from the object
                if let Some((_, val)) = obj.iter().next() {
                    if let Some(f) = val.as_f64() {
                        return Ok(f);
                    }
                    if let Some(i) = val.as_i64() {
                        return Ok(i as f64);
                    }
                }
            }
            // Try as direct array: [[value]]
            if let Some(row) = first.as_array() {
                if let Some(val) = row.first() {
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

    // Try format: {"results": [[value]]}
    if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
        if let Some(first) = results.first() {
            if let Some(row) = first.as_array() {
                if let Some(val) = row.first() {
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

    Err(format!("Failed to extract scalar from: {:?}", result))
}

/// Extract count value from query result
fn extract_count(result: &serde_json::Value) -> Result<i64, String> {
    // Try format: [{"column_name": value}]
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
            if let Some(row) = first.as_array() {
                if let Some(val) = row.first() {
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

    // Try format: {"results": [[value]]}
    if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
        if let Some(first) = results.first() {
            if let Some(row) = first.as_array() {
                if let Some(val) = row.first() {
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

    Err(format!("Failed to extract count from: {:?}", result))
}

// ============================================================================
// Aggregation Correctness Tests
// ============================================================================

#[test]
#[ignore] // Run with: cargo test --test correctness -- --ignored
fn test_sum_correctness() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Insert known values: 1 + 2 + 3 + 4 + 5 = 15
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=1.0 {}\n\
         cpu value=2.0 {}\n\
         cpu value=3.0 {}\n\
         cpu value=4.0 {}\n\
         cpu value=5.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000,
        now + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");

    // Wait for data to be processed
    thread::sleep(Duration::from_secs(1));

    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");

    assert!(
        (sum - 15.0).abs() < 0.0001,
        "SUM should be 15.0, got {}",
        sum
    );
}

#[test]
#[ignore]
fn test_avg_correctness() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Insert: 10, 20, 30 -> AVG = 20
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=10.0 {}\n\
         cpu value=20.0 {}\n\
         cpu value=30.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let avg = server
        .query_scalar(&db, "SELECT AVG(value) FROM cpu")
        .expect("Query failed");

    assert!(
        (avg - 20.0).abs() < 0.0001,
        "AVG should be 20.0, got {}",
        avg
    );
}

#[test]
#[ignore]
fn test_count_correctness() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Insert exactly 7 rows
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let mut data = String::new();
    for i in 0..7 {
        data.push_str(&format!("cpu value={}.0 {}\n", i, now + i * 1_000_000_000));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu")
        .expect("Query failed");

    assert_eq!(count, 7, "COUNT should be 7, got {}", count);
}

#[test]
#[ignore]
fn test_min_max_correctness() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Insert: 5, 2, 8, 1, 9 -> MIN=1, MAX=9
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=5.0 {}\n\
         cpu value=2.0 {}\n\
         cpu value=8.0 {}\n\
         cpu value=1.0 {}\n\
         cpu value=9.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000,
        now + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let min = server
        .query_scalar(&db, "SELECT MIN(value) FROM cpu")
        .expect("MIN query failed");
    let max = server
        .query_scalar(&db, "SELECT MAX(value) FROM cpu")
        .expect("MAX query failed");

    assert!(
        (min - 1.0).abs() < 0.0001,
        "MIN should be 1.0, got {}",
        min
    );
    assert!(
        (max - 9.0).abs() < 0.0001,
        "MAX should be 9.0, got {}",
        max
    );
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
#[ignore]
fn test_single_row_aggregation() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Single row: SUM, AVG, MIN, MAX should all equal the value
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    server
        .write(&db, &format!("cpu value=42.0 {}", now))
        .expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    let avg = server.query_scalar(&db, "SELECT AVG(value) FROM cpu").unwrap();
    let min = server.query_scalar(&db, "SELECT MIN(value) FROM cpu").unwrap();
    let max = server.query_scalar(&db, "SELECT MAX(value) FROM cpu").unwrap();

    assert!((sum - 42.0).abs() < 0.0001, "SUM of single row failed");
    assert!((avg - 42.0).abs() < 0.0001, "AVG of single row failed");
    assert!((min - 42.0).abs() < 0.0001, "MIN of single row failed");
    assert!((max - 42.0).abs() < 0.0001, "MAX of single row failed");
}

#[test]
#[ignore]
fn test_empty_result_count() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write some data
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    server
        .write(&db, &format!("cpu,host=a value=1.0 {}", now))
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Query with impossible filter
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE host = 'nonexistent'")
        .expect("Query failed");

    assert_eq!(count, 0, "COUNT of empty result should be 0");
}

#[test]
#[ignore]
fn test_duplicate_timestamps() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write multiple values at same timestamp
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=a value=1.0 {}\n\
         cpu,host=b value=2.0 {}\n\
         cpu,host=c value=3.0 {}",
        now, now, now
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // All 3 rows should be preserved (different tags)
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu")
        .expect("Query failed");
    assert_eq!(count, 3, "Should preserve all rows with same timestamp");

    // SUM should include all values
    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");
    assert!((sum - 6.0).abs() < 0.0001, "SUM should be 6.0");
}

#[test]
#[ignore]
fn test_out_of_order_writes() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write data out of chronological order
    let base = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=3.0 {}\n\
         cpu value=1.0 {}\n\
         cpu value=2.0 {}",
        base + 2_000_000_000, // t3
        base,                  // t1
        base + 1_000_000_000   // t2
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // All data should be preserved
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu")
        .expect("Query failed");
    assert_eq!(count, 3, "Out of order writes should be preserved");

    // Aggregations should be correct
    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");
    assert!((sum - 6.0).abs() < 0.0001, "SUM should be 6.0");
}

#[test]
#[ignore]
fn test_large_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Test with large numbers
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let large = 1e15_f64;
    let data = format!(
        "cpu value={} {}\n\
         cpu value={} {}",
        large,
        now,
        large,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");
    let expected = 2.0 * large;
    let tolerance = expected * 1e-10;
    assert!(
        (sum - expected).abs() < tolerance,
        "Large value SUM failed: expected {}, got {}",
        expected,
        sum
    );
}

#[test]
#[ignore]
fn test_small_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Test with very small numbers
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let small = 1e-15_f64;
    let data = format!(
        "cpu value={} {}\n\
         cpu value={} {}",
        small,
        now,
        small,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");
    let expected = 2.0 * small;
    assert!(
        (sum - expected).abs() < 1e-20,
        "Small value SUM failed: expected {}, got {}",
        expected,
        sum
    );
}

#[test]
#[ignore]
fn test_negative_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Test with negative numbers
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=-10.0 {}\n\
         cpu value=5.0 {}\n\
         cpu value=-3.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");
    assert!(
        (sum - (-8.0)).abs() < 0.0001,
        "Negative value SUM failed: expected -8.0, got {}",
        sum
    );

    let min = server
        .query_scalar(&db, "SELECT MIN(value) FROM cpu")
        .expect("Query failed");
    assert!(
        (min - (-10.0)).abs() < 0.0001,
        "MIN should be -10.0, got {}",
        min
    );
}

// ============================================================================
// Data Integrity Tests
// ============================================================================

#[test]
#[ignore]
fn test_write_read_roundtrip() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write 100 data points with known values
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let mut data = String::new();
    let mut expected_sum = 0.0;
    for i in 0..100 {
        let value = (i as f64) * 1.5;
        expected_sum += value;
        data.push_str(&format!("cpu value={} {}\n", value, now + i * 1_000_000_000));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Verify count
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu")
        .expect("Query failed");
    assert_eq!(count, 100, "Should have 100 rows");

    // Verify sum
    let sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("Query failed");
    assert!(
        (sum - expected_sum).abs() < 0.01,
        "SUM mismatch: expected {}, got {}",
        expected_sum,
        sum
    );
}

#[test]
#[ignore]
fn test_multiple_measurements() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=10.0 {}\n\
         memory value=20.0 {}\n\
         disk value=30.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Each measurement should be in its own table
    let cpu_sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu")
        .expect("cpu query failed");
    let mem_sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM memory")
        .expect("memory query failed");
    let disk_sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM disk")
        .expect("disk query failed");

    assert!((cpu_sum - 10.0).abs() < 0.0001);
    assert!((mem_sum - 20.0).abs() < 0.0001);
    assert!((disk_sum - 30.0).abs() < 0.0001);
}

#[test]
#[ignore]
fn test_tag_filtering() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,region=us-west value=10.0 {}\n\
         cpu,region=us-east value=20.0 {}\n\
         cpu,region=us-west value=30.0 {}\n\
         cpu,region=eu-west value=40.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Filter by tag
    let us_west_sum = server
        .query_scalar(&db, "SELECT SUM(value) FROM cpu WHERE region = 'us-west'")
        .expect("Query failed");
    assert!(
        (us_west_sum - 40.0).abs() < 0.0001,
        "us-west SUM should be 40.0"
    );

    let us_east_count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE region = 'us-east'")
        .expect("Query failed");
    assert_eq!(us_east_count, 1);
}

// ============================================================================
// Group By Tests
// ============================================================================

#[test]
#[ignore]
fn test_group_by_tag() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=a value=10.0 {}\n\
         cpu,host=b value=20.0 {}\n\
         cpu,host=a value=30.0 {}\n\
         cpu,host=b value=40.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    thread::sleep(Duration::from_secs(1));

    // Query with GROUP BY
    let result = server
        .query(&db, "SELECT host, SUM(value) FROM cpu GROUP BY host ORDER BY host")
        .expect("Query failed");

    // Result format: [{"host": "a", "sum(...)": 40.0}, {"host": "b", "sum(...)": 60.0}]
    let results = result.as_array().expect("Expected array result");
    assert_eq!(results.len(), 2, "Should have 2 groups");

    // Host 'a': 10 + 30 = 40
    let row_a = results[0].as_object().expect("Expected object");
    let host_a = row_a.get("host").and_then(|v| v.as_str()).expect("Expected host");
    assert_eq!(host_a, "a");

    // Find the SUM column (name may vary)
    let sum_a: f64 = row_a
        .iter()
        .filter(|(k, _)| k.contains("sum") || k.contains("SUM"))
        .map(|(_, v)| v.as_f64().unwrap_or(0.0))
        .next()
        .expect("Expected sum value");
    assert!((sum_a - 40.0).abs() < 0.0001, "Host a sum should be 40.0, got {}", sum_a);

    // Host 'b': 20 + 40 = 60
    let row_b = results[1].as_object().expect("Expected object");
    let host_b = row_b.get("host").and_then(|v| v.as_str()).expect("Expected host");
    assert_eq!(host_b, "b");

    let sum_b: f64 = row_b
        .iter()
        .filter(|(k, _)| k.contains("sum") || k.contains("SUM"))
        .map(|(_, v)| v.as_f64().unwrap_or(0.0))
        .next()
        .expect("Expected sum value");
    assert!((sum_b - 60.0).abs() < 0.0001, "Host b sum should be 60.0, got {}", sum_b);
}

// ============================================================================
// Floating Point Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_zero_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=0.0 {}\n\
         cpu value=0.0 {}\n\
         cpu value=0.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    let avg = server.query_scalar(&db, "SELECT AVG(value) FROM cpu").unwrap();
    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();

    assert!((sum - 0.0).abs() < 0.0001, "SUM of zeros should be 0");
    assert!((avg - 0.0).abs() < 0.0001, "AVG of zeros should be 0");
    assert_eq!(count, 3, "COUNT should be 3");
}

#[test]
#[ignore]
fn test_mixed_positive_negative_cancel() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Values that sum to exactly zero
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=100.0 {}\n\
         cpu value=-50.0 {}\n\
         cpu value=-50.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    assert!(
        sum.abs() < 0.0001,
        "SUM should be 0 (100 - 50 - 50), got {}",
        sum
    );
}

#[test]
#[ignore]
fn test_precision_decimals() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Test decimal precision
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=0.1 {}\n\
         cpu value=0.2 {}",
        now,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    // 0.1 + 0.2 should be close to 0.3 (floating point)
    assert!(
        (sum - 0.3).abs() < 0.0001,
        "SUM should be ~0.3, got {}",
        sum
    );
}

#[test]
#[ignore]
fn test_scientific_notation_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=1.5e10 {}\n\
         cpu value=2.5e10 {}",
        now,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    let expected = 4.0e10;
    let tolerance = expected * 1e-10;
    assert!(
        (sum - expected).abs() < tolerance,
        "Scientific notation SUM failed: expected {}, got {}",
        expected,
        sum
    );
}

// ============================================================================
// Integer Field Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_integer_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    // Integer fields use 'i' suffix in line protocol
    let data = format!(
        "cpu count=100i {}\n\
         cpu count=200i {}\n\
         cpu count=300i {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(count) FROM cpu").unwrap();
    assert!(
        (sum - 600.0).abs() < 0.0001,
        "Integer SUM should be 600, got {}",
        sum
    );
}

#[test]
#[ignore]
fn test_large_integer_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let large: i64 = 1_000_000_000_000; // 1 trillion
    let data = format!(
        "cpu count={}i {}\n\
         cpu count={}i {}",
        large,
        now,
        large,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(count) FROM cpu").unwrap();
    let expected = (2 * large) as f64;
    let tolerance = expected * 1e-10;
    assert!(
        (sum - expected).abs() < tolerance,
        "Large integer SUM failed: expected {}, got {}",
        expected,
        sum
    );
}

#[test]
#[ignore]
fn test_negative_integers() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu count=-100i {}\n\
         cpu count=50i {}\n\
         cpu count=-25i {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(count) FROM cpu").unwrap();
    assert!(
        (sum - (-75.0)).abs() < 0.0001,
        "Negative integer SUM should be -75, got {}",
        sum
    );
}

// ============================================================================
// Boolean Field Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_boolean_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=a active=true {}\n\
         cpu,host=b active=false {}\n\
         cpu,host=c active=true {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Count all
    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 3, "Should have 3 rows");

    // Filter by boolean
    let true_count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE active = true")
        .unwrap();
    assert_eq!(true_count, 2, "Should have 2 active=true rows");
}

// ============================================================================
// String Field Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_string_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "logs,host=a message=\"hello\" {}\n\
         logs,host=b message=\"world\" {}",
        now,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM logs").unwrap();
    assert_eq!(count, 2, "Should have 2 rows");

    // Filter by string
    let hello_count = server
        .query_count(&db, "SELECT COUNT(*) FROM logs WHERE message = 'hello'")
        .unwrap();
    assert_eq!(hello_count, 1, "Should have 1 message='hello' row");
}

// ============================================================================
// Mixed Field Types
// ============================================================================

#[test]
#[ignore]
fn test_multiple_field_types() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    // Line with float, integer, boolean, and string fields
    let data = format!(
        "system,host=server01 cpu=75.5,memory=8192i,active=true,status=\"running\" {}",
        now
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Query each field type
    let cpu = server.query_scalar(&db, "SELECT cpu FROM system").unwrap();
    assert!((cpu - 75.5).abs() < 0.0001, "CPU should be 75.5");

    let memory = server.query_scalar(&db, "SELECT memory FROM system").unwrap();
    assert!((memory - 8192.0).abs() < 0.0001, "Memory should be 8192");
}

// ============================================================================
// Tag Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_many_tags() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    // 10 tags
    let data = format!(
        "cpu,t1=v1,t2=v2,t3=v3,t4=v4,t5=v5,t6=v6,t7=v7,t8=v8,t9=v9,t10=v10 value=1.0 {}",
        now
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 1, "Should have 1 row");

    // Filter by various tags
    let filtered = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE t5 = 'v5'")
        .unwrap();
    assert_eq!(filtered, 1, "Tag filter should work");
}

#[test]
#[ignore]
fn test_numeric_tag_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=123,port=8080 value=1.0 {}\n\
         cpu,host=456,port=8081 value=2.0 {}",
        now,
        now + 1_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Tags are strings, even if they look like numbers
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE host = '123'")
        .unwrap();
    assert_eq!(count, 1, "Numeric tag filter should work");
}

#[test]
#[ignore]
fn test_hyphen_in_tag_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=server-01,region=us-west-2 value=1.0 {}",
        now
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE host = 'server-01'")
        .unwrap();
    assert_eq!(count, 1, "Hyphenated tag filter should work");

    let count2 = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE region = 'us-west-2'")
        .unwrap();
    assert_eq!(count2, 1, "Multi-hyphen tag filter should work");
}

// ============================================================================
// Timestamp Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_epoch_timestamp() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Unix epoch: January 1, 1970
    let data = "cpu value=1.0 0";

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 1, "Epoch timestamp should be accepted");
}

#[test]
#[ignore]
fn test_future_timestamp() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Year 2100: 4102444800 seconds since epoch
    let future_ts: i64 = 4102444800_000_000_000;
    let data = format!("cpu value=1.0 {}", future_ts);

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 1, "Future timestamp should be accepted");
}

#[test]
#[ignore]
fn test_millisecond_precision_timestamps() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Timestamps 1ms apart
    let base: i64 = 1700000000_000_000_000;
    let data = format!(
        "cpu value=1.0 {}\n\
         cpu value=2.0 {}\n\
         cpu value=3.0 {}",
        base,
        base + 1_000_000, // +1ms
        base + 2_000_000  // +2ms
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 3, "Millisecond-precision timestamps should be distinct");
}

#[test]
#[ignore]
fn test_nanosecond_precision_timestamps() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Timestamps 1 nanosecond apart
    let base: i64 = 1700000000_000_000_000;
    let data = format!(
        "cpu value=1.0 {}\n\
         cpu value=2.0 {}\n\
         cpu value=3.0 {}",
        base,
        base + 1, // +1ns
        base + 2  // +2ns
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 3, "Nanosecond-precision timestamps should be distinct");
}

// ============================================================================
// Query Filter Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_greater_than_filter() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=10.0 {}\n\
         cpu value=20.0 {}\n\
         cpu value=30.0 {}\n\
         cpu value=40.0 {}\n\
         cpu value=50.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000,
        now + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE value > 25")
        .unwrap();
    assert_eq!(count, 3, "Should have 3 values > 25");

    let count_ge = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE value >= 30")
        .unwrap();
    assert_eq!(count_ge, 3, "Should have 3 values >= 30");

    let count_lt = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE value < 25")
        .unwrap();
    assert_eq!(count_lt, 2, "Should have 2 values < 25");
}

#[test]
#[ignore]
fn test_between_filter() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=10.0 {}\n\
         cpu value=20.0 {}\n\
         cpu value=30.0 {}\n\
         cpu value=40.0 {}\n\
         cpu value=50.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000,
        now + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE value BETWEEN 20 AND 40")
        .unwrap();
    assert_eq!(count, 3, "BETWEEN 20 AND 40 should match 3 rows");
}

#[test]
#[ignore]
fn test_in_clause_filter() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,region=us-east value=1.0 {}\n\
         cpu,region=us-west value=2.0 {}\n\
         cpu,region=eu-west value=3.0 {}\n\
         cpu,region=ap-south value=4.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(
            &db,
            "SELECT COUNT(*) FROM cpu WHERE region IN ('us-east', 'us-west')",
        )
        .unwrap();
    assert_eq!(count, 2, "IN clause should match 2 rows");

    let count_not = server
        .query_count(
            &db,
            "SELECT COUNT(*) FROM cpu WHERE region NOT IN ('us-east', 'us-west')",
        )
        .unwrap();
    assert_eq!(count_not, 2, "NOT IN clause should match 2 rows");
}

#[test]
#[ignore]
fn test_like_filter() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=server01 value=1.0 {}\n\
         cpu,host=server02 value=2.0 {}\n\
         cpu,host=server10 value=3.0 {}\n\
         cpu,host=database01 value=4.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Prefix match
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE host LIKE 'server%'")
        .unwrap();
    assert_eq!(count, 3, "LIKE 'server%' should match 3 rows");

    // Single character wildcard
    let count2 = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu WHERE host LIKE 'server0_'")
        .unwrap();
    assert_eq!(count2, 2, "LIKE 'server0_' should match 2 rows");
}

#[test]
#[ignore]
fn test_compound_filter() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,region=us-east,host=a value=10.0 {}\n\
         cpu,region=us-east,host=b value=20.0 {}\n\
         cpu,region=us-west,host=c value=30.0 {}\n\
         cpu,region=us-west,host=d value=40.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // AND filter
    let count = server
        .query_count(
            &db,
            "SELECT COUNT(*) FROM cpu WHERE region = 'us-east' AND value > 15",
        )
        .unwrap();
    assert_eq!(count, 1, "AND filter should match 1 row");

    // OR filter
    let count_or = server
        .query_count(
            &db,
            "SELECT COUNT(*) FROM cpu WHERE region = 'us-east' OR value > 35",
        )
        .unwrap();
    assert_eq!(count_or, 3, "OR filter should match 3 rows");
}

// ============================================================================
// ORDER BY and LIMIT Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_order_by_asc_desc() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu value=30.0 {}\n\
         cpu value=10.0 {}\n\
         cpu value=20.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Test MIN matches ORDER BY ASC LIMIT 1
    let min = server.query_scalar(&db, "SELECT MIN(value) FROM cpu").unwrap();
    let first_asc = server
        .query_scalar(&db, "SELECT value FROM cpu ORDER BY value ASC LIMIT 1")
        .unwrap();
    assert!(
        (min - first_asc).abs() < 0.0001,
        "MIN should equal first value in ASC order"
    );

    // Test MAX matches ORDER BY DESC LIMIT 1
    let max = server.query_scalar(&db, "SELECT MAX(value) FROM cpu").unwrap();
    let first_desc = server
        .query_scalar(&db, "SELECT value FROM cpu ORDER BY value DESC LIMIT 1")
        .unwrap();
    assert!(
        (max - first_desc).abs() < 0.0001,
        "MAX should equal first value in DESC order"
    );
}

#[test]
#[ignore]
fn test_limit_offset() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let mut data = String::new();
    for i in 0..10 {
        data.push_str(&format!("cpu value={}.0 {}\n", i, now + i * 1_000_000_000));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // LIMIT without OFFSET
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM (SELECT * FROM cpu LIMIT 5)")
        .unwrap();
    assert_eq!(count, 5, "LIMIT 5 should return 5 rows");

    // LIMIT 0
    let count_zero = server
        .query_count(&db, "SELECT COUNT(*) FROM (SELECT * FROM cpu LIMIT 0)")
        .unwrap();
    assert_eq!(count_zero, 0, "LIMIT 0 should return 0 rows");
}

// ============================================================================
// DISTINCT Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_distinct_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,region=us-east value=1.0 {}\n\
         cpu,region=us-west value=2.0 {}\n\
         cpu,region=us-east value=3.0 {}\n\
         cpu,region=eu-west value=4.0 {}\n\
         cpu,region=us-west value=5.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000,
        now + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // DISTINCT should return 3 unique regions
    let result = server
        .query(&db, "SELECT DISTINCT region FROM cpu ORDER BY region")
        .unwrap();
    let rows = result.as_array().expect("Expected array");
    assert_eq!(rows.len(), 3, "Should have 3 distinct regions");
}

#[test]
#[ignore]
fn test_count_distinct() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=a value=1.0 {}\n\
         cpu,host=b value=2.0 {}\n\
         cpu,host=a value=3.0 {}\n\
         cpu,host=c value=4.0 {}\n\
         cpu,host=b value=5.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000,
        now + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(DISTINCT host) FROM cpu")
        .unwrap();
    assert_eq!(count, 3, "COUNT(DISTINCT host) should be 3");
}

// ============================================================================
// Large Dataset Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_large_batch_write() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let batch_size = 1000;
    let mut data = String::new();
    let mut expected_sum = 0.0;

    for i in 0..batch_size {
        let value = i as f64;
        expected_sum += value;
        data.push_str(&format!("cpu value={} {}\n", value, now + i * 1_000_000));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, batch_size as i64, "Should have {} rows", batch_size);

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    assert!(
        (sum - expected_sum).abs() < 0.01,
        "SUM should be {}, got {}",
        expected_sum,
        sum
    );
}

#[test]
#[ignore]
fn test_multiple_writes_accumulate() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // First write
    let data1 = format!(
        "cpu value=1.0 {}\n\
         cpu value=2.0 {}",
        now,
        now + 1_000_000_000
    );
    server.write(&db, &data1).expect("Write 1 failed");

    // Second write
    let data2 = format!(
        "cpu value=3.0 {}\n\
         cpu value=4.0 {}",
        now + 2_000_000_000,
        now + 3_000_000_000
    );
    server.write(&db, &data2).expect("Write 2 failed");

    // Third write
    let data3 = format!("cpu value=5.0 {}", now + 4_000_000_000);
    server.write(&db, &data3).expect("Write 3 failed");

    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 5, "All writes should accumulate");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM cpu").unwrap();
    assert!(
        (sum - 15.0).abs() < 0.0001,
        "SUM should be 15.0 (1+2+3+4+5)"
    );
}

// ============================================================================
// Many Fields Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_many_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    // 10 fields
    let data = format!(
        "system f1=1.0,f2=2.0,f3=3.0,f4=4.0,f5=5.0,f6=6.0,f7=7.0,f8=8.0,f9=9.0,f10=10.0 {}",
        now
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM system").unwrap();
    assert_eq!(count, 1, "Should have 1 row");

    // Query individual fields
    let f1 = server.query_scalar(&db, "SELECT f1 FROM system").unwrap();
    let f10 = server.query_scalar(&db, "SELECT f10 FROM system").unwrap();
    assert!((f1 - 1.0).abs() < 0.0001, "f1 should be 1.0");
    assert!((f10 - 10.0).abs() < 0.0001, "f10 should be 10.0");

    // Sum of all fields
    let sum = server
        .query_scalar(&db, "SELECT f1+f2+f3+f4+f5+f6+f7+f8+f9+f10 FROM system")
        .unwrap();
    assert!((sum - 55.0).abs() < 0.0001, "Sum of fields should be 55.0");
}

// ============================================================================
// Measurement Name Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_measurement_with_underscore() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!("system_metrics value=1.0 {}", now);

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM system_metrics")
        .unwrap();
    assert_eq!(count, 1, "Underscore measurement should work");
}

#[test]
#[ignore]
fn test_measurement_with_numbers() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!("cpu123 value=1.0 {}", now);

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu123")
        .unwrap();
    assert_eq!(count, 1, "Numeric suffix measurement should work");
}

// ============================================================================
// Aggregation with GROUP BY Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_group_by_with_having() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,host=a value=10.0 {}\n\
         cpu,host=a value=20.0 {}\n\
         cpu,host=b value=5.0 {}\n\
         cpu,host=c value=100.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // HAVING filters groups
    let result = server
        .query(
            &db,
            "SELECT host, SUM(value) as total FROM cpu GROUP BY host HAVING SUM(value) > 10 ORDER BY host",
        )
        .unwrap();

    let rows = result.as_array().expect("Expected array");
    // host 'a' has sum=30, host 'c' has sum=100, both > 10
    // host 'b' has sum=5, excluded by HAVING
    assert_eq!(rows.len(), 2, "HAVING should filter to 2 groups");
}

#[test]
#[ignore]
fn test_group_by_multiple_columns() {
    let server = TestServer::start();
    let db = server.unique_db();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let data = format!(
        "cpu,region=us,host=a value=10.0 {}\n\
         cpu,region=us,host=a value=20.0 {}\n\
         cpu,region=us,host=b value=30.0 {}\n\
         cpu,region=eu,host=a value=40.0 {}",
        now,
        now + 1_000_000_000,
        now + 2_000_000_000,
        now + 3_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let result = server
        .query(
            &db,
            "SELECT region, host, COUNT(*) as cnt FROM cpu GROUP BY region, host ORDER BY region, host",
        )
        .unwrap();

    let rows = result.as_array().expect("Expected array");
    // Should have 3 groups: (eu, a), (us, a), (us, b)
    assert_eq!(rows.len(), 3, "Should have 3 groups");
}

// ============================================================================
// Time Range Query Edge Cases
// ============================================================================

#[test]
#[ignore]
fn test_time_range_filter() {
    let server = TestServer::start();
    let db = server.unique_db();

    let base = 1700000000_000_000_000i64;
    let data = format!(
        "cpu value=1.0 {}\n\
         cpu value=2.0 {}\n\
         cpu value=3.0 {}\n\
         cpu value=4.0 {}\n\
         cpu value=5.0 {}",
        base,
        base + 1_000_000_000,
        base + 2_000_000_000,
        base + 3_000_000_000,
        base + 4_000_000_000
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Filter by timestamp range
    let count = server
        .query_count(
            &db,
            &format!(
                "SELECT COUNT(*) FROM cpu WHERE timestamp >= {} AND timestamp < {}",
                base + 1_000_000_000,
                base + 4_000_000_000
            ),
        )
        .unwrap();
    assert_eq!(count, 3, "Time range should match 3 rows");
}

// ============================================================================
// Test Runner Helper
// ============================================================================

/// Run all correctness tests
/// Use: cargo test --test correctness -- --ignored --test-threads=1
#[test]
#[ignore]
fn run_all_correctness_tests() {
    println!("Run correctness tests with: cargo test --test correctness -- --ignored --test-threads=1");
}
