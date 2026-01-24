//! Data Integrity Tests for MynaDB
//!
//! Phase A: Data Integrity & Recovery Testing
//!
//! These tests verify:
//! 1. Write/Read roundtrip correctness
//! 2. WAL crash recovery
//! 3. Compaction data preservation
//! 4. Data consistency across operations
//!
//! Run with: cargo test --test data_integrity

use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

/// Global counter for unique database names
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test configuration for different test scenarios
#[derive(Clone)]
struct TestConfig {
    wal_enabled: bool,
    wal_sync_on_write: bool,
    flush_interval_secs: u32,
    compaction_min_files: u32,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            wal_enabled: false,
            wal_sync_on_write: false,
            flush_interval_secs: 1, // Short interval for tests
            compaction_min_files: 100,  // High threshold - manual compaction only
        }
    }
}

impl TestConfig {
    fn with_wal() -> Self {
        Self {
            wal_enabled: true,
            wal_sync_on_write: true,
            ..Default::default()
        }
    }

    fn with_compaction(min_files: u32) -> Self {
        Self {
            compaction_min_files: min_files,
            ..Default::default()
        }
    }
}

/// Test harness for running MynaDB data integrity tests
struct TestServer {
    process: Option<Child>,
    port: u16,
    data_dir: PathBuf,
    config: TestConfig,
    _temp_dir: Option<tempfile::TempDir>,
}

impl TestServer {
    /// Start a new MynaDB test server with custom config
    fn start_with_config(config: TestConfig) -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let port = find_available_port();

        let mut server = TestServer {
            process: None,
            port,
            data_dir,
            config,
            _temp_dir: Some(temp_dir),
        };

        server.start_process();
        server
    }

    /// Start with default config (WAL disabled)
    fn start() -> Self {
        Self::start_with_config(TestConfig::default())
    }

    /// Start with WAL enabled for recovery tests
    fn start_with_wal() -> Self {
        Self::start_with_config(TestConfig::with_wal())
    }

    fn start_process(&mut self) {
        let config_path = self.data_dir.join("config.toml");
        let storage_dir = self.data_dir.join("data");
        fs::create_dir_all(&storage_dir).expect("Failed to create storage dir");

        let storage_dir_str = storage_dir.to_str().unwrap().replace('\\', "/");
        let config_content = format!(
            "[server]\n\
            http_port = {port}\n\
            mysql_enabled = false\n\
            flight_enabled = false\n\
            \n\
            [storage]\n\
            data_dir = \"{data_dir}\"\n\
            \n\
            [storage.object_store]\n\
            backend = \"local\"\n\
            data_dir = \"{data_dir}\"\n\
            \n\
            [cache]\n\
            flush_interval_secs = {flush_interval}\n\
            max_size_mb = 256\n\
            \n\
            [wal]\n\
            enabled = {wal_enabled}\n\
            sync_on_write = {wal_sync}\n\
            rotate_size_mb = 1\n\
            \n\
            [compaction]\n\
            min_files = {compaction_min_files}\n\
            max_concurrent_compactions = 2\n\
            \n\
            [retention]\n\
            enabled = false\n\
            \n\
            [logging]\n\
            level = \"warn\"\n",
            port = self.port,
            data_dir = storage_dir_str,
            flush_interval = self.config.flush_interval_secs,
            wal_enabled = self.config.wal_enabled,
            wal_sync = self.config.wal_sync_on_write,
            compaction_min_files = self.config.compaction_min_files,
        );
        fs::write(&config_path, &config_content).expect("Failed to write config");

        // Get binary path
        let binary_path = find_binary_path();

        let process = Command::new(&binary_path)
            .args(["-c", config_path.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect(&format!("Failed to start MynaDB from {:?}", binary_path));

        self.process = Some(process);
        self.wait_for_ready();
    }

    fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/health", self.port);
        for i in 0..60 {
            if let Ok(resp) = ureq::get(&url).call() {
                if resp.status() == 200 {
                    // Give a bit more time for full initialization
                    thread::sleep(Duration::from_millis(100));
                    return;
                }
            }
            if i % 10 == 0 {
                eprintln!("Waiting for server on port {}...", self.port);
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("Server failed to start within 30 seconds on port {}", self.port);
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Generate a unique database name
    fn unique_db(&self) -> String {
        format!("integritydb_{}", DB_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    /// Write data using line protocol
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

    /// Flush cache to Parquet
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
        // Wait for flush to complete and metadata to be registered
        // The metadata manager needs time to discover and register the new files
        thread::sleep(Duration::from_secs(4));
        Ok(())
    }

    /// Trigger compaction
    fn compact(&self, db: &str) -> Result<(), String> {
        let url = format!("{}/compact/{}", self.base_url(), db);
        match ureq::post(&url).call() {
            Ok(resp) => {
                let status = resp.status();
                if status != 200 {
                    let body = resp.into_string().unwrap_or_default();
                    return Err(format!("Compact failed with status: {} - {}", status, body));
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                return Err(format!("Compact failed with status {}: {}", code, body));
            }
            Err(e) => return Err(format!("Compact failed: {}", e)),
        }
        // Wait for compaction to complete
        thread::sleep(Duration::from_secs(3));
        Ok(())
    }

    /// Execute SQL query and return results (with retry for transient errors)
    fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/query/{}", self.base_url(), db);

        // Retry up to 5 times for table not found (timing issues after flush)
        for attempt in 0..5 {
            let resp = ureq::post(&url).send_string(sql);

            match resp {
                Ok(resp) => {
                    let body = resp
                        .into_string()
                        .map_err(|e| format!("Failed to read response: {}", e))?;
                    return serde_json::from_str(&body)
                        .map_err(|e| format!("Failed to parse JSON: {} (body: {})", e, body));
                }
                Err(ureq::Error::Status(code, resp)) => {
                    let body = resp.into_string().unwrap_or_default();
                    // Retry on "table not found" which might be a timing issue
                    if code == 400 && body.contains("not found") && attempt < 4 {
                        thread::sleep(Duration::from_secs(2));
                        continue;
                    }
                    return Err(format!("Query failed with status {}: {}", code, body));
                }
                Err(e) => return Err(format!("Query failed: {}", e)),
            }
        }
        Err("Query failed after retries".to_string())
    }

    /// Query and extract count
    fn query_count(&self, db: &str, sql: &str) -> Result<i64, String> {
        let result = self.query(db, sql)?;
        extract_count(&result)
    }

    /// Query and extract scalar
    fn query_scalar(&self, db: &str, sql: &str) -> Result<f64, String> {
        let result = self.query(db, sql)?;
        extract_scalar(&result)
    }

    /// Query and get all rows as Vec of HashMaps
    fn query_rows(&self, db: &str, sql: &str) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
        let result = self.query(db, sql)?;
        extract_rows(&result)
    }

    /// Stop the server (simulating crash for recovery tests)
    fn stop(&mut self) {
        if let Some(ref mut process) = self.process {
            let _ = process.kill();
            let _ = process.wait();
        }
        self.process = None;
    }

    /// Restart the server (for recovery tests)
    fn restart(&mut self) {
        self.stop();
        // Wait a bit for port to be released
        thread::sleep(Duration::from_millis(500));
        self.start_process();
    }

    /// Get the storage directory path
    fn storage_dir(&self) -> PathBuf {
        self.data_dir.join("data")
    }

    /// Count Parquet files at a specific level
    #[allow(dead_code)]
    fn count_parquet_files(&self, _db: &str, level: &str) -> usize {
        let storage = self.storage_dir();
        let pattern = format!("{}_", level);

        let mut count = 0;
        for entry in walkdir::WalkDir::new(&storage)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.path().extension().map_or(false, |ext| ext == "parquet") {
                if let Some(name) = entry.path().file_name().and_then(|n| n.to_str()) {
                    if name.starts_with(&pattern) {
                        count += 1;
                    }
                }
            }
        }
        count
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

fn find_binary_path() -> PathBuf {
    let release = std::env::current_dir()
        .unwrap()
        .join("target/release/mynadb");
    if release.exists() {
        release
    } else {
        std::env::current_dir()
            .unwrap()
            .join("target/debug/mynadb")
    }
}

fn extract_count(result: &serde_json::Value) -> Result<i64, String> {
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                for (_, val) in obj.iter() {
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
    Err(format!("Could not extract count from: {:?}", result))
}

fn extract_scalar(result: &serde_json::Value) -> Result<f64, String> {
    if let Some(arr) = result.as_array() {
        if let Some(first) = arr.first() {
            if let Some(obj) = first.as_object() {
                for (_, val) in obj.iter() {
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
    Err(format!("Could not extract scalar from: {:?}", result))
}

fn extract_rows(result: &serde_json::Value) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    if let Some(arr) = result.as_array() {
        let mut rows = Vec::new();
        for item in arr {
            if let Some(obj) = item.as_object() {
                let mut row = HashMap::new();
                for (k, v) in obj.iter() {
                    row.insert(k.clone(), v.clone());
                }
                rows.push(row);
            }
        }
        return Ok(rows);
    }
    Err(format!("Could not extract rows from: {:?}", result))
}

// ============================================================================
// SECTION 1: Write/Read Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_basic_float() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write known float values
    let data = "temperature,location=room1 value=23.456\n\
                temperature,location=room2 value=19.876\n\
                temperature,location=room3 value=25.123";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Verify data
    let count = server.query_count(&db, "SELECT COUNT(*) FROM temperature").expect("Count query failed");
    assert_eq!(count, 3, "Expected 3 rows");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM temperature").expect("Sum query failed");
    let expected_sum = 23.456 + 19.876 + 25.123;
    assert!((sum - expected_sum).abs() < 0.001, "Sum mismatch: {} vs {}", sum, expected_sum);
}

#[test]
fn test_roundtrip_integer_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write integer values
    let data = "events,type=click count=100i\n\
                events,type=view count=250i\n\
                events,type=scroll count=75i";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let sum = server.query_scalar(&db, "SELECT SUM(count) FROM events").expect("Sum query failed");
    assert_eq!(sum as i64, 425, "Integer sum mismatch");
}

#[test]
fn test_roundtrip_boolean_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write boolean values
    let data = "flags,tag=api active=true\n\
                flags,tag=db active=false\n\
                flags,tag=cache active=true";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    // First verify count works
    let count = server.query_count(&db, "SELECT COUNT(*) FROM flags").expect("Count failed");
    assert_eq!(count, 3);

    // Then get rows
    let rows = server.query_rows(&db, "SELECT tag, active FROM flags ORDER BY tag").expect("Query failed");
    assert_eq!(rows.len(), 3);

    // Verify boolean values preserved
    let active_count = rows.iter().filter(|r| {
        r.get("active").map_or(false, |v| v.as_bool().unwrap_or(false))
    }).count();
    assert_eq!(active_count, 2, "Expected 2 active=true");
}

#[test]
fn test_roundtrip_string_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write string values
    let data = r#"logs,level=info message="Hello World"
logs,level=error message="Something went wrong"
logs,level=debug message="Debug info here""#;

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM logs").expect("Count failed");
    assert_eq!(count, 3);
}

#[test]
fn test_roundtrip_mixed_types() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write mixed field types
    let data = r#"metrics,host=srv1 cpu=75.5,memory=8192i,healthy=true,status="running"
metrics,host=srv2 cpu=82.3,memory=16384i,healthy=true,status="running"
metrics,host=srv3 cpu=45.0,memory=4096i,healthy=false,status="stopped""#;

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM metrics").expect("Count failed");
    assert_eq!(count, 3);

    let avg_cpu = server.query_scalar(&db, "SELECT AVG(cpu) FROM metrics").expect("Avg failed");
    let expected_avg = (75.5 + 82.3 + 45.0) / 3.0;
    assert!((avg_cpu - expected_avg).abs() < 0.01);

    let total_memory = server.query_scalar(&db, "SELECT SUM(memory) FROM metrics").expect("Sum failed");
    assert_eq!(total_memory as i64, 8192 + 16384 + 4096);
}

#[test]
fn test_roundtrip_large_batch() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write 1000 rows
    let mut data = String::new();
    let mut expected_sum = 0.0;
    for i in 0..1000 {
        let value = (i as f64) * 0.1;
        expected_sum += value;
        data.push_str(&format!("batch,id={} value={}\n", i, value));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM batch").expect("Count failed");
    assert_eq!(count, 1000, "Row count mismatch");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM batch").expect("Sum failed");
    assert!((sum - expected_sum).abs() < 0.1, "Sum mismatch: {} vs {}", sum, expected_sum);
}

#[test]
fn test_roundtrip_special_characters_in_tags() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with special characters in tag values
    let data = "request,path=/api/v1/users value=1\n\
                request,path=/api/v2/items value=2\n\
                request,path=/health-check value=3";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM request").expect("Count failed");
    assert_eq!(count, 3);
}

#[test]
fn test_roundtrip_many_tags() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with many tags (avoid "service" tag as it's used for schema routing)
    let data = "telemetry,region=us-west,datacenter=dc1,rack=r42,host=srv001,app=api,version=v2,env=prod value=100\n\
                telemetry,region=us-east,datacenter=dc2,rack=r17,host=srv042,app=web,version=v3,env=staging value=200";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM telemetry").expect("Count failed");
    assert_eq!(count, 2);

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM telemetry").expect("Sum failed");
    assert_eq!(sum as i64, 300);
}

#[test]
fn test_roundtrip_many_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with many fields
    let data = "server,host=srv1 cpu=50.0,memory=75.0,disk=80.0,network_in=1000i,network_out=500i,connections=42i,uptime=86400i,load_1m=1.5,load_5m=1.2,load_15m=0.9";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let rows = server.query_rows(&db, "SELECT * FROM server").expect("Query failed");
    assert_eq!(rows.len(), 1);

    // Verify specific fields
    let row = &rows[0];
    assert_eq!(row.get("cpu").and_then(|v| v.as_f64()), Some(50.0));
    assert_eq!(row.get("memory").and_then(|v| v.as_f64()), Some(75.0));
}

#[test]
fn test_roundtrip_precision_floats() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Test floating point precision
    let data = "precision,test=1 val=0.123456789012345\n\
                precision,test=2 val=1e-15\n\
                precision,test=3 val=1e15\n\
                precision,test=4 val=-0.000000001";

    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM precision").expect("Count failed");
    assert_eq!(count, 4);

    // Verify extreme values are preserved
    let min = server.query_scalar(&db, "SELECT MIN(val) FROM precision").expect("Min failed");
    assert!(min < 0.0, "Min should be negative");
}

#[test]
fn test_roundtrip_timestamp_preservation() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with explicit timestamps
    let ts1 = 1704067200000000000i64; // 2024-01-01 00:00:00 UTC
    let ts2 = 1704153600000000000i64; // 2024-01-02 00:00:00 UTC

    let data = format!(
        "timeseries,sensor=1 value=100 {}\n\
         timeseries,sensor=1 value=200 {}",
        ts1, ts2
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM timeseries").expect("Count failed");
    assert_eq!(count, 2);

    // Verify time ordering
    let rows = server.query_rows(&db, "SELECT value FROM timeseries ORDER BY time").expect("Query failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("value").and_then(|v| v.as_f64()), Some(100.0));
    assert_eq!(rows[1].get("value").and_then(|v| v.as_f64()), Some(200.0));
}

// ============================================================================
// SECTION 2: WAL Recovery Tests
// ============================================================================

#[test]
fn test_wal_recovery_basic() {
    let config = TestConfig::with_wal();
    let mut server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write data (will be logged to WAL)
    let data = "recovery,test=basic value=42.5\n\
                recovery,test=basic value=17.3\n\
                recovery,test=basic value=99.9";

    server.write(&db, &data).expect("Write failed");

    // Small delay to ensure WAL is written
    thread::sleep(Duration::from_millis(500));

    // Crash without flushing (stop kills the process)
    server.stop();

    // Restart - should recover from WAL
    server.restart();

    // Verify data recovered
    let count = server.query_count(&db, "SELECT COUNT(*) FROM recovery").expect("Count failed after recovery");
    assert_eq!(count, 3, "Data should be recovered from WAL");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM recovery").expect("Sum failed after recovery");
    let expected = 42.5 + 17.3 + 99.9;
    assert!((sum - expected).abs() < 0.01, "Sum mismatch after recovery");
}

#[test]
fn test_wal_recovery_multiple_writes() {
    let config = TestConfig::with_wal();
    let mut server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Multiple write operations
    for i in 0..5 {
        let data = format!("multiwrite,batch={} value={}", i, i * 10);
        server.write(&db, &data).expect("Write failed");
        thread::sleep(Duration::from_millis(100));
    }

    // Crash without flushing
    server.stop();
    server.restart();

    // Verify all writes recovered
    let count = server.query_count(&db, "SELECT COUNT(*) FROM multiwrite").expect("Count failed");
    assert_eq!(count, 5, "All writes should be recovered");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM multiwrite").expect("Sum failed");
    assert_eq!(sum as i64, 0 + 10 + 20 + 30 + 40);
}

#[test]
fn test_wal_recovery_after_partial_flush() {
    let config = TestConfig::with_wal();
    let mut server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write first batch and flush
    server.write(&db, "partial,phase=1 value=100").expect("Write 1 failed");
    server.write(&db, "partial,phase=1 value=200").expect("Write 2 failed");
    server.flush().expect("Flush failed");

    // Write second batch (not flushed)
    server.write(&db, "partial,phase=2 value=300").expect("Write 3 failed");
    server.write(&db, "partial,phase=2 value=400").expect("Write 4 failed");

    thread::sleep(Duration::from_millis(500));

    // Crash
    server.stop();
    server.restart();

    // Verify both flushed and unflushed data
    let count = server.query_count(&db, "SELECT COUNT(*) FROM partial").expect("Count failed");
    assert_eq!(count, 4, "All data should be present");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM partial").expect("Sum failed");
    assert_eq!(sum as i64, 100 + 200 + 300 + 400);
}

#[test]
fn test_wal_recovery_large_batch() {
    let config = TestConfig::with_wal();
    let mut server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write 500 rows
    let mut data = String::new();
    let mut expected_sum = 0i64;
    for i in 0..500 {
        data.push_str(&format!("largewal,id={} value={}i\n", i, i));
        expected_sum += i;
    }

    server.write(&db, &data).expect("Write failed");
    thread::sleep(Duration::from_secs(1));

    // Crash and recover
    server.stop();
    server.restart();

    let count = server.query_count(&db, "SELECT COUNT(*) FROM largewal").expect("Count failed");
    assert_eq!(count, 500, "All 500 rows should be recovered");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM largewal").expect("Sum failed");
    assert_eq!(sum as i64, expected_sum);
}

// ============================================================================
// SECTION 3: Compaction Data Preservation Tests
// ============================================================================

#[test]
fn test_compaction_preserves_count() {
    let config = TestConfig::with_compaction(2); // Compact when 2+ files
    let server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Create multiple L0 files via multiple flush cycles
    for batch in 0..4 {
        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!("compacttest,batch={} value={}\n", batch, batch * 100 + i));
        }
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    // Record count before compaction
    let count_before = server.query_count(&db, "SELECT COUNT(*) FROM compacttest").expect("Count before failed");
    assert_eq!(count_before, 400);

    // Record sum before compaction
    let sum_before = server.query_scalar(&db, "SELECT SUM(value) FROM compacttest").expect("Sum before failed");

    // Trigger compaction
    server.compact(&db).expect("Compaction failed");

    // Verify count preserved
    let count_after = server.query_count(&db, "SELECT COUNT(*) FROM compacttest").expect("Count after failed");
    assert_eq!(count_after, count_before, "Row count should be preserved after compaction");

    // Verify sum preserved
    let sum_after = server.query_scalar(&db, "SELECT SUM(value) FROM compacttest").expect("Sum after failed");
    assert!((sum_after - sum_before).abs() < 0.01, "Sum should be preserved after compaction");
}

#[test]
fn test_compaction_preserves_values() {
    let config = TestConfig::with_compaction(2);
    let server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write specific known values in multiple batches
    let batches = vec![
        vec![1.1, 2.2, 3.3],
        vec![4.4, 5.5, 6.6],
        vec![7.7, 8.8, 9.9],
    ];

    for (batch_idx, values) in batches.iter().enumerate() {
        let mut data = String::new();
        for (val_idx, value) in values.iter().enumerate() {
            data.push_str(&format!("valtest,batch={},idx={} value={}\n", batch_idx, val_idx, value));
        }
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    // Calculate expected values
    let expected_sum: f64 = batches.iter().flatten().sum();
    let expected_min: f64 = 1.1;
    let expected_max: f64 = 9.9;
    let expected_count = 9i64;

    // Verify before compaction
    let sum_before = server.query_scalar(&db, "SELECT SUM(value) FROM valtest").expect("Sum failed");
    assert!((sum_before - expected_sum).abs() < 0.01);

    // Compact
    server.compact(&db).expect("Compaction failed");

    // Verify after compaction
    let sum_after = server.query_scalar(&db, "SELECT SUM(value) FROM valtest").expect("Sum failed");
    assert!((sum_after - expected_sum).abs() < 0.01, "Sum changed after compaction");

    let min_after = server.query_scalar(&db, "SELECT MIN(value) FROM valtest").expect("Min failed");
    assert!((min_after - expected_min).abs() < 0.01, "Min changed after compaction");

    let max_after = server.query_scalar(&db, "SELECT MAX(value) FROM valtest").expect("Max failed");
    assert!((max_after - expected_max).abs() < 0.01, "Max changed after compaction");

    let count_after = server.query_count(&db, "SELECT COUNT(*) FROM valtest").expect("Count failed");
    assert_eq!(count_after, expected_count, "Count changed after compaction");
}

#[test]
fn test_compaction_preserves_all_field_types() {
    let config = TestConfig::with_compaction(2);
    let server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write mixed field types across multiple flushes
    for batch in 0..3 {
        let data = format!(
            r#"mixedcompact,batch={} fval={}.5,ival={}i,bval={},sval="str{}""#,
            batch, batch, batch * 10, batch % 2 == 0, batch
        );
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    // Record before compaction
    let count_before = server.query_count(&db, "SELECT COUNT(*) FROM mixedcompact").expect("Count failed");
    let sum_ival_before = server.query_scalar(&db, "SELECT SUM(ival) FROM mixedcompact").expect("Sum ival failed");

    // Compact
    server.compact(&db).expect("Compaction failed");

    // Verify after
    let count_after = server.query_count(&db, "SELECT COUNT(*) FROM mixedcompact").expect("Count failed");
    assert_eq!(count_after, count_before);

    let sum_ival_after = server.query_scalar(&db, "SELECT SUM(ival) FROM mixedcompact").expect("Sum ival failed");
    assert_eq!(sum_ival_after as i64, sum_ival_before as i64);
}

#[test]
fn test_compaction_preserves_tags() {
    let config = TestConfig::with_compaction(2);
    let server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write data with specific tags
    let hosts = ["host1", "host2", "host3"];
    let regions = ["us", "eu", "asia"];

    for batch in 0..3 {
        let mut data = String::new();
        for host in &hosts {
            for region in &regions {
                data.push_str(&format!("tagtest,host={},region={} value={}\n", host, region, batch));
            }
        }
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    // Count per tag combination before
    let count_per_host_before = server.query_count(
        &db,
        "SELECT COUNT(*) FROM tagtest WHERE host = 'host1'"
    ).expect("Count failed");

    // Compact
    server.compact(&db).expect("Compaction failed");

    // Verify tag-based queries still work
    let count_per_host_after = server.query_count(
        &db,
        "SELECT COUNT(*) FROM tagtest WHERE host = 'host1'"
    ).expect("Count failed");
    assert_eq!(count_per_host_after, count_per_host_before, "Tag-filtered count changed");

    // Verify distinct tag values
    let total_count = server.query_count(&db, "SELECT COUNT(*) FROM tagtest").expect("Count failed");
    assert_eq!(total_count, 9 * 3); // 9 combinations (3 hosts × 3 regions) * 3 batches = 27
}

#[test]
fn test_compaction_multilevel() {
    let config = TestConfig::with_compaction(2);
    let server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Create many L0 files to trigger multi-level compaction
    let mut expected_sum = 0i64;
    for batch in 0..10 {
        let mut data = String::new();
        for i in 0..50 {
            let value = batch * 50 + i;
            expected_sum += value;
            data.push_str(&format!("multilevel,batch={} value={}i\n", batch, value));
        }
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    let count_before = server.query_count(&db, "SELECT COUNT(*) FROM multilevel").expect("Count failed");
    assert_eq!(count_before, 500);

    // Multiple compaction rounds
    for _ in 0..3 {
        server.compact(&db).expect("Compaction failed");
    }

    // Verify data integrity
    let count_after = server.query_count(&db, "SELECT COUNT(*) FROM multilevel").expect("Count failed");
    assert_eq!(count_after, 500, "Row count changed after multi-level compaction");

    let sum_after = server.query_scalar(&db, "SELECT SUM(value) FROM multilevel").expect("Sum failed");
    assert_eq!(sum_after as i64, expected_sum, "Sum changed after multi-level compaction");
}

// ============================================================================
// SECTION 4: Data Consistency Under Concurrent Operations
// ============================================================================

#[test]
fn test_consistency_write_during_flush() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Initial write
    server.write(&db, "concurrent,phase=1 value=100").expect("Write 1 failed");

    // Start flush (non-blocking in server)
    let flush_url = format!("{}/flush", server.base_url());
    let _ = ureq::post(&flush_url).call();

    // Write during flush
    server.write(&db, "concurrent,phase=2 value=200").expect("Write 2 failed");
    server.write(&db, "concurrent,phase=3 value=300").expect("Write 3 failed");

    // Wait for everything to settle
    thread::sleep(Duration::from_secs(3));

    // All writes should be present
    let count = server.query_count(&db, "SELECT COUNT(*) FROM concurrent").expect("Count failed");
    assert_eq!(count, 3, "All writes should be visible");

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM concurrent").expect("Sum failed");
    assert_eq!(sum as i64, 600);
}

#[test]
fn test_consistency_query_during_compaction() {
    let config = TestConfig::with_compaction(2);
    let server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write and flush multiple batches
    for batch in 0..4 {
        let data = format!("querycompact,batch={} value={}", batch, batch * 10);
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    let expected_count = 4i64;
    let expected_sum = 0 + 10 + 20 + 30;

    // Verify before compaction
    let count = server.query_count(&db, "SELECT COUNT(*) FROM querycompact").expect("Count failed");
    assert_eq!(count, expected_count);

    // Start compaction (non-blocking)
    let compact_url = format!("{}/compact/{}", server.base_url(), db);
    let _ = ureq::post(&compact_url).call();

    // Query during compaction - should still return correct results
    for _ in 0..5 {
        let count = server.query_count(&db, "SELECT COUNT(*) FROM querycompact").expect("Count during compact failed");
        assert_eq!(count, expected_count, "Query returned inconsistent count during compaction");

        let sum = server.query_scalar(&db, "SELECT SUM(value) FROM querycompact").expect("Sum during compact failed");
        assert_eq!(sum as i64, expected_sum, "Query returned inconsistent sum during compaction");

        thread::sleep(Duration::from_millis(500));
    }
}

// ============================================================================
// SECTION 5: Edge Cases and Error Handling
// ============================================================================

#[test]
fn test_empty_table_operations() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write and flush to create table
    server.write(&db, "emptytable,tag=1 value=1").expect("Write failed");
    server.flush().expect("Flush failed");

    // Query table that exists but filter returns nothing
    let count = server.query_count(&db, "SELECT COUNT(*) FROM emptytable WHERE value > 1000").expect("Count failed");
    assert_eq!(count, 0);
}

#[test]
fn test_null_handling_in_aggregations() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write rows where some might have null fields (via schema mismatch)
    server.write(&db, "nulltest,id=1 a=1,b=2").expect("Write failed");
    server.write(&db, "nulltest,id=2 a=3").expect("Write failed"); // no 'b' field
    server.write(&db, "nulltest,id=3 a=5,b=6").expect("Write failed");
    server.flush().expect("Flush failed");

    // SUM should handle nulls correctly
    let sum_a = server.query_scalar(&db, "SELECT SUM(a) FROM nulltest").expect("Sum a failed");
    assert_eq!(sum_a as i64, 1 + 3 + 5);

    // COUNT should handle nulls
    let count = server.query_count(&db, "SELECT COUNT(*) FROM nulltest").expect("Count failed");
    assert_eq!(count, 3);
}

#[test]
fn test_duplicate_timestamps() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write multiple rows with same timestamp
    let ts = 1704067200000000000i64;
    let data = format!(
        "duptime,sensor=a value=1 {}\n\
         duptime,sensor=b value=2 {}\n\
         duptime,sensor=c value=3 {}",
        ts, ts, ts
    );

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // All rows should be present despite same timestamp
    let count = server.query_count(&db, "SELECT COUNT(*) FROM duptime").expect("Count failed");
    assert_eq!(count, 3);

    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM duptime").expect("Sum failed");
    assert_eq!(sum as i64, 6);
}

#[test]
fn test_very_old_timestamp() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with a very old timestamp (year 2000)
    let old_ts = 946684800000000000i64; // 2000-01-01
    let data = format!("olddata,source=archive value=999 {}", old_ts);

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM olddata").expect("Count failed");
    assert_eq!(count, 1);
}

#[test]
fn test_future_timestamp() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with a future timestamp (year 2030)
    let future_ts = 1893456000000000000i64; // 2030-01-01
    let data = format!("futuredata,source=prediction value=42 {}", future_ts);

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM futuredata").expect("Count failed");
    assert_eq!(count, 1);
}

// ============================================================================
// SECTION 6: Checksum and Hash Verification
// ============================================================================

#[test]
fn test_data_checksum_consistency() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write known dataset
    let mut expected_values: Vec<f64> = Vec::new();
    let mut data = String::new();
    for i in 0..100 {
        let value = (i as f64) * 1.23456;
        expected_values.push(value);
        data.push_str(&format!("checksum,id={} value={}\n", i, value));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Calculate expected statistics
    let expected_sum: f64 = expected_values.iter().sum();
    let expected_min = expected_values.iter().cloned().fold(f64::INFINITY, f64::min);
    let expected_max = expected_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let expected_count = expected_values.len() as i64;

    // Verify all statistics match (serves as checksum)
    let sum = server.query_scalar(&db, "SELECT SUM(value) FROM checksum").expect("Sum failed");
    assert!((sum - expected_sum).abs() < 0.01, "Sum mismatch: {} vs {}", sum, expected_sum);

    let min = server.query_scalar(&db, "SELECT MIN(value) FROM checksum").expect("Min failed");
    assert!((min - expected_min).abs() < 0.0001, "Min mismatch");

    let max = server.query_scalar(&db, "SELECT MAX(value) FROM checksum").expect("Max failed");
    assert!((max - expected_max).abs() < 0.0001, "Max mismatch");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM checksum").expect("Count failed");
    assert_eq!(count, expected_count, "Count mismatch");
}

#[test]
fn test_data_integrity_across_restarts() {
    let config = TestConfig::with_wal();
    let mut server = TestServer::start_with_config(config);
    let db = server.unique_db();

    // Write and flush
    let mut data = String::new();
    let mut expected_sum = 0i64;
    for i in 0..200 {
        expected_sum += i;
        data.push_str(&format!("restarttest,id={} value={}i\n", i, i));
    }

    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Verify before restart
    let sum_before = server.query_scalar(&db, "SELECT SUM(value) FROM restarttest").expect("Sum failed");
    let count_before = server.query_count(&db, "SELECT COUNT(*) FROM restarttest").expect("Count failed");

    // Restart
    server.restart();

    // Verify after restart
    let sum_after = server.query_scalar(&db, "SELECT SUM(value) FROM restarttest").expect("Sum after restart failed");
    let count_after = server.query_count(&db, "SELECT COUNT(*) FROM restarttest").expect("Count after restart failed");

    assert_eq!(count_after, count_before, "Count changed after restart");
    assert_eq!(sum_after as i64, sum_before as i64, "Sum changed after restart");
    assert_eq!(sum_after as i64, expected_sum, "Data corrupted after restart");
}
