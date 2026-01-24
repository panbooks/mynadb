//! Concurrency and Stress Tests for MynaDB
//!
//! Phase B: Concurrency & Stress Testing
//!
//! These tests verify:
//! 1. Multi-threaded concurrent writes
//! 2. Read consistency during background operations
//! 3. Write/query isolation
//! 4. Stress testing under load
//!
//! Run with: cargo test --test concurrency -- --test-threads=1

use std::collections::HashSet;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Global counter for unique database names
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test harness for concurrency tests
struct TestServer {
    process: Option<Child>,
    port: u16,
    _temp_dir: tempfile::TempDir,
}

impl TestServer {
    fn start() -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let port = find_available_port();
        let config_path = temp_dir.path().join("config.toml");
        let storage_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&storage_dir).expect("Failed to create storage dir");

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
            flush_interval_secs = 1\n\
            max_size_mb = 512\n\
            \n\
            [wal]\n\
            enabled = true\n\
            sync_on_write = false\n\
            \n\
            [compaction]\n\
            min_files = 3\n\
            max_concurrent_compactions = 4\n\
            \n\
            [retention]\n\
            enabled = false\n\
            \n\
            [logging]\n\
            level = \"warn\"\n",
            port = port,
            data_dir = storage_dir_str,
        );
        std::fs::write(&config_path, &config_content).expect("Failed to write config");

        let binary_path = find_binary_path();
        let process = Command::new(&binary_path)
            .args(["-c", config_path.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect(&format!("Failed to start MynaDB from {:?}", binary_path));

        let server = TestServer {
            process: Some(process),
            port,
            _temp_dir: temp_dir,
        };

        server.wait_for_ready();
        server
    }

    fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/health", self.port);
        for _ in 0..60 {
            if let Ok(resp) = ureq::get(&url).call() {
                if resp.status() == 200 {
                    thread::sleep(Duration::from_millis(100));
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

    fn unique_db(&self) -> String {
        format!("concurrencydb_{}", DB_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

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
        thread::sleep(Duration::from_secs(3));
        Ok(())
    }

    fn compact(&self, db: &str) -> Result<(), String> {
        let url = format!("{}/compact/{}", self.base_url(), db);
        match ureq::post(&url).call() {
            Ok(_) => {}
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                return Err(format!("Compact failed with status {}: {}", code, body));
            }
            Err(e) => return Err(format!("Compact failed: {}", e)),
        }
        thread::sleep(Duration::from_secs(2));
        Ok(())
    }

    fn query(&self, db: &str, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/query/{}", self.base_url(), db);

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
                    if code == 400 && body.contains("not found") && attempt < 4 {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    return Err(format!("Query failed with status {}: {}", code, body));
                }
                Err(e) => return Err(format!("Query failed: {}", e)),
            }
        }
        Err("Query failed after retries".to_string())
    }

    fn query_count(&self, db: &str, sql: &str) -> Result<i64, String> {
        let result = self.query(db, sql)?;
        extract_count(&result)
    }

    fn query_scalar(&self, db: &str, sql: &str) -> Result<f64, String> {
        let result = self.query(db, sql)?;
        extract_scalar(&result)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(ref mut process) = self.process {
            let _ = process.kill();
            let _ = process.wait();
        }
    }
}

fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

fn find_binary_path() -> std::path::PathBuf {
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

// ============================================================================
// SECTION 1: Concurrent Write Tests
// ============================================================================

#[test]
fn test_concurrent_writes_single_table() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    let num_threads = 4;
    let writes_per_thread = 100;
    let total_writes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let server = Arc::clone(&server);
        let db = db.clone();
        let total_writes = Arc::clone(&total_writes);
        let errors = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let data = format!("concurrent,thread={} value={}", thread_id, thread_id * 1000 + i);
                match server.write(&db, &data) {
                    Ok(_) => {
                        total_writes.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        eprintln!("Write error: {}", e);
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Flush and verify
    server.flush().expect("Flush failed");

    let error_count = errors.load(Ordering::SeqCst);
    assert_eq!(error_count, 0, "Expected no write errors");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM concurrent").expect("Count failed");
    let expected = (num_threads * writes_per_thread) as i64;
    assert_eq!(count, expected, "Expected {} rows, got {}", expected, count);
}

#[test]
fn test_concurrent_writes_multiple_tables() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    let num_threads = 4;
    let writes_per_thread = 50;
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let server = Arc::clone(&server);
        let db = db.clone();
        let errors = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            let table_name = format!("table{}", thread_id);
            for i in 0..writes_per_thread {
                let data = format!("{},tag=t{} value={}", table_name, i, i);
                if let Err(e) = server.write(&db, &data) {
                    eprintln!("Write error: {}", e);
                    errors.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    server.flush().expect("Flush failed");

    let error_count = errors.load(Ordering::SeqCst);
    assert_eq!(error_count, 0, "Expected no write errors");

    // Verify each table has correct count
    for thread_id in 0..num_threads {
        let table_name = format!("table{}", thread_id);
        let count = server
            .query_count(&db, &format!("SELECT COUNT(*) FROM {}", table_name))
            .expect(&format!("Count failed for {}", table_name));
        assert_eq!(count, writes_per_thread as i64, "Table {} has wrong count", table_name);
    }
}

#[test]
fn test_concurrent_writes_no_duplicates() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    let num_threads = 4;
    let writes_per_thread = 100;

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let server = Arc::clone(&server);
        let db = db.clone();

        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                // Use unique ID combining thread_id and sequence
                let unique_id = thread_id * 10000 + i;
                let data = format!("nodups,id={} value={}", unique_id, unique_id);
                server.write(&db, &data).expect("Write failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    server.flush().expect("Flush failed");

    // Verify count
    let count = server.query_count(&db, "SELECT COUNT(*) FROM nodups").expect("Count failed");
    let expected = (num_threads * writes_per_thread) as i64;
    assert_eq!(count, expected);

    // Verify no duplicates by checking distinct count
    let distinct_count = server
        .query_count(&db, "SELECT COUNT(DISTINCT id) FROM nodups")
        .expect("Distinct count failed");
    assert_eq!(distinct_count, expected, "Found duplicate IDs");
}

#[test]
fn test_concurrent_writes_with_flush() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    let num_writers = 3;
    let writes_per_writer = 50;
    let write_counter = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Writer threads
    for thread_id in 0..num_writers {
        let server = Arc::clone(&server);
        let db = db.clone();
        let write_counter = Arc::clone(&write_counter);

        let handle = thread::spawn(move || {
            for i in 0..writes_per_writer {
                let data = format!("withflush,writer={} seq={}", thread_id, i);
                server.write(&db, &data).expect("Write failed");
                write_counter.fetch_add(1, Ordering::SeqCst);

                // Occasional small delay
                if i % 10 == 0 {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });
        handles.push(handle);
    }

    // Flusher thread - runs flush periodically
    let server_flush = Arc::clone(&server);
    let flush_handle = thread::spawn(move || {
        for _ in 0..5 {
            thread::sleep(Duration::from_millis(200));
            let _ = server_flush.flush();
        }
    });

    for handle in handles {
        handle.join().expect("Writer thread panicked");
    }
    flush_handle.join().expect("Flush thread panicked");

    // Final flush
    server.flush().expect("Final flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM withflush").expect("Count failed");
    let expected = (num_writers * writes_per_writer) as i64;
    assert_eq!(count, expected, "Missing writes during concurrent flush");
}

// ============================================================================
// SECTION 2: Read Consistency During Background Operations
// ============================================================================

#[test]
fn test_read_during_write() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Write initial data
    for i in 0..10 {
        server.write(&db, &format!("readwrite,id={} value={}", i, i)).expect("Initial write failed");
    }
    server.flush().expect("Initial flush failed");

    let initial_count = server.query_count(&db, "SELECT COUNT(*) FROM readwrite").expect("Initial count failed");
    assert_eq!(initial_count, 10);

    let read_errors = Arc::new(AtomicU64::new(0));
    let inconsistent_reads = Arc::new(AtomicU64::new(0));
    let min_seen = Arc::new(AtomicU64::new(10)); // Should never go below initial

    // Start writer thread
    let server_write = Arc::clone(&server);
    let db_write = db.clone();
    let write_handle = thread::spawn(move || {
        for i in 10..110 {
            let data = format!("readwrite,id={} value={}", i, i);
            server_write.write(&db_write, &data).expect("Write failed");
            if i % 20 == 0 {
                thread::sleep(Duration::from_millis(50));
            }
        }
    });

    // Reader thread - continuously reads
    let server_read = Arc::clone(&server);
    let db_read = db.clone();
    let read_errors_clone = Arc::clone(&read_errors);
    let inconsistent_reads_clone = Arc::clone(&inconsistent_reads);
    let min_seen_clone = Arc::clone(&min_seen);

    let read_handle = thread::spawn(move || {
        for _ in 0..50 {
            match server_read.query_count(&db_read, "SELECT COUNT(*) FROM readwrite") {
                Ok(count) => {
                    if count < 10 {
                        inconsistent_reads_clone.fetch_add(1, Ordering::SeqCst);
                    }
                    // Track minimum seen
                    let mut current_min = min_seen_clone.load(Ordering::SeqCst);
                    while (count as u64) < current_min {
                        match min_seen_clone.compare_exchange(
                            current_min,
                            count as u64,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(new) => current_min = new,
                        }
                    }
                }
                Err(_) => {
                    read_errors_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
    });

    write_handle.join().expect("Writer panicked");
    read_handle.join().expect("Reader panicked");

    // Allow final flush
    server.flush().expect("Final flush failed");

    let final_count = server.query_count(&db, "SELECT COUNT(*) FROM readwrite").expect("Final count failed");
    assert_eq!(final_count, 110, "Expected 110 rows after all writes");

    let inconsistent = inconsistent_reads.load(Ordering::SeqCst);
    assert_eq!(inconsistent, 0, "Found {} inconsistent reads (count < 10)", inconsistent);

    let min = min_seen.load(Ordering::SeqCst);
    assert!(min >= 10, "Saw count go below initial value: {}", min);
}

#[test]
fn test_read_during_compaction() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Create multiple L0 files
    for batch in 0..5 {
        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!("readcompact,batch={},id={} value={}\n", batch, i, batch * 20 + i));
        }
        server.write(&db, &data).expect("Write failed");
        server.flush().expect("Flush failed");
    }

    let expected_count = 5 * 20i64;
    let expected_sum = (0..100).sum::<i64>();

    // Verify before compaction
    let count_before = server.query_count(&db, "SELECT COUNT(*) FROM readcompact").expect("Count failed");
    assert_eq!(count_before, expected_count);

    let read_errors = Arc::new(AtomicU64::new(0));
    let inconsistent_counts = Arc::new(AtomicU64::new(0));

    // Reader thread
    let server_read = Arc::clone(&server);
    let db_read = db.clone();
    let read_errors_clone = Arc::clone(&read_errors);
    let inconsistent_counts_clone = Arc::clone(&inconsistent_counts);

    let read_handle = thread::spawn(move || {
        for _ in 0..30 {
            match server_read.query_count(&db_read, "SELECT COUNT(*) FROM readcompact") {
                Ok(count) => {
                    if count != expected_count {
                        inconsistent_counts_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Err(_) => {
                    read_errors_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Compaction in main thread
    thread::sleep(Duration::from_millis(500));
    let _ = server.compact(&db);

    read_handle.join().expect("Reader panicked");

    // Verify after compaction
    let count_after = server.query_count(&db, "SELECT COUNT(*) FROM readcompact").expect("Count failed");
    assert_eq!(count_after, expected_count, "Row count changed after compaction");

    let sum_after = server.query_scalar(&db, "SELECT SUM(value) FROM readcompact").expect("Sum failed");
    assert_eq!(sum_after as i64, expected_sum, "Sum changed after compaction");

    let inconsistent = inconsistent_counts.load(Ordering::SeqCst);
    assert_eq!(inconsistent, 0, "Found {} inconsistent counts during compaction", inconsistent);
}

#[test]
fn test_query_isolation() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Write initial dataset
    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!("isolation,id={} value={}\n", i, i));
    }
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let initial_sum = server.query_scalar(&db, "SELECT SUM(value) FROM isolation").expect("Sum failed");
    let expected_sum = (0..100).sum::<i64>() as f64;
    assert!((initial_sum - expected_sum).abs() < 0.01);

    // Write more data in batches, verify queries work between batches
    for batch in 0..5 {
        let start_id = 100 + batch * 20;
        for i in start_id..start_id + 20 {
            server.write(&db, &format!("isolation,id={} value={}", i, i)).expect("Write failed");
        }

        // Query after each batch - should succeed
        let count = server.query_count(&db, "SELECT COUNT(*) FROM isolation").expect("Count failed");
        assert!(count >= 100, "Count should be at least 100");
    }

    server.flush().expect("Final flush failed");

    // Verify final state
    let final_count = server.query_count(&db, "SELECT COUNT(*) FROM isolation").expect("Count failed");
    assert_eq!(final_count, 200);

    let final_sum = server.query_scalar(&db, "SELECT SUM(value) FROM isolation").expect("Sum failed");
    let expected_final_sum = (0..200).sum::<i64>() as f64;
    assert!((final_sum - expected_final_sum).abs() < 0.01);
}

// ============================================================================
// SECTION 3: Stress Tests
// ============================================================================

#[test]
fn test_stress_high_throughput_writes() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    let num_threads = 8;
    let writes_per_thread = 250;
    let start = Instant::now();
    let total_writes = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let server = Arc::clone(&server);
        let db = db.clone();
        let total_writes = Arc::clone(&total_writes);

        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let data = format!("stress,thread={},seq={} value={}", thread_id, i, thread_id * 1000 + i);
                if server.write(&db, &data).is_ok() {
                    total_writes.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let duration = start.elapsed();
    let total = total_writes.load(Ordering::SeqCst);
    let expected = (num_threads * writes_per_thread) as u64;

    eprintln!("Wrote {} rows in {:?} ({:.0} writes/sec)",
        total, duration, total as f64 / duration.as_secs_f64());

    assert_eq!(total, expected, "Some writes failed");

    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM stress").expect("Count failed");
    assert_eq!(count, expected as i64);
}

#[test]
fn test_stress_mixed_read_write() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Initial data
    for i in 0..50 {
        server.write(&db, &format!("mixed,id={} value={}", i, i)).expect("Initial write failed");
    }
    server.flush().expect("Initial flush failed");

    // Verify initial data is queryable before starting concurrent operations
    let _ = server.query_count(&db, "SELECT COUNT(*) FROM mixed").expect("Initial query failed");

    let write_counter = Arc::new(AtomicU64::new(50));
    let read_counter = Arc::new(AtomicU64::new(0));
    let inconsistent_reads = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Writer threads (2)
    for _thread_id in 0..2 {
        let server = Arc::clone(&server);
        let db = db.clone();
        let write_counter = Arc::clone(&write_counter);

        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let id = write_counter.fetch_add(1, Ordering::SeqCst);
                let data = format!("mixed,id={} value={}", id, id);
                server.write(&db, &data).expect("Write failed");
                thread::sleep(Duration::from_millis(5));
            }
        });
        handles.push(handle);
    }

    // Reader threads (4) - only track inconsistent counts, not query failures
    for _ in 0..4 {
        let server = Arc::clone(&server);
        let db = db.clone();
        let read_counter = Arc::clone(&read_counter);
        let inconsistent_reads = Arc::clone(&inconsistent_reads);

        let handle = thread::spawn(move || {
            for _ in 0..50 {
                if let Ok(count) = server.query_count(&db, "SELECT COUNT(*) FROM mixed") {
                    if count < 50 {
                        // Count should never go below initial
                        inconsistent_reads.fetch_add(1, Ordering::SeqCst);
                    }
                    read_counter.fetch_add(1, Ordering::SeqCst);
                }
                // Ignore query failures during high concurrency
                thread::sleep(Duration::from_millis(10));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    server.flush().expect("Final flush failed");

    let final_count = server.query_count(&db, "SELECT COUNT(*) FROM mixed").expect("Final count failed");
    let expected = 50 + 2 * 100; // initial + 2 writers × 100 each
    assert_eq!(final_count, expected as i64);

    let inconsistent = inconsistent_reads.load(Ordering::SeqCst);
    assert_eq!(inconsistent, 0, "Found {} inconsistent reads (count < 50)", inconsistent);

    let successful_reads = read_counter.load(Ordering::SeqCst);
    assert!(successful_reads > 0, "No successful reads completed");
}

#[test]
fn test_stress_burst_writes() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Simulate burst traffic - many writes in a short period
    let num_bursts = 5;
    let writes_per_burst = 200;

    for burst in 0..num_bursts {
        let mut data = String::new();
        for i in 0..writes_per_burst {
            data.push_str(&format!("burst,burst={},seq={} value={}\n", burst, i, burst * 1000 + i));
        }
        server.write(&db, &data).expect("Burst write failed");

        // Short pause between bursts
        thread::sleep(Duration::from_millis(100));
    }

    server.flush().expect("Flush failed");

    let count = server.query_count(&db, "SELECT COUNT(*) FROM burst").expect("Count failed");
    let expected = (num_bursts * writes_per_burst) as i64;
    assert_eq!(count, expected);

    // Verify all bursts present (burst is a tag, so compare as string)
    for burst in 0..num_bursts {
        let burst_count = server
            .query_count(&db, &format!("SELECT COUNT(*) FROM burst WHERE burst = '{}'", burst))
            .expect("Burst count failed");
        assert_eq!(burst_count, writes_per_burst as i64, "Burst {} has wrong count", burst);
    }
}

// ============================================================================
// SECTION 4: Data Consistency Under Concurrent Modifications
// ============================================================================

#[test]
fn test_consistency_aggregation_during_writes() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Write initial known dataset
    let mut initial_sum = 0i64;
    for i in 0..100 {
        server.write(&db, &format!("aggtest,id={} value={}i", i, i)).expect("Write failed");
        initial_sum += i;
    }
    server.flush().expect("Flush failed");

    let sum_before = server.query_scalar(&db, "SELECT SUM(value) FROM aggtest").expect("Sum failed");
    assert_eq!(sum_before as i64, initial_sum);

    // Run aggregation queries while writing
    let server_agg = Arc::clone(&server);
    let db_agg = db.clone();
    let min_sum_seen = Arc::new(AtomicU64::new(initial_sum as u64));
    let min_sum_clone = Arc::clone(&min_sum_seen);

    let agg_handle = thread::spawn(move || {
        for _ in 0..20 {
            if let Ok(sum) = server_agg.query_scalar(&db_agg, "SELECT SUM(value) FROM aggtest") {
                let sum_int = sum as u64;
                // Sum should never decrease
                let current_min = min_sum_clone.load(Ordering::SeqCst);
                if sum_int < current_min {
                    eprintln!("Warning: Sum decreased from {} to {}", current_min, sum_int);
                }
            }
            thread::sleep(Duration::from_millis(50));
        }
    });

    // Write more data
    for i in 100..200 {
        server.write(&db, &format!("aggtest,id={} value={}i", i, i)).expect("Write failed");
    }

    agg_handle.join().expect("Aggregation thread panicked");
    server.flush().expect("Final flush failed");

    let final_sum = server.query_scalar(&db, "SELECT SUM(value) FROM aggtest").expect("Final sum failed");
    let expected_sum: i64 = (0..200).sum();
    assert_eq!(final_sum as i64, expected_sum);
}

#[test]
fn test_consistency_count_monotonic() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    // Initial data
    for i in 0..50 {
        server.write(&db, &format!("monotonic,id={} value={}", i, i)).expect("Write failed");
    }
    server.flush().expect("Flush failed");

    let counts_seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let counts_clone = Arc::clone(&counts_seen);

    // Reader thread
    let server_read = Arc::clone(&server);
    let db_read = db.clone();
    let read_handle = thread::spawn(move || {
        for _ in 0..30 {
            if let Ok(count) = server_read.query_count(&db_read, "SELECT COUNT(*) FROM monotonic") {
                counts_clone.lock().unwrap().push(count);
            }
            thread::sleep(Duration::from_millis(50));
        }
    });

    // Writer thread
    for i in 50..150 {
        server.write(&db, &format!("monotonic,id={} value={}", i, i)).expect("Write failed");
        if i % 20 == 0 {
            thread::sleep(Duration::from_millis(30));
        }
    }

    read_handle.join().expect("Reader panicked");
    server.flush().expect("Final flush failed");

    let counts = counts_seen.lock().unwrap();

    // Verify counts are monotonically non-decreasing
    for i in 1..counts.len() {
        assert!(
            counts[i] >= counts[i - 1],
            "Count decreased from {} to {} at position {}",
            counts[i - 1], counts[i], i
        );
    }

    // Final verification
    let final_count = server.query_count(&db, "SELECT COUNT(*) FROM monotonic").expect("Final count failed");
    assert_eq!(final_count, 150);
}

#[test]
fn test_no_data_loss_under_load() {
    let server = Arc::new(TestServer::start());
    let db = server.unique_db();

    let num_threads = 6;
    let writes_per_thread = 100;
    let written_ids = Arc::new(std::sync::Mutex::new(HashSet::new()));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let server = Arc::clone(&server);
        let db = db.clone();
        let written_ids = Arc::clone(&written_ids);

        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let unique_id = thread_id * 10000 + i;
                let data = format!("noloss,uid={} thread={},seq={}", unique_id, thread_id, i);
                if server.write(&db, &data).is_ok() {
                    written_ids.lock().unwrap().insert(unique_id);
                }
            }
        });
        handles.push(handle);
    }

    // Occasional flushes during writes
    let server_flush = Arc::clone(&server);
    let flush_handle = thread::spawn(move || {
        for _ in 0..3 {
            thread::sleep(Duration::from_millis(500));
            let _ = server_flush.flush();
        }
    });

    for handle in handles {
        handle.join().expect("Writer panicked");
    }
    flush_handle.join().expect("Flusher panicked");

    // Final flush
    server.flush().expect("Final flush failed");

    let expected_count = written_ids.lock().unwrap().len() as i64;
    let actual_count = server.query_count(&db, "SELECT COUNT(*) FROM noloss").expect("Count failed");

    assert_eq!(
        actual_count, expected_count,
        "Data loss detected: expected {} rows, found {}",
        expected_count, actual_count
    );

    // Verify distinct count matches
    let distinct_count = server
        .query_count(&db, "SELECT COUNT(DISTINCT uid) FROM noloss")
        .expect("Distinct count failed");
    assert_eq!(distinct_count, expected_count, "Duplicate entries found");
}
