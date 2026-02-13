//! API Endpoint Coverage Tests for MynaDB
//!
//! Ensures all REST endpoints return correct status codes and response formats.
//! Covers health, monitoring, write, query, schema, compaction, and cache endpoints.
//!
//! Run with: cargo test --test api_endpoints -- --ignored --test-threads=1

mod common;

use common::*;
use std::io::Write as IoWrite;
use std::thread;
use std::time::Duration;

/// Write sample data for endpoint tests.
fn write_sample_data(server: &TestServer, db: &str) {
    let data = "cpu,host=server01,region=us-east usage=50.0,idle=50.0 1700000000000000000\n\
                cpu,host=server02,region=eu-west usage=60.0,idle=40.0 1700000060000000000\n\
                memory,host=server01 used=4096i,total=8192i 1700000000000000000";
    server.write(db, data).expect("Write failed");
    server.flush().expect("Flush failed");
}

// ============================================================================
// Health & monitoring
// ============================================================================

#[test]
#[ignore]
fn test_health_endpoint() {
    let server = TestServer::start();

    let (status, body) = server.health().expect("Health check failed");
    assert_eq!(status, 200, "Health should return 200");
    // Body should indicate healthy status
    assert!(
        !body.is_empty(),
        "Health response should not be empty"
    );
}

#[test]
#[ignore]
fn test_stats_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    let (status, body) = server.stats().expect("Stats check failed");
    assert_eq!(status, 200, "Stats should return 200");
    assert!(!body.is_empty(), "Stats response should not be empty");
}

#[test]
#[ignore]
fn test_memory_endpoint() {
    let server = TestServer::start();

    let (status, body) = server.memory().expect("Memory check failed");
    assert_eq!(status, 200, "Memory should return 200");
    assert!(!body.is_empty(), "Memory response should not be empty");
}

#[test]
#[ignore]
fn test_ping_endpoint() {
    let server = TestServer::start();

    let (status, _body) = server.raw_get("/ping").expect("Ping failed");
    assert!(
        status == 200 || status == 204,
        "Ping should return 200 or 204, got {}",
        status
    );
}

// ============================================================================
// Write & flush
// ============================================================================

#[test]
#[ignore]
fn test_write_endpoint_success() {
    let server = TestServer::start();
    let db = server.unique_db();

    let result = server.write(&db, "test_write,host=a value=1.0 1700000000000000000");
    assert!(result.is_ok(), "Write should succeed");

    // Verify data was written
    thread::sleep(Duration::from_secs(1));
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM test_write")
        .expect("Count query failed");
    assert_eq!(count, 1, "Should have 1 row");
}

#[test]
#[ignore]
fn test_write_endpoint_invalid_line_protocol() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Completely invalid line protocol
    let (status, _body) = server
        .raw_post(&format!("/write/{}", db), "this is not valid line protocol!!!")
        .expect("Request failed");

    // Server should return 400 or handle gracefully
    // Some implementations accept partial data, so we just check it doesn't crash
    assert!(
        status == 200 || status == 204 || status == 400,
        "Should return 200, 204 or 400 for invalid data, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_write_endpoint_gzip() {
    let server = TestServer::start();
    let db = server.unique_db();

    let line_protocol = "gzip_test,host=a value=42.0 1700000000000000000";

    // Compress with gzip
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(line_protocol.as_bytes())
        .expect("Gzip encode failed");
    let compressed = encoder.finish().expect("Gzip finish failed");

    server
        .write_gzip(&db, &compressed)
        .expect("Gzip write failed");

    thread::sleep(Duration::from_secs(1));
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM gzip_test")
        .expect("Count failed");
    assert_eq!(count, 1, "Gzip write should produce 1 row");

    let val = server
        .query_scalar(&db, "SELECT value FROM gzip_test")
        .expect("Query failed");
    assert!(
        (val - 42.0).abs() < 0.01,
        "Value should be 42.0, got {}",
        val
    );
}

#[test]
#[ignore]
fn test_write_endpoint_empty_body() {
    let server = TestServer::start();
    let db = server.unique_db();

    let (status, _body) = server
        .raw_post(&format!("/write/{}", db), "")
        .expect("Request failed");

    // Empty body should either be accepted (no-op) or return 400
    assert!(
        status == 200 || status == 204 || status == 400,
        "Should handle empty body gracefully, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_flush_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write data first
    server
        .write(&db, "flush_test,host=a value=1.0 1700000000000000000")
        .expect("Write failed");

    let result = server.flush();
    assert!(result.is_ok(), "Flush should succeed");
}

// ============================================================================
// Query endpoints
// ============================================================================

#[test]
#[ignore]
fn test_query_get_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    // GET /query/:db?sql=...
    let url = format!("/query/{}?sql=SELECT%20COUNT(*)%20FROM%20cpu", db);
    let (status, body) = server.raw_get(&url).expect("GET query failed");
    assert_eq!(status, 200, "GET query should return 200");
    assert!(!body.is_empty(), "Query response should not be empty");
}

#[test]
#[ignore]
fn test_query_post_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    let result = server
        .query(&db, "SELECT COUNT(*) FROM cpu")
        .expect("POST query failed");

    let count = extract_count(&result).expect("Failed to extract count");
    assert_eq!(count, 2, "Should have 2 CPU rows");
}

#[test]
#[ignore]
fn test_explain_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    let result = server
        .explain(&db, "SELECT * FROM cpu WHERE host = 'server01'")
        .expect("Explain failed");

    // Should have plan information
    assert!(
        result.get("logical_plan").is_some() || result.get("physical_plan").is_some() || result.is_string(),
        "Explain should return plan info, got: {:?}",
        result
    );
}

#[test]
#[ignore]
fn test_analyze_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    let result = server
        .analyze(&db, "SELECT * FROM cpu")
        .expect("Analyze failed");

    // Should return analysis info (tables, partitions, etc.)
    assert!(
        !result.is_null(),
        "Analyze should return non-null result"
    );
}

// ============================================================================
// Schema & metadata
// ============================================================================

#[test]
#[ignore]
fn test_tables_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    let result = server.tables(&db).expect("Tables endpoint failed");

    // Should list at least cpu and memory
    let tables_str = result.to_string();
    assert!(
        tables_str.contains("cpu") || result.get("tables").is_some(),
        "Tables response should contain 'cpu': {:?}",
        result
    );
}

#[test]
#[ignore]
fn test_schema_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    // The /schema/:db/:schema/:table endpoint returns DFSchema debug format, not JSON
    let url = format!("/schema/{}/default/cpu", db);
    let (status, body) = server.raw_get(&url).expect("Schema endpoint failed");
    assert_eq!(status, 200, "Schema endpoint should return 200, got {}", status);
    // Body should contain field names from our data
    assert!(
        body.contains("usage") && body.contains("host"),
        "Schema response should contain field names, got: {}",
        &body[..body.len().min(200)]
    );
}

#[test]
#[ignore]
fn test_partitions_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    let result = server
        .partitions(&db, "default", "cpu")
        .expect("Partitions endpoint failed");

    // Should return partition info
    assert!(
        !result.is_null(),
        "Partitions response should not be null"
    );
}

// ============================================================================
// Compaction
// ============================================================================

#[test]
#[ignore]
fn test_compact_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write multiple batches and flush to create L0 files
    for i in 0..3 {
        server
            .write(
                &db,
                &format!("compact_test,host=a value={} {}", i, 1700000000_000_000_000i64 + i * 60_000_000_000),
            )
            .expect("Write failed");
        server.flush().expect("Flush failed");
    }

    let result = server.compact(&db);
    assert!(result.is_ok(), "Compact should succeed");

    // Data should still be intact
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM compact_test")
        .expect("Count failed");
    assert_eq!(count, 3, "Data should be preserved after compaction");
}

#[test]
#[ignore]
fn test_compact_specific_measurement() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    // Compact specific measurement
    let url = format!("/compact/{}/cpu", db);
    let (status, _) = server.raw_post(&url, "").expect("Compact request failed");
    // Should succeed or return reasonable status
    assert!(
        status == 200 || status == 204 || status == 404,
        "Compact specific measurement should return 200/204/404, got {}",
        status
    );
}

// ============================================================================
// Cache
// ============================================================================

#[test]
#[ignore]
fn test_cache_stats_endpoint() {
    let server = TestServer::start();

    let result = server.cache_stats().expect("Cache stats failed");
    assert!(
        !result.is_null(),
        "Cache stats should return non-null result"
    );
}

#[test]
#[ignore]
fn test_cache_clear_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    // Query to populate cache
    let _ = server.query(&db, "SELECT COUNT(*) FROM cpu");

    let result = server.cache_clear().expect("Cache clear failed");
    assert!(
        !result.is_null(),
        "Cache clear should return non-null result"
    );
}

// ============================================================================
// InfluxDB-compatible endpoints
// ============================================================================

#[test]
#[ignore]
fn test_influxdb_write_compat_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();

    // InfluxDB-compatible write: POST /write?db=...
    let url = format!("/write?db={}", db);
    let (status, _) = server
        .raw_post(&url, "compat_test,host=a value=1.0 1700000000000000000")
        .expect("InfluxDB compat write failed");

    assert!(
        status == 200 || status == 204,
        "InfluxDB compat write should return 200/204, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_influxdb_query_compat_endpoint() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sample_data(&server, &db);

    // InfluxDB-compatible query: GET /query?db=...&q=...
    let result = server
        .query_influxql(&db, "SELECT COUNT(usage) FROM cpu")
        .expect("InfluxDB compat query failed");

    let values = influxql_values(&result);
    assert!(!values.is_empty(), "Should return query results");
}
