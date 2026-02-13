//! Authentication Integration Tests for MynaDB
//!
//! Tests HTTP Basic Auth enforcement on various endpoints.
//!
//! Run with: cargo test --test auth -- --ignored --test-threads=1

mod common;

use common::*;
use std::thread;
use std::time::Duration;

const AUTH_USER: &str = "admin";
const AUTH_PASS: &str = "secret123";

// ============================================================================
// Auth enabled: rejection tests
// ============================================================================

#[test]
#[ignore]
fn test_auth_enabled_rejects_unauthenticated() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);
    let db = server.unique_db();

    // Write without auth should fail
    let (status, _body) = server
        .raw_post(
            &format!("/write/{}", db),
            "cpu,host=a value=1.0 1700000000000000000",
        )
        .expect("Request failed");

    assert_eq!(
        status, 401,
        "Unauthenticated write should return 401, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_auth_enabled_accepts_valid_credentials() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);
    let db = server.unique_db();

    // Write with valid auth should succeed
    let result = server.write_with_auth(
        &db,
        "cpu,host=a value=1.0 1700000000000000000",
        AUTH_USER,
        AUTH_PASS,
    );
    assert!(
        result.is_ok(),
        "Write with valid auth should succeed: {:?}",
        result
    );

    // Flush with auth
    let (status, _) = server
        .raw_post_with_auth("/flush", "", AUTH_USER, AUTH_PASS)
        .expect("Flush request failed");
    assert!(
        status == 200 || status == 204,
        "Authenticated flush should return 200/204, got {}",
        status
    );

    thread::sleep(Duration::from_secs(2));

    // Query with valid auth should succeed
    let result = server.query_with_auth(&db, "SELECT COUNT(*) FROM cpu", AUTH_USER, AUTH_PASS);
    assert!(
        result.is_ok(),
        "Query with valid auth should succeed: {:?}",
        result
    );
}

#[test]
#[ignore]
fn test_auth_enabled_rejects_wrong_password() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);
    let db = server.unique_db();

    // Write with wrong password
    let (status, _body) = server
        .raw_post_with_auth(
            &format!("/write/{}", db),
            "cpu,host=a value=1.0 1700000000000000000",
            AUTH_USER,
            "wrong_password",
        )
        .expect("Request failed");

    assert_eq!(
        status, 401,
        "Wrong password should return 401, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_auth_public_endpoints_accessible() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);

    // /health should be accessible without auth
    let (status, _body) = server.raw_get("/health").expect("Health check failed");
    assert_eq!(
        status, 200,
        "/health should be accessible without auth, got {}",
        status
    );

    // /stats should be accessible without auth
    let (status, _body) = server.raw_get("/stats").expect("Stats failed");
    assert_eq!(
        status, 200,
        "/stats should be accessible without auth, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_auth_write_requires_auth() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);
    let db = server.unique_db();

    // Write without auth
    let (status, _) = server
        .raw_post(
            &format!("/write/{}", db),
            "cpu value=1.0 1700000000000000000",
        )
        .expect("Request failed");
    assert_eq!(status, 401, "Write should require auth");
}

#[test]
#[ignore]
fn test_auth_query_requires_auth() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);
    let db = server.unique_db();

    // First write with auth so data exists
    server
        .write_with_auth(
            &db,
            "cpu,host=a value=1.0 1700000000000000000",
            AUTH_USER,
            AUTH_PASS,
        )
        .expect("Auth write failed");

    // Query without auth
    let (status, _) = server
        .raw_post(&format!("/query/{}", db), "SELECT * FROM cpu")
        .expect("Request failed");
    assert_eq!(status, 401, "Query should require auth");
}

#[test]
#[ignore]
fn test_auth_influxql_requires_auth() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);
    let db = server.unique_db();

    // InfluxQL query without auth
    let (status, _) = server
        .raw_get(&format!(
            "/query?db={}&q=SELECT%20*%20FROM%20cpu&epoch=ms",
            db
        ))
        .expect("Request failed");
    assert_eq!(status, 401, "InfluxQL query should require auth");

    // InfluxQL query with auth should work
    let result = server.query_influxql_with_auth(
        &db,
        "SHOW DATABASES",
        AUTH_USER,
        AUTH_PASS,
    );
    assert!(
        result.is_ok(),
        "InfluxQL with auth should work: {:?}",
        result
    );
}

#[test]
#[ignore]
fn test_auth_disabled_allows_all() {
    // Start server without auth (default)
    let server = TestServer::start();
    let db = server.unique_db();

    // Write without auth should succeed
    let result = server.write(&db, "cpu,host=a value=1.0 1700000000000000000");
    assert!(
        result.is_ok(),
        "Write should succeed without auth when disabled"
    );

    // Query without auth should succeed
    thread::sleep(Duration::from_secs(1));
    let result = server.query(&db, "SELECT COUNT(*) FROM cpu");
    assert!(
        result.is_ok(),
        "Query should succeed without auth when disabled"
    );
}

#[test]
#[ignore]
fn test_auth_health_always_public() {
    // With auth enabled
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);

    let (status, _) = server.raw_get("/health").expect("Health check failed");
    assert_eq!(status, 200, "Health should always be public");
}

#[test]
#[ignore]
fn test_auth_flush_requires_auth() {
    let server = TestServer::start_with_auth(AUTH_USER, AUTH_PASS);

    // Flush without auth
    let (status, _) = server.raw_post("/flush", "").expect("Request failed");
    assert_eq!(status, 401, "Flush should require auth");

    // Flush with auth
    let (status, _) = server
        .raw_post_with_auth("/flush", "", AUTH_USER, AUTH_PASS)
        .expect("Request failed");
    assert!(
        status == 200 || status == 204,
        "Flush with auth should succeed, got {}",
        status
    );
}
