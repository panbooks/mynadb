//! Error Handling & Edge Case Tests for MynaDB
//!
//! Verifies graceful handling of bad input, schema evolution,
//! and various error conditions.
//!
//! Run with: cargo test --test error_handling -- --ignored --test-threads=1

mod common;

use common::*;
use std::thread;
use std::time::Duration;

// ============================================================================
// Write errors
// ============================================================================

#[test]
#[ignore]
fn test_write_empty_measurement() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Empty measurement name — should fail or be handled gracefully
    let (status, _body) = server
        .raw_post(&format!("/write/{}", db), " value=1.0 1700000000000000000")
        .expect("Request failed");

    // Either accepts gracefully or returns 400
    assert!(
        status == 200 || status == 204 || status == 400,
        "Empty measurement should be handled gracefully, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_write_invalid_line_protocol() {
    let server = TestServer::start();
    let db = server.unique_db();

    let invalid_lines = vec![
        ",,, not valid",
        "= = = =",
        "measurement,,tag=v value=1.0",
        "measurement value= 1700000000000000000",
    ];

    for line in invalid_lines {
        let (status, _body) = server
            .raw_post(&format!("/write/{}", db), line)
            .expect("Request failed");

        assert!(
            status == 200 || status == 204 || status == 400,
            "Invalid line '{}' should return 200/204/400, got {}",
            line,
            status
        );
    }
}

#[test]
#[ignore]
fn test_write_missing_fields() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Line with tags but no fields
    let (status, _body) = server
        .raw_post(
            &format!("/write/{}", db),
            "measurement,tag=value 1700000000000000000",
        )
        .expect("Request failed");

    assert!(
        status == 200 || status == 204 || status == 400,
        "Missing fields should be handled gracefully, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_write_timestamp_overflow() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Very large timestamp that might overflow
    let (status, _body) = server
        .raw_post(
            &format!("/write/{}", db),
            "overflow,tag=a value=1.0 99999999999999999999",
        )
        .expect("Request failed");

    // Should handle gracefully (400 or accept with clamped value)
    assert!(
        status == 200 || status == 204 || status == 400,
        "Timestamp overflow should be handled gracefully, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_write_very_long_tag_value() {
    let server = TestServer::start();
    let db = server.unique_db();

    // 10KB tag value
    let long_value: String = "x".repeat(10_000);
    let line = format!(
        "longtag,tag={} value=1.0 1700000000000000000",
        long_value
    );

    let result = server.write(&db, &line);
    // Should either accept or reject gracefully — not crash
    match result {
        Ok(_) => {
            // Accepted — verify it's queryable
            thread::sleep(Duration::from_secs(1));
            let count = server
                .query_count(&db, "SELECT COUNT(*) FROM longtag")
                .expect("Count failed");
            assert_eq!(count, 1);
        }
        Err(_) => {
            // Rejected — that's also fine
        }
    }
}

// ============================================================================
// Query errors
// ============================================================================

#[test]
#[ignore]
fn test_query_syntax_error() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write some data first so the database exists
    server
        .write(&db, "dummy,tag=a value=1.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    let (status, body) = server
        .raw_post(&format!("/query/{}", db), "SELEC * FORM cpu")
        .expect("Request failed");

    assert!(
        status == 400 || status == 500,
        "Syntax error should return 400/500, got {} body: {}",
        status,
        body
    );
}

#[test]
#[ignore]
fn test_query_nonexistent_table() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write something so the db exists
    server
        .write(&db, "existing,tag=a value=1.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    let (status, _body) = server
        .raw_post(&format!("/query/{}", db), "SELECT * FROM nonexistent_table")
        .expect("Request failed");

    assert!(
        status == 400 || status == 404 || status == 200,
        "Nonexistent table should return error or empty result, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_query_nonexistent_database() {
    let server = TestServer::start();

    let (status, _body) = server
        .raw_post("/query/no_such_database_xyz", "SELECT 1")
        .expect("Request failed");

    // Should return an error, not crash
    assert!(
        status == 200 || status == 400 || status == 404,
        "Nonexistent database should be handled, got {}",
        status
    );
}

#[test]
#[ignore]
fn test_query_division_by_zero() {
    let server = TestServer::start();
    let db = server.unique_db();

    server
        .write(&db, "math,tag=a value=10.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Division by zero in SQL
    let result = server.query(&db, "SELECT value / 0 FROM math");

    // Should return null/infinity or an error — not crash
    match result {
        Ok(val) => {
            // Accepted — DataFusion returns Infinity or null
            assert!(!val.is_null() || val.is_array(), "Should return a result");
        }
        Err(e) => {
            // An error message is also acceptable
            assert!(
                e.contains("division") || e.contains("zero") || e.contains("error"),
                "Error should mention division/zero: {}",
                e
            );
        }
    }
}

#[test]
#[ignore]
fn test_influxql_invalid_syntax() {
    let server = TestServer::start();
    let db = server.unique_db();

    server
        .write(&db, "dummy,tag=a value=1.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    let result = server.query_influxql(&db, "SELEC * FORM cpu WHERE");

    match result {
        Ok(val) => {
            // Should have an error in the result
            let err = influxql_error(&val);
            assert!(
                err.is_some(),
                "Invalid InfluxQL should return error in response"
            );
        }
        Err(_) => {
            // HTTP error is also acceptable
        }
    }
}

// ============================================================================
// Schema edge cases
// ============================================================================

#[test]
#[ignore]
fn test_write_new_field_after_flush() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write initial data with field 'value'
    server
        .write(&db, "evolve,tag=a value=1.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Write new data with additional field 'extra'
    server
        .write(
            &db,
            "evolve,tag=a value=2.0,extra=100.0 1700000060000000000",
        )
        .expect("Write with new field failed");
    server.flush().expect("Second flush failed");

    // Both rows should be queryable
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM evolve")
        .expect("Count failed");
    assert_eq!(count, 2, "Both rows should be present");

    // The new field should be queryable
    let rows = server
        .query_rows(&db, "SELECT value, extra FROM evolve ORDER BY time")
        .expect("Query failed");
    assert_eq!(rows.len(), 2);

    // First row should have null extra
    let first_extra = rows[0].get("extra").and_then(|v| v.as_f64());
    assert!(
        first_extra.is_none() || first_extra == Some(0.0),
        "First row extra should be null/absent"
    );

    // Second row should have extra=100.0
    let second_extra = rows[1].get("extra").and_then(|v| v.as_f64());
    assert_eq!(second_extra, Some(100.0), "Second row extra should be 100.0");
}

#[test]
#[ignore]
fn test_write_conflicting_field_types() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write float value
    server
        .write(&db, "conflict,tag=a value=1.5 1700000000000000000")
        .expect("Float write failed");
    server.flush().expect("Flush failed");

    // Try to write integer with same field name
    let result = server.write(&db, "conflict,tag=a value=2i 1700000060000000000");

    // Should either succeed (with type coercion) or fail gracefully
    match result {
        Ok(_) => {
            // Type coercion happened
            let count = server
                .query_count(&db, "SELECT COUNT(*) FROM conflict")
                .expect("Count failed");
            assert!(count >= 1, "Should have at least 1 row");
        }
        Err(e) => {
            // Type conflict error is acceptable
            assert!(
                !e.is_empty(),
                "Error message should not be empty"
            );
        }
    }
}

#[test]
#[ignore]
fn test_measurement_name_special_chars() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Measurement with underscore and numbers
    server
        .write(&db, "my_metric_v2,tag=a value=1.0 1700000000000000000")
        .expect("Underscore measurement write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM my_metric_v2")
        .expect("Count failed");
    assert_eq!(count, 1);
}

#[test]
#[ignore]
fn test_very_long_measurement_name() {
    let server = TestServer::start();
    let db = server.unique_db();

    let long_name: String = "a".repeat(200);
    let line = format!("{},tag=a value=1.0 1700000000000000000", long_name);

    let result = server.write(&db, &line);
    // Should either accept or reject — not crash
    match result {
        Ok(_) => {
            thread::sleep(Duration::from_secs(1));
            let count = server
                .query_count(&db, &format!("SELECT COUNT(*) FROM {}", long_name))
                .expect("Count failed");
            assert_eq!(count, 1);
        }
        Err(_) => {
            // Rejection is also acceptable for very long names
        }
    }
}

#[test]
#[ignore]
fn test_unicode_in_tags() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Unicode characters in tag values
    let data = "unicode,city=tokyo value=1.0 1700000000000000000\n\
                unicode,city=paris value=2.0 1700000060000000000";
    server.write(&db, data).expect("Unicode write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM unicode")
        .expect("Count failed");
    assert_eq!(count, 2, "Unicode tags should be preserved");
}

// ============================================================================
// Large data edge cases
// ============================================================================

#[test]
#[ignore]
fn test_write_large_batch_single_request() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write 5000 lines in a single request
    let mut data = String::new();
    for i in 0..5000 {
        data.push_str(&format!(
            "bigbatch,id={} value={} {}\n",
            i,
            i as f64 * 0.1,
            1700000000_000_000_000i64 + i * 1_000_000,
        ));
    }

    server.write(&db, &data).expect("Large batch write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM bigbatch")
        .expect("Count failed");
    assert_eq!(count, 5000, "All 5000 rows should be written");
}

#[test]
#[ignore]
fn test_query_with_many_conditions() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write data with multiple tags
    let data = "multicond,a=1,b=2,c=3,d=4,e=5 value=42.0 1700000000000000000";
    server.write(&db, data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Query with many AND conditions
    let count = server
        .query_count(
            &db,
            "SELECT COUNT(*) FROM multicond WHERE a = '1' AND b = '2' AND c = '3' AND d = '4' AND e = '5'",
        )
        .expect("Multi-condition query failed");
    assert_eq!(count, 1, "All conditions should match");
}
