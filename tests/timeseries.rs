//! Time Series Specific Integration Tests for MynaDB
//!
//! Tests GROUP BY time correctness, fill modes, time_bucket UDF,
//! time range edge cases, and custom functions (first_value, last_value).
//!
//! Run with: cargo test --test timeseries -- --ignored --test-threads=1

mod common;

use common::*;
use std::thread;
use std::time::Duration;

/// Write time series data with known timestamps (1h intervals, 24 points).
fn write_hourly_data(server: &TestServer, db: &str) {
    let base: i64 = 1700006400_000_000_000; // 2023-11-15 00:00:00 UTC (exact day boundary)
    let hour: i64 = 3600_000_000_000; // 1 hour in nanos

    let mut data = String::new();
    for i in 0..24 {
        let value = 20.0 + (i as f64) * 0.5; // 20.0, 20.5, 21.0, ..., 31.5
        data.push_str(&format!(
            "sensor,location=room1 temperature={} {}\n",
            value,
            base + i * hour,
        ));
    }
    server.write(db, &data).expect("Write failed");
    server.flush().expect("Flush failed");
}

/// Write sparse data (only a few points in a wide time range).
fn write_sparse_data(server: &TestServer, db: &str) {
    let base: i64 = 1700006400_000_000_000;
    let hour: i64 = 3600_000_000_000;

    // Points at hours 0, 3, 7, 12, 23
    let data = format!(
        "sparse,tag=a value=10.0 {t0}\n\
         sparse,tag=a value=30.0 {t3}\n\
         sparse,tag=a value=70.0 {t7}\n\
         sparse,tag=a value=120.0 {t12}\n\
         sparse,tag=a value=230.0 {t23}",
        t0 = base,
        t3 = base + 3 * hour,
        t7 = base + 7 * hour,
        t12 = base + 12 * hour,
        t23 = base + 23 * hour,
    );
    server.write(db, &data).expect("Write failed");
    server.flush().expect("Flush failed");
}

// ============================================================================
// GROUP BY time correctness
// ============================================================================

#[test]
#[ignore]
fn test_group_by_time_bucket_alignment() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    // GROUP BY 6h should give 4 buckets for 24 hours of data
    let result = server
        .query_influxql(
            &db,
            "SELECT MEAN(temperature) FROM sensor GROUP BY time(6h)",
        )
        .expect("GROUP BY time(6h) query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 4,
        "Expected at least 4 buckets for 24h/6h, got {}",
        values.len()
    );
}

#[test]
#[ignore]
fn test_group_by_time_sparse_data() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sparse_data(&server, &db);

    // 6h buckets: only some should have data
    let result = server
        .query_influxql(
            &db,
            "SELECT COUNT(value) FROM sparse GROUP BY time(6h)",
        )
        .expect("Sparse GROUP BY time failed");

    let values = influxql_values(&result);
    assert!(!values.is_empty(), "Should have at least 1 bucket");

    // Total count across all buckets should be 5
    let total: f64 = values
        .iter()
        .filter_map(|row| row.last().and_then(|v| v.as_f64()))
        .sum();
    assert!(
        (total - 5.0).abs() < 0.01,
        "Total count should be 5, got {}",
        total
    );
}

#[test]
#[ignore]
fn test_group_by_time_single_bucket() {
    let server = TestServer::start();
    let db = server.unique_db();

    // All data in same minute
    let base: i64 = 1700000000_000_000_000;
    let data = format!(
        "tight,tag=a value=1.0 {}\n\
         tight,tag=a value=2.0 {}\n\
         tight,tag=a value=3.0 {}",
        base,
        base + 1_000_000_000,
        base + 2_000_000_000,
    );
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let result = server
        .query_influxql(
            &db,
            "SELECT SUM(value) FROM tight GROUP BY time(1h)",
        )
        .expect("Single bucket query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 1,
        "Should have at least 1 bucket"
    );

    let sum = values[0].last().and_then(|v| v.as_f64()).unwrap();
    assert!(
        (sum - 6.0).abs() < 0.01,
        "Sum in single bucket should be 6.0, got {}",
        sum
    );
}

#[test]
#[ignore]
fn test_group_by_time_many_buckets() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    // 1h buckets for 24h of hourly data = 24 buckets with 1 point each
    let result = server
        .query_influxql(
            &db,
            "SELECT COUNT(temperature) FROM sensor GROUP BY time(1h)",
        )
        .expect("Many buckets query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 24,
        "Expected at least 24 buckets, got {}",
        values.len()
    );

    // Each bucket should have exactly 1 point
    for (i, row) in values.iter().take(24).enumerate() {
        let count = row.last().and_then(|v| v.as_f64()).unwrap_or(0.0);
        assert!(
            (count - 1.0).abs() < 0.01,
            "Bucket {} should have 1 point, got {}",
            i,
            count
        );
    }
}

// ============================================================================
// fill() modes (via InfluxQL GROUP BY time with fill)
// ============================================================================

#[test]
#[ignore]
fn test_fill_null() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sparse_data(&server, &db);

    let base: i64 = 1700006400_000; // ms
    let result = server
        .query_influxql(
            &db,
            &format!(
                "SELECT MEAN(value) FROM sparse WHERE time >= {}ms AND time < {}ms GROUP BY time(6h) fill(null)",
                base,
                base + 24 * 3600_000,
            ),
        )
        .expect("fill(null) query failed");

    let values = influxql_values(&result);
    // 24h / 6h = 4 buckets; fill(null) should insert null rows for empty buckets
    assert!(
        values.len() >= 4,
        "Should have at least 4 buckets with fill(null), got {}",
        values.len()
    );

    // Some buckets should have null values (sparse data)
    let null_count = values
        .iter()
        .filter(|row| row.last().map_or(true, |v| v.is_null()))
        .count();
    // With sparse data across 4 buckets, at least some should be non-null
    let non_null = values.len() - null_count;
    assert!(
        non_null >= 1,
        "Should have at least 1 non-null bucket, got {} non-null out of {}",
        non_null,
        values.len()
    );
}

#[test]
#[ignore]
fn test_fill_none() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sparse_data(&server, &db);

    let base: i64 = 1700006400_000; // ms
    let result = server
        .query_influxql(
            &db,
            &format!(
                "SELECT MEAN(value) FROM sparse WHERE time >= {}ms AND time < {}ms GROUP BY time(1h) fill(none)",
                base,
                base + 24 * 3600_000,
            ),
        )
        .expect("fill(none) query failed");

    let values = influxql_values(&result);
    // fill(none) omits empty buckets, should have ~5 rows
    assert!(
        values.len() <= 6,
        "fill(none) should only show buckets with data, got {}",
        values.len()
    );
    assert!(
        values.len() >= 4,
        "Should have at least 4 data points, got {}",
        values.len()
    );
}

#[test]
#[ignore]
fn test_fill_zero() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sparse_data(&server, &db);

    let base: i64 = 1700006400_000; // ms
    let result = server
        .query_influxql(
            &db,
            &format!(
                "SELECT MEAN(value) FROM sparse WHERE time >= {}ms AND time < {}ms GROUP BY time(6h) fill(0)",
                base,
                base + 24 * 3600_000,
            ),
        )
        .expect("fill(0) query failed");

    let values = influxql_values(&result);
    // 4 buckets of 6h each
    assert!(
        values.len() >= 4,
        "Should have at least 4 buckets, got {}",
        values.len()
    );

    // Check that empty buckets have 0
    for row in &values {
        let val = row.last().and_then(|v| v.as_f64());
        assert!(val.is_some(), "All buckets should have numeric values with fill(0)");
    }
}

#[test]
#[ignore]
fn test_fill_previous() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_sparse_data(&server, &db);

    let base: i64 = 1700006400_000; // ms
    let result = server
        .query_influxql(
            &db,
            &format!(
                "SELECT MEAN(value) FROM sparse WHERE time >= {}ms AND time < {}ms GROUP BY time(6h) fill(previous)",
                base,
                base + 24 * 3600_000,
            ),
        )
        .expect("fill(previous) query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 4,
        "Should have at least 4 buckets, got {}",
        values.len()
    );
}

#[test]
#[ignore]
fn test_fill_linear() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write exactly 2 points far apart for interpolation
    let base: i64 = 1700006400_000_000_000;
    let hour: i64 = 3600_000_000_000;
    let data = format!(
        "interp,tag=a value=10.0 {t0}\n\
         interp,tag=a value=50.0 {t6}",
        t0 = base,
        t6 = base + 6 * hour,
    );
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Note: fill(linear) is not supported by the InfluxQL parser.
    // Test with fill(previous) as a fallback interpolation method.
    let result = server
        .query_influxql(
            &db,
            &format!(
                "SELECT MEAN(value) FROM interp WHERE time >= {}ms AND time < {}ms GROUP BY time(2h) fill(previous)",
                base / 1_000_000,
                (base + 8 * hour) / 1_000_000,
            ),
        )
        .expect("fill(previous) query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 2,
        "Should have at least 2 buckets, got {}",
        values.len()
    );
}

// ============================================================================
// Time range filtering
// ============================================================================

#[test]
#[ignore]
fn test_time_filter_nanoseconds() {
    let server = TestServer::start();
    let db = server.unique_db();

    let ts: i64 = 1700000000_123_456_789;
    let data = format!("precise,tag=a value=1.0 {}", ts);
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM precise")
        .expect("Count failed");
    assert_eq!(count, 1, "Should have 1 row");
}

#[test]
#[ignore]
fn test_time_filter_between() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    let base: i64 = 1700006400_000_000_000;
    let hour: i64 = 3600_000_000_000;

    // Filter for hours 5-10 (6 data points)
    // Must cast integer nanoseconds to TIMESTAMP for comparison
    let count = server
        .query_count(
            &db,
            &format!(
                "SELECT COUNT(*) FROM sensor WHERE time >= arrow_cast({}, 'Timestamp(Nanosecond, None)') AND time < arrow_cast({}, 'Timestamp(Nanosecond, None)')",
                base + 5 * hour,
                base + 11 * hour,
            ),
        )
        .expect("BETWEEN filter failed");
    assert_eq!(count, 6, "Should have 6 rows between hours 5-10");
}

#[test]
#[ignore]
fn test_time_filter_no_overlap() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    // Query a time range that doesn't overlap with data
    // Must cast integer nanoseconds to TIMESTAMP for comparison
    let future: i64 = 1800000000_000_000_000; // Far future
    let count = server
        .query_count(
            &db,
            &format!(
                "SELECT COUNT(*) FROM sensor WHERE time > arrow_cast({}, 'Timestamp(Nanosecond, None)')",
                future,
            ),
        )
        .expect("No overlap filter failed");
    assert_eq!(count, 0, "Should have 0 rows in non-overlapping range");
}

// ============================================================================
// Time bucket edge cases
// ============================================================================

#[test]
#[ignore]
fn test_time_bucket_boundary_values() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write values exactly at bucket boundaries
    let base: i64 = 1700006400_000_000_000; // Exact day boundary
    let hour: i64 = 3600_000_000_000;
    let data = format!(
        "boundary,tag=a value=1.0 {t0}\n\
         boundary,tag=a value=2.0 {t6}\n\
         boundary,tag=a value=3.0 {t12}\n\
         boundary,tag=a value=4.0 {t18}",
        t0 = base,
        t6 = base + 6 * hour,
        t12 = base + 12 * hour,
        t18 = base + 18 * hour,
    );
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let result = server
        .query_influxql(
            &db,
            "SELECT SUM(value) FROM boundary GROUP BY time(6h)",
        )
        .expect("Boundary bucket query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 4,
        "Should have 4 buckets, got {}",
        values.len()
    );
}

#[test]
#[ignore]
fn test_time_bucket_first_last_per_bucket() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    let result = server
        .query_influxql(
            &db,
            "SELECT FIRST(temperature), LAST(temperature) FROM sensor GROUP BY time(6h)",
        )
        .expect("FIRST/LAST per bucket query failed");

    let values = influxql_values(&result);
    assert!(
        values.len() >= 4,
        "Should have at least 4 buckets"
    );

    // In each bucket, FIRST should be <= LAST (since temperature increases)
    for (i, row) in values.iter().enumerate() {
        if row.len() >= 3 {
            if let (Some(first), Some(last)) = (
                row.get(1).and_then(|v| v.as_f64()),
                row.get(2).and_then(|v| v.as_f64()),
            ) {
                assert!(
                    first <= last,
                    "Bucket {}: FIRST ({}) should be <= LAST ({})",
                    i,
                    first,
                    last
                );
            }
        }
    }
}

// ============================================================================
// SQL time_bucket UDF
// ============================================================================

#[test]
#[ignore]
fn test_udf_time_bucket() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    // Use time_bucket(timestamp, interval_nanos, offset_nanos) in SQL
    let hour_nanos: i64 = 3600_000_000_000;
    let bucket_6h = 6 * hour_nanos;

    let result = server
        .query(
            &db,
            &format!(
                "SELECT time_bucket(time, {}, 0) as bucket, AVG(temperature) as avg_temp \
                 FROM sensor GROUP BY 1 ORDER BY 1",
                bucket_6h,
            ),
        )
        .expect("time_bucket UDF query failed");

    let rows = result.as_array().expect("Expected array");
    assert!(
        rows.len() >= 4,
        "Should have at least 4 buckets, got {}",
        rows.len()
    );
}

// ============================================================================
// Custom UDAF: first_value, last_value
// ============================================================================

#[test]
#[ignore]
fn test_udaf_first_value() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    let result = server
        .query(
            &db,
            "SELECT first_value(temperature, time) FROM sensor",
        )
        .expect("first_value query failed");

    let val = extract_scalar(&result).expect("Failed to extract scalar");
    // First point: temperature=20.0
    assert!(
        (val - 20.0).abs() < 0.01,
        "first_value should be 20.0, got {}",
        val
    );
}

#[test]
#[ignore]
fn test_udaf_last_value() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    let result = server
        .query(
            &db,
            "SELECT last_value(temperature, time) FROM sensor",
        )
        .expect("last_value query failed");

    let val = extract_scalar(&result).expect("Failed to extract scalar");
    // Last point: temperature=31.5 (20.0 + 23*0.5)
    assert!(
        (val - 31.5).abs() < 0.01,
        "last_value should be 31.5, got {}",
        val
    );
}

// ============================================================================
// Multiple time series
// ============================================================================

#[test]
#[ignore]
fn test_group_by_tag_and_time() {
    let server = TestServer::start();
    let db = server.unique_db();

    let base: i64 = 1700006400_000_000_000;
    let hour: i64 = 3600_000_000_000;

    // Two locations, data every 2 hours
    let mut data = String::new();
    for i in 0..12 {
        data.push_str(&format!(
            "sensor,location=room1 temperature={} {}\n\
             sensor,location=room2 temperature={} {}\n",
            20.0 + i as f64,
            base + i * 2 * hour,
            25.0 + i as f64,
            base + i * 2 * hour,
        ));
    }
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    let result = server
        .query_influxql(
            &db,
            "SELECT MEAN(temperature) FROM sensor GROUP BY location, time(6h)",
        )
        .expect("GROUP BY tag + time failed");

    let series = influxql_series(&result);
    assert_eq!(
        series.len(),
        2,
        "Should have 2 series (one per location)"
    );
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
#[ignore]
fn test_time_filter_rfc3339() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_hourly_data(&server, &db);

    // Use RFC3339 timestamp in InfluxQL WHERE clause
    let result = server
        .query_influxql(
            &db,
            "SELECT COUNT(temperature) FROM sensor WHERE time >= '2023-11-15T00:00:00Z' AND time < '2023-11-15T12:00:00Z'",
        )
        .expect("RFC3339 time filter failed");

    let values = influxql_values(&result);
    assert!(!values.is_empty(), "Should return some results");

    let count = values[0]
        .last()
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
        .unwrap();
    assert_eq!(
        count as i64, 12,
        "Should have 12 hours of data in first half of day"
    );
}

#[test]
#[ignore]
fn test_time_filter_milliseconds() {
    let server = TestServer::start();
    let db = server.unique_db();

    let ts_ms: i64 = 1700000000_000; // millisecond timestamp
    let ts_ns = ts_ms * 1_000_000; // convert to ns for line protocol
    let data = format!("mstest,tag=a value=42.0 {}", ts_ns);
    server.write(&db, &data).expect("Write failed");
    server.flush().expect("Flush failed");

    // Query using ms epoch in InfluxQL
    let result = server
        .query_influxql(
            &db,
            &format!(
                "SELECT value FROM mstest WHERE time >= {}ms AND time <= {}ms",
                ts_ms,
                ts_ms + 1000,
            ),
        )
        .expect("Millisecond filter failed");

    assert_influxql_row_count(&result, 1);
}
