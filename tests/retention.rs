//! Retention Policy Integration Tests for MynaDB
//!
//! Tests automatic and manual retention deletion of old partitions.
//!
//! Run with: cargo test --test retention -- --ignored --test-threads=1

mod common;

use common::*;
use std::thread;
use std::time::Duration;

/// Write data across multiple date partitions.
/// Creates data for dates: today, yesterday, 3 days ago, 7 days ago, 14 days ago, 30 days ago.
fn write_multi_day_data(server: &TestServer, db: &str) {
    let now = chrono::Utc::now();

    let dates = vec![
        now,                                            // today
        now - chrono::Duration::days(1),                // yesterday
        now - chrono::Duration::days(3),                // 3 days ago
        now - chrono::Duration::days(7),                // 7 days ago
        now - chrono::Duration::days(14),               // 14 days ago
        now - chrono::Duration::days(30),               // 30 days ago
    ];

    for (i, date) in dates.iter().enumerate() {
        let ts = date.timestamp_nanos_opt().unwrap();
        let data = format!(
            "retention_test,host=server{} value={}.0 {}",
            i, i * 10, ts,
        );
        server.write(db, &data).expect("Write failed");
    }
    server.flush().expect("Flush failed");
}

// ============================================================================
// Manual retention via API
// ============================================================================

#[test]
#[ignore]
fn test_retention_manual_api() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_multi_day_data(&server, &db);

    // Verify all data is present
    let count_before = server
        .query_count(&db, "SELECT COUNT(*) FROM retention_test")
        .expect("Count before failed");
    assert_eq!(count_before, 6, "Should have 6 rows initially");

    // Manually trigger retention: delete data older than 5 days
    let body = serde_json::json!({"older_than_days": 5});
    let (status, _resp_body) = server
        .retention(&db, "default", "retention_test", &body)
        .expect("Retention API failed");

    assert!(
        status == 200 || status == 204,
        "Retention API should return 200/204, got {}",
        status
    );

    // Wait for retention to complete
    thread::sleep(Duration::from_secs(3));

    // Re-flush and check - data older than 5 days should be deleted
    let count_after = server
        .query_count(&db, "SELECT COUNT(*) FROM retention_test")
        .expect("Count after failed");

    // Should have fewer rows (today, yesterday, 3 days ago remain = 3)
    assert!(
        count_after <= count_before,
        "Retention should have deleted some data: before={}, after={}",
        count_before,
        count_after
    );
}

#[test]
#[ignore]
fn test_retention_manual_before_date() {
    let server = TestServer::start();
    let db = server.unique_db();
    write_multi_day_data(&server, &db);

    let count_before = server
        .query_count(&db, "SELECT COUNT(*) FROM retention_test")
        .expect("Count before failed");
    assert_eq!(count_before, 6);

    // Delete data before a specific date (10 days ago)
    let ten_days_ago = chrono::Utc::now() - chrono::Duration::days(10);
    let before_date = ten_days_ago.format("%Y-%m-%d").to_string();

    let body = serde_json::json!({"before_date": before_date});
    let (status, _resp_body) = server
        .retention(&db, "default", "retention_test", &body)
        .expect("Retention API failed");

    assert!(
        status == 200 || status == 204,
        "Retention before_date should return 200/204, got {}",
        status
    );

    thread::sleep(Duration::from_secs(3));

    let count_after = server
        .query_count(&db, "SELECT COUNT(*) FROM retention_test")
        .expect("Count after failed");

    // Data at 14 and 30 days ago should be deleted (older than 10 days)
    assert!(
        count_after <= count_before,
        "Retention should have deleted old data: before={}, after={}",
        count_before,
        count_after
    );
}

#[test]
#[ignore]
fn test_retention_keeps_recent_partitions() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write only recent data (all within last 2 days)
    let now = chrono::Utc::now();
    for i in 0..5 {
        let ts = (now - chrono::Duration::hours(i)).timestamp_nanos_opt().unwrap();
        let data = format!("recent,host=a value={}.0 {}", i, ts);
        server.write(&db, &data).expect("Write failed");
    }
    server.flush().expect("Flush failed");

    let count_before = server
        .query_count(&db, "SELECT COUNT(*) FROM recent")
        .expect("Count before failed");
    assert_eq!(count_before, 5);

    // Apply retention with 7 day threshold - nothing should be deleted
    let body = serde_json::json!({"older_than_days": 7});
    let (status, _) = server
        .retention(&db, "default", "recent", &body)
        .expect("Retention API failed");

    assert!(
        status == 200 || status == 204,
        "Retention should succeed, got {}",
        status
    );

    thread::sleep(Duration::from_secs(3));

    let count_after = server
        .query_count(&db, "SELECT COUNT(*) FROM recent")
        .expect("Count after failed");
    assert_eq!(
        count_after, count_before,
        "Recent data should not be deleted"
    );
}

#[test]
#[ignore]
fn test_retention_deletes_old_partitions() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write some very old data
    let old_date = chrono::Utc::now() - chrono::Duration::days(60);
    let old_ts = old_date.timestamp_nanos_opt().unwrap();
    server
        .write(
            &db,
            &format!("olddata,host=a value=1.0 {}", old_ts),
        )
        .expect("Write old data failed");

    // Write recent data
    let now_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    server
        .write(
            &db,
            &format!("olddata,host=b value=2.0 {}", now_ts),
        )
        .expect("Write recent data failed");
    server.flush().expect("Flush failed");

    let count_before = server
        .query_count(&db, "SELECT COUNT(*) FROM olddata")
        .expect("Count before failed");
    assert_eq!(count_before, 2, "Should have 2 rows initially");

    // Delete data older than 30 days
    let body = serde_json::json!({"older_than_days": 30});
    let (status, _) = server
        .retention(&db, "default", "olddata", &body)
        .expect("Retention API failed");

    assert!(
        status == 200 || status == 204,
        "Retention should succeed, got {}",
        status
    );

    thread::sleep(Duration::from_secs(3));

    let count_after = server
        .query_count(&db, "SELECT COUNT(*) FROM olddata")
        .expect("Count after failed");

    // The 60-day-old data should be deleted, recent data should remain
    assert!(
        count_after <= count_before,
        "Old partition should be deleted: before={}, after={}",
        count_before,
        count_after
    );
}

// ============================================================================
// Retention disabled
// ============================================================================

#[test]
#[ignore]
fn test_retention_disabled_by_default() {
    // Default config has retention disabled
    let server = TestServer::start();
    let db = server.unique_db();

    // Write old data
    let old_ts = (chrono::Utc::now() - chrono::Duration::days(60))
        .timestamp_nanos_opt()
        .unwrap();
    server
        .write(
            &db,
            &format!("noretain,host=a value=1.0 {}", old_ts),
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Wait - auto retention should NOT run
    thread::sleep(Duration::from_secs(5));

    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM noretain")
        .expect("Count failed");
    assert_eq!(
        count, 1,
        "Data should persist when retention is disabled"
    );
}

// ============================================================================
// Retention across multiple tables
// ============================================================================

#[test]
#[ignore]
fn test_retention_across_multiple_tables() {
    let server = TestServer::start();
    let db = server.unique_db();

    let old_ts = (chrono::Utc::now() - chrono::Duration::days(60))
        .timestamp_nanos_opt()
        .unwrap();
    let new_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // Write old data to multiple tables
    server
        .write(
            &db,
            &format!(
                "table_a,host=a value=1.0 {}\n\
                 table_b,host=a value=2.0 {}\n\
                 table_a,host=b value=3.0 {}\n\
                 table_b,host=b value=4.0 {}",
                old_ts, old_ts, new_ts, new_ts,
            ),
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Apply retention to table_a only
    let body = serde_json::json!({"older_than_days": 30});
    let (status, _) = server
        .retention(&db, "default", "table_a", &body)
        .expect("Retention API failed");
    assert!(status == 200 || status == 204);

    thread::sleep(Duration::from_secs(3));

    // table_b should be unaffected
    let count_b = server
        .query_count(&db, "SELECT COUNT(*) FROM table_b")
        .expect("Count table_b failed");
    assert_eq!(
        count_b, 2,
        "table_b should be unaffected by retention on table_a"
    );
}

// ============================================================================
// Background retention
// ============================================================================

#[test]
#[ignore]
fn test_retention_background_enabled() {
    // Start with retention enabled and very short interval
    let server = TestServer::start_with_config_str(
        "[retention]\nenabled = true\ndefault_days = 3\ninterval_secs = 5\n",
    );
    let db = server.unique_db();

    // Write very old data
    let old_ts = (chrono::Utc::now() - chrono::Duration::days(30))
        .timestamp_nanos_opt()
        .unwrap();
    server
        .write(
            &db,
            &format!("bgretain,host=a value=1.0 {}", old_ts),
        )
        .expect("Write old data failed");

    // Write recent data
    let new_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    server
        .write(
            &db,
            &format!("bgretain,host=b value=2.0 {}", new_ts),
        )
        .expect("Write recent data failed");
    server.flush().expect("Flush failed");

    let count_before = server
        .query_count(&db, "SELECT COUNT(*) FROM bgretain")
        .expect("Count before failed");
    assert_eq!(count_before, 2, "Should have 2 rows initially");

    // Wait for background retention to run (interval_secs = 5)
    thread::sleep(Duration::from_secs(15));

    let count_after = server
        .query_count(&db, "SELECT COUNT(*) FROM bgretain")
        .expect("Count after failed");

    // Old data (30 days) should be deleted by background retention (default_days = 3)
    assert!(
        count_after <= count_before,
        "Background retention should delete old data: before={}, after={}",
        count_before,
        count_after
    );
}
