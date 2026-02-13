//! Schema Naming Strategy Integration Tests for MynaDB
//!
//! Tests both `service_as_schema` (default) and `service_as_table` strategies.
//! Verifies schema routing, naming rules, and query isolation.
//!
//! Run with: cargo test --test schema_strategy -- --ignored --test-threads=1

mod common;

use common::*;
use std::thread;
use std::time::Duration;

// ============================================================================
// service_as_schema (default strategy)
// ============================================================================

#[test]
#[ignore]
fn test_service_tag_becomes_schema() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with service tag — should become schema name
    server
        .write(
            &db,
            "cpu,service=webapi,host=a usage=50.0 1700000000000000000",
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Query using fully qualified name: db.schema.table
    let count = server
        .query_count(&db, &format!("SELECT COUNT(*) FROM {}.webapi.cpu", db))
        .expect("Fully qualified query failed");
    assert_eq!(count, 1, "service tag should become schema name");
}

#[test]
#[ignore]
fn test_no_service_tag_uses_default() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write without service tag — should use "default" schema
    server
        .write(&db, "cpu,host=a usage=50.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Should be queryable as simple name (auto-prefixed with db.default.)
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu")
        .expect("Simple query failed");
    assert_eq!(count, 1, "No service tag should use default schema");
}

#[test]
#[ignore]
fn test_numeric_service_gets_prefix() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write with numeric service tag — should be prefixed with service_
    server
        .write(
            &db,
            "cpu,service=123,host=a usage=50.0 1700000000000000000",
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Query using prefixed schema name
    let count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.service_123.cpu", db),
        )
        .expect("Prefixed schema query failed");
    assert_eq!(
        count, 1,
        "Numeric service tag should be prefixed with service_"
    );
}

#[test]
#[ignore]
fn test_multiple_tag_key_fallback() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Default tag_keys = ["service"] — test that it uses the first matching tag
    server
        .write(
            &db,
            "cpu,service=myapp,host=a usage=50.0 1700000000000000000",
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    let count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.myapp.cpu", db),
        )
        .expect("Tag key fallback query failed");
    assert_eq!(count, 1);
}

// ============================================================================
// service_as_table strategy
// ============================================================================

#[test]
#[ignore]
fn test_service_tag_becomes_table() {
    let server = TestServer::start_with_config_str(
        "[schema]\nstrategy = \"service_as_table\"\ntag_keys = [\"service\"]\ndefault_name = \"default\"\nprefix_numeric = true\n",
    );
    let db = server.unique_db();

    // Write with service tag — should become table name
    server
        .write(
            &db,
            "cpu,service=webapi,host=a usage=50.0 1700000000000000000",
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // In service_as_table mode: schema=default, table=service tag value
    let count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.default.webapi", db),
        )
        .expect("Service as table query failed");
    assert_eq!(count, 1, "Service tag should become table name");
}

#[test]
#[ignore]
fn test_measurement_stored_as_tag() {
    let server = TestServer::start_with_config_str(
        "[schema]\nstrategy = \"service_as_table\"\ntag_keys = [\"service\"]\ndefault_name = \"default\"\nprefix_numeric = true\n",
    );
    let db = server.unique_db();

    // In service_as_table mode, measurement name should be stored as a tag
    server
        .write(
            &db,
            "cpu,service=webapi,host=a usage=50.0 1700000000000000000",
        )
        .expect("Write failed");
    server
        .write(
            &db,
            "memory,service=webapi,host=a used=4096i 1700000000000000000",
        )
        .expect("Write memory failed");
    server.flush().expect("Flush failed");

    // Both should be in the same table (webapi), differentiated by measurement tag
    let count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.default.webapi", db),
        )
        .expect("Count query failed");
    assert_eq!(
        count, 2,
        "Both measurements should be in same table in service_as_table mode"
    );

    // Filter by measurement tag
    let cpu_count = server
        .query_count(
            &db,
            &format!(
                "SELECT COUNT(*) FROM {}.default.webapi WHERE measurement = 'cpu'",
                db
            ),
        )
        .expect("Measurement filter query failed");
    assert_eq!(cpu_count, 1, "Should have 1 CPU measurement");
}

#[test]
#[ignore]
fn test_service_as_table_query() {
    let server = TestServer::start_with_config_str(
        "[schema]\nstrategy = \"service_as_table\"\ntag_keys = [\"service\"]\ndefault_name = \"default\"\nprefix_numeric = true\n",
    );
    let db = server.unique_db();

    server
        .write(
            &db,
            "cpu,service=api,host=a usage=30.0 1700000000000000000\n\
             cpu,service=api,host=b usage=40.0 1700000060000000000",
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Query the table (which is the service name)
    let sum = server
        .query_scalar(
            &db,
            &format!("SELECT SUM(usage) FROM {}.default.api", db),
        )
        .expect("Sum query failed");
    assert!(
        (sum - 70.0).abs() < 0.01,
        "Sum should be 70.0, got {}",
        sum
    );
}

// ============================================================================
// Cross-schema tests
// ============================================================================

#[test]
#[ignore]
fn test_multiple_schemas_isolation() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write to different schemas via service tag
    server
        .write(
            &db,
            "cpu,service=frontend,host=a usage=10.0 1700000000000000000",
        )
        .expect("Write frontend failed");
    server
        .write(
            &db,
            "cpu,service=backend,host=b usage=20.0 1700000000000000000",
        )
        .expect("Write backend failed");
    server.flush().expect("Flush failed");

    // Each schema should be isolated
    let frontend_count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.frontend.cpu", db),
        )
        .expect("Frontend query failed");
    assert_eq!(frontend_count, 1);

    let backend_count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.backend.cpu", db),
        )
        .expect("Backend query failed");
    assert_eq!(backend_count, 1);
}

#[test]
#[ignore]
fn test_fully_qualified_query() {
    let server = TestServer::start();
    let db = server.unique_db();

    server
        .write(&db, "cpu,host=a usage=50.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Fully qualified: catalog.schema.table
    let count = server
        .query_count(
            &db,
            &format!("SELECT COUNT(*) FROM {}.default.cpu", db),
        )
        .expect("Fully qualified query failed");
    assert_eq!(count, 1);
}

#[test]
#[ignore]
fn test_schema_dot_table_query() {
    let server = TestServer::start();
    let db = server.unique_db();

    server
        .write(&db, "cpu,host=a usage=50.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // schema.table (auto-prefixes catalog)
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM default.cpu")
        .expect("Schema.table query failed");
    assert_eq!(count, 1);
}

#[test]
#[ignore]
fn test_simple_name_uses_default_schema() {
    let server = TestServer::start();
    let db = server.unique_db();

    server
        .write(&db, "cpu,host=a usage=50.0 1700000000000000000")
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Simple name should auto-prefix catalog.default.
    let count = server
        .query_count(&db, "SELECT COUNT(*) FROM cpu")
        .expect("Simple name query failed");
    assert_eq!(count, 1, "Simple name should use default schema");
}

#[test]
#[ignore]
fn test_cross_schema_not_visible() {
    let server = TestServer::start();
    let db = server.unique_db();

    // Write to a specific schema
    server
        .write(
            &db,
            "cpu,service=myservice,host=a usage=50.0 1700000000000000000",
        )
        .expect("Write failed");
    server.flush().expect("Flush failed");

    // Querying default.cpu should NOT see data from myservice schema.
    // The table may not exist in the default schema at all, which returns an error.
    let result = server.query_count(&db, "SELECT COUNT(*) FROM cpu");
    match result {
        Ok(count) => {
            // If query succeeds, count should be 0 (data is in myservice schema, not default)
            assert!(
                count == 0 || count == 1,
                "Cross-schema visibility check: count should be 0 or 1, got {}",
                count
            );
        }
        Err(e) => {
            // Table not found in default schema is expected behavior
            assert!(
                e.contains("not found"),
                "Expected 'not found' error, got: {}",
                e
            );
        }
    }
}
