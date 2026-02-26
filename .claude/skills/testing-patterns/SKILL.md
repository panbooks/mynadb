---
name: testing-patterns
description: This skill should be used when the user is writing tests for MynaDB — unit tests, integration tests, benchmarks, or test scripts. Also use when discussing test infrastructure, mock data generation, property-based testing, or the TestServer harness.
version: 1.0.0
---

# Testing Patterns in MynaDB

## Test Organization

| Type | Location | Run Command |
|------|----------|-------------|
| Unit tests | `src/**/tests.rs` or `#[cfg(test)]` modules | `cargo test --lib` |
| Integration tests | `tests/*.rs` | `cargo test --test api_endpoints -- --ignored` |
| Benchmarks | `benches/*.rs` | `cargo bench --bench query_bench` |
| E2E scripts | `scripts/**/*.sh` | `bash scripts/test_*.sh` |
| Property tests | `tests/property.rs` | `cargo test --test property -- --ignored` |

## Unit Test Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_behavior() {
        let config = Config::default();
        let result = my_function(&config);
        assert_eq!(result, expected);
    }
}
```

Use `tempfile::TempDir` for tests needing filesystem:
```rust
#[test]
fn test_storage_write() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_local(temp.path());
    // ... test with auto-cleanup on drop
}
```

## Integration Test Pattern (TestServer)

**File:** `tests/common/mod.rs`

```rust
#[test]
#[ignore]  // Requires --ignored flag (starts actual server)
fn test_write_and_query() {
    let server = TestServer::start();
    let db = server.unique_db();  // "testdb_N" (atomic counter)

    server.write(&db, "cpu,host=a usage=50.0 1700000000000000000").unwrap();
    server.flush().unwrap();

    let count = server.query_count(&db, "SELECT COUNT(*) FROM cpu").unwrap();
    assert_eq!(count, 1);
}
```

### TestServer API

**Startup variants:**
- `TestServer::start()` — default config, temp directory
- `TestServer::start_with_config_str(toml)` — append custom TOML
- `TestServer::start_with_auth(user, pass)` — auth-enabled
- `TestServer::start_with_retention(days, interval)` — retention-enabled

**Write operations:**
- `server.write(db, data)` — POST /write/:db
- `server.write_gzip(db, bytes)` — gzip-compressed
- `server.write_with_auth(db, data, user, pass)`
- `server.flush()` — POST /flush
- `server.compact(db)` — POST /compact/:db

**Query operations:**
- `server.query(db, sql)` — raw JSON response
- `server.query_scalar(db, sql)` — first numeric value
- `server.query_count(db, sql)` — COUNT(*) result
- `server.query_rows(db, sql)` — `Vec<HashMap<String, Value>>`

**InfluxQL:**
- `server.query_influxql(db, query)` — GET /query
- `server.query_influxql_post(db, query)` — POST /query

**Metadata:**
- `server.tables(db)`, `server.schema_info(db, table)`
- `server.explain(db, sql)`, `server.analyze(db, sql)`
- `server.health()`, `server.stats()`, `server.memory()`
- `server.cache_stats()`, `server.cache_clear()`

### Response Parsing Helpers

```rust
extract_scalar(&json_value)      // First numeric value from result
extract_count(&json_value)       // Count from COUNT(*) query
extract_rows(&json_value)        // Vec<HashMap> of all rows
influxql_series(&result)         // Extract series array
influxql_values(&result)         // [[values]] from first series
influxql_columns(&result)        // Column names
influxql_row_count(&result)      // Total rows across series
```

## Port Allocation

```rust
pub fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind")
        .local_addr().unwrap().port()
}
```

Enables parallel test execution without port conflicts.

## Property-Based Testing

**File:** `tests/property.rs`

```rust
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_valid_line_parses(
        measurement in "[a-z][a-z0-9]{0,15}",
        value in any::<f64>().prop_filter("finite", |f| f.is_finite() && f.abs() < 1e15),
    ) {
        let line = format!("{} value={}", measurement, value);
        prop_assert!(parse_line_simple(&line).is_ok());
    }
}
```

## Benchmarking with Criterion

**Files:** `benches/*.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn bench_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("line_protocol");

    for size in [100, 1000, 10000] {
        let data = generate_lines(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("parse", size),
            &data,
            |b, data| b.iter(|| parse_multiple_lines(black_box(data), &config)),
        );
    }
    group.finish();
}

criterion_group!(benches, bench_parsing);
criterion_main!(benches);
```

**Key patterns:**
- `black_box()` prevents dead code elimination
- `Throughput::Elements()` for per-item metrics
- Seeded `StdRng::seed_from_u64(42)` for reproducibility
- `tokio::runtime::Runtime::new()` for async benchmarks
- `group.sample_size(50)` for expensive operations

### Available Benchmarks

| Bench | What It Measures |
|-------|-----------------|
| `line_protocol_bench` | Parsing throughput (simple, complex, batch) |
| `query_bench` | SELECT, aggregation, filter, GROUP BY, joins, window functions |
| `time_functions_bench` | time_bucket, first_value, last_value UDFs |
| `sled_bench` | Metadata storage backend performance |

## Concurrency Testing

```rust
#[test]
#[ignore]
fn test_concurrent_writes() {
    let server = TestServer::start();
    let db = server.unique_db();

    let handles: Vec<_> = (0..10).map(|i| {
        let server = server.clone();  // Arc internally
        let db = db.clone();
        std::thread::spawn(move || {
            server.write(&db, &format!("m,id={} v={}", i, i)).unwrap();
        })
    }).collect();

    for h in handles { h.join().unwrap(); }
    server.flush().unwrap();

    let count = server.query_count(&db, "SELECT COUNT(*) FROM m").unwrap();
    assert_eq!(count, 10);
}
```

## Mock Data Generation

**Line protocol:**
```rust
// Simple
"cpu,host=server01 usage=50.0 1700000000000000000"

// Parameterized
format!("metric,id={} value={} {}", i, i * 10, base_ts + i * 1000)

// Complex tags
format!("telemetry,service=svc{},host=server{},region=us-west cpu={:.1}", i%10, i%100, val)
```

**Arrow RecordBatch:**
```rust
fn create_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]));
    let time_array = TimestampNanosecondArray::from(vec![1_000_000_000i64; num_rows]);
    let value_array = Float64Array::from(vec![42.0; num_rows]);
    RecordBatch::try_new(schema, vec![Arc::new(time_array), Arc::new(value_array)]).unwrap()
}
```

## Shell Script Tests

**Pattern:** `scripts/` organized by category.

```bash
BASE_URL="http://localhost:7000"
DB="testdb"

# Write → flush → query → assert
curl -X POST $BASE_URL/write/$DB -d 'cpu,host=a usage=50.0'
curl -X POST $BASE_URL/flush
sleep 1
RESULT=$(curl -s "$BASE_URL/query/$DB?sql=SELECT+COUNT(*)+FROM+cpu")
echo "$RESULT" | grep -q '"1"' && echo "PASS" || echo "FAIL"
```

## Known Issues

- `test_estimate_savings` in `query::cache::dictionary_encoding` is flaky (expects >30% savings, sometimes gets ~6%)
- Integration tests should use `--test-threads=1` for stability
- Linter may strip functions only used by binary crate — use `#[allow(dead_code)]` or full path qualifiers

## Dev Dependencies

```toml
[dev-dependencies]
tempfile = "3"        # Temp directories
ureq = "2"            # Sync HTTP client
serde_json = "1"      # JSON parsing
proptest = "1"        # Property-based testing
criterion = "0.5"     # Benchmarks
rand = "0.8"          # Seeded randomness
walkdir = "2"         # Directory traversal
flate2 = "1"          # Gzip compression
base64 = "0.22"       # Encoding
```
