//! Benchmarks for sled DB performance in MetaStateMachine
//!
//! Measures performance characteristics for different data scales:
//! - File-level routing: ~60M entries (deprecated)
//! - Table-level routing: ~100K entries (current)
//! - Partition replicas: ~1M entries
//! - Nodes: ~100s entries
//!
//! Tests include:
//! - Sequential inserts
//! - Random lookups
//! - Prefix scans
//! - Mixed workloads

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sled::Db;
use std::time::Instant;
use tempfile::TempDir;

/// Key prefixes matching MetaStateMachine
mod keys {
    pub const PREFIX_FILE: &str = "/files/";
    pub const PREFIX_TABLE: &str = "/tables/";
    pub const PREFIX_REPLICA: &str = "/replicas/";
    pub const PREFIX_NODE: &str = "/nodes/";
}

/// Generate a file key (old file-level routing)
/// Format: /files/{catalog}/{schema}/{table}/date={date}/{filename}
fn generate_file_key(i: usize) -> String {
    let catalog = format!("catalog_{}", i % 10);
    let schema = format!("schema_{}", i % 100);
    let table = format!("table_{}", i % 1000);
    let date = format!("2024-01-{:02}", (i % 31) + 1);
    let filename = format!("L0_{:016x}.parquet", i);
    format!(
        "{}{}/{}/{}/date={}/{}",
        keys::PREFIX_FILE,
        catalog,
        schema,
        table,
        date,
        filename
    )
}

/// Generate a table key (new table-level routing)
/// Format: /tables/{catalog}/{schema}/{table}
fn generate_table_key(i: usize) -> String {
    let catalog = format!("catalog_{}", i % 10);
    let schema = format!("schema_{}", i % 100);
    let table = format!("table_{}", i % 1000);
    format!("{}{}/{}/{}", keys::PREFIX_TABLE, catalog, schema, table)
}

/// Generate a replica key (partition replicas)
/// Format: /replicas/{catalog}/{schema}/{table}/date={date}
fn generate_replica_key(i: usize) -> String {
    let catalog = format!("catalog_{}", i % 10);
    let schema = format!("schema_{}", i % 100);
    let table = format!("table_{}", i % 1000);
    let date = format!("2024-01-{:02}", (i % 31) + 1);
    format!(
        "{}{}/{}/{}/date={}",
        keys::PREFIX_REPLICA,
        catalog,
        schema,
        table,
        date
    )
}

/// Generate a node key
/// Format: /nodes/{node_id}
fn generate_node_key(i: usize) -> String {
    format!("{}data-node-{:04}", keys::PREFIX_NODE, i)
}

/// Simulated FileInfo value (~200 bytes typical)
fn generate_file_value(i: usize) -> Vec<u8> {
    let data = format!(
        r#"{{"path":"s3://bucket/catalog_{}/schema_{}/table_{}/date=2024-01-{:02}/L0_{:016x}.parquet","size":{},"row_count":{},"created_by":"data-node-{:04}","created_at":1700000000000}}"#,
        i % 10,
        i % 100,
        i % 1000,
        (i % 31) + 1,
        i,
        (i % 100) * 1024 * 1024,
        (i % 10000) * 100,
        i % 16
    );
    data.into_bytes()
}

/// Simulated TableMeta value (~150 bytes typical)
fn generate_table_value(i: usize) -> Vec<u8> {
    let data = format!(
        r#"{{"catalog":"catalog_{}","schema":"schema_{}","table":"table_{}","owner_node_id":"data-node-{:04}","version":{},"total_bytes":{},"total_rows":{},"file_count":{},"partition_count":{}}}"#,
        i % 10,
        i % 100,
        i % 1000,
        i % 16,
        i % 100,
        (i % 100) * 1024 * 1024 * 1024,
        (i % 10000) * 100000,
        (i % 1000) * 10,
        i % 365
    );
    data.into_bytes()
}

/// Simulated PartitionReplica value (~100 bytes typical)
fn generate_replica_value(i: usize) -> Vec<u8> {
    let data = format!(
        r#"{{"partition_key":"catalog_{}/schema_{}/table_{}/date=2024-01-{:02}","leader_node_id":"data-node-{:04}","standby_node_ids":["data-node-{:04}"],"replication_factor":2}}"#,
        i % 10,
        i % 100,
        i % 1000,
        (i % 31) + 1,
        i % 16,
        (i + 1) % 16
    );
    data.into_bytes()
}

/// Simulated NodeInfo value (~300 bytes typical)
fn generate_node_value(i: usize) -> Vec<u8> {
    let zone_suffix = ['a', 'b', 'c'][i % 3];
    let data = format!(
        r#"{{"node_id":"data-node-{:04}","role":"Data","state":"Active","mode":"Primary","endpoint":"10.0.{}.{}:7001","registered_at":1700000000000,"last_heartbeat":1700000001000,"labels":{{"zone":"us-east-1{}","rack":"rack-{}"}}}}"#,
        i,
        i / 256,
        i % 256,
        zone_suffix,
        i % 10
    );
    data.into_bytes()
}

/// Create a temporary sled database
fn create_temp_db() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let db = sled::Config::new()
        .path(dir.path())
        .cache_capacity(256 * 1024 * 1024) // 256MB cache
        .flush_every_ms(Some(1000))
        .open()
        .unwrap();
    (dir, db)
}

// ============================================================================
// Insert Benchmarks
// ============================================================================

fn bench_insert_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_files");
    group.sample_size(10); // Reduce sample size for large benchmarks

    for &count in &[1_000, 10_000, 100_000] {
        let (_dir, db) = create_temp_db();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("sequential", count),
            &count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for iter in 0..iters {
                        let start = Instant::now();
                        for i in 0..count {
                            let key = generate_file_key(iter as usize * count + i);
                            let value = generate_file_value(iter as usize * count + i);
                            let _ = db.insert(key.as_bytes(), value);
                        }
                        let _ = db.flush();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_insert_tables(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_tables");
    group.sample_size(10);

    for &count in &[1_000, 10_000, 100_000] {
        let (_dir, db) = create_temp_db();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("sequential", count),
            &count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for iter in 0..iters {
                        let start = Instant::now();
                        for i in 0..count {
                            let key = generate_table_key(iter as usize * count + i);
                            let value = generate_table_value(iter as usize * count + i);
                            let _ = db.insert(key.as_bytes(), value);
                        }
                        let _ = db.flush();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Lookup Benchmarks (after pre-populating)
// ============================================================================

fn bench_lookup_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_files");

    for &count in &[1_000, 10_000, 100_000] {
        let (_dir, db) = create_temp_db();

        // Pre-populate
        for i in 0..count {
            let key = generate_file_key(i);
            let value = generate_file_value(i);
            let _ = db.insert(key.as_bytes(), value);
        }
        let _ = db.flush();

        // Generate lookup keys
        let lookup_keys: Vec<String> = (0..1000).map(|i| generate_file_key(i % count)).collect();

        group.throughput(Throughput::Elements(1000));
        group.bench_with_input(
            BenchmarkId::new("random_1k_lookups", count),
            &lookup_keys,
            |b, keys| {
                b.iter(|| {
                    for key in keys {
                        let _ = black_box(db.get(key.as_bytes()));
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_lookup_tables(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_tables");

    for &count in &[1_000, 10_000, 100_000] {
        let (_dir, db) = create_temp_db();

        // Pre-populate
        for i in 0..count {
            let key = generate_table_key(i);
            let value = generate_table_value(i);
            let _ = db.insert(key.as_bytes(), value);
        }
        let _ = db.flush();

        // Generate lookup keys
        let lookup_keys: Vec<String> = (0..1000).map(|i| generate_table_key(i % count)).collect();

        group.throughput(Throughput::Elements(1000));
        group.bench_with_input(
            BenchmarkId::new("random_1k_lookups", count),
            &lookup_keys,
            |b, keys| {
                b.iter(|| {
                    for key in keys {
                        let _ = black_box(db.get(key.as_bytes()));
                    }
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Prefix Scan Benchmarks
// ============================================================================

fn bench_scan_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_prefix");
    group.sample_size(10);

    // Test scanning with different DB sizes
    for &count in &[10_000, 100_000] {
        let (_dir, db) = create_temp_db();

        // Pre-populate with files
        for i in 0..count {
            let key = generate_file_key(i);
            let value = generate_file_value(i);
            let _ = db.insert(key.as_bytes(), value);
        }
        let _ = db.flush();

        // Scan all files for a specific table (approx count/1000 entries)
        let prefix = format!(
            "{}catalog_0/schema_0/table_0/",
            keys::PREFIX_FILE
        );

        group.bench_with_input(
            BenchmarkId::new("single_table_files", count),
            &prefix,
            |b, prefix| {
                b.iter(|| {
                    let mut found = 0;
                    for item in db.scan_prefix(prefix.as_bytes()) {
                        if let Ok((_, _)) = item {
                            found += 1;
                        }
                    }
                    black_box(found)
                });
            },
        );
    }

    // Test scanning tables (much smaller dataset)
    {
        let (_dir, db) = create_temp_db();

        // Pre-populate with tables
        for i in 0..100_000 {
            let key = generate_table_key(i);
            let value = generate_table_value(i);
            let _ = db.insert(key.as_bytes(), value);
        }
        let _ = db.flush();

        // Scan all tables for a specific catalog (approx 10K entries)
        let prefix = format!("{}catalog_0/", keys::PREFIX_TABLE);

        group.bench_function("tables_for_catalog", |b| {
            b.iter(|| {
                let mut found = 0;
                for item in db.scan_prefix(prefix.as_bytes()) {
                    if let Ok((_, _)) = item {
                        found += 1;
                    }
                }
                black_box(found)
            });
        });

        // Scan all tables (full scan)
        group.bench_function("all_tables_100k", |b| {
            b.iter(|| {
                let mut found = 0;
                for item in db.scan_prefix(keys::PREFIX_TABLE.as_bytes()) {
                    if let Ok((_, _)) = item {
                        found += 1;
                    }
                }
                black_box(found)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Scale Comparison: File-level vs Table-level
// ============================================================================

fn bench_scale_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale_comparison");
    group.sample_size(10);

    // Simulate the actual scale difference:
    // - File-level: 100 catalogs × 100 schemas × 100 tables × 365 days × ~16 files/day = ~60M
    // - Table-level: 100 catalogs × 100 schemas × 100 tables = 1M (but typically ~100K active)

    // File-level simulation (100K entries = 0.17% of 60M)
    {
        let (_dir, db) = create_temp_db();

        group.bench_function("insert_100k_files", |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    for i in 0..100_000 {
                        let key = generate_file_key(i);
                        let value = generate_file_value(i);
                        let _ = db.insert(key.as_bytes(), value);
                    }
                    let _ = db.flush();
                    total += start.elapsed();
                    // Clear for next iteration
                    let _ = db.clear();
                }
                total
            });
        });
    }

    // Table-level simulation (100K entries)
    {
        let (_dir, db) = create_temp_db();

        group.bench_function("insert_100k_tables", |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    for i in 0..100_000 {
                        let key = generate_table_key(i);
                        let value = generate_table_value(i);
                        let _ = db.insert(key.as_bytes(), value);
                    }
                    let _ = db.flush();
                    total += start.elapsed();
                    // Clear for next iteration
                    let _ = db.clear();
                }
                total
            });
        });
    }

    group.finish();
}

// ============================================================================
// Mixed Workload (Read + Write)
// ============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(10);

    for &count in &[10_000, 100_000] {
        let (_dir, db) = create_temp_db();

        // Pre-populate
        for i in 0..count {
            let key = generate_table_key(i);
            let value = generate_table_value(i);
            let _ = db.insert(key.as_bytes(), value);
        }
        let _ = db.flush();

        // 80% reads, 20% writes
        group.throughput(Throughput::Elements(1000));
        group.bench_with_input(
            BenchmarkId::new("80_read_20_write", count),
            &count,
            |b, &count| {
                let mut write_idx = count;
                b.iter(|| {
                    for i in 0..1000 {
                        if i % 5 == 0 {
                            // Write (20%)
                            let key = generate_table_key(write_idx);
                            let value = generate_table_value(write_idx);
                            let _ = black_box(db.insert(key.as_bytes(), value));
                            write_idx += 1;
                        } else {
                            // Read (80%)
                            let key = generate_table_key(i % count);
                            let _ = black_box(db.get(key.as_bytes()));
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Large Scale Test (1M entries)
// ============================================================================

fn bench_large_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    // 1M tables (simulating large deployment)
    {
        let (_dir, db) = create_temp_db();

        // Pre-populate 1M entries
        println!("Pre-populating 1M entries...");
        for i in 0..1_000_000 {
            let key = generate_table_key(i);
            let value = generate_table_value(i);
            let _ = db.insert(key.as_bytes(), value);
            if i % 100_000 == 0 {
                let _ = db.flush();
            }
        }
        let _ = db.flush();
        println!("Done pre-populating.");

        // Random lookups in 1M dataset
        let lookup_keys: Vec<String> = (0..1000)
            .map(|i| generate_table_key((i * 997) % 1_000_000))
            .collect();

        group.throughput(Throughput::Elements(1000));
        group.bench_function("lookup_1k_in_1m", |b| {
            b.iter(|| {
                for key in &lookup_keys {
                    let _ = black_box(db.get(key.as_bytes()));
                }
            });
        });

        // Prefix scan in 1M dataset (all tables for one catalog = ~100K)
        let prefix = format!("{}catalog_0/", keys::PREFIX_TABLE);
        group.bench_function("scan_catalog_in_1m", |b| {
            b.iter(|| {
                let mut found = 0;
                for item in db.scan_prefix(prefix.as_bytes()) {
                    if let Ok((_, _)) = item {
                        found += 1;
                    }
                }
                black_box(found)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Memory and DB Size Analysis
// ============================================================================

fn bench_db_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("db_size");
    group.sample_size(10);

    // Measure DB file size for different entry counts
    for &count in &[10_000, 100_000] {
        let (dir, db) = create_temp_db();

        // Insert entries
        for i in 0..count {
            let key = generate_table_key(i);
            let value = generate_table_value(i);
            let _ = db.insert(key.as_bytes(), value);
        }
        let _ = db.flush();

        // Force compaction
        drop(db);

        // Measure directory size
        let mut total_size = 0u64;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            if let Ok(entry) = entry {
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        println!(
            "DB size for {} entries: {:.2} MB ({:.0} bytes/entry)",
            count,
            total_size as f64 / (1024.0 * 1024.0),
            total_size as f64 / count as f64
        );

        group.bench_function(format!("measure_size_{}", count), |b| {
            b.iter(|| black_box(total_size));
        });
    }

    group.finish();
}

// ============================================================================
// Progressive Scale Test
// ============================================================================

/// Test how performance degrades as DB grows
fn bench_progressive_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("progressive_scale");
    group.sample_size(10);

    let (_dir, db) = create_temp_db();
    let mut current_count = 0;

    for &target in &[1_000, 10_000, 100_000, 500_000] {
        // Add entries to reach target
        while current_count < target {
            let key = generate_table_key(current_count);
            let value = generate_table_value(current_count);
            let _ = db.insert(key.as_bytes(), value);
            current_count += 1;
        }
        let _ = db.flush();

        // Benchmark lookups at this scale
        let lookup_keys: Vec<String> = (0..1000)
            .map(|i| generate_table_key((i * 997) % target))
            .collect();

        group.throughput(Throughput::Elements(1000));
        group.bench_with_input(
            BenchmarkId::new("lookup_at_scale", target),
            &lookup_keys,
            |b, keys| {
                b.iter(|| {
                    for key in keys {
                        let _ = black_box(db.get(key.as_bytes()));
                    }
                });
            },
        );

        // Benchmark inserts at this scale
        group.bench_with_input(
            BenchmarkId::new("insert_at_scale", target),
            &target,
            |b, &_target| {
                let mut idx = current_count;
                b.iter(|| {
                    for _ in 0..100 {
                        let key = generate_table_key(idx);
                        let value = generate_table_value(idx);
                        let _ = black_box(db.insert(key.as_bytes(), value));
                        idx += 1;
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_files,
    bench_insert_tables,
    bench_lookup_files,
    bench_lookup_tables,
    bench_scan_prefix,
    bench_scale_comparison,
    bench_mixed_workload,
    bench_db_size,
    bench_progressive_scale,
);

// Large scale test is separate due to long runtime
criterion_group! {
    name = large_scale_benches;
    config = Criterion::default().sample_size(10).measurement_time(std::time::Duration::from_secs(60));
    targets = bench_large_scale,
}

criterion_main!(benches, large_scale_benches);
