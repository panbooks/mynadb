//! Benchmarks for query execution
//!
//! Measures query performance for:
//! - Simple SELECT queries
//! - Aggregation queries (SUM, AVG, COUNT, MIN, MAX)
//! - Filter queries with predicates
//! - GROUP BY queries
//! - Time range queries
//! - Complex analytical queries

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

/// Generate test data with controllable size
fn generate_test_data(rows: usize, seed: u64) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(seed);
    let base_ts: i64 = 1700000000_000_000_000;

    // Generate data
    let timestamps: Vec<i64> = (0..rows)
        .map(|i| base_ts + (i as i64 * 1_000_000_000))
        .collect();

    let dates: Vec<i32> = (0..rows).map(|i| 19700 + (i as i32 % 30)).collect(); // days since epoch

    let hosts: Vec<String> = (0..rows)
        .map(|i| format!("server{:03}", i % 100))
        .collect();

    let regions: Vec<String> = (0..rows)
        .map(|i| {
            match i % 4 {
                0 => "us-east",
                1 => "us-west",
                2 => "eu-west",
                _ => "ap-south",
            }
            .to_string()
        })
        .collect();

    let cpu_values: Vec<f64> = (0..rows).map(|_| rng.gen_range(0.0..100.0)).collect();

    let memory_values: Vec<f64> = (0..rows).map(|_| rng.gen_range(0.0..100.0)).collect();

    let disk_values: Vec<f64> = (0..rows).map(|_| rng.gen_range(0.0..100.0)).collect();

    let request_counts: Vec<i64> = (0..rows).map(|_| rng.gen_range(0..10000)).collect();

    let error_counts: Vec<i64> = (0..rows).map(|_| rng.gen_range(0..100)).collect();

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("date", DataType::Int32, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("cpu", DataType::Float64, false),
        Field::new("memory", DataType::Float64, false),
        Field::new("disk", DataType::Float64, false),
        Field::new("requests", DataType::Int64, false),
        Field::new("errors", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(Int32Array::from(dates)) as ArrayRef,
            Arc::new(StringArray::from(hosts)) as ArrayRef,
            Arc::new(StringArray::from(regions)) as ArrayRef,
            Arc::new(Float64Array::from(cpu_values)) as ArrayRef,
            Arc::new(Float64Array::from(memory_values)) as ArrayRef,
            Arc::new(Float64Array::from(disk_values)) as ArrayRef,
            Arc::new(Int64Array::from(request_counts)) as ArrayRef,
            Arc::new(Int64Array::from(error_counts)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Benchmark simple SELECT queries
fn bench_simple_select(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("simple_select");

    for size in [1000, 10000, 100000, 500000].iter() {
        let batch = generate_test_data(*size, 42);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("select_all", size), &batch, |b, batch| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    ctx.register_batch("metrics", batch.clone()).unwrap();
                    let df = ctx.sql("SELECT * FROM metrics").await.unwrap();
                    let result = df.collect().await.unwrap();
                    black_box(result);
                })
            })
        });

        group.bench_with_input(
            BenchmarkId::new("select_columns", size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();
                        ctx.register_batch("metrics", batch.clone()).unwrap();
                        let df = ctx
                            .sql("SELECT timestamp, host, cpu, memory FROM metrics")
                            .await
                            .unwrap();
                        let result = df.collect().await.unwrap();
                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark aggregation queries
fn bench_aggregations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("aggregations");

    let batch = generate_test_data(100000, 42);

    // Single aggregations
    let agg_queries = [
        ("count", "SELECT COUNT(*) FROM metrics"),
        ("sum", "SELECT SUM(cpu) FROM metrics"),
        ("avg", "SELECT AVG(cpu) FROM metrics"),
        ("min", "SELECT MIN(cpu) FROM metrics"),
        ("max", "SELECT MAX(cpu) FROM metrics"),
        (
            "multiple",
            "SELECT COUNT(*), SUM(cpu), AVG(cpu), MIN(cpu), MAX(cpu) FROM metrics",
        ),
        (
            "multiple_columns",
            "SELECT SUM(cpu), SUM(memory), SUM(disk), SUM(requests), SUM(errors) FROM metrics",
        ),
    ];

    for (name, sql) in agg_queries {
        group.bench_with_input(
            BenchmarkId::new("agg", name),
            &(batch.clone(), sql),
            |b, (batch, sql)| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();
                        ctx.register_batch("metrics", batch.clone()).unwrap();
                        let df = ctx.sql(sql).await.unwrap();
                        let result = df.collect().await.unwrap();
                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark filter queries
fn bench_filters(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("filters");

    let batch = generate_test_data(100000, 42);

    let filter_queries = [
        (
            "numeric_eq",
            "SELECT * FROM metrics WHERE cpu > 50",
            "~50% selectivity",
        ),
        (
            "numeric_range",
            "SELECT * FROM metrics WHERE cpu > 25 AND cpu < 75",
            "~50% selectivity",
        ),
        (
            "numeric_high_selectivity",
            "SELECT * FROM metrics WHERE cpu > 99",
            "~1% selectivity",
        ),
        (
            "string_eq",
            "SELECT * FROM metrics WHERE host = 'server001'",
            "~1% selectivity",
        ),
        (
            "string_like",
            "SELECT * FROM metrics WHERE region = 'us-east'",
            "~25% selectivity",
        ),
        (
            "compound",
            "SELECT * FROM metrics WHERE cpu > 50 AND memory > 50 AND region = 'us-east'",
            "~6% selectivity",
        ),
        (
            "or_condition",
            "SELECT * FROM metrics WHERE region = 'us-east' OR region = 'us-west'",
            "~50% selectivity",
        ),
    ];

    for (name, sql, _desc) in filter_queries {
        group.bench_with_input(
            BenchmarkId::new("filter", name),
            &(batch.clone(), sql),
            |b, (batch, sql)| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();
                        ctx.register_batch("metrics", batch.clone()).unwrap();
                        let df = ctx.sql(sql).await.unwrap();
                        let result = df.collect().await.unwrap();
                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark GROUP BY queries
fn bench_group_by(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("group_by");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    let group_by_queries = [
        (
            "single_column",
            "SELECT host, AVG(cpu) FROM metrics GROUP BY host",
        ),
        (
            "two_columns",
            "SELECT host, region, AVG(cpu) FROM metrics GROUP BY host, region",
        ),
        (
            "with_count",
            "SELECT host, COUNT(*), AVG(cpu), MAX(memory) FROM metrics GROUP BY host",
        ),
        (
            "with_filter",
            "SELECT host, AVG(cpu) FROM metrics WHERE cpu > 50 GROUP BY host",
        ),
        (
            "with_having",
            "SELECT host, AVG(cpu) as avg_cpu FROM metrics GROUP BY host HAVING AVG(cpu) > 50",
        ),
        (
            "high_cardinality",
            "SELECT timestamp, AVG(cpu) FROM metrics GROUP BY timestamp",
        ),
        (
            "low_cardinality",
            "SELECT region, AVG(cpu), AVG(memory), AVG(disk) FROM metrics GROUP BY region",
        ),
    ];

    for (name, sql) in group_by_queries {
        group.bench_with_input(
            BenchmarkId::new("group_by", name),
            &(batch.clone(), sql),
            |b, (batch, sql)| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();
                        ctx.register_batch("metrics", batch.clone()).unwrap();
                        let df = ctx.sql(sql).await.unwrap();
                        let result = df.collect().await.unwrap();
                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark time range queries
fn bench_time_range(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("time_range");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);
    let base_ts: i64 = 1700000000_000_000_000;

    // Different time ranges (selectivity)
    let ranges = [
        ("1_percent", base_ts, base_ts + 1000_000_000_000), // ~1%
        ("10_percent", base_ts, base_ts + 10000_000_000_000), // ~10%
        ("50_percent", base_ts, base_ts + 50000_000_000_000), // ~50%
        ("90_percent", base_ts, base_ts + 90000_000_000_000), // ~90%
    ];

    for (name, start, end) in ranges {
        let sql = format!(
            "SELECT * FROM metrics WHERE timestamp >= {} AND timestamp < {}",
            start, end
        );
        group.bench_with_input(
            BenchmarkId::new("range", name),
            &(batch.clone(), sql),
            |b, (batch, sql)| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();
                        ctx.register_batch("metrics", batch.clone()).unwrap();
                        let df = ctx.sql(&sql).await.unwrap();
                        let result = df.collect().await.unwrap();
                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark complex analytical queries
fn bench_complex_queries(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("complex_queries");
    group.sample_size(30);

    let batch = generate_test_data(100000, 42);

    let complex_queries = [
        (
            "top_n",
            "SELECT host, AVG(cpu) as avg_cpu FROM metrics GROUP BY host ORDER BY avg_cpu DESC LIMIT 10",
        ),
        (
            "percentile_approx",
            "SELECT region, APPROX_PERCENTILE_CONT(cpu, 0.95) as p95 FROM metrics GROUP BY region",
        ),
        (
            "window_like",
            "SELECT host, region, cpu, \
             AVG(cpu) OVER (PARTITION BY region) as region_avg \
             FROM metrics",
        ),
        (
            "multi_agg_multi_group",
            "SELECT region, host, \
             COUNT(*), SUM(requests), AVG(cpu), MAX(memory), MIN(disk) \
             FROM metrics \
             GROUP BY region, host",
        ),
        (
            "subquery",
            "SELECT * FROM metrics WHERE cpu > (SELECT AVG(cpu) FROM metrics)",
        ),
        (
            "case_when",
            "SELECT host, \
             CASE WHEN cpu > 80 THEN 'high' WHEN cpu > 50 THEN 'medium' ELSE 'low' END as load_level, \
             COUNT(*) \
             FROM metrics GROUP BY host, \
             CASE WHEN cpu > 80 THEN 'high' WHEN cpu > 50 THEN 'medium' ELSE 'low' END",
        ),
    ];

    for (name, sql) in complex_queries {
        group.bench_with_input(
            BenchmarkId::new("complex", name),
            &(batch.clone(), sql),
            |b, (batch, sql)| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();
                        ctx.register_batch("metrics", batch.clone()).unwrap();
                        let df = ctx.sql(sql).await.unwrap();
                        let result = df.collect().await.unwrap();
                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark query scaling with data size
fn bench_query_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_scaling");
    group.sample_size(30);

    // Test how query performance scales with data size
    let sql = "SELECT region, AVG(cpu), COUNT(*) FROM metrics GROUP BY region";

    for size in [10000, 50000, 100000, 250000, 500000, 1000000].iter() {
        let batch = generate_test_data(*size, 42);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("rows", size), &batch, |b, batch| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    ctx.register_batch("metrics", batch.clone()).unwrap();
                    let df = ctx.sql(sql).await.unwrap();
                    let result = df.collect().await.unwrap();
                    black_box(result);
                })
            })
        });
    }

    group.finish();
}

/// Benchmark repeated query execution (query cache warm-up effect)
fn bench_query_repetition(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_repetition");
    group.sample_size(100);

    let batch = generate_test_data(100000, 42);
    let sql = "SELECT region, AVG(cpu) FROM metrics WHERE cpu > 25 GROUP BY region";

    // Cold execution - create new context each time
    group.bench_with_input(
        BenchmarkId::new("execution", "cold"),
        &batch,
        |b, batch| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    ctx.register_batch("metrics", batch.clone()).unwrap();
                    let df = ctx.sql(sql).await.unwrap();
                    let result = df.collect().await.unwrap();
                    black_box(result);
                })
            })
        },
    );

    // Warm execution - reuse pre-created context
    // Create context outside the benchmark loop
    let ctx = rt.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_batch("metrics", batch.clone()).unwrap();
        ctx
    });

    group.bench_function("warm_reuse_context", |b| {
        b.iter(|| {
            rt.block_on(async {
                let df = ctx.sql(sql).await.unwrap();
                let result = df.collect().await.unwrap();
                black_box(result);
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_select,
    bench_aggregations,
    bench_filters,
    bench_group_by,
    bench_time_range,
    bench_complex_queries,
    bench_query_scaling,
    bench_query_repetition,
);

criterion_main!(benches);
