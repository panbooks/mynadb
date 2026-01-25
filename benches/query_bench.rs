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

    let hosts: Vec<String> = (0..rows).map(|i| format!("server{:03}", i % 100)).collect();

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
    group.bench_with_input(BenchmarkId::new("execution", "cold"), &batch, |b, batch| {
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

/// Benchmark edge cases: empty results, single row, boundary conditions
fn bench_edge_case_results(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("edge_case_results");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    let edge_queries = [
        // Empty result set (impossible condition)
        ("empty_result", "SELECT * FROM metrics WHERE cpu > 1000"),
        // Single row result
        ("single_row", "SELECT * FROM metrics LIMIT 1"),
        // Last row only
        (
            "offset_to_end",
            "SELECT * FROM metrics LIMIT 1 OFFSET 99999",
        ),
        // Count of empty result
        (
            "count_empty",
            "SELECT COUNT(*) FROM metrics WHERE cpu > 1000",
        ),
        // Aggregation on empty result
        (
            "agg_empty",
            "SELECT SUM(cpu), AVG(cpu) FROM metrics WHERE cpu > 1000",
        ),
        // IN subquery with no matches
        (
            "in_subquery_empty",
            "SELECT * FROM metrics WHERE host IN (SELECT host FROM metrics WHERE cpu > 1000)",
        ),
        // Very selective filter (1 in 100000)
        (
            "ultra_selective",
            "SELECT * FROM metrics WHERE host = 'server000' AND cpu > 99.9",
        ),
    ];

    for (name, sql) in edge_queries {
        group.bench_with_input(
            BenchmarkId::new("edge", name),
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

/// Benchmark DISTINCT and deduplication queries
fn bench_distinct(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("distinct");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    let distinct_queries = [
        // Low cardinality DISTINCT (4 regions)
        ("low_cardinality", "SELECT DISTINCT region FROM metrics"),
        // Medium cardinality DISTINCT (100 hosts)
        ("medium_cardinality", "SELECT DISTINCT host FROM metrics"),
        // High cardinality DISTINCT (100000 timestamps)
        ("high_cardinality", "SELECT DISTINCT timestamp FROM metrics"),
        // Multi-column DISTINCT
        ("multi_column", "SELECT DISTINCT host, region FROM metrics"),
        // DISTINCT with ORDER BY
        (
            "with_order_by",
            "SELECT DISTINCT region FROM metrics ORDER BY region",
        ),
        // COUNT DISTINCT
        ("count_distinct", "SELECT COUNT(DISTINCT host) FROM metrics"),
    ];

    for (name, sql) in distinct_queries {
        group.bench_with_input(
            BenchmarkId::new("distinct", name),
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

/// Benchmark UNION and set operations
fn bench_union(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("union");
    group.sample_size(30);

    let batch = generate_test_data(50000, 42);

    let union_queries = [
        // UNION ALL (no dedup)
        (
            "union_all",
            "SELECT host, cpu FROM metrics WHERE region = 'us-east' \
             UNION ALL \
             SELECT host, cpu FROM metrics WHERE region = 'us-west'",
        ),
        // UNION (with dedup)
        (
            "union_dedup",
            "SELECT region FROM metrics WHERE cpu > 80 \
             UNION \
             SELECT region FROM metrics WHERE memory > 80",
        ),
        // Multiple UNIONs
        (
            "triple_union",
            "SELECT host FROM metrics WHERE region = 'us-east' \
             UNION ALL \
             SELECT host FROM metrics WHERE region = 'us-west' \
             UNION ALL \
             SELECT host FROM metrics WHERE region = 'eu-west'",
        ),
        // INTERSECT
        (
            "intersect",
            "SELECT host FROM metrics WHERE cpu > 50 \
             INTERSECT \
             SELECT host FROM metrics WHERE memory > 50",
        ),
        // EXCEPT
        (
            "except",
            "SELECT host FROM metrics WHERE cpu > 50 \
             EXCEPT \
             SELECT host FROM metrics WHERE memory > 80",
        ),
    ];

    for (name, sql) in union_queries {
        group.bench_with_input(
            BenchmarkId::new("set_op", name),
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

/// Benchmark IN clause with varying sizes
fn bench_in_clause(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("in_clause");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    // Generate IN clauses of different sizes
    for size in [5, 10, 50, 100].iter() {
        let hosts: Vec<String> = (0..*size).map(|i| format!("'server{:03}'", i)).collect();
        let in_list = hosts.join(", ");
        let sql = format!("SELECT * FROM metrics WHERE host IN ({})", in_list);

        group.bench_with_input(
            BenchmarkId::new("in_list_size", size),
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

    // NOT IN
    let not_in_sql =
        "SELECT * FROM metrics WHERE host NOT IN ('server000', 'server001', 'server002')";
    group.bench_with_input(
        BenchmarkId::new("in_list_size", "not_in_3"),
        &(batch.clone(), not_in_sql),
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

    group.finish();
}

/// Benchmark string pattern matching
fn bench_like_patterns(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("like_patterns");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    let like_queries = [
        // Prefix match (can use index)
        ("prefix", "SELECT * FROM metrics WHERE host LIKE 'server0%'"),
        // Suffix match (full scan)
        ("suffix", "SELECT * FROM metrics WHERE host LIKE '%01'"),
        // Contains (full scan)
        ("contains", "SELECT * FROM metrics WHERE host LIKE '%ver0%'"),
        // Single char wildcard
        (
            "single_char",
            "SELECT * FROM metrics WHERE host LIKE 'server0_0'",
        ),
        // Exact match via LIKE
        (
            "exact_like",
            "SELECT * FROM metrics WHERE host LIKE 'server050'",
        ),
        // NOT LIKE
        (
            "not_like",
            "SELECT * FROM metrics WHERE host NOT LIKE 'server0%'",
        ),
        // ILIKE (case insensitive) - if supported
        // ("ilike", "SELECT * FROM metrics WHERE host ILIKE 'SERVER0%'"),
    ];

    for (name, sql) in like_queries {
        group.bench_with_input(
            BenchmarkId::new("like", name),
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

/// Benchmark ORDER BY edge cases
fn bench_order_by(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("order_by");
    group.sample_size(30);

    let batch = generate_test_data(100000, 42);

    let order_queries = [
        // Single column ASC
        ("single_asc", "SELECT * FROM metrics ORDER BY cpu"),
        // Single column DESC
        ("single_desc", "SELECT * FROM metrics ORDER BY cpu DESC"),
        // Multi-column sort
        (
            "multi_column",
            "SELECT * FROM metrics ORDER BY region, host, cpu DESC",
        ),
        // Sort with LIMIT (top-N optimization)
        (
            "with_limit_10",
            "SELECT * FROM metrics ORDER BY cpu DESC LIMIT 10",
        ),
        // Sort on string column
        ("string_column", "SELECT * FROM metrics ORDER BY host"),
        // Already sorted column (timestamp is sequential)
        ("presorted", "SELECT * FROM metrics ORDER BY timestamp"),
        // Reverse of presorted
        (
            "reverse_presorted",
            "SELECT * FROM metrics ORDER BY timestamp DESC",
        ),
        // NULLS FIRST/LAST
        (
            "nulls_last",
            "SELECT * FROM metrics ORDER BY cpu NULLS LAST",
        ),
    ];

    for (name, sql) in order_queries {
        group.bench_with_input(
            BenchmarkId::new("order", name),
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

/// Benchmark LIMIT and OFFSET combinations
fn bench_limit_offset(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("limit_offset");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    let limit_queries = [
        // Small LIMIT
        ("limit_10", "SELECT * FROM metrics LIMIT 10"),
        // Medium LIMIT
        ("limit_1000", "SELECT * FROM metrics LIMIT 1000"),
        // Large LIMIT (half the data)
        ("limit_50000", "SELECT * FROM metrics LIMIT 50000"),
        // OFFSET without ORDER (undefined behavior but valid)
        ("offset_1000", "SELECT * FROM metrics LIMIT 100 OFFSET 1000"),
        // Large OFFSET
        (
            "large_offset",
            "SELECT * FROM metrics LIMIT 100 OFFSET 90000",
        ),
        // LIMIT 0 (edge case)
        ("limit_0", "SELECT * FROM metrics LIMIT 0"),
        // LIMIT with ORDER BY (common pagination pattern)
        (
            "pagination",
            "SELECT * FROM metrics ORDER BY timestamp LIMIT 100 OFFSET 5000",
        ),
    ];

    for (name, sql) in limit_queries {
        group.bench_with_input(
            BenchmarkId::new("limit", name),
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

/// Benchmark joins (self-join on same table)
fn bench_self_join(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("self_join");
    group.sample_size(20);

    // Use smaller dataset for joins
    let batch = generate_test_data(10000, 42);

    let join_queries = [
        // Self join on same column
        (
            "inner_self",
            "SELECT a.host, a.cpu, b.memory \
             FROM metrics a \
             INNER JOIN metrics b ON a.host = b.host \
             LIMIT 1000",
        ),
        // Left join
        (
            "left_self",
            "SELECT a.host, a.cpu, b.memory \
             FROM metrics a \
             LEFT JOIN metrics b ON a.host = b.host AND b.cpu > 90 \
             LIMIT 1000",
        ),
        // Join with filter
        (
            "join_with_filter",
            "SELECT a.host, a.cpu \
             FROM metrics a \
             INNER JOIN metrics b ON a.host = b.host \
             WHERE a.cpu > 50 AND b.memory > 50 \
             LIMIT 1000",
        ),
        // Cross join (cartesian) - very expensive, small limit
        (
            "cross_small",
            "SELECT a.host, b.region \
             FROM (SELECT DISTINCT host FROM metrics LIMIT 10) a \
             CROSS JOIN (SELECT DISTINCT region FROM metrics) b",
        ),
    ];

    for (name, sql) in join_queries {
        group.bench_with_input(
            BenchmarkId::new("join", name),
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

/// Benchmark expression complexity
fn bench_expression_complexity(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("expression_complexity");
    group.sample_size(50);

    let batch = generate_test_data(100000, 42);

    let expr_queries = [
        // Simple arithmetic
        ("simple_arith", "SELECT cpu + memory FROM metrics"),
        // Complex arithmetic
        (
            "complex_arith",
            "SELECT (cpu * 0.4 + memory * 0.3 + disk * 0.3) / 100.0 FROM metrics",
        ),
        // Multiple CASE expressions
        (
            "multi_case",
            "SELECT \
             CASE WHEN cpu > 80 THEN 'high' ELSE 'normal' END, \
             CASE WHEN memory > 80 THEN 'high' ELSE 'normal' END, \
             CASE WHEN disk > 80 THEN 'high' ELSE 'normal' END \
             FROM metrics",
        ),
        // Nested CASE
        (
            "nested_case",
            "SELECT \
             CASE WHEN cpu > 90 THEN 'critical' \
                  WHEN cpu > 70 THEN CASE WHEN memory > 70 THEN 'warning' ELSE 'elevated' END \
                  ELSE 'normal' END \
             FROM metrics",
        ),
        // String concatenation
        (
            "string_concat",
            "SELECT host || '-' || region AS full_name FROM metrics",
        ),
        // COALESCE chain
        (
            "coalesce",
            "SELECT COALESCE(NULLIF(cpu, 0), memory, disk, 0) FROM metrics",
        ),
        // Deeply nested expression
        (
            "deeply_nested",
            "SELECT ((((cpu + 1) * 2) - 3) / 4) + ((((memory + 1) * 2) - 3) / 4) FROM metrics",
        ),
    ];

    for (name, sql) in expr_queries {
        group.bench_with_input(
            BenchmarkId::new("expr", name),
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
    bench_edge_case_results,
    bench_distinct,
    bench_union,
    bench_in_clause,
    bench_like_patterns,
    bench_order_by,
    bench_limit_offset,
    bench_self_join,
    bench_expression_complexity,
);

criterion_main!(benches);
