//! Benchmarks for time series functions
//!
//! Measures performance of:
//! - TimeInterval parsing (duration strings like "1h", "5m", "30s")
//! - time_bucket UDF execution
//! - FillOption parsing
//! - Time bucket calculations

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use mynadb::query::functions::create_time_bucket_udf;
use mynadb::query::timeseries::time_interval::{FillOption, TimeInterval};
use std::str::FromStr;
use std::sync::Arc;

/// Benchmark TimeInterval parsing
fn bench_time_interval_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_interval_parsing");

    // Various interval formats
    let intervals = [
        ("seconds", "30s"),
        ("minutes", "5m"),
        ("hours", "1h"),
        ("days", "7d"),
        ("weeks", "2w"),
        ("milliseconds", "100ms"),
        ("microseconds", "500us"),
        ("nanoseconds", "1000ns"),
        ("combined", "1h30m"),
    ];

    for (name, interval) in intervals {
        group.bench_function(name, |b| {
            b.iter(|| TimeInterval::from_str(black_box(interval)))
        });
    }

    group.finish();
}

/// Benchmark FillOption parsing
fn bench_fill_option_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("fill_option_parsing");

    let options = [
        ("null", "null"),
        ("none", "none"),
        ("zero", "0"),
        ("previous", "previous"),
        ("linear", "linear"),
        ("integer", "42"),
        ("float", "3.14"),
    ];

    for (name, option) in options {
        group.bench_function(name, |b| {
            b.iter(|| FillOption::from_str(black_box(option)))
        });
    }

    group.finish();
}

/// Benchmark time_bucket calculations with raw arrays
fn bench_time_bucket_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_bucket_raw");

    // Generate timestamp data
    let base_ts: i64 = 1700000000_000_000_000; // nanoseconds
    let interval_1min: i64 = 60_000_000_000; // 1 minute in nanos

    for size in [1000, 10000, 100000, 1000000].iter() {
        let timestamps: Vec<i64> = (0..*size)
            .map(|i| base_ts + (i as i64 * 1_000_000_000)) // 1 second apart
            .collect();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("bucket_1min", size), &timestamps, |b, ts| {
            b.iter(|| {
                // Simulate time_bucket: floor((time - offset) / interval) * interval + offset
                let offset = 0i64;
                let _bucketed: Vec<i64> = ts
                    .iter()
                    .map(|&t| ((t - offset) / interval_1min) * interval_1min + offset)
                    .collect();
            })
        });
    }

    group.finish();
}

/// Benchmark time_bucket UDF with DataFusion
fn bench_time_bucket_udf(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("time_bucket_udf");

    // Generate timestamp data
    let base_ts: i64 = 1700000000_000_000_000; // nanoseconds

    for size in [1000, 10000, 100000].iter() {
        let timestamps: Vec<i64> = (0..*size)
            .map(|i| base_ts + (i as i64 * 1_000_000_000)) // 1 second apart
            .collect();
        let values: Vec<f64> = (0..*size).map(|i| i as f64 * 0.1).collect();

        // Create RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps.clone())) as ArrayRef,
                Arc::new(datafusion::arrow::array::Float64Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("datafusion_udf", size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();

                        // Register time_bucket UDF
                        ctx.register_udf(create_time_bucket_udf());

                        // Create dataframe from batch
                        let df = ctx.read_batch(batch.clone()).unwrap();

                        // Apply time_bucket (1 minute = 60_000_000_000 nanos)
                        let result = df
                            .select(vec![
                                col("timestamp"),
                                col("value"),
                                datafusion::functions::expr_fn::floor(
                                    col("timestamp") / lit(60_000_000_000i64),
                                ) * lit(60_000_000_000i64),
                            ])
                            .unwrap()
                            .collect()
                            .await
                            .unwrap();

                        black_box(result);
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark time bucket aggregation with GROUP BY
fn bench_time_bucket_aggregation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("time_bucket_aggregation");
    group.sample_size(50); // Reduce sample size for expensive operations

    // Generate timestamp data with values
    let base_ts: i64 = 1700000000_000_000_000;
    let size = 100000;

    let timestamps: Vec<i64> = (0..size)
        .map(|i| base_ts + (i as i64 * 1_000_000_000)) // 1 second apart
        .collect();
    let values: Vec<f64> = (0..size).map(|i| (i % 100) as f64 + 0.5).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(datafusion::arrow::array::Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap();

    // Different aggregation intervals
    let intervals = [
        ("1min", 60_000_000_000i64),
        ("5min", 300_000_000_000i64),
        ("1hour", 3600_000_000_000i64),
    ];

    for (name, interval) in intervals {
        group.bench_with_input(
            BenchmarkId::new("group_by", name),
            &(batch.clone(), interval),
            |b, (batch, interval)| {
                b.iter(|| {
                    rt.block_on(async {
                        let ctx = SessionContext::new();

                        // Register the batch as a table
                        ctx.register_batch("data", batch.clone()).unwrap();

                        // Run GROUP BY time bucket query
                        let sql = format!(
                            "SELECT (timestamp / {}) * {} as bucket, AVG(value), COUNT(*) \
                             FROM data \
                             GROUP BY (timestamp / {}) * {} \
                             ORDER BY bucket",
                            interval, interval, interval, interval
                        );

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

/// Benchmark interval parsing throughput
fn bench_interval_parsing_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("interval_parsing_throughput");

    // Generate many interval strings
    let intervals: Vec<String> = (0..10000)
        .map(|i| format!("{}s", i % 3600))
        .collect();

    group.throughput(Throughput::Elements(intervals.len() as u64));
    group.bench_function("parse_10000_intervals", |b| {
        b.iter(|| {
            for interval in &intervals {
                let _ = TimeInterval::from_str(black_box(interval));
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_time_interval_parsing,
    bench_fill_option_parsing,
    bench_time_bucket_raw,
    bench_time_bucket_udf,
    bench_time_bucket_aggregation,
    bench_interval_parsing_throughput,
);

criterion_main!(benches);
