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

/// Benchmark edge case time intervals
fn bench_edge_case_intervals(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_case_intervals");

    let edge_intervals = [
        // Very large values
        ("large_seconds", "999999999s"),
        ("large_hours", "87600h"),    // 10 years in hours
        ("large_days", "36500d"),     // 100 years in days
        // Very small values
        ("one_nanosecond", "1ns"),
        ("one_microsecond", "1us"),
        ("one_millisecond", "1ms"),
        // Zero values
        ("zero_seconds", "0s"),
        ("zero_minutes", "0m"),
        // Combined large values
        ("combined_large", "365d24h60m60s"),
        // Maximum practical values
        ("week_max", "52w"),
        // Single digit with each unit
        ("single_digit_all", "1w1d1h1m1s"),
    ];

    for (name, interval) in edge_intervals {
        group.bench_function(name, |b| {
            b.iter(|| TimeInterval::from_str(black_box(interval)))
        });
    }

    group.finish();
}

/// Benchmark invalid/error path interval parsing
fn bench_invalid_intervals(c: &mut Criterion) {
    let mut group = c.benchmark_group("invalid_intervals");

    let invalid_intervals = [
        // Empty and whitespace
        ("empty", ""),
        ("whitespace", "   "),
        // Invalid units
        ("invalid_unit", "10x"),
        ("no_unit", "100"),
        // Negative values (if not supported)
        ("negative", "-5s"),
        // Invalid format
        ("letters_only", "abc"),
        ("mixed_invalid", "10s5"),
        // Overflow potential
        ("huge_number", "999999999999999999999s"),
        // Special characters
        ("special_chars", "10@s"),
        ("unicode", "10秒"),
    ];

    for (name, interval) in invalid_intervals {
        group.bench_function(name, |b| {
            b.iter(|| TimeInterval::from_str(black_box(interval)))
        });
    }

    group.finish();
}

/// Benchmark edge case fill options
fn bench_edge_case_fill_options(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_case_fill_options");

    let edge_fills = [
        // Case variations
        ("null_upper", "NULL"),
        ("none_upper", "NONE"),
        ("previous_upper", "PREVIOUS"),
        ("linear_upper", "LINEAR"),
        // Numeric edge cases
        ("negative_int", "-999"),
        ("negative_float", "-999.999"),
        ("zero", "0"),
        ("large_int", "9999999999"),
        ("scientific", "1e10"),
        ("tiny_float", "0.000001"),
        // Mixed case
        ("mixed_case", "PrEvIoUs"),
    ];

    for (name, fill) in edge_fills {
        group.bench_function(name, |b| {
            b.iter(|| FillOption::from_str(black_box(fill)))
        });
    }

    group.finish();
}

/// Benchmark invalid fill option parsing
fn bench_invalid_fill_options(c: &mut Criterion) {
    let mut group = c.benchmark_group("invalid_fill_options");

    let invalid_fills = [
        ("empty", ""),
        ("whitespace", "   "),
        ("unknown_keyword", "interpolate"),
        ("special_chars", "@#$%"),
        ("unicode", "前"),
        ("partial_keyword", "prev"),
        ("number_with_suffix", "100x"),
    ];

    for (name, fill) in invalid_fills {
        group.bench_function(name, |b| {
            b.iter(|| FillOption::from_str(black_box(fill)))
        });
    }

    group.finish();
}

/// Benchmark time bucket with boundary timestamps
fn bench_boundary_timestamps(c: &mut Criterion) {
    let mut group = c.benchmark_group("boundary_timestamps");

    let interval_1hour: i64 = 3600_000_000_000;

    // Different timestamp scenarios
    let scenarios = [
        // Unix epoch
        ("epoch", vec![0i64; 1000]),
        // Near epoch
        (
            "near_epoch",
            (0..1000).map(|i| i as i64 * 1_000_000_000).collect(),
        ),
        // Current era (2023-2024)
        (
            "current_era",
            (0..1000)
                .map(|i| 1700000000_000_000_000i64 + (i as i64 * 1_000_000_000))
                .collect(),
        ),
        // Far future (year 2100)
        (
            "far_future",
            (0..1000)
                .map(|i| 4102444800_000_000_000i64 + (i as i64 * 1_000_000_000))
                .collect(),
        ),
        // Mixed timestamps (random-like distribution)
        (
            "mixed_eras",
            (0..1000)
                .map(|i| {
                    let base = match i % 4 {
                        0 => 0i64,
                        1 => 1000000000_000_000_000i64,
                        2 => 1700000000_000_000_000i64,
                        _ => 2000000000_000_000_000i64,
                    };
                    base + (i as i64 * 1000)
                })
                .collect(),
        ),
    ];

    for (name, timestamps) in scenarios {
        group.bench_with_input(
            BenchmarkId::new("timestamps", name),
            &timestamps,
            |b, ts| {
                b.iter(|| {
                    let offset = 0i64;
                    let _bucketed: Vec<i64> = ts
                        .iter()
                        .map(|&t| ((t - offset) / interval_1hour) * interval_1hour + offset)
                        .collect();
                })
            },
        );
    }

    group.finish();
}

/// Benchmark time bucket with different bucket sizes
fn bench_bucket_size_variations(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_size_variations");

    let base_ts: i64 = 1700000000_000_000_000;
    let timestamps: Vec<i64> = (0..100000)
        .map(|i| base_ts + (i as i64 * 1_000_000_000))
        .collect();

    // Different bucket sizes from very small to very large
    let bucket_sizes = [
        ("1_second", 1_000_000_000i64),
        ("10_seconds", 10_000_000_000i64),
        ("1_minute", 60_000_000_000i64),
        ("5_minutes", 300_000_000_000i64),
        ("15_minutes", 900_000_000_000i64),
        ("1_hour", 3600_000_000_000i64),
        ("6_hours", 21600_000_000_000i64),
        ("1_day", 86400_000_000_000i64),
        ("1_week", 604800_000_000_000i64),
    ];

    for (name, interval) in bucket_sizes {
        group.throughput(Throughput::Elements(timestamps.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("bucket", name),
            &(timestamps.clone(), interval),
            |b, (ts, interval)| {
                b.iter(|| {
                    let offset = 0i64;
                    let _bucketed: Vec<i64> = ts
                        .iter()
                        .map(|&t| ((t - offset) / interval) * interval + offset)
                        .collect();
                })
            },
        );
    }

    group.finish();
}

/// Benchmark time bucket with offset variations
fn bench_bucket_offsets(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_offsets");

    let base_ts: i64 = 1700000000_000_000_000;
    let timestamps: Vec<i64> = (0..10000)
        .map(|i| base_ts + (i as i64 * 1_000_000_000))
        .collect();

    let interval: i64 = 3600_000_000_000; // 1 hour

    // Different offsets
    let offsets = [
        ("zero", 0i64),
        ("30_minutes", 1800_000_000_000i64),
        ("negative_30min", -1800_000_000_000i64),
        ("1_second", 1_000_000_000i64),
        ("timezone_offset", 5 * 3600_000_000_000i64), // UTC+5
    ];

    for (name, offset) in offsets {
        group.bench_with_input(
            BenchmarkId::new("offset", name),
            &(timestamps.clone(), offset),
            |b, (ts, offset)| {
                b.iter(|| {
                    let _bucketed: Vec<i64> = ts
                        .iter()
                        .map(|&t| ((t - offset) / interval) * interval + offset)
                        .collect();
                })
            },
        );
    }

    group.finish();
}

/// Benchmark time bucket aggregation with sparse data (many gaps)
fn bench_sparse_time_data(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sparse_time_data");
    group.sample_size(30);

    let base_ts: i64 = 1700000000_000_000_000;

    // Generate sparse data (only every 10th bucket has data)
    let sparse_timestamps: Vec<i64> = (0..10000)
        .filter(|i| i % 10 == 0)
        .map(|i| base_ts + (i as i64 * 60_000_000_000)) // 1 minute intervals, every 10th
        .collect();
    let sparse_values: Vec<f64> = (0..sparse_timestamps.len())
        .map(|i| i as f64 * 1.5)
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let sparse_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(sparse_timestamps)) as ArrayRef,
            Arc::new(datafusion::arrow::array::Float64Array::from(sparse_values)) as ArrayRef,
        ],
    )
    .unwrap();

    // Dense data for comparison
    let dense_timestamps: Vec<i64> = (0..10000)
        .map(|i| base_ts + (i as i64 * 60_000_000_000))
        .collect();
    let dense_values: Vec<f64> = (0..10000).map(|i| i as f64 * 1.5).collect();

    let dense_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(dense_timestamps)) as ArrayRef,
            Arc::new(datafusion::arrow::array::Float64Array::from(dense_values)) as ArrayRef,
        ],
    )
    .unwrap();

    let interval = 600_000_000_000i64; // 10 minutes
    let sql = format!(
        "SELECT (timestamp / {}) * {} as bucket, AVG(value), COUNT(*) \
         FROM data \
         GROUP BY (timestamp / {}) * {} \
         ORDER BY bucket",
        interval, interval, interval, interval
    );

    group.bench_with_input(
        BenchmarkId::new("data_density", "sparse_10pct"),
        &(sparse_batch.clone(), sql.clone()),
        |b, (batch, sql)| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    ctx.register_batch("data", batch.clone()).unwrap();
                    let df = ctx.sql(&sql).await.unwrap();
                    let result = df.collect().await.unwrap();
                    black_box(result);
                })
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("data_density", "dense_100pct"),
        &(dense_batch, sql),
        |b, (batch, sql)| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    ctx.register_batch("data", batch.clone()).unwrap();
                    let df = ctx.sql(&sql).await.unwrap();
                    let result = df.collect().await.unwrap();
                    black_box(result);
                })
            })
        },
    );

    group.finish();
}

/// Benchmark time bucket with out-of-order timestamps
fn bench_out_of_order_timestamps(c: &mut Criterion) {
    let mut group = c.benchmark_group("out_of_order_timestamps");

    let base_ts: i64 = 1700000000_000_000_000;
    let interval: i64 = 3600_000_000_000; // 1 hour

    // Ordered timestamps
    let ordered: Vec<i64> = (0..10000)
        .map(|i| base_ts + (i as i64 * 1_000_000_000))
        .collect();

    // Reversed timestamps
    let reversed: Vec<i64> = ordered.iter().rev().copied().collect();

    // Random-ish order (interleaved)
    let interleaved: Vec<i64> = {
        let mut result = Vec::with_capacity(10000);
        for i in 0..5000 {
            result.push(ordered[i * 2]);
            result.push(ordered[i * 2 + 1]);
        }
        // Swap pairs
        for i in (0..result.len()).step_by(4) {
            if i + 3 < result.len() {
                result.swap(i, i + 2);
                result.swap(i + 1, i + 3);
            }
        }
        result
    };

    let scenarios = [
        ("ordered", ordered),
        ("reversed", reversed),
        ("interleaved", interleaved),
    ];

    for (name, timestamps) in scenarios {
        group.bench_with_input(
            BenchmarkId::new("order", name),
            &timestamps,
            |b, ts| {
                b.iter(|| {
                    let offset = 0i64;
                    let _bucketed: Vec<i64> = ts
                        .iter()
                        .map(|&t| ((t - offset) / interval) * interval + offset)
                        .collect();
                })
            },
        );
    }

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
    bench_edge_case_intervals,
    bench_invalid_intervals,
    bench_edge_case_fill_options,
    bench_invalid_fill_options,
    bench_boundary_timestamps,
    bench_bucket_size_variations,
    bench_bucket_offsets,
    bench_sparse_time_data,
    bench_out_of_order_timestamps,
);

criterion_main!(benches);
