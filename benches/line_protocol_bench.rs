//! Benchmarks for InfluxDB line protocol parsing
//!
//! Measures parsing performance for:
//! - Single line parsing
//! - Batch parsing (sequential vs parallel)
//! - Various line complexities (simple, many tags, many fields)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mynadb::config::{SchemaConfig, SchemaTableStrategy};
use mynadb::storage::line_protocol::{parse_line_protocol, parse_multiple_lines};

fn default_schema_config() -> SchemaConfig {
    SchemaConfig {
        strategy: SchemaTableStrategy::ServiceAsSchema,
        tag_keys: vec!["service".to_string()],
        default_name: "default".to_string(),
        prefix_numeric: true,
    }
}

/// Generate a simple line with minimal tags/fields
fn generate_simple_line(i: usize) -> String {
    format!(
        "cpu,host=server{} usage={}.5 1700000000000000000",
        i % 100,
        i % 100
    )
}

/// Generate a line with many tags (10 tags)
fn generate_many_tags_line(i: usize) -> String {
    format!(
        "metrics,service=svc{},host=server{},region=us-west,dc=dc1,rack=r{},env=prod,app=myapp,version=1.0,team=platform,owner=ops value={}.0 1700000000000000000",
        i % 10, i % 100, i % 50, i
    )
}

/// Generate a line with many fields (10 fields)
fn generate_many_fields_line(i: usize) -> String {
    format!(
        "system,host=server{} cpu={}.1,mem={}.2,disk={}.3,net_in={}i,net_out={}i,load1={}.4,load5={}.5,load15={}.6,active=true,status=\"running\" 1700000000000000000",
        i % 100, i % 100, i % 100, i % 100, i * 1000, i * 500, i % 10, i % 10, i % 10
    )
}

/// Generate a complex line with many tags and fields
fn generate_complex_line(i: usize) -> String {
    format!(
        "telemetry,service=svc{},host=server{},region=us-west,dc=dc1,rack=r{},env=prod,app=myapp,version=1.0 cpu={}.1,mem={}.2,disk={}.3,net_in={}i,net_out={}i,load={}.4,requests={}i,errors={}i,latency_p99={}.5,active=true 1700000000000000000",
        i % 10, i % 100, i % 50, i % 100, i % 100, i % 100, i * 1000, i * 500, i % 10, i * 100, i % 10, i
    )
}

fn bench_single_line_parsing(c: &mut Criterion) {
    let config = default_schema_config();

    let mut group = c.benchmark_group("single_line_parsing");

    // Simple line
    let simple_line = generate_simple_line(42);
    group.bench_function("simple", |b| {
        b.iter(|| parse_line_protocol(black_box(&simple_line), &config))
    });

    // Many tags
    let many_tags_line = generate_many_tags_line(42);
    group.bench_function("many_tags", |b| {
        b.iter(|| parse_line_protocol(black_box(&many_tags_line), &config))
    });

    // Many fields
    let many_fields_line = generate_many_fields_line(42);
    group.bench_function("many_fields", |b| {
        b.iter(|| parse_line_protocol(black_box(&many_fields_line), &config))
    });

    // Complex (many tags + many fields)
    let complex_line = generate_complex_line(42);
    group.bench_function("complex", |b| {
        b.iter(|| parse_line_protocol(black_box(&complex_line), &config))
    });

    group.finish();
}

fn bench_batch_parsing(c: &mut Criterion) {
    let config = default_schema_config();

    let mut group = c.benchmark_group("batch_parsing");

    // Test different batch sizes
    for size in [100, 500, 1000, 5000, 10000].iter() {
        let lines: Vec<String> = (0..*size).map(generate_simple_line).collect();
        let data = lines.join("\n");

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("simple_lines", size), &data, |b, data| {
            b.iter(|| parse_multiple_lines(black_box(data), &config))
        });
    }

    group.finish();
}

fn bench_batch_complexity(c: &mut Criterion) {
    let config = default_schema_config();

    let mut group = c.benchmark_group("batch_complexity");
    let batch_size = 1000;

    // Simple lines batch
    let simple_lines: Vec<String> = (0..batch_size).map(generate_simple_line).collect();
    let simple_data = simple_lines.join("\n");
    group.throughput(Throughput::Elements(batch_size as u64));
    group.bench_function("simple_1000", |b| {
        b.iter(|| parse_multiple_lines(black_box(&simple_data), &config))
    });

    // Many tags batch
    let many_tags_lines: Vec<String> = (0..batch_size).map(generate_many_tags_line).collect();
    let many_tags_data = many_tags_lines.join("\n");
    group.bench_function("many_tags_1000", |b| {
        b.iter(|| parse_multiple_lines(black_box(&many_tags_data), &config))
    });

    // Many fields batch
    let many_fields_lines: Vec<String> = (0..batch_size).map(generate_many_fields_line).collect();
    let many_fields_data = many_fields_lines.join("\n");
    group.bench_function("many_fields_1000", |b| {
        b.iter(|| parse_multiple_lines(black_box(&many_fields_data), &config))
    });

    // Complex batch
    let complex_lines: Vec<String> = (0..batch_size).map(generate_complex_line).collect();
    let complex_data = complex_lines.join("\n");
    group.bench_function("complex_1000", |b| {
        b.iter(|| parse_multiple_lines(black_box(&complex_data), &config))
    });

    group.finish();
}

fn bench_parallel_threshold(c: &mut Criterion) {
    let config = default_schema_config();

    let mut group = c.benchmark_group("parallel_threshold");

    // Test around the parallel threshold (1000 lines)
    for size in [500, 800, 1000, 1200, 1500, 2000].iter() {
        let lines: Vec<String> = (0..*size).map(generate_simple_line).collect();
        let data = lines.join("\n");

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("lines", size), &data, |b, data| {
            b.iter(|| parse_multiple_lines(black_box(data), &config))
        });
    }

    group.finish();
}

fn bench_data_throughput(c: &mut Criterion) {
    let config = default_schema_config();

    let mut group = c.benchmark_group("data_throughput");

    // Generate 10MB of data
    let mut data = String::new();
    let mut line_count = 0;
    while data.len() < 10 * 1024 * 1024 {
        data.push_str(&generate_simple_line(line_count));
        data.push('\n');
        line_count += 1;
    }

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("10mb_simple", |b| {
        b.iter(|| parse_multiple_lines(black_box(&data), &config))
    });

    // Generate 10MB of complex data
    let mut complex_data = String::new();
    let mut complex_line_count = 0;
    while complex_data.len() < 10 * 1024 * 1024 {
        complex_data.push_str(&generate_complex_line(complex_line_count));
        complex_data.push('\n');
        complex_line_count += 1;
    }

    group.throughput(Throughput::Bytes(complex_data.len() as u64));
    group.bench_function("10mb_complex", |b| {
        b.iter(|| parse_multiple_lines(black_box(&complex_data), &config))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_line_parsing,
    bench_batch_parsing,
    bench_batch_complexity,
    bench_parallel_threshold,
    bench_data_throughput,
);

criterion_main!(benches);
