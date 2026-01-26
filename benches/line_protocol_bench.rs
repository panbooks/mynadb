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

/// Benchmark edge cases for field value parsing
fn bench_edge_case_field_values(c: &mut Criterion) {
    let config = default_schema_config();
    let mut group = c.benchmark_group("edge_case_field_values");

    // Scientific notation
    let scientific = "cpu,host=server01 value=1.23456789e-15 1700000000000000000";
    group.bench_function("scientific_notation", |b| {
        b.iter(|| parse_line_protocol(black_box(scientific), &config))
    });

    // Very large integer
    let large_int = "cpu,host=server01 value=9223372036854775807i 1700000000000000000";
    group.bench_function("max_i64", |b| {
        b.iter(|| parse_line_protocol(black_box(large_int), &config))
    });

    // Very small float (near zero)
    let tiny_float = "cpu,host=server01 value=0.000000000000001 1700000000000000000";
    group.bench_function("tiny_float", |b| {
        b.iter(|| parse_line_protocol(black_box(tiny_float), &config))
    });

    // Negative values
    let negative = "cpu,host=server01 temp=-273.15,count=-9999i 1700000000000000000";
    group.bench_function("negative_values", |b| {
        b.iter(|| parse_line_protocol(black_box(negative), &config))
    });

    // Boolean variations
    let booleans = "cpu,host=server01 a=true,b=false,c=t,d=f,e=TRUE,f=FALSE 1700000000000000000";
    group.bench_function("boolean_variations", |b| {
        b.iter(|| parse_line_protocol(black_box(booleans), &config))
    });

    // Long string value (1KB)
    let long_string_val = "x".repeat(1024);
    let long_string = format!(
        "log,host=server01 message=\"{}\" 1700000000000000000",
        long_string_val
    );
    group.bench_function("long_string_1kb", |b| {
        b.iter(|| parse_line_protocol(black_box(&long_string), &config))
    });

    // Mixed field types
    let mixed =
        "cpu,host=server01 f1=1.5,f2=100i,f3=\"hello\",f4=true,f5=-99.9,f6=0i 1700000000000000000";
    group.bench_function("mixed_field_types", |b| {
        b.iter(|| parse_line_protocol(black_box(mixed), &config))
    });

    group.finish();
}

/// Benchmark edge cases for measurement and tag names
fn bench_edge_case_names(c: &mut Criterion) {
    let config = default_schema_config();
    let mut group = c.benchmark_group("edge_case_names");

    // Very long measurement name (256 chars)
    let long_measurement = format!(
        "{},host=server01 value=1.0 1700000000000000000",
        "m".repeat(256)
    );
    group.bench_function("long_measurement_256", |b| {
        b.iter(|| parse_line_protocol(black_box(&long_measurement), &config))
    });

    // Very long tag key and value
    let long_tag = format!(
        "cpu,{}={} value=1.0 1700000000000000000",
        "k".repeat(128),
        "v".repeat(256)
    );
    group.bench_function("long_tag_key_value", |b| {
        b.iter(|| parse_line_protocol(black_box(&long_tag), &config))
    });

    // Many short tags (20 tags)
    let many_short_tags = "cpu,a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8,i=9,j=10,k=11,l=12,m=13,n=14,o=15,p=16,q=17,r=18,s=19,t=20 value=1.0 1700000000000000000";
    group.bench_function("20_short_tags", |b| {
        b.iter(|| parse_line_protocol(black_box(many_short_tags), &config))
    });

    // Single character names
    let single_char = "m,t=v f=1.0 1700000000000000000";
    group.bench_function("single_char_names", |b| {
        b.iter(|| parse_line_protocol(black_box(single_char), &config))
    });

    // Numeric tag values (triggers prefix_numeric)
    let numeric_service = "cpu,service=12345,host=server01 value=1.0 1700000000000000000";
    group.bench_function("numeric_service_tag", |b| {
        b.iter(|| parse_line_protocol(black_box(numeric_service), &config))
    });

    group.finish();
}

/// Benchmark timestamp edge cases
fn bench_edge_case_timestamps(c: &mut Criterion) {
    let config = default_schema_config();
    let mut group = c.benchmark_group("edge_case_timestamps");

    // No timestamp (uses current time)
    let no_timestamp = "cpu,host=server01 value=1.0";
    group.bench_function("no_timestamp", |b| {
        b.iter(|| parse_line_protocol(black_box(no_timestamp), &config))
    });

    // Unix epoch (0)
    let epoch = "cpu,host=server01 value=1.0 0";
    group.bench_function("epoch_zero", |b| {
        b.iter(|| parse_line_protocol(black_box(epoch), &config))
    });

    // Max reasonable timestamp (year 2100)
    let future = "cpu,host=server01 value=1.0 4102444800000000000";
    group.bench_function("year_2100", |b| {
        b.iter(|| parse_line_protocol(black_box(future), &config))
    });

    // Timestamp with nanosecond precision
    let precise = "cpu,host=server01 value=1.0 1700000000123456789";
    group.bench_function("nanosecond_precision", |b| {
        b.iter(|| parse_line_protocol(black_box(precise), &config))
    });

    group.finish();
}

/// Benchmark error/invalid input paths
fn bench_error_paths(c: &mut Criterion) {
    let config = default_schema_config();
    let mut group = c.benchmark_group("error_paths");

    // Empty line
    let empty = "";
    group.bench_function("empty_line", |b| {
        b.iter(|| parse_line_protocol(black_box(empty), &config))
    });

    // Comment line
    let comment = "# this is a comment";
    group.bench_function("comment_line", |b| {
        b.iter(|| parse_line_protocol(black_box(comment), &config))
    });

    // Whitespace only
    let whitespace = "   \t  ";
    group.bench_function("whitespace_only", |b| {
        b.iter(|| parse_line_protocol(black_box(whitespace), &config))
    });

    // Missing fields (invalid)
    let no_fields = "cpu,host=server01";
    group.bench_function("missing_fields", |b| {
        b.iter(|| parse_line_protocol(black_box(no_fields), &config))
    });

    // Malformed tag (no value)
    let bad_tag = "cpu,host= value=1.0";
    group.bench_function("empty_tag_value", |b| {
        b.iter(|| parse_line_protocol(black_box(bad_tag), &config))
    });

    // Invalid integer suffix
    let bad_int = "cpu,host=server01 value=abci 1700000000000000000";
    group.bench_function("invalid_integer", |b| {
        b.iter(|| parse_line_protocol(black_box(bad_int), &config))
    });

    group.finish();
}

/// Benchmark batch with mixed valid/invalid lines
fn bench_mixed_batch(c: &mut Criterion) {
    let config = default_schema_config();
    let mut group = c.benchmark_group("mixed_batch");

    // Batch with comments and empty lines interspersed
    let mut mixed_lines = Vec::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            mixed_lines.push("# comment line".to_string());
        } else if i % 15 == 0 {
            mixed_lines.push("".to_string());
        } else {
            mixed_lines.push(generate_simple_line(i));
        }
    }
    let mixed_data = mixed_lines.join("\n");
    group.bench_function("with_comments_empty", |b| {
        b.iter(|| parse_multiple_lines(black_box(&mixed_data), &config))
    });

    // Batch with some invalid lines
    let mut partial_invalid = Vec::new();
    for i in 0..1000 {
        if i % 20 == 0 {
            partial_invalid.push("invalid_line_no_fields".to_string());
        } else {
            partial_invalid.push(generate_simple_line(i));
        }
    }
    let partial_data = partial_invalid.join("\n");
    group.bench_function("5_percent_invalid", |b| {
        b.iter(|| parse_multiple_lines(black_box(&partial_data), &config))
    });

    // All lines are comments/empty (worst case for filtering)
    let all_invalid: Vec<String> = (0..1000)
        .map(|i| {
            if i % 2 == 0 {
                "# comment".to_string()
            } else {
                "".to_string()
            }
        })
        .collect();
    let all_invalid_data = all_invalid.join("\n");
    group.bench_function("all_comments_empty", |b| {
        b.iter(|| parse_multiple_lines(black_box(&all_invalid_data), &config))
    });

    group.finish();
}

/// Benchmark with different schema configurations
fn bench_schema_configs(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_configs");

    let line = "cpu,service=myapp,host=server01,region=us-west value=1.0 1700000000000000000";

    // ServiceAsSchema (default)
    let config_schema = SchemaConfig {
        strategy: SchemaTableStrategy::ServiceAsSchema,
        tag_keys: vec!["service".to_string()],
        default_name: "default".to_string(),
        prefix_numeric: true,
    };
    group.bench_function("service_as_schema", |b| {
        b.iter(|| parse_line_protocol(black_box(line), &config_schema))
    });

    // ServiceAsTable
    let config_table = SchemaConfig {
        strategy: SchemaTableStrategy::ServiceAsTable,
        tag_keys: vec!["service".to_string()],
        default_name: "default".to_string(),
        prefix_numeric: true,
    };
    group.bench_function("service_as_table", |b| {
        b.iter(|| parse_line_protocol(black_box(line), &config_table))
    });

    // Multiple tag keys (fallback chain)
    let config_multi = SchemaConfig {
        strategy: SchemaTableStrategy::ServiceAsSchema,
        tag_keys: vec!["app".to_string(), "service".to_string(), "svc".to_string()],
        default_name: "default".to_string(),
        prefix_numeric: true,
    };
    group.bench_function("multi_tag_keys", |b| {
        b.iter(|| parse_line_protocol(black_box(line), &config_multi))
    });

    // No matching tag key (uses default)
    let config_nomatch = SchemaConfig {
        strategy: SchemaTableStrategy::ServiceAsSchema,
        tag_keys: vec!["nonexistent".to_string()],
        default_name: "default".to_string(),
        prefix_numeric: true,
    };
    group.bench_function("no_matching_tag", |b| {
        b.iter(|| parse_line_protocol(black_box(line), &config_nomatch))
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
    bench_edge_case_field_values,
    bench_edge_case_names,
    bench_edge_case_timestamps,
    bench_error_paths,
    bench_mixed_batch,
    bench_schema_configs,
);

criterion_main!(benches);
