# Sled DB Benchmark Guide

This guide explains how to use the sled database benchmarks to understand MetaStateMachine performance characteristics at different scales.

## Background

MynaDB's Meta service uses [sled](https://github.com/spacejam/sled) as an embedded key-value store for cluster metadata. Understanding sled's performance is critical for:

- **Capacity planning**: How many entries can we store efficiently?
- **Routing strategy**: File-level (60M entries) vs Table-level (100K entries)
- **Query performance**: Lookup and scan latencies at scale

### Scale Comparison

| Routing Strategy | Typical Entries | Key Example |
|-----------------|-----------------|-------------|
| File-level (deprecated) | ~60 million | `/files/catalog/schema/table/date=2024-01-01/L0_abc.parquet` |
| Table-level (current) | ~100 thousand | `/tables/catalog/schema/table` |
| Partition replicas | ~1 million | `/replicas/catalog/schema/table/date=2024-01-01` |
| Nodes | ~100s | `/nodes/data-node-0001` |

## Running Benchmarks

### Prerequisites

```bash
# Ensure you're in the project root
cd /path/to/mynadb

# Build in release mode first (faster benchmarks)
cargo build --release
```

### Run All Benchmarks

```bash
# Full benchmark suite (10-30 minutes)
cargo bench --bench sled_bench

# Results are saved to: target/criterion/
```

### Run Specific Benchmark Groups

```bash
# Insert performance
cargo bench --bench sled_bench -- "insert_files"
cargo bench --bench sled_bench -- "insert_tables"

# Lookup performance
cargo bench --bench sled_bench -- "lookup_files"
cargo bench --bench sled_bench -- "lookup_tables"

# Prefix scan performance
cargo bench --bench sled_bench -- "scan_prefix"

# Scale comparison (file vs table routing)
cargo bench --bench sled_bench -- "scale_comparison"

# Mixed read/write workload
cargo bench --bench sled_bench -- "mixed_workload"

# Progressive scale test (1K → 500K)
cargo bench --bench sled_bench -- "progressive_scale"

# Large scale test (1M entries, longer runtime)
cargo bench --bench sled_bench -- "large_scale"

# DB size measurement
cargo bench --bench sled_bench -- "db_size"
```

### Run Single Test

```bash
# Specific scale
cargo bench --bench sled_bench -- "insert_tables/sequential/10000"
cargo bench --bench sled_bench -- "lookup_tables/random_1k_lookups/100000"
```

### Quick Validation

```bash
# Run with fewer samples for quick validation
cargo bench --bench sled_bench -- --sample-size 10 "insert_tables"
```

## Benchmark Groups Explained

### 1. Insert Benchmarks (`insert_files`, `insert_tables`)

Measures sequential insert throughput at different scales.

**What it tests:**
- Time to insert 1K, 10K, 100K entries
- Includes flush to disk

**Key metrics:**
- Throughput: entries/second
- Time per batch

**Expected results:**
```
insert_tables/sequential/1000    time: [5.2 ms 5.4 ms 5.6 ms]
insert_tables/sequential/10000   time: [48 ms 52 ms 56 ms]
insert_tables/sequential/100000  time: [520 ms 580 ms 640 ms]
```

### 2. Lookup Benchmarks (`lookup_files`, `lookup_tables`)

Measures random point lookup performance after pre-populating the database.

**What it tests:**
- 1000 random lookups in databases of different sizes
- Simulates routing lookups

**Key metrics:**
- Time for 1K lookups
- Per-lookup latency (divide by 1000)

**Expected results:**
```
lookup_tables/random_1k_lookups/1000    time: [180 µs 185 µs 190 µs]
lookup_tables/random_1k_lookups/10000   time: [210 µs 220 µs 230 µs]
lookup_tables/random_1k_lookups/100000  time: [280 µs 300 µs 320 µs]
```

**Interpretation:**
- ~0.2-0.3 µs per lookup even at 100K entries
- B-tree provides O(log n) lookups

### 3. Prefix Scan Benchmarks (`scan_prefix`)

Measures iteration performance for range queries.

**What it tests:**
- Scan all files for a single table
- Scan all tables for a catalog
- Full table scan

**Key metrics:**
- Time to iterate results
- Entries scanned per second

**Use cases:**
- Listing tables in a schema
- Finding all partitions for a table
- Cache invalidation on node departure

### 4. Scale Comparison (`scale_comparison`)

Direct comparison of file-level vs table-level routing.

**What it tests:**
- Insert 100K file entries
- Insert 100K table entries

**Why it matters:**
- Shows overhead difference between routing strategies
- File keys are longer (~100 bytes) vs table keys (~50 bytes)
- File values are larger (~200 bytes) vs table values (~150 bytes)

### 5. Mixed Workload (`mixed_workload`)

Simulates realistic read/write patterns.

**What it tests:**
- 80% reads, 20% writes
- Tests at 10K and 100K entry scales

**Why it matters:**
- Real workloads mix reads and writes
- Writes can affect read performance (LSM compaction)

### 6. Progressive Scale (`progressive_scale`)

Shows how performance degrades as the database grows.

**What it tests:**
- Lookup performance at 1K, 10K, 100K, 500K entries
- Insert performance at each scale

**Why it matters:**
- Identifies scaling limits
- Shows when to consider sharding or cleanup

### 7. Large Scale (`large_scale`)

Tests at 1 million entries.

**What it tests:**
- Random lookups in 1M entry database
- Prefix scan (100K results) in 1M entry database

**Runtime:** ~60 seconds per benchmark

### 8. DB Size (`db_size`)

Measures on-disk storage overhead.

**What it tests:**
- Total file size after inserting entries
- Bytes per entry

**Expected results:**
```
DB size for 10000 entries: 3.2 MB (320 bytes/entry)
DB size for 100000 entries: 28.5 MB (285 bytes/entry)
```

## Interpreting Results

### Criterion Output Format

```
insert_tables/sequential/10000
                        time:   [48.234 ms 52.156 ms 56.078 ms]
                        thrpt:  [178.31 Kelem/s 191.73 Kelem/s 207.32 Kelem/s]
                        change: [-2.1234% +0.5678% +3.2345%] (p = 0.12 > 0.05)
                        No change in performance detected.
```

- **time**: [lower bound, estimate, upper bound] with 95% confidence
- **thrpt**: Throughput in elements/second
- **change**: Comparison to previous run (if available)

### Performance Expectations

| Operation | 10K entries | 100K entries | 1M entries |
|-----------|-------------|--------------|------------|
| Single lookup | ~0.2 µs | ~0.3 µs | ~0.5 µs |
| 1K lookups | ~200 µs | ~300 µs | ~500 µs |
| Insert (batch 1K) | ~5 ms | ~6 ms | ~8 ms |
| Prefix scan (1K results) | ~1 ms | ~1.5 ms | ~2 ms |
| Prefix scan (100K results) | ~80 ms | ~100 ms | ~150 ms |

### Red Flags

- **Lookup > 10 µs**: Cache miss, disk I/O
- **Insert degradation > 2x**: Compaction pressure
- **Scan time grows faster than result count**: Fragmentation

## HTML Reports

Criterion generates HTML reports with graphs:

```bash
# After running benchmarks
open target/criterion/report/index.html

# Or specific benchmark
open target/criterion/insert_tables/report/index.html
```

Reports include:
- Timing distribution histograms
- Iteration time plots
- Comparison with previous runs
- Statistical analysis

## Tuning Sled

The benchmark uses these sled settings:

```rust
sled::Config::new()
    .cache_capacity(256 * 1024 * 1024)  // 256MB cache
    .flush_every_ms(Some(1000))          // Flush every second
```

To test different configurations, modify `create_temp_db()` in the benchmark file.

### Key Tuning Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `cache_capacity` | 256 MB | More cache = better read performance |
| `flush_every_ms` | 1000 ms | Lower = more durable, higher latency |
| `mode` | `LowSpace` | `HighThroughput` for write-heavy workloads |

## Comparing Routing Strategies

### File-Level Routing (Deprecated)

```
Entries: ~60 million (100 catalogs × 100 schemas × 100 tables × 365 days × 16 files/day)
Key size: ~100 bytes
Value size: ~200 bytes
Total size: ~18 GB

Problems:
- Memory pressure from large index
- Slow prefix scans for query planning
- Long startup time to load metadata
```

### Table-Level Routing (Current)

```
Entries: ~100 thousand (100 catalogs × 100 schemas × 10 tables average)
Key size: ~50 bytes
Value size: ~150 bytes
Total size: ~20 MB

Benefits:
- 600x fewer entries
- Fast lookups and scans
- Quick startup
- Better cache hit rate
```

## Continuous Benchmarking

To track performance over time:

```bash
# Save baseline
cargo bench --bench sled_bench -- --save-baseline main

# Compare against baseline after changes
cargo bench --bench sled_bench -- --baseline main
```

## Troubleshooting

### Benchmark Hangs

Large scale tests can take several minutes. Use `--sample-size 10` for faster runs:

```bash
cargo bench --bench sled_bench -- --sample-size 10 "large_scale"
```

### High Variance

If results show high variance (wide confidence intervals):

1. Close other applications
2. Disable CPU frequency scaling: `sudo cpupower frequency-set -g performance`
3. Increase sample size: `--sample-size 100`

### Disk Space

Large scale benchmarks create temporary databases:

```bash
# Check /tmp usage
df -h /tmp

# Clean up if needed
rm -rf /tmp/.tmp*
```

## See Also

- [Sled Documentation](https://docs.rs/sled)
- [Criterion User Guide](https://bheisler.github.io/criterion.rs/book/)
- [Architecture: Meta Routing](architecture-meta-routing.md)
