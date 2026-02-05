# Sled DB Benchmark Results

This document contains benchmark results for sled database performance at various scales, measuring the performance characteristics critical for MynaDB's Meta service.

## Test Environment

- **CPU**: Intel/AMD x64 (WSL2)
- **Memory**: Configured with 512MB sled cache
- **Storage**: Local SSD
- **Rust**: Release build with optimizations

## Summary

| Scale | Insert Rate | Per-Insert | Lookup Rate | Per-Lookup | Full Scan |
|-------|-------------|------------|-------------|------------|-----------|
| **1K** | 290K/sec | 3.5 µs | 4.2M/sec | 0.24 µs | - |
| **10K** | 350K/sec | 2.9 µs | 3.9M/sec | 0.26 µs | - |
| **100K** | 370K/sec | 2.7 µs | 3.5M/sec | 0.29 µs | 178 µs |
| **10M** | 73K/sec | 13.7 µs | 45K/sec | 22.2 µs | 16.4 sec |

## Detailed Results

### 1K - 100K Entries (Table-Level Routing Scale)

These benchmarks represent the typical scale for table-level routing (~100K tables).

#### Write Performance (Insert)

| Scale | Total Time | Throughput | Per-Entry Time |
|-------|-----------|------------|----------------|
| 1K entries | 3.45 ms | **290K ops/sec** | 3.45 µs |
| 10K entries | 28.6 ms | **350K ops/sec** | 2.86 µs |
| 100K entries | 270 ms | **370K ops/sec** | 2.70 µs |

#### Read Performance (Random Lookup)

| DB Size | 1K Lookups Time | Throughput | Per-Lookup Time |
|---------|-----------------|------------|-----------------|
| 1K entries | 239 µs | **4.18M ops/sec** | 0.24 µs |
| 10K entries | 258 µs | **3.87M ops/sec** | 0.26 µs |
| 100K entries | 285 µs | **3.50M ops/sec** | 0.29 µs |

#### Scan Performance (Prefix Scan)

| Operation | Time | Notes |
|-----------|------|-------|
| Scan single table files (10K DB) | 2.0 µs | ~10 results |
| Scan single table files (100K DB) | 21 µs | ~100 results |
| Scan all tables for catalog | 17 µs | ~10K results in 100K DB |
| Scan ALL 100K tables | 178 µs | Full table scan |

---

### 10M Entries (File-Level Routing Scale)

These benchmarks simulate file-level routing at a fraction of full scale (60M would be typical).

#### Insert Performance

| Milestone | Cumulative Time | Insert Rate | Degradation |
|-----------|-----------------|-------------|-------------|
| 1M entries | 10.1s | 99K/sec | - |
| 2M entries | 20.5s | 98K/sec | 1% |
| 3M entries | 34.3s | 87K/sec | 12% |
| 4M entries | 46.4s | 86K/sec | 13% |
| 5M entries | 58.2s | 86K/sec | 13% |
| 6M entries | 70.5s | 85K/sec | 14% |
| 7M entries | 87.9s | 80K/sec | 19% |
| 8M entries | 106.0s | 75K/sec | 24% |
| 9M entries | 121.5s | 74K/sec | 25% |
| **10M entries** | **136.8s** | **73K/sec** | **26%** |

**Observation**: Insert rate degrades ~26% from 1M to 10M entries due to increased B-tree depth and compaction overhead.

#### Lookup Performance at 10M Scale

| Metric | Value |
|--------|-------|
| 10K random lookups | 222.45 ms |
| Throughput | **45K lookups/sec** |
| Per-lookup latency | **22.24 µs** |

**Critical Finding**: Lookup latency increases from 0.29 µs (at 100K) to 22.2 µs (at 10M) - a **77x degradation**.

#### Scan Performance at 10M Scale

| Operation | Results | Time | Throughput |
|-----------|---------|------|------------|
| Prefix scan (catalog_0) | 100K entries | 110 ms | 0.91M/sec |
| Full scan (all entries) | 10M entries | 16.4 sec | 0.61M/sec |

#### Storage at 10M Scale

| Metric | Value |
|--------|-------|
| Total DB size | **2.69 GB** |
| Per-entry overhead | 282 bytes |

---

## Performance Degradation Analysis

### Lookup Latency vs Scale

```
Scale     | Per-Lookup | Relative to 100K
----------|------------|------------------
1K        | 0.24 µs    | 0.8x (faster)
10K       | 0.26 µs    | 0.9x
100K      | 0.29 µs    | 1.0x (baseline)
10M       | 22.2 µs    | 77x (slower!)
```

### Why 10M is 77x Slower

1. **B-tree Depth**: More levels to traverse
2. **Cache Misses**: 512MB cache can't hold 2.7GB of data
3. **Page Reads**: Cold lookups require disk I/O
4. **Key Comparison**: Longer keys increase comparison overhead

---

## Routing Strategy Comparison

| Strategy | Typical Entries | Lookup Latency | Viable? |
|----------|-----------------|----------------|---------|
| **Table-level** | ~100K | 0.29 µs | ✅ Excellent |
| **File-level** | ~10M | 22 µs | ⚠️ Marginal |
| **File-level** | ~60M | ~100+ µs (est.) | ❌ Too slow |

### Recommendation

**Table-level routing (100K entries) provides 77x faster lookups than file-level routing would at 10M+ entries.**

Benefits of table-level routing:
- Sub-microsecond lookups (0.29 µs)
- Full table scan in <1ms
- Minimal memory footprint (~30MB vs 2.7GB)
- Consistent performance under load

---

## Capacity Planning

### Memory Requirements

| Scale | DB Size | Recommended Cache | Cache Hit Rate |
|-------|---------|-------------------|----------------|
| 100K | ~30 MB | 64 MB | ~100% |
| 1M | ~300 MB | 512 MB | ~100% |
| 10M | ~2.7 GB | 4 GB | ~80% |

### Throughput Estimates

For a Meta service handling routing requests:

| Scale | Max Lookup QPS | Notes |
|-------|----------------|-------|
| 100K (table-level) | **3.5M QPS** | Single-threaded |
| 10M (file-level) | **45K QPS** | Single-threaded |

With table-level routing, a single Meta node can handle millions of routing lookups per second.

---

## Running These Benchmarks

```bash
# Run standard benchmarks (1K - 100K)
cargo bench --bench sled_bench

# Run 10M scale benchmark
cargo run --release --example sled_10m_bench
```

See [guide-sled-benchmark.md](guide-sled-benchmark.md) for detailed instructions.

---

## Conclusion

The benchmark results strongly validate the decision to use **table-level routing** instead of file-level routing:

1. **77x faster lookups** at realistic scales
2. **Consistent sub-microsecond latency** up to 100K entries
3. **600x fewer entries** to manage (100K tables vs 60M files)
4. **Minimal memory footprint** (~30MB vs 2.7GB+)

Table-level routing is the clear choice for production deployments.
