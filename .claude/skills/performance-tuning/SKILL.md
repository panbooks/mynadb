---
name: performance-tuning
description: This skill should be used when the user is profiling, benchmarking, or optimizing MynaDB performance — tuning cache sizes, query execution, compaction, memory limits, or diagnosing bottlenecks. Also use when working with profiling endpoints, slow query analysis, or resource monitoring.
version: 1.0.0
---

# Performance Tuning in MynaDB

## Profiling Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/debug/heap?duration=10` | GET | Heap profile (pprof, 100Hz) |
| `/debug/cpu?duration=10` | GET | CPU profile (pprof, 100Hz) |
| `/memory` | GET | Memory usage breakdown |
| `/stats` | GET | Write cache statistics |
| `/cache/stats` | GET | Cache hit rates and sizes |

**Heap/CPU profile params:** `duration` (1-300s), `format` (protobuf/flamegraph/text), `download` (bool).

## Query Observability

**File:** `src/observability/query_tracer.rs`

Tracks per-query: duration, row count, partitions scanned, files read, bytes scanned.

**Slow query logger** (`src/observability/slow_query_logger.rs`):
```toml
[tracing]
slow_query_threshold_ms = 1000   # Log queries > 1s
timeline_enabled = true
max_timelines = 1000
sample_rate = 1.0                # 0.0–1.0
```

Use `/explain/:db?sql=...&verbose=true` for query plan analysis and `/analyze/:db?sql=...` for partition/file counts.

## Write Path Tuning

### Cache Configuration
```toml
[cache]
flush_interval_secs = 60      # Periodic flush
max_size_mb = 4096             # 4GB, flushes at 80%
```

- **32 shards** reduce lock contention (constant `NUM_SHARDS`)
- **80% threshold** triggers early flush before OOM
- Monitor via `/stats` endpoint

### WAL Configuration
```toml
[wal]
enabled = true
sync_on_write = false          # true = durable (fsync per write), false = faster
rotate_size_mb = 128
```

`sync_on_write = false` is significantly faster but risks losing last few writes on crash.

## Query Execution Tuning

**File:** `src/query/query_engine/mod.rs`

### DataFusion Session Config
```rust
batch_size = 8192                              // Vectorized execution unit
target_partitions = num_cpus                   // CPU-aware parallelism
enable_round_robin_repartition = false         // Time-series optimization
repartition_file_scans = false                 // Partition-aware scanning
```

### Memory Limits
```rust
DEFAULT_MEMORY_LIMIT_BYTES = 16 * 1024^3      // 16GB
MEMORY_LIMIT_FRACTION = 0.8                    // Spill at 80%
sort_spill_reservation = 100MB
```

Adjust for server resources. DataFusion spills to disk when threshold exceeded.

### Predicate Pushdown (all enabled by default)
- `pushdown_filters`: Push predicates to Parquet reader
- `reorder_filters`: Optimize filter evaluation order
- `pruning`: Row group pruning via min/max statistics
- `enable_page_index`: Page-level pruning
- `bloom_filter_on_read`: String column filtering
- `top_down_join_key_reordering`: ORDER BY + LIMIT optimization

## Compaction Tuning

**Files:** `src/jobs/compactor/`

### LSM Compaction
```toml
[compaction]
min_files = 5                          # Trigger at 5 L0 files
major_min_files = 3                    # L1→L2 threshold
target_file_size_mb = 200              # Output file target
max_level = 4                          # L0 through L4
interval_secs = 60                     # Check frequency
max_concurrent_compactions = 8         # Semaphore limit
read_rate_limit_mb_per_sec = 200       # I/O throttle
```

### Full Compaction
```toml
full_compaction_enabled = true
full_compaction_hour = 3               # Run at 03:00 UTC
full_compaction_target_size_mb = 200   # Skip large partitions
full_compaction_max_concurrent = 2     # Lower to prevent OOM
```

**Trade-offs:**
- More concurrent compactions = faster catch-up, higher memory
- Lower target_file_size = more files, more metadata overhead
- Higher read_rate_limit = faster compaction, more I/O contention with queries

## Query Result Caching

```toml
[query_cache]
enabled = true
max_size_mb = 256
max_entries = 1000
ttl_secs = 300                         # 5-minute TTL
min_query_time_ms = 10                 # Only cache slow queries
```

- SQL normalized before hashing (lowercase, whitespace collapse)
- LRU eviction when full
- Invalidated on table writes
- Monitor via `/cache/stats`

## Metadata Caching

- Table cache TTL: 300s
- File listing cache TTL: 60s
- Parquet metadata cache: 10,000 max entries
- Pre-warm via `POST /cache/prewarm/:catalog`

## Hot Partition Cache

```toml
[hot_partition_cache]
enabled = false
max_size_mb = 1024                     # 1GB
hot_days = 2                           # Today + yesterday
ttl_secs = 1800                        # 30 minutes
```

Caches recent partition Parquet data in memory for frequently queried time ranges.

## Common Bottlenecks

### Write Path
| Symptom | Cause | Fix |
|---------|-------|-----|
| Flush too frequent | Cache too small | Increase `max_size_mb` |
| Write latency spikes | WAL fsync | Set `sync_on_write = false` |
| Lock contention | Single shard hot | Check tag cardinality distribution |

### Query Path
| Symptom | Cause | Fix |
|---------|-------|-----|
| Full table scan | No date filter | Add `WHERE date >= '...'` |
| High memory | Large batch size | Reduce `batch_size` or add `LIMIT` |
| Slow aggregation | Too many files | Run compaction to reduce L0 count |
| Cache misses | Short TTL | Increase `ttl_secs` for stable queries |

### Compaction
| Symptom | Cause | Fix |
|---------|-------|-----|
| L0 file buildup | Compaction too slow | Increase `max_concurrent_compactions` |
| OOM during compaction | Too many concurrent | Reduce `full_compaction_max_concurrent` |
| I/O contention | No rate limit | Set `read_rate_limit_mb_per_sec` |

## Benchmarking

```bash
# Line protocol parsing throughput
cargo bench --bench line_protocol_bench

# Query execution performance
cargo bench --bench query_bench

# Time function UDF performance
cargo bench --bench time_functions_bench
```

Benchmarks use Criterion with parameterized sizes (1K–1M rows), seeded randomness for reproducibility, and `black_box()` to prevent dead code elimination.
