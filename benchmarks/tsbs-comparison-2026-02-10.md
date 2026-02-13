# TSBS Benchmark: MynaDB vs InfluxDB 3 Core

**Date:** 2026-02-10
**Tool:** [TSBS (Time Series Benchmark Suite)](https://github.com/timescale/tsbs)

## Environment

| | |
|---|---|
| **OS** | Linux 6.6.87 (WSL2) |
| **CPU** | Shared (WSL2 host) |
| **RAM** | 15 GiB |
| **Storage** | Local filesystem |
| **MynaDB** | v0.1.1-121-g56a5a17 (release build) |
| **InfluxDB 3** | Core v3.8.0 |

## Data Generation

| Parameter | Value |
|---|---|
| Use case | `cpu-only` (10 CPU metrics per host) |
| Hosts | 10,000 |
| Interval | 5 minutes |
| Duration | 3 days (2024-01-01 to 2024-01-04) |
| Total rows | 8,640,000 |
| Total metrics | 86,400,000 |
| Raw data size | ~2.8 GiB (line protocol) |

## Write (Bulk Load) Performance

3-day dataset (8.64M rows, 86.4M metrics, ~2.8 GiB), 8 workers, batch 10K, gzip enabled.

| Configuration | Metrics/sec | Rows/sec | Total time |
|---|---|---|---|
| **MynaDB** WAL on, `sync_on_write=true` | **3,838,782** | **383,878** | **22.51s** |
| **MynaDB** WAL on, `sync_on_write=false` | **3,755,761** | **375,576** | **23.01s** |
| **MynaDB** WAL disabled | 3,606,848 | 360,685 | 23.95s |
| **InfluxDB 3** `--wal-flush-interval=100ms` | 4,132,202 | 413,220 | 20.91s |
| **InfluxDB 3** `--wal-flush-interval=200ms` | 3,914,462 | 391,446 | 22.07s |
| **InfluxDB 3** `--wal-flush-interval=500ms` | 1,596,746 | 159,675 | 54.11s |
| **InfluxDB 3** default (`--wal-flush-interval=1s`) | 800,389 | 80,039 | 107.95s |
| **InfluxDB 3** default + `no_sync=true` | 799,201 | 79,920 | 108.11s |

**MynaDB config:** 4 GiB cache, 5s flush interval.
**InfluxDB 3 config:** Core v3.8.0, default settings unless noted.

### Analysis

- **Default configs:** MynaDB (WAL+sync) delivers **4.8x higher throughput** than InfluxDB 3 (default 1s flush): 3.84M vs 0.80M metrics/sec.
- **Tuned InfluxDB 3:** With `--wal-flush-interval=100ms`, InfluxDB 3 reaches 4.13M metrics/sec, **matching MynaDB**. The default 1s flush interval is the primary bottleneck, not fsync.
- **MynaDB WAL overhead is near-zero:** WAL disabled vs WAL+sync=true are within noise (~3.6-3.8M metrics/sec). The WAL is fully asynchronous and does not block the write path.
- **InfluxDB 3 `no_sync=true` has no effect:** The `no_sync` query parameter (which skips waiting for WAL persistence before responding) made no measurable difference, confirming that InfluxDB 3's WAL does not do synchronous fsync — the bottleneck is the flush interval batching.

### Large Dataset (12-day, 34.56M rows, ~11 GiB)

Tested with the full 12-day dataset (8 workers), default configs:

| Metric | MynaDB | InfluxDB 3 |
|---|---|---|
| **Metrics/sec** | **4,183,522** | 800,273 |
| **Total time** | 82.6s | 431.9s |

MynaDB sustained >4M metrics/sec with an 8 GiB cache. InfluxDB 3 held steady at ~800K metrics/sec (default 1s flush).

## Query Performance

TSBS query benchmark, 500 queries per type, 4 concurrent workers.
MynaDB `high-cpu-all` and `groupby-orderby-limit` run with 1 worker / 100 queries due to memory constraints.

| Query Type | MynaDB mean (ms) | InfluxDB 3 mean (ms) | Winner |
|---|---|---|---|
| **single-groupby-1-1-1** | 70.60 | **24.13** | InfluxDB 3 (2.9x) |
| **single-groupby-5-1-1** | 72.37 | **24.29** | InfluxDB 3 (3.0x) |
| **single-groupby-5-8-1** | 306.44 | **25.69** | InfluxDB 3 (11.9x) |
| **cpu-max-all-1** | **75.32** | 217.91 | MynaDB (2.9x) |
| **cpu-max-all-8** | 483.79 | **210.66** | InfluxDB 3 (2.3x) |
| **double-groupby-1** | **373.42** | 899.05 | MynaDB (2.4x) |
| **high-cpu-all** | 1082.12* | **722.13** | InfluxDB 3 (1.5x) |
| **lastpoint** | **79.06** | 1688.01 | **MynaDB (21.3x)** |
| **groupby-orderby-limit** | **107.94*** | 1198.15 | MynaDB (11.1x) |

\* MynaDB ran with 1 worker / 100 queries; others ran with 4 workers / 500 queries.

### Query Descriptions

| Query | Description |
|---|---|
| single-groupby-1-1-1 | MAX of 1 metric, 1 host, 1 hour, grouped by 1 minute |
| single-groupby-5-1-1 | MAX of 5 metrics, 1 host, 1 hour, grouped by 1 minute |
| single-groupby-5-8-1 | MAX of 5 metrics, 8 hosts, 1 hour, grouped by 1 minute |
| cpu-max-all-1 | MAX of all 10 CPU metrics, 1 host, 8 hours, grouped by 1 hour |
| cpu-max-all-8 | MAX of all 10 CPU metrics, 8 hosts, 8 hours, grouped by 1 hour |
| double-groupby-1 | MEAN of 1 metric, all hosts, 12 hours, grouped by 1 hour and hostname |
| high-cpu-all | SELECT * WHERE usage_user > 90, 12-hour window |
| lastpoint | Last reading per host (GROUP BY hostname ORDER BY time DESC LIMIT 1) |
| groupby-orderby-limit | MAX of 1 metric, grouped by 1 minute, LIMIT 5 |

## Concurrency Benchmark (wrk)

InfluxQL query latency and throughput under varying concurrency, measured with `wrk -t1 -d10s`.
Dataset: 10K hosts, 6 hours, 5min interval (720K rows). Query cache disabled on MynaDB.

**Queries:**
- **Q1** (1 series): `SELECT max(usage_user) FROM cpu WHERE hostname = 'host_249' AND time >= now() - 4h GROUP BY time(5m)`
- **Q2** (~1% series): `SELECT max(usage_user) FROM cpu WHERE rack = '60' AND time >= now() - 4h GROUP BY time(5m)`
- **Q3** (~5% series): `SELECT max(usage_user) FROM cpu WHERE service = '15' AND time >= now() - 4h GROUP BY time(5m)`

### Latency (avg)

| Query | Conc | MynaDB | InfluxDB 3 | Ratio |
|-------|------|--------|------------|-------|
| Q1 (1 series) | 1 | **7.70ms** | 75.37ms | **9.8x** |
| Q1 (1 series) | 10 | **12.29ms** | 148.29ms | **12.1x** |
| Q1 (1 series) | 100 | **87.58ms** | 1,590ms | **18.2x** |
| Q2 (1% series) | 1 | **9.62ms** | 70.59ms | **7.3x** |
| Q2 (1% series) | 10 | **14.93ms** | 164.73ms | **11.0x** |
| Q2 (1% series) | 100 | **86.39ms** | 1,780ms | **20.6x** |
| Q3 (5% series) | 1 | **10.08ms** | 71.19ms | **7.1x** |
| Q3 (5% series) | 10 | **15.54ms** | 170.26ms | **11.0x** |
| Q3 (5% series) | 100 | **92.36ms** | 1,840ms | **19.9x** |

### Throughput (req/sec)

| Query | Conc | MynaDB | InfluxDB 3 | Ratio |
|-------|------|--------|------------|-------|
| Q1 (1 series) | 1 | **130** | 13.5 | **9.6x** |
| Q1 (1 series) | 10 | **817** | 68.2 | **12.0x** |
| Q1 (1 series) | 100 | **1,230** | 47.1 | **26.1x** |
| Q2 (1% series) | 1 | **104** | 14.1 | **7.4x** |
| Q2 (1% series) | 10 | **672** | 62.0 | **10.8x** |
| Q2 (1% series) | 100 | **1,190** | 50.1 | **23.8x** |
| Q3 (5% series) | 1 | **100** | 14.1 | **7.1x** |
| Q3 (5% series) | 10 | **648** | 59.6 | **10.9x** |
| Q3 (5% series) | 100 | **1,120** | 49.5 | **22.6x** |

### Key Takeaways

- MynaDB is **7-10x faster at low concurrency** and **18-26x faster at high concurrency (c=100)**.
- MynaDB scales well from c=1 to c=100: throughput increases from ~100-130 rps to ~1,100-1,230 rps.
- InfluxDB 3 saturates early: throughput barely changes from c=10 to c=100, with latency degrading to ~1.6-1.8s.
- Query selectivity (1 series vs 5% series) has minimal impact on MynaDB latency (~8-10ms at c=1), suggesting efficient partition pruning.

## Summary

### Writes
With default configurations, MynaDB (WAL+sync enabled) delivers **4.8x higher write throughput** than InfluxDB 3 Core: 3.84M vs 0.80M metrics/sec. However, with InfluxDB 3 tuned to `--wal-flush-interval=100ms`, throughput is essentially **equivalent** (~3.8M vs ~4.1M metrics/sec). The performance gap at default settings is due to InfluxDB 3's conservative 1-second WAL flush interval, not fsync overhead.

### Queries
Mixed results:
- **MynaDB wins** on lastpoint (`lastpoint`: 21.3x faster), aggregation-heavy queries spanning all hosts (`double-groupby-1`: 2.4x faster), and order-by-limit queries (`groupby-orderby-limit`: 11.1x faster).
- **InfluxDB 3 wins** on narrow, single/few-host point queries (`single-groupby-*`: 3-12x faster) due to its columnar storage optimizations for tag-based filtering.
- **cpu-max-all-1** (1 host, all metrics): MynaDB is 2.9x faster. With 8 hosts, InfluxDB 3 is 2.3x faster, suggesting MynaDB's multi-host query path has room for optimization.

### Key Observations
1. MynaDB's WAL has near-zero overhead on write throughput. WAL disabled vs WAL+sync=true shows no measurable difference (~3.6-3.8M metrics/sec), meaning the WAL is fully asynchronous.
2. InfluxDB 3's write bottleneck at default settings is the `--wal-flush-interval=1s` batching, not disk sync. Reducing to 100ms closes the gap entirely. The `no_sync` write parameter has no effect.
3. InfluxDB 3's query engine (based on DataFusion/Apache Arrow, same as MynaDB) shows better performance on narrow-host queries, likely due to its native tag indexing.
4. MynaDB's InfluxQL transpiler handles all TSBS query types. The `lastpoint` query (`GROUP BY tag ORDER BY time DESC LIMIT 1`) is transpiled to a `ROW_NUMBER()` window function and is 21.3x faster than InfluxDB 3.
5. Memory usage is a concern for MynaDB on large datasets - the cache + query engine can exceed available RAM on resource-constrained systems.

## Reproduction

```bash
# Generate data
tsbs_generate_data --use-case=cpu-only --format=influx --scale=10000 \
  --timestamp-start="2024-01-01T00:00:00Z" --timestamp-end="2024-01-04T00:00:00Z" \
  --log-interval=300s --seed=123 > data.txt

# Load MynaDB
tsbs_load_influx --urls=http://localhost:7000 --db-name=benchmark \
  --workers=8 --batch-size=10000 --gzip=true < data.txt

# Load InfluxDB 3
tsbs_load_influx --urls=http://localhost:8181 --db-name=benchmark \
  --workers=8 --batch-size=10000 --gzip=true < data.txt

# Generate queries
tsbs_generate_queries --use-case=cpu-only --format=influx --scale=10000 \
  --query-type=cpu-max-all-1 --timestamp-start="2024-01-01T00:00:00Z" \
  --timestamp-end="2024-01-04T00:00:01Z" --queries=500 --seed=123 > queries.dat

# Run queries
tsbs_run_queries_influx --urls=http://localhost:7000 --db-name=benchmark \
  --workers=4 --max-queries=500 --file=queries.dat
```
