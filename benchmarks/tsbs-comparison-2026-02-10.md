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

| Metric | MynaDB | InfluxDB 3 | Ratio |
|---|---|---|---|
| **Total time** | **20.35s** | 107.49s | **5.3x faster** |
| **Metrics/sec** | **4,245,636** | 803,803 | **5.3x faster** |
| **Rows/sec** | **424,564** | 80,380 | **5.3x faster** |
| Workers | 8 | 8 | |
| Batch size | 10,000 | 10,000 | |
| Gzip | yes | yes | |

MynaDB config: 4 GiB cache, 5s flush interval, WAL disabled.

### Large Dataset (12-day, 34.56M rows, ~11 GiB)

Tested with the full 12-day dataset (8 workers):

| Metric | MynaDB | InfluxDB 3 |
|---|---|---|
| **Metrics/sec** | **4,183,522** | 800,273 |
| **Total time** | 82.6s | 431.9s |

MynaDB sustained >4M metrics/sec with an 8 GiB cache. InfluxDB 3 held steady at ~800K metrics/sec.

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
| **lastpoint** | N/A (unsupported)\*\* | 1688.01 | - |
| **groupby-orderby-limit** | **107.94*** | 1198.15 | MynaDB (11.1x) |

\* MynaDB ran with 1 worker / 100 queries; others ran with 4 workers / 500 queries.
\*\* Lastpoint now supported via ROW_NUMBER() window function. See `correctness-comparison-2026-02-10.md` for verification. Re-benchmark pending.

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

## Summary

### Writes
MynaDB delivers **5.3x higher write throughput** than InfluxDB 3 Core, sustaining over 4 million metrics/sec (424K rows/sec) on the same hardware.

### Queries
Mixed results:
- **MynaDB wins** on aggregation-heavy queries spanning all hosts (`double-groupby-1`: 2.4x faster) and order-by-limit queries (`groupby-orderby-limit`: 11.1x faster).
- **InfluxDB 3 wins** on narrow, single/few-host point queries (`single-groupby-*`: 3-12x faster) due to its columnar storage optimizations for tag-based filtering.
- **cpu-max-all-1** (1 host, all metrics): MynaDB is 2.9x faster. With 8 hosts, InfluxDB 3 is 2.3x faster, suggesting MynaDB's multi-host query path has room for optimization.

### Key Observations
1. MynaDB's write path is highly optimized for bulk ingestion with its in-memory cache architecture.
2. InfluxDB 3's query engine (based on DataFusion/Apache Arrow, same as MynaDB) shows better performance on narrow-host queries, likely due to its native tag indexing.
3. MynaDB's InfluxQL transpiler handles all TSBS query types. The `lastpoint` query (`GROUP BY tag ORDER BY time DESC LIMIT 1`) is now supported via `ROW_NUMBER()` window function transpilation.
4. Memory usage is a concern for MynaDB on large datasets - the cache + query engine can exceed available RAM on resource-constrained systems.

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
