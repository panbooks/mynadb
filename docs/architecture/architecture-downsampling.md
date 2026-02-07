# Automated Downsampling Architecture

## Overview

Automated Downsampling reduces storage costs 10-100x by progressively aggregating old high-resolution data into lower-resolution tiers. Policies define per-table rules (e.g., raw 1s data → 1m after 7 days → 1h after 30 days → 1d after 90 days). A background job discovers eligible partitions, executes aggregation queries, writes tier tables as Parquet files, and the query rewriter transparently directs historical queries to the appropriate tier.

## Design Goals

1. **Policy-driven**: Per-table tier configurations with configurable aggregates
2. **Zero user intervention**: Background job runs on a configurable interval
3. **Transparent queries**: AnalyzerRule rewrites queries to use tier tables automatically
4. **Write-path unaffected**: Downsampling is read-only on source data, write-only to tier tables
5. **Backward compatible**: Disabled by default; zero overhead when not enabled

## Architecture Diagram

```
                    ┌──────────────────────────────────────────────────┐
                    │              POLICY MANAGEMENT                    │
                    │                                                   │
  POST /policies ───┼──► create_downsampling_policy_handler            │
  GET  /policies ───┼──► list_downsampling_policies_handler            │
  GET  /policies/:n ┼──► get_downsampling_policy_handler               │
  DELETE /policies/:n──► delete_downsampling_policy_handler            │
                    │         │                                        │
                    │         ▼                                        │
                    │    DownsamplingStorage                           │
                    │    {data_dir}/_downsampling/                     │
                    │         ├── {policy}.json    (definition)        │
                    │         └── status/                              │
                    │             └── {policy}.json (tier progress)    │
                    └───────────────────────────────────────┬──────────┘
                                                            │
                    ┌───────────────────────────────────────┼──────────┐
                    │           BACKGROUND JOB              │          │
                    │                                       ▼          │
                    │    DownsamplingJob                               │
                    │         │                                        │
                    │         ├─► tokio::interval(check_interval_secs) │
                    │         │                                        │
                    │         ├─► For each enabled policy:             │
                    │         │     ├─► parse_source_table()           │
                    │         │     ├─► discover_table_partitions()    │
                    │         │     │                                  │
                    │         │     ├─► For each tier:                 │
                    │         │     │     ├─► Compute cutoff date      │
                    │         │     │     ├─► Filter eligible parts    │
                    │         │     │     ├─► Skip already processed   │
                    │         │     │     │                            │
                    │         │     │     ├─► For each partition:      │
                    │         │     │     │     ├─► Acquire semaphore  │
                    │         │     │     │     ├─► Build agg SQL      │
                    │         │     │     │     ├─► Execute query      │
                    │         │     │     │     ├─► Write Parquet      │
                    │         │     │     │     └─► Update status      │
                    │         │     │     │                            │
                    │         │     │     └─► Save tier status         │
                    │         │     └───────────────────               │
                    │         │                                        │
                    │         └─► Log cycle statistics                 │
                    └──────────────────────────────────────────────────┘

                    ┌──────────────────────────────────────────────────┐
                    │           QUERY REWRITER (AnalyzerRule)           │
                    │                                                   │
  SQL Query ────────┼──► DownsamplingRewriter::analyze(LogicalPlan)    │
                    │         │                                        │
                    │         ├─► Walk plan tree for TableScan nodes   │
                    │         ├─► Skip __ds_ tier tables (no recursion)│
                    │         ├─► Match table against policies         │
                    │         ├─► Extract date/time range from filters │
                    │         ├─► Find best tier for the range         │
                    │         │   (coarsest tier whose cutoff < max)   │
                    │         └─► Log rewrite intent                   │
                    └──────────────────────────────────────────────────┘

                    ┌──────────────────────────────────────────────────┐
                    │           STORAGE LAYOUT                         │
                    │                                                   │
                    │  data/mydb/default/                              │
                    │    ├── cpu/                    (source table)     │
                    │    │   ├── date=2026-01-01/                      │
                    │    │   │   └── L0_*.parquet    (raw data)        │
                    │    │   ├── date=2026-01-15/                      │
                    │    │   │   └── L0_*.parquet                      │
                    │    │   └── date=2026-02-01/                      │
                    │    │       └── L0_*.parquet                      │
                    │    │                                             │
                    │    ├── cpu__ds_1m/             (1-minute tier)    │
                    │    │   ├── date=2026-01-01/                      │
                    │    │   │   └── L0_*.parquet    (downsampled)     │
                    │    │   └── date=2026-01-15/                      │
                    │    │       └── L0_*.parquet                      │
                    │    │                                             │
                    │    └── cpu__ds_1h/             (1-hour tier)      │
                    │        └── date=2026-01-01/                      │
                    │            └── L0_*.parquet    (downsampled)     │
                    └──────────────────────────────────────────────────┘
```

## Component Details

### DownsamplingPolicy (`src/jobs/downsampling/types.rs`)

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | Unique policy identifier |
| `source_table` | `String` | Source in `catalog.schema.table` format |
| `tiers` | `Vec<DownsamplingTier>` | Ordered tiers (finest → coarsest) |
| `enabled` | `bool` | Whether this policy is active |
| `created_at` | `i64` | Creation timestamp (epoch nanos) |
| `modified_at` | `i64` | Last modification timestamp |

### DownsamplingTier

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | Tier name (e.g., `"1m"`, `"1h"`, `"1d"`) |
| `after_days` | `u32` | Apply to partitions older than N days |
| `interval` | `String` | `time_bucket` interval (e.g., `"1 minute"`) |
| `aggregates` | `Vec<AggregateSpec>` | Aggregate functions to compute |
| `retention_days` | `Option<u32>` | Optional retention for tier data |

### AggregateSpec / AggregateFn

Supported aggregate functions: `min`, `max`, `avg`, `sum`, `count`, `first` (`first_value`), `last` (`last_value`).

Each spec maps to a SQL expression:
```
AggregateSpec { function: Min, column: "usage" }  →  min(usage) AS min_usage
AggregateSpec { function: Count, column: "*" }     →  count(*) AS count
```

### DownsamplingStorage (`src/jobs/downsampling/storage.rs`)

Persists policies and tier status as JSON files:

```
{data_dir}/_downsampling/
├── cpu_ds.json              # Policy definition
├── memory_ds.json
└── status/
    ├── cpu_ds.json          # Per-tier progress tracking
    └── memory_ds.json
```

Uses an in-memory `RwLock<HashMap>` cache for fast reads with disk persistence for durability across restarts.

### DownsamplingJob (`src/jobs/downsampling/job.rs`)

Background job that runs on `tokio::time::interval`:

1. Lists all enabled policies from storage
2. For each policy, discovers date partitions via `DatabaseManager`
3. For each tier, computes the cutoff date (`now - after_days`)
4. Filters partitions: older than cutoff AND not yet processed
5. Discovers tag columns from `information_schema.columns`
6. Builds aggregation SQL via `query_builder`
7. Executes via `QueryEngine::execute_query_with_db()`
8. Writes results via `ParquetWriter::write_batches_partitioned()`
9. Updates tier status (last processed date, rows, bytes)

Concurrency is controlled by a `Semaphore(max_concurrent_jobs)`.

### Query Builder (`src/jobs/downsampling/query_builder.rs`)

Generates SQL like:
```sql
SELECT time_bucket('1 minute', time) AS time,
       host,
       min(usage) AS min_usage,
       max(usage) AS max_usage,
       avg(usage) AS avg_usage
FROM mydb.default.cpu
WHERE date = '2026-01-15'
GROUP BY time_bucket('1 minute', time), host
ORDER BY time
```

Tag columns (string-type) are preserved as GROUP BY dimensions.

### DownsamplingRewriter (`src/query/analyzers/downsampling_rewriter.rs`)

A DataFusion `AnalyzerRule` that:

1. Walks the `LogicalPlan` tree looking for `TableScan` nodes
2. Checks if the scanned table has a matching downsampling policy
3. Extracts date/time range predicates from scan filters
4. Finds the coarsest tier whose `after_days` cutoff covers the query's max date
5. Logs the tier that would serve the query

Skips tables containing `__ds_` to prevent recursive rewriting.

## Tier Table Naming Convention

Tier tables are created as siblings of the source table in the same catalog/schema:

```
Source:  mydb.default.cpu
Tier 1m: mydb.default.cpu__ds_1m
Tier 1h: mydb.default.cpu__ds_1h
Tier 1d: mydb.default.cpu__ds_1d
```

The `__ds_` separator is unique enough to avoid collisions with real table names and is easy to detect for the query rewriter.

## Configuration

```toml
[downsampling]
# Enable automated downsampling (default: false)
enabled = true

# Interval between downsampling checks in seconds (default: 3600)
check_interval_secs = 3600

# Maximum concurrent downsampling operations (default: 4)
max_concurrent_jobs = 4

# Enable transparent query rewriting to tier tables (default: true)
query_rewriting_enabled = true
```

## Processing Flow

```
Partition: date=2026-01-01  (age: 37 days)

Policy: cpu_ds
  Tier "1m" (after_days: 7)   → ELIGIBLE (37 > 7)
  Tier "1h" (after_days: 30)  → ELIGIBLE (37 > 30)
  Tier "1d" (after_days: 90)  → NOT YET  (37 < 90)

Processing order:
  1. Tier "1m": Run aggregation → write to cpu__ds_1m/date=2026-01-01/
  2. Tier "1h": Run aggregation → write to cpu__ds_1h/date=2026-01-01/
  (Tier "1d" skipped until partition is 90+ days old)
```

## Status Tracking

Each policy tracks per-tier processing progress:

```json
{
  "policy_name": "cpu_ds",
  "tiers": [
    {
      "tier_name": "1m",
      "last_processed_date": "2026-01-20",
      "total_rows_written": 145000,
      "total_bytes_written": 2097152,
      "partitions_processed": 14,
      "last_run_at": 1738886400000
    }
  ]
}
```

Only partitions with dates newer than `last_processed_date` are processed in subsequent runs, ensuring idempotent progress.

## File Map

| File | Responsibility |
|------|----------------|
| `src/jobs/downsampling/mod.rs` | Module root, re-exports |
| `src/jobs/downsampling/types.rs` | Policy, Tier, AggregateSpec, Status types |
| `src/jobs/downsampling/storage.rs` | JSON-file persistence with in-memory cache |
| `src/jobs/downsampling/query_builder.rs` | Aggregation SQL generation |
| `src/jobs/downsampling/job.rs` | Background job, partition discovery, execution |
| `src/query/analyzers/downsampling_rewriter.rs` | AnalyzerRule for transparent query rewriting |
| `src/config.rs` | `DownsamplingConfig` struct and defaults |
| `src/services/api.rs` | Policy CRUD endpoints |
| `src/node/standalone.rs` | Initialization and wiring |

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Policy lookup | O(1) via HashMap cache |
| Partition discovery | O(n) via Object Store list |
| Aggregation | Depends on partition size (uses DataFusion) |
| Tier write | Single Parquet file per partition per tier |
| Storage reduction | 10-100x depending on interval ratio |
| Background overhead | Configurable interval + concurrency limit |
