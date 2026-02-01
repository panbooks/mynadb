# Continuous Aggregates Architecture

## Overview

Continuous Aggregates provide TimescaleDB-style materialized views with automatic incremental refresh. They pre-compute aggregations at configurable time granularities, enabling 10-100x faster dashboard queries while reducing storage costs by 80-95% through automatic downsampling.

## Design Goals

1. **Pluggable**: Enable/disable via configuration without code changes
2. **Isolated**: Self-contained module with minimal coupling to core systems
3. **Incremental**: Only process new data since last refresh (watermark-based)
4. **Hierarchical**: Support aggregates built on other aggregates
5. **Transparent**: Automatic query rewriting to use pre-aggregated data

## Architecture Diagram

```
                           ┌─────────────────────────────────────────────────────────────┐
                           │                    QUERY PATH                                │
                           │                                                              │
   SQL Query ──────────────┼──► Query Rewriter ──► Check matching ──► Rewrite to         │
                           │         │             aggregates          aggregate table    │
                           │         │                                       │            │
                           │         ▼                                       ▼            │
                           │    No match? ──────────────────────────► Execute on raw     │
                           │                                              table           │
                           └─────────────────────────────────────────────────────────────┘

                           ┌─────────────────────────────────────────────────────────────┐
                           │                   REFRESH PATH                               │
                           │                                                              │
   Background Job ─────────┼──► Check pending ──► Get watermark ──► Execute refresh      │
   (every 60s)             │    refreshes          (last processed)    SQL query          │
                           │         │                   │                   │            │
                           │         ▼                   ▼                   ▼            │
                           │    Semaphore ──────► Time range: ──────► Write to           │
                           │    (max concurrent)  [watermark, now)    aggregate table     │
                           │                                               │              │
                           │                                               ▼              │
                           │                                         Update watermark     │
                           └─────────────────────────────────────────────────────────────┘

                           ┌─────────────────────────────────────────────────────────────┐
                           │                   DATA FLOW                                  │
                           │                                                              │
                           │   Raw Data (7 days)                                          │
                           │        │                                                     │
                           │        ▼                                                     │
                           │   1-minute aggregates (30 days) ◄── Refresh every 5min      │
                           │        │                                                     │
                           │        ▼                                                     │
                           │   1-hour aggregates (365 days) ◄── Refresh every 1hour      │
                           │        │                                                     │
                           │        ▼                                                     │
                           │   1-day aggregates (forever) ◄── Refresh every 1day         │
                           │                                                              │
                           └─────────────────────────────────────────────────────────────┘
```

## Module Structure

```
src/aggregates/
├── mod.rs              # ContinuousAggregateManager - main entry point
├── config.rs           # ContinuousAggregateConfig - feature configuration
├── types.rs            # Core types (AggregateState, WatermarkInfo, RefreshResult)
├── definition.rs       # AggregateDefinition with builder pattern
├── storage.rs          # AggregateStorage - persistence layer
├── job.rs              # ContinuousAggregateJob - background refresh
└── query_rewriter.rs   # ContinuousAggregateRewriter - query transformation
```

## Core Components

### 1. ContinuousAggregateManager

The central coordinator that manages the lifecycle of continuous aggregates.

```rust
pub struct ContinuousAggregateManager {
    config: ContinuousAggregateConfig,
    storage: Arc<AggregateStorage>,
    job: Option<Arc<ContinuousAggregateJob>>,
    rewriter: Arc<ContinuousAggregateRewriter>,
    running: Arc<RwLock<bool>>,
}
```

**Responsibilities:**
- Start/stop background refresh job
- Create/drop aggregate definitions
- Provide query rewriter for QueryEngine integration
- Manual refresh triggers

### 2. AggregateDefinition

Defines what an aggregate computes and how it refreshes.

```rust
pub struct AggregateDefinition {
    pub name: String,                           // Unique identifier
    pub source_table: String,                   // e.g., "mydb.default.cpu"
    pub target_table: String,                   // e.g., "mydb.default._ca_hourly_cpu"
    pub aggregate_columns: Vec<AggregateColumn>,// What to compute
    pub group_by_columns: Vec<GroupByColumn>,   // How to group (includes time bucket)
    pub filter: Option<String>,                 // Optional WHERE clause
    pub refresh_policy: RefreshPolicy,          // When/how to refresh
    pub retention_days: Option<u32>,            // How long to keep data
    pub state: AggregateState,                  // Creating, Active, Paused, Error
    pub watermark: WatermarkInfo,               // Refresh progress tracking
}
```

### 3. WatermarkInfo

Tracks refresh progress for incremental updates.

```rust
pub struct WatermarkInfo {
    pub aggregate_name: String,
    pub high_watermark_ns: i64,     // All data before this is materialized
    pub low_watermark_ns: i64,      // Used for compaction
    pub last_refresh_at: i64,       // When last refreshed
    pub materialized_rows: u64,     // Total rows in aggregate
    pub materialized_bytes: u64,    // Total bytes in aggregate
    pub last_partition_date: Option<String>,
}
```

### 4. Aggregate Functions

Supported aggregation functions:

| Function | SQL | Re-aggregatable | Notes |
|----------|-----|-----------------|-------|
| `Avg` | `avg()` | No* | Stores sum+count for re-aggregation |
| `Sum` | `sum()` | Yes | Direct re-aggregation |
| `Count` | `count()` | Yes | Direct re-aggregation |
| `Min` | `min()` | Yes | Direct re-aggregation |
| `Max` | `max()` | Yes | Direct re-aggregation |
| `First` | `first_value()` | Yes | By time ordering |
| `Last` | `last_value()` | Yes | By time ordering |
| `Stddev` | `stddev()` | No | Requires raw data |
| `Percentile` | `percentile_cont()` | No | Requires raw data |

*Avg is stored with helper columns (sum, count) to enable hierarchical re-aggregation.

### 5. Query Rewriter

Analyzes incoming queries and rewrites them to use pre-aggregated data.

**Rewrite Conditions:**
1. Query source table matches aggregate source table
2. Query time bucket >= aggregate time bucket
3. Required aggregate functions exist in aggregate
4. Required group by columns exist in aggregate
5. Aggregate is in Active state

**Rewrite Process:**
```
Original:
  SELECT time_bucket(time, 3600000000000, 0) as hour, host, avg(cpu)
  FROM mydb.default.cpu
  WHERE time >= 1704067200000000000
  GROUP BY hour, host

After rewrite:
  SELECT hour, host, avg_cpu
  FROM mydb.default._ca_hourly_cpu
  WHERE hour >= 1704067200000000000
```

## Storage Model

### Definition Storage

Aggregate definitions are stored as JSON files:

```
{data_dir}/_aggregates/
├── hourly_cpu.json           # Definition file
├── daily_metrics.json
└── watermarks/
    ├── hourly_cpu.json       # Watermark state
    └── daily_metrics.json
```

### Aggregate Data Storage

Aggregates are stored as regular MynaDB tables:

```
{data_dir}/{catalog}/{schema}/
├── cpu/                      # Raw table
│   └── date=2024-01-15/
│       └── L0_*.parquet
└── _ca_hourly_cpu/           # Aggregate table (prefixed with _ca_)
    └── date=2024-01-15/
        └── L0_*.parquet
```

## Refresh Mechanism

### Incremental Refresh

1. **Get watermark**: Last processed timestamp
2. **Calculate range**: `[watermark, now - start_offset)`
3. **Generate SQL**:
   ```sql
   SELECT time_bucket(time, {interval}, 0) as {alias},
          {group_cols}, {agg_functions}
   FROM {source_table}
   WHERE time >= {watermark} AND time < {to_watermark}
   GROUP BY {group_cols}
   ```
4. **Execute and write**: Results to aggregate table
5. **Update watermark**: New high watermark

### Refresh Policy

```rust
pub struct RefreshPolicy {
    pub interval_secs: u64,         // Minimum time between refreshes
    pub start_offset_secs: u64,     // Don't refresh last N seconds (late data buffer)
    pub end_offset_secs: Option<u64>, // Bounded window (optional)
    pub max_rows_per_refresh: Option<u64>, // Limit memory usage
    pub full_refresh: bool,         // Recompute all vs incremental
}
```

### Concurrency Control

```rust
// Semaphore limits concurrent refreshes
let semaphore = Arc::new(Semaphore::new(config.max_concurrent_refreshes));

// Each refresh acquires a permit
let _permit = semaphore.acquire().await?;
```

## Hierarchical Aggregates

Aggregates can be built on other aggregates for multi-tier downsampling:

```
Raw data (cpu)
    │
    ▼ time_bucket = 1 minute
1-minute aggregate (_ca_cpu_1m)
    │
    ▼ time_bucket = 1 hour (uses _ca_cpu_1m as source)
1-hour aggregate (_ca_cpu_1h)
    │
    ▼ time_bucket = 1 day (uses _ca_cpu_1h as source)
1-day aggregate (_ca_cpu_1d)
```

**Re-aggregation Support:**

For `avg`, we store helper columns to enable correct re-aggregation:

```sql
-- 1-minute aggregate stores:
SELECT
    time_bucket(time, 60000000000, 0) as minute,
    avg(value) as avg_value,
    sum(value) as avg_value_sum,    -- Helper for re-aggregation
    count(value) as avg_value_count -- Helper for re-aggregation
FROM raw_table
GROUP BY minute

-- 1-hour aggregate (built on 1-minute) computes:
SELECT
    time_bucket(minute, 3600000000000, 0) as hour,
    sum(avg_value_sum) / sum(avg_value_count) as avg_value  -- Correct weighted average
FROM _ca_raw_table_1m
GROUP BY hour
```

## Real-time Union Queries

When `realtime_union_enabled = true`, queries combine aggregate data with real-time raw data:

```sql
-- Watermark at 2024-01-15 10:00:00
-- Query for last 24 hours

-- Generated query:
SELECT * FROM (
    -- Pre-aggregated data (before watermark)
    SELECT hour, host, avg_cpu
    FROM mydb.default._ca_hourly_cpu
    WHERE hour >= '2024-01-14 10:00:00' AND hour < '2024-01-15 10:00:00'

    UNION ALL

    -- Real-time data (after watermark)
    SELECT time_bucket(time, 3600000000000, 0) as hour, host, avg(cpu) as avg_cpu
    FROM mydb.default.cpu
    WHERE time >= '2024-01-15 10:00:00'
    GROUP BY hour, host
)
```

## State Machine

```
                    ┌──────────────────────────────────────┐
                    │                                      │
                    ▼                                      │
┌──────────┐   ┌────────┐   ┌────────┐   ┌─────────┐      │
│ Creating │──►│ Active │──►│ Paused │──►│Dropping │──────┘
└──────────┘   └────────┘   └────────┘   └─────────┘
     │              │            │
     │              ▼            │
     │         ┌─────────┐       │
     └────────►│  Error  │◄──────┘
               └─────────┘
```

| State | Description | Transitions |
|-------|-------------|-------------|
| `Creating` | Initial state, first refresh pending | → Active, Error |
| `Active` | Normal operation, being refreshed | → Paused, Error, Dropping |
| `Paused` | No automatic refresh (manual only) | → Active, Dropping |
| `Error` | Refresh failed, needs attention | → Active (after fix) |
| `Dropping` | Being deleted | → (removed) |

## Performance Considerations

### Query Performance

| Scenario | Raw Table | With Aggregates | Speedup |
|----------|-----------|-----------------|---------|
| Last 7 days, hourly | Scan 7 days of raw data | Scan 168 hourly rows | ~100x |
| Last 30 days, daily | Scan 30 days of raw data | Scan 30 daily rows | ~1000x |
| Dashboard refresh | 2-5 seconds | 20-50 ms | ~100x |

### Storage Savings

| Granularity | Raw Retention | Aggregate Retention | Storage Ratio |
|-------------|---------------|---------------------|---------------|
| 1-second raw | 7 days | - | 100% |
| 1-minute agg | - | 30 days | ~1.7% |
| 1-hour agg | - | 365 days | ~0.03% |
| 1-day agg | - | Forever | ~0.001% |

### Refresh Overhead

- **CPU**: Minimal (background job, semaphore-limited)
- **Memory**: Bounded by `max_rows_per_refresh`
- **I/O**: Incremental (only new data since watermark)
- **Latency**: Configurable via `refresh_interval_secs`

## Configuration Reference

```toml
[continuous_aggregates]
# Master switch - enable/disable the feature
enabled = false

# How often to check for aggregates needing refresh
refresh_check_interval_secs = 60

# Maximum concurrent refresh operations
max_concurrent_refreshes = 4

# Default refresh interval for new aggregates
default_refresh_interval_secs = 300

# Default retention for aggregate data
default_retention_days = 365

# Enable automatic query rewriting
query_rewriting_enabled = true

# Minimum query bucket ratio for rewriting
# Query bucket must be >= N times aggregate bucket
min_bucket_ratio = 1

# Storage directory for definitions (relative to data_dir)
definitions_dir = "_aggregates"

# Combine aggregate with real-time data in queries
realtime_union_enabled = true

# Lag tolerance before adding real-time union
lag_tolerance_secs = 60

# Allow aggregates built on other aggregates
hierarchical_enabled = true
```

## Integration Points

### QueryEngine Integration

```rust
// In QueryEngine::query()
if let Some(rewriter) = &self.aggregate_rewriter {
    if let Some(result) = rewriter.try_rewrite(sql).await {
        // Use rewritten SQL
        return self.execute_sql(&result.rewritten_sql).await;
    }
}
// Fall back to original SQL
```

### Standalone Node Integration

```rust
// In standalone node startup
if config.continuous_aggregates.enabled {
    let manager = ContinuousAggregateManager::new(
        config.continuous_aggregates.clone(),
        &config.storage.data_dir,
    );
    manager.start().await?;
}
```

### Cluster Mode Integration

In cluster mode, aggregate definitions should be stored in the Meta service for cluster-wide consistency:

```rust
// Future: MetaCommand for aggregate operations
pub enum MetaCommand {
    // ... existing commands ...

    // Aggregate operations
    CreateAggregate { definition: AggregateDefinition },
    DropAggregate { name: String },
    UpdateAggregateState { name: String, state: AggregateState },
    UpdateWatermark { name: String, watermark: WatermarkInfo },
}
```

## Future Enhancements

1. **SQL Syntax Support**: `CREATE CONTINUOUS AGGREGATE` statement
2. **Cluster-wide Definitions**: Store in Meta service
3. **Distributed Refresh**: Coordinate across Data nodes
4. **Materialized View Queries**: Direct SQL access to aggregates
5. **Automatic Aggregate Recommendation**: Suggest aggregates based on query patterns
6. **Compression**: Apply aggressive compression to aggregate tables
