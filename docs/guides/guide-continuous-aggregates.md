# Continuous Aggregates Guide

Continuous Aggregates automatically pre-compute and maintain time-bucketed aggregations of your data, enabling 10-100x faster dashboard queries.

## Quick Start

### 1. Enable the Feature

Add to your `config.toml`:

```toml
[continuous_aggregates]
enabled = true
```

### 2. Create an Aggregate (Programmatic)

```rust
use mynadb::aggregates::{AggregateDefinition, AggregateFunction};

// Create an hourly CPU aggregate
let definition = AggregateDefinition::builder("hourly_cpu", "mydb.default.cpu")
    .time_bucket("time", "1h", "hour")?    // Group by hour
    .group_by("host")                       // Also group by host
    .avg("cpu_usage", "avg_cpu")           // Compute average
    .max("cpu_usage", "max_cpu")           // Compute max
    .min("cpu_usage", "min_cpu")           // Compute min
    .count("sample_count")                  // Count samples
    .refresh_interval(300)                  // Refresh every 5 minutes
    .retention(365)                         // Keep for 1 year
    .build()?;

// Save it
manager.create_aggregate(definition).await?;
```

### 3. Query Automatically Uses Aggregate

Your existing queries are automatically rewritten:

```sql
-- Original query (you write this)
SELECT
    time_bucket(time, 3600000000000, 0) as hour,
    host,
    avg(cpu_usage) as avg_cpu
FROM mydb.default.cpu
WHERE time >= now() - interval '7 days'
GROUP BY hour, host

-- Automatically rewritten to (by the query rewriter)
SELECT hour, host, avg_cpu
FROM mydb.default._ca_hourly_cpu
WHERE hour >= now() - interval '7 days'
```

## Configuration

### Full Configuration Reference

```toml
[continuous_aggregates]
# Enable/disable the feature (default: false)
enabled = true

# How often to check for pending refreshes (default: 60)
refresh_check_interval_secs = 60

# Maximum concurrent refresh operations (default: 4)
max_concurrent_refreshes = 4

# Default refresh interval for new aggregates (default: 300 = 5 min)
default_refresh_interval_secs = 300

# Default retention for aggregate data (default: 365 days)
default_retention_days = 365

# Enable automatic query rewriting (default: true)
query_rewriting_enabled = true

# Combine aggregate + real-time data (default: true)
realtime_union_enabled = true

# Allow hierarchical aggregates (default: true)
hierarchical_enabled = true
```

## Creating Aggregates

### Basic Aggregate

```rust
// Hourly aggregation with basic stats
let def = AggregateDefinition::builder("hourly_metrics", "mydb.default.metrics")
    .time_bucket("time", "1h", "hour")?
    .avg("value", "avg_value")
    .build()?;
```

### Multi-Column Grouping

```rust
// Group by time bucket AND additional columns
let def = AggregateDefinition::builder("hourly_by_host_region", "mydb.default.metrics")
    .time_bucket("time", "1h", "hour")?
    .group_by("host")
    .group_by("region")
    .avg("value", "avg_value")
    .max("value", "max_value")
    .build()?;
```

### Multiple Aggregations

```rust
// Compute multiple statistics
let def = AggregateDefinition::builder("comprehensive_stats", "mydb.default.metrics")
    .time_bucket("time", "5m", "bucket")?
    .group_by("host")
    .avg("cpu", "avg_cpu")
    .max("cpu", "max_cpu")
    .min("cpu", "min_cpu")
    .sum("bytes_sent", "total_bytes")
    .count("request_count")
    .build()?;
```

### With Filter

```rust
// Only aggregate specific data
let def = AggregateDefinition::builder("hourly_errors", "mydb.default.logs")
    .time_bucket("time", "1h", "hour")?
    .group_by("service")
    .filter("level = 'error'")
    .count("error_count")
    .build()?;
```

### Custom Refresh Policy

```rust
let def = AggregateDefinition::builder("hourly_cpu", "mydb.default.cpu")
    .time_bucket("time", "1h", "hour")?
    .avg("cpu", "avg_cpu")
    .refresh_interval(60)     // Refresh every minute
    .retention(30)            // Keep 30 days
    .description("Hourly CPU averages for dashboards")
    .build()?;
```

## Time Bucket Granularities

| Granularity | String | Nanoseconds |
|-------------|--------|-------------|
| 1 second | `"1s"` | 1,000,000,000 |
| 1 minute | `"1m"` | 60,000,000,000 |
| 5 minutes | `"5m"` | 300,000,000,000 |
| 15 minutes | `"15m"` | 900,000,000,000 |
| 1 hour | `"1h"` | 3,600,000,000,000 |
| 1 day | `"1d"` | 86,400,000,000,000 |
| Custom | `"2h"`, `"30m"` | Calculated |

## Aggregate Functions

| Function | Usage | Notes |
|----------|-------|-------|
| `avg` | `.avg("column", "alias")` | Stores sum+count for hierarchical |
| `sum` | `.sum("column", "alias")` | Direct aggregation |
| `count` | `.count("alias")` | Counts all rows |
| `min` | `.min("column", "alias")` | Minimum value |
| `max` | `.max("column", "alias")` | Maximum value |

## Hierarchical Aggregates

Build coarser aggregates from finer ones for multi-tier downsampling:

```rust
// Tier 1: 1-minute aggregates from raw data
let minute_agg = AggregateDefinition::builder("cpu_1m", "mydb.default.cpu")
    .time_bucket("time", "1m", "minute")?
    .group_by("host")
    .avg("usage", "avg_usage")
    .sum("usage", "sum_usage")      // For re-aggregation
    .count("count_usage")            // For re-aggregation
    .refresh_interval(60)
    .retention(7)                    // Keep 7 days
    .build()?;

// Tier 2: 1-hour aggregates from 1-minute aggregates
let hour_agg = AggregateDefinition::builder("cpu_1h", "mydb.default._ca_cpu_1m")
    .time_bucket("minute", "1h", "hour")?
    .group_by("host")
    .avg("avg_usage", "avg_usage")   // Re-aggregates correctly using sum/count
    .refresh_interval(300)
    .retention(30)                   // Keep 30 days
    .build()?;

// Tier 3: 1-day aggregates from 1-hour aggregates
let day_agg = AggregateDefinition::builder("cpu_1d", "mydb.default._ca_cpu_1h")
    .time_bucket("hour", "1d", "day")?
    .group_by("host")
    .avg("avg_usage", "avg_usage")
    .refresh_interval(3600)
    .retention(365)                  // Keep 1 year
    .build()?;
```

## Query Rewriting

### How It Works

1. Query is parsed to extract:
   - Source table
   - Time bucket granularity
   - Aggregate functions
   - Group by columns

2. Matching aggregates are found:
   - Same source table
   - Compatible time bucket (query >= aggregate)
   - Required functions exist

3. Query is rewritten:
   - Table reference changed
   - Function calls replaced with column references

### When Rewriting Occurs

| Query Bucket | Aggregate Bucket | Rewrite? |
|--------------|------------------|----------|
| 1 hour | 1 hour | Yes |
| 1 hour | 5 minutes | Yes (coarser query) |
| 5 minutes | 1 hour | No (finer than aggregate) |
| 1 day | 1 hour | Yes |

### Disable Rewriting for Specific Queries

```sql
-- Add a hint to skip rewriting
SELECT /*+ NO_AGGREGATE_REWRITE */
    time_bucket(time, 3600000000000, 0) as hour,
    avg(cpu_usage)
FROM mydb.default.cpu
GROUP BY hour
```

## Real-time Union

When `realtime_union_enabled = true`, queries automatically combine:
- Pre-aggregated data (before watermark)
- Real-time raw data (after watermark)

This ensures queries always return up-to-date results while still benefiting from aggregates.

### Example

```
Watermark: 2024-01-15 10:00:00 (last refresh)
Query: Last 24 hours

Result combines:
├── Aggregate data: 2024-01-14 10:00 to 2024-01-15 10:00
└── Real-time data: 2024-01-15 10:00 to now (aggregated on-the-fly)
```

## Managing Aggregates

### List All Aggregates

```rust
let aggregates = manager.list_aggregates().await?;
for agg in aggregates {
    println!("{}: {} -> {} (state: {})",
        agg.name,
        agg.source_table,
        agg.target_table,
        agg.state
    );
}
```

### Get Aggregate Details

```rust
if let Some(agg) = manager.get_aggregate("hourly_cpu").await? {
    println!("Watermark: {}", agg.watermark.high_watermark_ns);
    println!("Rows: {}", agg.watermark.materialized_rows);
    println!("Last refresh: {}", agg.watermark.last_refresh_at);
}
```

### Manual Refresh

```rust
// Trigger immediate refresh
let result = manager.refresh_aggregate("hourly_cpu").await?;
println!("Refreshed {} rows in {}ms",
    result.rows_written,
    result.duration_ms
);
```

### Drop Aggregate

```rust
manager.drop_aggregate("hourly_cpu").await?;
```

## Best Practices

### 1. Choose Appropriate Granularities

| Use Case | Recommended Granularity |
|----------|------------------------|
| Real-time dashboards | 1 minute |
| Hourly reports | 1 hour |
| Daily summaries | 1 day |
| Long-term trends | 1 day or 1 week |

### 2. Match Refresh Interval to Use Case

```rust
// For real-time dashboards: frequent refresh
.refresh_interval(60)   // Every minute

// For daily reports: less frequent
.refresh_interval(3600) // Every hour

// For archival: infrequent
.refresh_interval(86400) // Daily
```

### 3. Set Appropriate Retention

```rust
// Raw data: short retention
// (handled by retention_manager)

// Fine aggregates: medium retention
.retention(30)  // 30 days for 1-minute data

// Coarse aggregates: long retention
.retention(365) // 1 year for hourly data
.retention(0)   // Forever for daily data (0 = no expiry)
```

### 4. Use Hierarchical Aggregates for Large Data

Instead of:
```
Raw (1 year) → 1-hour aggregate
```

Use:
```
Raw (7 days) → 1-min (30 days) → 1-hour (1 year) → 1-day (forever)
```

Benefits:
- Faster refresh (smaller source data)
- Less storage (progressive reduction)
- Flexible retention per tier

### 5. Include Re-aggregation Columns

For `avg` aggregates that will be used hierarchically:

```rust
// Include sum and count for correct re-aggregation
.avg("value", "avg_value")
.sum("value", "value_sum")    // Helper
.count("value_count")          // Helper
```

## Monitoring

### Aggregate Statistics

```rust
// Get stats for all aggregates
for agg in manager.list_aggregates().await? {
    println!("=== {} ===", agg.name);
    println!("  State: {:?}", agg.state);
    println!("  Watermark: {}", agg.watermark.high_watermark_ns);
    println!("  Rows: {}", agg.watermark.materialized_rows);
    println!("  Bytes: {}", agg.watermark.materialized_bytes);
    println!("  Last refresh: {}", agg.watermark.last_refresh_at);
}
```

### Job Statistics

```rust
let stats = manager.job_stats().await;
println!("Cycles completed: {}", stats.cycles_completed);
println!("Refreshes completed: {}", stats.refreshes_completed);
println!("Refreshes failed: {}", stats.refreshes_failed);
println!("Total rows processed: {}", stats.total_rows_processed);
```

## Troubleshooting

### Aggregate Not Refreshing

1. **Check state**: Must be `Active` or `Creating`
   ```rust
   let agg = manager.get_aggregate("my_agg").await?;
   println!("State: {:?}", agg.state);
   ```

2. **Check refresh interval**: Enough time since last refresh?
   ```rust
   let elapsed = now - agg.watermark.last_refresh_at;
   let interval = agg.refresh_policy.interval_secs * 1_000_000_000;
   if elapsed < interval {
       println!("Not due yet");
   }
   ```

3. **Check for errors**: Look at logs
   ```
   RUST_LOG=mynadb::aggregates=debug cargo run
   ```

### Query Not Using Aggregate

1. **Check if enabled**:
   ```toml
   [continuous_aggregates]
   query_rewriting_enabled = true
   ```

2. **Check granularity**: Query bucket must be >= aggregate bucket

3. **Check functions**: Required aggregates must exist in definition

4. **Check table name**: Source table must match exactly

### High Refresh Latency

1. **Reduce batch size**:
   ```rust
   RefreshPolicy {
       max_rows_per_refresh: Some(1_000_000), // Limit to 1M rows
       ..Default::default()
   }
   ```

2. **Increase concurrency**:
   ```toml
   max_concurrent_refreshes = 8
   ```

3. **Use hierarchical aggregates**: Refresh from aggregate instead of raw

## Examples

### Dashboard Metrics

```rust
// Create aggregates for a monitoring dashboard
let cpu_hourly = AggregateDefinition::builder("dashboard_cpu_hourly", "metrics.default.cpu")
    .time_bucket("time", "1h", "hour")?
    .group_by("host")
    .group_by("datacenter")
    .avg("usage", "avg_usage")
    .max("usage", "max_usage")
    .min("usage", "min_usage")
    .refresh_interval(300)
    .retention(30)
    .build()?;

let memory_hourly = AggregateDefinition::builder("dashboard_mem_hourly", "metrics.default.memory")
    .time_bucket("time", "1h", "hour")?
    .group_by("host")
    .avg("used_percent", "avg_used")
    .max("used_percent", "max_used")
    .refresh_interval(300)
    .retention(30)
    .build()?;
```

### IoT Sensor Data

```rust
// Multi-tier aggregation for IoT sensors
let sensor_1m = AggregateDefinition::builder("sensor_1m", "iot.default.readings")
    .time_bucket("time", "1m", "minute")?
    .group_by("sensor_id")
    .group_by("location")
    .avg("temperature", "avg_temp")
    .avg("humidity", "avg_humidity")
    .max("temperature", "max_temp")
    .min("temperature", "min_temp")
    .refresh_interval(60)
    .retention(7)
    .build()?;

let sensor_1h = AggregateDefinition::builder("sensor_1h", "iot.default._ca_sensor_1m")
    .time_bucket("minute", "1h", "hour")?
    .group_by("sensor_id")
    .group_by("location")
    .avg("avg_temp", "avg_temp")
    .max("max_temp", "max_temp")
    .min("min_temp", "min_temp")
    .refresh_interval(300)
    .retention(90)
    .build()?;
```

### Log Analytics

```rust
// Aggregate error counts by service
let error_hourly = AggregateDefinition::builder("errors_hourly", "logs.default.app_logs")
    .time_bucket("time", "1h", "hour")?
    .group_by("service")
    .group_by("error_code")
    .filter("level = 'error'")
    .count("error_count")
    .refresh_interval(300)
    .retention(30)
    .build()?;
```
