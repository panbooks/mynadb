# Automated Downsampling Guide

Reduce historical data storage 10-100x by automatically aggregating old high-resolution data into lower-resolution tiers.

## Quick Start

### 1. Enable Downsampling

Add to your `config.toml`:

```toml
[downsampling]
enabled = true
```

### 2. Create a Policy

```bash
curl -X POST http://localhost:7000/downsampling/policies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cpu_downsampling",
    "source_table": "mydb.default.cpu",
    "tiers": [
      {
        "name": "1m",
        "after_days": 7,
        "interval": "1 minute",
        "aggregates": ["min(usage)", "max(usage)", "avg(usage)"],
        "retention_days": 90
      },
      {
        "name": "1h",
        "after_days": 30,
        "interval": "1 hour",
        "aggregates": ["min(usage)", "max(usage)", "avg(usage)"],
        "retention_days": 365
      },
      {
        "name": "1d",
        "after_days": 90,
        "interval": "1 day",
        "aggregates": ["avg(usage)"]
      }
    ]
  }'
```

### 3. Verify

```bash
# List all policies
curl http://localhost:7000/downsampling/policies

# Check a specific policy and its progress
curl http://localhost:7000/downsampling/policies/cpu_downsampling
```

The background job will automatically start processing eligible partitions on the next check cycle.

## How It Works

### Tier Processing

Given the policy above, data flows through tiers based on age:

```
Age 0-7 days:   Raw data at full resolution (e.g., 1-second intervals)
Age 7+ days:    Aggregated to 1-minute intervals → cpu__ds_1m table
Age 30+ days:   Aggregated to 1-hour intervals   → cpu__ds_1h table
Age 90+ days:   Aggregated to 1-day intervals     → cpu__ds_1d table
```

### What Gets Preserved

Each tier computes the specified aggregates. For example, with `["min(usage)", "max(usage)", "avg(usage)"]`:

- You can still see the minimum, maximum, and average for each time bucket
- Tag columns (string columns like `host`, `region`) are preserved as GROUP BY dimensions
- The `time` column is bucketed to the tier interval

### Storage Layout

Tier tables are created alongside the source table:

```
data/mydb/default/
├── cpu/              ← Source table (full resolution)
│   ├── date=2026-01-01/
│   └── date=2026-02-01/
├── cpu__ds_1m/       ← 1-minute tier
│   ├── date=2026-01-01/
│   └── date=2026-01-15/
├── cpu__ds_1h/       ← 1-hour tier
│   └── date=2026-01-01/
└── cpu__ds_1d/       ← 1-day tier
    └── date=2025-10-01/
```

## Policy Configuration

### Tiers

Each tier defines:

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `name` | Yes | Unique tier identifier | `"1m"`, `"1h"`, `"1d"` |
| `after_days` | Yes | Process partitions older than N days | `7`, `30`, `90` |
| `interval` | Yes | Time bucket interval for aggregation | `"1 minute"`, `"1 hour"`, `"1 day"` |
| `aggregates` | Yes | Aggregate functions to compute | `["avg(usage)", "max(temp)"]` |
| `retention_days` | No | Auto-delete tier data after N days | `90`, `365` |

### Supported Aggregates

| Function | Example | Output Column |
|----------|---------|---------------|
| `min(column)` | `min(usage)` | `min_usage` |
| `max(column)` | `max(usage)` | `max_usage` |
| `avg(column)` | `avg(usage)` | `avg_usage` |
| `sum(column)` | `sum(bytes)` | `sum_bytes` |
| `count(*)` | `count(*)` | `count` |
| `first_value(column)` | `first_value(name)` | `first_value_name` |
| `last_value(column)` | `last_value(name)` | `last_value_name` |

### Multi-Tier Example

A typical production configuration with three tiers:

```json
{
  "name": "metrics_ds",
  "source_table": "prod.default.system_metrics",
  "tiers": [
    {
      "name": "5m",
      "after_days": 3,
      "interval": "5 minutes",
      "aggregates": ["min(cpu)", "max(cpu)", "avg(cpu)", "avg(memory)", "max(disk_io)"],
      "retention_days": 30
    },
    {
      "name": "1h",
      "after_days": 30,
      "interval": "1 hour",
      "aggregates": ["min(cpu)", "max(cpu)", "avg(cpu)", "avg(memory)"],
      "retention_days": 365
    },
    {
      "name": "1d",
      "after_days": 90,
      "interval": "1 day",
      "aggregates": ["avg(cpu)", "avg(memory)"]
    }
  ]
}
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/downsampling/policies` | GET | List all policies with status |
| `/downsampling/policies` | POST | Create a new policy |
| `/downsampling/policies/:name` | GET | Get policy details and tier progress |
| `/downsampling/policies/:name` | DELETE | Delete a policy |

### Create Policy Request

```json
{
  "name": "string (required)",
  "source_table": "catalog.schema.table (required)",
  "tiers": [
    {
      "name": "string",
      "after_days": 7,
      "interval": "1 minute",
      "aggregates": ["min(col)", "max(col)", "avg(col)"],
      "retention_days": 90
    }
  ],
  "enabled": true
}
```

### List Policies Response

```json
{
  "enabled": true,
  "policies": [
    {
      "name": "cpu_downsampling",
      "source_table": "mydb.default.cpu",
      "enabled": true,
      "tiers": [
        {
          "name": "1m",
          "after_days": 7,
          "interval": "1 minute",
          "aggregates": ["min(usage)", "max(usage)", "avg(usage)"],
          "retention_days": 90
        }
      ],
      "status": {
        "policy_name": "cpu_downsampling",
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
    }
  ]
}
```

## Configuration Reference

```toml
[downsampling]
# Enable automated downsampling (default: false)
enabled = true

# How often to check for eligible partitions, in seconds (default: 3600)
# Lower values mean faster processing but more CPU usage
check_interval_secs = 3600

# Maximum concurrent downsampling operations (default: 4)
# Each operation processes one partition for one tier
max_concurrent_jobs = 4

# Enable transparent query rewriting to use tier tables (default: true)
# When enabled, queries on old data may be redirected to tier tables
query_rewriting_enabled = true
```

## Querying Downsampled Data

### Direct Query

Tier tables are regular tables and can be queried directly:

```sql
-- Query the 1-minute tier directly
SELECT time, host, avg_usage
FROM mydb.default.cpu__ds_1m
WHERE date >= '2026-01-01' AND date <= '2026-01-31'
ORDER BY time

-- Query the 1-hour tier
SELECT time, avg_usage
FROM mydb.default.cpu__ds_1h
WHERE date >= '2025-01-01'
ORDER BY time
```

### Transparent Rewriting

When `query_rewriting_enabled = true`, queries on the source table are automatically analyzed. If the query's time range falls entirely within a tier's coverage, the query rewriter logs which tier would be optimal.

### Schema of Tier Tables

Tier tables have this structure:

| Column | Type | Description |
|--------|------|-------------|
| `time` | Timestamp | Bucketed time (start of interval) |
| `date` | Date32 | Date partition column |
| *tag columns* | Utf8 | All string columns from source (e.g., `host`) |
| `min_{col}` | Float64 | Minimum of source column |
| `max_{col}` | Float64 | Maximum of source column |
| `avg_{col}` | Float64 | Average of source column |
| `count` | Int64 | Row count per bucket (if configured) |

## Monitoring

### Check Processing Progress

```bash
# Get detailed status for a policy
curl http://localhost:7000/downsampling/policies/cpu_downsampling | jq '.status'
```

The status shows per-tier progress:
- `last_processed_date`: Most recent partition that was downsampled
- `total_rows_written`: Cumulative rows written to this tier
- `total_bytes_written`: Cumulative bytes written
- `partitions_processed`: Number of partitions processed
- `last_run_at`: Timestamp of the last processing run

### Log Messages

Key log entries to watch:

```
INFO  Downsampling: ENABLED (interval: 3600s, max_concurrent: 4)
INFO  Downsampling cycle: 2 policies, 14 partitions processed, 145000 rows written
DEBUG Downsampled mydb.default.cpu date=2026-01-15 → cpu__ds_1m (8500 rows)
WARN  Failed to downsample mydb.default.cpu date=2026-01-10 for tier '1h': ...
```

## Storage Savings Example

For a `cpu` table receiving 1 data point per second per host, with 100 hosts:

| Resolution | Points/day | Storage/day | Ratio |
|------------|-----------|-------------|-------|
| Raw (1s) | 8,640,000 | ~500 MB | 1x |
| 1 minute | 144,000 | ~8 MB | 60x |
| 1 hour | 2,400 | ~0.15 MB | 3,300x |
| 1 day | 100 | ~0.006 MB | 83,000x |

With the example 3-tier policy:
- First 7 days: ~3.5 GB (raw)
- Days 7-30: ~184 MB (1m tier) — **19x savings**
- Days 30-90: ~9 MB (1h tier) — **3,300x savings**
- Days 90+: ~0.4 MB/month (1d tier) — **83,000x savings**

## Authentication

When auth is enabled, the downsampling policy endpoints require `Admin` permission:

```bash
curl -u admin:secret -X POST http://localhost:7000/downsampling/policies \
  -H "Content-Type: application/json" \
  -d '{ ... }'
```
