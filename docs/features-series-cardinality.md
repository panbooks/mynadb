# Series Cardinality Tracking

## Overview

The series cardinality tracking feature monitors unique time series based on service and metric combinations. This is essential for:
- **Preventing metric explosion** - Detect when services create too many unique series
- **Capacity planning** - Understand data growth patterns
- **Cost optimization** - Identify high-cardinality services
- **Performance monitoring** - Track series count impact on query performance

## How It Works

### 1. Series Definition

A **unique series** is defined by:
- **Service** - Extracted from `MONITOR.service` tag
- **Metric** - The measurement name
- **Tag Hash** - Hash of all other tags (excluding `MONITOR.service`)

### 2. Automatic Tracking

When you write data with the special `MONITOR.service` tag, the system automatically tracks series:

```bash
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=api,MONITOR.ip=127.0.0.1,type=http,host=h1 usage=75.5,cnt=1i'
```

This creates a series record:
- **Service**: `api`
- **Metric**: `cpu`
- **Hash**: Hash of `{type=http, host=h1, MONITOR.ip=127.0.0.1}`

### 3. Hash Calculation

Tags are sorted alphabetically (excluding `MONITOR.service`) before hashing:

```
Tags: type=http, host=h1, MONITOR.ip=127.0.0.1
Sorted: MONITOR.ip=127.0.0.1, host=h1, type=http
Filter: host=h1, type=http (exclude MONITOR.service)
Hash: hash("host=h1type=http")
```

This ensures:
- ✅ Same tags in different order = same hash
- ✅ Different `MONITOR.service` values = same hash (if other tags match)
- ✅ Stable across restarts

## API Endpoint

### GET /series_stats/:db

Get series cardinality statistics grouped by service.

**Request:**
```bash
curl http://localhost:7000/series_stats/mydb
```

**Response:**
```json
{
  "database": "mydb",
  "total_series": 150,
  "service_count": 3,
  "services": [
    {
      "service": "api",
      "total_series": 75,
      "metrics": {
        "cpu": 25,
        "memory": 25,
        "disk": 25
      }
    },
    {
      "service": "web",
      "total_series": 50,
      "metrics": {
        "cpu": 20,
        "request_count": 30
      }
    },
    {
      "service": "worker",
      "total_series": 25,
      "metrics": {
        "cpu": 10,
        "queue_depth": 15
      }
    }
  ]
}
```

**Fields:**
- `total_series` - Total unique series across all services
- `service_count` - Number of distinct services
- `services[]` - Array of per-service statistics
  - `service` - Service name (from `MONITOR.service` tag)
  - `total_series` - Unique series count for this service
  - `metrics` - Map of metric name → series count

## Usage Examples

### Example 1: Basic Tracking

```bash
# Write data with MONITOR.service tag
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=api,host=server01 usage=75.5'

curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=api,host=server02 usage=82.3'

curl -X POST http://localhost:7000/write/mydb \
  -d 'memory,MONITOR.service=api,host=server01 used=16.5'

# Check series stats
curl http://localhost:7000/series_stats/mydb
```

**Result:**
```json
{
  "database": "mydb",
  "total_series": 3,
  "service_count": 1,
  "services": [
    {
      "service": "api",
      "total_series": 3,
      "metrics": {
        "cpu": 2,      // server01 + server02
        "memory": 1     // server01
      }
    }
  ]
}
```

### Example 2: Multiple Services

```bash
# API service
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=api,host=server01,region=us-west usage=75.5'

# Web service
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=web,host=server01,region=us-west usage=82.3'

# Worker service  
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=worker,host=server01,region=us-west usage=65.2'

# Check stats
curl http://localhost:7000/series_stats/mydb
```

**Result:**
```json
{
  "database": "mydb",
  "total_series": 3,
  "service_count": 3,
  "services": [
    {
      "service": "api",
      "total_series": 1,
      "metrics": {"cpu": 1}
    },
    {
      "service": "web",
      "total_series": 1,
      "metrics": {"cpu": 1}
    },
    {
      "service": "worker",
      "total_series": 1,
      "metrics": {"cpu": 1}
    }
  ]
}
```

### Example 3: High Cardinality Detection

```bash
# Service with many hosts (high cardinality)
for i in {1..100}; do
    curl -s -X POST http://localhost:7000/write/mydb \
      -d "cpu,MONITOR.service=api,host=server$(printf %03d $i) usage=$((RANDOM % 100)).5"
done

# Flush to persist series data
curl -X POST http://localhost:7000/flush

# Check stats
curl http://localhost:7000/series_stats/mydb
```

**Result:**
```json
{
  "database": "mydb",
  "total_series": 100,
  "service_count": 1,
  "services": [
    {
      "service": "api",
      "total_series": 100,
      "metrics": {
        "cpu": 100  // ⚠️ High cardinality warning!
      }
    }
  ]
}
```

## Data Storage

Series statistics are stored in:
```
./data/_system/series_stats/{database}.parquet
```

**Schema:**
```sql
CREATE TABLE series_stats (
    service VARCHAR NOT NULL,  -- From MONITOR.service tag
    metric  VARCHAR NOT NULL,  -- Measurement name
    hash    BIGINT NOT NULL    -- Hash of other tags
);
```

**Example file structure:**
```
./data/
├── mydb/
│   ├── cpu/
│   │   └── L0_1234567890.parquet
│   └── memory/
│       └── L0_1234567891.parquet
└── _system/
    └── series_stats/
        └── mydb.parquet  # Series tracking data
```

## Performance Considerations

### 1. Async Tracking

Series tracking happens **asynchronously** and doesn't block write operations:

```rust
// Non-blocking series tracking
tokio::spawn(async move {
    tracker.track_point(&db_name, &point_clone).await;
});

// Write continues immediately
state.cache.add_points(db, &measurement, points).await;
```

### 2. In-Memory Cache

The system maintains an in-memory cache of tracked series to avoid duplicate writes:
- First occurrence → Write to Parquet
- Subsequent occurrences → Skip (already tracked)

### 3. Deduplication

When reading stats, duplicate entries are automatically deduplicated using HashSet:

```rust
stats
    .entry(service)
    .or_insert_with(HashMap::new)
    .entry(metric)
    .or_insert_with(HashSet::new)  // Dedup by hash
    .insert(hash);
```

## Monitoring Best Practices

### 1. Set Up Alerts

Monitor series count to detect metric explosion:

```bash
# Get current count
SERIES_COUNT=$(curl -s http://localhost:7000/series_stats/mydb | jq '.total_series')

if [ $SERIES_COUNT -gt 10000 ]; then
    echo "⚠️ WARNING: High series count ($SERIES_COUNT)"
fi
```

### 2. Per-Service Limits

Identify services with high cardinality:

```bash
curl -s http://localhost:7000/series_stats/mydb | \
  jq -r '.services[] | select(.total_series > 1000) | 
    "⚠️ \(.service): \(.total_series) series"'
```

### 3. Metric-Level Analysis

Find high-cardinality metrics:

```bash
curl -s http://localhost:7000/series_stats/mydb | \
  jq -r '.services[] | .service as $svc | 
    .metrics | to_entries[] | 
    select(.value > 100) | 
    "⚠️ \($svc)/\(.key): \(.value) series"'
```

## Without MONITOR.service Tag

If you don't include `MONITOR.service` tag, series are tracked under service name `"unknown"`:

```bash
# No MONITOR.service tag
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'

# Check stats
curl http://localhost:7000/series_stats/mydb
```

**Result:**
```json
{
  "database": "mydb",
  "total_series": 1,
  "service_count": 1,
  "services": [
    {
      "service": "unknown",
      "total_series": 1,
      "metrics": {"cpu": 1}
    }
  ]
}
```

## Querying Series Stats with SQL

You can also query the series stats file directly:

```bash
# Register the stats file
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT service, metric, COUNT(*) as series_count 
      FROM '_system/series_stats/mydb' 
      GROUP BY service, metric 
      ORDER BY series_count DESC"
```

## Limitations

1. **Append-Only** - Stats file grows over time (no automatic cleanup)
2. **Manual Compaction** - Use Parquet compaction tools to deduplicate
3. **Cache Memory** - In-memory cache grows with unique series count
4. **Async Delays** - Stats may lag slightly behind writes (async tracking)

## Future Enhancements

- [ ] Automatic stats compaction/deduplication
- [ ] Time-based series tracking (new series per day)
- [ ] Cardinality limits and throttling
- [ ] Dashboard/UI for series monitoring
- [ ] Export to monitoring systems (Prometheus, Grafana)

## Summary

Series cardinality tracking provides:
- ✅ **Automatic** - No manual tracking needed
- ✅ **Service-grouped** - Clear visibility per service
- ✅ **Metric breakdown** - See which metrics are high-cardinality
- ✅ **Efficient** - Async, cached, deduplicated
- ✅ **RESTful API** - Easy integration with monitoring tools

Use `/series_stats/:db` to monitor your metric cardinality and prevent metric explosion! 📊
