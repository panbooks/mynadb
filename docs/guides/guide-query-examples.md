# DataFusion Query Examples

> **Note:** For a quick reference, see [guide-correct-query-examples.md](guide-correct-query-examples.md).
> For short table name examples, see [guide-query-examples-updated.md](guide-query-examples-updated.md).

## Overview

MynaDB now supports SQL queries powered by Apache DataFusion! You can query your time series data using standard SQL.

## Query Endpoints

### GET /query/:db?sql=<query>
Query with SQL in URL parameter (for simple queries)

### POST /query/:db
Query with SQL in request body (for complex queries)

### GET /tables/:db
List all available tables in a database

### GET /schema/:db/:table
Get the schema for a specific table

## Table Naming Convention

MynaDB uses a **catalog.schema.table** structure. The naming depends on the configured schema strategy:

### ServiceAsSchema (Default)
- Catalog = Database name from URL `/write/{db}`
- Schema = Service tag value (or "default")
- Table = Measurement name

Example:
- Line: `cpu,service=web,host=server01 usage=75.5`
- Path: `mydb.web.cpu`
- Query: `SELECT * FROM mydb.web.cpu` or just `SELECT * FROM cpu`

### ServiceAsTable Mode
- Catalog = Database name
- Schema = "default" (fixed)
- Table = Service tag value
- Measurement stored as a tag column

Example:
- Line: `cpu,service=web,host=server01 usage=75.5`
- Path: `mydb.default.web`
- Query: `SELECT * FROM mydb.default.web WHERE measurement = 'cpu'`

### Short Table Names

You can use short table names in queries - the system auto-prefixes them:
- `SELECT * FROM cpu` → `SELECT * FROM mydb.default.cpu` (auto-prefixed)

## Basic Query Examples

> **Note:** These examples use short table names (recommended). Fully qualified names like `mydb.default.cpu` also work.

### 1. Select All Data

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'
```

### 2. Filter by Tag

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE host = '"'"'server01'"'"''
```

### 3. Aggregate Queries

```bash
# Average CPU usage
curl 'http://localhost:7000/query/mydb?sql=SELECT AVG(usage) as avg_usage FROM cpu'

# Max and Min
curl 'http://localhost:7000/query/mydb?sql=SELECT MAX(usage) as max_usage, MIN(usage) as min_usage FROM cpu'

# Count
curl 'http://localhost:7000/query/mydb?sql=SELECT COUNT(*) as total_points FROM cpu'
```

### 4. Group By

```bash
# Average by host
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host'

# Count by region
curl 'http://localhost:7000/query/mydb?sql=SELECT region, COUNT(*) as count FROM cpu GROUP BY region'
```

### 5. Order By

```bash
# Top 10 highest CPU usage
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY usage DESC LIMIT 10'

# Latest records
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY time DESC LIMIT 5'
```

### 6. Time-based Filtering

```bash
# Records after a specific timestamp
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE time > 1703251234567890123'

# Between timestamps
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE time BETWEEN 1703251234567890123 AND 1703251300000000000'
```

## Complex Query Examples

### 1. Multi-condition Filtering

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE host = '"'"'server01'"'"' AND usage > 50.0 ORDER BY time DESC'
```

### 2. Statistical Analysis

```bash
# Multiple statistics
curl 'http://localhost:7000/query/mydb?sql=SELECT host, COUNT(*) as count, AVG(usage) as avg, MAX(usage) as max, MIN(usage) as min FROM cpu GROUP BY host'
```

### 3. JOIN Queries

```bash
# Join CPU and Memory data
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used
      FROM cpu c
      JOIN memory m ON c.host = m.host
      WHERE c.time = m.time"
```

### 4. Subqueries

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu WHERE usage > (SELECT AVG(usage) FROM cpu)"
```

### 5. Window Functions

```bash
# Running average
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT time, usage,
      AVG(usage) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg
      FROM cpu"
```

## Using POST for Complex Queries

For complex queries with special characters, use POST:

```bash
curl -X POST http://localhost:7000/query/mydb \
  -H "Content-Type: text/plain" \
  -d "SELECT
        host,
        AVG(usage) as avg_usage,
        MAX(usage) as max_usage,
        MIN(usage) as min_usage,
        COUNT(*) as count
      FROM cpu
      WHERE usage > 50.0
      GROUP BY host
      ORDER BY avg_usage DESC"
```

## Utility Endpoints

### List All Tables

```bash
curl http://localhost:7000/tables/mydb
```

Response:
```json
{
  "catalog": "mydb",
  "tables": [
    "mydb.default.cpu",
    "mydb.default.memory",
    "mydb.default.disk"
  ],
  "count": 3
}
```

### Get Table Schema

```bash
curl http://localhost:7000/schema/mydb/cpu
```

Response shows the schema with all columns and their data types.

## Complete Workflow Example

### 1. Write Data

```bash
curl -X POST http://localhost:7000/write/metrics -d '
cpu,host=server01,region=us-west usage=75.5,temp=45i
cpu,host=server02,region=us-west usage=45.2,temp=38i
cpu,host=server03,region=us-east usage=89.1,temp=52i
memory,host=server01 used=16.5,total=32.0
memory,host=server02 used=24.8,total=64.0
'
```

### 2. Flush to Disk

```bash
curl -X POST http://localhost:7000/flush
```

### 3. List Tables

```bash
curl http://localhost:7000/tables/metrics
```

### 4. Query the Data

```bash
# All CPU data
curl 'http://localhost:7000/query/metrics?sql=SELECT * FROM cpu'

# Average usage by region
curl 'http://localhost:7000/query/metrics?sql=SELECT region, AVG(usage) as avg_usage FROM cpu GROUP BY region'

# Servers with high usage
curl 'http://localhost:7000/query/metrics?sql=SELECT host, usage FROM cpu WHERE usage > 70 ORDER BY usage DESC'

# Join CPU and memory
curl -X POST http://localhost:7000/query/metrics \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used
      FROM cpu c
      JOIN memory m ON c.host = m.host"
```

## Advanced Features

### 1. Percentiles

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT
        approx_percentile_cont(usage, 0.5) as p50,
        approx_percentile_cont(usage, 0.95) as p95,
        approx_percentile_cont(usage, 0.99) as p99
      FROM cpu"
```

### 2. Time Windows

```bash
# Group by time buckets (requires timestamp conversion)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT
        (timestamp / 60000000000) as minute_bucket,
        AVG(usage) as avg_usage
      FROM cpu
      GROUP BY (timestamp / 60000000000)
      ORDER BY minute_bucket"
```

### 3. CASE Statements

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT
        host,
        usage,
        CASE
          WHEN usage < 30 THEN 'low'
          WHEN usage < 70 THEN 'medium'
          ELSE 'high'
        END as usage_level
      FROM cpu"
```

## Tips and Best Practices

1. **Always flush before querying** if you have recent writes in cache
2. **Use LIMIT** for large datasets to avoid timeouts
3. **Use POST** for queries with quotes or special characters
4. **URL-encode** parameters when using GET
5. **Index-friendly queries**: Filter on timestamp and tags for better performance
6. **Aggregate when possible** to reduce result size

## Query Performance

DataFusion provides excellent query performance by:
- Reading Parquet files directly (columnar format)
- Predicate pushdown (filtering at read time)
- Parallel query execution
- Memory-efficient processing

## Error Handling

Common errors:
- `Table not found`: Database or measurement doesn't exist
- `Column not found`: Check table schema
- `SQL error`: Invalid SQL syntax
- `No tables found`: Database is empty or hasn't been flushed

## Next Steps

Try building dashboards or analytics tools on top of the SQL API!