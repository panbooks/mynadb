# DataFusion Query Examples

## Overview

The Stats TSDB now supports SQL queries powered by Apache DataFusion! You can query your time series data using standard SQL.

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

Tables are automatically named as: `{database}_{measurement}`

Example:
- Database: `mydb`
- Measurement: `cpu`
- Table name: `mydb_cpu`

## Basic Query Examples

### 1. Select All Data

```bash
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 10'
```

### 2. Filter by Tag

```bash
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu WHERE host = '"'"'server01'"'"''
```

### 3. Aggregate Queries

```bash
# Average CPU usage
curl 'http://localhost:3000/query/mydb?sql=SELECT AVG(usage) as avg_usage FROM mydb_cpu'

# Max and Min
curl 'http://localhost:3000/query/mydb?sql=SELECT MAX(usage) as max_usage, MIN(usage) as min_usage FROM mydb_cpu'

# Count
curl 'http://localhost:3000/query/mydb?sql=SELECT COUNT(*) as total_points FROM mydb_cpu'
```

### 4. Group By

```bash
# Average by host
curl 'http://localhost:3000/query/mydb?sql=SELECT host, AVG(usage) as avg_usage FROM mydb_cpu GROUP BY host'

# Count by region
curl 'http://localhost:3000/query/mydb?sql=SELECT region, COUNT(*) as count FROM mydb_cpu GROUP BY region'
```

### 5. Order By

```bash
# Top 10 highest CPU usage
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu ORDER BY usage DESC LIMIT 10'

# Latest records
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu ORDER BY timestamp DESC LIMIT 5'
```

### 6. Time-based Filtering

```bash
# Records after a specific timestamp
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu WHERE timestamp > 1703251234567890123'

# Between timestamps
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu WHERE timestamp BETWEEN 1703251234567890123 AND 1703251300000000000'
```

## Complex Query Examples

### 1. Multi-condition Filtering

```bash
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu WHERE host = '"'"'server01'"'"' AND usage > 50.0 ORDER BY timestamp DESC'
```

### 2. Statistical Analysis

```bash
# Multiple statistics
curl 'http://localhost:3000/query/mydb?sql=SELECT host, COUNT(*) as count, AVG(usage) as avg, MAX(usage) as max, MIN(usage) as min FROM mydb_cpu GROUP BY host'
```

### 3. JOIN Queries

```bash
# Join CPU and Memory data
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used 
      FROM mydb_cpu c 
      JOIN mydb_memory m ON c.host = m.host 
      WHERE c.timestamp = m.timestamp"
```

### 4. Subqueries

```bash
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT * FROM mydb_cpu WHERE usage > (SELECT AVG(usage) FROM mydb_cpu)"
```

### 5. Window Functions

```bash
# Running average
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT timestamp, usage, 
      AVG(usage) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg 
      FROM mydb_cpu"
```

## Using POST for Complex Queries

For complex queries with special characters, use POST:

```bash
curl -X POST http://localhost:3000/query/mydb \
  -H "Content-Type: text/plain" \
  -d "SELECT 
        host,
        AVG(usage) as avg_usage,
        MAX(usage) as max_usage,
        MIN(usage) as min_usage,
        COUNT(*) as count
      FROM mydb_cpu
      WHERE usage > 50.0
      GROUP BY host
      ORDER BY avg_usage DESC"
```

## Utility Endpoints

### List All Tables

```bash
curl http://localhost:3000/tables/mydb
```

Response:
```json
{
  "database": "mydb",
  "tables": [
    "mydb_cpu",
    "mydb_memory",
    "mydb_disk"
  ],
  "count": 3
}
```

### Get Table Schema

```bash
curl http://localhost:3000/schema/mydb/cpu
```

Response shows the schema with all columns and their data types.

## Complete Workflow Example

### 1. Write Data

```bash
curl -X POST http://localhost:3000/write/metrics -d '
cpu,host=server01,region=us-west usage=75.5,temp=45i
cpu,host=server02,region=us-west usage=45.2,temp=38i
cpu,host=server03,region=us-east usage=89.1,temp=52i
memory,host=server01 used=16.5,total=32.0
memory,host=server02 used=24.8,total=64.0
'
```

### 2. Flush to Disk

```bash
curl -X POST http://localhost:3000/flush
```

### 3. List Tables

```bash
curl http://localhost:3000/tables/metrics
```

### 4. Query the Data

```bash
# All CPU data
curl 'http://localhost:3000/query/metrics?sql=SELECT * FROM metrics_cpu'

# Average usage by region
curl 'http://localhost:3000/query/metrics?sql=SELECT region, AVG(usage) as avg_usage FROM metrics_cpu GROUP BY region'

# Servers with high usage
curl 'http://localhost:3000/query/metrics?sql=SELECT host, usage FROM metrics_cpu WHERE usage > 70 ORDER BY usage DESC'

# Join CPU and memory
curl -X POST http://localhost:3000/query/metrics \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used 
      FROM metrics_cpu c 
      JOIN metrics_memory m ON c.host = m.host"
```

## Advanced Features

### 1. Percentiles

```bash
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT 
        approx_percentile_cont(usage, 0.5) as p50,
        approx_percentile_cont(usage, 0.95) as p95,
        approx_percentile_cont(usage, 0.99) as p99
      FROM mydb_cpu"
```

### 2. Time Windows

```bash
# Group by time buckets (requires timestamp conversion)
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT 
        (timestamp / 60000000000) as minute_bucket,
        AVG(usage) as avg_usage
      FROM mydb_cpu
      GROUP BY (timestamp / 60000000000)
      ORDER BY minute_bucket"
```

### 3. CASE Statements

```bash
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT 
        host,
        usage,
        CASE 
          WHEN usage < 30 THEN 'low'
          WHEN usage < 70 THEN 'medium'
          ELSE 'high'
        END as usage_level
      FROM mydb_cpu"
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