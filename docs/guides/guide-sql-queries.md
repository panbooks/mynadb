# SQL Query Guide

This guide covers everything you need to know about querying MynaDB using SQL powered by Apache DataFusion.

## Table of Contents

- [Quick Start](#quick-start)
- [API Endpoints](#api-endpoints)
- [Table Naming](#table-naming)
- [Basic Queries](#basic-queries)
- [Aggregations](#aggregations)
- [JOINs](#joins)
- [Advanced Queries](#advanced-queries)
- [Practical Examples](#practical-examples)
- [Complete Workflow](#complete-workflow)
- [Tips and Best Practices](#tips-and-best-practices)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

Query your time series data using simple SQL:

```bash
# Short table name (recommended)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu LIMIT 10'

# Fully qualified name (also works)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.cpu LIMIT 10'

# GET request with sql parameter
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/query/:db?sql=<query>` | GET | Query with SQL in URL (simple queries) |
| `/query/:db` | POST | Query with SQL in body (complex queries) |
| `/tables/:db` | GET | List all tables in a database |
| `/schema/:db/:table` | GET | Get schema for a specific table |

### GET vs POST

```bash
# GET - for simple queries (easy browser testing)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# POST - for complex queries (multi-line, special characters)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, AVG(usage) as avg_usage
      FROM cpu
      GROUP BY host
      ORDER BY avg_usage DESC"
```

---

## Table Naming

MynaDB uses a **catalog.schema.table** structure. Tables are automatically registered when you query.

### Supported Formats

| Format | Example | When to Use |
|--------|---------|-------------|
| Short name | `SELECT * FROM cpu` | **Recommended** - auto-prefixed |
| Schema.table | `SELECT * FROM default.cpu` | Explicit schema |
| Fully qualified | `SELECT * FROM mydb.default.cpu` | Cross-database queries |

### How Auto-Prefixing Works

```
You write:    SELECT * FROM cpu
System sees:  SELECT * FROM mydb.default.cpu
```

### Schema Strategies

The table structure depends on your `config.toml` settings:

**ServiceAsSchema (Default)**
- Schema = Service tag value (e.g., `service=web` creates schema `web`)
- Table = Measurement name
- Example: `cpu,service=web` → `mydb.web.cpu`

**ServiceAsTable**
- Schema = "default" (fixed)
- Table = Service tag value
- Measurement stored as a column
- Example: `cpu,service=web` → `mydb.default.web` with `measurement='cpu'`

### Utility Commands

```bash
# List all tables
curl http://localhost:7000/tables/mydb

# Get table schema
curl http://localhost:7000/schema/mydb/cpu
```

---

## Basic Queries

### Select Data

```bash
# All columns
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# Specific columns
curl 'http://localhost:7000/query/mydb?sql=SELECT host, usage, time FROM cpu'
```

### Filtering (WHERE)

```bash
# Single condition
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE usage > 80'

# Multiple conditions
curl 'http://localhost:7000/query/mydb' -d "SELECT * FROM cpu WHERE usage > 50 AND host = 'server01'"

# Time range
curl 'http://localhost:7000/query/mydb' -d "SELECT * FROM cpu
    WHERE time BETWEEN 1703251234567000000 AND 1703251240000000000"
```

### Sorting (ORDER BY)

```bash
# Ascending
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY usage ASC LIMIT 10'

# Descending
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY usage DESC LIMIT 10'

# Multiple columns
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY host ASC, usage DESC'

# Latest records
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY time DESC LIMIT 5'
```

### DISTINCT

```bash
# Unique hosts
curl 'http://localhost:7000/query/mydb?sql=SELECT DISTINCT host FROM cpu'

# Count unique hosts
curl 'http://localhost:7000/query/mydb?sql=SELECT COUNT(DISTINCT host) as unique_hosts FROM cpu'
```

---

## Aggregations

### Basic Aggregates

```bash
# COUNT
curl 'http://localhost:7000/query/mydb?sql=SELECT COUNT(*) as total FROM cpu'

# AVG
curl 'http://localhost:7000/query/mydb?sql=SELECT AVG(usage) as avg_usage FROM cpu'

# MIN / MAX
curl 'http://localhost:7000/query/mydb?sql=SELECT MIN(usage) as min, MAX(usage) as max FROM cpu'

# Multiple aggregates
curl 'http://localhost:7000/query/mydb' -d "SELECT
    COUNT(*) as count,
    AVG(usage) as avg,
    MIN(usage) as min,
    MAX(usage) as max
FROM cpu"
```

### GROUP BY

```bash
# Single column
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host'

# Multiple columns
curl 'http://localhost:7000/query/mydb?sql=SELECT host, region, AVG(usage) FROM cpu GROUP BY host, region'

# With statistics
curl 'http://localhost:7000/query/mydb' -d "SELECT
    host,
    COUNT(*) as count,
    AVG(usage) as avg,
    MAX(usage) as max,
    MIN(usage) as min
FROM cpu
GROUP BY host"
```

### HAVING

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) as avg FROM cpu GROUP BY host HAVING avg > 50'
```

### Percentiles

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT
        approx_percentile_cont(usage, 0.5) as p50,
        approx_percentile_cont(usage, 0.95) as p95,
        approx_percentile_cont(usage, 0.99) as p99
      FROM cpu"
```

---

## JOINs

### INNER JOIN

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage, m.used
      FROM cpu c
      JOIN memory m ON c.host = m.host"
```

### LEFT JOIN

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.*, m.used
      FROM cpu c
      LEFT JOIN memory m ON c.host = m.host"
```

### Multiple JOINs

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage, m.used, d.free
      FROM cpu c
      LEFT JOIN memory m ON c.host = m.host
      LEFT JOIN disk d ON c.host = d.host"
```

### JOIN with Conditions

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used
      FROM cpu c
      JOIN memory m ON c.host = m.host
      WHERE c.usage > 80 AND m.used > 80"
```

---

## Advanced Queries

### Subqueries

```bash
# Find above-average usage
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu WHERE usage > (SELECT AVG(usage) FROM cpu)"

# Find anomalies (usage > 2x average)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu WHERE usage > 2 * (SELECT AVG(usage) FROM cpu)"

# Derived tables
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM (
        SELECT host, AVG(usage) as avg_usage
        FROM cpu
        GROUP BY host
      ) WHERE avg_usage > 50"
```

### Common Table Expressions (CTE)

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "WITH avg_by_host AS (
        SELECT host, AVG(usage) as avg_usage
        FROM cpu
        GROUP BY host
      )
      SELECT * FROM avg_by_host WHERE avg_usage > 50"
```

### Window Functions

```bash
# Moving average
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, usage,
        AVG(usage) OVER (PARTITION BY host ORDER BY time) as moving_avg
      FROM cpu"

# Rank by usage
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, usage,
        RANK() OVER (ORDER BY usage DESC) as rank
      FROM cpu"

# Running average (last 3 rows)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT time, usage,
        AVG(usage) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg
      FROM cpu"
```

### CASE Statements

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, usage,
        CASE
          WHEN usage < 30 THEN 'low'
          WHEN usage < 70 THEN 'medium'
          ELSE 'high'
        END as usage_level
      FROM cpu"
```

### UNION

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, 'cpu' as metric_type FROM cpu
      UNION ALL
      SELECT host, 'memory' as metric_type FROM memory"
```

### Time Bucketing

```bash
# Group by minute buckets
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT
        (timestamp / 60000000000) as minute_bucket,
        AVG(usage) as avg_usage
      FROM cpu
      GROUP BY (timestamp / 60000000000)
      ORDER BY minute_bucket"
```

---

## Practical Examples

### Top 10 Hosts by CPU Usage

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, AVG(usage) as avg_usage
      FROM cpu
      GROUP BY host
      ORDER BY avg_usage DESC
      LIMIT 10"
```

### Resource Utilization Summary

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT
        c.host,
        c.usage as cpu_usage,
        m.used as memory_used,
        d.free as disk_free
      FROM cpu c
      LEFT JOIN memory m ON c.host = m.host
      LEFT JOIN disk d ON c.host = d.host
      ORDER BY c.usage DESC"
```

### Compare Metrics Across Regions

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT region,
        AVG(usage) as avg_usage,
        MIN(usage) as min_usage,
        MAX(usage) as max_usage
      FROM cpu
      GROUP BY region"
```

### Records per Host

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT host, COUNT(*) as count FROM cpu GROUP BY host'
```

### High CPU and Memory Usage Hosts

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used
      FROM cpu c
      JOIN memory m ON c.host = m.host
      WHERE c.usage > 80 AND m.used > 80"
```

---

## Complete Workflow

### 1. Write Data

```bash
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01,region=us-west usage=75.5,temp=45i
cpu,host=server02,region=us-east usage=82.3,temp=50i
cpu,host=server03,region=us-west usage=45.2,temp=38i
memory,host=server01,region=us-west used=16.5,total=32.0
memory,host=server02,region=us-east used=20.1,total=32.0'
```

### 2. Flush to Disk

```bash
curl -X POST http://localhost:7000/flush
```

### 3. List Tables

```bash
curl http://localhost:7000/tables/mydb
```

### 4. Query Data

```bash
# All CPU data
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu'

# Average by region
curl 'http://localhost:7000/query/mydb?sql=SELECT region, AVG(usage) as avg_usage FROM cpu GROUP BY region'

# High usage servers
curl 'http://localhost:7000/query/mydb?sql=SELECT host, usage FROM cpu WHERE usage > 70 ORDER BY usage DESC'

# Join CPU and memory
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used
      FROM cpu c
      JOIN memory m ON c.host = m.host"
```

---

## Tips and Best Practices

### Query Optimization

1. **Use LIMIT** for large datasets to avoid timeouts
2. **Filter early** - use WHERE clauses on timestamp and tags
3. **Aggregate when possible** to reduce result size
4. **Use short table names** - cleaner and auto-prefixed

### Request Format

1. **Use POST for complex queries** - easier multi-line formatting
2. **Use GET for simple queries** - quick browser testing
3. **URL-encode GET parameters** with special characters

### Performance Notes

DataFusion provides excellent query performance by:
- Reading Parquet files directly (columnar format)
- Predicate pushdown (filtering at read time)
- Parallel query execution
- Memory-efficient processing

### Debugging

Check server logs to see SQL transformation:
```
📝 SQL: SELECT * FROM cpu
📝 Prefixed: SELECT * FROM mydb.default.cpu
✅ Query executed successfully
```

---

## Troubleshooting

### "Table not found"

```bash
# Verify tables exist
curl http://localhost:7000/tables/mydb

# Flush data to disk first
curl -X POST http://localhost:7000/flush
```

### "No data returned"

```bash
# Check record count
curl 'http://localhost:7000/query/mydb' -d 'SELECT COUNT(*) FROM cpu'

# Check table schema
curl http://localhost:7000/schema/mydb/cpu
```

### "Column not found"

```bash
# Verify column names in schema
curl http://localhost:7000/schema/mydb/cpu
```

### Common Mistakes

| Wrong | Correct |
|-------|---------|
| `SELECT * FROM mydb.cpu` | `SELECT * FROM cpu` or `SELECT * FROM mydb.default.cpu` |
| `curl /query -d 'SELECT...'` | `curl /query/mydb -d 'SELECT...'` |
| Not flushing before query | `curl -X POST /flush` first |

---

## Summary

**Query Format:**
```bash
curl 'http://localhost:7000/query/<database>' -d '<SQL>'
```

**Valid Examples:**
```bash
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu'
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.cpu'
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu'
```

**Key Points:**
- Use short table names (recommended) - auto-prefixed by the system
- Use POST for complex multi-line queries
- Always flush before querying recent writes
- Check `/tables/:db` to see available tables
