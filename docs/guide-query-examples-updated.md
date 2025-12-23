# SQL Query Examples (Updated with Short Table Names)

This document shows SQL query examples using the new **short table name** feature.

## Basic Queries

### Select All Data

```bash
# Short form (NEW - recommended)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# Full form (still works)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 10'
```

### Filter by Condition

```bash
# WHERE clause
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE usage > 80'

# Multiple conditions
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE usage > 50 AND host = '"'"'server01'"'"''
```

### Select Specific Columns

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT host, usage, timestamp FROM cpu'
```

## Aggregations

### COUNT

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT COUNT(*) as total FROM cpu'
```

### AVG, MIN, MAX

```bash
# Average
curl 'http://localhost:7000/query/mydb?sql=SELECT AVG(usage) as avg_usage FROM cpu'

# Min and Max
curl 'http://localhost:7000/query/mydb?sql=SELECT MIN(usage) as min, MAX(usage) as max FROM cpu'
```

### GROUP BY

```bash
# Group by single column
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host'

# Group by multiple columns
curl 'http://localhost:7000/query/mydb?sql=SELECT host, region, AVG(usage) FROM cpu GROUP BY host, region'
```

### HAVING

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) as avg FROM cpu GROUP BY host HAVING avg > 50'
```

## Sorting

### ORDER BY

```bash
# Ascending
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY usage ASC LIMIT 10'

# Descending
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY usage DESC LIMIT 10'

# Multiple columns
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu ORDER BY host ASC, usage DESC'
```

## JOINs (New Short Syntax)

### INNER JOIN

```bash
# Join CPU and Memory data
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

## Time-Based Queries

### Recent Data

```bash
# Using timestamp comparison
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu 
      WHERE timestamp > 1703251234567000000 
      ORDER BY timestamp DESC"
```

### Time Range

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu 
      WHERE timestamp BETWEEN 1703251234567000000 AND 1703251240000000000"
```

## Advanced Queries

### Subqueries

```bash
# Find hosts with above-average usage
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu 
      WHERE usage > (SELECT AVG(usage) FROM cpu)"
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
             AVG(usage) OVER (PARTITION BY host ORDER BY timestamp) as moving_avg
      FROM cpu"

# Rank by usage
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, usage,
             RANK() OVER (ORDER BY usage DESC) as rank
      FROM cpu"
```

## Practical Examples

### 1. Top 10 Hosts by CPU Usage

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, AVG(usage) as avg_usage 
      FROM cpu 
      GROUP BY host 
      ORDER BY avg_usage DESC 
      LIMIT 10"
```

### 2. Hosts with High CPU and Memory Usage

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used
      FROM cpu c
      JOIN memory m ON c.host = m.host
      WHERE c.usage > 80 AND m.used > 80"
```

### 3. Count Records per Host

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT host, COUNT(*) as count FROM cpu GROUP BY host'
```

### 4. Latest Reading per Host

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT DISTINCT ON (host) host, usage, timestamp
      FROM cpu
      ORDER BY host, timestamp DESC"
```

### 5. Hourly Averages

```bash
# Note: Requires date/time functions support in DataFusion
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        AVG(usage) as avg_usage
      FROM cpu
      GROUP BY hour
      ORDER BY hour"
```

### 6. Compare Metrics Across Regions

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT region, 
             AVG(usage) as avg_usage,
             MIN(usage) as min_usage,
             MAX(usage) as max_usage
      FROM cpu
      GROUP BY region"
```

### 7. Find Anomalies (Usage > 2x Average)

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu
      WHERE usage > 2 * (SELECT AVG(usage) FROM cpu)"
```

### 8. Resource Utilization Summary

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
      WHERE c.timestamp > 1703251234567000000
      ORDER BY c.usage DESC"
```

## CASE Statements

### Categorize by Threshold

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, usage,
        CASE 
          WHEN usage < 50 THEN 'Low'
          WHEN usage < 80 THEN 'Medium'
          ELSE 'High'
        END as category
      FROM cpu"
```

## DISTINCT

```bash
# Get unique hosts
curl 'http://localhost:7000/query/mydb?sql=SELECT DISTINCT host FROM cpu'

# Count unique hosts
curl 'http://localhost:7000/query/mydb?sql=SELECT COUNT(DISTINCT host) as unique_hosts FROM cpu'
```

## UNION

```bash
# Combine results from different measurements
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, 'cpu' as metric_type FROM cpu
      UNION ALL
      SELECT host, 'memory' as metric_type FROM memory"
```

## Tips

### 1. Use POST for Long Queries

For complex queries, use POST instead of GET:

```bash
curl -X POST http://localhost:7000/query/mydb -d "YOUR_LONG_SQL_HERE"
```

### 2. URL Encoding for GET Requests

If using GET with special characters, URL-encode the SQL:

```bash
# Spaces become %20, etc.
curl 'http://localhost:7000/query/mydb?sql=SELECT%20*%20FROM%20cpu'
```

### 3. Check Server Logs

The server logs show the rewritten SQL:

```
📝 Original SQL: SELECT * FROM cpu
📝 Rewritten SQL: SELECT * FROM mydb_cpu
```

### 4. Mixed Mode Works

You can mix short and full table names:

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu JOIN mydb_memory ON cpu.host = mydb_memory.host"
```

### 5. Use Aliases for Readability

```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage, m.used
      FROM cpu AS c
      JOIN memory AS m ON c.host = m.host"
```

## Summary

The new short table name feature makes queries:
- ✅ Simpler and more readable
- ✅ Easier to write (less typing)
- ✅ More portable across databases
- ✅ Backward compatible with full names

Enjoy writing cleaner SQL! 🎉
