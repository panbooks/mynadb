# Correct Query Examples

## Quick Answer

After the fix, you can now query using **short table names** (without database prefix):

```bash
# ✅ CORRECT - Simple short name (RECOMMENDED)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu LIMIT 10'

# ✅ CORRECT - Full name also works
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb_cpu LIMIT 10'

# ✅ CORRECT - GET request
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'
```

## Complete Examples

### 1. Basic Queries

```bash
# Get all CPU data
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu LIMIT 10'

# Filter by condition
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu WHERE usage > 80'

# Aggregation
curl 'http://localhost:7000/query/mydb' -d 'SELECT host, AVG(usage) FROM cpu GROUP BY host'

# Order by timestamp
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu ORDER BY time DESC LIMIT 10'
```

### 2. JOIN Queries

```bash
# Join two measurements
curl 'http://localhost:7000/query/mydb' -d "SELECT c.host, c.usage, m.used 
FROM cpu c 
JOIN memory m ON c.host = m.host"

# Multiple joins
curl 'http://localhost:7000/query/mydb' -d "SELECT c.*, m.used, d.free
FROM cpu c
LEFT JOIN memory m ON c.host = m.host
LEFT JOIN disk d ON c.host = d.host"
```

### 3. Complex Queries

```bash
# Subquery
curl 'http://localhost:7000/query/mydb' -d "SELECT * FROM (
  SELECT host, AVG(usage) as avg_usage 
  FROM cpu 
  GROUP BY host
) WHERE avg_usage > 50"

# Window functions
curl 'http://localhost:7000/query/mydb' -d "SELECT host, usage, 
  AVG(usage) OVER (PARTITION BY host) as avg_by_host
FROM cpu"

# Common Table Expression (CTE)
curl 'http://localhost:7000/query/mydb' -d "WITH cpu_stats AS (
  SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host
)
SELECT * FROM cpu_stats WHERE avg_usage > 70"
```

### 4. GET vs POST

```bash
# GET request (for simple queries)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# POST request (for complex queries)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu WHERE usage > 80'

# POST with pretty formatting
curl 'http://localhost:7000/query/mydb' -d "
SELECT 
  host,
  AVG(usage) as avg_usage,
  MAX(usage) as max_usage,
  MIN(usage) as min_usage
FROM cpu
GROUP BY host
ORDER BY avg_usage DESC
"
```

## How It Works

When you query via `/query/mydb`, the system automatically:

1. **Auto-registers** the database tables (if not already registered)
2. **Prefixes** unqualified table names with the database name
3. **Executes** the query using DataFusion

### Example

```
You write:    SELECT * FROM cpu
System sees:  SELECT * FROM mydb_cpu
```

## Supported Table Name Formats

Both formats work:

| Format | Example | When to Use |
|--------|---------|------------|
| Short name | `SELECT * FROM cpu` | **Recommended** - cleaner |
| Full name | `SELECT * FROM mydb_cpu` | Explicit - also works |

## Common Mistakes

### ❌ WRONG

```bash
# Don't use dot notation (not supported yet)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.cpu'  # ❌

# Don't forget the database in the URL
curl 'http://localhost:7000/query' -d 'SELECT * FROM cpu'  # ❌
```

### ✅ CORRECT

```bash
# Use short name
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu'  # ✅

# Or use full underscore name
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb_cpu'  # ✅
```

## Verifying Tables

Before querying, you can verify what tables are available:

```bash
# List all tables in a database
curl 'http://localhost:7000/databases/mydb/tables'

# Get schema for a specific table
curl 'http://localhost:7000/schema/mydb/cpu'
```

## Full Workflow Example

```bash
# 1. Create database
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"mydb"}'

# 2. Write data
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01,region=us-west usage=75.5,temp=45i
cpu,host=server02,region=us-east usage=82.3,temp=50i
memory,host=server01,region=us-west used=16.5,total=32.0
memory,host=server02,region=us-east used=20.1,total=32.0'

# 3. Flush to disk
curl -X POST http://localhost:7000/flush

# 4. Query using short names
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu LIMIT 10'

curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM memory LIMIT 10'

# 5. Query with JOIN
curl 'http://localhost:7000/query/mydb' -d "
SELECT 
  c.host,
  c.usage as cpu_usage,
  m.used as memory_used
FROM cpu c
JOIN memory m ON c.host = m.host
WHERE c.usage > 80
"

# 6. Aggregation
curl 'http://localhost:7000/query/mydb' -d "
SELECT 
  host,
  AVG(usage) as avg_cpu
FROM cpu
GROUP BY host
ORDER BY avg_cpu DESC
"
```

## Tips

1. **Use POST for complex queries** - Easier to format multi-line SQL
2. **Use GET for simple queries** - Quick testing in browser
3. **Use short table names** - Cleaner, less typing
4. **Check server logs** - See how SQL is being prefixed
5. **Flush before querying** - Make sure data is written to Parquet files

## Troubleshooting

### "Table not found"

```bash
# Make sure database is registered
curl 'http://localhost:7000/databases/mydb/tables'

# Make sure you flushed data to disk
curl -X POST http://localhost:7000/flush
```

### "No data returned"

```bash
# Check if data exists
curl 'http://localhost:7000/query/mydb' -d 'SELECT COUNT(*) FROM cpu'

# Check schema
curl 'http://localhost:7000/schema/mydb/cpu'
```

### Server logs show the SQL transformation

When you run a query, check the server logs:

```
📝 SQL: SELECT * FROM cpu
📝 Prefixed: SELECT * FROM mydb_cpu
✅ Query executed successfully
```

This helps debug what's happening!

## Summary

**The correct query format is:**

```bash
curl 'http://localhost:7000/query/<database>' -d '<SQL with short or full table names>'
```

**Examples:**
- `curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu'` ✅
- `curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb_cpu'` ✅
- `curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu'` ✅

Happy querying! 🚀
