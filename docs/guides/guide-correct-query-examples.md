# Correct Query Examples

> **Primary Reference:** This is the recommended quick-start guide for SQL queries.
> For comprehensive examples, see [guide-query-examples.md](guide-query-examples.md).
> For short table name details, see [guide-query-examples-updated.md](guide-query-examples-updated.md).

## Quick Answer

After the fix, you can now query using **short table names** (without database prefix):

```bash
# ✅ CORRECT - Simple short name (RECOMMENDED)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu LIMIT 10'

# ✅ CORRECT - Fully qualified name also works
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.cpu LIMIT 10'

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

# Order by time
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

MynaDB uses a **catalog.schema.table** structure. When you query via `/query/mydb`:

1. **Auto-registers** the database tables (if not already registered)
2. **Prefixes** unqualified table names with the catalog and schema
3. **Executes** the query using DataFusion

### Schema Strategies

The naming behavior depends on the configured strategy (see `config.toml`):

**ServiceAsSchema (Default):**
- Schema = Service tag value (e.g., `service=web` → schema `web`)
- Table = Measurement name
- Query: `SELECT * FROM cpu` → `SELECT * FROM mydb.default.cpu`

**ServiceAsTable:**
- Schema = "default" (fixed)
- Table = Service tag value
- Measurement stored as a tag column
- Query: `SELECT * FROM service_1 WHERE measurement = 'cpu'`

### Example

```
You write:    SELECT * FROM cpu
System sees:  SELECT * FROM mydb.default.cpu
```

## Supported Table Name Formats

Multiple formats work:

| Format | Example | When to Use |
|--------|---------|------------|
| Short name | `SELECT * FROM cpu` | **Recommended** - cleaner |
| Schema.table | `SELECT * FROM default.cpu` | Explicit schema reference |
| Fully qualified | `SELECT * FROM mydb.default.cpu` | Cross-database queries |

## Common Mistakes

### ❌ WRONG

```bash
# Missing schema (catalog.table without schema)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.cpu'  # ❌

# Don't forget the database in the URL
curl 'http://localhost:7000/query' -d 'SELECT * FROM cpu'  # ❌
```

### ✅ CORRECT

```bash
# Use short name (recommended)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu'  # ✅

# Or use fully qualified name (catalog.schema.table)
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.cpu'  # ✅
```

## Verifying Tables

Before querying, you can verify what tables are available:

```bash
# List all tables in a database
curl 'http://localhost:7000/tables/mydb'

# Get schema for a specific table
curl 'http://localhost:7000/schema/mydb/cpu'
```

## Full Workflow Example

```bash
# 1. Write data (database is auto-created)
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01,region=us-west usage=75.5,temp=45i
cpu,host=server02,region=us-east usage=82.3,temp=50i
memory,host=server01,region=us-west used=16.5,total=32.0
memory,host=server02,region=us-east used=20.1,total=32.0'

# 2. Flush to disk
curl -X POST http://localhost:7000/flush

# 3. Query using short names
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu LIMIT 10'

curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM memory LIMIT 10'

# 4. Query with JOIN
curl 'http://localhost:7000/query/mydb' -d "
SELECT
  c.host,
  c.usage as cpu_usage,
  m.used as memory_used
FROM cpu c
JOIN memory m ON c.host = m.host
WHERE c.usage > 80
"

# 5. Aggregation
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
curl 'http://localhost:7000/tables/mydb'

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
📝 Prefixed: SELECT * FROM mydb.default.cpu
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
- `curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.cpu'` ✅
- `curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu'` ✅
