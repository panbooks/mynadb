# SQL Table Name Prefixing

## Overview

The query engine automatically prefixes unqualified table names with the database name. This allows you to write simpler, more natural SQL queries without repeating the database name.

## Feature

When you query a database via `/query/:db`, you can use **short table names** and they are automatically prefixed with the database name.

### Simple Queries

```sql
-- You write
SELECT * FROM cpu WHERE usage > 50

-- Automatically prefixed to
SELECT * FROM mydb_cpu WHERE usage > 50
```

### JOIN Queries

```sql
-- You write
SELECT * FROM memory JOIN disk ON memory.host = disk.host

-- Automatically prefixed to
SELECT * FROM mydb_memory JOIN mydb_disk ON mydb_memory.host = mydb_disk.host
```

## How It Works

The query engine uses a simple prefixing system:

1. **Table Registration**: Tables are registered with database prefix (e.g., `mydb_cpu`)
2. **Automatic Prefixing**: Unqualified table names get the database prefix added
3. **Smart Detection**: Already-prefixed names are left unchanged

```
Original:  SELECT * FROM cpu
Prefixed:  SELECT * FROM mydb_cpu

Original:  SELECT * FROM memory JOIN disk ON memory.host = disk.host  
Prefixed:  SELECT * FROM mydb_memory JOIN mydb_disk ON mydb_memory.host = mydb_disk.host
```

## Supported Formats

You can use either format:

```bash
# Short format (recommended)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu'

# Full prefixed format
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu'
```

## Examples

### Basic Queries

```bash
# Get all CPU data
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# Filter by condition
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu WHERE usage > 80'

# Aggregation
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) FROM cpu GROUP BY host'
```

### JOIN Queries

```bash
# Join two measurements
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.host, c.usage, m.used 
      FROM cpu c 
      JOIN memory m ON c.host = m.host"

# Multiple joins
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.*, m.used, d.free
      FROM cpu c
      LEFT JOIN memory m ON c.host = m.host
      LEFT JOIN disk d ON c.host = d.host"
```

### Complex Queries

```bash
# Subqueries (table names in subqueries are also rewritten)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM (
        SELECT host, AVG(usage) as avg_usage 
        FROM cpu 
        GROUP BY host
      ) WHERE avg_usage > 50"

# Window functions
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, usage, 
             AVG(usage) OVER (PARTITION BY host) as avg_by_host
      FROM cpu"
```

## Supported Formats

You can use either format:

```bash
# Short format (recommended)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu'

# Fully qualified format
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb.cpu'
```

## Special Cases

### 1. Already Qualified Tables

Table names with dots are not modified:

```sql
-- Original
SELECT * FROM mydb.cpu

-- Result (unchanged)
SELECT * FROM mydb.cpu
```

### 2. Table Names with Underscores

This is why **dot notation** is important! Table or database names can contain underscores:

```sql
-- Database: my_metrics, Table: cpu_stats
-- You write:
SELECT * FROM cpu_stats

-- Qualified to:
SELECT * FROM my_metrics.cpu_stats

-- NOT: my_metrics_cpu_stats (ambiguous!)
```

### 3. Subqueries

Subqueries work naturally:

```sql
-- Original
SELECT * FROM (SELECT * FROM cpu) AS t

-- Qualified
SELECT * FROM (SELECT * FROM mydb.cpu) AS t
```

## Server Logs

When SQL prefixing occurs, you'll see it in the server logs:

```
📝 SQL: SELECT * FROM cpu
📝 Prefixed: SELECT * FROM mydb_cpu
✅ Query executed successfully
```

If no prefixing is needed (table names already prefixed), only the original SQL is logged.

## Benefits

### 1. Simpler Queries

Write cleaner, more readable SQL:

```sql
-- Short format (recommended)
SELECT * FROM cpu JOIN memory ON cpu.host = memory.host

-- Full prefixed format (also works)
SELECT * FROM mydb_cpu JOIN mydb_memory ON mydb_cpu.host = mydb_memory.host
```

### 2. Database-Agnostic Queries

The same SQL works across different databases by changing only the URL:

```bash
# Production database
curl 'http://localhost:7000/query/prod?sql=SELECT * FROM cpu'

# Testing database  
curl 'http://localhost:7000/query/test?sql=SELECT * FROM cpu'
```

### 3. Easier Migration

Queries look more similar to standard SQL databases, making migration easier.

### 4. Better Developer Experience

Less typing, less chance of typos, more natural SQL.

## Implementation

This feature uses a simple but effective approach:

- Tables registered with database prefix (e.g., `mydb_cpu`) in DataFusion's catalog
- Simple pattern matching to detect and prefix unqualified table names
- Skips table names that already have the database prefix
- Leverages DataFusion's query planning and optimization

## Testing

Try these examples to verify the feature works:

```bash
# 1. Start server
cargo run

# 2. Create database and write data
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"testdb"}'

curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server01 usage=75.5,temp=45i'

curl -X POST http://localhost:7000/write/testdb \
  -d 'memory,host=server01 used=16.5,total=32.0'

curl -X POST http://localhost:7000/flush

# 3. Test short table names (NEW)
curl 'http://localhost:7000/query/testdb?sql=SELECT * FROM cpu'
curl 'http://localhost:7000/query/testdb?sql=SELECT * FROM memory'

# 4. Test JOIN (NEW)
curl -X POST http://localhost:7000/query/testdb \
  -d "SELECT c.*, m.used FROM cpu c JOIN memory m ON c.host = m.host"

# 5. Test prefixed names (also works)
curl 'http://localhost:7000/query/testdb?sql=SELECT * FROM testdb_cpu'
```

All queries should work correctly!

## Summary

The SQL table name prefixing feature makes querying your time-series data more natural and convenient:

- ✅ Use short table names: `FROM cpu` instead of `FROM mydb_cpu`
- ✅ Automatic prefixing based on database in URL
- ✅ Works with JOINs, subqueries, and complex SQL
- ✅ Smart detection of already-prefixed names
- ✅ Better developer experience

Enjoy simpler SQL queries! 🎉
