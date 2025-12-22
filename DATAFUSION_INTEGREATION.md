# DataFusion Integration Summary

## What is DataFusion?

Apache DataFusion is a fast, extensible query engine written in Rust. It provides:
- SQL query capabilities
- Parquet file reading
- Columnar execution
- Memory-efficient processing
- High performance analytics

## Why Add DataFusion?

### Before (Write-only Database)
- ✅ Write data in line protocol
- ✅ Store in Parquet files
- ❌ No way to read the data back
- ❌ Need external tools to query

### After (Full Database with SQL)
- ✅ Write data in line protocol
- ✅ Store in Parquet files
- ✅ **Query with SQL**
- ✅ **Built-in analytics**
- ✅ **No external tools needed**

## Architecture Changes

### New Component: Query Engine Module

```rust
// src/query_engine.rs
pub struct QueryEngine {
    ctx: SessionContext,      // DataFusion session
    data_dir: String,         // Where Parquet files are stored
}
```

### Key Methods

1. **`register_database(db_name)`**
    - Scans Parquet files in database directory
    - Registers each measurement as a table
    - Tables named as `{db}_{measurement}`

2. **`execute_query(sql)`**
    - Runs SQL query
    - Returns Arrow RecordBatches
    - Efficient columnar processing

3. **`execute_query_json(sql)`**
    - Executes query
    - Converts results to JSON
    - Returns formatted response

4. **`list_tables()`**
    - Shows all registered tables
    - Useful for discovery

5. **`get_table_schema(table)`**
    - Returns table schema
    - Shows column names and types

## New HTTP Endpoints

### 1. Query Endpoint (GET)
```
GET /query/:db?sql=<query>
```

**Example:**
```bash
curl 'http://localhost:3000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 10'
```

### 2. Query Endpoint (POST)
```
POST /query/:db
Body: SQL query as plain text
```

**Example:**
```bash
curl -X POST http://localhost:3000/query/mydb \
  -d "SELECT host, AVG(usage) FROM mydb_cpu GROUP BY host"
```

### 3. List Tables
```
GET /tables/:db
```

**Example:**
```bash
curl http://localhost:3000/tables/mydb
```

**Response:**
```json
{
  "database": "mydb",
  "tables": ["mydb_cpu", "mydb_memory", "mydb_disk"],
  "count": 3
}
```

### 4. Get Schema
```
GET /schema/:db/:table
```

**Example:**
```bash
curl http://localhost:3000/schema/mydb/cpu
```

## SQL Capabilities

### Basic Queries
```sql
-- Select all
SELECT * FROM mydb_cpu LIMIT 10;

-- Filter
SELECT * FROM mydb_cpu WHERE host = 'server01';

-- Sort
SELECT * FROM mydb_cpu ORDER BY usage DESC;
```

### Aggregations
```sql
-- Average
SELECT AVG(usage) as avg_usage FROM mydb_cpu;

-- Group by
SELECT host, AVG(usage) as avg_usage FROM mydb_cpu GROUP BY host;

-- Multiple aggregations
SELECT 
  host,
  COUNT(*) as count,
  AVG(usage) as avg_usage,
  MAX(usage) as max_usage,
  MIN(usage) as min_usage
FROM mydb_cpu
GROUP BY host;
```

### JOINs
```sql
-- Join CPU and memory data
SELECT 
  c.host,
  c.usage as cpu_usage,
  m.used as memory_used
FROM mydb_cpu c
JOIN mydb_memory m ON c.host = m.host;
```

### Window Functions
```sql
-- Moving average
SELECT 
  timestamp,
  usage,
  AVG(usage) OVER (
    ORDER BY timestamp 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) as moving_avg
FROM mydb_cpu;
```

### Subqueries
```sql
-- Above average
SELECT * FROM mydb_cpu 
WHERE usage > (SELECT AVG(usage) FROM mydb_cpu);
```

### Advanced Analytics
```sql
-- Percentiles
SELECT 
  approx_percentile_cont(usage, 0.5) as p50,
  approx_percentile_cont(usage, 0.95) as p95,
  approx_percentile_cont(usage, 0.99) as p99
FROM mydb_cpu;

-- CASE statements
SELECT 
  host,
  usage,
  CASE 
    WHEN usage < 30 THEN 'low'
    WHEN usage < 70 THEN 'medium'
    ELSE 'high'
  END as usage_level
FROM mydb_cpu;
```

## How It Works

### 1. Write Flow (Unchanged)
```
Write Request → Parse → Cache → Flush → Parquet File
```

### 2. Query Flow (New)
```
Query Request → Register Tables → Parse SQL → Execute → Return JSON
```

### Detailed Query Flow

```
1. User sends SQL query: "SELECT * FROM mydb_cpu"
   ↓
2. Query Engine registers database
   - Scans ./data/mydb/ directory
   - Finds cpu_1234567890.parquet
   - Registers as table "mydb_cpu"
   ↓
3. DataFusion parses SQL
   - Validates syntax
   - Creates logical plan
   - Optimizes plan
   ↓
4. DataFusion executes query
   - Reads Parquet file (columnar)
   - Applies filters (predicate pushdown)
   - Processes in parallel
   - Returns RecordBatches
   ↓
5. Query Engine converts to JSON
   - Iterates through batches
   - Converts Arrow arrays to JSON values
   - Formats response
   ↓
6. HTTP response sent to user
```

## Performance Benefits

### Parquet + DataFusion = Fast Queries

1. **Columnar Storage**
    - Only read needed columns
    - Skip irrelevant data
    - Better compression

2. **Predicate Pushdown**
    - Filters applied at read time
    - Don't read filtered rows
    - Minimal memory usage

3. **Parallel Execution**
    - Multi-threaded processing
    - Utilize all CPU cores
    - Fast aggregations

4. **Memory Efficiency**
    - Stream processing
    - Don't load all data at once
    - Handle large datasets

### Example Performance

**Query: SELECT AVG(usage) FROM mydb_cpu WHERE host = 'server01'**

Traditional approach:
1. Read entire file
2. Filter in memory
3. Calculate average
   Time: ~500ms for 1M rows

DataFusion approach:
1. Read only 'usage' and 'host' columns
2. Filter while reading
3. Streaming aggregation
   Time: ~50ms for 1M rows (10x faster!)

## Code Integration

### State Management

```rust
#[derive(Clone)]
struct AppState {
    cache: DataCache,                    // For writes
    query_engine: Arc<QueryEngine>,      // For queries
}
```

### Handler Example

```rust
async fn query_handler(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<QueryParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Register database tables
    state.query_engine.register_database(&db).await?;
    
    // Execute query
    let json = state.query_engine.execute_query_json(&params.sql).await?;
    
    // Return JSON response
    (StatusCode::OK, json)
}
```

## Best Practices

### 1. Always Flush Before Querying
```bash
# Write data
curl -X POST http://localhost:3000/write/mydb -d '...'

# Flush to disk
curl -X POST http://localhost:3000/flush

# Now query
curl 'http://localhost:3000/query/mydb?sql=...'
```

### 2. Use LIMIT for Exploration
```sql
-- Good: Fast preview
SELECT * FROM mydb_cpu LIMIT 10;

-- Bad: Might return millions of rows
SELECT * FROM mydb_cpu;
```

### 3. Filter Early
```sql
-- Good: Filter then aggregate
SELECT AVG(usage) FROM mydb_cpu WHERE host = 'server01';

-- Less efficient: Aggregate all then filter
SELECT host, AVG(usage) as avg FROM mydb_cpu GROUP BY host HAVING host = 'server01';
```

### 4. Use POST for Complex Queries
```bash
# GET: URL encoding issues
curl 'http://localhost:3000/query/mydb?sql=SELECT%20*%20...'

# POST: Clean and easy
curl -X POST http://localhost:3000/query/mydb -d "SELECT * ..."
```

## Use Cases

### 1. Real-time Monitoring Dashboard
```sql
-- Latest metrics
SELECT host, usage, timestamp 
FROM mydb_cpu 
ORDER BY timestamp DESC 
LIMIT 20;
```

### 2. Historical Analysis
```sql
-- Average by day (requires timestamp conversion)
SELECT 
  (timestamp / 86400000000000) as day,
  AVG(usage) as daily_avg
FROM mydb_cpu
GROUP BY (timestamp / 86400000000000);
```

### 3. Anomaly Detection
```sql
-- Find outliers (>2 std dev)
SELECT * FROM mydb_cpu
WHERE usage > (
  SELECT AVG(usage) + 2 * STDDEV(usage) FROM mydb_cpu
);
```

### 4. Capacity Planning
```sql
-- 95th percentile usage by host
SELECT 
  host,
  approx_percentile_cont(usage, 0.95) as p95_usage
FROM mydb_cpu
GROUP BY host
ORDER BY p95_usage DESC;
```

### 5. Correlation Analysis
```sql
-- CPU vs Memory correlation
SELECT 
  c.host,
  c.usage as cpu_usage,
  m.used as memory_used
FROM mydb_cpu c
JOIN mydb_memory m ON c.host = m.host AND c.timestamp = m.timestamp
WHERE c.usage > 70;
```

## Testing

Run the comprehensive test:
```bash
./test_with_queries.sh
```

This will:
1. Write sample data
2. Flush to disk
3. List tables
4. Run various SQL queries
5. Demonstrate all features

## Future Enhancements

Potential improvements:

1. **Query Result Caching**
    - Cache frequent queries
    - Invalidate on new writes
    - Faster dashboard updates

2. **Materialized Views**
    - Pre-computed aggregations
    - Auto-refresh on flush
    - Instant dashboard loads

3. **Query Optimization Hints**
    - Index recommendations
    - Query rewriting suggestions
    - Performance insights

4. **Streaming Queries**
    - Subscribe to query results
    - WebSocket updates
    - Real-time dashboards

5. **Query History**
    - Log all queries
    - Performance metrics
    - Slow query detection

## Conclusion

DataFusion transforms Stats from a write-only time series database into a full-featured analytics database. You can now:
- Write data easily (line protocol)
- Store efficiently (Parquet)
- Query powerfully (SQL)
- All in one system! 🎉

See **QUERY_EXAMPLES.md** for comprehensive query examples.