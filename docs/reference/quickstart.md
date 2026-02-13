# Quick Start Guide

## Step 1: Build and Run

```bash
cargo build --release
./target/release/mynadb
```

You should see:
```
MynaDB - The Voice of Your Machines
  HTTP server listening on http://0.0.0.0:7000
  Arrow Flight listening on grpc://0.0.0.0:7001
  Data directory: ./data
  Flush interval: 60 seconds
```

## Step 2: Write Some Data

Open a new terminal and run:

```bash
# Simple write
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'
```

**Note**: Data is now cached in memory and queryable immediately via Memtable. It will be flushed to Parquet files in the background.

## Step 3: Check Cache Statistics

```bash
curl http://localhost:7000/stats
```

## Step 4: Write More Data

```bash
curl -X POST http://localhost:7000/write/mydb -d '
cpu,host=server01 usage=80.5
cpu,host=server02 usage=45.2
memory,host=server01 used=16.5,total=32.0
'
```

## Step 5: Query Your Data

Data is queryable immediately (no flush needed thanks to Memtable):

### List available tables
```bash
curl http://localhost:7000/tables/mydb
```

### Simple query
```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu LIMIT 5"
```

### Aggregate query
```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host"
```

### Time range query
```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu WHERE time > now() - INTERVAL '1 hour' ORDER BY time DESC"
```

## Step 6: Manual Flush (Optional)

To immediately write data to Parquet files instead of waiting for the background flush:

```bash
curl -X POST http://localhost:7000/flush
```

## Step 7: Verify Storage

After flushing (either manually or after the background interval):

```bash
ls -lh ./data/mydb/default/cpu/
```

You should see date-partitioned directories:
```
date=2024-01-10/
    L0_abc123.parquet
    L0_def456.parquet
```

**Key Points:**
- Data is partitioned by date (`date=YYYY-MM-DD/`)
- Files are prefixed with compaction level (`L0_`, `L1_`, `L2_`, ...)
- Multiple writes within a flush interval are combined into a single Parquet file per measurement

## Step 8: Explain a Query

View the execution plan before running an expensive query:

```bash
curl 'http://localhost:7000/explain/mydb?sql=SELECT+*+FROM+cpu+WHERE+time+>+now()+-+INTERVAL+%271+hour%27'
```

## Understanding the Data Flow

1. **Write Request** → Data is parsed (InfluxDB line protocol) and written to WAL
2. **WAL + Cache** → Data stored in WAL for durability and in-memory cache for queries
3. **Memtable** → Cached data is immediately queryable (no flush required!)
4. **Background Flush** → Every 60 seconds, cached data is written to Parquet files
5. **Compaction** → Background job merges small files (L0) into larger files (L1, L2, ...)

## Example Timeline

```
00:00 - Write 10 CPU points → Cache: 10 points (queryable immediately)
00:05 - Write 20 CPU points → Cache: 30 points
00:15 - Write 15 CPU points → Cache: 45 points
01:00 - AUTO FLUSH → date=YYYY-MM-DD/L0_abc.parquet (45 points)
01:05 - Write 10 CPU points → Cache: 10 points (queryable immediately)
02:00 - AUTO FLUSH → date=YYYY-MM-DD/L0_def.parquet (10 points)
```

## What's Next?

Try writing your own data! The format is:
```
measurement[,tag=value...] field=value[,field=value...] [timestamp]
```

Example:
```bash
curl -X POST http://localhost:7000/write/metrics -d '
requests,service=api,method=GET count=100i,latency=45.2
requests,service=api,method=POST count=50i,latency=128.7
database,type=postgres queries=1000i,avg_time=23.5
'
```

## Next Steps

1. Read `guide-configuration.md` for all configuration options
2. Read `guide-sql-queries.md` for SQL query reference
3. See `../architecture/architecture-overview.md` for system design
4. See `../features/features-flight-usage.md` for Arrow Flight API details
