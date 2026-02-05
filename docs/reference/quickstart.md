# Quick Start Guide

## Step 1: Build and Run

```bash
cd stats
cargo run
```

You should see:
```
🚀 Time Series Database Server running on http://0.0.0.0:7000
📊 Data directory: ./data
⏱️  Flush interval: 30 seconds
🔄 Background flush started: every 30 seconds
```

## Step 2: Write Some Data

Open a new terminal and run:

```bash
# Simple write
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'
```

**Note**: Data is now cached in memory! It won't be written to disk immediately.

## Step 3: Check Cache Statistics

```bash
curl http://localhost:7000/stats
```

Output:
```
Cache Statistics:
  Points: 1
  Measurements: 1
```

## Step 4: Write More Data

```bash
curl -X POST http://localhost:7000/write/mydb -d '
cpu,host=server01 usage=80.5
cpu,host=server02 usage=45.2
memory,host=server01 used=16.5,total=32.0
'
```

Check stats again:
```bash
curl http://localhost:7000/stats
```

## Step 5: Manual Flush (Optional)

To immediately write data to disk instead of waiting 30 seconds:

```bash
curl -X POST http://localhost:7000/flush
```

Output:
```
Flushed 2 file(s) to disk
```

## Step 6: Run Full Test Suite

```bash
./test_with_queries.sh
```

This will write data AND run SQL queries to demonstrate all features.

## Step 7: Query Your Data

Now that data is flushed to disk, you can query it with SQL!

### List available tables
```bash
curl http://localhost:7000/tables/mydb
```

### Simple query
```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 5'
```

### Aggregate query
```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) as avg_usage FROM mydb_cpu GROUP BY host'
```

### Complex query (using POST)
```bash
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM mydb_cpu WHERE usage > 50 ORDER BY time DESC"
```

## Step 8: Verify the Data

After flushing (either manually or after 30 seconds):

```bash
ls -lh ./data/mydb/
```

You should see Parquet files like:
```
cpu_1703251234567.parquet
memory_1703251234890.parquet
```

**Key Difference from Before:**
- Multiple write requests within 30 seconds → Combined into single Parquet file per measurement
- Significantly reduces the number of files created

## Understanding the Caching Flow

1. **Write Request** → Data is parsed and stored in memory (Arrow format)
2. **Cache Accumulates** → Multiple writes within 30 seconds are buffered together
3. **Background Flush** → Every 30 seconds, all cached data is written to Parquet
4. **Result** → One Parquet file per measurement per 30-second window

## Example Timeline

```
00:00 - Write 10 CPU points → Cache: 10 points
00:05 - Write 20 CPU points → Cache: 30 points
00:15 - Write 15 CPU points → Cache: 45 points
00:30 - AUTO FLUSH → cpu_timestamp.parquet (45 points total)
00:35 - Write 10 CPU points → Cache: 10 points
01:00 - AUTO FLUSH → cpu_timestamp2.parquet (10 points total)
```

## Manual Flush Use Cases

Use `POST /flush` when you need to:
- Ensure data is persisted before shutdown
- Test data writing immediately
- Flush important data that can't wait 30 seconds
- Debug or verify data storage

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