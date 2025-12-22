# Quick Start Guide

## Step 1: Build and Run

```bash
cd stats
cargo run
```

You should see:
```
🚀 Time Series Database Server running on http://0.0.0.0:3000
📊 Data directory: ./data
```

## Step 2: Write Some Data

Open a new terminal and run:

```bash
# Simple write
curl -X POST http://localhost:3000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'
```

## Step 3: Run Full Test Suite

```bash
./test.sh
```

This will write various types of data to test all features.

## Step 4: Verify the Data

Check the created files:

```bash
ls -lh ./data/mydb/
```

You should see Parquet files like:
```
cpu_1703251234567.parquet
memory_1703251234890.parquet
disk_1703251235123.parquet
```

## Step 5: Read the Data (Optional)

If you have Python with PyArrow installed:

```bash
# Read all data from a database
python3 read_parquet.py ./data/mydb

# Or read a specific file
python3 read_parquet.py ./data/mydb/cpu_1703251234567.parquet
```

## Understanding the Flow

1. **HTTP Request** → Server receives POST to `/write/:db`
2. **Line Protocol Parser** → Parses the data format: `measurement,tag=value field=value`
3. **Parquet Writer** → Stores data in columnar format at `./data/:db/:measurement_timestamp.parquet`

## Example Data Formats

### Simple metric
```
cpu usage=50.0
```

### With tags
```
cpu,host=server01,region=us-west usage=75.5
```

### Multiple fields
```
cpu,host=server01 usage=75.5,temp=45i,cores=8i
```

### Different types
```
sensor temperature=72.5,count=42i,active=true,status="online"
```

## What's Next?

Try writing your own data! The format is:
```
measurement[,tag=value...] field=value[,field=value...] [timestamp]
```

Example:
```bash
curl -X POST http://localhost:3000/write/metrics -d '
requests,service=api,method=GET count=100i,latency=45.2
requests,service=api,method=POST count=50i,latency=128.7
database,type=postgres queries=1000i,avg_time=23.5
'
```