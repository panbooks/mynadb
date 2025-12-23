# Stats - Time Series Database

A simple time series database built from scratch in Rust that:
1. Receives data via HTTP in InfluxDB line protocol format
2. Parses the line protocol
3. **Buffers data in memory (Arrow format)**
4. **Flushes to disk every 30 seconds** in Parquet format

## Architecture

```
┌─────────────────────────────────────────────────┐
│              HTTP Requests (Write)               │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│          Line Protocol Parser                    │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│    In-Memory Cache (Arrow format)                │
│    - Auto-flush every 30 seconds                 │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│         Parquet Files (Columnar Storage)         │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│    DataFusion Query Engine (SQL)                 │
│    - Read Parquet files                          │
│    - Execute SQL queries                         │
│    - Return JSON results                         │
└─────────────────────────────────────────────────┘
```

### Components

1. **HTTP Server** - Built with Axum, handles write and query requests
2. **Line Protocol Parser** - Parses InfluxDB line protocol format
3. **In-Memory Cache** - Buffers data in Arrow format with automatic flushing
4. **Parquet Writer** - Stores data in columnar Parquet format using Arrow
5. **DataFusion Query Engine** - SQL query engine for reading and analyzing data

### Key Improvement: Caching Layer

Instead of writing a new Parquet file for every request, data is:
- ✅ Stored in memory using Arrow format
- ✅ Automatically flushed to disk every **30 seconds**
- ✅ Grouped by database and measurement
- ✅ Reduces number of small files significantly

This means:
- **Before**: 100 requests = 100 Parquet files
- **After**: 100 requests over 30 seconds = 1 Parquet file per measurement

## Line Protocol Format

```
measurement[,tag=value...] field=value[,field=value...] [timestamp]
```

### Examples

```bash
# Simple metric
cpu usage=23.5

# With tags
cpu,host=server01,region=us-west usage=23.5,temp=45i

# With timestamp (nanoseconds)
cpu,host=server01 usage=23.5 1672531200000000000

# Multiple fields
weather,location=paris temperature=22.5,humidity=60i,conditions="sunny"

# Different data types
sensor,device=temp01 value=98.6,count=42i,active=true,status="online"
```

### Field Types

- **Float**: `23.5`, `98.6`
- **Integer**: `42i`, `100i` (note the 'i' suffix)
- **String**: `"value"` (quoted)
- **Boolean**: `true`, `false`, `t`, `f`

## Setup & Run

```bash
# Build
cargo build

# Run
cargo run
```

Server starts on `http://0.0.0.0:7000`

## API Endpoints

### Write Data

```
POST /write/:db
```

Write time series data to a database. Data is buffered in memory and flushed to disk every 30 seconds.

**Example:**

```bash
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01,region=us-west usage=23.5,temp=45i'
```

### Query Data (SQL)

```
GET /query/:db?sql=<query>
POST /query/:db (SQL in body)
```

Query data using SQL powered by DataFusion.

**Important**: Table names are automatically prefixed with the database name. You can use either:
- Short form: `SELECT * FROM cpu` (recommended)
- Full form: `SELECT * FROM mydb_cpu` (also works)

**Examples:**

```bash
# Simple query (short form - recommended)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# Full form also works
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu LIMIT 10'

# Aggregation
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) FROM cpu GROUP BY host'

# Complex query (POST)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu WHERE usage > 50 ORDER BY timestamp DESC"

# Join multiple measurements
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT c.*, m.used FROM cpu c JOIN memory m ON c.host = m.host"
```

### List Tables

```
GET /tables/:db
```

List all available tables (measurements) in a database.

```bash
curl http://localhost:7000/tables/mydb
```

### Get Schema

```
GET /schema/:db/:table
```

Get the schema for a specific table.

```bash
curl http://localhost:7000/schema/mydb/cpu
```

### Cache Statistics

```
GET /stats
```

Get current cache statistics (points in memory, number of measurements).

```bash
curl http://localhost:7000/stats
```

### Manual Flush

```
POST /flush
```

Manually trigger a flush of cached data to disk (in addition to automatic 30-second flushes).

```bash
curl -X POST http://localhost:7000/flush
```

### Compaction

```
POST /compact/:db
POST /compact/:db/:measurement
```

Compact (merge) multiple small Parquet files into larger files to improve query performance and reduce file count.

**Compact entire database:**
```bash
curl -X POST http://localhost:7000/compact/mydb
```

**Compact specific measurement:**
```bash
curl -X POST http://localhost:7000/compact/mydb/cpu
```

See [COMPACTION.md](./COMPACTION.md) for detailed documentation.

### Health Check

```
GET /health
```

```bash
curl http://localhost:7000/health
```

## Data Storage

Data is stored in `./data/` directory:

```
./data/
└── mydb/                    # Database name
    ├── cpu_1703251234567.parquet
    ├── memory_1703251234890.parquet
    └── disk_1703251235123.parquet
```

Each measurement is stored in separate Parquet files with timestamps in the filename.

## Schema

Parquet files have the following schema:

```
timestamp      (Timestamp nanosecond)  - When the data was recorded
measurement    (String)                - Measurement name
<tag_columns>  (String, nullable)      - One column per tag
<field_columns>(Float64/Int64/String)  - One column per field
```

**Example schema for `cpu` measurement:**

```
timestamp: Timestamp(Nanosecond, None)
measurement: Utf8
host: Utf8
region: Utf8
usage: Float64
temp: Int64
```

## Testing

### Test Line Protocol Parser

```bash
# Simple write
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu usage=50.0'

# With tags
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server01,dc=us-east usage=75.5,cores=8i'

# Multiple measurements
curl -X POST http://localhost:7000/write/testdb -d '
cpu,host=server01 usage=50.0,temp=45i
memory,host=server01 used=16.5,total=32.0
network,host=server01,interface=eth0 rx_bytes=1024000i,tx_bytes=2048000i
'

# Different data types
curl -X POST http://localhost:7000/write/testdb -d '
sensor,device=temp01 temperature=72.5,humidity=55i,active=true,status="normal"
'
```

### Verify Parquet Files

You can use `parquet-tools` or Python to read the files:

**Python example:**

```python
import pyarrow.parquet as pq

table = pq.read_table('./data/testdb/cpu_1703251234567.parquet')
print(table.to_pandas())
```

**Rust example:**

```rust
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;

let file = File::open("./data/testdb/cpu_1703251234567.parquet")?;
let reader = SerializedFileReader::new(file)?;
// Read and display data
```

## Example Output

When you write data, you'll see:

```
=== Write Request ===
Database: mydb
Raw data:
cpu,host=server01,region=us-west usage=23.5,temp=45i
Parsed 1 points
Writing 1 points for measurement 'cpu'
✅ Written to: ./data/mydb/cpu_1703251234567.parquet
==================
```

## Project Structure

```
stats/
├── Cargo.toml              # Dependencies
├── src/
│   ├── main.rs             # HTTP server and routing
│   ├── line_protocol.rs    # Line protocol parser
│   └── parquet_writer.rs   # Parquet/Arrow writer
└── data/                   # Data storage (created at runtime)
```

## Dependencies

- **tokio** - Async runtime
- **axum** - Web framework
- **arrow** - Columnar data format
- **parquet** - Parquet file format
- **datafusion** - SQL query engine
- **chrono** - Date/time handling
- **serde** - Serialization

## Features

✅ HTTP API for data ingestion  
✅ InfluxDB line protocol parsing  
✅ Support for tags and multiple fields  
✅ Multiple data types (float, integer, string, boolean)  
✅ Automatic timestamp generation  
✅ **In-memory caching with Arrow format**  
✅ **Automatic background flushing every 30 seconds**  
✅ **Manual flush endpoint for immediate writes**  
✅ Columnar storage in Parquet format  
✅ Organized by database and measurement  
✅ Schema inference from data  
✅ Cache statistics endpoint  
✅ **SQL query engine powered by Apache DataFusion**  
✅ **Complex queries: aggregations, joins, window functions**  
✅ **Table discovery and schema inspection**  
✅ **JSON query results**  
✅ **Compaction to merge small files and improve performance**  
✅ **Concurrent operation safety with locking mechanism**

## Next Steps

Potential enhancements:
- [x] Query API to read data back
- [x] DataFusion SQL query engine
- [x] In-memory caching with auto-flush
- [x] Compaction to reduce small file problem
- [ ] Data compression and compaction optimization
- [ ] WAL (Write-Ahead Log) for durability
- [ ] Indexing for faster queries
- [ ] Retention policies
- [ ] Authentication and authorization
- [ ] Clustering and replication
- [ ] Background automatic compaction

## License

This is a demo project for learning purposes.