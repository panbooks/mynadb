# Stats - Time Series Database

A simple time series database built from scratch in Rust that:
1. Receives data via HTTP in InfluxDB line protocol format
2. Parses the line protocol
3. Stores data in Parquet format using Apache Arrow

## Architecture

```
HTTP Request → Line Protocol Parser → Parquet Writer → Disk Storage
```

### Components

1. **HTTP Server** - Built with Axum, handles write requests
2. **Line Protocol Parser** - Parses InfluxDB line protocol format
3. **Parquet Writer** - Stores data in columnar Parquet format using Arrow

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

Server starts on `http://0.0.0.0:3000`

## API Endpoints

### Write Data

```
POST /write/:db
```

Write time series data to a database.

**Example:**

```bash
curl -X POST http://localhost:3000/write/mydb \
  -d 'cpu,host=server01,region=us-west usage=23.5,temp=45i'
```

**Multiple points:**

```bash
curl -X POST http://localhost:3000/write/mydb -d '
cpu,host=server01 usage=23.5,cores=8i
cpu,host=server02 usage=45.2,cores=16i
memory,host=server01 used=8.5,total=32.0
disk,host=server01,mount=/data used=120.5,total=500.0
'
```

### Health Check

```bash
curl http://localhost:3000/health
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
curl -X POST http://localhost:3000/write/testdb \
  -d 'cpu usage=50.0'

# With tags
curl -X POST http://localhost:3000/write/testdb \
  -d 'cpu,host=server01,dc=us-east usage=75.5,cores=8i'

# Multiple measurements
curl -X POST http://localhost:3000/write/testdb -d '
cpu,host=server01 usage=50.0,temp=45i
memory,host=server01 used=16.5,total=32.0
network,host=server01,interface=eth0 rx_bytes=1024000i,tx_bytes=2048000i
'

# Different data types
curl -X POST http://localhost:3000/write/testdb -d '
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
- **chrono** - Date/time handling
- **serde** - Serialization

## Features

✅ HTTP API for data ingestion  
✅ InfluxDB line protocol parsing  
✅ Support for tags and multiple fields  
✅ Multiple data types (float, integer, string, boolean)  
✅ Automatic timestamp generation  
✅ Columnar storage in Parquet format  
✅ Organized by database and measurement  
✅ Schema inference from data

## Next Steps

Potential enhancements:
- [ ] Query API to read data back
- [ ] Data compression and compaction
- [ ] WAL (Write-Ahead Log) for durability
- [ ] Indexing for faster queries
- [ ] Retention policies
- [ ] Authentication and authorization
- [ ] Clustering and replication

## License

This is a demo project for learning purposes.