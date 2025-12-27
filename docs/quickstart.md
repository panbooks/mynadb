# Quick Start Guide

## Build and Run

```bash
# Build
cargo build --release

# Run (uses config.toml by default)
./target/release/stats

# Or with custom config
CONFIG_PATH=config.production.toml ./target/release/stats
```

Server starts on:
- HTTP: `http://localhost:7000`
- Arrow Flight: `grpc://localhost:7001`

## Write Data

```bash
# Simple write
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'

# With tags and timestamp
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01,region=us-east usage=23.5,idle=76.5 1609459200000000000'

# Multiple measurements
curl -X POST http://localhost:7000/write/mydb -d '
cpu,host=server01 usage=80.5
cpu,host=server02 usage=45.2
memory,host=server01 used=16.5,total=32.0
'
```

## Query Data

### HTTP API
```bash
# Simple query
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'

# Aggregation
curl 'http://localhost:7000/query/mydb?sql=SELECT host, AVG(usage) FROM cpu GROUP BY host'

# POST for complex queries
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu WHERE usage > 50 ORDER BY timestamp DESC"
```

### Arrow Flight (datafusion-cli)
```bash
datafusion-cli --host localhost --port 7001

> SELECT * FROM mydb.default.cpu;
> SELECT host, AVG(usage) FROM mydb.default.cpu GROUP BY host;
```

## Other Operations

```bash
# Check cache stats
curl http://localhost:7000/stats

# Manual flush to disk
curl -X POST http://localhost:7000/flush

# List tables
curl http://localhost:7000/tables/mydb

# Compact database
curl -X POST http://localhost:7000/compact/mydb

# Series cardinality
curl http://localhost:7000/series_stats/mydb

# Health check
curl http://localhost:7000/health
```

## Configuration

Edit `config.toml`:

```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data0/stats/data"

[cache]
flush_interval_secs = 30
max_size_mb = 1024

[compaction]
min_files = 4
target_file_size_mb = 500
```

## Data Flow

1. **Write** - Data cached in memory (Arrow format)
2. **Flush** - Every 30s, cache written to Parquet files (L0)
3. **Compaction** - L0 files merged into L1, L2, etc.
4. **Query** - DataFusion reads Parquet files

## File Structure

```
data/
└── mydb/                  # catalog (database)
    └── default/           # schema (from service tag)
        └── cpu/           # table (measurement)
            └── date=2024-01-15/
                └── L0_*.parquet
```
