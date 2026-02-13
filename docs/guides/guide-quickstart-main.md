# Quick Start Guide

## Installation

```bash
cargo build --release
```

## Configuration

### 1. Use Default Config
```bash
# config.toml is loaded automatically
./target/release/mynadb
```

### 2. Use Production Config
```bash
./target/release/mynadb -c config.production.toml
```

### 3. Create Custom Config
```bash
# Copy template
cp config.toml config.custom.toml

# Edit settings
nano config.custom.toml

# Run with custom config
./target/release/mynadb -c config.custom.toml
```

## Key Configuration Settings

### Change Ports
```toml
[server]
http_port = 8000      # Change from 7000
flight_port = 8001    # Change from 7001
```

### Change Data Directory
```toml
[storage]
data_dir = "/data0/mynadb/data"  # Production SSD path
```

### Tune Performance
```toml
[cache]
flush_interval_secs = 60    # Flush every minute (default)
max_size_mb = 4096           # 4GB cache (default)

[compaction]
min_files = 15
target_file_size_mb = 500
```

## Usage Examples

### Write Data
```bash
# InfluxDB line protocol
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01,region=us-east usage=23.5,idle=76.5 1609459200000000000'
```

### Query Data (HTTP)
```bash
# Using POST with body (recommended)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu LIMIT 10"

# Using GET with query parameter
curl 'http://localhost:7000/query/mydb?sql=SELECT+*+FROM+cpu+LIMIT+10'
```

### Query Data (Flight)
```bash
datafusion-cli --host localhost --port 7001
> SELECT host, AVG(usage) FROM mydb.default.cpu GROUP BY host;
```

### Compact Data
```bash
# Compact entire catalog
curl -X POST http://localhost:7000/compact/mydb

# Compact specific measurement
curl -X POST http://localhost:7000/compact/mydb/cpu
```

### Check Health
```bash
curl http://localhost:7000/health
```

### View Cache Stats
```bash
curl http://localhost:7000/stats
```

### Manual Flush
```bash
curl -X POST http://localhost:7000/flush
```

## Common Tasks

### Change Port to 8000
**config.toml:**
```toml
[server]
http_port = 8000
flight_port = 8001
```

**Run:**
```bash
./target/release/mynadb
# HTTP: http://localhost:8000
# Flight: grpc://localhost:8001
```

### Use Production Data Path
**config.production.toml:**
```toml
[storage]
data_dir = "/data0/mynadb/data"
```

**Run:**
```bash
sudo mkdir -p /data0/mynadb/data
sudo chown $USER:$USER /data0/mynadb/data
./target/release/mynadb -c config.production.toml
```

### Optimize for High Write Throughput
**config.toml:**
```toml
[cache]
flush_interval_secs = 120  # 2 minutes
max_size_mb = 8192         # 8GB cache

[compaction]
min_files = 25             # Less frequent compaction
target_file_size_mb = 1024
```

### Optimize for Low Query Latency
**config.toml:**
```toml
[cache]
flush_interval_secs = 15   # 15 seconds
max_size_mb = 2048

[compaction]
min_files = 5              # More frequent compaction
target_file_size_mb = 200
```

## Understanding the Data Model

### Write Path
```
HTTP POST /write/:db → Line Protocol Parser → WAL → Write Cache (in-memory)
    → Background Flush (every 60s) → Parquet Files (date-partitioned)
    → Compactor → Merged Files (L0 → L1 → L2 ...)
```

### Storage Structure
```
data/
└── mydb/                          # catalog (from URI /write/mydb)
    └── default/                   # schema (from service tag or "default")
        └── cpu/                   # table (measurement name)
            └── date=2024-01-10/   # date partition
                ├── L0_*.parquet   # recent writes
                ├── L1_*.parquet   # first compaction level
                └── L2_*.parquet   # higher compaction levels
```

### Query Syntax
```bash
# Fully qualified: catalog.schema.table
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM mydb.default.cpu LIMIT 10"

# Schema.table (auto-prefixes catalog)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM default.cpu LIMIT 10"

# Simple name (auto-prefixes catalog.default.)
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM cpu LIMIT 10"
```

## Troubleshooting

### Config File Not Found
```
WARN Could not load config file: No such file or directory
WARN Using default configuration
```
**Solution:** Create `config.toml` or use `-c` to specify the path.

### Port Already in Use
```
Error: Address already in use (os error 98)
```
**Solution:** Change port in config or stop conflicting service.

### Permission Denied on Data Directory
```
Error: Permission denied (os error 13)
```
**Solution:**
```bash
sudo mkdir -p /data0/mynadb/data
sudo chown $USER:$USER /data0/mynadb/data
```

## Production Checklist

- [ ] Use `-c config.production.toml` with appropriate settings
- [ ] Set `data_dir` to dedicated SSD/NVMe mount
- [ ] Adjust cache size based on available RAM
- [ ] Configure firewall for ports (7000, 7001, 7002)
- [ ] Enable WAL with `sync_on_write = true` for durability
- [ ] Enable authentication with `[auth] enabled = true`
- [ ] Set up systemd service for auto-start
- [ ] Configure log rotation
- [ ] Monitor disk usage in data directory
- [ ] Set up retention policies

## Next Steps

1. Read `guide-configuration.md` for detailed configuration options
2. Read `guide-sql-queries.md` for SQL query examples
3. See `../architecture/architecture-overview.md` for system design
