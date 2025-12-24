# Quick Start Guide

## Installation

```bash
cd stats-service
cargo build --release
```

## Configuration

### 1. Use Default Config
```bash
# config.toml is loaded automatically
./target/release/stats
```

### 2. Use Production Config
```bash
CONFIG_PATH=config.production.toml ./target/release/stats
```

### 3. Create Custom Config
```bash
# Copy template
cp config.toml config.custom.toml

# Edit settings
nano config.custom.toml

# Run with custom config
CONFIG_PATH=config.custom.toml ./target/release/stats
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
data_dir = "/data0/data"  # Production SSD path
```

### Tune Performance
```toml
[cache]
flush_interval_secs = 60   # Flush every minute
max_size_mb = 4096         # 4GB cache

[compaction]
min_files = 15
target_file_size_mb = 2048  # 2GB target files
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
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'
```

### Query Data (Flight)
```bash
datafusion-cli --host localhost --port 7001
> USE mydb;
> SELECT host, AVG(usage) FROM cpu GROUP BY host;
```

### Manage Databases
```bash
# Create database
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"mydb"}'

# List databases
curl http://localhost:7000/databases

# Delete database
curl -X DELETE http://localhost:7000/databases/mydb
```

### Compact Data
```bash
# Compact entire database
curl -X POST http://localhost:7000/compact/mydb

# Compact specific measurement
curl -X POST http://localhost:7000/compact/mydb/cpu
```

### Monitor Cardinality
```bash
curl http://localhost:7000/series_stats/mydb
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CONFIG_PATH` | Path to config file | `config.toml` |

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
./target/release/stats
# HTTP: http://localhost:8000
# Flight: grpc://localhost:8001
```

### Use Production Data Path
**config.production.toml:**
```toml
[storage]
data_dir = "/data0/data"
```

**Run:**
```bash
sudo mkdir -p /data0/data
sudo chown $USER:$USER /data0/data
CONFIG_PATH=config.production.toml ./target/release/stats
```

### Optimize for High Write Throughput
**config.toml:**
```toml
[cache]
flush_interval_secs = 120  # 2 minutes
max_size_mb = 8192         # 8GB cache

[compaction]
min_files = 25             # Less frequent compaction
target_file_size_mb = 4096 # 4GB files
```

### Optimize for Low Query Latency
**config.toml:**
```toml
[cache]
flush_interval_secs = 15   # 15 seconds
max_size_mb = 512          # Smaller cache

[compaction]
min_files = 5              # More frequent compaction
target_file_size_mb = 512  # Smaller files
```

## Troubleshooting

### Config File Not Found
```bash
⚠️  Could not load config file: No such file or directory
⚠️  Using default configuration
```
**Solution:** Create `config.toml` or specify `CONFIG_PATH`

### Port Already in Use
```bash
Error: Address already in use (os error 98)
```
**Solution:** Change port in config or stop conflicting service

### Permission Denied on Data Directory
```bash
Error: Permission denied (os error 13)
```
**Solution:** 
```bash
sudo mkdir -p /data0/data
sudo chown $USER:$USER /data0/data
```

## Production Checklist

- [ ] Copy `config.production.toml` to deployment location
- [ ] Set `data_dir` to dedicated SSD/NVMe mount
- [ ] Adjust cache size based on available RAM
- [ ] Configure firewall for ports (7000, 7001)
- [ ] Set up systemd service for auto-start
- [ ] Configure log rotation
- [ ] Monitor disk usage in data directory
- [ ] Set up alerting for high cardinality
- [ ] Test backup/restore procedures

## Next Steps

1. Read `CONFIG.md` for detailed configuration guide
2. Read `ARCHITECTURE.md` for system design
3. See `FLIGHT_USAGE.md` for Arrow Flight details
