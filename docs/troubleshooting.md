# Troubleshooting

## Compaction

### "No compaction needed (minimum file threshold not met)"

**Cause**: Less than 5 L0 files exist.

**Solution**: Create more files before compacting:
```bash
# Write and flush multiple times to create separate files
for i in {1..8}; do
    curl -X POST http://localhost:7000/write/mydb -d "cpu,host=server$i usage=$i"
    curl -X POST http://localhost:7000/flush
done

# Now compact
curl -X POST http://localhost:7000/compact/mydb
```

### Compaction takes too long

**Solutions**:
- Increase `target_file_size_mb` in config
- Compact during off-peak hours
- Compact one table at a time: `POST /compact/mydb/default/cpu`

## Queries

### "Unsupported type: Utf8View"

**Cause**: Older code not handling new Arrow string types.

**Solution**: This is fixed in current version. If you see this, update your installation.

### Query returns empty results

**Causes & Solutions**:
1. Data not flushed yet → `curl -X POST http://localhost:7000/flush`
2. Wrong table name → Check with `curl http://localhost:7000/tables/mydb`
3. Wrong schema → Use fully qualified: `SELECT * FROM mydb.schema.table`

### "Schema contains duplicate qualified field name"

**Cause**: Both partition column and parquet column have same name.

**Solution**: Don't include partition column (like `date`) in parquet schema if using Hive partitioning.

## Server

### "Config file not found"

```
⚠️  Could not load config file: No such file or directory
⚠️  Using default configuration
```

**Solution**: Create `config.toml` or set `CONFIG_PATH` environment variable.

### "Address already in use"

**Solution**: Change ports in config.toml or stop conflicting service:
```toml
[server]
http_port = 8000
flight_port = 8001
```

### "Permission denied on data directory"

**Solution**:
```bash
sudo mkdir -p /data0/stats/data
sudo chown $USER:$USER /data0/stats/data
```

## Arrow Flight

### datafusion-cli can't connect

**Solutions**:
1. Check server is running: `curl http://localhost:7000/health`
2. Check Flight port (default 7001): `nc -zv localhost 7001`
3. Use correct syntax: `datafusion-cli --host localhost --port 7001`

### "Unknown database" in datafusion-cli

**Solution**: Issue `USE dbname;` before querying:
```sql
> USE mydb;
> SELECT * FROM cpu;
```

## Data

### Data goes to wrong schema

**Cause**: Tag key not matching config.

**Solution**: Check `tag_keys` in config.toml matches your metric tags:
```toml
[schema]
tag_keys = ["service", "hierarchy_2", "MONITOR.service"]
```

### Numeric schema causing SQL errors

**Solution**: Enable `prefix_numeric = true` in config.toml:
```toml
[schema]
prefix_numeric = true  # "123" becomes "svc_123"
```
