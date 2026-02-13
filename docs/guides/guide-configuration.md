# Configuration Guide

MynaDB uses TOML configuration files for flexible deployment across different environments.

## Configuration Files

- **`config.toml`** - Default development configuration
- **`config.production.toml`** - Production configuration example

## Loading Configuration

By default, the server loads `config.toml` from the current directory. You can specify a different config file using the `-c` flag:

```bash
# Use default config.toml
mynadb

# Use production config
mynadb -c config.production.toml

# Use custom config
mynadb -c /etc/mynadb/config.toml

# Standalone mode (default)
mynadb standalone -c config.toml

# Cluster mode (separate node roles)
mynadb meta start -c config.meta.toml
mynadb data start -c config.data.toml
mynadb router start -c config.router.toml
mynadb admin start -c config.admin.toml
```

If the config file is not found, the server will use built-in defaults.

## Configuration Sections

### [server]
HTTP, gRPC, and MySQL server settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `http_host` | string | `"0.0.0.0"` | HTTP API bind address |
| `http_port` | u16 | `7000` | HTTP API port |
| `flight_host` | string | `"0.0.0.0"` | Arrow Flight gRPC bind address |
| `flight_port` | u16 | `7001` | Arrow Flight gRPC port |
| `mysql_enabled` | bool | `false` | Enable MySQL wire protocol |
| `mysql_host` | string | `"0.0.0.0"` | MySQL protocol bind address |
| `mysql_port` | u16 | `7002` | MySQL protocol port |

**Example:**
```toml
[server]
http_host = "0.0.0.0"
http_port = 7000
flight_host = "0.0.0.0"
flight_port = 7001
mysql_enabled = true
mysql_port = 7002
```

### [storage]
Data storage settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data_dir` | string | `"./data"` | Data directory path |
| `backend` | string | `"local"` | Storage backend: "local", "s3", "minio", "oss" |

**Example:**
```toml
[storage]
data_dir = "/data0/mynadb/data"  # Use absolute path in production

[storage.object_store]
backend = "local"
# bucket = "my-bucket"     # For S3/MinIO/OSS
# endpoint = "http://..."  # Custom endpoint
# region = "us-east-1"     # S3 region
```

**Recommended Production Settings:**
- Use SSD/NVMe storage for best performance
- Mount dedicated disk for data directory
- Use absolute paths

### [schema]
Schema extraction configuration - determines how to map line protocol data to catalog/schema/table structure

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `strategy` | string | `"service_as_schema"` | Naming strategy: `"service_as_schema"` or `"service_as_table"` |
| `tag_keys` | array | `["service"]` | Tag keys to extract schema from (first match wins) |
| `default_name` | string | `"default"` | Default schema when no matching tag is found |
| `prefix_numeric` | bool | `true` | Prefix numeric schemas with "service_" (e.g., "1" → "service_1") |

**Example:**
```toml
[schema]
strategy = "service_as_schema"
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]
default_name = "default"
prefix_numeric = true
```

**Strategy: ServiceAsSchema (default)**
- Service tag value → schema name, measurement → table name
- `service=production` → schema `production`, measurement `cpu` → table `cpu`

**Strategy: ServiceAsTable**
- Schema is always `default`, service tag → table name
- `service=web` → schema `default`, table `web`

**Numeric Prefix Examples:**
- Tag `service="123"` → Schema: `service_123` (avoids SQL parser issues)
- Tag `service="web"` → Schema: `web` (text values unchanged)

**Multiple Tag Keys:**
```toml
[schema]
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]
```
The first matching tag key in the list is used. This supports data sources that use different tag naming conventions.

### [cache]
In-memory write cache settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flush_interval_secs` | u64 | `60` | Background flush interval (seconds) |
| `max_size_mb` | u64 | `4096` | Max cache size before forcing flush (MB) |
| `parquet_cache_mb` | usize | `None` | Local disk cache for Parquet files (cluster mode) |

**Example:**
```toml
[cache]
flush_interval_secs = 60
max_size_mb = 4096  # 4GB cache
```

**Tuning Guidelines:**
- **Low Latency**: Smaller `flush_interval_secs` (10-20s), smaller cache
- **High Throughput**: Larger `flush_interval_secs` (60-120s), larger cache (4-8GB)
- Monitor memory usage and adjust `max_size_mb` accordingly

### [query_cache]
Query result caching

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable query result caching |
| `max_size_mb` | u64 | `256` | Maximum cache size in MB |
| `max_entries` | usize | `1000` | Maximum number of cached entries |
| `ttl_secs` | u64 | `300` | TTL for cached entries in seconds |
| `min_query_time_ms` | u64 | `10` | Only cache queries slower than this |

**Example:**
```toml
[query_cache]
enabled = true
max_size_mb = 256
ttl_secs = 300
min_query_time_ms = 10
```

### [wal]
Write-ahead log for durability

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable WAL |
| `sync_on_write` | bool | `false` | Sync to disk on every write (true=durable, false=faster) |
| `rotate_size_mb` | u64 | `128` | Rotate WAL file when it reaches this size |

**Example:**
```toml
[wal]
enabled = true
sync_on_write = false
rotate_size_mb = 128
```

### [compaction]
Parquet file compaction settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_files` | usize | `5` | Minimum L0 files to trigger LSM compaction |
| `major_min_files` | usize | `3` | Minimum L1+ files to trigger major compaction |
| `target_file_size_mb` | u64 | `200` | Target size after compaction (MB) |
| `max_level` | u32 | `5` | Maximum compaction levels (L0-L5) |
| `interval_secs` | u64 | `70` | Background LSM compaction interval |
| `full_compaction_enabled` | bool | `true` | Enable full compaction for yesterday's partitions |
| `full_compaction_hour` | u32 | `3` | Hour to run full compaction (0-23) |
| `full_compaction_interval_secs` | u64 | `3600` | Full compaction check interval |
| `full_compaction_target_size_mb` | u64 | `200` | Skip partitions larger than this in full compaction |
| `max_concurrent_compactions` | usize | `8` | Max parallel LSM compaction operations |
| `full_compaction_max_concurrent` | usize | `2` | Max parallel full compaction operations |
| `read_rate_limit_mb_per_sec` | u64 | `0` | Read rate limit in MB/sec (0 = unlimited) |
| `write_rate_limit_mb_per_sec` | u64 | `0` | Write rate limit in MB/sec (0 = unlimited) |

**Example:**
```toml
[compaction]
min_files = 5
major_min_files = 3
target_file_size_mb = 200
max_level = 5
interval_secs = 70
full_compaction_enabled = true
full_compaction_hour = 3
full_compaction_interval_secs = 3600
full_compaction_target_size_mb = 200
max_concurrent_compactions = 8
full_compaction_max_concurrent = 2
read_rate_limit_mb_per_sec = 0
write_rate_limit_mb_per_sec = 0
```

### [retention]
Automatic deletion of old date-partitioned data

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable background retention checks |
| `default_days` | u32 | `7` | Delete partitions older than this |
| `interval_secs` | u64 | `3600` | Check interval in seconds |
| `exclude_databases` | array | `["_system"]` | Databases to skip |
| `exclude_tables` | array | `[]` | Tables to skip (fully qualified) |

**Example:**
```toml
[retention]
enabled = true
default_days = 7
interval_secs = 3600
exclude_databases = ["_system"]
```

### [auth]
Authentication and authorization

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `false` | Enable authentication |
| `username` | string | `"admin"` | Admin username |
| `password` | string | `"secret"` | Admin password |

**Example:**
```toml
[auth]
enabled = true
username = "admin"
password = "secret"
```

### [logging]
Log output configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | string | `"info"` | Log level: trace, debug, info, warn, error |
| `log_dir` | string | `"logs"` | Log file directory |

**Example:**
```toml
[logging]
level = "info"
log_dir = "/data0/mynadb/logs"
```

### [continuous_aggregates]
Materialized views with automatic refresh

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `false` | Enable continuous aggregates |
| `refresh_check_interval_secs` | u64 | `60` | Refresh check interval |
| `max_concurrent_refreshes` | usize | `4` | Max parallel refreshes |
| `query_rewriting_enabled` | bool | `true` | Auto-rewrite queries to use aggregates |

### [otlp]
OpenTelemetry Protocol native ingestion

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `false` | Enable OTLP ingestion |
| `grpc_host` | string | `"0.0.0.0"` | OTLP gRPC bind address |
| `grpc_port` | u16 | `4317` | OTLP gRPC port |
| `database` | string | `"otel"` | Default database for OTLP data |

### [iceberg]
Apache Iceberg table format integration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `false` | Enable Iceberg integration |
| `rest_catalog_enabled` | bool | `true` | Enable REST catalog API |

### [admin]
Admin console configuration (cluster mode)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `http_host` | string | `"0.0.0.0"` | Admin HTTP bind address |
| `http_port` | u16 | `18080` | Admin web interface port |
| `meta_endpoints` | array | `[]` | Meta service URLs for cluster discovery |

**Example:**
```toml
[admin]
http_host = "0.0.0.0"
http_port = 18080
meta_endpoints = [
    "http://127.0.0.1:6000",
    "http://127.0.0.1:6100",
    "http://127.0.0.1:6200"
]
```

## Example Configurations

### Development (config.toml)
```toml
[server]
http_host = "0.0.0.0"
http_port = 7000
flight_host = "0.0.0.0"
flight_port = 7001

[storage]
data_dir = "./data"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true

[cache]
flush_interval_secs = 60
max_size_mb = 4096

[compaction]
min_files = 5
target_file_size_mb = 200
max_level = 5
interval_secs = 70
max_concurrent_compactions = 8
full_compaction_max_concurrent = 2

[wal]
enabled = true
sync_on_write = false
rotate_size_mb = 128
```

### Production (config.production.toml)
```toml
[server]
http_host = "0.0.0.0"
http_port = 7000
flight_host = "0.0.0.0"
flight_port = 7001
mysql_enabled = true
mysql_port = 7002

[storage]
data_dir = "/data0/mynadb/data"

[storage.object_store]
backend = "local"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true

[cache]
flush_interval_secs = 60
max_size_mb = 4096

[query_cache]
enabled = true
max_size_mb = 256
ttl_secs = 300
min_query_time_ms = 10

[compaction]
min_files = 10
major_min_files = 4
target_file_size_mb = 500
max_level = 7
interval_secs = 60
full_compaction_enabled = true
full_compaction_hour = 3
max_concurrent_compactions = 16
full_compaction_max_concurrent = 4
read_rate_limit_mb_per_sec = 200
write_rate_limit_mb_per_sec = 200

[wal]
enabled = true
sync_on_write = true
rotate_size_mb = 128

[retention]
enabled = true
default_days = 7
interval_secs = 3600

[auth]
enabled = true
username = "admin"
password = "change-me-in-production"

[logging]
level = "info"
log_dir = "/data0/mynadb/logs"
```

### High-Throughput Ingest
```toml
[cache]
flush_interval_secs = 120  # Batch more writes
max_size_mb = 8192  # 8GB cache

[compaction]
min_files = 20  # Less frequent compaction
major_min_files = 5
target_file_size_mb = 1024
max_level = 7
interval_secs = 120
max_concurrent_compactions = 16
full_compaction_max_concurrent = 4
```

### Low-Latency Queries
```toml
[cache]
flush_interval_secs = 15  # Faster data availability
max_size_mb = 2048

[compaction]
min_files = 5
major_min_files = 3
target_file_size_mb = 200
max_level = 5
interval_secs = 30  # More frequent compaction
max_concurrent_compactions = 8
full_compaction_max_concurrent = 2
```

## Command Line Options

```bash
mynadb -c <config_file>        # Specify config file (default: config.toml)
mynadb standalone              # Run in standalone mode (default)
mynadb meta start              # Start meta node (cluster mode)
mynadb data start              # Start data node (cluster mode)
mynadb router start            # Start router node (cluster mode)
mynadb admin start             # Start admin console (cluster mode)
mynadb version                 # Show version information
```

## Validation

The server validates the configuration on startup. If validation fails, it falls back to defaults and logs warnings.

## Runtime Changes

Configuration changes require a server restart. Future versions may support hot-reloading for non-critical settings.
