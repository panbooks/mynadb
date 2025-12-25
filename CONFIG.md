# Configuration Guide

The stats-service uses TOML configuration files for flexible deployment across different environments.

## Configuration Files

- **`config.toml`** - Default development configuration
- **`config.production.toml`** - Production configuration example

## Loading Configuration

By default, the server loads `config.toml` from the current directory. You can specify a different config file using the `CONFIG_PATH` environment variable:

```bash
# Use default config.toml
./stats

# Use production config
CONFIG_PATH=config.production.toml ./stats

# Use custom config
CONFIG_PATH=/etc/stats-service/config.toml ./stats
```

If the config file is not found, the server will use built-in defaults.

## Configuration Sections

### [server]
HTTP and gRPC server settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `http_host` | string | `"0.0.0.0"` | HTTP API bind address |
| `http_port` | u16 | `7000` | HTTP API port |
| `flight_host` | string | `"0.0.0.0"` | Arrow Flight gRPC bind address |
| `flight_port` | u16 | `7001` | Arrow Flight gRPC port |

**Example:**
```toml
[server]
http_host = "0.0.0.0"
http_port = 7000
flight_host = "0.0.0.0"
flight_port = 7001
```

### [storage]
Data storage settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data_dir` | string | `"./data"` | Data directory path |
| `system_dir` | string | `"_system"` | System metadata directory name |

**Example:**
```toml
[storage]
data_dir = "/data0/data"  # Use absolute path in production
system_dir = "_system"
```

**Recommended Production Settings:**
- Use SSD/NVMe storage for best performance
- Mount dedicated disk for data directory
- Use absolute paths

### [schema]
Schema extraction configuration - determines how to extract schema names from metric tags

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tag_key` | string | `"service"` | Tag key to extract schema from |
| `default_name` | string | `"default"` | Default schema when tag is absent |
| `prefix_numeric` | bool | `true` | Prefix numeric schemas with "service_" |
| `extract_last_part` | bool | `false` | Extract last part after separator |
| `separator` | string | `"."` | Separator for hierarchical names |

**Example:**
```toml
[schema]
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
separator = "."
```

**Use Cases:**

**1. Simple Service Tags:**
```toml
[schema]
tag_key = "service"
extract_last_part = false
```
- Tag `service="web"` → Schema: `web`
- Tag `service="api"` → Schema: `api`

**2. Hierarchical Service Tags (Extract Last Part):**
```toml
[schema]
tag_key = "service"
extract_last_part = true
separator = "."
```
- Tag `service="MONITOR.web"` → Schema: `web`
- Tag `service="PROD.api"` → Schema: `api`

**3. Custom Tag Keys:**
```toml
[schema]
tag_key = "hierarchy_2"
extract_last_part = false
```
- Tag `hierarchy_2="service_1"` → Schema: `service_1`
- Tag `hierarchy_2="service_2"` → Schema: `service_2`

**4. Numeric Service IDs:**
```toml
[schema]
tag_key = "service"
prefix_numeric = true
```
- Tag `service="123"` → Schema: `service_123` (avoids SQL parser issues)
- Tag `service="web"` → Schema: `web`

### [cache]
In-memory cache settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flush_interval_secs` | u64 | `30` | Background flush interval (seconds) |
| `max_size_mb` | u64 | `1024` | Max cache size before forcing flush (MB) |

**Example:**
```toml
[cache]
flush_interval_secs = 30
max_size_mb = 2048  # 2GB cache
```

**Tuning Guidelines:**
- **Low Latency**: Smaller `flush_interval_secs` (10-20s), smaller cache
- **High Throughput**: Larger `flush_interval_secs` (60-120s), larger cache (2-4GB)
- Monitor memory usage and adjust `max_size_mb` accordingly

### [compaction]
Parquet file compaction settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_files` | usize | `5` | Minimum files to trigger compaction |
| `max_file_size_mb` | u64 | `100` | Max file size before compaction (MB) |
| `target_file_size_mb` | u64 | `500` | Target size after compaction (MB) |
| `max_level` | u32 | `5` | Maximum compaction levels |

**Example:**
```toml
[compaction]
min_files = 10
max_file_size_mb = 200
target_file_size_mb = 1024  # 1GB target files
max_level = 7
```

**Tuning Guidelines:**
- **Write-Heavy**: Higher `min_files` (10-20), larger `target_file_size_mb` (1-2GB)
- **Read-Heavy**: Lower `min_files` (5-10), moderate `target_file_size_mb` (500MB-1GB)
- More levels = better compression, slower queries

### [series_tracker]
Series cardinality tracking settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flush_interval_secs` | u64 | `60` | Stats flush interval (seconds) |
| `stats_dir` | string | `"series_stats"` | Stats directory name |

**Example:**
```toml
[series_tracker]
flush_interval_secs = 60
stats_dir = "series_stats"
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
system_dir = "_system"

[schema]
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
separator = "."

[cache]
flush_interval_secs = 30
max_size_mb = 1024

[compaction]
min_files = 5
max_file_size_mb = 100
target_file_size_mb = 500
max_level = 5

[series_tracker]
flush_interval_secs = 60
stats_dir = "series_stats"
```

### Production (config.production.toml)
```toml
[server]
http_host = "0.0.0.0"
http_port = 7000
flight_host = "0.0.0.0"
flight_port = 7001

[storage]
data_dir = "/data0/data"  # SSD/NVMe mount
system_dir = "_system"

[schema]
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
separator = "."

[cache]
flush_interval_secs = 30
max_size_mb = 4096  # 4GB cache

[compaction]
min_files = 15
max_file_size_mb = 256
target_file_size_mb = 2048  # 2GB files
max_level = 7

[series_tracker]
flush_interval_secs = 120  # 2 minutes
stats_dir = "series_stats"
```

### High-Throughput Ingest
```toml
[cache]
flush_interval_secs = 60  # Batch more writes
max_size_mb = 8192  # 8GB cache

[compaction]
min_files = 20  # Less frequent compaction
max_file_size_mb = 512
target_file_size_mb = 4096  # 4GB files
```

### Low-Latency Queries
```toml
[cache]
flush_interval_secs = 15  # Faster data availability
max_size_mb = 512

[compaction]
min_files = 5  # Keep files small
max_file_size_mb = 100
target_file_size_mb = 512
```

## Environment Variables

- **`CONFIG_PATH`**: Path to configuration file (default: `config.toml`)

Example:
```bash
export CONFIG_PATH=/etc/stats-service/config.toml
./stats
```

## Validation

The server validates the configuration on startup. If validation fails, it falls back to defaults and logs warnings.

## Runtime Changes

Configuration changes require a server restart. Future versions may support hot-reloading for non-critical settings.
