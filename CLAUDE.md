# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**MynaDB** - *The Voice of Your Machines*

MynaDB is a time series database written in Rust that ingests data via InfluxDB line protocol, stores it in Parquet format, and provides SQL queries via Apache DataFusion.

## Build and Run

```bash
# Build
cargo build --release

# Run (starts HTTP on 7000, Arrow Flight on 7001, MySQL on 7002)
cargo run --release

# Run with custom config
cargo run --release -- -c config.production.toml

# Run tests
cargo test

# Run specific module tests (uses substring matching)
cargo test --lib cache
cargo test --lib compactor
cargo test --lib wal

# Run benchmarks
cargo bench --bench line_protocol_bench
cargo bench --bench query_bench
cargo bench --bench time_functions_bench

# Format code (project uses rustfmt)
cargo fmt

# Create release archive
./build.sh
```

## Source Code Structure

```
src/
├── main.rs          # Entry point, server initialization
├── config.rs        # Configuration structs and loading
├── aggregates/      # Continuous Aggregates - materialized views with auto-refresh
│   ├── config.rs        # Feature configuration
│   ├── definition.rs    # Aggregate definition with builder pattern
│   ├── job.rs           # Background refresh job
│   ├── query_rewriter.rs # Automatic query rewriting
│   ├── storage.rs       # Definition persistence
│   └── types.rs         # Core types (state, watermark, results)
├── services/        # API Layer - HTTP, Flight, MySQL, auth
├── storage/         # Write Path - line protocol, cache, WAL, parquet
├── query/           # Read Path - DataFusion integration, optimizers
│   ├── analyzers/       # Query semantic analysis rules
│   ├── cache/           # Metadata and query result caching
│   ├── catalog/         # Schema/table metadata management
│   ├── functions/       # Custom UDFs (time_bucket, first_value, last_value)
│   ├── logical_optimizers/   # Logical plan optimizations
│   ├── physical_optimizers/  # Physical plan optimizations
│   ├── providers/       # TableProvider implementations (Parquet, memtable, union)
│   ├── timeseries/      # Time series utilities (GROUP BY time, fill)
│   ├── query_engine.rs  # Main query orchestrator
│   └── executor.rs      # Custom execution pipeline
└── jobs/            # Background Jobs - compaction, retention
```

### Module Responsibilities

| Directory | Key Files | Purpose |
|-----------|-----------|---------|
| `aggregates/` | `definition.rs`, `job.rs`, `query_rewriter.rs` | Continuous aggregates (materialized views) |
| `services/` | `api.rs`, `flight_service.rs`, `mysql_service.rs` | External interfaces (REST, gRPC, MySQL wire) |
| `storage/` | `cache.rs`, `wal.rs`, `line_protocol.rs`, `parquet_writer.rs` | Write path, data persistence |
| `query/` | `query_engine.rs`, `executor.rs` | SQL execution, DataFusion integration |
| `query/analyzers/` | `timestamp_to_date.rs` | Query semantic analysis rules |
| `query/cache/` | `metadata_cache.rs`, `query_cache.rs` | Table metadata and query result caching |
| `query/catalog/` | `manager.rs`, `memory_manager.rs` | Metadata management |
| `query/functions/` | `time_bucket.rs`, `first_last.rs` | Custom UDFs/UDAFs |
| `query/logical_optimizers/` | `partition_filter_pushdown.rs` | Logical plan optimizations |
| `query/physical_optimizers/` | `scan_config.rs` | Physical plan optimizations |
| `query/providers/` | `table_provider.rs`, `memtable_provider.rs`, `union_provider.rs` | TableProvider implementations |
| `query/timeseries/` | `time_interval.rs`, `group_by_time.rs`, `fill_processor.rs` | Time series query processing |
| `jobs/` | `compactor.rs`, `retention_manager.rs` | Background maintenance |
| `cluster/` | `coordinator/`, `node/`, `election.rs` | Enterprise cluster mode (etcd-based) |

### Adding New Code

| Type | Location |
|------|----------|
| New API endpoint | `services/api.rs` + route in `main.rs` |
| New protocol | `services/` |
| Query optimizer rule | `query/analyzers/` or `query/logical_optimizers/` |
| Custom UDF/UDAF | `query/functions/` |
| TableProvider | `query/providers/` |
| Time series utility | `query/timeseries/` |
| Query/metadata cache | `query/cache/` |
| Background task | `jobs/` |
| Continuous aggregate | `aggregates/` |
| Configuration option | `config.rs` |
| Cluster coordination | `cluster/` |

## Architecture

### Data Flow

```
HTTP POST /write/:db → Line Protocol Parser → WAL → DataCache (in-memory)
    → Background Flush → Parquet Files (L0)
    → Compactor → Merged Files (L1, L2, ...)

HTTP GET /query/:db or Arrow Flight or MySQL → QueryEngine (DataFusion) → Parquet Files → Results
```

### Storage Structure (Catalog/Schema/Table)

The storage structure depends on the `schema.strategy` configuration:

**Strategy 1: ServiceAsSchema (default)**
```
data/
└── {catalog}/              # Database name from URI /write/{db}
    ├── {schema}/           # Extracted from "service" tag (or "default")
    │   └── {table}/        # Measurement name
    │       └── date=YYYY-MM-DD/
    │           ├── L0_*.parquet    # Recent writes
    │           ├── L1_*.parquet    # First compaction level
    │           └── L2_*.parquet    # Higher compaction levels
    └── _system/
        └── series_stats/
```

**Strategy 2: ServiceAsTable**
```
data/
└── {catalog}/              # Database name from URI /write/{db}
    ├── default/            # Fixed schema name
    │   └── {table}/        # Extracted from "service" tag (or "default")
    │       └── date=YYYY-MM-DD/
    │           └── *.parquet
    └── _system/
```

**Naming Rules:**
- Numeric service tags are prefixed with `service_` (e.g., `service=1` → `service_1`)
- Text values remain unchanged
- No service tag → uses `default_name` from config
- In ServiceAsTable mode, measurement name is stored as a `measurement` tag

### Key Configuration (config.toml)

```toml
[server]
http_port = 7000
flight_port = 7001
mysql_enabled = true     # Enable MySQL wire protocol
mysql_port = 7002

[storage]
data_dir = "/data0/mynadb/data"

# Object store: "local", "s3", "minio", "oss"
[storage.object_store]
backend = "local"
# bucket = "my-bucket"   # For S3/MinIO/OSS
# endpoint = "http://..."  # Custom endpoint

[schema]
strategy = "service_as_schema"  # or "service_as_table"
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]
default_name = "default"
prefix_numeric = true

[cache]
flush_interval_secs = 60
max_size_mb = 4096

[wal]
enabled = true
sync_on_write = false    # true=durable, false=faster
rotate_size_mb = 128

[query_cache]
enabled = true
max_size_mb = 256
ttl_secs = 300
min_query_time_ms = 10   # Only cache slow queries

[compaction]
min_files = 5
max_level = 5
max_concurrent_compactions = 8
full_compaction_max_concurrent = 2

[retention]
enabled = true
default_days = 7
interval_secs = 3600

[auth]
enabled = false
username = "admin"
password = "secret"

[continuous_aggregates]
enabled = false              # Enable continuous aggregates feature
refresh_check_interval_secs = 60
max_concurrent_refreshes = 4
query_rewriting_enabled = true

[logging]
level = "info"
log_dir = "/data0/mynadb/logs"

# Enterprise cluster mode (optional)
[cluster]
enabled = false
# See docs/architecture-cluster.md for full cluster configuration
```

## Cluster Mode (Enterprise)

MynaDB supports a distributed cluster mode using shared object storage (S3/MinIO/OSS) and etcd for coordination. Key characteristics:
- **Compute-storage separation**: Stateless compute nodes, shared S3 storage
- **Partition-level locking**: Writes coordinated via etcd distributed locks
- **Cache invalidation**: Pub/sub for cross-node cache consistency
- **Leader election**: Single node runs background tasks (compaction, retention)

See `docs/architecture-cluster.md` for full architecture details and `src/cluster/` for implementation.

## Code Standards

- **No hard-coded strings**: Use constants for paths, tag names, file extensions
- **Use PathBuf**: Always use `Path::new().join()` instead of format strings for file paths
- **Single source of truth**: Define constants at module top or in a constants module
- **Helper methods**: Extract repeated path construction into methods like `get_stats_file_path()`

## API Endpoints

### Data Operations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/write/:db` | POST | Write data (InfluxDB line protocol) |
| `/query/:db` | GET/POST | SQL queries |
| `/explain/:db` | GET/POST | Query execution plan |
| `/analyze/:db` | GET/POST | Query analysis (partitions, files) |
| `/flush` | POST | Manual cache flush |

### Schema & Metadata
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tables/:db` | GET | List tables |
| `/schema/:db/:table` | GET | Get table schema |
| `/partitions/:catalog/:schema/:table` | GET | List partitions |
| `/partitions/:catalog/:schema/:table/:date` | DELETE | Delete partition |

### Maintenance
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/compact/:db[/:measurement]` | POST | Trigger compaction |
| `/retention/:catalog/:schema/:table` | POST | Delete old partitions |
| `/cache/stats` | GET | Cache statistics |
| `/cache/prewarm/:catalog` | POST | Pre-warm metadata cache |
| `/cache/clear` | POST | Clear all caches |

### Monitoring & Debug
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/stats` | GET | Write cache statistics |
| `/memory` | GET | Memory usage stats |
| `/debug/heap` | GET | Heap profile |
| `/debug/cpu` | GET | CPU profile |

## Query Performance Optimizations

The query engine is configured with the following optimizations enabled by default:

### Predicate Push-down
- **Parquet pushdown filters**: Filter predicates are pushed down to the Parquet reader
- **Filter reordering**: Filters are reordered for optimal execution
- **Row group pruning**: Uses Parquet statistics to skip row groups that don't match predicates
- **Page-level pruning**: Uses page index for finer-grained filtering
- **Bloom filter**: Uses Bloom filters for string column filtering (if available in Parquet files)

### EXPLAIN Endpoint
```bash
# Get execution plan for a query
curl "http://localhost:7000/explain/mydb?sql=SELECT%20*%20FROM%20cpu%20WHERE%20time%20%3E%201704067200000000000&verbose=true"

# Response includes:
# - logical_plan: DataFusion logical plan
# - physical_plan: Physical execution plan showing pushdown
# - optimizations: Which optimizations are applied
```

### ANALYZE Endpoint
```bash
# Get query analysis with partition and file counts
curl "http://localhost:7000/analyze/mydb?sql=SELECT%20*%20FROM%20cpu%20WHERE%20date%20%3E%3D%20'2024-01-10'"

# Response includes:
# - tables: Referenced tables
# - time_range: Extracted time/date filters
# - partitions_to_scan: Number of partitions after pruning
# - files_to_scan: Number of Parquet files to read
# - optimizations: Applied optimizations
# - plan_summary: Brief execution summary
```

## Retention Policy

MynaDB automatically deletes old date-partitioned data to manage disk space.

### Configuration

```toml
[retention]
enabled = true                    # Enable background retention checks
default_days = 7                  # Delete partitions older than 7 days
interval_secs = 3600              # Check every hour
exclude_databases = ["_system"]   # Databases to skip (optional)
exclude_tables = ["mydb.default.important"]  # Tables to skip (optional)
```

### How It Works

1. Background task runs every `interval_secs`
2. Scans all catalogs → schemas → tables in `data_dir`
3. For each table, discovers date partitions (`date=YYYY-MM-DD/`)
4. Deletes partitions older than `default_days`
5. Skips excluded databases and tables
6. Logs deletion statistics (partitions deleted, bytes freed)

### Manual Retention

Use the API to manually trigger retention for specific tables:

```bash
# Delete partitions older than 30 days
curl -X POST http://localhost:7000/retention/mydb/default/cpu \
  -H "Content-Type: application/json" \
  -d '{"older_than_days": 30}'

# Delete partitions before specific date
curl -X POST http://localhost:7000/retention/mydb/default/cpu \
  -H "Content-Type: application/json" \
  -d '{"before_date": "2024-01-01"}'
```

## Query Syntax

Query syntax depends on the schema/table strategy:

**ServiceAsSchema Mode:**
```sql
-- Fully qualified: catalog.schema.table
SELECT * FROM mydb.service_1.cpu

-- Schema.table (auto-prefixes catalog)
SELECT * FROM service_1.cpu

-- Simple name (auto-prefixes catalog.default.)
SELECT * FROM cpu
```

**ServiceAsTable Mode:**
```sql
-- Fully qualified: catalog.schema.table
SELECT * FROM mydb.default.service_1

-- Schema.table (auto-prefixes catalog)
SELECT * FROM default.service_1

-- Filter by measurement tag
SELECT * FROM mydb.default.service_1 WHERE measurement = 'cpu'
```

## Test Scripts

All test scripts are in `scripts/`:

| Script | Category | Description |
|--------|----------|-------------|
| `test_build.sh` | Build | Verify compilation |
| `test_write.sh` | Write/Schema | Test catalog.schema.table structure |
| `test_schemas.sh` | Write/Schema | Test schema/table listing APIs |
| `test_partitions.sh` | Partition | Partition management |
| `test_partition_query.sh` | Partition | Query partitions after restart |
| `test_compaction.sh` | Compaction | Basic leveled compaction |
| `test_compaction_multilevel.sh` | Compaction | Multi-level L0→L1→L2 |
| `test_compaction_dual_strategy.sh` | Compaction | LSM + Full compaction strategies |
| `test_auth.sh` | Auth | Basic authentication |
| `test_auth_comprehensive.sh` | Auth | Full auth test suite |
| `test_mysql.sh` | MySQL | MySQL wire protocol |
| `test_group_by_time.sh` | Query | Time bucket aggregations |
| `test_comprehensive.sh` | E2E | Full integration test |

Profiling scripts: `generate_flamegraph.sh`, `analyze_profile.sh`, `analyze_memory.sh`
