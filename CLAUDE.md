# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stats Service is a time series database written in Rust that ingests data via InfluxDB line protocol, stores it in Parquet format, and provides SQL queries via Apache DataFusion.

## Build and Run

```bash
# Build
cargo build --release

# Run (starts HTTP on port 7000, Arrow Flight on port 7001)
cargo run --release

# Run tests
cargo test

# Run specific module tests
cargo test --lib series_tracker
cargo test --lib cache
cargo test --lib compactor

# Create release archive
./build.sh
```

## Architecture

### Data Flow

```
HTTP POST /write/:db → Line Protocol Parser → DataCache (in-memory)
    → Background Flush (30s) → Parquet Files (L0)
    → Compactor → Merged Files (L1, L2, ...)

HTTP GET /query/:db or Arrow Flight → QueryEngine (DataFusion) → Parquet Files → Results
```

### Storage Structure (Catalog/Schema/Table)

The storage structure depends on the `schema.strategy` configuration:

**Strategy 1: ServiceAsSchema (default)**
```
data/
└── {catalog}/              # Database name from URI /write/{db}
    ├── {schema}/           # Extracted from "service" tag (or "default")
    │   └── {table}/        # Measurement name
    │       ├── L0_*.parquet    # Recent writes
    │       ├── L1_*.parquet    # First compaction level
    │       └── L2_*.parquet    # Higher compaction levels
    └── _system/
        └── series_stats/
            └── {catalog}.parquet
```

**Strategy 2: ServiceAsTable**
```
data/
└── {catalog}/              # Database name from URI /write/{db}
    ├── default/            # Fixed schema name
    │   └── {table}/        # Extracted from "service" tag (or "default")
    │       ├── L0_*.parquet    # Recent writes
    │       └── ...
    └── _system/
        └── series_stats/
            └── {catalog}.parquet
```

**Naming Rules:**
- Numeric service tags are prefixed with `service_` (e.g., `service=1` → `service_1`)
- Text values remain unchanged
- No service tag → uses `default_name` from config
- In ServiceAsTable mode, measurement name is stored as a `measurement` tag

### Core Modules

| Module | File | Purpose |
|--------|------|---------|
| HTTP Server | `main.rs` | REST API endpoints, Axum routing |
| Arrow Flight | `flight_service.rs` | gRPC streaming queries, DataFusion-cli compatible |
| Cache | `cache.rs` | In-memory buffering, background flush to Parquet |
| Query Engine | `query_engine.rs` | DataFusion SQL execution, table auto-registration |
| Compactor | `compactor.rs` | LSM-tree style multi-level compaction |
| Retention Manager | `retention_manager.rs` | Automatic deletion of old partitions |
| Series Tracker | `series_tracker.rs` | Cardinality tracking per database |
| Line Protocol | `line_protocol.rs` | InfluxDB line protocol parser |
| Parquet Writer | `parquet_writer.rs` | Arrow to Parquet file I/O |
| Config | `config.rs` | TOML configuration loading |
| Database Manager | `database_manager.rs` | Database CRUD operations |

### Key Configuration (config.toml)

```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data0/stats/data"

[schema]
strategy = "service_as_schema"  # "service_as_schema" (default) or "service_as_table"
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]  # Tags to extract service from
default_name = "default"
prefix_numeric = true  # "1" → "service_1"

[cache]
flush_interval_secs = 30

[compaction]
min_files = 4
max_level = 5

[retention]
enabled = true           # Enable automatic retention
default_days = 7         # Delete partitions older than 7 days
interval_secs = 3600     # Check every hour
```

## Schema/Table Naming Strategies

Stats Service supports two strategies for mapping InfluxDB line protocol to catalog.schema.table structure:

### Strategy 1: ServiceAsSchema (Default)

**Configuration:**
```toml
[schema]
strategy = "service_as_schema"
```

**Behavior:**
- Service tag value → Schema name
- Measurement name → Table name
- Direct mapping from InfluxDB semantics

**Example:**
```bash
# Line Protocol
cpu,service=1,host=server01 usage=23.5

# Storage Path
data/mydb/service_1/cpu/

# SQL Query
SELECT * FROM mydb.service_1.cpu
```

**Use Case:** Multiple services, each with multiple measurements. Each service gets its own schema.

### Strategy 2: ServiceAsTable

**Configuration:**
```toml
[schema]
strategy = "service_as_table"
```

**Behavior:**
- Fixed schema name (from `default_name` config)
- Service tag value → Table name
- Measurement name → Stored as `measurement` tag in data

**Example:**
```bash
# Line Protocol
cpu,service=1,host=server01 usage=23.5

# Storage Path
data/mydb/default/service_1/

# SQL Query
SELECT * FROM mydb.default.service_1 WHERE measurement = 'cpu'
```

**Use Case:** Service-centric view where each service is a separate table. Query by measurement using the `measurement` tag.

**Important Notes:**
- The `tag_keys` configuration works for both strategies (extracts service value)
- Numeric service values are prefixed with `service_` when `prefix_numeric = true`
- When service tag is missing, uses `default_name` for both schema and table
- In ServiceAsTable mode, original measurement name is preserved as a tag column

## Code Standards

- **No hard-coded strings**: Use constants for paths, tag names, file extensions
- **Use PathBuf**: Always use `Path::new().join()` instead of format strings for file paths
- **Single source of truth**: Define constants at module top or in a constants module
- **Helper methods**: Extract repeated path construction into methods like `get_stats_file_path()`

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/write/:db` | POST | Write data (InfluxDB line protocol) |
| `/query/:db` | GET/POST | SQL queries |
| `/compact/:db[/:measurement]` | POST | Trigger compaction |
| `/flush` | POST | Manual cache flush |
| `/stats` | GET | Cache statistics |
| `/tables/:db` | GET | List tables |
| `/schema/:db/:table` | GET | Get table schema |
| `/partitions/:catalog/:schema/:table` | GET | List partitions |
| `/partitions/:catalog/:schema/:table/:date` | DELETE | Delete specific partition |
| `/retention/:catalog/:schema/:table` | POST | Delete old partitions (body: `{older_than_days: N}` or `{before_date: "YYYY-MM-DD"}`) |
| `/health` | GET | Health check |

## Retention Policy

Stats Service automatically deletes old date-partitioned data to manage disk space.

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
| `test_partitions.sh` | Partition | Partition management features |
| `test_partition_query.sh` | Partition | Query partitions after restart |
| `test_compaction.sh` | Compaction | Basic leveled compaction |
| `test_compaction_multilevel.sh` | Compaction | Multi-level L0→L1→L2... |
| `test_compaction_nonpartitioned.sh` | Compaction | Non-partitioned tables |
| `test_compaction_aggressive.sh` | Compaction | Lower threshold compaction |
| `test_compaction_dual_strategy.sh` | Compaction | **NEW:** Comprehensive dual strategy test (LSM + Full) |
| `test_compaction_quick.sh` | Compaction | **NEW:** Quick validation of both strategies |
| `test_full_compaction_now.sh` | Compaction | **NEW:** Force full compaction at current hour |
| `test_series_stats.sh` | Series Stats | Cardinality tracking |
| `test_comprehensive.sh` | Comprehensive | Full test with queries |

See `scripts/README_COMPACTION_TESTS.md` for detailed compaction testing documentation.

