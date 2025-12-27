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

Schema naming: Numeric service tags are prefixed with `svc_` (e.g., `service=1` → `svc_1`). Text values remain unchanged. No service tag → `default` schema.

### Core Modules

| Module | File | Purpose |
|--------|------|---------|
| HTTP Server | `main.rs` | REST API endpoints, Axum routing |
| Arrow Flight | `flight_service.rs` | gRPC streaming queries, DataFusion-cli compatible |
| Cache | `cache.rs` | In-memory buffering, background flush to Parquet |
| Query Engine | `query_engine.rs` | DataFusion SQL execution, table auto-registration |
| Compactor | `compactor.rs` | LSM-tree style multi-level compaction |
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
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]  # Tags to extract schema from
default_name = "default"
prefix_numeric = true  # "1" → "svc_1"

[cache]
flush_interval_secs = 30

[compaction]
min_files = 4
max_level = 5
```

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
| `/health` | GET | Health check |

## Query Syntax

```sql
-- Fully qualified: catalog.schema.table
SELECT * FROM mydb.svc_1.cpu

-- Schema.table (auto-prefixes catalog)
SELECT * FROM svc_1.cpu

-- Simple name (auto-prefixes catalog.default.)
SELECT * FROM cpu
```
