# CLAUDE_CONTEXT.md

This document provides a detailed technical overview of the Stats Service (MynaDB) codebase for AI assistants working on this project.

## Project Overview

Stats Service is a time-series database written in Rust that:
- Ingests data via InfluxDB line protocol over HTTP
- Stores data in Parquet format with date-based partitioning
- Provides SQL queries via Apache DataFusion and Arrow Flight
- Supports InfluxDB-compatible GROUP BY time() syntax

## Architecture

### High-Level Data Flow

```
HTTP POST /write/:db
    → Line Protocol Parser (line_protocol.rs)
    → DataCache (cache.rs, in-memory buffer)
    → Background Flush (30s interval)
    → Parquet Files (L0)
    → Compactor (LSM-tree style)
    → Merged Files (L1, L2, ...)

HTTP GET /query/:db or Arrow Flight
    → QueryEngine (query_engine.rs)
    → GROUP BY time() Transformer (group_by_time.rs)
    → DataFusion SQL Execution
    → Fill Post-Processing (fill_processor.rs)
    → Results
```

### Directory Structure

```
src/
├── main.rs                 # Axum HTTP server, route definitions
├── config.rs               # TOML configuration loading
├── jobs/
│   ├── compactor.rs        # LSM-tree style multi-level compaction
│   ├── database_manager.rs # Database CRUD operations
│   ├── retention_manager.rs# Automatic partition deletion
│   ├── profiling.rs        # CPU profiling (pprof)
│   └── logger.rs           # Logging configuration
├── query/
│   ├── query_engine.rs     # Main query execution, table registration
│   ├── executor.rs         # Custom DataFusion executor with rules
│   ├── table_provider.rs   # Custom Parquet table provider
│   ├── query_cache.rs      # Query result caching
│   ├── group_by_time.rs    # InfluxDB GROUP BY time() transformer
│   ├── time_interval.rs    # Duration parsing (5m, 1h, etc.)
│   ├── fill_processor.rs   # Gap-filling for time series
│   ├── functions/
│   │   └── time_bucket.rs  # time_bucket() UDF
│   ├── analyzers/
│   │   └── timestamp_to_date.rs  # Timestamp to date conversion
│   ├── logical_optimizers/
│   │   └── partition_filter_pushdown.rs
│   └── physical_optimizers/
│       └── scan_config.rs
├── services/
│   ├── api.rs              # HTTP API handlers
│   ├── flight_service.rs   # Arrow Flight gRPC service
│   ├── mysql_service.rs    # MySQL wire protocol (partial)
│   ├── auth.rs             # Authentication
│   └── rate_limiter.rs     # Token bucket rate limiting
└── storage/
    ├── storage.rs          # Storage abstraction (local/S3)
    ├── cache.rs            # In-memory write buffer (sharded)
    ├── line_protocol.rs    # InfluxDB line protocol parser
    ├── parquet_writer.rs   # Arrow to Parquet conversion
    └── wal.rs              # Write-ahead log
```

### Storage Layout

```
data/
└── {catalog}/                    # Database name from /write/{db}
    ├── {schema}/                 # Service name (or "default")
    │   └── {table}/              # Measurement name
    │       └── date=YYYY-MM-DD/  # Date partition
    │           ├── L0_*.parquet  # Recent writes
    │           ├── L1_*.parquet  # First compaction
    │           └── L2_*.parquet  # Higher levels
    └── _system/
        └── series_stats/
            └── {catalog}.parquet # Cardinality tracking
```

## Key Components

### QueryEngine (query_engine.rs)

The central query orchestrator with:
- **TableMetadataCache**: Caches table info, file listings, Parquet metadata
- **QueryResultCache**: LRU cache for query results
- **Table Registration**: Auto-registers tables from Parquet files
- **Query Pipeline**:
  1. Check query cache
  2. Extract and register table references
  3. Qualify table names with catalog.schema
  4. Inject date predicates for partition pruning
  5. Transform GROUP BY time() syntax
  6. Execute via DataFusion
  7. Apply fill post-processing
  8. Cache results

Key methods:
- `execute_query_with_db()` - Main entry point
- `register_table()` - Registers Parquet tables with DataFusion
- `qualify_table_names()` - Adds catalog.schema prefix
- `inject_date_predicates()` - Converts timestamp filters to date filters

### DataCache (cache.rs)

Sharded in-memory buffer for writes:
- 16 shards for concurrent access
- Background flush every 30 seconds
- WAL support for durability
- Points organized by catalog/schema/table/date

### Compactor (compactor.rs)

LSM-tree style compaction:
- L0 files from flush → L1 → L2 → ... → L5 (max)
- Configurable thresholds (min_files, target_size)
- Full compaction at midnight for yesterday's data
- Per-partition locking to prevent concurrent compaction
- Rate limiting via semaphores

### Line Protocol Parser (line_protocol.rs)

Parses InfluxDB line protocol:
```
measurement,tag1=value1,tag2=value2 field1=1.0,field2="str" timestamp
```

Features:
- Parallel parsing for large payloads (rayon)
- Schema strategy support (ServiceAsSchema vs ServiceAsTable)
- Numeric prefix handling for service tags

### GROUP BY time() Support

New feature implemented across multiple files:

**time_interval.rs**: Parses duration literals
- Supported: `ns`, `us`, `ms`, `s`, `m`, `h`, `d`, `w`
- FillOption: `null`, `none`, `previous`, `linear`, `<number>`

**group_by_time.rs**: SQL transformation
- Detects `GROUP BY time(interval[, offset])` syntax
- Maps InfluxDB functions to DataFusion (mean→avg, etc.)
- Extracts fill() clause for post-processing

**functions/time_bucket.rs**: Vectorized UDF
- `time_bucket(timestamp, interval_nanos, offset_nanos)`
- Aligns to round-number boundaries

**fill_processor.rs**: Gap-filling
- Inserts missing time buckets
- Supports grouped data (multiple series)

## DataFusion Integration

### Custom Rules

The query pipeline uses custom DataFusion rules:

**Analyzer Rules** (timestamp_to_date.rs):
- Converts timestamp comparisons to date partition filters

**Logical Optimizer Rules** (partition_filter_pushdown.rs):
- Pushes partition filters down for pruning

**Physical Optimizer Rules** (scan_config.rs):
- Configures Parquet scan settings

### Session Configuration

Optimized settings in `create_optimized_context()`:
- Predicate pushdown enabled
- Row group/page pruning enabled
- Bloom filter on read
- Parallel partition processing
- Memory limits with disk spilling

### Custom Table Provider

`ParquetTableProvider` provides:
- Automatic file discovery (flat or partitioned)
- Schema inference from Parquet files
- Partition pruning based on date predicates
- File metadata caching

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/write/:db` | POST | Write data (InfluxDB line protocol) |
| `/query/:db` | GET/POST | SQL queries |
| `/explain/:db` | GET/POST | Query execution plan |
| `/analyze/:db` | GET/POST | Query analysis |
| `/compact/:db[/:measurement]` | POST | Trigger compaction |
| `/flush` | POST | Manual cache flush |
| `/stats` | GET | Write cache statistics |
| `/tables/:db` | GET | List tables |
| `/schema/:db/:table` | GET | Get table schema |
| `/partitions/:catalog/:schema/:table` | GET | List partitions |
| `/retention/:catalog/:schema/:table` | POST | Delete old partitions |
| `/cache/stats` | GET | Cache statistics |
| `/cache/clear` | POST | Clear all caches |
| `/health` | GET | Health check |

## Dependencies

Key dependencies from Cargo.toml:
- **datafusion 43**: SQL query engine
- **arrow 53**: Columnar data format
- **parquet 53**: File format
- **arrow-flight 53**: gRPC streaming
- **axum 0.7**: HTTP framework
- **tokio**: Async runtime
- **chrono**: Date/time handling
- **regex**: Pattern matching (for GROUP BY time())
- **rayon**: Parallel processing

## Configuration

Main config file: `config.toml`

```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data0/stats/data"

[schema]
strategy = "service_as_schema"  # or "service_as_table"
tag_keys = ["service", "hierarchy_2"]
default_name = "default"

[cache]
flush_interval_secs = 30

[query_cache]
enabled = true
max_size_mb = 256
ttl_secs = 300

[compaction]
min_files = 4
max_level = 5

[retention]
enabled = true
default_days = 7
```

## Testing

Run tests with:
```bash
cargo test                          # All tests
cargo test --lib time_interval      # Specific module
cargo test --lib group_by_time      # GROUP BY time() tests
```

Test scripts in `scripts/`:
- `test_write.sh` - Write operations
- `test_compaction.sh` - Compaction
- `test_group_by_time.sh` - GROUP BY time() queries

## Common Patterns

### Error Handling
- Functions return `Result<T, String>` for simple errors
- DataFusion errors wrapped with context messages

### Caching Strategy
- Metadata cache with TTL (5 minutes)
- Query result cache with LRU eviction
- File listing cache (60 seconds)
- Parquet metadata cache (size-based eviction)

### Concurrency
- `parking_lot::RwLock` for synchronous locks
- `tokio::sync::RwLock` for async locks
- Sharded data structures for write throughput
- Semaphores for compaction rate limiting

## Recent Changes

The GROUP BY time() feature was recently added, introducing:
- New modules: `time_interval`, `group_by_time`, `fill_processor`, `functions/time_bucket`
- Integration in `query_engine.rs` execute path
- UDF registration in session context creation
