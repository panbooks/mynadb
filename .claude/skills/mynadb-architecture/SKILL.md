---
name: mynadb-architecture
description: This skill should be used when the user asks about MynaDB's architecture, data flow, module responsibilities, how components connect, where to add new code, or how the system works internally. Also use when planning new features or debugging issues that require understanding the system design.
version: 1.0.0
---

# MynaDB Architecture Knowledge

## System Overview

MynaDB is a time-series database written in Rust (~35K+ lines) that ingests via InfluxDB line protocol, stores in Parquet format, and queries via Apache DataFusion.

## Deployment Modes

- **Standalone** (`src/node/standalone.rs`): All-in-one single process (default)
- **Router** (`src/node/router.rs`): Client protocol endpoints + query routing
- **Data** (`src/node/data.rs`): Storage + query execution
- **Meta** (`src/meta/`): Raft-based metadata coordination
- **Admin** (`src/node/admin.rs`): Admin console

## Data Flow

### Write Path
```
HTTP POST /write/:db (InfluxDB line protocol)
  → src/services/api.rs::handle_write()
  → src/storage/line_protocol.rs::parse_line_protocol() → Vec<Point>
  → src/storage/wal.rs::Wal::append() (if enabled)
  → src/storage/cache/mod.rs::DataCache::add_points()
      → hash to 1-of-32 shards, lock-free append
  → [periodic 60s flush OR 80% size threshold]
  → src/storage/parquet_writer.rs::write_batches_partitioned()
      → L0_{timestamp}.parquet files
  → metadata_manager.register_file()
  → cache invalidation
```

### Query Path
```
HTTP GET /query/:db?sql=...
  → src/services/api.rs::handle_query()
  → check query_cache (if enabled, TTL-based)
  → src/query/query_engine/mod.rs::QueryEngine::execute_query_with_db()
      → register memtable from DataCache (for unflushed data)
      → discover Parquet files (S3 LIST + MANIFEST filter)
      → DataFusion SessionContext::sql()
          → analyzers → logical optimizers → physical optimizers
          → ParquetTableProvider / MemtableProvider / UnionTableProvider
          → predicate pushdown, bloom filters, row group pruning
      → collect RecordBatches
  → cache result (if eligible: > min_query_time_ms)
  → format JSON response
```

### Compaction Path
```
Background timer (interval_secs)
  → src/jobs/compactor/background.rs (LSM) or full.rs (date-based)
  → acquire lock (in-process or etcd distributed)
  → list files from partition prefix
  → filter by MANIFEST.json (skip superseded)
  → group by level: L0 → L1 (minor), L1 → L2 (major), etc.
  → read + merge + cardinality-aware sort
  → write output file (next level)
  → mark source files in MANIFEST as superseded
  → delete source files
  → update metadata
```

## Key Structs

| Struct | File | Purpose |
|--------|------|---------|
| `Config` | `src/config.rs` | All configuration (1600 lines) |
| `AppState` | `src/services/api.rs` | Shared request context |
| `Point` | `src/storage/line_protocol.rs` | Single data point (tags + fields + time) |
| `DataCache` | `src/storage/cache/mod.rs` | 32-shard write memtable |
| `QueryEngine` | `src/query/query_engine/mod.rs` | DataFusion wrapper |
| `Compactor` | `src/jobs/compactor/mod.rs` | LSM compaction |
| `Storage` | `src/storage/storage.rs` | ObjectStore abstraction |
| `DataLayout` | `src/storage/layout.rs` | Directory structure |
| `MetadataManager` | `src/query/catalog/manager.rs` | Async metadata trait |

## Storage Layout

```
{data_dir}/
  store/{catalog}/{schema}/{table}/date=YYYY-MM-DD/node_id=X/
    L0_*.parquet, L1_*.parquet, L2_*.parquet, MANIFEST.json
  wal/
  meta/catalog_snapshot.json
  cache/
  system/
  index/
```

## Where to Add New Code

| Type | Location | Pattern Reference |
|------|----------|-------------------|
| API endpoint | `src/services/api.rs` + route in router builder | Existing handlers |
| New protocol | `src/services/{protocol}/` | `src/services/prometheus/` |
| Query optimizer | `src/query/analyzers/` or `src/query/logical_optimizers/` | `timestamp_to_date.rs` |
| Custom UDF/UDAF | `src/query/functions/` | `time_bucket.rs`, `first_last.rs` |
| TableProvider | `src/query/providers/` | `table_provider.rs` |
| Background job | `src/jobs/` | `retention_manager.rs` |
| Config option | `src/config.rs` + `config.toml` | Existing sections |
| Feature module | `src/{feature}/` | `src/aggregates/`, `src/jobs/downsampling/` |

## Configuration Sections

Server, Storage, Schema, Cache, WAL, QueryCache, HotPartitionCache, Compaction, Retention, Auth, Roles, Audit, ContinuousAggregates, Downsampling, Compression, Streaming, TLS, SystemMetrics, Logging, Cluster.

## Protocols

| Protocol | Port | File |
|----------|------|------|
| HTTP REST | 7000 | `src/services/api.rs` |
| Arrow Flight gRPC | 7001 | `src/services/flight_service.rs` |
| MySQL wire | 7002 | `src/services/mysql_service.rs` |
| Prometheus remote write/read | 7000 | `src/services/prometheus/` |
| InfluxQL (Grafana compat) | 7000 | `src/query/influxql/` |
