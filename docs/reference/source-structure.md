# Source Code Structure

MynaDB source code is organized into logical modules under `src/`.

## Overview

```
src/
├── main.rs              # Entry point, CLI parsing, server initialization
├── config.rs            # Configuration structs and TOML loading
├── lib.rs               # Library crate exports
├── flight_protocol.rs   # Arrow Flight protocol helpers
├── admin/               # Admin Console - web UI and cluster management API
├── aggregates/          # Continuous Aggregates - materialized views with auto-refresh
├── auth/                # Authentication and RBAC
├── data/                # Data Node - storage, query execution (cluster mode)
├── iceberg/             # Apache Iceberg table format integration
├── jobs/                # Background Jobs - compaction, retention, downsampling
├── meta/                # Meta Node - Raft consensus, metadata service (cluster mode)
├── metrics/             # Internal metrics collection
├── node/                # Shared node infrastructure (standalone + cluster)
├── observability/       # Tracing, OpenTelemetry, profiling
├── proto/               # Protobuf definitions for gRPC services
├── query/               # Read Path - DataFusion integration, optimizers, UDFs
├── router/              # Router Node - client protocols, query routing (cluster mode)
├── services/            # API Layer - HTTP, Flight, MySQL, OTLP, Grafana
├── storage/             # Write Path - line protocol, cache, WAL, Parquet
└── utils/               # Shared utilities
```

## Module Details

### Core (`main.rs`, `config.rs`)

| File | Purpose |
|------|---------|
| `main.rs` | CLI (clap), subcommands: standalone, meta, data, router, admin |
| `config.rs` | All configuration structs with serde defaults |
| `lib.rs` | Library crate re-exports |

### Services (`services/`)

External-facing interfaces and middleware.

| File/Dir | Purpose |
|----------|---------|
| `api.rs` | HTTP REST API handlers (write, query, explain, analyze, flush, compact, etc.) |
| `flight_service.rs` | Arrow Flight gRPC service for high-performance queries |
| `mysql_service.rs` | MySQL wire protocol compatibility layer |
| `auth.rs` | Basic authentication middleware |
| `rate_limiter.rs` | Token bucket rate limiting |
| `grafana.rs` | Grafana-compatible InfluxDB endpoints |
| `tls.rs` | TLS/SSL configuration |
| `shared.rs` | Shared service utilities |
| `otlp/` | OpenTelemetry Protocol (OTLP) ingestion (gRPC + HTTP) |
| `prometheus/` | Prometheus metrics endpoint |
| `dashboard/` | Built-in monitoring dashboard |

### Storage (`storage/`)

Write path components for data ingestion and persistence.

| File/Dir | Purpose |
|----------|---------|
| `line_protocol.rs` | InfluxDB line protocol parser |
| `cache/` | In-memory write cache with background flush |
| `wal.rs` | Write-ahead log for durability |
| `storage.rs` | Storage abstraction (local filesystem, S3, MinIO, OSS) |
| `parquet_writer.rs` | Arrow RecordBatch to Parquet file conversion |
| `batch_converter.rs` | Record batch format conversion |
| `compression.rs` | Adaptive compression codec selection |
| `layout.rs` | Storage directory layout and path construction |
| `manifest.rs` | Compaction manifest tracking |
| `file_cache/` | Local disk cache for Parquet files (cluster mode) |

**Data Flow:**
```
HTTP POST /write → line_protocol → WAL → cache → parquet_writer → storage
```

### Query (`query/`)

Read path components for SQL query execution.

| File/Dir | Purpose |
|----------|---------|
| `query_engine/` | Main query execution, table registration, DataFusion integration |
| `executor.rs` | Custom SessionState with analyzer/optimizer rules |
| `streaming.rs` | Streaming query subscriptions (WebSocket/SSE) |
| `sql_utils.rs` | SQL parsing utilities |
| `types.rs` | Query result types |
| `analyzers/` | Semantic analysis rules (e.g., timestamp→date conversion) |
| `cache/` | Metadata cache and query result cache |
| `catalog/` | Schema/table metadata management |
| `functions/` | Custom UDFs (time_bucket, first_value, last_value, forecasting) |
| `influxql/` | InfluxQL compatibility layer |
| `logical_optimizers/` | Logical plan optimization (e.g., partition pushdown) |
| `physical_optimizers/` | Physical plan optimization (e.g., scan config) |
| `providers/` | TableProvider implementations (Parquet, memtable, union) |
| `timeseries/` | Time series utilities (GROUP BY time, fill, intervals) |

**Query Pipeline:**
```
SQL → Parse → Analyze → Logical Optimize → Physical Plan → Physical Optimize → Execute
```

### Jobs (`jobs/`)

Background maintenance tasks.

| File/Dir | Purpose |
|----------|---------|
| `compactor/` | LSM-tree style multi-level Parquet compaction |
| `retention_manager.rs` | Automatic deletion of old date partitions |
| `database_manager.rs` | Database/schema/table CRUD operations |
| `downsampling/` | Automated data downsampling |
| `recompression.rs` | Adaptive recompression of cold data |
| `logger.rs` | Tracing/logging initialization |
| `profiling.rs` | Heap and CPU profiling endpoints |

### Aggregates (`aggregates/`)

Continuous aggregates (materialized views with automatic refresh).

| File | Purpose |
|------|---------|
| `config.rs` | Feature configuration |
| `definition.rs` | Aggregate definition with builder pattern |
| `job.rs` | Background refresh job |
| `query_rewriter.rs` | Automatic query rewriting to use aggregates |
| `storage.rs` | Definition persistence |
| `types.rs` | Core types (state, watermark, results) |

### Admin (`admin/`)

Admin Console for cluster management (separate node role).

| File | Purpose |
|------|---------|
| `api.rs` | Cluster/node/table/event handlers |
| `query_api.rs` | Query console endpoints (execute, explain, analyze, catalogs, schemas) |
| `storage_api.rs` | Storage browser endpoints (info, catalogs, schemas, partitions, compaction) |
| `static_files.rs` | SPA routes for the React admin UI |
| `state.rs` | AdminState (config, meta_client, http_client, event_cache) |

### Cluster Nodes

| Directory | Purpose |
|-----------|---------|
| `meta/` | Meta node: Raft consensus, node registry, leader election, table distribution |
| `router/` | Router node: client protocols, query routing, write routing |
| `data/` | Data node: storage engine, query execution, compaction, replication |
| `node/` | Shared infrastructure used by standalone and cluster modes |

### Other Modules

| Directory | Purpose |
|-----------|---------|
| `auth/` | Authentication, RBAC, JWT tokens, audit logging |
| `iceberg/` | Apache Iceberg REST catalog API and table format |
| `metrics/` | Internal system metrics collection |
| `observability/` | OpenTelemetry tracing, profiling configuration |
| `proto/` | Protobuf service definitions for meta gRPC |
| `utils/` | Shared utility functions |

## Module Dependencies

```
main.rs
    ├── config.rs
    ├── node/          ──→ services/*, storage/*, query/*, jobs/*
    ├── meta/          ──→ proto/
    ├── router/        ──→ meta/client, proto/
    ├── data/          ──→ storage/*, query/*, jobs/*, meta/client
    ├── admin/         ──→ meta/client
    ├── services/*     ──→ storage/*, query/*, jobs/*
    ├── storage/*      ──→ (self-contained)
    ├── query/*        ──→ storage/*
    └── jobs/*         ──→ storage/*, query/*
```

## Adding New Code

| Type | Location |
|------|----------|
| New API endpoint | `services/api.rs` + route in `main.rs` |
| New protocol (gRPC, etc.) | `services/` |
| Data format parser | `storage/` |
| Query optimizer rule | `query/analyzers/` or `query/logical_optimizers/` |
| Custom UDF/UDAF | `query/functions/` |
| TableProvider | `query/providers/` |
| Time series utility | `query/timeseries/` |
| Query/metadata cache | `query/cache/` |
| Background task | `jobs/` |
| Continuous aggregate | `aggregates/` |
| Configuration option | `config.rs` |
| Cluster coordination | `meta/` or `data/` |
| Admin console endpoint | `admin/` |
