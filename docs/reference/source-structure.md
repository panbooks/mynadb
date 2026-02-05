# Source Code Structure

Stats Service source code is organized into 4 logical modules under `src/`.

## Overview

```
src/
├── main.rs          # Entry point, server initialization
├── config.rs        # Configuration structs and loading
├── services/        # API Layer - external interfaces
├── storage/         # Write Path - data ingestion and persistence
├── query/           # Read Path - query execution and optimization
└── jobs/            # Background Jobs - maintenance tasks
```

## Module Details

### Core (`main.rs`, `config.rs`)

| File | Purpose |
|------|---------|
| `main.rs` | Application entry point, HTTP/Flight/MySQL server setup, routing |
| `config.rs` | TOML configuration parsing, all config structs with defaults |

### Services (`services/`)

External-facing interfaces and middleware.

| File | Purpose |
|------|---------|
| `api.rs` | HTTP REST API handlers (write, query, admin endpoints) |
| `flight_service.rs` | Arrow Flight gRPC service for high-performance queries |
| `mysql_service.rs` | MySQL wire protocol compatibility layer |
| `auth.rs` | Basic authentication middleware |
| `rate_limiter.rs` | Token bucket rate limiting |

### Storage (`storage/`)

Write path components for data ingestion and persistence.

| File | Purpose |
|------|---------|
| `line_protocol.rs` | InfluxDB line protocol parser |
| `cache.rs` | In-memory write cache with background flush |
| `wal.rs` | Write-ahead log for durability |
| `storage.rs` | Storage abstraction (local filesystem, S3, MinIO) |
| `parquet_writer.rs` | Arrow RecordBatch to Parquet file conversion |

**Data Flow:**
```
HTTP POST /write → line_protocol → cache → wal → parquet_writer → storage
```

### Query (`query/`)

Read path components for SQL query execution.

| File | Purpose |
|------|---------|
| `query_engine.rs` | Main query execution, table registration, DataFusion integration |
| `query_cache.rs` | Query result caching with TTL |
| `executor.rs` | Custom SessionState with analyzer/optimizer rules |
| `table_provider.rs` | Custom ParquetTableProvider (optional) |
| `analyzers/*.rs` | Semantic analysis rules (e.g., timestamp→date) |
| `logical_optimizers/*.rs` | Logical plan optimization (e.g., partition pushdown) |
| `physical_optimizers/*.rs` | Physical plan optimization (e.g., scan config) |

**Query Pipeline:**
```
SQL → Parse → Analyze → Logical Optimize → Physical Plan → Physical Optimize → Execute
```

### Jobs (`jobs/`)

Background maintenance tasks.

| File | Purpose |
|------|---------|
| `compactor.rs` | LSM-tree style multi-level Parquet compaction |
| `retention_manager.rs` | Automatic deletion of old date partitions |
| `database_manager.rs` | Database/schema/table CRUD operations |
| `logger.rs` | Tracing/logging initialization |
| `profiling.rs` | Heap and CPU profiling endpoints |

## Module Dependencies

```
main.rs
    ├── config.rs
    ├── services/* ──→ storage/*, query/*, jobs/*
    ├── storage/*  ──→ (self-contained)
    ├── query/*    ──→ storage/*
    └── jobs/*     ──→ storage/*, query/*
```

## Adding New Code

| Type | Location |
|------|----------|
| New API endpoint | `services/api.rs` |
| New protocol (gRPC, etc.) | `services/` |
| Data format parser | `storage/` |
| Query optimizer rule | `query/analyzers/` or `query/logical_optimizers/` |
| Background task | `jobs/` |
| Configuration option | `config.rs` |
