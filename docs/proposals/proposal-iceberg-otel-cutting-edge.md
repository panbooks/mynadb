# Proposal: Cutting-Edge Features — Iceberg Table Format & OpenTelemetry Native

**Status:** Draft
**Author:** System Architecture Team
**Date:** 2026-02-25
**Target Version:** 3.0

---

## Executive Summary

MynaDB has a mature foundation: DataFusion query engine, Parquet storage, InfluxDB line protocol, SQL/InfluxQL/Arrow Flight/MySQL protocols, LSM compaction, continuous aggregates, Prometheus remote write/read, and distributed cluster mode. This proposal outlines two cutting-edge features that would position MynaDB as a **next-generation general-purpose TSDB**:

1. **Apache Iceberg Table Format** — Replace the custom table format with Iceberg for simplified metadata management, atomic commits, time travel, and native data lakehouse interoperability (Spark, Trino, DuckDB, Flink).

2. **OpenTelemetry Native Ingestion** — OTLP gRPC/HTTP endpoints for unified metrics, logs, and traces ingestion, making MynaDB a complete observability backend.

**Core Thesis:** The custom table format (MANIFEST.json, in-memory metadata HashMaps, filesystem scanning) accounts for ~3000-4000 lines of complex code. Iceberg replaces this with a battle-tested, industry-standard format while unlocking ecosystem interoperability and time travel for free. Combined with OTLP support, MynaDB becomes a unified observability + analytics platform.

---

## Part 1: Apache Iceberg Table Format

### 1.1 Problem Statement

MynaDB's current table format is a hand-built system comprising:

| Component | Implementation | Lines |
|-----------|----------------|-------|
| File tracking | MANIFEST.json per partition + in-memory HashMap | ~500 |
| Metadata store | `InMemoryStore` with 6 HashMaps (catalogs, schemas, tables, partitions, files, partition_files) | ~800 |
| File discovery | Recursive S3 LIST + manifest filtering | ~600 |
| Schema tracking | Per-file inference, merge at read/compaction time | ~200 |
| Concurrency control | In-process locks + etcd distributed locks | ~500 |
| Cache invalidation | Manual events, pub/sub in cluster mode | ~200 |
| Snapshot persistence | `catalog_snapshot.json` in meta directory | ~300 |

**Total: ~3100 lines of custom table format code.**

This works but has significant limitations:

1. **S3 LIST is expensive at scale** — Every query does recursive `list_with_delimiter()` calls. At 1000 partitions, this costs ~$5/query on S3 and adds latency.
2. **No atomic commits** — MANIFEST.json update + file write is not atomic. A crash between them leaves inconsistent state.
3. **No time travel** — Once compaction deletes files, historical state is lost.
4. **No ecosystem access** — Spark, Trino, DuckDB cannot read MynaDB data without ETL.
5. **Schema inference is fragile** — Reading up to 20 files to discover all columns is slow and can miss rare columns.
6. **Distributed coordination requires etcd** — Compaction locking depends on external infrastructure.

### 1.2 Current Table Format Architecture

#### File Naming and Levels

```
store/{catalog}/{schema}/{table}/date={YYYY-MM-DD}/node_id={node_id}/
  L0_1703251234567.parquet    # raw flush output
  L1_2000.parquet             # minor compaction (L0 -> L1)
  L2_3000.parquet             # major compaction (L1 -> L2)
  FULL_4000.parquet           # full compaction (level 255)
  MANIFEST.json               # tracks superseded files
```

- **L0**: Written by cache flush, no preprocessing
- **L1**: Minor compaction output (merged L0s, min_files=5 trigger)
- **L2-L4**: Major compaction output (merged lower levels, major_min_files=4 trigger)
- **FULL**: Full date-partition compaction (runs at configured hour, e.g. 01:00)

#### Metadata Tracking

`InMemoryStore` maintains 6 HashMaps:
- `catalogs: (name) -> CatalogInfo`
- `schemas: (catalog, schema) -> SchemaInfo`
- `tables: (catalog, schema, table) -> TableInfo`
- `partitions: (catalog, schema, table, partition_key) -> PartitionInfo`
- `files: (path) -> FileInfo` (level, size, row_count, min/max timestamp, column stats)
- `partition_files: (catalog, schema, table, partition_key) -> Vec<file_path>`

Persisted as `catalog_snapshot.json` in `{data_dir}/meta/`.

#### File Discovery (Query Time)

```
1. List date=YYYY-MM-DD/ directories under table prefix
2. Apply WHERE clause date filters to prune directories
3. For each date partition:
   a. List node_id=X/ directories
   b. List .parquet files in each node_id/ directory
   c. Read MANIFEST.json for superseded file list
   d. Filter out superseded files
4. Infer schema by reading up to 20 parquet file footers
5. Return Vec<ParquetFileInfo> to DataFusion for execution
```

#### Concurrency Model

- **Snapshot isolation via MANIFEST**: Files marked superseded before deletion. Queries skip superseded files.
- **Per-partition in-process locks**: Prevents concurrent compaction on same partition.
- **Distributed locks via etcd**: In cluster mode, prevents cross-node compaction conflicts.
- **No file-level locking**: Object store doesn't support it; manifest provides eventual consistency.

### 1.3 Proposed Design: Iceberg-Based Table Format

#### What Iceberg Replaces

| Current Component | Iceberg Replacement | Benefit |
|-------------------|---------------------|---------|
| MANIFEST.json | Manifest files (Avro) | Atomic snapshot commits, file-level statistics |
| In-memory 6 HashMaps | Table metadata.json | Persistent, versioned, survives restarts |
| Recursive S3 LIST | Manifest tracks all files | O(1) file discovery, no LIST calls |
| Schema inference from files | Schema in metadata | Instant, complete, with evolution history |
| `catalog_snapshot.json` | Iceberg metadata files | Versioned, no separate persistence layer |
| Per-partition etcd locks | Optimistic concurrency | Lock-free, no external coordinator |
| Manual cache invalidation | Snapshot version change | Built-in versioning = cache key |
| (not available) | Snapshot history | Time travel queries for free |
| (not available) | Standard REST Catalog | Spark/Trino/DuckDB/Flink access for free |

#### Architecture

```
Write Path:
  Line Protocol -> WAL -> DataCache (memtable)
       |
       v
  ParquetWriter (unchanged: write data file)
       |
       v
  Iceberg commit(appendFiles)  ← replaces MANIFEST.json + HashMap registration
  → atomic metadata.json swap
  → manifest updated with new file

Query Path:
  SQL -> DataFusion
       |
       v
  IcebergTableProvider (replaces ParquetTableProvider)
  → read metadata.json (cached in memory)
  → manifest gives exact file list  ← no S3 LIST
  → schema from metadata            ← no file inference
  → DataFusion execution (same)

Compaction Path:
  Timer triggers compaction
       |
       v
  Read file list from Iceberg manifest  ← no S3 LIST
  Filter by level (stored as file property)
       |
       v
  Read + merge + sort (unchanged)
       |
       v
  Iceberg commit(rewriteFiles)  ← atomic: old files removed, new added
  → GC job cleans up orphaned files later

Retention Path:
  expire_snapshots(older_than)  ← replaces directory deletion
  remove_orphan_files()         ← Iceberg handles cleanup
```

#### Partition Spec

The existing `date=YYYY-MM-DD/node_id=X/` directory layout maps to an Iceberg partition spec:

```json
{
  "spec-id": 0,
  "fields": [
    { "source-id": 1, "transform": "day",      "name": "date" },
    { "source-id": 2, "transform": "identity",  "name": "node_id" }
  ]
}
```

Both partition columns are preserved. The `node_id` partition is critical for:
- **Write locality**: Each data node writes to its own partition, no conflicts
- **Query routing**: Router pushes queries to the node owning the data
- **Compaction scope**: Each node compacts only its own files

With Iceberg, the router can prune by `node_id` at the manifest level — `WHERE node_id = 'data-node-3'` skips files instantly from metadata, no filesystem scan needed.

#### LSM Levels in Iceberg

Iceberg has no built-in concept of "levels". Store level as a file-level property:

```rust
table.new_append()
     .append_file(DataFile {
         path: "store/.../data-{uuid}.parquet",
         partition: PartitionData { date, node_id },
         record_count,
         file_size_in_bytes,
         // Custom properties:
         properties: HashMap::from([("level", "0")]),
     })
     .commit().await;
```

Compaction reads files by level from manifest properties:

```rust
let table = iceberg.load_table(...).await;
let snapshot = table.current_snapshot();
let files = snapshot.files_for_partition(date, node_id);
let l0_files = files.filter(|f| f.properties["level"] == "0");
// ... merge, write new L1 file, commit rewriteFiles
```

#### Flush Path (Detailed)

```rust
// Current (custom format)
async fn flush_partition(catalog, schema, table, date, node_id, points) {
    let path = format!("store/{}/{}/{}/date={}/node_id={}/L0_{}.parquet",
                        catalog, schema, table, date, node_id, timestamp);
    write_parquet(&path, points).await;
    manifest.append(&path);              // MANIFEST.json
    metadata.register_file(file_info);   // in-memory HashMap
}

// Proposed (Iceberg)
async fn flush_partition(catalog, schema, table, date, node_id, points) {
    let path = format!("store/{}/{}/{}/date={}/node_id={}/data-{}.parquet",
                        catalog, schema, table, date, node_id, uuid());
    write_parquet(&path, points).await;

    let iceberg_table = iceberg.load_table(catalog, schema, table).await;
    iceberg_table.new_append()
         .append_file(DataFile {
             path,
             partition: PartitionData { date, node_id },
             record_count,
             file_size_in_bytes,
             column_sizes, lower_bounds, upper_bounds, null_value_counts,
             properties: HashMap::from([("level", "0")]),
         })
         .commit().await;  // atomic metadata swap
}
```

#### Compaction Path (Detailed)

```rust
// Current (custom format)
async fn compact(catalog, schema, table, partition) {
    let files = list_prefix(partition_path).await;        // S3 LIST (expensive)
    let files = filter_by_manifest(files);                // MANIFEST.json check
    let l0_files = files.filter(|f| f.level == 0);
    if l0_files.len() < min_files { return; }
    let merged = read_and_merge(l0_files).await;
    let output_path = format!(".../L1_{}.parquet", timestamp);
    write_parquet(&output_path, merged).await;
    manifest.mark_superseded(&l0_files);                  // update MANIFEST.json
    for f in &l0_files { storage.delete(&f.path).await; } // delete old files
    metadata.register_file(output_info);                  // update HashMap
}

// Proposed (Iceberg)
async fn compact(catalog, schema, table, partition) {
    let iceberg_table = iceberg.load_table(catalog, schema, table).await;
    let snapshot = iceberg_table.current_snapshot();
    let files = snapshot.files_for_partition(partition);   // from manifest, no S3 LIST
    let l0_files = files.filter(|f| f.properties["level"] == "0");
    if l0_files.len() < min_files { return; }
    let merged = read_and_merge(l0_files).await;
    let output_path = format!(".../data-{}.parquet", uuid());
    write_parquet(&output_path, merged).await;

    iceberg_table.new_rewrite()
         .delete_files(l0_files.iter().map(|f| f.path.clone()))
         .add_file(DataFile {
             path: output_path,
             properties: HashMap::from([("level", "1")]),
             ..stats
         })
         .commit().await;  // atomic: old removed + new added in one commit
    // Iceberg GC job cleans orphaned data files later
}
```

### 1.4 Trade-Off Analysis

#### Where Iceberg is Better

| Aspect | Current | Iceberg | Impact |
|--------|---------|---------|--------|
| S3 costs | LIST per partition per query | No LIST calls | **$$$** at scale |
| Atomicity | MANIFEST.json + eventual consistency | Atomic metadata swap | **Correctness** |
| Time travel | Not available | Built-in via snapshots | **New capability** |
| Ecosystem | No external engine can read data | Spark/Trino/DuckDB/Flink native | **Huge unlock** |
| Schema management | Infer from 20 files + merge | Tracked in metadata with evolution | **Reliability** |
| Cluster coordination | Requires etcd for compaction locks | Optimistic concurrency | **Simpler ops** |
| Code complexity | ~3100 lines of custom code | Replaced by iceberg-rust crate | **Maintainability** |

#### Where Iceberg is Worse or Requires Care

| Aspect | Concern | Mitigation |
|--------|---------|------------|
| Commit latency | ~10-50ms per commit vs ~1ms for MANIFEST.json | Batch commits: commit every N flushes or every M seconds. At 60s flush interval, negligible. |
| Small file problem | Many L0 files = many manifest entries = slower planning | Same mitigation as today: compaction merges small files. Iceberg planning is O(manifest entries), still fast. |
| Schema flexibility | Iceberg enforces strict schemas | Use schema evolution: commit `addColumn` before writing file with new column. Old files without column return NULL. |
| LSM levels | No native concept | Store as file-level property in manifest. Works cleanly. |
| Dependency size | iceberg-rust crate adds compilation time | Acceptable trade-off for the value gained. |
| Migration | Existing deployments need data migration | Provide migration tool: scan existing Parquet files, generate Iceberg metadata. Zero data movement needed — metadata only. |

### 1.5 Code Impact

#### Files to Delete (~3000-4000 lines)

| File | Lines | Reason |
|------|-------|--------|
| `src/query/catalog/memory_manager.rs` | ~800 | Iceberg catalog replaces in-memory store |
| `src/storage/manifest.rs` + MANIFEST logic | ~500 | Iceberg manifests replace MANIFEST.json |
| `src/storage/discovery.rs` (LIST + filter) | ~600 | Iceberg metadata knows all files |
| `src/storage/listing.rs` (helpers) | ~300 | No more filesystem scanning |
| `src/query/cache/partition_files_cache.rs` | ~400 | No LIST calls = no cache needed |
| Schema inference logic | ~200 | Schema stored in Iceberg metadata |
| `catalog_snapshot.json` persistence | ~300 | Iceberg metadata is persistent |
| Per-partition lock manager | ~200 | Optimistic concurrency replaces locks |

#### Files to Keep (Core Value)

| File | Reason |
|------|--------|
| `src/storage/cache/mod.rs` (DataCache) | Iceberg has no memtable concept |
| `src/storage/wal.rs` | Durability between flush intervals |
| `src/storage/line_protocol.rs` | Ingestion format, orthogonal to table format |
| `src/storage/parquet_writer.rs` | Data file writing logic (modify to Iceberg commit) |
| `src/jobs/compactor/` | Merge strategy logic (modify bookkeeping to use Iceberg) |
| `src/query/query_engine/` | DataFusion integration (swap TableProvider) |
| All UDFs, analyzers, optimizers | Independent of table format |

#### New Files

```
src/iceberg/
  mod.rs                      — Module declarations
  config.rs                   — IcebergConfig struct
  table_manager.rs            — Wraps iceberg-rust: load/create/commit tables
  schema_tracker.rs           — Arrow <-> Iceberg schema conversion + evolution
  catalog_provider.rs         — DataFusion CatalogProvider backed by Iceberg
  table_provider.rs           — DataFusion TableProvider reading from Iceberg manifests
  rest_catalog.rs             — Iceberg REST Catalog API (for Spark/Trino access)
  sync.rs                     — Background metadata sync (for migration/hybrid mode)
  gc.rs                       — Snapshot expiration + orphan file cleanup
```

### 1.6 Dependencies

```toml
iceberg = "0.4"                    # Apache Iceberg Rust implementation
iceberg-catalog-rest = "0.4"       # REST catalog (for external access)
iceberg-datafusion = "0.4"         # DataFusion integration
apache-avro = "0.17"               # Avro for manifest files
```

### 1.7 Implementation Phases

| Phase | Scope | Effort | Independently Shippable |
|-------|-------|--------|------------------------|
| 1 | Iceberg metadata generation from existing Parquet files (schema, manifests, snapshots) | 5-7 days | Yes — enables Spark/Trino to read existing data |
| 2 | Background metadata sync job (periodic + on flush/compaction trigger) | 3-4 days | Yes — keeps Iceberg metadata current |
| 3 | REST Catalog API (Iceberg spec) | 3-5 days | Yes — Spark/Trino can discover tables |
| 4 | IcebergTableProvider for DataFusion (replace ParquetTableProvider) | 4-6 days | Yes — eliminates S3 LIST from query path |
| 5 | Flush path: Iceberg commit instead of MANIFEST.json | 3-4 days | Depends on Phase 4 |
| 6 | Compaction path: rewriteFiles instead of manifest marking | 3-4 days | Depends on Phase 5 |
| 7 | Delete old metadata code (memory_manager, manifest, discovery, caches) | 2-3 days | Depends on Phase 5-6 |
| 8 | Time travel queries (AT SNAPSHOT / AT TIMESTAMP) | 2-3 days | Depends on Phase 4 |
| 9 | Read external Iceberg tables from MynaDB SQL | 4-6 days | Independent |
| 10 | Migration tool (existing deployments -> Iceberg metadata) | 2-3 days | Depends on Phase 1 |

**Total: ~32-45 days**

Phases 1-3 can be shipped as a **read-only Iceberg layer** (external engines can read MynaDB data). Phases 4-7 are the **core migration** (MynaDB internally uses Iceberg). Phases 8-10 are **bonus features**.

---

## Part 2: OpenTelemetry Native Ingestion

### 2.1 Problem Statement

Observability is converging on OpenTelemetry as the standard. Currently:
- MynaDB supports Prometheus remote write/read (metrics only)
- No native log or trace ingestion
- Users must run a separate OTel Collector to convert OTLP -> InfluxDB line protocol

With native OTLP support, MynaDB becomes a **unified observability backend** — one database for metrics, logs, and traces, queryable via SQL.

### 2.2 Competitive Landscape

| Database | OTLP Support | Unified Metrics+Logs+Traces |
|----------|-------------|---------------------------|
| GreptimeDB | Native OTLP gRPC + HTTP | Yes |
| SigNoz | Built on ClickHouse + OTLP | Yes |
| VictoriaMetrics | VictoriaLogs (separate) | Partial |
| InfluxDB 3 | No native OTLP | No |
| TimescaleDB | Via pg extension | Partial |
| **MynaDB (proposed)** | **Native OTLP gRPC + HTTP** | **Yes** |

### 2.3 Architecture

```
OTLP gRPC (port 4317)                 OTLP HTTP (/v1/* on port 7000)
  MetricsService/Export                  POST /v1/metrics
  LogsService/Export                     POST /v1/logs
  TraceService/Export                    POST /v1/traces
         |                                        |
         +-----------> OTLP Converter Layer <-----+
                        (protobuf -> Vec<Point>)
                               |
              +----------------+----------------+
              |                |                |
        metrics_converter  logs_converter  traces_converter
              |                |                |
              v                v                v
           DataCache.add_points(catalog="otel", schema, table, points)
                               |
                               v
                     Existing write path
                     (WAL -> flush -> Parquet -> compaction)
```

This follows the exact same pattern as `src/services/prometheus/remote_write.rs` — convert an external protocol's data model into `Point` structs and write via `DataCache.add_points()`.

### 2.4 Table Schema Design

#### 2.4.1 Metrics

One table per metric name in the `otel` database. Metric name `http.server.request.duration` -> table `otel.default.http_server_request_duration`.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `time` | TimestampNanosecond | data_point.time_unix_nano | Primary time column |
| `date` | Date32 | Computed from time | Partition column |
| `value` | Float64 | Gauge/Sum value | NULL for histogram/summary |
| `count` | UInt64 | Histogram/Summary count | NULL for gauge/sum |
| `sum` | Float64 | Histogram/Summary sum | NULL for gauge/sum |
| `bucket_le` | Float64 | Histogram bucket boundary | Exploded: one row per bucket |
| `bucket_count` | UInt64 | Histogram bucket count | |
| `quantile` | Float64 | Summary quantile | Exploded: one row per quantile |
| `quantile_value` | Float64 | Summary quantile value | |
| `start_time` | TimestampNanosecond | start_time_unix_nano | For cumulative metrics |
| `flags` | UInt32 | Data point flags | |
| `exemplar_trace_id` | Utf8 | Exemplar trace link | |
| `exemplar_span_id` | Utf8 | Exemplar span link | |
| resource.* | Utf8 (tags) | Resource attributes | service.name, host.name, etc. |
| scope.* | Utf8 (tags) | Scope attributes | Instrumentation library |
| attrs.* | Utf8 (tags) | Data point attributes | Metric-specific labels |

**Metric type handling:**
- **Gauge**: Single row, `value` column
- **Sum** (monotonic/non-monotonic): Single row, `value` column, `is_monotonic` tag
- **Histogram**: Exploded into N rows per bucket (`bucket_le`, `bucket_count`) + 1 summary row (`count`, `sum`)
- **ExponentialHistogram**: `scale`, `zero_count`, bucket arrays as JSON
- **Summary**: Exploded per quantile (`quantile`, `quantile_value`) + 1 summary row (`count`, `sum`)

#### 2.4.2 Logs

Single table: `otel.default.otel_logs`

| Column | Type | Notes |
|--------|------|-------|
| `time` | TimestampNanosecond | time_unix_nano |
| `date` | Date32 | Partition column |
| `observed_time` | TimestampNanosecond | observed_time_unix_nano |
| `severity_number` | Int32 | 1-24 (TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21) |
| `severity_text` | Utf8 | "INFO", "ERROR", etc. |
| `body` | Utf8 | Log message (string or JSON-serialized) |
| `flags` | UInt32 | Log record flags |
| `trace_id` | Utf8 | Hex-encoded (32 chars), links to traces |
| `span_id` | Utf8 | Hex-encoded (16 chars), links to traces |
| resource.* | Utf8 (tags) | service.name, host.name, etc. |
| scope.* | Utf8 (tags) | Instrumentation library |
| attrs.* | Utf8 (tags) | Log record attributes |

#### 2.4.3 Traces

Main table: `otel.default.otel_traces`

| Column | Type | Notes |
|--------|------|-------|
| `time` | TimestampNanosecond | start_time_unix_nano |
| `date` | Date32 | Partition column |
| `trace_id` | Utf8 | Hex-encoded (32 chars) |
| `span_id` | Utf8 | Hex-encoded (16 chars) |
| `parent_span_id` | Utf8 | Empty string if root span |
| `trace_state` | Utf8 | W3C trace state |
| `span_name` | Utf8 | Operation name |
| `span_kind` | Utf8 | INTERNAL/SERVER/CLIENT/PRODUCER/CONSUMER |
| `end_time` | TimestampNanosecond | end_time_unix_nano |
| `duration_ns` | Int64 | end_time - start_time (computed) |
| `status_code` | Utf8 | OK/ERROR/UNSET |
| `status_message` | Utf8 | Error description |
| `events_count` | Int32 | Number of span events |
| `links_count` | Int32 | Number of span links |
| resource.* | Utf8 (tags) | service.name, etc. |
| scope.* | Utf8 (tags) | |
| attrs.* | Utf8 (tags) | Span attributes |

Span events table: `otel.default.otel_trace_events`

| Column | Type | Notes |
|--------|------|-------|
| `time` | TimestampNanosecond | event.time_unix_nano |
| `date` | Date32 | |
| `trace_id` | Utf8 | Parent span's trace_id |
| `span_id` | Utf8 | Parent span's span_id |
| `event_name` | Utf8 | Event name |
| `event_attrs` | Utf8 | JSON-serialized attributes |

Span links table: `otel.default.otel_trace_links`

| Column | Type | Notes |
|--------|------|-------|
| `time` | TimestampNanosecond | Parent span's start_time |
| `date` | Date32 | |
| `trace_id` | Utf8 | Source trace_id |
| `span_id` | Utf8 | Source span_id |
| `linked_trace_id` | Utf8 | Linked trace_id |
| `linked_span_id` | Utf8 | Linked span_id |
| `link_attrs` | Utf8 | JSON-serialized attributes |

### 2.5 Configuration

```toml
[otlp]
enabled = false                    # Enable OTLP ingestion
grpc_host = "0.0.0.0"             # OTLP gRPC listen address
grpc_port = 4317                   # Standard OTLP gRPC port
http_enabled = true                # Enable HTTP OTLP on main HTTP port
database = "otel"                  # Default database for OTLP data
schema = "default"                 # Default schema
resource_as_tags = true            # Map resource attributes to tags
scope_as_tags = false              # Map scope attributes to tags (verbose)
flatten_attributes = true          # "host.name" -> "host_name"
max_batch_size = 10000             # Max points per batch
```

### 2.6 API Endpoints

**gRPC (port 4317)** — Standard OTLP Collector service:
- `opentelemetry.proto.collector.metrics.v1.MetricsService/Export`
- `opentelemetry.proto.collector.logs.v1.LogsService/Export`
- `opentelemetry.proto.collector.trace.v1.TraceService/Export`

**HTTP (on existing port 7000):**
- `POST /v1/metrics` — Content-Type: `application/x-protobuf` or `application/json`
- `POST /v1/logs` — Content-Type: `application/x-protobuf` or `application/json`
- `POST /v1/traces` — Content-Type: `application/x-protobuf` or `application/json`

### 2.7 New Files

```
src/services/otlp/
  mod.rs                    — Module declarations, shared types
  grpc_service.rs           — tonic MetricsService/LogsService/TraceService implementations
  http_handlers.rs          — Axum POST /v1/metrics, /v1/logs, /v1/traces
  converter.rs              — Shared: resource attrs, timestamp, attribute flattening
  metrics_converter.rs      — ExportMetricsServiceRequest -> Vec<Point>
  logs_converter.rs         — ExportLogsServiceRequest -> Vec<Point>
  traces_converter.rs       — ExportTraceServiceRequest -> Vec<Point>
```

### 2.8 Dependencies

```toml
opentelemetry-proto = { version = "0.25", features = ["gen-tonic", "metrics", "logs", "trace", "with-serde"] }
```

Already in Cargo.toml: `tonic`, `prost`, `opentelemetry`.

### 2.9 Integration Points

| Existing File | Change |
|---------------|--------|
| `src/config.rs` | Add `OtlpConfig` struct + field on `Config` |
| `src/services/mod.rs` | Add `pub mod otlp;` |
| `src/node/standalone.rs` | Start OTLP gRPC server on port 4317 |
| `src/node/common.rs` | Add `start_otlp_grpc_server()` helper |
| `src/services/api.rs` | Add `/v1/metrics`, `/v1/logs`, `/v1/traces` routes |
| `Cargo.toml` | Add `opentelemetry-proto` |

### 2.10 Implementation Phases

| Phase | Scope | Effort | Independently Shippable |
|-------|-------|--------|------------------------|
| 1 | Metrics gRPC ingestion (gauge, sum, histogram, summary) | 3-5 days | Yes |
| 2 | Logs gRPC ingestion (severity, body, trace correlation) | 2-3 days | Yes (depends on Phase 1 infra) |
| 3 | Traces gRPC ingestion (spans + events + links tables) | 3-4 days | Yes (depends on Phase 1 infra) |
| 4 | HTTP endpoints (/v1/*) with JSON + protobuf support | 1-2 days | Yes (depends on Phase 1 infra) |
| 5 | Attribute flattening + exemplar support | 1-2 days | Yes |
| 6 | Production hardening (backpressure, metrics, partial success, TLS) | 2-3 days | Yes |

**Total: ~12-19 days**

---

## Part 3: Combined Roadmap

### Recommended Build Order

| Order | Feature | Phase | Why |
|-------|---------|-------|-----|
| 1 | OTLP | Phase 1: Metrics gRPC | Highest immediate value, many users want OTel |
| 2 | OTLP | Phases 2-4: Logs + Traces + HTTP | Complete the observability backend |
| 3 | Iceberg | Phase 1: Metadata generation | Enables Spark/Trino to read existing data |
| 4 | Iceberg | Phases 2-3: Sync + REST Catalog | External engines discover and query tables |
| 5 | Iceberg | Phase 4: IcebergTableProvider | Eliminates S3 LIST from MynaDB query path |
| 6 | Iceberg | Phases 5-6: Flush + Compaction migration | Core table format migration |
| 7 | Iceberg | Phase 7: Delete old code | Clean up ~3000+ lines |
| 8 | Both | Polish: OTLP hardening + Time travel | Advanced features |

### Verification Plan

1. **OTLP**: Configure OTel Collector with `otlp` exporter pointing to MynaDB. Verify metrics/logs/traces appear in `otel` database via SQL.
2. **OTLP**: Point Grafana Tempo datasource at MynaDB for trace visualization.
3. **Iceberg**: Use `pyiceberg` or `spark-sql` with `RestCatalog` pointing at MynaDB REST API. Run `SELECT * FROM namespace.table`.
4. **Iceberg**: From MynaDB SQL, query `SELECT * FROM lakehouse.ns.table` for external Iceberg data.
5. **Combined**: Send OTLP data to MynaDB, query via Spark SQL through Iceberg catalog. Full round-trip.
6. **Regression**: `cargo build --release` compiles, `cargo test` passes, existing write/query paths unaffected.

---

## Appendix A: Market Positioning

With both features, MynaDB occupies a unique position:

```
                    Time Series          Observability          Data Lakehouse
                    ──────────           ─────────────          ──────────────
InfluxDB 3          ✅ (core)            ❌                     ❌
Prometheus          ✅ (metrics)         ✅ (metrics only)      ❌
GreptimeDB          ✅                   ✅ (OTLP)              ❌
TimescaleDB         ✅                   ❌                     ❌
VictoriaMetrics     ✅                   Partial                ❌
ClickHouse          ✅ (OLAP)            ❌                     Partial (Iceberg read)
StarRocks           ❌                   ❌                     ✅ (Iceberg read)
                    ──────────           ─────────────          ──────────────
MynaDB (proposed)   ✅ (core)            ✅ (OTLP native)       ✅ (Iceberg native)
```

No existing database combines all three. This is the differentiation.

## Appendix B: Iceberg Redesign — What Changes, What Stays

### Deleted Modules (~3000-4000 lines)

- `src/query/catalog/memory_manager.rs` — In-memory metadata store (6 HashMaps)
- `src/storage/manifest.rs` — MANIFEST.json read/write
- `src/storage/discovery.rs` — Recursive S3 LIST + manifest filtering
- `src/storage/listing.rs` — File listing helpers
- `src/query/cache/partition_files_cache.rs` — S3 LIST result cache
- Schema inference logic in `ParquetTableProvider`
- `catalog_snapshot.json` persistence
- Per-partition lock manager

### Modified Modules

- `src/storage/parquet_writer.rs` — Add Iceberg commit after file write
- `src/jobs/compactor/` — Replace manifest marking with Iceberg `rewriteFiles`
- `src/jobs/retention_manager.rs` — Use Iceberg `expire_snapshots` + `remove_orphan_files`
- `src/query/providers/table_provider.rs` — Swap to `IcebergTableProvider`
- `src/node/standalone.rs` — Initialize Iceberg, start sync/GC jobs

### Unchanged Modules (Core Value)

- `src/storage/cache/` — DataCache (memtable)
- `src/storage/wal.rs` — Write-ahead log
- `src/storage/line_protocol.rs` — InfluxDB line protocol parser
- `src/query/query_engine/` — DataFusion session, analyzers, optimizers
- `src/query/functions/` — All custom UDFs (time_bucket, forecasting, geospatial)
- `src/query/timeseries/` — Time-series utilities
- `src/services/` — All protocol handlers (HTTP, Flight, MySQL, Prometheus)
- `src/aggregates/` — Continuous aggregates
- `src/jobs/downsampling/` — Downsampling
- `src/metrics/` — Metrics collection
- `src/auth/` — RBAC
