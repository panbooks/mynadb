# OpenTelemetry Native Ingestion Architecture

## Overview

MynaDB natively speaks the OpenTelemetry Protocol (OTLP), accepting metrics, logs, and traces directly from OTel-instrumented applications and the OpenTelemetry Collector. Data arrives via gRPC (port 4317) or HTTP (`/v1/metrics`, `/v1/logs`, `/v1/traces`), is converted to MynaDB's internal Point format, and flows through the standard write path (WAL -> DataCache -> Parquet).

This makes MynaDB a drop-in observability backend — no intermediate adapters, no schema configuration, no format conversion pipelines.

## Design Goals

1. **Zero-config**: Point an OTel Collector at MynaDB and data appears automatically
2. **Protocol-native**: Accept the same protobuf/JSON payloads the Collector emits
3. **Unified write path**: Reuse DataCache.add_points() — same durability guarantees as line protocol
4. **Flat schema**: Map nested OTLP structures to tag/field columns for fast time-series queries
5. **Backward compatible**: Disabled by default; zero overhead when not enabled

## Architecture Diagram

```
                    ┌──────────────────────────────────────────────────────┐
                    │              OTLP gRPC (port 4317)                   │
                    │                                                      │
  OTel Collector ───┼──► tonic::Server                                    │
  or SDK direct     │         │                                           │
                    │         ├──► MetricsService::export()               │
                    │         │         │                                  │
                    │         ├──► LogsService::export()                  │
                    │         │         │                                  │
                    │         └──► TraceService::export()                 │
                    │                   │                                  │
                    └───────────────────┼──────────────────────────────────┘
                                        │
                    ┌───────────────────┼──────────────────────────────────┐
                    │           OTLP HTTP (main port, e.g. 7000)          │
                    │                   │                                  │
  curl / SDK ───────┼──► POST /v1/metrics ──► metrics_http_handler()     │
                    │──► POST /v1/logs    ──► logs_http_handler()        │
                    │──► POST /v1/traces  ──► traces_http_handler()      │
                    │                   │                                  │
                    └───────────────────┼──────────────────────────────────┘
                                        │
                    ┌───────────────────┼──────────────────────────────────┐
                    │              CONVERTER LAYER                         │
                    │                   │                                  │
                    │    ┌──────────────┼──────────────┐                  │
                    │    │  metrics_converter          │                  │
                    │    │  - Gauge → Point(value)     │                  │
                    │    │  - Sum → Point(value)       │                  │
                    │    │  - Histogram → Points       │                  │
                    │    │    (summary + buckets)       │                  │
                    │    │  - Summary → Points         │                  │
                    │    │    (quantiles)               │                  │
                    │    │  - ExpHistogram → Point     │                  │
                    │    │  One table per metric name  │                  │
                    │    └─────────────────────────────┘                  │
                    │    ┌─────────────────────────────┐                  │
                    │    │  logs_converter             │                  │
                    │    │  - LogRecord → Point         │                  │
                    │    │  All → otel_logs table       │                  │
                    │    └─────────────────────────────┘                  │
                    │    ┌─────────────────────────────┐                  │
                    │    │  traces_converter           │                  │
                    │    │  - Span → otel_traces        │                  │
                    │    │  - Event → otel_trace_events │                  │
                    │    │  - Link → otel_trace_links   │                  │
                    │    └─────────────────────────────┘                  │
                    │                   │                                  │
                    └───────────────────┼──────────────────────────────────┘
                                        │
                                        ▼
                    ┌──────────────────────────────────────────────────────┐
                    │         STANDARD WRITE PATH (existing)               │
                    │                                                      │
                    │    DataCache.add_points(db, schema, table, points)   │
                    │         │                                           │
                    │         ├─► WAL append                              │
                    │         ├─► RecordBatch conversion                  │
                    │         ├─► Dictionary encoding                     │
                    │         └─► Background flush → Parquet files        │
                    │                                                      │
                    │    QueryEngine discovers → SQL available             │
                    └──────────────────────────────────────────────────────┘
```

## Module Structure

```
src/services/otlp/
├── mod.rs                    # Module declarations
├── converter.rs              # Shared utilities:
│                             #   - timestamp_nanos_to_date()
│                             #   - resource_attrs_to_tags()
│                             #   - attributes_to_tags()
│                             #   - any_value_to_string()
│                             #   - bytes_to_hex()
│                             #   - sanitize_tag_name()
├── metrics_converter.rs      # ExportMetricsServiceRequest → HashMap<table, Vec<Point>>
├── logs_converter.rs         # ExportLogsServiceRequest → Vec<Point>
├── traces_converter.rs       # ExportTraceServiceRequest → TraceConversionResult
│                             #   { spans, events, links }
├── grpc_service.rs           # tonic service implementations:
│                             #   - OtlpMetricsService
│                             #   - OtlpLogsService
│                             #   - OtlpTraceService
└── http_handlers.rs          # Axum handlers for /v1/{metrics,logs,traces}
                              #   Supports protobuf + JSON content types
```

## Core Components

### 1. Converter Layer

The converter layer translates OTLP protobuf messages into MynaDB's `Point` struct. Each converter handles one signal type.

**Shared utilities** (`converter.rs`):

| Function | Purpose |
|----------|---------|
| `timestamp_nanos_to_date()` | Convert nanosecond timestamp to Date32 partition key |
| `resource_attrs_to_tags()` | Flatten resource attributes (service.name, etc.) into tags |
| `attributes_to_tags()` | Flatten data point attributes into tags |
| `any_value_to_string()` | Serialize AnyValue (string, int, bool, array, kvlist) |
| `bytes_to_hex()` | Encode trace_id/span_id bytes as hex strings |
| `sanitize_tag_name()` | Replace `-`, ` `, `/` with safe characters |

**Attribute flattening** (when `flatten_attributes = true`):

```
Resource attributes:  resource.service.name = "myapp"
                      resource.host.name = "server01"
Data point attributes: attrs.http.method = "GET"
                       attrs.http.status_code = "200"
```

### 2. Metrics Converter

Each OTLP metric type maps to a different Point layout:

| Metric Type | Fields | Notes |
|-------------|--------|-------|
| **Gauge** | `value` (Float64) | Direct value |
| **Sum** | `value` (Float64) | + `otel.aggregation_temporality`, `otel.is_monotonic` tags |
| **Histogram** | `count`, `sum`, `min`, `max` | Summary point + per-bucket points with `le` tag |
| **Summary** | `count`, `sum` | Summary point + per-quantile points |
| **ExponentialHistogram** | `count`, `sum`, `min`, `max`, `scale`, `zero_count` | Single point per data point |

**Table naming**: One table per metric name, dots replaced with underscores.
- `http.server.request.duration` → table `http_server_request_duration`
- `process.cpu.time` → table `process_cpu_time`

### 3. Logs Converter

All logs go to a single table: `otel_logs`.

| Column | Type | Source |
|--------|------|--------|
| time | TimestampNs | `time_unix_nano` (or `observed_time_unix_nano`) |
| severity_number | Int64 | Numeric severity (1-24) |
| severity_text | Utf8 | "TRACE", "DEBUG", "INFO", etc. |
| body | Utf8 | Log message body (serialized AnyValue) |
| trace_id | Utf8 | 32-char hex string |
| span_id | Utf8 | 16-char hex string |
| observed_time | Int64 | When the log was observed (if differs from time) |
| resource.* | Utf8 tags | Service name, host, etc. |
| attrs.* | Utf8 tags | Log record attributes |

### 4. Traces Converter

Spans, events, and links go to separate tables:

**`otel_traces`** (spans):

| Column | Type | Source |
|--------|------|--------|
| time | TimestampNs | `start_time_unix_nano` |
| trace_id | Utf8 | 32-char hex |
| span_id | Utf8 | 16-char hex |
| parent_span_id | Utf8 | Empty string for root spans |
| span_name | Utf8 | Operation name |
| span_kind | Utf8 | INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER |
| end_time | Int64 | End timestamp (nanoseconds) |
| duration_ns | Int64 | `end - start` |
| status_code | Utf8 | UNSET, OK, ERROR |
| status_message | Utf8 | Error description |
| resource.* | Utf8 tags | Service name, host, etc. |
| attrs.* | Utf8 tags | Span attributes |

**`otel_trace_events`** (span events / logs):

| Column | Type |
|--------|------|
| time | TimestampNs |
| trace_id | Utf8 |
| span_id | Utf8 |
| event_name | Utf8 |
| event.* | Utf8 tags |

**`otel_trace_links`** (span links):

| Column | Type |
|--------|------|
| time | TimestampNs |
| trace_id | Utf8 |
| span_id | Utf8 |
| linked_trace_id | Utf8 |
| linked_span_id | Utf8 |

### 5. gRPC Service

The gRPC server runs as a separate tonic server on port 4317 (the standard OTLP port). It registers three services on one server:

```rust
Server::builder()
    .add_service(MetricsServiceServer::new(metrics_service))
    .add_service(LogsServiceServer::new(logs_service))
    .add_service(TraceServiceServer::new(trace_service))
    .serve(addr)
```

Each service shares an `OtlpGrpcState` containing the `DataCache` and `OtlpConfig`. TLS is supported using the same certificate configuration as Flight gRPC.

### 6. HTTP Handlers

HTTP endpoints on the main server accept both protobuf and JSON:

| Content-Type | Encoding |
|-------------|----------|
| `application/x-protobuf` (default) | Protobuf binary |
| `application/json` | JSON (OTLP JSON encoding) |

Content type is auto-detected from the `Content-Type` header.

## Integration Points

| File | Change |
|------|--------|
| `src/config.rs` | `OtlpConfig` struct with `[otlp]` TOML section |
| `src/services/mod.rs` | `pub mod otlp;` |
| `src/services/api.rs` | OTLP HTTP handler wrappers + route registration |
| `src/node/common.rs` | `start_otlp_grpc_server()` helper |
| `src/node/standalone.rs` | Start OTLP gRPC server in server lifecycle |

## Data Flow Example

1. OTel Collector sends `ExportMetricsServiceRequest` via gRPC to port 4317
2. `OtlpMetricsService::export()` receives the request
3. `metrics_converter::convert_metrics()` transforms to `HashMap<String, Vec<Point>>`
4. For each table: `DataCache.add_points("otel", "default", "http_server_request_duration", points)`
5. Background flush writes Parquet files to `store/otel/default/http_server_request_duration/date=2026-02-12/`
6. QueryEngine discovers the table
7. `SELECT * FROM otel.default.http_server_request_duration WHERE time > now() - interval '1 hour'`

## Performance Considerations

- **Batch conversion**: Entire `ExportMetricsServiceRequest` batches are converted in one pass
- **Zero-copy tags**: Resource and scope tags are cloned once per batch, not per data point
- **Table grouping**: Points are pre-grouped by table name before calling `add_points()`
- **Non-blocking HTTP**: OTLP HTTP handlers are async and non-blocking
- **Shared state**: gRPC services share a single `DataCache` reference (no cloning per request)
