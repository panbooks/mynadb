# OpenTelemetry Native Ingestion Guide

Receive metrics, logs, and traces directly from OpenTelemetry-instrumented applications. No adapters, no format conversions — MynaDB speaks OTLP natively.

## Quick Start

### 1. Enable the Feature

Add to your `config.toml`:

```toml
[otlp]
enabled = true
```

### 2. Point Your OTel Collector at MynaDB

```yaml
# otel-collector-config.yaml
exporters:
  otlp/mynadb:
    endpoint: "mynadb-host:4317"
    tls:
      insecure: true

service:
  pipelines:
    metrics:
      exporters: [otlp/mynadb]
    logs:
      exporters: [otlp/mynadb]
    traces:
      exporters: [otlp/mynadb]
```

### 3. Query Your Data

```sql
-- Metrics (one table per metric name)
SELECT * FROM otel.default.http_server_request_duration
WHERE time > now() - interval '1 hour';

-- Logs
SELECT severity_text, body, "resource.service.name"
FROM otel.default.otel_logs
WHERE time > now() - interval '15 minutes'
ORDER BY time DESC LIMIT 100;

-- Traces
SELECT trace_id, span_name, duration_ns / 1000000 as duration_ms, status_code
FROM otel.default.otel_traces
WHERE time > now() - interval '1 hour'
  AND "resource.service.name" = 'checkout-service'
ORDER BY duration_ns DESC LIMIT 20;
```

## Configuration

### Full Configuration Reference

```toml
[otlp]
# Enable OTLP ingestion (default: false)
enabled = true

# gRPC listen address (default: "0.0.0.0")
grpc_host = "0.0.0.0"

# gRPC listen port (default: 4317, the standard OTLP port)
grpc_port = 4317

# Enable HTTP endpoints on the main HTTP server (default: true)
# Adds POST /v1/metrics, /v1/logs, /v1/traces
http_enabled = true

# Target database for all OTLP data (default: "otel")
database = "otel"

# Target schema within the database (default: "default")
schema = "default"

# Include resource attributes as tags (default: true)
# e.g., service.name, host.name, os.type
resource_as_tags = true

# Flatten attribute names with dot-separated prefixes (default: true)
# When true:  resource.service.name, attrs.http.method
# When false: service.name, http.method
flatten_attributes = true
```

## Ingestion Methods

### gRPC (Recommended)

The standard OTLP gRPC endpoint on port 4317. Used by the OTel Collector and most SDKs.

```bash
# Test with grpcurl
grpcurl -plaintext -d '{
  "resource_metrics": [{
    "resource": {
      "attributes": [{"key": "service.name", "value": {"string_value": "test"}}]
    },
    "scope_metrics": [{
      "metrics": [{
        "name": "cpu.usage",
        "gauge": {
          "data_points": [{
            "time_unix_nano": 1707753600000000000,
            "as_double": 42.5,
            "attributes": [{"key": "host", "value": {"string_value": "server01"}}]
          }]
        }
      }]
    }]
  }]
}' mynadb-host:4317 opentelemetry.proto.collector.metrics.v1.MetricsService/Export
```

### HTTP

The OTLP HTTP endpoints on the main HTTP server. Supports both protobuf and JSON.

```bash
# Send metrics via HTTP (JSON)
curl -X POST http://localhost:7000/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "resource_metrics": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"string_value": "myapp"}}]
      },
      "scope_metrics": [{
        "metrics": [{
          "name": "http.requests.total",
          "sum": {
            "data_points": [{
              "time_unix_nano": 1707753600000000000,
              "as_int": 42,
              "attributes": [{"key": "method", "value": {"string_value": "GET"}}]
            }],
            "aggregation_temporality": 2,
            "is_monotonic": true
          }
        }]
      }]
    }]
  }'

# Send logs via HTTP (JSON)
curl -X POST http://localhost:7000/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resource_logs": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"string_value": "myapp"}}]
      },
      "scope_logs": [{
        "log_records": [{
          "time_unix_nano": 1707753600000000000,
          "severity_number": 9,
          "severity_text": "INFO",
          "body": {"string_value": "Application started successfully"}
        }]
      }]
    }]
  }'

# Send traces via HTTP (JSON)
curl -X POST http://localhost:7000/v1/traces \
  -H "Content-Type: application/json" \
  -d '{
    "resource_spans": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"string_value": "myapp"}}]
      },
      "scope_spans": [{
        "spans": [{
          "trace_id": "0102030405060708090a0b0c0d0e0f10",
          "span_id": "0102030405060708",
          "name": "GET /api/users",
          "kind": 2,
          "start_time_unix_nano": 1707753600000000000,
          "end_time_unix_nano": 1707753600050000000,
          "status": {"code": 1}
        }]
      }]
    }]
  }'
```

## Table Schema Reference

### Metrics

Each metric creates its own table. Table name = metric name with dots/dashes replaced by underscores.

| Metric | Table Name |
|--------|-----------|
| `http.server.request.duration` | `http_server_request_duration` |
| `process.cpu.time` | `process_cpu_time` |
| `system.memory.usage` | `system_memory_usage` |

Common columns across all metric tables:

| Column | Type | Description |
|--------|------|-------------|
| `time` | Timestamp | Data point timestamp |
| `date` | Date32 | Partition key |
| `value` | Float64 | Gauge/Sum value |
| `count` | Int64 | Histogram/Summary count |
| `sum` | Float64 | Histogram/Summary sum |
| `min` | Float64 | Histogram min |
| `max` | Float64 | Histogram max |
| `bucket_le` | Float64 | Histogram bucket boundary |
| `bucket_count` | Int64 | Histogram bucket count |
| `quantile` | Float64 | Summary quantile value |
| `quantile_value` | Float64 | Summary quantile result |
| `resource.*` | Utf8 | Resource attributes as tags |
| `attrs.*` | Utf8 | Data point attributes as tags |

### Logs (`otel_logs`)

| Column | Type | Description |
|--------|------|-------------|
| `time` | Timestamp | Log timestamp |
| `severity_number` | Int64 | 1-24 (TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21) |
| `severity_text` | Utf8 | Human-readable severity |
| `body` | Utf8 | Log message |
| `trace_id` | Utf8 | 32-char hex (for correlated traces) |
| `span_id` | Utf8 | 16-char hex |
| `resource.*` | Utf8 | Resource attributes |
| `attrs.*` | Utf8 | Log attributes |

### Traces (`otel_traces`)

| Column | Type | Description |
|--------|------|-------------|
| `time` | Timestamp | Span start time |
| `trace_id` | Utf8 | 32-char hex |
| `span_id` | Utf8 | 16-char hex |
| `parent_span_id` | Utf8 | Parent span (empty for root) |
| `span_name` | Utf8 | Operation name |
| `span_kind` | Utf8 | SERVER, CLIENT, etc. |
| `end_time` | Int64 | End timestamp (ns) |
| `duration_ns` | Int64 | Duration in nanoseconds |
| `status_code` | Utf8 | UNSET, OK, ERROR |
| `status_message` | Utf8 | Error description |
| `resource.*` | Utf8 | Resource attributes |
| `attrs.*` | Utf8 | Span attributes |

### Trace Events (`otel_trace_events`)

| Column | Type |
|--------|------|
| `time` | Timestamp |
| `trace_id` | Utf8 |
| `span_id` | Utf8 |
| `event_name` | Utf8 |
| `event.*` | Utf8 |

### Trace Links (`otel_trace_links`)

| Column | Type |
|--------|------|
| `time` | Timestamp |
| `trace_id` | Utf8 |
| `span_id` | Utf8 |
| `linked_trace_id` | Utf8 |
| `linked_span_id` | Utf8 |

## Example Queries

### Service Latency (P99)

```sql
SELECT
    time_bucket(time, 60000000000, 0) as minute,
    "resource.service.name" as service,
    max(value) as p99_ms
FROM otel.default.http_server_request_duration
WHERE time > now() - interval '1 hour'
  AND "attrs.quantile" = '0.99'
GROUP BY minute, service
ORDER BY minute;
```

### Error Logs

```sql
SELECT time, severity_text, body, "resource.service.name" as service
FROM otel.default.otel_logs
WHERE severity_number >= 17  -- ERROR and above
  AND time > now() - interval '30 minutes'
ORDER BY time DESC;
```

### Slow Traces

```sql
SELECT
    trace_id,
    span_name,
    duration_ns / 1000000 as duration_ms,
    "resource.service.name" as service,
    status_code
FROM otel.default.otel_traces
WHERE time > now() - interval '1 hour'
  AND duration_ns > 1000000000  -- > 1 second
ORDER BY duration_ns DESC
LIMIT 20;
```

### Trace Waterfall

```sql
SELECT
    span_name,
    span_kind,
    duration_ns / 1000000 as duration_ms,
    time as start_time,
    parent_span_id
FROM otel.default.otel_traces
WHERE trace_id = 'abcdef1234567890abcdef1234567890'
ORDER BY time;
```

### Log-Trace Correlation

```sql
SELECT
    l.time, l.severity_text, l.body,
    t.span_name, t.duration_ns / 1000000 as span_ms
FROM otel.default.otel_logs l
JOIN otel.default.otel_traces t
  ON l.trace_id = t.trace_id AND l.span_id = t.span_id
WHERE l.time > now() - interval '15 minutes'
  AND l.severity_number >= 17
ORDER BY l.time DESC;
```

## SDK Direct Integration

Skip the Collector entirely by pointing application SDKs directly at MynaDB:

### Go

```go
import "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"

exporter, _ := otlpmetricgrpc.New(ctx,
    otlpmetricgrpc.WithEndpoint("mynadb-host:4317"),
    otlpmetricgrpc.WithInsecure(),
)
```

### Python

```python
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

exporter = OTLPMetricExporter(endpoint="mynadb-host:4317", insecure=True)
```

### Java

```java
OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
    .setEndpoint("http://mynadb-host:4317")
    .build();
```

## TLS Configuration

When MynaDB's TLS is enabled (`[tls]` section), the OTLP gRPC server also uses TLS:

```toml
[tls]
enabled = true
cert_file = "/etc/mynadb/tls/server.crt"
key_file = "/etc/mynadb/tls/server.key"
```

Configure your OTel Collector accordingly:

```yaml
exporters:
  otlp/mynadb:
    endpoint: "mynadb-host:4317"
    tls:
      insecure: false
      ca_file: /etc/otel/ca.crt
```

## Troubleshooting

### Data not appearing

1. Check OTLP is enabled: `enabled = true` in `[otlp]`
2. Check port 4317 is accessible: `nc -z mynadb-host 4317`
3. Check logs for ingestion: `grep "OTLP" logs/mynadb.log`
4. Wait for flush interval (default 60s) for data to appear in queries

### Wrong database/schema

By default, all OTLP data goes to `otel.default.*`. Change with:

```toml
[otlp]
database = "observability"
schema = "production"
```

### Attribute names not matching

When `flatten_attributes = true`, attributes are prefixed:
- Resource attributes: `resource.service.name`
- Data point attributes: `attrs.http.method`

Query with quoted identifiers:
```sql
SELECT "resource.service.name" FROM otel.default.otel_logs;
```
