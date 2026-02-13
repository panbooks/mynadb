# Query Tracing & Profiling

Distributed tracing and slow query logging for production debugging and performance analysis.

## Features

### 1. OpenTelemetry Integration
- Full OpenTelemetry (OTLP) support for distributed tracing
- Configurable OTLP endpoint for exporting traces to Jaeger, Zipkin, etc.
- Automatic span creation for query execution phases
- Configurable sampling rate (0.0 to 1.0)

### 2. Query Timeline Tracking
- Detailed execution timeline for each query
- Tracks query execution phases:
  - Parse: SQL parsing
  - Analyze: Semantic analysis
  - Optimize (Logical): Logical plan optimization
  - Create Physical Plan: Physical plan creation
  - Optimize (Physical): Physical plan optimization
  - Execute: Query execution
- In-memory LRU cache of recent query timelines
- Unique query ID for each execution

### 3. Slow Query Logging
- Automatic detection of slow queries
- Configurable threshold (milliseconds)
- Logs to both structured logging and in-memory cache
- Includes query metadata:
  - Query text
  - Database name
  - Execution duration
  - Row count
  - Partitions scanned
  - Files read
  - Bytes scanned
  - Error information (if failed)

## Configuration

Add to your `config.toml`:

```toml
[tracing]
# Enable OpenTelemetry distributed tracing
enabled = false

# OTLP endpoint (e.g., "http://localhost:4317")
# Leave empty to disable OTLP export (still uses local tracing)
otlp_endpoint = "http://localhost:4317"

# Service name for tracing
service_name = "mynadb"

# Slow query threshold in milliseconds
slow_query_threshold_ms = 1000

# Enable slow query logging
slow_query_logging = true

# Enable query timeline tracking
timeline_enabled = true

# Maximum number of query timelines to keep in memory
max_timelines = 1000

# Sample rate for tracing (0.0 to 1.0)
sample_rate = 1.0
```

## API Endpoints

### GET /traces/recent
Get recent query timelines.

**Query Parameters:**
- `limit` (optional): Maximum number of timelines to return (default: 100)

**Example:**
```bash
curl "http://localhost:7000/traces/recent?limit=10"
```

**Response:**
```json
[
  {
    "query_id": "550e8400-e29b-41d4-a716-446655440000",
    "query": "SELECT * FROM cpu WHERE time > 1704067200000000000",
    "database": "mydb",
    "start_time": "2024-01-15T10:30:00Z",
    "total_duration_us": 125000,
    "events": [
      {
        "name": "parse",
        "timestamp": "2024-01-15T10:30:00.000Z",
        "duration_us": 1000
      },
      {
        "name": "analyze",
        "timestamp": "2024-01-15T10:30:00.001Z",
        "duration_us": 500
      },
      {
        "name": "optimize_logical",
        "timestamp": "2024-01-15T10:30:00.002Z",
        "duration_us": 2000
      },
      {
        "name": "create_physical_plan",
        "timestamp": "2024-01-15T10:30:00.004Z",
        "duration_us": 1500
      },
      {
        "name": "optimize_physical",
        "timestamp": "2024-01-15T10:30:00.006Z",
        "duration_us": 1000
      },
      {
        "name": "execute",
        "timestamp": "2024-01-15T10:30:00.007Z",
        "duration_us": 119000
      }
    ],
    "result_metadata": {
      "row_count": 1000,
      "partitions_scanned": 2,
      "files_read": 5,
      "bytes_scanned": 1024000
    }
  }
]
```

### GET /traces/:query_id
Get a specific query timeline by ID.

**Example:**
```bash
curl "http://localhost:7000/traces/550e8400-e29b-41d4-a716-446655440000"
```

### GET /traces/slow
Get recent slow queries.

**Query Parameters:**
- `limit` (optional): Maximum number of slow queries to return (default: 100)

**Example:**
```bash
curl "http://localhost:7000/traces/slow?limit=10"
```

**Response:**
```json
[
  {
    "timestamp": "2024-01-15T10:30:00Z",
    "query": "SELECT COUNT(*) FROM large_table",
    "database": "mydb",
    "duration_ms": 1500,
    "row_count": 1000000,
    "partitions_scanned": 10,
    "files_read": 50,
    "bytes_scanned": 10240000
  }
]
```

### POST /traces/clear
Clear all stored trace data.

**Example:**
```bash
curl -X POST "http://localhost:7000/traces/clear"
```

## Integration with Query Engine

The query tracer is automatically integrated into the query execution pipeline. When enabled, it:

1. Creates a unique query ID for each execution
2. Tracks each phase of query execution
3. Records timing information for each phase
4. Captures result metadata (row count, files read, etc.)
5. Logs slow queries that exceed the threshold
6. Stores timeline in memory for API access

## OpenTelemetry Export

When `otlp_endpoint` is configured, traces are automatically exported to the specified OTLP endpoint. This enables:

- Distributed tracing across services
- Integration with observability platforms (Jaeger, Zipkin, Grafana Tempo)
- Correlation of queries with application traces
- Production debugging and performance analysis

Example setup with Jaeger:

```bash
# Start Jaeger
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest

# Configure MynaDB
[tracing]
enabled = true
otlp_endpoint = "http://localhost:4317"
service_name = "mynadb"

# View traces at http://localhost:16686
```

## Performance Impact

- **Timeline tracking**: Minimal overhead (~1-2% query time)
- **Slow query logging**: Only triggered when threshold is exceeded
- **OpenTelemetry export**: Async export with configurable sampling
- **Memory usage**: LRU cache with configurable max entries

## Use Cases

1. **Production Debugging**
   - Identify slow queries in production
   - Understand query execution bottlenecks
   - Correlate queries with application performance issues

2. **Performance Analysis**
   - Analyze query execution patterns
   - Identify optimization opportunities
   - Track performance over time

3. **Distributed Tracing**
   - Trace queries across microservices
   - Correlate database operations with application traces
   - End-to-end request tracing

4. **Monitoring & Alerting**
   - Export metrics to monitoring systems
   - Alert on slow query patterns
   - Track query performance SLOs

## Example: Analyzing a Slow Query

```bash
# 1. Execute a query
curl "http://localhost:7000/query/mydb?sql=SELECT%20*%20FROM%20large_table"

# 2. Check slow queries
curl "http://localhost:7000/traces/slow?limit=1" | python3 -m json.tool

# Output shows:
# - Query took 2500ms (exceeded 1000ms threshold)
# - Scanned 100 partitions
# - Read 500 files
# - Processed 50GB of data

# 3. Get detailed timeline
curl "http://localhost:7000/traces/<query_id>" | python3 -m json.tool

# Output shows execution phase breakdown:
# - Parse: 1ms
# - Analyze: 0.5ms
# - Optimize (Logical): 2ms
# - Create Physical Plan: 1.5ms
# - Optimize (Physical): 1ms
# - Execute: 2494ms (bottleneck!)

# 4. Optimize the query based on insights
# - Add partition pruning filters
# - Use time bucket aggregation
# - Enable result caching
```

## Architecture

```
Query Execution
    ↓
QueryTracer (if enabled)
    ↓
┌─────────────────┬──────────────────┬────────────────┐
│ TimelineBuilder │ SlowQueryLogger  │ OpenTelemetry  │
│   (in-memory)   │   (in-memory)    │   (export)     │
└─────────────────┴──────────────────┴────────────────┘
         ↓                ↓                    ↓
    Timeline API    Slow Query API       OTLP Endpoint
    /traces/*       /traces/slow         (Jaeger/etc)
```

## Module Structure

```
src/observability/
├── mod.rs                    # Module exports
├── config.rs                 # Configuration structs
├── timeline.rs               # Query timeline tracking
├── slow_query_logger.rs      # Slow query detection & logging
├── query_tracer.rs          # Main tracer orchestrator
└── init.rs                   # OpenTelemetry initialization
```

## Testing

Run the tracing tests:

```bash
# Unit tests
cargo test --lib observability

# Integration test
./scripts/test_tracing.sh
```

## Limitations

- Timelines are stored in-memory only (not persisted)
- LRU eviction when max_timelines is reached
- OpenTelemetry export requires external OTLP endpoint
- Sampling may miss some traces in high-throughput scenarios

## Future Enhancements

- [ ] Persistent timeline storage (optional)
- [ ] Query fingerprinting and aggregation
- [ ] Per-table/per-query tracing policies
- [ ] Automatic slow query recommendations
- [ ] Integration with Prometheus metrics
- [ ] Query plan caching based on fingerprints
