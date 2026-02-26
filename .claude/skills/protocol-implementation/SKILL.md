---
name: protocol-implementation
description: This skill should be used when the user is adding new protocol support (HTTP endpoints, gRPC services, wire protocols), implementing ingestion formats, or working with request/response handling in src/services/. Also use when adding Prometheus, InfluxQL, or other compatibility layers.
version: 1.0.0
---

# Protocol Implementation Patterns in MynaDB

## Protocol Overview

| Protocol | Port | File | Pattern |
|----------|------|------|---------|
| HTTP REST | 7000 | `src/services/api.rs` | Axum handlers |
| Arrow Flight gRPC | 7001 | `src/services/flight_service.rs` | Tonic Flight trait |
| MySQL wire | 7002 | `src/services/mysql_service.rs` | msql-srv MysqlShim |
| Prometheus remote | 7000 | `src/services/prometheus/` | Protobuf + Snappy |
| InfluxQL compat | 7000 | `src/query/influxql/` | Lexer → Parser → SQL transpile |

## Shared State

All protocols share `AppState` (`src/services/api.rs`):
```rust
pub struct AppState {
    pub cache: DataCache,
    pub query_engine: Arc<QueryEngine>,
    pub db_manager: Arc<DatabaseManager>,
    pub compactor: Arc<Compactor>,
    pub wal: Option<Arc<Wal>>,
    pub config: Config,
    // Optional: metadata_manager, streaming_manager, user_store, etc.
}
```

Shared handler logic lives in `src/services/shared.rs` — reusable across standalone and data node modes.

## Adding an HTTP Endpoint

**File:** `src/services/api.rs`

```rust
// 1. Handler function
async fn handle_my_endpoint(
    Path(db): Path<String>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Business logic
    match do_work(&state, &db).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// 2. Register in build_router()
.route("/my-endpoint/:db", get(handle_my_endpoint))
```

**Return types:** `(StatusCode, String)` or `(StatusCode, Json<T>)`.

**Body extraction:** Use `extract_write_body()` for POST bodies with gzip support.

## Adding a gRPC Service

**File:** `src/services/flight_service.rs`

Flight service pattern:
```rust
pub struct StatsFlightService {
    query_engine: Arc<QueryEngine>,
    sessions: Arc<RwLock<HashMap<String, FlightSession>>>,
}

#[tonic::async_trait]
impl Flight for StatsFlightService {
    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let auth = self.get_auth_context(&request)?;
        let sql = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid SQL"))?;
        // Execute query, stream RecordBatches as FlightData
    }
}
```

**Error mapping:** Use `Status::unauthenticated()`, `Status::invalid_argument()`, `Status::internal()`.

## Adding a Wire Protocol

**File:** `src/services/mysql_service.rs`

MySQL uses `MysqlShim` trait from msql-srv:
```rust
impl<W: Read + Write> MysqlShim<W> for MysqlSession {
    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        // Bridge sync → async: runtime.block_on(async { ... })
        // Execute via QueryEngine
        // Write columns + rows to QueryResultWriter
    }
}
```

**Key:** MySQL protocol is synchronous — use `tokio::runtime::Handle::block_on()` to call async QueryEngine.

## Adding a Protobuf Protocol (Prometheus Pattern)

**Files:** `src/services/prometheus/remote_write.rs`, `remote_read.rs`, `types.rs`

```rust
// Write handler: snappy-compressed protobuf
async fn handle_remote_write(body: Bytes) -> impl IntoResponse {
    let decompressed = snap::raw::Decoder::new().decompress_vec(&body)?;
    let request = WriteRequest::decode(&decompressed[..])?;
    // Convert TimeSeries → Vec<Point>
    // cache.add_points()
    StatusCode::NO_CONTENT
}
```

**Pattern:** Decode protobuf → Convert to internal `Point` → Write to `DataCache`.

## InfluxQL Compatibility Layer

**Files:** `src/query/influxql/`

```
InfluxQL query → Lexer (tokens) → Parser (AST) → Transpiler (SQL) → DataFusion
```

- Metadata queries (SHOW *) are handled directly without SQL transpilation
- SELECT queries are transpiled to SQL and executed via DataFusion
- Response formatted as InfluxDB JSON: `{"results": [{"series": [...]}]}`

## Data Model Conversions

| External | Internal |
|----------|----------|
| Prometheus label `__name__` | measurement name |
| Prometheus labels | tags |
| Prometheus sample (ms) | timestamp (ns, × 1,000,000) |
| InfluxDB line protocol | `Point { measurement, tags, fields, timestamp }` |
| Arrow Flight ticket | SQL string bytes |

## Authentication Flow

All protocols follow the same pattern:
1. Extract credentials (header, handshake, or metadata)
2. Build `AuthContext` with user + permissions
3. Pass through to `execute_query_impl_auth()` variants
4. Check `Permission::Read` / `Permission::Write` / `Permission::Admin`

## Server Startup

**File:** `src/node/common.rs`

Servers are started via shared helpers:
```rust
// HTTP
start_http_server(config, app_state, shutdown).await;
// Flight
start_flight_server(config, flight_service, shutdown).await;
// MySQL
start_mysql_server(config, query_engine, shutdown).await;
```

Each node type (standalone, router, data) picks which servers to start.
