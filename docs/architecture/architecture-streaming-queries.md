# Streaming Queries Architecture

## Overview

Streaming Queries provide push-based, real-time query result delivery via WebSocket and Server-Sent Events (SSE). Instead of polling, clients subscribe to a SQL query and receive automatic updates whenever new data is written to the referenced tables.

## Design Goals

1. **Zero-polling**: Eliminate repetitive query polling for dashboards and monitors
2. **Protocol choice**: WebSocket for bidirectional (multi-subscription), SSE for simple HTTP streaming
3. **Write-path integration**: Notifications triggered directly from the data ingestion pipeline
4. **Backpressure**: Graceful degradation when consumers are slower than producers
5. **Backward compatible**: Disabled by default; zero overhead when not enabled

## Architecture Diagram

```
                    ┌──────────────────────────────────────────────────┐
                    │               CLIENT CONNECTIONS                  │
                    │                                                   │
  WebSocket ────────┼──► ws_stream_query_handler                       │
  (bidirectional)   │         │                                        │
                    │         ▼                                        │
                    │    Parse StreamQueryRequest ──► subscribe()       │
                    │         │                           │             │
                    │         ▼                           ▼             │
                    │    Spawn per-subscription    StreamingQueryManager│
                    │    forwarder task                   │             │
                    │                                    │             │
  SSE ──────────────┼──► sse_stream_query_handler        │             │
  (unidirectional)  │         │                          │             │
                    │         ▼                          │             │
                    │    subscribe() ─────────────►     │             │
                    │    ReceiverStream → Sse            │             │
                    └───────────────────────────────────┼──────────────┘
                                                        │
                    ┌───────────────────────────────────┼──────────────┐
                    │           STREAMING QUERY MANAGER  │              │
                    │                                    ▼              │
                    │    DashMap<Uuid, Subscription>                    │
                    │         │                                        │
                    │         │  subscribe(db, sql, auth_ctx)          │
                    │         │    1. Check global/per-conn limits     │
                    │         │    2. Check auth (database access)     │
                    │         │    3. Extract table references         │
                    │         │    4. Execute initial query            │
                    │         │    5. Create mpsc channel              │
                    │         │    6. Store subscription               │
                    │         │    7. Return SubscriptionHandle        │
                    │         │                                        │
                    │         │  on_data_written(WriteEvent)           │
                    │         │    1. Find affected subscriptions      │
                    │         │       (match db + table)               │
                    │         │    2. Re-execute each query            │
                    │         │    3. try_send (backpressure)          │
                    │         │                                        │
                    │         │  unsubscribe(id)                       │
                    │         │    Remove from DashMap                 │
                    └─────────┴────────────────────────────────────────┘

                    ┌──────────────────────────────────────────────────┐
                    │              WRITE PATH INTEGRATION               │
                    │                                                   │
  POST /write/:db ──┼──► process_write_data()                          │
                    │         │                                        │
                    │         ▼                                        │
                    │    shared::process_write_impl()                  │
                    │         │                                        │
                    │         ├─► WAL write                            │
                    │         ├─► Parse line protocol                  │
                    │         ├─► Group by (schema, table)             │
                    │         ├─► cache.add_points()                   │
                    │         └─► Return WriteResult {                 │
                    │               point_count,                       │
                    │               tables: [(schema, table), ...]     │
                    │             }                                    │
                    │                    │                              │
                    │                    ▼                              │
                    │    tokio::spawn (async, non-blocking)            │
                    │         │                                        │
                    │         ├─► Sleep(debounce_ms)                   │
                    │         └─► streaming_mgr.on_data_written()      │
                    │              for each (schema, table)            │
                    └──────────────────────────────────────────────────┘
```

## Component Details

### StreamingQueryManager (`src/query/streaming.rs`)

The central coordinator for all streaming subscriptions.

| Field | Type | Purpose |
|-------|------|---------|
| `subscriptions` | `DashMap<Uuid, Subscription>` | Lock-free concurrent subscription store |
| `query_engine` | `Arc<QueryEngine>` | Re-executes queries on write notifications |
| `config` | `StreamingConfig` | Limits, debounce, backpressure settings |
| `total_count` | `AtomicUsize` | Fast global subscription count |

### Subscription (internal)

Each subscription tracks:
- `id: Uuid` — Unique identifier
- `query: String` — The SQL query to re-execute
- `database: String` — Target database/catalog
- `tables: Vec<String>` — Extracted table references (for write filtering)
- `sender: mpsc::Sender<StreamMessage>` — Channel to push results to the client

### StreamMessage (wire format)

```json
// Initial result (sent once on subscribe)
{"type": "initial", "data": "[{\"time\":...,\"cpu\":85.2}, ...]"}

// Update (sent on each relevant write)
{"type": "update", "data": "[{\"time\":...,\"cpu\":91.0}, ...]"}

// Error
{"type": "error", "message": "Access denied to database: restricted_db"}

// Heartbeat (WebSocket only, every N seconds)
{"type": "heartbeat", "timestamp": "2026-02-07T12:00:00Z"}
```

The `data` field contains a JSON array string (the raw query result), avoiding double-serialization overhead.

## Backpressure Strategy

```
Producer (write path)                    Consumer (WebSocket/SSE client)
        │                                         │
        ▼                                         │
  on_data_written()                               │
        │                                         │
        ▼                                         │
  try_send(Update)  ──── channel buffer ────►  recv()
        │                (backpressure_threshold)  │
        │                                         │
   Buffer full?                                   │
   ├─ No  → message queued                       │
   └─ Yes → message DROPPED (warn log)           │
             Consumer too slow                     │
```

- Uses `mpsc::channel(backpressure_threshold)` (default: 1000 messages)
- `try_send()` is non-blocking — never stalls the write path
- Slow consumers miss updates but never block other subscribers or writers

## Debounce Mechanism

Rapid successive writes (e.g., batch ingestion) are debounced:

```
Write 1 (t=0ms)  ──┐
Write 2 (t=10ms) ──┤──► Sleep(100ms) ──► on_data_written() [once]
Write 3 (t=50ms) ──┘
```

The debounce is per write request (not per-subscription), controlled by `update_debounce_ms` (default: 100ms).

## Connection Lifecycle

### WebSocket

```
Client ──► WS Upgrade /stream/query
           │
           ├─► Spawn heartbeat task (every 30s)
           ├─► Spawn send task (ws_tx → WebSocket)
           │
           ▼ (receive loop)
     ◄──── {"database":"mydb","query":"SELECT * FROM cpu"}
     ────► {"type":"subscribed","subscription_id":"..."}
     ────► {"type":"initial","data":"[...]"}
     ────► {"type":"update","data":"[...]"}   (on writes)
     ────► {"type":"heartbeat","timestamp":"..."}
           ...
     ◄──── Close frame
           │
           ├─► Abort heartbeat + send tasks
           └─► Unsubscribe all subscriptions for this connection
```

### SSE

```
Client ──► GET /stream/query/mydb?sql=SELECT+*+FROM+cpu
           │
           ├─► subscribe() → SubscriptionHandle
           ├─► Wrap in ReceiverStream → Sse response
           │
           ▼ (event stream)
     ────► data: {"type":"initial","data":"[...]"}
     ────► data: {"type":"update","data":"[...]"}
           ...
     ◄──── Client disconnect
           │
           └─► StreamWithCleanup::drop() → unsubscribe()
```

## Authentication Integration

- WebSocket: `AuthContext` extracted from request extensions (set by auth middleware)
- SSE: Same `AuthContext` extraction
- `subscribe()` checks `auth_ctx.can_access_database(db)` before allowing subscription
- Initial query uses `execute_query_json_with_db_auth()` for table-level access control
- When auth is disabled, `AuthContext::anonymous()` permits everything

## Configuration

```toml
[streaming]
enabled = true                        # Default: false
max_subscriptions = 10000             # Global limit across all connections
max_subscriptions_per_connection = 10 # Per WebSocket connection limit
update_debounce_ms = 100              # Batch write notifications within window
backpressure_threshold = 1000         # Channel buffer before dropping updates
heartbeat_interval_secs = 30          # WebSocket keepalive interval
```

## File Map

| File | Responsibility |
|------|----------------|
| `src/query/streaming.rs` | StreamingQueryManager, Subscription, StreamMessage, WriteEvent |
| `src/config.rs` | StreamingConfig struct and defaults |
| `src/services/api.rs` | WebSocket handler, SSE handler, subscription list endpoint |
| `src/services/shared.rs` | WriteResult return type from process_write_impl |
| `src/node/standalone.rs` | StreamingQueryManager initialization |
| `src/query/mod.rs` | Module registration |

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Subscription lookup | O(1) via DashMap |
| Write notification overhead | O(n) where n = affected subscriptions |
| Write path blocking | None (async spawn + debounce) |
| Memory per subscription | ~200 bytes + channel buffer |
| Max concurrent subscriptions | 10,000 (configurable) |
