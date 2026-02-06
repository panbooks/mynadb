# Streaming Queries: WebSocket & Server-Sent Events Internals

**Document Type:** Architecture Deep Dive
**Date:** 2026-02-06
**Status:** Design Document

---

## Table of Contents

1. [Overview](#overview)
2. [WebSocket Fundamentals](#websocket-fundamentals)
3. [Server-Sent Events (SSE) Fundamentals](#server-sent-events-sse-fundamentals)
4. [Architecture Design](#architecture-design)
5. [Implementation Details](#implementation-details)
6. [Data Flow](#data-flow)
7. [Performance Optimization](#performance-optimization)
8. [Comparison: WebSocket vs SSE](#comparison-websocket-vs-sse)
9. [Production Considerations](#production-considerations)

---

## Overview

**Goal:** Enable real-time query result streaming to clients for monitoring dashboards, alerting systems, and live data visualization.

**Use Cases:**
- Grafana dashboards showing live CPU/memory metrics
- Alerting systems monitoring threshold breaches
- Real-time log tailing
- Live IoT sensor data visualization

**Core Concept:**
```
Write → In-Memory Cache → Trigger Subscriptions → Push to Clients
```

Traditional polling:
```
Client ─(poll every 5s)─→ Server ─(query DB)─→ Response
        ←─(data)─────────        ←──────────
```

Streaming (push-based):
```
Client ──(subscribe)──→ Server ──(store subscription)──→ Subscription Manager
                                                              │
                                                              ↓
New Data Written → Cache → Notify Manager → Filter Subscriptions → Push to Clients
```

---

## WebSocket Fundamentals

### What is WebSocket?

WebSocket is a **full-duplex, bidirectional communication protocol** over a single TCP connection.

**HTTP vs WebSocket:**

```
HTTP:
Client ────Request───→ Server
Client ←───Response─── Server
(Connection closes)

WebSocket:
Client ────Upgrade Request───→ Server
Client ←───101 Switching Protocols─── Server
Client ↔──────Bidirectional Messages──────↔ Server
(Connection stays open)
```

### WebSocket Handshake

**Step 1: HTTP Upgrade Request**
```http
GET /stream/query HTTP/1.1
Host: mynadb.example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

**Step 2: Server Response**
```http
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
```

**Step 3: Connection Established**
- TCP connection remains open
- Both client and server can send messages at any time
- Messages are framed (not plain HTTP)

### WebSocket Frame Structure

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
|N|V|V|V|       |S|             |   (if payload len==126/127)   |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
|     Extended payload length continued, if payload len == 127  |
+ - - - - - - - - - - - - - - - +-------------------------------+
|                               |Masking-key, if MASK set to 1  |
+-------------------------------+-------------------------------+
| Masking-key (continued)       |          Payload Data         |
+-------------------------------- - - - - - - - - - - - - - - - +
:                     Payload Data continued ...                :
+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
|                     Payload Data continued ...                |
+---------------------------------------------------------------+
```

**Frame Types (opcodes):**
- `0x0` - Continuation frame
- `0x1` - Text frame
- `0x2` - Binary frame
- `0x8` - Close frame
- `0x9` - Ping frame
- `0xA` - Pong frame

---

## Server-Sent Events (SSE) Fundamentals

### What is SSE?

SSE is a **unidirectional, server-to-client push protocol** over HTTP.

**Key Characteristics:**
- Built on HTTP (no special protocol)
- Server pushes, client receives only
- Text-based (UTF-8)
- Automatic reconnection
- Event IDs for resumption

### SSE Connection Flow

**Step 1: Client Request**
```http
GET /stream/query/mydb?sql=SELECT+*+FROM+cpu HTTP/1.1
Host: mynadb.example.com
Accept: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

**Step 2: Server Response (streaming)**
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
Transfer-Encoding: chunked

data: {"time": 1704067200000000000, "cpu": 45.2}

data: {"time": 1704067205000000000, "cpu": 48.7}

event: heartbeat
data: {"status": "alive"}

id: 1234567890
event: update
data: {"time": 1704067210000000000, "cpu": 52.3}

```

### SSE Event Format

```
field: value\n
field: value\n
\n  (double newline = end of event)
```

**Fields:**
- `event`: Event type (optional, defaults to "message")
- `data`: Event payload (can span multiple lines)
- `id`: Event ID for resumption
- `retry`: Reconnection time in milliseconds

**Multi-line data:**
```
data: {
data:   "time": 1704067200000000000,
data:   "cpu": 45.2
data: }

```

---

## Architecture Design

### Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Browser     │  │  Grafana     │  │  Mobile App  │          │
│  │  (SSE)       │  │  (WebSocket) │  │  (WebSocket) │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
└─────────┼──────────────────┼──────────────────┼─────────────────┘
          │                  │                  │
          ├──────────────────┴──────────────────┤
          │                                     │
          ▼                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Protocol Handler Layer                      │
│  ┌─────────────────────┐       ┌─────────────────────┐          │
│  │  SSE Handler        │       │  WebSocket Handler  │          │
│  │  - HTTP streaming   │       │  - Bidirectional    │          │
│  │  - Auto-reconnect   │       │  - Binary support   │          │
│  │  - Event IDs        │       │  - Ping/Pong        │          │
│  └──────────┬──────────┘       └──────────┬──────────┘          │
└─────────────┼──────────────────────────────┼─────────────────────┘
              │                              │
              └──────────────┬───────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Subscription Manager                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Subscription Registry                                    │   │
│  │  - Active subscriptions: DashMap<Uuid, Subscription>     │   │
│  │  - Table index: HashMap<TableRef, Vec<Uuid>>             │   │
│  │  - Query cache: LRU<QueryHash, Vec<Uuid>>                │   │
│  └────────────────────────┬──────────────────────────────────┘   │
│                           │                                      │
│  ┌────────────────────────▼──────────────────────────────────┐   │
│  │  Notification Dispatcher                                  │   │
│  │  - Match writes to subscriptions                          │   │
│  │  - Execute queries                                        │   │
│  │  - Push results to clients                                │   │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────┬───────────────────────────────────────────────────┘
              │
              │ (listens for write events)
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Write Path                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Line Protocol Ingestion                                  │   │
│  │  ────→ DataCache (in-memory)                             │   │
│  │  ────→ Emit WriteEvent(catalog, schema, table, timestamp)│   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Core Data Structures

```rust
// Subscription represents a single streaming query
pub struct Subscription {
    id: Uuid,
    query: StreamQuery,
    sender: SubscriptionSender,
    created_at: Instant,
    last_activity: AtomicInstant,
    metadata: SubscriptionMetadata,
}

pub struct StreamQuery {
    database: String,
    sql: String,
    parsed_plan: Option<LogicalPlan>,
    affected_tables: Vec<TableRef>,
    filters: QueryFilters,
}

pub struct QueryFilters {
    time_range: Option<TimeRange>,
    predicates: Vec<Expr>,
}

pub enum SubscriptionSender {
    WebSocket {
        tx: mpsc::UnboundedSender<Message>,
        socket_id: Uuid,
    },
    ServerSentEvents {
        tx: mpsc::Sender<Event>,
        last_event_id: AtomicU64,
    },
}

pub struct SubscriptionMetadata {
    client_ip: IpAddr,
    user_agent: Option<String>,
    authenticated_user: Option<String>,
}
```

---

## Implementation Details

### 1. WebSocket Implementation (using `axum` + `tokio-tungstenite`)

```rust
// src/services/streaming/websocket.rs
use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::IntoResponse,
};
use tokio::sync::mpsc;

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket(socket, state))
}

async fn handle_websocket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Spawn task to send messages to client
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Handle incoming messages from client
    let recv_task = tokio::spawn(async move {
        let mut active_subscriptions = Vec::new();

        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => {
                    match serde_json::from_str::<ClientMessage>(&text) {
                        Ok(ClientMessage::Subscribe { database, query }) => {
                            // Create subscription
                            let subscription_id = state.streaming_manager
                                .subscribe(database, query, SubscriptionSender::WebSocket {
                                    tx: tx.clone(),
                                    socket_id: Uuid::new_v4(),
                                })
                                .await;

                            match subscription_id {
                                Ok(id) => {
                                    active_subscriptions.push(id);

                                    // Send initial query results
                                    let initial_results = state.query_engine
                                        .execute(&database, &query)
                                        .await
                                        .unwrap();

                                    let response = ServerMessage::Initial {
                                        subscription_id: id,
                                        data: serialize_results(&initial_results),
                                    };

                                    let _ = tx.send(Message::Text(
                                        serde_json::to_string(&response).unwrap()
                                    ));
                                }
                                Err(e) => {
                                    let error = ServerMessage::Error {
                                        message: format!("Subscription failed: {}", e),
                                    };
                                    let _ = tx.send(Message::Text(
                                        serde_json::to_string(&error).unwrap()
                                    ));
                                }
                            }
                        }
                        Ok(ClientMessage::Unsubscribe { subscription_id }) => {
                            state.streaming_manager
                                .unsubscribe(subscription_id)
                                .await;
                            active_subscriptions.retain(|&id| id != subscription_id);
                        }
                        Ok(ClientMessage::Ping) => {
                            let _ = tx.send(Message::Pong(vec![]));
                        }
                        Err(e) => {
                            error!("Failed to parse client message: {}", e);
                        }
                    }
                }
                Message::Binary(data) => {
                    // Handle binary messages (e.g., compressed queries)
                    warn!("Binary messages not implemented yet");
                }
                Message::Ping(payload) => {
                    let _ = tx.send(Message::Pong(payload));
                }
                Message::Close(_) => {
                    info!("Client closed WebSocket connection");
                    break;
                }
                _ => {}
            }
        }

        // Cleanup subscriptions when connection closes
        for subscription_id in active_subscriptions {
            state.streaming_manager.unsubscribe(subscription_id).await;
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    Subscribe {
        database: String,
        query: String,
    },
    Unsubscribe {
        subscription_id: Uuid,
    },
    Ping,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum ServerMessage {
    Initial {
        subscription_id: Uuid,
        data: Vec<serde_json::Value>,
    },
    Update {
        subscription_id: Uuid,
        data: Vec<serde_json::Value>,
    },
    Error {
        message: String,
    },
}
```

**Client-Side WebSocket (JavaScript):**
```javascript
const ws = new WebSocket('ws://mynadb.example.com/stream/query');

ws.onopen = () => {
    console.log('WebSocket connected');

    // Subscribe to query
    ws.send(JSON.stringify({
        type: 'Subscribe',
        database: 'mydb',
        query: 'SELECT * FROM cpu WHERE host = "server01"'
    }));
};

ws.onmessage = (event) => {
    const message = JSON.parse(event.data);

    switch (message.type) {
        case 'Initial':
            console.log('Initial data:', message.data);
            updateChart(message.data);
            break;
        case 'Update':
            console.log('New data:', message.data);
            appendToChart(message.data);
            break;
        case 'Error':
            console.error('Error:', message.message);
            break;
    }
};

ws.onerror = (error) => {
    console.error('WebSocket error:', error);
};

ws.onclose = () => {
    console.log('WebSocket disconnected');
    // Implement reconnection logic
    setTimeout(() => connectWebSocket(), 5000);
};

// Heartbeat to keep connection alive
setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'Ping' }));
    }
}, 30000);
```

---

### 2. Server-Sent Events Implementation

```rust
// src/services/streaming/sse.rs
use axum::{
    response::sse::{Event, Sse},
    extract::{Query, State},
};
use futures::stream::{Stream, StreamExt};
use std::convert::Infallible;

pub async fn sse_handler(
    Query(params): Query<SseQueryParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let database = params.db;
    let query = params.sql;

    // Create a channel for events
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    // Subscribe to streaming query
    let subscription_id = state.streaming_manager
        .subscribe(
            database.clone(),
            query.clone(),
            SubscriptionSender::ServerSentEvents {
                tx: tx.clone(),
                last_event_id: AtomicU64::new(0),
            },
        )
        .await?;

    // Send initial query results
    tokio::spawn({
        let state = state.clone();
        let database = database.clone();
        let query = query.clone();
        let tx = tx.clone();

        async move {
            match state.query_engine.execute(&database, &query).await {
                Ok(results) => {
                    let json = serialize_results(&results);
                    let event = Event::default()
                        .event("initial")
                        .id(0.to_string())
                        .data(json);
                    let _ = tx.send(event).await;
                }
                Err(e) => {
                    let event = Event::default()
                        .event("error")
                        .data(format!("Query failed: {}", e));
                    let _ = tx.send(event).await;
                }
            }
        }
    });

    // Create event stream
    let stream = async_stream::stream! {
        let mut event_id = 0u64;

        // Send keepalive events every 15 seconds
        let mut keepalive_interval = tokio::time::interval(Duration::from_secs(15));

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    event_id += 1;
                    yield Ok(event);
                }
                _ = keepalive_interval.tick() => {
                    let keepalive = Event::default()
                        .event("keepalive")
                        .comment("Connection alive");
                    yield Ok(keepalive);
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive")
    ))
}

#[derive(Deserialize)]
pub struct SseQueryParams {
    db: String,
    sql: String,
    #[serde(default)]
    last_event_id: Option<String>,
}
```

**Client-Side SSE (JavaScript):**
```javascript
const eventSource = new EventSource(
    'http://mynadb.example.com/stream/query/mydb?sql=' +
    encodeURIComponent('SELECT * FROM cpu WHERE host = "server01"')
);

// Initial data
eventSource.addEventListener('initial', (event) => {
    const data = JSON.parse(event.data);
    console.log('Initial data:', data);
    updateChart(data);
});

// Updates
eventSource.addEventListener('update', (event) => {
    const data = JSON.parse(event.data);
    console.log('New data:', data);
    appendToChart(data);
});

// Error handling
eventSource.addEventListener('error', (event) => {
    const message = event.data;
    console.error('Error:', message);
});

eventSource.onerror = (error) => {
    console.error('SSE connection error:', error);
    // EventSource automatically reconnects
};

// Cleanup
window.addEventListener('beforeunload', () => {
    eventSource.close();
});
```

---

### 3. Subscription Manager

This is the heart of the streaming system.

```rust
// src/streaming/manager.rs
use dashmap::DashMap;
use tokio::sync::{mpsc, RwLock};

pub struct StreamingManager {
    // All active subscriptions
    subscriptions: Arc<DashMap<Uuid, Subscription>>,

    // Index: TableRef -> List of subscription IDs
    table_index: Arc<DashMap<TableRef, Vec<Uuid>>>,

    // Query engine for executing queries
    query_engine: Arc<QueryEngine>,

    // Write event receiver
    write_events: Arc<RwLock<broadcast::Receiver<WriteEvent>>>,

    // Configuration
    config: StreamingConfig,
}

#[derive(Clone)]
pub struct StreamingConfig {
    pub max_subscriptions_per_connection: usize,
    pub max_total_subscriptions: usize,
    pub update_debounce_ms: u64,
    pub backpressure_threshold: usize,
}

pub struct WriteEvent {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub timestamp: i64,
    pub row_count: usize,
}

impl StreamingManager {
    pub fn new(
        query_engine: Arc<QueryEngine>,
        write_events: broadcast::Receiver<WriteEvent>,
        config: StreamingConfig,
    ) -> Self {
        let manager = Self {
            subscriptions: Arc::new(DashMap::new()),
            table_index: Arc::new(DashMap::new()),
            query_engine,
            write_events: Arc::new(RwLock::new(write_events)),
            config,
        };

        // Start background task to listen for write events
        manager.start_event_listener();

        manager
    }

    pub async fn subscribe(
        &self,
        database: String,
        sql: String,
        sender: SubscriptionSender,
    ) -> Result<Uuid> {
        // Check subscription limits
        if self.subscriptions.len() >= self.config.max_total_subscriptions {
            return Err(Error::TooManySubscriptions);
        }

        // Parse query to extract affected tables
        let stream_query = self.parse_stream_query(database, sql).await?;

        // Create subscription
        let subscription = Subscription {
            id: Uuid::new_v4(),
            query: stream_query.clone(),
            sender,
            created_at: Instant::now(),
            last_activity: AtomicInstant::new(Instant::now()),
            metadata: SubscriptionMetadata::default(),
        };

        let id = subscription.id;

        // Register in main map
        self.subscriptions.insert(id, subscription);

        // Index by affected tables
        for table in &stream_query.affected_tables {
            self.table_index
                .entry(table.clone())
                .or_insert_with(Vec::new)
                .push(id);
        }

        info!("Created subscription {}: {}", id, stream_query.sql);

        Ok(id)
    }

    pub async fn unsubscribe(&self, subscription_id: Uuid) {
        if let Some((_, subscription)) = self.subscriptions.remove(&subscription_id) {
            // Remove from table index
            for table in &subscription.query.affected_tables {
                if let Some(mut subs) = self.table_index.get_mut(table) {
                    subs.retain(|&id| id != subscription_id);
                }
            }

            info!("Removed subscription {}", subscription_id);
        }
    }

    fn start_event_listener(&self) {
        let subscriptions = self.subscriptions.clone();
        let table_index = self.table_index.clone();
        let query_engine = self.query_engine.clone();
        let mut write_events = self.write_events.blocking_write().resubscribe();
        let debounce_duration = Duration::from_millis(self.config.update_debounce_ms);

        tokio::spawn(async move {
            let mut pending_notifications = HashMap::<Uuid, Instant>::new();

            loop {
                tokio::select! {
                    Ok(event) = write_events.recv() => {
                        let table_ref = TableRef {
                            catalog: event.catalog,
                            schema: event.schema,
                            table: event.table,
                        };

                        // Find affected subscriptions
                        if let Some(subscription_ids) = table_index.get(&table_ref) {
                            for &sub_id in subscription_ids.iter() {
                                // Debounce: only notify once per debounce window
                                let should_notify = pending_notifications
                                    .get(&sub_id)
                                    .map(|last_notify| last_notify.elapsed() >= debounce_duration)
                                    .unwrap_or(true);

                                if should_notify {
                                    pending_notifications.insert(sub_id, Instant::now());

                                    // Spawn task to execute query and push results
                                    let subscriptions = subscriptions.clone();
                                    let query_engine = query_engine.clone();

                                    tokio::spawn(async move {
                                        if let Some(subscription) = subscriptions.get(&sub_id) {
                                            Self::notify_subscription(
                                                &subscription,
                                                &query_engine,
                                            ).await;
                                        }
                                    });
                                }
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Cleanup old pending notifications
                        pending_notifications.retain(|_, instant| {
                            instant.elapsed() < debounce_duration
                        });
                    }
                }
            }
        });
    }

    async fn notify_subscription(
        subscription: &Subscription,
        query_engine: &QueryEngine,
    ) {
        // Execute query
        let results = match query_engine
            .execute(&subscription.query.database, &subscription.query.sql)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Query failed for subscription {}: {}", subscription.id, e);
                return;
            }
        };

        // Serialize results
        let json_data = match serialize_results(&results) {
            Ok(j) => j,
            Err(e) => {
                warn!("Failed to serialize results: {}", e);
                return;
            }
        };

        // Push to client
        match &subscription.sender {
            SubscriptionSender::WebSocket { tx, .. } => {
                let message = ServerMessage::Update {
                    subscription_id: subscription.id,
                    data: json_data,
                };

                let json = serde_json::to_string(&message).unwrap();

                if tx.send(Message::Text(json)).is_err() {
                    warn!("Failed to send WebSocket message, client may have disconnected");
                }
            }
            SubscriptionSender::ServerSentEvents { tx, last_event_id } => {
                let event_id = last_event_id.fetch_add(1, Ordering::SeqCst);

                let event = Event::default()
                    .event("update")
                    .id(event_id.to_string())
                    .data(serde_json::to_string(&json_data).unwrap());

                if tx.send(event).await.is_err() {
                    warn!("Failed to send SSE event, client may have disconnected");
                }
            }
        }

        // Update last activity
        subscription.last_activity.store(Instant::now());
    }

    async fn parse_stream_query(
        &self,
        database: String,
        sql: String,
    ) -> Result<StreamQuery> {
        // Parse SQL to extract table references
        let parsed = self.query_engine.parse_sql(&sql).await?;
        let affected_tables = extract_tables_from_plan(&parsed)?;

        Ok(StreamQuery {
            database,
            sql,
            parsed_plan: Some(parsed),
            affected_tables,
            filters: QueryFilters::default(),
        })
    }
}

fn extract_tables_from_plan(plan: &LogicalPlan) -> Result<Vec<TableRef>> {
    let mut tables = Vec::new();

    plan.visit(&mut |node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.push(TableRef {
                catalog: scan.table_name.catalog().unwrap_or("default").to_string(),
                schema: scan.table_name.schema().unwrap_or("default").to_string(),
                table: scan.table_name.table().to_string(),
            });
        }
        Ok(())
    })?;

    Ok(tables)
}
```

---

### 4. Write Path Integration

Emit events when data is written:

```rust
// src/storage/cache.rs
use tokio::sync::broadcast;

pub struct DataCache {
    // ... existing fields
    write_event_tx: broadcast::Sender<WriteEvent>,
}

impl DataCache {
    pub async fn write_line_protocol(
        &self,
        database: &str,
        measurement: &str,
        data: &str,
    ) -> Result<()> {
        // Parse line protocol
        let points = self.parse_line_protocol(data)?;

        // Write to in-memory cache
        self.append_to_cache(database, measurement, points.clone()).await?;

        // Write to WAL
        self.wal.write(database, measurement, data).await?;

        // Emit write event for streaming subscriptions
        let event = WriteEvent {
            catalog: database.to_string(),
            schema: "default".to_string(),  // Or extract from service tag
            table: measurement.to_string(),
            timestamp: Utc::now().timestamp_nanos(),
            row_count: points.len(),
        };

        // Broadcast to all listeners (non-blocking)
        let _ = self.write_event_tx.send(event);

        Ok(())
    }
}
```

---

## Data Flow

### Complete Flow: Write → Subscription → Client

```
┌─────────────────────────────────────────────────────────────────────┐
│ Step 1: Client Subscribes                                           │
└─────────────────────────────────────────────────────────────────────┘

Client                                   Server
  │                                        │
  │ ─────── WebSocket Upgrade ────────→   │
  │ ←────── 101 Switching Protocols ─────  │
  │                                        │
  │ ─────── Subscribe {db, query} ──────→  │
  │                                        │
  │                                   ┌────▼─────┐
  │                                   │ Streaming│
  │                                   │ Manager  │
  │                                   └────┬─────┘
  │                                        │
  │                                   1. Parse query
  │                                   2. Extract tables
  │                                   3. Create subscription
  │                                   4. Register in table index
  │                                        │
  │ ←────── Initial Results ─────────────  │
  │                                        │

┌─────────────────────────────────────────────────────────────────────┐
│ Step 2: Data Written                                                │
└─────────────────────────────────────────────────────────────────────┘

Writer                Write Path              Subscription Manager
  │                      │                            │
  │ ─── POST /write ───→ │                            │
  │                      │                            │
  │                  ┌───▼────┐                       │
  │                  │  Line  │                       │
  │                  │Protocol│                       │
  │                  │ Parser │                       │
  │                  └───┬────┘                       │
  │                      │                            │
  │                  ┌───▼────┐                       │
  │                  │  Data  │                       │
  │                  │  Cache │                       │
  │                  └───┬────┘                       │
  │                      │                            │
  │                      │ Emit WriteEvent            │
  │                      ├────────────────────────────→
  │                      │                       ┌────▼────┐
  │                      │                       │  Event  │
  │                      │                       │ Listener│
  │                      │                       └────┬────┘
  │                      │                            │
  │                      │                       1. Lookup affected
  │                      │                          subscriptions
  │                      │                       2. Check debounce
  │                      │                       3. Spawn notify tasks
  │                      │                            │

┌─────────────────────────────────────────────────────────────────────┐
│ Step 3: Notify Subscriptions                                        │
└─────────────────────────────────────────────────────────────────────┘

Subscription Manager          Query Engine           Client
       │                            │                   │
       │ ──── Execute Query ──────→ │                   │
       │                            │                   │
       │                       ┌────▼────┐              │
       │                       │DataFusion              │
       │                       │ Execute │              │
       │                       └────┬────┘              │
       │                            │                   │
       │ ←────── Results ──────────  │                   │
       │                                                 │
   ┌───▼────┐                                            │
   │Serialize                                            │
   │ Results│                                            │
   └───┬────┘                                            │
       │                                                 │
       │ ──────────── Push Update ────────────────────→  │
       │                                            ┌────▼────┐
       │                                            │ Browser │
       │                                            │ Updates │
       │                                            │  Chart  │
       │                                            └─────────┘
```

### Detailed Sequence Diagram

```
Client          WebSocket Handler    Subscription Manager    Write Path    Query Engine
  │                    │                      │                   │              │
  │ ── Subscribe ────→ │                      │                   │              │
  │                    │ ── Create Sub ─────→ │                   │              │
  │                    │                      │ ── Index Table ─→ │              │
  │                    │ ←── Sub ID ──────────│                   │              │
  │                    │ ─────────────────────│── Execute Query ─→│              │
  │                    │ ←────────────────────│←─── Results ──────│              │
  │ ←─ Initial Data ── │                      │                   │              │
  │                    │                      │                   │              │
  ⋮                    ⋮                      ⋮                   ⋮              ⋮
  │                    │                      │                   │              │
  │                    │                      │ ←─ WriteEvent ────│              │
  │                    │                      │                   │              │
  │                    │                      │ (find affected    │              │
  │                    │                      │  subscriptions)   │              │
  │                    │                      │                   │              │
  │                    │                      │ ─────────────────────── Query ──→│
  │                    │                      │ ←────────────────────── Results ─│
  │                    │ ←─ Push Update ──────│                   │              │
  │ ←─ New Data ────── │                      │                   │              │
  │                    │                      │                   │              │
```

---

## Performance Optimization

### 1. Subscription Indexing

**Problem:** O(N) scan of all subscriptions on every write.

**Solution:** Multi-level indexing.

```rust
pub struct SubscriptionIndex {
    // Primary index: TableRef → List<Uuid>
    table_index: DashMap<TableRef, Vec<Uuid>>,

    // Secondary index: QueryHash → List<Uuid> (deduplication)
    query_hash_index: DashMap<u64, Vec<Uuid>>,

    // Predicate index: (TableRef, Column, Value) → List<Uuid>
    // For efficient filtering (e.g., only notify WHERE host='server01')
    predicate_index: DashMap<PredicateKey, Vec<Uuid>>,
}

#[derive(Hash, Eq, PartialEq)]
struct PredicateKey {
    table: TableRef,
    column: String,
    operator: String,
    value: String,
}

impl SubscriptionIndex {
    pub fn find_affected_subscriptions(
        &self,
        event: &WriteEvent,
    ) -> Vec<Uuid> {
        let table_ref = TableRef::from(event);

        // Get all subscriptions for this table
        let mut candidates = self.table_index
            .get(&table_ref)
            .map(|v| v.clone())
            .unwrap_or_default();

        // Further filter by predicates if data available
        if let Some(row_data) = &event.sample_data {
            candidates.retain(|&sub_id| {
                self.matches_predicates(sub_id, row_data)
            });
        }

        candidates
    }
}
```

### 2. Query Result Caching

**Problem:** Re-executing identical queries for multiple subscriptions.

**Solution:** Cache recent query results.

```rust
pub struct SubscriptionQueryCache {
    cache: LruCache<QueryCacheKey, CachedQueryResult>,
}

#[derive(Hash, Eq, PartialEq)]
struct QueryCacheKey {
    database: String,
    sql_hash: u64,
    timestamp: i64,  // Rounded to debounce interval
}

struct CachedQueryResult {
    results: Vec<RecordBatch>,
    cached_at: Instant,
}

impl StreamingManager {
    async fn execute_with_cache(
        &self,
        subscription: &Subscription,
    ) -> Result<Vec<RecordBatch>> {
        let cache_key = QueryCacheKey {
            database: subscription.query.database.clone(),
            sql_hash: hash(&subscription.query.sql),
            timestamp: (Utc::now().timestamp() / self.config.update_debounce_ms as i64)
                * self.config.update_debounce_ms as i64,
        };

        if let Some(cached) = self.query_cache.get(&cache_key) {
            if cached.cached_at.elapsed() < Duration::from_millis(self.config.update_debounce_ms) {
                return Ok(cached.results.clone());
            }
        }

        // Execute query
        let results = self.query_engine
            .execute(&subscription.query.database, &subscription.query.sql)
            .await?;

        // Cache results
        self.query_cache.insert(cache_key, CachedQueryResult {
            results: results.clone(),
            cached_at: Instant::now(),
        });

        Ok(results)
    }
}
```

### 3. Backpressure Handling

**Problem:** Slow clients block the server.

**Solution:** Drop old updates if client can't keep up.

```rust
pub enum BackpressureStrategy {
    Drop,      // Drop old updates
    Block,     // Block until client catches up
    Buffer,    // Buffer up to threshold, then drop
}

impl StreamingManager {
    async fn send_with_backpressure(
        &self,
        sender: &SubscriptionSender,
        data: ServerMessage,
    ) {
        match sender {
            SubscriptionSender::WebSocket { tx, .. } => {
                // Check channel capacity
                if tx.capacity() < self.config.backpressure_threshold {
                    match self.config.backpressure_strategy {
                        BackpressureStrategy::Drop => {
                            warn!("Dropping update due to slow client");
                            return;
                        }
                        BackpressureStrategy::Block => {
                            // Will block until client catches up
                            let _ = tx.send(Message::Text(
                                serde_json::to_string(&data).unwrap()
                            ));
                        }
                        BackpressureStrategy::Buffer => {
                            // Try to send, but don't wait
                            if tx.try_send(Message::Text(
                                serde_json::to_string(&data).unwrap()
                            )).is_err() {
                                warn!("Buffer full, dropping update");
                            }
                        }
                    }
                } else {
                    let _ = tx.send(Message::Text(
                        serde_json::to_string(&data).unwrap()
                    ));
                }
            }
            _ => {}
        }
    }
}
```

### 4. Batching & Debouncing

**Problem:** High-frequency writes cause too many notifications.

**Solution:** Batch updates within debounce window.

```rust
pub struct DebouncedNotifier {
    pending_notifications: DashMap<Uuid, PendingNotification>,
    debounce_duration: Duration,
}

struct PendingNotification {
    subscription_id: Uuid,
    first_event_time: Instant,
    event_count: AtomicUsize,
}

impl DebouncedNotifier {
    pub async fn schedule_notification(
        &self,
        subscription_id: Uuid,
        event: WriteEvent,
    ) {
        let mut notification = self.pending_notifications
            .entry(subscription_id)
            .or_insert_with(|| PendingNotification {
                subscription_id,
                first_event_time: Instant::now(),
                event_count: AtomicUsize::new(0),
            });

        notification.event_count.fetch_add(1, Ordering::Relaxed);

        // If debounce window elapsed, trigger notification
        if notification.first_event_time.elapsed() >= self.debounce_duration {
            self.trigger_notification(subscription_id).await;
            self.pending_notifications.remove(&subscription_id);
        }
    }

    async fn trigger_notification(&self, subscription_id: Uuid) {
        // Execute query and push results
        // ...
    }
}
```

---

## Comparison: WebSocket vs SSE

| Feature | WebSocket | Server-Sent Events |
|---------|-----------|-------------------|
| **Protocol** | Custom over TCP | HTTP |
| **Direction** | Bidirectional | Unidirectional (server → client) |
| **Data Format** | Text or Binary | Text only (UTF-8) |
| **Reconnection** | Manual | Automatic |
| **Browser Support** | All modern browsers | All modern browsers |
| **Firewall Friendly** | Sometimes blocked | Always works (HTTP) |
| **Latency** | Lower (no HTTP overhead) | Slightly higher |
| **Complexity** | Higher | Lower |
| **Use Cases** | Chat, gaming, real-time collaboration | Live dashboards, notifications |
| **Message Order** | Guaranteed | Guaranteed |
| **Event IDs** | Manual | Built-in |

### When to Use WebSocket

✅ **Use WebSocket when:**
- You need bidirectional communication (client sends commands)
- Binary data (e.g., compressed query results)
- Sub-100ms latency required
- Complex protocol (authentication, multiplexing)

Example: Real-time collaborative query editor

### When to Use SSE

✅ **Use SSE when:**
- Unidirectional updates (server → client only)
- Text-based data (JSON)
- Simpler implementation preferred
- Firewall/proxy compatibility critical
- Automatic reconnection desired

Example: Grafana dashboard live updates

---

## Production Considerations

### 1. Connection Limits

```toml
[streaming]
max_total_connections = 10000
max_subscriptions_per_connection = 10
max_subscriptions_per_user = 100
connection_timeout_secs = 3600  # 1 hour idle timeout
```

```rust
pub struct ConnectionLimiter {
    active_connections: AtomicUsize,
    max_connections: usize,
}

impl ConnectionLimiter {
    pub fn acquire(&self) -> Result<ConnectionGuard> {
        let current = self.active_connections.fetch_add(1, Ordering::SeqCst);

        if current >= self.max_connections {
            self.active_connections.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::TooManyConnections);
        }

        Ok(ConnectionGuard {
            limiter: self.clone(),
        })
    }
}

struct ConnectionGuard {
    limiter: ConnectionLimiter,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.limiter.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
}
```

### 2. Memory Management

**Problem:** Thousands of subscriptions consume memory.

**Solution:** Periodic cleanup + memory limits.

```rust
pub struct SubscriptionCleaner {
    streaming_manager: Arc<StreamingManager>,
    idle_timeout: Duration,
}

impl SubscriptionCleaner {
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            let now = Instant::now();
            let mut removed = 0;

            self.streaming_manager.subscriptions.retain(|_, sub| {
                let idle_time = now.duration_since(sub.last_activity.load());

                if idle_time > self.idle_timeout {
                    info!("Removing idle subscription: {}", sub.id);
                    removed += 1;
                    false
                } else {
                    true
                }
            });

            if removed > 0 {
                info!("Cleaned up {} idle subscriptions", removed);
            }
        }
    }
}
```

### 3. Metrics & Monitoring

```rust
pub struct StreamingMetrics {
    active_subscriptions: IntGauge,
    total_subscriptions_created: IntCounter,
    notifications_sent: IntCounter,
    notifications_dropped: IntCounter,
    query_execution_time: Histogram,
}

impl StreamingManager {
    fn record_notification(&self, duration: Duration, dropped: bool) {
        self.metrics.notifications_sent.inc();

        if dropped {
            self.metrics.notifications_dropped.inc();
        }

        self.metrics.query_execution_time.observe(duration.as_secs_f64());
    }
}
```

**Prometheus Metrics:**
```
# HELP mynadb_streaming_active_subscriptions Number of active streaming subscriptions
# TYPE mynadb_streaming_active_subscriptions gauge
mynadb_streaming_active_subscriptions 1234

# HELP mynadb_streaming_notifications_sent_total Total notifications sent
# TYPE mynadb_streaming_notifications_sent_total counter
mynadb_streaming_notifications_sent_total 567890

# HELP mynadb_streaming_query_duration_seconds Query execution time for streaming updates
# TYPE mynadb_streaming_query_duration_seconds histogram
mynadb_streaming_query_duration_seconds_bucket{le="0.01"} 1000
mynadb_streaming_query_duration_seconds_bucket{le="0.1"} 5000
mynadb_streaming_query_duration_seconds_bucket{le="1.0"} 8000
```

### 4. Load Balancing

**Challenge:** Sticky sessions required (subscription state in memory).

**Solutions:**

**Option 1: Redis Pub/Sub (cross-node subscriptions)**
```rust
pub struct DistributedStreamingManager {
    local_subscriptions: Arc<DashMap<Uuid, Subscription>>,
    redis_pubsub: Arc<RedisPubSub>,
}

impl DistributedStreamingManager {
    async fn on_write_event(&self, event: WriteEvent) {
        // Publish to Redis
        let channel = format!("writes:{}.{}.{}", event.catalog, event.schema, event.table);
        self.redis_pubsub.publish(&channel, &event).await;
    }

    async fn listen_redis_events(&self) {
        let mut pubsub = self.redis_pubsub.subscribe("writes:*").await;

        while let Some(event) = pubsub.next().await {
            // Notify local subscriptions
            self.notify_local_subscriptions(event).await;
        }
    }
}
```

**Option 2: Consistent Hashing (route by table)**
```
Load Balancer (hash by table name)
        │
        ├──→ Node 1 (tables: cpu, memory)
        ├──→ Node 2 (tables: disk, network)
        └──→ Node 3 (tables: process, system)
```

---

## Summary

### Key Takeaways

1. **WebSocket** provides bidirectional, low-latency streaming with binary support
2. **SSE** offers simpler unidirectional streaming with automatic reconnection
3. **Subscription Manager** is the core component that:
   - Maintains subscription registry
   - Listens for write events
   - Executes queries on relevant updates
   - Pushes results to connected clients
4. **Optimizations** include:
   - Multi-level indexing for fast subscription lookup
   - Query result caching
   - Debouncing to reduce notification frequency
   - Backpressure handling for slow clients

### Architectural Principles

- **Event-Driven:** Write events trigger notifications (not polling)
- **Non-Blocking:** Async I/O throughout
- **Scalable:** Supports 10,000+ concurrent subscriptions
- **Fault-Tolerant:** Automatic reconnection, idle cleanup
- **Observable:** Comprehensive metrics for monitoring

---

## Next Steps

1. **Prototype:** Implement basic WebSocket handler
2. **Integration:** Connect to write cache events
3. **Testing:** Load test with 10,000 subscriptions
4. **Optimization:** Profile and optimize hot paths
5. **Production:** Deploy with monitoring and alerting
