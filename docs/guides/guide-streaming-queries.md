# Streaming Queries Guide

Real-time push-based query results via WebSocket and Server-Sent Events (SSE). Subscribe to a SQL query and receive automatic updates when new data arrives.

## Quick Start

### 1. Enable the Feature

Add to your `config.toml`:

```toml
[streaming]
enabled = true
```

### 2. Subscribe via WebSocket

Connect to `ws://localhost:7000/stream/query` and send a JSON subscription request:

```bash
# Using websocat (install: cargo install websocat)
websocat ws://localhost:7000/stream/query

# Send subscription request:
{"database": "mydb", "query": "SELECT * FROM cpu WHERE host = 'server01' ORDER BY time DESC LIMIT 10"}
```

Response stream:

```json
{"type":"subscribed","subscription_id":"a1b2c3d4-...","database":"mydb","query":"SELECT ..."}
{"type":"initial","data":"[{\"time\":1707300000,\"host\":\"server01\",\"cpu\":82.5}, ...]"}
{"type":"update","data":"[{\"time\":1707300060,\"host\":\"server01\",\"cpu\":85.1}, ...]"}
{"type":"heartbeat","timestamp":"2026-02-07T12:00:30Z"}
{"type":"update","data":"[{\"time\":1707300120,\"host\":\"server01\",\"cpu\":79.3}, ...]"}
```

### 3. Subscribe via SSE

For simpler HTTP-based streaming (one query per connection):

```bash
curl -N "http://localhost:7000/stream/query/mydb?sql=SELECT%20*%20FROM%20cpu%20ORDER%20BY%20time%20DESC%20LIMIT%2010"
```

Response (SSE format):

```
data: {"type":"initial","data":"[{\"time\":1707300000,\"cpu\":82.5}, ...]"}

data: {"type":"update","data":"[{\"time\":1707300060,\"cpu\":85.1}, ...]"}

data: {"type":"update","data":"[{\"time\":1707300120,\"cpu\":79.3}, ...]"}
```

## WebSocket vs SSE

| Feature | WebSocket (`/stream/query`) | SSE (`/stream/query/:db`) |
|---------|----------------------------|---------------------------|
| Protocol | Full-duplex | Server-push only |
| Multiple subscriptions | Yes (up to 10 per connection) | One per connection |
| Heartbeat | Built-in (configurable) | HTTP keep-alive |
| Browser support | All modern browsers | All modern browsers |
| Reconnection | Manual | Automatic (EventSource API) |
| Best for | Dashboards with many panels | Single metric streams |

## Writing Data (Triggers Updates)

Subscribers are notified when data is written to tables referenced in their query:

```bash
# This write triggers updates for any subscription querying the "cpu" table in "mydb"
curl -X POST "http://localhost:7000/write/mydb" \
  -d 'cpu,host=server01 usage=85.1'
```

The write path is never blocked by streaming notifications — they run asynchronously.

## Client Examples

### JavaScript (WebSocket)

```javascript
const ws = new WebSocket('ws://localhost:7000/stream/query');

ws.onopen = () => {
  ws.send(JSON.stringify({
    database: 'mydb',
    query: 'SELECT * FROM cpu ORDER BY time DESC LIMIT 100'
  }));
};

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  switch (msg.type) {
    case 'subscribed':
      console.log('Subscribed:', msg.subscription_id);
      break;
    case 'initial':
    case 'update':
      const rows = JSON.parse(msg.data);
      updateDashboard(rows);
      break;
    case 'heartbeat':
      // Connection alive
      break;
    case 'error':
      console.error('Error:', msg.message || msg.data);
      break;
  }
};
```

### JavaScript (SSE / EventSource)

```javascript
const source = new EventSource(
  'http://localhost:7000/stream/query/mydb?sql=SELECT+*+FROM+cpu+ORDER+BY+time+DESC+LIMIT+100'
);

source.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === 'initial' || msg.type === 'update') {
    const rows = JSON.parse(msg.data);
    updateDashboard(rows);
  }
};

source.onerror = () => {
  console.log('SSE connection lost, auto-reconnecting...');
};
```

### Python

```python
import websocket
import json

ws = websocket.create_connection("ws://localhost:7000/stream/query")
ws.send(json.dumps({
    "database": "mydb",
    "query": "SELECT * FROM cpu ORDER BY time DESC LIMIT 10"
}))

while True:
    msg = json.loads(ws.recv())
    if msg["type"] in ("initial", "update"):
        rows = json.loads(msg["data"])
        print(f"Got {len(rows)} rows")
    elif msg["type"] == "error":
        print(f"Error: {msg['message']}")
        break
```

## Authentication

When auth is enabled, streaming endpoints respect RBAC:

```bash
# WebSocket with Basic Auth
websocat -H "Authorization: Basic $(echo -n 'admin:secret' | base64)" \
  ws://localhost:7000/stream/query

# SSE with query param auth
curl -N "http://localhost:7000/stream/query/mydb?sql=SELECT+*+FROM+cpu&u=admin&p=secret"
```

Database and table-level access controls are enforced:
- Subscription is rejected if the user cannot access the requested database
- The initial query runs with full auth checks (table-level permissions)

## Monitoring Subscriptions

List all active streaming subscriptions:

```bash
curl http://localhost:7000/stream/subscriptions
```

Response:

```json
{
  "enabled": true,
  "total": 3,
  "subscriptions": [
    {
      "id": "a1b2c3d4-e5f6-...",
      "database": "mydb",
      "query": "SELECT * FROM cpu ORDER BY time DESC LIMIT 10",
      "tables": ["cpu"]
    },
    {
      "id": "b2c3d4e5-f6a7-...",
      "database": "mydb",
      "query": "SELECT avg(usage) FROM memory GROUP BY host",
      "tables": ["memory"]
    }
  ]
}
```

## Configuration Reference

```toml
[streaming]
# Enable streaming query endpoints (default: false)
enabled = true

# Maximum concurrent subscriptions across all connections (default: 10000)
max_subscriptions = 10000

# Maximum subscriptions per individual WebSocket connection (default: 10)
max_subscriptions_per_connection = 10

# Debounce interval in milliseconds (default: 100)
# Batches rapid writes to avoid re-executing queries on every single point
update_debounce_ms = 100

# Maximum buffered messages per subscription before dropping (default: 1000)
# If a consumer falls behind, oldest updates are dropped
backpressure_threshold = 1000

# WebSocket heartbeat interval in seconds (default: 30)
heartbeat_interval_secs = 30
```

## Backpressure Behavior

When a subscriber is slow (e.g., network congestion), the system applies backpressure:

1. Each subscription has a channel buffer of `backpressure_threshold` messages
2. When the buffer fills, new updates are **dropped** (not queued indefinitely)
3. A warning is logged: `Subscription {id} buffer full, dropping update`
4. The write path is **never blocked** — notifications are fire-and-forget

This ensures the database write throughput is unaffected by slow streaming consumers.

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/stream/query` | GET (WebSocket Upgrade) | Subscribe to streaming queries via WebSocket |
| `/stream/query/:db` | GET | Subscribe to a single query via SSE |
| `/stream/subscriptions` | GET | List all active subscriptions |

### WebSocket Message Format

**Client → Server (subscribe):**
```json
{"database": "mydb", "query": "SELECT * FROM cpu"}
```

**Server → Client:**
```json
{"type": "subscribed", "subscription_id": "uuid", "database": "mydb", "query": "..."}
{"type": "initial", "data": "[...]"}
{"type": "update", "data": "[...]"}
{"type": "error", "message": "..."}
{"type": "heartbeat", "timestamp": "2026-02-07T12:00:00Z"}
```
