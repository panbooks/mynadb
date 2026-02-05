# MynaDB Admin Console Guide

The Admin Console provides a web-based interface for managing MynaDB clusters. It offers real-time monitoring, node management, table operations, and scale-in orchestration.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Getting Started](#getting-started)
3. [Configuration](#configuration)
4. [API Reference](#api-reference)
5. [Data Layout](#data-layout)
6. [Frontend Development](#frontend-development)
7. [WebSocket Events](#websocket-events)
8. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Browser                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Leptos WASM Frontend                        │   │
│  │   Dashboard │ Nodes │ Scale-In │ Tables │ Events        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                    │ HTTP/REST        │ WebSocket              │
└────────────────────┼──────────────────┼────────────────────────┘
                     ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Admin Backend (Axum)                         │
│   /admin/api/v1/*  │  /admin/api/v1/events/ws                  │
│        │                      │                                 │
│        ▼                      ▼                                 │
│   ┌─────────┐          ┌───────────┐                           │
│   │ REST API│          │ WS Handler│──► Event Broadcast        │
│   └────┬────┘          └─────┬─────┘                           │
│        │                     │                                  │
│        └─────────┬───────────┘                                 │
│                  ▼                                              │
│            MetaClient (gRPC)                                    │
└─────────────────┼───────────────────────────────────────────────┘
                  ▼
         ┌───────────────┐
         │  Meta Service │
         │   gRPC API    │
         └───────────────┘
```

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| **Admin Backend** | `src/admin/` | Axum HTTP server with REST API and WebSocket |
| **Admin Types** | `admin-types/` | Shared type definitions (Rust crate) |
| **Admin Frontend** | `admin-frontend/` | Leptos WASM single-page application |
| **Node Runner** | `src/node/admin.rs` | CLI integration for starting/stopping |

### Request Flow

1. **Browser** → Loads SPA from `/admin/`
2. **Frontend** → Makes REST calls to `/admin/api/v1/*`
3. **Backend** → Translates to gRPC calls via `MetaClient`
4. **Meta Service** → Returns cluster data
5. **Backend** → Formats response as JSON
6. **Frontend** → Renders UI with reactive updates

---

## Getting Started

### Prerequisites

- Running Meta service cluster
- Rust toolchain (for backend)
- Trunk (for frontend development): `cargo install trunk`

### Quick Start

```bash
# 1. Start a Meta cluster first (required)
mynadb meta start -c config.toml

# 2. Start the Admin console
mynadb admin start -c config.toml

# 3. Access the console
open http://localhost:8080/admin/
```

### CLI Commands

```bash
# Start admin console (foreground)
mynadb admin start -c config.toml

# Stop admin console
mynadb admin stop -c config.toml

# View help
mynadb admin --help
```

### Development Mode

For frontend development with hot-reload:

```bash
# Terminal 1: Start backend
mynadb admin start -c config.toml

# Terminal 2: Start frontend dev server
cd admin-frontend
trunk serve --proxy-backend=http://localhost:8080/admin/api
```

---

## Configuration

Add the `[admin]` section to your `config.toml`:

```toml
[admin]
# Host to bind HTTP server
http_host = "0.0.0.0"

# Port for admin console
http_port = 8080

# Meta service endpoints for gRPC connection
meta_endpoints = ["localhost:7100", "localhost:7101", "localhost:7102"]
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `http_host` | String | `"0.0.0.0"` | IP address to bind |
| `http_port` | u16 | `8080` | HTTP port |
| `meta_endpoints` | Vec<String> | `["localhost:7100"]` | Meta service gRPC endpoints |

### Environment Variables

You can also use environment variables (useful for containers):

```bash
export MYNADB_ADMIN_HTTP_HOST=0.0.0.0
export MYNADB_ADMIN_HTTP_PORT=8080
export MYNADB_ADMIN_META_ENDPOINTS=meta1:7100,meta2:7100
```

---

## API Reference

All API endpoints are prefixed with `/admin/api/v1/`.

### Response Format

All responses follow this structure:

```json
{
  "success": true,
  "data": { ... },
  "error": null
}
```

On error:

```json
{
  "success": false,
  "data": null,
  "error": "Error message"
}
```

### Cluster Endpoints

#### GET /cluster/status

Returns cluster health overview.

**Response:**
```json
{
  "success": true,
  "data": {
    "healthy": true,
    "meta_leader": "meta-node-1",
    "node_counts": {
      "meta": { "total": 3, "active": 3, "inactive": 0 },
      "router": { "total": 2, "active": 2, "inactive": 0 },
      "data": {
        "total": 4,
        "active": 4,
        "inactive": 0,
        "primary": 2,
        "standby": 2,
        "retiring": 0,
        "drained": 0
      },
      "compactor": { "total": 1, "active": 1, "inactive": 0 }
    },
    "total_tables": 150,
    "total_partitions": 4500,
    "warnings": []
  }
}
```

### Node Endpoints

#### GET /nodes

List all nodes with optional filters.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `role` | string | Filter by role: `meta`, `router`, `data`, `compactor` |
| `state` | string | Filter by state: `active`, `starting`, `retiring`, `drained`, `inactive` |
| `mode` | string | Filter data nodes by mode: `primary`, `standby` |

**Example:**
```bash
curl "http://localhost:8080/admin/api/v1/nodes?role=data&mode=primary"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "nodes": [
      {
        "node_id": "data-node-1",
        "role": "Data",
        "state": "Active",
        "mode": "Primary",
        "advertise_addr": "192.168.1.10:7000",
        "zone": "zone-a",
        "tables_owned": 75,
        "last_heartbeat_ago_secs": 2
      }
    ],
    "total": 1
  }
}
```

#### GET /nodes/:id

Get detailed information about a specific node.

**Example:**
```bash
curl "http://localhost:8080/admin/api/v1/nodes/data-node-1"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "node": {
      "node_id": "data-node-1",
      "role": "Data",
      "state": "Active",
      "mode": "Primary",
      "advertise_addr": "192.168.1.10:7000",
      "http_addr": "192.168.1.10:7000",
      "flight_addr": "192.168.1.10:7001",
      "registered_at": 1704067200000,
      "last_heartbeat": 1704153600000,
      "labels": { "zone": "zone-a", "rack": "rack-1" },
      "backing_standby": "data-node-3",
      "backed_primaries": []
    },
    "tables_owned": ["catalog1/schema1/table1", "catalog1/schema1/table2"],
    "health": {
      "healthy": true,
      "last_heartbeat_ms": 1704153600000,
      "heartbeat_age_secs": 2
    }
  }
}
```

### Table Endpoints

#### GET /tables

List all tables with filters and pagination.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `catalog` | string | - | Filter by catalog |
| `schema` | string | - | Filter by schema |
| `owner` | string | - | Filter by owner node ID |
| `search` | string | - | Search in catalog/schema/table names |
| `offset` | u32 | 0 | Pagination offset |
| `limit` | u32 | 100 | Page size (max 1000) |

**Example:**
```bash
curl "http://localhost:8080/admin/api/v1/tables?catalog=mydb&limit=10"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "tables": [
      {
        "catalog": "mydb",
        "schema": "default",
        "table": "cpu_metrics",
        "primary_owner": "data-node-1",
        "owner_count": 2,
        "partition_count": 30,
        "total_bytes": 1073741824,
        "total_rows": 10000000
      }
    ],
    "total": 150,
    "offset": 0,
    "limit": 10
  }
}
```

#### POST /tables/:key/reassign

Reassign a table to a new owner node.

**Path Parameters:**
- `key`: Table key in format `catalog/schema/table`

**Request Body:**
```json
{
  "new_owner_id": "data-node-2",
  "force": false
}
```

**Example:**
```bash
curl -X POST "http://localhost:8080/admin/api/v1/tables/mydb/default/cpu_metrics/reassign" \
  -H "Content-Type: application/json" \
  -d '{"new_owner_id": "data-node-2"}'
```

### Scale-In Endpoints

#### GET /scale-in/recommend

Get AI-recommended nodes for scale-in based on cluster state.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `primaries` | usize | Number of primary nodes to reduce |
| `standbys` | usize | Number of standby nodes to reduce |

**Example:**
```bash
curl "http://localhost:8080/admin/api/v1/scale-in/recommend?primaries=1&standbys=1"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "recommended_primaries": ["data-node-4"],
    "recommended_standbys": ["data-node-6"],
    "reasons": {
      "data-node-4": "Lowest table count (12 tables)",
      "data-node-6": "Backs fewest primaries (1 primary)"
    },
    "warnings": ["Removing data-node-4 will require reassigning 12 tables"]
  }
}
```

#### POST /scale-in/plan

Preview a scale-in operation (dry run).

**Request Body:**
```json
{
  "node_ids": ["data-node-4", "data-node-6"],
  "dry_run": true
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "node_ids": ["data-node-4", "data-node-6"],
    "tables_to_reassign": 15,
    "estimated_duration_secs": 150,
    "warnings": []
  }
}
```

#### POST /scale-in/execute

Execute a scale-in operation (starts node retirement).

**Request Body:**
```json
{
  "node_ids": ["data-node-4"]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "started": true,
    "message": "Started retiring 1 nodes",
    "nodes_to_retire": ["data-node-4"]
  }
}
```

### Events Endpoints

#### GET /events

List recent events (placeholder - use WebSocket for real-time).

**Response:**
```json
{
  "success": true,
  "data": {
    "events": [],
    "total": 0,
    "message": "Events API not yet implemented. Use WebSocket at /api/v1/events/ws for real-time events."
  }
}
```

#### WebSocket: /events/ws

Connect to receive real-time cluster events.

```javascript
const ws = new WebSocket('ws://localhost:8080/admin/api/v1/events/ws');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Event:', data.type, data.payload);
};
```

**Event Format:**
```json
{
  "type": "node_registered",
  "payload": {
    "node_id": "data-node-5",
    "role": "Data"
  },
  "timestamp": 1704153600000
}
```

**Event Types:**
- `heartbeat` - Periodic heartbeat (every 30s)
- `node_registered` - New node joined
- `node_deregistered` - Node left cluster
- `table_created` - New table registered
- `table_reassigned` - Table ownership changed
- `retire_started` - Node retirement began
- `retire_completed` - Node retirement finished

---

## Data Layout

### Project Structure

```
mynadb-dev/
├── Cargo.toml                    # Workspace definition
├── admin-types/                  # Shared types crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs               # Module exports
│       ├── cluster.rs           # ClusterStatus, NodeCounts
│       ├── nodes.rs             # NodeRole, NodeState, NodeInfo
│       ├── tables.rs            # TableMeta, TableSummary
│       ├── scale_in.rs          # ScaleInPlan, ScaleInPhase
│       └── events.rs            # EventType, AdminEvent
├── admin-frontend/               # Leptos WASM frontend
│   ├── Cargo.toml
│   ├── Trunk.toml               # Build configuration
│   ├── index.html               # Entry HTML
│   ├── tailwind.config.js       # Tailwind CSS config
│   ├── style/
│   │   ├── main.css             # Custom styles
│   │   └── tailwind.css         # Tailwind imports
│   └── src/
│       ├── lib.rs               # WASM entry point
│       ├── app.rs               # Root component + routing
│       ├── api/
│       │   ├── mod.rs
│       │   ├── client.rs        # HTTP fetch wrapper
│       │   └── events.rs        # WebSocket client
│       ├── components/
│       │   ├── mod.rs
│       │   ├── layout.rs        # Sidebar, header
│       │   ├── node_card.rs     # Node display card
│       │   ├── status_badge.rs  # Status indicators
│       │   ├── progress.rs      # Progress bars
│       │   └── toast.rs         # Notifications
│       ├── pages/
│       │   ├── mod.rs
│       │   ├── dashboard.rs     # Cluster overview
│       │   ├── nodes.rs         # Node list/detail
│       │   ├── tables.rs        # Table management
│       │   ├── scale_in.rs      # Scale-in wizard
│       │   └── events.rs        # Event viewer
│       └── state/
│           └── mod.rs           # Global state (signals)
└── src/
    ├── admin/                    # Admin backend module
    │   ├── mod.rs               # Router builder
    │   ├── api.rs               # REST API handlers
    │   ├── state.rs             # AdminState + MetaClient
    │   ├── websocket.rs         # WebSocket handler
    │   └── static_files.rs      # SPA file serving
    ├── node/
    │   └── admin.rs             # Admin node runner
    ├── config.rs                # AdminConfig struct
    └── main.rs                  # CLI integration
```

### Type Sharing Architecture

The `admin-types` crate enables sharing types between backend and frontend:

```
┌─────────────────┐
│  admin-types    │  ← Shared crate
│  (Rust types)   │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌───────┐ ┌───────────┐
│Backend│ │ Frontend  │
│(Axum) │ │(Leptos)   │
│       │ │ (WASM)    │
└───────┘ └───────────┘
```

### Backend Module Structure

```rust
// src/admin/mod.rs
pub fn build_admin_router(config: &AdminConfig, meta_endpoints: Vec<String>) -> Router {
    let state = AdminState::new(config.clone(), meta_endpoints);

    Router::new()
        .merge(static_files::routes())      // SPA serving
        .nest("/api/v1", api::routes())     // REST API
        .route("/api/v1/events/ws", get(websocket::handler))
        .with_state(state)
}
```

### State Management

```rust
// src/admin/state.rs
pub struct AdminState {
    pub config: AdminConfig,
    pub meta_endpoints: Vec<String>,
    pub meta_client: Arc<RwLock<Option<Arc<MetaClient>>>>,
    pub event_tx: broadcast::Sender<AdminEvent>,
}
```

---

## Frontend Development

### Building the Frontend

```bash
cd admin-frontend

# Development (with hot-reload)
trunk serve

# Production build
trunk build --release

# Output in admin-frontend/dist/
```

### Frontend Architecture

The frontend uses Leptos with client-side rendering (CSR):

```rust
// src/app.rs
#[component]
pub fn App() -> impl IntoView {
    view! {
        <Router>
            <Layout>
                <Routes>
                    <Route path="/" view=DashboardPage />
                    <Route path="/nodes" view=NodesPage />
                    <Route path="/nodes/:id" view=NodeDetailPage />
                    <Route path="/tables" view=TablesPage />
                    <Route path="/scale-in" view=ScaleInPage />
                    <Route path="/events" view=EventsPage />
                </Routes>
            </Layout>
        </Router>
    }
}
```

### State Management with Signals

```rust
// src/state/mod.rs
#[derive(Clone)]
pub struct AppState {
    pub cluster_status: RwSignal<Option<ClusterStatus>>,
    pub nodes: RwSignal<Vec<NodeSummary>>,
    pub events: RwSignal<Vec<AdminEvent>>,
    pub loading: RwSignal<bool>,
    pub error: RwSignal<Option<String>>,
}
```

### API Client Usage

```rust
// src/api/client.rs
pub async fn fetch_cluster_status() -> Result<ClusterStatus, String> {
    let response = Request::get("/admin/api/v1/cluster/status")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let api_response: ApiResponse<ClusterStatus> = response
        .json()
        .await
        .map_err(|e| e.to_string())?;

    if api_response.success {
        api_response.data.ok_or("No data".to_string())
    } else {
        Err(api_response.error.unwrap_or("Unknown error".to_string()))
    }
}
```

---

## WebSocket Events

### Connection Lifecycle

```
Client                          Server
  │                               │
  ├──── WS Connect ──────────────►│
  │                               │
  │◄─── Welcome (optional) ───────┤
  │                               │
  │◄─── Heartbeat (30s) ──────────┤
  │                               │
  │◄─── Event (node_registered) ──┤
  │                               │
  │◄─── Event (table_created) ────┤
  │                               │
  ├──── Close ───────────────────►│
  │                               │
```

### Reconnection Strategy

The frontend implements automatic reconnection:

```rust
pub struct EventClient {
    url: String,
    ws: Option<WebSocket>,
    reconnect_attempts: u32,
    max_reconnect_delay: Duration,
}

impl EventClient {
    pub fn connect(&mut self) {
        // Exponential backoff: 1s, 2s, 4s, 8s, ... max 30s
        let delay = min(
            Duration::from_secs(1 << self.reconnect_attempts),
            self.max_reconnect_delay
        );
        // ... reconnect logic
    }
}
```

---

## Troubleshooting

### Common Issues

#### "Failed to connect to Meta"

**Cause:** Admin console cannot reach Meta service.

**Solutions:**
1. Verify Meta service is running: `mynadb meta start -c config.toml`
2. Check `meta_endpoints` in config matches actual Meta addresses
3. Verify network connectivity: `nc -zv localhost 7100`

#### "No Meta leader elected"

**Cause:** Meta cluster hasn't achieved quorum.

**Solutions:**
1. Ensure at least 2 of 3 Meta nodes are running (for 3-node cluster)
2. Check Meta logs for Raft election issues
3. Wait for leader election (typically < 10 seconds)

#### WebSocket disconnects frequently

**Cause:** Network instability or server restarts.

**Solutions:**
1. Frontend auto-reconnects with exponential backoff
2. Check server logs for errors
3. Verify firewall allows WebSocket connections

#### Frontend shows blank page

**Cause:** WASM not loaded or API errors.

**Solutions:**
1. Open browser DevTools, check Console for errors
2. Verify backend is running on correct port
3. Check CORS headers if running frontend separately

### Debug Logging

Enable debug logging for admin module:

```bash
RUST_LOG=mynadb::admin=debug mynadb admin start -c config.toml
```

### Health Check

Quick health verification:

```bash
# Check API is responding
curl -s http://localhost:8080/admin/api/v1/cluster/status | jq .success

# Check WebSocket
websocat ws://localhost:8080/admin/api/v1/events/ws
```

---

## Security Considerations

### Current State

The admin console currently has no authentication. It should only be exposed on trusted networks.

### Future Enhancements

Planned security features:
- [ ] Basic authentication
- [ ] JWT token support
- [ ] RBAC (Role-Based Access Control)
- [ ] Audit logging
- [ ] HTTPS support

### Network Security

Recommended deployment:
```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Browser   │────►│ Load Balancer│────►│Admin Console│
│             │     │  (HTTPS/TLS) │     │  (Internal) │
└─────────────┘     └──────────────┘     └─────────────┘
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.1.0 | 2024-01 | Initial implementation |

---

## Related Documentation

- [MynaDB Architecture](./architecture-cluster.md)
- [Meta Service API](./meta-service-api.md)
- [Scale-In Operations](./scale-in-guide.md)
