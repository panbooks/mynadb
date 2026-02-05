# Node Registration Architecture

> **Status: IMPLEMENTED** - DataNode registration with Meta service fully integrated.

This document describes the complete flow of how nodes (DataNode, Router) register with the Meta service, maintain their presence via heartbeats, and how the system handles node failures.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Cluster Node Registration                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌──────────┐         ┌──────────┐         ┌──────────┐         ┌──────────┐  │
│   │ DataNode │         │ DataNode │         │  Router  │         │  Router  │  │
│   │    1     │         │    2     │         │    1     │         │    2     │  │
│   └────┬─────┘         └────┬─────┘         └────┬─────┘         └────┬─────┘  │
│        │                    │                    │                    │        │
│        │    Register + Heartbeat (gRPC)          │                    │        │
│        └──────────────┬─────┴────────────────────┴────────────────────┘        │
│                       │                                                         │
│                       ▼                                                         │
│        ┌─────────────────────────────────────────────────────┐                 │
│        │                  Meta Service                        │                 │
│        │  ┌─────────┐   ┌─────────┐   ┌─────────┐           │                 │
│        │  │ Meta-1  │◄──│ Meta-2  │──►│ Meta-3  │           │                 │
│        │  │(Leader) │   │(Follower)   │(Follower)│           │                 │
│        │  └────┬────┘   └─────────┘   └─────────┘           │                 │
│        │       │                                             │                 │
│        │       │ Raft Consensus                              │                 │
│        │       ▼                                             │                 │
│        │  ┌─────────────────────────────────────┐           │                 │
│        │  │           sled Database              │           │                 │
│        │  │  /nodes/{node_id} → NodeInfo        │           │                 │
│        │  │  /leases/{lease_id} → LeaseInfo     │           │                 │
│        │  │  /events/{seq} → Event              │           │                 │
│        │  └─────────────────────────────────────┘           │                 │
│        └─────────────────────────────────────────────────────┘                 │
│                       │                                                         │
│                       │ Event Stream (gRPC streaming)                          │
│                       ▼                                                         │
│        ┌─────────────────────────────────────────────────────┐                 │
│        │              Event Subscribers                       │                 │
│        │   Router.ShardCache ◄── NodeJoined/NodeLeft events  │                 │
│        └─────────────────────────────────────────────────────┘                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Structures

### NodeInfo

Represents a registered node in the cluster.

```rust
pub struct NodeInfo {
    pub node_id: String,           // Unique identifier (e.g., "data-node-1")
    pub role: NodeRole,            // Meta, Router, Data, Compactor
    pub state: NodeState,          // Starting, Active, Draining, Inactive
    pub advertise_addr: String,    // Primary address for communication
    pub http_addr: Option<String>, // HTTP API address (if applicable)
    pub flight_addr: Option<String>, // Arrow Flight address
    pub registered_at: i64,        // Registration timestamp (ms)
    pub last_heartbeat: i64,       // Last heartbeat timestamp (ms)
    pub labels: HashMap<String, String>, // Custom labels
}
```

### NodeRole

```rust
pub enum NodeRole {
    Unknown = 0,
    Meta = 1,      // Metadata service node
    Router = 2,    // Query router node
    Data = 3,      // Data storage node
    Compactor = 4, // Background compaction node
}
```

### NodeState

```rust
pub enum NodeState {
    Unknown = 0,
    Starting = 1,  // Node is starting up
    Active = 2,    // Node is healthy and serving
    Draining = 3,  // Node is draining (preparing to shutdown)
    Inactive = 4,  // Node is inactive/unhealthy
}
```

### LeaseInfo

Tracks node registration lease for automatic expiration.

```rust
pub struct LeaseInfo {
    pub lease_id: u64,      // Unique lease identifier
    pub node_id: String,    // Associated node
    pub ttl_ms: u64,        // Time-to-live in milliseconds
    pub created_at: i64,    // Creation timestamp
    pub expires_at: i64,    // Expiration timestamp
}
```

---

## Registration Flow

### Step 1: DataNode Startup

**File:** `src/node/data.rs:75-124`

```rust
// 1. Connect to Meta service
let meta_client = MetaClient::connect(&data_config.meta_endpoints).await?;

// 2. Create NodeInfo
let node_info = NodeInfo {
    node_id: data_config.node_id.clone(),
    role: NodeRole::Data,
    state: NodeState::Starting,
    advertise_addr: data_config.advertise_addr.clone(),
    flight_addr: Some(data_config.advertise_addr.clone()),
    http_addr: None,
    registered_at: current_timestamp_ms(),
    last_heartbeat: current_timestamp_ms(),
    labels: HashMap::new(),
};

// 3. Register with Meta service (30 second lease TTL)
let lease_id = meta_client.register_node(node_info, 30000).await?;

// 4. Start heartbeat loop
tokio::spawn(heartbeat_loop(client, lease_id, shutdown_flag));
```

### Step 2: gRPC Request to Meta Service

**File:** `src/meta/client/meta_client.rs:197-227`

```
DataNode                          Meta Service (gRPC)
   │                                    │
   │  RegisterNodeRequest               │
   │  ┌─────────────────────────┐       │
   │  │ info: NodeInfo          │       │
   │  │   node_id: "data-1"     │       │
   │  │   role: Data (3)        │       │
   │  │   state: Starting (1)   │       │
   │  │   advertise_addr: ...   │       │
   │  │ lease_ttl_ms: 30000     │       │
   │  └─────────────────────────┘       │
   │ ─────────────────────────────────► │
   │                                    │
   │  RegisterNodeResponse              │
   │  ┌─────────────────────────┐       │
   │  │ success: true           │       │
   │  │ lease_id: 1             │       │
   │  │ error: ""               │       │
   │  └─────────────────────────┘       │
   │ ◄───────────────────────────────── │
```

### Step 3: Meta Service gRPC Handler

**File:** `src/meta/service/grpc.rs:33-72`

```rust
async fn register_node(&self, request: Request<RegisterNodeRequest>)
    -> Result<Response<RegisterNodeResponse>, Status>
{
    let req = request.into_inner();
    let node_info = NodeInfo::from(req.info);

    // Delegate to MetaNode (routes through Raft)
    match self.node.register_node(node_info, req.lease_ttl_ms).await {
        Ok(lease_id) => Ok(Response::new(RegisterNodeResponse {
            success: true,
            lease_id,
            error: String::new(),
        })),
        Err(e) => Ok(Response::new(RegisterNodeResponse {
            success: false,
            lease_id: 0,
            error: e,
        })),
    }
}
```

### Step 4: Raft Consensus

**File:** `src/meta/service/meta_node.rs:144-151, 318-351`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Raft Consensus                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   MetaNode.register_node()                                                  │
│         │                                                                   │
│         ▼                                                                   │
│   Create MetaCommand::RegisterNode { info, lease_ttl_ms }                  │
│         │                                                                   │
│         ▼                                                                   │
│   raft.client_write(cmd)  ────────────────────────────────────┐            │
│         │                                                      │            │
│         │                                                      │            │
│   ┌─────▼─────┐     ┌───────────┐     ┌───────────┐          │            │
│   │  Meta-1   │     │  Meta-2   │     │  Meta-3   │          │            │
│   │ (Leader)  │────►│ (Follower)│────►│ (Follower)│          │            │
│   └─────┬─────┘     └───────────┘     └───────────┘          │            │
│         │                                                      │            │
│         │ 1. Append to log                                    │            │
│         │ 2. Replicate to followers                           │            │
│         │ 3. Wait for quorum (2/3)                            │            │
│         │ 4. Commit                                           │            │
│         │ 5. Apply to state machine ◄─────────────────────────┘            │
│         │                                                                   │
│         ▼                                                                   │
│   state_machine.apply_command(RegisterNode)                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Important:** Only the **leader** Meta node applies commands to the state machine. Followers only replicate the log entries.

### Step 5: State Machine Apply

**File:** `src/meta/raft/state_machine.rs:232-258`

```rust
fn register_node(&self, info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
    // 1. Generate unique lease ID
    let lease_id = self.next_lease_id();

    // 2. Create and store lease (for auto-expiration)
    let lease = LeaseInfo::new(lease_id, info.node_id.clone(), lease_ttl_ms);
    self.db.insert(
        format!("/leases/{}", lease_id).as_bytes(),
        bincode::serialize(&lease).unwrap(),
    );

    // 3. Store node info
    self.db.insert(
        format!("/nodes/{}", info.node_id).as_bytes(),
        bincode::serialize(&info).unwrap(),
    );

    // 4. Publish NodeJoined event
    let event_payload = serde_json::to_vec(&info).unwrap();
    self.publish_event("nodes", EventType::NodeJoined, event_payload, &info.node_id);

    MetaResponse::NodeRegistered { lease_id }
}
```

### Step 6: Event Publication

**File:** `src/meta/raft/state_machine.rs:573-600`

```rust
fn publish_event(&self, topic: &str, event_type: EventType, payload: Vec<u8>, source: &str) {
    let seq = self.next_sequence();  // Atomic counter

    let event = Event {
        sequence: seq,
        event_type,              // NodeJoined
        topic: topic.to_string(), // "nodes"
        payload,                 // JSON-encoded NodeInfo
        timestamp: current_timestamp_ms(),
        source_node: source.to_string(),
    };

    // 1. Persist to sled (for catch-up queries)
    self.db.insert(
        format!("/events/{:020}", seq).as_bytes(),
        bincode::serialize(&event).unwrap(),
    );

    // 2. Broadcast to subscribers (instant push)
    let _ = self.event_broadcast.send(event);
}
```

### Step 7: Event Delivery to Subscribers

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Event Delivery                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Meta State Machine                                                        │
│         │                                                                   │
│         │ event_broadcast.send(NodeJoined)                                 │
│         │                                                                   │
│         ▼                                                                   │
│   ┌─────────────────────────────────────────────────────────┐              │
│   │              tokio::sync::broadcast                      │              │
│   │                                                         │              │
│   │   Receiver 1        Receiver 2        Receiver 3        │              │
│   │   (Router-1)        (Router-2)        (Admin UI)        │              │
│   └───────┬─────────────────┬─────────────────┬─────────────┘              │
│           │                 │                 │                             │
│           ▼                 ▼                 ▼                             │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                      │
│   │ gRPC stream │   │ gRPC stream │   │ gRPC stream │                      │
│   │ to Router-1 │   │ to Router-2 │   │ to Admin    │                      │
│   └─────────────┘   └─────────────┘   └─────────────┘                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 8: Router Updates ShardCache

**File:** `src/router/routing/cache_invalidation.rs:100-116`

```rust
fn handle_node_joined(shard_cache: &ShardCache, event: &MetaEvent) {
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&event.payload) {
        let node_id = payload["node_id"].as_str().unwrap_or_default();
        let grpc_addr = payload["advertise_addr"].as_str().unwrap_or_default();

        if !node_id.is_empty() && !grpc_addr.is_empty() {
            info!("Node joined: {} at {}", node_id, grpc_addr);

            // Add to data_nodes (single source of truth)
            shard_cache.upsert_data_node(DataNodeInfo {
                node_id: node_id.to_string(),
                grpc_addr: grpc_addr.to_string(),
                state: NodeState::Active,
                load_score: 0.5,
            });
        }
    }
}
```

---

## Heartbeat Flow

### Heartbeat Loop (DataNode)

**File:** `src/node/data.rs:190-225`

```rust
async fn heartbeat_loop(
    client: Arc<MetaClient>,
    lease_id: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        if shutdown.load(Ordering::SeqCst) {
            info!("Heartbeat loop stopping due to shutdown");
            break;
        }

        let id = lease_id.load(Ordering::SeqCst);
        if id == 0 {
            continue;
        }

        match client.heartbeat(id).await {
            Ok(next_interval_ms) => {
                // Adjust interval if server suggests different timing
                if next_interval_ms > 0 && next_interval_ms != 5000 {
                    interval = tokio::time::interval(
                        Duration::from_millis(next_interval_ms as u64)
                    );
                }
            }
            Err(e) => {
                warn!("Heartbeat failed: {}. Will retry...", e);
            }
        }
    }
}
```

### Heartbeat Timeline

```
Time (seconds)    DataNode                    Meta Service
     0            register_node() ──────────► Creates lease (expires at T+30)
                  lease_id=1                  NodeJoined event

     5            heartbeat(1) ─────────────► Renews lease (expires at T+35)

    10            heartbeat(1) ─────────────► Renews lease (expires at T+40)

    15            heartbeat(1) ─────────────► Renews lease (expires at T+45)

    20            heartbeat(1) ─────────────► Renews lease (expires at T+50)

    25            heartbeat(1) ─────────────► Renews lease (expires at T+55)

    30            ... continues every 5s ...
```

### Heartbeat Processing (Meta)

**File:** `src/meta/raft/state_machine.rs:260-290`

```rust
fn heartbeat(&self, node_id: &str, lease_id: u64, timestamp: i64) -> MetaResponse {
    // 1. Update lease expiration
    let lease_key = format!("/leases/{}", lease_id);
    if let Ok(Some(data)) = self.db.get(lease_key.as_bytes()) {
        if let Ok(mut lease) = bincode::deserialize::<LeaseInfo>(&data) {
            if lease.node_id == node_id {
                lease.renew();  // Extends expiration by TTL
                self.db.insert(lease_key.as_bytes(), bincode::serialize(&lease).unwrap());
            }
        }
    }

    // 2. Update node's last_heartbeat and transition Starting → Active
    let node_key = format!("/nodes/{}", node_id);
    if let Ok(Some(data)) = self.db.get(node_key.as_bytes()) {
        if let Ok(mut info) = bincode::deserialize::<NodeInfo>(&data) {
            info.last_heartbeat = timestamp;
            if info.state == NodeState::Starting {
                info.state = NodeState::Active;  // Transition to Active
                // Could publish NodeStateChanged event here
            }
            self.db.insert(node_key.as_bytes(), bincode::serialize(&info).unwrap());
        }
    }

    MetaResponse::Success
}
```

---

## Node Failure Detection

### Lease Expiration Background Task

**File:** `src/meta/service/meta_node.rs:495-516`

```rust
pub fn start_background_tasks(self: &Arc<Self>) {
    let node = Arc::clone(self);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            // Only run on leader
            if !node.is_leader().await {
                continue;
            }

            // Check and expire stale leases
            let cmd = MetaCommand::ExpireLeases;
            if let Err(e) = node.apply_command(cmd).await {
                warn!("Failed to expire leases: {}", e);
            }
        }
    });
}
```

### Lease Expiration Logic

**File:** `src/meta/raft/state_machine.rs:615-637`

```rust
fn expire_leases(&self) -> MetaResponse {
    let now = current_timestamp_ms();
    let mut expired_nodes = Vec::new();

    // Scan all leases
    for item in self.db.scan_prefix("/leases/") {
        if let Ok((key, value)) = item {
            if let Ok(lease) = bincode::deserialize::<LeaseInfo>(&value) {
                if lease.expires_at < now {
                    // Lease expired - remove it
                    self.db.remove(&key);
                    expired_nodes.push(lease.node_id);
                }
            }
        }
    }

    // Deregister expired nodes (publishes NodeLeft events)
    for node_id in expired_nodes {
        self.deregister_node(&node_id);
    }

    MetaResponse::Success
}
```

### Node Failure Timeline

```
Time (seconds)    DataNode                    Meta Service
     0            register_node() ──────────► Lease expires at T+30

     5            heartbeat(1) ─────────────► Lease expires at T+35

    10            ╳ DataNode crashes
                  (no more heartbeats)

    15            ...                         Lease still valid (expires T+35)

    20            ...                         Lease still valid (expires T+35)

    25            ...                         Lease still valid (expires T+35)

    35            ...                         expire_leases() runs
                                              │
                                              ├─► Lease expired!
                                              ├─► deregister_node("data-1")
                                              ├─► Remove /nodes/data-1
                                              ├─► Remove /leases/1
                                              └─► Publish NodeLeft event
                                                      │
                                                      ▼
                                              Router receives event
                                              ShardCache.remove_data_node()
                                              ShardCache.remove_partitions_for_node()
```

---

## Graceful Shutdown

### Shutdown Flow (DataNode)

**File:** `src/node/data.rs:150-176`

```rust
// Signal handler for Ctrl+C
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    info!("Received shutdown signal, cleaning up...");

    // 1. Signal heartbeat loop to stop
    shutdown_flag.store(true, Ordering::SeqCst);

    // 2. Deregister from Meta service
    if let Some(ref client) = meta_client {
        let id = lease_id.load(Ordering::SeqCst);
        if id > 0 {
            if let Err(e) = client.deregister_node(id).await {
                warn!("Failed to deregister: {}", e);
            } else {
                info!("Deregistered from Meta service");
            }
        }
    }

    // 3. Clean up PID file
    common::remove_pid_file(&data_dir, NODE_TYPE);

    std::process::exit(0);
});
```

### Deregistration Processing

**File:** `src/meta/raft/state_machine.rs:295-320`

```rust
fn deregister_node(&self, node_id: &str) -> MetaResponse {
    let node_key = format!("/nodes/{}", node_id);

    // 1. Get node info before removal (for event payload)
    let node_info = self.db.get(node_key.as_bytes())
        .ok()
        .flatten()
        .and_then(|data| bincode::deserialize::<NodeInfo>(&data).ok());

    // 2. Remove node from storage
    self.db.remove(node_key.as_bytes());

    // 3. Publish NodeLeft event
    if let Some(info) = node_info {
        let payload = serde_json::to_vec(&info).unwrap_or_default();
        self.publish_event("nodes", EventType::NodeLeft, payload, node_id);
    }

    MetaResponse::Success
}
```

---

## Storage Keys

| Key Pattern | Type | Description |
|-------------|------|-------------|
| `/nodes/{node_id}` | `NodeInfo` | Registered node information |
| `/leases/{lease_id}` | `LeaseInfo` | Node lease for auto-expiration |
| `/events/{seq:020}` | `Event` | Event log (zero-padded sequence) |

---

## Event Types

| Event | Topic | Trigger | Payload |
|-------|-------|---------|---------|
| `NodeJoined` | `nodes` | `register_node()` | `NodeInfo` (JSON) |
| `NodeLeft` | `nodes` | `deregister_node()` or lease expiry | `NodeInfo` (JSON) |
| `NodeStateChanged` | `nodes` | State transition (Starting→Active) | `{node_id, old_state, new_state}` |

---

## Configuration

### DataNode Config

```toml
[data]
node_id = "data-node-1"
advertise_addr = "10.0.0.10:8815"
meta_endpoints = ["meta1:7100", "meta2:7100", "meta3:7100"]
```

### Lease TTL

- Default: 30 seconds
- Heartbeat interval: 5 seconds
- Safety margin: 6 heartbeats before lease expires

### Meta Background Task

- Lease expiration check: Every 1 second
- Only runs on leader node

---

## Failure Scenarios

### Scenario 1: DataNode Crash

```
1. DataNode stops sending heartbeats
2. Lease expires after TTL (30s from last heartbeat)
3. Meta leader's expire_leases() detects expiration
4. Meta publishes NodeLeft event
5. Routers remove node from ShardCache
6. Subsequent writes route to other healthy nodes
```

### Scenario 2: Network Partition (DataNode ↔ Meta)

```
1. DataNode cannot reach Meta service
2. Heartbeats fail but DataNode continues running
3. Lease expires on Meta side
4. Meta publishes NodeLeft event
5. Routers stop routing to this DataNode
6. DataNode should detect isolation and stop accepting writes
   (to avoid split-brain)
```

### Scenario 3: Meta Leader Failover

```
1. Meta leader crashes
2. Raft elects new leader (within election timeout)
3. New leader starts running background tasks
4. Lease expiration continues on new leader
5. Client connections automatically reconnect to new leader
```

### Scenario 4: Graceful Shutdown

```
1. DataNode receives SIGINT/SIGTERM
2. Sets shutdown flag (stops heartbeat loop)
3. Calls deregister_node(lease_id)
4. Meta immediately publishes NodeLeft event
5. Routers update ShardCache
6. DataNode exits
```

---

## Sequence Diagram: Complete Registration

```
┌────────┐          ┌────────────┐          ┌────────────┐          ┌────────┐
│DataNode│          │MetaClient  │          │Meta Leader │          │ Router │
└───┬────┘          └─────┬──────┘          └─────┬──────┘          └───┬────┘
    │                     │                       │                     │
    │ connect()           │                       │                     │
    │────────────────────►│                       │                     │
    │                     │ gRPC connect          │                     │
    │                     │──────────────────────►│                     │
    │                     │◄──────────────────────│                     │
    │◄────────────────────│                       │                     │
    │                     │                       │                     │
    │ register_node()     │                       │                     │
    │────────────────────►│                       │                     │
    │                     │ RegisterNodeRequest   │                     │
    │                     │──────────────────────►│                     │
    │                     │                       │                     │
    │                     │                       │ Raft: replicate     │
    │                     │                       │ ─────────────────── │
    │                     │                       │                     │
    │                     │                       │ Apply to state      │
    │                     │                       │ machine             │
    │                     │                       │                     │
    │                     │                       │ publish NodeJoined  │
    │                     │                       │─────────────────────┼────►
    │                     │                       │                     │
    │                     │                       │                     │ Update
    │                     │                       │                     │ ShardCache
    │                     │                       │                     │
    │                     │ RegisterNodeResponse  │                     │
    │                     │◄──────────────────────│                     │
    │◄────────────────────│ lease_id=1           │                     │
    │                     │                       │                     │
    │ spawn heartbeat_loop│                       │                     │
    │ ──────────────────  │                       │                     │
    │                     │                       │                     │
    │        [every 5s]   │                       │                     │
    │ heartbeat(1)        │                       │                     │
    │────────────────────►│ HeartbeatRequest      │                     │
    │                     │──────────────────────►│                     │
    │                     │                       │ Renew lease         │
    │                     │ HeartbeatResponse     │                     │
    │                     │◄──────────────────────│                     │
    │◄────────────────────│                       │                     │
    │                     │                       │                     │
```

---

## Files Reference

| File | Purpose |
|------|---------|
| `src/node/data.rs` | DataNode startup, registration, heartbeat loop |
| `src/meta/client/meta_client.rs` | gRPC client for Meta service |
| `src/meta/service/grpc.rs` | Meta gRPC server handlers |
| `src/meta/service/meta_node.rs` | Raft routing, background tasks |
| `src/meta/raft/state_machine.rs` | State machine apply, event publishing |
| `src/meta/types.rs` | NodeInfo, LeaseInfo, Event types |
| `src/router/routing/cache_invalidation.rs` | Event subscription, ShardCache updates |
| `src/router/routing/shard_cache.rs` | Node tracking, partition routing |

---

## Related Documentation

- [Meta Service Raft Implementation](architecture-meta-raft.md) - Raft consensus details
- [Cluster Architecture](architecture-cluster-v2.md) - Full cluster design
- [Router Architecture](architecture-router.md) - Query routing design
