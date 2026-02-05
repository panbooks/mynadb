# MynaDB Cluster Architecture v2

## Overview

MynaDB Cluster v2 introduces a **built-in Raft-based Meta Service** that eliminates the need for external dependencies like etcd or Redis. This follows the industry trend established by Kafka KRaft, ClickHouse Keeper, and TiDB PD.

The architecture separates concerns into three node types:
- **Meta Node**: Raft-based metadata and coordination service
- **Router Node**: Client protocol handling and request routing
- **Data Node**: Storage, caching, and query execution

## Architecture Diagram

```
                                    CLIENTS
                        (HTTP / Arrow Flight / MySQL Wire)
                                       │
                                       ▼
                              ┌─────────────────┐
                              │  LOAD BALANCER  │
                              └────────┬────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
          ▼                            ▼                            ▼
   ┌─────────────┐              ┌─────────────┐              ┌─────────────┐
   │  ROUTER 1   │              │  ROUTER 2   │              │  ROUTER N   │
   │ (Stateless) │              │ (Stateless) │              │ (Stateless) │
   │             │              │             │              │             │
   │ • HTTP API  │              │ • HTTP API  │              │ • HTTP API  │
   │ • Flight    │              │ • Flight    │              │ • Flight    │
   │ • MySQL     │              │ • MySQL     │              │ • MySQL     │
   │ • Shard Map │              │ • Shard Map │              │ • Shard Map │
   │   (cached)  │              │   (cached)  │              │   (cached)  │
   └──────┬──────┘              └──────┬──────┘              └──────┬──────┘
          │                            │                            │
          └────────────────────────────┼────────────────────────────┘
                                       │ gRPC (internal)
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
          ▼                            ▼                            ▼
   ┌─────────────┐              ┌─────────────┐              ┌─────────────┐
   │ DATA NODE 1 │              │ DATA NODE 2 │              │ DATA NODE N │
   │             │              │             │              │             │
   │ • DataCache │              │ • DataCache │              │ • DataCache │
   │ • QueryEng  │              │ • QueryEng  │              │ • QueryEng  │
   │ • WAL       │              │ • WAL       │              │ • WAL       │
   │ • 3-tier    │              │ • 3-tier    │              │ • 3-tier    │
   │   cache     │              │   cache     │              │   cache     │
   └──────┬──────┘              └──────┬──────┘              └──────┬──────┘
          │                            │                            │
          └────────────────────────────┼────────────────────────────┘
                                       │
            ┌──────────────────────────┼──────────────────────────┐
            │                          │                          │
            ▼                          ▼                          ▼
     ┌────────────┐             ┌────────────┐             ┌────────────┐
     │ META NODE 1│◄───────────►│ META NODE 2│◄───────────►│ META NODE 3│
     │  (LEADER)  │ Raft Log    │ (FOLLOWER) │ Raft Log    │ (FOLLOWER) │
     │            │ Replication │            │ Replication │            │
     │ • openraft │             │ • openraft │             │ • openraft │
     │ • sled KV  │             │ • sled KV  │             │ • sled KV  │
     │ Port: 7100 │             │ Port: 7100 │             │ Port: 7100 │
     └────────────┘             └────────────┘             └────────────┘
                                       │
                                       ▼
     ┌────────────────────────────────────────────────────────────────────┐
     │                      SHARED OBJECT STORAGE                          │
     │                     (S3 / MinIO / OSS / Local)                      │
     │                                                                     │
     │  /{catalog}/{schema}/{table}/date=YYYY-MM-DD/                       │
     │      ├── L0_*.parquet   (recent writes)                             │
     │      ├── L1_*.parquet   (first compaction)                          │
     │      └── L2_*.parquet   (higher levels)                             │
     └────────────────────────────────────────────────────────────────────┘
```

## Node Types

| Node Type | Count | Responsibilities | Stateful | Scaling |
|-----------|-------|------------------|----------|---------|
| **Meta** | 3 or 5 (fixed) | Raft consensus, metadata, locks, events | Yes (sled) | Fixed quorum |
| **Router** | 2+ (elastic) | Protocol handling, write routing, query coordination | No | Horizontal |
| **Data** | N (elastic) | Local writes, query execution, cache, flush | Yes (WAL/cache) | Horizontal |
| **Standalone** | 1 | All-in-one (default mode) | Yes | None |

## Key Improvements Over v1 (etcd-based)

| Aspect | v1 (etcd) | v2 (Built-in Meta) |
|--------|-----------|-------------------|
| External dependencies | etcd cluster required | None |
| Deployment complexity | Deploy + manage etcd | Single binary |
| Lock acquisition | Non-atomic (race conditions) | Raft linearizable |
| Pub/sub reliability | Best-effort (TTL keys) | Sequence-based (no loss) |
| Failover time | 5-10 seconds | <1 second |
| Network hops | App → etcd → App | App → Meta (embedded) |

---

## Meta Node

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Consensus | openraft | Raft implementation |
| Storage | sled | Embedded KV store |
| RPC | tonic/gRPC | Service communication |
| Serialization | bincode | Efficient binary encoding |

### State Machine Data

The Meta state machine stores all cluster metadata in sled with these key prefixes:

```
/nodes/{node_id}           → NodeInfo (registration, health, role)
/files/{path}              → FileInfo (parquet file metadata)
/locks/{key}               → LockInfo (distributed locks with fencing tokens)
/leaders/{election}        → LeaderInfo (leader election state)
/events/{sequence}         → Event (append-only event log)
```

### Commands (Raft Log Entries)

```rust
pub enum MetaCommand {
    // Node management
    RegisterNode { info: NodeInfo, lease_id: u64, lease_ttl_ms: u64 },
    Heartbeat { node_id: String, lease_id: u64, timestamp: i64 },
    DeregisterNode { node_id: String },
    ExpireLeases,

    // File registry
    RegisterFiles { files: Vec<FileInfo> },
    RemoveFiles { paths: Vec<String> },

    // Distributed locks (with fencing tokens)
    AcquireLock { key: String, holder: String, ttl_ms: u64 },
    ReleaseLock { key: String, holder: String, fencing_token: u64 },
    RenewLock { key: String, holder: String, fencing_token: u64, ttl_ms: u64 },

    // Leader election
    Campaign { election: String, node_id: String, ttl_ms: u64 },
    Resign { election: String, node_id: String, term: u64 },

    // Events
    PublishEvent { topic: String, event_type: EventType, payload: Vec<u8>, source_node: String },
}
```

### gRPC API

```protobuf
service MetaService {
    // Node management
    rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc DeregisterNode(DeregisterNodeRequest) returns (DeregisterNodeResponse);
    rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);

    // File registry
    rpc RegisterFiles(RegisterFilesRequest) returns (RegisterFilesResponse);
    rpc RemoveFiles(RemoveFilesRequest) returns (RemoveFilesResponse);
    rpc ListFilesForPartition(ListFilesForPartitionRequest) returns (ListFilesResponse);
    rpc GetFilesForQuery(GetFilesForQueryRequest) returns (GetFilesForQueryResponse);

    // Distributed locks
    rpc TryLock(TryLockRequest) returns (TryLockResponse);
    rpc Unlock(UnlockRequest) returns (UnlockResponse);
    rpc RenewLock(RenewLockRequest) returns (RenewLockResponse);
    rpc GetLock(GetLockRequest) returns (GetLockResponse);

    // Leader election
    rpc Campaign(CampaignRequest) returns (CampaignResponse);
    rpc Resign(ResignRequest) returns (ResignResponse);
    rpc GetLeader(GetLeaderRequest) returns (GetLeaderResponse);

    // Event streaming
    rpc Subscribe(SubscribeRequest) returns (stream Event);
    rpc Publish(PublishRequest) returns (PublishResponse);

    // Cluster status
    rpc GetClusterStatus(GetClusterStatusRequest) returns (ClusterStatusResponse);
}
```

### Configuration

```toml
[meta]
node_id = 1
bind_addr = "0.0.0.0:7100"
advertise_addr = "meta1:7100"
data_dir = "/data/meta"
peers = ["meta1:7100", "meta2:7100", "meta3:7100"]

[meta.raft]
heartbeat_interval_ms = 150
election_timeout_min_ms = 300
election_timeout_max_ms = 500
snapshot_threshold = 10000
max_log_size_mb = 256

[meta.events]
max_events = 100000
retention_secs = 86400
```

---

## Router Node

### Responsibilities

1. **Protocol Handling**: HTTP REST, Arrow Flight, MySQL wire protocol
2. **Write Routing**: Dispatch writes to Data nodes based on partition affinity
3. **Query Coordination**: Fan-out queries and aggregate results
4. **Shard Mapping**: Cache partition→node mappings from Meta service
5. **Load Balancing**: Distribute requests across Data nodes

### Shard Cache

The Router maintains a local cache of partition ownership:

```rust
pub struct ShardCache {
    /// Partition → assigned Data node mapping
    partition_owners: DashMap<String, DataNodeInfo>,

    /// Active Data nodes (healthy only)
    data_nodes: RwLock<Vec<DataNodeInfo>>,

    /// Last sequence number from Meta event stream
    last_event_seq: AtomicU64,
}
```

The cache is updated via Meta service events (nodes joining/leaving, files registered).

### Write Routing Algorithm

```
1. Parse line protocol, extract partition key (catalog/schema/table/date)
2. For each partition:
   a. Check ShardCache for existing owner
   b. If miss: Hash(partition) % active_nodes → select node
3. Group points by target Data node
4. Dispatch writes in parallel via gRPC
5. Aggregate responses
```

### Query Coordination

```
1. Parse SQL, extract table references and time range
2. Determine target nodes:
   - Point query: route to partition owner
   - Range query: fan-out to nodes with relevant partitions
   - Full scan: query all active Data nodes
3. Send query to targets in parallel
4. Collect partial results
5. Aggregate if needed (GROUP BY, ORDER BY, LIMIT)
6. Return final result
```

### Configuration

```toml
[router]
node_id = "router-1"

[router.server]
http_port = 7000
flight_port = 7001
mysql_port = 7002
mysql_enabled = true

[router.meta]
endpoints = ["meta1:7100", "meta2:7100", "meta3:7100"]
connect_timeout_ms = 5000
request_timeout_ms = 30000

[router.routing]
cache_refresh_interval_ms = 1000
data_node_pool_size = 10
data_node_connect_timeout_ms = 5000
data_node_request_timeout_ms = 60000

[router.query]
cache_enabled = true
cache_max_size_mb = 512
cache_ttl_secs = 300
slow_query_threshold_ms = 100
max_parallel_fanout = 16
```

---

## Data Node

### Responsibilities

1. **Write Handling**: Receive writes from Router, write to WAL and cache
2. **Query Execution**: Execute queries locally using DataFusion
3. **Cache Management**: Manage in-memory DataCache with background flush
4. **File Registration**: Register flushed Parquet files with Meta service
5. **Lock Coordination**: Acquire partition locks from Meta for writes

### Internal gRPC Service

```protobuf
service DataNodeService {
    // Write operations
    rpc Write(WriteRequest) returns (WriteResponse);
    rpc BatchWrite(stream WriteRequest) returns (WriteResponse);

    // Query operations
    rpc Query(QueryRequest) returns (stream QueryResponse);
    rpc QueryArrow(QueryArrowRequest) returns (stream QueryArrowResponse);

    // Admin
    rpc Flush(FlushRequest) returns (FlushResponse);
    rpc GetStats(GetStatsRequest) returns (GetStatsResponse);
    rpc Health(HealthRequest) returns (HealthResponse);
}
```

### Write Flow

```
Router → gRPC Write Request
   │
   ├─► Validate fencing token (from Meta lock)
   │
   ├─► Parse line protocol
   │
   ├─► Write to WAL (durability)
   │
   ├─► Add to DataCache (in-memory)
   │
   └─► Return success

(Background)
   │
   ├─► Flush cache to Parquet (on threshold/timer)
   │
   ├─► Upload to Object Storage
   │
   └─► Register files with Meta service
```

### Configuration

```toml
[data]
node_id = "data-1"
advertise_addr = "data1:7200"
grpc_port = 7200

[data.meta]
endpoints = ["meta1:7100", "meta2:7100", "meta3:7100"]
connect_timeout_ms = 5000

[data.storage]
data_dir = "/data/mynadb"

[data.storage.object_store]
backend = "s3"
bucket = "mynadb-data"
endpoint = "http://minio:9000"

[data.cache]
max_size_mb = 4096
flush_interval_secs = 60

[data.wal]
enabled = true
sync_on_write = false
rotate_size_mb = 128
```

---

## Event System

### Event Types

```rust
pub enum EventType {
    NodeJoined = 1,
    NodeLeft = 2,
    NodeStateChanged = 3,
    FilesRegistered = 4,
    FilesRemoved = 5,
    LeaderChanged = 6,
    CacheInvalidate = 7,
    SchemaChanged = 8,
}
```

### Event Subscription

Routers and Data nodes subscribe to Meta events for cache invalidation:

```rust
// Subscribe to specific topics
let config = SubscriberConfig {
    endpoints: vec!["meta1:7100", "meta2:7100"],
    topics: vec!["nodes", "files"],
    reconnect_delay_ms: 1000,
};

let mut subscriber = EventSubscriber::new(config);
let (event_rx, handle) = subscriber.start(0); // Start from sequence 0

// Process events
while let Some(event) = event_rx.recv().await {
    match event.event_type {
        EventType::NodeJoined => update_node_list(&event),
        EventType::FilesRegistered => update_shard_cache(&event),
        _ => {}
    }
}
```

### Sequence-Based Delivery

Events have monotonic sequence numbers ensuring no message loss:

```
Event 1 → Event 2 → Event 3 → Event 4
   ↑                    ↑
Subscriber A         Subscriber B
(seq: 1)             (seq: 3)

On reconnect, subscribers resume from last_seq + 1
```

---

## Data Flow

### Write Path

```
Client → POST /write/:db (line protocol)
   │
   ▼
┌──────────────────────────────────────────────────────────────────┐
│                         ROUTER NODE                               │
│  1. Parse line protocol                                           │
│  2. Group points by partition (catalog/schema/table/date)         │
│  3. Select Data nodes via ShardCache (hash-based affinity)        │
│  4. Dispatch writes in parallel                                   │
└──────────────────────────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │ gRPC               │ gRPC               │ gRPC
          ▼                    ▼                    ▼
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │ DATA NODE 1 │      │ DATA NODE 2 │      │ DATA NODE N │
   │  5. WAL     │      │             │      │             │
   │  6. Cache   │      │             │      │             │
   │  7. Return  │      │             │      │             │
   └──────┬──────┘      └─────────────┘      └─────────────┘
          │
          │ Background Flush
          │
          ├─► 8. Convert to Parquet
          ├─► 9. Upload to Object Storage
          └─► 10. RegisterFiles → Meta Service
```

### Read Path

```
Client → GET /query/:db?sql=SELECT...
   │
   ▼
┌──────────────────────────────────────────────────────────────────┐
│                         ROUTER NODE                               │
│  1. Check query cache                                             │
│  2. Parse SQL, extract tables/time range                          │
│  3. GetFilesForQuery → Meta Service                               │
│  4. Determine target Data nodes                                   │
│  5. Fan-out query in parallel                                     │
└──────────────────────────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │ gRPC               │ gRPC               │ gRPC
          ▼                    ▼                    ▼
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │ DATA NODE 1 │      │ DATA NODE 2 │      │ DATA NODE N │
   │  6. Local   │      │             │      │             │
   │     cache   │      │             │      │             │
   │  7. Parquet │      │             │      │             │
   │     fetch   │      │             │      │             │
   │  8. Query   │      │             │      │             │
   │  9. Return  │      │             │      │             │
   └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    ROUTER NODE (Aggregation)                      │
│  10. Collect partial results                                      │
│  11. Aggregate (GROUP BY, ORDER BY, LIMIT)                        │
│  12. Cache result (if slow query)                                 │
│  13. Return to client                                             │
└──────────────────────────────────────────────────────────────────┘
```

---

## Module Structure

```
src/
├── meta/                           # META NODE
│   ├── mod.rs                      # Module exports
│   ├── config.rs                   # MetaConfig, RaftConfig
│   ├── types.rs                    # NodeInfo, FileInfo, LockInfo, Event, etc.
│   ├── raft/
│   │   ├── mod.rs
│   │   ├── state_machine.rs        # Apply MetaCommand to sled
│   │   └── network.rs              # Peer connection management
│   ├── service/
│   │   ├── mod.rs
│   │   ├── grpc.rs                 # gRPC service implementation
│   │   └── meta_node.rs            # MetaNode coordinator
│   └── client/
│       ├── mod.rs
│       ├── meta_client.rs          # Client for Router/Data nodes
│       └── event_subscriber.rs     # Event subscription client
│
├── router/                         # ROUTER NODE
│   ├── mod.rs                      # Module exports
│   ├── config.rs                   # RouterConfig
│   ├── routing/
│   │   ├── mod.rs
│   │   ├── shard_cache.rs          # Partition→Node mapping cache
│   │   ├── write_router.rs         # Write dispatch logic
│   │   ├── query_coordinator.rs    # Query fan-out & aggregation
│   │   └── cache_invalidation.rs   # Event-based cache updates
│   └── client/
│       ├── mod.rs
│       └── data_node_pool.rs       # Connection pool to Data nodes
│
├── data/                           # DATA NODE
│   ├── mod.rs                      # Module exports
│   ├── config.rs                   # DataNodeConfig
│   └── server/
│       ├── mod.rs
│       └── grpc.rs                 # Internal gRPC service
│
└── proto/
    ├── meta.proto                  # Meta service definitions
    └── datanode.proto              # DataNode service definitions
```

---

## Performance Characteristics

| Operation | Standalone | Router+Data (cache hit) | Router+Data (cache miss) |
|-----------|------------|------------------------|-------------------------|
| Write Latency (p50) | 5-10ms | 15-25ms | 15-25ms |
| Write Latency (p99) | 20-50ms | 40-80ms | 40-80ms |
| Query Latency (p50) | 10-50ms | 10-50ms | 50-150ms |
| Lock Acquire | N/A | 5-10ms | 5-10ms |
| Meta Failover | N/A | 0.5-1s | 0.5-1s |
| Write Throughput | 100K pts/s | 100K pts/s/router | - |

### Scalability

| Component | Scaling | Bottleneck |
|-----------|---------|------------|
| Router | Horizontal (2-N) | CPU for parsing, network |
| Data | Horizontal (3-N) | Object storage bandwidth |
| Meta | Fixed (3 or 5) | Raft consensus |

---

## Failure Handling

### Meta Node Failure

1. Raft detects leader failure (heartbeat timeout: 150ms)
2. Followers start election (timeout: 300-500ms)
3. New leader elected (~650ms total)
4. Routers reconnect to new leader
5. Operations resume

### Data Node Failure

1. Router detects via health check or connection error
2. ShardCache marks node as unhealthy
3. Writes routed to other nodes
4. No data loss (shared object storage)

### Router Node Failure

1. Load balancer detects failure
2. Traffic redirected to other Routers
3. Stateless - no recovery needed

### Network Partition

1. Minority partition loses Meta quorum
2. Writes blocked in minority partition
3. Majority partition continues normally
4. On heal, minority nodes resync

---

## Comparison with v1

| Feature | v1 (etcd) | v2 (Built-in Meta) |
|---------|-----------|-------------------|
| External deps | etcd cluster | None |
| Single binary | No | Yes |
| Lock atomicity | Non-atomic | Raft linearizable |
| Event delivery | Best-effort | Guaranteed (sequence) |
| Failover time | 5-10s | <1s |
| Write routing | Any node + contention | Affinity-based |
| Query execution | Single node | Distributed fan-out |
| Horizontal scale | Limited | Router + Data nodes |
