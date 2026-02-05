# MynaDB Enterprise Cluster Architecture

## Overview

MynaDB Enterprise Edition transforms the standalone time series database into a distributed, horizontally-scalable cluster with compute-storage separation. This architecture enables auto-scaling, high availability, and cloud-native deployments.

## Architecture Diagram

```
                              Clients
                                 │
                                 ▼
                         ┌─────────────┐
                         │Load Balancer│
                         │  (HAProxy)  │
                         └──────┬──────┘
                                │
           ┌────────────────────┼────────────────────┐
           │                    │                    │
           ▼                    ▼                    ▼
    ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
    │   Node 1    │      │   Node 2    │      │   Node 3    │
    │  (Compute)  │      │  (Compute)  │      │  (Compute)  │
    │ + LocalCache│      │ + LocalCache│      │ + LocalCache│
    └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
           │                    │                    │
           └────────────────────┼────────────────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────┐
    │                   SHARED OBJECT STORAGE                  │
    │                    (S3 / MinIO / OSS)                    │
    │  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
    │  │  data/   │  │   wal/   │  │ metadata/│              │
    │  │ parquet  │  │ segments │  │  catalog │              │
    │  └──────────┘  └──────────┘  └──────────┘              │
    └─────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌─────────────────────────────────────────────────────────┐
    │                  COORDINATION SERVICE                    │
    │                         (etcd)                           │
    │    • Distributed Locks  • Leader Election  • Pub/Sub    │
    └─────────────────────────────────────────────────────────┘
```

## Why Shared-Storage Architecture?

| Benefit | Description |
|---------|-------------|
| **Auto-scaling** | Stateless compute nodes can be added/removed freely |
| **Durability** | S3 provides 11-nines durability as source of truth |
| **Cost Efficient** | Single data copy + cheap object storage |
| **Simple Operations** | No data rebalancing or migration needed |
| **Cloud-Native** | Perfect for Kubernetes and containerized deployments |

## Component Details

### 1. Coordinator Layer

**Location**: `src/cluster/coordinator/`

The coordinator provides distributed primitives for cluster coordination.

#### Coordinator Trait
```rust
#[async_trait]
pub trait Coordinator: Send + Sync {
    // Key-value operations
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    async fn put(&self, key: &str, value: &[u8]) -> Result<(), String>;
    async fn delete(&self, key: &str) -> Result<(), String>;

    // Distributed locking
    async fn try_lock(&self, key: &str, holder: &str, ttl: Duration) -> Result<bool, String>;
    async fn unlock(&self, key: &str, holder: &str) -> Result<(), String>;

    // Watch and pub/sub
    async fn watch(&self, prefix: &str) -> Result<WatchStream, String>;
    async fn publish(&self, channel: &str, message: &[u8]) -> Result<(), String>;
    async fn subscribe(&self, channel: &str) -> Result<Subscription, String>;

    // Leader election
    async fn campaign(&self, name: &str, value: &str, lease_id: i64) -> Result<bool, String>;
    async fn resign(&self, name: &str) -> Result<(), String>;
}
```

#### etcd Implementation
- **File**: `src/cluster/coordinator/etcd.rs`
- **Features**:
  - Distributed locks with TTL and auto-renewal
  - Leader election with lease-based campaigns
  - Watch streams for key changes
  - Pub/sub for cache invalidation

#### etcd Key Structure
```
/mynadb/
├── nodes/{node_id}              # Node registration (ephemeral leases)
├── locks/
│   ├── write/{catalog}/{schema}/{table}/{date}  # Partition write locks
│   ├── compaction/{path}        # Compaction locks
│   └── metadata/{path}          # Metadata update locks
├── leader/
│   └── compactor                # Compaction leader election
└── cache/
    └── invalidate               # Cache invalidation channel
```

### 2. Node Identity & Registration

**Location**: `src/cluster/node/`

#### Node Information
```rust
pub struct NodeInfo {
    pub node_id: String,           // Unique node identifier
    pub advertise_addr: String,    // Address for other nodes to connect
    pub role: NodeRole,            // Compute, Compactor, or Hybrid
    pub state: NodeState,          // Starting, Active, Draining, Left
    pub http_port: u16,
    pub flight_port: u16,
    pub mysql_port: Option<u16>,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub version: String,
    pub hostname: String,
    pub is_leader: bool,
}

pub enum NodeRole {
    Compute,    // Query processing only
    Compactor,  // Background compaction only
    Hybrid,     // Both compute and compaction
}

pub enum NodeState {
    Starting,   // Node is initializing
    Active,     // Node is healthy and serving requests
    Draining,   // Node is preparing to leave (no new writes)
    Left,       // Node has left the cluster
}
```

#### Registration Flow
```
Startup:
1. Generate or load node_id
2. Create lease in etcd (30s TTL)
3. Register node info under /mynadb/nodes/{node_id}
4. Start heartbeat task (renews lease every 5s)

Graceful Shutdown:
1. Set state to Draining
2. Wait for in-flight requests
3. Deregister from etcd
4. Release lease
```

### 3. Write Coordination

**Location**: `src/cluster/write_coordinator.rs`

Coordinates writes across nodes to prevent conflicts using partition-level locking.

#### Write Flow
```
1. Client → POST /write/:db
2. Parse line protocol
3. Group points by partition (catalog/schema/table/date)
4. For each partition:
   a. Acquire lock: /mynadb/locks/write/{partition}
   b. Write to local WAL (with node_id prefix)
   c. Buffer in memory cache
5. Return 204 OK
6. (Background) Flush to S3
7. Release lock
8. Publish cache invalidation
```

#### Lock Options
```rust
pub struct LockOptions {
    pub ttl: Duration,           // Lock timeout (default: 30s)
    pub acquire_timeout: Duration, // How long to wait for lock
    pub auto_renew: bool,        // Auto-renew while held
}
```

### 4. Cache Invalidation

**Location**: `src/cluster/cache_invalidation.rs`

Keeps local caches consistent across nodes using pub/sub messaging.

#### Invalidation Message
```rust
pub struct CacheInvalidation {
    pub paths: Vec<String>,         // Affected file paths
    pub reason: InvalidationReason, // NewFile, FileDeleted, Compacted
    pub source_node: String,        // Node that originated the change
    pub timestamp: i64,             // Unix timestamp
}

pub enum InvalidationReason {
    NewFile,      // New Parquet file written
    FileDeleted,  // File removed (retention/compaction)
    Compacted,    // Files merged during compaction
    MetadataUpdate, // Schema or partition metadata changed
}
```

#### Invalidation Flow
```
1. Node A writes new Parquet file
2. Node A publishes invalidation to /mynadb/cache/invalidate
3. Nodes B, C receive message via subscription
4. Each node invalidates affected cache entries:
   - Metadata cache (table/partition info)
   - File cache (Parquet file cache)
   - Query cache (cached query results)
```

### 5. Leader Election

**Location**: `src/cluster/election.rs`

Ensures only one node runs background tasks like compaction.

#### Election States
```rust
pub enum LeaderState {
    NotStarted,   // Election not initiated
    Campaigning,  // Attempting to become leader
    Leader,       // Currently the leader
    Follower,     // Another node is leader
    Resigned,     // Voluntarily gave up leadership
}
```

#### Leader Election Flow
```
1. On startup, campaign for leadership
2. If elected:
   a. Set state to Leader
   b. Start compaction jobs
   c. Renew lease periodically
3. If not elected:
   a. Set state to Follower
   b. Watch for leader changes
4. On leader failure:
   a. All followers detect via lease expiry
   b. Re-campaign with exponential backoff
   c. New leader elected within 30s
```

### 6. Cluster API

**Location**: `src/cluster/api/`

HTTP endpoints for cluster status and management.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/status` | GET | Overall cluster status |
| `/cluster/nodes` | GET | List all registered nodes |
| `/cluster/leader` | GET | Current leader information |
| `/cluster/health` | GET | Cluster health check |
| `/cluster/drain` | POST | Drain a node (prepare for removal) |
| `/cluster/remove` | DELETE | Remove a node from cluster |

#### Status Response
```json
{
  "enabled": true,
  "node_id": "mynadb-1",
  "role": "hybrid",
  "state": "active",
  "is_leader": true,
  "leader_id": "mynadb-1",
  "total_nodes": 3,
  "healthy_nodes": 3,
  "coordinator_type": "etcd",
  "coordinator_connected": true
}
```

### 7. Metrics

**Location**: `src/cluster/metrics.rs`

Prometheus metrics for cluster monitoring.

| Metric | Type | Description |
|--------|------|-------------|
| `mynadb_cluster_nodes_total` | Gauge | Total number of nodes |
| `mynadb_cluster_nodes_healthy` | Gauge | Number of healthy nodes |
| `mynadb_cluster_is_leader` | Gauge | 1 if this node is leader |
| `mynadb_cluster_leader_elections_total` | Counter | Leader election count |
| `mynadb_cluster_lock_acquisitions_total` | Counter | Lock acquisition count |
| `mynadb_cluster_lock_wait_seconds` | Histogram | Lock wait time |
| `mynadb_cluster_cache_invalidations_total` | Counter | Cache invalidations sent |

## Configuration

### Cluster Configuration
```toml
[cluster]
enabled = true
node_id = ""                    # Auto-generated if empty
advertise_addr = "0.0.0.0:7000"
role = "hybrid"                 # "compute", "compactor", "hybrid"

[cluster.coordinator]
type = "etcd"
endpoints = ["etcd-1:2379", "etcd-2:2379", "etcd-3:2379"]
lock_prefix = "/mynadb/locks/"
cache_prefix = "/mynadb/cache/"
lock_ttl_secs = 30
heartbeat_interval_secs = 5

[cluster.cache]
invalidation_channel = "/mynadb/cache/invalidate"
invalidation_method = "pubsub"

[cluster.write]
acquire_lock_timeout_secs = 10
flush_notify = true

[cluster.compaction]
leader_only = true
election_key = "/mynadb/leader/compactor"

[cluster.health]
check_interval_secs = 10
unhealthy_threshold = 3
```

## Deployment

### Kubernetes

The cluster includes ready-to-use Kubernetes manifests:

```
deploy/kubernetes/
├── configmap.yaml     # Configuration
├── deployment.yaml    # StatefulSet for nodes
├── service.yaml       # Services (headless + load balancer)
└── hpa.yaml           # Horizontal Pod Autoscaler
```

#### Deploy to Kubernetes
```bash
# Create namespace
kubectl create namespace mynadb

# Apply manifests
kubectl apply -f deploy/kubernetes/

# Check status
kubectl get pods -n mynadb
kubectl get svc -n mynadb
```

### Docker Compose

For local development and testing:

```bash
# Start cluster with etcd and MinIO
docker-compose -f deploy/docker-compose.cluster.yaml up -d

# Verify nodes registered
curl http://localhost:7000/cluster/nodes

# Write to node 1
curl -X POST http://localhost:7000/write/testdb \
  -d "cpu,host=a value=1"

# Query from node 2 (should see the data)
curl http://localhost:7010/query/testdb \
  -d "SELECT * FROM cpu"

# Check cluster status
curl http://localhost:7000/cluster/status
```

## Data Flow

### Write Path (Clustered)
```
Client
   │
   ▼
Load Balancer
   │
   ▼
Node N (receives write)
   │
   ├─► Parse line protocol
   │
   ├─► Acquire partition lock (etcd)
   │
   ├─► Write to WAL (node-specific path)
   │
   ├─► Buffer in memory cache
   │
   ├─► Return 204 OK
   │
   ├─► (Background) Flush to S3
   │
   ├─► Release lock
   │
   └─► Publish cache invalidation
          │
          ▼
       Other nodes invalidate caches
```

### Query Path (Clustered)
```
Client
   │
   ▼
Load Balancer
   │
   ▼
Node N (receives query)
   │
   ├─► Check metadata cache (refresh if stale)
   │
   ├─► Check local Parquet cache
   │      │
   │      └─► Cache miss? Fetch from S3
   │
   ├─► Query memtable (in-flight writes)
   │
   ├─► Execute query (DataFusion)
   │
   └─► Return results
```

## Failure Handling

### Node Failure
1. Lease expires in etcd (30s TTL)
2. Other nodes detect via watch
3. Node removed from cluster view
4. If leader failed, re-election triggers
5. New leader resumes background tasks

### Network Partition
1. Partitioned nodes lose etcd connection
2. Locks auto-expire (TTL-based)
3. Nodes fall back to standalone mode
4. On reconnection, re-register and sync

### Coordinator Failure
1. etcd cluster tolerates minority failures
2. If majority fails, cluster pauses writes
3. Reads continue from local cache
4. On recovery, normal operation resumes

## Performance Characteristics

| Metric | Standalone | Cluster (cache hit) | Cluster (cache miss) |
|--------|------------|---------------------|----------------------|
| Write Latency (p50) | 5-10ms | 10-30ms | 50-100ms |
| Write Latency (p99) | 20-50ms | 50-100ms | 150-300ms |
| Query Latency (p50) | 10-50ms | 10-50ms | 100-300ms |
| Write Throughput | 100K pts/s | 100K pts/s/node | - |

## Module Structure

```
src/cluster/
├── mod.rs                  # Module exports
├── config.rs               # ClusterConfig struct
├── coordinator/
│   ├── mod.rs
│   ├── interface.rs        # Coordinator trait
│   ├── etcd.rs             # etcd implementation
│   ├── locks.rs            # DistributedLock helpers
│   └── noop.rs             # No-op for standalone mode
├── node/
│   ├── mod.rs
│   ├── identity.rs         # NodeInfo, NodeRole, NodeState
│   ├── registration.rs     # Register/deregister with etcd
│   └── health.rs           # Health check logic
├── cache_invalidation.rs   # Pub/sub invalidation
├── election.rs             # Leader election
├── write_coordinator.rs    # Partition locking
├── metrics.rs              # Prometheus metrics
└── api/
    ├── mod.rs
    ├── status.rs           # Status endpoints
    └── management.rs       # Drain/remove endpoints
```

## Dependencies

```toml
# Cluster support
etcd-client = "0.14"      # etcd coordination
prometheus = "0.13"       # Metrics
uuid = "1.7"              # Node ID generation
hostname = "0.4"          # Host identification
```

## Comparison with Alternatives

| Feature | MynaDB Cluster | Shared-Nothing (Raft) | Master-Slave |
|---------|---------------|----------------------|--------------|
| Complexity | Medium | Very High | Low |
| Auto-scaling | Excellent | Hard (rebalancing) | None |
| Write scaling | Per-partition | Sharded | Single master |
| Durability | S3 (11 nines) | Raft quorum | Replication lag |
| Failover time | Instant (stateless) | 10-15s | 10-30s |
| Best for | Cloud, elastic | On-prem, strong consistency | Simple HA |
