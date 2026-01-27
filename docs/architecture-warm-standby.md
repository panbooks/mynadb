# Architecture: Warm Standby Replication

This document describes the warm standby replication architecture for zero-downtime failover in MynaDB cluster mode.

## Overview

Warm standby replication maintains in-memory copies of partition data on standby nodes, enabling instant failover when a leader node fails. Unlike traditional WAL replay (which can take 30-120 seconds), warm standby provides sub-second failover.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WARM STANDBY ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Router                                                                 │
│     │                                                                    │
│     ▼                                                                    │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    Partition P1 Replica Group                      │   │
│  │                                                                    │   │
│  │   ┌──────────────┐      Replication      ┌──────────────┐         │   │
│  │   │   Data-1     │─────────────────────▶│   Data-2     │         │   │
│  │   │   (Leader)   │      Stream           │  (Standby)   │         │   │
│  │   └──────────────┘                       └──────────────┘         │   │
│  │         │                                      │                   │   │
│  │         ▼                                      ▼                   │   │
│  │   ┌──────────┐                          ┌──────────┐              │   │
│  │   │ MemTable │                          │ MemTable │ ← Same data! │   │
│  │   └────┬─────┘                          └──────────┘              │   │
│  │        │                                (no flush)                 │   │
│  │        ▼                                                          │   │
│  │   ┌──────────┐                                                    │   │
│  │   │ Parquet  │ → S3                                               │   │
│  │   └──────────┘                                                    │   │
│  │                                                                    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Problem: Slow WAL Replay

With traditional WAL-based recovery:

| Flush Interval | WAL Size | Replay Time | Downtime |
|----------------|----------|-------------|----------|
| 5 minutes | ~100MB | 15-30s | 15-30s |
| 10 minutes | ~200MB | 30-60s | 30-60s |
| 30 minutes | ~500MB | 60-120s | 60-120s |

**Warm standby eliminates replay entirely** - the standby already has the data in memory.

## Key Design Decisions

### 1. Replication Granularity: Per-Partition

Each partition can have its own leader and standbys:

```
Partition: mydb/default/cpu/date=2024-01-15
  Leader: data-1
  Standbys: [data-2, data-3]

Partition: mydb/default/memory/date=2024-01-15
  Leader: data-2
  Standbys: [data-1, data-3]
```

Benefits:
- Load distribution across nodes
- Independent failover per partition
- Matches Kafka's partition model

### 2. Replication Mode: Async by Default

| Mode | Behavior | Latency | Durability |
|------|----------|---------|------------|
| **Async** | ACK after local write | +0ms | Best effort |
| **Sync** | ACK after 1 standby ACK | +1-5ms | Strong |
| **SyncAll** | ACK after all standbys ACK | +5-10ms | Strongest |

Default is async for performance. Use sync for critical data.

### 3. Standby Does Not Flush

| Component | Leader | Standby |
|-----------|--------|---------|
| Write to WAL | Yes | No |
| Write to MemTable | Yes | Yes |
| Flush to Parquet | Yes | No |
| Register files with Meta | Yes | No |

When promoted, standby starts flushing normally.

## Component Architecture

### Meta Service

Stores partition replica assignments:

```rust
pub struct PartitionReplica {
    pub partition_key: String,      // "catalog/schema/table/date=YYYY-MM-DD"
    pub leader_node_id: String,     // Current leader
    pub standby_node_ids: Vec<String>, // Standbys for failover
    pub replication_factor: u8,     // 1=no standby, 2=1 standby, etc.
    pub last_replicated_seq: u64,   // For monitoring lag
    pub replication_lag_ms: u64,
    pub state: ReplicaState,        // InSync, CatchingUp, Offline
}
```

**Key files:**
- `src/meta/types.rs` - PartitionReplica, ReplicaState
- `src/meta/raft/state_machine.rs` - Replica CRUD, auto-failover

### Router Service

Maintains local cache of replica assignments for instant routing decisions:

```rust
pub struct ShardCache {
    partition_replicas: DashMap<String, PartitionReplicaInfo>,
    data_nodes: RwLock<HashMap<String, DataNodeInfo>>,
    // ...
}
```

**Zero-downtime routing logic:**

```rust
pub fn select_node_for_partition(&self, key: &PartitionKey) -> Option<DataNodeInfo> {
    // 1. Check partition replica info
    if let Some(replica) = self.partition_replicas.get(&partition_key) {
        // 2. Check leader health
        if let Some(leader) = self.get_data_node(&replica.leader_node_id) {
            if leader.state == NodeState::Active {
                return Some(leader);  // Leader healthy
            }
        }

        // 3. Leader unhealthy → IMMEDIATELY use standby!
        for standby_id in &replica.standby_node_ids {
            if let Some(standby) = self.get_data_node(standby_id) {
                if standby.state == NodeState::Active {
                    return Some(standby);  // Instant failover!
                }
            }
        }
    }

    // 4. Fallback to hash-based routing
    self.select_node_by_hash(key)
}
```

**Key files:**
- `src/router/routing/shard_cache.rs` - ShardCache with instant failover
- `src/router/routing/cache_invalidation.rs` - Event handling

### Data Node - Leader

Streams writes to standbys:

```rust
pub struct ReplicationStream {
    standby_node_id: String,
    standby_addr: String,
    pending: RwLock<VecDeque<ReplicationEntry>>,
    last_sent_seq: AtomicU64,
    last_acked_seq: AtomicU64,
}

impl ReplicationStream {
    pub fn replicate(&self, entry: ReplicationEntry) {
        self.pending.write().push_back(entry);
        // Background task flushes every batch_interval_ms
    }
}
```

**Key files:**
- `src/data/replication/stream.rs` - ReplicationStream, ReplicationManager
- `src/data/replication/types.rs` - ReplicationEntry, ReplicateRequest

### Data Node - Standby

Receives and applies replicated data:

```rust
pub struct StandbyReceiver {
    cache: Arc<DataCache>,
    partitions: RwLock<HashMap<String, Arc<PartitionReceiveState>>>,
}

impl StandbyReceiver {
    pub async fn receive_batch(&self, ...) -> Result<ReplicateResponse, String> {
        // Apply to memtable (no WAL, no flush)
        self.cache.add_batch_standby(catalog, schema, table, date, batch).await
    }
}
```

**Key files:**
- `src/data/replication/receiver.rs` - StandbyReceiver
- `src/data/server/flight.rs` - ReplicationTicket, handle_replication_write()
- `src/storage/cache.rs` - add_batch_standby()

## Failover Flow

### Timeline: Traditional WAL Replay

```
T=0ms:     Leader crashes
T=0-10s:   Lease expiration (heartbeat timeout)
T=10s:     Meta detects failure, promotes standby
T=10s:     New leader starts WAL replay
T=10-70s:  WAL replay in progress (30-60s for 200MB)
T=70s:     New leader ready for writes
───────────────────────────────────────────────────
Total downtime: ~70 seconds
```

### Timeline: Warm Standby

```
T=0ms:     Leader crashes
T=0-100ms: Router write fails (connection timeout)
T=100ms:   Router marks leader as Suspect
T=100ms:   Router's select_node_for_partition() returns STANDBY
T=150ms:   Write succeeds on standby
───────────────────────────────────────────────────
Total downtime: ~150ms (near zero!)

Background (async, doesn't affect writes):
T=10s:     Meta detects via lease expiration
T=10.1s:   Meta officially promotes standby
T=10.2s:   Standby starts flushing to Parquet
```

### Router-Driven vs Meta-Driven Failover

| Aspect | Router-Driven | Meta-Driven |
|--------|---------------|-------------|
| Detection | Write failure (~100ms) | Lease expiry (~10s) |
| Decision | Local (ShardCache) | Raft consensus |
| Failover time | ~150ms | ~10s |
| Parquet flush | After Meta confirms | After Meta confirms |

**Key insight**: Router provides instant failover for writes. Meta provides official promotion for Parquet flush.

## Data Flow

### Write Path (Leader)

```
1. Client → Router
2. Router → Leader Data Node (via Flight DoPut)
3. Leader: Write to WAL + MemTable
4. Leader: Stream to Standby (async)
5. Leader: ACK to Router
6. Background: MemTable → Parquet → S3
```

### Replication Path

```
1. Leader: Batch entries every 100ms
2. Leader → Standby (via Flight DoPut with ReplicationTicket)
3. Standby: add_batch_standby() (MemTable only, no flush)
4. Standby → Leader: ACK with last_applied_seq
```

### Failover Path

```
1. Leader becomes unreachable
2. Router: Write to leader fails
3. Router: mark_node_suspect(leader_id)
4. Router: select_node_for_partition() → Standby
5. Router → Standby: Direct writes
6. Meta (async): Detects via lease expiry
7. Meta: PromoteStandby → Publishes ReplicaPromoted event
8. Standby: Starts flushing to Parquet
```

## Memory Considerations

| Component | Leader | Standby |
|-----------|--------|---------|
| MemTable | Full data | Full data (replica) |
| WAL buffer | Yes | No |
| Query cache | Yes | Yes (for reads) |

**Total cluster memory**: ~2x single node (for RF=2)

Optimization options:
- Standby can use compressed memtable
- Standby can spill to local disk if memory pressure

## Event Types

| Event | Publisher | Subscribers | Purpose |
|-------|-----------|-------------|---------|
| ReplicaAssigned | Meta | Router | Initial assignment |
| ReplicaPromoted | Meta | Router, Data | Leader change |
| ReplicaStateChanged | Meta | Router | Sync status |
| NodeLeft | Meta | Router | Cleanup |

## Configuration

```toml
[data_node.replication]
enabled = true
mode = "async"           # "async", "sync", "sync_all"
batch_interval_ms = 100  # Batch replication entries
max_batch_size = 1000    # Max entries per batch
ack_timeout_ms = 5000    # Standby ACK timeout
max_lag_alert_ms = 10000 # Alert threshold

[meta.failover]
lease_ttl_secs = 10      # Node lease TTL
auto_promote = true      # Auto-promote standby on failure
```

## Comparison with Other Systems

| System | Replication Model | Failover Time | Complexity |
|--------|-------------------|---------------|------------|
| Kafka | ISR (sync replicas) | 1-5s | Medium |
| ClickHouse | ReplicatedMergeTree | 10-30s | High |
| TiKV | Raft per Region | 10-30s | Very High |
| InfluxDB IOx | Shared storage | 30-60s (WAL) | Low |
| **MynaDB** | Warm standby | **<1s** | Medium |

## File Reference

| File | Purpose |
|------|---------|
| `src/meta/types.rs` | PartitionReplica, ReplicaState, EventType |
| `src/meta/raft/state_machine.rs` | Replica CRUD, auto-failover |
| `src/router/routing/shard_cache.rs` | Instant failover routing |
| `src/router/routing/cache_invalidation.rs` | Event handling |
| `src/data/replication/mod.rs` | Replication module |
| `src/data/replication/types.rs` | ReplicationEntry, Config |
| `src/data/replication/stream.rs` | Leader→Standby stream |
| `src/data/replication/receiver.rs` | Standby receiver |
| `src/data/server/flight.rs` | Flight service with replication |
| `src/data/config.rs` | ReplicationConfig |
| `src/storage/cache.rs` | add_batch_standby() |

## Future Enhancements

1. **Read from standby**: Route reads to standbys for load distribution
2. **Automatic standby assignment**: Meta auto-assigns standbys based on load
3. **Cross-datacenter replication**: Async replication to remote DCs
4. **Replication metrics**: Prometheus metrics for lag monitoring
