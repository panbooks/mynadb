---
name: distributed-coordination
description: This skill should be used when the user is working with MynaDB's cluster mode — node coordination, Raft consensus, distributed locking, leader election, cache invalidation, replication, table ownership, write routing, or scale-in/scale-out operations.
version: 1.0.0
---

# Distributed Coordination in MynaDB

## Cluster Architecture

```
           ┌─────────────┐
           │  Meta Nodes  │  (Raft consensus, metadata store)
           │  (OpenRaft)  │
           └──────┬───────┘
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   ┌─────────┐ ┌───────┐ ┌───────┐
   │ Router  │ │ Data  │ │ Data  │
   │  Nodes  │ │ Node  │ │ Node  │
   │ (HTTP,  │ │(Store,│ │(Store,│
   │ Flight) │ │Query) │ │Query) │
   └─────────┘ └───────┘ └───────┘
```

## Node Types

| Type | File | Role |
|------|------|------|
| Meta | `src/meta/` | Raft-based metadata, consensus |
| Router | `src/node/router.rs` | Client endpoints, write routing |
| Data | `src/node/data.rs` | Storage, query execution |
| Standalone | `src/node/standalone.rs` | All-in-one single process |
| Admin | `src/node/admin.rs` | Admin console |

## Raft Consensus (OpenRaft)

**Files:** `src/meta/raft/`

- **Log store:** Sled embedded database (`log_store.rs`)
- **State machine:** Sled-backed, applies replicated `MetaCommand`s
- **Network:** gRPC peer-to-peer with channel pooling (`network.rs`)
- **Events:** Broadcast channel (capacity 1000) for pub/sub

```rust
// TypeConfig for OpenRaft
type NodeId = u64;
type D = MetaCommand;   // Replicated commands
type R = MetaResponse;  // Command responses
```

## Distributed Locking

**Files:** `src/meta/meta_node/locks.rs`, `src/meta/raft/state_machine/lock.rs`

```rust
// Acquire lock with TTL and fencing token
try_lock(key: &str, holder: &str, ttl_ms: u64) -> (bool, fencing_token)
unlock(key: &str, holder: &str, fencing_token: u64)
renew_lock(key: &str, holder: &str, ttl_ms: u64) -> expires_at
```

**Fencing tokens:** Monotonically increasing, prevent split-brain writes. Used by compactor for partition-level locking.

## Table Ownership & Routing

**Files:** `src/meta/types/table.rs`, `src/router/write_router.rs`

Tables are owned by data nodes (not partitions — 100K entries vs 60M):

```rust
pub struct TableMeta {
    catalog, schema, table,
    owners: Vec<TableOwnerEntry>,  // [primary, secondary, ...]
    version,                        // Cache invalidation
}

pub struct TableOwnerEntry {
    node_id, ttl_ms,
    acquired_at, expires_at,
}
```

**Routing flow:**
1. Router receives write → extracts table key
2. Looks up primary owner in `ShardCache` (DashMap)
3. Forwards data via Arrow Flight to data node
4. On failure → adds secondary owner (reroute)

**Distribution config** (`src/meta/types/distribution.rs`):
- 0% = single node (default)
- 50% = half of primary nodes
- 100% = all primary nodes
- Hierarchy: table > schema > catalog > default

## Leader Election (Application-Level)

**File:** `src/meta/meta_node/elections.rs`

Separate from Raft leader — per-purpose elections:
```rust
campaign(election: &str, node_id: &str, ttl_ms: u64) -> (bool, term)
resign(election: &str, node_id: &str, term: u64)
```

Used for: compaction leader, retention leader. Terms act as fencing tokens.

## Cache Invalidation

**File:** `src/router/cache_invalidation.rs`

Event-driven via Meta pub/sub:
```rust
pub enum EventType {
    NodeJoined, NodeLeft, NodeStateChanged,
    LeaderChanged, SchemaChanged,
    TableOwnerAdded, TableOwnerRemoved, TableReassigned,
    TableOwnerExpired, TablesCacheRefresh,
    FlushForRecovery,
    DistributionConfigChanged,
}
```

Routers subscribe to events, update `ShardCache` on changes. Catch-up via `get_events_from(sequence)` for lagging subscribers.

## Cold Standby Architecture

**Files:** `src/meta/types/node.rs`, `src/meta/raft/state_machine/standby.rs`

```rust
pub enum DataNodeMode {
    Primary,   // Serves traffic
    Standby,   // Cold backup, replication only
}
```

- **No promotion:** Standbys never become primaries (K8s replaces failed pods)
- **Pairing:** Each primary backed by a specific standby
- **Ratio:** Configurable (e.g., 2:1 = 1 standby backs 2 primaries)
- **Zone-aware:** Uses Kubernetes topology labels for failure domain isolation

## Replication

**File:** `src/data/replication/`

```
Primary → async batch → Standby (cache only, no flush)
Primary crashes → Meta detects → Standby flushes to S3
K8s replaces Primary → loads from S3
```

Config: `batch_interval_ms = 100`, `max_batch_size = 1000`

## Scale-In (Retirement)

**File:** `src/meta/meta_node/scale_in.rs`

```
Active → Retiring → Drained → Pod terminated
```

1. Mark node as Retiring (no new writes routed)
2. Reassign tables to other nodes (with locks)
3. Version bump invalidates router caches
4. Mark as Drained when complete

## Node States

```rust
pub enum NodeState {
    Starting,   // Registering with Meta
    Active,     // Serving traffic
    Retiring,   // Scale-in: draining
    Drained,    // Safe to stop
    Inactive,   // Lost heartbeat
}
```

## Key Patterns

- **Table-level (not partition-level) routing:** Keeps metadata small (~100K entries)
- **TTL-based ownership:** Auto-cleanup on node crash, no manual intervention
- **Fencing tokens everywhere:** Locks, elections, table versions prevent stale operations
- **Event-driven cache:** No polling, immediate invalidation via pub/sub
- **Optimistic concurrency:** Version numbers on table metadata for conflict detection
