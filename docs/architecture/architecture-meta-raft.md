# MynaDB Meta Service Raft Implementation

> **Status: INTEGRATED** - OpenRaft 0.9 fully integrated into MetaNode for multi-node HA.

This document describes the internal implementation of the Raft consensus layer for the Meta service, including storage formats, trait implementations, and component interactions.

## Overview

The Meta service uses [OpenRaft 0.9](https://github.com/datafuselabs/openraft) for consensus, providing:
- Leader election and automatic failover
- Log replication across Meta nodes
- Linearizable writes through Raft consensus
- Snapshot-based state transfer for new/recovering nodes

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Meta Service Raft Stack                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Application Layer                             │   │
│  │  MetaCommand/MetaResponse (RegisterNode, AcquireLock, etc.)     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                   │                                     │
│                                   ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    OpenRaft Core                                 │   │
│  │  Raft<TypeConfig> - Leader election, log replication            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                   │                                     │
│         ┌─────────────────────────┼─────────────────────────┐          │
│         │                         │                         │          │
│         ▼                         ▼                         ▼          │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐       │
│  │ LogStorage  │         │StateMachine │         │  Network    │       │
│  │ SledLogStore│         │MetaState    │         │MetaNetwork  │       │
│  │             │         │Machine      │         │Factory      │       │
│  └─────────────┘         └─────────────┘         └─────────────┘       │
│         │                         │                         │          │
│         ▼                         ▼                         ▼          │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐       │
│  │   sled DB   │         │   sled DB   │         │    gRPC     │       │
│  │ /raft/log/* │         │  /nodes/*   │         │RaftInternal │       │
│  │ /raft/vote  │         │  /files/*   │         │  Service    │       │
│  └─────────────┘         │  /locks/*   │         └─────────────┘       │
│                          └─────────────┘                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Type Configuration

### TypeConfig (`src/meta/raft/mod.rs`)

Defines all concrete types for OpenRaft:

```rust
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = MetaCommand;           // Command type (replicated)
    type R = MetaResponse;          // Response type (from apply)
    type NodeId = u64;              // Node identifier
    type Node = BasicNode;          // Node metadata (contains addr)
    type Entry = Entry<Self>;       // Log entry type
    type SnapshotData = Cursor<Vec<u8>>;  // Snapshot data
    type AsyncRuntime = TokioRuntime;     // Async runtime
    type Responder = OneshotResponder<Self>; // Client response channel
}
```

### Type Aliases

```rust
pub type NodeId = u64;
pub type MetaRaft = Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, BasicNode>;
pub type StoredMembership = openraft::StoredMembership<NodeId, BasicNode>;
```

---

## Log Storage (`src/meta/raft/log_store.rs`)

### Purpose

Persistent storage for Raft log entries, vote state, and committed log ID using sled embedded database.

### Storage Keys

| Key | Type | Description |
|-----|------|-------------|
| `/raft/vote` | `Vote` | Current voted-for candidate |
| `/raft/committed` | `LogId` | Last committed log ID |
| `/raft/last_purged` | `LogId` | Last purged log ID (for compaction) |
| `/raft/log/{index:020}` | `Entry` | Log entries (zero-padded index) |

### Structure

```rust
pub struct SledLogStore {
    db: Db,                           // Sled database instance
    vote: RwLock<Option<Vote>>,       // Cached vote state
    committed: RwLock<Option<LogId>>, // Cached committed log ID
    last_purged: RwLock<Option<LogId>>, // Cached last purged log ID
}
```

### RaftLogStorage Trait Implementation

| Method | Description |
|--------|-------------|
| `get_log_state()` | Returns `LogState { last_purged_log_id, last_log_id }` |
| `get_log_reader()` | Returns clone of self for concurrent reads |
| `save_vote(vote)` | Persist vote to `/raft/vote` and flush |
| `read_vote()` | Read cached vote |
| `save_committed(log_id)` | Persist committed log ID |
| `append(entries, callback)` | Append entries, call callback when flushed |
| `truncate(log_id)` | Remove entries from log_id onwards (conflict resolution) |
| `purge(log_id)` | Remove entries up to log_id (compaction) |

### RaftLogReader Trait Implementation

| Method | Description |
|--------|-------------|
| `try_get_log_entries(range)` | Read entries in range from sled |

### Log Entry Format

Entries are stored with zero-padded indices for proper ordering:

```
/raft/log/00000000000000000001 → Entry { log_id: {term: 1, index: 1}, payload: ... }
/raft/log/00000000000000000002 → Entry { log_id: {term: 1, index: 2}, payload: ... }
...
```

---

## State Machine (`src/meta/raft/state_machine.rs`)

### Purpose

Applies committed log entries to the metadata store and manages snapshots.

### Storage Keys

| Prefix | Type | Description |
|--------|------|-------------|
| `/raft/sm/last_applied` | `LogId` | Last applied log ID |
| `/raft/sm/membership` | `StoredMembership` | Current membership config |
| `/raft/sm/snapshot_meta` | `SnapshotMeta` | Snapshot metadata |
| `/nodes/{node_id}` | `NodeInfo` | Node registry |
| `/files/{path}` | `FileInfo` | File registry |
| `/locks/{key}` | `LockInfo` | Distributed locks |
| `/leaders/{election}` | `LeaderInfo` | Leader election state |
| `/events/{seq:020}` | `Event` | Event log |
| `/leases/{lease_id}` | `LeaseInfo` | Node leases |
| `/meta/sequence` | `u64` | Event sequence counter |
| `/meta/fencing` | `u64` | Fencing token counter |

### Structure

```rust
pub struct MetaStateMachine {
    db: Db,                              // Sled database
    event_sequence: AtomicU64,           // Event sequence counter
    fencing_counter: AtomicU64,          // Fencing token counter
    lease_counter: AtomicU64,            // Lease ID counter
    last_applied: RwLock<Option<LogId>>, // Last applied log ID
    last_membership: RwLock<StoredMembership>, // Current membership
}
```

### RaftStateMachine Trait Implementation

| Method | Description |
|--------|-------------|
| `applied_state()` | Returns `(last_applied, membership)` |
| `apply(entries)` | Apply entries, return responses |
| `get_snapshot_builder()` | Returns `MetaSnapshotBuilder` |
| `begin_receiving_snapshot()` | Prepare to receive snapshot data |
| `install_snapshot(meta, data)` | Install received snapshot |
| `get_current_snapshot()` | Return current snapshot if exists |

### Command Application Flow

```
Entry received
     │
     ▼
┌────────────────┐
│ Entry Payload? │
└────────────────┘
     │
     ├─► Blank: Return Success
     │
     ├─► Membership: Update stored membership
     │
     └─► Normal(MetaCommand):
              │
              ▼
         ┌──────────────────────┐
         │ apply_command(cmd)   │
         │   RegisterNode       │
         │   AcquireLock        │
         │   RegisterFiles      │
         │   Campaign           │
         │   ...                │
         └──────────────────────┘
              │
              ▼
         Update last_applied
              │
              ▼
         Flush to disk
```

### Snapshot Format

```rust
pub struct MetaSnapshot {
    pub last_applied: Option<LogId>,  // Log ID at snapshot time
    pub membership: StoredMembership, // Membership config
    pub data: Vec<u8>,                // Serialized state (all KV pairs)
    pub event_sequence: u64,          // Counter value
    pub fencing_counter: u64,         // Counter value
}
```

Snapshot data contains all KV pairs except Raft log entries:

```rust
// Serialized as: Vec<(Vec<u8>, Vec<u8>)>
// Excludes: /raft/log/*, /raft/vote, /raft/committed, /raft/last_purged
```

---

## Network Layer (`src/meta/raft/network.rs`)

### Purpose

gRPC-based peer-to-peer communication for Raft RPCs.

### Structure

```rust
pub struct MetaNetworkFactory {
    channels: Arc<RwLock<HashMap<NodeId, Channel>>>,  // Cached gRPC channels
    peers: Arc<RwLock<HashMap<NodeId, String>>>,      // Peer addresses
}

pub struct MetaNetworkConnection {
    target: NodeId,              // Target node ID
    target_addr: String,         // Target address
    factory: MetaNetworkFactory, // For channel reuse
}
```

### RaftNetworkFactory Trait

```rust
impl RaftNetworkFactory<TypeConfig> for MetaNetworkFactory {
    type Network = MetaNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        MetaNetworkConnection {
            target,
            target_addr: node.addr.clone(),
            factory: self.clone(),
        }
    }
}
```

### RaftNetwork Trait

| Method | Description |
|--------|-------------|
| `append_entries(req)` | Send AppendEntries RPC |
| `install_snapshot(req)` | Send InstallSnapshot RPC |
| `vote(req)` | Send RequestVote RPC |

### gRPC Protocol

Uses `RaftInternal` service from `proto/meta.proto`:

```protobuf
service RaftInternal {
    rpc AppendEntries(RaftMessage) returns (RaftMessage);
    rpc InstallSnapshot(RaftMessage) returns (RaftMessage);
    rpc Vote(RaftMessage) returns (RaftMessage);
}

message RaftMessage {
    bytes data = 1;  // bincode-serialized OpenRaft request/response
}
```

### Message Serialization

```
OpenRaft Request
      │
      ▼
bincode::serialize()
      │
      ▼
RaftMessage { data: bytes }
      │
      ▼
gRPC transport
      │
      ▼
RaftMessage { data: bytes }
      │
      ▼
bincode::deserialize()
      │
      ▼
OpenRaft Response
```

---

## Data Flow

### Write Path (Leader)

```
Client Request (gRPC)
        │
        ▼
MetaGrpcService
        │
        ▼
raft.client_write(MetaCommand)
        │
        ▼
┌───────────────────────────────────────┐
│           OpenRaft Core               │
│  1. Append to local log               │
│  2. Replicate to followers            │
│  3. Wait for quorum                   │
│  4. Commit entry                      │
│  5. Apply to state machine            │
└───────────────────────────────────────┘
        │
        ▼
MetaStateMachine.apply()
        │
        ▼
MetaResponse
```

### Write Path (Follower)

```
AppendEntries RPC (from Leader)
        │
        ▼
RaftInternalService
        │
        ▼
raft.append_entries(req)
        │
        ▼
┌───────────────────────────────────────┐
│           OpenRaft Core               │
│  1. Validate leader term              │
│  2. Append entries to log             │
│  3. Apply committed entries           │
└───────────────────────────────────────┘
        │
        ▼
AppendEntriesResponse
```

### Leader Election

```
Leader heartbeat timeout
        │
        ▼
┌───────────────────────────────────────┐
│         Candidate State               │
│  1. Increment term                    │
│  2. Vote for self                     │
│  3. Send RequestVote to all peers     │
└───────────────────────────────────────┘
        │
        ▼
Receive majority votes?
        │
    ┌───┴───┐
    │       │
   Yes      No
    │       │
    ▼       ▼
 Leader   Follower
```

### Snapshot Transfer

```
Leader detects follower far behind
        │
        ▼
MetaSnapshotBuilder.build_snapshot()
        │
        ▼
Serialize all state to MetaSnapshot
        │
        ▼
full_snapshot() RPC (chunked transfer)
        │
        ▼
Follower: begin_receiving_snapshot()
        │
        ▼
Follower: install_snapshot()
        │
        ▼
Restore state from snapshot
```

---

## Configuration

### Cargo.toml

```toml
[dependencies]
openraft = { version = "0.9", features = ["serde", "storage-v2"] }
sled = "0.34"
```

### Meta Node Config

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
```

---

## Error Handling

### Storage Errors

```rust
fn storage_error(e: impl Display, context: &str) -> StorageError<NodeId> {
    let msg = format!("{}: {}", context, e);
    StorageError::IO {
        source: StorageIOError::write(AnyError::error(msg)),
    }
}
```

### Network Errors

```rust
// Connection failures return Unreachable
Unreachable::from(AnyError::error(e.to_string()))

// OpenRaft will retry with backoff
fn backoff(&self) -> Backoff {
    Backoff::new(std::iter::repeat(Duration::from_millis(500)))
}
```

---

## Testing

### Unit Tests

```bash
# Test log store
cargo test --lib log_store

# Test state machine
cargo test --lib state_machine
```

### Log Store Test

```rust
#[tokio::test]
async fn test_log_store_vote() {
    let dir = tempdir().unwrap();
    let mut store = SledLogStore::open(dir.path()).unwrap();

    // Initially no vote
    assert!(store.read_vote().await.unwrap().is_none());

    // Save a vote
    let vote = Vote::new(1, 2);
    store.save_vote(&vote).await.unwrap();

    // Read it back
    let read_vote = store.read_vote().await.unwrap();
    assert_eq!(read_vote, Some(vote));
}
```

---

## Integration Status

### Completed Tasks

1. ✅ **Create Raft Instance**: `Raft<TypeConfig>` instantiated in `MetaNode::new()`
2. ✅ **Wire RaftInternalService**: gRPC handlers forward RPCs to Raft instance
3. ✅ **Client Write Routing**: All writes route through `raft.client_write()`
4. ✅ **Leadership Queries**: `is_leader()` and `current_leader()` use Raft metrics
5. ✅ **Cluster Membership**: `initialize_cluster()`, `add_learner()`, `change_membership()` methods

### Future Enhancements

- [ ] Read replicas (followers serve reads with linearizable guarantee)
- [ ] Learner nodes (non-voting replicas for read scaling)
- [ ] Joint consensus for safe membership changes
- [ ] Raft metrics export (Prometheus)
- [ ] Automatic snapshot on log size threshold
- [ ] Admin API for cluster management operations

---

## Related Documentation

- [Meta Service Architecture](architecture-meta.md) - High-level design
- [Cluster v2 Architecture](architecture-cluster-v2.md) - Full cluster architecture
- [Cluster v2 Guide](guide-cluster-v2.md) - Operations guide
