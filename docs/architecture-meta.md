# MynaDB Meta Service Architecture

> Future enhancement: Replace external etcd/Redis with built-in Meta Service

## Problem Statement

Current MynaDB cluster mode requires deploying and managing external etcd or Redis for coordination. This adds operational complexity.

**Proposed Solution**: Add a `meta` role that provides built-in Raft-based coordination, eliminating the need for external dependencies.

---

## Industry State-of-the-Art

### Embedded Meta Service (The Trend)

| System | Meta Service | Consensus | Key Insight |
|--------|-------------|-----------|-------------|
| **Kafka KRaft** | Controller nodes | Raft | Replaced ZooKeeper in 2022 |
| **ClickHouse Keeper** | Keeper nodes | NuRaft | ZK-compatible, embeddable |
| **InfluxDB Enterprise** | Meta nodes | Raft | 3 nodes for metadata |
| **TiDB (PD)** | Placement Driver | embedded etcd | TSO, scheduling |
| **Apache Doris** | FE nodes | BDB JE | Schema, partitions |
| **CockroachDB** | None | Multi-Raft | Every range has Raft group |
| **MongoDB** | Config Servers | Raft | 3-node replica set |

### Key Takeaway

**Kafka's move from ZooKeeper to KRaft** (2022-2024) is the clearest industry signal:
- 10+ years of ZooKeeper dependency eliminated
- Simpler deployment (single binary)
- Better performance (no extra network hop)
- Easier operations (one system to monitor)

---

## Proposed Architecture

### Node Roles (4 Types)

| Role | Responsibilities | Typical Count |
|------|------------------|---------------|
| **Meta** | Raft consensus, metadata, coordination | 3 or 5 (fixed) |
| **Compute** | Writes + Reads (HTTP/Flight/MySQL) | Elastic (N) |
| **Compactor** | Background compaction + retention | 1-3 |
| **Hybrid** | Compute + Compactor (small deployments) | Any |

### Deployment Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        MynaDB Cluster                                │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Meta Service (Raft)                       │    │
│  │                                                              │    │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │    │
│  │   │  Meta Node 1 │  │  Meta Node 2 │  │  Meta Node 3 │      │    │
│  │   │  (LEADER)    │◄─┤  (Follower)  │◄─┤  (Follower)  │      │    │
│  │   │  Port: 7100  │  │  Port: 7100  │  │  Port: 7100  │      │    │
│  │   └──────────────┘  └──────────────┘  └──────────────┘      │    │
│  │         │                  │                  │              │    │
│  │         └──────────────────┼──────────────────┘              │    │
│  │                            │ Raft replication                │    │
│  └────────────────────────────┼─────────────────────────────────┘    │
│                               │                                      │
│              ┌────────────────┼────────────────┐                    │
│              │                │                │                    │
│              ▼                ▼                ▼                    │
│      ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│      │ Compute Node │  │ Compute Node │  │ Compute Node │ ← Elastic│
│      │  HTTP:7000   │  │  HTTP:7000   │  │  HTTP:7000   │          │
│      │  Flight:7001 │  │  Flight:7001 │  │  Flight:7001 │          │
│      │  MySQL:7002  │  │  MySQL:7002  │  │  MySQL:7002  │          │
│      └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│             │                 │                 │                   │
│             └─────────────────┼─────────────────┘                   │
│                               │                                      │
│                               ▼                                      │
│              ┌────────────────────────────────┐                     │
│              │      Shared Object Storage      │                     │
│              │      (S3 / MinIO / OSS)        │                     │
│              └────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Meta Service Responsibilities

### 1. Cluster State (Raft replicated)
```
/mynadb/cluster/
  ├── nodes/{node_id}          # Node registration, health, role
  ├── leader/compaction        # Who runs compaction
  └── config/                  # Cluster-wide settings
```

### 2. Schema Metadata (Raft replicated)
```
/mynadb/schemas/
  ├── {catalog}/
  │   ├── {schema}/
  │   │   ├── {table}/
  │   │   │   ├── schema.json      # Column definitions
  │   │   │   ├── retention.json   # Retention policy
  │   │   │   └── partitions/      # Partition manifest
```

### 3. Distributed Locks
```
/mynadb/locks/
  ├── {catalog}/{schema}/{table}/{date}  # Write locks
  └── compaction/{catalog}/{schema}/{table}
```

### 4. Cache Invalidation Events
```
/mynadb/events/
  └── cache_invalidation  # Append-only event log
```

---

## Implementation Recommendation

### Technology Stack

- **Raft**: `openraft` crate (pure Rust, used by Databend)
- **Storage**: `sled` embedded key-value store
- **RPC**: gRPC with tonic

### Module Structure

```
src/meta/
├── mod.rs                  # Meta service module
├── config.rs               # Meta node configuration
├── raft/
│   ├── state_machine.rs    # Raft state machine (sled)
│   ├── network.rs          # Raft RPC
│   └── log_store.rs        # Raft log storage
├── service/
│   ├── lock_service.rs     # Distributed lock API
│   ├── schema_service.rs   # Schema metadata API
│   ├── node_service.rs     # Node registration API
│   └── event_service.rs    # Pub/sub events
├── client/
│   └── meta_client.rs      # Client for compute nodes
└── api/
    └── grpc.rs             # gRPC service definitions
```

### Configuration

```toml
# Meta node configuration
[meta]
enabled = true
node_id = 1
bind_addr = "0.0.0.0:7100"
data_dir = "/data/meta"
peers = ["meta1:7100", "meta2:7100", "meta3:7100"]

[meta.raft]
heartbeat_interval_ms = 150
election_timeout_ms = 1000
snapshot_threshold = 10000

# Compute node configuration
[cluster]
enabled = true
coordinator_type = "meta"  # Use built-in meta service
meta_endpoints = ["meta1:7100", "meta2:7100", "meta3:7100"]
```

---

## Comparison: etcd vs Meta Service

| Aspect | With etcd | With Meta Service |
|--------|-----------|-------------------|
| **Deployment** | Deploy etcd separately | Single binary `--role meta` |
| **Dependencies** | etcd binaries, certs | None |
| **Operations** | Monitor 2 systems | Monitor 1 system |
| **Network** | Extra hop for metadata | Local state machine |
| **Upgrades** | Coordinate MynaDB + etcd | Single upgrade |
| **Learning Curve** | Learn etcd operations | MynaDB only |

---

## Example Deployment

```bash
# Start 3 meta nodes (fixed cluster)
mynadb --role meta --meta-id 1 --peers meta1:7100,meta2:7100,meta3:7100
mynadb --role meta --meta-id 2 --peers meta1:7100,meta2:7100,meta3:7100
mynadb --role meta --meta-id 3 --peers meta1:7100,meta2:7100,meta3:7100

# Start N compute nodes (elastic)
mynadb --role compute --meta-endpoints meta1:7100,meta2:7100,meta3:7100
mynadb --role compute --meta-endpoints meta1:7100,meta2:7100,meta3:7100
```

---

## Implementation Phases

### Phase 1: Core Meta Service
1. Add `openraft` + `sled` dependencies
2. Implement Raft state machine
3. Add gRPC API for meta operations
4. Add `coordinator_type = "meta"` support

### Phase 2: Meta Role
1. New role: `role = "meta"`
2. Meta nodes only run Raft consensus
3. Compute/Compactor nodes connect to meta

### Phase 3: Migration
1. Keep etcd support for existing users
2. Document migration path
3. Eventually deprecate etcd coordinator

---

## Status

**Not implemented yet** - This document describes a future enhancement.

Current cluster mode still requires external etcd or Redis:

```toml
[cluster]
enabled = true
[cluster.coordinator]
coordinator_type = "etcd"  # or "redis"
endpoints = ["etcd1:2379", "etcd2:2379"]
```
