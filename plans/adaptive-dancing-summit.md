# Plan: MynaDB Cluster Architecture Analysis & Improvements

## Overview

Deep analysis of two critical questions for making MynaDB cluster SOTA:
1. Should Meta node store partition/catalog/schema/table info? Is it safe?
2. Data node crash → redirect issues and better approaches for reliability

---

## Question 1: Meta Node Metadata Storage

### Current Implementation

**What Meta stores (via Raft consensus):**

| Data Type | Storage Key | Contents |
|-----------|-------------|----------|
| Nodes | `/nodes/{node_id}` | node_id, role, state, addresses, heartbeat |
| Files | `/files/{catalog}/{schema}/{table}/{partition}/{filename}` | path, size, row_count, time_range, level, created_by |
| Locks | `/locks/{key}` | holder, expires_at, fencing_token |
| Leaders | `/leaders/{election}` | leader_id, term, TTL |
| Events | `/events/{seq}` | sequence, type, payload, source |
| Leases | `/leases/{lease_id}` | node_id, expires_at |

**Key Files:**
- `src/meta/raft/state_machine.rs` - Raft state machine with all metadata operations
- `src/meta/types.rs` - FileInfo, NodeInfo structs
- `src/meta/service/meta_node.rs` - Meta service coordination

### Analysis: Is It Safe?

**✅ SAFE aspects:**

1. **Raft Consensus**: All metadata changes go through Raft
   - Linearizable consistency
   - Committed writes survive leader crashes
   - 3-node Meta cluster tolerates 1 failure

2. **Fencing Tokens**: Prevent zombie writes
   - Monotonic counter in Meta
   - Data nodes validate tokens on write

3. **Lease-based Failure Detection**:
   - 10s TTL, 2s heartbeat
   - Auto-deregistration on crash

**⚠️ UNSAFE/PROBLEMATIC aspects:**

1. **File Registry Stale State**:
   ```
   Data node writes Parquet → CRASH → Never registers with Meta
   Result: Orphaned files in S3, invisible to queries
   ```
   - Files exist but not discoverable
   - No automatic reconciliation

2. **No Schema Enforcement at Meta**:
   - Meta accepts any FileInfo without validation
   - Schema drift possible across partitions
   - No catalog/schema/table creation tracking

3. **Partition Ownership is Implicit**:
   - Inferred from `created_by` in FileInfo
   - No explicit partition→node assignment
   - Router uses hash-based routing, not Meta's knowledge

4. **Event Sequence Gaps**:
   - If subscriber misses events, must poll from last_seq
   - Large gaps can cause stale cache

### Recommendation for Question 1

**Current design is ADEQUATE but needs improvements:**

| Issue | Recommended Fix | Priority |
|-------|-----------------|----------|
| Orphaned files | Add periodic S3 scan + reconciliation job | High |
| Schema drift | Add schema registry in Meta | Medium |
| Implicit ownership | Add explicit partition assignment API | Medium |
| Event gaps | Add snapshot-based cache rebuild | Low |

---

## Question 2: Data Node Crash → Redirect Issues

### Current Behavior

```
Data Node 1 crashes
  → Router detects via health check (2-10s)
  → Router marks node as Suspect
  → Writes redirect to Data Node 2 via hash-based selection
  → Data Node 2 writes to same shared S3 path
```

### Potential Issues Identified

**Issue 1: Data Loss Before Flush**
```
Write → In-memory cache → CRASH
Result: Data lost (unless WAL enabled)
```
- Current: WAL optional, default disabled
- Risk: High for real-time data

**Issue 2: No Data Replication Between Nodes**
```
Current: Shared storage only (S3/MinIO)
         No synchronous replication to other Data nodes
```
- If Data node crashes before S3 flush, data is lost
- WAL is local-only, not replicated

**Issue 3: Partition Ownership Ambiguity**
```
Before crash: Node1 owns partition P1
After crash:  Node2 receives writes for P1
Problem:      Node1 recovers, both think they own P1
```
- Current mitigation: Fencing tokens (but router may not acquire)
- Risk: Split-brain writes possible

**Issue 4: Query Inconsistency During Failover**
```
Query hits Node2 while Node1 has unflushed data
Result: Query misses in-flight data
```
- No read-your-writes guarantee across nodes

**Issue 5: Recovery Complexity**
```
Node1 crashes → recovers after 5 minutes
- Old lease expired
- New lease acquired
- Partition assignments unclear
- May duplicate writes
```

### SOTA Architectures Comparison

| System | Approach | Durability | Availability | Complexity |
|--------|----------|------------|--------------|------------|
| **Kafka** | ISR replication (N replicas) | High | High | Medium |
| **ClickHouse** | ReplicatedMergeTree (ZK) | High | High | High |
| **TiKV** | Raft per Region | Very High | Very High | Very High |
| **InfluxDB IOx** | Shared storage + WAL | Medium | Medium | Low |
| **QuestDB** | Single node + replication | Medium | Medium | Low |
| **TimescaleDB** | PostgreSQL streaming replication | High | High | Medium |

### Recommended Approaches

#### Approach A: Enhanced Shared Storage (Recommended - Low Complexity)

**Principle**: Keep shared storage model, add durability guarantees

```
┌─────────────────────────────────────────────────────────────┐
│                      WRITE PATH                              │
├─────────────────────────────────────────────────────────────┤
│  Client → Router → Data Node                                 │
│                      │                                       │
│                      ├── 1. Write to local WAL (sync)        │
│                      ├── 2. Write to replicated WAL (S3)     │
│                      ├── 3. Add to memtable                  │
│                      └── 4. ACK to client                    │
│                                                              │
│  Background: Memtable → Parquet → S3                         │
│              WAL cleanup after Parquet confirmed             │
└─────────────────────────────────────────────────────────────┘
```

**Changes Required:**
1. **Replicated WAL in S3**: Write WAL segments to S3, not just local
2. **WAL-based Recovery**: Any node can recover partition from S3 WAL
3. **Partition Locking**: Acquire Meta lock before writing partition
4. **Ordered Recovery**: On node crash, another node replays S3 WAL

**Pros**:
- Minimal architecture change
- Uses existing S3 infrastructure
- Simple recovery model

**Cons**:
- S3 write latency on hot path (can batch)
- Single point of write (no parallel writes to same partition)

#### Approach B: Replication Factor (Like Kafka)

**Principle**: Each partition has N replicas on different nodes

```
┌─────────────────────────────────────────────────────────────┐
│                    PARTITION REPLICAS                        │
├─────────────────────────────────────────────────────────────┤
│  Partition: db1/schema1/table1/date=2024-01-15              │
│                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │ Data-1   │    │ Data-2   │    │ Data-3   │              │
│  │ (Leader) │───▶│ (Replica)│───▶│ (Replica)│              │
│  └──────────┘    └──────────┘    └──────────┘              │
│       │                                                      │
│       ▼                                                      │
│  Write ACK after 2/3 replicas confirm (ISR)                 │
└─────────────────────────────────────────────────────────────┘
```

**Changes Required:**
1. **Partition Assignment in Meta**: Explicit leader + replica list per partition
2. **Replication Protocol**: Leader forwards writes to replicas
3. **ISR (In-Sync Replicas)**: Track which replicas are caught up
4. **Leader Election**: On leader crash, promote replica
5. **Catch-up Protocol**: Fallen-behind replica fetches from leader

**Pros**:
- Strong durability (survives N-1 failures)
- Fast failover (replica already has data)
- Read scalability (read from any replica)

**Cons**:
- High complexity
- 3x storage cost (for RF=3)
- Network overhead for replication

#### Approach C: Hot Standby Nodes

**Principle**: Reserve idle nodes that can take over crashed node's partitions

```
┌─────────────────────────────────────────────────────────────┐
│                    HOT STANDBY POOL                          │
├─────────────────────────────────────────────────────────────┤
│  Active: Data-1, Data-2, Data-3                              │
│  Standby: Data-4, Data-5 (idle, no partitions)              │
│                                                              │
│  On Data-1 crash:                                            │
│    1. Meta assigns Data-1's partitions to Data-4            │
│    2. Data-4 recovers from S3 WAL                           │
│    3. Data-4 becomes active for those partitions            │
│    4. Data-5 remains standby                                │
└─────────────────────────────────────────────────────────────┘
```

**Changes Required:**
1. **Standby Node Registration**: New node state `Standby`
2. **Automatic Failover**: Meta reassigns partitions on node crash
3. **Fast Recovery**: Standby pre-warms cache from S3

**Pros**:
- Simple mental model
- No replication overhead during normal operation
- Quick failover (nodes already running)

**Cons**:
- Wasted resources (idle nodes)
- Recovery time depends on S3 WAL size
- Still has data loss window (unflushed memtable)

---

## Recommended Implementation Plan

### Phase 1: Critical Durability Fixes (Immediate)

**Goal**: Prevent data loss on node crash

1. **Enable WAL by default** in production configs
2. **Add S3 WAL replication** (write WAL segments to S3)
3. **Implement partition locking** (acquire Meta lock before write)

**Files to modify:**
- `src/storage/wal.rs` - Add S3 WAL writer
- `src/config.rs` - Default WAL enabled
- `src/router/routing/write_router.rs` - Acquire partition lock
- `src/meta/raft/state_machine.rs` - Partition lock API

### Phase 2: Partition Management (Short-term)

**Goal**: Explicit partition ownership

1. **Add PartitionInfo to Meta**:
   ```rust
   struct PartitionInfo {
       key: PartitionKey,
       owner_node_id: String,
       state: PartitionState,  // Active, Reassigning, Draining
       last_write_seq: u64,
   }
   ```

2. **Partition Assignment API**:
   - `assign_partition(key, node_id)` - Explicit assignment
   - `reassign_partition(key, from_node, to_node)` - On failover
   - `list_partitions(node_id)` - Get node's partitions

3. **Router uses Meta for partition owner** (not just hash)

**Files to modify:**
- `src/meta/types.rs` - Add PartitionInfo
- `src/meta/raft/state_machine.rs` - Partition CRUD
- `src/router/routing/shard_cache.rs` - Use Meta partition info

### Phase 3: Replication (Long-term)

**Goal**: Multi-replica durability

Choose one:
- **A) S3 WAL + Recovery**: Lower complexity, good durability
- **B) ISR Replication**: Higher complexity, best durability

Recommend starting with **A**, then adding **B** for critical workloads.

---

## Summary

| Question | Current State | Risk Level | Recommendation |
|----------|---------------|------------|----------------|
| Meta stores metadata | Yes, via Raft | Low | Add orphan reconciliation, schema registry |
| Data node crash redirect | Hash-based failover | **High** | Add S3 WAL, partition locking |
| Data replication | None (shared storage only) | **High** | Phase 1: S3 WAL, Phase 2: ISR replication |
| Partition ownership | Implicit | Medium | Add explicit assignment in Meta |

**Priority Order:**
1. 🔴 Enable WAL by default + S3 WAL replication
2. 🔴 Partition locking before writes
3. 🟡 Explicit partition assignment in Meta
4. 🟢 Schema registry
5. 🟢 ISR replication (long-term)
