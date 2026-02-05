# Scale-In Architecture for Object Store Clusters

## Overview

MynaDB's Scale-In feature enables **true cluster shrinking** - reducing the number of nodes from N to M (e.g., 100 nodes → 80 nodes) with zero downtime and no data loss. This is made possible by the Object Store architecture where data is stored in shared S3/MinIO storage.

## Key Insight: Object Store Changes Everything

With S3/MinIO, data is stored in **shared object storage**:
- Any node can read any partition's Parquet files from S3
- The `node_id` prefix is only for **write separation**, not read restriction
- Once memtable flushes to S3, the data is accessible from anywhere

**This means:** We can truly shrink the cluster without promoting Standbys or migrating data!

```
OBJECT STORE ARCHITECTURE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  S3 Bucket (SHARED STORAGE - All nodes can read!)                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ data-0/mydb/default/cpu/date=2024-01-15/L0_*.parquet           │   │
│  │ data-1/mydb/default/cpu/date=2024-01-15/L0_*.parquet           │   │
│  │ data-2/mydb/default/cpu/date=2024-01-15/L0_*.parquet ← RETIRING│   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↑                                          │
│                    ANY node can read this!                              │
│                                                                         │
│  When data-2 retires:                                                   │
│  - Flush memtable to S3 (warm → cold)                                  │
│  - Reassign leadership of data-2's partitions to data-0 or data-1      │
│  - data-0/data-1 can query data-2's S3 files directly!                │
│  - No data migration needed!                                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Node States for Scale-In

```rust
pub enum NodeState {
    Starting = 1,    // Node is starting up
    Active = 2,      // Normal operation
    Draining = 3,    // Graceful maintenance (brief)
    Inactive = 4,    // Node stopped
    Retiring = 5,    // Scale-in: no new writes, flushing
    Drained = 6,     // Scale-in complete, safe to stop
}
```

### State Transition Flow

```
NodeState Flow for Scale-In:
  Active → Retiring → Drained → Stopped
           ↑          ↑         ↑
           │          │         │
    No new writes   Memtable   Queries
    to this node    flushed    reassigned
```

---

## Scale-In: Primary Node Process

### Step-by-Step Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 1: Mark as RETIRING                                               │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  Action: POST /nodes/{node_id}/retire                                  │
│                                                                         │
│  Effects:                                                               │
│  • NodeState = Retiring                                                │
│  • Router STOPS sending NEW partition writes to this node              │
│  • Existing partitions → leadership reassigned NOW                     │
│  • Queries still served (data still in memtable)                       │
│                                                                         │
│  Meta State Machine:                                                    │
│  • Remove node from "available for new partitions" pool                │
│  • Begin partition leadership reassignment                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 2: Reassign Partition Leadership                                  │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  For each partition where this node is leader:                         │
│                                                                         │
│  1. Select new leader from OTHER active Primary nodes                  │
│     • Prefer least-loaded Primary                                      │
│     • New leader will write NEW data to its own S3 prefix              │
│                                                                         │
│  2. Update PartitionReplica in Meta:                                   │
│     • leader_node_id = new_primary                                     │
│                                                                         │
│  3. Publish ReplicaReassigned event → Router cache invalidation        │
│                                                                         │
│  Note: New writes go to new leader, but old data stays in S3           │
│        (at retiring node's prefix). Queries can read both!             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 3: Flush Memtable to S3 (5-10 minutes)                           │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  Action: Wait for DataCache flush interval OR force flush              │
│                                                                         │
│  What happens:                                                          │
│  • All warm data in memtable → Parquet files in S3                     │
│  • After flush: 0 rows in memtable                                     │
│  • All data now "cold" (in S3, queryable by any node)                  │
│                                                                         │
│  Trigger condition for next step:                                       │
│  • memtable_rows == 0 for this node                                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 4: Transition to DRAINED                                          │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  Now that all data is in S3, other nodes can serve queries:            │
│                                                                         │
│  For each partition this node was serving:                             │
│  • Query routing → new leader (or any Primary)                         │
│  • New leader queries S3:                                              │
│    - Its own prefix: s3://bucket/{new_leader}/catalog/schema/table/   │
│    - Retiring node's prefix: s3://bucket/{retiring}/catalog/...       │
│                                                                         │
│  After this step: NodeState = Drained                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 5: Stop the Node                                                  │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  Action: Node deregisters and shuts down                               │
│                                                                         │
│  • No data loss (all data in S3)                                       │
│  • No query disruption (other nodes serving queries)                   │
│  • Standbys stop replicating from this node                            │
│                                                                         │
│  Cleanup (optional, deferred):                                          │
│  • S3 data under retiring node's prefix can be:                        │
│    a) Left as-is (queries still work)                                  │
│    b) Compacted into new leader's prefix (reduces S3 prefixes)         │
│    c) Archived to cold storage tier                                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Scale-In: Standby Node Process

Standby scale-in is simpler because Standbys don't own unique data:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  STANDBY SCALE-IN                                                       │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  What Standbys have:                                                    │
│  • Replicated memtable data (warm, not yet in S3)                      │
│  • This data WILL be flushed by the Primary (it's authoritative)       │
│                                                                         │
│  Scale-in process:                                                      │
│  1. Mark Standby as Retiring                                           │
│  2. Stop replication streams TO this Standby                           │
│  3. Update PartitionReplica: remove from standby_node_ids              │
│  4. Ensure other Standbys exist for failover coverage                  │
│  5. Stop the Standby node                                              │
│                                                                         │
│  Data safety:                                                           │
│  • Memtable data on Standby is a COPY (Primary has original)           │
│  • Primary will flush to S3 regardless                                 │
│  • No data loss possible                                               │
│                                                                         │
│  Constraint:                                                            │
│  • Must maintain minimum Standby count for failover                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Node Selection Algorithm

### Who Chooses Which Nodes to Scale In?

#### Option A: Operator Designates Specific Nodes

```bash
# Operator explicitly retires a specific node
curl -X POST http://meta-0:9000/nodes/data-15/retire
```

#### Option B: Meta Service Auto-Recommends

```bash
# Ask Meta for recommendations
curl "http://meta-0:9000/cluster/scale-in/recommend?primaries=5&standbys=3"
```

#### Option C: Hybrid (Recommended)

```bash
# Step 1: Get recommendations
curl "http://meta-0:9000/cluster/scale-in/recommend?primaries=5"

# Step 2: Review and approve
curl -X POST http://meta-0:9000/cluster/scale-in/execute \
  -d '{"nodes": ["data-15", "data-23", "data-47"]}'
```

### Selection Algorithm

```rust
fn recommend_scale_in(reduce_primaries: usize, reduce_standbys: usize) -> ScaleInRecommendation {
    // For Primaries: prefer nodes with fewer partitions
    let primaries = get_primaries_sorted_by_partition_count_asc();

    // For Standbys: prefer nodes with most redundancy
    let standbys = get_standbys_sorted_by_redundancy_desc();

    // Apply constraints:
    for node in primaries.take(reduce_primaries) {
        // Check: After removing, are all partitions still covered?
        // Check: Is there at least 1 other Primary to take over?
        if can_safely_remove(&node) {
            candidates.push(node);
        }
    }

    for node in standbys.take(reduce_standbys) {
        // Check: After removing, do all partitions still have >= 1 standby?
        if can_safely_remove(&node) {
            candidates.push(node);
        }
    }

    ScaleInRecommendation { candidates, reasons, warnings }
}
```

---

## Router Behavior During Scale-In

### Write Routing

```
┌─────────────────────────────────────────────────────────────────────────┐
│  WRITE ROUTING                                                          │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  Router.select_node_for_partition():                                   │
│                                                                         │
│  ✓ Active nodes      → ELIGIBLE for writes                            │
│  ✗ Retiring nodes    → NOT ELIGIBLE (scale-in in progress)            │
│  ✗ Drained nodes     → NOT ELIGIBLE (about to stop)                   │
│  ✗ Draining nodes    → NOT ELIGIBLE (maintenance)                     │
│  ✗ Standby nodes     → NOT ELIGIBLE (cold backup)                     │
│                                                                         │
│  Selection priority:                                                    │
│  1. Partition leader (if Active)                                       │
│  2. Active Standby (instant failover if leader unhealthy)              │
│  3. Hash-based selection among Active Primary nodes                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Read Routing

```
┌─────────────────────────────────────────────────────────────────────────┐
│  READ ROUTING                                                           │
│  ─────────────────────────────────────────────────────────────────────  │
│                                                                         │
│  Router.get_queryable_data_nodes():                                    │
│                                                                         │
│  ✓ Active nodes      → ELIGIBLE for reads                             │
│  ✓ Retiring nodes    → ELIGIBLE (may have unflushed memtable data!)   │
│  ✗ Drained nodes     → NOT ELIGIBLE (data already in S3)              │
│  ✗ Standby nodes     → NOT ELIGIBLE (cold standby architecture)       │
│                                                                         │
│  Why Retiring nodes serve reads:                                        │
│  • Data may still be in memtable (not yet flushed to S3)              │
│  • Other nodes can't query this data until it's in S3                  │
│  • Once flushed (Drained state), reads go to other nodes              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## API Reference

### Meta Service HTTP API (Port 9000)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/nodes/{node_id}/retire` | POST | Start graceful retirement |
| `/nodes/{node_id}/retire/status` | GET | Get retirement progress |
| `/nodes/{node_id}/retire/cancel` | POST | Cancel retirement |
| `/cluster/scale-in/recommend` | GET | Get recommended nodes |
| `/cluster/scale-in/execute` | POST | Batch retire multiple nodes |

### Data Node HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/retire` | POST | Start local retirement |
| `/retire/status` | GET | Get local retirement status |
| `/retire/cancel` | POST | Cancel local retirement |

### Meta gRPC API

| RPC | Description |
|-----|-------------|
| `StartRetire` | Mark node as Retiring |
| `CompleteRetire` | Mark node as Drained |
| `CancelRetire` | Restore node to Active |
| `GetRetireStatus` | Get retirement status |
| `ReassignPartitionForRetire` | Transfer partition leadership |
| `RemoveStandbyFromReplicas` | Remove Standby from partition replicas |
| `RecommendScaleIn` | Get scale-in recommendations |

---

## Data Structures

### RetireStatus

```rust
pub struct RetireStatus {
    pub node_id: String,
    pub state: NodeState,           // Retiring or Drained
    pub started_at: i64,            // Unix timestamp ms
    pub partitions_total: usize,    // Total partitions this node led
    pub partitions_reassigned: usize,
    pub memtable_rows_remaining: u64,
    pub estimated_completion_secs: u64,
}
```

### ScaleInRecommendation

```rust
pub struct ScaleInRecommendation {
    pub recommended_primaries: Vec<String>,
    pub recommended_standbys: Vec<String>,
    pub reasons: HashMap<String, String>,  // node_id -> reason
    pub warnings: Vec<String>,             // e.g., "Will leave partition X with 0 standbys"
}
```

---

## Files Modified

| Component | Files |
|-----------|-------|
| Meta Types | `src/meta/types.rs` |
| State Machine | `src/meta/raft/state_machine.rs` |
| Proto | `proto/meta.proto` |
| gRPC Service | `src/meta/service/grpc.rs` |
| Meta HTTP | `src/meta/service/http.rs` (NEW) |
| MetaNode | `src/meta/service/meta_node.rs` |
| MetaClient | `src/meta/client/meta_client.rs` |
| Data Node API | `src/data/server/api.rs` |
| Data Node | `src/node/data.rs` |
| Router Cache | `src/router/routing/shard_cache.rs` |
| Cache Invalidation | `src/router/routing/cache_invalidation.rs` |
| Query Coordinator | `src/router/routing/query_coordinator.rs` |
| Router API | `src/router/api.rs` |
| Meta Config | `src/meta/config.rs` |

---

## Example: Shrink 100 → 80 Nodes

```bash
# Current: 60 Primaries + 40 Standbys = 100 nodes
# Target: 48 Primaries + 32 Standbys = 80 nodes
# Remove: 12 Primaries + 8 Standbys = 20 nodes

# Step 1: Get recommendations
curl "http://meta-0:9000/cluster/scale-in/recommend?primaries=12&standbys=8"

# Response:
{
  "recommended_primaries": ["data-15", "data-23", "data-47", ...],
  "recommended_standbys": ["data-78", "data-82", ...],
  "reasons": {
    "data-15": "lowest_partition_count (23 partitions)",
    "data-23": "lowest_partition_count (25 partitions)"
  },
  "warnings": []
}

# Step 2: Execute scale-in
curl -X POST http://meta-0:9000/cluster/scale-in/execute \
  -H "Content-Type: application/json" \
  -d '{
    "nodes": ["data-15", "data-23", "data-47", "data-78", "data-82", ...]
  }'

# Step 3: Monitor progress
watch 'curl -s http://meta-0:9000/nodes/data-15/retire/status'

# Step 4: Wait for all nodes to reach "drained" state
# Step 5: Stop the drained nodes

# Result: 80 nodes running, zero downtime, no data loss
```

---

## Summary

| Scenario | Object Store Approach |
|----------|----------------------|
| **Scale-In Primary** | Retire → Flush to S3 → Reassign partitions → Stop (other Primaries read from S3) |
| **Scale-In Standby** | Retire → Stop replication → Remove from replicas → Stop (no data loss) |
| **Node Selection** | Hybrid: Meta recommends (least loaded), Operator approves |
| **Data Migration** | NOT NEEDED! All data in shared S3, any node can query any prefix |

**Key Advantage of Object Store:** True scale-in without data migration or Standby promotion!
