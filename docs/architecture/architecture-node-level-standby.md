# Node-Level Standby Architecture

## Overview

This document describes the node-level standby assignment architecture, where each Primary node is explicitly paired with a Standby node. This replaces the previous partition-level assignment where each partition independently selected its standbys.

## Problem with Partition-Level Assignment

### The Data Loss Bug

In partition-level assignment, each partition independently selects standbys based on load balancing:

```
Primary-1:
  Partition-A → replicated to Standby-1
  Partition-B → replicated to Standby-2  ← Different standby!
  Partition-C → replicated to Standby-1
```

When Primary-1 crashes, the recovery code had a critical bug:

```rust
fn find_standby_for_primary(&self, primary_id: &str) -> Result<String, RecoveryError> {
    for replica in &replicas {
        if replica.leader_node_id == primary_id {
            // BUG: Returns FIRST standby found, ignores others!
            if let Some(standby_id) = replica.standby_node_ids.first() {
                return Ok(standby_id.clone());
            }
        }
    }
}
```

**Result**: Only Standby-1 was asked to flush. Partition-B's cache on Standby-2 was never flushed → **DATA LOSS**.

### Complexity Issues

Even with a fix to find ALL standbys, partition-level has inherent complexity:

1. **Recovery requires scanning all partitions** - O(partitions) to find all standbys
2. **Fragmented standby state** - Standby has partial data from multiple Primaries
3. **Multiple flush targets** - Must coordinate flush across multiple standbys
4. **Race conditions** - Different standbys may be at different replication states

## Node-Level Solution

### Core Concept

Each Primary node is explicitly paired with a Standby node at registration time:

```
Primary-1.backing_standby = Standby-1
Primary-2.backing_standby = Standby-1
Primary-3.backing_standby = Standby-2
Primary-4.backing_standby = Standby-2
```

**All data from Primary-1 replicates to Standby-1, regardless of partition.**

### Data Model

```rust
pub struct NodeInfo {
    pub node_id: String,
    pub role: NodeRole,
    pub state: NodeState,
    pub mode: Option<DataNodeMode>,

    // Node-level standby pairing
    /// For Primary nodes: the Standby that backs this Primary
    pub backing_standby: Option<String>,
    /// For Standby nodes: list of Primaries this Standby backs
    pub backed_primaries: Vec<String>,
}
```

### Registration Flow

**With ratio 2:1 (2 Primaries per 1 Standby):**

```
T1: Primary-1 registers
    → mode = Primary
    → backing_standby = None (no Standby available yet)
    → QUEUED for pairing

T2: Primary-2 registers
    → mode = Primary
    → backing_standby = None
    → QUEUED for pairing

T3: Standby-1 registers
    → mode = Standby
    → Find primaries needing backup (Primary-1, Primary-2)
    → backed_primaries = [Primary-1, Primary-2]
    → Update Primary-1.backing_standby = Standby-1
    → Update Primary-2.backing_standby = Standby-1

T4: Primary-3 registers
    → mode = Primary
    → Standby-1 already at capacity (2 primaries)
    → backing_standby = None (QUEUED)

T5: Standby-2 registers
    → Find primaries needing backup (Primary-3, Primary-4 if exists)
    → backed_primaries = [Primary-3]
    → Update Primary-3.backing_standby = Standby-2
```

### Replication Flow

```rust
// When Primary receives a write
async fn handle_write(&self, batch: RecordBatch) {
    // Write to local cache
    self.cache.add_batch(batch.clone()).await;

    // Replicate to assigned standby (node-level, not partition-level)
    if let Some(standby_id) = &self.backing_standby {
        self.replication_manager.replicate_to_standby(standby_id, batch).await;
    }
}
```

### Recovery Flow

```rust
async fn handle_primary_crash(&self, primary_id: &str) {
    // O(1) lookup - no partition scanning needed
    let standby_id = self.get_node(primary_id)?.backing_standby
        .ok_or(RecoveryError::NoStandbyForPrimary)?;

    // Standby has ALL of Primary's data
    self.trigger_standby_flush(&standby_id, primary_id).await?;

    // Redistribute partitions to remaining Primaries
    self.redistribute_partitions(primary_id).await?;
}
```

## Comparison: Partition-Level vs Node-Level

### Architecture Diagram

**Partition-Level (Old):**
```
Primary-1                          Standby-1
├─ Partition-A ──────────────────→ Partition-A cache
├─ Partition-B ─────┐
└─ Partition-C ─────│────────────→ Partition-C cache
                    │
                    └────────────→ Standby-2
                                   Partition-B cache
```

**Node-Level (New):**
```
Primary-1 ════════════════════════→ Standby-1
├─ Partition-A                      ├─ Partition-A cache
├─ Partition-B                      ├─ Partition-B cache
└─ Partition-C                      └─ Partition-C cache

Primary-2 ════════════════════════→ Standby-1
├─ Partition-D                      ├─ Partition-D cache
└─ Partition-E                      └─ Partition-E cache
```

### Feature Comparison

| Aspect | Partition-Level | Node-Level |
|--------|-----------------|------------|
| **Data model** | Per-partition standby list | Per-node standby assignment |
| **Replication target** | Varies per partition | Fixed per primary |
| **Recovery lookup** | O(partitions) scan | O(1) lookup |
| **Risk of missed standby** | High | None |
| **Standby state** | Fragmented partitions | Complete mirror |
| **Load balancing** | Fine-grained | Coarse (per-node) |
| **Memory on standby** | Lower (partial) | Higher (complete) |
| **Implementation complexity** | High | Low |

### Recovery Comparison

| Scenario | Partition-Level | Node-Level |
|----------|-----------------|------------|
| Primary-1 crashes | Find all standbys across partitions | Get Primary-1.backing_standby |
| Flush trigger | Multiple standbys, coordinate | Single standby |
| Data completeness | Risk of missing standby | Guaranteed complete |
| Recovery time | O(partitions) + O(standbys) | O(1) |

### Reliability Comparison

| Failure Mode | Partition-Level | Node-Level |
|--------------|-----------------|------------|
| Primary crash | May lose data on missed standby | All data on one standby |
| Standby crash | Only affects its partitions | Affects all backed primaries |
| Network partition | Complex (per-partition state) | Simple (per-node state) |
| Split brain | Per-partition conflicts | Per-node conflicts |

## Design Decisions

### Why Node-Level is Better

1. **Reliability over optimization**: The primary goal is preventing data loss. Node-level guarantees a single standby has all data.

2. **Simplicity**: One standby per primary is easier to reason about, implement, and debug.

3. **O(1) recovery**: No partition scanning needed. Immediate standby lookup.

4. **Complete mirror**: Standby is a true mirror of Primary, enabling clean failover.

5. **Consistent replication**: All writes go to same standby, no per-partition variance.

### Trade-offs Accepted

1. **Higher standby memory**: Standby holds complete mirror (2x with 2:1 ratio)
2. **Less load balancing flexibility**: Can't optimize per-partition
3. **Standby hotspots**: If one Primary is hot, its Standby is also hot

### Mitigations

1. **Memory**: Size standbys appropriately (same as primaries)
2. **Load balancing**: Use ratio to spread load (2:1, 3:1)
3. **Hotspots**: Monitor and rebalance if needed

## Implementation Notes

### NodeInfo Changes

```rust
pub struct NodeInfo {
    // ... existing fields ...

    /// For Primary: Standby that backs this node (node-level pairing)
    #[serde(default)]
    pub backing_standby: Option<String>,

    /// For Standby: Primaries this node backs (node-level pairing)
    #[serde(default)]
    pub backed_primaries: Vec<String>,
}
```

### Registration Changes

```rust
fn register_node(&self, mut info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
    // ... existing mode assignment ...

    if info.mode == Some(DataNodeMode::Primary) {
        // Try to assign a standby
        if let Some(standby_id) = self.find_available_standby() {
            info.backing_standby = Some(standby_id.clone());
            self.add_primary_to_standby(&standby_id, &info.node_id);
        }
    } else if info.mode == Some(DataNodeMode::Standby) {
        // Assign to primaries needing backup
        let primaries = self.find_primaries_needing_backup();
        info.backed_primaries = primaries.clone();
        for primary_id in &primaries {
            self.set_primary_backing_standby(primary_id, &info.node_id);
        }
    }

    // ... store node ...
}
```

### Replication Changes

```rust
impl ReplicationManager {
    /// Replicate to the Primary's assigned standby (node-level)
    pub async fn replicate_to_backing_standby(
        &self,
        batch: RecordBatch,
    ) {
        if let Some(standby_id) = &self.backing_standby {
            self.replicate_to_standby(standby_id, batch).await;
        }
    }
}
```

### Recovery Changes

```rust
fn find_standby_for_primary(&self, primary_id: &str) -> Result<String, RecoveryError> {
    // O(1) lookup - no partition scanning
    let node = self.get_node(primary_id)?;
    node.backing_standby.clone()
        .ok_or(RecoveryError::NoStandbyForPrimary { primary_id: primary_id.to_string() })
}
```

## Migration from Partition-Level

### Removed Components

1. `PartitionReplica.standby_node_ids` - No longer needed for replication
2. `ReplicaAssigner.select_standbys_*` - No longer needed
3. `get_standbys_for_partition()` - Replaced by node-level lookup
4. Per-partition standby assignment logic

### Kept Components

1. `PartitionReplica.leader_node_id` - Still needed for partition ownership
2. Partition redistribution on Primary crash
3. Replication factor concept (for future multi-standby support)

## Future Enhancements

### Multi-Standby Support

For higher reliability, a Primary could have multiple standbys:

```rust
pub backing_standbys: Vec<String>,  // Multiple standbys per primary
```

### Zone-Aware Pairing

Pair Primaries with Standbys in different availability zones:

```rust
fn find_available_standby(&self, primary: &NodeInfo) -> Option<String> {
    // Prefer standby in different zone
    self.standbys
        .filter(|s| s.zone != primary.zone)
        .min_by_key(|s| s.backed_primaries.len())
}
```

### Dynamic Rebalancing

Rebalance pairings when load is uneven:

```rust
async fn rebalance_pairings(&self) {
    // Move primaries between standbys to balance load
}
```
