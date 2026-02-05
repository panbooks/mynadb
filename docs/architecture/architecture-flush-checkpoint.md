# Flush Checkpoint Protocol

## Overview

The Flush Checkpoint Protocol ensures cache consistency between Primary and Standby nodes in the Cold Standby architecture. When a Primary flushes data to Parquet, it notifies all Standbys to clear their corresponding cached data, preventing stale data and duplication on failover.

## Problem Statement

In the Cold Standby architecture:
1. Primary receives writes and replicates data to Standby nodes
2. Standby nodes store replicated data in their memtable (cache)
3. Primary periodically flushes cache to Parquet files
4. **Problem**: Without notification, Standby cache becomes stale after Primary flush

### Data Duplication Scenario

```
Timeline:
T1: Primary receives batch A (seq=1), replicates to Standby
T2: Primary receives batch B (seq=2), replicates to Standby
T3: Primary flushes to Parquet (data is now in files)
T4: Primary crashes
T5: Standby promoted to Primary
T6: Standby flushes cache (batches A & B written AGAIN!)
    -> DUPLICATE DATA in Parquet files
```

## Solution: Sequence-Based Cache Synchronization

### Core Concept

1. Each replicated batch carries a monotonically increasing sequence number
2. Standby stores batches with their sequence numbers
3. On flush, Primary sends FlushCheckpoint with the highest flushed sequence
4. Standby clears all batches with `seq <= flushed_seq`

### Protocol Flow

```
                Primary                              Standby
                   │                                    │
    Write batch A ─┤                                    │
                   │──── Replicate(seq=1, batch A) ────>│ Store (seq=1, A)
    Write batch B ─┤                                    │
                   │──── Replicate(seq=2, batch B) ────>│ Store (seq=2, B)
    Write batch C ─┤                                    │
                   │──── Replicate(seq=3, batch C) ────>│ Store (seq=3, C)
                   │                                    │
   ┌───────────────┴───────────────┐                    │
   │ FLUSH to Parquet              │                    │
   │ - Take cache data             │                    │
   │ - Write to Parquet files      │                    │
   │ - Record flushed_seq = 3      │                    │
   └───────────────┬───────────────┘                    │
                   │                                    │
                   │─ FlushCheckpoint(flushed_seq=3) ──>│
                   │                                    │ Clear seq <= 3
                   │<─ FlushCheckpointResponse ─────────│
                   │                                    │
    Write batch D ─┤                                    │
                   │──── Replicate(seq=4, batch D) ────>│ Store (seq=4, D)
                   │                                    │
```

## Implementation Details

### 1. Sequence Tracking in Cache

**File**: `src/storage/cache.rs`

```rust
/// A batch with its replication sequence number (0 for Primary/local data)
type SeqBatch = (u64, RecordBatch);

/// Internal cache data structure: key -> list of (sequence, batch) pairs
type CacheData = HashMap<CacheKey, Vec<SeqBatch>>;
```

- Primary writes use `seq=0` via `add_batch()` or `add_points()` (non-replicated local data)
- Standby stores batches with the sequence from replication via `add_batch_standby_with_seq(seq)`
- `clear_up_to_seq(flushed_seq)` removes batches where `seq <= flushed_seq`

**Important**: The `add_batch_standby()` method (without seq) is deprecated for new code.
Always use `add_batch_standby_with_seq()` to enable proper FlushCheckpoint synchronization.

### Sequence Flow

```
1. Primary receives write → add_batch() with seq=0
2. Primary replicates → ReplicationManager assigns seq from counter
3. Standby receives → Flight service extracts seq from ReplicationTicket
4. Standby stores → add_batch_standby_with_seq(seq) preserves sequence
5. Primary flushes → broadcasts FlushCheckpoint with current_seq
6. Standby clears → clear_up_to_seq(flushed_seq) removes old data
```

### 2. FlushCheckpoint Message

**File**: `src/data/replication/types.rs`

```rust
pub struct FlushCheckpoint {
    pub source_node_id: String,
    pub partition_key: Option<String>,
    pub flushed_seq: u64,
    pub timestamp: i64,
    pub files_written: u32,
    pub rows_flushed: u64,
}
```

### 3. ReplicationManager Tracking

**File**: `src/data/replication/stream.rs`

```rust
impl ReplicationManager {
    /// Record a flush operation
    pub fn record_flush(&self, flushed_seq: u64);

    /// Broadcast FlushCheckpoint to all standbys
    pub async fn broadcast_flush_checkpoint(&self, checkpoint: FlushCheckpoint) -> usize;
}
```

### 4. StandbyReceiver Handling

**File**: `src/data/replication/receiver.rs`

```rust
impl StandbyReceiver {
    /// Handle FlushCheckpoint from Primary
    pub fn handle_flush_checkpoint(&self, checkpoint: &FlushCheckpoint) -> FlushCheckpointResponse {
        let bytes_cleared = self.cache.clear_up_to_seq(checkpoint.flushed_seq);
        // ... return response
    }
}
```

## Edge Cases

### 1. Sequence Zero (seq=0)

- Used for Primary/local writes (not replicated)
- When `clear_up_to_seq(0)` is called, only seq=0 batches are cleared
- Higher sequence numbers remain in cache

### 2. Partial Flush

- If Primary flushes only some partitions, `partition_key` can be set
- Standby only clears the specified partition
- Currently, global flush clears all `seq <= flushed_seq` across all partitions

### 3. Out-of-Order Checkpoints

- Checkpoints are processed in order received
- If an older checkpoint arrives after a newer one, it's effectively a no-op
- Batches already cleared won't be affected

### 4. Standby Promotion Before Checkpoint

```
T1: Primary flushes (seq=10)
T2: Primary crashes before sending checkpoint
T3: Standby promoted with cache containing seq 1-10

Resolution:
- Standby flushes on promotion
- Data may be duplicated (same as pre-checkpoint behavior)
- Compaction will eventually merge duplicates
```

### 5. Network Partition

```
T1: Primary sends checkpoint
T2: Network partition - checkpoint lost
T3: Standby still has stale cache

Resolution:
- FlushCheckpoint is best-effort
- On reconnection, Standby can request latest checkpoint
- Worst case: duplicate data handled by compaction
```

## Configuration

No additional configuration required. The protocol uses existing replication configuration:

```toml
[replication]
enabled = true
mode = "async"  # or "sync"
batch_interval_ms = 100
ack_timeout_ms = 5000
```

## Metrics and Monitoring

### Key Metrics

1. **Checkpoint broadcast latency**: Time to send checkpoint to all standbys
2. **Bytes cleared per checkpoint**: Memory freed on standby
3. **Checkpoint success rate**: Percentage of standbys acknowledging

### Logging

```
INFO  Received flush checkpoint from primary-1: seq=100, files=5, rows=10000
INFO  Cleared 1048576 bytes from cache (seq <= 100)
INFO  Broadcasting flush checkpoint (seq=100) to 2 standby(s)
INFO  Flush checkpoint broadcast complete: 2/2 standbys notified
```

## Testing

### Unit Tests

Located in:
- `src/storage/cache.rs` - `test_cache_shard_clear_up_to_seq_*`
- `src/data/replication/types.rs` - `test_flush_checkpoint_*`
- `src/data/replication/receiver.rs` - `test_flush_checkpoint_*`

### Test Scenarios

1. **Basic clearing**: Clear batches with seq <= threshold
2. **All cleared**: All batches have seq <= threshold
3. **None cleared**: All batches have seq > threshold
4. **Edge boundary**: Exact match on threshold
5. **Multiple keys**: Different tables/partitions
6. **Empty cache**: No-op with empty cache
7. **Size tracking**: Verify size counters updated correctly

## Future Enhancements

### 1. Partition-Level Checkpoints

Send checkpoints per partition for finer-grained control:

```rust
// Current: Global checkpoint
FlushCheckpoint { flushed_seq: 100, partition_key: None }

// Future: Per-partition checkpoint
FlushCheckpoint { flushed_seq: 100, partition_key: Some("db/schema/table") }
```

### 2. Checkpoint Persistence

Store last checkpoint in Meta for recovery:
- On Standby restart, query Meta for latest checkpoint
- Clear cache for already-flushed sequences

### 3. Sync Mode Checkpoint

For `mode = "sync"`, wait for checkpoint acknowledgment before returning from flush:

```rust
// Current: Fire-and-forget
broadcast_flush_checkpoint(checkpoint).await;

// Future: Wait for all standbys
let acks = broadcast_flush_checkpoint_sync(checkpoint, timeout).await?;
```

## Related Documentation

- [Architecture: Cluster Mode](architecture-cluster.md)
- [Architecture: Cold Standby](architecture-cold-standby.md)
- [Architecture: Scale-In](architecture-scale-in.md)
