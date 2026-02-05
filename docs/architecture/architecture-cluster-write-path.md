# Write Path Architecture (Cluster Mode)

This document details the complete write path in MynaDB cluster mode, from HTTP request through to Parquet file persistence and metadata registration.

## Overview

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Client  │────▶│  Router  │────▶│   Data   │────▶│   Meta   │
│          │     │   Node   │     │   Node   │     │  Service │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
                      │                 │                │
                      │   gRPC Write    │   File Reg     │
                      │◀────────────────│◀───────────────│
                      │                 │                │
                      │   Event Stream  │                │
                      │◀─────────────────────────────────│
```

## Detailed Write Flow

### Example Request

```bash
curl -X POST "http://router:7000/write/mydb" \
  -d "cpu,host=server1,region=us-east value=85.5 1704067200000000000
cpu,host=server2,region=us-west value=72.3 1704067200000000000"
```

### Step-by-Step Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. CLIENT REQUEST                                                           │
│                                                                              │
│  POST /write/mydb                                                           │
│  Body: "cpu,host=server1,region=us-east value=85.5 1704067200000000000"     │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  2. ROUTER: HTTP Handler                                                     │
│  File: src/router/api.rs                                                    │
│                                                                              │
│  pub async fn write_handler(                                                │
│      Path(db): Path<String>,        // "mydb"                               │
│      body: String,                  // line protocol                        │
│  ) -> impl IntoResponse {                                                   │
│      state.write_router.route_write(&db, &body).await                       │
│  }                                                                          │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  3. ROUTER: Parse & Partition                                                │
│  File: src/router/routing/write_router.rs                                   │
│                                                                              │
│  // Parse line protocol into Points                                         │
│  let points = parse_multiple_lines(line_protocol, &schema_config);          │
│                                                                              │
│  // Group by partition key: catalog/schema/table/date                       │
│  let partitions = group_by_partition(catalog, points);                      │
│                                                                              │
│  Example partition keys:                                                    │
│    - mydb/default/cpu/date=2024-01-01                                       │
│    - mydb/default/memory/date=2024-01-01                                    │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  4. ROUTER: Select Target Data Nodes                                         │
│  File: src/router/routing/shard_cache.rs                                    │
│                                                                              │
│  pub fn select_node_for_partition(&self, key: &PartitionKey) -> Option<..> {│
│      // Strategy 1: Check cached partition owner (sticky routing)           │
│      if let Some(owner) = self.get_partition_owner(key) {                   │
│          if owner.state == NodeState::Active {                              │
│              return Some(owner);  // Route to existing owner                │
│          }                                                                  │
│      }                                                                      │
│                                                                              │
│      // Strategy 2: Consistent hash (fallback)                              │
│      let hash = hash_partition_key(key);                                    │
│      let idx = (hash as usize) % nodes.len();                               │
│      Some(nodes[idx].clone())                                               │
│  }                                                                          │
│                                                                              │
│  Example: "mydb/default/cpu/date=2024-01-01" → DataNode-2                   │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  5. ROUTER: Batch & Dispatch (Parallel)                                      │
│  File: src/router/routing/write_router.rs                                   │
│                                                                              │
│  // Group partitions by target node                                         │
│  let node_batches = group_by_target_node(partitions);                       │
│  // Result:                                                                 │
│  //   DataNode-1 → [partition_a, partition_b]                               │
│  //   DataNode-2 → [partition_c]                                            │
│                                                                              │
│  // Dispatch to all nodes in parallel                                       │
│  let results = futures::future::join_all(                                   │
│      node_batches.into_iter().map(|(node_id, batches)| {                    │
│          send_write_request(node_id, batches)                               │
│      })                                                                     │
│  ).await;                                                                   │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ gRPC
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  6. DATA NODE: Receive Write Request                                         │
│  File: src/data/server/grpc.rs                                              │
│                                                                              │
│  async fn write(&self, request: Request<WriteRequest>) -> ... {             │
│      let req = request.into_inner();                                        │
│      // req.catalog, req.line_protocol, req.fencing_token                   │
│                                                                              │
│      // Validate fencing token (prevents stale writes)                      │
│      if !validate_fencing_token(req.fencing_token) {                        │
│          return Err(Status::failed_precondition("stale token"));            │
│      }                                                                      │
│                                                                              │
│      // Ingest to cache                                                     │
│      self.ingest_line_protocol(&req.catalog, &line_protocol).await          │
│  }                                                                          │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  7. DATA NODE: In-Memory Cache                                               │
│  File: src/storage/cache.rs                                                 │
│                                                                              │
│  pub async fn add_points(&self, catalog, schema, table, points) {           │
│      // Convert Points → Arrow RecordBatch                                  │
│      let batch = points_to_record_batch(&points);                           │
│                                                                              │
│      // Track tag cardinality (for optimal Parquet sort order)              │
│      cardinality_tracker.track_point(&point.tags);                          │
│                                                                              │
│      // Add to sharded cache (32 shards, consistent hash)                   │
│      let shard_idx = hash(key) % 32;                                        │
│      shards[shard_idx].add(key, batch);                                     │
│                                                                              │
│      // Trigger flush at 80% capacity                                       │
│      if current_size >= threshold {                                         │
│          flush_notify.notify_one();                                         │
│      }                                                                      │
│  }                                                                          │
│                                                                              │
│  Cache Structure:                                                           │
│  ┌─────────────────────────────────────────────────────────┐                │
│  │ Shard 0  │ Shard 1  │ ... │ Shard 31 │  (32 shards)     │                │
│  ├──────────┼──────────┼─────┼──────────┤                  │                │
│  │ key→[rb] │ key→[rb] │ ... │ key→[rb] │  (RecordBatches) │                │
│  └─────────────────────────────────────────────────────────┘                │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  8. DATA NODE: Flush to Parquet                                              │
│  File: src/storage/cache.rs (flush method)                                  │
│                                                                              │
│  pub async fn flush(&self) -> Result<Vec<FileWriteResult>, String> {        │
│      // Phase 1: Quick swap (minimal locking, ~1ms)                         │
│      let all_data = shards.iter().map(|s| s.take()).collect();              │
│      total_size_bytes.store(0);  // Reset counter                           │
│      // Lock released - writers continue immediately                        │
│                                                                              │
│      // Phase 2: Write Parquet files (slow, no locks held)                  │
│      for ((catalog, schema, table, date), batches) in all_data {            │
│          writer.write_batches_partitioned(                                  │
│              catalog, schema, table, date, batches, tag_order               │
│          );                                                                 │
│      }                                                                      │
│  }                                                                          │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  9. DATA NODE: Write Parquet File                                            │
│  File: src/storage/parquet_writer.rs                                        │
│                                                                              │
│  Output file: mydb/default/cpu/date=2024-01-01/L0_1704067200000.parquet     │
│                                                                              │
│  Parquet Configuration:                                                     │
│    - Row Group Size: 500,000 rows                                           │
│    - Data Page Size: 1 MB                                                   │
│    - Compression: Zstd                                                      │
│    - Bloom Filters: Enabled (1% FPP)                                        │
│    - Sort Order: low-cardinality tags → high-cardinality tags → time        │
│                                                                              │
│  File Naming: L{level}_{timestamp}.parquet                                  │
│    - L0 = raw writes                                                        │
│    - L1+ = compacted files                                                  │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  10. DATA NODE: Acquire Lock & Register with Meta                            │
│  File: src/meta/client/meta_client.rs                                       │
│                                                                              │
│  // Acquire distributed lock before writing                                 │
│  let fencing_token = meta_client.try_lock(                                  │
│      "partition/mydb/default/cpu/date=2024-01-01",                          │
│      ttl_ms: 30000                                                          │
│  ).await?;  // Returns fencing_token: 42                                    │
│                                                                              │
│  // After writing, register file with Meta                                  │
│  meta_client.register_files(vec![FileInfo {                                 │
│      path: "mydb/default/cpu/date=2024-01-01/L0_1704067200000.parquet",     │
│      catalog: "mydb",                                                       │
│      schema: "default",                                                     │
│      table: "cpu",                                                          │
│      partition: "date=2024-01-01",                                          │
│      level: 0,                                                              │
│      size_bytes: 1048576,                                                   │
│      row_count: 50000,                                                      │
│      min_time: 1704067200000000000,                                         │
│      max_time: 1704153600000000000,                                         │
│      created_by: "DataNode-2",                                              │
│  }]).await?;                                                                │
│                                                                              │
│  // Release lock                                                            │
│  meta_client.unlock("partition/...", fencing_token).await?;                 │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  11. META SERVICE: Store & Broadcast                                         │
│  File: src/meta/service/grpc.rs                                             │
│                                                                              │
│  // Store file info in Raft state machine (sled)                            │
│  state_machine.apply(Command::RegisterFiles { files });                     │
│                                                                              │
│  // Broadcast event to all subscribers                                      │
│  event_publisher.publish(Event {                                            │
│      event_type: EventType::FilesRegistered,                                │
│      sequence: 1001,                                                        │
│      payload: json!({ "files": [...] }),                                    │
│  });                                                                        │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ Event Stream
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  12. ROUTER: Cache Invalidation                                              │
│  File: src/router/routing/cache_invalidation.rs                             │
│                                                                              │
│  // Receive FilesRegistered event                                           │
│  async fn process_events(event_rx, shard_cache) {                           │
│      while let Some(event) = event_rx.recv().await {                        │
│          match event.event_type {                                           │
│              EventType::FilesRegistered => {                                │
│                  // Update partition → node mapping                         │
│                  shard_cache.set_partition_owner(                           │
│                      PartitionKey::new("mydb", "default", "cpu", "2024-01-01"),│
│                      DataNodeInfo { node_id: "DataNode-2", ... }            │
│                  );                                                         │
│              }                                                              │
│          }                                                                  │
│      }                                                                      │
│  }                                                                          │
│                                                                              │
│  → Future writes to mydb/default/cpu/date=2024-01-01 route to DataNode-2    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Components

| Component | File | Purpose |
|-----------|------|---------|
| **WriteRouter** | `src/router/routing/write_router.rs` | Parse line protocol, partition, route to Data nodes |
| **ShardCache** | `src/router/routing/shard_cache.rs` | Partition → Data node mapping (sticky routing) |
| **DataNodePool** | `src/router/client/data_node_pool.rs` | gRPC connection pool to Data nodes |
| **DataCache** | `src/storage/cache.rs` | 32-shard in-memory write buffer |
| **ParquetWriter** | `src/storage/parquet_writer.rs` | Write optimized Parquet files |
| **MetaClient** | `src/meta/client/meta_client.rs` | Distributed locks, file registration |
| **CacheInvalidation** | `src/router/routing/cache_invalidation.rs` | Event-driven cache updates |

## Design Patterns

### 1. Sticky Routing

Partitions preferentially route to the Data node that already owns their files:

```
Write 1: mydb/default/cpu/date=2024-01-01 → hash → DataNode-2
Write 2: mydb/default/cpu/date=2024-01-01 → cached → DataNode-2 (sticky!)
Write 3: mydb/default/cpu/date=2024-01-01 → cached → DataNode-2 (sticky!)
```

**Benefits:**
- Improved cache locality (data stays hot on one node)
- Reduced lock contention (same node writes to same partition)
- Better compaction efficiency (files grouped on same node)

### 2. Fencing Tokens (Split-Brain Prevention)

```
Timeline:
  T1: DataNode-2 acquires lock → fencing_token: 42
  T2: DataNode-2 network partition (appears dead)
  T3: Lock expires (TTL)
  T4: DataNode-1 acquires lock → fencing_token: 43
  T5: DataNode-2 recovers, tries to write with token 42
      → Meta REJECTS (token 42 < current 43)
      → Prevents duplicate/corrupt writes
```

### 3. Swap-and-Flush (Non-Blocking Writes)

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: Swap (fast, ~1ms)                                  │
│   - Atomically swap data pointers from all 32 shards        │
│   - Reset size counters                                     │
│   - Release all locks immediately                           │
│   - Writers continue to NEW empty shards                    │
├─────────────────────────────────────────────────────────────┤
│ Phase 2: Write (slow, ~100ms+)                              │
│   - Sort RecordBatches by cardinality-based tag order       │
│   - Write Parquet files to object storage                   │
│   - Register files with Meta service                        │
│   - NO LOCKS HELD - writes continue unblocked               │
└─────────────────────────────────────────────────────────────┘
```

### 4. Event-Driven Cache Consistency

```
Meta Service broadcasts FilesRegistered event:
     │
     ├──▶ Router-1: updates ShardCache mapping
     ├──▶ Router-2: updates ShardCache mapping
     └──▶ Router-3: updates ShardCache mapping

All routers now agree: DataNode-2 owns mydb/default/cpu/date=2024-01-01
```

**Benefits:**
- No polling required
- Eventual consistency across all Router nodes
- Sequence numbers prevent message loss

### 5. Cardinality-Based Sort Order

Tags are sorted by cardinality (low → high) before time:

```
Tags: host (3 unique), region (2 unique), rack (50 unique)

Sort order: region → host → rack → time

Result: Better compression, faster predicate pushdown
```

## Storage Structure

```
s3://bucket/                           # Object storage (S3/MinIO/OSS)
└── mydb/                              # catalog
    └── default/                       # schema
        └── cpu/                       # table
            └── date=2024-01-01/       # partition (date-based)
                ├── L0_1704067200000.parquet   # Level 0 (raw flush)
                ├── L0_1704070800000.parquet   # Level 0 (raw flush)
                ├── L0_1704074400000.parquet   # Level 0 (raw flush)
                └── L1_1704153600000.parquet   # Level 1 (compacted)
```

**File Naming Convention:**
- `L{level}_{timestamp_ms}.parquet`
- Level 0: Raw writes from flush
- Level 1+: Compacted files (fewer, larger)

## Error Handling

### Router-Level Errors

| Error | Handling |
|-------|----------|
| No active Data nodes | Return 503 Service Unavailable |
| Parse error | Return 400 Bad Request |
| Partial failure | Return success for written points, error for failed |

### Data Node-Level Errors

| Error | Handling |
|-------|----------|
| Cache full | Return backpressure error, client retries |
| Parquet write failure | Retry with exponential backoff |
| Meta connection lost | Queue file registrations, retry on reconnect |

### Meta Service Errors

| Error | Handling |
|-------|----------|
| Lock acquisition timeout | Router retries with different node |
| Stale fencing token | Data node aborts write, refreshes token |
| Raft leader change | Client automatically redirects to new leader |

## Performance Characteristics

| Metric | Typical Value |
|--------|---------------|
| Parse throughput | ~500K points/sec/core |
| Cache add latency | ~10μs per batch |
| Flush latency | 50-500ms (depends on data size) |
| gRPC round-trip | 1-5ms (same datacenter) |
| Event propagation | <100ms (Meta → Router) |

## Configuration

```toml
[cache]
max_size_mb = 4096          # In-memory cache size
flush_interval_secs = 60    # Background flush interval

[router]
write_timeout_ms = 30000    # gRPC write timeout
max_batch_size = 10000      # Max points per batch to Data node

[meta]
lock_ttl_ms = 30000         # Distributed lock TTL
event_buffer_size = 10000   # Event stream buffer
```

## See Also

- [Cluster Architecture Overview](architecture-cluster-v2.md)
- [Meta Service Architecture](architecture-meta.md)
- [Compaction Architecture](architecture-compaction-multilevel.md)
- [WAL Design](architecture-wal-design.md)
