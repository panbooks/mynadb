# Meta Node Routing Architecture: Design Comparison

This document compares different metadata routing strategies for MynaDB cluster mode,
analyzing trade-offs between granularity, performance, and complexity.

## Executive Summary

| Strategy | Meta Entries | Best For | Complexity |
|----------|-------------|----------|------------|
| **File-Level** | 60M | N/A (not recommended) | High |
| **Partition-Level** | 3M | Analytics workloads | Medium |
| **Table-Level** (Recommended) | 100K | Time-series monitoring | Low |

**Recommendation**: Table-Level routing for time-series workloads where 95%+ queries
access recent data (today's partition).

---

## Problem Statement

### Scale Requirements

| Metric | Minimum | Maximum |
|--------|---------|---------|
| Tables per database | 10,000 | 100,000 |
| Partitions per table | 7 | 30 |
| Files per partition (before compaction) | 10 | 20 |
| **Total file entries (worst case)** | 700,000 | **60,000,000** |

### Query Access Pattern (Time-Series)

```
Today's partition:     95% of queries  ████████████████████████████████████░░
Last 7 days:           4% of queries   ██░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
Older (8-30 days):     1% of queries   ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

This access pattern is typical for:
- Infrastructure monitoring (Prometheus-style)
- Application metrics
- IoT sensor data
- Log aggregation

---

## Strategy 1: File-Level Routing (Original Design)

### Storage Structure

```
/files/{catalog}/{schema}/{table}/{partition}/{filename} → FileInfo
```

### Data Structure

```rust
pub struct FileInfo {
    pub path: String,           // Full S3 path
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub partition: String,      // "date=2024-01-15"
    pub level: u8,              // Compaction level
    pub size_bytes: u64,
    pub row_count: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub created_at: i64,
    pub created_by: String,     // Node ID
}
```

### Operations

| Operation | Complexity | Description |
|-----------|------------|-------------|
| `register_files(files)` | O(N) | Insert N file entries |
| `remove_files(paths)` | O(M×N) | Scan M files to find N matches |
| `list_files_for_partition()` | O(F) | Scan F files in partition |
| `list_files_for_table()` | O(P×F) | Scan all partitions × files |

### Problems

1. **Scale**: 60M entries at maximum scale
2. **Raft Overhead**: Each file registration = 1 Raft log entry
3. **Snapshot Size**: ~18GB to serialize all file metadata
4. **Scan Performance**: Linear scans over millions of entries
5. **Redundancy**: File metadata already exists in S3 (object metadata)

### When to Use

**Never** - This design doesn't scale for production workloads.

---

## Strategy 2: Partition-Level Routing

### Storage Structure

```
/partitions/{catalog}/{schema}/{table}/{partition} → PartitionMeta
/tables/{catalog}/{schema}/{table}                 → TableSummary (cached)
```

### Data Structure

```rust
pub struct PartitionMeta {
    pub partition_key: String,      // "date=2024-01-15"
    pub owner_node_id: String,      // Which node owns this partition
    pub file_count: u32,
    pub total_bytes: u64,
    pub total_rows: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub max_level: u8,              // Highest compaction level
    pub last_flush_at: i64,
    pub version: u64,               // Optimistic concurrency
}

pub struct TableSummary {
    pub partition_count: u32,
    pub total_bytes: u64,
    pub total_rows: u64,
    pub oldest_partition: String,
    pub newest_partition: String,
}
```

### Operations

| Operation | Complexity | Description |
|-----------|------------|-------------|
| `register_partition()` | O(1) | Insert/update partition entry |
| `update_partition_stats()` | O(1) | Update aggregated stats |
| `get_partition_owner()` | O(1) | Hash lookup |
| `list_partitions_for_table()` | O(P) | Scan P partitions |
| `list_partitions_in_range()` | O(P) | Scan with time filter |

### Scale Analysis

| Metric | Count | Size |
|--------|-------|------|
| Partition entries | 3,000,000 | ~600 MB |
| Table summaries | 100,000 | ~20 MB |
| **Total** | 3,100,000 | **~620 MB** |

### Advantages

1. **20x reduction** from file-level (3M vs 60M entries)
2. **Partition-level load balancing** possible
3. **Time-range pruning** without listing files
4. **Fine-grained failover** (partition granularity)

### Disadvantages

1. **Daily partition growth**: 100K new entries per day (one per table)
2. **Multi-partition queries**: Need multiple lookups or scatter-gather
3. **Cache fragmentation**: Partitions of same table on different nodes
4. **Complexity**: More routing logic required

### When to Use

- **Analytics workloads** where queries span many partitions equally
- **Large partitions** (GB+ per partition) that benefit from distribution
- **Need to horizontally scale a single table's traffic**

---

## Strategy 3: Table-Level Routing (Recommended)

### Storage Structure

```
/tables/{catalog}/{schema}/{table} → TableMeta
```

### Data Structure

```rust
pub struct TableMeta {
    // Identity
    pub catalog: String,
    pub schema: String,
    pub table: String,

    // Routing (primary purpose)
    pub owner_node_id: String,

    // Cached statistics (updated periodically)
    pub partition_count: u32,
    pub total_bytes: u64,
    pub total_rows: u64,
    pub oldest_partition: String,   // "date=2024-01-01"
    pub newest_partition: String,   // "date=2024-01-31"
    pub min_time: i64,
    pub max_time: i64,

    // Metadata
    pub created_at: i64,
    pub last_modified: i64,
    pub version: u64,
}
```

### Operations

| Operation | Complexity | Description |
|-----------|------------|-------------|
| `register_table()` | O(1) | Insert table entry |
| `update_table_stats()` | O(1) | Update aggregated stats |
| `get_table_owner()` | O(1) | Hash lookup (main operation) |
| `list_tables_for_node()` | O(T) | Scan tables for a node |
| `reassign_table()` | O(1) | Change ownership |

### Scale Analysis

| Metric | Count | Size |
|--------|-------|------|
| Table entries | 100,000 | ~20 MB |
| **Total** | 100,000 | **~20 MB** |

### Advantages

1. **600x reduction** from file-level (100K vs 60M entries)
2. **30x reduction** from partition-level (100K vs 3M entries)
3. **Zero daily growth** (new partitions don't create Meta entries)
4. **Single lookup per query** regardless of partition count
5. **Excellent cache locality** (all table data on one node)
6. **Simple routing logic**
7. **Data node autonomy** (manages its own partitions locally)

### Disadvantages

1. **No partition-level load balancing** for single table
2. **Hot table problem** if one table dominates traffic
3. **Coarse failover granularity** (entire table moves on failure)

### Mitigations for Hot Tables

```rust
// Option 1: Application-level sharding
// Split by tag value at write time
cpu_region_us  → data-1
cpu_region_eu  → data-2
cpu_region_ap  → data-3

// Option 2: Load-aware assignment
// Assign new tables to least-loaded node
fn assign_table(nodes: &[Node]) -> &Node {
    nodes.iter()
        .min_by_key(|n| n.table_count * n.avg_qps)
        .unwrap()
}

// Option 3: Manual reassignment
// Move hot tables to dedicated nodes
POST /meta/tables/mydb/default/hot_table/reassign
{ "new_owner": "data-dedicated-1" }
```

### When to Use

- **Time-series monitoring** (95%+ queries for recent data)
- **Many small-to-medium tables** (typical microservices metrics)
- **Date-partitioned data** where today is always hottest
- **Simplicity is valued** over fine-grained control

---

## Detailed Comparison

### Meta Storage Requirements

| Strategy | Entries at Scale | Storage Size | Growth/Day |
|----------|-----------------|--------------|------------|
| File-Level | 60,000,000 | ~18 GB | +N files |
| Partition-Level | 3,000,000 | ~600 MB | +100K |
| **Table-Level** | **100,000** | **~20 MB** | **~0** |

### Query Routing Performance

| Query Type | File-Level | Partition-Level | Table-Level |
|------------|------------|-----------------|-------------|
| Today's data | Scan partition files | 1 lookup | **1 lookup** |
| Last 7 days | Scan 7 partitions | 7 lookups | **1 lookup** |
| Last 30 days | Scan 30 partitions | 30 lookups | **1 lookup** |
| Full table scan | Scan all files | P lookups | **1 lookup** |

### Cache Efficiency

```
Table-Level (GOOD):
┌─────────────────────────────────────────────────────┐
│  Data Node "data-1" owns table "cpu"                │
│                                                     │
│  Memtable (today)  ████████████  HOT - always warm  │
│  Yesterday L1      ███████       WARM - likely hit  │
│  2 days ago L2     ████          WARM - often hit   │
│  Older partitions  ██            COLD - rare access │
│                                                     │
│  Sequential access pattern = excellent prefetch     │
└─────────────────────────────────────────────────────┘

Partition-Level (POOR for time-series):
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  data-1     │  │  data-2     │  │  data-3     │
│  cpu/today  │  │  cpu/day-1  │  │  cpu/day-2  │
│  (HOT)      │  │  (COLD)     │  │  (COLD)     │
└─────────────┘  └─────────────┘  └─────────────┘
      ↑
  95% of queries only hit data-1 anyway
  Distribution provides no benefit
  But adds network hops for multi-day queries
```

### Raft Log Impact

| Operation | File-Level | Partition-Level | Table-Level |
|-----------|------------|-----------------|-------------|
| Flush 20 files | 20 log entries | 1 log entry | 0-1 log entries* |
| New partition | 0 | 1 log entry | 0 log entries |
| Compaction (merge 10→1) | 11 entries | 1 entry | 0-1 entries* |

*Table-level only updates Meta periodically for stats sync, not every flush

### Failure Recovery

| Aspect | File-Level | Partition-Level | Table-Level |
|--------|------------|-----------------|-------------|
| Failover unit | File | Partition | Table |
| Recovery granularity | Fine | Medium | Coarse |
| Data to move | Individual files | One partition | All partitions |
| Typical recovery size | ~50MB | ~500MB | ~5GB |
| Recovery complexity | High | Medium | Low |

---

## Query Flow Comparison

### File-Level (Original)

```
Client → Router → Meta.list_files(table, partition) → Router → Data Node
                        ↓
              Scan 20 file entries
              Return file list
              (repeated per partition)
```

### Partition-Level

```
Client → Router → Meta.get_partition_owner(table, partition) → Data Node
                        ↓                                          ↓
              1 lookup per partition                    S3.list(partition/)
              (7-30 lookups for range)                  Local file discovery
```

### Table-Level (Recommended)

```
Client → Router → Meta.get_table_owner(table) → Data Node
                        ↓                           ↓
              1 lookup total              Local partition knowledge
              Cached at Router            S3.list() with local cache
```

---

## Implementation Complexity

### File-Level

```rust
// Many operations, all O(N) or worse
register_files(files: Vec<FileInfo>)
remove_files(paths: Vec<String>)
list_files_for_partition(catalog, schema, table, partition)
list_files_for_table(catalog, schema, table)
list_files_by_level(catalog, schema, table, partition, level)
list_files_by_time_range(catalog, schema, table, min, max)
get_file(path)
// ... more operations
```

### Partition-Level

```rust
// Moderate number of operations
register_partition(partition: PartitionMeta)
update_partition_stats(partition_key, delta)
get_partition_owner(catalog, schema, table, partition)
list_partitions(catalog, schema, table)
list_partitions_in_range(catalog, schema, table, start, end)
reassign_partition(partition_key, new_owner)
```

### Table-Level

```rust
// Minimal operations (simplest)
register_table(table: TableMeta)
update_table_stats(table_key, stats_delta)
get_table_owner(catalog, schema, table)  // Main operation
list_tables_for_node(node_id)
reassign_table(table_key, new_owner)
```

---

## Migration Path

### From File-Level to Table-Level

1. **Phase 1**: Add table-level APIs (backward compatible)
   - New `TableMeta` storage
   - New commands: `RegisterTable`, `UpdateTableStats`
   - Keep file-level APIs working

2. **Phase 2**: Update Data Nodes
   - Switch flush callback to table-level updates
   - Implement local partition/file discovery
   - Stop calling file-level Meta APIs

3. **Phase 3**: Deprecate file-level APIs
   - Log warnings on file-level API usage
   - Document migration path

4. **Phase 4**: Remove file-level storage
   - Delete file entries from Meta
   - Remove file-level code

---

## Implementation: Partition Files Cache

Data nodes use a local cache for S3 file listings to avoid repeated LIST operations.

### Cache Design

```
src/query/cache/partition_files_cache.rs
```

```rust
pub struct PartitionFilesCache {
    /// Cache entries: partition_key -> cached file list
    cache: DashMap<String, CachedPartitionFiles>,
    /// Storage backend for LIST operations
    storage: Arc<Storage>,
    /// Default TTL (30 seconds)
    default_ttl: Duration,
}

pub struct CachedFileInfo {
    pub path: String,      // File path
    pub level: u8,         // Compaction level (L0, L1, L2)
    pub size_bytes: Option<u64>,
}
```

### Flow

```
Query arrives at Data Node
        ↓
PartitionFilesCache.list_files(catalog, schema, table, partition)
        ↓
    ┌───────────────────┐
    │  Cache hit?       │
    │  TTL not expired? │
    └─────────┬─────────┘
              │
        ┌─────┴─────┐
        ▼           ▼
    Yes (hit)   No (miss)
        │           │
        │    Storage.list(prefix)
        │           │
        │    Cache result (30s TTL)
        ▼           ▼
    Return cached file list
```

### Configuration

```toml
[partition_files_cache]
enabled = true      # Enable caching
ttl_secs = 30       # Cache TTL (short to see new files)
max_entries = 10000 # Max partitions to cache
```

### Cache Invalidation

- **TTL-based**: Entries auto-expire after `ttl_secs`
- **After flush**: Call `invalidate(catalog, schema, table, partition)`
- **After compaction**: Call `invalidate_table(catalog, schema, table)`

### Integration Points

1. **Query Engine**: Uses cache to list files for table providers
2. **Compactor**: Invalidates cache after compaction
3. **Flush callback**: Invalidates cache for flushed partitions

---

## Conclusion

For MynaDB's time-series use case with:
- 10K-100K tables
- Date partitioning (1 partition/day)
- 95%+ queries accessing today's data

**Table-Level Routing is the clear winner**:

| Benefit | Impact |
|---------|--------|
| 600x fewer Meta entries | 20MB vs 18GB storage |
| Zero daily growth | No partition entry creation |
| Single routing lookup | Simpler, faster queries |
| Excellent cache locality | Better query performance |
| Simpler implementation | Easier to maintain |
| Data node autonomy | Reduced Meta dependency |

The only scenario where Partition-Level would be preferred is for analytics workloads
with uniform access across all time ranges, which is not the typical TSDB pattern.
