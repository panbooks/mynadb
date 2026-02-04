# Proposal: Partition-Aware Query Routing via Path Restructure

**Author:** MynaDB Team
**Created:** 2026-02-04
**Status:** Draft
**Target Version:** v2.x

---

## Executive Summary

This proposal addresses performance defects in MynaDB's table-level routing mechanism. The current design routes queries to ALL nodes that have ever written to a table, regardless of which date partitions each node actually holds.

**Key Finding:** Query routing cannot prune by date partition, resulting in N× query cost where N = number of historical table owners.

**Proposed Solution:** Restructure the object store path from `{node_id}/{catalog}/...` to `{catalog}/.../date={date}/{node_id}/`, enabling partition discovery via simple S3 LIST operations.

**Impact:**
- Query fan-out reduced by 60-90% for typical queries
- **Zero Meta storage overhead** (vs 16.8MB+ for alternatives)
- True compute-storage separation
- Simplified scale-in/out operations

---

## Table of Contents

1. [Background](#1-background)
2. [Problem Statement](#2-problem-statement)
3. [Solution Overview](#3-solution-overview)
4. [Path Structure Comparison](#4-path-structure-comparison)
5. [Detailed Design](#5-detailed-design)
6. [Cache Affinity Analysis](#6-cache-affinity-analysis)
7. [Migration Strategy](#7-migration-strategy)
8. [Alternative Approaches Considered](#8-alternative-approaches-considered)
9. [Implementation Plan](#9-implementation-plan)
10. [Appendix](#appendix)

---

## 1. Background

### 1.1 Current Architecture

MynaDB uses **table-level routing** for distributed query execution:

- Tables are partitioned by date (`date=YYYY-MM-DD`)
- Meta service stores `TableMeta` with table → node ownership mapping
- All partitions of a table are assumed to be co-located on the same node(s)
- Router queries all owner nodes for any query on a table

### 1.2 Current Path Structure

```
s3://bucket/
└── {node_id}/
    └── {catalog}/
        └── {schema}/
            └── {table}/
                └── date={YYYY-MM-DD}/
                    └── *.parquet
```

### 1.3 Current TableMeta

```rust
struct TableMeta {
    catalog: String,
    schema: String,
    table: String,
    owner_node_ids: Vec<String>,    // Grows unbounded
    legacy_prefixes: Vec<String>,   // Retired nodes
    // ... statistics
}
```

---

## 2. Problem Statement

### 2.1 The Core Issue

The current design assumes all partitions of a table reside on the same node(s). However, in practice:

1. **Scale-out:** New nodes receive writes for existing tables
2. **Failover:** Writes reroute to backup nodes during outages
3. **Distributed writes:** Big tables intentionally spread across multiple nodes

Result: Different date partitions of the same table may exist on different nodes.

### 2.2 Query Routing Limitation

**Current behavior:** Router queries ALL nodes in `owner_node_ids` for ANY query.

```
Query: SELECT * FROM cpu WHERE date = '2024-01-15'

TableMeta.owner_node_ids = [A, B, C, D, E]

Current routing: Query all 5 nodes
Optimal routing: Query only node(s) with date=01-15 (likely 1-2 nodes)
```

### 2.3 Impact Analysis

| Scenario | Data Location | Nodes Queried | Wasted Queries |
|----------|---------------|---------------|----------------|
| 5 owners, query 1 day | 1-2 nodes | 5 | 60-80% |
| 10 owners, query 1 day | 1-2 nodes | 10 | 80-90% |
| 20 owners, query 1 week | 3-5 nodes | 20 | 75-85% |

### 2.4 Identified Defects

| Defect | Severity | Description |
|--------|----------|-------------|
| Unbounded owner_node_ids | Medium | Grows with every failover/scale event |
| No date-based pruning | **High** | All owners queried regardless of date filter |
| Complex scale-in | Medium | Requires legacy_prefixes, hash assignment |
| No cross-node compaction | Low | Same partition fragmented forever |

---

## 3. Solution Overview

### 3.1 Proposed Path Structure

```
s3://bucket/
└── {catalog}/
    └── {schema}/
        └── {table}/
            └── date={YYYY-MM-DD}/
                ├── {node-A}/
                │   └── *.parquet
                └── {node-B}/
                    └── *.parquet
```

**Key Change:** Move `{node_id}` from path prefix to partition subdirectory.

### 3.2 How It Solves the Problem

```
Query: SELECT * FROM cpu WHERE date = '2024-01-15'

1. LIST s3://bucket/mydb/default/cpu/date=2024-01-15/
   → CommonPrefixes: [node-A/, node-B/]

2. Route:
   - node-A/ files → Node A (cache hit)
   - node-B/ files → Node B (cache hit)

3. No Meta lookup needed for routing!
```

### 3.3 Benefits Summary

| Benefit | Current (Node-ID First) | Proposed (Node-ID Last) |
|---------|------------------------|-------------------------|
| Meta storage for routing | 16.8MB+ | **0** |
| Retention complexity | O(nodes × dates) | **O(dates)** |
| Scale-in complexity | High (legacy_prefixes) | **None** |
| Cross-node compaction | Impossible | **Possible** |
| Cache affinity | Strong | **Strong** (with smart routing) |

---

## 4. Path Structure Comparison

### 4.1 Dimension Analysis

#### 4.1.1 Retention Logic

**Node-ID First (Current):**
```
To delete date=2024-01-01 for table cpu:

For each node in [A, B, C, D, E, ...legacy...]:
    DELETE s3://bucket/{node}/mydb/default/cpu/date=2024-01-01/*

Cost: O(nodes) operations per date
      Requires Meta lookup for all owner_node_ids + legacy_prefixes
```

**Node-ID Last (Proposed):**
```
To delete date=2024-01-01 for table cpu:

DELETE s3://bucket/mydb/default/cpu/date=2024-01-01/*

Cost: O(1) operation per date
      No Meta lookup needed
```

**Winner: Node-ID Last**

---

#### 4.1.2 Route Simplicity

**Node-ID First (Current):**
```
Query routing flow:

1. Parse SQL → extract table reference
2. Lookup TableMeta from Meta service
3. Get owner_node_ids = [A, B, C, D, E]
4. Get legacy_prefixes = [X, Y, Z]
5. Without node_coverage: query ALL nodes (inefficient)
   With node_coverage: need 16.8MB+ storage
6. Route to determined nodes
```

**Node-ID Last (Proposed):**
```
Query routing flow:

1. Parse SQL → extract table reference, date filter
2. LIST s3://bucket/mydb/default/cpu/date=2024-01-15/
   → Response: CommonPrefixes = [node-A/, node-B/]
3. Route to A and B

No Meta storage needed!
```

**Winner: Node-ID Last**

---

#### 4.1.3 Query Performance

**Node-ID First (Current):**
```
Latency breakdown:
- Meta lookup: ~5-10ms (cached) or ~50ms (remote)
- No S3 LIST needed (path known from Meta)
- Query execution: varies

Total routing overhead: ~5-50ms
```

**Node-ID Last (Proposed):**
```
Latency breakdown:
- S3 LIST: ~50-100ms (first time)
- S3 LIST: ~0ms (cached, TTL 1-5 min)
- Query execution: varies

Total routing overhead: ~0-100ms (amortized ~5ms with caching)
```

**Winner: Tie** (with proper caching)

---

#### 4.1.4 Parallel Query

**Node-ID First (Current):**
```
Range query: WHERE date BETWEEN '01-01' AND '01-31'

Without node_coverage:
  → Query ALL owner nodes for ALL dates
  → Each node scans dates it might have
  → Redundant scans, no pruning

With node_coverage:
  → Know which nodes have which dates
  → Better parallelism
  → But requires 16.8MB+ Meta storage
```

**Node-ID Last (Proposed):**
```
Range query: WHERE date BETWEEN '01-01' AND '01-31'

1. LIST each date partition (parallel):
   date=01-01/ → [node-A/]
   date=01-15/ → [node-A/, node-B/]
   date=01-31/ → [node-A/, node-B/, node-C/]

2. Build precise file list per node:
   Node A: files from 01-01 to 01-31
   Node B: files from 01-15 to 01-31
   Node C: files from 01-31 only

3. Send each node exactly its files
   → Optimal parallelism, zero redundant work
```

**Winner: Node-ID Last**

---

#### 4.1.5 Cache Affinity (Critical)

**Node-ID First (Current):**
```
Write: Node A writes → /node-A/.../file.parquet
       Node A caches file locally

Query: Routed to Node A (owner)
       Node A reads from local cache

Cache hit rate: HIGH
Latency: ~0.1ms (local SSD) vs ~50-100ms (S3)
```

**Node-ID Last (Naive):**
```
Write: Node A writes → /.../node-A/file.parquet
       Node A caches file locally

Query: Could be routed to ANY node
       If routed to Node B → cache miss!

Cache hit rate: LOW (PROBLEM!)
```

**Node-ID Last (Smart Routing):**
```
Write: Node A writes → /.../node-A/file.parquet
       Node A caches file locally

Query:
  1. LIST partition → [node-A/, node-B/]
  2. Route node-A/ files to Node A (writer)
  3. Route node-B/ files to Node B (writer)

Cache hit rate: HIGH (writer = reader preserved!)
```

**Winner: Tie** (with smart routing implementation)

---

#### 4.1.6 Compaction

**Node-ID First (Current):**
```
Partition structure:
  /node-A/mydb/cpu/date=01-15/L0_001.parquet
  /node-B/mydb/cpu/date=01-15/L0_002.parquet

Compaction:
  - Node A compacts only /node-A/... files
  - Node B compacts only /node-B/... files
  - Same partition data FRAGMENTED FOREVER
  - Cannot merge cross-node
```

**Node-ID Last (Proposed):**
```
Partition structure:
  /mydb/cpu/date=01-15/node-A/L0_001.parquet
  /mydb/cpu/date=01-15/node-B/L0_002.parquet

Compaction options:

  Option 1 (per-node): Same as current

  Option 2 (cross-node merge):
    Compact ALL files in partition →
    /mydb/cpu/date=01-15/merged/L1_001.parquet

    Result: Unified partition, optimal compression
```

**Winner: Node-ID Last**

---

#### 4.1.7 Scale-In (Node Retirement)

**Node-ID First (Current):**
```
Node B retires:

1. Data remains at /node-B/... (cannot move easily)
2. Must update Meta:
   - Remove B from owner_node_ids
   - Add B to legacy_prefixes
3. Router must:
   - Check legacy_prefixes
   - Assign legacy prefixes to active nodes via hash
   - Hash changes when cluster size changes!
4. Complex, error-prone
```

**Node-ID Last (Proposed):**
```
Node B retires:

1. Data remains at /.../node-B/ subdirectories
2. No Meta update needed!
3. Query routing:
   - LIST partition → still discovers node-B/
   - Node B offline → route B's files to any active node
4. Over time:
   - Cross-node compaction merges B's data
   - B's subdirectories naturally disappear
```

**Winner: Node-ID Last**

---

#### 4.1.8 Scale-Out (Node Addition)

**Node-ID First (Current):**
```
Node C joins:

1. C starts receiving writes for some tables
2. C writes to /node-C/...
3. Must update Meta: add C to owner_node_ids
4. owner_node_ids grows unbounded over time
```

**Node-ID Last (Proposed):**
```
Node C joins:

1. C starts receiving writes for some tables
2. C writes to /.../node-C/
3. No Meta update needed
4. Next query LIST discovers node-C/ automatically
```

**Winner: Node-ID Last**

---

#### 4.1.9 Failure Handling

**Node-ID First (Current):**
```
Node A fails during query:

1. Router sent query to A (based on Meta ownership)
2. A times out
3. Retry options:
   - Need to find other nodes with same partition
   - Requires node_coverage or broadcast to ALL nodes
4. Failover node must fetch from S3 (cache miss)
5. Complex failover logic
```

**Node-ID Last (Proposed):**
```
Node A fails during query:

1. Router knows exact files: /date=01-15/node-A/file.parquet
2. A times out
3. Simple retry: send A's files to any active node (B or C)
4. B fetches from S3 (cache miss, but works)
5. True compute-storage separation
```

**Winner: Node-ID Last**

---

#### 4.1.10 Multi-Tenant Isolation

**Node-ID First (Current):**
```
S3 path: /node-A/tenant-X/...
         /node-A/tenant-Y/...

IAM by node: Allow s3://bucket/node-A/*
  → Node A sees ALL tenants (bad for isolation)

IAM by tenant: Complex path patterns needed
```

**Node-ID Last (Proposed):**
```
S3 path: /tenant-X/.../node-A/
         /tenant-Y/.../node-B/

IAM by tenant: Allow s3://bucket/tenant-X/*
  → Clean tenant isolation
  → Tenant data under single prefix
```

**Winner: Node-ID Last**

---

#### 4.1.11 Object Store Listing Efficiency

**Node-ID First (Current):**
```
List all tables for catalog mydb:

For each node in cluster:
    LIST s3://bucket/{node}/mydb/

Cost: O(nodes) LIST operations
```

**Node-ID Last (Proposed):**
```
List all tables for catalog mydb:

LIST s3://bucket/mydb/

Cost: O(1) LIST operation
```

**Winner: Node-ID Last**

---

### 4.2 Summary Comparison Table

| Dimension | Node-ID First | Node-ID Last | Winner |
|-----------|---------------|--------------|--------|
| Retention logic | O(nodes × dates) | O(dates) | **Last** |
| Route simplicity | Complex (Meta) | Simple (LIST) | **Last** |
| Query performance | No LIST latency | LIST (cacheable) | Tie |
| Parallel query | Needs node_coverage | Natural from LIST | **Last** |
| **Cache affinity** | Strong | Strong (smart routing) | **Tie** |
| Compaction | Per-node only | Cross-node possible | **Last** |
| Scale-in | Complex (legacy) | Natural (LIST) | **Last** |
| Scale-out | Meta grows | Natural discovery | **Last** |
| Failure handling | Complex failover | Any node reads | **Last** |
| Write path | Simple | Simple | Tie |
| Multi-tenant | Node isolation | Tenant isolation | **Last** |
| Listing efficiency | O(nodes) | O(1) | **Last** |
| **Meta storage** | **16.8MB+** | **0** | **Last** |

**Final Score: Node-ID Last wins 9/13, Tie 3/13, Node-ID First wins 1/13**

---

## 5. Detailed Design

### 5.1 Path Structure Specification

```
Old path format:
  s3://{bucket}/{node_id}/{catalog}/{schema}/{table}/date={YYYY-MM-DD}/{file}.parquet

New path format:
  s3://{bucket}/{catalog}/{schema}/{table}/date={YYYY-MM-DD}/{node_id}/{file}.parquet
```

### 5.2 Query Routing Algorithm

```rust
/// Discover nodes with data for given partition
async fn discover_partition_nodes(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
) -> Vec<String> {
    let prefix = format!("{}/{}/{}/date={}/", catalog, schema, table, date);

    // Check cache first
    if let Some(nodes) = self.partition_cache.get(&prefix) {
        return nodes;
    }

    // LIST from object store
    let result = self.object_store
        .list_with_delimiter(&prefix)
        .await?;

    // Extract node_ids from CommonPrefixes
    let nodes: Vec<String> = result
        .common_prefixes
        .iter()
        .filter_map(|p| extract_node_id(p))
        .collect();

    // Cache result
    self.partition_cache.insert(prefix, nodes.clone(), TTL_SECONDS);

    nodes
}

/// Route query with smart affinity
async fn route_query(
    &self,
    catalog: &str,
    table: &TableRef,
    date_range: &DateRange,
) -> Vec<QueryTask> {
    let mut tasks = Vec::new();

    for date in date_range.iter_dates() {
        let nodes = self.discover_partition_nodes(catalog, &table.schema, &table.name, &date).await;

        for node_id in nodes {
            // Route to writer node for cache affinity
            if self.is_node_available(&node_id) {
                tasks.push(QueryTask {
                    target_node: node_id.clone(),
                    files: self.list_node_files(catalog, table, &date, &node_id).await,
                });
            } else {
                // Failover: any available node can read
                let fallback = self.select_fallback_node();
                tasks.push(QueryTask {
                    target_node: fallback,
                    files: self.list_node_files(catalog, table, &date, &node_id).await,
                });
            }
        }
    }

    // Merge tasks by target node
    merge_tasks_by_node(tasks)
}
```

### 5.3 Write Path Changes

```rust
/// Generate write path (new format)
fn get_write_path(
    node_id: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
    file_name: &str,
) -> String {
    // Old: {node_id}/{catalog}/{schema}/{table}/date={date}/{file}
    // New: {catalog}/{schema}/{table}/date={date}/{node_id}/{file}
    format!(
        "{}/{}/{}/date={}/{}/{}",
        catalog, schema, table, date, node_id, file_name
    )
}
```

### 5.4 Retention Manager Changes

```rust
/// Delete partitions older than retention days (simplified)
async fn enforce_retention(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    retention_days: u32,
) -> Result<RetentionStats> {
    let cutoff_date = Utc::now() - Duration::days(retention_days as i64);
    let table_prefix = format!("{}/{}/{}/", catalog, schema, table);

    // List all date partitions (single LIST)
    let partitions = self.object_store
        .list_with_delimiter(&table_prefix)
        .await?;

    let mut deleted_partitions = 0;
    let mut deleted_bytes = 0;

    for partition_prefix in partitions.common_prefixes {
        let date = extract_date_from_prefix(&partition_prefix)?;

        if date < cutoff_date {
            // Delete entire partition (all node subdirs)
            let stats = self.object_store
                .delete_prefix(&partition_prefix)
                .await?;

            deleted_partitions += 1;
            deleted_bytes += stats.bytes_deleted;
        }
    }

    Ok(RetentionStats { deleted_partitions, deleted_bytes })
}
```

### 5.5 Compaction Changes

```rust
/// Cross-node compaction (new capability)
async fn compact_partition_cross_node(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
) -> Result<CompactionStats> {
    let partition_prefix = format!("{}/{}/{}/date={}/", catalog, schema, table, date);

    // List all node subdirectories
    let node_dirs = self.object_store
        .list_with_delimiter(&partition_prefix)
        .await?;

    // Collect all files from all nodes
    let mut all_files = Vec::new();
    for node_dir in node_dirs.common_prefixes {
        let files = self.object_store.list(&node_dir).await?;
        all_files.extend(files);
    }

    if all_files.len() < self.config.min_files_for_compaction {
        return Ok(CompactionStats::skipped());
    }

    // Compact all files into merged output
    let output_path = format!("{}merged/", partition_prefix);
    let stats = self.compact_files(all_files, &output_path).await?;

    // Delete original node subdirectories
    for node_dir in node_dirs.common_prefixes {
        self.object_store.delete_prefix(&node_dir).await?;
    }

    Ok(stats)
}
```

---

## 6. Cache Affinity Analysis

### 6.1 Why Cache Affinity Matters

```
Local SSD read:  ~0.1ms
S3 fetch:        ~50-100ms
Difference:      500-1000×
```

For a query scanning 100 files:
- Cache hit: 100 × 0.1ms = 10ms
- Cache miss: 100 × 50ms = 5000ms (5 seconds!)

### 6.2 Smart Routing Preserves Affinity

```
Write flow:
  1. Node A receives write for partition P
  2. A writes to s3://bucket/.../date=P/node-A/file.parquet
  3. A caches file locally

Query flow:
  1. Router LISTs partition → discovers [node-A/, node-B/]
  2. Router routes:
     - node-A/ files → Node A (writer = reader, cache HIT)
     - node-B/ files → Node B (writer = reader, cache HIT)
  3. Both nodes read from local cache

Result: Cache affinity preserved!
```

### 6.3 Failover with Graceful Degradation

```
Node A fails:

  1. Router discovers [node-A/, node-B/] from LIST
  2. Router detects A is unavailable
  3. Router assigns A's files to Node C (fallback)
  4. Node C fetches from S3 (cache miss, ~50ms per file)
  5. Query completes (slower, but works)
  6. Node C caches fetched files
  7. Next query to same partition → C has cache hit

Graceful degradation: Works correctly, just slower on first failover query
```

### 6.4 Cache Warm-Up Strategy

```rust
/// Pre-warm cache for frequently accessed partitions
async fn warm_cache(&self, table: &str, recent_days: u32) {
    let today = Utc::now().date();

    for day_offset in 0..recent_days {
        let date = today - Duration::days(day_offset as i64);
        let nodes = self.discover_partition_nodes(table, &date).await;

        for node_id in nodes {
            if self.is_local_node(&node_id) {
                // Pre-fetch our own files into cache
                let files = self.list_node_files(table, &date, &node_id).await;
                for file in files {
                    self.file_cache.prefetch(&file).await;
                }
            }
        }
    }
}
```

---

## 7. Migration Strategy

### 7.1 Dual-Path Support (Phase 1)

Support both path formats during transition:

```rust
impl QueryEngine {
    async fn discover_files(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Vec<ParquetFile> {
        let mut files = Vec::new();

        // Try new path structure first (preferred)
        let new_prefix = format!("{}/{}/{}/date={}/", catalog, schema, table, date);
        if let Ok(new_files) = self.list_partition_files_new(&new_prefix).await {
            files.extend(new_files);
        }

        // Also check old path structure (for unmigrated data)
        if self.config.support_legacy_paths {
            for node_id in self.get_known_node_ids().await {
                let old_prefix = format!("{}/{}/{}/{}/date={}/",
                    node_id, catalog, schema, table, date);
                if let Ok(old_files) = self.list_files(&old_prefix).await {
                    files.extend(old_files);
                }
            }
        }

        files
    }
}
```

### 7.2 Write Path Migration (Phase 2)

New writes use new path format:

```rust
impl ParquetWriter {
    fn get_output_path(&self, partition: &Partition) -> String {
        if self.config.use_new_path_format {
            // New: catalog/schema/table/date/node_id/file
            format!(
                "{}/{}/{}/date={}/{}/{}",
                partition.catalog,
                partition.schema,
                partition.table,
                partition.date,
                self.node_id,
                generate_file_name()
            )
        } else {
            // Old: node_id/catalog/schema/table/date/file
            format!(
                "{}/{}/{}/{}/date={}/{}",
                self.node_id,
                partition.catalog,
                partition.schema,
                partition.table,
                partition.date,
                generate_file_name()
            )
        }
    }
}
```

### 7.3 Background Migration (Phase 3)

Gradually migrate old data:

```rust
/// Background job to migrate old path format to new
async fn migrate_partition(
    &self,
    node_id: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
) -> Result<MigrationStats> {
    let old_prefix = format!("{}/{}/{}/{}/date={}/",
        node_id, catalog, schema, table, date);
    let new_prefix = format!("{}/{}/{}/date={}/{}/",
        catalog, schema, table, date, node_id);

    let files = self.object_store.list(&old_prefix).await?;
    let mut migrated = 0;

    for file in files {
        let new_path = file.path.replace(&old_prefix, &new_prefix);

        // Copy to new location
        self.object_store.copy(&file.path, &new_path).await?;
        migrated += 1;
    }

    // Verify and delete old files
    if self.verify_migration(&old_prefix, &new_prefix).await? {
        self.object_store.delete_prefix(&old_prefix).await?;
    }

    Ok(MigrationStats { files_migrated: migrated })
}
```

### 7.4 Compaction-Based Migration (Alternative)

Let natural compaction migrate data:

```rust
/// Compaction automatically migrates to new path format
async fn compact_with_migration(&self, partition: &Partition) -> Result<()> {
    // Read from both old and new paths
    let files = self.discover_files_all_paths(partition).await?;

    // Write compacted output to NEW path format only
    let output_path = format!(
        "{}/{}/{}/date={}/merged/",
        partition.catalog, partition.schema, partition.table, partition.date
    );

    self.compact_files(files, &output_path).await?;

    // Delete old files (both old and new format sources)
    for file in files {
        self.object_store.delete(&file.path).await?;
    }

    Ok(())
}
```

### 7.5 Migration Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| Phase 1 | Week 1-2 | Deploy dual-path read support |
| Phase 2 | Week 3 | Enable new path for writes |
| Phase 3a | Week 4+ | Background migration job |
| Phase 3b | Ongoing | Compaction-based migration |
| Phase 4 | Month 2+ | Disable legacy path support |

---

## 8. Alternative Approaches Considered

### 8.1 Node → Date Range Array

Store partition coverage in Meta:

```rust
struct TableMeta {
    node_coverage: Vec<NodeCoverage>,
}

struct NodeCoverage {
    node_id: String,
    ranges: Vec<DateRange>,
}
```

**Storage:** 16.8 MB for 100K tables

**Rejected because:**
- Significant Meta storage overhead
- Must keep coverage in sync with actual data
- Complex update logic on every write
- Path restructure achieves same result with zero storage

### 8.2 Per-Partition Map

Store node list per partition in Meta:

```rust
struct TableMeta {
    partition_nodes: BTreeMap<String, Vec<String>>,  // date -> [node_ids]
}
```

**Storage:** 126 MB for 100K tables (30 days each)

**Rejected because:**
- Very high storage overhead
- Grows linearly with partition count
- Meta becomes bottleneck

### 8.3 Keep Current Design + Add node_coverage

Add coverage tracking without changing paths.

**Rejected because:**
- Still requires 16.8MB+ Meta storage
- Doesn't solve scale-in complexity
- Doesn't enable cross-node compaction
- Path restructure is strictly better

---

## 9. Implementation Plan

### 9.1 Milestones

| Phase | Description | Duration | Risk |
|-------|-------------|----------|------|
| 1 | Dual-path read support | 1 week | Low |
| 2 | New path write support | 1 week | Low |
| 3 | Query routing with LIST | 1 week | Medium |
| 4 | Retention manager update | 3 days | Low |
| 5 | Compaction changes | 1 week | Medium |
| 6 | Migration tooling | 1 week | Low |
| 7 | Testing & validation | 1 week | Low |

**Total: ~6 weeks**

### 9.2 Files to Modify

| File | Changes |
|------|---------|
| `src/storage/parquet_writer.rs` | New path format |
| `src/query/query_engine/discovery.rs` | Dual-path discovery |
| `src/router/routing/query_coordinator.rs` | LIST-based routing |
| `src/router/routing/shard_cache.rs` | Partition node cache |
| `src/jobs/retention_manager.rs` | Simplified retention |
| `src/jobs/compactor/` | Cross-node compaction |
| `src/config.rs` | Feature flags |

### 9.3 Feature Flags

```toml
[storage]
# Enable new path format for writes
use_new_path_format = true

# Support reading from old path format (migration)
support_legacy_paths = true

# Enable cross-node compaction
cross_node_compaction = false

[query]
# Use LIST-based partition discovery
list_based_routing = true

# Cache TTL for partition node discovery
partition_cache_ttl_secs = 60
```

### 9.4 Testing Plan

| Test Category | Description |
|---------------|-------------|
| Unit tests | Path generation, LIST parsing |
| Integration tests | Dual-path read, migration |
| Performance tests | LIST latency, cache hit rate |
| Scale tests | 100K tables, 30 partitions each |
| Chaos tests | Node failure during query/write |
| Migration tests | Old → new path conversion |

---

## 10. Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Query fan-out | N nodes | 1-3 nodes | Router logs |
| Meta storage | 16.8MB+ | 0 | Meta DB size |
| P99 query latency | Baseline | -30% | Prometheus |
| Retention time | O(n×d) | O(d) | Job duration |
| Cache hit rate | ~80% | >90% | Cache metrics |

---

## Appendix

### A.1 Example Query Flow (New Design)

```
Query: SELECT avg(value) FROM cpu
       WHERE date BETWEEN '2024-01-10' AND '2024-01-15'
       GROUP BY host

Router:
  1. Parse SQL → table=cpu, dates=[01-10, 01-11, 01-12, 01-13, 01-14, 01-15]

  2. Parallel LIST for each date:
     LIST /mydb/default/cpu/date=2024-01-10/ → [node-A/]
     LIST /mydb/default/cpu/date=2024-01-11/ → [node-A/]
     LIST /mydb/default/cpu/date=2024-01-12/ → [node-A/, node-B/]
     LIST /mydb/default/cpu/date=2024-01-13/ → [node-B/]
     LIST /mydb/default/cpu/date=2024-01-14/ → [node-B/]
     LIST /mydb/default/cpu/date=2024-01-15/ → [node-B/, node-C/]

  3. Build query tasks:
     Node A: scan dates 01-10, 01-11, 01-12 (A's files only)
     Node B: scan dates 01-12, 01-13, 01-14, 01-15 (B's files only)
     Node C: scan date 01-15 (C's files only)

  4. Execute in parallel:
     A: reads from local cache (wrote these files)
     B: reads from local cache (wrote these files)
     C: reads from local cache (wrote these files)

  5. Aggregate partial results at Router

  6. Return final result

Nodes queried: 3 (vs ALL owners with current design)
Cache hit rate: 100% (writer = reader)
```

### A.2 Scale-In Example (New Design)

```
Before: Cluster has nodes [A, B, C]
        Table cpu has data on all three nodes

Scale-in: Remove Node B

During transition:
  1. B marked as retiring
  2. New writes go to A and C only
  3. B's existing data remains at /.../node-B/ subdirectories
  4. Queries:
     - LIST discovers [node-A/, node-B/, node-C/]
     - B is offline → B's files assigned to A or C
     - A/C fetch B's files from S3 (cache miss, one time)

After compaction:
  1. Cross-node compaction runs on old partitions
  2. Merges A + B + C files → single merged/ directory
  3. B's subdirectories deleted
  4. Future queries only see merged/ (no B references)

No Meta updates required!
No legacy_prefixes needed!
Natural cleanup via compaction!
```

### A.3 Failure Handling Example

```
Scenario: Node A fails mid-query

Current (Node-ID First):
  1. Query routed to A (from Meta ownership)
  2. A times out
  3. Router checks: who else has this partition?
     - Requires node_coverage (16.8MB storage)
     - OR broadcast to all nodes (wasteful)
  4. Complex failover logic

New (Node-ID Last):
  1. Router LIST'd partition → knows exact files:
     /date=01-15/node-A/file1.parquet
     /date=01-15/node-A/file2.parquet
  2. A times out
  3. Router simply reassigns A's files to available node B
  4. B fetches from S3 (cache miss, but works)
  5. Query completes

Simple, predictable failover!
```

---

## References

- [MynaDB Architecture: Cluster Mode](./architecture-cluster.md)
- [AWS S3 LIST API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [TableMeta Source](../src/meta/types/table.rs)
- [Query Coordinator Source](../src/router/routing/query_coordinator.rs)

---

*End of Proposal*
