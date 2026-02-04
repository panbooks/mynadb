# Proposal: Partition-Aware Query Routing via Path Restructure

**Author:** MynaDB Team
**Created:** 2026-02-04
**Status:** Draft
**Target Version:** v2.x

---

## Executive Summary

This proposal addresses performance defects in MynaDB's table-level routing mechanism. The current design routes queries to ALL nodes that have ever written to a table, regardless of which date partitions each node actually holds.

**Key Finding:** Query routing cannot prune by date partition, resulting in N× query cost where N = number of historical table owners.

**Proposed Solution:** Restructure the object store path to use `node_id` as a Hive-style partition key:
```
{catalog}/{schema}/{table}/date={YYYY-MM-DD}/node_id={node_id}/file.parquet
```

This leverages DataFusion's built-in partition pruning - each Data Node simply adds `WHERE node_id = 'my_id'` to queries, and DataFusion automatically skips irrelevant files.

**Impact:**
- Query fan-out reduced by 60-90% for typical queries
- **Zero Meta storage overhead** (vs 16.8MB+ for alternatives)
- **~50 lines of code changes** (vs ~500 for file-level routing)
- True compute-storage separation
- Simplified scale-in/out operations
- `node_id` becomes a queryable column for debugging/analysis

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
                ├── node_id={node-A}/
                │   └── *.parquet
                └── node_id={node-B}/
                    └── *.parquet
```

**Key Change:** Make `node_id` a Hive-style partition key, just like `date`.

### 3.2 How It Solves the Problem

```
Query: SELECT * FROM cpu WHERE date = '2024-01-15'

Routing Flow:
1. Router caches partition → nodes mapping (stable, cacheable)
   LIST s3://bucket/mydb/default/cpu/date=2024-01-15/
   → CommonPrefixes: [node_id=A/, node_id=B/]

2. Router sends to nodes A and B:
   - Query: SELECT * FROM cpu WHERE date = '2024-01-15'
   - Each node knows its own node_id

3. Data Node query execution:
   - Node A injects: WHERE node_id = 'A'
   - DataFusion automatically prunes node_id=B/ directory
   - Only reads files from node_id=A/

Result: Automatic partition pruning via DataFusion!
```

### 3.3 Why Hive-Style `node_id` Partition Key?

Making `node_id` a partition key (like `date`) brings several advantages:

1. **DataFusion handles pruning automatically** - No custom file discovery logic needed
2. **`node_id` becomes a queryable column** - Useful for debugging (`SELECT DISTINCT node_id FROM cpu`)
3. **Existing partition code reused** - Same logic that handles `date=` partitions works for `node_id=`
4. **Minimal code changes** - Data Node just injects one predicate

### 3.4 Benefits Summary

| Benefit | Current (Node-ID First) | Proposed (Hive Partition) |
|---------|------------------------|---------------------------|
| Meta storage for routing | 16.8MB+ | **0** |
| Implementation complexity | N/A | **~50 lines** |
| Retention complexity | O(nodes × dates) | **O(dates)** |
| Scale-in complexity | High (legacy_prefixes) | **None** |
| Cross-node compaction | Impossible | **Possible** |
| Cache affinity | Strong | **Strong** (writer = reader) |
| `node_id` queryable | No | **Yes** |

### 3.5 Why Hive Partition is Elegant

The key insight is that **DataFusion already knows how to do partition pruning**. By making `node_id` a partition key:

1. **No new file discovery code** - DataFusion's `ListingTable` handles it
2. **No file lists in QueryTicket** - Each node discovers its own files
3. **No LocalFileCache** - DataFusion caches discovered files
4. **Natural predicate injection** - Just one `WHERE` clause addition

**Comparison of approaches:**

| Approach | Router Responsibility | Data Node Responsibility | Code Changes |
|----------|----------------------|--------------------------|--------------|
| File-level routing | LIST all files, assign to nodes | Execute with file list | ~500 lines |
| Partition-level (Hive) | Cache partition→nodes | Inject `node_id` predicate | **~50 lines** |

The Router doesn't need to know about files at all - it only tracks the stable mapping of partitions to nodes. Files change frequently (compaction), but partition ownership is stable.

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

**Node-ID Last with Hive Partition (Proposed):**
```
Range query: WHERE date BETWEEN '01-01' AND '01-31'

1. Router checks partition cache (stable, cacheable 5min+):
   date=01-01 → nodes: [A]
   date=01-15 → nodes: [A, B]
   date=01-31 → nodes: [A, B, C]

2. Router determines: need to query {A, B, C}

3. Send SAME query to each node (no file lists!):
   SELECT * FROM cpu WHERE date BETWEEN '01-01' AND '01-31'

4. Each Data Node injects its node_id:
   Node A: WHERE ... AND node_id = 'A'
   Node B: WHERE ... AND node_id = 'B'
   Node C: WHERE ... AND node_id = 'C'

5. DataFusion prunes automatically:
   - A reads only node_id=A/ directories
   - B reads only node_id=B/ directories
   - C reads only node_id=C/ directories

→ Optimal parallelism, zero redundant work
→ No file lists passed to nodes
→ Each node discovers its own files locally
```

**Winner: Node-ID Last (Hive Partition)**

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

**Node-ID Last with Hive Partition (Proposed):**
```
Write: Node A writes → /.../node_id=A/file.parquet
       Node A caches file locally

Query:
  1. Router: partition=01-15 has nodes [A, B]
  2. Router sends SAME query to both A and B
  3. Node A executes with: WHERE node_id = 'A'
     → DataFusion only reads /.../node_id=A/ files
     → Cache HIT (Node A wrote these files!)
  4. Node B executes with: WHERE node_id = 'B'
     → DataFusion only reads /.../node_id=B/ files
     → Cache HIT (Node B wrote these files!)

Cache hit rate: HIGH (writer = reader naturally preserved!)
No explicit file routing needed - just send query to all relevant nodes!
```

**Winner: Tie** (both achieve high cache affinity)

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

**Node-ID Last with Hive Partition (Proposed):**
```
Partition structure:
  /mydb/cpu/date=01-15/node_id=A/L0_001.parquet
  /mydb/cpu/date=01-15/node_id=B/L0_002.parquet

Compaction options:

  Option 1 (per-node): Same as current

  Option 2 (cross-node merge):
    Compact ALL files in partition →
    /mydb/cpu/date=01-15/node_id=merged/L1_001.parquet

    Result: Unified partition, optimal compression
    After: Queries with WHERE node_id='merged' see all data
```

**Winner: Node-ID Last (Hive Partition)**

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

**Node-ID Last with Hive Partition (Proposed):**
```
Node B retires:

1. Data remains at /.../node_id=B/ subdirectories
2. No Meta update needed!
3. Query routing:
   - Router cache still shows: partition → [..., B, ...]
   - B offline → send B's work to fallback node C
   - C queries with: WHERE node_id = 'B'
   - DataFusion reads /.../node_id=B/ from S3
4. Over time:
   - Cross-node compaction merges B's data into node_id=merged/
   - node_id=B/ directories naturally disappear
```

**Winner: Node-ID Last (Hive Partition)**

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

**Node-ID Last with Hive Partition (Proposed):**
```
Node C joins:

1. C starts receiving writes for some tables
2. C writes to /.../node_id=C/
3. No Meta update needed
4. Router cache refresh (or LIST) discovers node_id=C/ automatically
5. Queries naturally include C:
   - Router sends query to C
   - C queries with: WHERE node_id = 'C'
```

**Winner: Node-ID Last (Hive Partition)**

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

**Node-ID Last with Hive Partition (Proposed):**
```
Node A fails during query:

1. Router knows: partition has nodes [A, B]
2. A times out
3. Simple retry: send A's query to any active node (C):
   - Query includes: WHERE node_id = 'A'
   - C reads /.../node_id=A/ from S3
4. C fetches from S3 (cache miss, but works)
5. True compute-storage separation
6. No file lists needed - just pass node_id!
```

**Winner: Node-ID Last (Hive Partition)**

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

| Dimension | Node-ID First | Node-ID Last (Hive) | Winner |
|-----------|---------------|---------------------|--------|
| Retention logic | O(nodes × dates) | O(dates) | **Last** |
| Route simplicity | Complex (Meta) | Simple (cache + predicate) | **Last** |
| Query performance | No LIST latency | Cache hit (stable mapping) | Tie |
| Parallel query | Needs node_coverage | Natural (node_id predicate) | **Last** |
| **Cache affinity** | Strong | Strong (writer = reader) | **Tie** |
| Compaction | Per-node only | Cross-node possible | **Last** |
| Scale-in | Complex (legacy) | Natural (just query) | **Last** |
| Scale-out | Meta grows | Natural discovery | **Last** |
| Failure handling | Complex failover | Simple retry with node_id | **Last** |
| Write path | Simple | Simple | Tie |
| Multi-tenant | Node isolation | Tenant isolation | **Last** |
| Listing efficiency | O(nodes) | O(1) | **Last** |
| **Meta storage** | **16.8MB+** | **0** | **Last** |
| **Implementation** | N/A | **~50 lines** | **Last** |

**Final Score: Node-ID Last (Hive Partition) wins 10/14, Tie 3/14, Node-ID First wins 1/14**

---

## 5. Detailed Design

### 5.1 Path Structure Specification

```
Old path format:
  s3://{bucket}/{node_id}/{catalog}/{schema}/{table}/date={YYYY-MM-DD}/{file}.parquet

New path format (Hive-style partition):
  s3://{bucket}/{catalog}/{schema}/{table}/date={YYYY-MM-DD}/node_id={node_id}/{file}.parquet
```

The `node_id=` prefix makes it a Hive-style partition key that DataFusion recognizes automatically.

### 5.2 Router: Partition → Nodes Caching

The Router maintains a cache of which nodes have data for each partition. This mapping is stable (changes only on scale events), making it highly cacheable.

```rust
/// Cache for partition → nodes mapping
/// Key: "catalog/schema/table/date=YYYY-MM-DD"
/// Value: Vec<node_id>
struct PartitionNodeCache {
    cache: DashMap<String, (Vec<String>, Instant)>,
    ttl: Duration,  // e.g., 5 minutes
}

impl PartitionNodeCache {
    /// Discover nodes with data for given partition
    async fn get_or_discover(
        &self,
        object_store: &ObjectStore,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Vec<String> {
        let key = format!("{}/{}/{}/date={}", catalog, schema, table, date);

        // Check cache
        if let Some((nodes, timestamp)) = self.cache.get(&key).map(|r| r.clone()) {
            if timestamp.elapsed() < self.ttl {
                return nodes;
            }
        }

        // LIST from object store (only get CommonPrefixes)
        let prefix = format!("{}/", key);
        let result = object_store
            .list_with_delimiter(&prefix)
            .await
            .unwrap_or_default();

        // Extract node_ids from CommonPrefixes like "node_id=A/"
        let nodes: Vec<String> = result
            .common_prefixes
            .iter()
            .filter_map(|p| p.strip_prefix("node_id=")?.strip_suffix("/"))
            .map(String::from)
            .collect();

        // Cache result
        self.cache.insert(key, (nodes.clone(), Instant::now()));

        nodes
    }
}

/// Route query to appropriate nodes
async fn route_query(&self, table: &TableRef, date_range: &DateRange) -> Vec<NodeId> {
    let mut target_nodes = HashSet::new();

    for date in date_range.iter_dates() {
        let nodes = self.partition_cache.get_or_discover(
            &self.object_store,
            &table.catalog,
            &table.schema,
            &table.name,
            &date
        ).await;

        for node_id in nodes {
            if self.is_node_available(&node_id) {
                target_nodes.insert(node_id);
            }
        }
    }

    target_nodes.into_iter().collect()
}
```

**Key Point:** Router only caches partition → nodes mapping, NOT file lists. File discovery is delegated to Data Nodes.

### 5.3 Data Node: Automatic Partition Pruning

The Data Node leverages DataFusion's built-in Hive partition pruning. When executing a query, it injects `WHERE node_id = 'my_id'` to filter to only its own files.

```rust
/// Data Node query execution with node_id injection
impl FlightService {
    async fn do_get(&self, ticket: QueryTicket) -> Result<FlightDataStream> {
        // Inject node_id predicate to leverage DataFusion partition pruning
        let sql_with_node_filter = inject_node_id_predicate(
            &ticket.sql,
            &self.node_id,
        );

        // Execute query - DataFusion automatically prunes node_id partitions
        let batches = self.query_engine
            .execute_query_with_db(&sql_with_node_filter, &ticket.catalog)
            .await?;

        Ok(batches.into())
    }
}

/// Inject WHERE node_id = 'X' into the query
fn inject_node_id_predicate(sql: &str, node_id: &str) -> String {
    // Parse SQL and add predicate
    // For queries with WHERE: add "AND node_id = 'X'"
    // For queries without WHERE: add "WHERE node_id = 'X'"

    // Simple implementation using sqlparser
    let mut statements = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::GenericDialect {},
        sql
    ).unwrap();

    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            add_node_id_filter(query, node_id);
        }
    }

    statements.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; ")
}

/// Add node_id filter to query
fn add_node_id_filter(query: &mut Query, node_id: &str) {
    if let SetExpr::Select(select) = &mut *query.body {
        let node_filter = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("node_id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(node_id.to_string()))),
        };

        select.selection = match select.selection.take() {
            Some(existing) => Some(Expr::BinaryOp {
                left: Box::new(existing),
                op: BinaryOperator::And,
                right: Box::new(node_filter),
            }),
            None => Some(node_filter),
        };
    }
}
```

**How DataFusion Partition Pruning Works:**

1. TableProvider registers the table with partition columns `[date, node_id]`
2. Query has predicate `WHERE node_id = 'A'`
3. DataFusion's `ListingTable` automatically skips directories not matching the predicate
4. Only files under `node_id=A/` are read

This requires **no changes to DataFusion** - it's built-in functionality.

### 5.4 Write Path Changes

```rust
/// Generate write path with Hive-style node_id partition
fn get_write_path(
    node_id: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    date: &str,
    file_name: &str,
) -> String {
    // Old: {node_id}/{catalog}/{schema}/{table}/date={date}/{file}
    // New: {catalog}/{schema}/{table}/date={date}/node_id={node_id}/{file}
    format!(
        "{}/{}/{}/date={}/node_id={}/{}",
        catalog, schema, table, date, node_id, file_name
    )
}
```

### 5.5 Schema Changes

The table schema now includes `node_id` as a partition column:

```rust
/// Register table with partition columns
fn register_table(&self, catalog: &str, schema: &str, table: &str) -> Result<()> {
    let table_path = format!("{}/{}/{}/", catalog, schema, table);

    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_table_partition_cols(vec![
            ("date".to_string(), DataType::Utf8),
            ("node_id".to_string(), DataType::Utf8),  // NEW!
        ]);

    // ... register with DataFusion
}
```

**Implication:** `node_id` becomes a queryable column:

```sql
-- See which nodes have data for a table
SELECT DISTINCT node_id FROM cpu WHERE date = '2024-01-15';

-- Debug: query specific node's data
SELECT * FROM cpu WHERE date = '2024-01-15' AND node_id = 'data-1';
```

### 5.6 Retention Manager Changes

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
            // Delete entire partition (all node_id= subdirs included)
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

### 5.7 Compaction Changes

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

    // List all node_id= subdirectories
    let node_dirs = self.object_store
        .list_with_delimiter(&partition_prefix)
        .await?;

    // Collect all files from all nodes
    let mut all_files = Vec::new();
    for node_dir in node_dirs.common_prefixes {
        // node_dir is like "node_id=A/"
        let files = self.object_store.list(&node_dir).await?;
        all_files.extend(files);
    }

    if all_files.len() < self.config.min_files_for_compaction {
        return Ok(CompactionStats::skipped());
    }

    // Compact all files into merged output under "node_id=merged/"
    let output_path = format!("{}node_id=merged/", partition_prefix);
    let stats = self.compact_files(all_files, &output_path).await?;

    // Delete original node_id= subdirectories
    for node_dir in node_dirs.common_prefixes {
        if !node_dir.contains("node_id=merged") {
            self.object_store.delete_prefix(&node_dir).await?;
        }
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

### 6.2 Hive Partition Automatically Preserves Affinity

```
Write flow:
  1. Node A receives write for partition P
  2. A writes to s3://bucket/.../date=P/node_id=A/file.parquet
  3. A caches file locally

Query flow (Hive Partition approach):
  1. Router knows partition P → nodes [A, B]
  2. Router sends SAME query to A and B
  3. Node A injects: WHERE node_id = 'A'
     → DataFusion reads only /.../node_id=A/ (cache HIT!)
  4. Node B injects: WHERE node_id = 'B'
     → DataFusion reads only /.../node_id=B/ (cache HIT!)

Result: Cache affinity preserved automatically!
No explicit file→node routing needed!
```

### 6.3 Failover with Graceful Degradation

```
Node A fails:

  1. Router knows partition P → nodes [A, B]
  2. Router detects A is unavailable
  3. Router sends A's work to fallback Node C:
     Query with WHERE node_id = 'A'
  4. Node C executes: DataFusion reads /.../node_id=A/
     → Fetches from S3 (cache miss, ~50ms per file)
  5. Query completes (slower, but works)
  6. Node C caches fetched files
  7. Next failover query → C has cache hit

Graceful degradation: Works correctly, just slower on first failover query
```

### 6.4 Cache Warm-Up Strategy

```rust
/// Pre-warm cache for frequently accessed partitions (Data Node startup)
async fn warm_cache(&self, recent_days: u32) {
    let today = Utc::now().date();
    let my_node_id = &self.node_id;

    for table in self.discover_my_tables().await {
        for day_offset in 0..recent_days {
            let date = today - Duration::days(day_offset as i64);

            // Just run a lightweight query to warm the cache
            // DataFusion's partition pruning will only read our node_id files
            let sql = format!(
                "SELECT COUNT(*) FROM {}.{}.{} WHERE date = '{}' AND node_id = '{}'",
                table.catalog, table.schema, table.name, date, my_node_id
            );
            let _ = self.query_engine.execute_query(&sql).await;
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

### 9.1 Milestones (Simplified with Hive Partition Approach)

| Phase | Description | LOC Estimate | Risk |
|-------|-------------|--------------|------|
| 1 | Write path: add `node_id=` to path | ~20 lines | Low |
| 2 | TableProvider: add `node_id` partition column | ~10 lines | Low |
| 3 | Data Node: inject `node_id` predicate | ~30 lines | Low |
| 4 | Router: partition→nodes cache | ~50 lines | Low |
| 5 | Dual-path read support (migration) | ~30 lines | Low |
| 6 | Testing & validation | Tests | Low |

**Total: ~140 lines of production code**

Compare to previous approach requiring ~500+ lines for LocalFileCache and custom file routing.

### 9.2 Files to Modify

| File | Changes |
|------|---------|
| `src/storage/parquet_writer.rs` | Add `node_id=` to write path (~5 lines) |
| `src/query/providers/table_provider.rs` | Add `node_id` partition column (~10 lines) |
| `src/data/server/flight.rs` | Inject `node_id` predicate (~30 lines) |
| `src/router/routing/query_coordinator.rs` | Partition cache (~50 lines) |
| `src/jobs/retention_manager.rs` | Simplified retention (already works!) |
| `src/jobs/compactor/` | Cross-node compaction (optional, later) |

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
# Cache TTL for partition node discovery (stable, can be long)
partition_cache_ttl_secs = 300  # 5 minutes
```

### 9.4 Testing Plan

| Test Category | Description |
|---------------|-------------|
| Unit tests | Path generation with `node_id=`, predicate injection |
| Integration tests | Query with `node_id` partition pruning |
| Performance tests | Verify DataFusion prunes `node_id=` directories |
| Scale tests | 100K tables, 30 partitions, 5 nodes each |
| Migration tests | Old → new path during compaction |

### 9.5 Implementation Code Summary

**Total changes for core functionality (~50 lines):**

```rust
// 1. Write path (parquet_writer.rs) - ~5 lines
fn get_write_path(...) -> String {
    format!("{}/{}/{}/date={}/node_id={}/{}",
        catalog, schema, table, date, node_id, file_name)
}

// 2. TableProvider (table_provider.rs) - ~10 lines
let listing_options = ListingOptions::new(...)
    .with_table_partition_cols(vec![
        ("date".to_string(), DataType::Utf8),
        ("node_id".to_string(), DataType::Utf8),  // ADD THIS
    ]);

// 3. Data Node predicate injection (flight.rs) - ~30 lines
async fn do_get(&self, ticket: QueryTicket) -> Result<...> {
    let sql = inject_node_id_predicate(&ticket.sql, &self.node_id);
    self.query_engine.execute_query_with_db(&sql, &ticket.catalog).await
}
```

That's it. DataFusion handles the rest automatically.

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

### A.1 Example Query Flow (New Design with Hive Partition)

```
Query: SELECT avg(value) FROM cpu
       WHERE date BETWEEN '2024-01-10' AND '2024-01-15'
       GROUP BY host

Router:
  1. Parse SQL → table=cpu, dates=[01-10 to 01-15]

  2. Check partition cache (or LIST if cache miss):
     date=2024-01-10 → nodes: [A]
     date=2024-01-11 → nodes: [A]
     date=2024-01-12 → nodes: [A, B]
     date=2024-01-13 → nodes: [B]
     date=2024-01-14 → nodes: [B]
     date=2024-01-15 → nodes: [B, C]

  3. Determine target nodes: {A, B, C}

  4. Send query to each node (same SQL):
     SELECT avg(value) FROM cpu
     WHERE date BETWEEN '2024-01-10' AND '2024-01-15'
     GROUP BY host

Data Node (e.g., Node A):
  1. Receive query
  2. Inject node_id predicate:
     SELECT avg(value) FROM cpu
     WHERE date BETWEEN '2024-01-10' AND '2024-01-15'
       AND node_id = 'A'  ← INJECTED
     GROUP BY host

  3. DataFusion partition pruning:
     - Scans date=2024-01-10/node_id=A/  ✓
     - Scans date=2024-01-11/node_id=A/  ✓
     - Scans date=2024-01-12/node_id=A/  ✓
     - SKIPS date=2024-01-12/node_id=B/  (pruned by predicate)
     - SKIPS date=2024-01-13/ through 01-15 for node_id=A (no files exist)

  4. Read from local cache (wrote these files)
  5. Return partial aggregates

Router:
  5. Collect partial results from A, B, C
  6. Merge aggregates
  7. Return final result

Nodes queried: 3 (vs ALL owners with current design)
Cache hit rate: 100% (writer = reader)
No file lists passed between Router and Data Nodes!
```

### A.2 Scale-In Example (New Design with Hive Partition)

```
Before: Cluster has nodes [A, B, C]
        Table cpu has data on all three nodes

Storage layout:
  /mydb/default/cpu/date=2024-01-15/node_id=A/file1.parquet
  /mydb/default/cpu/date=2024-01-15/node_id=B/file2.parquet
  /mydb/default/cpu/date=2024-01-15/node_id=C/file3.parquet

Scale-in: Remove Node B

During transition:
  1. B marked as retiring
  2. New writes go to A and C only
  3. B's existing data remains at /node_id=B/ subdirectories
  4. Queries:
     - Router cache: date=01-15 → [A, B, C]
     - B is offline → Router sends B's work to fallback node (A or C)
     - Fallback node queries with: WHERE node_id = 'B'
     - DataFusion reads from /node_id=B/ (S3 fetch, cache miss once)

After compaction:
  1. Cross-node compaction runs on old partitions
  2. Merges all files → /node_id=merged/L1_001.parquet
  3. Original node_id=A/, node_id=B/, node_id=C/ deleted
  4. Future queries see only node_id=merged/

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

New (Hive Partition):
  1. Router knows: date=01-15 has nodes [A, B]
  2. Router sends query to both A and B
  3. A times out
  4. Router retries A's portion on fallback node C:
     - Query: SELECT * FROM cpu WHERE date='01-15' AND node_id='A'
     - C fetches from S3 /date=01-15/node_id=A/ (cache miss, but works)
  5. Query completes with results from B and C

Simple, predictable failover!
No file lists needed - just pass node_id to retry!
```

---

## References

- [MynaDB Architecture: Cluster Mode](./architecture-cluster.md)
- [AWS S3 LIST API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [TableMeta Source](../src/meta/types/table.rs)
- [Query Coordinator Source](../src/router/routing/query_coordinator.rs)

---

*End of Proposal*
