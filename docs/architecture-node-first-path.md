# Node-First Path Structure for Distributed Queries

This document describes the node-first path structure for Parquet files, which enables distributed parallel queries with optimal cache efficiency.

## Overview

In cluster mode, each Data node prefixes all Parquet files with its `node_id`. This enables:

- **Distributed parallel queries**: Router fans out to multiple nodes
- **Local cache affinity**: Each node only reads/caches its own files
- **Zero collision risk**: Different directories per node
- **Easy node decommission**: Delete entire prefix

## Path Structure

```
Standalone mode:  mydb/default/cpu/date=2026-01-27/L0_1706000001000.parquet
Cluster mode:     {node_id}/mydb/default/cpu/date=2026-01-27/L0_1706000001000.parquet

Example with 3 Data nodes:
  data-1/mydb/default/cpu/date=2026-01-27/L0_001.parquet
  data-2/mydb/default/cpu/date=2026-01-27/L0_002.parquet
  data-3/mydb/default/cpu/date=2026-01-27/L0_003.parquet
```

## Architecture

### Write Path

```
Client Write → Router → data-1 (selected)
                          │
                          ├─► MemTable (replicate to standby)
                          │
                          └─► Flush: data-1/mydb/.../L0_xxx.parquet
                                      ^^^^^^
                                      node_id prefix

                          └─► Register with Meta:
                                path: "data-1/mydb/.../L0_xxx.parquet"
                                created_by: "data-1"
```

When a Data node flushes its MemTable to Parquet:
1. The `ParquetWriter` generates paths with `{node_id}/` prefix
2. Files are written to shared object storage (S3/MinIO)
3. Files are registered with Meta service including `created_by` field

### Query Path (Distributed Parallel)

```
Client Query → Router
                 │
                 ├─► Meta: get_files_for_query("mydb", ["cpu"])
                 │         Returns files grouped by created_by
                 │
                 ├─► data-1: Read [data-1/mydb/.../L0_001.parquet]
                 │           (from LOCAL CACHE)
                 │
                 ├─► data-2: Read [data-2/mydb/.../L0_002.parquet]
                 │           (from LOCAL CACHE)
                 │
                 └─► data-3: Read [data-3/mydb/.../L0_003.parquet]
                             (from LOCAL CACHE)

                 └─► Aggregate partial results

Benefit: N nodes = N× parallelism, 100% cache hit rate
```

The `QueryCoordinator` in the Router:
1. Queries Meta for files matching the query's tables and time range
2. Groups files by `created_by` (the node that wrote each file)
3. Sends each Data node only its own files via `query_with_files()`
4. Aggregates partial results from all nodes

### Fallback Mode

If Meta service is unavailable, the Router falls back to **broadcast mode**:
- Query is sent to all active Data nodes
- Each node scans its local storage for matching files
- Results are aggregated as usual

## Components

### ParquetWriter (`src/storage/parquet_writer.rs`)

Generates paths with node_id prefix:

```rust
fn get_partition_path(node_id: &str, catalog: &str, schema: &str, table: &str, date: i32) -> String {
    format!("{}/{}/{}/{}/{}", node_id, catalog, schema, table, format_date32_as_partition(date))
}
```

### DataCache (`src/storage/cache.rs`)

Stores node_id and passes it to ParquetWriter during flush:

```rust
pub struct DataCache {
    node_id: String,
    // ...
}
```

### QueryCoordinator (`src/router/routing/query_coordinator.rs`)

Routes queries based on file ownership:

```rust
// Query Meta for files grouped by created_by
let (files, _nodes) = meta_client.get_files_for_query(catalog, tables, min_time, max_time).await?;

// Group files by created_by (node_id)
let mut node_files: HashMap<String, Vec<String>> = HashMap::new();
for file in files {
    node_files.entry(file.created_by.clone())
        .or_default()
        .push(file.path.clone());
}

// Send each node only its own files
for (node_id, file_paths) in node_files {
    pool.query_with_files(&node_id, &catalog, &sql, file_paths).await
}
```

### ParquetLocalCache (`src/data/cache/parquet_cache.rs`)

Local disk cache for Parquet files with node affinity:

```rust
pub struct ParquetLocalCache {
    cache_dir: PathBuf,
    max_size_bytes: u64,
    node_id: String,
    // LRU tracking...
}

impl ParquetLocalCache {
    /// Only cache files owned by this node
    fn is_owned_by_this_node(&self, path: &str) -> bool {
        path.starts_with(&format!("{}/", self.node_id))
    }
}
```

## Configuration

### Data Node Configuration

```toml
[data]
node_id = "data-1"           # Unique node identifier
advertise_addr = "data-1:7001"
meta_endpoints = ["meta-1:9000", "meta-2:9000", "meta-3:9000"]
```

### Cache Configuration

```toml
[cache]
max_size_mb = 512              # In-memory write cache
flush_interval_secs = 60
parquet_cache_mb = 2048        # Local disk cache for Parquet files (cluster mode)
```

The `parquet_cache_mb` option enables local disk caching of Parquet files:
- Only caches files owned by this node (path starts with node_id)
- Uses LRU eviction when cache is full
- Recommended: 2-4x the size of the write cache

## File Registration with Meta

When files are flushed, they are registered with Meta including:

| Field | Description |
|-------|-------------|
| `path` | Full path including node_id prefix |
| `catalog` | Database name |
| `schema` | Schema name |
| `table` | Table name |
| `partition` | Partition key (e.g., "2026-01-27") |
| `created_by` | Node ID that created the file |
| `created_at` | Timestamp |

The `created_by` field is critical for distributed query routing.

## Benefits

### 1. Distributed Parallelism

With N Data nodes, queries can achieve N× throughput by reading files in parallel:

```
Query: SELECT COUNT(*) FROM cpu WHERE time > '2026-01-26'

Without node-first paths:
  - All nodes scan ALL files → duplicate work
  - Cache pollution across nodes

With node-first paths:
  - Each node scans ONLY its files → no duplicate work
  - 100% cache hit when reading own files
```

### 2. Cache Efficiency

Each node only caches files it created:

```
data-1 cache: data-1/mydb/.../*.parquet  ← 100% hit rate
data-2 cache: data-2/mydb/.../*.parquet  ← 100% hit rate
data-3 cache: data-3/mydb/.../*.parquet  ← 100% hit rate
```

No cache pollution from other nodes' files.

### 3. Node Decommission

To remove a node from the cluster:

```bash
# 1. Drain the node (stop accepting writes)
# 2. Wait for in-memory data to flush
# 3. Delete all files with node_id prefix
aws s3 rm s3://bucket/data-1/ --recursive
```

### 4. Debugging

Files are easily traceable to their source node:

```bash
# Find all files from data-2
aws s3 ls s3://bucket/data-2/ --recursive

# Check which node wrote a specific file
echo "data-1/mydb/default/cpu/date=2026-01-27/L0_xxx.parquet" | cut -d'/' -f1
# Output: data-1
```

## Migration from Non-Prefixed Paths

For existing clusters without node_id prefixes:

### Option A: Clean Migration (New Clusters)

1. Deploy new version with node-first paths
2. New files are written with `{node_id}/` prefix
3. Old files remain accessible (backward compatible read)
4. Run compaction to migrate old files to new paths

### Option B: Manual Migration (Existing Clusters)

```bash
# Move files to add node_id prefix
# (Run on each Data node for its own files)
aws s3 mv s3://bucket/mydb/ s3://bucket/data-1/mydb/ --recursive
```

## Monitoring

### Metrics to Watch

- **Files per node**: Should be roughly balanced
- **Cache hit rate**: Should be near 100% for owned files
- **Query fan-out**: Number of nodes queried per request
- **File registration latency**: Time to register with Meta

### Debugging Queries

```sql
-- Check file distribution
SELECT created_by, COUNT(*) as file_count, SUM(size_bytes) as total_bytes
FROM meta.files
WHERE catalog = 'mydb'
GROUP BY created_by;
```

## Related Documentation

- [Cluster Architecture](architecture-cluster.md)
- [Write Path](architecture-write-path.md)
- [Query Engine](architecture-query-engine.md)
- [Caching Layer](architecture-caching-layer.md)
