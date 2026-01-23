# MynaDB Query Performance Optimization Roadmap

**Version:** 1.0
**Date:** 2026-01-23
**Author:** MynaDB Team

---

## Executive Summary

This document outlines a progressive optimization roadmap for improving query performance in MynaDB. The optimizations are organized into six phases, prioritized by impact-to-effort ratio. Implementation should follow the recommended order to maximize cumulative benefits.

### Current Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Query Path                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   SQL Query                                                          │
│      │                                                               │
│      ▼                                                               │
│   ┌─────────────────┐                                                │
│   │  SQL Parser     │  (DataFusion)                                  │
│   └────────┬────────┘                                                │
│            ▼                                                         │
│   ┌─────────────────┐     ┌─────────────────┐                        │
│   │ Query Planner   │────▶│  Query Cache    │ (if cache hit)         │
│   └────────┬────────┘     └─────────────────┘                        │
│            ▼                                                         │
│   ┌─────────────────┐                                                │
│   │ Table Provider  │                                                │
│   └────────┬────────┘                                                │
│            │                                                         │
│      ┌─────┴─────┐                                                   │
│      ▼           ▼                                                   │
│ ┌─────────┐ ┌─────────────┐                                          │
│ │Memtable │ │Parquet Files│                                          │
│ │ (Cache) │ │  (L0-L5)    │                                          │
│ └────┬────┘ └──────┬──────┘                                          │
│      │             │                                                 │
│      └──────┬──────┘                                                 │
│             ▼                                                        │
│   ┌─────────────────┐                                                │
│   │  Union Results  │                                                │
│   └────────┬────────┘                                                │
│            ▼                                                         │
│   ┌─────────────────┐                                                │
│   │  Fill Processor │  (for GROUP BY time)                           │
│   └────────┬────────┘                                                │
│            ▼                                                         │
│        Results                                                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Low-Hanging Fruit

**Timeline:** Week 1-2
**Impact:** High
**Effort:** Low

These optimizations provide immediate performance gains with minimal code changes.

### 1.1 Parquet Metadata Caching

**Problem:** Each query reads Parquet file footers from disk, even for repeated queries on the same files.

**Solution:** Cache Parquet file metadata (footer, schema, row group statistics) in memory.

**Implementation:**
```rust
pub struct ParquetMetadataCache {
    cache: LruCache<PathBuf, Arc<ParquetMetaData>>,
    max_size_bytes: usize,
}

impl ParquetMetadataCache {
    pub async fn get_metadata(&self, path: &Path) -> Result<Arc<ParquetMetaData>> {
        if let Some(meta) = self.cache.get(path) {
            return Ok(meta.clone());
        }
        let meta = read_parquet_metadata(path).await?;
        self.cache.insert(path.to_path_buf(), Arc::new(meta));
        Ok(meta)
    }
}
```

**Expected Impact:** 20-30% faster for repeated queries

**Files to Modify:**
- `src/query/metadata_cache.rs` - Add ParquetMetadataCache
- `src/query/table_provider.rs` - Use cached metadata

---

### 1.2 Predicate Pushdown for Tags

**Problem:** Tag filters (e.g., `WHERE host = 'server01'`) are applied after reading all data.

**Solution:** Push tag predicates to Parquet reader to skip irrelevant row groups using column statistics.

**Implementation:**
```rust
// In table_provider.rs
fn create_physical_plan(&self, filters: &[Expr]) -> Result<Arc<dyn ExecutionPlan>> {
    let pushdown_filters = extract_pushdown_filters(filters);

    ParquetExec::builder(file_scan_config)
        .with_predicate(pushdown_filters)  // Push filters to Parquet reader
        .build()
}
```

**Expected Impact:** 50%+ faster when filtering by tags

**Files to Modify:**
- `src/query/table_provider.rs` - Implement filter pushdown
- `src/query/logical_optimizers/partition_filter_pushdown.rs` - Extend for tag columns

---

### 1.3 Projection Pushdown

**Problem:** `SELECT a, b FROM table` may read all columns from Parquet files.

**Solution:** Only read columns referenced in SELECT, WHERE, GROUP BY, and ORDER BY clauses.

**Implementation:**
```rust
// DataFusion already supports this, ensure it's enabled
let config = SessionConfig::new()
    .with_parquet_pruning(true)
    .with_collect_statistics(true);
```

**Expected Impact:** Reduces I/O by 30-80% depending on query selectivity

**Files to Modify:**
- `src/query/query_engine.rs` - Verify projection pushdown is enabled
- `src/query/executor.rs` - Configure DataFusion settings

---

### 1.4 Row Group Pruning with Statistics

**Problem:** Queries scan all row groups even when time range filters could exclude most of them.

**Solution:** Use min/max statistics stored in Parquet row group metadata to skip irrelevant groups.

**Implementation:**
```rust
// Ensure row group statistics are written
let writer_props = WriterProperties::builder()
    .set_statistics_enabled(EnabledStatistics::Chunk)  // Per-row-group stats
    .set_max_statistics_size(4096)
    .build();

// DataFusion uses these automatically when predicate pushdown is enabled
```

**Expected Impact:** Major speedup for time-range queries (often 10x+)

**Files to Modify:**
- `src/storage/parquet_writer.rs` - Ensure statistics are written
- `src/query/table_provider.rs` - Verify statistics-based pruning

---

## Phase 2: Indexing & Bloom Filters

**Timeline:** Week 3-4
**Impact:** High
**Effort:** Medium

These optimizations add lightweight indexing structures for faster data location.

### 2.1 Bloom Filters for Tag Columns

**Problem:** Point queries on high-cardinality tags (e.g., `WHERE request_id = 'abc123'`) scan many files.

**Solution:** Write Bloom filters for tag columns in Parquet files; use them to skip files that definitely don't contain the value.

**Implementation:**
```rust
// Writing Bloom filters
let writer_props = WriterProperties::builder()
    .set_bloom_filter_enabled(true)
    .set_bloom_filter_fpp(0.01)  // 1% false positive rate
    .set_bloom_filter_ndv(10000)  // Expected distinct values
    .build();

// Reading with Bloom filter
let bloom_filter = parquet_reader.get_column_bloom_filter("host")?;
if !bloom_filter.check(&search_value) {
    // Skip this file entirely
    continue;
}
```

**Expected Impact:** 10x faster point lookups on indexed columns

**Files to Modify:**
- `src/storage/parquet_writer.rs` - Enable Bloom filters for tag columns
- `src/query/table_provider.rs` - Use Bloom filters for file pruning

**Configuration:**
```toml
[storage.bloom_filter]
enabled = true
columns = ["host", "service", "region"]  # Tag columns to index
false_positive_rate = 0.01
```

---

### 2.2 Sparse Time Index

**Problem:** Finding files containing a specific time range requires scanning file metadata.

**Solution:** Maintain a lightweight index mapping time ranges to files/row groups.

**Implementation:**
```rust
/// Sparse index entry for a Parquet file
#[derive(Serialize, Deserialize)]
pub struct TimeIndexEntry {
    pub file_path: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub row_count: usize,
    pub row_groups: Vec<RowGroupTimeRange>,
}

/// Time index for a table partition
pub struct TimeIndex {
    entries: BTreeMap<i64, Vec<TimeIndexEntry>>,  // Keyed by min_timestamp
}

impl TimeIndex {
    pub fn find_files(&self, start: i64, end: i64) -> Vec<&TimeIndexEntry> {
        self.entries
            .range(..=end)
            .flat_map(|(_, entries)| entries)
            .filter(|e| e.max_timestamp >= start)
            .collect()
    }
}
```

**Expected Impact:** Skip file metadata reads entirely for time-bounded queries

**Files to Create:**
- `src/query/time_index.rs` - Time index implementation

**Files to Modify:**
- `src/storage/parquet_writer.rs` - Update index on write
- `src/jobs/compactor.rs` - Rebuild index after compaction

---

### 2.3 Tag Value Index (Inverted Index)

**Problem:** Finding all files containing `service='payments'` requires checking each file.

**Solution:** Maintain an inverted index mapping tag values to file lists.

**Implementation:**
```rust
/// Inverted index for tag values
pub struct TagIndex {
    /// tag_name -> tag_value -> list of files
    index: HashMap<String, HashMap<String, Vec<String>>>,
}

impl TagIndex {
    pub fn find_files(&self, tag: &str, value: &str) -> Option<&Vec<String>> {
        self.index.get(tag)?.get(value)
    }

    pub fn add_file(&mut self, file: &str, tags: &HashMap<String, String>) {
        for (tag, value) in tags {
            self.index
                .entry(tag.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .push(file.to_string());
        }
    }
}
```

**Expected Impact:** O(1) file lookup for tag-based queries

**Files to Create:**
- `src/query/tag_index.rs` - Tag index implementation

---

### 2.4 Partition Index File

**Problem:** Discovering available partitions requires directory scanning.

**Solution:** Maintain a single index file per table listing all partitions and their metadata.

**Implementation:**
```rust
/// Partition index stored as JSON or binary
#[derive(Serialize, Deserialize)]
pub struct PartitionIndex {
    pub table: String,
    pub partitions: Vec<PartitionInfo>,
    pub last_updated: i64,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionInfo {
    pub date: String,           // "2024-01-15"
    pub files: Vec<FileInfo>,
    pub total_rows: usize,
    pub total_bytes: usize,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}
```

**Storage Location:**
```
data/{catalog}/{schema}/{table}/_index/
    partition_index.json
    time_index.bin
    tag_index.bin
```

**Expected Impact:** Faster partition discovery, especially for tables with many partitions

---

## Phase 3: Caching Layers

**Timeline:** Week 5-6
**Impact:** Medium-High
**Effort:** Medium

These optimizations add intelligent caching at various levels.

### 3.1 Hot Partition Cache

**Problem:** Recent data (today, yesterday) is queried frequently but read from disk each time.

**Solution:** Keep decoded Arrow RecordBatches for recent partitions in memory.

**Implementation:**
```rust
pub struct HotPartitionCache {
    /// (catalog, schema, table, date) -> cached batches
    cache: RwLock<HashMap<(String, String, String, i32), Vec<RecordBatch>>>,
    max_size_bytes: usize,
    current_size: AtomicUsize,
}

impl HotPartitionCache {
    pub fn is_hot(&self, date: i32) -> bool {
        let today = current_date_i32();
        date >= today - 1  // Today and yesterday are "hot"
    }

    pub async fn get_or_load(
        &self,
        key: &CacheKey,
        loader: impl Future<Output = Vec<RecordBatch>>,
    ) -> Vec<RecordBatch> {
        if let Some(batches) = self.cache.read().get(key) {
            return batches.clone();
        }
        let batches = loader.await;
        self.cache.write().insert(key.clone(), batches.clone());
        batches
    }
}
```

**Configuration:**
```toml
[cache.hot_partition]
enabled = true
max_size_mb = 1024        # 1GB for hot data
hot_days = 2              # Keep today and yesterday
preload_on_startup = true # Preload hot partitions
```

**Expected Impact:** Near-instant queries for recent data

---

### 3.2 Decoded Column Cache

**Problem:** Frequently accessed columns are decompressed repeatedly.

**Solution:** Cache decompressed Arrow arrays for hot columns.

**Implementation:**
```rust
pub struct ColumnCache {
    /// (file, row_group, column) -> decoded array
    cache: LruCache<(String, usize, String), ArrayRef>,
    max_size_bytes: usize,
}
```

**Expected Impact:** Avoid repeated decompression for popular columns

---

### 3.3 Query Plan Cache

**Problem:** Same SQL queries are parsed and optimized repeatedly.

**Solution:** Cache the optimized logical/physical plans for common queries.

**Implementation:**
```rust
pub struct QueryPlanCache {
    /// Normalized SQL -> optimized plan
    cache: LruCache<String, Arc<dyn ExecutionPlan>>,
    ttl: Duration,
}

impl QueryPlanCache {
    pub fn normalize_sql(sql: &str) -> String {
        // Replace literals with placeholders
        // "SELECT * FROM t WHERE x = 5" -> "SELECT * FROM t WHERE x = ?"
        normalize_query(sql)
    }
}
```

**Expected Impact:** Faster repeated queries (skip parsing/optimization)

---

### 3.4 Aggregation Result Cache

**Problem:** Dashboard queries like `SELECT avg(cpu) FROM metrics WHERE time > now()-1h GROUP BY host` run repeatedly.

**Solution:** Cache aggregation results with automatic invalidation on new writes.

**Implementation:**
```rust
pub struct AggregationCache {
    /// Query hash -> (result, valid_until_timestamp)
    cache: RwLock<HashMap<u64, CachedAggregation>>,
}

#[derive(Clone)]
pub struct CachedAggregation {
    pub result: Vec<RecordBatch>,
    pub computed_at: i64,
    pub valid_until: i64,  // Max timestamp in result + buffer
}

impl AggregationCache {
    pub fn is_valid(&self, entry: &CachedAggregation, table_max_ts: i64) -> bool {
        // Invalidate if new data arrived after cache was computed
        table_max_ts <= entry.valid_until
    }
}
```

**Expected Impact:** Instant dashboard queries (100x+ for complex aggregations)

---

## Phase 4: Storage Optimization

**Timeline:** Week 7
**Impact:** Medium
**Effort:** Low

These optimizations improve how data is stored in Parquet files.

### 4.1 Optimal Row Group Size

**Problem:** Default row group size may not be optimal for query patterns.

**Solution:** Tune row group size based on typical query selectivity.

**Recommendations:**
| Use Case | Row Group Size | Rationale |
|----------|----------------|-----------|
| Point queries | 64MB | More row groups = finer pruning |
| Scan queries | 128MB | Fewer row groups = less overhead |
| Mixed | 64MB | Good balance |

**Implementation:**
```toml
[storage.parquet]
row_group_size_mb = 64
page_size_kb = 1024
```

---

### 4.2 Dictionary Encoding for Tags

**Problem:** Tag columns have low cardinality but store repeated strings.

**Solution:** Force dictionary encoding for tag columns.

**Implementation:**
```rust
let writer_props = WriterProperties::builder()
    .set_dictionary_enabled(true)
    .set_column_dictionary_enabled(ColumnPath::new(vec!["host".into()]), true)
    .set_column_dictionary_enabled(ColumnPath::new(vec!["service".into()]), true)
    .build();
```

**Expected Impact:** 50-90% smaller files, faster reads

---

### 4.3 Sorted Data Within Files

**Problem:** Data within files is in write order, not optimal for queries.

**Solution:** Sort data by (timestamp, primary_tag) before writing.

**Implementation:**
```rust
async fn write_batch(&self, batch: RecordBatch) -> Result<()> {
    // Sort by timestamp, then by primary tag
    let sorted = sort_batch(
        &batch,
        &[
            SortColumn { name: "timestamp", desc: false },
            SortColumn { name: "host", desc: false },
        ],
    )?;
    self.writer.write(&sorted).await
}
```

**Benefits:**
- Better compression (similar values adjacent)
- Faster time-range scans (data is clustered)
- Better row group pruning

---

### 4.4 Compression Tuning

**Problem:** Default compression may not balance speed and ratio optimally.

**Solution:** Use ZSTD with tuned compression level.

**Recommendations:**
| Compression | Speed | Ratio | Use Case |
|-------------|-------|-------|----------|
| SNAPPY | Fast | Low | Real-time queries |
| ZSTD(1) | Fast | Good | General use |
| ZSTD(3) | Medium | Better | Balanced (recommended) |
| ZSTD(9) | Slow | Best | Archival |

**Implementation:**
```rust
let writer_props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    .build();
```

---

## Phase 5: Pre-Aggregation & Rollups

**Timeline:** Week 8+
**Impact:** Very High
**Effort:** High

These optimizations pre-compute results for common query patterns.

### 5.1 Continuous Aggregation

**Problem:** Dashboard queries compute the same aggregations repeatedly.

**Solution:** Background process that continuously computes and stores aggregations.

**Architecture:**
```
┌─────────────────────────────────────────────────────────────────┐
│                    Continuous Aggregation                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Raw Data (1s resolution)                                        │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Aggregation Workers                             │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │ │
│  │  │ 1-minute │  │ 5-minute │  │ 1-hour   │  │ 1-day    │    │ │
│  │  │ rollup   │  │ rollup   │  │ rollup   │  │ rollup   │    │ │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │ │
│  │       │             │             │             │           │ │
│  └───────┼─────────────┼─────────────┼─────────────┼───────────┘ │
│          ▼             ▼             ▼             ▼             │
│       _rollup_1m/   _rollup_5m/  _rollup_1h/  _rollup_1d/       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Implementation:**
```rust
pub struct ContinuousAggregation {
    source_table: String,
    target_table: String,
    interval: Duration,
    aggregations: Vec<AggregationSpec>,
    group_by: Vec<String>,
}

#[derive(Clone)]
pub struct AggregationSpec {
    pub function: AggFunction,  // Sum, Avg, Min, Max, Count
    pub column: String,
    pub alias: String,
}

impl ContinuousAggregation {
    pub async fn process_window(&self, start: i64, end: i64) -> Result<()> {
        let sql = format!(
            "SELECT time_bucket(time, {}) as bucket, {}, {} FROM {} WHERE time >= {} AND time < {} GROUP BY bucket, {}",
            self.interval.as_nanos(),
            self.group_by.join(", "),
            self.aggregations.iter().map(|a| a.to_sql()).collect::<Vec<_>>().join(", "),
            self.source_table,
            start, end,
            self.group_by.join(", ")
        );

        let results = self.query_engine.execute(&sql).await?;
        self.write_to_rollup_table(results).await
    }
}
```

**Configuration:**
```toml
[[continuous_aggregation]]
name = "cpu_1m"
source = "metrics.default.cpu"
target = "metrics.default._rollup_cpu_1m"
interval = "1m"
group_by = ["host", "region"]
aggregations = [
    { function = "avg", column = "usage", alias = "avg_usage" },
    { function = "max", column = "usage", alias = "max_usage" },
    { function = "count", column = "*", alias = "count" },
]
```

**Query Routing:**
```rust
impl QueryEngine {
    fn select_best_source(&self, query: &Query) -> TableSource {
        let interval = query.get_time_bucket_interval();
        let range = query.get_time_range();

        // Use rollup if query interval >= rollup interval
        if let Some(rollup) = self.find_matching_rollup(interval, range) {
            return TableSource::Rollup(rollup);
        }
        TableSource::Raw
    }
}
```

**Expected Impact:** 100x+ faster for dashboard queries

---

### 5.2 Downsampling Tiers

**Problem:** Historical data at full resolution wastes storage and slows queries.

**Solution:** Automatically downsample old data to lower resolutions.

**Storage Tiers:**
| Age | Resolution | Storage |
|-----|------------|---------|
| 0-7 days | 1 second | Full resolution |
| 7-30 days | 1 minute | 60x reduction |
| 30-365 days | 1 hour | 3600x reduction |
| 1+ year | 1 day | 86400x reduction |

**Implementation:**
```rust
pub struct DownsamplingConfig {
    pub tiers: Vec<DownsamplingTier>,
}

pub struct DownsamplingTier {
    pub age_days: u32,
    pub resolution: Duration,
    pub aggregations: Vec<AggregationSpec>,
}
```

---

### 5.3 Materialized Views

**Problem:** Complex queries with JOINs or heavy aggregations are slow.

**Solution:** Pre-compute and store query results as materialized views.

**Implementation:**
```sql
-- Define materialized view
CREATE MATERIALIZED VIEW top_hosts AS
SELECT
    host,
    avg(cpu_usage) as avg_cpu,
    max(memory_usage) as max_memory
FROM metrics.default.system
WHERE time > now() - interval '1 hour'
GROUP BY host
REFRESH EVERY 5 MINUTES;

-- Query uses materialized view automatically
SELECT * FROM top_hosts WHERE avg_cpu > 80;
```

---

## Phase 6: Parallel & Async Execution

**Timeline:** Week 9+
**Impact:** Medium-High
**Effort:** High

These optimizations improve query execution parallelism.

### 6.1 Parallel File Scanning

**Problem:** Files within a partition are scanned sequentially.

**Solution:** Scan multiple files concurrently using a thread pool.

**Implementation:**
```rust
pub async fn scan_files(files: Vec<PathBuf>, parallelism: usize) -> Vec<RecordBatch> {
    let semaphore = Arc::new(Semaphore::new(parallelism));

    let futures: Vec<_> = files
        .into_iter()
        .map(|file| {
            let permit = semaphore.clone().acquire_owned();
            async move {
                let _permit = permit.await;
                read_parquet_file(&file).await
            }
        })
        .collect();

    futures::future::join_all(futures).await
}
```

**Configuration:**
```toml
[query.execution]
file_parallelism = 8      # Concurrent file reads
partition_parallelism = 4 # Concurrent partitions
```

---

### 6.2 Async I/O with io_uring (Linux)

**Problem:** Traditional async I/O has syscall overhead.

**Solution:** Use io_uring for true async file I/O on Linux.

**Implementation:**
```rust
#[cfg(target_os = "linux")]
pub async fn read_file_io_uring(path: &Path) -> Result<Vec<u8>> {
    use tokio_uring::fs::File;

    let file = File::open(path).await?;
    let buf = vec![0u8; file.metadata().await?.len() as usize];
    let (res, buf) = file.read_at(buf, 0).await;
    res?;
    Ok(buf)
}
```

**Expected Impact:** 20-30% better I/O throughput on Linux

---

### 6.3 SIMD Vectorized Aggregations

**Problem:** Scalar aggregation loops don't utilize CPU vector units.

**Solution:** Use SIMD instructions for aggregations (Arrow already supports this).

**Verification:**
```rust
// Ensure Arrow is compiled with SIMD support
#[cfg(target_arch = "x86_64")]
fn check_simd_support() {
    if is_x86_feature_detected!("avx2") {
        info!("AVX2 SIMD support enabled");
    }
}
```

**Compile Flags:**
```toml
# Cargo.toml
[profile.release]
lto = true
codegen-units = 1

# .cargo/config.toml
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "target-cpu=native"]
```

---

### 6.4 Partition-Level Parallelism

**Problem:** Date partitions are processed sequentially.

**Solution:** Process multiple partitions concurrently and merge results.

**Implementation:**
```rust
pub async fn query_parallel_partitions(
    partitions: Vec<String>,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    let tasks: Vec<_> = partitions
        .into_iter()
        .map(|partition| {
            let query = query.to_string();
            tokio::spawn(async move {
                query_single_partition(&partition, &query).await
            })
        })
        .collect();

    let results = futures::future::try_join_all(tasks).await?;
    merge_sorted_results(results)
}
```

---

## Benchmarking & Monitoring

### Key Metrics to Track

| Metric | Description | Target |
|--------|-------------|--------|
| Query Latency P50 | Median query time | < 100ms |
| Query Latency P99 | 99th percentile | < 1s |
| Files Scanned | Files read per query | Minimize |
| Rows Scanned | Rows read per query | Minimize |
| Cache Hit Rate | Query cache effectiveness | > 80% |
| I/O Bytes | Bytes read from disk | Minimize |

### Benchmark Queries

```sql
-- Point query (should use Bloom filter)
SELECT * FROM metrics WHERE host = 'server-12345' AND time > now() - '1h';

-- Time range scan (should use partition pruning)
SELECT avg(value) FROM metrics WHERE time BETWEEN '2024-01-01' AND '2024-01-02';

-- Aggregation with GROUP BY (should use pre-aggregation if available)
SELECT host, avg(cpu) FROM metrics WHERE time > now() - '1d' GROUP BY host;

-- Dashboard query (should hit cache)
SELECT time_bucket(time, '5m'), avg(value) FROM metrics
WHERE time > now() - '1h' GROUP BY 1 ORDER BY 1;
```

### Performance Testing Script

```bash
#!/bin/bash
# scripts/benchmark_queries.sh

QUERIES=(
    "SELECT count(*) FROM mydb.default.cpu"
    "SELECT * FROM mydb.default.cpu WHERE host = 'server01' LIMIT 100"
    "SELECT avg(usage) FROM mydb.default.cpu WHERE time > now() - interval '1 hour'"
    "SELECT host, avg(usage) FROM mydb.default.cpu GROUP BY host"
)

for query in "${QUERIES[@]}"; do
    echo "Query: $query"
    time curl -s "http://localhost:7000/query/mydb?sql=$(urlencode "$query")" > /dev/null
    echo "---"
done
```

---

## Implementation Priority Matrix

| Optimization | Impact | Effort | Priority | Dependencies |
|--------------|--------|--------|----------|--------------|
| 1.1 Metadata Cache | High | Low | P0 | None |
| 1.2 Tag Pushdown | High | Low | P0 | None |
| 1.4 Row Group Pruning | High | Low | P0 | None |
| 2.1 Bloom Filters | High | Medium | P1 | 1.x |
| 2.2 Time Index | High | Medium | P1 | 1.x |
| 3.1 Hot Partition Cache | High | Medium | P1 | 1.x |
| 3.4 Aggregation Cache | High | Medium | P2 | 3.1 |
| 4.2 Dictionary Encoding | Medium | Low | P2 | None |
| 4.3 Sorted Data | Medium | Medium | P2 | None |
| 5.1 Continuous Aggregation | Very High | High | P3 | 3.x |
| 5.2 Downsampling | High | High | P3 | 5.1 |
| 6.1 Parallel File Scan | Medium | Medium | P3 | None |

---

## Appendix A: Configuration Reference

```toml
# config.toml - Performance tuning options

[query]
# Enable custom table provider with optimizations
use_custom_table_provider = true

[query.execution]
# Maximum concurrent file reads
file_parallelism = 8
# Maximum concurrent partition processing
partition_parallelism = 4
# Target batch size for processing
batch_size = 8192

[query.cache]
# Query result cache
enabled = true
max_size_mb = 256
ttl_secs = 300
min_query_time_ms = 10

[cache.metadata]
# Parquet metadata cache
enabled = true
max_entries = 10000
ttl_secs = 3600

[cache.hot_partition]
# Hot partition cache
enabled = true
max_size_mb = 1024
hot_days = 2

[storage.parquet]
# Parquet file settings
row_group_size_mb = 64
page_size_kb = 1024
compression = "zstd"
compression_level = 3
dictionary_enabled = true
statistics_enabled = true

[storage.bloom_filter]
# Bloom filter settings
enabled = true
columns = ["host", "service", "region"]
false_positive_rate = 0.01

[storage.index]
# Index settings
time_index_enabled = true
tag_index_enabled = true
partition_index_enabled = true

[continuous_aggregation]
# Enable continuous aggregation
enabled = false
```

---

## Appendix B: Monitoring Queries

```sql
-- Check query cache hit rate
SELECT
    cache_hits,
    cache_misses,
    cache_hits::float / (cache_hits + cache_misses) * 100 as hit_rate_pct
FROM system.query_cache_stats;

-- Check file scan statistics
SELECT
    table_name,
    avg(files_scanned) as avg_files,
    avg(rows_scanned) as avg_rows,
    avg(bytes_read) as avg_bytes
FROM system.query_stats
WHERE time > now() - interval '1 hour'
GROUP BY table_name;

-- Check slow queries
SELECT
    query,
    duration_ms,
    files_scanned,
    rows_scanned
FROM system.query_log
WHERE duration_ms > 1000
ORDER BY duration_ms DESC
LIMIT 10;
```

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-23 | MynaDB Team | Initial document |
