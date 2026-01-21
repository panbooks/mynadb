# Query Performance Optimization Plans

Current status: Query throughput significantly lower than InfluxDB IOx and GreptimeDB

## Current Query Path

```
SQL Query
    → DataFusion parse/plan
    → Table registration (scan directory every query)
    → Parquet file discovery (list files)
    → Predicate pushdown (basic)
    → Sequential file reads
    → Result serialization
```

## Bottleneck Analysis

| Bottleneck | Impact | Current State |
|------------|--------|---------------|
| Table re-registration | High | Scans directory every query |
| No metadata cache | High | Reads Parquet footers every query |
| Partition pruning | Medium | Basic date filtering only |
| No result caching | Medium | Re-executes identical queries |
| Row group size | Medium | May not be optimal |
| No secondary indexes | High | Full scan for tag filters |
| Single-threaded file reads | High | Not leveraging parallelism |

---

## Optimization Plans

### 1. Persistent Table Registration with Metadata Cache (Critical)

**Problem:** Re-registering tables and scanning directories on every query.

**Current:**
```rust
async fn execute_query(&self, db: &str, sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    self.register_tables(&ctx, db).await?;  // Scans filesystem every time!
    ctx.sql(sql).await?.collect().await
}
```

**Solution:** Cache table metadata and SessionContext, invalidate on writes.

**Implementation:**
```rust
struct QueryEngine {
    // Cached session contexts per database
    contexts: RwLock<HashMap<String, Arc<CachedContext>>>,
    metadata_cache: Arc<MetadataCache>,
}

struct CachedContext {
    ctx: SessionContext,
    tables: HashSet<String>,
    last_updated: Instant,
    version: u64,  // Incremented on writes
}

struct MetadataCache {
    // Cache Parquet file metadata (footer, schema, statistics)
    file_metadata: DashMap<PathBuf, Arc<ParquetMetaData>>,
    // Cache partition info
    partitions: DashMap<TableKey, Vec<PartitionInfo>>,
}
```

**Expected Impact:** 10-50x improvement for repeated queries

---

### 2. Parquet Metadata Caching (Critical)

**Problem:** Reading Parquet footer from disk on every query.

**Current:** DataFusion reads footer for each file to get schema and statistics.

**Solution:** Cache Parquet metadata in memory.

**Implementation:**
```rust
use parquet::file::metadata::ParquetMetaData;
use parquet::arrow::async_reader::AsyncFileReader;

struct CachedParquetReader {
    inner: AsyncFileReader,
    metadata_cache: Arc<DashMap<String, Arc<ParquetMetaData>>>,
}

impl AsyncFileReader for CachedParquetReader {
    async fn get_metadata(&mut self) -> Result<Arc<ParquetMetaData>> {
        let path = self.path();
        if let Some(meta) = self.metadata_cache.get(&path) {
            return Ok(meta.clone());
        }
        let meta = self.inner.get_metadata().await?;
        self.metadata_cache.insert(path, meta.clone());
        Ok(meta)
    }
}
```

**Expected Impact:** 5-10x improvement for metadata-heavy queries

---

### 3. Smart Partition Pruning (High Impact)

**Problem:** Only basic date partition pruning, not leveraging time range in queries.

**Current:**
```sql
SELECT * FROM cpu WHERE date >= '2024-01-10'  -- Prunes by date partition
SELECT * FROM cpu WHERE timestamp > 1704844800000000000  -- No pruning!
```

**Solution:** Convert timestamp predicates to date predicates for pruning.

**Implementation:**
```rust
fn extract_time_range(expr: &Expr) -> Option<(i64, i64)> {
    // Parse expressions like:
    // timestamp > X  → (X, i64::MAX)
    // timestamp < X  → (i64::MIN, X)
    // timestamp BETWEEN X AND Y → (X, Y)
    // time > 'ISO8601' → parse and convert
}

fn timestamp_to_date_filter(time_range: (i64, i64)) -> Expr {
    let start_date = timestamp_nanos_to_date32(time_range.0);
    let end_date = timestamp_nanos_to_date32(time_range.1);
    col("date").between(lit(start_date), lit(end_date))
}
```

**Expected Impact:** 10-100x for time-bounded queries

---

### 4. Secondary Index for Tags (High Impact)

**Problem:** Tag filters require full Parquet scan.

**Query:** `SELECT * FROM cpu WHERE host = 'server01'`

**Current:** Scans all row groups, relies on Parquet statistics (min/max only).

**Solution:** Build inverted index for high-cardinality tags.

**Implementation:**
```rust
struct TagIndex {
    // tag_key -> tag_value -> list of (file, row_group) containing value
    index: HashMap<String, HashMap<String, Vec<FileRowGroup>>>,
}

struct FileRowGroup {
    file_path: PathBuf,
    row_group_idx: usize,
    row_range: Range<usize>,
}

impl TagIndex {
    fn build_from_parquet(path: &Path) -> Self {
        // Read Parquet, extract unique tag values per row group
        // Store mapping: tag_value -> [(file, row_group), ...]
    }

    fn lookup(&self, tag: &str, value: &str) -> Vec<FileRowGroup> {
        self.index.get(tag)
            .and_then(|m| m.get(value))
            .cloned()
            .unwrap_or_default()
    }
}
```

**Storage:** Persist as separate index files or embed in Parquet metadata.

**Expected Impact:** 10-1000x for selective tag queries

---

### 5. Bloom Filter Integration (Medium-High Impact)

**Problem:** Not using Bloom filters for string column filtering.

**Current:** Bloom filters written to Parquet but not used in queries.

**Solution:** Enable DataFusion's Bloom filter pruning.

**Implementation:**
```rust
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;

let parquet_options = ParquetReadOptions::default()
    .with_pushdown_filters(true)
    .with_reorder_filters(true)
    .with_enable_page_index(true)
    .with_bloom_filter_on_read(true);  // Enable Bloom filter

// Or via SessionConfig
let config = SessionConfig::new()
    .with_parquet_bloom_filter_on_read(true)
    .with_parquet_pruning(true);
```

**Expected Impact:** 2-10x for string equality filters

---

### 6. Query Result Caching (Medium Impact)

**Problem:** Identical queries re-execute full query plan.

**Solution:** Cache query results with TTL and invalidation.

**Implementation:**
```rust
struct QueryCache {
    cache: DashMap<QueryCacheKey, CachedResult>,
    max_size_mb: usize,
}

#[derive(Hash, Eq, PartialEq)]
struct QueryCacheKey {
    db: String,
    sql_hash: u64,  // Hash of normalized SQL
    table_versions: Vec<(String, u64)>,  // Table -> version
}

struct CachedResult {
    batches: Vec<RecordBatch>,
    created_at: Instant,
    ttl: Duration,
    size_bytes: usize,
}

impl QueryCache {
    fn get(&self, key: &QueryCacheKey) -> Option<Vec<RecordBatch>> {
        self.cache.get(key)
            .filter(|r| r.created_at.elapsed() < r.ttl)
            .map(|r| r.batches.clone())
    }

    fn invalidate_table(&self, db: &str, table: &str) {
        self.cache.retain(|k, _| !(k.db == db && k.sql_hash.contains_table(table)));
    }
}
```

**Expected Impact:** 100x+ for repeated identical queries

---

### 7. Parallel Parquet Reading (Medium Impact)

**Problem:** Sequential file reads don't saturate I/O bandwidth.

**Solution:** Configure DataFusion for parallel file reading.

**Implementation:**
```rust
let config = SessionConfig::new()
    .with_target_partitions(num_cpus::get())  // Parallel partitions
    .with_batch_size(8192)  // Optimal batch size
    .with_parquet_parallel_column_reading(true);  // Parallel column reads

// For object store
let object_store_config = ObjectStoreConfig::new()
    .with_max_concurrent_requests(32)
    .with_prefetch_size(4 * 1024 * 1024);  // 4MB prefetch
```

**Expected Impact:** 2-4x on multi-core systems with fast storage

---

### 8. Optimized Row Group Size (Medium Impact)

**Problem:** Sub-optimal row group size affects both read and filter performance.

**Current:** Using default or arbitrary row group size.

**Optimal Settings:**
- Row group size: 100K-1M rows (balance between parallelism and overhead)
- Page size: 1MB (good for predicate pushdown)
- Dictionary encoding: Enable for low-cardinality columns

**Implementation:**
```rust
let writer_props = WriterProperties::builder()
    .set_max_row_group_size(500_000)  // 500K rows per group
    .set_data_page_size_limit(1024 * 1024)  // 1MB pages
    .set_dictionary_enabled(true)
    .set_dictionary_page_size_limit(1024 * 1024)
    .build();
```

**Expected Impact:** 10-30% improvement in scan performance

---

### 9. Predicate Pushdown Optimization (Medium Impact)

**Problem:** Not all predicates pushed down to Parquet reader.

**Solution:** Ensure all supported predicates are pushed down.

**Implementation:**
```rust
// Configure DataFusion for aggressive pushdown
let config = SessionConfig::new()
    .with_parquet_pushdown_filters(true)
    .with_parquet_reorder_filters(true)
    .with_parquet_pruning(true)
    .set_bool("datafusion.execution.parquet.enable_page_index", true)
    .set_bool("datafusion.execution.parquet.pushdown_filters", true);

// Verify pushdown with EXPLAIN
// Look for "predicate=..." in ParquetExec node
```

**Expected Impact:** 2-5x for filtered queries

---

### 10. File Listing Cache (Medium Impact)

**Problem:** Directory listing on every query is slow.

**Solution:** Cache file listings, invalidate on writes.

**Implementation:**
```rust
struct FileListingCache {
    listings: DashMap<PathBuf, CachedListing>,
}

struct CachedListing {
    files: Vec<PathBuf>,
    last_modified: SystemTime,
    version: u64,
}

impl FileListingCache {
    async fn list_files(&self, dir: &Path) -> Vec<PathBuf> {
        if let Some(cached) = self.listings.get(dir) {
            // Check if directory modified
            let current_mtime = fs::metadata(dir).await?.modified()?;
            if current_mtime <= cached.last_modified {
                return cached.files.clone();
            }
        }
        // Refresh cache
        let files = self.scan_directory(dir).await?;
        self.listings.insert(dir.to_path_buf(), CachedListing { files: files.clone(), .. });
        files
    }

    fn invalidate(&self, dir: &Path) {
        self.listings.remove(dir);
    }
}
```

**Expected Impact:** 2-5x for tables with many files

---

### 11. Columnar Projection Pushdown (Low-Medium Impact)

**Problem:** Reading all columns even when only few needed.

**Solution:** Ensure column projection pushed to Parquet reader.

**Implementation:**
```rust
// DataFusion does this automatically, but verify:
// EXPLAIN SELECT col1, col2 FROM table
// Should show "projection=[col1, col2]" in ParquetExec

// For custom readers:
let projection = Some(vec![0, 2, 5]);  // Only columns 0, 2, 5
let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
    .with_projection(projection)
    .build()?;
```

**Expected Impact:** Proportional to columns skipped (50%+ for wide tables)

---

### 12. Memory Management (Low-Medium Impact)

**Problem:** Memory pressure causes spilling or OOM.

**Solution:** Configure memory limits and spilling.

**Implementation:**
```rust
let runtime = RuntimeEnv::new(
    RuntimeConfig::new()
        .with_memory_limit(4 * 1024 * 1024 * 1024, 0.8)  // 4GB, 80% threshold
        .with_disk_manager(DiskManagerConfig::NewOs)  // Enable spilling
)?;

let config = SessionConfig::new()
    .with_batch_size(8192)
    .with_target_partitions(8);

let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
```

**Expected Impact:** Prevents OOM, may improve cache efficiency

---

## Implementation Priority

| Priority | Optimization | Impact | Effort | Dependencies |
|----------|-------------|--------|--------|--------------|
| 1 | #1 Table/Context caching | Critical | Medium | None |
| 2 | #2 Parquet metadata cache | Critical | Medium | None |
| 3 | #3 Smart partition pruning | High | Medium | None |
| 4 | #5 Bloom filter integration | Medium-High | Low | Parquet config |
| 5 | #4 Secondary tag index | High | High | Design decision |
| 6 | #6 Query result caching | Medium | Medium | dashmap |
| 7 | #7 Parallel reading | Medium | Low | Config only |
| 8 | #10 File listing cache | Medium | Low | None |
| 9 | #9 Predicate pushdown | Medium | Low | Config only |
| 10 | #8 Row group tuning | Medium | Low | Writer config |

---

## Quick Wins (Config Changes Only)

```rust
// Add to query engine initialization
let config = SessionConfig::new()
    // Parallelism
    .with_target_partitions(num_cpus::get())
    .with_batch_size(8192)

    // Parquet optimizations
    .with_parquet_pruning(true)
    .with_parquet_pushdown_filters(true)
    .with_parquet_reorder_filters(true)
    .with_parquet_bloom_filter_on_read(true)

    // Memory
    .with_memory_limit(4 * 1024 * 1024 * 1024)  // 4GB
    ;
```

---

## Comparison with IOx/GreptimeDB

| Feature | IOx | GreptimeDB | Current | After Optimization |
|---------|-----|------------|---------|-------------------|
| Metadata cache | ✓ | ✓ | ✗ | ✓ |
| Partition pruning | ✓ | ✓ | Basic | Smart |
| Bloom filters | ✓ | ✓ | Write only | Read + Write |
| Inverted index | ✓ | ✓ | ✗ | Optional |
| Result cache | ✓ | ✓ | ✗ | ✓ |
| Parallel I/O | ✓ | ✓ | Limited | Full |
| Columnar pushdown | ✓ | ✓ | ✓ | ✓ |

---

## Benchmarking

```bash
# Query latency benchmark
hyperfine --warmup 3 \
  'curl -s "http://localhost:7000/query/mydb?sql=SELECT..."'

# Concurrent query throughput
wrk -t8 -c64 -d30s -s query.lua http://localhost:7000/

# Profile query execution
RUST_LOG=datafusion=debug ./target/release/tsdb-stats

# Explain query plan
curl "http://localhost:7000/explain/mydb?sql=SELECT...&verbose=true"
```

---

## Target Architecture

```
SQL Query
    → Parse/Plan (cached plans optional)
    → Check query cache → [HIT] Return cached result
    → Table lookup (cached metadata)
    → Partition pruning (date + timestamp)
    → Tag index lookup (if applicable)
    → Bloom filter check
    → Parallel Parquet reads (cached footers)
    → Predicate pushdown (page-level)
    → Columnar projection
    → Aggregate/Join execution
    → Cache result
    → Return
```

**Target:** Match or exceed IOx/GreptimeDB query throughput
