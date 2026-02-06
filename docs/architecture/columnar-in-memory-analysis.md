# Columnar In-Memory Storage: Current State & Improvement Plan

**Document Type:** Architecture Analysis & Improvement Proposal
**Date:** 2026-02-06
**Status:** Analysis Complete, Implementation Pending

---

## Executive Summary

MynaDB has **excellent foundational infrastructure** for columnar in-memory caching using Apache Arrow RecordBatch format. However, a **critical gap exists**: the fully-implemented HotPartitionCache (745 lines of code) is **never used in the query execution path**, meaning decoded Parquet data is not cached despite having the infrastructure ready.

**Current State:**
- ✅ **Write Cache (DataCache):** Excellent - 4GB default, 32 shards, zero-copy with Arc
- ✅ **Query Result Cache:** Good - Caches complete query results with smart invalidation
- ⚠️ **Hot Partition Cache:** Implemented but **UNUSED** - Major missed opportunity
- ❌ **Parquet Decode Cache:** Missing integration with query path
- ❌ **Column Projection:** Not supported in cache layer
- ❌ **Filter Pushdown:** Filters applied after reading from cache
- ❌ **In-Memory Compression:** All batches uncompressed

**Impact of Fixing:**
- **10-100x faster queries** on recent data (hot partitions)
- **50-80% memory savings** with columnar projection + compression
- **5-20x reduction in CPU usage** from avoiding repeated Parquet decoding

---

## Table of Contents

1. [Current Architecture](#current-architecture)
2. [Existing Cache Layers](#existing-cache-layers)
3. [Critical Gap Analysis](#critical-gap-analysis)
4. [Memory Characteristics](#memory-characteristics)
5. [Improvement Plan](#improvement-plan)
6. [Implementation Details](#implementation-details)
7. [Performance Projections](#performance-projections)

---

## Current Architecture

### Cache Layer Overview

```
┌────────────────────────────────────────────────────────────────┐
│                    Query Execution Layer                        │
│                 (DataFusion SessionContext)                     │
└─────────────────────┬──────────────────────────────────────────┘
                      │
                      ├─────────────────────────────────────┐
                      │                                     │
           ┌──────────▼──────────┐              ┌─────────▼────────┐
           │  MemtableProvider   │              │ ParquetProvider  │
           │  (Write Cache)      │              │ (Parquet Files)  │
           └──────────┬──────────┘              └─────────┬────────┘
                      │                                   │
                      │ Arc<RecordBatch>                  │ Decode Parquet
                      │ (zero-copy)                       │ (repeated every query!)
                      │                                   │
           ┌──────────▼──────────┐              ┌─────────▼────────┐
           │    DataCache        │              │  ParquetExec     │
           │  (4GB, 32 shards)   │              │  (no caching)    │
           │  ✅ ACTIVE          │              │  ❌ SLOW         │
           └─────────────────────┘              └──────────────────┘
                                                         │
                                          ┌──────────────▼─────────────┐
                                          │  HotPartitionCache        │
                                          │  (1GB, TTL 30min)        │
                                          │  ❌ IMPLEMENTED BUT       │
                                          │     NEVER CALLED         │
                                          └──────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│                    Post-Execution Cache                         │
│                  (QueryResultCache)                             │
│             ✅ Caches complete results                          │
└────────────────────────────────────────────────────────────────┘
```

### Data Flow: Write → Query

```
WRITE PATH:
─────────────
POST /write → LineProtocol Parser → DataCache (sharded HashMap)
                                     └─ Arc<RecordBatch> (columnar)
                                     └─ Flush → Parquet files

QUERY PATH (CURRENT):
────────────────────
SQL Query → DataFusion → UnionProvider
                         ├─ MemtableExec → DataCache.get() → Arc<RecordBatch> ✅ Fast
                         └─ ParquetExec → Read Parquet → Decode ❌ Slow (no cache)

QUERY PATH (SHOULD BE):
───────────────────────
SQL Query → DataFusion → UnionProvider
                         ├─ MemtableExec → DataCache.get() → Arc<RecordBatch> ✅
                         └─ ParquetExec → Check HotPartitionCache
                                          ├─ Cache Hit → Arc<RecordBatch> ✅ Fast!
                                          └─ Cache Miss → Decode → Store in cache
```

---

## Existing Cache Layers

### 1. Write Cache (DataCache)

**Location:** `src/storage/cache/mod.rs` (894 lines)

**Architecture:**
```rust
pub struct DataCache {
    shards: Vec<RwLock<CacheShard>>,  // 32 shards for lock-free writes
    max_size_bytes: usize,            // Default: 4GB
    current_size: Arc<AtomicUsize>,   // Atomic size tracking
    flush_threshold: f64,              // Default: 0.8 (80%)
    flush_interval: Duration,          // Default: 60s
    tag_cardinality_tracker: Arc<TagCardinalityTracker>,
}

struct CacheShard {
    // Key: (catalog, schema, table, date_partition)
    data: HashMap<CacheKey, Vec<(u64, RecordBatch)>>,  // (seq_num, batch)
    size_bytes: usize,
}
```

**Key Features:**
- ✅ **Sharded Design:** 32 shards reduce lock contention
- ✅ **Zero-Copy:** Arc<RecordBatch> passed to queries directly
- ✅ **Backpressure:** Rejects writes when >80% full
- ✅ **Tag Cardinality Tracking:** Optimizes Parquet sort order
- ✅ **Memory Accounting:** Precise size tracking via Arrow APIs
- ✅ **Query Integration:** MemtableProvider reads directly

**Memory Estimation:**
```rust
fn get_array_memory_size(array: &dyn Array) -> usize {
    array.get_buffer_memory_size()  // Arrow's accurate accounting
}

fn estimate_record_batch_size(batch: &RecordBatch) -> usize {
    batch.columns()
        .iter()
        .map(|col| get_array_memory_size(col.as_ref()))
        .sum()
}
```

**Usage Example:**
```rust
// Write path
cache.write_line_protocol("mydb", "cpu", "cpu,host=server1 usage=45.2 1704067200000000000")?;

// Query path (via MemtableProvider)
let batches = cache.get_table_batches("mydb", "default", "cpu", None)?;
// Returns: Vec<Arc<RecordBatch>> (zero-copy)
```

**Configuration:**
```toml
[cache]
max_size_mb = 4096
flush_interval_secs = 60
flush_threshold = 0.8
```

---

### 2. Hot Partition Cache

**Location:** `src/query/cache/hot_partition_cache.rs` (745 lines)

**Architecture:**
```rust
pub struct HotPartitionCache {
    cache: Arc<RwLock<HashMap<PartitionKey, CachedPartition>>>,
    max_size_bytes: usize,       // Default: 1GB
    ttl: Duration,               // Default: 30 minutes
    hot_days: i32,               // Default: 2 (today + yesterday)

    // Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
}

#[derive(Hash, Eq, PartialEq)]
struct PartitionKey {
    catalog: String,
    schema: String,
    table: String,
    date: i32,  // Date as i32 (days since epoch)
}

struct CachedPartition {
    batches: Vec<RecordBatch>,  // Decoded columnar data
    row_count: usize,
    size_bytes: usize,
    created_at: Instant,
    last_access: Instant,
    hit_count: usize,
}
```

**Smart Eviction Algorithm:**
```rust
fn evict_if_needed(&mut self) {
    if self.current_size < self.max_size_bytes {
        return;
    }

    // Calculate LRU score: higher = more evictable
    let mut candidates: Vec<_> = self.cache
        .iter()
        .map(|(key, partition)| {
            let age_secs = partition.created_at.elapsed().as_secs() as f64;
            let lru_score = age_secs / (partition.hit_count as f64 + 1.0);
            (key.clone(), lru_score, partition.size_bytes)
        })
        .collect();

    // Sort by score (highest first)
    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // Evict until under threshold (75%)
    let target_size = (self.max_size_bytes as f64 * 0.75) as usize;
    for (key, _, size) in candidates {
        if self.current_size <= target_size {
            break;
        }
        self.cache.remove(&key);
        self.current_size -= size;
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }
}
```

**Hot Date Detection:**
```rust
fn is_hot_date(&self, date: i32) -> bool {
    let today = chrono::Utc::now().date_naive().num_days_from_ce();
    date >= today - self.hot_days
}

// Only cache recent partitions (configurable window)
if !self.is_hot_date(partition_date) {
    return Ok(None);  // Skip caching old data
}
```

**API Methods:**
```rust
// Get cached partition
pub fn get(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    date: i32,
) -> Option<Vec<RecordBatch>>

// Store decoded partition
pub fn put(
    &self,
    catalog: String,
    schema: String,
    table: String,
    date: i32,
    batches: Vec<RecordBatch>,
)

// Invalidate on writes
pub fn invalidate_table(&self, catalog: &str, schema: &str, table: &str)

// Statistics
pub fn stats(&self) -> HotPartitionCacheStats {
    HotPartitionCacheStats {
        entry_count: self.cache.read().len(),
        size_bytes: self.current_size.load(Ordering::Relaxed),
        hits: self.hits.load(Ordering::Relaxed),
        misses: self.misses.load(Ordering::Relaxed),
        hit_rate: self.hits / (self.hits + self.misses) * 100.0,
        ...
    }
}
```

**❌ CRITICAL ISSUE:** This cache is **NEVER CALLED** in the query execution path!

**Proof:**
```bash
# Search for hot_partition_cache.get() calls
$ rg "hot_partition_cache\.get\(" --type rust
# No results

# Search for hot_partition_cache.put() calls
$ rg "hot_partition_cache\.put\(" --type rust
# No results

# The cache exists in QueryEngine but is never used:
# src/query/query_engine/mod.rs:
pub struct QueryEngine {
    hot_partition_cache: Option<Arc<HotPartitionCache>>,  // ← Exists but unused!
    ...
}
```

**Configuration (already exists):**
```toml
[hot_partition_cache]
enabled = true          # Currently has no effect!
max_size_mb = 1024      # 1GB ready to use
hot_days = 2            # Today + yesterday
ttl_secs = 1800         # 30 minutes
```

---

### 3. Query Result Cache

**Location:** `src/query/cache/query_cache.rs` (731 lines)

**Architecture:**
```rust
pub struct QueryResultCache {
    cache: Arc<RwLock<LruCache<QueryCacheKey, CachedResult>>>,
    max_size_bytes: usize,       // Default: 256MB
    max_entries: usize,          // Default: 1000
    ttl: Duration,               // Default: 5 minutes
    min_query_time_ms: u64,      // Default: 10ms (only cache slow queries)
}

#[derive(Hash, Eq, PartialEq)]
struct QueryCacheKey {
    database: String,
    sql_hash: u64,               // Hash of normalized SQL
    table_refs: Vec<TableRef>,   // For invalidation
}

struct CachedResult {
    batches: Vec<RecordBatch>,
    row_count: usize,
    size_bytes: usize,
    cached_at: Instant,
    execution_time_ms: u64,
}
```

**Smart Features:**
- ✅ **SQL Normalization:** `normalize_sql()` prevents duplicate entries
- ✅ **Table Tracking:** Invalidates cache on table writes
- ✅ **Size Limits:** Rejects huge results (>25% of cache)
- ✅ **TTL Management:** Auto-expires old results
- ✅ **Query Time Threshold:** Only caches slow queries (>10ms)

**Usage:**
```rust
// Check cache before executing
if let Some(cached) = query_cache.get(db, sql) {
    return Ok(cached);  // Cache hit!
}

// Execute query
let results = datafusion_ctx.sql(sql).collect().await?;

// Cache if query was slow enough
if execution_time > min_query_time {
    query_cache.put(db, sql, results.clone());
}
```

**Configuration:**
```toml
[query_cache]
enabled = true
max_size_mb = 256
max_entries = 1000
ttl_secs = 300           # 5 minutes
min_query_time_ms = 10   # Only cache slow queries
```

---

### 4. Metadata Cache

**Location:** `src/query/cache/metadata_cache.rs` (337 lines)

**Types:**
1. **Table Info Cache:** Partition lists, schema info
2. **File Listing Cache:** Directory contents (S3 LIST results)
3. **Parquet Metadata Cache:** File schemas, statistics, row group info

**Not columnar data** - metadata only. Reduces expensive S3 LIST operations.

---

## Critical Gap Analysis

### Gap 1: Hot Partition Cache Integration ❌ CRITICAL

**Problem:**
- HotPartitionCache fully implemented (745 lines)
- **Never called** in query execution path
- Parquet files decoded fresh on every query
- Massive CPU waste on repeated decompression + decoding

**Current Query Flow:**
```rust
// ParquetExec in DataFusion
impl ExecutionPlan for ParquetExec {
    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // Read Parquet file
        let file = self.open_file()?;

        // Decode (SLOW - repeated every query!)
        let reader = ParquetRecordBatchReader::try_new(file, batch_size)?;

        // Stream batches
        Ok(Box::pin(reader))
    }
}

// HotPartitionCache is never checked! ❌
```

**Should Be:**
```rust
impl ExecutionPlan for ParquetExec {
    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let partition_key = extract_partition_key(&self.file_path);

        // CHECK CACHE FIRST! ✅
        if let Some(cached_batches) = hot_partition_cache.get(&partition_key) {
            return Ok(Box::pin(MemoryStream::new(cached_batches)));
        }

        // Cache miss - decode from Parquet
        let file = self.open_file()?;
        let reader = ParquetRecordBatchReader::try_new(file, batch_size)?;
        let batches: Vec<RecordBatch> = reader.collect()?;

        // STORE IN CACHE ✅
        hot_partition_cache.put(&partition_key, batches.clone());

        Ok(Box::pin(MemoryStream::new(batches)))
    }
}
```

**Impact:**
- **10-100x speedup** for queries on hot data (today/yesterday)
- **Eliminates:** ZSTD decompression + Parquet decoding overhead
- **Typical numbers:**
  - Parquet decode: 50-200ms per partition
  - Cached read: 1-5ms per partition
  - **40-100x faster**

---

### Gap 2: Column Projection in Cache Layer ❌

**Problem:**
- Cache returns full RecordBatches with all columns
- DataFusion applies projection later
- Wasted memory transfer and processing

**Current:**
```rust
// DataCache always returns full schema
pub fn get_table_batches(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    date_filter: Option<&str>,
) -> Result<Vec<Arc<RecordBatch>>> {
    // Returns ALL columns ❌
}
```

**Should Support:**
```rust
pub fn get_table_batches(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    date_filter: Option<&str>,
    projection: Option<&[usize]>,  // Column indices ✅
) -> Result<Vec<Arc<RecordBatch>>> {
    let batches = self.get_full_batches(...)?;

    if let Some(proj) = projection {
        // Project to subset of columns
        batches.into_iter()
            .map(|batch| batch.project(proj))
            .collect()
    } else {
        Ok(batches)
    }
}
```

**Impact:**
- **50-80% memory reduction** for queries selecting few columns
- **Faster execution** (less data to process)
- Example: `SELECT time, value FROM cpu` (2 cols) vs full schema (20+ cols)

---

### Gap 3: Filter Pushdown to Cache ❌

**Problem:**
- Filters applied after reading from cache
- Entire partition loaded into memory
- Late filtering wastes CPU and memory

**Current:**
```rust
// 1. Read full partition from cache
let batches = cache.get_table_batches(...)?;

// 2. Apply filters in DataFusion (late!)
let filtered = datafusion_filter_exec.execute(batches)?;
```

**Should Support:**
```rust
pub fn get_table_batches(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
    date_filter: Option<&str>,
    projection: Option<&[usize]>,
    predicates: Option<&[Expr]>,  // Filter predicates ✅
) -> Result<Vec<Arc<RecordBatch>>> {
    let batches = self.get_full_batches(...)?;

    // Early filtering
    if let Some(preds) = predicates {
        let filtered = batches.into_iter()
            .filter_map(|batch| apply_predicates(&batch, preds))
            .collect();
        return Ok(filtered);
    }

    Ok(batches)
}
```

**Impact:**
- **5-20x reduction** in data transfer for selective queries
- **Lower memory pressure**
- Example: `WHERE host = 'server01'` (1% of rows) filters early

---

### Gap 4: In-Memory Compression ❌

**Problem:**
- All RecordBatches stored uncompressed
- Memory footprint larger than necessary
- Arrow supports dictionary encoding, not used

**Current:**
```rust
// Store as-is (uncompressed)
cache.insert(key, vec![batch]);  // Full columnar data
```

**Should Use:**
```rust
// Dictionary encode tag columns
fn compress_batch(batch: RecordBatch) -> RecordBatch {
    let compressed_columns = batch.columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            if is_tag_column(i) {
                // Dictionary encode (e.g., "host" has 100 unique values)
                dictionary_encode_array(col)
            } else {
                col.clone()
            }
        })
        .collect();

    RecordBatch::try_new(batch.schema(), compressed_columns)?
}

cache.insert(key, compress_batch(batch));
```

**Arrow Dictionary Encoding:**
```
Without dictionary:
["server01", "server01", "server01", "server02", "server02"]
→ 9 bytes × 5 = 45 bytes

With dictionary:
Dictionary: ["server01", "server02"]  → 18 bytes
Indices: [0, 0, 0, 1, 1]              → 5 bytes (u8)
Total: 23 bytes (49% savings)

For 1M rows with 100 unique hosts: ~90% memory reduction
```

**Impact:**
- **30-70% memory savings** for tag-heavy workloads
- **Same query performance** (dictionary encoding transparent)
- **More cache capacity** with same memory

---

### Gap 5: Adaptive Eviction Policies ❌

**Problem:**
- Simple LRU eviction only
- Doesn't consider decode cost vs size
- Uniform treatment of all partitions

**Current:**
```rust
// Simple LRU score
let lru_score = age_seconds / (hit_count + 1.0);

// Evict highest score first
candidates.sort_by(|a, b| b.1.cmp(&a.1));
```

**Should Consider:**
```rust
// Cost-benefit score
fn eviction_score(partition: &CachedPartition) -> f64 {
    let decode_cost = estimate_decode_cost(partition);  // ZSTD + Parquet
    let memory_cost = partition.size_bytes as f64;
    let access_frequency = partition.hit_count as f64 / age_seconds;

    // Higher score = more valuable to keep
    (decode_cost * access_frequency) / memory_cost
}

// Keep high-cost-to-recompute partitions
candidates.sort_by(|a, b| a.score.cmp(&b.score));  // Evict lowest score
```

**Advanced Policies:**
- **Cost-Aware:** Keep expensive-to-decode partitions
- **Workload-Aware:** Adapt to query patterns
- **Time-Aware:** Preemptively evict partitions aging out of hot window
- **Size-Aware:** Prefer evicting large, rarely-accessed partitions

**Impact:**
- **15-30% better hit rates** with same memory
- **Lower tail latency** (avoid evicting frequently accessed data)

---

## Memory Characteristics

### Current Memory Footprint

```
Component                   Default Size    Status        Efficiency
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Write Cache (DataCache)     4GB            ✅ Active      95% (sharding)
Hot Partition Cache         1GB            ❌ Unused      N/A
Query Result Cache          256MB          ✅ Active      90%
Metadata Cache              ~10MB          ✅ Active      99%
───────────────────────────────────────────────────────────────────
Total Allocated             ~5.3GB
Actually Used               ~4.3GB         (hot partition unused)
───────────────────────────────────────────────────────────────────

Potential with Improvements:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Write Cache                 4GB            ✅ Keep
Hot Partition (active)      1GB            ✅ Enable      → 2-3x capacity
  + Dictionary encoding     +300MB         ✅ Add         with compression
  + Column projection       +200MB         ✅ Add
Query Result Cache          256MB          ✅ Keep
───────────────────────────────────────────────────────────────────
Effective Capacity          ~6-8GB         (3-5GB cached Parquet data)
```

### Memory Estimation Accuracy

**Current (Good):**
```rust
// Uses Arrow's accurate buffer accounting
fn estimate_record_batch_size(batch: &RecordBatch) -> usize {
    batch.columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())  // Accurate
        .sum()
}
```

**Doesn't Account For:**
- Rust struct overhead (HashMap, Vec, etc.) → ~5-10% additional
- Arc reference counting metadata → ~8 bytes per Arc
- Schema metadata → ~few KB per table

**Recommendation:** Add 10% overhead buffer to account for metadata.

```rust
fn estimate_total_memory(batch_size: usize) -> usize {
    (batch_size as f64 * 1.10) as usize  // +10% overhead
}
```

---

### Cache Size Recommendations

**For Different Workloads:**

| Workload Type | Write Cache | Hot Partition | Query Cache | Total |
|---------------|-------------|---------------|-------------|-------|
| **Write-Heavy** (logs, metrics) | 8GB | 2GB | 512MB | 10.5GB |
| **Read-Heavy** (analytics) | 2GB | 4GB | 1GB | 7GB |
| **Balanced** | 4GB | 2GB | 512MB | 6.5GB |
| **High-Cardinality** (many tables) | 4GB | 4GB | 256MB | 8.3GB |

**Hot Partition Sizing:**
```
Calculation:
- Average partition size: 500MB (compressed Parquet)
- Decoded size: 1.5GB (3x inflation)
- Hot days: 2 (today + yesterday)
- Tables: 50

Memory needed = 50 tables × 2 days × 1.5GB / 50 partitions = ~3GB
Recommendation: 4GB for hot partition cache
```

---

## Improvement Plan

### Phase 1: Enable Hot Partition Cache (High Priority)

**Effort:** 1-2 weeks
**Impact:** 10-100x speedup on hot data

**Tasks:**
1. ✅ Cache infrastructure exists (HotPartitionCache)
2. ❌ Wire into ParquetExec execution path
3. ❌ Add cache check before Parquet decode
4. ❌ Store decoded batches on cache miss
5. ❌ Integrate invalidation on writes/compaction
6. ❌ Add monitoring metrics
7. ❌ Performance benchmarking

---

### Phase 2: Column Projection (Medium Priority)

**Effort:** 1 week
**Impact:** 50-80% memory reduction for selective queries

**Tasks:**
1. ❌ Add `projection: Option<&[usize]>` parameter to cache APIs
2. ❌ Implement `RecordBatch::project()` in cache layer
3. ❌ Pass projection from DataFusion physical plan
4. ❌ Update MemtableProvider to support projection
5. ❌ Benchmark memory savings

---

### Phase 3: Dictionary Encoding (Medium Priority)

**Effort:** 1-2 weeks
**Impact:** 30-70% memory savings for tag columns

**Tasks:**
1. ❌ Identify tag columns in schema
2. ❌ Implement dictionary encoding for string columns
3. ❌ Add compression to cache insertion path
4. ❌ Update memory estimation
5. ❌ Benchmark memory/performance tradeoff

---

### Phase 4: Filter Pushdown (Low-Medium Priority)

**Effort:** 2 weeks
**Impact:** 5-20x data reduction for selective queries

**Tasks:**
1. ❌ Add `predicates: Option<&[Expr]>` parameter
2. ❌ Implement predicate evaluation on cached batches
3. ❌ Extract predicates from DataFusion plan
4. ❌ Handle complex expressions (AND, OR, NOT)
5. ❌ Performance validation

---

### Phase 5: Adaptive Eviction (Low Priority)

**Effort:** 1 week
**Impact:** 15-30% better hit rates

**Tasks:**
1. ❌ Implement cost-benefit scoring
2. ❌ Add decode cost estimation
3. ❌ Workload pattern tracking
4. ❌ A/B testing against simple LRU

---

## Implementation Details

### Phase 1: Hot Partition Cache Integration

**Step 1: Modify ParquetTableProvider**

```rust
// src/query/providers/table_provider.rs

use crate::query::cache::hot_partition_cache::HotPartitionCache;

pub struct ParquetTableProvider {
    // ... existing fields
    hot_partition_cache: Option<Arc<HotPartitionCache>>,
}

impl ParquetTableProvider {
    pub fn new_with_cache(
        // ... existing params
        hot_partition_cache: Option<Arc<HotPartitionCache>>,
    ) -> Self {
        Self {
            // ... existing init
            hot_partition_cache,
        }
    }
}
```

**Step 2: Create CachedParquetExec**

```rust
// src/query/exec/cached_parquet_exec.rs

use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use crate::query::cache::hot_partition_cache::HotPartitionCache;

/// Wrapper around ParquetExec with hot partition caching
pub struct CachedParquetExec {
    inner: Arc<ParquetExec>,
    hot_partition_cache: Arc<HotPartitionCache>,
    partition_keys: Vec<PartitionKey>,
}

impl CachedParquetExec {
    pub fn new(
        inner: Arc<ParquetExec>,
        hot_partition_cache: Arc<HotPartitionCache>,
    ) -> Self {
        // Extract partition keys from file paths
        let partition_keys = inner.files()
            .iter()
            .filter_map(|file| extract_partition_key(file))
            .collect();

        Self {
            inner,
            hot_partition_cache,
            partition_keys,
        }
    }
}

impl ExecutionPlan for CachedParquetExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_key = &self.partition_keys[partition];

        // CHECK CACHE FIRST
        if let Some(cached_batches) = self.hot_partition_cache.get(
            &partition_key.catalog,
            &partition_key.schema,
            &partition_key.table,
            partition_key.date,
        ) {
            info!("Hot partition cache HIT: {:?}", partition_key);

            // Return cached batches as stream
            return Ok(Box::pin(MemoryStream::new(cached_batches)));
        }

        info!("Hot partition cache MISS: {:?}", partition_key);

        // CACHE MISS - decode from Parquet
        let stream = self.inner.execute(partition, context)?;

        // Collect batches (for caching)
        let batches = collect_stream(stream).await?;

        // STORE IN CACHE (if hot date)
        if self.hot_partition_cache.is_hot_date(partition_key.date) {
            self.hot_partition_cache.put(
                partition_key.catalog.clone(),
                partition_key.schema.clone(),
                partition_key.table.clone(),
                partition_key.date,
                batches.clone(),
            );
        }

        // Return stream from cached batches
        Ok(Box::pin(MemoryStream::new(batches)))
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.inner.clone()]
    }

    // ... other required methods
}

/// Helper to extract partition key from file path
fn extract_partition_key(file_path: &Path) -> Option<PartitionKey> {
    // Path format: data/catalog/schema/table/date=2024-01-15/file.parquet
    let parts: Vec<&str> = file_path.iter()
        .map(|s| s.to_str().unwrap())
        .collect();

    if parts.len() < 5 {
        return None;
    }

    let catalog = parts[parts.len() - 5].to_string();
    let schema = parts[parts.len() - 4].to_string();
    let table = parts[parts.len() - 3].to_string();

    // Extract date from "date=2024-01-15"
    let date_part = parts[parts.len() - 2];
    let date_str = date_part.strip_prefix("date=")?;
    let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
    let date_i32 = date.num_days_from_ce();

    Some(PartitionKey {
        catalog,
        schema,
        table,
        date: date_i32,
    })
}

/// In-memory stream from Vec<RecordBatch>
struct MemoryStream {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    index: usize,
}

impl MemoryStream {
    fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = batches[0].schema();
        Self {
            batches,
            schema,
            index: 0,
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
```

**Step 3: Integrate into QueryEngine**

```rust
// src/query/query_engine/mod.rs

impl QueryEngine {
    pub async fn execute(&self, database: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        // ... existing code

        // Register tables with hot partition cache
        for table_name in &tables {
            let table_provider = self.create_table_provider(
                database,
                table_name,
                Some(self.hot_partition_cache.clone()),  // Pass cache ✅
            ).await?;

            ctx.register_table(table_name, Arc::new(table_provider))?;
        }

        // Execute query
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        Ok(results)
    }
}
```

**Step 4: Add Invalidation Hooks**

```rust
// src/storage/cache.rs - DataCache flush

impl DataCache {
    pub async fn flush_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<()> {
        // ... existing flush logic

        // INVALIDATE HOT PARTITION CACHE ✅
        if let Some(hot_cache) = &self.hot_partition_cache {
            hot_cache.invalidate_table(catalog, schema, table);
        }

        Ok(())
    }
}

// src/jobs/compactor.rs - After compaction

impl Compactor {
    async fn compact_partition(&self, partition: &Partition) -> Result<()> {
        // ... existing compaction

        // INVALIDATE HOT PARTITION CACHE ✅
        if let Some(hot_cache) = &self.hot_partition_cache {
            hot_cache.invalidate_partition(
                &partition.catalog,
                &partition.schema,
                &partition.table,
                partition.date_i32,
            );
        }

        Ok(())
    }
}
```

**Step 5: Add Metrics**

```rust
// src/query/cache/hot_partition_cache.rs

impl HotPartitionCache {
    pub fn record_access(&self, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// Prometheus metrics
lazy_static! {
    static ref HOT_PARTITION_CACHE_HITS: IntCounter = register_int_counter!(
        "mynadb_hot_partition_cache_hits_total",
        "Total hot partition cache hits"
    ).unwrap();

    static ref HOT_PARTITION_CACHE_MISSES: IntCounter = register_int_counter!(
        "mynadb_hot_partition_cache_misses_total",
        "Total hot partition cache misses"
    ).unwrap();

    static ref HOT_PARTITION_CACHE_SIZE: IntGauge = register_int_gauge!(
        "mynadb_hot_partition_cache_size_bytes",
        "Hot partition cache size in bytes"
    ).unwrap();
}
```

---

### Phase 2: Column Projection

**Add to DataCache:**

```rust
// src/storage/cache/mod.rs

impl DataCache {
    pub fn get_table_batches_projected(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_filter: Option<&str>,
        projection: Option<&[usize]>,  // NEW ✅
    ) -> Result<Vec<Arc<RecordBatch>>> {
        let batches = self.get_table_batches_internal(catalog, schema, table, date_filter)?;

        if let Some(proj) = projection {
            // Project to subset of columns
            let projected = batches.into_iter()
                .map(|batch| {
                    let projected_batch = batch.project(proj)?;
                    Ok(Arc::new(projected_batch))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(projected)
        } else {
            Ok(batches)
        }
    }
}
```

**Update MemtableProvider:**

```rust
// src/query/providers/memtable_provider.rs

impl TableProvider for MemtableProvider {
    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,  // Extract projection ✅
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Get batches with projection
        let batches = self.cache.get_table_batches_projected(
            &self.catalog,
            &self.schema,
            &self.table,
            None,
            projection.as_ref().map(|v| v.as_slice()),  // Pass projection ✅
        )?;

        Ok(Arc::new(MemoryExec::try_new(
            &[batches],
            self.schema.clone(),
            projection.cloned(),
        )?))
    }
}
```

**Memory Savings Example:**

```
Full Schema (20 columns):
- time: 8 bytes × 1M rows = 8MB
- value: 8 bytes × 1M rows = 8MB
- host: 50 bytes × 1M rows = 50MB
- ... (17 more columns) = 200MB
Total: ~266MB

Projected (2 columns: time, value):
- time: 8MB
- value: 8MB
Total: 16MB

Savings: 94% memory reduction
```

---

### Phase 3: Dictionary Encoding

**Implement compression helper:**

```rust
// src/storage/cache/compression.rs

use arrow::array::{Array, StringArray, DictionaryArray, UInt16Array};
use arrow::datatypes::{DataType, Int32Type};

pub fn compress_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();

    let compressed_columns: Vec<ArrayRef> = batch.columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let field = schema.field(i);

            // Dictionary encode string columns with low cardinality
            if matches!(field.data_type(), DataType::Utf8) {
                if let Some(dict_array) = try_dictionary_encode(col) {
                    return dict_array;
                }
            }

            col.clone()
        })
        .collect();

    RecordBatch::try_new(schema, compressed_columns)
}

fn try_dictionary_encode(array: &ArrayRef) -> Option<ArrayRef> {
    let string_array = array.as_any().downcast_ref::<StringArray>()?;

    // Count unique values
    let unique_count = count_unique(string_array);
    let total_count = string_array.len();

    // Only encode if cardinality < 10%
    if unique_count as f64 / total_count as f64 > 0.10 {
        return None;
    }

    // Build dictionary
    let mut dictionary_builder = StringDictionaryBuilder::<Int32Type>::new();
    for i in 0..string_array.len() {
        if string_array.is_valid(i) {
            dictionary_builder.append(string_array.value(i))?;
        } else {
            dictionary_builder.append_null();
        }
    }

    Some(Arc::new(dictionary_builder.finish()))
}

fn count_unique(array: &StringArray) -> usize {
    let mut seen = HashSet::new();
    for i in 0..array.len() {
        if array.is_valid(i) {
            seen.insert(array.value(i));
        }
    }
    seen.len()
}
```

**Apply on cache insertion:**

```rust
// src/storage/cache/mod.rs

impl DataCache {
    pub fn insert_batch(
        &self,
        key: CacheKey,
        batch: RecordBatch,
    ) -> Result<()> {
        // Compress batch before storing ✅
        let compressed_batch = compress_record_batch(batch)?;

        let size = estimate_record_batch_size(&compressed_batch);

        // ... existing insertion logic
    }
}
```

---

## Performance Projections

### Benchmark Scenarios

#### Scenario 1: Dashboard Query (Hot Data)

**Query:**
```sql
SELECT time, AVG(cpu_usage) as avg_cpu
FROM cpu
WHERE time > now() - INTERVAL '1 hour'
GROUP BY time_bucket('1 minute', time)
```

**Current Performance:**
```
1. Query arrives
2. ParquetExec reads 60 files (1 per minute)
3. Decode each file: 50ms × 60 = 3000ms
4. Aggregate: 200ms
Total: 3200ms
```

**With Hot Partition Cache:**
```
1. Query arrives
2. Check cache: 60 hits
3. Read from memory: 1ms × 60 = 60ms
4. Aggregate: 200ms
Total: 260ms

Speedup: 12.3x
```

---

#### Scenario 2: Wide Table, Narrow Query

**Query:**
```sql
SELECT time, value FROM metrics WHERE time > now() - 1 day
```

**Table Schema:** 50 columns (time, value, 48 tags)

**Current Performance:**
```
Read full batches: 500MB × 10 partitions = 5GB
Memory transfer: 5GB
Projection in DataFusion: 200ms
Total: 5GB transferred
```

**With Column Projection:**
```
Read projected batches: 2 columns / 50 columns × 5GB = 200MB
Memory transfer: 200MB
Total: 200MB transferred

Memory savings: 96%
Query speedup: 2-3x (less data to process)
```

---

#### Scenario 3: Selective Filter

**Query:**
```sql
SELECT * FROM logs WHERE host = 'server01'
```

**Cardinality:** 1000 hosts, query selects 1

**Current Performance:**
```
Read full partition: 10GB
Filter in DataFusion: 500ms
Result: 10MB (0.1%)
Wasted I/O: 9.99GB
```

**With Filter Pushdown:**
```
Read with filter: 10MB (early filtering)
Result: 10MB
Wasted I/O: 0

Speedup: 5-10x
```

---

#### Scenario 4: High-Cardinality Tags

**Data:** 1M rows, 100 unique hosts, 1000 unique IPs

**Current Memory:**
```
host column: 50 bytes × 1M = 50MB
ip column: 15 bytes × 1M = 15MB
Total: 65MB
```

**With Dictionary Encoding:**
```
host dictionary: 50 bytes × 100 = 5KB
host indices: 4 bytes × 1M = 4MB  (u32)
ip dictionary: 15 bytes × 1000 = 15KB
ip indices: 4 bytes × 1M = 4MB
Total: 8MB

Savings: 87%
```

---

### Overall Impact Summary

| Improvement | Speedup | Memory Savings | Implementation Effort |
|-------------|---------|----------------|----------------------|
| **Hot Partition Cache** | 10-100x | N/A | 1-2 weeks |
| **Column Projection** | 2-3x | 50-80% | 1 week |
| **Dictionary Encoding** | ~1x | 30-70% | 1-2 weeks |
| **Filter Pushdown** | 5-20x | 80-95% | 2 weeks |
| **Adaptive Eviction** | 1.2-1.5x | N/A | 1 week |

**Combined Impact:**
- **Query Latency:** 20-200x faster for typical dashboard queries
- **Memory Efficiency:** 2-5x more effective cache capacity
- **System Throughput:** 5-10x higher queries per second

---

## Next Steps

### Immediate (Week 1-2)

1. **Enable Hot Partition Cache**
   - Implement CachedParquetExec
   - Wire into QueryEngine
   - Add invalidation hooks
   - Deploy to staging
   - Benchmark on production queries

### Short-term (Week 3-4)

2. **Column Projection**
   - Add projection parameter to cache APIs
   - Update providers
   - Performance validation

3. **Monitoring Dashboard**
   - Cache hit rates
   - Memory usage trends
   - Query latency P50/P95/P99

### Medium-term (Month 2-3)

4. **Dictionary Encoding**
   - Implement compression
   - Cardinality analysis
   - Memory benchmarks

5. **Filter Pushdown**
   - Predicate extraction from plans
   - Early filtering logic
   - End-to-end testing

### Long-term (Month 4+)

6. **Adaptive Eviction**
   - Cost modeling
   - Workload analysis
   - Policy refinement

---

## Conclusion

MynaDB has **world-class columnar caching infrastructure** that is currently **underutilized**. The HotPartitionCache exists (745 lines of production-ready code) but remains disconnected from the query path—representing a **massive missed opportunity** for 10-100x performance improvements on hot data.

**Key Takeaways:**

1. **Low-Hanging Fruit:** Enabling HotPartitionCache requires minimal changes (1-2 weeks) for massive gains
2. **Memory Efficiency:** Current caches use ~4.3GB; with improvements, same memory can cache 2-5x more data
3. **Proven Architecture:** All caches use Arrow RecordBatch (columnar), enabling zero-copy and efficient processing
4. **Incremental Path:** Each improvement can be implemented and validated independently

**Recommendation:** Prioritize Phase 1 (Hot Partition Cache) immediately. This single change will transform query performance on recent data with minimal risk.
