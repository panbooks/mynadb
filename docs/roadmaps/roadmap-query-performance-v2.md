# MynaDB Query Performance Optimization Roadmap v2

**Version:** 2.0
**Date:** 2026-01-23
**Status:** Strategic Plan for State-of-the-Art Performance

---

## Executive Summary

This document provides a comprehensive analysis of MynaDB's current query performance implementation and outlines the strategic plan to achieve state-of-the-art performance comparable to InfluxDB IOx and GreptimeDB.

**Key Finding:** MynaDB has a strong foundation with ~60% of Phase 1-2 optimizations already implemented. The critical gaps are in hot data caching, indexing, and continuous aggregation.

---

## Current Architecture

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
│   │ Query Planner   │────▶│  Query Cache    │ ✅ Implemented         │
│   └────────┬────────┘     └─────────────────┘                        │
│            ▼                                                         │
│   ┌─────────────────┐     ┌─────────────────┐                        │
│   │ Table Provider  │────▶│ Metadata Cache  │ ✅ Implemented         │
│   └────────┬────────┘     └─────────────────┘                        │
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
│   │  Fill Processor │  ✅ GROUP BY time() support                    │
│   └────────┬────────┘                                                │
│            ▼                                                         │
│        Results                                                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Status

### Completed Features (Strong Foundation)

| Feature | Location | Status | Notes |
|---------|----------|--------|-------|
| **Query Result Cache** | `query/cache/query_cache.rs` | ✅ Done | Full LRU, TTL, invalidation |
| **Metadata Cache** | `query/cache/metadata_cache.rs` | ✅ Done | Table, file listing, Parquet metadata |
| **Parquet Metadata Cache** | `query/cache/metadata_cache.rs` | ✅ Done | 10K entries, size-based eviction |
| **Predicate Pushdown** | `query/query_engine.rs` | ✅ Done | DataFusion config enabled |
| **Projection Pushdown** | `query/providers/table_provider.rs` | ✅ Done | Column pruning works |
| **Row Group Pruning** | `query/query_engine.rs` | ✅ Done | Statistics-based pruning |
| **Page Index Pruning** | `query/query_engine.rs` | ✅ Done | Page-level filtering |
| **Bloom Filters (Write)** | `storage/parquet_writer.rs` | ✅ Done | All Utf8 columns, 1% FPR |
| **Timestamp→Date Analysis** | `query/analyzers/timestamp_to_date.rs` | ✅ Done | Auto partition pruning |
| **Partition Filter Pushdown** | `query/logical_optimizers/` | ✅ Done | Extracts & pushes filters |
| **GROUP BY time()** | `query/timeseries/group_by_time.rs` | ✅ Done | InfluxQL-style support |
| **Fill Processor** | `query/timeseries/fill_processor.rs` | ✅ Done | previous, linear, null, value |
| **Parallel File Scanning** | `query/providers/table_provider.rs` | ✅ Done | 4 concurrent files |
| **Memory Management** | `query/query_engine.rs` | ✅ Done | 16GB limit, disk spilling |
| **Custom UDFs** | `query/functions/` | ✅ Done | time_bucket, first_value, last_value |

### DataFusion Configuration (Optimized)

```rust
// Current configuration in query_engine.rs (lines 63-152)

// Predicate Push-down
config.execution.parquet.pushdown_filters = true;
config.execution.parquet.reorder_filters = true;

// Row Group & Page Pruning
config.execution.parquet.pruning = true;
config.execution.parquet.enable_page_index = true;
config.execution.parquet.bloom_filter_on_read = true;

// Parallel Parquet Reading
config.execution.target_partitions = num_cpus;
config.execution.batch_size = 8192;
config.execution.parquet.maximum_parallel_row_group_writers = num_cpus;

// Memory Management
memory_limit = 16GB;
threshold = 80%;
disk_spilling = enabled;
```

### Parquet Storage Settings (Optimized)

```rust
// Current configuration in parquet_writer.rs

ROW_GROUP_SIZE = 500_000 rows
DATA_PAGE_SIZE = 1MB
COMPRESSION = ZSTD(level=3)
STATISTICS = Page-level enabled
BLOOM_FILTER_FPR = 0.01 (1%)
BLOOM_FILTER_NDV = 10,000
DICTIONARY_ENCODING = enabled
```

---

## Critical Gaps Analysis

### Gap 1: Bloom Filter Read-Side (P0)

**Current State:** Bloom filters are written to Parquet files but not explicitly used for file pruning at query time.

**Impact:** 10x faster point lookups on string columns

**Implementation Required:**
```rust
// In providers/table_provider.rs
fn should_read_file(file: &ParquetFileInfo, filters: &[Expr]) -> bool {
    for filter in filters {
        if let Some((column, value)) = extract_equality_predicate(filter) {
            if let Some(bloom_filter) = get_bloom_filter(file, column) {
                if !bloom_filter.check(&value) {
                    return false;  // Definitely not in file
                }
            }
        }
    }
    true
}
```

**Files to Modify:**
- `src/query/providers/table_provider.rs`
- `src/query/cache/metadata_cache.rs` (cache bloom filters)

---

### Gap 2: Hot Partition Cache (P0)

**Current State:** Every query reads Parquet files and decompresses columns, even for recent frequently-accessed data.

**Impact:** 50-100x faster for queries on recent data (today/yesterday)

**Implementation Required:**
```rust
// New file: src/query/cache/hot_partition_cache.rs

pub struct HotPartitionCache {
    /// (catalog, schema, table, date) -> decoded RecordBatches
    cache: RwLock<HashMap<PartitionKey, Vec<RecordBatch>>>,
    max_size_bytes: usize,
    current_size: AtomicUsize,
    hot_days: u32,  // Number of days to keep hot
}

impl HotPartitionCache {
    pub fn is_hot(&self, date: NaiveDate) -> bool {
        let today = Utc::now().date_naive();
        (today - date).num_days() < self.hot_days as i64
    }

    pub async fn get_or_load(
        &self,
        key: &PartitionKey,
        loader: impl Future<Output = Vec<RecordBatch>>,
    ) -> Vec<RecordBatch> {
        // Check cache first
        if let Some(batches) = self.cache.read().get(key) {
            return batches.clone();
        }
        // Load and cache
        let batches = loader.await;
        self.insert(key.clone(), batches.clone());
        batches
    }

    pub fn invalidate(&self, catalog: &str, schema: &str, table: &str, date: NaiveDate) {
        let key = PartitionKey::new(catalog, schema, table, date);
        self.cache.write().remove(&key);
    }
}
```

**Configuration:**
```toml
[cache.hot_partition]
enabled = true
max_size_mb = 2048      # 2GB for hot data
hot_days = 2            # today + yesterday
preload_on_startup = true
```

---

### Gap 3: Query Plan Cache (P1)

**Current State:** Every query parses SQL and optimizes the plan from scratch.

**Impact:** 2-5x faster for repeated queries (dashboard refresh)

**Implementation Required:**
```rust
// New file: src/query/cache/plan_cache.rs

pub struct QueryPlanCache {
    /// Normalized SQL -> (LogicalPlan, PhysicalPlan)
    cache: RwLock<LruCache<String, CachedPlan>>,
    ttl: Duration,
}

struct CachedPlan {
    logical_plan: LogicalPlan,
    physical_plan: Arc<dyn ExecutionPlan>,
    created_at: Instant,
    table_versions: HashMap<String, u64>,
}

impl QueryPlanCache {
    pub fn normalize_sql(sql: &str) -> String {
        // Replace literals with placeholders
        // "SELECT * FROM t WHERE x = 5" -> "SELECT * FROM t WHERE x = ?"
        // "... timestamp > 1704844800" -> "... timestamp > ?"
        normalize_query(sql)
    }

    pub fn get(&self, sql: &str) -> Option<CachedPlan> {
        let normalized = Self::normalize_sql(sql);
        self.cache.read().get(&normalized)
            .filter(|p| p.created_at.elapsed() < self.ttl)
            .cloned()
    }
}
```

---

### Gap 4: Time Index (P1)

**Current State:** Must read Parquet metadata to determine time ranges in files.

**Impact:** 5-10x faster time-bounded queries, skip metadata reads entirely

**Implementation Required:**
```rust
// New file: src/query/index/time_index.rs

/// Sparse index entry for a Parquet file
#[derive(Serialize, Deserialize)]
pub struct TimeIndexEntry {
    pub file_path: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub row_count: usize,
    pub file_size: u64,
}

/// Time index for a table
pub struct TimeIndex {
    /// Sorted by min_timestamp for efficient range queries
    entries: BTreeMap<i64, Vec<TimeIndexEntry>>,
}

impl TimeIndex {
    /// Find files that may contain data in the given time range
    /// O(log n) complexity
    pub fn find_files(&self, start_ts: i64, end_ts: i64) -> Vec<&TimeIndexEntry> {
        self.entries
            .range(..=end_ts)
            .flat_map(|(_, entries)| entries)
            .filter(|e| e.max_timestamp >= start_ts)
            .collect()
    }

    /// Persist index to disk
    pub async fn save(&self, path: &Path) -> Result<()> {
        let data = bincode::serialize(&self.entries)?;
        tokio::fs::write(path, data).await?;
        Ok(())
    }

    /// Load index from disk
    pub async fn load(path: &Path) -> Result<Self> {
        let data = tokio::fs::read(path).await?;
        let entries = bincode::deserialize(&data)?;
        Ok(Self { entries })
    }
}
```

**Storage Location:**
```
data/{catalog}/{schema}/{table}/_index/
    time_index.bin
```

---

### Gap 5: Tag Inverted Index (P1)

**Current State:** Tag filters require scanning all files or relying on bloom filters.

**Impact:** 100x faster for selective tag queries (e.g., `WHERE host = 'specific-server'`)

**Implementation Required:**
```rust
// New file: src/query/index/tag_index.rs

/// Inverted index for tag values
#[derive(Serialize, Deserialize)]
pub struct TagIndex {
    /// tag_name -> tag_value -> list of file paths
    index: HashMap<String, HashMap<String, Vec<String>>>,
}

impl TagIndex {
    /// Find files containing a specific tag value
    /// O(1) complexity
    pub fn find_files(&self, tag: &str, value: &str) -> Vec<&str> {
        self.index
            .get(tag)
            .and_then(|m| m.get(value))
            .map(|files| files.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Add file to index
    pub fn add_file(&mut self, file_path: &str, tags: &HashMap<String, HashSet<String>>) {
        for (tag_name, values) in tags {
            for value in values {
                self.index
                    .entry(tag_name.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(file_path.to_string());
            }
        }
    }

    /// Remove file from index (for compaction)
    pub fn remove_file(&mut self, file_path: &str) {
        for tag_values in self.index.values_mut() {
            for files in tag_values.values_mut() {
                files.retain(|f| f != file_path);
            }
        }
    }
}
```

**Storage Location:**
```
data/{catalog}/{schema}/{table}/_index/
    tag_index.bin
```

---

### Gap 6: Partition Index (P1)

**Current State:** Directory listing performed on every query to discover partitions.

**Impact:** Instant partition discovery, no filesystem scan

**Implementation Required:**
```rust
// New file: src/query/index/partition_index.rs

#[derive(Serialize, Deserialize)]
pub struct PartitionIndex {
    pub table: String,
    pub partitions: Vec<PartitionInfo>,
    pub last_updated: DateTime<Utc>,
    pub version: u64,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionInfo {
    pub date: NaiveDate,
    pub files: Vec<FileInfo>,
    pub total_rows: usize,
    pub total_bytes: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

#[derive(Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub level: u8,  // L0, L1, L2, etc.
    pub rows: usize,
    pub bytes: u64,
}
```

**Storage Location:**
```
data/{catalog}/{schema}/{table}/_index/
    partition_index.json
```

---

### Gap 7: Continuous Aggregation (P2)

**Current State:** No pre-computed aggregations. Every dashboard query computes from raw data.

**Impact:** 100-1000x faster for dashboard/monitoring queries

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

**Implementation Required:**
```rust
// New file: src/jobs/continuous_aggregation.rs

pub struct ContinuousAggregationConfig {
    pub name: String,
    pub source_table: String,
    pub target_table: String,
    pub interval: Duration,
    pub aggregations: Vec<AggregationSpec>,
    pub group_by: Vec<String>,
    pub lag: Duration,  // How far behind real-time to process
}

pub struct AggregationSpec {
    pub function: AggFunction,  // Sum, Avg, Min, Max, Count, First, Last
    pub column: String,
    pub alias: String,
}

pub struct ContinuousAggregationWorker {
    config: ContinuousAggregationConfig,
    query_engine: Arc<QueryEngine>,
    last_processed: AtomicI64,
}

impl ContinuousAggregationWorker {
    pub async fn process_window(&self, start: i64, end: i64) -> Result<()> {
        let sql = format!(
            "SELECT time_bucket(time, {}) as bucket, {}, {} \
             FROM {} \
             WHERE time >= {} AND time < {} \
             GROUP BY 1, {}",
            self.config.interval.as_nanos(),
            self.config.group_by.join(", "),
            self.aggregations_to_sql(),
            self.config.source_table,
            start, end,
            self.config.group_by.join(", ")
        );

        let results = self.query_engine.execute(&sql).await?;
        self.write_to_rollup_table(results).await
    }
}
```

**Query Routing:**
```rust
impl QueryEngine {
    fn select_best_source(&self, query: &ParsedQuery) -> TableSource {
        // Extract time bucket interval from query
        let query_interval = query.get_time_bucket_interval();
        let time_range = query.get_time_range();

        // Find matching rollup with interval <= query interval
        for rollup in &self.rollups {
            if rollup.interval <= query_interval {
                // Verify rollup covers the requested time range
                if rollup.covers_range(time_range) {
                    return TableSource::Rollup(rollup.target_table.clone());
                }
            }
        }

        TableSource::Raw(query.table.clone())
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
lag = "10s"
group_by = ["host", "region"]
aggregations = [
    { function = "avg", column = "usage", alias = "avg_usage" },
    { function = "max", column = "usage", alias = "max_usage" },
    { function = "min", column = "usage", alias = "min_usage" },
    { function = "count", column = "*", alias = "count" },
]

[[continuous_aggregation]]
name = "cpu_1h"
source = "metrics.default._rollup_cpu_1m"  # Build from 1m rollup
target = "metrics.default._rollup_cpu_1h"
interval = "1h"
lag = "1m"
group_by = ["host", "region"]
aggregations = [
    { function = "avg", column = "avg_usage", alias = "avg_usage" },
    { function = "max", column = "max_usage", alias = "max_usage" },
    { function = "min", column = "min_usage", alias = "min_usage" },
    { function = "sum", column = "count", alias = "count" },
]
```

---

### Gap 8: Downsampling Tiers (P3)

**Current State:** All historical data kept at full resolution.

**Impact:** 90%+ storage reduction, faster historical queries

**Storage Tiers:**
| Age | Resolution | Storage Reduction |
|-----|------------|-------------------|
| 0-7 days | 1 second | 1x (raw) |
| 7-30 days | 1 minute | 60x |
| 30-365 days | 1 hour | 3600x |
| 1+ year | 1 day | 86400x |

**Configuration:**
```toml
[downsampling]
enabled = true

[[downsampling.tiers]]
age_days = 7
resolution = "1m"
aggregations = ["avg", "min", "max", "count"]

[[downsampling.tiers]]
age_days = 30
resolution = "1h"
aggregations = ["avg", "min", "max", "count"]

[[downsampling.tiers]]
age_days = 365
resolution = "1d"
aggregations = ["avg", "min", "max", "count"]
```

---

## Implementation Roadmap

### Priority Matrix

| Priority | Feature | Impact | Effort | ROI | Week |
|----------|---------|--------|--------|-----|------|
| **P0** | Bloom Filter Read-side | High | Low | ⭐⭐⭐⭐⭐ | 1 |
| **P0** | Hot Partition Cache | Very High | Medium | ⭐⭐⭐⭐⭐ | 1-2 |
| **P1** | Query Plan Cache | Medium | Low | ⭐⭐⭐⭐ | 2 |
| **P1** | Partition Index | Medium | Low | ⭐⭐⭐ | 2 |
| **P1** | Time Index | High | Medium | ⭐⭐⭐⭐ | 3 |
| **P1** | Tag Inverted Index | High | Medium | ⭐⭐⭐⭐ | 3-4 |
| **P2** | Continuous Aggregation | Very High | High | ⭐⭐⭐⭐⭐ | 5-7 |
| **P3** | Downsampling Tiers | High | High | ⭐⭐⭐ | 8+ |

### Timeline

```
Week 1-2:   P0 - Bloom Filter Read-side + Hot Partition Cache
            └── Immediate 10-100x improvement for common queries

Week 2-3:   P1 - Query Plan Cache + Partition Index
            └── 2-5x improvement for repeated queries

Week 3-4:   P1 - Time Index + Tag Inverted Index
            └── 10-100x improvement for filtered queries

Week 5-7:   P2 - Continuous Aggregation Framework
            └── 100-1000x improvement for dashboard queries

Week 8+:    P3 - Downsampling Tiers
            └── 90%+ storage reduction for historical data
```

---

## New Directory Structure

After implementation, the query module will have:

```
src/query/
├── analyzers/
│   └── timestamp_to_date.rs     # ✅ Exists
├── cache/
│   ├── metadata_cache.rs        # ✅ Exists
│   ├── query_cache.rs           # ✅ Exists
│   ├── hot_partition_cache.rs   # 🆕 To implement
│   └── plan_cache.rs            # 🆕 To implement
├── catalog/
│   └── ...                      # ✅ Exists
├── index/                       # 🆕 New directory
│   ├── mod.rs
│   ├── time_index.rs            # 🆕 To implement
│   ├── tag_index.rs             # 🆕 To implement
│   └── partition_index.rs       # 🆕 To implement
├── functions/
│   └── ...                      # ✅ Exists
├── logical_optimizers/
│   └── ...                      # ✅ Exists
├── physical_optimizers/
│   └── ...                      # ✅ Exists
├── providers/
│   └── ...                      # ✅ Exists (enhance for bloom filter)
├── timeseries/
│   └── ...                      # ✅ Exists
├── query_engine.rs              # ✅ Exists (enhance for query routing)
└── ...

src/jobs/
├── compactor.rs                 # ✅ Exists (enhance for index updates)
├── retention_manager.rs         # ✅ Exists
└── continuous_aggregation.rs    # 🆕 To implement
```

---

## Expected Performance After Implementation

### Query Latency Improvements

| Query Type | Current | After P0 | After P1 | After P2 |
|------------|---------|----------|----------|----------|
| Point lookup (`host='x'`) | 100ms | 10ms | 5ms | 5ms |
| Time range (last 1h) | 500ms | 50ms | 20ms | 20ms |
| Recent data (today) | 200ms | 5ms | 5ms | 5ms |
| Dashboard aggregation | 2000ms | 1000ms | 500ms | 20ms |
| Historical (last 30d) | 5000ms | 2000ms | 500ms | 100ms |

### Comparison with Competitors (After Full Implementation)

| Feature | InfluxDB IOx | GreptimeDB | MynaDB (Current) | MynaDB (Target) |
|---------|--------------|------------|------------------|-----------------|
| Metadata cache | ✅ | ✅ | ✅ | ✅ |
| Query result cache | ✅ | ✅ | ✅ | ✅ |
| Bloom filter (write) | ✅ | ✅ | ✅ | ✅ |
| Bloom filter (read) | ✅ | ✅ | ❌ | ✅ |
| Hot partition cache | ✅ | ✅ | ❌ | ✅ |
| Query plan cache | ✅ | ? | ❌ | ✅ |
| Time index | ✅ | ✅ | ❌ | ✅ |
| Tag inverted index | ✅ | ✅ | ❌ | ✅ |
| Continuous aggregation | ✅ | ✅ | ❌ | ✅ |
| Downsampling | ✅ | ✅ | ❌ | ✅ |

**Target:** Full feature parity with state-of-the-art time series databases.

---

## Benchmarking

### Key Metrics to Track

| Metric | Description | Current | Target |
|--------|-------------|---------|--------|
| Query Latency P50 | Median query time | ~100ms | <10ms |
| Query Latency P99 | 99th percentile | ~2s | <100ms |
| Files Scanned/Query | Average files read | N | N/10 |
| Cache Hit Rate | Query cache effectiveness | ~30% | >80% |
| Hot Partition Hit | Recent data from memory | 0% | >95% |

### Benchmark Queries

```sql
-- Point query (should use bloom filter + tag index)
SELECT * FROM cpu WHERE host = 'server-12345' AND time > now() - interval '1h';

-- Time range scan (should use time index + partition pruning)
SELECT avg(value) FROM cpu WHERE time BETWEEN '2024-01-01' AND '2024-01-02';

-- Dashboard query (should use continuous aggregation)
SELECT time_bucket(time, '5m'), host, avg(cpu)
FROM cpu
WHERE time > now() - interval '1h'
GROUP BY 1, 2;

-- Recent data (should hit hot partition cache)
SELECT * FROM cpu WHERE time > now() - interval '5m' ORDER BY time DESC LIMIT 100;
```

### Performance Testing Script

```bash
#!/bin/bash
# scripts/benchmark_query_performance.sh

echo "=== MynaDB Query Performance Benchmark ==="

# Point query
echo "Point Query (host filter):"
time curl -s "http://localhost:7000/query/mydb?sql=SELECT%20*%20FROM%20cpu%20WHERE%20host='server01'%20LIMIT%20100" > /dev/null

# Time range
echo "Time Range Query:"
time curl -s "http://localhost:7000/query/mydb?sql=SELECT%20count(*)%20FROM%20cpu%20WHERE%20time%20%3E%20now()%20-%20interval%20'1%20hour'" > /dev/null

# Aggregation
echo "Aggregation Query:"
time curl -s "http://localhost:7000/query/mydb?sql=SELECT%20host,%20avg(usage)%20FROM%20cpu%20GROUP%20BY%20host" > /dev/null

# Dashboard simulation
echo "Dashboard Query:"
time curl -s "http://localhost:7000/query/mydb?sql=SELECT%20time_bucket(time,%20'5m'),%20avg(usage)%20FROM%20cpu%20WHERE%20time%20%3E%20now()%20-%20interval%20'1%20hour'%20GROUP%20BY%201" > /dev/null
```

---

## Configuration Reference

```toml
# config.toml - Full performance configuration

[query]
use_custom_table_provider = true

[query.execution]
file_parallelism = 8
partition_parallelism = 4
batch_size = 8192

# Existing query result cache
[query_cache]
enabled = true
max_size_mb = 256
ttl_secs = 300
min_query_time_ms = 10

# NEW: Hot partition cache
[cache.hot_partition]
enabled = true
max_size_mb = 2048
hot_days = 2
preload_on_startup = true

# NEW: Query plan cache
[cache.plan]
enabled = true
max_entries = 1000
ttl_secs = 600

# NEW: Index settings
[index]
time_index_enabled = true
tag_index_enabled = true
tag_index_columns = ["host", "service", "region"]
partition_index_enabled = true

# NEW: Continuous aggregation
[continuous_aggregation]
enabled = true
worker_threads = 2

[[continuous_aggregation.definitions]]
name = "cpu_1m"
source = "metrics.default.cpu"
interval = "1m"
group_by = ["host"]
aggregations = ["avg", "max", "min", "count"]

[[continuous_aggregation.definitions]]
name = "cpu_1h"
source = "metrics.default._rollup_cpu_1m"
interval = "1h"
group_by = ["host"]
aggregations = ["avg", "max", "min", "sum:count"]

# NEW: Downsampling
[downsampling]
enabled = true

[[downsampling.tiers]]
after_days = 7
resolution = "1m"

[[downsampling.tiers]]
after_days = 30
resolution = "1h"
```

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-23 | MynaDB Team | Initial roadmap |
| 2.0 | 2026-01-23 | MynaDB Team | Gap analysis, implementation details, priority matrix |
