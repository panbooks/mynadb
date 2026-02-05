# MynaDB Index Performance Roadmap

This document outlines the progressive index optimizations planned to achieve state-of-the-art query performance for MynaDB.

## Current State

MynaDB already implements several performance optimizations:

| Feature | Status | Location |
|---------|--------|----------|
| Data sorted by timestamp | ✅ Implemented | `parquet_writer.rs`, `compactor.rs` |
| Parquet row group statistics | ✅ Implemented | Built-in Parquet feature |
| Bloom filters (write) | ✅ Implemented | `parquet_writer.rs` (1% FPR, 10K NDV) |
| Bloom filter read-side | ✅ Implemented | `table_provider.rs` |
| Hot partition cache | ✅ Implemented | `hot_partition_cache.rs` |
| Metadata cache | ✅ Implemented | `metadata_cache.rs` |
| Query result cache | ✅ Implemented | `query_cache.rs` |

## Index Strategy Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Query Performance Pyramid                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│                         ┌─────────────┐                              │
│                         │   Rollups   │  ← P5: Pre-aggregated        │
│                         └─────────────┘                              │
│                    ┌─────────────────────┐                           │
│                    │   Series Index      │  ← P4: Series lookups     │
│                    └─────────────────────┘                           │
│               ┌───────────────────────────────┐                      │
│               │   Tag Inverted Index          │  ← P3: Point queries │
│               └───────────────────────────────┘                      │
│          ┌─────────────────────────────────────────┐                 │
│          │   Sparse Time Index + Min/Max Files    │  ← P1/P2         │
│          └─────────────────────────────────────────┘                 │
│     ┌───────────────────────────────────────────────────┐            │
│     │   Partition Manifest + Zone Maps                  │  ← P0/P7   │
│     └───────────────────────────────────────────────────┘            │
│ ┌───────────────────────────────────────────────────────────┐        │
│ │   Bloom Filters + Row Group Stats + Sorted Data (CURRENT) │        │
│ └───────────────────────────────────────────────────────────┘        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Priority Matrix

| Priority | Index Type | Complexity | Impact | Query Types Improved |
|----------|------------|------------|--------|---------------------|
| **P0** | Partition Manifest | Low | High | All queries |
| **P1** | Sparse Time Index | Low | High | Time range queries |
| **P2** | Min/Max Index Files | Low | Medium | All filtered queries |
| **P3** | Tag Inverted Index | Medium | Very High | Point queries (`WHERE tag='value'`) |
| **P4** | Series Index | Medium | High | Series-specific queries |
| **P5** | Pre-aggregated Rollups | Medium | Very High | Dashboard/aggregation queries |
| **P6** | Bitmap Index | Medium | High | Low-cardinality filters |
| **P7** | Zone Maps | Low | Medium | Data skipping |

---

## P0: Partition Manifest

### Problem
Currently, discovering partitions requires filesystem `readdir()` syscalls, which is slow for tables with many partitions.

### Solution
Maintain a manifest file at the table level with partition metadata.

### File Structure
```
data/{catalog}/{schema}/{table}/
├── _manifest.json          ← Partition manifest
├── date=2024-01-15/
│   └── *.parquet
└── date=2024-01-14/
    └── *.parquet
```

### Manifest Format
```json
{
  "version": 1,
  "table": "cpu",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T12:00:00Z",
  "partitions": [
    {
      "date": "2024-01-15",
      "files": 3,
      "total_rows": 100000,
      "total_bytes": 15728640,
      "min_timestamp": 1705276800000000000,
      "max_timestamp": 1705363199000000000,
      "compaction_level": 1
    },
    {
      "date": "2024-01-14",
      "files": 2,
      "total_rows": 80000,
      "total_bytes": 12582912,
      "min_timestamp": 1705190400000000000,
      "max_timestamp": 1705276799000000000,
      "compaction_level": 2
    }
  ]
}
```

### Operations
| Operation | Action |
|-----------|--------|
| Write/Flush | Update manifest with new partition stats |
| Compaction | Update manifest with merged file stats |
| Query | Read manifest instead of scanning directories |
| Retention | Remove partition entries when deleted |

### Expected Impact
- **10-100x faster** partition discovery
- Eliminates filesystem overhead for multi-partition tables
- Enables instant partition pruning

### Implementation Notes
```rust
pub struct PartitionManifest {
    pub version: u32,
    pub table: String,
    pub partitions: Vec<PartitionEntry>,
}

pub struct PartitionEntry {
    pub date: String,
    pub files: usize,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub compaction_level: u8,
}
```

---

## P1: Sparse Time Index

### Problem
Finding data for a specific time range requires scanning Parquet file metadata across all files.

### Solution
Build a sparse index mapping time ranges to file locations with O(log n) lookup.

### File Structure
```
data/{catalog}/{schema}/{table}/
├── _time_index.idx         ← Sparse time index
└── date=2024-01-15/
    └── *.parquet
```

### Index Structure
```
┌────────────────────────────────────────────────────────────┐
│                    Sparse Time Index                        │
├────────────────────────────────────────────────────────────┤
│  B-tree or Skip List Structure                              │
│                                                             │
│  Key: timestamp (nanoseconds)                               │
│  Value: FileLocation {                                      │
│    file_path: String,                                       │
│    row_group_id: u32,                                       │
│    row_offset: u64,                                         │
│    row_count: u64,                                          │
│  }                                                          │
│                                                             │
│  Example entries (sparse - every Nth timestamp):            │
│  ┌──────────────────┬─────────────────────────────────┐    │
│  │ 1705276800000000 │ (L1_001.parquet, rg0, 0, 8192)  │    │
│  │ 1705280400000000 │ (L1_001.parquet, rg1, 8192, 8192)│   │
│  │ 1705284000000000 │ (L1_002.parquet, rg0, 0, 8192)  │    │
│  │ ...              │ ...                              │    │
│  └──────────────────┴─────────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
```

### Query Flow
```
Query: WHERE time BETWEEN t1 AND t2

1. Binary search in time index for t1 → find starting file/row_group
2. Binary search for t2 → find ending file/row_group
3. Read only the relevant files and row groups
4. Apply remaining filters
```

### Expected Impact
- **O(log n)** time range lookup instead of O(n) metadata scan
- **5-20x faster** for narrow time range queries
- Especially effective for point-in-time queries

### Implementation Notes
```rust
pub struct SparseTimeIndex {
    // B-tree for O(log n) range queries
    entries: BTreeMap<i64, FileLocation>,
    // Granularity: one entry per row group
    granularity: TimeIndexGranularity,
}

pub struct FileLocation {
    pub file_path: PathBuf,
    pub row_group_id: u32,
    pub row_offset: u64,
    pub row_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

impl SparseTimeIndex {
    /// Find files overlapping a time range
    pub fn find_range(&self, start: i64, end: i64) -> Vec<&FileLocation>;

    /// Add entry (called during write/compaction)
    pub fn insert(&mut self, timestamp: i64, location: FileLocation);

    /// Remove entries for a file (called during compaction)
    pub fn remove_file(&mut self, file_path: &Path);
}
```

---

## P2: Min/Max Index Files

### Problem
Reading Parquet footer metadata requires file I/O and decompression, even for simple statistics.

### Solution
Store lightweight sidecar index files with pre-extracted statistics.

### File Structure
```
data/{catalog}/{schema}/{table}/date=2024-01-15/
├── L1_1705312800000.parquet
├── L1_1705312800000.parquet.idx    ← Sidecar index
├── L1_1705316400000.parquet
└── L1_1705316400000.parquet.idx    ← Sidecar index
```

### Index Format
```json
{
  "version": 1,
  "file": "L1_1705312800000.parquet",
  "file_size": 5242880,
  "total_rows": 16384,
  "row_groups": [
    {
      "id": 0,
      "num_rows": 8192,
      "compressed_size": 2621440,
      "columns": {
        "timestamp": {
          "min": 1705312800000000000,
          "max": 1705316399000000000,
          "null_count": 0
        },
        "host": {
          "min": "server01",
          "max": "server99",
          "null_count": 0,
          "distinct_count": 50
        },
        "value": {
          "min": 0.0,
          "max": 100.0,
          "null_count": 10,
          "sum": 450000.0
        }
      }
    },
    {
      "id": 1,
      "num_rows": 8192,
      "columns": { ... }
    }
  ]
}
```

### Query Flow
```
Query: WHERE host = 'server01' AND value > 50

1. Read .idx files (small, no decompression)
2. Check host min/max: "server01" in ["server01", "server99"]? Yes
3. Check value max: 100.0 > 50? Yes, might have matching rows
4. Read only matching row groups from Parquet file
```

### Expected Impact
- **10x faster** metadata access than Parquet footer
- No decompression overhead
- Enables aggressive file-level pruning

### Implementation Notes
```rust
pub struct MinMaxIndex {
    pub version: u32,
    pub file: String,
    pub file_size: u64,
    pub total_rows: u64,
    pub row_groups: Vec<RowGroupStats>,
}

pub struct RowGroupStats {
    pub id: u32,
    pub num_rows: u64,
    pub compressed_size: u64,
    pub columns: HashMap<String, ColumnStats>,
}

pub struct ColumnStats {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: u64,
    pub distinct_count: Option<u64>,
    pub sum: Option<f64>,  // For numeric columns
}
```

---

## P3: Tag Inverted Index

### Problem
Point queries like `WHERE host = 'server01'` must scan all files and check bloom filters.

### Solution
Build an inverted index mapping tag values directly to file locations.

### File Structure
```
data/{catalog}/{schema}/{table}/
├── _tag_index/
│   ├── host.idx            ← Inverted index for 'host' column
│   ├── service.idx         ← Inverted index for 'service' column
│   └── region.idx          ← Inverted index for 'region' column
└── date=2024-01-15/
    └── *.parquet
```

### Index Structure
```
┌─────────────────────────────────────────────────────────────┐
│                 Tag Inverted Index (host.idx)                │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  HashMap<TagValue, Vec<Location>>                            │
│                                                              │
│  "server01" → [                                              │
│    (date=2024-01-15/L1_001.parquet, rg0, rows 0-100),       │
│    (date=2024-01-15/L1_001.parquet, rg1, rows 50-150),      │
│    (date=2024-01-14/L1_003.parquet, rg0, rows 0-200),       │
│  ]                                                           │
│                                                              │
│  "server02" → [                                              │
│    (date=2024-01-15/L1_001.parquet, rg0, rows 101-200),     │
│    (date=2024-01-15/L1_002.parquet, rg0, rows 0-300),       │
│  ]                                                           │
│                                                              │
│  "server03" → [ ... ]                                        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Storage Format (Binary)
```
┌────────────────────────────────────────┐
│ Header                                  │
│   magic: [u8; 4] = "TIDX"              │
│   version: u32                          │
│   column_name_len: u32                  │
│   column_name: [u8]                     │
│   num_entries: u64                      │
├────────────────────────────────────────┤
│ Value Table (sorted for binary search)  │
│   value_offset: u64[]                   │
│   location_offset: u64[]                │
├────────────────────────────────────────┤
│ Values (string pool)                    │
│   "server01\0server02\0server03\0..."  │
├────────────────────────────────────────┤
│ Locations (per value)                   │
│   [file_id, row_group, start, end][]   │
└────────────────────────────────────────┘
```

### Query Flow
```
Query: SELECT * FROM cpu WHERE host = 'server01' AND time > t1

1. Lookup "server01" in host.idx → get file locations (O(1))
2. Filter locations by time range
3. Read only specific row groups from specific files
4. No full table scan needed!
```

### Expected Impact
- **100-1000x faster** for high-cardinality point queries
- Transforms O(n) scan to O(1) lookup
- Critical for operational dashboards

### Implementation Notes
```rust
pub struct TagInvertedIndex {
    column_name: String,
    // In-memory: HashMap for fast lookup
    index: HashMap<String, Vec<RowLocation>>,
    // On-disk: sorted values for binary search
}

pub struct RowLocation {
    pub file_path: PathBuf,
    pub row_group_id: u32,
    pub row_start: u64,
    pub row_end: u64,
}

impl TagInvertedIndex {
    /// Lookup a specific value
    pub fn get(&self, value: &str) -> Option<&Vec<RowLocation>>;

    /// Lookup multiple values (for IN queries)
    pub fn get_many(&self, values: &[&str]) -> Vec<&RowLocation>;

    /// Add entries (during write)
    pub fn insert(&mut self, value: &str, location: RowLocation);

    /// Rebuild after compaction
    pub fn rebuild(&mut self, files: &[ParquetFile]);
}
```

### Configuration
```toml
[index.tag_inverted]
enabled = true
# Columns to index (high-cardinality tags)
columns = ["host", "service", "instance", "pod"]
# Maximum distinct values before falling back to bloom filter
max_cardinality = 100000
# Update strategy: "realtime" or "batch"
update_mode = "batch"
```

---

## P4: Series Index

### Problem
Time-series databases often query by "series" (unique combination of measurement + tags).

### Solution
Index series keys for O(1) lookup of all data for a specific time series.

### Series Key Format
```
Series Key = measurement + "|" + sorted(tag_key=tag_value, ...)

Examples:
  "cpu|host=server01|region=us-east"
  "memory|host=server01|region=us-east"
  "cpu|host=server02|region=us-west"
```

### Index Structure
```
┌─────────────────────────────────────────────────────────────┐
│                      Series Index                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  HashMap<SeriesKey, SeriesMetadata>                          │
│                                                              │
│  "cpu|host=server01|region=us-east" → {                      │
│    series_id: 1,                                             │
│    first_seen: 1705190400000000000,                          │
│    last_seen: 1705363199000000000,                           │
│    locations: [                                              │
│      (date=2024-01-15, files=[L1_001, L1_002]),             │
│      (date=2024-01-14, files=[L1_003]),                     │
│    ]                                                         │
│  }                                                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Query Flow
```
Query: SELECT * FROM cpu WHERE host='server01' AND region='us-east'

1. Build series key: "cpu|host=server01|region=us-east"
2. Lookup in series index → get file locations
3. Read only files containing this series
```

### Expected Impact
- **Fast series enumeration** for schema discovery
- **Efficient multi-tag queries** without scanning
- Enables InfluxDB-compatible `SHOW SERIES` queries

---

## P5: Pre-aggregated Rollups

### Problem
Aggregation queries over long time ranges (30d, 90d, 1y) must read massive amounts of raw data.

### Solution
Pre-compute aggregates at different time granularities during compaction.

### File Structure
```
data/{catalog}/{schema}/{table}/
├── date=2024-01-15/             ← Raw data (1s granularity)
│   └── *.parquet
├── _rollup_1m/                   ← 1-minute aggregates
│   └── date=2024-01-15/
│       └── *.parquet
├── _rollup_1h/                   ← 1-hour aggregates
│   └── date=2024-01-15/
│       └── *.parquet
└── _rollup_1d/                   ← 1-day aggregates
    └── 2024-01.parquet           ← Monthly files for daily rollups
```

### Rollup Schema
```
Raw data:
| timestamp | host     | cpu_usage | memory_usage |
|-----------|----------|-----------|--------------|
| t1        | server01 | 45.2      | 60.1         |
| t1+1s     | server01 | 46.1      | 60.3         |
| t1+2s     | server01 | 44.8      | 59.9         |

1-minute rollup:
| timestamp | host     | cpu_usage_min | cpu_usage_max | cpu_usage_avg | cpu_usage_count | ... |
|-----------|----------|---------------|---------------|---------------|-----------------|-----|
| t1_minute | server01 | 44.8          | 46.1          | 45.37         | 60              | ... |
```

### Aggregates Stored
For each numeric field:
- `min` - Minimum value
- `max` - Maximum value
- `sum` - Sum of values
- `count` - Number of values
- `first` - First value in window
- `last` - Last value in window

### Query Rewriting
```sql
-- Original query
SELECT time_bucket('1h', time) as hour,
       avg(cpu_usage) as avg_cpu
FROM cpu
WHERE time > now() - interval '30 days'
GROUP BY hour

-- Rewritten to use rollups (automatic)
SELECT time as hour,
       cpu_usage_sum / cpu_usage_count as avg_cpu
FROM cpu._rollup_1h
WHERE time > now() - interval '30 days'
```

### Expected Impact
- **100-1000x faster** for dashboard queries
- **Reduced I/O** by orders of magnitude
- Essential for long-range analytics

### Configuration
```toml
[rollups]
enabled = true

[[rollups.levels]]
name = "1m"
interval = "1 minute"
retention_days = 90

[[rollups.levels]]
name = "1h"
interval = "1 hour"
retention_days = 365

[[rollups.levels]]
name = "1d"
interval = "1 day"
retention_days = 1825  # 5 years
```

---

## P6: Bitmap Index

### Problem
Low-cardinality columns (status, type, level) don't benefit much from bloom filters.

### Solution
Use compressed bitmap indexes (Roaring Bitmaps) for efficient set operations.

### Structure
```
Column: status (values: "ok", "warn", "error", "critical")

Roaring Bitmap per value:
┌─────────────────────────────────────────────┐
│ "ok"      → [0,1,2,5,6,7,10,11,12,15,...]   │  (compressed)
│ "warn"    → [3,8,13,18,...]                  │
│ "error"   → [4,9,14,19,...]                  │
│ "critical"→ [16,17,...]                      │
└─────────────────────────────────────────────┘

Row IDs where status = "ok": bits 0,1,2,5,6,7,10...
```

### Query Operations
```sql
-- Single value: direct bitmap lookup
WHERE status = 'error'
→ Return bitmap for "error"

-- IN clause: bitmap OR
WHERE status IN ('warn', 'error', 'critical')
→ bitmap['warn'] OR bitmap['error'] OR bitmap['critical']

-- NOT: bitmap complement
WHERE status != 'ok'
→ NOT bitmap['ok']

-- Combined: bitmap AND
WHERE status = 'error' AND region = 'us-east'
→ bitmap['status=error'] AND bitmap['region=us-east']
```

### Expected Impact
- **10-100x faster** for low-cardinality filters
- Extremely efficient for `IN` and `NOT IN` queries
- Compact storage with Roaring compression

### When to Use
| Cardinality | Recommended Index |
|-------------|-------------------|
| < 100 | Bitmap Index |
| 100 - 10,000 | Tag Inverted Index |
| > 10,000 | Bloom Filter |

---

## P7: Zone Maps

### Problem
Current statistics are per-row-group; file-level statistics would enable faster pruning.

### Solution
Maintain file-level zone maps with comprehensive statistics.

### Structure
```json
{
  "file": "L1_1705312800000.parquet",
  "zone_map": {
    "timestamp": {
      "min": 1705312800000000000,
      "max": 1705316399000000000,
      "null_count": 0,
      "all_null": false
    },
    "host": {
      "min": "server01",
      "max": "server99",
      "null_count": 0,
      "distinct_count": 50,
      "bloom_filter_offset": 1024
    },
    "value": {
      "min": 0.0,
      "max": 100.0,
      "null_count": 10,
      "sum": 450000.0,
      "all_same": false
    }
  }
}
```

### Expected Impact
- Fast file-level pruning without reading Parquet metadata
- Complements row-group level statistics

---

## Implementation Phases

### Phase 1: Foundation (1-2 weeks)
```
Week 1:
├── P0: Partition Manifest
│   ├── Create PartitionManifest struct
│   ├── Update on write/flush
│   ├── Update on compaction
│   └── Use in query planning
│
└── P2: Min/Max Index Files
    ├── Create sidecar .idx files during write
    ├── Update during compaction
    └── Use for file pruning
```

### Phase 2: Time Optimization (1-2 weeks)
```
Week 2-3:
└── P1: Sparse Time Index
    ├── B-tree based time index
    ├── Build during write/compaction
    ├── Query planner integration
    └── Incremental updates
```

### Phase 3: Tag Optimization (2-3 weeks)
```
Week 4-6:
└── P3: Tag Inverted Index
    ├── Index structure and storage format
    ├── Build during write (batch mode)
    ├── Rebuild during compaction
    ├── Query planner integration
    └── Configuration for indexed columns
```

### Phase 4: Advanced Features (3-4 weeks)
```
Week 7-10:
├── P5: Pre-aggregated Rollups
│   ├── Rollup schema generation
│   ├── Rollup computation during compaction
│   ├── Query rewriter for automatic rollup usage
│   └── Retention management
│
├── P4: Series Index
│   ├── Series key generation
│   ├── Index structure
│   └── SHOW SERIES support
│
└── P6: Bitmap Index
    ├── Roaring bitmap integration
    ├── Auto-detect low-cardinality columns
    └── Query planner integration
```

---

## Performance Targets

| Query Type | Current | With Indexes | Target |
|------------|---------|--------------|--------|
| Point query (`WHERE host='x'`) | 100ms | 1ms | <5ms |
| Time range (1 hour) | 50ms | 10ms | <20ms |
| Time range (1 day) | 500ms | 50ms | <100ms |
| Time range (30 days) | 10s | 100ms | <500ms (with rollups) |
| Aggregation (1 hour) | 200ms | 20ms | <50ms |
| Aggregation (30 days) | 30s | 200ms | <1s (with rollups) |
| SHOW SERIES | 5s | 50ms | <100ms |

---

## Monitoring & Observability

### Index Statistics Endpoint
```
GET /index/stats/{catalog}/{schema}/{table}
```

```json
{
  "partition_manifest": {
    "enabled": true,
    "partitions": 365,
    "last_updated": "2024-01-15T12:00:00Z"
  },
  "time_index": {
    "enabled": true,
    "entries": 50000,
    "size_bytes": 1048576
  },
  "tag_indexes": {
    "host": {
      "distinct_values": 1000,
      "entries": 500000,
      "size_bytes": 5242880
    },
    "service": {
      "distinct_values": 50,
      "entries": 25000,
      "size_bytes": 262144
    }
  },
  "rollups": {
    "1m": {"partitions": 90, "total_rows": 1000000},
    "1h": {"partitions": 365, "total_rows": 100000},
    "1d": {"partitions": 1825, "total_rows": 5000}
  }
}
```

---

## Configuration Reference

```toml
[index]
# Global index settings
enabled = true
# Directory for index files (default: alongside data)
index_dir = ""

[index.partition_manifest]
enabled = true
# Update mode: "sync" (immediate) or "async" (background)
update_mode = "async"

[index.time_index]
enabled = true
# Granularity: "row_group" or "file"
granularity = "row_group"

[index.min_max]
enabled = true
# Columns to include (empty = all)
columns = []

[index.tag_inverted]
enabled = true
# Columns to index
columns = ["host", "service", "instance"]
# Max cardinality before disabling
max_cardinality = 100000

[index.bitmap]
enabled = true
# Auto-detect columns with cardinality below threshold
auto_detect_threshold = 100

[index.series]
enabled = true
# Tag columns to include in series key
tag_columns = ["host", "service", "region"]

[rollups]
enabled = true
# Compute rollups during compaction
compute_on_compaction = true

[[rollups.levels]]
name = "1m"
interval = "1 minute"
retention_days = 90

[[rollups.levels]]
name = "1h"
interval = "1 hour"
retention_days = 365

[[rollups.levels]]
name = "1d"
interval = "1 day"
retention_days = 1825
```

---

## References

- [Apache Parquet Format](https://parquet.apache.org/docs/file-format/)
- [InfluxDB TSI (Time Series Index)](https://docs.influxdata.com/influxdb/v1/concepts/tsi-details/)
- [ClickHouse Data Skipping Indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes)
- [TimescaleDB Continuous Aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [Roaring Bitmaps](https://roaringbitmap.org/)
- [Apache DataFusion Pruning](https://datafusion.apache.org/user-guide/configs.html)
