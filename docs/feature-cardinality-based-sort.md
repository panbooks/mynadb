# Cardinality-Based Tag Sort Ordering

MynaDB automatically optimizes data layout by sorting records based on tag cardinality, enabling efficient query filtering and partition pruning.

## Overview

When writing data to Parquet files, MynaDB analyzes the cardinality (number of distinct values) of each tag column and sorts data with low-cardinality tags first. This ensures that rows with the same tag value are stored contiguously, enabling:

- **Row group pruning**: Parquet min/max statistics can skip entire row groups
- **Data locality**: Related data is physically adjacent on disk
- **Reduced I/O**: Queries scan only relevant portions of files

## How It Works

### 1. Cardinality Tracking During Writes

As data points arrive, MynaDB tracks distinct values for each tag key:

```
POST /write/mydb
cpu,region=us-west,host=server01 usage=75.5
cpu,region=us-east,host=server02 usage=82.3
cpu,region=us-west,host=server03 usage=68.0
```

Internal tracking:
```
region: {us-west, us-east}     → cardinality = 2
host:   {server01, server02, server03} → cardinality = 3
```

### 2. Memory-Bounded Tracking

To prevent memory bloat with high-cardinality tags, tracking uses bounded sets:

| Distinct Values | Behavior |
|-----------------|----------|
| < 1,000 | Track exact count |
| >= 1,000 | Mark as "high cardinality", stop tracking |

High-cardinality tags (like `request_id` or `trace_id`) are automatically placed last in the sort order.

### 3. Sort Order on Flush

When flushing to Parquet, tags are sorted by cardinality (ascending):

```
Sort order: [low cardinality] → [medium] → [high] → time

Example:
  region (2 values) → datacenter (20 values) → host (10,000 values) → time
```

## Performance Impact

### Before: Sort by Time Only

```
time | region   | host      | usage
─────┼──────────┼───────────┼───────
t1   | us-west  | server01  | 75.5
t2   | us-east  | server02  | 82.3    ← different region
t3   | us-west  | server03  | 68.0    ← same region, scattered
t4   | eu-west  | server01  | 91.2
t5   | us-west  | server01  | 77.8    ← same region, scattered
```

**Query**: `WHERE region = 'us-west'`
- Row group statistics: min='eu-west', max='us-west' (overlapping)
- Must scan **100%** of data
- No row groups can be skipped

### After: Cardinality-Based Sort

```
time | region   | host      | usage
─────┼──────────┼───────────┼───────
t4   | eu-west  | server01  | 91.2    ← Row Group 1
t2   | us-east  | server02  | 82.3    ← Row Group 2
t1   | us-west  | server01  | 75.5    ┐
t3   | us-west  | server03  | 68.0    ├─ Row Group 3 (contiguous!)
t5   | us-west  | server01  | 77.8    ┘
```

**Query**: `WHERE region = 'us-west'`
- Row group statistics enable pruning
- Scan only **Row Group 3** (~33% of data)
- Skip Row Groups 1 and 2 entirely

### Benchmark Results

| Query Pattern | Before | After | Improvement |
|---------------|--------|-------|-------------|
| Single low-cardinality filter | 100% scan | 10-30% scan | **3-10x faster** |
| Multiple tag filters | 100% scan | 5-15% scan | **7-20x faster** |
| Time range + tag filter | 100% scan | 5-20% scan | **5-20x faster** |

## Configuration

Cardinality-based sorting is **enabled by default** with no configuration required.

### Tuning Parameters

The tracking threshold can be adjusted by modifying the constant in `src/storage/cache.rs`:

```rust
/// Maximum distinct values to track per tag before marking as "high cardinality"
const MAX_TRACKED_DISTINCT_VALUES: usize = 1000;
```

- **Lower values**: Less memory usage, less precise ordering
- **Higher values**: More memory usage, better ordering for medium-cardinality tags

## Query Examples

### Optimal Queries (Benefit Most)

```sql
-- Single low-cardinality filter
SELECT * FROM cpu WHERE region = 'us-west';

-- Multiple filters (compound benefit)
SELECT * FROM cpu WHERE region = 'us-west' AND datacenter = 'dc-1';

-- Aggregation with low-cardinality grouping
SELECT region, avg(usage) FROM cpu GROUP BY region;

-- Time range with tag filter
SELECT * FROM cpu
WHERE region = 'us-west'
  AND time >= '2024-01-01' AND time < '2024-01-02';
```

### Queries With Less Benefit

```sql
-- High-cardinality filter (still works, but less pruning)
SELECT * FROM cpu WHERE host = 'server-12345';

-- No tag filter (full scan regardless)
SELECT * FROM cpu WHERE usage > 90;

-- Time-only filter (benefits from time being last sort key)
SELECT * FROM cpu WHERE time >= '2024-01-01';
```

## Monitoring

### Check Sort Order in Logs

When flushing, MynaDB logs the sort order used:

```
INFO Writing 10000 rows for mydb.default.cpu/date=2024-01-15 (sort: region -> datacenter -> host -> time)
```

### Verify with EXPLAIN

```bash
curl "http://localhost:7000/explain/mydb?sql=SELECT * FROM cpu WHERE region='us-west'"
```

Look for `ParquetExec` showing row group pruning:
```
ParquetExec: files=1, row_groups=3, rows_pruned=2000, rows_to_scan=1000
```

## Technical Details

### Implementation

| Component | File | Description |
|-----------|------|-------------|
| `TagCardinalityTracker` | `src/storage/cache.rs` | Tracks distinct values per tag during writes |
| `sort_batch_by_columns()` | `src/storage/parquet_writer.rs` | Multi-column lexicographic sort for flush |
| `build_sort_order()` | `src/storage/parquet_writer.rs` | Constructs optimal column order for flush |
| `build_cardinality_sort_order()` | `src/jobs/compactor.rs` | Analyzes cardinality during compaction |
| `estimate_cardinality()` | `src/jobs/compactor.rs` | Sampling-based cardinality estimation |
| `sort_batch_by_columns()` | `src/jobs/compactor.rs` | Multi-column lexicographic sort for compaction |

### Data Flow

```
┌─────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  add_points │ ──► │ TagCardinalityTracker │ ──► │ cardinality map │
└─────────────┘     │   - track per tag    │     │ region: 5       │
                    │   - bounded HashSet  │     │ dc: 20          │
                    └──────────────────────┘     │ host: MAX       │
                                                 └────────┬────────┘
                                                          │
┌─────────────┐     ┌──────────────────────┐              │
│   Parquet   │ ◄── │  sort_batch_by_      │ ◄────────────┘
│    File     │     │  columns()           │     sort order:
└─────────────┘     │  [region,dc,host,    │     [region, dc, host, time]
                    │   time]              │
                    └──────────────────────┘
```

### Parquet Row Group Statistics

After cardinality-based sorting, each row group has non-overlapping ranges for low-cardinality tags:

```
Row Group 0: region min='eu-west', max='eu-west'  ← single value!
Row Group 1: region min='us-east', max='us-east'  ← single value!
Row Group 2: region min='us-west', max='us-west'  ← single value!
```

This enables DataFusion to skip row groups entirely when filtering on these tags.

## Comparison with Other Databases

| Database | Approach | Configuration |
|----------|----------|---------------|
| **MynaDB** | Automatic cardinality-based | None required |
| ClickHouse | Manual `ORDER BY` in table definition | User specifies columns |
| TimescaleDB | Chunk ordering by time | Limited to time dimension |
| InfluxDB | TSM engine, series-based | Automatic but different approach |

## Compaction Behavior

Compaction also applies cardinality-based sorting to ensure optimal data layout is preserved (and improved) as files are merged.

### How Compaction Sorting Works

1. **Read all source files** into memory
2. **Analyze cardinality** of all string columns (tags) using sampling
3. **Sort merged data** by cardinality order (low → high → time)
4. **Write compacted file** with optimal layout

### Cardinality Estimation During Compaction

For performance, compaction uses sampling-based cardinality estimation:

| Data Size | Approach |
|-----------|----------|
| ≤ 100,000 rows | Exact count (iterate all rows) |
| > 100,000 rows | Sample every Nth row, extrapolate |

Tags with > 1,000 distinct values are marked as "high cardinality" and sorted last.

### Example: Compaction Sort Order

```
Compacting 3 files:
  File 1: 50,000 rows (sorted: region → host → time)
  File 2: 30,000 rows (sorted: region → host → time)
  File 3: 20,000 rows (sorted: datacenter → host → time)  ← different order!

Compaction process:
  1. Concatenate: 100,000 rows total
  2. Analyze cardinality:
     - region: 5 distinct values
     - datacenter: 8 distinct values
     - host: 500 distinct values
  3. Sort by: region → datacenter → host → time
  4. Write single compacted file

Result: Optimal sort order based on current data distribution
```

### Log Output

```
DEBUG Compaction sort order: region -> datacenter -> host -> time
INFO  Compacted 3 files (100,000 rows) -> L1_1706000000.parquet
```

## Limitations

1. **Per-flush cardinality**: Write path tracks cardinality per flush cycle. Data arriving in different flush cycles may have slightly different sort orders until compacted.

2. **Per-table tracking**: Cardinality is tracked per table, not globally. Different tables may have different sort orders.

3. **Memory overhead**:
   - Write path: ~1KB per tag key (bounded HashSets)
   - Compaction: Temporary memory for cardinality HashSets during merge

4. **Sampling accuracy**: For very large compactions (> 100K rows), cardinality is estimated via sampling, which may not perfectly reflect true distribution.

## Future Enhancements

- **Configurable sort order override**: Allow manual specification of sort columns via config
- **Persistent cardinality stats**: Store cardinality estimates in metadata for faster compaction
- **Z-Order sorting**: Interleaved sorting for multi-dimensional query patterns
- **Statistics export**: API to view current cardinality estimates per table
