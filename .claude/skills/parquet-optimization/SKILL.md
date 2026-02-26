---
name: parquet-optimization
description: This skill should be used when the user is working with Parquet file reading, writing, compression, bloom filters, row group sizing, column statistics, predicate pushdown, or storage optimization. Also use when discussing compaction strategies, file sizing, or storage efficiency.
version: 1.0.0
---

# Parquet Optimization for Time-Series Data

## MynaDB Parquet Configuration

**File:** `src/storage/parquet_writer.rs`

Current defaults:
- Row group size: 1,000,000 rows
- Bloom filter FPP: 1% (0.01)
- Bloom filters on string/tag columns
- Compression: configurable (default zstd)
- Column statistics: min/max enabled
- Page-level index: enabled for fine-grained pruning

## Compression Strategy

**File:** `src/config.rs` → `CompressionConfig`

MynaDB supports adaptive compression:

| Strategy | Codec | When |
|----------|-------|------|
| Hot data (< 7 days) | Snappy or LZ4 | Fast read/write, moderate ratio |
| Cold data (> 30 days) | Zstd level 9 | Maximum compression, slower read |
| Fixed | Single codec | All data uses same codec |

Background recompression job (`src/jobs/recompression.rs`) re-encodes old data from hot to cold codec.

**Codec comparison for time-series:**

| Codec | Compression Ratio | Write Speed | Read Speed | Best For |
|-------|-------------------|-------------|------------|----------|
| Snappy | ~2x | Very fast | Very fast | Hot data, real-time queries |
| LZ4 | ~2.5x | Fast | Very fast | General purpose |
| Zstd(1) | ~3.5x | Fast | Fast | Balanced |
| Zstd(9) | ~4-5x | Slow | Fast | Cold archival |
| Zstd(19) | ~5-6x | Very slow | Fast | Maximum compression |

## Row Group Sizing

**Impact on performance:**
- Too small (< 100K rows): excessive metadata overhead, poor compression
- Too large (> 10M rows): memory pressure during read, poor predicate selectivity
- Sweet spot for TSDB: **500K - 2M rows**

MynaDB uses 1M rows per row group as default.

**Row group pruning:** DataFusion reads row group statistics (min/max per column) to skip row groups that don't match WHERE clause predicates. Effective for:
- Time range queries (`WHERE time > X AND time < Y`)
- Tag equality (`WHERE host = 'server-1'`)

## Bloom Filters

Bloom filters enable efficient string column filtering without reading actual data.

**When to use:**
- High-cardinality string columns (hostname, trace_id, user_id)
- Equality predicates (`WHERE tag = 'value'`)
- NOT useful for range predicates

**MynaDB configuration:**
- FPP 1% = ~10 bits per element
- Applied to all Utf8 (string/tag) columns
- Increases file size by ~1-2%
- Can eliminate 99% of non-matching row groups

## Column Statistics

Parquet stores min/max statistics per column per row group. MynaDB leverages:
- **Time column stats**: Skip row groups outside query time range
- **Date column stats**: Partition pruning at row group level
- **Tag column stats**: Skip groups where tag value is outside range

## Sort Order Optimization

**File:** `src/jobs/compactor/sorting.rs`

MynaDB uses cardinality-aware sort ordering:
1. Sample up to 100K rows to estimate cardinality per column
2. Sort columns by cardinality: low-cardinality first → high-cardinality last → time last
3. Low-cardinality columns first = better row group pruning + better compression

**Example:** For data with columns (host, region, cpu, time):
- host: 100 distinct values → sort position 2
- region: 5 distinct values → sort position 1
- cpu: 10,000+ distinct values → sort position 3
- time: unique → sort position 4

Sort order: `region, host, cpu, time`

This maximizes:
- Row group pruning (region/host filters eliminate entire groups)
- Dictionary encoding efficiency (repeated values grouped together)
- Run-length encoding effectiveness

## File Sizing

**Target sizes:**
- L0 (flush): Variable, depends on flush interval and write rate
- L1 (minor compaction): ~200MB target
- L2+ (major compaction): ~500MB target
- Full compaction: Single file per date partition per node

**Why 200-500MB:**
- S3 optimal read size: 8-256MB (parallel range reads)
- Parquet footer overhead: negligible at this size
- Memory during compaction: ~2x file size needed
- DataFusion parallelism: one task per row group

## Predicate Pushdown Chain

```
SQL WHERE clause
  → DataFusion logical plan (Filter node)
  → ParquetTableProvider::scan() receives filters
  → supports_filters_pushdown() → Exact/Inexact
  → ParquetExec with pushed predicates
  → Row group pruning (min/max stats)
  → Page-level pruning (page index)
  → Bloom filter check (string equality)
  → Row-level filter (after decode)
```

Each level eliminates data progressively, reducing I/O.

## Writing Best Practices

1. **Sort before write**: Sorted data compresses 2-5x better than unsorted
2. **Use dictionary encoding**: Automatic for strings < 10K distinct values
3. **Align row groups with partitions**: Don't split logical groups across row groups
4. **Write statistics**: Always enable column statistics for pruning
5. **Enable bloom filters**: 1-2% file size cost for 99% filter efficiency

## Reading Best Practices

1. **Project only needed columns**: Column pruning avoids reading unused data
2. **Push predicates to scan**: Time and tag filters benefit from pushdown
3. **Use LIMIT**: DataFusion's TopK optimization stops early
4. **Avoid SELECT ***: Columnar format penalty for reading all columns
5. **Cache file metadata**: Avoid re-reading Parquet footer on every query
