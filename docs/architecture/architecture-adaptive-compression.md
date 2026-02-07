# Adaptive Compression Architecture

## Overview

MynaDB's adaptive compression system selects optimal Parquet compression codecs per-column and per-partition-age, balancing write throughput (hot data) against storage efficiency (cold data). It replaces the previous hardcoded ZSTD-3 compression with a configurable strategy that adapts to data characteristics.

## Design Goals

1. **Hot data favors speed**: Recent data uses fast codecs (Snappy/LZ4) for low-latency writes and reads
2. **Cold data favors ratio**: Older data uses high-ratio codecs (ZSTD-9) to minimize storage costs
3. **Type-aware selection**: Numeric columns get different treatment than string columns
4. **Backward compatible**: Default configuration produces identical behavior to the previous hardcoded ZSTD-3
5. **Zero query impact**: Parquet readers transparently decompress any codec, so queries work regardless of compression

## Architecture

```
                    ┌─────────────────────────┐
                    │    config.toml           │
                    │  [storage.compression]   │
                    │    strategy = "adaptive"  │
                    │    hot_codec = "snappy"   │
                    │    cold_codec = "zstd"    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  CompressionStrategy    │
                    │  ├─ Fixed(Compression)   │
                    │  └─ Adaptive(Config)     │
                    └────────────┬────────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
             ┌───────────┐ ┌──────────┐ ┌──────────────┐
             │ParquetWriter│ │Compactor │ │Downsampling │
             │  (writes)  │ │(compacts)│ │  Job         │
             └─────┬──────┘ └────┬─────┘ └──────┬───────┘
                   │             │               │
                   ▼             ▼               ▼
         ┌──────────────────────────────────────────────┐
         │        build_writer_properties()              │
         │  Per-column: compression + bloom filters      │
         │  Per-age: hot → snappy, cold → zstd-9         │
         └──────────────────────────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  WriterProperties       │
                    │  ├─ Global compression   │
                    │  ├─ Column overrides     │
                    │  └─ Bloom filter config  │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Parquet File Output    │
                    │  (mixed codecs per col) │
                    └─────────────────────────┘
```

## Components

### CompressionStrategy (`src/storage/compression.rs`)

The central enum that encapsulates compression selection logic:

```rust
pub enum CompressionStrategy {
    Fixed(Compression),           // Single codec for everything
    Adaptive(AdaptiveConfig),     // Per-column, per-age selection
}
```

**Selection priority** (Adaptive mode):

1. **Age-based override** (highest priority):
   - Partition age <= `hot_days` → hot codec (default: Snappy)
   - Partition age >= `cold_days` → cold codec (default: ZSTD-9)
2. **Type-based selection** (warm data):
   - Int/UInt/Float/Timestamp → numeric codec (default: LZ4)
   - Utf8/Dictionary → string codec (default: ZSTD-3)
   - Boolean → uncompressed (already 1 bit per value)
3. **Default fallback**: ZSTD-3

### Per-Column Application

`build_writer_properties()` iterates over schema fields and applies:
- Compression codec per column (via `set_column_compression`)
- Bloom filters for string columns (tag filtering optimization)
- Dictionary encoding for all columns
- Page-level statistics for predicate pushdown

### RecompressionJob (`src/jobs/recompression.rs`)

Background job that rewrites files when partitions age into a new compression tier:

```
Day 0-7:   Written with Snappy (fast writes)
Day 8:     Recompression rewrites with LZ4/ZSTD-3 (warm tier)
Day 30+:   Recompression rewrites with ZSTD-9 (cold tier)
```

**Tracking**: Uses an in-memory `HashSet<String>` keyed by `"catalog/schema/table/date=X:tier"` to avoid reprocessing.

**Safety**: Only writes back if the new file is not more than 5% larger than the original (handles edge cases where a different codec may not improve compression for certain data patterns).

### Integration Points

| Component | How it uses CompressionStrategy |
|-----------|-------------------------------|
| **DataCache** | `set_compression_strategy()` configures the underlying ParquetWriter for flush writes |
| **ParquetWriter** | `with_compression()` constructor, uses strategy in `create_optimized_writer_props()` |
| **Compactor** | `with_metadata_and_compression()` constructor, uses strategy for compacted file output |
| **DownsamplingJob** | `with_compression()` constructor, tier table files use the strategy |
| **RecompressionJob** | Owns strategy directly, rewrites files that aged into new tiers |

## Data Flow

### Write Path (hot data)
```
Line Protocol → DataCache → ParquetWriter.write_batches_partitioned()
                                └─ strategy.build_writer_properties(schema, age=None)
                                    └─ hot_compression (Snappy) for all columns
```

### Compaction Path
```
Compactor.compact_partition()
    └─ self.create_optimized_writer_props(schema, partition_age)
        └─ strategy.build_writer_properties(schema, age=Some(N))
            └─ Per-column codec based on age + data type
```

### Recompression Path
```
RecompressionJob.run_cycle()
    └─ For each partition:
        ├─ Calculate age in days
        ├─ Determine tier (hot/warm/cold)
        ├─ Skip if already recompressed at this tier
        └─ Read → rewrite with optimal compression → put back
```

## Configuration

```toml
[storage.compression]
strategy = "adaptive"          # "fixed" or "adaptive"
default_codec = "zstd"         # Default for unmatched columns
zstd_level = 3                 # ZSTD level for warm data
hot_days = 7                   # Partition age threshold for hot
cold_days = 30                 # Partition age threshold for cold
hot_codec = "snappy"           # Fast codec for recent data
cold_codec = "zstd"            # High-ratio codec for old data
cold_zstd_level = 9            # ZSTD level for cold data
numeric_codec = "lz4"          # Codec for numeric columns (warm)
string_codec = "zstd"          # Codec for string columns (warm)
recompression_enabled = false  # Enable background recompression
recompression_interval_secs = 86400  # Check interval (24h)
```

## Compression Codec Characteristics

| Codec | Speed | Ratio | Best For |
|-------|-------|-------|----------|
| Snappy | Very fast | Low | Hot data, real-time ingestion |
| LZ4 | Fast | Medium | Numeric columns, warm data |
| ZSTD-3 | Medium | Good | General purpose, warm data |
| ZSTD-9 | Slow | Excellent | Cold data, archival storage |
| Uncompressed | N/A | 1:1 | Boolean columns (already compact) |

## File Format Compatibility

Parquet files store the compression codec in each column chunk's metadata. This means:
- Files written with different codecs can coexist in the same partition
- Readers automatically detect and decompress using the correct codec
- No schema changes or migration needed when switching strategies
- Mixed-codec partitions (from recompression) are fully transparent to queries
