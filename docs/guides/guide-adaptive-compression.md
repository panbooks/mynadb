# Adaptive Compression Guide

Reduce storage costs 2-5x for historical data by automatically selecting optimal compression codecs based on data type and partition age.

## Quick Start

### 1. Enable Adaptive Compression

Add to your `config.toml`:

```toml
[storage.compression]
strategy = "adaptive"
```

That's it. The defaults are tuned for typical time series workloads:
- Hot data (< 7 days): Snappy (fast writes/reads)
- Warm data (7-30 days): LZ4 for numbers, ZSTD-3 for strings
- Cold data (> 30 days): ZSTD-9 (maximum compression)

### 2. Enable Background Recompression (Optional)

To automatically rewrite files as they age into better compression tiers:

```toml
[storage.compression]
strategy = "adaptive"
recompression_enabled = true
recompression_interval_secs = 86400  # Check once per day
```

### 3. Verify

Check logs for compression strategy messages:

```
INFO  Compression strategy: ADAPTIVE (per-column, age-based)
INFO  Recompression: ENABLED (interval: 86400s)
```

## How It Works

### Per-Column Compression

Different data types compress better with different codecs:

| Data Type | Codec | Why |
|-----------|-------|-----|
| Int64, Float64, Timestamp | LZ4 | Fast decompression for numeric scans |
| Utf8 (tags like `host`, `region`) | ZSTD-3 | Good ratio for repeated strings |
| Boolean | Uncompressed | Already 1 bit per value |

### Age-Based Tiers

Data transitions through compression tiers as it ages:

```
Age 0-7 days:    Snappy    (fast writes, low CPU)
Age 7-30 days:   Type-based (LZ4/ZSTD per column)
Age 30+ days:    ZSTD-9    (maximum compression)
```

When recompression is enabled, a background job rewrites files when they cross tier boundaries.

### What Gets Compressed

The adaptive strategy applies to all Parquet write paths:

- **Cache flush**: New data written from memory to L0 files
- **Compaction**: Merged files from L0 → L1 → L2
- **Downsampling**: Tier table files (if downsampling is enabled)
- **Recompression**: Existing files rewritten with optimal codec

## Configuration Reference

```toml
[storage.compression]
# Compression strategy: "fixed" or "adaptive" (default: "fixed")
strategy = "fixed"

# Default codec for all columns in fixed mode, or unmatched columns in adaptive
# Options: "zstd", "snappy", "lz4", "gzip", "brotli", "uncompressed"
default_codec = "zstd"

# ZSTD compression level for warm data (1-22, default: 3)
zstd_level = 3

# --- Adaptive mode settings (only used when strategy = "adaptive") ---

# Hot tier: partitions younger than this many days
hot_days = 7

# Cold tier: partitions older than this many days
cold_days = 30

# Codec for hot (recent) data - optimized for write speed
hot_codec = "snappy"

# Codec for cold (old) data - optimized for compression ratio
cold_codec = "zstd"

# ZSTD level for cold data (higher = better ratio, slower)
cold_zstd_level = 9

# Codec for numeric columns (Int, Float, Timestamp) in warm tier
numeric_codec = "lz4"

# Codec for string columns (Utf8, Dictionary) in warm tier
string_codec = "zstd"

# --- Background recompression ---

# Enable background recompression of aging partitions
recompression_enabled = false

# How often to scan for partitions needing recompression (seconds)
recompression_interval_secs = 86400
```

## Example Configurations

### High-Throughput Ingestion

Minimize CPU overhead for maximum write speed:

```toml
[storage.compression]
strategy = "adaptive"
hot_days = 14          # Keep fast compression longer
hot_codec = "snappy"
cold_days = 60
cold_codec = "zstd"
cold_zstd_level = 5   # Moderate cold compression
```

### Storage-Optimized

Maximize compression for cost-sensitive deployments:

```toml
[storage.compression]
strategy = "adaptive"
hot_days = 1           # Only 1 day of fast compression
hot_codec = "lz4"      # LZ4 instead of Snappy (better ratio)
cold_days = 7
cold_codec = "zstd"
cold_zstd_level = 12   # Very high compression
recompression_enabled = true
recompression_interval_secs = 3600  # Check every hour
```

### Fixed Codec (Simple)

Use a single codec for everything (backward compatible):

```toml
[storage.compression]
strategy = "fixed"
default_codec = "zstd"
zstd_level = 3
```

## Storage Savings Example

For a typical metrics workload (100 hosts, 1-second resolution):

| Tier | Codec | Ratio vs Raw | Size/day |
|------|-------|-------------|----------|
| Hot (Snappy) | Snappy | ~4:1 | ~125 MB |
| Warm (LZ4/ZSTD-3) | Mixed | ~6:1 | ~83 MB |
| Cold (ZSTD-9) | ZSTD-9 | ~10:1 | ~50 MB |

With a 90-day retention policy:
- **Fixed ZSTD-3**: ~7.5 GB (90 days x 83 MB)
- **Adaptive**: ~5.1 GB (7 x 125 + 23 x 83 + 60 x 50 MB) -- **32% savings**

## Monitoring

### Log Messages

```
INFO  Compression strategy: ADAPTIVE (per-column, age-based)
INFO  Recompression: ENABLED (interval: 86400s)
DEBUG Adaptive compression: 5 columns, partition_age=Some(3) days
INFO  Recompressed partition catalog/schema/table/date=2026-01-01: 3 files, 15234567 bytes saved
```

### Recompression Progress

The recompression job logs per-cycle summaries:

```
INFO  Recompression cycle complete: 12 files recompressed, 45678901 bytes saved
DEBUG Recompression cycle: no work needed
```

## Query Compatibility

Adaptive compression is fully transparent to queries. Parquet files store codec metadata per column chunk, so:

- No query syntax changes needed
- Mixed-codec files in the same partition work seamlessly
- DataFusion, Arrow Flight, and MySQL wire protocol all work normally
- `EXPLAIN` and `ANALYZE` queries are unaffected

## FAQ

**Q: Does changing the strategy affect existing files?**
A: No. Existing files keep their original compression. Only new writes use the new strategy. Enable recompression to gradually rewrite old files.

**Q: Can I switch from adaptive back to fixed?**
A: Yes. Change `strategy = "fixed"` and restart. Existing files remain readable regardless of their codec.

**Q: Does recompression block queries?**
A: No. Recompression reads and rewrites files atomically. Queries may briefly read the old or new version, both are valid.

**Q: What if recompression makes a file larger?**
A: The job skips files where the new compression would increase size by more than 5%. Some data patterns compress better with certain codecs.
