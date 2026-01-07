# Memory Optimization Guide

This document explains memory usage in Stats Service and how to optimize it.

## Memory Components

### 1. **Bloom Filters (Largest Consumer)**

Bloom filters are used for series deduplication to prevent duplicate records in PMC/UMC tables.

**Memory Formula:**
```
bits = -n * ln(p) / (ln(2)^2)
bytes = bits / 8
```

Where:
- `n` = expected_series_per_day
- `p` = false_positive_rate

**Key Points:**
- Memory is allocated **per database, per schema (PMC+UMC), per day**
- Total bloom memory = `memory_per_day × cache_retention_days × 2 (PMC+UMC)`
- Lower false_positive_rate = more accurate but MORE memory
- Higher expected_series = MORE memory

**Example Calculation:**

```toml
[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
expected_series_per_day = 2000000000  # 2 billion
false_positive_rate = 0.005           # 0.5%
```

With `cache_retention_days = 2`:
- Per day per schema: ~2.75 GB
- Total: 2.75 GB × 2 days × 2 schemas = **~11 GB**

### 2. **Data Cache**

In-memory buffer for recent writes before flushing to Parquet.

**Memory:** Configured via `cache.max_size_mb` (default: 4096 MB = 4 GB)

### 3. **DataFusion/Arrow**

Query engine internals: ~1-2 GB

### 4. **Other Components**

- HTTP/gRPC buffers: < 100 MB
- Series tracker pending writes: < 100 MB
- Rust overhead: ~500 MB

## Monitoring Memory Usage

### 1. Built-in Memory Endpoint

```bash
curl http://localhost:7000/memory
```

**Output:**
```
Memory Usage Statistics:
══════════════════════════════════════════

Data Cache:
  Size: 0 B (0 bytes)
  Limit: 4.00 GB (4294967296 bytes)

Series Deduplication (Bloom/HashSet):
  Total: 10.50 GB (11274289152 bytes)

  Per Database:
    arch_monitor_roc               10.27 GB (   11029381120 bytes, strategy: bloom_filter)
    arch_monitor_rec                244 MB (     244908032 bytes, strategy: bloom_filter)

──────────────────────────────────────────
Approximate Total: 14.50 GB (15569256448 bytes)
══════════════════════════════════════════
```

### 2. Memory Analysis Script

```bash
cd scripts
./analyze_memory.sh
```

This script:
- Shows system and process memory
- Calculates expected bloom filter memory
- Provides optimization recommendations
- Compares expected vs actual usage

### 3. System Tools

```bash
# Process memory
ps aux | grep tsdb-stats

# Detailed memory tracking
pidstat -r -p $(pgrep tsdb-stats) 1

# Memory map
pmap -x $(pgrep tsdb-stats)
```

## Optimization Strategies

### Strategy 1: Reduce Bloom Filter Size (Recommended)

**Option A: Lower Expected Series**

Set realistic estimates based on actual cardinality:

```bash
# Query actual series counts
mysql> SELECT COUNT(*) FROM _system.umc.your_database;
mysql> SELECT COUNT(*) FROM _system.pmc.your_database;
```

Update config:
```toml
[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
expected_series_per_day = 1000000000  # Was: 2B (saves ~50%)
false_positive_rate = 0.005
```

**Option B: Increase False Positive Rate**

Trade accuracy for memory (1% FP rate is usually acceptable):

```toml
[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
expected_series_per_day = 2000000000
false_positive_rate = 0.01  # Was: 0.005 (saves ~35%)
```

**Option C: Reduce Cache Retention Days**

Only keep recent data in bloom filters:

```toml
[series_tracker]
cache_retention_days = 1  # Was: 2 (saves 50%)
```

**Combined Example:**

```toml
[series_tracker]
cache_retention_days = 1  # Reduced from 2

[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
expected_series_per_day = 1000000000  # Reduced from 2B
false_positive_rate = 0.01             # Increased from 0.005

[[series_tracker.bloom_custom]]
database = "arch_monitor_rec"
expected_series_per_day = 5000000000  # Reduced from 10B
false_positive_rate = 0.02            # Increased from 0.01
```

**Savings:** ~70% (from 59GB to ~18GB)

**Option D: Schema-Specific Configuration (NEW)**

Configure different bloom filter settings for PMC vs UMC schemas since they have different cardinalities:
- **PMC (Physical Metric Count)**: includes ALL tags → higher cardinality
- **UMC (User Metric Count)**: excludes system tags → lower cardinality

```toml
[series_tracker]
cache_retention_days = 1

# PMC has higher cardinality (includes all tags)
[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
schema = "pmc"
expected_series_per_day = 2000000000  # 2 billion
false_positive_rate = 0.01

# UMC has lower cardinality (excludes system tags)
[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
schema = "umc"
expected_series_per_day = 500000000   # 500 million (25% of PMC)
false_positive_rate = 0.01
```

**Benefits:**
- Right-size memory allocation per schema
- Avoid over-allocating for UMC
- PMC: 2.75 GB per day
- UMC: 0.69 GB per day
- Total: 3.44 GB × 1 day = **~3.4 GB** (instead of 5.5 GB if both use 2B)

### Strategy 2: Switch to HashSet

For databases with < 100M series/day, HashSet is more memory-efficient:

```toml
[series_tracker]
default_strategy = "hashset"

# Remove from bloom_databases list
bloom_databases = []  # Or only keep high-cardinality DBs
```

**Memory Comparison:**
- Bloom filter (1% FP): ~1.4 bytes/series
- HashSet: ~24 bytes/series

**Break-even:** ~17M series (HashSet better below this)

### Strategy 3: Split Databases

Separate high and low cardinality metrics:

```
# High cardinality (use bloom)
arch_monitor_roc_high → bloom filter

# Low cardinality (use hashset)
arch_monitor_roc_low → hashset
```

Configure:
```toml
bloom_databases = ["*_high"]  # Only high-cardinality DBs
default_strategy = "hashset"   # Default for others
```

### Strategy 4: Reduce Data Cache

If writes are infrequent:

```toml
[cache]
max_size_mb = 1024  # Reduced from 4096 (saves 3GB)
flush_interval_secs = 15  # Flush more frequently
```

## Configuration Templates

### Small Deployment (<1B series/day)

```toml
[cache]
max_size_mb = 1024

[series_tracker]
cache_retention_days = 1
default_strategy = "hashset"
bloom_databases = []

[series_tracker.bloom]
expected_series_per_day = 100000000  # 100M
false_positive_rate = 0.01
```

**Expected Memory:** ~5 GB

### Medium Deployment (1-5B series/day)

```toml
[cache]
max_size_mb = 2048

[series_tracker]
cache_retention_days = 1
default_strategy = "hashset"
bloom_databases = ["high_cardinality_db"]

[[series_tracker.bloom_custom]]
database = "high_cardinality_db"
expected_series_per_day = 2000000000
false_positive_rate = 0.01
```

**Expected Memory:** ~10 GB

### Large Deployment (>5B series/day)

```toml
[cache]
max_size_mb = 4096

[series_tracker]
cache_retention_days = 1
default_strategy = "bloom"
bloom_databases = ["*"]

[[series_tracker.bloom_custom]]
database = "arch_monitor_roc"
expected_series_per_day = 2000000000
false_positive_rate = 0.01

[[series_tracker.bloom_custom]]
database = "arch_monitor_rec"
expected_series_per_day = 5000000000
false_positive_rate = 0.02
```

**Expected Memory:** ~20 GB

## Troubleshooting

### High Memory Usage

1. **Check actual vs configured:**
   ```bash
   curl http://localhost:7000/memory
   ./scripts/analyze_memory.sh
   ```

2. **Query actual cardinality:**
   ```sql
   SELECT database, COUNT(*)
   FROM _system.umc
   GROUP BY database;
   ```

3. **Compare to config** and adjust `expected_series_per_day`

### Out of Memory (OOM)

1. **Immediate fix:** Restart with lower config
   ```bash
   # Edit config.toml - reduce expected_series or increase FP rate
   systemctl restart stats-service
   ```

2. **Long-term fix:** Optimize based on actual usage

### Slow Startup

Large bloom filters take time to load from disk.

**Solutions:**
- Reduce `cache_retention_days`
- Delete old series stats: `rm -rf /data0/stats/data/_system/{pmc,umc}/*`
- Use hashset strategy

## Best Practices

1. **Monitor actual cardinality monthly**
   - Query UMC/PMC tables
   - Adjust config based on trends

2. **Start conservative**
   - Begin with lower `expected_series_per_day`
   - Increase if you see duplicates

3. **Use different strategies per database**
   - Bloom for high cardinality (>100M)
   - HashSet for low cardinality (<100M)

4. **Test changes**
   ```bash
   # Before
   curl http://localhost:7000/memory

   # Change config and restart

   # After
   curl http://localhost:7000/memory
   ```

5. **Balance accuracy vs memory**
   - 1% FP rate is usually fine
   - 0.5% FP only if critical

## FAQ

**Q: Why is my memory 24GB with 2B expected series?**

A: Bloom filters for 2 databases × 2 schemas × 2 days. See calculation above.

**Q: Can I reduce memory without losing data?**

A: Yes! Bloom filters are just for deduplication caching. Reducing size may cause temporary duplicates until next flush, but won't lose data.

**Q: What happens if bloom filter is too small?**

A: More false positives → more duplicate series records in UMC/PMC tables (but data tables are unaffected).

**Q: Should I use bloom or hashset?**

A:
- Bloom: >100M series/day (saves memory)
- HashSet: <100M series/day (exact, no false positives)

**Q: How do I clear bloom filters?**

A: Delete series stats and restart:
```bash
rm -rf /data0/stats/data/_system/{pmc,umc}/*
systemctl restart stats-service
```

## Related Documentation

- [Series Cardinality Tracking](features-series-cardinality.md)
- [Configuration Guide](../config.toml)
- [Architecture Overview](../CLAUDE.md)
