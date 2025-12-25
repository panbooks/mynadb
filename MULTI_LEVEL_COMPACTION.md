# Multi-Level Compaction Strategy

## Overview

The compactor now supports a full multi-level compaction strategy similar to LSM-tree (Log-Structured Merge-Tree) databases. Files are compacted through multiple levels: **L0 → L1 → L2 → L3 → L4**.

**Supports both partitioned and non-partitioned tables:**
- **Partitioned tables**: Time-series data with Hive-style partitions (e.g., `date=2025-12-24`)
- **Non-partitioned tables**: Metadata tables like `series_stats` without date partitions

## Architecture

### Table Types

#### Partitioned Tables
```
data/
  benchmark/
    svc_1/
      cpu/
        date=2025-12-24/
          L0_1703251234567.parquet
          L0_1703251234890.parquet
          L1_1703251235123.parquet
        date=2025-12-25/
          L0_1703251235456.parquet
```

#### Non-Partitioned Tables
```
data/
  _system/
    default/
      series_stats/
        L0_1703251234567.parquet
        L0_1703251234890.parquet
        L1_1703251235123.parquet
```

### File Levels

- **L0 (Level 0)**: Fresh write files, created by the cache flush process
- **L1 (Level 1)**: First compaction level, result of L0 minor compaction
- **L2 (Level 2)**: Second compaction level, result of L1 major compaction
- **L3 (Level 3)**: Third compaction level, result of L2 major compaction
- **L4 (Level 4)**: Final compaction level, result of L3 major compaction (largest files)

### File Naming Convention

Files are named with level prefix and timestamp:
```
L0_1703251234567.parquet  # Level 0 file
L1_1703251234890.parquet  # Level 1 file
L2_1703251235123.parquet  # Level 2 file
L3_1703251235456.parquet  # Level 3 file
L4_1703251235789.parquet  # Level 4 file
```

## Automatic Table Type Detection

The compactor automatically detects whether a table is partitioned or non-partitioned:

### Detection Algorithm

```rust
fn has_partitions(table_path) -> bool {
    // Check if table directory contains subdirectories starting with "date="
    for entry in read_dir(table_path) {
        if entry.is_dir() && entry.name.starts_with("date=") {
            return true;  // Partitioned table
        }
    }
    return false;  // Non-partitioned table
}
```

### Compaction Path Selection

| Table Type | Parquet File Location | Compaction Target |
|------------|----------------------|-------------------|
| **Partitioned** | `table/date=YYYY-MM-DD/*.parquet` | Each partition separately |
| **Non-partitioned** | `table/*.parquet` | Table directory directly |

### Examples

**Partitioned Table** (`benchmark.svc_1.cpu`):
- Has subdirectories: `date=2025-12-24/`, `date=2025-12-25/`
- Compacts each partition independently
- Result label: `date=2025-12-24`

**Non-Partitioned Table** (`_system.default.series_stats`):
- No subdirectories with `date=` prefix
- Compacts all files in table root directory
- Result label: `[non-partitioned]`

## Compaction Rules

### Minor Compaction (L0 → L1)

**Trigger Condition**: When the number of L0 files >= `min_files` (default: 5)

**Process**:
1. Collect all L0 files in a partition
2. Group files by size (avoiding exceeding `target_file_size`)
3. Read and merge each group
4. Write compacted data to new L1 file(s)
5. Delete source L0 files

**Priority**: Highest (always runs first)

### Major Compaction (L1 → L2, L2 → L3, L3 → L4)

**Trigger Condition**: When the number of files at level N >= `major_min_files` (default: 4)

**Process**:
1. Collect all files at level N
2. Group files by size
3. Read and merge each group
4. Write compacted data to new L(N+1) file(s)
5. Delete source level N files

**Priority**: Sequential (L1→L2, then L2→L3, then L3→L4)

**Limitation**: L4 is the final level; L4 files are never compacted further (unless they accumulate and trigger another L4 compaction to merge themselves)

## Configuration

### Config File (`config.toml`)

```toml
[compaction]
# Minimum number of L0 files to trigger minor compaction
min_files = 5

# Minimum number of files at L1+ to trigger major compaction
major_min_files = 4

# Maximum file size in MB before compaction
max_file_size_mb = 100

# Target file size in MB after compaction
target_file_size_mb = 500

# Maximum compaction level (0-4: L0, L1, L2, L3, L4)
max_level = 4

# Background compaction interval in seconds (0 = disabled)
interval_secs = 60
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_files` | usize | 5 | Minimum L0 files to trigger L0→L1 compaction |
| `major_min_files` | usize | 4 | Minimum files at L1+ to trigger major compaction |
| `max_file_size_mb` | u64 | 100 | Files larger than this are prioritized for compaction |
| `target_file_size_mb` | u64 | 500 | Target size for compacted files |
| `max_level` | u32 | 4 | Maximum level (L0-L4) |
| `interval_secs` | u64 | 60 | Background compaction interval |

## Compaction Flow Example

### Scenario: Processing 20 L0 Files

```
Initial State:
  L0: 20 files (each ~50MB)

After First Minor Compaction (L0 → L1):
  L0: 0 files
  L1: 4 files (each ~250MB)

After Second Batch of Writes:
  L0: 10 new files
  L1: 4 files

After Second Minor Compaction:
  L0: 0 files
  L1: 6 files (4 old + 2 new)

After First Major Compaction (L1 → L2):
  L0: 0 files
  L1: 0 files
  L2: 2 files (each ~750MB)

After Multiple Rounds:
  L0: 3 files
  L1: 2 files
  L2: 5 files
  L3: 0 files
  L4: 0 files

After Next Compaction Cycle:
  L0: 0 files (compacted to L1)
  L1: 1 file (3 L0 → 1 L1)
  L2: 0 files (5 L2 → 1 L3)
  L3: 1 file
  L4: 0 files
```

## API Endpoints

### Manual Compaction

```bash
# Compact all tables in a catalog
POST /compact/:catalog

# Compact specific table
POST /compact/:catalog/:schema/:table

# Compact specific partition
POST /compact/:catalog/:schema/:table/:partition
```

### Example

```bash
# Trigger compaction for benchmark catalog
curl -X POST http://localhost:7000/compact/benchmark

# Response (Partitioned table)
{
  "status": "success",
  "message": "Compacted 3 partition(s) in catalog 'benchmark'",
  "results": [
    {
      "catalog": "benchmark",
      "schema": "svc_1",
      "table": "cpu",
      "date_partition": "date=2025-12-24",
      "l0_files_before": 10,
      "l0_files_after": 0,
      "files_before": 15,
      "files_after": 7,
      "bytes_before": 750000000,
      "bytes_after": 735000000,
      "rows_compacted": 1500000
    }
  ]
}

# Compact non-partitioned table (series_stats)
curl -X POST http://localhost:7000/compact/_system/default/series_stats

# Response (Non-partitioned table)
{
  "status": "success",
  "message": "Compacted table '_system.default.series_stats'",
  "result": {
    "catalog": "_system",
    "schema": "default",
    "table": "series_stats",
    "date_partition": "[non-partitioned]",
    "l0_files_before": 8,
    "l0_files_after": 0,
    "files_before": 8,
    "files_after": 2,
    "bytes_before": 120000000,
    "bytes_after": 118000000,
    "rows_compacted": 50000
  }
}
      "l0_files_before": 10,
      "l0_files_after": 0,
      "files_before": 15,
      "files_after": 7,
      "bytes_before": 750000000,
      "bytes_after": 735000000,
      "rows_compacted": 1500000
    }
  ]
}
```

## Performance Benefits

### Read Performance

1. **Fewer Files to Scan**: Reduced number of files improves query planning
2. **Better Compression**: Larger files achieve better compression ratios
3. **Partition Pruning**: Fewer files means faster partition filtering
4. **Memory Efficiency**: Less overhead from file handles and metadata

### Write Performance

1. **Deferred Compaction**: Writes go to L0, compaction happens asynchronously
2. **Batched Operations**: Group multiple small files into larger ones
3. **Reduced Fragmentation**: Progressive merging keeps data organized

### Storage Benefits

1. **Better Compression**: Larger files compress better
2. **Metadata Overhead**: Fewer files = less filesystem metadata
3. **Deletion Efficiency**: Old files cleaned up after compaction

## Implementation Details

### Compaction Process

```rust
// Main compaction logic in compact_table_partition()

1. Lock partition to prevent concurrent access
2. List and parse all parquet files
3. Sort by level and timestamp

4. Minor Compaction (L0 → L1):
   if l0_files.len() >= config.min_files {
       compact_level_files(partition_path, l0_files, 0, config)
   }

5. Major Compactions (L1 → L2, L2 → L3, L3 → L4):
   for level in 1..=max_level {
       level_files = files.filter(|f| f.level == level)
       if level_files.len() >= config.major_min_files && level < max_level {
           compact_level_files(partition_path, level_files, level, config)
       }
   }

6. Return compaction results
```

### Grouping Algorithm

```rust
// Group files by size to avoid exceeding target size
fn group_files_by_size(files, config) {
    groups = []
    current_group = []
    current_size = 0
    
    for file in files {
        if current_size + file.size > config.target_file_size {
            groups.push(current_group)
            current_group = []
            current_size = 0
        }
        current_group.push(file)
        current_size += file.size
    }
    
    groups.push(current_group)
    return groups
}
```

## Monitoring

### Log Messages

```
INFO Compacting benchmark.svc_1.cpu/date=2025-12-24: 15 files (10 L0), 750.00 MB
INFO Minor compaction: 10 L0 files → L1
INFO Created L1 file: L1_1703251234567.parquet (250000 rows from 10 L0 files)
INFO Major compaction: 4 L1 files → L2
INFO Created L2 file: L2_1703251234890.parquet (1000000 rows from 4 L1 files)
INFO Compaction complete: benchmark.svc_1.cpu/date=2025-12-24: 15 -> 7 files, 750.00 MB -> 735.00 MB
```

### Metrics in Response

```json
{
  "l0_files_before": 10,
  "l0_files_after": 0,
  "files_before": 15,
  "files_after": 7,
  "bytes_before": 750000000,
  "bytes_after": 735000000,
  "rows_compacted": 1500000
}
```

## Best Practices

### Configuration Tuning

1. **High Write Rate**: Increase `min_files` and `major_min_files` to reduce compaction frequency
2. **Low Write Rate**: Decrease thresholds for more aggressive compaction
3. **Large Dataset**: Increase `target_file_size_mb` for better compression
4. **Query-Heavy**: Lower `min_files` to keep file count minimal

### Operational Guidelines

1. **Monitor L0 Files**: High L0 count indicates write pressure
2. **Check Compaction Logs**: Ensure background compaction is running
3. **Manual Trigger**: Use API endpoints during low-traffic periods
4. **Partition Strategy**: Design partitions by date for efficient retention management

## Troubleshooting

### Too Many L0 Files

**Symptom**: L0 files accumulate, query performance degrades

**Solutions**:
- Decrease `min_files` to trigger compaction earlier
- Increase `interval_secs` for more frequent background compaction
- Manually trigger compaction: `POST /compact/:catalog`

### Large L4 Files

**Symptom**: L4 files grow too large

**Solutions**:
- Decrease `target_file_size_mb`
- Implement L4 self-compaction (merge L4 files together)
- Use time-based partitioning (e.g., daily partitions)

### Compaction Too Slow

**Symptom**: Compaction takes too long, blocks writes

**Solutions**:
- Increase `target_file_size_mb` to reduce number of operations
- Increase `major_min_files` to reduce frequency
- Scale horizontally (partition by service/metric)

## Future Enhancements

1. **Parallel Compaction**: Compact multiple partitions simultaneously
2. **Incremental Compaction**: Partial compaction of very large levels
3. **Time-Based Triggers**: Compact based on file age, not just count
4. **L4 Self-Compaction**: Merge L4 files when they accumulate
5. **Compaction Metrics**: Expose Prometheus metrics for monitoring
