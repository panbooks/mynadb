# Compaction

## Overview

Compaction solves the "small file problem" by merging multiple small Parquet files into fewer large files using an LSM-tree style leveled strategy (L0 → L1 → L2 → L3 → L4).

## File Levels

| Level | Description |
|-------|-------------|
| L0 | Raw files from cache flush |
| L1 | First compaction (merged L0 files) |
| L2 | Second compaction (merged L1 files) |
| L3 | Third compaction (merged L2 files) |
| L4 | Final level (largest files) |

**File naming**: `L{level}_{timestamp}.parquet`

## Configuration

```toml
[compaction]
min_files = 5              # L0 files to trigger minor compaction
major_min_files = 4        # L1+ files to trigger major compaction
max_file_size_mb = 100     # Files larger than this are prioritized
target_file_size_mb = 500  # Target size after compaction
max_level = 4              # Maximum level (L0-L4)
interval_secs = 60         # Background compaction interval
```

## API

```bash
# Compact entire database
curl -X POST http://localhost:7000/compact/mydb

# Compact specific table
curl -X POST http://localhost:7000/compact/mydb/default/cpu

# Response
{
  "status": "success",
  "results": [
    {
      "table": "mydb.default.cpu",
      "date_partition": "date=2024-01-15",
      "l0_files_before": 10,
      "l0_files_after": 0,
      "files_before": 15,
      "files_after": 4
    }
  ]
}
```

## Compaction Process

```
1. Lock partition
2. List all parquet files, sort by level/timestamp
3. Minor compaction: L0 → L1 (if L0 files >= min_files)
4. Major compaction: L1 → L2 → L3 → L4 (if files >= major_min_files)
5. Delete source files, keep compacted files
6. Release lock
```

## Table Types

### Partitioned Tables (default)
```
data/mydb/default/cpu/
├── date=2024-01-15/
│   ├── L0_*.parquet
│   └── L1_*.parquet
└── date=2024-01-16/
    └── L0_*.parquet
```
Each partition is compacted independently.

### Non-Partitioned Tables
```
data/_system/series_stats/mydb/
├── L0_*.parquet
└── L1_*.parquet
```
Files compacted directly in table directory.

## Best Practices

| Workload | Configuration |
|----------|---------------|
| High write rate | Increase `min_files`, `major_min_files` |
| Query-heavy | Lower `min_files` for fewer files |
| Large dataset | Increase `target_file_size_mb` |

## Troubleshooting

**Too many L0 files accumulating**
- Decrease `min_files` or `interval_secs`
- Run manual compaction: `POST /compact/mydb`

**Compaction too slow**
- Increase `target_file_size_mb`
- Compact during off-peak hours

**Queries return stale data after compaction**
- DataFusion auto-refreshes on next query
- Or restart server to force refresh
