# Non-Partitioned Table Compaction Support

## Overview

The compactor now supports **both partitioned and non-partitioned tables** with automatic detection and appropriate handling.

## Table Structure Comparison

### Partitioned Tables (Time-Series Data)

**Structure:**
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

**Characteristics:**
- Contains subdirectories with `date=YYYY-MM-DD` format
- Each partition is compacted independently
- Optimized for time-series data with retention policies
- Examples: `cpu`, `memory`, `disk` metric tables

### Non-Partitioned Tables (Metadata/System Tables)

**Structure:**
```
data/
  _system/
    default/
      series_stats/
        L0_1703251234567.parquet
        L0_1703251234890.parquet
        L1_1703251235123.parquet
```

**Characteristics:**
- Parquet files directly in table directory (no date subdirectories)
- All files compacted together as a single unit
- Suitable for metadata, series tracking, and system tables
- Examples: `series_stats`, configuration tables

## Automatic Detection

### Detection Logic

The compactor automatically determines table type by scanning the table directory:

```rust
fn has_partitions(table_path: &str) -> Result<bool, String> {
    // Scan table directory for subdirectories
    for entry in fs::read_dir(table_path) {
        if entry.is_dir() {
            // Check for Hive-style partition format
            if entry.name.starts_with("date=") {
                return Ok(true);  // Partitioned table detected
            }
        }
    }
    Ok(false)  // No partitions found, treat as flat table
}
```

### Decision Flow

```
Table Directory
    │
    ├─ Contains "date=*" subdirs? ──YES──> Partitioned Table
    │                                       └─> Compact each partition
    │
    └─ No "date=*" subdirs? ──YES──> Non-Partitioned Table
                                      └─> Compact table directory
```

## Compaction Behavior

### For Partitioned Tables

**Function:** `compact_table_partition()`

**Process:**
1. Iterate through all `date=*` subdirectories
2. For each partition:
   - Lock partition
   - List parquet files in partition directory
   - Perform multi-level compaction (L0→L1→L2→L3→L4)
   - Return result with partition label

**Example Result:**
```json
{
  "catalog": "benchmark",
  "schema": "svc_1",
  "table": "cpu",
  "date_partition": "date=2025-12-24",
  "l0_files_before": 10,
  "l0_files_after": 0
}
```

### For Non-Partitioned Tables

**Function:** `compact_non_partitioned_table()`

**Process:**
1. Lock entire table
2. List parquet files directly in table directory
3. Perform multi-level compaction (L0→L1→L2→L3→L4)
4. Return result with `[non-partitioned]` label

**Example Result:**
```json
{
  "catalog": "_system",
  "schema": "default",
  "table": "series_stats",
  "date_partition": "[non-partitioned]",
  "l0_files_before": 8,
  "l0_files_after": 0
}
```

## API Usage

### Compact Entire Catalog (Mixed Tables)

```bash
# Compacts all tables, automatically handling both types
curl -X POST http://localhost:7000/compact/_system

# Response includes both partitioned and non-partitioned results
{
  "status": "success",
  "message": "Compacted 5 table(s) in catalog '_system'",
  "results": [
    {
      "table": "series_stats",
      "date_partition": "[non-partitioned]",
      "files_before": 8,
      "files_after": 2
    },
    {
      "table": "metrics",
      "date_partition": "date=2025-12-24",
      "files_before": 15,
      "files_after": 7
    }
  ]
}
```

### Compact Specific Non-Partitioned Table

```bash
curl -X POST http://localhost:7000/compact/_system/default/series_stats

# Response
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
```

## Implementation Details

### Key Functions

#### 1. `compact_table_all_partitions()`

Entry point for table compaction. Detects table type and routes to appropriate handler.

```rust
pub async fn compact_table_all_partitions(
    catalog: &str,
    schema: &str,
    table: &str,
    config: &CompactionConfig,
) -> Result<Vec<CompactionResult>, String> {
    let has_partitions = self.has_partitions(&table_path)?;
    
    if has_partitions {
        // Handle partitioned table
        compact_each_partition()
    } else {
        // Handle non-partitioned table
        compact_non_partitioned_table()
    }
}
```

#### 2. `compact_non_partitioned_table()`

Handles compaction for non-partitioned tables.

```rust
async fn compact_non_partitioned_table(
    catalog: &str,
    schema: &str,
    table: &str,
    config: &CompactionConfig,
) -> Result<Option<CompactionResult>, String> {
    // Lock with empty partition ID
    let lock = self.get_lock(catalog, schema, table, "").await;
    
    // List files in table root directory
    let file_infos = self.list_parquet_file_infos(&table_path)?;
    
    // Perform multi-level compaction
    // L0 → L1 (if L0 files >= min_files)
    // L1 → L2 (if L1 files >= major_min_files)
    // L2 → L3 (if L2 files >= major_min_files)
    // L3 → L4 (if L3 files >= major_min_files)
    
    // Return result with "[non-partitioned]" label
}
```

#### 3. `has_partitions()`

Detection helper function.

```rust
fn has_partitions(&self, table_path: &str) -> Result<bool, String> {
    for entry in fs::read_dir(table_path)? {
        if entry.is_dir() && entry.name.starts_with("date=") {
            return Ok(true);
        }
    }
    Ok(false)
}
```

### Lock Management

**Partitioned Tables:**
- Lock key: `(catalog, schema, table, "date=2025-12-24")`
- Different partitions can be compacted concurrently

**Non-Partitioned Tables:**
- Lock key: `(catalog, schema, table, "")`
- Empty string used as partition identifier
- Prevents concurrent compaction of same table

## Use Cases

### Series Stats Table

**Purpose:** Track unique series (tag combinations) across all metrics

**Structure:** Non-partitioned (grows over time, not time-bucketed)

**Compaction:** Regularly merge small files into larger ones for efficient cardinality queries

```bash
curl -X POST http://localhost:7000/compact/_system/default/series_stats
```

### Configuration Tables

**Purpose:** Store system configuration, metadata, lookup tables

**Structure:** Non-partitioned (relatively small, low write rate)

**Compaction:** Occasional compaction during maintenance windows

### System Metrics

**Purpose:** Internal monitoring, compaction stats, query logs

**Structure:** Could be partitioned or non-partitioned depending on volume

**Compaction:** Handled automatically by catalog-level compaction

## Testing

### Test Script

Run the included test script to verify both table types:

```bash
chmod +x test_non_partitioned_compaction.sh
./test_non_partitioned_compaction.sh
```

### Expected Behavior

1. **Non-Partitioned Table:**
   - 8 L0 files → compacted to 2 L1 files
   - Files directly in `./data/_system/default/series_stats/`
   - Result label: `[non-partitioned]`

2. **Partitioned Table:**
   - 6 L0 files → compacted to 1-2 L1 files
   - Files in `./data/benchmark/svc_1/cpu/date=2025-12-24/`
   - Result label: `date=2025-12-24`

3. **Mixed Catalog:**
   - Catalog compaction handles both types transparently
   - No errors or special handling required

## Monitoring

### Log Messages

**Non-Partitioned Table:**
```
INFO Compacting non-partitioned _system.default.series_stats: 8 files (8 L0), 120.00 MB
INFO Minor compaction: 8 L0 files → L1
INFO Created L1 file: L1_1703251234890.parquet (50000 rows from 8 L0 files)
INFO Compaction complete: _system.default.series_stats: 8 -> 2 files, 120.00 MB -> 118.00 MB
```

**Partitioned Table:**
```
INFO Compacting benchmark.svc_1.cpu/date=2025-12-24: 10 files (10 L0), 750.00 MB
INFO Minor compaction: 10 L0 files → L1
INFO Created L1 file: L1_1703251234567.parquet (250000 rows from 10 L0 files)
INFO Compaction complete: benchmark.svc_1.cpu/date=2025-12-24: 10 -> 2 files, 750.00 MB -> 735.00 MB
```

### Distinguishing Features

| Feature | Partitioned | Non-Partitioned |
|---------|------------|-----------------|
| Log prefix | `{catalog}.{schema}.{table}/{partition}` | `non-partitioned {catalog}.{schema}.{table}` |
| Result label | `date=YYYY-MM-DD` | `[non-partitioned]` |
| File location | `table/date=*/` | `table/` |

## Migration Guide

### Converting Non-Partitioned → Partitioned

If you need to add partitioning to an existing table:

1. **Create partition directories:**
   ```bash
   mkdir -p data/catalog/schema/table/date=2025-12-24
   ```

2. **Move existing files:**
   ```bash
   mv data/catalog/schema/table/*.parquet data/catalog/schema/table/date=2025-12-24/
   ```

3. **Trigger compaction:**
   ```bash
   curl -X POST http://localhost:7000/compact/catalog/schema/table
   ```

4. **Verify:**
   - Compactor will now detect it as partitioned
   - Each partition compacted independently

### Keeping Non-Partitioned

For tables that should remain non-partitioned:

1. **Ensure no `date=*` directories exist in table path**
2. **Write files directly to table directory**
3. **Compactor automatically handles as non-partitioned**

## Best Practices

### When to Use Non-Partitioned Tables

✅ **Good Use Cases:**
- System metadata (series stats, configurations)
- Small lookup tables (< 1GB total)
- Slowly-growing reference data
- Tables with uniform access patterns (no time-based filtering)

❌ **Avoid for:**
- High-volume time-series data
- Data with retention policies
- Tables requiring partition pruning for performance

### Configuration Tuning

**Non-Partitioned Tables:**
```toml
[compaction]
min_files = 3              # Lower threshold (less write volume)
major_min_files = 3        # Compact more aggressively
target_file_size_mb = 200  # Smaller target (smaller dataset)
```

**Partitioned Tables:**
```toml
[compaction]
min_files = 5              # Higher threshold (more write volume)
major_min_files = 4
target_file_size_mb = 500  # Larger target (optimize compression)
```

## Troubleshooting

### Table Not Being Compacted

**Symptom:** Non-partitioned table not included in catalog compaction

**Cause:** Table might have been misdetected as partitioned

**Solution:**
```bash
# Check for unexpected subdirectories
ls -la data/catalog/schema/table/

# Remove any date=* directories if present
# Or explicitly compact the table
curl -X POST http://localhost:7000/compact/catalog/schema/table
```

### Performance Issues

**Symptom:** Compaction takes too long for non-partitioned table

**Cause:** Table has grown too large for single-directory compaction

**Solution:**
- Migrate to partitioned structure
- Use date-based partitioning for better scalability
- Implement archival/retention policies

## Future Enhancements

1. **Custom Partition Detection:** Support other partition formats beyond `date=`
2. **Hybrid Tables:** Mix of partitioned and non-partitioned data in same table
3. **Compaction Strategies:** Different strategies for different table types
4. **Metadata Tables:** Special handling for system tables with different compaction rules
