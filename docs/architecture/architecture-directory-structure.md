# Measurement Directory Structure

## Overview

The storage structure has been reorganized to use **measurement directories** instead of flat files. This provides better organization and sets the foundation for partitioning support.

## Old Structure (Before)

```
./data/
└── mydb/
    ├── config.json
    ├── cpu_1766469521005.parquet
    ├── cpu_1766469551004.parquet
    └── memory_1766470391009.parquet
```

**Problems:**
- ❌ All files in one directory
- ❌ Hard to manage many measurements
- ❌ Difficult to implement partitioning
- ❌ Cluttered namespace

## New Structure (After) ✨

```
./data/
└── mydb/
    ├── config.json
    ├── cpu/
    │   ├── 1766469521005.parquet
    │   └── 1766469551004.parquet
    └── memory/
        └── 1766470391009.parquet
```

**Benefits:**
- ✅ Clean organization by measurement
- ✅ Easy to find all data for a measurement
- ✅ Ready for partitioning (future)
- ✅ Better file management
- ✅ Cleaner filenames (no measurement prefix)

## Directory Hierarchy

```
data/
├── database1/
│   ├── config.json              # Database configuration
│   ├── measurement1/            # Measurement directory
│   │   ├── timestamp1.parquet   # Data files
│   │   ├── timestamp2.parquet
│   │   └── timestamp3.parquet
│   ├── measurement2/
│   │   └── timestamp.parquet
│   └── measurement3/
│       ├── timestamp1.parquet
│       └── timestamp2.parquet
└── database2/
    ├── config.json
    └── measurement1/
        └── timestamp.parquet
```

## File Naming Convention

### Old Format:
```
{measurement}_{timestamp}.parquet
Example: cpu_1766469521005.parquet
```

### New Format:
```
{timestamp}.parquet
Example: 1766469521005.parquet
```

**Simpler!** The measurement name is already in the directory path.

## Example with Real Data

### After Writing Data:

```bash
# Write CPU data
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'

# Write memory data
curl -X POST http://localhost:7000/write/mydb \
  -d 'memory,host=server01 used=16.5'

# Write more CPU data
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server02 usage=45.2'
```

### Resulting Structure:

```
./data/mydb/
├── config.json
├── cpu/
│   ├── 1766469521005.parquet  (server01 data)
│   └── 1766469551004.parquet  (server02 data)
└── memory/
    └── 1766470391009.parquet  (server01 data)
```

## Query Engine Changes

### Auto-Registration

The query engine now:
1. Scans the database directory
2. Finds all subdirectories (measurements)
3. Registers each measurement directory as a table
4. DataFusion automatically reads all Parquet files in each directory

### Table Names

Table naming remains the same:
```
{database}_{measurement}
```

**Examples:**
- `mydb_cpu` → data from `./data/mydb/cpu/`
- `mydb_memory` → data from `./data/mydb/memory/`
- `mydb_disk` → data from `./data/mydb/disk/`

## Code Changes

### ParquetWriter

**Before:**
```rust
let filename = format!(
    "{}/{}_{}.parquet",
    db_path,
    measurement,
    timestamp
);
```

**After:**
```rust
let measurement_path = format!("{}/{}/{}", data_dir, db_name, measurement);
create_dir_all(&measurement_path)?;

let filename = format!(
    "{}/{}.parquet",
    measurement_path,
    timestamp
);
```

### QueryEngine

**Before:**
```rust
// Scan for *.parquet files
// Extract measurement from filename: cpu_timestamp.parquet
```

**After:**
```rust
// Scan for subdirectories
// Each directory = one measurement
// Register entire directory as table
```

## Benefits for Future Features

### 1. Partitioning (Coming Soon!)

Easy to add partitioning within measurement directories:

```
./data/mydb/cpu/
├── region=us-west/
│   ├── host=server01/
│   │   └── 1766469521005.parquet
│   └── host=server02/
│       └── 1766469551004.parquet
└── region=us-east/
    └── host=server03/
        └── 1766469581008.parquet
```

### 2. Retention Policies

Easy to apply retention per measurement:

```bash
# Delete old CPU data
rm -rf ./data/mydb/cpu/$(date -d '30 days ago' +%s)*.parquet

# Or delete entire measurement
rm -rf ./data/mydb/old_measurement/
```

### 3. Compaction

Easy to compact files per measurement:

```bash
# Compact all CPU files
compact ./data/mydb/cpu/*.parquet -> cpu/compacted.parquet
```

### 4. Backup

Easy to backup specific measurements:

```bash
# Backup only CPU data
tar -czf cpu_backup.tar.gz ./data/mydb/cpu/

# Backup specific database
tar -czf mydb_backup.tar.gz ./data/mydb/
```

## Migration from Old Structure

If you have existing data in the old format, here's how to migrate:

### Migration Script

```bash
#!/bin/bash

# migrate_structure.sh
DB_PATH="./data/mydb"

# Find all old-format parquet files
for file in $DB_PATH/*.parquet; do
    if [ -f "$file" ]; then
        # Extract measurement and timestamp from filename
        # Format: measurement_timestamp.parquet
        filename=$(basename "$file")
        measurement=$(echo "$filename" | sed 's/_[0-9]*\.parquet$//')
        timestamp=$(echo "$filename" | sed "s/^${measurement}_//" | sed 's/\.parquet$//')
        
        # Create measurement directory
        mkdir -p "$DB_PATH/$measurement"
        
        # Move file with new name
        mv "$file" "$DB_PATH/$measurement/$timestamp.parquet"
        
        echo "Migrated: $file -> $DB_PATH/$measurement/$timestamp.parquet"
    fi
done

echo "Migration complete!"
```

### Run Migration:

```bash
chmod +x migrate_structure.sh
./migrate_structure.sh
```

## Compatibility

### Backward Compatibility

⚠️ **Breaking Change:** Old file structure is no longer supported.

If you have existing data:
1. Use the migration script above
2. Or start fresh with new structure

### Query Compatibility

✅ **No changes to queries!** Table names remain the same:

```sql
-- Still works exactly the same
SELECT * FROM mydb_cpu WHERE host = 'server01';
```

## Verification

### Check Your Structure:

```bash
# List database structure
tree ./data/mydb/

# Expected output:
# ./data/mydb/
# ├── config.json
# ├── cpu/
# │   ├── 1766469521005.parquet
# │   └── 1766469551004.parquet
# └── memory/
#     └── 1766470391009.parquet
```

### Verify Data:

```bash
# Flush any cached data
curl -X POST http://localhost:7000/flush

# List tables
curl http://localhost:7000/databases/mydb/tables

# Query data
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu'
```

## Performance Impact

### File Operations

✅ **Slightly faster writes** - Less path length  
✅ **Faster directory scans** - Fewer files per directory  
✅ **Better OS caching** - Related files grouped together  

### Query Performance

✅ **Same performance** - DataFusion reads directories efficiently  
✅ **Better partition pruning** (future) - When partitioning is added  

## Summary

The new measurement directory structure provides:

1. **Better Organization** - Logical grouping by measurement
2. **Cleaner Filenames** - No measurement prefix needed
3. **Future Ready** - Foundation for partitioning
4. **Easier Management** - Clear separation of measurements
5. **Same Query API** - No changes to how you query data

**Migration required** for existing data, but worth it for the improved structure! 🎉
