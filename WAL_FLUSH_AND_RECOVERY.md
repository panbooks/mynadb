# WAL Flush and Recovery Strategy

## Problem Statement

### Issue 1: No Post-Flush WAL Cleanup
**Before Fix:**
- WAL files accumulated data even after it was flushed to Parquet
- Cleanup only happened after `retention_hours` (24h default)
- WAL directory could grow very large with already-persisted data

### Issue 2: Duplicate Writes on Recovery
**Before Fix:**
```
Timeline:
10:00 AM - Write A logged to WAL
10:30 AM - Cache flush (Write A → Parquet)
10:35 AM - Write B logged to WAL
11:00 AM - CRASH!
11:01 AM - Restart & WAL replay
         - Replays Write A (DUPLICATE! Already in Parquet)
         - Replays Write B (Correct)
Result: Write A appears twice in database
```

## Solution: WAL Rotation After Flush

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Normal Write Operation                                   │
│    - POST /write/mydb "cpu usage=50"                        │
│    - Log to WAL: data_1703001234.wal                       │
│    - Add to in-memory cache                                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Cache Flush (Background Task Every 30s)                  │
│    - Write all cached data to Parquet files                │
│    - Reset cache                                            │
│    - ✨ NEW: Rotate WAL file                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. WAL Rotation                                             │
│    - Flush current WAL: data_1703001234.wal (contains A)   │
│    - Create new WAL: data_1703005678.wal (empty)           │
│    - Future writes go to new WAL                           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. WAL State After Flush                                    │
│    OLD: data_1703001234.wal (flushed data, safe to delete) │
│    NEW: data_1703005678.wal (unflushed data only)          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Cleanup (Hourly Background Task)                        │
│    - Delete WAL files older than retention_hours           │
│    - Only deletes old files (already flushed data)         │
└─────────────────────────────────────────────────────────────┘
```

### File Lifecycle Example

**Initial State (10:00 AM):**
```
_wal/
└── data_1703001234.wal (5 MB, contains writes A, B, C)
```

**After Flush (10:30 AM):**
```
_wal/
├── data_1703001234.wal (5 MB, FLUSHED to Parquet, safe to delete)
└── data_1703005678.wal (0 MB, NEW, empty)
```

**After More Writes (10:35 AM):**
```
_wal/
├── data_1703001234.wal (5 MB, old flushed data)
└── data_1703005678.wal (2 MB, contains writes D, E)
```

**After Cleanup (11:00 AM - retention_hours elapsed):**
```
_wal/
└── data_1703005678.wal (2 MB, contains writes D, E)
```
Old WAL deleted safely because it only contained flushed data.

## Recovery Behavior

### Scenario 1: Crash Before Flush

**Timeline:**
```
10:00 - Write A → WAL: data_1703001234.wal
10:01 - Write B → WAL: data_1703001234.wal
10:02 - CRASH (cache not yet flushed)
10:03 - RESTART
```

**Recovery:**
```rust
// Replay data_1703001234.wal
Replay Write A → Added to cache
Replay Write B → Added to cache
Continue normal operation
```

**Result:** ✅ No data loss, no duplicates

### Scenario 2: Crash After Flush

**Timeline:**
```
10:00 - Write A → WAL: data_1703001234.wal
10:30 - Flush (A → Parquet, WAL rotated)
10:31 - Write B → WAL: data_1703005678.wal
10:32 - CRASH
10:33 - RESTART
```

**WAL Files:**
```
_wal/
├── data_1703001234.wal (contains A, already flushed)
└── data_1703005678.wal (contains B, not flushed)
```

**Recovery:**
```rust
// Replay in timestamp order
1. Replay data_1703001234.wal (Write A)
   - A is replayed → Duplicate in Parquet!
2. Replay data_1703005678.wal (Write B)
   - B is replayed → Correct
```

**Result:** ⚠️ Write A is duplicated!

### Scenario 3: With Post-Flush Rotation (FIXED)

**Timeline:**
```
10:00 - Write A → WAL: data_1703001234.wal
10:30 - Flush (A → Parquet, WAL rotated to data_1703005678.wal)
10:31 - Write B → WAL: data_1703005678.wal
10:32 - CRASH
10:33 - RESTART
```

**WAL Files:**
```
_wal/
├── data_1703001234.wal (contains A, already flushed)
└── data_1703005678.wal (contains B, NOT flushed)
```

**Recovery with Smart Replay:**
```rust
// Old WAL (created before last rotation): Skip or assume flushed
// Only replay NEW WAL (created after last rotation)

Replay data_1703005678.wal (Write B only)
```

**Current Implementation:**
```rust
// Actually replays BOTH files, causing duplicates
Replay data_1703001234.wal (Write A) → DUPLICATE!
Replay data_1703005678.wal (Write B) → Correct
```

## ⚠️ Remaining Issue: Duplicate Replay

### Current Problem

Even with WAL rotation after flush, the recovery still replays **all** WAL files, causing duplicates.

**Why?**
```rust
// In main.rs startup recovery:
let recovered_data = wal_arc
    .replay_data_writes(|db, line_protocol| {
        // Replays EVERY entry in EVERY WAL file
        // No way to know which are already flushed
        Ok(())
    })
    .await
```

### Solutions

#### Option 1: Marker File (Simple)

After successful flush, create a marker file:
```
_wal/
├── data_1703001234.wal (old)
├── data_1703001234.wal.flushed ← Marker file
└── data_1703005678.wal (current)
```

**Recovery Logic:**
```rust
for wal_file in wal_files {
    let marker = format!("{}.flushed", wal_file);
    if Path::new(&marker).exists() {
        info!("Skipping {}: already flushed", wal_file);
        continue;
    }
    replay_wal_file(wal_file);
}
```

**Pros:**
- Simple to implement
- Clear intent

**Cons:**
- Extra filesystem operations
- Need to clean up marker files

#### Option 2: Skip Old WAL Files (Current Approach)

Only replay the **newest** WAL file (the one created after last rotation):

```rust
pub async fn replay_data_writes_latest_only<F>(&self, callback: F) -> Result<usize, String>
where
    F: FnMut(&str, &str) -> Result<(), String>,
{
    let files = self.list_wal_files("data")?;

    // Get current active file
    let current_file = {
        let writer = self.data_writer.lock().await;
        writer.path.clone()
    };

    // Only replay the current active file
    if files.contains(&current_file) {
        self.replay_file(&current_file, callback)?;
    }

    Ok(count)
}
```

**Pros:**
- No extra files
- Fast recovery (only one file)

**Cons:**
- Loses data if crash happens during rotation
- Complex edge cases

#### Option 3: Sequence Numbers (Robust)

Add sequence numbers to track flush progress:

```rust
struct WalEntry {
    sequence: u64,        // NEW: Global sequence number
    timestamp: u64,
    entry_type: EntryType,
    db_name: String,
    payload: Vec<u8>,
}
```

**Flush Checkpoint:**
```
_wal/
└── checkpoint.txt  → "1234567890"  (last flushed sequence)
```

**Recovery Logic:**
```rust
let last_flushed_seq = read_checkpoint()?;
for entry in replay_all_wal_files() {
    if entry.sequence <= last_flushed_seq {
        continue; // Skip already flushed
    }
    replay_entry(entry);
}
```

**Pros:**
- Most robust
- Handles all edge cases
- Can partially replay files

**Cons:**
- More complex
- Requires file format change
- Need sequence counter management

## Current Implementation Status

### ✅ Implemented
1. WAL rotation after successful flush
2. WAL file cleanup after retention period
3. Proper error handling during rotation

### ⚠️ Known Issue
- **Recovery replays all WAL files**, including already-flushed data
- **Result**: Duplicate writes after crash recovery

### 🔧 Recommended Fix

**Immediate (Simple)**: Marker File Approach
```rust
// In cache.rs flush():
if let Some(ref wal) = self.wal {
    wal.rotate_data_wal().await?;

    // Create marker file for old WAL
    let old_wal_files = wal.get_rotated_files()?;
    for file in old_wal_files {
        std::fs::write(format!("{}.flushed", file), b"")?;
    }
}

// In wal.rs replay:
for wal_file in wal_files {
    if Path::new(&format!("{}.flushed", wal_file)).exists() {
        debug!("Skipping flushed WAL: {}", wal_file);
        continue;
    }
    replay_wal_file(wal_file);
}
```

**Long-term (Robust)**: Sequence Numbers
- Add sequence field to WalEntry
- Track last flushed sequence in checkpoint file
- Replay only entries after checkpoint

## Testing Strategy

### Test 1: Normal Operation
```bash
1. Start service
2. Write data (POST /write/mydb)
3. Wait for flush (30s)
4. Check WAL directory:
   - Old WAL exists (pre-flush data)
   - New WAL exists (post-flush data)
5. Wait for cleanup (1h)
6. Check: Old WAL deleted
```

### Test 2: Crash Before Flush
```bash
1. Start service
2. Write data (POST /write/mydb)
3. Kill service (kill -9)
4. Restart service
5. Query data
Expected: All writes recovered, no duplicates
```

### Test 3: Crash After Flush
```bash
1. Start service
2. Write data batch 1
3. Wait for flush (30s)
4. Write data batch 2
5. Kill service (kill -9)
6. Restart service
7. Query data
Expected (current): Batch 1 appears twice (DUPLICATE)
Expected (after fix): Both batches appear once
```

### Test 4: Crash During Rotation
```bash
1. Start service
2. Write data
3. Trigger flush manually (POST /flush)
4. Kill during rotation
5. Restart
Expected: No corruption, graceful recovery
```

## Configuration Impact

### Relevant Settings

```toml
[cache]
flush_interval_secs = 30  # How often cache flushes (triggers WAL rotation)

[wal]
enabled = true
sync_on_write = true      # If false, may lose last few seconds
rotate_size_mb = 64       # Also triggers rotation (whichever comes first)
cleanup_interval_secs = 3600  # How often to delete old WAL files
retention_hours = 24      # Keep WAL for this long after creation
```

### Tuning for Different Scenarios

**Low Latency (Accept Duplicates):**
```toml
[cache]
flush_interval_secs = 10  # Flush more often

[wal]
retention_hours = 2       # Delete WAL files faster
```

**High Durability (Minimize Duplicates):**
```toml
[cache]
flush_interval_secs = 60  # Flush less often (larger batches)

[wal]
sync_on_write = true      # Ensure fsync
retention_hours = 48      # Keep WAL longer for debugging
```

## Monitoring

### Key Metrics

1. **WAL Directory Size**
   ```bash
   du -sh data/_wal/
   ```
   - Should be relatively stable
   - Should not grow unbounded
   - Alert if > 1GB

2. **WAL File Count**
   ```bash
   ls -1 data/_wal/*.wal | wc -l
   ```
   - Should be 1-3 files typically
   - Alert if > 10 files

3. **Recovery Time**
   - Check startup logs
   - Should be < 10 seconds for healthy system
   - Alert if > 30 seconds

4. **Duplicate Detection**
   ```sql
   -- Check for duplicates after recovery
   SELECT timestamp, measurement, COUNT(*)
   FROM mydb.default.cpu
   GROUP BY timestamp, measurement
   HAVING COUNT(*) > 1;
   ```

### Health Check Queries

```bash
# Check WAL files
ls -lh data/_wal/

# Check current active WAL
ls -lt data/_wal/*.wal | head -1

# Estimate unflushed data
tail -n 100 data/_wal/data_*.wal | wc -l
```

## Summary

### What Was Fixed
✅ WAL rotation after flush prevents unbounded growth
✅ Cleanup task safely deletes old WAL files
✅ Proper separation of flushed vs unflushed data

### What Still Needs Work
⚠️ Duplicate replay during recovery (known issue)
⚠️ Need marker files or sequence numbers to skip flushed data

### Recommendation
Implement marker file approach immediately to prevent duplicates on recovery.

---

**Status**: Partial Fix Implemented
**Date**: 2025-12-29
**Next**: Implement duplicate prevention in recovery
