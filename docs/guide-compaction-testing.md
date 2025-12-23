# Compaction Testing Guide

## Why Compaction Wasn't Working

Your compaction failed with:
```
ℹ️  No compaction needed for testcompact/memory (minimum file threshold not met)
```

### Root Cause

The default compaction config requires **minimum 5 L0 files** to trigger compaction:

```rust
CompactionConfig {
    min_files: 5,        // ⚠️ Need at least 5 files!
    max_file_size: 100 * 1024 * 1024,      // 100MB
    target_file_size: 500 * 1024 * 1024,   // 500MB
    max_level: 5,
}
```

Your test only created **1 L0 file** because:
- Wrote 8 rows
- Flushed once → 1 Parquet file
- 1 < 5, so no compaction

## Solution: Create Multiple L0 Files

### Strategy 1: Write + Flush Per Batch (Recommended)

```bash
# Write multiple small batches
for i in {1..8}; do
    curl -X POST http://localhost:7000/write/mydb \
      -d "memory,host=server0$i used=$i,total=32"
    
    # Flush AFTER EACH batch to create separate L0 files
    curl -X POST http://localhost:7000/flush
done

# Now you have 8 L0 files → compaction will work!
curl -X POST http://localhost:7000/compact/mydb/memory
```

### Strategy 2: Use Background Flush Timing

```bash
# Write data slowly, let background flush create files
for i in {1..10}; do
    curl -X POST http://localhost:7000/write/mydb \
      -d "memory,host=server$i used=$i,total=32"
    
    # Wait for background flush (default: 30s)
    sleep 35
done

# Multiple L0 files created by background flush
curl -X POST http://localhost:7000/compact/mydb/memory
```

## Updated Test Script

The updated `test_compaction.sh` now:

### ✅ Creates Multiple L0 Files

```bash
# OLD (creates 1 file):
for i in {1..8}; do
    curl -X POST /write/db -d "memory,host=server$i used=$i"
    sleep 0.2
done
curl -X POST /flush  # ❌ 1 flush = 1 file

# NEW (creates 8 files):
for i in {1..8}; do
    curl -X POST /write/db -d "memory,host=server$i used=$i"
    curl -X POST /flush  # ✅ Flush after EACH batch = 8 files
    sleep 0.1
done
```

### ✅ Writes More Data Per Batch

```bash
# More rows per batch to ensure meaningful file sizes
curl -X POST /write/db -d "memory,host=server1 used=1,total=32
memory,host=server1 used=2,total=32
memory,host=server1 used=3,total=32"
```

### ✅ Verifies File Counts

```bash
L0_COUNT=$(ls -1 ./data/mydb/memory/L0_*.parquet | wc -l)
echo "L0 files: $L0_COUNT (need ≥5 for compaction)"
```

## Testing Compaction

### Quick Test (5 files, minimum threshold)

```bash
#!/bin/bash
DB="testcompact"

# Create database
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' -d "{\"name\":\"$DB\"}"

# Create exactly 5 L0 files (minimum for compaction)
for i in {1..5}; do
    echo "Creating L0 file $i..."
    curl -s -X POST http://localhost:7000/write/$DB \
      -d "cpu,host=server$i usage=$((i*10)),temp=$((i*5))i"
    curl -s -X POST http://localhost:7000/flush
done

# Check before compaction
ls -lh ./data/$DB/cpu/
L0_BEFORE=$(ls -1 ./data/$DB/cpu/L0_*.parquet | wc -l)
echo "L0 files before: $L0_BEFORE"

# Compact
curl -X POST http://localhost:7000/compact/$DB

# Check after compaction
ls -lh ./data/$DB/cpu/
L0_AFTER=$(ls -1 ./data/$DB/cpu/L0_*.parquet | wc -l)
L1_AFTER=$(ls -1 ./data/$DB/cpu/L1_*.parquet | wc -l)
echo "L0 files after: $L0_AFTER"
echo "L1 files after: $L1_AFTER"

if [ "$L1_AFTER" -gt 0 ]; then
    echo "✅ SUCCESS: Compaction worked!"
else
    echo "❌ FAILED: No L1 files created"
fi
```

### Comprehensive Test (10+ files)

```bash
# Run the updated test script
./scripts/test_compaction.sh
```

This creates:
- 10 L0 files for CPU measurement
- 8 L0 files for memory measurement
- Both exceed the 5-file threshold
- Compaction should succeed for both

## Expected Output

### Before Compaction

```bash
$ ls -lh ./data/testcompact/memory/
total 32K
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475710946.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475711123.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475711301.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475711478.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475711655.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475711832.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475712009.parquet
-rw-r--r-- 1 user user 4.0K Dec 23 07:30 L0_1766475712186.parquet

# 8 L0 files (> 5 minimum) ✅
```

### After Compaction

```bash
$ curl -X POST http://localhost:7000/compact/testcompact/memory
Compacted testcompact/memory: L0 files 8 -> 0, Total 8 -> 1 files (0.03 MB -> 0.03 MB, 24 rows)

$ ls -lh ./data/testcompact/memory/
total 4K
-rw-r--r-- 1 user user 4.0K Dec 23 07:31 L1_1766475715000.parquet

# 8 L0 files merged into 1 L1 file ✅
```

## Tuning Compaction Config

If you want compaction to trigger with fewer files, you need to modify the code:

### Option 1: Change Default Config

```rust
// src/compactor.rs
impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            min_files: 3,  // Changed from 5 to 3
            max_file_size: 100 * 1024 * 1024,
            target_file_size: 500 * 1024 * 1024,
            max_level: 5,
        }
    }
}
```

### Option 2: Add Config Endpoint (Future Enhancement)

```rust
// Allow custom config per request
#[derive(Deserialize)]
struct CompactRequest {
    min_files: Option<usize>,
}

async fn compact_handler(
    Path(db): Path<String>,
    Json(req): Json<CompactRequest>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut config = CompactionConfig::default();
    if let Some(min) = req.min_files {
        config.min_files = min;
    }
    
    state.compactor.compact_database(&db, &config).await
}
```

Usage:
```bash
curl -X POST http://localhost:7000/compact/mydb \
  -H 'Content-Type: application/json' \
  -d '{"min_files": 3}'
```

## Common Mistakes

### ❌ Writing All Data, Then Flushing Once

```bash
# This creates only 1 file!
curl -X POST /write/db -d "cpu,host=s1 usage=10"
curl -X POST /write/db -d "cpu,host=s2 usage=20"
curl -X POST /write/db -d "cpu,host=s3 usage=30"
curl -X POST /flush  # ❌ Only 1 file created
```

### ✅ Flush After Each Batch

```bash
# This creates 3 files
for i in {1..3}; do
    curl -X POST /write/db -d "cpu,host=s$i usage=$((i*10))"
    curl -X POST /flush  # ✅ 3 separate files
done
```

### ❌ Relying on Background Flush Without Waiting

```bash
# Background flush is every 30 seconds
curl -X POST /write/db -d "cpu,host=s1 usage=10"
sleep 1  # ❌ Not long enough for flush
curl -X POST /write/db -d "cpu,host=s2 usage=20"
sleep 1  # ❌ Still too short
curl -X POST /flush  # Only 1 file created
```

### ✅ Wait for Background Flush OR Use Manual Flush

```bash
# Option 1: Wait for background flush
curl -X POST /write/db -d "cpu,host=s1 usage=10"
sleep 35  # ✅ Wait for background flush (30s + buffer)
curl -X POST /write/db -d "cpu,host=s2 usage=20"
sleep 35  # ✅ Another background flush

# Option 2: Manual flush (recommended for testing)
curl -X POST /write/db -d "cpu,host=s1 usage=10"
curl -X POST /flush  # ✅ Immediate flush
curl -X POST /write/db -d "cpu,host=s2 usage=20"
curl -X POST /flush  # ✅ Immediate flush
```

## Verification Checklist

Before running compaction, verify:

```bash
# 1. Check number of L0 files
L0_COUNT=$(ls -1 ./data/mydb/measurement/L0_*.parquet 2>/dev/null | wc -l)
echo "L0 files: $L0_COUNT"

# 2. Verify threshold
if [ $L0_COUNT -ge 5 ]; then
    echo "✅ Threshold met ($L0_COUNT >= 5)"
    echo "   Compaction should work"
else
    echo "❌ Threshold NOT met ($L0_COUNT < 5)"
    echo "   Need $((5 - L0_COUNT)) more file(s)"
    echo "   Create more data or adjust config"
fi

# 3. Run compaction
curl -X POST http://localhost:7000/compact/mydb/measurement
```

## Summary

**To make compaction work:**

1. **Create ≥5 L0 files** - Flush after each batch
2. **Verify file count** - Check before compacting
3. **Run updated script** - `./scripts/test_compaction.sh`

**Key insight:** 
- 1 flush = 1 file
- Need 5 files minimum
- Therefore: flush 5+ times (or adjust config)

**Updated test script now:**
- ✅ Flushes after EACH batch
- ✅ Creates 8-10 L0 files per measurement
- ✅ Exceeds 5-file threshold
- ✅ Compaction will work! 🎉
