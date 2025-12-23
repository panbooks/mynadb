# Compaction Quick Fix

## Problem

```
ℹ️  No compaction needed for testcompact/memory (minimum file threshold not met)
```

## Root Cause

- **Minimum files required:** 5
- **Files you created:** 1
- **Result:** Compaction skipped

## Solution: Create 5+ Files

### Quick Test (Copy & Paste)

```bash
DB="testcompact"

# Create database
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' -d "{\"name\":\"$DB\"}"

# Create 8 L0 files (exceeds 5 minimum)
for i in {1..8}; do
    echo "Creating file $i/8..."
    curl -s -X POST http://localhost:7000/write/$DB \
      -d "memory,host=server$i used=$((i*2)),total=32
memory,host=server$i used=$((i*3)),total=32
memory,host=server$i used=$((i*4)),total=32"
    
    # Flush AFTER EACH batch to create separate files
    curl -s -X POST http://localhost:7000/flush
done

# Verify we have enough files
echo ""
echo "Checking L0 files..."
ls -lh ./data/$DB/memory/
L0_COUNT=$(ls -1 ./data/$DB/memory/L0_*.parquet 2>/dev/null | wc -l)
echo ""
echo "L0 files created: $L0_COUNT (need ≥5)"

# Compact
if [ $L0_COUNT -ge 5 ]; then
    echo ""
    echo "✅ Running compaction..."
    curl -X POST http://localhost:7000/compact/$DB/memory
    echo ""
    echo ""
    echo "After compaction:"
    ls -lh ./data/$DB/memory/
else
    echo ""
    echo "❌ Not enough files for compaction"
fi
```

## Or Use Updated Test Script

```bash
# Run the updated test script
cd /root/projects/stats-service
./scripts/test_compaction.sh
```

This script now:
- Creates 10 L0 files for CPU
- Creates 8 L0 files for memory
- Both exceed 5-file threshold
- **Compaction will work!** ✅

## Key Insight

```
1 flush = 1 file
Need 5 files minimum
Therefore: flush at least 5 times
```

### Before (Wrong ❌)

```bash
# Write 8 batches
for i in {1..8}; do
    curl -X POST /write/db -d "data"
done

# Flush once
curl -X POST /flush

# Result: 1 file only → No compaction
```

### After (Correct ✅)

```bash
# Write + flush each batch
for i in {1..8}; do
    curl -X POST /write/db -d "data"
    curl -X POST /flush  # Flush EACH batch
done

# Result: 8 files → Compaction works! 🎉
```

## Expected Output

When it works, you'll see:

```bash
Compacted testcompact/memory: L0 files 8 -> 0, Total 8 -> 1 files (0.03 MB -> 0.03 MB, 24 rows)
```

Files before:
```
L0_1766475710946.parquet
L0_1766475711123.parquet
L0_1766475711301.parquet
L0_1766475711478.parquet
L0_1766475711655.parquet
L0_1766475711832.parquet
L0_1766475712009.parquet
L0_1766475712186.parquet
```

Files after:
```
L1_1766475715000.parquet  (all 8 files merged into 1)
```

## Done! 🚀

That's it! The updated test script will now demonstrate working compaction.
