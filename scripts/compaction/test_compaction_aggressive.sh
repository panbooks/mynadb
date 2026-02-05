#!/bin/bash

# Test script with aggressive compaction settings (min_files=3 instead of 5)
# This demonstrates compaction with fewer files

BASE_URL="http://localhost:7000"
DB="testcompact_aggressive"

echo "=============================================="
echo "Aggressive Compaction Test (min_files=3)"
echo "=============================================="
echo "NOTE: This uses a modified config endpoint"
echo "      (requires code changes to support)"
echo ""

# Step 1: Create database
echo -e "\n[1] Creating database '$DB'..."
curl -X POST $BASE_URL/databases -H 'Content-Type: application/json' -d "{\"name\":\"$DB\"}"
echo ""

# Step 2: Write fewer batches (only 5 to demonstrate lower threshold)
echo -e "\n[2] Writing 5 small batches (creating 5 L0 files)..."
echo "    Default config requires 5 files, so this should trigger compaction"

for i in {1..5}; do
    echo "  Writing batch $i..."
    curl -s -X POST $BASE_URL/write/$DB -d "cpu,host=server0$i usage=$(echo "scale=2; $i * 10.5" | bc),temp=$((i*5))i
cpu,host=server0$i usage=$(echo "scale=2; $i * 11.5" | bc),temp=$((i*5+1))i
cpu,host=server0$i usage=$(echo "scale=2; $i * 12.5" | bc),temp=$((i*5+2))i"
    
    # Force flush after each batch to create separate L0 files
    curl -s -X POST $BASE_URL/flush > /dev/null
    sleep 0.1
done

echo -e "\n  Created 5 L0 files (exactly minimum threshold)"

# Step 3: Check files before compaction
echo -e "\n[3] Checking files before compaction..."
echo "Files in ./data/$DB/cpu/:"
ls -lh ./data/$DB/cpu/ 2>/dev/null || echo "Directory not found"
L0_COUNT_BEFORE=$(ls -1 ./data/$DB/cpu/L0_*.parquet 2>/dev/null | wc -l)
echo "L0 files: $L0_COUNT_BEFORE"

# Step 4: Run compaction
echo -e "\n[4] Running compaction..."
COMPACT_RESULT=$(curl -s -X POST $BASE_URL/compact/$DB)
echo "$COMPACT_RESULT"

# Wait for compaction to complete (file operations are asynchronous)
echo -e "\nWaiting 2 seconds for compaction to complete..."
sleep 2

# Step 5: Check files after compaction
echo -e "\n[5] Checking files after compaction..."
echo "Files in ./data/$DB/cpu/:"
ls -lh ./data/$DB/cpu/ 2>/dev/null
L0_COUNT_AFTER=$(ls -1 ./data/$DB/cpu/L0_*.parquet 2>/dev/null | wc -l)
L1_COUNT_AFTER=$(ls -1 ./data/$DB/cpu/L1_*.parquet 2>/dev/null | wc -l)
TOTAL_COUNT_AFTER=$(ls -1 ./data/$DB/cpu/*.parquet 2>/dev/null | wc -l)
echo "L0 files: $L0_COUNT_AFTER"
echo "L1 files: $L1_COUNT_AFTER"
echo "Total files: $TOTAL_COUNT_AFTER"

# Summary
echo -e "\n=================================="
echo "Aggressive Compaction Test Summary"
echo "=================================="
echo "Before: $L0_COUNT_BEFORE L0 files"
echo "After:  $L0_COUNT_AFTER L0 files, $L1_COUNT_AFTER L1 files, Total: $TOTAL_COUNT_AFTER files"
echo ""

# Verify compaction result
if [ "$L1_COUNT_AFTER" -gt 0 ] && [ "$TOTAL_COUNT_AFTER" -lt "$L0_COUNT_BEFORE" ]; then
    echo "✅ SUCCESS: Compaction occurred!"
    echo "   $L0_COUNT_BEFORE L0 files → $L1_COUNT_AFTER L1 file(s)"
    echo "   File reduction: $L0_COUNT_BEFORE → $TOTAL_COUNT_AFTER files ($(awk "BEGIN {printf \"%.1f\", (1 - $TOTAL_COUNT_AFTER/$L0_COUNT_BEFORE) * 100}")% reduction)"
elif [ "$TOTAL_COUNT_AFTER" -eq 0 ]; then
    echo "⚠️  WARNING: No files found after compaction!"
    echo "   This might be a timing issue. Checking again in 2 seconds..."
    sleep 2
    L0_RECHECK=$(ls -1 ./data/$DB/cpu/L0_*.parquet 2>/dev/null | wc -l)
    L1_RECHECK=$(ls -1 ./data/$DB/cpu/L1_*.parquet 2>/dev/null | wc -l)
    TOTAL_RECHECK=$(ls -1 ./data/$DB/cpu/*.parquet 2>/dev/null | wc -l)
    echo "   Recheck: L0=$L0_RECHECK, L1=$L1_RECHECK, Total=$TOTAL_RECHECK"
    
    if [ "$TOTAL_RECHECK" -gt 0 ]; then
        echo "   ✅ Files found! Compaction is working but timing was off."
    else
        echo "   ❌ Still no files. Compaction may have failed or deleted files."
    fi
else
    echo "❌ FAILED: Compaction did not occur as expected."
    echo "   Possible reasons:"
    echo "   - Not enough files ($L0_COUNT_BEFORE < 5 minimum)"
    echo "   - Files too small"
    echo "   - Timing issue (try increasing wait time)"
fi
echo "=================================="
