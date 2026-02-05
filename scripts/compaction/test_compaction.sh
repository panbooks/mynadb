#!/bin/bash

# Test script for Leveled Parquet file compaction feature

BASE_URL="http://localhost:7000"
DB="testcompact"

echo "=================================="
echo "Leveled Compaction Feature Test"
echo "=================================="

# Step 1: Create database
echo -e "\n[1] Creating database '$DB'..."
curl -X POST $BASE_URL/databases -H 'Content-Type: application/json' -d "{\"name\":\"$DB\"}"
echo ""

# Step 2: Write multiple batches to create many L0 files
# NOTE: Each flush creates a separate L0 file. We need at least 5 files for compaction.
echo -e "\n[2] Writing multiple small batches to create many L0 files..."
echo "    (Creating 10 separate L0 files by flushing after each batch)"

for i in {1..10}; do
    echo "  Writing batch $i..."
    # Write a few rows per batch
    curl -s -X POST $BASE_URL/write/$DB -d "cpu,host=server0$i usage=$(echo "scale=2; $i * 10.5" | bc),temp=$((i*5))i
cpu,host=server0$i usage=$(echo "scale=2; $i * 11.5" | bc),temp=$((i*5+1))i
cpu,host=server0$i usage=$(echo "scale=2; $i * 12.5" | bc),temp=$((i*5+2))i"
    
    # Force flush after each batch to create separate L0 files
    curl -s -X POST $BASE_URL/flush > /dev/null
    sleep 0.1
done

echo -e "\n  Created 10 L0 files (minimum 5 required for compaction)"

# Step 3: Check files before compaction
echo -e "\n[4] Checking Parquet files before compaction..."
echo "Files in ./data/$DB/cpu/:"
ls -lh ./data/$DB/cpu/ 2>/dev/null || echo "Directory not found"
L0_COUNT_BEFORE=$(ls -1 ./data/$DB/cpu/L0_*.parquet 2>/dev/null | wc -l)
TOTAL_COUNT_BEFORE=$(ls -1 ./data/$DB/cpu/*.parquet 2>/dev/null | wc -l)
echo "L0 files: $L0_COUNT_BEFORE"
echo "Total files: $TOTAL_COUNT_BEFORE"

# Step 4: Run compaction (L0 files will be compacted to L1)
echo -e "\n[5] Running compaction on database '$DB' (L0 → L1)..."
COMPACT_RESULT=$(curl -s -X POST $BASE_URL/compact/$DB)
echo "$COMPACT_RESULT"

# Wait for compaction to complete (file operations are asynchronous)
echo -e "\nWaiting 2 seconds for compaction to complete..."
sleep 2

# Step 5: Check files after first compaction
echo -e "\n[6] Checking Parquet files after first compaction..."
echo "Files in ./data/$DB/cpu/:"
ls -lh ./data/$DB/cpu/ 2>/dev/null || echo "Directory not found"
L0_COUNT_AFTER=$(ls -1 ./data/$DB/cpu/L0_*.parquet 2>/dev/null | wc -l)
L1_COUNT_AFTER=$(ls -1 ./data/$DB/cpu/L1_*.parquet 2>/dev/null | wc -l)
TOTAL_COUNT_AFTER=$(ls -1 ./data/$DB/cpu/*.parquet 2>/dev/null | wc -l)
echo "L0 files: $L0_COUNT_AFTER"
echo "L1 files: $L1_COUNT_AFTER"
echo "Total files: $TOTAL_COUNT_AFTER"

# Step 6: Verify data integrity with query
echo -e "\n[7] Verifying data integrity with SQL query..."
curl -s "$BASE_URL/query/$DB?sql=SELECT COUNT(*) as row_count FROM ${DB}_cpu"
echo ""
curl -s "$BASE_URL/query/$DB?sql=SELECT * FROM ${DB}_cpu ORDER BY time LIMIT 5"
echo ""

# Step 7: Write more data and compact again
echo -e "\n[8] Writing more data to test multi-level compaction..."
echo "    (Creating 10 more L0 files)"

for i in {11..20}; do
    echo "  Writing batch $i..."
    curl -s -X POST $BASE_URL/write/$DB -d "cpu,host=server$i usage=$(echo "scale=2; $i * 10.5" | bc),temp=$((i*5))i
cpu,host=server$i usage=$(echo "scale=2; $i * 11.5" | bc),temp=$((i*5+1))i
cpu,host=server$i usage=$(echo "scale=2; $i * 12.5" | bc),temp=$((i*5+2))i"
    
    # Force flush after each batch
    curl -s -X POST $BASE_URL/flush > /dev/null
    sleep 0.1
done

echo -e "\nRunning second compaction (L0 → L1, and possibly L1 → L2)..."
COMPACT_RESULT2=$(curl -s -X POST $BASE_URL/compact/$DB)
echo "$COMPACT_RESULT2"

# Wait for compaction
echo -e "\nWaiting 2 seconds for compaction to complete..."
sleep 2

# Step 8: Check final file distribution
echo -e "\n[9] Final file distribution..."
echo "Files in ./data/$DB/cpu/:"
ls -lh ./data/$DB/cpu/ 2>/dev/null || echo "Directory not found"
L0_FINAL=$(ls -1 ./data/$DB/cpu/L0_*.parquet 2>/dev/null | wc -l)
L1_FINAL=$(ls -1 ./data/$DB/cpu/L1_*.parquet 2>/dev/null | wc -l)
L2_FINAL=$(ls -1 ./data/$DB/cpu/L2_*.parquet 2>/dev/null | wc -l)
TOTAL_FINAL=$(ls -1 ./data/$DB/cpu/*.parquet 2>/dev/null | wc -l)
echo "L0 files: $L0_FINAL"
echo "L1 files: $L1_FINAL"
echo "L2 files: $L2_FINAL"
echo "Total files: $TOTAL_FINAL"

# Step 9: Test specific measurement compaction
echo -e "\n[10] Testing specific measurement compaction..."
echo "    (Creating 8 separate L0 files for memory measurement)"

# Write memory data - create separate L0 files
for i in {1..8}; do
    echo "  Writing memory batch $i..."
    # Write multiple rows per batch
    curl -s -X POST $BASE_URL/write/$DB -d "memory,host=server0$i used=$(echo "scale=2; $i * 2.5" | bc),total=$((i*16))i
memory,host=server0$i used=$(echo "scale=2; $i * 3.5" | bc),total=$((i*16))i
memory,host=server0$i used=$(echo "scale=2; $i * 4.5" | bc),total=$((i*16))i"
    
    # Force flush after each batch to create separate L0 files
    curl -s -X POST $BASE_URL/flush > /dev/null
    sleep 0.1
done

echo -e "\n  Created 8 L0 files for memory measurement"
echo -e "\nChecking memory files before compaction..."
ls -lh ./data/$DB/memory/ 2>/dev/null || echo "Directory not found"
MEMORY_L0_BEFORE=$(ls -1 ./data/$DB/memory/L0_*.parquet 2>/dev/null | wc -l)
echo "Memory L0 files before compaction: $MEMORY_L0_BEFORE"

echo -e "\nCompacting specific measurement 'memory'..."
MEMORY_COMPACT=$(curl -s -X POST $BASE_URL/compact/$DB/memory)
echo "$MEMORY_COMPACT"

# Wait for compaction
echo -e "\nWaiting 2 seconds for compaction to complete..."
sleep 2

echo -e "\nChecking memory files after compaction..."
ls -lh ./data/$DB/memory/ 2>/dev/null || echo "Directory not found"

# Step 10: Summary
echo -e "\n=================================="
echo "Leveled Compaction Test Summary"
echo "=================================="
echo "Database: $DB"
echo ""
echo "CPU measurement:"
echo "  Before: L0=$L0_COUNT_BEFORE, Total=$TOTAL_COUNT_BEFORE"
echo "  After:  L0=$L0_FINAL, L1=$L1_FINAL, L2=$L2_FINAL, Total=$TOTAL_FINAL"
if [ $TOTAL_COUNT_BEFORE -gt 0 ]; then
    echo "  Reduction: $(($TOTAL_COUNT_BEFORE - $TOTAL_FINAL)) files ($(awk "BEGIN {printf \"%.1f\", (1 - $TOTAL_FINAL/$TOTAL_COUNT_BEFORE) * 100}")% reduction)"
fi
echo ""
echo "Memory measurement:"
MEMORY_L0=$(ls -1 ./data/$DB/memory/L0_*.parquet 2>/dev/null | wc -l)
MEMORY_L1=$(ls -1 ./data/$DB/memory/L1_*.parquet 2>/dev/null | wc -l)
MEMORY_TOTAL=$(ls -1 ./data/$DB/memory/*.parquet 2>/dev/null | wc -l)
echo "  Before: L0=$MEMORY_L0_BEFORE"
echo "  After:  L0=$MEMORY_L0, L1=$MEMORY_L1, Total=$MEMORY_TOTAL"
if [ $MEMORY_L0_BEFORE -gt 0 ]; then
    echo "  Reduction: $(($MEMORY_L0_BEFORE - $MEMORY_TOTAL)) files ($(awk "BEGIN {printf \"%.1f\", (1 - $MEMORY_TOTAL/$MEMORY_L0_BEFORE) * 100}")% reduction)"
fi
echo ""
echo "Compaction Config:"
echo "  Minimum files for compaction: 5"
echo "  Maximum file size: 100MB"
echo "  Target file size: 500MB"
echo "  Maximum level: 5"
echo "=================================="
echo ""
echo "✅ Test completed! Compaction should have occurred for both measurements."
echo "   Each measurement started with 8-10 L0 files (>5 minimum threshold)"
echo "   and should now have fewer, larger files in L1 level."
