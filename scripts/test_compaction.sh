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
echo -e "\n[2] Writing multiple small batches to create many L0 files..."

for i in {1..10}; do
    echo "  Writing batch $i..."
    curl -s -X POST $BASE_URL/write/$DB -d "cpu,host=server0$i usage=$(echo "scale=2; $i * 10.5" | bc),temp=$((i*5))i"
    sleep 0.2
done

echo -e "\nWaiting 2 seconds for cache flush..."
sleep 2

# Force flush to ensure all data is on disk
echo -e "\n[3] Forcing cache flush (creates L0 files)..."
curl -X POST $BASE_URL/flush
echo ""

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
curl -s "$BASE_URL/query/$DB?sql=SELECT * FROM ${DB}_cpu ORDER BY timestamp LIMIT 5"
echo ""

# Step 7: Write more data and compact again
echo -e "\n[8] Writing more data to test multi-level compaction..."
for i in {11..20}; do
    echo "  Writing batch $i..."
    curl -s -X POST $BASE_URL/write/$DB -d "cpu,host=server$i usage=$(echo "scale=2; $i * 10.5" | bc),temp=$((i*5))i"
    sleep 0.2
done

echo -e "\nForcing flush..."
curl -s -X POST $BASE_URL/flush
echo ""

echo -e "\nRunning second compaction..."
COMPACT_RESULT2=$(curl -s -X POST $BASE_URL/compact/$DB)
echo "$COMPACT_RESULT2"

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
# Write memory data
for i in {1..8}; do
    echo "  Writing memory batch $i..."
    curl -s -X POST $BASE_URL/write/$DB -d "memory,host=server0$i used=$(echo "scale=2; $i * 2.5" | bc),total=$((i*16))i"
    sleep 0.2
done

echo -e "\nForcing flush..."
curl -s -X POST $BASE_URL/flush
echo ""

echo -e "\nCompacting specific measurement 'memory'..."
curl -s -X POST $BASE_URL/compact/$DB/memory
echo ""

# Step 10: Summary
echo -e "\n=================================="
echo "Leveled Compaction Test Summary"
echo "=================================="
echo "Database: $DB"
echo ""
echo "CPU measurement:"
echo "  Before: L0=$L0_COUNT_BEFORE, Total=$TOTAL_COUNT_BEFORE"
echo "  After:  L0=$L0_FINAL, L1=$L1_FINAL, L2=$L2_FINAL, Total=$TOTAL_FINAL"
echo "  Reduction: $(($TOTAL_COUNT_BEFORE - $TOTAL_FINAL)) files"
echo ""
echo "Memory measurement:"
MEMORY_L0=$(ls -1 ./data/$DB/memory/L0_*.parquet 2>/dev/null | wc -l)
MEMORY_L1=$(ls -1 ./data/$DB/memory/L1_*.parquet 2>/dev/null | wc -l)
MEMORY_TOTAL=$(ls -1 ./data/$DB/memory/*.parquet 2>/dev/null | wc -l)
echo "  L0=$MEMORY_L0, L1=$MEMORY_L1, Total=$MEMORY_TOTAL"
echo "=================================="
