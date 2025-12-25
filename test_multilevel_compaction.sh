#!/bin/bash

# Test multi-level compaction (L0 → L1 → L2 → L3 → L4)

set -e

echo "=== Multi-Level Compaction Test ==="
echo ""

CATALOG="test_compaction"
SCHEMA="testdata"
TABLE="metrics"
PARTITION_DIR="./data/$CATALOG/$SCHEMA/$TABLE/date=2025-12-24"

# Create test directory structure
mkdir -p "$PARTITION_DIR"

echo "Step 1: Generate 10 L0 files (simulating writes)"
for i in {1..10}; do
    timestamp=$((1703251234567 + i))
    touch "$PARTITION_DIR/L0_${timestamp}.parquet"
    echo "  Created L0_${timestamp}.parquet"
done

echo ""
echo "Current files:"
ls -lh "$PARTITION_DIR"

echo ""
echo "Step 2: Trigger compaction (L0 → L1)"
echo "  Expected: 10 L0 files → 2-3 L1 files (depends on grouping)"
curl -s -X POST "http://localhost:7000/compact/$CATALOG"

sleep 2

echo ""
echo "After L0 → L1 compaction:"
ls -lh "$PARTITION_DIR"

echo ""
echo "Step 3: Generate more L0 files and repeat to get enough L1 files"
for round in {1..2}; do
    echo ""
    echo "  Round $round: Adding 5 more L0 files"
    for i in {1..5}; do
        timestamp=$((1703251234567 + 100 * round + i))
        touch "$PARTITION_DIR/L0_${timestamp}.parquet"
    done
    
    curl -s -X POST "http://localhost:7000/compact/$CATALOG"
    sleep 1
done

echo ""
echo "After multiple rounds:"
ls -lh "$PARTITION_DIR"

echo ""
echo "Step 4: Verify multi-level compaction"
echo "  L0 files:"
ls "$PARTITION_DIR"/L0_*.parquet 2>/dev/null | wc -l
echo "  L1 files:"
ls "$PARTITION_DIR"/L1_*.parquet 2>/dev/null | wc -l
echo "  L2 files:"
ls "$PARTITION_DIR"/L2_*.parquet 2>/dev/null | wc -l
echo "  L3 files:"
ls "$PARTITION_DIR"/L3_*.parquet 2>/dev/null | wc -l
echo "  L4 files:"
ls "$PARTITION_DIR"/L4_*.parquet 2>/dev/null | wc -l

echo ""
echo "=== Test Complete ==="
echo ""
echo "Note: With min_files=5 and major_min_files=4:"
echo "  - L0 → L1 triggers when 5+ L0 files exist"
echo "  - L1 → L2 triggers when 4+ L1 files exist"
echo "  - L2 → L3 triggers when 4+ L2 files exist"
echo "  - L3 → L4 triggers when 4+ L3 files exist"
echo "  - L4 is the final level (max_level=4)"
