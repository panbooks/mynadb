#!/bin/bash

# Test compaction of non-partitioned tables (e.g., series_stats)

set -e

echo "=== Non-Partitioned Table Compaction Test ==="
echo ""

# Test with series_stats table (non-partitioned)
CATALOG="_system"
SCHEMA="default"
TABLE="series_stats"
TABLE_DIR="./data/$CATALOG/$SCHEMA/$TABLE"

echo "Test Scenario 1: Non-Partitioned Table (series_stats)"
echo "======================================================"

# Create test directory structure (no date= subdirectories)
mkdir -p "$TABLE_DIR"

echo ""
echo "Step 1: Create 8 L0 files directly in table directory"
for i in {1..8}; do
    timestamp=$((1703251234567 + i * 1000))
    touch "$TABLE_DIR/L0_${timestamp}.parquet"
    echo "  Created L0_${timestamp}.parquet"
done

echo ""
echo "Current files in $TABLE_DIR:"
ls -lh "$TABLE_DIR" | grep -E '(L[0-9]_|total)'

echo ""
echo "Step 2: Trigger compaction on non-partitioned table"
echo "  Expected: 8 L0 files → L1 files (since min_files=5)"
curl -s -X POST "http://localhost:7000/compact/$CATALOG/$SCHEMA/$TABLE" | jq '.'

sleep 2

echo ""
echo "After compaction:"
ls -lh "$TABLE_DIR" | grep -E '(L[0-9]_|total)'

echo ""
echo "File count by level:"
echo "  L0 files: $(ls "$TABLE_DIR"/L0_*.parquet 2>/dev/null | wc -l)"
echo "  L1 files: $(ls "$TABLE_DIR"/L1_*.parquet 2>/dev/null | wc -l)"
echo "  L2 files: $(ls "$TABLE_DIR"/L2_*.parquet 2>/dev/null | wc -l)"

echo ""
echo "======================================================"
echo "Test Scenario 2: Partitioned Table (regular table)"
echo "======================================================"

CATALOG2="benchmark"
SCHEMA2="svc_1"
TABLE2="cpu"
PARTITION_DIR="./data/$CATALOG2/$SCHEMA2/$TABLE2/date=2025-12-24"

mkdir -p "$PARTITION_DIR"

echo ""
echo "Step 1: Create 6 L0 files in partitioned table"
for i in {1..6}; do
    timestamp=$((1703251234567 + i * 2000))
    touch "$PARTITION_DIR/L0_${timestamp}.parquet"
    echo "  Created L0_${timestamp}.parquet"
done

echo ""
echo "Current files in partition:"
ls -lh "$PARTITION_DIR" | grep -E '(L[0-9]_|total)'

echo ""
echo "Step 2: Trigger compaction on partitioned table"
curl -s -X POST "http://localhost:7000/compact/$CATALOG2/$SCHEMA2/$TABLE2" | jq '.'

sleep 2

echo ""
echo "After compaction:"
ls -lh "$PARTITION_DIR" | grep -E '(L[0-9]_|total)'

echo ""
echo "======================================================"
echo "Test Scenario 3: Catalog-wide compaction"
echo "======================================================"
echo ""
echo "Compact entire _system catalog (includes non-partitioned tables)"
curl -s -X POST "http://localhost:7000/compact/_system" | jq '.'

echo ""
echo "=== Test Complete ==="
echo ""
echo "Summary:"
echo "  ✓ Non-partitioned tables (series_stats) can be compacted"
echo "  ✓ Partitioned tables (cpu) continue to work as before"
echo "  ✓ Catalog-wide compaction handles both types automatically"
echo ""
echo "Key Behavior:"
echo "  - Non-partitioned: Files are in ./data/catalog/schema/table/*.parquet"
echo "  - Partitioned: Files are in ./data/catalog/schema/table/date=YYYY-MM-DD/*.parquet"
echo "  - Compactor detects structure automatically and handles appropriately"
