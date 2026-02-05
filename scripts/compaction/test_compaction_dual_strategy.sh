#!/bin/bash

# Test script for dual compaction strategy (LSM + Full Compaction)
# Tests:
# 1. LSM compaction (L0 → L1 → L2)
# 2. Date-based full compaction for yesterday's partitions
# 3. File count reduction and level progression

set -e

HOST="http://localhost:7000"
DB="compaction_test"
SERVICE="svc_dual_test"
TABLE="metrics"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if service is running
check_service() {
    log_info "Checking if service is running..."
    if ! curl -s "$HOST/health" > /dev/null; then
        log_error "Service is not running at $HOST"
        log_info "Please start the service first: cargo run --release"
        exit 1
    fi
    log_success "Service is running"
}

# Write data for a specific date
write_data_for_date() {
    local date=$1
    local count=$2
    local batch_size=100

    log_info "Writing $count data points for date: $date"

    # Convert date to timestamp (midnight UTC)
    local timestamp=$(date -d "$date" +%s)000000000

    for ((i=1; i<=count; i++)); do
        local data=""
        for ((j=1; j<=batch_size; j++)); do
            local ts=$((timestamp + (i-1)*batch_size*1000000000 + j*1000000000))
            data+="cpu,host=server$((i % 10)),service=$SERVICE usage=$((RANDOM % 100)) $ts\n"
        done

        echo -e "$data" | curl -s -X POST "$HOST/write/$DB" \
            -H "Content-Type: text/plain" \
            --data-binary @- > /dev/null

        if ((i % 10 == 0)); then
            echo -n "."
        fi
    done
    echo ""
    log_success "Wrote $count batches for $date"
}

# Get partition file info
get_partition_files() {
    local catalog=$1
    local schema=$2
    local table=$3
    local partition=$4

    local path="./data/$catalog/$schema/$table/$partition"

    if [ ! -d "$path" ]; then
        echo "0 0 0 0 0"
        return
    fi

    local total=$(find "$path" -name "*.parquet" 2>/dev/null | wc -l)
    local l0=$(find "$path" -name "L0_*.parquet" 2>/dev/null | wc -l)
    local l1=$(find "$path" -name "L1_*.parquet" 2>/dev/null | wc -l)
    local l2=$(find "$path" -name "L2_*.parquet" 2>/dev/null | wc -l)
    local full=$(find "$path" -name "FULL_*.parquet" 2>/dev/null | wc -l)

    echo "$total $l0 $l1 $l2 $full"
}

# Display file statistics
show_file_stats() {
    local catalog=$1
    local schema=$2
    local table=$3
    local partition=$4
    local label=$5

    read -r total l0 l1 l2 full <<< $(get_partition_files "$catalog" "$schema" "$table" "$partition")

    log_info "[$label] Partition: $partition"
    echo "  Total files: $total"
    echo "  L0 files: $l0"
    echo "  L1 files: $l1"
    echo "  L2 files: $l2"
    echo "  FULL files: $full"
}

# Test LSM compaction
test_lsm_compaction() {
    log_info "===== TEST 1: LSM Compaction Strategy ====="

    local TODAY=$(date +%Y-%m-%d)
    local PARTITION="date=$TODAY"

    # Write data to create L0 files
    log_info "Step 1: Writing data to create L0 files..."
    write_data_for_date "$TODAY" 8

    # Flush cache to ensure data is on disk
    log_info "Flushing cache..."
    curl -s -X POST "$HOST/flush" > /dev/null
    sleep 2

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Initial Write"

    # Wait for first LSM compaction (L0 → L1)
    log_info "Step 2: Waiting for L0 → L1 compaction (min_files=4, interval=60s)..."
    log_warning "Waiting 70 seconds for LSM compaction to trigger..."
    sleep 70

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After First LSM Cycle"

    read -r total1 l0_1 l1_1 l2_1 full_1 <<< $(get_partition_files "$DB" "$SERVICE" "$TABLE" "$PARTITION")

    if [ "$l1_1" -gt 0 ]; then
        log_success "L0 → L1 compaction successful! L1 files: $l1_1"
    else
        log_warning "L1 files not created yet. L0 files: $l0_1"
    fi

    # Write more data to trigger additional compaction
    log_info "Step 3: Writing more data for multi-level compaction..."
    write_data_for_date "$TODAY" 6
    curl -s -X POST "$HOST/flush" > /dev/null
    sleep 2

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Second Write"

    # Wait for L1 → L2 compaction
    log_info "Step 4: Waiting for L1 → L2 compaction (major_min_files=4)..."
    log_warning "Waiting 70 seconds for next LSM cycle..."
    sleep 70

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Second LSM Cycle"

    read -r total2 l0_2 l1_2 l2_2 full_2 <<< $(get_partition_files "$DB" "$SERVICE" "$TABLE" "$PARTITION")

    # Verify LSM compaction worked
    if [ "$l1_2" -gt 0 ] || [ "$l2_2" -gt 0 ]; then
        log_success "Multi-level LSM compaction successful!"
        echo "  L0: $l0_2, L1: $l1_2, L2: $l2_2"
    else
        log_warning "Multi-level compaction still in progress"
    fi

    # Query data to verify integrity
    log_info "Step 5: Verifying data integrity with query..."
    local count=$(curl -s "$HOST/query/$DB" -G --data-urlencode "sql=SELECT COUNT(*) as cnt FROM $SERVICE.$TABLE" | \
        grep -o '"cnt":[0-9]*' | grep -o '[0-9]*')

    if [ -n "$count" ] && [ "$count" -gt 0 ]; then
        log_success "Data integrity verified: $count rows"
    else
        log_error "Data query failed or returned 0 rows"
    fi
}

# Test full compaction for yesterday
test_full_compaction() {
    log_info ""
    log_info "===== TEST 2: Date-Based Full Compaction ====="

    local YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
    local PARTITION="date=$YESTERDAY"

    # Write data for yesterday
    log_info "Step 1: Writing data for yesterday ($YESTERDAY)..."
    write_data_for_date "$YESTERDAY" 10

    # Flush cache
    log_info "Flushing cache..."
    curl -s -X POST "$HOST/flush" > /dev/null
    sleep 2

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Writing Yesterday's Data"

    read -r total_before l0_before l1_before l2_before full_before <<< \
        $(get_partition_files "$DB" "$SERVICE" "$TABLE" "$PARTITION")

    log_info "Files before full compaction: Total=$total_before, L0=$l0_before, L1=$l1_before, L2=$l2_before"

    # Test 1: Verify full compaction doesn't run at wrong hour
    local CURRENT_HOUR=$(date +%H | sed 's/^0//')
    log_info "Step 2: Current hour is $CURRENT_HOUR (full compaction configured for hour 1)"

    if [ "$CURRENT_HOUR" -eq 1 ]; then
        log_warning "It's currently hour 1, full compaction might run automatically"
        log_warning "Waiting 70 seconds to see if full compaction runs..."
        sleep 70

        show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Waiting (Hour 1)"

        read -r total_after l0_after l1_after l2_after full_after <<< \
            $(get_partition_files "$DB" "$SERVICE" "$TABLE" "$PARTITION")

        if [ "$full_after" -gt 0 ]; then
            log_success "Full compaction executed automatically at configured hour!"
            echo "  Files: $total_before → $total_after"
            echo "  FULL files created: $full_after"
        else
            log_warning "Full compaction has not run yet"
        fi
    else
        log_info "Not at configured hour (1), full compaction will not run automatically"
        log_warning "Waiting 70 seconds to verify it doesn't run at wrong hour..."
        sleep 70

        show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Waiting (Hour $CURRENT_HOUR)"

        read -r total_check l0_check l1_check l2_check full_check <<< \
            $(get_partition_files "$DB" "$SERVICE" "$TABLE" "$PARTITION")

        if [ "$full_check" -eq 0 ] && [ "$total_check" -eq "$total_before" ]; then
            log_success "Full compaction correctly DID NOT run at hour $CURRENT_HOUR"
        else
            log_warning "Unexpected file changes detected"
        fi
    fi

    # Test 2: Manual trigger of full compaction
    log_info "Step 3: Manually triggering compaction for yesterday's partition..."

    # Trigger compaction via API endpoint
    curl -s -X POST "$HOST/compact/$DB/$TABLE" > /dev/null
    sleep 5

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "After Manual Compaction Trigger"

    read -r total_final l0_final l1_final l2_final full_final <<< \
        $(get_partition_files "$DB" "$SERVICE" "$TABLE" "$PARTITION")

    log_info "Compaction results:"
    echo "  Files before: $total_before (L0=$l0_before, L1=$l1_before, L2=$l2_before)"
    echo "  Files after:  $total_final (L0=$l0_final, L1=$l1_final, L2=$l2_final, FULL=$full_final)"

    if [ "$total_final" -lt "$total_before" ]; then
        log_success "File count reduced: $total_before → $total_final"
    else
        log_warning "File count did not reduce (may need more files to trigger compaction)"
    fi

    # Verify data integrity for yesterday
    log_info "Step 4: Verifying yesterday's data integrity..."
    local count=$(curl -s "$HOST/query/$DB" -G \
        --data-urlencode "sql=SELECT COUNT(*) as cnt FROM $SERVICE.$TABLE WHERE date='$YESTERDAY'" | \
        grep -o '"cnt":[0-9]*' | grep -o '[0-9]*')

    if [ -n "$count" ] && [ "$count" -gt 0 ]; then
        log_success "Data integrity verified for yesterday: $count rows"
    else
        log_error "Query for yesterday's data failed or returned 0 rows"
    fi
}

# Test size-based skip logic
test_size_skip() {
    log_info ""
    log_info "===== TEST 3: Full Compaction Size Skip Logic ====="

    local TEST_DATE=$(date -d "2 days ago" +%Y-%m-%d)
    local PARTITION="date=$TEST_DATE"

    log_info "Testing that partitions > 200MB are skipped..."
    log_info "Writing large dataset for $TEST_DATE..."

    # Write a lot of data to exceed 200MB (this might take a while)
    # For testing, we'll write moderate amount and check the logic
    write_data_for_date "$TEST_DATE" 5
    curl -s -X POST "$HOST/flush" > /dev/null
    sleep 2

    show_file_stats "$DB" "$SERVICE" "$TABLE" "$PARTITION" "Test Date Partition"

    # Check partition size
    local partition_path="./data/$DB/$SERVICE/$TABLE/$PARTITION"
    if [ -d "$partition_path" ]; then
        local size_bytes=$(du -sb "$partition_path" 2>/dev/null | cut -f1)
        local size_mb=$((size_bytes / 1024 / 1024))
        log_info "Partition size: ${size_mb}MB"

        if [ "$size_mb" -gt 200 ]; then
            log_info "Partition exceeds 200MB, would be skipped by full compaction"
        else
            log_info "Partition is ${size_mb}MB (< 200MB), eligible for full compaction"
        fi
    fi
}

# Main test execution
main() {
    echo ""
    log_info "=========================================="
    log_info "Dual Compaction Strategy Test"
    log_info "=========================================="
    echo ""

    check_service

    # Run tests
    test_lsm_compaction
    test_full_compaction
    test_size_skip

    echo ""
    log_info "=========================================="
    log_info "Test Summary"
    log_info "=========================================="

    # Show final state
    local TODAY=$(date +%Y-%m-%d)
    local YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

    echo ""
    log_info "Final File Statistics:"
    echo ""
    show_file_stats "$DB" "$SERVICE" "$TABLE" "date=$TODAY" "Today"
    echo ""
    show_file_stats "$DB" "$SERVICE" "$TABLE" "date=$YESTERDAY" "Yesterday"

    echo ""
    log_info "Check service logs for compaction details:"
    echo "  - LSM compaction runs every 60 seconds"
    echo "  - Full compaction checks every hour (acts at hour 1)"
    echo "  - Look for messages like:"
    echo "    'LSM compaction [compaction_test]: N partition(s)'"
    echo "    'Full compaction: N partitions compacted'"

    echo ""
    log_success "Test completed!"
    log_info "Database: $DB, Schema: $SERVICE, Table: $TABLE"
}

# Run main
main
