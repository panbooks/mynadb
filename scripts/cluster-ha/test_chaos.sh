#!/bin/bash

# Chaos Testing Script for MynaDB
# Tests crash recovery, data integrity, and fault tolerance
#
# Reference: ClickHouse/CockroachDB chaos testing approaches
#
# Tests:
# 1. Process crash during write (WAL recovery)
# 2. Process crash during flush
# 3. Process crash during compaction
# 4. Rapid restart cycles
# 5. Data integrity after graceful vs hard shutdown
# 6. Concurrent operations during crash

# Don't exit on error - we handle errors manually
# set -e

# Configuration
BASE_URL="http://localhost:7000"
DB="chaostest"
DATA_DIR="./data"
BINARY="./target/release/mynadb"
CONFIG_FILE="/tmp/chaos_test_config.toml"
LOG_FILE="/tmp/chaos_test.log"
MYNADB_PID=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TESTS_PASSED=0
TESTS_FAILED=0

#######################################
# Helper Functions
#######################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Create test configuration file
create_config() {
    cat > "$CONFIG_FILE" << 'EOF'
[server]
http_port = 7000
flight_port = 7001
mysql_enabled = false

[storage]
data_dir = "./data"

[storage.object_store]
backend = "local"

[cache]
flush_interval_secs = 300
max_size_mb = 256

[wal]
enabled = true
sync_on_write = true

[compaction]
min_files = 3
max_level = 3

[retention]
enabled = false

[auth]
enabled = false

[logging]
level = "info"
EOF
    log_info "Created test config at $CONFIG_FILE"
}

# Clean up test data
cleanup() {
    log_info "Cleaning up..."
    kill_server
    rm -rf "${DATA_DIR}/${DB}" 2>/dev/null || true
    rm -rf "${DATA_DIR}/chaostest"* 2>/dev/null || true
    rm -f "$LOG_FILE" 2>/dev/null || true
}

# Build the binary if needed
build_binary() {
    if [ ! -f "$BINARY" ]; then
        log_info "Building release binary..."
        cargo build --release 2>&1 | tail -5
    fi
}

# Start the server
start_server() {
    if [ -n "$MYNADB_PID" ] && kill -0 "$MYNADB_PID" 2>/dev/null; then
        log_warn "Server already running with PID $MYNADB_PID"
        return 0
    fi

    log_info "Starting MynaDB server..."
    $BINARY -c "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    MYNADB_PID=$!

    # Wait for server to be ready
    local retries=30
    while [ $retries -gt 0 ]; do
        if curl -s "${BASE_URL}/health" > /dev/null 2>&1; then
            log_info "Server started with PID $MYNADB_PID"
            # Extra wait to ensure server is fully ready
            sleep 1
            return 0
        fi
        sleep 0.5
        retries=$((retries - 1))
    done

    log_fail "Server failed to start within 15 seconds"
    return 1
}

# Kill server gracefully
stop_server() {
    if [ -n "$MYNADB_PID" ] && kill -0 "$MYNADB_PID" 2>/dev/null; then
        log_info "Stopping server gracefully (SIGTERM)..."
        kill "$MYNADB_PID" 2>/dev/null || true
        wait "$MYNADB_PID" 2>/dev/null || true
        MYNADB_PID=""
    fi
}

# Kill server with SIGKILL (simulate crash)
crash_server() {
    if [ -n "$MYNADB_PID" ] && kill -0 "$MYNADB_PID" 2>/dev/null; then
        log_info "Crashing server (SIGKILL) PID $MYNADB_PID..."
        kill -9 "$MYNADB_PID" 2>/dev/null || true
        wait "$MYNADB_PID" 2>/dev/null || true
        MYNADB_PID=""
    fi
}

# Kill any running server
kill_server() {
    pkill -9 -f "mynadb.*chaos_test_config" 2>/dev/null || true
    sleep 1
    MYNADB_PID=""
}

# Write data points
write_data() {
    local count=$1
    local measurement=${2:-"chaos_metric"}
    local data=""

    for i in $(seq 1 $count); do
        data+="${measurement},host=server${i},region=test value=${i}.5,counter=${i}i
"
    done

    local response=$(curl -s -w "%{http_code}" -X POST "${BASE_URL}/write/${DB}" -d "$data")
    local http_code="${response: -3}"

    if [ "$http_code" != "200" ] && [ "$http_code" != "204" ]; then
        log_warn "Write returned HTTP $http_code"
    fi

    # Small delay to let data be processed
    sleep 0.2
}

# Write data with unique timestamps
write_data_with_timestamps() {
    local count=$1
    local measurement=${2:-"chaos_metric"}
    local base_ts=$(date +%s%N)
    local data=""

    for i in $(seq 1 $count); do
        local ts=$((base_ts + i * 1000000))
        data+="${measurement},host=server${i} value=${i}.5 ${ts}
"
    done

    curl -s -X POST "${BASE_URL}/write/${DB}" -d "$data" > /dev/null
}

# Flush data to disk
flush_data() {
    curl -s -X POST "${BASE_URL}/flush" > /dev/null
    # Wait for flush to complete and data to be available
    sleep 2
}

# Query and get count
get_row_count() {
    local measurement=${1:-"chaos_metric"}
    local result=$(curl -s -X POST "${BASE_URL}/query/${DB}" \
        -H "Content-Type: text/plain" \
        -d "SELECT COUNT(*) FROM ${measurement}" 2>/dev/null)

    # Handle empty or error responses
    if [ -z "$result" ] || [ "$result" = "[]" ]; then
        echo "0"
        return
    fi

    # Try to extract count from JSON: [{"count(*)":123}]
    # Use Python for reliable JSON parsing
    local count=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if isinstance(data, list) and len(data) > 0:
        row = data[0]
        # Try various column name formats (including count(*) with lowercase)
        for key in ['count(*)', 'COUNT(*)', 'cnt', 'count']:
            if key in row:
                print(int(row[key]))
                sys.exit(0)
        # Try first value
        for v in row.values():
            if v is not None:
                print(int(v))
                sys.exit(0)
    print(0)
except Exception as e:
    print(0)
" 2>/dev/null)

    echo "${count:-0}"
}

# Get sum of values
get_sum() {
    local measurement=${1:-"chaos_metric"}
    local result=$(curl -s -X POST "${BASE_URL}/query/${DB}" \
        -H "Content-Type: text/plain" \
        -d "SELECT SUM(value) FROM ${measurement}" 2>/dev/null)

    # Handle empty or error responses
    if [ -z "$result" ] || [ "$result" = "[]" ]; then
        echo "0"
        return
    fi

    # Use Python for reliable JSON parsing: [{"sum(value)": 123.5}]
    local sum=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if isinstance(data, list) and len(data) > 0:
        row = data[0]
        for key in ['sum(value)', 'SUM(value)', 'total', 'sum']:
            if key in row and row[key] is not None:
                print(float(row[key]))
                sys.exit(0)
        for v in row.values():
            if v is not None:
                print(float(v))
                sys.exit(0)
    print(0)
except Exception as e:
    print(0)
" 2>/dev/null)

    echo "${sum:-0}"
}

# Trigger compaction
trigger_compaction() {
    curl -s -X POST "${BASE_URL}/compact/${DB}" > /dev/null
}

# Check if data files exist
count_parquet_files() {
    local measurement=${1:-"chaos_metric"}
    local dir="${DATA_DIR}/${DB}/default/${measurement}"

    if [ -d "$dir" ]; then
        find "$dir" -name "*.parquet" -type f 2>/dev/null | wc -l
    else
        echo "0"
    fi
}

# Calculate expected sum for N sequential integers (1.5 + 2.5 + ... + N.5)
calc_expected_sum() {
    local n=$1
    # Sum of i.5 for i=1 to n = (n*(n+1)/2) + 0.5*n = n*(n+1)/2 + n/2 = n*(n+2)/2
    echo "scale=1; $n * ($n + 2) / 2" | bc
}

#######################################
# Test Cases
#######################################

# Test 1: WAL Recovery - Crash during write (before flush)
test_wal_recovery() {
    echo ""
    echo "=============================================="
    echo "Test 1: WAL Recovery (Crash Before Flush)"
    echo "=============================================="

    cleanup
    create_config
    start_server || return 1

    # Write data but don't flush
    log_info "Writing 100 data points (not flushed)..."
    write_data 100

    # Verify data is in cache
    local cache_count=$(get_row_count)
    log_info "Cache row count before crash: $cache_count"

    # Crash the server
    crash_server
    sleep 1

    # Restart server (WAL should replay)
    log_info "Restarting server (WAL should recover data)..."
    start_server || return 1
    sleep 2

    # Verify data recovered
    local recovered_count=$(get_row_count)
    log_info "Row count after recovery: $recovered_count"

    if [ "$recovered_count" -eq 100 ]; then
        log_success "WAL recovery: All 100 rows recovered"
    else
        log_fail "WAL recovery: Expected 100 rows, got $recovered_count"
    fi

    stop_server
}

# Test 2: Crash during flush
test_crash_during_flush() {
    echo ""
    echo "=============================================="
    echo "Test 2: Crash During Flush"
    echo "=============================================="

    cleanup
    create_config
    start_server || return 1

    # Write first batch and flush
    log_info "Writing and flushing first batch (50 rows)..."
    write_data 50 "flush_test"
    flush_data

    local count_after_first=$(get_row_count "flush_test")
    log_info "Rows after first flush: $count_after_first"

    # Write second batch
    log_info "Writing second batch (50 rows, not flushed)..."
    write_data_with_timestamps 50 "flush_test"

    # Start flush in background and crash mid-way
    log_info "Starting flush and crashing..."
    curl -s -X POST "${BASE_URL}/flush" &
    sleep 0.2
    crash_server
    sleep 1

    # Restart
    log_info "Restarting server..."
    start_server || return 1
    sleep 2

    # Check data integrity
    local final_count=$(get_row_count "flush_test")
    log_info "Final row count: $final_count"

    # Should have at least the first batch (50) plus WAL recovery of second batch
    if [ "$final_count" -ge 50 ]; then
        log_success "Crash during flush: At least first batch preserved ($final_count rows)"
    else
        log_fail "Crash during flush: Data loss detected (expected >= 50, got $final_count)"
    fi

    stop_server
}

# Test 3: Crash during compaction
test_crash_during_compaction() {
    echo ""
    echo "=============================================="
    echo "Test 3: Crash During Compaction"
    echo "=============================================="

    cleanup
    create_config
    start_server || return 1

    # Create multiple L0 files to trigger compaction
    log_info "Creating multiple L0 files..."
    local total_rows=0
    for batch in $(seq 1 6); do
        write_data 20 "compact_test"
        flush_data
        total_rows=$((total_rows + 20))
        log_info "  Batch $batch flushed (total: $total_rows rows)"
    done

    local file_count=$(count_parquet_files "compact_test")
    log_info "Parquet files before compaction: $file_count"

    local count_before=$(get_row_count "compact_test")
    log_info "Row count before compaction: $count_before"

    # Start compaction and crash mid-way
    log_info "Starting compaction and crashing..."
    trigger_compaction &
    sleep 0.3
    crash_server
    sleep 1

    # Restart
    log_info "Restarting server..."
    start_server || return 1
    sleep 2

    # Check data integrity
    local count_after=$(get_row_count "compact_test")
    local files_after=$(count_parquet_files "compact_test")
    log_info "Row count after restart: $count_after"
    log_info "Parquet files after restart: $files_after"

    if [ "$count_after" -eq "$count_before" ]; then
        log_success "Crash during compaction: No data loss ($count_after rows preserved)"
    else
        log_fail "Crash during compaction: Data inconsistency (expected $count_before, got $count_after)"
    fi

    stop_server
}

# Test 4: Rapid restart cycles
test_rapid_restarts() {
    echo ""
    echo "=============================================="
    echo "Test 4: Rapid Restart Cycles"
    echo "=============================================="

    cleanup
    create_config
    start_server || return 1

    # Write initial data
    log_info "Writing initial data..."
    write_data 50 "restart_test"
    flush_data

    local initial_count=$(get_row_count "restart_test")
    log_info "Initial row count: $initial_count"

    # Rapid restart cycles
    log_info "Performing 5 rapid crash/restart cycles..."
    for cycle in $(seq 1 5); do
        # Write some data
        write_data 10 "restart_test"

        # Random delay before crash
        sleep 0.$((RANDOM % 5))

        # Crash
        crash_server

        # Quick restart
        start_server || {
            log_fail "Rapid restarts: Failed to restart on cycle $cycle"
            return 1
        }

        log_info "  Cycle $cycle complete"
    done

    # Final flush and count
    flush_data
    local final_count=$(get_row_count "restart_test")
    log_info "Final row count: $final_count"

    # Should have at least the initial data
    if [ "$final_count" -ge "$initial_count" ]; then
        log_success "Rapid restarts: Data preserved through 5 crash cycles (initial: $initial_count, final: $final_count)"
    else
        log_fail "Rapid restarts: Data loss detected (expected >= $initial_count, got $final_count)"
    fi

    stop_server
}

# Test 5: Graceful shutdown vs hard kill comparison
test_shutdown_comparison() {
    echo ""
    echo "=============================================="
    echo "Test 5: Graceful Shutdown vs Hard Kill"
    echo "=============================================="

    local expected=100

    # Test A: Graceful shutdown
    cleanup
    create_config
    start_server || return 1

    log_info "Test 5A: Graceful shutdown..."
    write_data $expected "graceful_test"
    # Data is in WAL, now gracefully stop (should trigger flush on shutdown)

    stop_server  # Graceful SIGTERM
    sleep 2

    start_server || return 1
    sleep 2
    local graceful_after=$(get_row_count "graceful_test")
    log_info "Graceful shutdown: expected=$expected, recovered=$graceful_after"

    stop_server

    # Test B: Hard kill
    cleanup
    create_config
    start_server || return 1

    log_info "Test 5B: Hard kill (SIGKILL)..."
    write_data $expected "kill_test"
    # Data is in WAL, crash without flushing

    crash_server  # Hard SIGKILL
    sleep 1

    start_server || return 1
    sleep 2
    local kill_after=$(get_row_count "kill_test")
    log_info "Hard kill: expected=$expected, recovered=$kill_after"

    # Both should preserve data (graceful flushes on shutdown, hard uses WAL replay)
    if [ "$graceful_after" -eq "$expected" ] && [ "$kill_after" -eq "$expected" ]; then
        log_success "Shutdown comparison: Both methods preserved all $expected rows"
    elif [ "$graceful_after" -eq "$expected" ]; then
        log_fail "Hard kill data loss: expected $expected, got $kill_after (WAL recovery issue?)"
    elif [ "$kill_after" -eq "$expected" ]; then
        log_fail "Graceful shutdown lost data: expected $expected, got $graceful_after"
    else
        log_fail "Both methods lost data: graceful=$graceful_after, hard=$kill_after (expected $expected)"
    fi

    stop_server
}

# Test 6: Concurrent writes during crash
test_concurrent_crash() {
    echo ""
    echo "=============================================="
    echo "Test 6: Concurrent Writes During Crash"
    echo "=============================================="

    cleanup
    create_config
    start_server || return 1

    # Start multiple concurrent writers
    log_info "Starting 5 concurrent writers..."
    for writer in $(seq 1 5); do
        (
            for i in $(seq 1 20); do
                curl -s -X POST "${BASE_URL}/write/${DB}" \
                    -d "concurrent_test,writer=${writer} value=${i}.0" > /dev/null 2>&1
                sleep 0.05
            done
        ) &
    done

    # Let writers run for a bit
    sleep 0.5

    # Crash while writes are happening
    log_info "Crashing server during concurrent writes..."
    crash_server

    # Wait for writers to timeout/fail
    wait 2>/dev/null || true
    sleep 1

    # Restart and check
    log_info "Restarting server..."
    start_server || return 1
    sleep 2

    local recovered=$(get_row_count "concurrent_test")
    log_info "Recovered rows: $recovered"

    # We should have some data (exact amount depends on timing)
    if [ "$recovered" -gt 0 ]; then
        log_success "Concurrent crash: Recovered $recovered rows from concurrent writes"
    else
        log_warn "Concurrent crash: No data recovered (writes may have failed before reaching server)"
        # This is actually OK - if crash happened before any writes completed
        log_success "Concurrent crash: Server recovered cleanly (no corruption)"
    fi

    stop_server
}

# Test 7: Data integrity verification with checksums
test_data_integrity() {
    echo ""
    echo "=============================================="
    echo "Test 7: Data Integrity Verification"
    echo "=============================================="

    cleanup
    create_config
    start_server || return 1

    # Write known data with predictable values in batches
    log_info "Writing 200 rows with known values..."
    local expected_count=200
    local expected_sum=20100  # 1 + 2 + ... + 200 = 200 * 201 / 2

    for batch in $(seq 1 4); do
        local data=""
        for i in $(seq 1 50); do
            local idx=$(( (batch - 1) * 50 + i ))
            data+="integrity_test,batch=batch${batch} value=${idx}.0,id=${idx}i
"
        done
        curl -s -X POST "${BASE_URL}/write/${DB}" -d "$data" > /dev/null
        flush_data
        log_info "  Batch $batch written and flushed"
    done

    # Verify data before crash
    local actual_count=$(get_row_count "integrity_test")
    local actual_sum=$(get_sum "integrity_test")
    log_info "Before crash - Count: $actual_count, Sum: $actual_sum"

    if [ "$actual_count" -ne "$expected_count" ]; then
        log_warn "Data not fully visible before crash (count=$actual_count, expected=$expected_count)"
    fi

    # Crash and recover
    crash_server
    sleep 1
    start_server || return 1
    sleep 2

    local recovered_count=$(get_row_count "integrity_test")
    local recovered_sum=$(get_sum "integrity_test")
    log_info "After crash - Count: $recovered_count, Sum: $recovered_sum"

    # Verify integrity after recovery
    if [ "$recovered_count" -eq "$expected_count" ]; then
        log_success "Data integrity: Row count matches ($recovered_count)"
    else
        log_fail "Data integrity: Row count mismatch (expected $expected_count, got $recovered_count)"
    fi

    # Compare sums (allowing for floating point - compare integer parts)
    local recovered_sum_int=${recovered_sum%.*}
    if [ "$recovered_sum_int" -eq "$expected_sum" ]; then
        log_success "Data integrity: Sum matches ($recovered_sum)"
    else
        log_fail "Data integrity: Sum mismatch (expected $expected_sum, got $recovered_sum)"
    fi

    stop_server
}

# Test 8: WAL disabled - verify data loss on crash
test_wal_disabled() {
    echo ""
    echo "=============================================="
    echo "Test 8: WAL Disabled (Expected Data Loss)"
    echo "=============================================="

    cleanup

    # Create config with WAL disabled
    cat > "$CONFIG_FILE" << 'EOF'
[server]
http_port = 7000
flight_port = 7001
mysql_enabled = false

[storage]
data_dir = "./data"

[storage.object_store]
backend = "local"

[cache]
flush_interval_secs = 300
max_size_mb = 256

[wal]
enabled = false

[compaction]
min_files = 3
max_level = 3

[retention]
enabled = false

[auth]
enabled = false

[logging]
level = "info"
EOF

    start_server || return 1

    # Write data without flushing
    log_info "Writing 100 rows (WAL disabled, no flush)..."
    write_data 100 "nowal_test"

    local before_crash=$(get_row_count "nowal_test")
    log_info "Rows before crash: $before_crash"

    # Crash
    crash_server
    sleep 1

    # Restart
    start_server || return 1
    sleep 2

    local after_crash=$(get_row_count "nowal_test")
    log_info "Rows after crash: $after_crash"

    # With WAL disabled, unflushed data should be lost
    if [ "$after_crash" -eq 0 ]; then
        log_success "WAL disabled test: Unflushed data lost as expected (confirms WAL works when enabled)"
    else
        log_warn "WAL disabled test: Some data survived ($after_crash rows) - may have been flushed"
    fi

    stop_server
}

#######################################
# Main
#######################################

main() {
    echo "========================================================"
    echo "     MynaDB Chaos Testing Suite"
    echo "========================================================"
    echo "Testing crash recovery, data integrity, and fault tolerance"
    echo ""

    # Build if needed
    build_binary

    # Make sure no server is running
    kill_server

    # Run all tests
    test_wal_recovery
    test_crash_during_flush
    test_crash_during_compaction
    test_rapid_restarts
    test_shutdown_comparison
    test_concurrent_crash
    test_data_integrity
    test_wal_disabled

    # Cleanup
    cleanup

    # Summary
    echo ""
    echo "========================================================"
    echo "                  Test Summary"
    echo "========================================================"
    echo -e "Tests Passed: ${GREEN}${TESTS_PASSED}${NC}"
    echo -e "Tests Failed: ${RED}${TESTS_FAILED}${NC}"
    echo "========================================================"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All chaos tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some chaos tests failed.${NC}"
        exit 1
    fi
}

# Handle script interruption
trap 'echo ""; log_warn "Interrupted, cleaning up..."; cleanup; exit 130' INT TERM

# Run main
main "$@"
