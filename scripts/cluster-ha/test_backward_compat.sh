#!/bin/bash

# Backward Compatibility Test Script for MynaDB
# Tests that data written by one version can be read by another version
#
# Reference: ClickHouse tests data written by old versions with new versions
#
# Usage:
#   ./scripts/test_backward_compat.sh [old_binary] [new_binary]
#
# If no arguments provided, uses current binary for both (self-compat test)

# Configuration
BASE_URL="http://localhost:7000"
DB="compat_test"
DATA_DIR="./data"
CONFIG_FILE="/tmp/compat_test_config.toml"
LOG_FILE="/tmp/compat_test.log"
MYNADB_PID=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_PASSED=0
TESTS_FAILED=0

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; ((TESTS_PASSED++)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((TESTS_FAILED++)); }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# Create test configuration
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
min_files = 100

[retention]
enabled = false

[auth]
enabled = false

[logging]
level = "warn"
EOF
}

cleanup() {
    if [ -n "$MYNADB_PID" ] && kill -0 "$MYNADB_PID" 2>/dev/null; then
        kill "$MYNADB_PID" 2>/dev/null
        wait "$MYNADB_PID" 2>/dev/null
    fi
    pkill -9 -f "mynadb.*compat_test_config" 2>/dev/null || true
    MYNADB_PID=""
}

start_server() {
    local binary=$1
    log_info "Starting server: $binary"

    "$binary" -c "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    MYNADB_PID=$!

    local retries=30
    while [ $retries -gt 0 ]; do
        if curl -s "${BASE_URL}/health" > /dev/null 2>&1; then
            log_info "Server started with PID $MYNADB_PID"
            sleep 1
            return 0
        fi
        sleep 0.5
        retries=$((retries - 1))
    done

    log_fail "Server failed to start"
    return 1
}

stop_server() {
    if [ -n "$MYNADB_PID" ] && kill -0 "$MYNADB_PID" 2>/dev/null; then
        log_info "Stopping server..."
        kill "$MYNADB_PID" 2>/dev/null
        wait "$MYNADB_PID" 2>/dev/null
        MYNADB_PID=""
    fi
    sleep 1
}

write_test_data() {
    local measurement=$1
    local count=$2

    log_info "Writing $count rows to $measurement..."
    local data=""
    for i in $(seq 1 $count); do
        data+="${measurement},version=v1,idx=${i} value=${i}.5,counter=${i}i
"
    done
    curl -s -X POST "${BASE_URL}/write/${DB}" -d "$data" > /dev/null
    sleep 0.5
}

flush_data() {
    curl -s -X POST "${BASE_URL}/flush" > /dev/null
    sleep 2
}

get_row_count() {
    local measurement=$1
    local result=$(curl -s -X POST "${BASE_URL}/query/${DB}" -d "SELECT COUNT(*) FROM ${measurement}" 2>/dev/null)

    if [ -z "$result" ] || [ "$result" = "[]" ]; then
        echo "0"
        return
    fi

    python3 -c "
import json
result = '''$result'''
try:
    data = json.loads(result)
    if isinstance(data, list) and len(data) > 0:
        for v in data[0].values():
            print(int(v))
            raise SystemExit(0)
    print(0)
except SystemExit:
    pass
except Exception:
    print(0)
"
}

get_sum() {
    local measurement=$1
    local result=$(curl -s -X POST "${BASE_URL}/query/${DB}" -d "SELECT SUM(value) FROM ${measurement}" 2>/dev/null)

    if [ -z "$result" ] || [ "$result" = "[]" ]; then
        echo "0"
        return
    fi

    python3 -c "
import json
result = '''$result'''
try:
    data = json.loads(result)
    if isinstance(data, list) and len(data) > 0:
        for v in data[0].values():
            if v is not None:
                print(float(v))
                raise SystemExit(0)
    print(0)
except SystemExit:
    pass
except Exception:
    print(0)
"
}

# Test 1: Self-compatibility (same binary write and read)
test_self_compat() {
    local binary=$1

    echo ""
    echo "=============================================="
    echo "Test 1: Self-Compatibility"
    echo "=============================================="

    rm -rf "${DATA_DIR}/${DB}" 2>/dev/null

    start_server "$binary" || return 1

    # Write data
    write_test_data "selftest" 100
    flush_data

    local count1=$(get_row_count "selftest")
    log_info "Rows after write: $count1"

    # Restart and read
    stop_server
    start_server "$binary" || return 1

    local count2=$(get_row_count "selftest")
    log_info "Rows after restart: $count2"

    if [ "$count2" -eq 100 ]; then
        log_success "Self-compatibility: Data persisted correctly ($count2 rows)"
    else
        log_fail "Self-compatibility: Expected 100 rows, got $count2"
    fi

    stop_server
}

# Test 2: Cross-version compatibility (write with old, read with new)
test_cross_version() {
    local old_binary=$1
    local new_binary=$2

    echo ""
    echo "=============================================="
    echo "Test 2: Cross-Version Compatibility"
    echo "=============================================="
    echo "  Old binary: $old_binary"
    echo "  New binary: $new_binary"

    rm -rf "${DATA_DIR}/${DB}" 2>/dev/null

    # Write with old version
    log_info "Writing data with OLD version..."
    start_server "$old_binary" || return 1

    write_test_data "crosstest" 100
    flush_data

    local old_count=$(get_row_count "crosstest")
    local old_sum=$(get_sum "crosstest")
    log_info "Old version - Rows: $old_count, Sum: $old_sum"

    stop_server

    # Read with new version
    log_info "Reading data with NEW version..."
    start_server "$new_binary" || return 1

    local new_count=$(get_row_count "crosstest")
    local new_sum=$(get_sum "crosstest")
    log_info "New version - Rows: $new_count, Sum: $new_sum"

    # Verify data matches
    if [ "$new_count" -eq "$old_count" ]; then
        log_success "Cross-version: Row count matches ($new_count)"
    else
        log_fail "Cross-version: Row count mismatch (old=$old_count, new=$new_count)"
    fi

    # Compare sums (integer part)
    local old_sum_int=${old_sum%.*}
    local new_sum_int=${new_sum%.*}
    if [ "$new_sum_int" -eq "$old_sum_int" ]; then
        log_success "Cross-version: Sum matches ($new_sum)"
    else
        log_fail "Cross-version: Sum mismatch (old=$old_sum, new=$new_sum)"
    fi

    stop_server
}

# Test 3: Schema evolution - add new columns
test_schema_evolution() {
    local binary=$1

    echo ""
    echo "=============================================="
    echo "Test 3: Schema Evolution"
    echo "=============================================="

    rm -rf "${DATA_DIR}/${DB}" 2>/dev/null

    start_server "$binary" || return 1

    # Write data with original schema
    log_info "Writing data with original schema (value only)..."
    curl -s -X POST "${BASE_URL}/write/${DB}" -d '
schema_test value=1.0
schema_test value=2.0
schema_test value=3.0
' > /dev/null
    flush_data

    # Write data with extended schema (new columns)
    log_info "Writing data with extended schema (value + new_field)..."
    curl -s -X POST "${BASE_URL}/write/${DB}" -d '
schema_test value=4.0,new_field=100.0
schema_test value=5.0,new_field=200.0
' > /dev/null
    flush_data

    # Query should work with both schemas
    local count=$(get_row_count "schema_test")
    log_info "Total rows: $count"

    # Query the new field (should return nulls for old rows)
    local result=$(curl -s -X POST "${BASE_URL}/query/${DB}" \
        -d "SELECT value, new_field FROM schema_test ORDER BY value")
    log_info "Query result: $result"

    if [ "$count" -eq 5 ]; then
        log_success "Schema evolution: All rows readable ($count)"
    else
        log_fail "Schema evolution: Expected 5 rows, got $count"
    fi

    stop_server
}

# Test 4: Compacted data compatibility
test_compacted_compat() {
    local binary=$1

    echo ""
    echo "=============================================="
    echo "Test 4: Compacted Data Compatibility"
    echo "=============================================="

    rm -rf "${DATA_DIR}/${DB}" 2>/dev/null

    # Temporarily modify config to allow compaction
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

[compaction]
min_files = 3

[retention]
enabled = false

[auth]
enabled = false

[logging]
level = "warn"
EOF

    start_server "$binary" || return 1

    # Write multiple batches to trigger compaction
    log_info "Writing multiple batches..."
    local total=0
    for batch in $(seq 1 5); do
        local data=""
        for i in $(seq 1 20); do
            local idx=$(( (batch - 1) * 20 + i ))
            data+="compact_compat,batch=${batch} value=${idx}.0
"
        done
        curl -s -X POST "${BASE_URL}/write/${DB}" -d "$data" > /dev/null
        flush_data
        total=$((total + 20))
        log_info "  Batch $batch flushed"
    done

    local before_compact=$(get_row_count "compact_compat")
    log_info "Rows before compaction: $before_compact"

    # Trigger compaction
    log_info "Triggering compaction..."
    curl -s -X POST "${BASE_URL}/compact/${DB}" > /dev/null
    sleep 3

    local after_compact=$(get_row_count "compact_compat")
    log_info "Rows after compaction: $after_compact"

    # Restart and verify
    stop_server
    start_server "$binary" || return 1

    local after_restart=$(get_row_count "compact_compat")
    log_info "Rows after restart: $after_restart"

    if [ "$after_restart" -eq 100 ]; then
        log_success "Compacted compatibility: All rows preserved ($after_restart)"
    else
        log_fail "Compacted compatibility: Expected 100 rows, got $after_restart"
    fi

    stop_server

    # Restore original config
    create_config
}

main() {
    local old_binary=${1:-"./target/release/mynadb"}
    local new_binary=${2:-"./target/release/mynadb"}

    echo "========================================================"
    echo "     MynaDB Backward Compatibility Test Suite"
    echo "========================================================"
    echo "Old binary: $old_binary"
    echo "New binary: $new_binary"
    echo ""

    # Verify binaries exist
    if [ ! -f "$old_binary" ]; then
        echo "Error: Old binary not found: $old_binary"
        exit 1
    fi
    if [ ! -f "$new_binary" ]; then
        echo "Error: New binary not found: $new_binary"
        exit 1
    fi

    create_config
    cleanup

    # Run tests
    test_self_compat "$new_binary"

    if [ "$old_binary" != "$new_binary" ]; then
        test_cross_version "$old_binary" "$new_binary"
    else
        log_info "Skipping cross-version test (same binary)"
    fi

    test_schema_evolution "$new_binary"
    test_compacted_compat "$new_binary"

    # Cleanup
    cleanup
    rm -rf "${DATA_DIR}/${DB}" 2>/dev/null

    # Summary
    echo ""
    echo "========================================================"
    echo "                  Test Summary"
    echo "========================================================"
    echo -e "Tests Passed: ${GREEN}${TESTS_PASSED}${NC}"
    echo -e "Tests Failed: ${RED}${TESTS_FAILED}${NC}"
    echo "========================================================"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All compatibility tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some compatibility tests failed.${NC}"
        exit 1
    fi
}

trap 'cleanup; exit 130' INT TERM

main "$@"
