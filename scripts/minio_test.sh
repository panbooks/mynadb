#!/bin/bash
#
# MinIO Integration Test Script for MynaDB
# Tests MynaDB's ability to use MinIO as object storage backend
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
MINIO_ENDPOINT="http://localhost:9000"
MINIO_CONSOLE="http://localhost:9001"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_BUCKET="mynadb-test"
MINIO_DATA_DIR="/tmp/minio-test-data"

MYNADB_PORT=7000
MYNADB_BINARY="./target/release/mynadb"
MYNADB_CONFIG="/tmp/mynadb-minio-test.toml"
MYNADB_LOG="/tmp/mynadb-minio-test.log"
MYNADB_DATA_DIR="/tmp/mynadb-minio-test-data"

TEST_DB="minio_test"

# Counters
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
print_header() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
}

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

print_info() {
    echo -e "  $1"
}

wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=${3:-30}
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null | grep -q "200\|204"; then
            return 0
        fi
        sleep 1
        ((attempt++))
    done
    return 1
}

cleanup() {
    print_header "Cleanup"

    # Stop MynaDB
    print_step "Stopping MynaDB..."
    pkill -f "$MYNADB_CONFIG" 2>/dev/null || true
    sleep 1

    # Stop MinIO
    print_step "Stopping MinIO test instance..."
    pkill -f "minio server $MINIO_DATA_DIR" 2>/dev/null || true
    sleep 1

    # Remove test data
    print_step "Removing test data..."
    rm -rf "$MINIO_DATA_DIR" 2>/dev/null || true
    rm -rf "$MYNADB_DATA_DIR" 2>/dev/null || true
    rm -f "$MYNADB_CONFIG" 2>/dev/null || true
    rm -f "$MYNADB_LOG" 2>/dev/null || true

    print_success "Cleanup complete"
}

# Trap for cleanup on exit
trap cleanup EXIT

# ============================================================================
# PHASE 1: Setup
# ============================================================================
print_header "Phase 1: Setup"

# Check prerequisites
print_step "Checking prerequisites..."

if ! command -v minio &> /dev/null; then
    print_error "MinIO not found. Please install MinIO first."
    exit 1
fi
print_info "MinIO binary: $(which minio)"

if ! command -v mc &> /dev/null; then
    print_error "MinIO client (mc) not found. Please install mc first."
    exit 1
fi
print_info "MinIO client: $(which mc)"

if [ ! -f "$MYNADB_BINARY" ]; then
    print_step "Building MynaDB..."
    RUSTFLAGS="-A dead_code -A unused_imports" cargo build --release 2>&1 | tail -3
fi
print_info "MynaDB binary: $MYNADB_BINARY"

print_success "Prerequisites check passed"

# ============================================================================
# PHASE 2: Start MinIO
# ============================================================================
print_header "Phase 2: Start MinIO Server"

# Create data directory
print_step "Creating MinIO data directory..."
mkdir -p "$MINIO_DATA_DIR"

# Start MinIO
print_step "Starting MinIO server..."
MINIO_ROOT_USER=$MINIO_ACCESS_KEY MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY \
    nohup minio server "$MINIO_DATA_DIR" --address ":9000" --console-address ":9001" \
    > /tmp/minio-test.log 2>&1 &

print_step "Waiting for MinIO to be ready..."
if wait_for_service "$MINIO_ENDPOINT/minio/health/live" "MinIO" 30; then
    print_success "MinIO server started"
else
    print_error "MinIO failed to start"
    cat /tmp/minio-test.log
    exit 1
fi

# Configure MinIO client
print_step "Configuring MinIO client..."
mc alias set localtest "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4 > /dev/null 2>&1
print_success "MinIO client configured"

# Create bucket
print_step "Creating test bucket..."
mc mb "localtest/$MINIO_BUCKET" --ignore-existing > /dev/null 2>&1
print_success "Bucket '$MINIO_BUCKET' created"

# ============================================================================
# PHASE 3: Configure and Start MynaDB
# ============================================================================
print_header "Phase 3: Configure and Start MynaDB"

# Create test config
print_step "Creating MynaDB config for MinIO..."
cat > "$MYNADB_CONFIG" << EOF
[server]
http_host = "0.0.0.0"
http_port = $MYNADB_PORT
flight_host = "0.0.0.0"
flight_port = 7001

[storage]
data_dir = "$MYNADB_DATA_DIR"
system_dir = "_system"

[storage.object_store]
backend = "minio"
bucket = "$MINIO_BUCKET"
region = "us-east-1"
access_key = "$MINIO_ACCESS_KEY"
secret_key = "$MINIO_SECRET_KEY"
endpoint = "$MINIO_ENDPOINT"
allow_http = true

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true

[cache]
flush_interval_secs = 300
max_size_mb = 256

[query_cache]
enabled = true
max_size_mb = 64
max_entries = 100
ttl_secs = 60
min_query_time_ms = 5

[compaction]
min_files = 3
target_file_size_mb = 100
max_level = 3
interval_secs = 300
max_concurrent_compactions = 2
full_compaction_max_concurrent = 1

[logging]
level = "info"
log_dir = "/tmp"
file_prefix = "mynadb-minio-test"
rotation = "daily"
max_files = 3

[retention]
enabled = false

[wal]
enabled = true
sync_on_write = false
rotate_size_mb = 64
cleanup_interval_secs = 3600
retention_hours = 1

[auth]
enabled = false
EOF
print_success "Config created: $MYNADB_CONFIG"

# Create data directory
mkdir -p "$MYNADB_DATA_DIR"

# Start MynaDB
print_step "Starting MynaDB with MinIO backend..."
nohup "$MYNADB_BINARY" -c "$MYNADB_CONFIG" > "$MYNADB_LOG" 2>&1 &

print_step "Waiting for MynaDB to be ready..."
if wait_for_service "http://localhost:$MYNADB_PORT/health" "MynaDB" 30; then
    print_success "MynaDB started with MinIO backend"
else
    print_error "MynaDB failed to start"
    cat "$MYNADB_LOG"
    exit 1
fi

# ============================================================================
# PHASE 4: Write Test Data
# ============================================================================
print_header "Phase 4: Write Test Data"

# Write CPU metrics
print_step "Writing 100 CPU metrics..."
for i in $(seq 1 100); do
    host="server0$((i % 5 + 1))"
    usage=$((RANDOM % 100)).$((RANDOM % 99))
    temp=$((RANDOM % 50 + 20))
    curl -s -X POST "http://localhost:$MYNADB_PORT/write/$TEST_DB" \
        -d "cpu,host=$host,region=us-east usage=$usage,temp=$temp" > /dev/null
done
print_success "Wrote 100 CPU metrics"

# Write memory metrics
print_step "Writing 50 memory metrics..."
for i in $(seq 1 50); do
    host="server0$((i % 5 + 1))"
    used=$((RANDOM % 80 + 10))
    free=$((RANDOM % 20 + 5))
    curl -s -X POST "http://localhost:$MYNADB_PORT/write/$TEST_DB" \
        -d "memory,host=$host,region=us-west used=$used,free=$free" > /dev/null
done
print_success "Wrote 50 memory metrics"

# Write disk metrics
print_step "Writing 50 disk metrics..."
for i in $(seq 1 50); do
    host="server0$((i % 5 + 1))"
    mount="/data$((i % 3))"
    read_bytes=$((RANDOM * 1000))
    write_bytes=$((RANDOM * 500))
    curl -s -X POST "http://localhost:$MYNADB_PORT/write/$TEST_DB" \
        -d "disk,host=$host,mount=$mount read_bytes=$read_bytes,write_bytes=$write_bytes" > /dev/null
done
print_success "Wrote 50 disk metrics"

# Write network metrics
print_step "Writing 30 network metrics..."
for i in $(seq 1 30); do
    host="server0$((i % 5 + 1))"
    interface="eth$((i % 2))"
    rx_bytes=$((RANDOM * 10000))
    tx_bytes=$((RANDOM * 8000))
    curl -s -X POST "http://localhost:$MYNADB_PORT/write/$TEST_DB" \
        -d "network,host=$host,interface=$interface rx_bytes=$rx_bytes,tx_bytes=$tx_bytes" > /dev/null
done
print_success "Wrote 30 network metrics"

print_info "Total metrics written: 230"

# ============================================================================
# PHASE 5: Flush to MinIO
# ============================================================================
print_header "Phase 5: Flush Data to MinIO"

print_step "Flushing data to MinIO..."
FLUSH_RESULT=$(curl -s -X POST "http://localhost:$MYNADB_PORT/flush")
print_info "Flush result: $FLUSH_RESULT"

if echo "$FLUSH_RESULT" | grep -q "Flushed"; then
    print_success "Data flushed to MinIO"
else
    print_error "Flush failed"
fi

# Verify files in MinIO
print_step "Verifying files in MinIO bucket..."
sleep 2
FILE_COUNT=$(mc ls --recursive "localtest/$MINIO_BUCKET/" 2>/dev/null | wc -l)
print_info "Files in MinIO: $FILE_COUNT"

if [ "$FILE_COUNT" -gt 0 ]; then
    print_success "Files successfully stored in MinIO"
    mc ls --recursive "localtest/$MINIO_BUCKET/" --summarize 2>/dev/null | tail -3
else
    print_error "No files found in MinIO"
fi

# ============================================================================
# PHASE 6: Query Tests
# ============================================================================
print_header "Phase 6: Query Tests"

# Test 1: Count queries
print_step "Test: COUNT queries..."
CPU_COUNT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu" | grep -oP '"cnt":\s*\K[0-9]+')
MEMORY_COUNT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20memory" | grep -oP '"cnt":\s*\K[0-9]+')
DISK_COUNT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20disk" | grep -oP '"cnt":\s*\K[0-9]+')
NETWORK_COUNT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20network" | grep -oP '"cnt":\s*\K[0-9]+')

print_info "CPU records: $CPU_COUNT (expected: 100)"
print_info "Memory records: $MEMORY_COUNT (expected: 50)"
print_info "Disk records: $DISK_COUNT (expected: 50)"
print_info "Network records: $NETWORK_COUNT (expected: 30)"

if [ "$CPU_COUNT" -eq 100 ] && [ "$MEMORY_COUNT" -eq 50 ] && [ "$DISK_COUNT" -eq 50 ] && [ "$NETWORK_COUNT" -eq 30 ]; then
    print_success "COUNT queries returned correct values"
else
    print_error "COUNT queries returned incorrect values"
fi

# Test 2: GROUP BY aggregation
print_step "Test: GROUP BY aggregation..."
AGG_RESULT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20host,%20AVG(usage)%20as%20avg_usage,%20COUNT(*)%20as%20cnt%20FROM%20cpu%20GROUP%20BY%20host%20ORDER%20BY%20host")
HOST_COUNT=$(echo "$AGG_RESULT" | grep -o '"host"' | wc -l)

if [ "$HOST_COUNT" -eq 5 ]; then
    print_success "GROUP BY aggregation works (5 hosts)"
else
    print_error "GROUP BY aggregation failed (expected 5 hosts, got $HOST_COUNT)"
fi

# Test 3: WHERE filter
print_step "Test: WHERE filter..."
FILTER_RESULT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu%20WHERE%20host%20%3D%20'server01'")
FILTERED_COUNT=$(echo "$FILTER_RESULT" | grep -oP '"cnt":\s*\K[0-9]+')

if [ "$FILTERED_COUNT" -gt 0 ] && [ "$FILTERED_COUNT" -lt 100 ]; then
    print_success "WHERE filter works (server01 count: $FILTERED_COUNT)"
else
    print_error "WHERE filter failed"
fi

# Test 4: ORDER BY + LIMIT
print_step "Test: ORDER BY + LIMIT..."
LIMIT_RESULT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20host,%20usage%20FROM%20cpu%20ORDER%20BY%20usage%20DESC%20LIMIT%205")
LIMIT_COUNT=$(echo "$LIMIT_RESULT" | grep -o '"host"' | wc -l)

if [ "$LIMIT_COUNT" -eq 5 ]; then
    print_success "ORDER BY + LIMIT works (returned 5 rows)"
else
    print_error "ORDER BY + LIMIT failed (expected 5, got $LIMIT_COUNT)"
fi

# Test 5: UNION query
print_step "Test: UNION query..."
UNION_RESULT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20'cpu'%20as%20metric,%20COUNT(*)%20as%20cnt%20FROM%20cpu%20UNION%20ALL%20SELECT%20'memory',%20COUNT(*)%20FROM%20memory")
UNION_COUNT=$(echo "$UNION_RESULT" | grep -o '"metric"' | wc -l)

if [ "$UNION_COUNT" -eq 2 ]; then
    print_success "UNION query works"
else
    print_error "UNION query failed"
fi

# Test 6: MIN/MAX/SUM aggregations
print_step "Test: MIN/MAX/SUM aggregations..."
AGG_RESULT=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20MIN(usage)%20as%20min_val,%20MAX(usage)%20as%20max_val,%20SUM(usage)%20as%20sum_val%20FROM%20cpu")

if echo "$AGG_RESULT" | grep -q "min_val" && echo "$AGG_RESULT" | grep -q "max_val"; then
    print_success "MIN/MAX/SUM aggregations work"
else
    print_error "MIN/MAX/SUM aggregations failed"
fi

# Test 7: Query analyzer
print_step "Test: Query analyzer (ANALYZE endpoint)..."
ANALYZE_RESULT=$(curl -s "http://localhost:$MYNADB_PORT/analyze/$TEST_DB?sql=SELECT%20*%20FROM%20cpu")

if echo "$ANALYZE_RESULT" | grep -q "predicate_pushdown"; then
    print_success "Query analyzer works"
    print_info "Optimizations enabled: predicate_pushdown, partition_pruning"
else
    print_error "Query analyzer failed"
fi

# ============================================================================
# PHASE 7: Server Restart Persistence Test
# ============================================================================
print_header "Phase 7: Server Restart Persistence Test"

# Get counts before restart
print_step "Recording counts before restart..."
BEFORE_CPU=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu" | grep -oP '"cnt":\s*\K[0-9]+')
BEFORE_MEMORY=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20memory" | grep -oP '"cnt":\s*\K[0-9]+')
print_info "Before restart - CPU: $BEFORE_CPU, Memory: $BEFORE_MEMORY"

# Stop MynaDB
print_step "Stopping MynaDB..."
pkill -f "$MYNADB_CONFIG" 2>/dev/null || true
sleep 2

# Verify it's stopped
if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$MYNADB_PORT/health" 2>/dev/null | grep -q "000"; then
    print_info "MynaDB stopped"
else
    print_error "MynaDB did not stop properly"
fi

# Restart MynaDB
print_step "Restarting MynaDB..."
nohup "$MYNADB_BINARY" -c "$MYNADB_CONFIG" > "$MYNADB_LOG" 2>&1 &

if wait_for_service "http://localhost:$MYNADB_PORT/health" "MynaDB" 30; then
    print_info "MynaDB restarted"
else
    print_error "MynaDB failed to restart"
    exit 1
fi

# Get counts after restart
print_step "Verifying data after restart..."
AFTER_CPU=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu" | grep -oP '"cnt":\s*\K[0-9]+')
AFTER_MEMORY=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20memory" | grep -oP '"cnt":\s*\K[0-9]+')
print_info "After restart - CPU: $AFTER_CPU, Memory: $AFTER_MEMORY"

if [ "$BEFORE_CPU" -eq "$AFTER_CPU" ] && [ "$BEFORE_MEMORY" -eq "$AFTER_MEMORY" ]; then
    print_success "Data persistence verified after restart"
else
    print_error "Data mismatch after restart"
fi

# ============================================================================
# PHASE 8: Additional Write After Restart
# ============================================================================
print_header "Phase 8: Additional Write After Restart"

print_step "Writing additional data after restart..."
for i in $(seq 1 20); do
    curl -s -X POST "http://localhost:$MYNADB_PORT/write/$TEST_DB" \
        -d "cpu,host=server06,region=eu-west usage=$((RANDOM % 100)).$((RANDOM % 99))" > /dev/null
done
print_success "Wrote 20 additional CPU metrics"

print_step "Flushing new data..."
curl -s -X POST "http://localhost:$MYNADB_PORT/flush" > /dev/null
sleep 3  # Wait for data to be queryable

print_step "Verifying new data..."
NEW_CPU=$(curl -s "http://localhost:$MYNADB_PORT/query/$TEST_DB?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu" | grep -oP '"cnt":\s*\K[0-9]+')
EXPECTED_CPU=$((BEFORE_CPU + 20))

if [ "$NEW_CPU" -eq "$EXPECTED_CPU" ]; then
    print_success "Additional writes work after restart (total CPU: $NEW_CPU)"
else
    print_error "Additional write failed (expected $EXPECTED_CPU, got $NEW_CPU)"
fi

# Verify new data in MinIO
print_step "Verifying updated files in MinIO..."
FINAL_FILE_COUNT=$(mc ls --recursive "localtest/$MINIO_BUCKET/" 2>/dev/null | wc -l)
print_info "Total files in MinIO: $FINAL_FILE_COUNT"

if [ "$FINAL_FILE_COUNT" -gt "$FILE_COUNT" ]; then
    print_success "New files added to MinIO"
else
    print_info "Files may have been compacted"
fi

# ============================================================================
# PHASE 9: Final MinIO Verification
# ============================================================================
print_header "Phase 9: Final MinIO Verification"

print_step "MinIO bucket contents:"
mc ls --recursive "localtest/$MINIO_BUCKET/" 2>/dev/null

print_step "Storage summary:"
mc ls --recursive "localtest/$MINIO_BUCKET/" --summarize 2>/dev/null | tail -3

# ============================================================================
# Summary
# ============================================================================
print_header "Test Summary"

TOTAL_TESTS=$((TESTS_PASSED + TESTS_FAILED))
echo ""
echo -e "  Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "  Tests Failed: ${RED}$TESTS_FAILED${NC}"
echo -e "  Total Tests:  $TOTAL_TESTS"
echo ""

if [ "$TESTS_FAILED" -eq 0 ]; then
    echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  ALL TESTS PASSED - MinIO Integration Working Correctly${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
    exit 0
else
    echo -e "${RED}════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}  SOME TESTS FAILED - Check output above for details${NC}"
    echo -e "${RED}════════════════════════════════════════════════════════════${NC}"
    exit 1
fi
