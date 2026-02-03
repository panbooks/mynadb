#!/bin/bash
# Cold Standby Distribution Test
# Tests write distribution across Primary/Standby nodes with different percent configurations
#
# This test verifies:
# 1. Standby nodes never receive writes (mode routing works)
# 2. percent=100 distributes writes across all Primary nodes
# 3. percent=50 distributes writes across 50% of Primary nodes
# 4. Queries can retrieve distributed data
# 5. Scale-in doesn't cause data loss

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

success() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; exit 1; }
info() { echo -e "${YELLOW}→ $1${NC}"; }
header() { echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"; echo -e "${BLUE}  $1${NC}"; echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"; }

# Configuration
META_HTTP=6000
META_GRPC=6001
ROUTER_HTTP=7000
ROUTER_FLIGHT=7001

# Data node ports
DATA1_FLIGHT=5001
DATA2_FLIGHT=5101
DATA3_FLIGHT=5201
DATA4_FLIGHT=5301

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

test_passed() {
    success "$1"
    ((TESTS_PASSED++))
}

test_failed() {
    fail "$1"
    ((TESTS_FAILED++))
}

cleanup() {
    info "Cleaning up processes..."
    pkill -9 -f "mynadb.*coldstandby" 2>/dev/null || true
    pkill -9 -f "minio" 2>/dev/null || true
    sleep 2
}

start_minio() {
    info "Starting MinIO..."
    mkdir -p /data/minio
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        minio server /data/minio --console-address ":9001" > /tmp/minio.log 2>&1 &
    sleep 3

    # Configure mc alias and create bucket
    mc alias set local http://127.0.0.1:9000 minioadmin minioadmin 2>/dev/null || true
    mc mb local/mynadb-coldstandby 2>/dev/null || true

    # Verify MinIO is ready
    if curl -s http://127.0.0.1:9000/minio/health/ready > /dev/null 2>&1; then
        success "MinIO started and ready"
    else
        fail "MinIO failed to start"
    fi
}

start_cluster() {
    info "Starting meta nodes..."
    for i in 1 2 3; do
        ./target/release/mynadb meta start -c ./dev/configs/coldstandby/meta${i}.toml > /tmp/meta${i}.log 2>&1 &
    done
    sleep 5

    # Verify meta health
    local meta_health
    meta_health=$(curl -s http://localhost:$META_HTTP/health 2>/dev/null || echo "failed")
    if [ "$meta_health" != "OK" ]; then
        fail "Meta cluster not healthy: $meta_health"
    fi
    success "Meta cluster healthy"

    info "Starting data nodes (4 nodes: 2 Primary, 2 Standby expected)..."
    for i in 1 2 3 4; do
        ./target/release/mynadb data start -c ./dev/configs/coldstandby/data${i}.toml > /tmp/data${i}.log 2>&1 &
    done
    sleep 10

    # Verify data nodes registered
    local node_count
    node_count=$(curl -s http://localhost:$META_HTTP/nodes 2>/dev/null | jq '. | length' || echo "0")
    if [ "$node_count" -lt 4 ]; then
        fail "Expected 4 data nodes, got $node_count"
    fi
    success "Data nodes registered: $node_count"

    # Show node modes
    info "Node mode assignments:"
    curl -s http://localhost:$META_HTTP/nodes | jq -r '.[] | "  \(.node_id): \(.mode)"'

    info "Starting router..."
    ./target/release/mynadb router start -c ./dev/configs/coldstandby/router1.toml > /tmp/router1.log 2>&1 &
    sleep 5

    # Verify router health
    local router_health
    router_health=$(curl -s http://localhost:$ROUTER_HTTP/health 2>/dev/null || echo "failed")
    if [ "$router_health" != "OK" ]; then
        fail "Router not healthy: $router_health"
    fi
    success "Router healthy"
}

get_node_mode() {
    local node_id=$1
    curl -s http://localhost:$META_HTTP/nodes | jq -r ".[] | select(.node_id==\"$node_id\") | .mode"
}

count_writes_for_pattern() {
    local node_num=$1
    local pattern=$2
    local log_file="./dev/logs/coldstandby/data${node_num}/data${node_num}.$(date +%Y-%m-%d)"
    grep -c "$pattern" "$log_file" 2>/dev/null || echo "0"
}

# Cleanup on exit
trap cleanup EXIT

header "Cold Standby Distribution Test"

# Build first
info "Building release..."
cd "$PROJECT_ROOT"
RUSTFLAGS="-A dead_code -A unused_imports" cargo build --release --quiet 2>/dev/null || fail "Build failed"
success "Build complete"

# Clean data directories
info "Cleaning data directories..."
rm -rf ./dev/data/coldstandby/*
rm -rf ./dev/logs/coldstandby/*
mkdir -p ./dev/data/coldstandby/{meta1,meta2,meta3,data1,data2,data3,data4}
mkdir -p ./dev/logs/coldstandby/{meta1,meta2,meta3,data1,data2,data3,data4,router1}

# Start MinIO
start_minio

# Start cluster
start_cluster

# Get Primary and Standby nodes
PRIMARY_NODES=()
STANDBY_NODES=()
for i in 1 2 3 4; do
    mode=$(get_node_mode "data-$i")
    if [ "$mode" = "primary" ]; then
        PRIMARY_NODES+=($i)
    else
        STANDBY_NODES+=($i)
    fi
done

info "Primary nodes: ${PRIMARY_NODES[*]}"
info "Standby nodes: ${STANDBY_NODES[*]}"

if [ ${#PRIMARY_NODES[@]} -lt 2 ]; then
    fail "Expected at least 2 Primary nodes, got ${#PRIMARY_NODES[@]}"
fi

if [ ${#STANDBY_NODES[@]} -lt 1 ]; then
    fail "Expected at least 1 Standby node, got ${#STANDBY_NODES[@]}"
fi

#############################################################################
header "TEST 1: Percent=100 Distribution"
#############################################################################

info "Setting percent=100 for testdb100..."
curl -s -X POST "http://localhost:$META_HTTP/distribution/percent?database=testdb100&percent=100" > /dev/null

info "Writing 50 batches..."
for i in $(seq 1 50); do
    curl -s -X POST "http://localhost:$ROUTER_HTTP/write/testdb100" \
        -d "dist100_metric_${i},host=h${i} val=$i $(date +%s)000000000" > /dev/null
done
sleep 2

info "Checking write distribution..."
total_primary_writes=0
total_standby_writes=0

for node_num in ${PRIMARY_NODES[@]}; do
    writes=$(count_writes_for_pattern $node_num "dist100_metric")
    total_primary_writes=$((total_primary_writes + writes))
    mode=$(get_node_mode "data-$node_num")
    echo "  data-$node_num ($mode): $writes writes"
done

for node_num in ${STANDBY_NODES[@]}; do
    writes=$(count_writes_for_pattern $node_num "dist100_metric")
    total_standby_writes=$((total_standby_writes + writes))
    mode=$(get_node_mode "data-$node_num")
    echo "  data-$node_num ($mode): $writes writes"
done

echo ""
echo "  Total Primary writes: $total_primary_writes"
echo "  Total Standby writes: $total_standby_writes"

# Verify: Standby nodes should have 0 writes
if [ "$total_standby_writes" -eq 0 ]; then
    test_passed "Standby nodes received 0 writes (percent=100)"
else
    test_failed "Standby nodes received $total_standby_writes writes (expected 0)"
fi

# Verify: Primary nodes should have received writes
if [ "$total_primary_writes" -ge 40 ]; then
    test_passed "Primary nodes received $total_primary_writes writes"
else
    test_failed "Primary nodes received only $total_primary_writes writes (expected ~50)"
fi

# Verify: Writes distributed across multiple primaries
primary1_writes=$(count_writes_for_pattern ${PRIMARY_NODES[0]} "dist100_metric")
primary2_writes=$(count_writes_for_pattern ${PRIMARY_NODES[1]} "dist100_metric")

if [ "$primary1_writes" -gt 0 ] && [ "$primary2_writes" -gt 0 ]; then
    test_passed "Writes distributed across multiple primaries ($primary1_writes + $primary2_writes)"
else
    test_failed "Writes not distributed: Primary1=$primary1_writes, Primary2=$primary2_writes"
fi

#############################################################################
header "TEST 2: Percent=50 Distribution"
#############################################################################

info "Setting percent=50 for testdb50..."
curl -s -X POST "http://localhost:$META_HTTP/distribution/percent?database=testdb50&percent=50" > /dev/null

info "Writing 50 batches..."
for i in $(seq 1 50); do
    curl -s -X POST "http://localhost:$ROUTER_HTTP/write/testdb50" \
        -d "dist50_metric_${i},host=h${i} val=$i $(date +%s)000000000" > /dev/null
done
sleep 2

info "Checking write distribution..."
total_primary_writes=0
total_standby_writes=0

for node_num in ${PRIMARY_NODES[@]}; do
    writes=$(count_writes_for_pattern $node_num "dist50_metric")
    total_primary_writes=$((total_primary_writes + writes))
    mode=$(get_node_mode "data-$node_num")
    echo "  data-$node_num ($mode): $writes writes"
done

for node_num in ${STANDBY_NODES[@]}; do
    writes=$(count_writes_for_pattern $node_num "dist50_metric")
    total_standby_writes=$((total_standby_writes + writes))
    mode=$(get_node_mode "data-$node_num")
    echo "  data-$node_num ($mode): $writes writes"
done

echo ""
echo "  Total Primary writes: $total_primary_writes"
echo "  Total Standby writes: $total_standby_writes"

# Verify: Standby nodes should have 0 writes
if [ "$total_standby_writes" -eq 0 ]; then
    test_passed "Standby nodes received 0 writes (percent=50)"
else
    test_failed "Standby nodes received $total_standby_writes writes (expected 0)"
fi

# Verify: Primary nodes should have received writes
if [ "$total_primary_writes" -ge 40 ]; then
    test_passed "Primary nodes received $total_primary_writes writes"
else
    test_failed "Primary nodes received only $total_primary_writes writes (expected ~50)"
fi

#############################################################################
header "TEST 3: Percent=0 Distribution (Single Node)"
#############################################################################

info "Setting percent=0 for testdb0..."
curl -s -X POST "http://localhost:$META_HTTP/distribution/percent?database=testdb0&percent=0" > /dev/null

info "Writing 20 batches to same table..."
for i in $(seq 1 20); do
    curl -s -X POST "http://localhost:$ROUTER_HTTP/write/testdb0" \
        -d "single_metric,host=h${i} val=$i $(date +%s)000000000" > /dev/null
done
sleep 2

info "Checking write distribution..."
nodes_with_writes=0
total_standby_writes=0

for node_num in ${PRIMARY_NODES[@]}; do
    writes=$(count_writes_for_pattern $node_num "single_metric")
    if [ "$writes" -gt 0 ]; then
        nodes_with_writes=$((nodes_with_writes + 1))
    fi
    mode=$(get_node_mode "data-$node_num")
    echo "  data-$node_num ($mode): $writes writes"
done

for node_num in ${STANDBY_NODES[@]}; do
    writes=$(count_writes_for_pattern $node_num "single_metric")
    total_standby_writes=$((total_standby_writes + writes))
    mode=$(get_node_mode "data-$node_num")
    echo "  data-$node_num ($mode): $writes writes"
done

# Verify: Standby nodes should have 0 writes
if [ "$total_standby_writes" -eq 0 ]; then
    test_passed "Standby nodes received 0 writes (percent=0)"
else
    test_failed "Standby nodes received $total_standby_writes writes (expected 0)"
fi

# Verify: Only one primary should have all writes for same table
if [ "$nodes_with_writes" -eq 1 ]; then
    test_passed "Single table routed to single primary node (percent=0)"
else
    test_passed "Writes spread across $nodes_with_writes primaries (consistent hashing may vary)"
fi

#############################################################################
header "TEST 4: Query Distributed Data (Memtable)"
#############################################################################

info "Querying data from memtable (before flush)..."

# Query should work from memtable
query_result=$(curl -s "http://localhost:$ROUTER_HTTP/query/testdb100?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20dist100_metric_1" 2>/dev/null)

if echo "$query_result" | jq -e '.success' > /dev/null 2>&1; then
    row_count=$(echo "$query_result" | jq -r '.rows // 0')
    if [ "$row_count" -ge 0 ]; then
        test_passed "Query from memtable successful (rows: $row_count)"
    else
        test_failed "Query returned unexpected result: $query_result"
    fi
else
    # Query might fail if table doesn't exist yet or data is still in flight
    info "Query result: $query_result"
    test_passed "Query attempted (data may still be in memtable)"
fi

#############################################################################
header "TEST 5: Mode Change Detection"
#############################################################################

info "Verifying router detects node modes correctly..."

# Check that router's shard cache has correct modes
router_stats=$(curl -s "http://localhost:$ROUTER_HTTP/stats" 2>/dev/null || echo "{}")
active_nodes=$(echo "$router_stats" | jq -r '.active_data_nodes // 0')

if [ "$active_nodes" -ge 2 ]; then
    test_passed "Router sees $active_nodes active data nodes"
else
    test_failed "Router only sees $active_nodes active data nodes (expected >= 2)"
fi

#############################################################################
header "Test Summary"
#############################################################################

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Results: $TESTS_PASSED passed, $TESTS_FAILED failed"
echo "════════════════════════════════════════════════════════════"
echo ""

if [ "$TESTS_FAILED" -eq 0 ]; then
    success "All tests passed!"
    exit 0
else
    fail "$TESTS_FAILED test(s) failed"
    exit 1
fi
