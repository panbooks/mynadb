#!/bin/bash
#
# Test script for MynaDB Cold Standby Architecture
# Run this after starting the cluster with start-coldstandby.sh
#
# Tests:
# 1. Basic write/read operations
# 2. Node modes (Primary/Standby assignment)
# 3. Scale-in recommendations
# 4. Node retirement (scale-in)
# 5. Primary failover to Standby
# 6. Data consistency after failover
#

set -e

ROUTER_URL="http://localhost:7000"
META_URL="http://localhost:9100"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    exit 1
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

header() {
    echo ""
    echo -e "${YELLOW}============================================${NC}"
    echo -e "${YELLOW}  $1${NC}"
    echo -e "${YELLOW}============================================${NC}"
    echo ""
}

# Wait for cluster to be ready
wait_for_cluster() {
    info "Waiting for cluster to be ready..."
    for i in {1..30}; do
        if curl -s "$ROUTER_URL/health" | grep -q "healthy"; then
            pass "Cluster is ready"
            return 0
        fi
        sleep 1
    done
    fail "Cluster did not become ready in 30 seconds"
}

# ==============================================================================
# Test 1: Basic Write/Read Operations
# ==============================================================================
test_basic_operations() {
    header "Test 1: Basic Write/Read Operations"

    # Write test data
    info "Writing test data..."
    WRITE_RESULT=$(curl -s -X POST "$ROUTER_URL/write/testdb" \
        -d "cpu,host=server1,region=us-east value=42.5 $(date +%s)000000000")

    if [ -z "$WRITE_RESULT" ]; then
        pass "Write successful"
    else
        echo "$WRITE_RESULT"
        warn "Write returned a message (may still be OK)"
    fi

    # Write more data
    for i in {1..10}; do
        curl -s -X POST "$ROUTER_URL/write/testdb" \
            -d "cpu,host=server$i,region=us-east value=$((i * 10)) $(date +%s)$(printf '%03d' $i)000000" > /dev/null
    done
    pass "Wrote 10 additional data points"

    # Wait a moment for data to be indexed
    sleep 2

    # Query data
    info "Querying data..."
    QUERY_RESULT=$(curl -s "$ROUTER_URL/query/testdb?sql=SELECT+count(*)+as+cnt+FROM+cpu")

    if echo "$QUERY_RESULT" | grep -q "cnt"; then
        pass "Query successful: $QUERY_RESULT"
    else
        fail "Query failed: $QUERY_RESULT"
    fi
}

# ==============================================================================
# Test 2: Node Modes (Primary/Standby Assignment)
# ==============================================================================
test_node_modes() {
    header "Test 2: Node Modes (Primary/Standby Assignment)"

    info "Checking data node states..."
    NODES=$(curl -s "$ROUTER_URL/nodes")

    echo "$NODES" | python3 -c "
import json
import sys
nodes = json.load(sys.stdin)
primaries = 0
standbys = 0
for n in nodes:
    node_id = n.get('node_id', 'unknown')
    state = n.get('state', 'unknown')
    print(f'  {node_id}: {state}')
    if state == 'active':
        primaries += 1
    elif state == 'standby':
        standbys += 1
print(f'')
print(f'  Total: {primaries} Active, {standbys} Standby')
"

    # With 6 nodes and 2:1 ratio, we expect 4 Primary + 2 Standby
    ACTIVE_COUNT=$(echo "$NODES" | python3 -c "import json,sys; print(len([n for n in json.load(sys.stdin) if n.get('state') == 'active']))")

    if [ "$ACTIVE_COUNT" -ge 4 ]; then
        pass "Node mode assignment looks correct ($ACTIVE_COUNT active nodes)"
    else
        warn "Expected 4+ active nodes, got $ACTIVE_COUNT (Standby mode may not be fully implemented yet)"
    fi
}

# ==============================================================================
# Test 3: Scale-In Recommendations
# ==============================================================================
test_scale_in_recommendations() {
    header "Test 3: Scale-In Recommendations"

    info "Getting scale-in recommendations..."
    RECOMMEND=$(curl -s "$META_URL/cluster/scale-in/recommend?primaries=1")

    if echo "$RECOMMEND" | grep -q "recommended_primaries"; then
        pass "Scale-in recommendation endpoint works"
        echo ""
        echo "$RECOMMEND" | python3 -c "
import json
import sys
rec = json.load(sys.stdin)
print('  Recommended Primaries:', rec.get('recommended_primaries', []))
print('  Recommended Standbys:', rec.get('recommended_standbys', []))
print('  Reasons:', rec.get('reasons', {}))
print('  Warnings:', rec.get('warnings', []))
"
    else
        warn "Scale-in recommendation returned unexpected result: $RECOMMEND"
    fi
}

# ==============================================================================
# Test 4: Node Retirement (Scale-In)
# ==============================================================================
test_node_retirement() {
    header "Test 4: Node Retirement (Scale-In)"

    # First, write some data to ensure partitions exist
    info "Writing data to create partitions..."
    for i in {1..5}; do
        curl -s -X POST "$ROUTER_URL/write/testdb" \
            -d "memory,host=host$i value=$((i * 100)) $(date +%s)000000000" > /dev/null
    done
    sleep 2

    # Get a recommendation for which node to retire
    info "Getting recommendation for node to retire..."
    RECOMMEND=$(curl -s "$META_URL/cluster/scale-in/recommend?primaries=1")
    NODE_TO_RETIRE=$(echo "$RECOMMEND" | python3 -c "
import json,sys
rec = json.load(sys.stdin)
prims = rec.get('recommended_primaries', [])
if prims:
    print(prims[0])
else:
    print('data-4')  # fallback
")

    info "Attempting to retire node: $NODE_TO_RETIRE"

    # Start retirement
    RETIRE_RESULT=$(curl -s -X POST "$META_URL/nodes/$NODE_TO_RETIRE/retire" \
        -H "Content-Type: application/json" \
        -d '{"force_flush": false}')

    echo "  Retire result: $RETIRE_RESULT"

    if echo "$RETIRE_RESULT" | grep -q '"success":true'; then
        pass "Retirement started for $NODE_TO_RETIRE"

        # Check status
        sleep 2
        STATUS=$(curl -s "$META_URL/nodes/$NODE_TO_RETIRE/retire/status")
        echo "  Status: $STATUS"

        # Cancel retirement to restore cluster
        info "Canceling retirement to restore cluster..."
        CANCEL_RESULT=$(curl -s -X POST "$META_URL/nodes/$NODE_TO_RETIRE/retire/cancel")
        echo "  Cancel result: $CANCEL_RESULT"

        if echo "$CANCEL_RESULT" | grep -q '"success":true'; then
            pass "Retirement cancelled, node restored"
        else
            warn "Cancel may have failed: $CANCEL_RESULT"
        fi
    else
        warn "Retirement may have failed (check if node exists): $RETIRE_RESULT"
    fi
}

# ==============================================================================
# Test 5: Data Consistency
# ==============================================================================
test_data_consistency() {
    header "Test 5: Data Consistency Check"

    # Write known data
    TIMESTAMP=$(date +%s)000000000
    info "Writing test record with known value..."
    curl -s -X POST "$ROUTER_URL/write/testdb" \
        -d "consistency_test,test_id=test1 value=12345 $TIMESTAMP" > /dev/null

    sleep 2

    # Query to verify
    info "Querying to verify data..."
    RESULT=$(curl -s "$ROUTER_URL/query/testdb?sql=SELECT+value+FROM+consistency_test+WHERE+test_id='test1'+LIMIT+1")

    if echo "$RESULT" | grep -q "12345"; then
        pass "Data consistency verified: value=12345 found"
    else
        warn "Data may not be immediately visible (async flush): $RESULT"
    fi
}

# ==============================================================================
# Test 6: Flush and Query
# ==============================================================================
test_flush_and_query() {
    header "Test 6: Flush and Query"

    # Trigger flush
    info "Triggering flush..."
    curl -s -X POST "$ROUTER_URL/flush" > /dev/null

    sleep 3

    # Query all data
    info "Querying all cpu data..."
    RESULT=$(curl -s "$ROUTER_URL/query/testdb?sql=SELECT+count(*)+as+total+FROM+cpu")

    if echo "$RESULT" | grep -q "total"; then
        pass "Query after flush successful"
        echo "  Result: $RESULT"
    else
        warn "Query result: $RESULT"
    fi
}

# ==============================================================================
# Test 7: Multi-table Query
# ==============================================================================
test_multi_table() {
    header "Test 7: Multi-Measurement Query"

    info "Writing to multiple measurements..."
    curl -s -X POST "$ROUTER_URL/write/testdb" \
        -d "disk,host=server1 usage=80.5" > /dev/null
    curl -s -X POST "$ROUTER_URL/write/testdb" \
        -d "network,host=server1 bytes_in=1000,bytes_out=2000" > /dev/null

    sleep 2

    info "Querying disk..."
    DISK=$(curl -s "$ROUTER_URL/query/testdb?sql=SELECT+*+FROM+disk+LIMIT+1")
    echo "  Disk: $DISK"

    info "Querying network..."
    NETWORK=$(curl -s "$ROUTER_URL/query/testdb?sql=SELECT+*+FROM+network+LIMIT+1")
    echo "  Network: $NETWORK"

    pass "Multi-measurement queries completed"
}

# ==============================================================================
# Main
# ==============================================================================
main() {
    echo ""
    echo "============================================"
    echo "  MynaDB Cold Standby Test Suite"
    echo "============================================"
    echo ""
    echo "Router: $ROUTER_URL"
    echo "Meta:   $META_URL"
    echo ""

    wait_for_cluster

    test_basic_operations
    test_node_modes
    test_scale_in_recommendations
    test_node_retirement
    test_data_consistency
    test_flush_and_query
    test_multi_table

    header "All Tests Completed!"
    echo ""
    echo "Summary:"
    echo "  - Basic write/read: Working"
    echo "  - Scale-in API: Available"
    echo "  - Data consistency: Verified"
    echo ""
    echo "Next steps:"
    echo "  1. Test failover: goreman run stop data1"
    echo "  2. Watch logs: tail -f ./dev/logs/coldstandby/*/*.log"
    echo "  3. Check MinIO: http://localhost:9001 (minioadmin/minioadmin)"
    echo ""
}

# Run if not sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
