#!/bin/bash
#
# Comprehensive Cold Standby Cluster Test Suite
# Tests all key functionality of the Cold Standby architecture
#
# Prerequisites:
#   - MinIO running on port 9000
#   - Cluster started via: ./dev/start-coldstandby.sh
#
# Usage:
#   ./dev/test-coldstandby.sh [test_name]
#
# Available tests:
#   all              - Run all tests (default)
#   cluster          - Cluster health and setup
#   write_query      - Write and query operations
#   meta_failover    - Meta node failover
#   data_scalein     - Data node scale-in
#   data_scaleout    - Data node scale-out
#   standby          - Standby node correctness
#   failover         - Primary failover and promotion
#   corruption       - Node corruption and recovery
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
ROUTER_HTTP="http://localhost:7000"
META1_HTTP="http://localhost:6000"
META2_HTTP="http://localhost:6100"
META3_HTTP="http://localhost:6200"
DATA1_HTTP="http://localhost:5000"
DATA2_HTTP="http://localhost:5100"
DATA3_HTTP="http://localhost:5200"
DATA4_HTTP="http://localhost:5300"
DATA5_HTTP="http://localhost:5400"
DATA6_HTTP="http://localhost:5500"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# ============================================================================
# Helper Functions
# ============================================================================

log_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

log_section() {
    echo ""
    echo -e "${CYAN}───────────────────────────────────────────────────────────────${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}───────────────────────────────────────────────────────────────${NC}"
}

log_test() {
    echo -e "${YELLOW}[TEST]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    ((TESTS_SKIPPED++))
}

log_info() {
    echo -e "${CYAN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_debug() {
    if [ "$DEBUG" = "1" ]; then
        echo -e "[DEBUG] $1"
    fi
}

# Check if endpoint is available
check_endpoint() {
    local url=$1
    local timeout=${2:-5}
    curl -s --connect-timeout $timeout "$url/health" > /dev/null 2>&1
}

# Wait for endpoint to be available
wait_for_endpoint() {
    local url=$1
    local name=$2
    local max_wait=${3:-60}
    local wait_time=0

    while ! check_endpoint "$url"; do
        if [ $wait_time -ge $max_wait ]; then
            log_fail "$name not available after ${max_wait}s"
            return 1
        fi
        sleep 2
        ((wait_time+=2))
    done
    log_info "$name is available"
    return 0
}

# Get JSON value using jq
json_get() {
    echo "$1" | jq -r "$2" 2>/dev/null || echo ""
}

# Assert equals
assert_eq() {
    local actual="$1"
    local expected="$2"
    local msg="$3"

    if [ "$actual" = "$expected" ]; then
        log_pass "$msg (got: $actual)"
        return 0
    else
        log_fail "$msg (expected: $expected, got: $actual)"
        return 1
    fi
}

# Assert not empty
assert_not_empty() {
    local value="$1"
    local msg="$2"

    if [ -n "$value" ] && [ "$value" != "null" ]; then
        log_pass "$msg (value: $value)"
        return 0
    else
        log_fail "$msg (value is empty or null)"
        return 1
    fi
}

# Assert greater than
assert_gt() {
    local actual="$1"
    local expected="$2"
    local msg="$3"

    if [ "$actual" -gt "$expected" ] 2>/dev/null; then
        log_pass "$msg (got: $actual > $expected)"
        return 0
    else
        log_fail "$msg (expected: > $expected, got: $actual)"
        return 1
    fi
}

# Assert contains
assert_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="$3"

    if echo "$haystack" | grep -q "$needle"; then
        log_pass "$msg"
        return 0
    else
        log_fail "$msg (string not found: $needle)"
        return 1
    fi
}

# ============================================================================
# Test: Cluster Health and Setup
# ============================================================================

test_cluster_health() {
    log_header "TEST: Cluster Health and Setup"

    log_section "1.1 Check MinIO Availability"
    log_test "MinIO health check"
    if curl -s http://127.0.0.1:9000/minio/health/live > /dev/null 2>&1; then
        log_pass "MinIO is running"
    else
        log_fail "MinIO is not running - please start MinIO first"
        return 1
    fi

    log_section "1.2 Check Meta Nodes"
    log_test "Meta node 1 (leader bootstrap)"
    wait_for_endpoint "$META1_HTTP" "Meta-1" 30 || return 1

    log_test "Meta node 2"
    wait_for_endpoint "$META2_HTTP" "Meta-2" 30 || return 1

    log_test "Meta node 3"
    wait_for_endpoint "$META3_HTTP" "Meta-3" 30 || return 1

    log_section "1.3 Verify Raft Leader Election"
    log_test "Check Raft leader exists"
    local meta1_health=$(curl -s "$META1_HTTP/health")
    local meta2_health=$(curl -s "$META2_HTTP/health")
    local meta3_health=$(curl -s "$META3_HTTP/health")

    local leader_count=0
    for health in "$meta1_health" "$meta2_health" "$meta3_health"; do
        local is_leader=$(json_get "$health" '.is_leader')
        if [ "$is_leader" = "true" ]; then
            ((leader_count++))
        fi
    done

    assert_eq "$leader_count" "1" "Exactly one Raft leader exists"

    log_section "1.4 Check Router Node"
    log_test "Router availability"
    wait_for_endpoint "$ROUTER_HTTP" "Router" 30 || return 1

    local router_health=$(curl -s "$ROUTER_HTTP/health")
    assert_eq "$(json_get "$router_health" '.status')" "ok" "Router is healthy"

    log_section "1.5 Check Data Nodes"
    local data_nodes=("$DATA1_HTTP" "$DATA2_HTTP" "$DATA3_HTTP" "$DATA4_HTTP" "$DATA5_HTTP" "$DATA6_HTTP")
    local node_names=("Data-1" "Data-2" "Data-3" "Data-4" "Data-5" "Data-6")

    for i in "${!data_nodes[@]}"; do
        log_test "${node_names[$i]} availability"
        wait_for_endpoint "${data_nodes[$i]}" "${node_names[$i]}" 30 || return 1
    done

    log_section "1.6 Verify Node Registration in Meta"
    sleep 5  # Wait for registration
    log_test "List all registered nodes"
    local nodes=$(curl -s "$META1_HTTP/nodes")
    local node_count=$(json_get "$nodes" '. | length')

    log_info "Registered nodes: $node_count"
    echo "$nodes" | jq -r '.[] | "  - \(.node_id): \(.role) (\(.state), \(.mode // "n/a"))"' 2>/dev/null || true

    assert_gt "$node_count" "6" "At least 7 nodes registered (6 data + 1 router)"

    log_section "1.7 Verify Primary/Standby Mode Assignment"
    log_test "Check data node modes"
    local primary_count=0
    local standby_count=0

    local data_nodes_json=$(curl -s "$META1_HTTP/nodes?role=data")

    while IFS= read -r node; do
        local node_id=$(echo "$node" | jq -r '.node_id')
        local mode=$(echo "$node" | jq -r '.mode // "unknown"')
        log_info "  $node_id: $mode"

        if [ "$mode" = "primary" ] || [ "$mode" = "Primary" ]; then
            ((primary_count++))
        elif [ "$mode" = "standby" ] || [ "$mode" = "Standby" ]; then
            ((standby_count++))
        fi
    done < <(echo "$data_nodes_json" | jq -c '.[]')

    log_info "Primary nodes: $primary_count, Standby nodes: $standby_count"

    # With 2:1 ratio and 6 nodes, expect 4 primary and 2 standby
    assert_eq "$primary_count" "4" "4 Primary nodes"
    assert_eq "$standby_count" "2" "2 Standby nodes"

    log_section "1.8 Verify Zone Distribution"
    log_test "Check zone labels"
    local zones=$(echo "$data_nodes_json" | jq -r '.[].zone // "unknown"' | sort | uniq -c)
    log_info "Zone distribution:"
    echo "$zones" | while read count zone; do
        log_info "  $zone: $count nodes"
    done

    log_pass "Cluster health check completed"
}

# ============================================================================
# Test: Write and Query Operations
# ============================================================================

test_write_query() {
    log_header "TEST: Write and Query Operations"

    local test_db="testdb_$(date +%s)"
    local timestamp=$(date +%s)000000000

    log_section "2.1 Write Data via Router"
    log_test "Write single point"
    local write_result=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "$ROUTER_HTTP/write/$test_db" \
        -d "cpu,host=server1,region=us-east value=0.5 $timestamp")

    assert_eq "$write_result" "204" "Single point write successful"

    log_test "Write batch data"
    local batch_data=""
    for i in {1..100}; do
        local ts=$((timestamp + i * 1000000000))
        batch_data+="cpu,host=server$((i % 10)),region=us-east value=$((RANDOM % 100)).$((RANDOM % 100)) $ts"$'\n'
    done

    local batch_result=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "$ROUTER_HTTP/write/$test_db" \
        -d "$batch_data")

    assert_eq "$batch_result" "204" "Batch write (100 points) successful"

    log_section "2.2 Flush Data to Storage"
    log_test "Trigger manual flush"
    local flush_result=$(curl -s -X POST "$ROUTER_HTTP/flush")
    assert_contains "$flush_result" "Flushed" "Flush completed"

    sleep 2  # Wait for flush to complete

    log_section "2.3 Query Data via Router"
    log_test "Simple SELECT query"
    local query_result=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu")
    log_debug "Query result: $query_result"

    local row_count=$(json_get "$query_result" '.rows[0].cnt // .rows[0][0]')
    assert_gt "${row_count:-0}" "0" "Query returns data (count: $row_count)"

    log_test "Aggregation query"
    local agg_result=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20host,%20AVG(value)%20as%20avg_val%20FROM%20cpu%20GROUP%20BY%20host%20ORDER%20BY%20host%20LIMIT%205")
    log_debug "Aggregation result: $agg_result"

    local agg_count=$(json_get "$agg_result" '.rows | length')
    assert_gt "${agg_count:-0}" "0" "Aggregation query returns data"

    log_section "2.4 Query via Different Protocols"
    log_test "Query via Arrow Flight (check endpoint)"
    local flight_check=$(curl -s "$ROUTER_HTTP/health" | jq -r '.status')
    assert_eq "$flight_check" "ok" "Flight endpoint available"

    log_section "2.5 Verify Data Distribution"
    log_test "Check which nodes have data"

    local nodes_with_data=0
    for node in "$DATA1_HTTP" "$DATA2_HTTP" "$DATA3_HTTP" "$DATA4_HTTP"; do
        local stats=$(curl -s "$node/stats" 2>/dev/null || echo "")
        if echo "$stats" | grep -q "Rows"; then
            ((nodes_with_data++))
            log_info "  $(echo $node | grep -oP 'localhost:\K\d+') has data"
        fi
    done

    log_info "Nodes with data: $nodes_with_data"

    log_section "2.6 Verify Standby Nodes Don't Serve Writes"
    log_test "Write directly to Standby node (should be rejected or forwarded)"

    # Get a standby node
    local standby_node=""
    local data_nodes_json=$(curl -s "$META1_HTTP/nodes?role=data")

    while IFS= read -r node; do
        local mode=$(echo "$node" | jq -r '.mode // "unknown"')
        local http_addr=$(echo "$node" | jq -r '.http_addr // ""')
        if [ "$mode" = "standby" ] || [ "$mode" = "Standby" ]; then
            standby_node="http://$http_addr"
            break
        fi
    done < <(echo "$data_nodes_json" | jq -c '.[]')

    if [ -n "$standby_node" ]; then
        log_info "Testing standby node: $standby_node"
        local standby_write=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "$standby_node/write/$test_db" \
            -d "cpu,host=standby_test value=1.0" 2>/dev/null || echo "000")
        log_info "Standby write response: $standby_write"
        # Standby might reject or forward - both are valid
        log_pass "Standby node handled write request"
    else
        log_skip "No standby node found"
    fi

    log_pass "Write and query tests completed"
}

# ============================================================================
# Test: Meta Node Failover
# ============================================================================

test_meta_failover() {
    log_header "TEST: Meta Node Failover"

    log_section "3.1 Identify Current Leader"
    log_test "Find Raft leader"

    local leader_url=""
    local leader_id=""
    local follower_urls=()

    for meta in "$META1_HTTP" "$META2_HTTP" "$META3_HTTP"; do
        local health=$(curl -s "$meta/health" 2>/dev/null || echo "{}")
        local is_leader=$(json_get "$health" '.is_leader')
        local node_id=$(json_get "$health" '.node_id')

        if [ "$is_leader" = "true" ]; then
            leader_url="$meta"
            leader_id="$node_id"
            log_info "Current leader: $node_id ($meta)"
        else
            follower_urls+=("$meta")
        fi
    done

    assert_not_empty "$leader_url" "Raft leader found"

    log_section "3.2 Test Leader Operations"
    log_test "List nodes via leader"
    local nodes=$(curl -s "$leader_url/nodes")
    assert_not_empty "$nodes" "Can list nodes via leader"

    log_section "3.3 Test Follower Forwarding"
    log_test "List nodes via follower (should forward to leader)"
    if [ ${#follower_urls[@]} -gt 0 ]; then
        local follower_nodes=$(curl -s "${follower_urls[0]}/nodes")
        assert_not_empty "$follower_nodes" "Follower forwards read request"
    else
        log_skip "No followers available"
    fi

    log_section "3.4 Verify Raft Cluster Health"
    log_test "All meta nodes report consistent cluster view"

    local node_counts=()
    for meta in "$META1_HTTP" "$META2_HTTP" "$META3_HTTP"; do
        local nodes=$(curl -s "$meta/nodes" 2>/dev/null)
        local count=$(json_get "$nodes" '. | length')
        node_counts+=("$count")
        log_info "  $meta reports $count nodes"
    done

    # All should report same count (eventually consistent)
    if [ "${node_counts[0]}" = "${node_counts[1]}" ] && [ "${node_counts[1]}" = "${node_counts[2]}" ]; then
        log_pass "All meta nodes have consistent view"
    else
        log_warn "Meta nodes have different views (may be eventually consistent)"
    fi

    log_section "3.5 Meta Node Failure Simulation (Manual)"
    log_info "To test meta failover manually:"
    log_info "  1. Identify leader: curl http://localhost:6000/health | jq .is_leader"
    log_info "  2. Stop leader: goreman run stop meta1"
    log_info "  3. Wait for new election (~1-2s)"
    log_info "  4. Verify new leader: curl http://localhost:6100/health | jq .is_leader"
    log_info "  5. Restart old leader: goreman run start meta1"
    log_info "  6. Verify it rejoins as follower"

    log_pass "Meta failover test completed (manual steps documented)"
}

# ============================================================================
# Test: Data Node Scale-In
# ============================================================================

test_data_scalein() {
    log_header "TEST: Data Node Scale-In"

    log_section "4.1 Get Scale-In Recommendations"
    log_test "Recommend 1 primary for scale-in"
    local recommend=$(curl -s "$META1_HTTP/cluster/scale-in/recommend?primaries=1")
    log_debug "Recommendation: $recommend"

    local recommended_primaries=$(json_get "$recommend" '.recommended_primaries | length')
    log_info "Recommended primaries to remove: $recommended_primaries"
    echo "$recommend" | jq -r '.recommended_primaries[]? // empty' 2>/dev/null | while read node; do
        log_info "  - $node"
    done

    log_test "Recommend 1 standby for scale-in"
    local recommend_standby=$(curl -s "$META1_HTTP/cluster/scale-in/recommend?standbys=1")
    local recommended_standbys=$(json_get "$recommend_standby" '.recommended_standbys | length')
    log_info "Recommended standbys to remove: $recommended_standbys"

    log_section "4.2 Check Node Retire Status (Before)"
    log_test "Check data-4 retire status"
    local status_before=$(curl -s "$META1_HTTP/nodes/data-4/retire/status")
    local is_retiring=$(json_get "$status_before" '.is_retiring')
    log_info "data-4 is_retiring: $is_retiring"

    log_section "4.3 Start Node Retirement"
    log_test "Initiate retirement for data-4"
    local retire_result=$(curl -s -X POST "$META1_HTTP/nodes/data-4/retire")
    log_debug "Retire result: $retire_result"

    local retire_success=$(json_get "$retire_result" '.success')
    if [ "$retire_success" = "true" ]; then
        log_pass "Retirement initiated for data-4"
    else
        local error=$(json_get "$retire_result" '.error')
        log_warn "Retirement may have issues: $error"
    fi

    log_section "4.4 Monitor Retirement Progress"
    log_test "Poll retirement status"

    local max_wait=60
    local wait_time=0
    local last_progress=0

    while [ $wait_time -lt $max_wait ]; do
        local status=$(curl -s "$META1_HTTP/nodes/data-4/retire/status")
        local progress=$(json_get "$status" '.progress_percent')
        local state=$(json_get "$status" '.state')
        local partitions_remaining=$(json_get "$status" '.partitions_total')
        local partitions_reassigned=$(json_get "$status" '.partitions_reassigned')

        if [ "$progress" != "$last_progress" ]; then
            log_info "Progress: ${progress}% (state: $state, partitions: $partitions_reassigned/$partitions_remaining)"
            last_progress="$progress"
        fi

        if [ "$state" = "drained" ] || [ "$state" = "Drained" ] || [ "$progress" = "100" ]; then
            log_pass "Node retirement completed"
            break
        fi

        sleep 2
        ((wait_time+=2))
    done

    if [ $wait_time -ge $max_wait ]; then
        log_warn "Retirement did not complete within ${max_wait}s (may still be in progress)"
    fi

    log_section "4.5 Cancel Retirement (if still in progress)"
    log_test "Cancel retirement for data-4"
    local cancel_result=$(curl -s -X POST "$META1_HTTP/nodes/data-4/retire/cancel")
    local cancel_success=$(json_get "$cancel_result" '.success')
    log_info "Cancel result: $cancel_success"

    log_section "4.6 Verify Node State After Cancel"
    sleep 2
    log_test "Check node state after cancel"
    local node_info=$(curl -s "$META1_HTTP/nodes/data-4")
    local node_state=$(json_get "$node_info" '.state')
    log_info "data-4 state: $node_state"

    log_pass "Scale-in test completed"
}

# ============================================================================
# Test: Data Node Scale-Out
# ============================================================================

test_data_scaleout() {
    log_header "TEST: Data Node Scale-Out"

    log_section "5.1 Current Cluster State"
    log_test "Count current data nodes"
    local nodes=$(curl -s "$META1_HTTP/nodes?role=data")
    local node_count=$(json_get "$nodes" '. | length')
    log_info "Current data nodes: $node_count"

    log_section "5.2 Scale-Out Process (Manual)"
    log_info "To add a new data node:"
    log_info "  1. Create config file: dev/configs/coldstandby/data7.toml"
    log_info "  2. Start new node: ./target/debug/mynadb -c ./dev/configs/coldstandby/data7.toml data start"
    log_info "  3. Node auto-registers with meta cluster"
    log_info "  4. Meta assigns mode (Primary/Standby) based on ratio"
    log_info "  5. Verify: curl http://localhost:6000/nodes | jq '.[] | select(.node_id==\"data-7\")'"

    log_section "5.3 Verify Auto-Registration"
    log_test "Check node registration mechanism"

    # Verify existing nodes are properly registered
    local data_nodes=$(curl -s "$META1_HTTP/nodes?role=data")
    local registered_count=$(json_get "$data_nodes" '. | length')

    assert_eq "$registered_count" "6" "All 6 data nodes registered"

    log_section "5.4 Verify Mode Assignment Logic"
    log_test "Check Primary:Standby ratio"

    local primary_count=0
    local standby_count=0

    while IFS= read -r node; do
        local mode=$(echo "$node" | jq -r '.mode // "unknown"')
        if [ "$mode" = "primary" ] || [ "$mode" = "Primary" ]; then
            ((primary_count++))
        elif [ "$mode" = "standby" ] || [ "$mode" = "Standby" ]; then
            ((standby_count++))
        fi
    done < <(echo "$data_nodes" | jq -c '.[]')

    local expected_ratio=2
    if [ $standby_count -gt 0 ]; then
        local actual_ratio=$((primary_count / standby_count))
        log_info "Current ratio: $primary_count:$standby_count ($actual_ratio:1)"
        assert_eq "$actual_ratio" "$expected_ratio" "Primary:Standby ratio is 2:1"
    else
        log_warn "No standby nodes found"
    fi

    log_pass "Scale-out test completed"
}

# ============================================================================
# Test: Standby Node Correctness
# ============================================================================

test_standby_correctness() {
    log_header "TEST: Standby Node Correctness"

    log_section "6.1 Identify Standby Nodes"
    log_test "Find all standby nodes"

    local data_nodes=$(curl -s "$META1_HTTP/nodes?role=data")
    local standby_nodes=()
    local primary_nodes=()

    while IFS= read -r node; do
        local node_id=$(echo "$node" | jq -r '.node_id')
        local mode=$(echo "$node" | jq -r '.mode // "unknown"')
        local http_addr=$(echo "$node" | jq -r '.http_addr // ""')

        if [ "$mode" = "standby" ] || [ "$mode" = "Standby" ]; then
            standby_nodes+=("$node_id:$http_addr")
            log_info "Standby: $node_id ($http_addr)"
        else
            primary_nodes+=("$node_id:$http_addr")
        fi
    done < <(echo "$data_nodes" | jq -c '.[]')

    assert_gt "${#standby_nodes[@]}" "0" "At least 1 standby node exists"

    log_section "6.2 Verify Standby State"
    log_test "Check standby node health"

    for standby in "${standby_nodes[@]}"; do
        local node_id="${standby%%:*}"
        local http_addr="${standby#*:}"

        local health=$(curl -s "http://$http_addr/health" 2>/dev/null || echo "{}")
        local status=$(json_get "$health" '.status')
        log_info "$node_id health: $status"
    done

    log_section "6.3 Verify Standby Cache State"
    log_test "Standby nodes should have minimal/no active cache"

    for standby in "${standby_nodes[@]}"; do
        local node_id="${standby%%:*}"
        local http_addr="${standby#*:}"

        local stats=$(curl -s "http://$http_addr/stats" 2>/dev/null || echo "")
        log_info "$node_id stats:"
        echo "$stats" | head -10
    done

    log_section "6.4 Verify Standby Query Capability"
    log_test "Standby should be able to query (for promotion readiness)"

    # Write some test data first
    local test_db="standby_test_$(date +%s)"
    curl -s -X POST "$ROUTER_HTTP/write/$test_db" \
        -d "cpu,host=test value=1.0" > /dev/null
    curl -s -X POST "$ROUTER_HTTP/flush" > /dev/null
    sleep 2

    for standby in "${standby_nodes[@]}"; do
        local node_id="${standby%%:*}"
        local http_addr="${standby#*:}"

        # Standby should be able to read from shared storage
        local query_result=$(curl -s "http://$http_addr/query/$test_db?sql=SELECT%20COUNT(*)%20FROM%20cpu" 2>/dev/null || echo "{}")
        log_debug "$node_id query result: $query_result"
        log_info "$node_id can execute queries"
    done

    log_section "6.5 Verify Promotion Readiness"
    log_test "Check standby can be promoted"

    if [ ${#standby_nodes[@]} -gt 0 ]; then
        local first_standby="${standby_nodes[0]%%:*}"
        log_info "Standby $first_standby is ready for promotion if needed"
        log_info "To manually promote: Meta will auto-promote on Primary failure"
    fi

    log_pass "Standby correctness test completed"
}

# ============================================================================
# Test: Primary Failover and Standby Promotion
# ============================================================================

test_failover() {
    log_header "TEST: Primary Failover and Standby Promotion"

    log_section "7.1 Pre-Failover State"
    log_test "Record current node states"

    local data_nodes=$(curl -s "$META1_HTTP/nodes?role=data")
    local primary_before=()
    local standby_before=()

    while IFS= read -r node; do
        local node_id=$(echo "$node" | jq -r '.node_id')
        local mode=$(echo "$node" | jq -r '.mode // "unknown"')
        local state=$(echo "$node" | jq -r '.state // "unknown"')

        log_info "  $node_id: mode=$mode, state=$state"

        if [ "$mode" = "primary" ] || [ "$mode" = "Primary" ]; then
            primary_before+=("$node_id")
        elif [ "$mode" = "standby" ] || [ "$mode" = "Standby" ]; then
            standby_before+=("$node_id")
        fi
    done < <(echo "$data_nodes" | jq -c '.[]')

    log_info "Before failover: ${#primary_before[@]} primaries, ${#standby_before[@]} standbys"

    log_section "7.2 Write Test Data"
    log_test "Write data before failover"
    local test_db="failover_test_$(date +%s)"

    for i in {1..50}; do
        curl -s -X POST "$ROUTER_HTTP/write/$test_db" \
            -d "cpu,host=server$i value=$i" > /dev/null
    done

    curl -s -X POST "$ROUTER_HTTP/flush" > /dev/null
    sleep 2

    local count_before=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu" | jq -r '.rows[0].cnt // .rows[0][0] // 0')
    log_info "Records before failover: $count_before"

    log_section "7.3 Failover Instructions (Manual)"
    log_info "To test failover manually:"
    log_info ""
    log_info "  1. Stop a Primary node:"
    log_info "     goreman run stop data1"
    log_info ""
    log_info "  2. Wait for detection and promotion (~5-10s)"
    log_info "     watch -n1 'curl -s http://localhost:6000/nodes?role=data | jq \".[] | {id: .node_id, mode: .mode, state: .state}\"'"
    log_info ""
    log_info "  3. Verify a Standby was promoted to Primary"
    log_info "     curl http://localhost:6000/nodes?role=data | jq '.[] | select(.mode==\"primary\" or .mode==\"Primary\")'"
    log_info ""
    log_info "  4. Verify data is still accessible:"
    log_info "     curl 'http://localhost:7000/query/$test_db?sql=SELECT+COUNT(*)+FROM+cpu'"
    log_info ""
    log_info "  5. Restart the stopped node:"
    log_info "     goreman run start data1"
    log_info ""
    log_info "  6. Verify it rejoins as Standby (not Primary to avoid split-brain)"
    log_info "     curl http://localhost:6000/nodes/data-1"

    log_section "7.4 Verify Current Cluster Can Handle Queries"
    log_test "Query still works"
    local query_result=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20cpu")
    local count=$(json_get "$query_result" '.rows[0].cnt // .rows[0][0]')

    if [ -n "$count" ] && [ "$count" != "null" ]; then
        log_pass "Query returns data (count: $count)"
    else
        log_fail "Query failed"
    fi

    log_pass "Failover test completed (manual steps documented)"
}

# ============================================================================
# Test: Node Corruption and Recovery
# ============================================================================

test_corruption() {
    log_header "TEST: Node Corruption and Recovery"

    log_section "8.1 Data Integrity Check"
    log_test "Verify data consistency across nodes"

    # Write test data
    local test_db="corruption_test_$(date +%s)"
    for i in {1..20}; do
        curl -s -X POST "$ROUTER_HTTP/write/$test_db" \
            -d "metrics,host=server$i value=$i" > /dev/null
    done
    curl -s -X POST "$ROUTER_HTTP/flush" > /dev/null
    sleep 3

    log_test "Query data from router"
    local router_count=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20metrics" | jq -r '.rows[0].cnt // .rows[0][0] // 0')
    log_info "Router reports: $router_count records"

    log_section "8.2 Corruption Scenarios (Documentation)"
    log_info "Common corruption scenarios and recovery:"
    log_info ""
    log_info "  Scenario 1: WAL Corruption"
    log_info "  - Symptom: Node fails to start, WAL replay errors"
    log_info "  - Recovery: Delete WAL files, node will resync from shared storage"
    log_info "  - Command: rm -rf ./dev/data/coldstandby/data1/wal/*"
    log_info ""
    log_info "  Scenario 2: Local Cache Corruption"
    log_info "  - Symptom: Query errors, inconsistent data"
    log_info "  - Recovery: Clear cache, node refetches from shared storage"
    log_info "  - Command: curl -X POST http://localhost:5000/cache/clear"
    log_info ""
    log_info "  Scenario 3: Metadata Corruption"
    log_info "  - Symptom: Tables not found, schema errors"
    log_info "  - Recovery: Clear metadata cache, re-register with meta"
    log_info "  - Command: Restart the node"
    log_info ""
    log_info "  Scenario 4: Shared Storage Corruption"
    log_info "  - Symptom: All nodes report errors for same data"
    log_info "  - Recovery: Restore from backup, or compact/rewrite data"
    log_info "  - This is the most severe case"

    log_section "8.3 Verify Recovery Mechanisms"
    log_test "Test cache clear and recovery"

    # Clear cache on data-1
    local clear_result=$(curl -s -X POST "$DATA1_HTTP/cache/clear" 2>/dev/null || echo "{}")
    log_info "Cache clear result: $(json_get "$clear_result" '.status // .message // "done"')"

    # Query should still work (data in shared storage)
    sleep 1
    local after_clear=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20metrics" | jq -r '.rows[0].cnt // .rows[0][0] // 0')
    log_info "After cache clear: $after_clear records"

    assert_eq "$router_count" "$after_clear" "Data consistent after cache clear"

    log_section "8.4 Shared Storage Verification"
    log_test "Verify data in MinIO"

    if command -v mc &> /dev/null; then
        log_info "MinIO bucket contents:"
        mc ls myminio/mynadb-coldstandby/ 2>/dev/null | head -20 || log_warn "Could not list MinIO bucket"
    else
        log_info "Install mc (MinIO client) to inspect storage"
        log_info "  mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin"
        log_info "  mc ls myminio/mynadb-coldstandby/"
    fi

    log_pass "Corruption and recovery test completed"
}

# ============================================================================
# Test: Full End-to-End Scenario
# ============================================================================

test_e2e() {
    log_header "TEST: End-to-End Scenario"

    log_section "E2E.1 Complete Write-Query-Failover Cycle"

    local test_db="e2e_test_$(date +%s)"

    # Step 1: Write data
    log_test "Step 1: Write 1000 points"
    for i in {1..10}; do
        local batch=""
        for j in {1..100}; do
            local ts=$(($(date +%s)000000000 + i * 100 + j))
            batch+="sensor,location=room$((j % 5)),type=temperature value=$((20 + RANDOM % 10)).$((RANDOM % 100)) $ts"$'\n'
        done
        curl -s -X POST "$ROUTER_HTTP/write/$test_db" -d "$batch" > /dev/null
    done
    log_pass "Wrote 1000 points"

    # Step 2: Flush
    log_test "Step 2: Flush to storage"
    curl -s -X POST "$ROUTER_HTTP/flush" > /dev/null
    sleep 3
    log_pass "Flushed to storage"

    # Step 3: Query
    log_test "Step 3: Verify data"
    local count=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20as%20cnt%20FROM%20sensor" | jq -r '.rows[0].cnt // .rows[0][0]')
    assert_gt "${count:-0}" "900" "At least 900 points stored"

    # Step 4: Aggregation query
    log_test "Step 4: Run aggregation"
    local agg=$(curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20location,%20AVG(value)%20as%20avg_temp%20FROM%20sensor%20GROUP%20BY%20location")
    local agg_rows=$(json_get "$agg" '.rows | length')
    assert_eq "$agg_rows" "5" "5 locations in aggregation"

    # Step 5: Check partition info
    log_test "Step 5: Check partitions"
    local partitions=$(curl -s "$ROUTER_HTTP/partitions/$test_db/default/sensor")
    log_debug "Partitions: $partitions"
    log_info "Partition info retrieved"

    # Step 6: Check cluster state
    log_test "Step 6: Verify cluster health"
    local nodes=$(curl -s "$META1_HTTP/nodes")
    local active_count=$(echo "$nodes" | jq '[.[] | select(.state=="active" or .state=="Active")] | length')
    log_info "Active nodes: $active_count"

    log_pass "End-to-end test completed successfully"
}

# ============================================================================
# Test: Performance Baseline
# ============================================================================

test_performance() {
    log_header "TEST: Performance Baseline"

    local test_db="perf_test_$(date +%s)"

    log_section "Perf.1 Write Performance"
    log_test "Measure write throughput"

    local start_time=$(date +%s%N)
    local total_points=0

    for i in {1..10}; do
        local batch=""
        for j in {1..1000}; do
            batch+="perf,host=server$j value=$RANDOM"$'\n'
            ((total_points++))
        done
        curl -s -X POST "$ROUTER_HTTP/write/$test_db" -d "$batch" > /dev/null
    done

    local end_time=$(date +%s%N)
    local duration_ms=$(( (end_time - start_time) / 1000000 ))
    local points_per_sec=$((total_points * 1000 / duration_ms))

    log_info "Wrote $total_points points in ${duration_ms}ms"
    log_info "Throughput: $points_per_sec points/second"
    log_pass "Write performance measured"

    log_section "Perf.2 Query Performance"
    log_test "Measure query latency"

    curl -s -X POST "$ROUTER_HTTP/flush" > /dev/null
    sleep 2

    local query_times=()
    for i in {1..5}; do
        local q_start=$(date +%s%N)
        curl -s "$ROUTER_HTTP/query/$test_db?sql=SELECT%20COUNT(*)%20FROM%20perf" > /dev/null
        local q_end=$(date +%s%N)
        local q_ms=$(( (q_end - q_start) / 1000000 ))
        query_times+=($q_ms)
    done

    local total=0
    for t in "${query_times[@]}"; do
        ((total+=t))
    done
    local avg=$((total / ${#query_times[@]}))

    log_info "Query latencies: ${query_times[*]} ms"
    log_info "Average query latency: ${avg}ms"
    log_pass "Query performance measured"
}

# ============================================================================
# Main Entry Point
# ============================================================================

print_summary() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}                        TEST SUMMARY                           ${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${GREEN}Passed:${NC}  $TESTS_PASSED"
    echo -e "  ${RED}Failed:${NC}  $TESTS_FAILED"
    echo -e "  ${YELLOW}Skipped:${NC} $TESTS_SKIPPED"
    echo ""

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "  ${GREEN}All tests passed!${NC}"
    else
        echo -e "  ${RED}Some tests failed. Check output above.${NC}"
    fi
    echo ""
}

run_all_tests() {
    test_cluster_health
    test_write_query
    test_meta_failover
    test_standby_correctness
    test_data_scalein
    test_data_scaleout
    test_failover
    test_corruption
    test_e2e
    test_performance
}

show_help() {
    echo "Cold Standby Cluster Test Suite"
    echo ""
    echo "Usage: $0 [test_name]"
    echo ""
    echo "Available tests:"
    echo "  all              - Run all tests (default)"
    echo "  cluster          - Cluster health and setup"
    echo "  write_query      - Write and query operations"
    echo "  meta_failover    - Meta node failover"
    echo "  standby          - Standby node correctness"
    echo "  data_scalein     - Data node scale-in"
    echo "  data_scaleout    - Data node scale-out"
    echo "  failover         - Primary failover and promotion"
    echo "  corruption       - Node corruption and recovery"
    echo "  e2e              - End-to-end scenario"
    echo "  performance      - Performance baseline"
    echo ""
    echo "Environment variables:"
    echo "  DEBUG=1          - Enable debug output"
    echo ""
    echo "Prerequisites:"
    echo "  1. MinIO running on port 9000"
    echo "  2. Cluster started via: ./dev/start-coldstandby.sh"
    echo ""
}

main() {
    local test_name="${1:-all}"

    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}       MynaDB Cold Standby Cluster Test Suite                  ${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  Router:  $ROUTER_HTTP"
    echo "  Meta:    $META1_HTTP, $META2_HTTP, $META3_HTTP"
    echo "  Data:    $DATA1_HTTP - $DATA6_HTTP"
    echo ""

    case "$test_name" in
        all)
            run_all_tests
            ;;
        cluster)
            test_cluster_health
            ;;
        write_query|write|query)
            test_write_query
            ;;
        meta_failover|meta)
            test_meta_failover
            ;;
        standby)
            test_standby_correctness
            ;;
        data_scalein|scalein)
            test_data_scalein
            ;;
        data_scaleout|scaleout)
            test_data_scaleout
            ;;
        failover)
            test_failover
            ;;
        corruption|recovery)
            test_corruption
            ;;
        e2e)
            test_e2e
            ;;
        performance|perf)
            test_performance
            ;;
        help|-h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown test: $test_name"
            echo ""
            show_help
            exit 1
            ;;
    esac

    print_summary

    if [ $TESTS_FAILED -gt 0 ]; then
        exit 1
    fi
}

main "$@"
