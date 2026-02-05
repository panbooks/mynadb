#!/bin/bash
#
# Test Master-Slave HA on local host
#
# This script runs both master and slave on the same machine
# using different ports and data directories.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TEST_DIR="/tmp/ha-test"
MASTER_HTTP_PORT=7000
MASTER_FLIGHT_PORT=7001
MASTER_REPL_PORT=7003
SLAVE_HTTP_PORT=7100
SLAVE_FLIGHT_PORT=7101

# Find the binary
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/target/release/mynadb"

if [ ! -f "$BINARY" ]; then
    BINARY="$PROJECT_DIR/target/debug/mynadb"
fi

if [ ! -f "$BINARY" ]; then
    echo -e "${RED}Error: mynadb binary not found. Run 'cargo build --release' first.${NC}"
    exit 1
fi

echo -e "${BLUE}Using binary: $BINARY${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"

    # Kill any running instances
    pkill -f "config-master.toml" 2>/dev/null || true
    pkill -f "config-slave.toml" 2>/dev/null || true

    sleep 1
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Set trap for cleanup
trap cleanup EXIT

# Create test directories
setup_directories() {
    echo -e "${BLUE}=== Setting up test directories ===${NC}"
    rm -rf "$TEST_DIR"
    mkdir -p "$TEST_DIR"/{master,slave}/{data,logs}
    echo -e "${GREEN}Created $TEST_DIR${NC}"
}

# Create master config
create_master_config() {
    cat > "$TEST_DIR/config-master.toml" << EOF
[server]
http_port = $MASTER_HTTP_PORT
flight_port = $MASTER_FLIGHT_PORT
mysql_enabled = false

[storage]
data_dir = "$TEST_DIR/master/data"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"

[logging]
log_dir = "$TEST_DIR/master/logs"
level = "info"
file_prefix = "master"

[cache]
flush_interval_secs = 5
max_size_mb = 100

[wal]
enabled = true
sync_on_write = false

[compaction]
interval_secs = 0

[retention]
enabled = false

[ha]
enabled = true
role = "master"
node_id = "master-01"

[ha.replication]
mode = "async"
listen_addr = "127.0.0.1:$MASTER_REPL_PORT"

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10
EOF
    echo -e "${GREEN}Created master config${NC}"
}

# Create slave config
create_slave_config() {
    cat > "$TEST_DIR/config-slave.toml" << EOF
[server]
http_port = $SLAVE_HTTP_PORT
flight_port = $SLAVE_FLIGHT_PORT
mysql_enabled = false

[storage]
data_dir = "$TEST_DIR/slave/data"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"

[logging]
log_dir = "$TEST_DIR/slave/logs"
level = "info"
file_prefix = "slave"

[cache]
flush_interval_secs = 5
max_size_mb = 100

[wal]
enabled = true
sync_on_write = false

[compaction]
interval_secs = 0

[retention]
enabled = false

[ha]
enabled = true
role = "slave"
node_id = "slave-01"

[ha.replication]
master_addr = "127.0.0.1:$MASTER_REPL_PORT"
reconnect_interval_secs = 2

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10

[ha.failover]
mode = "manual"
failure_threshold = 3
promotion_delay_secs = 2
EOF
    echo -e "${GREEN}Created slave config${NC}"
}

# Start master
start_master() {
    echo -e "${BLUE}=== Starting Master ===${NC}"
    "$BINARY" -c "$TEST_DIR/config-master.toml" > "$TEST_DIR/master/stdout.log" 2>&1 &
    MASTER_PID=$!
    echo "Master PID: $MASTER_PID"

    # Wait for master to be ready
    echo -n "Waiting for master to start..."
    for i in {1..30}; do
        if curl -s "http://localhost:$MASTER_HTTP_PORT/health" > /dev/null 2>&1; then
            echo -e " ${GREEN}Ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done

    echo -e " ${RED}Failed!${NC}"
    echo "Master logs:"
    cat "$TEST_DIR/master/stdout.log"
    exit 1
}

# Start slave
start_slave() {
    echo -e "${BLUE}=== Starting Slave ===${NC}"
    "$BINARY" -c "$TEST_DIR/config-slave.toml" > "$TEST_DIR/slave/stdout.log" 2>&1 &
    SLAVE_PID=$!
    echo "Slave PID: $SLAVE_PID"

    # Wait for slave to be ready
    echo -n "Waiting for slave to start..."
    for i in {1..30}; do
        if curl -s "http://localhost:$SLAVE_HTTP_PORT/health" > /dev/null 2>&1; then
            echo -e " ${GREEN}Ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done

    echo -e " ${RED}Failed!${NC}"
    echo "Slave logs:"
    cat "$TEST_DIR/slave/stdout.log"
    exit 1
}

# Check HA status
check_ha_status() {
    echo -e "${BLUE}=== Checking HA Status ===${NC}"

    echo -e "\n${YELLOW}Master status:${NC}"
    curl -s "http://localhost:$MASTER_HTTP_PORT/ha/status" | jq .

    echo -e "\n${YELLOW}Slave status:${NC}"
    curl -s "http://localhost:$SLAVE_HTTP_PORT/ha/status" | jq .
}

# Wait for slave to connect
wait_for_slave_connection() {
    echo -n "Waiting for slave to connect to master..."
    for i in {1..30}; do
        CONNECTED=$(curl -s "http://localhost:$SLAVE_HTTP_PORT/ha/status" | jq -r '.replication.connected // false')
        if [ "$CONNECTED" = "true" ]; then
            echo -e " ${GREEN}Connected!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done

    echo -e " ${RED}Failed!${NC}"
    return 1
}

# Test 1: Basic replication
test_replication() {
    echo -e "\n${BLUE}=== Test 1: Basic Replication ===${NC}"

    # Write data to master
    echo "Writing data to master..."
    WRITE_RESULT=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST "http://localhost:$MASTER_HTTP_PORT/write/testdb" \
        -d 'cpu,host=server1,region=us-east value=42.5
cpu,host=server2,region=us-west value=38.2
cpu,host=server3,region=eu-west value=45.0
memory,host=server1 used=8589934592,total=17179869184
memory,host=server2 used=4294967296,total=8589934592')
    echo "Write result: $WRITE_RESULT"

    echo "Waiting for replication..."
    sleep 2

    # Flush both master and slave to persist data for querying
    echo "Flushing data..."
    curl -s -X POST "http://localhost:$MASTER_HTTP_PORT/flush"
    curl -s -X POST "http://localhost:$SLAVE_HTTP_PORT/flush"
    sleep 1

    # List tables to verify data was written
    echo -e "\n${YELLOW}Tables on master:${NC}"
    curl -s "http://localhost:$MASTER_HTTP_PORT/tables/testdb"

    # Query from master (use POST with SQL in body)
    echo -e "\n${YELLOW}Query from master:${NC}"
    MASTER_RESULT=$(curl -s -X POST "http://localhost:$MASTER_HTTP_PORT/query/testdb" \
        -d "SELECT COUNT(*) as cnt FROM testdb.default.cpu")
    echo "Raw result: $MASTER_RESULT"
    echo "$MASTER_RESULT" | jq .

    # Query from slave
    echo -e "\n${YELLOW}Query from slave:${NC}"
    SLAVE_RESULT=$(curl -s -X POST "http://localhost:$SLAVE_HTTP_PORT/query/testdb" \
        -d "SELECT COUNT(*) as cnt FROM testdb.default.cpu")
    echo "Raw result: $SLAVE_RESULT"
    echo "$SLAVE_RESULT" | jq .

    # Compare results
    MASTER_COUNT=$(echo "$MASTER_RESULT" | jq -r '.[0].cnt // 0')
    SLAVE_COUNT=$(echo "$SLAVE_RESULT" | jq -r '.[0].cnt // 0')

    if [ "$MASTER_COUNT" = "$SLAVE_COUNT" ] && [ "$MASTER_COUNT" = "3" ]; then
        echo -e "${GREEN}✓ Test 1 PASSED: Replication working (count: $MASTER_COUNT)${NC}"
    else
        echo -e "${RED}✗ Test 1 FAILED: Count mismatch (master: $MASTER_COUNT, slave: $SLAVE_COUNT)${NC}"
        return 1
    fi
}

# Test 2: Write rejection on slave
test_slave_write_rejection() {
    echo -e "\n${BLUE}=== Test 2: Write Rejection on Slave ===${NC}"

    # Try to write to slave
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:$SLAVE_HTTP_PORT/write/testdb" \
        -d 'cpu,host=server99 value=99.9')

    if [ "$HTTP_CODE" = "503" ]; then
        echo -e "${GREEN}✓ Test 2 PASSED: Slave correctly rejected write (HTTP $HTTP_CODE)${NC}"
    else
        echo -e "${RED}✗ Test 2 FAILED: Expected HTTP 503, got $HTTP_CODE${NC}"
        return 1
    fi
}

# Test 3: Check replication sequence
test_replication_sequence() {
    echo -e "\n${BLUE}=== Test 3: Replication Sequence ===${NC}"

    MASTER_SEQ=$(curl -s "http://localhost:$MASTER_HTTP_PORT/ha/status" | jq -r '.replication.current_sequence // 0')
    SLAVE_SEQ=$(curl -s "http://localhost:$SLAVE_HTTP_PORT/ha/status" | jq -r '.replication.current_sequence // 0')

    echo "Master sequence: $MASTER_SEQ"
    echo "Slave sequence:  $SLAVE_SEQ"

    if [ "$MASTER_SEQ" -gt 0 ]; then
        echo -e "${GREEN}✓ Test 3 PASSED: Sequence tracking working${NC}"
    else
        echo -e "${YELLOW}⚠ Test 3 WARNING: Sequence is 0 (may need more writes)${NC}"
    fi
}

# Test 4: Bulk write and verify
test_bulk_replication() {
    echo -e "\n${BLUE}=== Test 4: Bulk Write Replication ===${NC}"

    # Write 100 points
    echo "Writing 100 points to master..."
    for i in {1..100}; do
        curl -s -X POST "http://localhost:$MASTER_HTTP_PORT/write/bulktest" \
            -d "metric,id=$i value=$i" > /dev/null
    done

    echo "Waiting for replication..."
    sleep 3

    # Flush both master and slave to persist data for querying
    echo "Flushing data..."
    curl -s -X POST "http://localhost:$MASTER_HTTP_PORT/flush"
    curl -s -X POST "http://localhost:$SLAVE_HTTP_PORT/flush"
    sleep 1

    # Count on both sides (use POST with SQL in body)
    MASTER_RESULT=$(curl -s -X POST "http://localhost:$MASTER_HTTP_PORT/query/bulktest" \
        -d "SELECT COUNT(*) as cnt FROM bulktest.default.metric")
    SLAVE_RESULT=$(curl -s -X POST "http://localhost:$SLAVE_HTTP_PORT/query/bulktest" \
        -d "SELECT COUNT(*) as cnt FROM bulktest.default.metric")

    MASTER_COUNT=$(echo "$MASTER_RESULT" | jq -r '.[0].cnt // 0')
    SLAVE_COUNT=$(echo "$SLAVE_RESULT" | jq -r '.[0].cnt // 0')

    echo "Master count: $MASTER_COUNT"
    echo "Slave count:  $SLAVE_COUNT"

    if [ "$MASTER_COUNT" = "$SLAVE_COUNT" ] && [ "$MASTER_COUNT" = "100" ]; then
        echo -e "${GREEN}✓ Test 4 PASSED: All 100 points replicated${NC}"
    else
        echo -e "${RED}✗ Test 4 FAILED: Count mismatch (master: $MASTER_COUNT, slave: $SLAVE_COUNT)${NC}"
        echo "Master raw: $MASTER_RESULT"
        echo "Slave raw: $SLAVE_RESULT"
        return 1
    fi
}

# Test 5: Failover
test_failover() {
    echo -e "\n${BLUE}=== Test 5: Manual Failover ===${NC}"

    # Stop master
    echo "Stopping master..."
    pkill -f "config-master.toml" 2>/dev/null || true
    sleep 2

    # Check slave status (should show disconnected)
    echo -e "\n${YELLOW}Slave status after master stopped:${NC}"
    curl -s "http://localhost:$SLAVE_HTTP_PORT/ha/status" | jq '{connected: .replication.connected, role: .role}'

    # Promote slave
    echo -e "\nPromoting slave to master..."
    PROMOTE_RESULT=$(curl -s -X POST "http://localhost:$SLAVE_HTTP_PORT/ha/promote" \
        -H "Content-Type: application/json" \
        -d '{"confirm": true}')
    echo "$PROMOTE_RESULT" | jq .

    STATUS=$(echo "$PROMOTE_RESULT" | jq -r '.status')
    if [ "$STATUS" = "success" ]; then
        echo -e "${GREEN}Promotion successful${NC}"
    else
        echo -e "${RED}Promotion failed${NC}"
        return 1
    fi

    # Verify new master can accept writes
    echo -e "\nWriting to new master..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:$SLAVE_HTTP_PORT/write/testdb" \
        -d 'cpu,host=newmaster value=100.0')

    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "${GREEN}✓ Test 5 PASSED: Failover successful, new master accepts writes${NC}"
    else
        echo -e "${RED}✗ Test 5 FAILED: New master rejected write (HTTP $HTTP_CODE)${NC}"
        return 1
    fi

    # Check new master status
    echo -e "\n${YELLOW}New master status:${NC}"
    curl -s "http://localhost:$SLAVE_HTTP_PORT/ha/status" | jq '{role: .role, state: .state}'
}

# Main execution
main() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║        Master-Slave HA Local Test Suite                    ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Setup
    setup_directories
    create_master_config
    create_slave_config

    # Start services
    start_master
    start_slave

    # Wait for connection
    wait_for_slave_connection

    # Initial status
    check_ha_status

    # Run tests
    FAILED=0

    test_replication || FAILED=1
    test_slave_write_rejection || FAILED=1
    test_replication_sequence || FAILED=1
    test_bulk_replication || FAILED=1
    test_failover || FAILED=1

    # Summary
    echo -e "\n${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║                      Test Summary                          ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"

    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests PASSED!${NC}"
    else
        echo -e "${RED}Some tests FAILED!${NC}"
    fi

    echo -e "\nLog files:"
    echo "  Master: $TEST_DIR/master/stdout.log"
    echo "  Slave:  $TEST_DIR/slave/stdout.log"

    exit $FAILED
}

# Run main
main "$@"
