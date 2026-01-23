#!/bin/bash
#
# Setup Master-Slave HA for local manual testing
#
# Usage:
#   ./scripts/ha_setup_local.sh setup    # Create configs and directories
#   ./scripts/ha_setup_local.sh master   # Start master (run in terminal 1)
#   ./scripts/ha_setup_local.sh slave    # Start slave (run in terminal 2)
#   ./scripts/ha_setup_local.sh status   # Check status of both
#   ./scripts/ha_setup_local.sh test     # Run quick replication test
#   ./scripts/ha_setup_local.sh promote  # Promote slave to master
#   ./scripts/ha_setup_local.sh cleanup  # Stop and remove test data
#

set -e

TEST_DIR="/tmp/ha-test"
MASTER_HTTP=7000
SLAVE_HTTP=7100
REPL_PORT=7003

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/target/release/mynadb"

if [ ! -f "$BINARY" ]; then
    BINARY="$PROJECT_DIR/target/debug/mynadb"
fi

setup() {
    echo "Setting up HA test environment..."

    rm -rf "$TEST_DIR"
    mkdir -p "$TEST_DIR"/{master,slave}/{data,logs}

    # Master config
    cat > "$TEST_DIR/config-master.toml" << EOF
[server]
http_port = $MASTER_HTTP
flight_port = 7001
mysql_port = 7002

[storage]
data_dir = "$TEST_DIR/master/data"

[logging]
log_dir = "$TEST_DIR/master/logs"
level = "debug"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"

[cache]
flush_interval_secs = 5

[wal]
enabled = true

[ha]
enabled = true
role = "master"
node_id = "master-01"

[ha.replication]
listen_addr = "127.0.0.1:$REPL_PORT"

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10
EOF

    # Slave config
    cat > "$TEST_DIR/config-slave.toml" << EOF
[server]
http_port = $SLAVE_HTTP
flight_port = 7101
mysql_port = 7102

[storage]
data_dir = "$TEST_DIR/slave/data"

[logging]
log_dir = "$TEST_DIR/slave/logs"
level = "debug"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"

[cache]
flush_interval_secs = 5

[wal]
enabled = true

[ha]
enabled = true
role = "slave"
node_id = "slave-01"

[ha.replication]
master_addr = "127.0.0.1:$REPL_PORT"

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10

[ha.failover]
mode = "manual"
EOF

    echo "Done! Configs created in $TEST_DIR"
    echo ""
    echo "Next steps:"
    echo "  Terminal 1: $0 master"
    echo "  Terminal 2: $0 slave"
    echo "  Terminal 3: $0 status"
}

start_master() {
    if [ ! -f "$BINARY" ]; then
        echo "Error: Binary not found. Run 'cargo build --release' first."
        exit 1
    fi

    echo "Starting master on port $MASTER_HTTP..."
    echo "Replication port: $REPL_PORT"
    echo "Press Ctrl+C to stop"
    echo ""

    exec "$BINARY" -c "$TEST_DIR/config-master.toml"
}

start_slave() {
    if [ ! -f "$BINARY" ]; then
        echo "Error: Binary not found. Run 'cargo build --release' first."
        exit 1
    fi

    echo "Starting slave on port $SLAVE_HTTP..."
    echo "Connecting to master at 127.0.0.1:$REPL_PORT"
    echo "Press Ctrl+C to stop"
    echo ""

    exec "$BINARY" -c "$TEST_DIR/config-slave.toml"
}

show_status() {
    echo "=== Master Status (port $MASTER_HTTP) ==="
    curl -s "http://localhost:$MASTER_HTTP/ha/status" 2>/dev/null | jq . || echo "Master not running"

    echo ""
    echo "=== Slave Status (port $SLAVE_HTTP) ==="
    curl -s "http://localhost:$SLAVE_HTTP/ha/status" 2>/dev/null | jq . || echo "Slave not running"
}

run_test() {
    echo "=== Quick Replication Test ==="
    echo ""

    # Write to master
    echo "1. Writing to master..."
    curl -s -w " (HTTP %{http_code})" -X POST "http://localhost:$MASTER_HTTP/write/hatest" \
        -d 'test,source=master value=42'
    echo ""

    # Wait for replication
    echo "2. Waiting for replication..."
    sleep 2

    # Flush both to persist data for querying
    echo "3. Flushing data..."
    curl -s -X POST "http://localhost:$MASTER_HTTP/flush"
    curl -s -X POST "http://localhost:$SLAVE_HTTP/flush"
    sleep 1

    # Query both (use POST with SQL in body)
    echo "4. Query master:"
    curl -s -X POST "http://localhost:$MASTER_HTTP/query/hatest" \
        -d "SELECT * FROM hatest.default.test" | jq .

    echo ""
    echo "5. Query slave:"
    curl -s -X POST "http://localhost:$SLAVE_HTTP/query/hatest" \
        -d "SELECT * FROM hatest.default.test" | jq .

    echo ""
    echo "6. Try write to slave (should fail):"
    curl -s -w "HTTP %{http_code}\n" -X POST "http://localhost:$SLAVE_HTTP/write/hatest" \
        -d 'test,source=slave value=99'
}

promote_slave() {
    echo "Promoting slave to master..."
    curl -s -X POST "http://localhost:$SLAVE_HTTP/ha/promote" \
        -H "Content-Type: application/json" \
        -d '{"confirm": true}' | jq .

    echo ""
    echo "New status:"
    curl -s "http://localhost:$SLAVE_HTTP/ha/status" | jq '{role: .role, state: .state}'
}

cleanup() {
    echo "Stopping processes..."
    pkill -f "config-master.toml" 2>/dev/null || true
    pkill -f "config-slave.toml" 2>/dev/null || true

    echo "Removing test data..."
    rm -rf "$TEST_DIR"

    echo "Done!"
}

case "${1:-help}" in
    setup)
        setup
        ;;
    master)
        start_master
        ;;
    slave)
        start_slave
        ;;
    status)
        show_status
        ;;
    test)
        run_test
        ;;
    promote)
        promote_slave
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Master-Slave HA Local Testing Helper"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  setup    - Create test directories and config files"
        echo "  master   - Start master node (run in terminal 1)"
        echo "  slave    - Start slave node (run in terminal 2)"
        echo "  status   - Show HA status of both nodes"
        echo "  test     - Run quick replication test"
        echo "  promote  - Promote slave to master"
        echo "  cleanup  - Stop processes and remove test data"
        echo ""
        echo "Quick Start:"
        echo "  1. $0 setup"
        echo "  2. $0 master   # Terminal 1"
        echo "  3. $0 slave    # Terminal 2"
        echo "  4. $0 status   # Terminal 3"
        echo "  5. $0 test"
        ;;
esac
