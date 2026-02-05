#!/bin/bash
#
# MynaDB Admin Console Test Script
#
# This script tests the admin console by:
# 1. Starting a Meta cluster (3 nodes)
# 2. Starting Data nodes (2 primary + 2 standby)
# 3. Starting a Router node
# 4. Starting the Admin console
# 5. Running API endpoint tests
# 6. Optionally starting the frontend dev server
#
# Usage:
#   ./scripts/test_admin_console.sh [--frontend] [--cleanup-only]
#
# Options:
#   --frontend     Also start the Leptos frontend dev server (requires trunk)
#   --cleanup-only Only cleanup any running processes from previous runs
#   --no-cluster   Skip cluster startup, only test admin console (assumes cluster running)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="/tmp/mynadb-admin-test"
LOG_DIR="$DATA_DIR/logs"

# Ports
META_BASE_PORT=17100
ROUTER_HTTP_PORT=17000
ROUTER_FLIGHT_PORT=17001
DATA_BASE_HTTP_PORT=18000
DATA_BASE_FLIGHT_PORT=18100
ADMIN_PORT=18080

# Process tracking
PIDS=()

# Parse arguments
START_FRONTEND=false
CLEANUP_ONLY=false
NO_CLUSTER=false

for arg in "$@"; do
    case $arg in
        --frontend)
            START_FRONTEND=true
            shift
            ;;
        --cleanup-only)
            CLEANUP_ONLY=true
            shift
            ;;
        --no-cluster)
            NO_CLUSTER=true
            shift
            ;;
    esac
done

# ============================================================================
# Utility Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
}

cleanup() {
    log_header "Cleaning Up"

    # Kill tracked processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping process $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Kill any remaining mynadb processes from test
    pkill -f "mynadb.*$DATA_DIR" 2>/dev/null || true

    # Kill trunk if running
    pkill -f "trunk serve" 2>/dev/null || true

    # Wait for processes to stop
    sleep 2

    # Remove data directory
    if [ -d "$DATA_DIR" ]; then
        log_info "Removing test data directory: $DATA_DIR"
        rm -rf "$DATA_DIR"
    fi

    log_success "Cleanup complete"
}

wait_for_port() {
    local port=$1
    local name=$2
    local max_wait=${3:-30}
    local waited=0

    while ! nc -z localhost "$port" 2>/dev/null; do
        if [ $waited -ge $max_wait ]; then
            log_error "$name failed to start on port $port after ${max_wait}s"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done

    log_success "$name is ready on port $port (waited ${waited}s)"
    return 0
}

wait_for_http() {
    local url=$1
    local name=$2
    local max_wait=${3:-30}
    local waited=0

    while ! curl -s "$url" > /dev/null 2>&1; do
        if [ $waited -ge $max_wait ]; then
            log_error "$name failed to respond at $url after ${max_wait}s"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done

    log_success "$name is responding at $url (waited ${waited}s)"
    return 0
}

# Trap for cleanup on exit
trap cleanup EXIT

# ============================================================================
# Cleanup Only Mode
# ============================================================================

if [ "$CLEANUP_ONLY" = true ]; then
    cleanup
    exit 0
fi

# ============================================================================
# Setup
# ============================================================================

log_header "Setting Up Test Environment"

cd "$PROJECT_DIR"

# Build the project
log_info "Building mynadb..."
cargo build --release 2>&1 | tail -5

MYNADB="$PROJECT_DIR/target/release/mynadb"
if [ ! -f "$MYNADB" ]; then
    log_error "Binary not found at $MYNADB"
    exit 1
fi
log_success "Build complete"

# Create directories
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"

# ============================================================================
# Generate Configuration Files
# ============================================================================

log_header "Generating Configuration Files"

# Meta node configs
for i in 1 2 3; do
    META_PORT=$((META_BASE_PORT + i - 1))
    META_HTTP_PORT=$((META_BASE_PORT + 100 + i - 1))

    cat > "$DATA_DIR/meta$i.toml" << EOF
[storage]
data_dir = "$DATA_DIR/meta$i"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[meta]
node_id = "meta-$i"
grpc_port = $META_PORT
http_port = $META_HTTP_PORT
raft_dir = "$DATA_DIR/meta$i/raft"
initial_cluster = "meta-1=localhost:$META_BASE_PORT,meta-2=localhost:$((META_BASE_PORT+1)),meta-3=localhost:$((META_BASE_PORT+2))"
EOF
    log_info "Created meta$i.toml (gRPC: $META_PORT, HTTP: $META_HTTP_PORT)"
done

# Router config
cat > "$DATA_DIR/router.toml" << EOF
[storage]
data_dir = "$DATA_DIR/router"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[router]
node_id = "router-1"
http_port = $ROUTER_HTTP_PORT
flight_port = $ROUTER_FLIGHT_PORT
mysql_enabled = false

[router.meta]
endpoints = ["localhost:$META_BASE_PORT", "localhost:$((META_BASE_PORT+1))", "localhost:$((META_BASE_PORT+2))"]
EOF
log_info "Created router.toml (HTTP: $ROUTER_HTTP_PORT, Flight: $ROUTER_FLIGHT_PORT)"

# Data node configs (2 primary + 2 standby)
for i in 1 2; do
    DATA_HTTP=$((DATA_BASE_HTTP_PORT + i - 1))
    DATA_FLIGHT=$((DATA_BASE_FLIGHT_PORT + i - 1))

    cat > "$DATA_DIR/data$i.toml" << EOF
[storage]
data_dir = "$DATA_DIR/data$i"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[data]
node_id = "data-primary-$i"
mode = "primary"
http_port = $DATA_HTTP
flight_port = $DATA_FLIGHT
zone = "zone-a"

[data.meta]
endpoints = ["localhost:$META_BASE_PORT", "localhost:$((META_BASE_PORT+1))", "localhost:$((META_BASE_PORT+2))"]
EOF
    log_info "Created data$i.toml (Primary, HTTP: $DATA_HTTP)"
done

for i in 3 4; do
    DATA_HTTP=$((DATA_BASE_HTTP_PORT + i - 1))
    DATA_FLIGHT=$((DATA_BASE_FLIGHT_PORT + i - 1))
    BACKING=$((i - 2))

    cat > "$DATA_DIR/data$i.toml" << EOF
[storage]
data_dir = "$DATA_DIR/data$i"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[data]
node_id = "data-standby-$((i-2))"
mode = "standby"
backing_primary = "data-primary-$BACKING"
http_port = $DATA_HTTP
flight_port = $DATA_FLIGHT
zone = "zone-b"

[data.meta]
endpoints = ["localhost:$META_BASE_PORT", "localhost:$((META_BASE_PORT+1))", "localhost:$((META_BASE_PORT+2))"]
EOF
    log_info "Created data$i.toml (Standby, HTTP: $DATA_HTTP)"
done

# Admin config
cat > "$DATA_DIR/admin.toml" << EOF
[storage]
data_dir = "$DATA_DIR/admin"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[admin]
http_host = "0.0.0.0"
http_port = $ADMIN_PORT
meta_endpoints = ["localhost:$META_BASE_PORT", "localhost:$((META_BASE_PORT+1))", "localhost:$((META_BASE_PORT+2))"]
EOF
log_info "Created admin.toml (HTTP: $ADMIN_PORT)"

log_success "All configuration files generated"

# ============================================================================
# Start Cluster (unless --no-cluster)
# ============================================================================

if [ "$NO_CLUSTER" = false ]; then
    log_header "Starting Meta Cluster"

    for i in 1 2 3; do
        log_info "Starting Meta node $i..."
        $MYNADB meta start -c "$DATA_DIR/meta$i.toml" > "$LOG_DIR/meta$i.log" 2>&1 &
        PIDS+=($!)
        sleep 1
    done

    # Wait for Meta cluster to form
    log_info "Waiting for Meta cluster to form..."
    sleep 5

    for i in 1 2 3; do
        META_HTTP_PORT=$((META_BASE_PORT + 100 + i - 1))
        wait_for_http "http://localhost:$META_HTTP_PORT/health" "Meta-$i" 30
    done

    log_header "Starting Data Nodes"

    for i in 1 2 3 4; do
        log_info "Starting Data node $i..."
        $MYNADB data start -c "$DATA_DIR/data$i.toml" > "$LOG_DIR/data$i.log" 2>&1 &
        PIDS+=($!)
        sleep 1
    done

    # Wait for Data nodes
    for i in 1 2 3 4; do
        DATA_HTTP=$((DATA_BASE_HTTP_PORT + i - 1))
        wait_for_http "http://localhost:$DATA_HTTP/health" "Data-$i" 30
    done

    log_header "Starting Router"

    log_info "Starting Router node..."
    $MYNADB router start -c "$DATA_DIR/router.toml" > "$LOG_DIR/router.log" 2>&1 &
    PIDS+=($!)

    wait_for_http "http://localhost:$ROUTER_HTTP_PORT/health" "Router" 30

    log_success "Cluster is running"
fi

# ============================================================================
# Start Admin Console
# ============================================================================

log_header "Starting Admin Console"

log_info "Starting Admin console on port $ADMIN_PORT..."
$MYNADB admin start -c "$DATA_DIR/admin.toml" > "$LOG_DIR/admin.log" 2>&1 &
PIDS+=($!)

wait_for_http "http://localhost:$ADMIN_PORT/admin/api/v1/cluster/status" "Admin Console" 30

log_success "Admin console is running at http://localhost:$ADMIN_PORT/admin/"

# ============================================================================
# Write Test Data (if cluster started)
# ============================================================================

if [ "$NO_CLUSTER" = false ]; then
    log_header "Writing Test Data"

    # Write some test data to create tables
    for i in 1 2 3; do
        curl -s -X POST "http://localhost:$ROUTER_HTTP_PORT/write/testdb" \
            -d "cpu,host=server$i,region=us-east value=0.$i $(date +%s)000000000" > /dev/null
        log_info "Created table: testdb.default.cpu (record $i)"
    done

    for i in 1 2; do
        curl -s -X POST "http://localhost:$ROUTER_HTTP_PORT/write/testdb" \
            -d "memory,host=server$i,region=us-west used=$((50+i*10)) $(date +%s)000000000" > /dev/null
        log_info "Created table: testdb.default.memory (record $i)"
    done

    curl -s -X POST "http://localhost:$ROUTER_HTTP_PORT/write/proddb" \
        -d "requests,service=api,env=prod count=100 $(date +%s)000000000" > /dev/null
    log_info "Created table: proddb.default.requests"

    log_success "Test data written"
fi

# ============================================================================
# API Endpoint Tests
# ============================================================================

log_header "Testing Admin API Endpoints"

ADMIN_BASE="http://localhost:$ADMIN_PORT/admin/api/v1"
TESTS_PASSED=0
TESTS_FAILED=0

test_endpoint() {
    local name=$1
    local method=$2
    local endpoint=$3
    local expected_field=$4
    local data=$5

    echo -n "  Testing $name... "

    if [ "$method" = "GET" ]; then
        response=$(curl -s "$ADMIN_BASE$endpoint")
    else
        response=$(curl -s -X "$method" "$ADMIN_BASE$endpoint" \
            -H "Content-Type: application/json" \
            -d "$data")
    fi

    if echo "$response" | jq -e ".success == true" > /dev/null 2>&1; then
        if [ -n "$expected_field" ]; then
            if echo "$response" | jq -e ".data.$expected_field" > /dev/null 2>&1; then
                echo -e "${GREEN}PASSED${NC}"
                TESTS_PASSED=$((TESTS_PASSED + 1))
            else
                echo -e "${RED}FAILED${NC} (missing field: $expected_field)"
                echo "    Response: $(echo "$response" | jq -c .)"
                TESTS_FAILED=$((TESTS_FAILED + 1))
            fi
        else
            echo -e "${GREEN}PASSED${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        fi
    else
        echo -e "${RED}FAILED${NC}"
        echo "    Response: $(echo "$response" | jq -c .)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# Cluster endpoints
test_endpoint "GET /cluster/status" "GET" "/cluster/status" "healthy"

# Node endpoints
test_endpoint "GET /nodes" "GET" "/nodes" "nodes"
test_endpoint "GET /nodes (filter by role)" "GET" "/nodes?role=data" "total"
test_endpoint "GET /nodes (filter by mode)" "GET" "/nodes?role=data&mode=primary" "nodes"

if [ "$NO_CLUSTER" = false ]; then
    test_endpoint "GET /nodes/:id" "GET" "/nodes/data-primary-1" "node"
fi

# Table endpoints
test_endpoint "GET /tables" "GET" "/tables" "tables"
test_endpoint "GET /tables (with pagination)" "GET" "/tables?limit=10&offset=0" "total"

if [ "$NO_CLUSTER" = false ]; then
    test_endpoint "GET /tables (with search)" "GET" "/tables?search=cpu" "tables"
fi

# Scale-in endpoints
test_endpoint "GET /scale-in/recommend" "GET" "/scale-in/recommend?primaries=1&standbys=1" "recommended_primaries"
test_endpoint "POST /scale-in/plan" "POST" "/scale-in/plan" "tables_to_reassign" '{"node_ids":["data-primary-1"],"dry_run":true}'

# Events endpoint
test_endpoint "GET /events" "GET" "/events" "message"

# Summary
echo ""
echo -e "  ${BLUE}Test Summary:${NC}"
echo -e "    Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "    Failed: ${RED}$TESTS_FAILED${NC}"
echo ""

if [ $TESTS_FAILED -gt 0 ]; then
    log_warn "Some tests failed!"
fi

# ============================================================================
# Test WebSocket Connection
# ============================================================================

log_header "Testing WebSocket Connection"

if command -v websocat &> /dev/null; then
    log_info "Testing WebSocket connection (5 second timeout)..."

    # Connect and wait for heartbeat
    timeout 35 websocat -t "ws://localhost:$ADMIN_PORT/admin/api/v1/events/ws" 2>/dev/null | head -1 > "$DATA_DIR/ws_response.txt" &
    WS_PID=$!

    sleep 35

    if [ -s "$DATA_DIR/ws_response.txt" ]; then
        WS_RESPONSE=$(cat "$DATA_DIR/ws_response.txt")
        if echo "$WS_RESPONSE" | jq -e '.type == "heartbeat"' > /dev/null 2>&1; then
            log_success "WebSocket heartbeat received"
        else
            log_info "WebSocket response: $WS_RESPONSE"
        fi
    else
        log_warn "No WebSocket message received (this is expected if no events occurred)"
    fi

    kill $WS_PID 2>/dev/null || true
else
    log_warn "websocat not installed, skipping WebSocket test"
    log_info "Install with: cargo install websocat"
fi

# ============================================================================
# Start Frontend (optional)
# ============================================================================

if [ "$START_FRONTEND" = true ]; then
    log_header "Starting Frontend Development Server"

    if command -v trunk &> /dev/null; then
        cd "$PROJECT_DIR/admin-frontend"

        log_info "Starting Trunk dev server..."
        trunk serve --port 8081 --proxy-backend="http://localhost:$ADMIN_PORT" > "$LOG_DIR/frontend.log" 2>&1 &
        PIDS+=($!)

        wait_for_port 8081 "Frontend" 60

        log_success "Frontend is running at http://localhost:8081/"
    else
        log_error "trunk not installed. Install with: cargo install trunk"
        log_info "Frontend will not be started"
    fi
fi

# ============================================================================
# Print Summary
# ============================================================================

log_header "Test Summary"

echo "Services running:"
echo ""
if [ "$NO_CLUSTER" = false ]; then
    echo "  Meta Cluster:"
    for i in 1 2 3; do
        META_PORT=$((META_BASE_PORT + i - 1))
        echo "    - Meta-$i: localhost:$META_PORT (gRPC)"
    done
    echo ""
    echo "  Data Nodes:"
    for i in 1 2; do
        DATA_HTTP=$((DATA_BASE_HTTP_PORT + i - 1))
        echo "    - data-primary-$i: localhost:$DATA_HTTP (HTTP)"
    done
    for i in 1 2; do
        DATA_HTTP=$((DATA_BASE_HTTP_PORT + i + 1))
        echo "    - data-standby-$i: localhost:$DATA_HTTP (HTTP)"
    done
    echo ""
    echo "  Router:"
    echo "    - HTTP: localhost:$ROUTER_HTTP_PORT"
    echo "    - Flight: localhost:$ROUTER_FLIGHT_PORT"
    echo ""
fi
echo "  Admin Console:"
echo "    - Web UI: http://localhost:$ADMIN_PORT/admin/"
echo "    - API: http://localhost:$ADMIN_PORT/admin/api/v1/"
echo "    - WebSocket: ws://localhost:$ADMIN_PORT/admin/api/v1/events/ws"
echo ""

if [ "$START_FRONTEND" = true ]; then
    echo "  Frontend Dev Server:"
    echo "    - URL: http://localhost:8081/"
    echo ""
fi

echo "Useful commands:"
echo ""
echo "  # Get cluster status"
echo "  curl http://localhost:$ADMIN_PORT/admin/api/v1/cluster/status | jq ."
echo ""
echo "  # List all nodes"
echo "  curl http://localhost:$ADMIN_PORT/admin/api/v1/nodes | jq ."
echo ""
echo "  # List all tables"
echo "  curl http://localhost:$ADMIN_PORT/admin/api/v1/tables | jq ."
echo ""
echo "  # Get scale-in recommendations"
echo "  curl 'http://localhost:$ADMIN_PORT/admin/api/v1/scale-in/recommend?primaries=1' | jq ."
echo ""
echo "  # Connect to WebSocket"
echo "  websocat ws://localhost:$ADMIN_PORT/admin/api/v1/events/ws"
echo ""

echo "Log files are in: $LOG_DIR"
echo ""
echo "Press Ctrl+C to stop all services and cleanup"

# ============================================================================
# Wait for user to stop
# ============================================================================

# Keep script running until user interrupts
while true; do
    sleep 1
done
