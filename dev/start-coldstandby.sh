#!/bin/bash
#
# Start MynaDB Cold Standby cluster for local development
# Tests Cold Standby architecture with scale-in capabilities
#
# Requires: goreman (go install github.com/mattn/goreman@latest)
# Requires: MinIO running on port 9000
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== MynaDB Cold Standby Development Environment ==="
echo ""
echo "This environment tests Cold Standby architecture with:"
echo "  - 3 Meta nodes (Raft consensus, HTTP API for scale-in)"
echo "  - 6 Data nodes:"
echo "      - 4 Primary nodes (serve traffic)"
echo "      - 2 Standby nodes (cold backups)"
echo "  - 1 Router node"
echo "  - Primary:Standby ratio = 2:1"
echo ""

# Check for goreman
if ! command -v goreman &> /dev/null; then
    echo "ERROR: goreman is not installed"
    echo ""
    echo "Install with:"
    echo "  go install github.com/mattn/goreman@latest"
    exit 1
fi

# Check for MinIO
if ! curl -s http://127.0.0.1:9000/minio/health/live &> /dev/null; then
    echo "ERROR: MinIO is not running on port 9000"
    echo ""
    echo "Start MinIO with:"
    echo "  minio server /data --console-address :9001"
    echo ""
    echo "Or with Docker:"
    echo "  docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address :9001"
    exit 1
fi

echo "MinIO detected on port 9000"

# Create bucket if it doesn't exist
if command -v mc &> /dev/null; then
    mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin 2>/dev/null || true
    mc mb myminio/mynadb-coldstandby 2>/dev/null || true
    echo "Bucket mynadb-coldstandby ready"
fi

# Build binary if it doesn't exist
BUILD_MODE="${BUILD_MODE:-debug}"
if [ "$BUILD_MODE" = "release" ]; then
    BINARY="./target/release/mynadb"
    if [ ! -f "$BINARY" ]; then
        echo ""
        echo "Building MynaDB (release mode)..."
        RUSTFLAGS="-A dead_code -A unused_imports" cargo build --release
        # Update Procfile to use release binary
        sed -i 's|target/debug|target/release|g' ./dev/Procfile.coldstandby
        echo "Build complete!"
    else
        echo "Using existing binary: $BINARY"
        # Still update Procfile for release binary
        sed -i 's|target/debug|target/release|g' ./dev/Procfile.coldstandby
    fi
else
    BINARY="./target/debug/mynadb"
    if [ ! -f "$BINARY" ]; then
        echo ""
        echo "Building MynaDB (debug mode)..."
        RUSTFLAGS="-A dead_code -A unused_imports" cargo build
        echo "Build complete!"
    else
        echo "Using existing binary: $BINARY"
    fi
fi

# Create data and log directories
echo ""
echo "Creating data directories..."
for node in meta1 meta2 meta3 data1 data2 data3 data4 data5 data6; do
    mkdir -p "./dev/data/coldstandby/$node"
    mkdir -p "./dev/logs/coldstandby/$node"
done
mkdir -p "./dev/logs/coldstandby/router1"

# Clean old data if requested
if [ "$CLEAN" = "1" ]; then
    echo "Cleaning old data..."
    rm -rf ./dev/data/coldstandby/*/
    rm -rf ./dev/logs/coldstandby/*/
fi

echo ""
echo "Starting Cold Standby cluster..."
echo ""
echo "Port assignments (base + offset pattern):"
echo "  Meta nodes:"
echo "    meta1:   HTTP 6000, gRPC 6001"
echo "    meta2:   HTTP 6100, gRPC 6101"
echo "    meta3:   HTTP 6200, gRPC 6201"
echo ""
echo "  Router:"
echo "    router1: HTTP 7000, Flight 7001, MySQL 7002"
echo ""
echo "  Data nodes (4 Primary + 2 Standby):"
echo "    data1:   HTTP 5000, Flight 5001 (zone-a)"
echo "    data2:   HTTP 5100, Flight 5101 (zone-b)"
echo "    data3:   HTTP 5200, Flight 5201 (zone-c)"
echo "    data4:   HTTP 5300, Flight 5301 (zone-a)"
echo "    data5:   HTTP 5400, Flight 5401 (zone-b)"
echo "    data6:   HTTP 5500, Flight 5501 (zone-c)"
echo ""
echo "Access points:"
echo "  HTTP:   http://localhost:7000"
echo "  Flight: localhost:7001"
echo "  MySQL:  localhost:7002"
echo "  Meta:   http://localhost:6000"
echo ""
echo "=============================================="
echo "                 TESTING GUIDE"
echo "=============================================="
echo ""
echo "1. Write data:"
echo "   curl -X POST http://localhost:7000/write/testdb \\"
echo "     -d 'cpu,host=server1 value=0.5'"
echo ""
echo "2. Query data:"
echo "   curl 'http://localhost:7000/query/testdb?sql=SELECT+*+FROM+cpu'"
echo ""
echo "3. Check node modes (Primary/Standby):"
echo "   curl http://localhost:7000/nodes | jq '.[] | {id: .node_id, state: .state}'"
echo ""
echo "4. Check scale-in recommendations:"
echo "   curl 'http://localhost:6000/cluster/scale-in/recommend?primaries=1'"
echo ""
echo "5. Retire a node (scale-in):"
echo "   curl -X POST http://localhost:6000/nodes/data-4/retire"
echo ""
echo "6. Check retirement status:"
echo "   curl http://localhost:6000/nodes/data-4/retire/status"
echo ""
echo "7. Test failover (kill a Primary):"
echo "   goreman run stop data1"
echo ""
echo "8. Cancel retirement:"
echo "   curl -X POST http://localhost:6000/nodes/data-4/retire/cancel"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# Start with goreman
goreman -f ./dev/Procfile.coldstandby start
