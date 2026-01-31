#!/bin/bash
#
# Start MynaDB Replica Set cluster for local development
# Tests warm standby replication with zone-aware placement
#
# Requires: goreman (go install github.com/mattn/goreman@latest)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== MynaDB Replica Set Development Environment ==="
echo ""
echo "This environment tests warm standby replication with:"
echo "  - 3 Meta nodes (Raft consensus)"
echo "  - 3 Data nodes (zone-a, zone-b, zone-c)"
echo "  - 1 Router node"
echo "  - Replication factor: 2 (leader + 1 standby)"
echo ""

# Check for goreman
if ! command -v goreman &> /dev/null; then
    echo "ERROR: goreman is not installed"
    echo ""
    echo "Install with:"
    echo "  go install github.com/mattn/goreman@latest"
    echo ""
    echo "Or use Homebrew on macOS:"
    echo "  brew install goreman"
    exit 1
fi

# Build binary if it doesn't exist (use debug for faster iteration, release for production)
BUILD_MODE="${BUILD_MODE:-debug}"
if [ "$BUILD_MODE" = "release" ]; then
    BINARY="./target/release/mynadb"
    if [ ! -f "$BINARY" ]; then
        echo "Building MynaDB (release mode)..."
        cargo build --release
        if [ ! -f "$BINARY" ]; then
            echo "ERROR: Build failed - binary not found"
            exit 1
        fi
        echo "Build complete!"
    else
        echo "Using existing binary: $BINARY"
    fi
else
    BINARY="./target/debug/mynadb"
    if [ ! -f "$BINARY" ]; then
        echo "Building MynaDB (debug mode)..."
        cargo build
        if [ ! -f "$BINARY" ]; then
            echo "ERROR: Build failed - binary not found"
            exit 1
        fi
        echo "Build complete!"
    else
        echo "Using existing binary: $BINARY"
    fi
fi
echo ""

# Create data and log directories
echo "Creating data directories..."
mkdir -p ./dev/data/replicaset/meta1
mkdir -p ./dev/data/replicaset/meta2
mkdir -p ./dev/data/replicaset/meta3
mkdir -p ./dev/data/replicaset/data1
mkdir -p ./dev/data/replicaset/data2
mkdir -p ./dev/data/replicaset/data3
mkdir -p ./dev/logs/replicaset/meta1
mkdir -p ./dev/logs/replicaset/meta2
mkdir -p ./dev/logs/replicaset/meta3
mkdir -p ./dev/logs/replicaset/router1
mkdir -p ./dev/logs/replicaset/data1
mkdir -p ./dev/logs/replicaset/data2
mkdir -p ./dev/logs/replicaset/data3

echo ""
echo "Starting replica set cluster..."
echo ""
echo "Port assignments:"
echo "  meta1:   gRPC 7100"
echo "  meta2:   gRPC 7101"
echo "  meta3:   gRPC 7102"
echo "  router1: HTTP 7000, Flight 7001, MySQL 7002"
echo "  data1:   gRPC 7200 (zone-a)"
echo "  data2:   gRPC 7201 (zone-b)"
echo "  data3:   gRPC 7202 (zone-c)"
echo ""
echo "Access points:"
echo "  HTTP:   http://localhost:7000"
echo "  Flight: localhost:7001"
echo "  MySQL:  localhost:7002"
echo ""
echo "Testing warm standby replication:"
echo ""
echo "  # Write data"
echo "  curl -X POST http://localhost:7000/write/testdb -d 'cpu,host=server1 value=0.5'"
echo ""
echo "  # Check cluster status"
echo "  curl http://localhost:7000/cluster/status"
echo ""
echo "  # Check partition replicas"
echo "  curl http://localhost:7000/cluster/partitions"
echo ""
echo "  # Kill a data node to test failover:"
echo "  goreman run stop data1"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# Start with goreman
goreman -f ./dev/Procfile.replicaset start
