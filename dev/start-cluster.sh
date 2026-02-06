#!/bin/bash
#
# Start MynaDB cluster for local development
# Requires: goreman (go install github.com/mattn/goreman@latest)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== MynaDB Cluster Development Environment ==="
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

# Build debug binary if it doesn't exist
if [ ! -f "./target/debug/mynadb" ]; then
    echo "Building MynaDB (debug mode)..."
    cargo build
    if [ ! -f "./target/debug/mynadb" ]; then
        echo "ERROR: Build failed - binary not found"
        exit 1
    fi
    echo "Build complete!"
else
    echo "Using existing binary: ./target/debug/mynadb"
fi
echo ""

# Create data and log directories
echo "Creating data directories..."
mkdir -p ./dev/data/cluster/meta1
mkdir -p ./dev/data/cluster/meta2
mkdir -p ./dev/data/cluster/meta3
mkdir -p ./dev/data/cluster/data1
mkdir -p ./dev/data/cluster/data2
mkdir -p ./dev/logs/cluster/meta1
mkdir -p ./dev/logs/cluster/meta2
mkdir -p ./dev/logs/cluster/meta3
mkdir -p ./dev/logs/cluster/router1
mkdir -p ./dev/logs/cluster/data1
mkdir -p ./dev/logs/cluster/data2

echo ""
echo "Starting cluster..."
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
echo "  Data nodes:"
echo "    data1:   HTTP 5000, Flight 5001"
echo "    data2:   HTTP 5100, Flight 5101"
echo ""
echo "  Admin:"
echo "    admin:   HTTP 18080"
echo ""
echo "Access points:"
echo "  HTTP API:   http://localhost:7000"
echo "  Flight:     localhost:7001"
echo "  MySQL:      localhost:7002"
echo "  Meta:       http://localhost:6000"
echo "  Admin UI:   http://localhost:18080/admin"
echo ""
echo "Note: Build the admin UI first with:"
echo "  cd admin-ui && npm install && npm run build"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# Start with goreman
goreman -f ./dev/Procfile start
