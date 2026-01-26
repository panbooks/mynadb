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

# Build release binary
echo "Building MynaDB (release mode)..."
cargo build --release

if [ ! -f "./target/release/mynadb" ]; then
    echo "ERROR: Build failed - binary not found"
    exit 1
fi

echo "Build complete!"
echo ""

# Create data and log directories
echo "Creating data directories..."
mkdir -p ./dev/data/meta1
mkdir -p ./dev/data/meta2
mkdir -p ./dev/data/meta3
mkdir -p ./dev/data/data1
mkdir -p ./dev/data/data2
mkdir -p ./dev/logs/meta1
mkdir -p ./dev/logs/meta2
mkdir -p ./dev/logs/meta3
mkdir -p ./dev/logs/router1
mkdir -p ./dev/logs/data1
mkdir -p ./dev/logs/data2

echo ""
echo "Starting cluster..."
echo ""
echo "Port assignments:"
echo "  meta1:   gRPC 7100"
echo "  meta2:   gRPC 7101"
echo "  meta3:   gRPC 7102"
echo "  router1: HTTP 7000, Flight 7001, MySQL 7002"
echo "  data1:   gRPC 7200"
echo "  data2:   gRPC 7201"
echo ""
echo "Access points:"
echo "  HTTP:   http://localhost:7000"
echo "  Flight: localhost:7001"
echo "  MySQL:  localhost:7002"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# Start with goreman
goreman -f ./dev/Procfile start
