#!/bin/bash
#
# Start MynaDB in standalone mode for local development
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== MynaDB Standalone Development Environment ==="
echo ""

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
mkdir -p ./dev/data/standalone
mkdir -p ./dev/logs/standalone

echo ""
echo "Starting standalone instance..."
echo ""
echo "Port assignments:"
echo "  HTTP:   7000"
echo "  Flight: 7001"
echo "  MySQL:  7002"
echo ""
echo "Access points:"
echo "  HTTP:   http://localhost:7000"
echo "  Flight: localhost:7001"
echo "  MySQL:  localhost:7002"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Check if goreman is available
if command -v goreman &> /dev/null; then
    goreman -f ./dev/Procfile.standalone start
else
    # Run directly without goreman
    ./target/release/mynadb --role standalone -c ./dev/configs/standalone.toml
fi
