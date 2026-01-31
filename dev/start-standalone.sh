#!/bin/bash
#
# Start MynaDB in standalone mode for local development
#
# Usage:
#   ./dev/start-standalone.sh         # Default: local storage
#   ./dev/start-standalone.sh local   # Local filesystem storage
#   ./dev/start-standalone.sh minio   # MinIO (S3-compatible) storage
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Parse mode argument (default: local)
MODE="${1:-local}"

case "$MODE" in
    local)
        CONFIG="./dev/configs/standalone/local.toml"
        PROCFILE="./dev/Procfile.standalone-local"
        STORAGE_DESC="Local filesystem"
        ;;
    minio)
        CONFIG="./dev/configs/standalone/minio.toml"
        PROCFILE="./dev/Procfile.standalone-minio"
        STORAGE_DESC="MinIO (S3-compatible)"
        ;;
    *)
        echo "Usage: $0 [local|minio]"
        echo ""
        echo "Modes:"
        echo "  local  - Use local filesystem storage (default)"
        echo "  minio  - Use MinIO (S3-compatible) storage"
        exit 1
        ;;
esac

echo "=== MynaDB Standalone Development Environment ==="
echo ""
echo "Storage: $STORAGE_DESC"
echo ""

# Build debug binary
echo "Building MynaDB (debug mode)..."
cargo build

if [ ! -f "./target/debug/mynadb" ]; then
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
echo "Starting standalone instance ($MODE mode)..."
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

if [ "$MODE" = "minio" ]; then
    echo "MinIO requirements:"
    echo "  - MinIO running on localhost:9000"
    echo "  - Bucket 'mynadb' created"
    echo ""
fi

echo "Press Ctrl+C to stop"
echo ""

# Check if goreman is available
if command -v goreman &> /dev/null; then
    goreman -f "$PROCFILE" start
else
    # Run directly without goreman
    ./target/debug/mynadb -c "$CONFIG" standalone
fi
