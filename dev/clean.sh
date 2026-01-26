#!/bin/bash
#
# Clean up dev environment data and logs
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Cleaning dev environment..."

# Stop any running processes first
"$SCRIPT_DIR/stop-all.sh"

# Remove data directories
echo "Removing data directories..."
rm -rf "$SCRIPT_DIR/data"

# Remove log directories
echo "Removing log directories..."
rm -rf "$SCRIPT_DIR/logs"

echo "Clean complete!"
