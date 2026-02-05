#!/bin/bash
# Direct flamegraph generation (no go tool needed)
#
# Usage:
#   ./generate_flamegraph.sh [ENDPOINT] [DURATION] [SERVER]
#
# Arguments:
#   ENDPOINT  - Profile type: heap or cpu (default: heap)
#   DURATION  - Profile duration in seconds (default: 10)
#   SERVER    - Server URL (default: http://localhost:7000)
#
# Examples:
#   ./generate_flamegraph.sh heap 30
#   ./generate_flamegraph.sh cpu 60 http://production-server:7000

set -e

ENDPOINT=${1:-heap}
DURATION=${2:-10}
SERVER=${3:-http://localhost:7000}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT="/tmp/flamegraph_${ENDPOINT}_${TIMESTAMP}.svg"

echo "═══════════════════════════════════════════════════════"
echo "  Flamegraph Generation"
echo "═══════════════════════════════════════════════════════"
echo "  Endpoint:  ${ENDPOINT}"
echo "  Duration:  ${DURATION} seconds"
echo "  Server:    ${SERVER}"
echo "  Output:    ${OUTPUT}"
echo "═══════════════════════════════════════════════════════"
echo ""

echo "Generating ${ENDPOINT} flamegraph for ${DURATION} seconds..."
echo "(This will take ${DURATION} seconds to complete)"
echo ""

if curl -s "${SERVER}/debug/${ENDPOINT}?duration=${DURATION}&format=flamegraph" -o "$OUTPUT"; then
    FILE_SIZE=$(ls -lh "$OUTPUT" | awk '{print $5}')
    echo "✓ Flamegraph saved to: $OUTPUT (${FILE_SIZE})"
    echo ""
    echo "───────────────────────────────────────────────────────"
    echo "  How to View"
    echo "───────────────────────────────────────────────────────"
    echo ""
    echo "1. Open in browser:"
    echo "   firefox $OUTPUT"
    echo "   # or"
    echo "   google-chrome $OUTPUT"
    echo "   # or"
    echo "   open $OUTPUT  # macOS"
    echo ""
    echo "2. Direct file URL:"
    echo "   file://$OUTPUT"
    echo ""
    echo "3. Copy to local machine (if running remotely):"
    echo "   scp <server>:$OUTPUT ~/Downloads/"
    echo ""
    echo "───────────────────────────────────────────────────────"
    echo "  Understanding the Flamegraph"
    echo "───────────────────────────────────────────────────────"
    echo ""
    echo "- Width represents time/samples (wider = more time spent)"
    echo "- Height represents call stack depth"
    echo "- Colors are random (no semantic meaning)"
    echo "- Click boxes to zoom in"
    echo "- Search for function names with Ctrl+F"
    echo ""
else
    echo "✗ Failed to generate flamegraph"
    echo ""
    echo "Possible issues:"
    echo "  - Server not running on ${SERVER}"
    echo "  - Endpoint /debug/${ENDPOINT} not available"
    echo "  - Authentication required"
    echo ""
    exit 1
fi
