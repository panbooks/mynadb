#!/bin/bash
# Fetch profile and analyze with go tool pprof
#
# Usage:
#   ./analyze_profile.sh [ENDPOINT] [DURATION] [SERVER]
#
# Arguments:
#   ENDPOINT  - Profile type: heap or cpu (default: heap)
#   DURATION  - Profile duration in seconds (default: 10)
#   SERVER    - Server URL (default: http://localhost:7000)
#
# Examples:
#   ./analyze_profile.sh heap 30
#   ./analyze_profile.sh cpu 60 http://production-server:7000

set -e

ENDPOINT=${1:-heap}
DURATION=${2:-10}
SERVER=${3:-http://localhost:7000}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PROFILE_FILE="/tmp/profile_${ENDPOINT}_${TIMESTAMP}.pb"

echo "═══════════════════════════════════════════════════════"
echo "  Memory Profiling Analysis"
echo "═══════════════════════════════════════════════════════"
echo "  Endpoint:  ${ENDPOINT}"
echo "  Duration:  ${DURATION} seconds"
echo "  Server:    ${SERVER}"
echo "  Output:    ${PROFILE_FILE}"
echo "═══════════════════════════════════════════════════════"
echo ""

echo "Fetching ${ENDPOINT} profile for ${DURATION} seconds..."
echo "(This will take ${DURATION} seconds to complete)"
echo ""

if curl -s "${SERVER}/debug/${ENDPOINT}?duration=${DURATION}&format=protobuf" -o "$PROFILE_FILE"; then
    FILE_SIZE=$(ls -lh "$PROFILE_FILE" | awk '{print $5}')
    echo "✓ Profile saved to: $PROFILE_FILE (${FILE_SIZE})"
    echo ""
    echo "───────────────────────────────────────────────────────"
    echo "Analyzing with go tool pprof..."
    echo "───────────────────────────────────────────────────────"
    echo ""

    # Generate text report
    go tool pprof -text "$PROFILE_FILE"

    echo ""
    echo "═══════════════════════════════════════════════════════"
    echo "  Additional Analysis Options"
    echo "═══════════════════════════════════════════════════════"
    echo ""
    echo "1. Interactive web UI (recommended):"
    echo "   go tool pprof -http=:8080 $PROFILE_FILE"
    echo ""
    echo "2. Top 20 functions:"
    echo "   go tool pprof -top $PROFILE_FILE"
    echo ""
    echo "3. Call graph:"
    echo "   go tool pprof -png $PROFILE_FILE > /tmp/profile.png"
    echo ""
    echo "4. Flamegraph:"
    echo "   go tool pprof -http=:8080 $PROFILE_FILE"
    echo "   (then select 'Flame Graph' from the VIEW menu)"
    echo ""
else
    echo "✗ Failed to fetch profile"
    echo ""
    echo "Possible issues:"
    echo "  - Server not running on ${SERVER}"
    echo "  - Endpoint /debug/${ENDPOINT} not available"
    echo "  - Authentication required"
    echo ""
    exit 1
fi
