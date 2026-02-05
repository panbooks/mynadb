#!/bin/bash
# Memory allocation analysis using pprof
#
# This script generates load, captures a heap profile, and analyzes it with go tool pprof
#
# Usage:
#   ./analyze_memory.sh [DURATION] [SERVER]
#
# Arguments:
#   DURATION  - Profile duration in seconds (default: 10)
#   SERVER    - Server URL (default: http://localhost:7000)
#
# Requirements:
#   - go tool pprof (part of Go installation)
#   - Stats service running
#
# Examples:
#   ./analyze_memory.sh 30
#   ./analyze_memory.sh 60 http://production-server:7000

set -e

DURATION=${1:-10}
SERVER=${2:-http://localhost:7000}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PROFILE_FILE="/tmp/heap_profile_${TIMESTAMP}.pb"

echo "═══════════════════════════════════════════════════════"
echo "  Memory Allocation Profiling with Load"
echo "═══════════════════════════════════════════════════════"
echo "  Duration:  ${DURATION} seconds"
echo "  Server:    ${SERVER}"
echo "  Output:    ${PROFILE_FILE}"
echo "═══════════════════════════════════════════════════════"
echo ""

# Check if service is running
if ! curl -s "${SERVER}/health" > /dev/null 2>&1; then
    echo "✗ Error: Service not responding at ${SERVER}"
    echo "  Make sure the stats service is running"
    exit 1
fi

echo "Step 1: Generating write load..."
echo "  Writing test data to create memory allocations..."

# Write test data to generate memory activity
for i in {1..100}; do
    TIMESTAMP_NS=$(($(date +%s) * 1000000000 + RANDOM))
    curl -s -X POST "${SERVER}/write/testdb" \
        -d "cpu,host=server${i},region=us-east usage=$((RANDOM % 100)),temp=$((RANDOM % 50)) ${TIMESTAMP_NS}" > /dev/null 2>&1 &
done

wait
echo "  ✓ Wrote 100 metrics"
echo ""

echo "Step 2: Starting heap profiling (${DURATION} seconds)..."
echo "  Profiling in background while generating load..."

# Start profiling in background
curl -s "${SERVER}/debug/heap?duration=${DURATION}&format=protobuf" -o "$PROFILE_FILE" &
PROFILE_PID=$!

# Wait a moment for profiler to start
sleep 2

echo "Step 3: Generating query load during profiling..."

# Generate query load during profiling
LOAD_DURATION=$((DURATION - 3))
for i in $(seq 1 $LOAD_DURATION); do
    # Multiple query types
    curl -s "${SERVER}/query/testdb?sql=SELECT * FROM cpu LIMIT 10" > /dev/null 2>&1 &
    curl -s "${SERVER}/query/testdb?sql=SELECT COUNT(*) FROM cpu" > /dev/null 2>&1 &
    curl -s "${SERVER}/query/testdb?sql=SELECT host, AVG(usage) FROM cpu GROUP BY host" > /dev/null 2>&1 &

    # Some writes
    TIMESTAMP_NS=$(($(date +%s) * 1000000000 + RANDOM))
    curl -s -X POST "${SERVER}/write/testdb" \
        -d "mem,host=server$i usage=$((RANDOM % 100)) ${TIMESTAMP_NS}" > /dev/null 2>&1 &

    sleep 1
done

# Wait for profiling to complete
wait $PROFILE_PID

echo "  ✓ Profiling complete"
echo ""

# Check if profile was generated
if [ ! -f "$PROFILE_FILE" ]; then
    echo "✗ Error: Profile file not created"
    exit 1
fi

FILE_SIZE=$(ls -lh "$PROFILE_FILE" | awk '{print $5}')
echo "✓ Profile saved: $PROFILE_FILE (${FILE_SIZE})"
echo ""

# Check if go tool pprof is available
if ! command -v go &> /dev/null; then
    echo "⚠ Warning: go tool pprof not found"
    echo "  Install Go to analyze the profile:"
    echo "    https://golang.org/dl/"
    echo ""
    echo "  Profile saved at: $PROFILE_FILE"
    echo "  Copy to a machine with Go installed to analyze"
    exit 0
fi

echo "───────────────────────────────────────────────────────"
echo "  Profile Analysis - Top Memory Allocators"
echo "───────────────────────────────────────────────────────"
echo ""

# Generate text report
go tool pprof -text "$PROFILE_FILE" 2>/dev/null | head -40

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Analysis Options"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "1. Interactive Web UI (Recommended):"
echo "   go tool pprof -http=:8080 $PROFILE_FILE"
echo ""
echo "   This opens a web interface where you can:"
echo "   - View flamegraphs"
echo "   - Explore call graphs"
echo "   - Search for specific functions"
echo "   - Compare profiles"
echo ""
echo "2. Top 50 allocators:"
echo "   go tool pprof -top50 $PROFILE_FILE"
echo ""
echo "3. Generate flamegraph SVG:"
echo "   go tool pprof -svg $PROFILE_FILE > /tmp/heap_flamegraph.svg"
echo ""
echo "4. Show allocations by source:"
echo "   go tool pprof -list=main $PROFILE_FILE"
echo ""
echo "5. Compare two profiles:"
echo "   go tool pprof -base=profile1.pb profile2.pb"
echo ""
echo "───────────────────────────────────────────────────────"
echo "  Understanding the Output"
echo "───────────────────────────────────────────────────────"
echo ""
echo "  flat:  Memory allocated by this function"
echo "  cum:   Memory allocated by this function + callees"
echo ""
echo "  Focus on functions with high 'flat' values for"
echo "  direct memory allocations, or high 'cum' values"
echo "  for functions that trigger many allocations."
echo ""
echo "───────────────────────────────────────────────────────"
echo "  Next Steps"
echo "───────────────────────────────────────────────────────"
echo ""
echo "1. Run profiling during peak load:"
echo "   ./analyze_memory.sh 60  # 60 second profile"
echo ""
echo "2. Compare different time periods:"
echo "   # Baseline profile"
echo "   curl \"${SERVER}/debug/heap?duration=30&format=protobuf\" -o baseline.pb"
echo "   # Profile during high load"
echo "   curl \"${SERVER}/debug/heap?duration=30&format=protobuf\" -o highload.pb"
echo "   # Compare"
echo "   go tool pprof -base=baseline.pb highload.pb"
echo ""
echo "3. For bloom filter memory analysis:"
echo "   ./analyze_bloom_memory.sh"
echo ""
