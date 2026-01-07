#!/bin/bash
# Quick test to generate load and see profiling data

SERVER=${1:-http://localhost:7000}

echo "Generating load and profiling..."

# Start profiling in background
curl -s "${SERVER}/debug/heap?duration=5&format=text" > /tmp/profile_result.txt &
PROFILE_PID=$!

# Generate continuous load during profiling
echo "Writing data and querying..."
for i in {1..100}; do
    # Writes
    curl -s -X POST "${SERVER}/write/testdb" \
        -d "cpu,host=h${i} value=${i} $(($(date +%s)*1000000000))" > /dev/null 2>&1 &

    # Queries
    curl -s "${SERVER}/query/testdb?sql=SELECT * FROM cpu LIMIT 10" > /dev/null 2>&1 &

    # Stats calls
    curl -s "${SERVER}/stats" > /dev/null 2>&1 &
done

# Wait for profiling to complete
wait $PROFILE_PID

# Show results
cat /tmp/profile_result.txt
