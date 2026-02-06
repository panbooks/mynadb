#!/bin/bash
set -e

echo "====================================="
echo "Testing Query Tracing & Profiling"
echo "====================================="

# Start server in background
echo "Starting MynaDB server..."
cargo run --release > /tmp/mynadb.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:7000/health > /dev/null 2>&1; then
        echo "Server started successfully!"
        break
    fi
    sleep 1
done

# Verify server is running
if ! curl -s http://localhost:7000/health > /dev/null 2>&1; then
    echo "ERROR: Server failed to start"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "=== Step 1: Write test data ==="
curl -X POST http://localhost:7000/write/testdb \
  -H "Content-Type: text/plain" \
  -d 'cpu,host=server1 usage=50.0 1704067200000000000
cpu,host=server1 usage=60.0 1704067260000000000
cpu,host=server1 usage=70.0 1704067320000000000'

echo ""
echo "=== Step 2: Execute a test query ==="
curl -s "http://localhost:7000/query/testdb?sql=SELECT%20*%20FROM%20cpu" | head -20

echo ""
echo "=== Step 3: Check recent traces ==="
curl -s "http://localhost:7000/traces/recent?limit=5" | python3 -m json.tool 2>/dev/null || echo "Tracing not enabled"

echo ""
echo "=== Step 4: Check slow queries ==="
curl -s "http://localhost:7000/traces/slow?limit=5" | python3 -m json.tool 2>/dev/null || echo "Slow query logging not enabled"

echo ""
echo "=== Step 5: Execute a slower query ==="
curl -s "http://localhost:7000/query/testdb?sql=SELECT%20COUNT(*)%20FROM%20cpu" | head -20

echo ""
echo "=== Step 6: Check traces again ==="
curl -s "http://localhost:7000/traces/recent?limit=5" | python3 -m json.tool 2>/dev/null || echo "Tracing not enabled"

echo ""
echo "=== Cleanup ==="
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "====================================="
echo "Test completed!"
echo "====================================="
