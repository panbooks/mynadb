#!/bin/bash

# Test partition query after restarting service

echo "=== Testing partition query fix ==="

# Kill existing service
pkill -f stats-service || true
sleep 2

# Rebuild
cargo build --release
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Start service in background
./target/release/stats-service &
SERVICE_PID=$!
sleep 3

echo "Service started with PID: $SERVICE_PID"

# Test query with date partition
echo -e "\n=== Test 1: Query with date filter ==="
curl -s 'http://localhost:7000/query/benchmark' \
  -d "select * from svc_1.cpu where date='2025-12-24' limit 2"

# Test query without date filter
echo -e "\n\n=== Test 2: Query without date filter ==="
curl -s 'http://localhost:7000/query/benchmark' \
  -d "select * from svc_1.cpu limit 2"

# Check schema
echo -e "\n\n=== Test 3: Check table schema ==="
curl -s 'http://localhost:7000/query/benchmark' \
  -d "describe svc_1.cpu"

# Cleanup
echo -e "\n\n=== Stopping service ==="
kill $SERVICE_PID

echo "Done"
