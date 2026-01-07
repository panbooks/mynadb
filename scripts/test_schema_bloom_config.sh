#!/bin/bash
set -e

echo "========================================="
echo "Schema-Level Bloom Filter Configuration Test"
echo "========================================="
echo

# Clean up
echo "1. Cleaning up old data..."
rm -rf /tmp/test_schema_bloom
mkdir -p /tmp/test_schema_bloom

# Create test config with schema-specific bloom filter settings
cat > /tmp/test_schema_bloom/config.toml << 'EOF'
[server]
http_host = "127.0.0.1"
http_port = 7100
flight_host = "127.0.0.1"
flight_port = 7101

[storage]
data_dir = "/tmp/test_schema_bloom/data"
system_dir = "_system"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true

[cache]
flush_interval_secs = 5
max_size_mb = 100

[series_tracker]
track_only = false
flush_interval_secs = 3
system_tags = ["MONITOR.ip", "MONITOR.endpoint"]
cache_retention_days = 1
default_strategy = "bloom"
bloom_databases = ["testdb"]

[series_tracker.bloom]
expected_series_per_day = 10000
false_positive_rate = 0.01

# Schema-specific config: PMC has higher cardinality
[[series_tracker.bloom_custom]]
database = "testdb"
schema = "pmc"
expected_series_per_day = 100000
false_positive_rate = 0.01

# Schema-specific config: UMC has lower cardinality
[[series_tracker.bloom_custom]]
database = "testdb"
schema = "umc"
expected_series_per_day = 20000
false_positive_rate = 0.01

[logging]
level = "info"
log_dir = "/tmp/test_schema_bloom/logs"
file_prefix = "test"
rotation = "never"

[retention]
enabled = false

[wal]
enabled = false

[compaction]
interval_secs = 3600
min_files = 10

[auth]
enabled = false
EOF

echo "✓ Created config with schema-specific bloom settings"
echo "  - PMC: 100,000 expected series/day"
echo "  - UMC: 20,000 expected series/day"
echo

# Start service
echo "2. Starting service..."
cargo build --release > /dev/null 2>&1
../target/release/tsdb-stats -c /tmp/test_schema_bloom/config.toml > /tmp/test_schema_bloom/service.log 2>&1 &
SERVICE_PID=$!

# Wait for service to start
sleep 3
if ! kill -0 $SERVICE_PID 2>/dev/null; then
    echo "✗ Service failed to start"
    cat /tmp/test_schema_bloom/service.log
    exit 1
fi
echo "✓ Service started (PID: $SERVICE_PID)"
echo

# Write test data
echo "3. Writing test data..."
# Data with system tags - PMC and UMC will differ
curl -s -X POST "http://localhost:7100/write/testdb" \
  -H "Content-Type: text/plain" \
  --data-binary @- << 'DATA'
cpu,service=web,host=server1,MONITOR.ip=1.2.3.4,MONITOR.endpoint=api usage=50.0 1609459200000000000
cpu,service=web,host=server2,MONITOR.ip=1.2.3.5,MONITOR.endpoint=api usage=60.0 1609459200000000000
cpu,service=web,host=server1,MONITOR.ip=1.2.3.4,MONITOR.endpoint=web usage=70.0 1609459200000000000
memory,service=web,host=server1,MONITOR.ip=1.2.3.4 used=80.0 1609459200000000000
DATA

if [ $? -eq 0 ]; then
    echo "✓ Wrote 4 data points"
else
    echo "✗ Write failed"
    kill $SERVICE_PID 2>/dev/null
    exit 1
fi
echo

# Wait for series tracker flush
echo "4. Waiting for series tracker flush (5 seconds)..."
sleep 5
echo

# Check memory usage
echo "5. Checking memory usage..."
MEMORY_OUTPUT=$(curl -s http://localhost:7100/memory)
echo "$MEMORY_OUTPUT"
echo

# Verify schema-specific strategies are shown
if echo "$MEMORY_OUTPUT" | grep -q "testdb.*pmc"; then
    echo "✓ PMC schema strategy found in memory stats"
else
    echo "✗ PMC schema strategy NOT found"
fi

if echo "$MEMORY_OUTPUT" | grep -q "testdb.*umc"; then
    echo "✓ UMC schema strategy found in memory stats"
else
    echo "✗ UMC schema strategy NOT found"
fi
echo

# Query series stats
echo "6. Querying series stats..."
echo "PMC (includes all tags including MONITOR.*):"
PMC_RESULT=$(curl -s -X POST "http://localhost:7100/query/testdb" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT service, metric, COUNT(*) as series_count FROM _system.pmc.testdb GROUP BY service, metric ORDER BY service, metric"}' | python3 -m json.tool 2>/dev/null || echo "Query failed")
echo "$PMC_RESULT"
echo

echo "UMC (excludes system tags like MONITOR.*):"
UMC_RESULT=$(curl -s -X POST "http://localhost:7100/query/testdb" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT service, metric, COUNT(*) as series_count FROM _system.umc.testdb GROUP BY service, metric ORDER BY service, metric"}' | python3 -m json.tool 2>/dev/null || echo "Query failed")
echo "$UMC_RESULT"
echo

# Cleanup
echo "7. Cleaning up..."
kill $SERVICE_PID 2>/dev/null
wait $SERVICE_PID 2>/dev/null
echo "✓ Service stopped"
echo

echo "========================================="
echo "Test completed!"
echo "========================================="
echo
echo "Expected behavior:"
echo "- PMC should track more series (includes MONITOR.ip and MONITOR.endpoint variations)"
echo "- UMC should track fewer series (excludes MONITOR.* tags)"
echo "- Memory stats should show separate bloom filters for PMC and UMC schemas"
echo "- PMC bloom filter should use 100,000 expected series config"
echo "- UMC bloom filter should use 20,000 expected series config"
