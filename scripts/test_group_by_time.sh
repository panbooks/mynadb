#!/bin/bash
# Test script for GROUP BY time() functionality
#
# This script tests the InfluxDB-compatible GROUP BY time(interval) syntax.

set -e

# Configuration
HOST="${HOST:-localhost}"
HTTP_PORT="${HTTP_PORT:-7000}"
DB="testdb_groupby"
BASE_URL="http://${HOST}:${HTTP_PORT}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== GROUP BY time() Test Script ==="
echo "Base URL: $BASE_URL"
echo "Database: $DB"
echo ""

# Helper function to print test result
check_result() {
    local test_name="$1"
    local result="$2"
    local expected="$3"

    if echo "$result" | grep -q "$expected"; then
        echo -e "${GREEN}[PASS]${NC} $test_name"
        return 0
    else
        echo -e "${RED}[FAIL]${NC} $test_name"
        echo "Expected to contain: $expected"
        echo "Got: $result"
        return 1
    fi
}

# Helper function to make HTTP requests
query() {
    local sql="$1"
    curl -s "${BASE_URL}/query/${DB}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-urlencode "sql=${sql}"
}

write() {
    local data="$1"
    curl -s -X POST "${BASE_URL}/write/${DB}" \
        -H "Content-Type: text/plain" \
        -d "$data"
}

echo "=== Step 1: Write test data ==="

# Generate test data with timestamps every minute for 2 hours
# Using nanosecond timestamps
BASE_TS=1737504000000000000  # 2025-01-22 00:00:00 UTC in nanoseconds

# Write CPU metrics every 5 minutes for 2 hours
DATA=""
for i in $(seq 0 5 120); do
    TS=$((BASE_TS + i * 60000000000))  # Add i minutes in nanoseconds
    USAGE=$((50 + RANDOM % 50))  # Random usage between 50-100
    DATA="${DATA}cpu,host=server01 usage=${USAGE}.0 ${TS}\n"
done

echo -e "$DATA" | curl -s -X POST "${BASE_URL}/write/${DB}" \
    -H "Content-Type: text/plain" \
    --data-binary @-

echo "Wrote CPU metrics (25 data points over 2 hours)"

# Also write memory metrics
DATA=""
for i in $(seq 0 10 120); do
    TS=$((BASE_TS + i * 60000000000))
    USED=$((4000 + RANDOM % 4000))
    DATA="${DATA}memory,host=server01 used=${USED} ${TS}\n"
done

echo -e "$DATA" | curl -s -X POST "${BASE_URL}/write/${DB}" \
    -H "Content-Type: text/plain" \
    --data-binary @-

echo "Wrote memory metrics (13 data points over 2 hours)"

# Flush to ensure data is persisted
echo "Flushing data..."
curl -s -X POST "${BASE_URL}/flush" > /dev/null

# Wait for flush to complete
sleep 2

echo ""
echo "=== Step 2: Test basic queries (verify data is there) ==="

# Test basic SELECT
echo "Test: Basic SELECT * FROM cpu"
RESULT=$(query "SELECT * FROM cpu LIMIT 5")
echo "$RESULT" | head -20
echo ""

echo "Test: Count records"
RESULT=$(query "SELECT count(*) as cnt FROM cpu")
echo "$RESULT"
echo ""

echo ""
echo "=== Step 3: Test GROUP BY time() syntax ==="

# Test 3.1: Basic GROUP BY time(30m)
echo "Test 3.1: GROUP BY time(30m)"
RESULT=$(query "SELECT mean(usage) FROM cpu GROUP BY time(30m)")
echo "$RESULT" | head -20
echo ""

# Test 3.2: GROUP BY time(1h) with WHERE clause
echo "Test 3.2: GROUP BY time(1h) with WHERE clause"
RESULT=$(query "SELECT mean(usage) FROM cpu WHERE timestamp >= ${BASE_TS} GROUP BY time(1h)")
echo "$RESULT" | head -20
echo ""

# Test 3.3: GROUP BY time with offset
echo "Test 3.3: GROUP BY time(1h, 15m) - with 15 minute offset"
RESULT=$(query "SELECT mean(usage) FROM cpu GROUP BY time(1h, 15m)")
echo "$RESULT" | head -20
echo ""

# Test 3.4: GROUP BY time with additional columns
echo "Test 3.4: GROUP BY time(30m), host"
RESULT=$(query "SELECT mean(usage) FROM cpu GROUP BY time(30m), host")
echo "$RESULT" | head -20
echo ""

# Test 3.5: Multiple aggregates
echo "Test 3.5: Multiple aggregates - mean, min, max"
RESULT=$(query "SELECT mean(usage), min(usage), max(usage) FROM cpu GROUP BY time(30m)")
echo "$RESULT" | head -20
echo ""

# Test 3.6: sum and count
echo "Test 3.6: sum and count aggregates"
RESULT=$(query "SELECT sum(usage), count(usage) FROM cpu GROUP BY time(30m)")
echo "$RESULT" | head -20
echo ""

echo ""
echo "=== Step 4: Test fill options ==="

# Test 4.1: fill(null)
echo "Test 4.1: GROUP BY time(15m) fill(null)"
RESULT=$(query "SELECT mean(usage) FROM cpu GROUP BY time(15m) fill(null)")
echo "$RESULT" | head -30
echo ""

# Test 4.2: fill(0)
echo "Test 4.2: GROUP BY time(15m) fill(0)"
RESULT=$(query "SELECT mean(usage) FROM cpu GROUP BY time(15m) fill(0)")
echo "$RESULT" | head -30
echo ""

# Test 4.3: fill(previous)
echo "Test 4.3: GROUP BY time(15m) fill(previous)"
RESULT=$(query "SELECT mean(usage) FROM cpu GROUP BY time(15m) fill(previous)")
echo "$RESULT" | head -30
echo ""

echo ""
echo "=== Step 5: Test with ISO timestamp format ==="

# Test using ISO timestamp in WHERE clause
echo "Test 5.1: WHERE with ISO timestamp"
RESULT=$(query "SELECT mean(usage) FROM cpu WHERE timestamp >= '2025-01-22T00:00:00Z' GROUP BY time(30m)")
echo "$RESULT" | head -20
echo ""

echo "Test 5.2: WHERE with ISO timestamp range"
RESULT=$(query "SELECT mean(usage) FROM cpu WHERE timestamp >= '2025-01-22T00:00:00Z' AND timestamp < '2025-01-22T02:00:00Z' GROUP BY time(30m)")
echo "$RESULT" | head -20
echo ""

echo ""
echo "=== Step 6: Test memory table with GROUP BY time() ==="

echo "Test 6.1: GROUP BY time(30m) on memory table"
RESULT=$(query "SELECT mean(used) FROM memory GROUP BY time(30m)")
echo "$RESULT" | head -20
echo ""

echo "Test 6.2: GROUP BY time(1h, 15m) on memory table (with offset)"
RESULT=$(query "SELECT mean(used) FROM memory WHERE timestamp >= '2025-01-22T00:00:00Z' GROUP BY time(1h, 15m)")
echo "$RESULT" | head -20
echo ""

echo ""
echo "=== Step 7: Test EXPLAIN to verify transformation ==="

echo "Test 7.1: EXPLAIN GROUP BY time(30m)"
RESULT=$(curl -s "${BASE_URL}/explain/${DB}?sql=SELECT%20mean(usage)%20FROM%20cpu%20GROUP%20BY%20time(30m)")
echo "$RESULT" | head -30
echo ""

echo ""
echo "=== Step 8: Verify time_bucket UDF directly ==="

echo "Test 8.1: Direct time_bucket usage"
RESULT=$(query "SELECT time_bucket(timestamp, 1800000000000, 0) as bucket, avg(usage) FROM cpu GROUP BY time_bucket(timestamp, 1800000000000, 0) ORDER BY bucket")
echo "$RESULT" | head -20
echo ""

echo ""
echo "=== All tests completed ==="
