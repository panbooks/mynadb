#!/bin/bash
# Test InfluxQL Query Language Support
# Requires a running MynaDB server

set -e

HOST="${HOST:-localhost}"
PORT="${PORT:-7000}"
BASE_URL="http://${HOST}:${PORT}"
DB="influxql_test"
PASSED=0
FAILED=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "============================================"
echo "InfluxQL Query Language Tests"
echo "============================================"
echo "Base URL: $BASE_URL"
echo ""

# Check server
if ! curl -s "${BASE_URL}/health" > /dev/null 2>&1; then
    echo -e "${RED}Error: MynaDB server is not running at ${BASE_URL}${NC}"
    exit 1
fi

run_test() {
    local name="$1"
    local expected="$2"
    local actual="$3"

    if [[ "$actual" == *"$expected"* ]]; then
        echo -e "${GREEN}  PASS${NC} $name"
        ((PASSED++))
    else
        echo -e "${RED}  FAIL${NC} $name"
        echo "    Expected to contain: $expected"
        echo "    Got: $actual"
        ((FAILED++))
    fi
}

run_test_code() {
    local name="$1"
    local expected="$2"
    local actual="$3"

    if [[ "$actual" == "$expected" ]]; then
        echo -e "${GREEN}  PASS${NC} $name"
        ((PASSED++))
    else
        echo -e "${RED}  FAIL${NC} $name"
        echo "    Expected: $expected"
        echo "    Got: $actual"
        ((FAILED++))
    fi
}

# ===== Setup: Write test data =====
echo "--- Setup: Writing test data ---"

# Write some data points
DATA="cpu,host=server01,region=us-west usage=23.5,temp=45.0 1704067200000000000
cpu,host=server01,region=us-west usage=55.2,temp=47.0 1704067260000000000
cpu,host=server02,region=us-east usage=12.1,temp=38.0 1704067200000000000
cpu,host=server02,region=us-east usage=15.3,temp=39.0 1704067260000000000
mem,host=server01,region=us-west free=1024.0,used=2048.0 1704067200000000000
mem,host=server01,region=us-west free=980.0,used=2092.0 1704067260000000000"

WRITE_RESULT=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE_URL}/write/${DB}" -d "$DATA")
run_test_code "Write test data" "204" "$WRITE_RESULT"

# Flush to ensure data is queryable
curl -s -X POST "${BASE_URL}/flush" > /dev/null 2>&1
sleep 1

echo ""
echo "--- InfluxQL SELECT Queries ---"

# Test 1: Simple SELECT *
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20*%20FROM%20cpu")
run_test "SELECT * FROM cpu" "results" "$RESULT"

# Test 2: SELECT specific fields
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20usage%20FROM%20cpu")
run_test "SELECT usage FROM cpu" "results" "$RESULT"

# Test 3: SELECT with WHERE
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20*%20FROM%20cpu%20WHERE%20host%20%3D%20'server01'")
run_test "SELECT with WHERE host='server01'" "results" "$RESULT"

# Test 4: SELECT with LIMIT
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20*%20FROM%20cpu%20LIMIT%202")
run_test "SELECT with LIMIT" "results" "$RESULT"

# Test 5: Aggregate function - mean
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20mean(usage)%20FROM%20cpu")
run_test "SELECT mean(usage) FROM cpu" "results" "$RESULT"

# Test 6: Aggregate function - count
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20count(usage)%20FROM%20cpu")
run_test "SELECT count(usage) FROM cpu" "results" "$RESULT"

# Test 7: Aggregate function - sum, min, max
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20sum(usage)%2C%20min(usage)%2C%20max(usage)%20FROM%20cpu")
run_test "SELECT sum, min, max" "results" "$RESULT"

echo ""
echo "--- InfluxQL with db in path ---"

# Test 8: Query with db in path
RESULT=$(curl -s "${BASE_URL}/influxql/query/${DB}?q=SELECT%20*%20FROM%20cpu%20LIMIT%201")
run_test "GET /influxql/query/:db" "results" "$RESULT"

echo ""
echo "--- InfluxQL POST queries ---"

# Test 9: POST with query in body
RESULT=$(curl -s -X POST "${BASE_URL}/influxql/query?db=${DB}" -d "q=SELECT%20*%20FROM%20cpu%20LIMIT%201")
run_test "POST /influxql/query (form body)" "results" "$RESULT"

echo ""
echo "--- InfluxQL SHOW Queries ---"

# Test 10: SHOW MEASUREMENTS
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SHOW%20MEASUREMENTS")
run_test "SHOW MEASUREMENTS" "results" "$RESULT"

# Test 11: SHOW DATABASES
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SHOW%20DATABASES")
run_test "SHOW DATABASES" "results" "$RESULT"

echo ""
echo "--- InfluxQL Error Handling ---"

# Test 12: Invalid query
RESULT=$(curl -s -w "\n%{http_code}" "${BASE_URL}/influxql/query?db=${DB}&q=INVALID%20QUERY")
HTTP_CODE=$(echo "$RESULT" | tail -1)
run_test_code "Invalid query returns 400" "400" "$HTTP_CODE"

# Test 13: Response format check
RESULT=$(curl -s "${BASE_URL}/influxql/query?db=${DB}&q=SELECT%20*%20FROM%20cpu%20LIMIT%201")
run_test "Response has series" "series" "$RESULT"
run_test "Response has columns" "columns" "$RESULT"

echo ""
echo "============================================"
echo "Test Summary"
echo "============================================"
echo -e "Passed: ${GREEN}${PASSED}${NC}"
echo -e "Failed: ${RED}${FAILED}${NC}"
echo ""

if [ $FAILED -gt 0 ]; then
    exit 1
fi

echo -e "${GREEN}All tests passed!${NC}"
