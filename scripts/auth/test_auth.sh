#!/bin/bash

# Test script for HTTP Basic Authentication
# Tests various endpoints with and without authentication

set -e

# Disable proxy to avoid 502 errors
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

BASE_URL="http://localhost:7000"
DB="testdb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

print_section() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

print_test() {
    echo -e "${YELLOW}TEST:${NC} $1"
}

print_pass() {
    echo -e "${GREEN}✓ PASS:${NC} $1"
    ((TESTS_PASSED++))
}

print_fail() {
    echo -e "${RED}✗ FAIL:${NC} $1"
    ((TESTS_FAILED++))
}

print_summary() {
    echo ""
    echo "=========================================="
    echo "Test Summary"
    echo "=========================================="
    echo -e "Passed: ${GREEN}${TESTS_PASSED}${NC}"
    echo -e "Failed: ${RED}${TESTS_FAILED}${NC}"
    echo "Total:  $((TESTS_PASSED + TESTS_FAILED))"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "\n${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "\n${RED}Some tests failed!${NC}"
        return 1
    fi
}

# Test helper function
test_endpoint() {
    local description=$1
    local method=$2
    local endpoint=$3
    local data=$4
    local auth=$5
    local expected_status=$6

    print_test "$description"

    local curl_cmd="curl -s -w '\n%{http_code}' -X $method"

    if [ -n "$auth" ]; then
        curl_cmd="$curl_cmd -u $auth"
    fi

    if [ -n "$data" ]; then
        curl_cmd="$curl_cmd -H 'Content-Type: application/json' -d '$data'"
    fi

    curl_cmd="$curl_cmd $BASE_URL$endpoint"

    local response=$(eval $curl_cmd)
    local status=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | head -n-1)

    if [ "$status" = "$expected_status" ]; then
        print_pass "Expected status $expected_status, got $status"
    else
        print_fail "Expected status $expected_status, got $status. Response: $body"
    fi
}

# Test write endpoint with line protocol
test_write() {
    local description=$1
    local auth=$2
    local expected_status=$3

    print_test "$description"

    local data="cpu,host=server01,region=us-west value=23.5 $(date +%s)000000000"

    local curl_cmd="curl -s -w '\n%{http_code}' -X POST"

    if [ -n "$auth" ]; then
        curl_cmd="$curl_cmd -u $auth"
    fi

    curl_cmd="$curl_cmd -d '$data' $BASE_URL/write/$DB"

    local response=$(eval $curl_cmd)
    local status=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | head -n-1)

    if [ "$status" = "$expected_status" ]; then
        print_pass "Expected status $expected_status, got $status"
    else
        print_fail "Expected status $expected_status, got $status. Response: $body"
    fi
}

# Wait for service to be ready
wait_for_service() {
    echo "Waiting for service to be ready..."
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -s "$BASE_URL/health" > /dev/null 2>&1; then
            echo "Service is ready!"
            return 0
        fi
        sleep 1
        ((attempt++))
    done

    echo "Service failed to start within $max_attempts seconds"
    return 1
}

# Check if service is running
check_service() {
    if ! curl -s "$BASE_URL/health" > /dev/null 2>&1; then
        echo "Error: Service is not running at $BASE_URL"
        echo "Please start the service first with: cargo run --release"
        exit 1
    fi
}

print_section "Authentication Test Suite"

check_service

# ==========================================
# Test 1: Public Endpoints (Always Accessible)
# ==========================================
print_section "Test 1: Public Endpoints (No Auth Required)"

test_endpoint \
    "Health endpoint without auth" \
    "GET" \
    "/health" \
    "" \
    "" \
    "200"

test_endpoint \
    "Stats endpoint without auth" \
    "GET" \
    "/stats" \
    "" \
    "" \
    "200"

# ==========================================
# Test 2: Protected Endpoints (Auth Disabled)
# ==========================================
print_section "Test 2: Protected Endpoints (Auth Currently Disabled in Config)"

echo "Note: If auth.enabled=false in config.toml, all endpoints should work without auth"

test_write \
    "Write endpoint without auth (auth disabled)" \
    "" \
    "200"

test_endpoint \
    "Query endpoint without auth (auth disabled)" \
    "GET" \
    "/query/$DB?q=SELECT+*+FROM+cpu+LIMIT+1" \
    "" \
    "" \
    "200"

test_endpoint \
    "Tables endpoint without auth (auth disabled)" \
    "GET" \
    "/tables/$DB" \
    "" \
    "" \
    "200"

test_endpoint \
    "Flush endpoint without auth (auth disabled)" \
    "POST" \
    "/flush" \
    "" \
    "" \
    "200"

# ==========================================
# Test 3: Test with Correct Credentials
# ==========================================
print_section "Test 3: Endpoints with Correct Credentials"

echo "Testing with username 'admin' and password 'admin2026' (from config)"

test_write \
    "Write endpoint with correct credentials" \
    "admin:admin2026" \
    "200"

test_endpoint \
    "Query endpoint with correct credentials" \
    "GET" \
    "/query/$DB?q=SELECT+*+FROM+cpu+LIMIT+1" \
    "" \
    "admin:admin2026" \
    "200"

test_endpoint \
    "Tables endpoint with correct credentials" \
    "GET" \
    "/tables/$DB" \
    "" \
    "admin:admin2026" \
    "200"

# ==========================================
# Test 4: Test with Wrong Credentials
# ==========================================
print_section "Test 4: Endpoints with Wrong Credentials"

echo "Note: These tests will fail if auth is disabled in config"

test_write \
    "Write endpoint with wrong password" \
    "admin:wrongpassword" \
    "401"

test_write \
    "Write endpoint with wrong username" \
    "wronguser:admin2026" \
    "401"

test_endpoint \
    "Query endpoint with wrong credentials" \
    "GET" \
    "/query/$DB?q=SELECT+*+FROM+cpu+LIMIT+1" \
    "" \
    "hacker:password123" \
    "401"

# ==========================================
# Test 5: Public Endpoints (Always Work)
# ==========================================
print_section "Test 5: Public Endpoints Should Work Even with Wrong Auth"

test_endpoint \
    "Health endpoint with wrong credentials (should still work)" \
    "GET" \
    "/health" \
    "" \
    "wronguser:wrongpass" \
    "200"

test_endpoint \
    "Stats endpoint with wrong credentials (should still work)" \
    "GET" \
    "/stats" \
    "" \
    "wronguser:wrongpass" \
    "200"

# ==========================================
# Test 6: Various Write Operations
# ==========================================
print_section "Test 6: Various Write Operations with Auth"

print_test "Write multiple metrics with auth"
curl_data="cpu,host=server01 value=23.5 $(date +%s)000000000
memory,host=server01 value=67.8 $(date +%s)000000000
disk,host=server01 value=89.2 $(date +%s)000000000"

response=$(curl -s -w '\n%{http_code}' -X POST -u admin:admin2026 -d "$curl_data" "$BASE_URL/write/$DB")
status=$(echo "$response" | tail -n1)

if [ "$status" = "200" ]; then
    print_pass "Multiple metrics write succeeded"
else
    print_fail "Multiple metrics write failed with status $status"
fi

# ==========================================
# Test 7: Query Operations with Auth
# ==========================================
print_section "Test 7: Query Operations with Auth"

test_endpoint \
    "Query with table listing" \
    "GET" \
    "/tables/$DB" \
    "" \
    "admin:admin2026" \
    "200"

# Try to query the data we just wrote
sleep 1  # Wait for flush

test_endpoint \
    "Query written data" \
    "GET" \
    "/query/$DB?q=SELECT+COUNT(*)+FROM+cpu" \
    "" \
    "admin:admin2026" \
    "200"

# ==========================================
# Test 8: Special Cases
# ==========================================
print_section "Test 8: Special Cases"

test_endpoint \
    "Empty auth header" \
    "GET" \
    "/query/$DB?q=SELECT+1" \
    "" \
    ":" \
    "401"

test_endpoint \
    "Malformed auth (no colon)" \
    "GET" \
    "/query/$DB?q=SELECT+1" \
    "" \
    "adminadmin2026" \
    "401"

# ==========================================
# Summary
# ==========================================
print_summary

# Instructions for enabling auth
echo ""
echo "=========================================="
echo "How to Enable Authentication"
echo "=========================================="
echo "1. Edit config.toml"
echo "2. Set [auth].enabled = true"
echo "3. Restart the service"
echo "4. Run this script again to verify auth is enforced"
echo ""
echo "Expected behavior when auth is enabled:"
echo "  - /health and /stats: Always accessible (200)"
echo "  - /write, /query, /tables, etc without auth: Rejected (401)"
echo "  - /write, /query, /tables, etc with correct auth: Allowed (200)"
echo "  - All endpoints with wrong auth: Rejected (401)"
