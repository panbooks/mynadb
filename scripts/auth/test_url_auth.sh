#!/bin/bash

# Test script for URL parameter authentication (u and p parameters)
# Tests query parameter auth in addition to header-based auth

set -e

# Disable proxy to avoid 502 errors
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

BASE_URL="http://localhost:7000"
DB="testdb"
USERNAME="admin"
PASSWORD="admin2026"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

print_section() {
    echo ""
    echo "=========================================="
    echo -e "${BLUE}$1${NC}"
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

print_info() {
    echo -e "${BLUE}INFO:${NC} $1"
}

test_url() {
    local description=$1
    local url=$2
    local expected_status=$3
    local method=${4:-GET}
    local data=$5

    print_test "$description"

    local curl_cmd="curl -s -w '\n%{http_code}' -X $method"

    if [ -n "$data" ]; then
        curl_cmd="$curl_cmd -d '$data'"
    fi

    curl_cmd="$curl_cmd '$url'"

    local response=$(eval $curl_cmd)
    local status=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | head -n-1)

    if [ "$status" = "$expected_status" ]; then
        print_pass "Expected $expected_status, got $status"
        return 0
    else
        print_fail "Expected $expected_status, got $status. Response: $(echo "$body" | head -1)"
        return 1
    fi
}

check_service() {
    if ! curl -s "$BASE_URL/health" > /dev/null 2>&1; then
        echo "Error: Service is not running at $BASE_URL"
        echo "Please start the service first with: cargo run --release"
        exit 1
    fi
}

print_section "URL Parameter Authentication Test Suite"
print_info "Testing u= and p= query parameters for authentication"
print_info "Service: $BASE_URL"
print_info "Username: $USERNAME"
print_info "Password: $PASSWORD"

check_service

# ==========================================
# Test 1: Public Endpoints (Should Always Work)
# ==========================================
print_section "Test 1: Public Endpoints (No Auth Required)"

test_url \
    "Health endpoint without params" \
    "$BASE_URL/health" \
    "200"

test_url \
    "Health endpoint with auth params (should be ignored)" \
    "$BASE_URL/health?u=$USERNAME&p=$PASSWORD" \
    "200"

test_url \
    "Stats endpoint without params" \
    "$BASE_URL/stats" \
    "200"

# ==========================================
# Test 2: Protected Endpoints with URL Auth
# ==========================================
print_section "Test 2: Protected Endpoints with URL Parameters (Auth Disabled)"

echo "Note: These tests assume auth.enabled=false in config.toml"
echo "      All should return 200 regardless of credentials"
echo ""

# Write test
data="cpu,host=server01,region=us-west value=23.5 $(date +%s)000000000"
test_url \
    "Write with URL params (auth disabled)" \
    "$BASE_URL/write/$DB?u=$USERNAME&p=$PASSWORD" \
    "200" \
    "POST" \
    "$data"

# Query test
test_url \
    "Query with URL params (auth disabled)" \
    "$BASE_URL/query/$DB?u=$USERNAME&p=$PASSWORD&q=SELECT+1" \
    "200"

# Tables test
test_url \
    "Tables with URL params (auth disabled)" \
    "$BASE_URL/tables/$DB?u=$USERNAME&p=$PASSWORD" \
    "200"

# ==========================================
# Test 3: URL Auth vs Header Auth
# ==========================================
print_section "Test 3: Comparing URL Auth vs Header Auth"

# Test with header auth
status_header=$(curl -s -w "%{http_code}" -o /dev/null -u $USERNAME:$PASSWORD "$BASE_URL/tables/$DB")
echo -e "${YELLOW}TEST:${NC} Header auth status: $status_header"

# Test with URL auth
status_url=$(curl -s -w "%{http_code}" -o /dev/null "$BASE_URL/tables/$DB?u=$USERNAME&p=$PASSWORD")
echo -e "${YELLOW}TEST:${NC} URL param auth status: $status_url"

if [ "$status_header" = "$status_url" ]; then
    print_pass "Header and URL auth behave the same ($status_header)"
else
    print_fail "Header ($status_header) and URL ($status_url) auth differ"
fi

# ==========================================
# Test 4: Wrong Credentials via URL
# ==========================================
print_section "Test 4: Wrong Credentials via URL Parameters"

echo "Note: These will pass (200) if auth is disabled in config"
echo ""

test_url \
    "Write with wrong password" \
    "$BASE_URL/write/$DB?u=$USERNAME&p=wrongpass" \
    "401" \
    "POST" \
    "$data"

test_url \
    "Query with wrong username" \
    "$BASE_URL/query/$DB?u=hacker&p=$PASSWORD&q=SELECT+1" \
    "401"

test_url \
    "Tables with both credentials wrong" \
    "$BASE_URL/tables/$DB?u=bad&p=credentials" \
    "401"

# ==========================================
# Test 5: Partial Credentials
# ==========================================
print_section "Test 5: Partial or Missing Credentials"

test_url \
    "Only username provided" \
    "$BASE_URL/tables/$DB?u=$USERNAME" \
    "401"

test_url \
    "Only password provided" \
    "$BASE_URL/tables/$DB?p=$PASSWORD" \
    "401"

test_url \
    "No credentials at all" \
    "$BASE_URL/tables/$DB" \
    "401"

# ==========================================
# Test 6: URL Encoding
# ==========================================
print_section "Test 6: URL Encoding in Credentials"

print_info "Testing with special characters that need URL encoding"

# Test with URL-encoded password
url_encoded_pass=$(printf '%s' "$PASSWORD" | jq -sRr @uri 2>/dev/null || echo "$PASSWORD")
test_url \
    "URL-encoded password" \
    "$BASE_URL/tables/$DB?u=$USERNAME&p=$url_encoded_pass" \
    "200"

# Test with special characters (if password changes)
test_url \
    "Password with special chars (encoded)" \
    "$BASE_URL/tables/$DB?u=test&p=p%40ss%26word%21" \
    "401"  # Will fail as these aren't real credentials

# ==========================================
# Test 7: Combined with Other Query Params
# ==========================================
print_section "Test 7: Auth Params Mixed with Query Parameters"

test_url \
    "Query with auth and SQL params" \
    "$BASE_URL/query/$DB?u=$USERNAME&p=$PASSWORD&q=SELECT+COUNT(*)+FROM+cpu" \
    "200"

test_url \
    "Query with auth params in different order" \
    "$BASE_URL/query/$DB?q=SELECT+1&u=$USERNAME&p=$PASSWORD" \
    "200"

# ==========================================
# Test 8: Fallback to Header Auth
# ==========================================
print_section "Test 8: Header Auth as Fallback"

print_info "If URL params are invalid/missing, should try header auth"

# No URL params, but valid header
status=$(curl -s -w "%{http_code}" -o /dev/null -u $USERNAME:$PASSWORD "$BASE_URL/tables/$DB")
print_test "Valid header auth (no URL params)"
if [ "$status" = "200" ]; then
    print_pass "Header auth worked as fallback (status: $status)"
else
    print_fail "Header auth should work as fallback (status: $status)"
fi

# Wrong URL params, but valid header
status=$(curl -s -w "%{http_code}" -o /dev/null -u $USERNAME:$PASSWORD "$BASE_URL/tables/$DB?u=wrong&p=creds")
print_test "Wrong URL params, valid header"
if [ "$status" = "200" ]; then
    print_pass "Header auth used when URL params are wrong (status: $status)"
else
    print_fail "Should fall back to header auth (status: $status)"
fi

# ==========================================
# Summary
# ==========================================
echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "Passed: ${GREEN}${TESTS_PASSED}${NC}"
echo -e "Failed: ${RED}${TESTS_FAILED}${NC}"
echo "Total:  $((TESTS_PASSED + TESTS_FAILED))"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}✓ All tests passed!${NC}"
    exit_code=0
else
    echo -e "\n${RED}✗ Some tests failed!${NC}"
    exit_code=1
fi

# Usage examples
echo ""
echo "=========================================="
echo "URL Parameter Authentication Examples"
echo "=========================================="
echo ""
echo "Write data with URL auth:"
echo "  curl \"$BASE_URL/write/$DB?u=$USERNAME&p=$PASSWORD\" \\"
echo "    -X POST -d 'cpu,host=server01 value=23.5'"
echo ""
echo "Query with URL auth:"
echo "  curl \"$BASE_URL/query/$DB?u=$USERNAME&p=$PASSWORD&q=SELECT+*+FROM+cpu+LIMIT+10\""
echo ""
echo "List tables with URL auth:"
echo "  curl \"$BASE_URL/tables/$DB?u=$USERNAME&p=$PASSWORD\""
echo ""
echo "Note: Header-based auth still works:"
echo "  curl -u $USERNAME:$PASSWORD \"$BASE_URL/tables/$DB\""
echo ""

exit $exit_code
