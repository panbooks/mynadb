#!/bin/bash

# Comprehensive Authentication Test Script
# Tests authentication with both enabled and disabled states
# Requires service restart capability

set -e

# Disable proxy to avoid 502 errors
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

BASE_URL="http://localhost:7000"
DB="auth_test_db"
CONFIG_FILE="config.toml"
BACKUP_CONFIG="config.toml.auth_test_backup"

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

cleanup() {
    if [ -f "$BACKUP_CONFIG" ]; then
        echo ""
        print_info "Restoring original config..."
        mv "$BACKUP_CONFIG" "$CONFIG_FILE"
    fi
}

trap cleanup EXIT

check_config_exists() {
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "Error: $CONFIG_FILE not found!"
        echo "Please run this script from the stats-service root directory"
        exit 1
    fi
}

backup_config() {
    print_info "Backing up config.toml..."
    cp "$CONFIG_FILE" "$BACKUP_CONFIG"
}

set_auth_enabled() {
    local enabled=$1
    print_info "Setting auth.enabled = $enabled in config..."

    # Use sed to modify the config
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s/^enabled = .*/enabled = $enabled/" "$CONFIG_FILE"
    else
        # Linux
        sed -i "s/^enabled = .*/enabled = $enabled/" "$CONFIG_FILE"
    fi
}

wait_for_service() {
    local max_attempts=30
    local attempt=0

    print_info "Waiting for service to be ready..."

    while [ $attempt -lt $max_attempts ]; do
        if curl -s "$BASE_URL/health" > /dev/null 2>&1; then
            print_info "Service is ready!"
            sleep 1  # Give it one more second to fully initialize
            return 0
        fi
        sleep 1
        ((attempt++))
    done

    echo "Error: Service failed to start within $max_attempts seconds"
    return 1
}

test_http() {
    local description=$1
    local method=$2
    local endpoint=$3
    local auth=$4
    local expected_status=$5
    local data=$6

    local curl_cmd="curl -s -w '\n%{http_code}' -X $method"

    if [ -n "$auth" ]; then
        curl_cmd="$curl_cmd -u $auth"
    fi

    if [ -n "$data" ]; then
        curl_cmd="$curl_cmd -d '$data'"
    fi

    curl_cmd="$curl_cmd '$BASE_URL$endpoint'"

    local response=$(eval $curl_cmd 2>&1)
    local status=$(echo "$response" | tail -n1)

    if [ "$status" = "$expected_status" ]; then
        print_pass "$description (expected $expected_status, got $status)"
        return 0
    else
        print_fail "$description (expected $expected_status, got $status)"
        return 1
    fi
}

# ==========================================
# Main Test Suite
# ==========================================

print_section "Comprehensive Authentication Test Suite"

check_config_exists
backup_config

# ==========================================
# Phase 1: Auth Disabled
# ==========================================
print_section "Phase 1: Testing with Authentication DISABLED"

set_auth_enabled "false"

print_info "Please restart the service with the updated config"
print_info "Press Enter when service is ready..."
read -r

if ! wait_for_service; then
    echo "Service not available. Exiting."
    exit 1
fi

echo ""
print_info "Testing public endpoints (should always work)..."
test_http "Health endpoint (no auth)" "GET" "/health" "" "200"
test_http "Stats endpoint (no auth)" "GET" "/stats" "" "200"

echo ""
print_info "Testing protected endpoints WITHOUT auth (should work when disabled)..."
write_data="cpu,host=server01,region=us-west value=23.5 $(date +%s)000000000"
test_http "Write endpoint (no auth, auth disabled)" "POST" "/write/$DB" "" "200" "$write_data"
test_http "Query endpoint (no auth, auth disabled)" "GET" "/query/$DB?q=SELECT+1" "" "200"
test_http "Tables endpoint (no auth, auth disabled)" "GET" "/tables/$DB" "" "200"
test_http "Flush endpoint (no auth, auth disabled)" "POST" "/flush" "" "200"

echo ""
print_info "Testing protected endpoints WITH auth (should still work)..."
test_http "Write endpoint (with auth, auth disabled)" "POST" "/write/$DB" "admin:admin2026" "200" "$write_data"
test_http "Query endpoint (with auth, auth disabled)" "GET" "/query/$DB?q=SELECT+1" "admin:admin2026" "200"

# ==========================================
# Phase 2: Auth Enabled
# ==========================================
print_section "Phase 2: Testing with Authentication ENABLED"

set_auth_enabled "true"

print_info "Please restart the service with the updated config"
print_info "Press Enter when service is ready..."
read -r

if ! wait_for_service; then
    echo "Service not available. Exiting."
    exit 1
fi

echo ""
print_info "Testing public endpoints (should still work without auth)..."
test_http "Health endpoint (no auth, auth enabled)" "GET" "/health" "" "200"
test_http "Stats endpoint (no auth, auth enabled)" "GET" "/stats" "" "200"

echo ""
print_info "Testing public endpoints with WRONG auth (should still work)..."
test_http "Health endpoint (wrong auth, auth enabled)" "GET" "/health" "wrong:credentials" "200"
test_http "Stats endpoint (wrong auth, auth enabled)" "GET" "/stats" "wrong:credentials" "200"

echo ""
print_info "Testing protected endpoints WITHOUT auth (should be rejected)..."
test_http "Write endpoint (no auth, auth enabled)" "POST" "/write/$DB" "" "401" "$write_data"
test_http "Query endpoint (no auth, auth enabled)" "GET" "/query/$DB?q=SELECT+1" "" "401"
test_http "Tables endpoint (no auth, auth enabled)" "GET" "/tables/$DB" "" "401"
test_http "Flush endpoint (no auth, auth enabled)" "POST" "/flush" "" "401"

echo ""
print_info "Testing protected endpoints WITH WRONG auth (should be rejected)..."
test_http "Write endpoint (wrong password)" "POST" "/write/$DB" "admin:wrongpass" "401" "$write_data"
test_http "Write endpoint (wrong username)" "POST" "/write/$DB" "wronguser:admin2026" "401" "$write_data"
test_http "Query endpoint (wrong auth)" "GET" "/query/$DB?q=SELECT+1" "hacker:password" "401"
test_http "Tables endpoint (wrong auth)" "GET" "/tables/$DB" "bad:credentials" "401"

echo ""
print_info "Testing protected endpoints WITH CORRECT auth (should work)..."
test_http "Write endpoint (correct auth)" "POST" "/write/$DB" "admin:admin2026" "200" "$write_data"
test_http "Query endpoint (correct auth)" "GET" "/query/$DB?q=SELECT+1" "admin:admin2026" "200"
test_http "Tables endpoint (correct auth)" "GET" "/tables/$DB" "admin:admin2026" "200"
test_http "Flush endpoint (correct auth)" "POST" "/flush" "admin:admin2026" "200"

# ==========================================
# Phase 3: Advanced Test Cases
# ==========================================
print_section "Phase 3: Advanced Test Cases"

echo ""
print_info "Testing edge cases with correct auth..."
test_http "Compact endpoint" "POST" "/compact/$DB" "admin:admin2026" "200"

# Test schema endpoint
test_http "Schema listing" "GET" "/tables/$DB" "admin:admin2026" "200"

# Test query with POST method
query_data='{"query": "SELECT COUNT(*) FROM cpu"}'
test_http "Query via POST (correct auth)" "POST" "/query/$DB" "admin:admin2026" "200" "$query_data"

echo ""
print_info "Testing edge cases without auth (should fail)..."
test_http "Compact endpoint (no auth)" "POST" "/compact/$DB" "" "401"
test_http "Query via POST (no auth)" "POST" "/query/$DB" "" "401" "$query_data"

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
    exit 0
else
    echo -e "\n${RED}✗ Some tests failed!${NC}"
    exit 1
fi
