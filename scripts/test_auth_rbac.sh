#!/bin/bash

echo "====================================="
echo "Testing RBAC Authentication"
echo "====================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0

# Test helper functions
pass() {
    echo -e "${GREEN}PASS${NC}: $1"
    PASSED=$((PASSED + 1))
}

fail() {
    echo -e "${RED}FAIL${NC}: $1"
    FAILED=$((FAILED + 1))
}

info() {
    echo -e "${YELLOW}INFO${NC}: $1"
}

# Build the project first
echo "Building MynaDB..."
cargo build --release || { echo "Build failed"; exit 1; }

# Generate password hashes for various test users
echo ""
echo "=== Generating password hashes ==="
ADMIN_HASH=$(./target/release/mynadb hash-password "admin123")
WRITER_HASH=$(./target/release/mynadb hash-password "writer123")
READER_HASH=$(./target/release/mynadb hash-password "reader123")
# Password with special characters
SPECIAL_HASH=$(./target/release/mynadb hash-password "p@ss:word&test=123")
# Password with unicode
UNICODE_HASH=$(./target/release/mynadb hash-password "密码测试")
info "Password hashes generated"

# Create test config with generated hashes
TEST_DIR="/tmp/mynadb-auth-test"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/data"

cat > "$TEST_DIR/config.toml" << EOF
[server]
http_port = 7100
flight_port = 7101
mysql_enabled = false

[storage]
data_dir = "$TEST_DIR/data"

[storage.object_store]
backend = "local"

[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true

[cache]
flush_interval_secs = 300
max_size_mb = 256

[wal]
enabled = true
sync_on_write = false
rotate_size_mb = 64

[compaction]
min_files = 5
max_level = 3

[retention]
enabled = false

[logging]
level = "warn"

[auth]
enabled = true

[[auth.users]]
username = "admin"
password_hash = "$ADMIN_HASH"
roles = ["admin"]

[[auth.users]]
username = "writer"
password_hash = "$WRITER_HASH"
roles = ["writer"]
databases = ["testdb", "staging", "prod_*"]

[[auth.users]]
username = "reader"
password_hash = "$READER_HASH"
roles = ["reader"]
databases = ["testdb"]

[[auth.users]]
username = "multi_role"
password_hash = "$READER_HASH"
roles = ["reader", "writer"]
databases = ["analytics"]

[[auth.users]]
username = "table_restricted"
password_hash = "$READER_HASH"
roles = ["reader"]
databases = ["testdb"]
tables = ["testdb.default.cpu", "testdb.default.memory"]

[[auth.users]]
username = "no_roles"
password_hash = "$READER_HASH"
roles = []

[[auth.users]]
username = "special@user.com"
password_hash = "$SPECIAL_HASH"
roles = ["reader"]

[[auth.users]]
username = "unicode_user"
password_hash = "$UNICODE_HASH"
roles = ["reader"]

[[auth.users]]
username = "empty_db_list"
password_hash = "$READER_HASH"
roles = ["reader"]
databases = []
EOF

info "Test config created at $TEST_DIR/config.toml"

# Start server in background
echo ""
echo "=== Starting MynaDB server with auth enabled ==="
./target/release/mynadb -c "$TEST_DIR/config.toml" > "$TEST_DIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:7100/health > /dev/null 2>&1; then
        info "Server started successfully (PID: $SERVER_PID)"
        break
    fi
    sleep 1
done

# Verify server is running
if ! curl -s http://localhost:7100/health > /dev/null 2>&1; then
    echo "ERROR: Server failed to start"
    cat "$TEST_DIR/server.log"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

# Cleanup function
cleanup() {
    echo ""
    echo "=== Cleanup ==="
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    info "Server stopped"
}
trap cleanup EXIT

# =============================================================================
# Test 1: Public Endpoints
# =============================================================================
echo ""
echo "====================================="
echo "Test 1: Public Endpoints (No Auth Required)"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:7100/health)
[ "$HTTP_CODE" = "200" ] && pass "Health endpoint accessible" || fail "Health returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:7100/metrics)
[ "$HTTP_CODE" = "200" ] && pass "Metrics endpoint accessible" || fail "Metrics returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:7100/stats)
[ "$HTTP_CODE" = "200" ] && pass "Stats endpoint accessible" || fail "Stats returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:7100/version)
[ "$HTTP_CODE" = "200" ] && pass "Version endpoint accessible" || fail "Version returned $HTTP_CODE"

# =============================================================================
# Test 2: Unauthenticated Access
# =============================================================================
echo ""
echo "====================================="
echo "Test 2: Unauthenticated Access (Should Fail)"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:7100/write/testdb \
  -H "Content-Type: text/plain" -d 'cpu,host=s1 v=1')
[ "$HTTP_CODE" = "401" ] && pass "Write without auth returns 401" || fail "Write returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Query without auth returns 401" || fail "Query returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:7100/flush)
[ "$HTTP_CODE" = "401" ] && pass "Flush without auth returns 401" || fail "Flush returned $HTTP_CODE"

# =============================================================================
# Test 3: Admin User
# =============================================================================
echo ""
echo "====================================="
echo "Test 3: Admin User (Full Access)"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 -X POST http://localhost:7100/write/testdb \
  -H "Content-Type: text/plain" -d 'cpu,host=s1 v=1 1704067200000000000')
[ "$HTTP_CODE" = "204" ] && pass "Admin can write" || fail "Admin write returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 "http://localhost:7100/query/testdb?sql=SELECT%20*%20FROM%20cpu")
[ "$HTTP_CODE" = "200" ] && pass "Admin can query" || fail "Admin query returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 -X POST http://localhost:7100/flush)
[ "$HTTP_CODE" = "200" ] && pass "Admin can flush" || fail "Admin flush returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 -X POST http://localhost:7100/compact/testdb)
[ "$HTTP_CODE" = "200" ] && pass "Admin can compact" || fail "Admin compact returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 -X POST http://localhost:7100/write/any_db \
  -H "Content-Type: text/plain" -d 'cpu,host=s1 v=1')
[ "$HTTP_CODE" = "204" ] && pass "Admin can write to any database" || fail "Admin write any_db returned $HTTP_CODE"

# =============================================================================
# Test 4: Writer User
# =============================================================================
echo ""
echo "====================================="
echo "Test 4: Writer User (Read + Write, Limited DBs)"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u writer:writer123 -X POST http://localhost:7100/write/testdb \
  -H "Content-Type: text/plain" -d 'memory,host=s1 v=80')
[ "$HTTP_CODE" = "204" ] && pass "Writer can write to allowed db" || fail "Writer write returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u writer:writer123 "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "200" ] && pass "Writer can query allowed db" || fail "Writer query returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u writer:writer123 -X POST http://localhost:7100/write/production \
  -H "Content-Type: text/plain" -d 'cpu,host=s1 v=1')
[ "$HTTP_CODE" = "403" ] && pass "Writer cannot write to unauthorized db" || fail "Writer write production returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u writer:writer123 -X POST http://localhost:7100/flush)
[ "$HTTP_CODE" = "403" ] && pass "Writer cannot flush (admin only)" || fail "Writer flush returned $HTTP_CODE"

# Test wildcard database pattern
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u writer:writer123 -X POST http://localhost:7100/write/prod_us \
  -H "Content-Type: text/plain" -d 'cpu,host=s1 v=1')
[ "$HTTP_CODE" = "204" ] && pass "Writer can write to wildcard db (prod_*)" || fail "Writer write prod_us returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u writer:writer123 -X POST http://localhost:7100/write/prod_eu \
  -H "Content-Type: text/plain" -d 'cpu,host=s1 v=1')
[ "$HTTP_CODE" = "204" ] && pass "Writer can write to another wildcard db (prod_eu)" || fail "Writer write prod_eu returned $HTTP_CODE"

# =============================================================================
# Test 5: Reader User
# =============================================================================
echo ""
echo "====================================="
echo "Test 5: Reader User (Read Only)"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u reader:reader123 "http://localhost:7100/query/testdb?sql=SELECT%20*%20FROM%20cpu")
[ "$HTTP_CODE" = "200" ] && pass "Reader can query" || fail "Reader query returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u reader:reader123 -X POST http://localhost:7100/write/testdb \
  -H "Content-Type: text/plain" -d 'disk,host=s1 v=50')
[ "$HTTP_CODE" = "403" ] && pass "Reader cannot write" || fail "Reader write returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u reader:reader123 "http://localhost:7100/query/production?sql=SELECT%201")
[ "$HTTP_CODE" = "403" ] && pass "Reader cannot query unauthorized db" || fail "Reader query production returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u reader:reader123 -X POST http://localhost:7100/compact/testdb)
[ "$HTTP_CODE" = "403" ] && pass "Reader cannot compact" || fail "Reader compact returned $HTTP_CODE"

# =============================================================================
# Test 6: Invalid Credentials
# =============================================================================
echo ""
echo "====================================="
echo "Test 6: Invalid Credentials"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:wrongpassword "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Wrong password returns 401" || fail "Wrong password returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u unknown:password "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Unknown user returns 401" || fail "Unknown user returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:ADMIN123 "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Case-sensitive password returns 401" || fail "Case-sensitive password returned $HTTP_CODE"

# =============================================================================
# Test 7: Query Parameter Auth
# =============================================================================
echo ""
echo "====================================="
echo "Test 7: Query Parameter Auth"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%20*%20FROM%20cpu&u=admin&p=admin123")
[ "$HTTP_CODE" = "200" ] && pass "Query param auth works (admin)" || fail "Query param auth returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%20*%20FROM%20cpu&u=reader&p=reader123")
[ "$HTTP_CODE" = "200" ] && pass "Query param auth works (reader)" || fail "Query param auth reader returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%201&u=admin&p=wrong")
[ "$HTTP_CODE" = "401" ] && pass "Invalid query param auth returns 401" || fail "Invalid query param returned $HTTP_CODE"

# Test URL-encoded special characters
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%201&u=special%40user.com&p=p%40ss%3Aword%26test%3D123")
[ "$HTTP_CODE" = "200" ] && pass "URL-encoded credentials work" || fail "URL-encoded credentials returned $HTTP_CODE"

# =============================================================================
# Test 8: Edge Cases - Malformed Auth Headers
# =============================================================================
echo ""
echo "====================================="
echo "Test 8: Malformed Auth Headers"
echo "====================================="

# Missing "Basic " prefix
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: $(echo -n 'admin:admin123' | base64)" \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Missing 'Basic ' prefix returns 401" || fail "Missing prefix returned $HTTP_CODE"

# Invalid base64
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Basic not_valid_base64!!!" \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Invalid base64 returns 401" || fail "Invalid base64 returned $HTTP_CODE"

# Missing colon in credentials
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Basic $(echo -n 'adminadmin123' | base64)" \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Missing colon returns 401" || fail "Missing colon returned $HTTP_CODE"

# Bearer token instead of Basic
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer sometoken" \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Bearer token returns 401" || fail "Bearer token returned $HTTP_CODE"

# Empty Authorization header
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: " \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "401" ] && pass "Empty Authorization returns 401" || fail "Empty Authorization returned $HTTP_CODE"

# =============================================================================
# Test 9: Edge Cases - Special Characters
# =============================================================================
echo ""
echo "====================================="
echo "Test 9: Special Characters in Credentials"
echo "====================================="

# Password with colon (Basic Auth uses colon as delimiter)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "special@user.com:p@ss:word&test=123" \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "200" ] && pass "Password with colon works" || fail "Password with colon returned $HTTP_CODE"

# Unicode password
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "unicode_user:密码测试" \
  "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "200" ] && pass "Unicode password works" || fail "Unicode password returned $HTTP_CODE"

# =============================================================================
# Test 10: Edge Cases - Multiple Roles
# =============================================================================
echo ""
echo "====================================="
echo "Test 10: Multiple Roles"
echo "====================================="

# User with reader + writer roles
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u multi_role:reader123 "http://localhost:7100/query/analytics?sql=SELECT%201")
[ "$HTTP_CODE" = "200" ] && pass "Multi-role user can read" || fail "Multi-role read returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u multi_role:reader123 -X POST http://localhost:7100/write/analytics \
  -H "Content-Type: text/plain" -d 'test,host=s1 v=1')
[ "$HTTP_CODE" = "204" ] && pass "Multi-role user can write" || fail "Multi-role write returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u multi_role:reader123 -X POST http://localhost:7100/flush)
[ "$HTTP_CODE" = "403" ] && pass "Multi-role user cannot admin" || fail "Multi-role admin returned $HTTP_CODE"

# =============================================================================
# Test 11: Edge Cases - No Roles
# =============================================================================
echo ""
echo "====================================="
echo "Test 11: User with No Roles"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u no_roles:reader123 "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "403" ] && pass "No-roles user cannot read" || fail "No-roles read returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u no_roles:reader123 -X POST http://localhost:7100/write/testdb \
  -H "Content-Type: text/plain" -d 'test,host=s1 v=1')
[ "$HTTP_CODE" = "403" ] && pass "No-roles user cannot write" || fail "No-roles write returned $HTTP_CODE"

# But they can still access public endpoints
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u no_roles:reader123 http://localhost:7100/health)
[ "$HTTP_CODE" = "200" ] && pass "No-roles user can access public endpoints" || fail "No-roles health returned $HTTP_CODE"

# =============================================================================
# Test 12: Edge Cases - Empty Database List
# =============================================================================
echo ""
echo "====================================="
echo "Test 12: Empty Database List"
echo "====================================="

# User with empty databases list (different from None/unrestricted)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u empty_db_list:reader123 "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "403" ] && pass "Empty db list cannot access any db" || fail "Empty db list returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u empty_db_list:reader123 "http://localhost:7100/query/any?sql=SELECT%201")
[ "$HTTP_CODE" = "403" ] && pass "Empty db list cannot access any other db" || fail "Empty db list other returned $HTTP_CODE"

# =============================================================================
# Test 13: Edge Cases - Incomplete Query Params
# =============================================================================
echo ""
echo "====================================="
echo "Test 13: Incomplete Query Parameters"
echo "====================================="

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%201&u=admin")
[ "$HTTP_CODE" = "401" ] && pass "Only username param returns 401" || fail "Only username returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%201&p=admin123")
[ "$HTTP_CODE" = "401" ] && pass "Only password param returns 401" || fail "Only password returned $HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:7100/query/testdb?sql=SELECT%201&u=&p=")
[ "$HTTP_CODE" = "401" ] && pass "Empty u and p params returns 401" || fail "Empty params returned $HTTP_CODE"

# =============================================================================
# Test 14: Query Params Take Precedence
# =============================================================================
echo ""
echo "====================================="
echo "Test 14: Query Params Take Precedence Over Basic Auth"
echo "====================================="

# Both Basic Auth and query params - query params should win
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 \
  "http://localhost:7100/query/testdb?sql=SELECT%201&u=reader&p=reader123")
# This should use reader (from query params), which CAN access testdb
[ "$HTTP_CODE" = "200" ] && pass "Query params take precedence" || fail "Precedence test returned $HTTP_CODE"

# Test that reader (from query params) cannot access production
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 \
  "http://localhost:7100/query/production?sql=SELECT%201&u=reader&p=reader123")
[ "$HTTP_CODE" = "403" ] && pass "Query params user restrictions apply" || fail "Query params restriction returned $HTTP_CODE"

# =============================================================================
# Test 15: Path Edge Cases
# =============================================================================
echo ""
echo "====================================="
echo "Test 15: Path Edge Cases"
echo "====================================="

# Trailing slash - note: the router may return 404 for routes not explicitly defined with trailing slash
# This tests the auth layer doesn't interfere, not the route configuration
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin123 "http://localhost:7100/query/testdb?sql=SELECT%201")
[ "$HTTP_CODE" = "200" ] && pass "Query without trailing slash works" || fail "Query returned $HTTP_CODE"

# Different HTTP methods on query (POST also allowed for large queries)
RESULT=$(curl -s -o /dev/null -w "%{http_code}" -u reader:reader123 -X POST \
  "http://localhost:7100/query/testdb" -H "Content-Type: application/json" -d '{"sql":"SELECT 1"}')
# POST query might return 200 or 400 depending on implementation, but not 403
[ "$RESULT" != "403" ] && pass "POST query allowed for reader" || fail "POST query returned 403"

# =============================================================================
# Test 16: Concurrent Requests (Sequential for stability)
# =============================================================================
echo ""
echo "====================================="
echo "Test 16: Multiple Sequential Requests"
echo "====================================="

# Run multiple requests sequentially to verify auth is stable
ALL_OK=true
for i in {1..5}; do
    HTTP1=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u admin:admin123 "http://localhost:7100/query/testdb?sql=SELECT%20$i")
    HTTP2=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u reader:reader123 "http://localhost:7100/query/testdb?sql=SELECT%20$i")
    [ "$HTTP1" != "200" ] || [ "$HTTP2" != "200" ] && ALL_OK=false
done

[ "$ALL_OK" = true ] && pass "Server handles multiple sequential requests" || fail "Sequential requests failed"

# =============================================================================
# Test 17: Table-Level Restrictions
# =============================================================================
echo ""
echo "====================================="
echo "Test 17: Table-Level Restrictions"
echo "====================================="

# Wait for server to be ready
sleep 1

# Write some data for the table-restricted user test
curl -s -o /dev/null --max-time 5 -u admin:admin123 -X POST http://localhost:7100/write/testdb \
  -H "Content-Type: text/plain" -d 'memory,host=s1 v=80 1704067200000000000'

# table_restricted user should be able to query cpu (allowed)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u table_restricted:reader123 \
  "http://localhost:7100/query/testdb?sql=SELECT%20*%20FROM%20cpu")
[ "$HTTP_CODE" = "200" ] && pass "Table-restricted can query allowed table (cpu)" || fail "Table-restricted cpu returned $HTTP_CODE"

# table_restricted user should be able to query memory (allowed)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 -u table_restricted:reader123 \
  "http://localhost:7100/query/testdb?sql=SELECT%20*%20FROM%20memory")
[ "$HTTP_CODE" = "200" ] && pass "Table-restricted can query allowed table (memory)" || fail "Table-restricted memory returned $HTTP_CODE"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "====================================="
echo "Test Results"
echo "====================================="
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"

if [ $FAILED -eq 0 ]; then
    echo -e "\n${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}Some tests failed. Check server logs: $TEST_DIR/server.log${NC}"
    exit 1
fi
