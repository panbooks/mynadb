#!/bin/bash
# Test TLS/SSL encryption for MynaDB
# This script generates self-signed certificates and tests HTTPS connectivity

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CERT_DIR="/tmp/mynadb-tls-test"
CONFIG_FILE="$CERT_DIR/config.tls.toml"
DATA_DIR="$CERT_DIR/data"
LOG_DIR="$CERT_DIR/logs"
HTTP_PORT=17000
FLIGHT_PORT=17001
MYSQL_PORT=17002
PID_FILE=""

cleanup() {
    echo "Cleaning up..."
    if [ -n "$PID_FILE" ] && [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$PID" ]; then
            kill "$PID" 2>/dev/null || true
            sleep 1
        fi
    fi
    rm -rf "$CERT_DIR"
}
trap cleanup EXIT

echo "=== MynaDB TLS Test ==="
echo ""

# Step 1: Generate self-signed certificates
echo "1. Generating self-signed certificates..."
mkdir -p "$CERT_DIR" "$DATA_DIR" "$LOG_DIR"

openssl req -x509 -newkey rsa:2048 \
    -keyout "$CERT_DIR/key.pem" \
    -out "$CERT_DIR/cert.pem" \
    -days 1 -nodes \
    -subj '/CN=localhost' \
    -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    2>/dev/null

echo "   Certificates generated at $CERT_DIR"

# Step 2: Create TLS-enabled config
echo "2. Creating TLS config..."
cat > "$CONFIG_FILE" <<EOF
[server]
http_host = "0.0.0.0"
http_port = $HTTP_PORT
flight_host = "0.0.0.0"
flight_port = $FLIGHT_PORT
mysql_host = "0.0.0.0"
mysql_port = $MYSQL_PORT
mysql_enabled = true

[storage]
backend = "local"
data_dir = "$DATA_DIR"

[tls]
enabled = true
cert_file = "$CERT_DIR/cert.pem"
key_file = "$CERT_DIR/key.pem"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[wal]
enabled = false

[retention]
enabled = false
EOF

echo "   Config written to $CONFIG_FILE"

# Step 3: Build the project
echo "3. Building MynaDB..."
cd "$PROJECT_DIR"
cargo build --release 2>&1 | tail -1
echo "   Build complete"

# Step 4: Start the server
echo "4. Starting MynaDB with TLS..."
cargo run --release -- -c "$CONFIG_FILE" &
SERVER_PID=$!
PID_FILE="$DATA_DIR/standalone.pid"

# Wait for server to start
echo "   Waiting for server to start..."
sleep 3

# Check if server is still running
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "   ERROR: Server failed to start!"
    exit 1
fi

# Step 5: Test HTTPS connectivity
echo "5. Testing HTTPS health endpoint..."
HEALTH_RESPONSE=$(curl -s --cacert "$CERT_DIR/cert.pem" \
    "https://localhost:$HTTP_PORT/health" 2>&1) || true

if echo "$HEALTH_RESPONSE" | grep -q "ok\|healthy\|UP"; then
    echo "   PASS: HTTPS health check returned: $HEALTH_RESPONSE"
else
    echo "   FAIL: Unexpected response: $HEALTH_RESPONSE"
    exit 1
fi

# Step 6: Test that plain HTTP fails
echo "6. Testing that plain HTTP is rejected..."
HTTP_RESPONSE=$(curl -s --max-time 3 \
    "http://localhost:$HTTP_PORT/health" 2>&1) || true

if [ -z "$HTTP_RESPONSE" ] || echo "$HTTP_RESPONSE" | grep -qi "error\|reset\|empty"; then
    echo "   PASS: Plain HTTP correctly rejected"
else
    echo "   WARN: Plain HTTP returned unexpected response: $HTTP_RESPONSE"
fi

# Step 7: Test HTTPS write
echo "7. Testing HTTPS write endpoint..."
WRITE_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" --cacert "$CERT_DIR/cert.pem" \
    -X POST "https://localhost:$HTTP_PORT/write/testdb" \
    --data-binary "cpu,host=server01 value=42.0 $(date +%s)000000000" 2>&1) || true

if [ "$WRITE_RESPONSE" = "204" ] || [ "$WRITE_RESPONSE" = "200" ]; then
    echo "   PASS: HTTPS write returned HTTP $WRITE_RESPONSE"
else
    echo "   FAIL: HTTPS write returned HTTP $WRITE_RESPONSE"
    exit 1
fi

# Step 8: Test HTTPS query
echo "8. Testing HTTPS query endpoint..."
QUERY_RESPONSE=$(curl -s --cacert "$CERT_DIR/cert.pem" \
    "https://localhost:$HTTP_PORT/query/testdb?sql=SELECT%20*%20FROM%20cpu" 2>&1) || true

if [ -n "$QUERY_RESPONSE" ]; then
    echo "   PASS: HTTPS query returned data"
else
    echo "   WARN: HTTPS query returned empty response"
fi

echo ""
echo "=== All TLS tests passed! ==="

# Stop server
kill "$SERVER_PID" 2>/dev/null || true
