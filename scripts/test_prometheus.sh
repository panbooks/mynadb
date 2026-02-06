#!/bin/bash
# Test Prometheus Remote Storage API
# This script tests the Prometheus remote read/write endpoints

set -e

HOST="${HOST:-localhost}"
PORT="${PORT:-7000}"
BASE_URL="http://${HOST}:${PORT}"
DB="prometheus_test"
PASSED=0
FAILED=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "============================================"
echo "Prometheus Remote Storage API Tests"
echo "============================================"
echo "Base URL: $BASE_URL"
echo ""

# Check if server is running
if ! curl -s "${BASE_URL}/health" > /dev/null 2>&1; then
    echo -e "${RED}Error: MynaDB server is not running at ${BASE_URL}${NC}"
    echo "Start the server with: cargo run --release"
    exit 1
fi

# Helper function to run tests
run_test() {
    local name="$1"
    local expected="$2"
    local actual="$3"

    if [[ "$actual" == *"$expected"* ]]; then
        echo -e "${GREEN}✓ $name${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗ $name${NC}"
        echo "  Expected: $expected"
        echo "  Actual: $actual"
        ((FAILED++))
    fi
}

echo "--- Remote Write Tests ---"

# Test 1: Write with valid protobuf (manually crafted minimal protobuf)
# This creates a minimal WriteRequest with one TimeSeries
# The protobuf encoding:
# - Field 1 (timeseries): tag = 0x0a, length-delimited
# - TimeSeries.labels: tag = 0x0a, length-delimited
# - TimeSeries.samples: tag = 0x12, length-delimited

echo "Note: Remote write requires snappy-compressed protobuf"
echo "Testing error handling for invalid requests..."

# Test 2: Invalid request (not snappy compressed)
RESULT=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/api/v1/write/${DB}" \
    -H "Content-Type: application/x-protobuf" \
    -d "invalid data")
HTTP_CODE=$(echo "$RESULT" | tail -1)
run_test "Remote write rejects non-snappy data" "400" "$HTTP_CODE"

# Test 3: Empty body
RESULT=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/api/v1/write/${DB}" \
    -H "Content-Type: application/x-protobuf" \
    -d "")
HTTP_CODE=$(echo "$RESULT" | tail -1)
run_test "Remote write handles empty body" "400" "$HTTP_CODE"

echo ""
echo "--- Remote Read Tests ---"

# Test 4: Invalid request (not snappy compressed)
RESULT=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/api/v1/read/${DB}" \
    -H "Content-Type: application/x-protobuf" \
    -d "invalid data")
HTTP_CODE=$(echo "$RESULT" | tail -1)
run_test "Remote read rejects non-snappy data" "400" "$HTTP_CODE"

# Test 5: Empty body
RESULT=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/api/v1/read/${DB}" \
    -H "Content-Type: application/x-protobuf" \
    -d "")
HTTP_CODE=$(echo "$RESULT" | tail -1)
run_test "Remote read handles empty body" "400" "$HTTP_CODE"

echo ""
echo "--- Using Python to test with actual Prometheus protobuf ---"

# Check if Python is available with required modules
if python3 -c "import snappy" 2>/dev/null; then
    PYTHON_TEST=true
    echo "Python snappy module available, running full tests..."
else
    PYTHON_TEST=false
    echo -e "${YELLOW}Python snappy module not available, skipping protobuf tests${NC}"
    echo "Install with: pip install python-snappy"
fi

if [ "$PYTHON_TEST" = true ]; then
    # Create a Python script to generate valid Prometheus protobuf
    python3 << 'PYTHON_SCRIPT'
import struct
import snappy
import urllib.request
import sys

# Simple protobuf encoder for Prometheus types
def encode_varint(value):
    """Encode an integer as a varint."""
    result = []
    while value > 0x7f:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result) if result else b'\x00'

def encode_string(tag, s):
    """Encode a string field."""
    s_bytes = s.encode('utf-8')
    return bytes([tag << 3 | 2]) + encode_varint(len(s_bytes)) + s_bytes

def encode_double(tag, value):
    """Encode a double field."""
    return bytes([tag << 3 | 1]) + struct.pack('<d', value)

def encode_int64(tag, value):
    """Encode a signed int64 field."""
    return bytes([tag << 3 | 0]) + encode_varint(value if value >= 0 else (value + (1 << 64)))

def encode_message(tag, data):
    """Encode a sub-message field."""
    return bytes([tag << 3 | 2]) + encode_varint(len(data)) + data

# Build a Label message
def build_label(name, value):
    return encode_string(1, name) + encode_string(2, value)

# Build a Sample message
def build_sample(value, timestamp_ms):
    return encode_double(1, value) + encode_int64(2, timestamp_ms)

# Build a TimeSeries message
def build_timeseries(labels, samples):
    data = b''
    for name, value in labels:
        data += encode_message(1, build_label(name, value))
    for value, ts in samples:
        data += encode_message(2, build_sample(value, ts))
    return data

# Build a WriteRequest message
def build_write_request(timeseries_list):
    data = b''
    for ts in timeseries_list:
        data += encode_message(1, ts)
    return data

# Build a LabelMatcher message
def build_label_matcher(match_type, name, value):
    return encode_int64(1, match_type) + encode_string(2, name) + encode_string(3, value)

# Build a Query message
def build_query(start_ms, end_ms, matchers):
    data = encode_int64(1, start_ms) + encode_int64(2, end_ms)
    for matcher in matchers:
        data += encode_message(3, matcher)
    return data

# Build a ReadRequest message
def build_read_request(queries):
    data = b''
    for q in queries:
        data += encode_message(1, q)
    return data

# Create test data
import time
current_time_ms = int(time.time() * 1000)

ts1 = build_timeseries(
    [("__name__", "cpu_usage"), ("host", "server01"), ("region", "us-west")],
    [(42.5, current_time_ms - 60000), (43.1, current_time_ms)]
)

ts2 = build_timeseries(
    [("__name__", "memory_usage"), ("host", "server01"), ("region", "us-west")],
    [(1024.0, current_time_ms - 60000), (1048.5, current_time_ms)]
)

write_request = build_write_request([ts1, ts2])
compressed_write = snappy.compress(write_request)

# Test write endpoint
print("Testing remote write with valid protobuf...")
req = urllib.request.Request(
    "http://localhost:7000/api/v1/write/prometheus_test",
    data=compressed_write,
    headers={
        "Content-Type": "application/x-protobuf",
        "Content-Encoding": "snappy"
    },
    method="POST"
)

try:
    response = urllib.request.urlopen(req)
    print(f"Write response: {response.status}")
    if response.status == 204:
        print("✓ Remote write successful")
    else:
        print(f"✗ Unexpected status: {response.status}")
        sys.exit(1)
except urllib.error.HTTPError as e:
    print(f"✗ Write failed: {e.code} - {e.read().decode()}")
    sys.exit(1)

# Test read endpoint
print("\nTesting remote read...")
query = build_query(
    current_time_ms - 3600000,  # 1 hour ago
    current_time_ms + 60000,    # 1 minute in future
    [build_label_matcher(0, "__name__", "cpu_usage")]  # EQ matcher
)

read_request = build_read_request([query])
compressed_read = snappy.compress(read_request)

req = urllib.request.Request(
    "http://localhost:7000/api/v1/read/prometheus_test",
    data=compressed_read,
    headers={
        "Content-Type": "application/x-protobuf",
        "Content-Encoding": "snappy"
    },
    method="POST"
)

try:
    response = urllib.request.urlopen(req)
    print(f"Read response: {response.status}")
    if response.status == 200:
        # Response is snappy-compressed protobuf
        data = response.read()
        decompressed = snappy.decompress(data)
        print(f"Read returned {len(decompressed)} bytes of protobuf data")
        print("✓ Remote read successful")
    else:
        print(f"✗ Unexpected status: {response.status}")
except urllib.error.HTTPError as e:
    print(f"✗ Read failed: {e.code} - {e.read().decode()}")

print("\nPython protobuf tests complete!")
PYTHON_SCRIPT

    if [ $? -eq 0 ]; then
        ((PASSED+=2))
    else
        ((FAILED+=2))
    fi
fi

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
