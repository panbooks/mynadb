#!/bin/bash

# Test script for Metrics Dashboard feature
# Tests all metrics endpoints and validates the dashboard data

set -e

BASE_URL="http://localhost:7000"
DB="testdb"

echo "===== MynaDB Metrics Dashboard Test ====="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}→ $1${NC}"
}

# Check if server is running
print_info "Checking server health..."
if ! curl -s "$BASE_URL/health" > /dev/null; then
    print_error "Server not running at $BASE_URL"
    exit 1
fi
print_success "Server is running"
echo ""

# Test 1: Write some data to generate metrics
print_info "Test 1: Writing test data to generate metrics..."
for i in {1..100}; do
    DATA="cpu,host=server$((i % 5)) usage=$((50 + i % 50)) $(($(date +%s)*1000000000 + i))"
    curl -s -X POST "$BASE_URL/write/$DB" -d "$DATA" > /dev/null
done
print_success "Wrote 100 data points"
echo ""

# Sleep to let metrics update
print_info "Waiting 3 seconds for metrics to update..."
sleep 3
echo ""

# Test 2: Query data to generate query metrics
print_info "Test 2: Running queries to generate query metrics..."
for i in {1..20}; do
    curl -s "$BASE_URL/query/$DB?sql=SELECT%20*%20FROM%20cpu%20LIMIT%2010" > /dev/null
done
print_success "Executed 20 queries"
echo ""

# Sleep to let metrics update
print_info "Waiting 3 seconds for metrics to update..."
sleep 3
echo ""

# Test 3: Fetch Prometheus metrics
print_info "Test 3: Fetching Prometheus metrics..."
METRICS=$(curl -s "$BASE_URL/metrics")

if [ -z "$METRICS" ]; then
    print_error "Failed to fetch Prometheus metrics"
    exit 1
fi

print_success "Fetched Prometheus metrics"
echo ""

# Test 4: Verify key metrics are present
print_info "Test 4: Verifying key metrics in Prometheus format..."

check_metric() {
    local metric_name=$1
    if echo "$METRICS" | grep -q "$metric_name"; then
        print_success "Found metric: $metric_name"
        return 0
    else
        print_error "Missing metric: $metric_name"
        return 1
    fi
}

FAIL=0

# Write metrics
check_metric "mynadb_write_lines_per_second" || FAIL=1
check_metric "mynadb_write_bytes_per_second" || FAIL=1
check_metric "mynadb_write_lines_total" || FAIL=1
check_metric "mynadb_write_latency_milliseconds" || FAIL=1

# Query metrics
check_metric "mynadb_query_active_queries" || FAIL=1
check_metric "mynadb_query_queries_total" || FAIL=1
check_metric "mynadb_query_latency_milliseconds" || FAIL=1

# Cache metrics
check_metric "mynadb_cache_metadata_hit_rate" || FAIL=1
check_metric "mynadb_cache_query_cache_hit_rate" || FAIL=1

# Storage metrics
check_metric "mynadb_storage_wal_size_bytes" || FAIL=1
check_metric "mynadb_storage_parquet_files" || FAIL=1

# Memory metrics
check_metric "mynadb_memory_rss_bytes" || FAIL=1
check_metric "mynadb_memory_cache_bytes" || FAIL=1

# System metrics
check_metric "mynadb_uptime_seconds" || FAIL=1
check_metric "mynadb_up" || FAIL=1

echo ""

# Test 5: Fetch JSON metrics
print_info "Test 5: Fetching JSON metrics..."
JSON_METRICS=$(curl -s "$BASE_URL/metrics.json")

if [ -z "$JSON_METRICS" ]; then
    print_error "Failed to fetch JSON metrics"
    exit 1
fi

# Validate JSON structure
if ! echo "$JSON_METRICS" | jq . > /dev/null 2>&1; then
    print_error "Invalid JSON format"
    exit 1
fi

print_success "Fetched valid JSON metrics"
echo ""

# Test 6: Verify JSON metrics structure
print_info "Test 6: Verifying JSON metrics structure..."

check_json_field() {
    local field=$1
    if echo "$JSON_METRICS" | jq -e "$field" > /dev/null 2>&1; then
        print_success "Found field: $field"
        return 0
    else
        print_error "Missing field: $field"
        return 1
    fi
}

check_json_field ".timestamp" || FAIL=1
check_json_field ".uptime_secs" || FAIL=1
check_json_field ".write.lines_per_sec" || FAIL=1
check_json_field ".write.total_lines" || FAIL=1
check_json_field ".query.active_queries" || FAIL=1
check_json_field ".query.total_queries" || FAIL=1
check_json_field ".cache.metadata_hit_rate" || FAIL=1
check_json_field ".storage.wal_size_bytes" || FAIL=1
check_json_field ".storage.parquet_files_l0" || FAIL=1
check_json_field ".memory.rss_bytes" || FAIL=1

echo ""

# Test 7: Display sample metrics values
print_info "Test 7: Sample metrics values..."
echo ""

echo "Write Metrics:"
echo "  Lines/sec:    $(echo "$JSON_METRICS" | jq -r '.write.lines_per_sec')"
echo "  Total lines:  $(echo "$JSON_METRICS" | jq -r '.write.total_lines')"
echo "  Total writes: $(echo "$JSON_METRICS" | jq -r '.write.total_writes')"
echo ""

echo "Query Metrics:"
echo "  Active:       $(echo "$JSON_METRICS" | jq -r '.query.active_queries')"
echo "  Total:        $(echo "$JSON_METRICS" | jq -r '.query.total_queries')"
echo "  Queries/sec:  $(echo "$JSON_METRICS" | jq -r '.query.queries_per_sec')"
echo "  P95 latency:  $(echo "$JSON_METRICS" | jq -r '.query.latency_ms.p95') ms"
echo ""

echo "Cache Metrics:"
echo "  Metadata hit rate:    $(echo "$JSON_METRICS" | jq -r '.cache.metadata_hit_rate')"
echo "  Query cache hit rate: $(echo "$JSON_METRICS" | jq -r '.cache.query_cache_hit_rate')"
echo ""

echo "Storage Metrics:"
echo "  WAL size:     $(echo "$JSON_METRICS" | jq -r '.storage.wal_size_bytes') bytes"
echo "  Parquet L0:   $(echo "$JSON_METRICS" | jq -r '.storage.parquet_files_l0')"
echo "  Parquet L1:   $(echo "$JSON_METRICS" | jq -r '.storage.parquet_files_l1')"
echo ""

echo "Memory Metrics:"
echo "  RSS:          $(echo "$JSON_METRICS" | jq -r '.memory.rss_bytes') bytes"
echo "  Cache:        $(echo "$JSON_METRICS" | jq -r '.memory.cache_bytes') bytes"
echo ""

# Test 8: Verify metrics are updating
print_info "Test 8: Verifying metrics update..."
LINES_BEFORE=$(echo "$JSON_METRICS" | jq -r '.write.total_lines')

# Write more data
for i in {1..50}; do
    DATA="cpu,host=server6 usage=75 $(($(date +%s)*1000000000 + i))"
    curl -s -X POST "$BASE_URL/write/$DB" -d "$DATA" > /dev/null
done

sleep 2

# Fetch metrics again
JSON_METRICS_AFTER=$(curl -s "$BASE_URL/metrics.json")
LINES_AFTER=$(echo "$JSON_METRICS_AFTER" | jq -r '.write.total_lines')

if [ "$LINES_AFTER" -gt "$LINES_BEFORE" ]; then
    print_success "Metrics are updating (lines: $LINES_BEFORE -> $LINES_AFTER)"
else
    print_error "Metrics not updating (lines: $LINES_BEFORE -> $LINES_AFTER)"
    FAIL=1
fi

echo ""

# Final summary
echo "======================================"
if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo ""
    echo "Metrics Dashboard is working correctly:"
    echo "  - Prometheus endpoint: $BASE_URL/metrics"
    echo "  - JSON endpoint:       $BASE_URL/metrics.json"
    echo ""
    echo "You can now:"
    echo "  1. Integrate with Grafana using the Prometheus endpoint"
    echo "  2. Use the JSON endpoint for custom dashboards"
    echo "  3. Monitor write throughput, query latency, cache hit rates, etc."
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
