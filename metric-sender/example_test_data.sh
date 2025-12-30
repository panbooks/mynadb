#!/bin/bash

# Example script to populate test data in pmc.arch_monitor_roc table
# This allows testing the metric-sender without real data

set -e

HOST="http://localhost:7000"
DB="_system"
DATE=$(date +%Y-%m-%d)

echo "Populating test data for date: $DATE"
echo "Database: pmc.arch_monitor_roc"
echo ""

# Function to write data
write_data() {
    local service=$1
    local metric=$2
    local count=$3

    echo "Writing $count data points for service=$service, metric=$metric..."

    local timestamp=$(date +%s)000000000

    for ((i=1; i<=count; i++)); do
        local ts=$((timestamp + i*1000000000))
        local host="host$((i % 10))"
        local value=$((RANDOM % 100))

        echo "cpu,service=$service,metric=$metric,host=$host usage=$value $ts"
    done | curl -s -X POST "$HOST/write/pmc" \
        -H "Content-Type: text/plain" \
        --data-binary @- > /dev/null
}

echo "Creating test data..."
echo ""

# Service: api
write_data "api" "request_count" 50
write_data "api" "response_time" 50
write_data "api" "error_rate" 30

# Service: db
write_data "db" "query_time" 40
write_data "db" "connection_count" 40
write_data "db" "slow_queries" 20

# Service: cache
write_data "cache" "hit_rate" 35
write_data "cache" "miss_rate" 35
write_data "cache" "eviction_count" 25

echo ""
echo "Flushing cache..."
curl -s -X POST "$HOST/flush" > /dev/null

sleep 2

echo ""
echo "Test data created successfully!"
echo ""
echo "Verify data:"
echo "  curl '$HOST/query/$DB?sql=SELECT+COUNT(*)+FROM+pmc.arch_monitor_roc'"
echo ""
echo "Run metric-sender:"
echo "  cd metric-sender"
echo "  go run main.go"
echo ""
echo "Or:"
echo "  cd metric-sender"
echo "  make run"
