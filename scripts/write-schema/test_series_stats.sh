#!/bin/bash

# Test script for series cardinality tracking

BASE_URL="http://localhost:7000"
DB="seriestest"

echo "========================================"
echo "Series Cardinality Tracking Test"
echo "========================================"

# Step 1: Create database
echo -e "\n[1] Creating database '$DB'..."
curl -X POST $BASE_URL/databases -H 'Content-Type: application/json' -d "{\"name\":\"$DB\"}"
echo ""

# Step 2: Write data with MONITOR.service tags
echo -e "\n[2] Writing data with MONITOR.service tags..."

echo "  Writing API service metrics..."
for i in {1..5}; do
    curl -s -X POST $BASE_URL/write/$DB \
      -d "cpu,MONITOR.service=api,MONITOR.ip=10.0.1.$i,host=api-server-$i,region=us-west usage=$((50 + i)),cnt=1i"
    curl -s -X POST $BASE_URL/write/$DB \
      -d "memory,MONITOR.service=api,MONITOR.ip=10.0.1.$i,host=api-server-$i,region=us-west used=$((10 + i)),total=32"
done
echo "    ✅ 5 hosts × 2 metrics = 10 series for API"

echo "  Writing Web service metrics..."
for i in {1..3}; do
    curl -s -X POST $BASE_URL/write/$DB \
      -d "cpu,MONITOR.service=web,host=web-server-$i,type=http usage=$((60 + i)),cnt=1i"
    curl -s -X POST $BASE_URL/write/$DB \
      -d "request_count,MONITOR.service=web,host=web-server-$i,type=http count=$((100 + i))i"
done
echo "    ✅ 3 hosts × 2 metrics = 6 series for Web"

echo "  Writing Worker service metrics..."
for i in {1..2}; do
    curl -s -X POST $BASE_URL/write/$DB \
      -d "cpu,MONITOR.service=worker,host=worker-$i,queue=default usage=$((40 + i)),cnt=1i"
    curl -s -X POST $BASE_URL/write/$DB \
      -d "queue_depth,MONITOR.service=worker,host=worker-$i,queue=default depth=$((10 + i))i"
    curl -s -X POST $BASE_URL/write/$DB \
      -d "disk,MONITOR.service=worker,host=worker-$i,queue=default free=$((500 + i))i"
done
echo "    ✅ 2 hosts × 3 metrics = 6 series for Worker"

echo -e "\nTotal expected series: 10 (api) + 6 (web) + 6 (worker) = 22 series"

# Step 3: Flush to ensure series stats are persisted
echo -e "\n[3] Flushing cache to persist series data..."
curl -s -X POST $BASE_URL/flush > /dev/null
echo "✅ Flushed"

# Wait a moment for async series tracking to complete
echo "Waiting 2 seconds for series tracking to complete..."
sleep 2

# Step 4: Get series stats
echo -e "\n[4] Getting series cardinality statistics..."
STATS=$(curl -s $BASE_URL/series_stats/$DB)
echo "$STATS" | jq '.'

# Step 5: Verify counts
echo -e "\n[5] Verification:"
TOTAL_SERIES=$(echo "$STATS" | jq '.total_series')
SERVICE_COUNT=$(echo "$STATS" | jq '.service_count')
API_SERIES=$(echo "$STATS" | jq '.services[] | select(.service == "api") | .total_series')
WEB_SERIES=$(echo "$STATS" | jq '.services[] | select(.service == "web") | .total_series')
WORKER_SERIES=$(echo "$STATS" | jq '.services[] | select(.service == "worker") | .total_series')

echo "Total series: $TOTAL_SERIES (expected: 22)"
echo "Service count: $SERVICE_COUNT (expected: 3)"
echo "API service: $API_SERIES series (expected: 10)"
echo "Web service: $WEB_SERIES series (expected: 6)"
echo "Worker service: $WORKER_SERIES series (expected: 6)"

# Step 6: Test duplicate detection
echo -e "\n[6] Testing duplicate series detection..."
echo "  Writing duplicate data (should not create new series)..."
curl -s -X POST $BASE_URL/write/$DB \
  -d "cpu,MONITOR.service=api,MONITOR.ip=10.0.1.1,host=api-server-1,region=us-west usage=55,cnt=1i"
curl -s -X POST $BASE_URL/write/$DB \
  -d "cpu,MONITOR.service=api,MONITOR.ip=10.0.1.1,host=api-server-1,region=us-west usage=56,cnt=1i"

echo "  Flushing..."
curl -s -X POST $BASE_URL/flush > /dev/null
sleep 2

echo "  Checking series count (should still be 22)..."
STATS_AFTER=$(curl -s $BASE_URL/series_stats/$DB)
TOTAL_AFTER=$(echo "$STATS_AFTER" | jq '.total_series')
echo "Total series after duplicates: $TOTAL_AFTER (expected: 22)"

if [ "$TOTAL_AFTER" -eq 22 ]; then
    echo "✅ SUCCESS: Duplicates correctly ignored"
else
    echo "⚠️  WARNING: Series count changed (expected 22, got $TOTAL_AFTER)"
fi

# Step 7: Test different tag combinations
echo -e "\n[7] Testing tag combination hashing..."
echo "  Writing data with same tags but different order..."
curl -s -X POST $BASE_URL/write/$DB \
  -d "cpu,MONITOR.service=api,host=test1,region=us-east,type=prod usage=70"
curl -s -X POST $BASE_URL/write/$DB \
  -d "cpu,MONITOR.service=api,type=prod,host=test1,region=us-east usage=71"

echo "  These should create only 1 new series (same tag set, different order)"
curl -s -X POST $BASE_URL/flush > /dev/null
sleep 2

STATS_TAG_TEST=$(curl -s $BASE_URL/series_stats/$DB)
TOTAL_TAG_TEST=$(echo "$STATS_TAG_TEST" | jq '.total_series')
echo "Total series: $TOTAL_TAG_TEST (expected: 23)"

if [ "$TOTAL_TAG_TEST" -eq 23 ]; then
    echo "✅ SUCCESS: Tag ordering correctly handled"
else
    echo "⚠️  WARNING: Unexpected series count (expected 23, got $TOTAL_TAG_TEST)"
fi

# Step 8: Test without MONITOR.service tag
echo -e "\n[8] Testing data without MONITOR.service tag..."
curl -s -X POST $BASE_URL/write/$DB \
  -d "network,host=server99,interface=eth0 rx_bytes=1000i,tx_bytes=2000i"
curl -s -X POST $BASE_URL/flush > /dev/null
sleep 2

STATS_UNKNOWN=$(curl -s $BASE_URL/series_stats/$DB)
UNKNOWN_SERVICE=$(echo "$STATS_UNKNOWN" | jq '.services[] | select(.service == "unknown")')

echo "Series with unknown service:"
echo "$UNKNOWN_SERVICE" | jq '.'

if [ -n "$UNKNOWN_SERVICE" ]; then
    echo "✅ SUCCESS: Data without MONITOR.service tracked as 'unknown'"
else
    echo "⚠️  No 'unknown' service found"
fi

# Step 9: Summary
echo -e "\n========================================"
echo "Series Cardinality Test Summary"
echo "========================================"
echo "Database: $DB"
echo ""

FINAL_STATS=$(curl -s $BASE_URL/series_stats/$DB)
echo "$FINAL_STATS" | jq '{
    database,
    total_series,
    service_count,
    services: .services | map({service, total_series, metric_count: (.metrics | length)})
}'

echo ""
echo "✅ Test completed!"
echo ""
echo "Try these commands:"
echo "  # View full stats"
echo "  curl http://localhost:7000/series_stats/$DB | jq '.'"
echo ""
echo "  # List high-cardinality services (>5 series)"
echo "  curl -s http://localhost:7000/series_stats/$DB | jq '.services[] | select(.total_series > 5)'"
echo ""
echo "  # Check stats file"
echo "  ls -lh ./data/_system/series_stats/"
echo "========================================"
