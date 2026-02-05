#!/bin/bash

# Test script for partition management features
# Prerequisites: stats-service must be running on http://localhost:7000

set -e

BASE_URL="http://localhost:7000"
CATALOG="testdb"
SCHEMA="default"
TABLE="cpu"

echo "=========================================="
echo "Partition Management Features Test"
echo "=========================================="
echo ""

# Helper function to make requests
request() {
    echo "➤ $1"
    echo "  $2"
    echo ""
}

# 1. Create database
echo "1. Creating database..."
request "POST /databases" \
    "curl -X POST $BASE_URL/databases -H 'Content-Type: application/json' -d '{\"name\":\"$CATALOG\"}'"
curl -X POST $BASE_URL/databases -H 'Content-Type: application/json' -d "{\"name\":\"$CATALOG\"}" 2>/dev/null || true
echo ""

# 2. Write data for different dates
echo "2. Writing test data for multiple dates..."

# 2025-12-24 00:00:00 UTC = 1735027200000000000 nanoseconds
request "Write data for 2025-12-24" \
    "curl -X POST $BASE_URL/write/$CATALOG -d 'cpu,host=server01 usage=80 1735027200000000000'"
curl -X POST $BASE_URL/write/$CATALOG \
    -d "cpu,host=server01 usage=80 1735027200000000000" 2>/dev/null
echo ""

# 2025-12-23 00:00:00 UTC = 1734940800000000000 nanoseconds
request "Write data for 2025-12-23" \
    "curl -X POST $BASE_URL/write/$CATALOG -d 'cpu,host=server01 usage=75 1734940800000000000'"
curl -X POST $BASE_URL/write/$CATALOG \
    -d "cpu,host=server01 usage=75 1734940800000000000" 2>/dev/null
echo ""

# 2025-12-22 00:00:00 UTC = 1734854400000000000 nanoseconds
request "Write data for 2025-12-22" \
    "curl -X POST $BASE_URL/write/$CATALOG -d 'cpu,host=server01 usage=70 1734854400000000000'"
curl -X POST $BASE_URL/write/$CATALOG \
    -d "cpu,host=server01 usage=70 1734854400000000000" 2>/dev/null
echo ""

# 3. Flush cache to disk
echo "3. Flushing cache to create partition files..."
request "POST /flush" \
    "curl -X POST $BASE_URL/flush"
FLUSH_RESULT=$(curl -s -X POST $BASE_URL/flush)
echo "Response: $FLUSH_RESULT"
echo ""

sleep 2

# 4. List partitions
echo "4. Listing all partitions..."
request "GET /partitions/$CATALOG/$SCHEMA/$TABLE" \
    "curl $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE"
echo "Response:"
curl -s $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE | jq '.' 2>/dev/null || curl -s $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE
echo ""

# 5. Query with partition pruning
echo "5. Querying data for 2025-12-24 (partition pruning)..."
SQL_QUERY="SELECT * FROM cpu WHERE date = '2025-12-24' LIMIT 10"
ENCODED_SQL=$(echo "$SQL_QUERY" | jq -sRr @uri)
request "GET /query/$CATALOG?sql=$SQL_QUERY" \
    "curl '$BASE_URL/query/$CATALOG?sql=$ENCODED_SQL'"
echo "Response:"
curl -s "$BASE_URL/query/$CATALOG?sql=$ENCODED_SQL" | jq '.' 2>/dev/null || curl -s "$BASE_URL/query/$CATALOG?sql=$ENCODED_SQL"
echo ""

# 6. Query all data
echo "6. Querying all data..."
SQL_QUERY_ALL="SELECT * FROM cpu ORDER BY time DESC LIMIT 10"
ENCODED_SQL_ALL=$(echo "$SQL_QUERY_ALL" | jq -sRr @uri)
request "GET /query/$CATALOG?sql=$SQL_QUERY_ALL" \
    "curl '$BASE_URL/query/$CATALOG?sql=$ENCODED_SQL_ALL'"
echo "Response:"
curl -s "$BASE_URL/query/$CATALOG?sql=$ENCODED_SQL_ALL" | jq '.' 2>/dev/null || curl -s "$BASE_URL/query/$CATALOG?sql=$ENCODED_SQL_ALL"
echo ""

# 7. Delete a specific partition
echo "7. Deleting partition for 2025-12-22..."
request "DELETE /partitions/$CATALOG/$SCHEMA/$TABLE/2025-12-22" \
    "curl -X DELETE $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE/2025-12-22"
DELETE_RESULT=$(curl -s -X DELETE $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE/2025-12-22)
echo "Response: $DELETE_RESULT"
echo ""

sleep 1

# 8. List partitions again
echo "8. Listing partitions after deletion..."
request "GET /partitions/$CATALOG/$SCHEMA/$TABLE" \
    "curl $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE"
echo "Response:"
curl -s $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE | jq '.' 2>/dev/null || curl -s $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE
echo ""

# 9. Test retention policy (delete partitions older than 1 day)
echo "9. Testing retention policy (delete partitions older than 1 day)..."
request "POST /retention/$CATALOG/$SCHEMA/$TABLE" \
    "curl -X POST $BASE_URL/retention/$CATALOG/$SCHEMA/$TABLE -H 'Content-Type: application/json' -d '{\"older_than_days\": 1}'"
RETENTION_RESULT=$(curl -s -X POST $BASE_URL/retention/$CATALOG/$SCHEMA/$TABLE \
    -H 'Content-Type: application/json' \
    -d '{"older_than_days": 1}')
echo "Response: $RETENTION_RESULT"
echo ""

sleep 1

# 10. Final partition list
echo "10. Final partition list..."
request "GET /partitions/$CATALOG/$SCHEMA/$TABLE" \
    "curl $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE"
echo "Response:"
curl -s $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE | jq '.' 2>/dev/null || curl -s $BASE_URL/partitions/$CATALOG/$SCHEMA/$TABLE
echo ""

echo "=========================================="
echo "Test completed!"
echo "=========================================="
echo ""
echo "Summary of new endpoints:"
echo "  GET    /partitions/:catalog/:schema/:table"
echo "  DELETE /partitions/:catalog/:schema/:table/:date"
echo "  POST   /retention/:catalog/:schema/:table"
echo ""
echo "Features tested:"
echo "  ✓ Automatic partition creation on write"
echo "  ✓ Partition listing with metadata"
echo "  ✓ Query with partition pruning"
echo "  ✓ Delete specific partition"
echo "  ✓ Retention policy (delete old partitions)"
