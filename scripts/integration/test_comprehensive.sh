#!/bin/bash

# Comprehensive test script for Stats Time Series Database with DataFusion

BASE_URL="http://localhost:7000"
DB_NAME="testdb"

echo "🧪 Testing Stats Time Series Database with DataFusion"
echo "======================================================"
echo ""

# Test 1: Health check
echo "1️⃣  Testing health endpoint..."
curl -s ${BASE_URL}/health
echo -e "\n"

# Test 2: Write CPU data
echo "2️⃣  Writing CPU metrics..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
cpu,host=server01,region=us-west usage=75.5,temp=45i,cores=8i
cpu,host=server02,region=us-west usage=45.2,temp=38i,cores=16i
cpu,host=server03,region=us-east usage=89.1,temp=52i,cores=8i
cpu,host=server04,region=us-east usage=23.4,temp=35i,cores=4i
'
echo -e "\n"

# Test 3: Write memory data
echo "3️⃣  Writing memory metrics..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
memory,host=server01 used=16.5,total=32.0,cached=4.2
memory,host=server02 used=24.8,total=64.0,cached=8.1
memory,host=server03 used=12.3,total=32.0,cached=3.8
memory,host=server04 used=8.9,total=16.0,cached=2.1
'
echo -e "\n"

# Test 4: Write network data
echo "4️⃣  Writing network metrics..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
network,host=server01,interface=eth0 rx_bytes=1048576i,tx_bytes=2097152i,errors=0i
network,host=server01,interface=eth1 rx_bytes=524288i,tx_bytes=1048576i,errors=2i
network,host=server02,interface=eth0 rx_bytes=2097152i,tx_bytes=4194304i,errors=1i
'
echo -e "\n"

# Test 5: Check cache stats
echo "5️⃣  Checking cache statistics..."
curl -s ${BASE_URL}/stats
echo -e "\n"

# Test 6: Manually flush to disk
echo "6️⃣  Flushing cache to disk..."
curl -X POST ${BASE_URL}/flush
echo -e "\n"

# Wait a moment for flush to complete
sleep 1

# Test 7: List tables
echo "7️⃣  Listing available tables..."
curl -s ${BASE_URL}/tables/${DB_NAME} | python3 -m json.tool
echo -e "\n"

# Test 8: Get CPU table schema
echo "8️⃣  Getting CPU table schema..."
curl -s ${BASE_URL}/schema/${DB_NAME}/cpu
echo -e "\n"

echo "========== SQL Query Tests =========="
echo ""

# Test 9: Simple SELECT query
echo "9️⃣  Query: SELECT * FROM cpu (LIMIT 5)"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20*%20FROM%20${DB_NAME}_cpu%20LIMIT%205" | python3 -m json.tool
echo -e "\n"

# Test 10: Aggregation - Average CPU usage
echo "🔟 Query: Average CPU usage"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20AVG(usage)%20as%20avg_usage%20FROM%20${DB_NAME}_cpu" | python3 -m json.tool
echo -e "\n"

# Test 11: Group by host
echo "1️⃣1️⃣  Query: Average CPU usage by host"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20host,%20AVG(usage)%20as%20avg_usage%20FROM%20${DB_NAME}_cpu%20GROUP%20BY%20host" | python3 -m json.tool
echo -e "\n"

# Test 12: Filter query
echo "1️⃣2️⃣  Query: High CPU usage (>70)"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20host,%20usage%20FROM%20${DB_NAME}_cpu%20WHERE%20usage%20%3E%2070%20ORDER%20BY%20usage%20DESC" | python3 -m json.tool
echo -e "\n"

# Test 13: Complex aggregation with POST
echo "1️⃣3️⃣  Query: Statistics by region (using POST)"
curl -X POST ${BASE_URL}/query/${DB_NAME} \
  -H "Content-Type: text/plain" \
  -d "SELECT region, COUNT(*) as count, AVG(usage) as avg_usage, MAX(usage) as max_usage, MIN(usage) as min_usage FROM ${DB_NAME}_cpu GROUP BY region" | python3 -m json.tool
echo -e "\n"

# Test 14: JOIN query
echo "1️⃣4️⃣  Query: JOIN CPU and Memory data"
curl -X POST ${BASE_URL}/query/${DB_NAME} \
  -H "Content-Type: text/plain" \
  -d "SELECT c.host, c.usage as cpu_usage, m.used as memory_used, m.total as memory_total FROM ${DB_NAME}_cpu c JOIN ${DB_NAME}_memory m ON c.host = m.host" | python3 -m json.tool
echo -e "\n"

# Test 15: Order by time
echo "1️⃣5️⃣  Query: Latest CPU records"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20host,%20usage,%20time%20FROM%20${DB_NAME}_cpu%20ORDER%20BY%20time%20DESC%20LIMIT%203" | python3 -m json.tool
echo -e "\n"

# Test 16: Count records
echo "1️⃣6️⃣  Query: Count of records per measurement"
echo "  CPU count:"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20COUNT(*)%20as%20count%20FROM%20${DB_NAME}_cpu" | python3 -m json.tool
echo "  Memory count:"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20COUNT(*)%20as%20count%20FROM%20${DB_NAME}_memory" | python3 -m json.tool
echo "  Network count:"
curl -s "${BASE_URL}/query/${DB_NAME}?sql=SELECT%20COUNT(*)%20as%20count%20FROM%20${DB_NAME}_network" | python3 -m json.tool
echo -e "\n"

echo "✅ All tests completed!"
echo ""
echo "📁 Data files: ./data/${DB_NAME}/"
echo "   Run: ls -lh ./data/${DB_NAME}/"
echo ""
echo "💡 Try your own queries:"
echo "   curl 'http://localhost:7000/query/${DB_NAME}?sql=YOUR_SQL_HERE'"
echo ""
echo "📖 See QUERY_EXAMPLES.md for more query examples"