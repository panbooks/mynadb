#!/bin/bash

# Test script for Stats Time Series Database

BASE_URL="http://localhost:7000"
DB_NAME="testdb"

echo "🧪 Testing Stats Time Series Database"
echo "======================================"
echo ""

# Test 1: Health check
echo "1️⃣  Testing health endpoint..."
curl -s ${BASE_URL}/health
echo -e "\n"

# Test 2: Simple metric
echo "2️⃣  Writing simple CPU metric..."
curl -X POST ${BASE_URL}/write/${DB_NAME} \
  -d 'cpu usage=50.0'
echo -e "\n"

# Test 3: Metric with tags
echo "3️⃣  Writing CPU metric with tags..."
curl -X POST ${BASE_URL}/write/${DB_NAME} \
  -d 'cpu,host=server01,region=us-west usage=75.5,cores=8i'
echo -e "\n"

# Test 4: Multiple measurements
echo "4️⃣  Writing multiple measurements..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
cpu,host=server01 usage=50.0,temp=45i
memory,host=server01 used=16.5,total=32.0
disk,host=server01,mount=/data used=120.5,total=500.0
'
echo -e "\n"

# Test 5: Different data types
echo "5️⃣  Writing different data types..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
sensor,device=temp01,location=warehouse temperature=72.5,humidity=55i,active=true,status="normal"
'
echo -e "\n"

# Test 6: Time series data (simulating monitoring)
echo "6️⃣  Writing time series monitoring data..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
cpu,host=server01,region=us-west usage=45.2,iowait=2.1,system=5.3
cpu,host=server02,region=us-west usage=67.8,iowait=1.5,system=8.2
cpu,host=server03,region=us-east usage=23.4,iowait=0.8,system=3.1
memory,host=server01,region=us-west used=24.5,cached=8.2,buffers=1.3
memory,host=server02,region=us-west used=48.7,cached=12.1,buffers=2.1
network,host=server01,interface=eth0 rx_bytes=1048576i,tx_bytes=2097152i,errors=0i
network,host=server01,interface=eth1 rx_bytes=524288i,tx_bytes=1048576i,errors=2i
'
echo -e "\n"

# Test 7: Application metrics
echo "7️⃣  Writing application metrics..."
curl -X POST ${BASE_URL}/write/${DB_NAME} -d '
http_requests,service=api,method=GET,status=200 count=1523i,duration=45.2
http_requests,service=api,method=POST,status=201 count=342i,duration=128.7
http_requests,service=api,method=GET,status=404 count=23i,duration=12.3
database,service=postgres,operation=select query_time=23.5,rows_returned=150i
database,service=postgres,operation=insert query_time=45.2,rows_affected=1i
cache,service=redis,operation=get hit_rate=0.95,latency=1.2
'
echo -e "\n"

echo "✅ All tests completed!"
echo ""
echo "📁 Check your data in: ./data/${DB_NAME}/"
echo "   Run: ls -lh ./data/${DB_NAME}/"