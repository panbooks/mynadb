#!/bin/bash

# Test the new catalog.schema.table structure
# Note: Numeric service tags are prefixed with "svc_" (e.g., service=1 -> schema "service_1")

echo "=== Test 1: Write with service tag (service=1 -> schema service_1) ==="
curl -X POST http://localhost:7000/write/mydb -d 'cpu,arch=x86,datacenter=ap-southeast-1b,hostname=host_9991227,os=Ubuntu15.10,rack=62,region=ap-southeast-1,service=1,service_environment=production,service_version=1,team=SF cnt=1i 1766534400000000000'

echo -e "\n\n=== Test 2: Write with different service (service=2 -> schema service_2) ==="
curl -X POST http://localhost:7000/write/mydb -d 'cpu,arch=x86,datacenter=ap-southeast-1b,hostname=host_9991228,os=Ubuntu15.10,rack=62,region=ap-southeast-1,service=2,service_environment=production,service_version=1,team=SF cnt=2i 1766534401000000000'

echo -e "\n\n=== Test 3: Write without service tag (should use 'default') ==="
curl -X POST http://localhost:7000/write/mydb -d 'memory,host=server01 usage=85.5 1766534402000000000'

echo -e "\n\n=== Test 4: Different measurement in same service ==="
curl -X POST http://localhost:7000/write/mydb -d 'disk,host=server01,service=1 usage=45.0 1766534403000000000'

echo -e "\n\n=== Test 5: Batch write - multiple points, multiple services ==="
curl -X POST http://localhost:7000/write/mydb -d 'cpu,service=1,hostname=host01 cnt=10i
cpu,service=1,hostname=host02 cnt=20i
cpu,service=2,hostname=host03 cnt=30i
memory,host=host04 usage=90.0'

echo -e "\n\n=== Test 6: Write with text service name ==="
curl -X POST http://localhost:7000/write/mydb -d 'cpu,service=production,hostname=prod01 cnt=100i'

sleep 10

echo -e "\n\n=== Flushing cache to disk... ==="
curl -X POST http://localhost:7000/flush
echo ""

echo -e "\n=== Waiting for flush to complete... ==="
sleep 2

echo -e "\n=== Verify file structure ==="
echo "Directory structure under data/mydb/:"
tree /data0/stats/data/mydb/ 2>/dev/null || find data/mydb/ -type f 2>/dev/null

echo -e "\n\n=== Test 7: Query service=1 (schema service_1) fully qualified ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.service_1.cpu LIMIT 10'

echo -e "\n\n=== Test 8: Query service=2 (schema service_2) fully qualified ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.service_2.cpu LIMIT 10'

echo -e "\n\n=== Test 9: Query default schema fully qualified ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.memory LIMIT 10'

echo -e "\n\n=== Test 10: Query with simple table name (auto-prefixed to mydb.default.X) ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM memory LIMIT 10'

echo -e "\n\n=== Test 11: Query with partially qualified name (schema.table) ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM service_1.cpu LIMIT 10'

echo -e "\n\n=== Test 12: Query text service name ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.production.cpu LIMIT 10'

echo -e "\n\n=== Test 13: Cross-schema join ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT a.hostname as host1, a.cnt as cnt1, b.hostname as host2, b.cnt as cnt2 FROM mydb.service_1.cpu a, mydb.service_2.cpu b LIMIT 5'

echo -e "\n\n=== Test 14: Aggregation ==="
curl 'http://localhost:7000/query/mydb' -d 'SELECT COUNT(*) as total FROM service_1.cpu'

echo -e "\n\nAll tests completed!"
