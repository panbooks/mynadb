#!/bin/bash

# Test script for schema and table listing APIs
# Demonstrates the catalog.schema.table hierarchy

echo "=== Setup: Create database ==="
curl -X POST http://localhost:7000/databases -H 'Content-Type: application/json' -d '{"name":"testdb"}'

echo -e "\n\n=== Write data with different services (schemas) ==="
echo "Writing to service=webapp (schema: webapp)..."
curl -X POST http://localhost:7000/write/testdb -d 'cpu,service=webapp,hostname=web01 usage=45.5'

echo -e "\nWriting to service=api (schema: api)..."
curl -X POST http://localhost:7000/write/testdb -d 'cpu,service=api,hostname=api01 usage=78.2'

echo -e "\nWriting to service=database (schema: database)..."
curl -X POST http://localhost:7000/write/testdb -d 'cpu,service=database,hostname=db01 usage=92.1'

echo -e "\nWriting without service tag (schema: default)..."
curl -X POST http://localhost:7000/write/testdb -d 'memory,hostname=server01 usage=65.0'

echo -e "\nWriting multiple measurements to same service..."
curl -X POST http://localhost:7000/write/testdb -d 'memory,service=webapp,hostname=web01 usage=80.0
disk,service=webapp,hostname=web01 usage=55.0
network,service=webapp,hostname=web01 bytes=1000000'

echo -e "\nWriting numeric service (will be prefixed with svc_)..."
curl -X POST http://localhost:7000/write/testdb -d 'cpu,service=1,hostname=host01 usage=30.0'

echo -e "\n\n=== Waiting for background flush... ==="
sleep 3

echo -e "\n\n=== Test 1: List all schemas in database ==="
curl http://localhost:7000/databases/testdb/schemas

echo -e "\n\n=== Test 2: List tables in 'webapp' schema ==="
curl http://localhost:7000/databases/testdb/schemas/webapp/tables

echo -e "\n\n=== Test 3: List tables in 'default' schema ==="
curl http://localhost:7000/databases/testdb/schemas/default/tables

echo -e "\n\n=== Test 4: List tables in 'api' schema ==="
curl http://localhost:7000/databases/testdb/schemas/api/tables

echo -e "\n\n=== Test 5: List tables in 'svc_1' schema (numeric service) ==="
curl http://localhost:7000/databases/testdb/schemas/svc_1/tables

echo -e "\n\n=== Test 6: List all tables (legacy endpoint) ==="
curl http://localhost:7000/databases/testdb/tables

echo -e "\n\n=== Verify directory structure ==="
echo "Directory structure for testdb:"
tree data/testdb/ 2>/dev/null || find data/testdb/ -type d 2>/dev/null | sort

echo -e "\n\n=== Test 7: Query specific schema.table ==="
curl 'http://localhost:7000/query/testdb' -d 'SELECT * FROM webapp.cpu LIMIT 5'

echo -e "\n\n=== Test 8: Query across schemas ==="
curl 'http://localhost:7000/query/testdb' -d 'SELECT COUNT(*) as webapp_count FROM webapp.cpu'

echo -e "\n\n=== Test 9: Cross-schema aggregation ==="
curl 'http://localhost:7000/query/testdb' -d "SELECT 'webapp' as service, COUNT(*) as count FROM webapp.cpu
UNION ALL
SELECT 'api' as service, COUNT(*) as count FROM api.cpu
UNION ALL
SELECT 'database' as service, COUNT(*) as count FROM database.cpu"

echo -e "\n\nAll tests completed!"
