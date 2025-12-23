# InfluxDB-Compatible Write API

## Overview

The Stats TSDB now supports InfluxDB-compatible write API with query parameters, making it easy to migrate from InfluxDB or use existing InfluxDB client libraries.

## Two Ways to Write Data

### Method 1: Path-Based (Stats Native)

```bash
POST /write/:db
```

**Example:**
```bash
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=23.5'
```

### Method 2: Query Parameter (InfluxDB-Compatible) ✨

```bash
POST /write?db=:db
```

**Example:**
```bash
curl -X POST "http://localhost:7000/write?db=mydb" \
  -d 'cpu,host=server01 usage=23.5'
```

Both methods are **identical** in functionality - they use the same underlying handler.

## InfluxDB Compatibility

### Supported Features

✅ **Query parameter format**: `?db=database_name`  
✅ **Line protocol**: Full InfluxDB line protocol support  
✅ **Multiple points**: Batch writes  
✅ **Tags and fields**: All data types  
✅ **Timestamps**: Nanosecond precision  

### Not Yet Supported

❌ **Retention policy**: `?rp=policy` (ignored if provided)  
❌ **Precision parameter**: `?precision=ms` (always nanoseconds)  
❌ **Authentication**: No auth tokens yet  

## Examples

### Basic Write

```bash
curl -X POST "http://localhost:7000/write?db=mydb" \
  -d 'cpu,host=server04,region=us-west load=42'
```

### With Timestamp

```bash
# Using current timestamp
TIMESTAMP=$(date +%s%N)
curl -X POST "http://localhost:7000/write?db=mydb" \
  -d "cpu,host=server04,region=us-west load=42 $TIMESTAMP"
```

### Multiple Points

```bash
curl -X POST "http://localhost:7000/write?db=mydb" -d '
cpu,host=server01,region=us-west usage=45.2,temp=38i
cpu,host=server02,region=us-west usage=67.8,temp=42i
cpu,host=server03,region=us-east usage=23.4,temp=35i
memory,host=server01 used=16.5,total=32.0
memory,host=server02 used=24.8,total=64.0
'
```

### Different Data Types

```bash
curl -X POST "http://localhost:7000/write?db=metrics" -d '
sensor,device=temp01 temperature=72.5,humidity=55i,active=true,status="normal"
'
```

## URL Encoding

When using query parameters, special characters must be URL-encoded:

```bash
# Space in database name
curl -X POST "http://localhost:7000/write?db=my%20database" \
  -d 'cpu usage=50'

# Multiple parameters (future use)
curl -X POST "http://localhost:7000/write?db=mydb&precision=ms" \
  -d 'cpu usage=50'
```

## Error Responses

### Invalid Database Name

```bash
curl -X POST "http://localhost:7000/write" \
  -d 'cpu usage=50'
# Error: Missing db parameter
```

### Invalid Line Protocol

```bash
curl -X POST "http://localhost:7000/write?db=mydb" \
  -d 'invalid line protocol'
# Response: "No valid points to write"
```

## Migration from InfluxDB

### Before (InfluxDB):
```bash
curl -X POST "http://localhost:8086/write?db=mydb" \
  -d 'cpu,host=server01 usage=23.5'
```

### After (Stats TSDB):
```bash
# Just change the host and port!
curl -X POST "http://localhost:7000/write?db=mydb" \
  -d 'cpu,host=server01 usage=23.5'
```

**That's it!** The API is compatible.

## Using InfluxDB Client Libraries

Most InfluxDB client libraries should work with minimal changes:

### Python Example

```python
from influxdb import InfluxDBClient

# Create client
client = InfluxDBClient(
    host='localhost',
    port=7000,
    database='mydb'
)

# Write data
json_body = [
    {
        "measurement": "cpu",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "fields": {
            "usage": 23.5,
            "temp": 45
        }
    }
]

client.write_points(json_body)
```

### Go Example

```go
package main

import (
    "fmt"
    "time"
    client "github.com/influxdata/influxdb1-client/v2"
)

func main() {
    c, _ := client.NewHTTPClient(client.HTTPConfig{
        Addr: "http://localhost:7000",
    })

    bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
        Database: "mydb",
    })

    tags := map[string]string{"host": "server01"}
    fields := map[string]interface{}{
        "usage": 23.5,
    }

    pt, _ := client.NewPoint("cpu", tags, fields, time.Now())
    bp.AddPoint(pt)

    c.Write(bp)
}
```

### Node.js Example

```javascript
const Influx = require('influx');

const influx = new Influx.InfluxDB({
    host: 'localhost',
    port: 7000,
    database: 'mydb'
});

influx.writePoints([
    {
        measurement: 'cpu',
        tags: { host: 'server01' },
        fields: { usage: 23.5 },
    }
]);
```

## Comparison: Both Write Methods

| Feature | Path-Based | Query Parameter |
|---------|-----------|-----------------|
| URL Format | `/write/mydb` | `/write?db=mydb` |
| InfluxDB Compatible | ❌ | ✅ |
| Stats Native | ✅ | ❌ |
| Functionality | Identical | Identical |
| Use Case | New apps | InfluxDB migration |

## Complete Workflow

### 1. Create Database (if needed)

```bash
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"mydb"}'
```

### 2. Write Data (InfluxDB-compatible)

```bash
curl -X POST "http://localhost:7000/write?db=mydb" \
  -d 'cpu,host=server01,region=us-west usage=75.5,temp=45i'
```

### 3. Check Cache

```bash
curl http://localhost:7000/stats
```

### 4. Query Data

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu'
```

## Testing Script

Save as `test_influxdb_api.sh`:

```bash
#!/bin/bash

DB="testdb"
BASE_URL="http://localhost:7000"

echo "Testing InfluxDB-compatible API"

# Create database
curl -X POST $BASE_URL/databases \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$DB\"}"

echo -e "\n\n"

# Write using InfluxDB-compatible API
echo "Writing data with ?db parameter..."
curl -X POST "$BASE_URL/write?db=$DB" -d '
cpu,host=server01,region=us-west usage=45.2,temp=38i
cpu,host=server02,region=us-east usage=67.8,temp=42i
memory,host=server01 used=16.5,total=32.0
'

echo -e "\n\n"

# Check stats
echo "Cache stats:"
curl $BASE_URL/stats

echo -e "\n\n"

# Flush
echo "Flushing..."
curl -X POST $BASE_URL/flush

echo -e "\n\n"

# Query
echo "Querying data:"
curl "$BASE_URL/query/$DB?sql=SELECT * FROM ${DB}_cpu"

echo -e "\n\nDone!"
```

Run with:
```bash
chmod +x test_influxdb_api.sh
./test_influxdb_api.sh
```

## Performance

Both write methods have **identical performance** since they use the same handler internally:

```rust
// Both routes call the same function:
async fn process_write_data(db: &str, data: &str, state: AppState)
```

## Best Practices

1. **Use query parameter format** if migrating from InfluxDB
2. **Use path-based format** for new applications
3. **Batch writes** for better performance (same for both methods)
4. **URL-encode** database names with special characters
5. **Check response codes** for error handling

## Summary

Stats TSDB now supports InfluxDB-compatible write API with `?db=database` query parameter, making migration from InfluxDB seamless while maintaining its own path-based API for new applications.

Both methods are fully supported and identical in functionality! 🎉
