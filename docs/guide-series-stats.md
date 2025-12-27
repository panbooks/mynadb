# Series Stats Quick Start

## Quick Test

```bash
# 1. Create database
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"mydb"}'

# 2. Write data with MONITOR.service tag
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=api,MONITOR.ip=127.0.0.1,type=http,host=h1 usage=75.5,cnt=1i'

curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=api,MONITOR.ip=127.0.0.2,type=http,host=h2 usage=82.3,cnt=1i'

curl -X POST http://localhost:7000/write/mydb \
  -d 'memory,MONITOR.service=web,host=h1 used=16.5,total=32'

# 3. Flush data
curl -X POST http://localhost:7000/flush

# 4. Wait for tracking
sleep 2

# 5. Get series stats
curl http://localhost:7000/series_stats/mydb | jq '.'
```

## Expected Output

```json
{
  "database": "mydb",
  "total_series": 3,
  "service_count": 2,
  "services": [
    {
      "service": "api",
      "total_series": 2,
      "metrics": {
        "cpu": 2
      }
    },
    {
      "service": "web",
      "total_series": 1,
      "metrics": {
        "memory": 1
      }
    }
  ]
}
```

## Key Points

### 1. Use MONITOR.service Tag

```bash
# ✅ CORRECT - Has MONITOR.service tag
cpu,MONITOR.service=api,host=h1 usage=75

# ❌ Without tag - tracked as "unknown"
cpu,host=h1 usage=75
```

### 2. Series = Service + Metric + Tag Combination

```bash
# These are 2 DIFFERENT series (different hosts)
cpu,MONITOR.service=api,host=h1 usage=75
cpu,MONITOR.service=api,host=h2 usage=82

# These are 2 DIFFERENT series (different services)
cpu,MONITOR.service=api,host=h1 usage=75
cpu,MONITOR.service=web,host=h1 usage=82

# These are the SAME series (duplicate)
cpu,MONITOR.service=api,host=h1 usage=75
cpu,MONITOR.service=api,host=h1 usage=76  # Different value, same tags
```

### 3. Tag Order Doesn't Matter

```bash
# These are the SAME series (same tags, different order)
cpu,MONITOR.service=api,host=h1,type=http usage=75
cpu,MONITOR.service=api,type=http,host=h1 usage=75
```

### 4. MONITOR.service Excluded from Hash

```bash
# These are COUNTED SEPARATELY by service
# But have the SAME tag hash (other tags are identical)
cpu,MONITOR.service=api,host=h1 usage=75   # api service
cpu,MONITOR.service=web,host=h1 usage=82   # web service
```

## Run Test Script

```bash
chmod +x ./scripts/test_series_stats.sh
./scripts/test_series_stats.sh
```

This will:
- Create test database
- Write data for 3 services (api, web, worker)
- Test duplicate detection
- Test tag ordering
- Show comprehensive stats

## Monitoring Commands

```bash
# Get total series count
curl -s http://localhost:7000/series_stats/mydb | jq '.total_series'

# List all services
curl -s http://localhost:7000/series_stats/mydb | jq '.services[].service'

# Find high-cardinality services
curl -s http://localhost:7000/series_stats/mydb | \
  jq '.services[] | select(.total_series > 10)'

# Get metrics for specific service
curl -s http://localhost:7000/series_stats/mydb | \
  jq '.services[] | select(.service == "api") | .metrics'
```

## Data Location

Series stats are stored in:
```
./data/_system/series_stats/{database}.parquet
```

Check files:
```bash
ls -lh ./data/_system/series_stats/
```

## Integration Example

```python
import requests
import json

# Write data
data = """
cpu,MONITOR.service=api,host=server01 usage=75.5
cpu,MONITOR.service=api,host=server02 usage=82.3
memory,MONITOR.service=api,host=server01 used=16.5
"""
requests.post("http://localhost:7000/write/mydb", data=data)

# Flush
requests.post("http://localhost:7000/flush")

# Get stats
response = requests.get("http://localhost:7000/series_stats/mydb")
stats = response.json()

print(f"Total series: {stats['total_series']}")
for service in stats['services']:
    print(f"  {service['service']}: {service['total_series']} series")
```

## Troubleshooting

### No stats returned

```bash
# Check if _system directory exists
ls -la ./data/_system/series_stats/

# Check if you included MONITOR.service tag
curl http://localhost:7000/series_stats/mydb | jq '.'
```

### Stats don't update

```bash
# Series tracking is async - wait a moment
sleep 2

# Make sure you flushed data
curl -X POST http://localhost:7000/flush
```

### "unknown" service appears

```bash
# This means data was written without MONITOR.service tag
# Add the tag to your writes:
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,MONITOR.service=myservice,host=h1 usage=75'
```

## Done! 🎉

You now have series cardinality tracking enabled. Monitor your series count regularly to prevent metric explosion!
