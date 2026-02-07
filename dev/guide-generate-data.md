# Test Data Generator

Generate realistic time-series data for testing and exploring MynaDB.

## Quick Start

```bash
# Make sure the cluster is running first
goreman -f dev/Procfile.cluster-local start

# Interactive mode - pick data types from a menu
./dev/generate-data.sh

# Or generate everything at once
./dev/generate-data.sh --type all
```

## Usage

```
./dev/generate-data.sh [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--type TYPE` | _(interactive)_ | Data types, comma-separated or `all` |
| `--points N` | `100` | Points per source (per host/sensor/service) |
| `--date YYYY-MM-DD` | today | Date for generated timestamps |
| `--db NAME` | `testdb` | Target database name |
| `--endpoint URL` | `http://localhost:7000` | Write endpoint |
| `--batch N` | `200` | Lines per HTTP write batch |
| `--quiet` | | Minimal output |

Environment variables `MYNADB_ENDPOINT` and `MYNADB_DATABASE` can also set defaults.

## Data Types

### 1. `cpu` — Server CPU Metrics

5 hosts (`web-1`, `web-2`, `web-3`, `api-1`, `db-1`) across 3 regions.
Each host has a different base load to simulate varied workloads.

```
measurement: cpu
tags:        host, region, service=monitoring
fields:      usage_user, usage_system, usage_idle, load_1m, load_5m, cores
```

### 2. `memory` — Memory & Swap Usage

Same 5 hosts with different total memory sizes (16–64 GB).

```
measurement: memory
tags:        host, region, service=monitoring
fields:      total_mb, used_mb, available_mb, usage_percent, swap_used_mb
```

### 3. `disk` — Disk I/O Throughput

5 hosts x 2 devices (`sda`, `sdb`) = 10 sources.

```
measurement: disk_io
tags:        host, region, device, service=monitoring
fields:      read_bytes, write_bytes, read_ops, write_ops, io_util
```

### 4. `http` — HTTP Request Metrics

3 services (`frontend`, `api-gateway`, `auth-service`) with realistic latency
distribution: 90% fast (<50ms), 5% moderate, 5% slow (>200ms).

```
measurement: http_requests
tags:        service, method, endpoint, status
fields:      latency_ms, request_bytes, response_bytes
```

### 5. `iot` — IoT Sensor Readings

8 sensors across warehouses, offices, and outdoor locations.
Temperature follows a daily cycle (cooler at night, warmer midday).
Battery percentage slowly drains over the day.

```
measurement: sensor_data
tags:        sensor_id, location, type, service=iot_gateway
fields:      temperature, humidity, pressure, battery_pct
```

### 6. `app` — Application Metrics

3 services (`order-service`, `payment-service`, `notification-service`).
Active users peak during business hours (9am–5pm).
Error events have higher response times.

```
measurement: app_events
tags:        service, host, level
fields:      response_time_ms, queue_depth, error_count, active_users
```

## Examples

### Generate specific types

```bash
# CPU and HTTP metrics only
./dev/generate-data.sh --type cpu,http

# IoT sensor data only
./dev/generate-data.sh --type iot
```

### Generate historical data

```bash
# Data for a specific past date
./dev/generate-data.sh --type all --date 2025-01-15

# Multiple days of history
for d in 2025-02-{01..07}; do
  ./dev/generate-data.sh --type cpu,http --date "$d" --quiet
done
```

### Control volume

```bash
# Light load: 10 points per source
./dev/generate-data.sh --type all --points 10

# Heavy load: 1000 points per source
./dev/generate-data.sh --type all --points 1000

# Full day at 1-minute resolution (1440 points)
./dev/generate-data.sh --type cpu --points 1440
```

### Write to a different target

```bash
# Write directly to a data node (skip router)
./dev/generate-data.sh --type cpu --endpoint http://localhost:5000

# Write to a different database
./dev/generate-data.sh --type all --db production_test
```

### Scripted / CI usage

```bash
# Quiet mode for scripts
./dev/generate-data.sh --type all --points 50 --quiet

# Using environment variables
export MYNADB_ENDPOINT=http://mynadb-router:7000
export MYNADB_DATABASE=benchdb
./dev/generate-data.sh --type cpu,http --points 5000 --quiet
```

## Sample Queries After Loading Data

Once data is loaded, try these queries in the Admin Query Console
(`http://localhost:18080/admin/query`) or via curl:

```bash
# Average CPU usage by host
curl 'http://localhost:7000/query/testdb' \
  -d 'sql=SELECT host, AVG(usage_user) as avg_cpu FROM monitoring.cpu GROUP BY host'

# Peak memory usage
curl 'http://localhost:7000/query/testdb' \
  -d 'sql=SELECT host, MAX(usage_percent) as peak_mem FROM monitoring.memory GROUP BY host'

# HTTP latency percentiles by service
curl 'http://localhost:7000/query/testdb' \
  -d 'sql=SELECT service, COUNT(*) as requests, AVG(latency_ms) as avg_ms FROM http_requests.http_requests GROUP BY service'

# Sensor temperature averages by location
curl 'http://localhost:7000/query/testdb' \
  -d 'sql=SELECT location, AVG(temperature) as avg_temp, MIN(temperature) as min_temp, MAX(temperature) as max_temp FROM iot_gateway.sensor_data GROUP BY location'

# Application error count by service
curl 'http://localhost:7000/query/testdb' \
  -d 'sql=SELECT service, SUM(error_count) as total_errors, COUNT(*) as events FROM app_events GROUP BY service'

# List all schemas in the database
curl 'http://localhost:7000/schemas/testdb'

# List tables in a specific schema
curl 'http://localhost:7000/tables/testdb'
```

## Data Volume Reference

Total lines generated per type at default `--points 100`:

| Type | Sources | Lines | Approx Size |
|------|---------|-------|-------------|
| `cpu` | 5 hosts | 500 | ~60 KB |
| `memory` | 5 hosts | 500 | ~55 KB |
| `disk` | 10 (5x2) | 1,000 | ~110 KB |
| `http` | 3 services | 300 | ~40 KB |
| `iot` | 8 sensors | 800 | ~95 KB |
| `app` | 3 services | 300 | ~40 KB |
| **all** | | **3,400** | **~400 KB** |

At `--points 1000`, multiply by 10 (~34,000 lines, ~4 MB).

## Schema Layout

The generated data uses the `service` tag which maps to schemas under
MynaDB's `ServiceAsSchema` strategy:

```
testdb/                    # catalog (database)
├── monitoring/            # schema (from service=monitoring)
│   ├── cpu/               # table (measurement)
│   ├── memory/
│   └── disk_io/
├── frontend/              # schema (from service=frontend)
│   └── http_requests/
├── api-gateway/
│   └── http_requests/
├── auth-service/
│   └── http_requests/
├── iot_gateway/
│   └── sensor_data/
├── order-service/
│   └── app_events/
├── payment-service/
│   └── app_events/
└── notification-service/
    └── app_events/
```

## Troubleshooting

**"Cannot connect to endpoint"**

The cluster must be running before generating data:
```bash
goreman -f dev/Procfile.cluster-local start
```

**Data written but queries return empty**

Data stays in the write cache until flushed. Either wait for the automatic
flush interval or trigger it manually:
```bash
curl -X POST http://localhost:5000/flush
```

**"Invalid date"**

Date must be in `YYYY-MM-DD` format:
```bash
# Correct
./dev/generate-data.sh --date 2025-01-15

# Wrong
./dev/generate-data.sh --date "Jan 15 2025"
```
