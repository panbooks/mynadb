# Metric Sender

A Go script that continuously scrapes series count metrics from stats-service at configurable intervals and outputs them in line protocol format.

## Overview

This tool runs as a daemon that queries the stats-service at regular intervals (default: every 5 minutes) to get series counts grouped by service and metric, then outputs them in a format suitable for ingestion into time series databases.

## What It Does

1. Loads configuration from `config.toml` (or uses defaults)
2. Runs continuously with a configurable ticker (default: 5 minutes)
3. Queries the configured stats-service URL and database on each tick
4. Discovers all schemas and tables in the database automatically
5. Runs SQL queries to count series grouped by service and metric
6. Uses current date automatically
7. Outputs metrics in line protocol format
8. Handles graceful shutdown on Ctrl+C or SIGTERM

## Prerequisites

- Go 1.21 or higher
- Stats-service running (default: `http://localhost:7000`)
- Database with series stats data (default: `_system`)

## Installation

```bash
cd metric-sender
go build
```

## Usage

### Run directly with Go

```bash
cd metric-sender
go run main.go
```

### Run compiled binary

```bash
cd metric-sender
go build
./metric-sender
```

## Output Format

```
metrics,series_count,service={service},metric={metric} cnt={count} {timestamp}
```

### Example Output

```
Metric sender started. Running every 5 minutes.
Press Ctrl+C to stop.

=== 2025-12-30 15:30:00 ===
Querying stats-service...
SQL: select service, metric, count(*) as c from pmc.benchmark where date = '2025-12-30' group by service, metric

# Total metrics: 20

metrics,series_count,service=service_0,metric=cpu cnt=49959 1735578600000000000
metrics,series_count,service=service_1,metric=cpu cnt=49937 1735578600000000000
metrics,series_count,service=service_2,metric=cpu cnt=49845 1735578600000000000
...

Done. Scraped 20 series metrics for date 2025-12-30
Next run in 5 minutes...

=== 2025-12-30 15:35:00 ===
Querying stats-service...
SQL: select service, metric, count(*) as c from pmc.benchmark where date = '2025-12-30' group by service, metric

# Total metrics: 20
...
```

### Stopping the Script

Press `Ctrl+C` for graceful shutdown:

```
^C

Received signal interrupt. Shutting down gracefully...
```

## Output Fields

- **measurement**: `metrics` (fixed)
- **tag**: `series_count` (fixed)
- **tag**: `service={service}` (dynamic from query)
- **tag**: `metric={metric}` (dynamic from query)
- **field**: `cnt={count}` (series count from query)
- **timestamp**: Unix nanoseconds

## Configuration

Configuration is managed via `config.toml` in the same directory as the binary. If the file doesn't exist, the application uses default values.

### config.toml

```toml
# Stats Service URL
url = "http://localhost:7000"

# Database to query (usually "_system" for series stats)
database = "_system"

# Scraping interval in minutes
interval_minutes = 5
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | string | `http://localhost:7000` | Stats service base URL |
| `database` | string | `_system` | Database to query (catalog name) |
| `interval_minutes` | int | `5` | How often to scrape metrics (in minutes) |

### Using Default Configuration

If `config.toml` is not found, the application will print a message and use default values:

```
Config file not found at config.toml, using defaults
Metric sender started. Running every 5 minutes.
URL: http://localhost:7000
Database: _system
```

## Customization

### Query Different Database

Edit `config.toml` to change which database to query:

```toml
url = "http://localhost:7000"
database = "mydb"  # Change from "_system" to your database
interval_minutes = 5
```

### Change Scraping Interval

Edit `config.toml` to run more or less frequently:

```toml
url = "http://localhost:7000"
database = "_system"
interval_minutes = 1  # Run every minute instead of every 5 minutes
```

### Use Remote Stats Service

Edit `config.toml` to connect to a remote instance:

```toml
url = "http://192.168.1.100:7000"  # Remote server
database = "_system"
interval_minutes = 5
```

### Query Different Date

Modify the SQL query to use a specific date:

```go
// Instead of currentDate := time.Now().Format("2006-01-02")
currentDate := "2025-12-29"  // Query specific date
```

### Add More Fields

Modify the SQL query to select additional columns:

```go
sqlQuery := fmt.Sprintf(
    "select service, metric, region, count(*) as c from %s.%s where date = '%s' group by date, service, metric, region",
    database, schema, currentDate,
)
```

Then update `SeriesMetric` struct and `printMetric` function accordingly.

## Integration Examples

### Run as Background Service

```bash
# Run in background
nohup ./metric-sender >> /var/log/metric-sender.log 2>&1 &

# Check if running
ps aux | grep metric-sender

# Stop
pkill -INT metric-sender
```

### Run with systemd

Create `/etc/systemd/system/metric-sender.service`:

```ini
[Unit]
Description=Metric Sender - Stats Service Series Counter
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/projects/stats-service/metric-sender
ExecStart=/root/projects/stats-service/metric-sender/metric-sender
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable metric-sender
sudo systemctl start metric-sender
sudo systemctl status metric-sender

# View logs
sudo journalctl -u metric-sender -f
```

### Write to InfluxDB Continuously

```bash
./metric-sender | while IFS= read -r line; do
    if [[ $line == metrics,* ]]; then
        echo "$line" | curl -XPOST 'http://localhost:8086/write?db=monitoring' --data-binary @-
    fi
done
```

### Write to stats-service Continuously

```bash
./metric-sender | while IFS= read -r line; do
    if [[ $line == metrics,* ]]; then
        echo "$line" | curl -XPOST 'http://localhost:7000/write/monitoring' --data-binary @-
    fi
done
```

### Save to Rotating Log Files

```bash
./metric-sender | tee -a /var/log/metrics_$(date +%Y%m%d).log
```

## Error Handling

The script will exit with error if:

- Stats-service is not reachable
- HTTP request fails (non-200 status)
- JSON parsing fails
- No data returned

Check stderr for error messages:

```bash
./metric-sender 2> errors.log
```

## Development

### Run Tests

```bash
go test ./...
```

### Format Code

```bash
go fmt ./...
```

### Build for Multiple Platforms

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o metric-sender-linux

# macOS
GOOS=darwin GOARCH=amd64 go build -o metric-sender-mac

# Windows
GOOS=windows GOARCH=amd64 go build -o metric-sender.exe
```

## Troubleshooting

### Connection Refused

```
Error querying stats-service: HTTP request failed: dial tcp: connection refused
```

**Solution:** Ensure stats-service is running:
```bash
curl http://localhost:7000/health
```

### No Data Returned

```
Done. Scraped 0 series metrics for date 2025-12-30
```

**Possible causes:**
1. No data for current date in `pmc.arch_monitor_roc`
2. Table doesn't exist
3. Wrong database/schema name

**Debug:**
```bash
# Check if table exists
curl 'http://localhost:7000/query/_system?sql=SHOW+TABLES'

# Check if data exists for today
curl 'http://localhost:7000/query/_system?sql=SELECT+COUNT(*)+FROM+pmc.arch_monitor_roc+WHERE+date='"'"'2025-12-30'"'"
```

### JSON Parse Error

```
Error querying stats-service: failed to parse JSON: invalid character
```

**Solution:** Check the API response format:
```bash
curl 'http://localhost:7000/query/_system?sql=SELECT+service,+metric,+count(*)+FROM+pmc.arch_monitor_roc+LIMIT+1'
```

## Performance

- Query time: ~100-500ms (depends on data size)
- Memory usage: ~10-50MB (depends on result set size)
- Typical output: 100-10,000 metrics

## License

This tool is part of the stats-service project.
