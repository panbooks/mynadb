# MynaDB Datasource Plugin for Grafana

Official Grafana datasource plugin for MynaDB time series database.

## Features

- **SQL Query Support**: Write standard SQL queries against your MynaDB database
- **Time Series Visualization**: Native support for time series graphs
- **Table Format**: Display query results in tabular format
- **Grafana Variables**: Full support for Grafana template variables and macros
- **Time Range Macros**: Automatic time range filtering with `$__timeFilter(column)`
- **Dynamic Intervals**: Automatic interval calculation with `$__interval`

## Installation

### From Grafana UI (when published)

1. Go to Configuration → Plugins
2. Search for "MynaDB"
3. Click Install

### Manual Installation

1. Download the latest release from GitHub
2. Extract to your Grafana plugins directory:
   ```bash
   unzip mynadb-datasource-1.0.0.zip -d /var/lib/grafana/plugins/
   ```
3. Restart Grafana:
   ```bash
   systemctl restart grafana-server
   ```

### Development Setup

```bash
# Install dependencies
npm install

# Build plugin
npm run build

# Development mode with watch
npm run dev

# Link to Grafana for testing
ln -s $(pwd)/dist /var/lib/grafana/plugins/mynadb-datasource
```

## Configuration

1. Add datasource: **Configuration → Data Sources → Add data source → MynaDB**
2. Configure connection:
   - **URL**: MynaDB HTTP API endpoint (e.g., `http://localhost:7000`)
   - **Default Database**: Catalog/database name (default: `default`)
   - **Access**: Select "Server (default)" for backend-to-backend communication
3. Click **Save & Test**

## Query Examples

### Time Series Query

```sql
SELECT
  time_bucket('$__interval', time) as time,
  avg(cpu_usage) as value
FROM cpu
WHERE $__timeFilter(time)
  AND host = '$host'
GROUP BY time
ORDER BY time
```

### Table Query

```sql
SELECT
  time,
  host,
  cpu_usage,
  memory_usage
FROM system_metrics
WHERE $__timeFilter(time)
ORDER BY time DESC
LIMIT 1000
```

### With Custom Variables

```sql
SELECT
  time_bucket('5m', time) as time,
  avg(response_time) as value
FROM http_requests
WHERE $__timeFilter(time)
  AND service = '$service'
  AND region = '$region'
GROUP BY time
ORDER BY time
```

## Grafana Macros

### Time Macros

- `$__timeFilter(column)` - Replaced with time range filter
  ```sql
  time >= 1609459200000000000 AND time <= 1609545600000000000
  ```

- `$__from` - Start time in nanoseconds
  ```sql
  WHERE time >= $__from
  ```

- `$__to` - End time in nanoseconds
  ```sql
  WHERE time <= $__to
  ```

- `$__interval` - Dynamic interval based on time range
  - < 1 hour: `1m`
  - < 6 hours: `5m`
  - < 24 hours: `15m`
  - < 1 week: `1h`
  - \> 1 week: `1d`

### Template Variables

Create variables in dashboard settings and use them in queries:

```sql
-- Variable query to populate dropdown
SELECT DISTINCT host FROM cpu

-- Use variable in query
WHERE host = '$host'
```

## Query Formats

### Time Series Format

Use for graphs, gauges, and time series visualizations. Requires:
- Time column (first column, timestamp in nanoseconds)
- Value column (second column, numeric)

### Table Format

Use for tabular data display. Returns all columns as-is.

## Troubleshooting

### Cannot connect to MynaDB

- Verify MynaDB is running: `curl http://localhost:7000/health`
- Check URL configuration in datasource settings
- Ensure no firewall blocking port 7000

### Queries return no data

- Verify time column exists and is named correctly in `$__timeFilter`
- Check time range matches your data
- Test query directly: `curl "http://localhost:7000/query/default?sql=SHOW TABLES"`

### Time format issues

- MynaDB uses nanoseconds internally
- Grafana sends milliseconds in time range
- Plugin handles conversion automatically

## Development

### Project Structure

```
src/
├── datasource.ts       # Main datasource class
├── QueryEditor.tsx     # Query editor UI component
├── ConfigEditor.tsx    # Datasource config UI
├── module.ts           # Plugin registration
├── types.ts            # TypeScript type definitions
└── plugin.json         # Plugin manifest
```

### Build Commands

```bash
npm run build          # Production build
npm run dev            # Development build
npm run watch          # Watch mode for development
npm run test           # Run tests
npm run sign           # Sign plugin for publishing
```

## Support

- **Issues**: https://github.com/panbooks/mynadb/issues
- **Documentation**: https://github.com/panbooks/mynadb
- **Community**: GitHub Discussions

## License

Apache License 2.0
