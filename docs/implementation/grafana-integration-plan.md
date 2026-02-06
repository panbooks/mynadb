# Grafana Integration Implementation Plan

**Status:** Not Started
**Priority:** P1 - High Impact Feature
**Estimated Effort:** 3-4 weeks
**Target:** Phase 4 (Ecosystem Integration)

---

## Executive Summary

Grafana integration is critical for MynaDB adoption. Currently, users must manually configure Grafana datasources using generic HTTP/MySQL connectors, losing MynaDB-specific features like time-series optimizations, native variable support, and query builder assistance.

**Goal:** Create an official Grafana datasource plugin that provides:
- Native MynaDB query builder UI
- SQL and InfluxQL query modes
- Grafana variable support ($__timeFilter, $__interval, etc.)
- Time-series specific helpers
- One-click installation from Grafana marketplace

---

## Current State Analysis

### What MynaDB Has ✅

1. **HTTP Query API** (`GET/POST /query/:db`)
   - Location: `src/services/api.rs:query_handler()`
   - Returns JSON results via `execute_query_impl()`
   - Accepts SQL queries as query parameter or POST body

2. **Arrow Flight gRPC** (Port 7001)
   - High-performance binary protocol for large result sets
   - Could be used for streaming data to Grafana

3. **MySQL Wire Protocol** (Port 7002)
   - Grafana can connect using MySQL datasource
   - But loses MynaDB-specific features

4. **Schema Discovery**
   - `GET /tables/:db` - List tables
   - `GET /schema/:db/:table` - Get table schema
   - Essential for query builder

### What's Missing ❌

1. **Grafana Datasource Plugin**
   - No TypeScript frontend plugin
   - No Go backend plugin
   - No query builder UI
   - No variable interpolation

2. **Grafana-Specific API Endpoints**
   - `/grafana/search` - For metric/table discovery
   - `/grafana/query` - With Grafana-specific response format
   - `/grafana/tag-keys` - For template variables
   - `/grafana/tag-values` - For variable value lists

3. **Data Format Compatibility**
   - Current JSON format may not match Grafana expectations
   - Time-series data needs proper formatting
   - Need to support both "table" and "timeseries" response types

---

## Implementation Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Grafana Frontend                          │
│                                                              │
│  ┌────────────────┐          ┌──────────────────┐          │
│  │  Query Editor  │◄────────►│  MynaDB Plugin   │          │
│  │  (TypeScript)  │          │  (TypeScript)    │          │
│  └────────────────┘          └──────────────────┘          │
│         │                            │                       │
└─────────┼────────────────────────────┼───────────────────────┘
          │                            │
          │ HTTP/JSON                  │ HTTP/JSON
          ▼                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    MynaDB Backend                            │
│                                                              │
│  ┌────────────────┐          ┌──────────────────┐          │
│  │ Grafana API    │◄────────►│  Query Engine    │          │
│  │ Handlers (Rust)│          │  (DataFusion)    │          │
│  └────────────────┘          └──────────────────┘          │
│         │                                                    │
│         ▼                                                    │
│  ┌──────────────────────────────────────────────┐          │
│  │  /grafana/search   - Metric discovery         │          │
│  │  /grafana/query    - Execute query            │          │
│  │  /grafana/tag-keys - Variable keys            │          │
│  │  /grafana/tag-values - Variable values        │          │
│  └──────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Backend API Enhancements (Week 1)

### 1.1 Add Grafana-Specific Endpoints

**Location:** `src/services/grafana.rs` (NEW FILE)

#### Endpoint: `/grafana/search`

Used by Grafana to discover available metrics/tables for dropdown lists.

```rust
// src/services/grafana.rs

use axum::{extract::{State, Query}, Json};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct SearchRequest {
    /// Target database/catalog
    pub target: Option<String>,
    /// Search type: "table", "column", "schema"
    #[serde(rename = "type")]
    pub search_type: Option<String>,
}

#[derive(Serialize)]
pub struct SearchResult {
    /// Text to display in Grafana dropdown
    pub text: String,
    /// Value to use in queries
    pub value: String,
    /// Optional: makes it expandable in Grafana UI
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expandable: Option<bool>,
}

pub async fn search_handler(
    State(state): State<AppState>,
    Query(params): Query<SearchRequest>,
) -> Json<Vec<SearchResult>> {
    let db = params.target.as_deref().unwrap_or("default");

    // Query catalog for tables
    let tables = state.query_engine
        .list_tables(db)
        .await
        .unwrap_or_default();

    let results = tables
        .into_iter()
        .map(|table| SearchResult {
            text: table.clone(),
            value: table,
            expandable: Some(true),
        })
        .collect();

    Json(results)
}
```

#### Endpoint: `/grafana/query`

Execute queries with Grafana-specific response format.

```rust
#[derive(Deserialize)]
pub struct GrafanaQueryRequest {
    /// Time range
    pub range: GrafanaTimeRange,
    /// List of query targets
    pub targets: Vec<GrafanaTarget>,
    /// Max data points Grafana can display
    pub maxDataPoints: Option<i32>,
    /// Interval calculated by Grafana
    pub interval: Option<String>,
    /// Interval in milliseconds
    pub intervalMs: Option<i64>,
}

#[derive(Deserialize)]
pub struct GrafanaTimeRange {
    pub from: String, // ISO8601 or epoch timestamp
    pub to: String,
}

#[derive(Deserialize)]
pub struct GrafanaTarget {
    /// Unique query identifier
    pub refId: String,
    /// SQL query with variables
    pub rawSql: Option<String>,
    /// Query format: "time_series" or "table"
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String {
    "time_series".to_string()
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum GrafanaResponse {
    TimeSeries(Vec<TimeSeriesResponse>),
    Table(Vec<TableResponse>),
}

#[derive(Serialize)]
pub struct TimeSeriesResponse {
    /// Target name (appears as legend in Grafana)
    pub target: String,
    /// Data points: [value, timestamp_ms]
    pub datapoints: Vec<[Option<f64>; 2]>,
}

#[derive(Serialize)]
pub struct TableResponse {
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Row data
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(rename = "type")]
    pub response_type: String, // "table"
}

#[derive(Serialize)]
pub struct ColumnDef {
    pub text: String,
    #[serde(rename = "type")]
    pub col_type: String, // "time", "number", "string"
}

pub async fn query_handler(
    State(state): State<AppState>,
    Json(request): Json<GrafanaQueryRequest>,
) -> Json<Vec<GrafanaResponse>> {
    let mut responses = Vec::new();

    for target in request.targets {
        if let Some(mut sql) = target.rawSql {
            // Replace Grafana variables
            sql = replace_grafana_variables(&sql, &request);

            // Execute query
            match state.query_engine.execute_query(&sql).await {
                Ok(batches) => {
                    if target.format == "time_series" {
                        responses.push(GrafanaResponse::TimeSeries(
                            convert_to_timeseries(batches, &target.refId)
                        ));
                    } else {
                        responses.push(GrafanaResponse::Table(
                            convert_to_table(batches)
                        ));
                    }
                }
                Err(e) => {
                    eprintln!("Query error: {}", e);
                }
            }
        }
    }

    Json(responses)
}

fn replace_grafana_variables(sql: &str, request: &GrafanaQueryRequest) -> String {
    let mut sql = sql.to_string();

    // Replace $__timeFilter(column)
    let time_filter = format!(
        "{} >= {} AND {} <= {}",
        "time", // extract column name from $__timeFilter(time)
        parse_grafana_time(&request.range.from),
        "time",
        parse_grafana_time(&request.range.to)
    );
    sql = sql.replace("$__timeFilter(time)", &time_filter);

    // Replace $__interval
    if let Some(interval) = &request.interval {
        sql = sql.replace("$__interval", interval);
    }

    // Replace $__from and $__to
    sql = sql.replace("$__from", &parse_grafana_time(&request.range.from).to_string());
    sql = sql.replace("$__to", &parse_grafana_time(&request.range.to).to_string());

    sql
}

fn parse_grafana_time(time_str: &str) -> i64 {
    // Parse ISO8601 or epoch milliseconds
    if let Ok(timestamp) = time_str.parse::<i64>() {
        timestamp * 1_000_000 // Convert ms to nanoseconds for MynaDB
    } else {
        // Parse ISO8601 format
        // Use chrono for parsing
        0 // placeholder
    }
}
```

#### Endpoint: `/grafana/tag-keys`

Return available tag keys for template variables.

```rust
pub async fn tag_keys_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Vec<TagKeyResult>> {
    let db = params.get("db").map(|s| s.as_str()).unwrap_or("default");

    // Get distinct tag keys from tables
    // This requires querying metadata or sample data

    Json(vec![
        TagKeyResult { text: "host".to_string(), value: "host".to_string() },
        TagKeyResult { text: "region".to_string(), value: "region".to_string() },
        TagKeyResult { text: "service".to_string(), value: "service".to_string() },
    ])
}

#[derive(Serialize)]
pub struct TagKeyResult {
    pub text: String,
    pub value: String,
}
```

#### Endpoint: `/grafana/tag-values`

Return available values for a given tag key.

```rust
pub async fn tag_values_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Vec<TagValueResult>> {
    let db = params.get("db").map(|s| s.as_str()).unwrap_or("default");
    let key = params.get("key").map(|s| s.as_str()).unwrap_or("");

    // Execute: SELECT DISTINCT {key} FROM {table}
    let sql = format!("SELECT DISTINCT {} FROM system_tags LIMIT 1000", key);

    // Convert results to tag values
    Json(vec![
        TagValueResult { text: "web-01".to_string() },
        TagValueResult { text: "web-02".to_string() },
    ])
}

#[derive(Serialize)]
pub struct TagValueResult {
    pub text: String,
}
```

### 1.2 Update Router

**Location:** `src/services/api.rs`

```rust
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // ... existing routes ...

        // Grafana datasource endpoints
        .route("/grafana/search", get(grafana::search_handler))
        .route("/grafana/query", post(grafana::query_handler))
        .route("/grafana/tag-keys", get(grafana::tag_keys_handler))
        .route("/grafana/tag-values", get(grafana::tag_values_handler))
        .route("/grafana/health", get(|| async { "OK" }))

        .with_state(state)
}
```

### 1.3 Add Data Format Converters

```rust
fn convert_to_timeseries(
    batches: Vec<RecordBatch>,
    target: &str,
) -> Vec<TimeSeriesResponse> {
    // Assuming schema: [time, value, metric_name]
    let mut series: HashMap<String, Vec<[Option<f64>; 2]>> = HashMap::new();

    for batch in batches {
        let time_col = batch.column(0); // timestamp
        let value_col = batch.column(1); // value

        for row in 0..batch.num_rows() {
            // Extract time and value
            // Group by metric name if exists
            // Add to datapoints
        }
    }

    series.into_iter()
        .map(|(name, datapoints)| TimeSeriesResponse {
            target: name,
            datapoints,
        })
        .collect()
}

fn convert_to_table(batches: Vec<RecordBatch>) -> Vec<TableResponse> {
    // Convert Arrow RecordBatch to Grafana table format
    vec![]
}
```

---

## Phase 2: Grafana Plugin Scaffold (Week 2)

### 2.1 Create Plugin Structure

Use Grafana's official CLI tool to scaffold the plugin:

```bash
# Install Grafana plugin tools
npm install -g @grafana/create-plugin

# Create plugin
npx @grafana/create-plugin

# Prompts:
# - Plugin name: mynadb-datasource
# - Plugin type: datasource
# - Organization: mynadb
# - Add backend: Yes
```

**Directory Structure:**
```
grafana-mynadb-datasource/
├── src/
│   ├── datasource.ts          # Main datasource class
│   ├── QueryEditor.tsx         # Query builder UI
│   ├── ConfigEditor.tsx        # Datasource configuration
│   ├── module.ts               # Plugin registration
│   ├── types.ts                # TypeScript types
│   └── components/
│       ├── SQLEditor.tsx       # SQL mode editor
│       ├── InfluxQLEditor.tsx  # InfluxQL mode editor
│       └── VisualBuilder.tsx   # Visual query builder
├── pkg/                        # Go backend (if needed)
│   ├── plugin.go
│   └── query.go
├── dist/                       # Build output
├── plugin.json                 # Plugin manifest
├── package.json
├── tsconfig.json
└── README.md
```

### 2.2 Implement TypeScript Datasource

**File:** `src/datasource.ts`

```typescript
import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MutableDataFrame,
  FieldType,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { MynaDBQuery, MynaDBDataSourceOptions } from './types';

export class MynaDBDataSource extends DataSourceApi<MynaDBQuery, MynaDBDataSourceOptions> {
  url: string;
  database: string;

  constructor(instanceSettings: DataSourceInstanceSettings<MynaDBDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.url || '';
    this.database = instanceSettings.jsonData.database || 'default';
  }

  async query(options: DataQueryRequest<MynaDBQuery>): Promise<DataQueryResponse> {
    const { range, maxDataPoints, interval, intervalMs } = options;

    // Build request payload
    const request = {
      range: {
        from: range.from.toISOString(),
        to: range.to.toISOString(),
      },
      maxDataPoints,
      interval,
      intervalMs,
      targets: options.targets
        .filter(target => !target.hide)
        .map(target => ({
          refId: target.refId,
          rawSql: this.interpolateVariables(target.rawSql || '', options.scopedVars, range),
          format: target.format || 'time_series',
        })),
    };

    // Send to MynaDB
    const response = await getBackendSrv().post(`${this.url}/grafana/query`, request);

    // Convert response to Grafana data frames
    return { data: this.transformResponse(response) };
  }

  async testDatasource() {
    try {
      await getBackendSrv().get(`${this.url}/health`);
      return {
        status: 'success',
        message: 'Successfully connected to MynaDB',
      };
    } catch (err) {
      return {
        status: 'error',
        message: err.message || 'Cannot connect to MynaDB',
      };
    }
  }

  async metricFindQuery(query: string): Promise<Array<{ text: string; value: string }>> {
    const response = await getBackendSrv().get(`${this.url}/grafana/search`, {
      target: query,
    });
    return response;
  }

  interpolateVariables(sql: string, scopedVars: any, range: any): string {
    // Use Grafana's template service
    return getTemplateSrv().replace(sql, scopedVars);
  }

  transformResponse(response: any[]): MutableDataFrame[] {
    const frames: MutableDataFrame[] = [];

    for (const item of response) {
      if (item.datapoints) {
        // Time series format
        const frame = new MutableDataFrame({
          name: item.target,
          fields: [
            { name: 'Time', type: FieldType.time },
            { name: 'Value', type: FieldType.number },
          ],
        });

        for (const [value, time] of item.datapoints) {
          frame.add({ Time: time, Value: value });
        }

        frames.push(frame);
      } else if (item.columns) {
        // Table format
        const frame = new MutableDataFrame({
          fields: item.columns.map((col: any) => ({
            name: col.text,
            type: this.mapType(col.type),
          })),
        });

        for (const row of item.rows) {
          frame.add(row);
        }

        frames.push(frame);
      }
    }

    return frames;
  }

  mapType(grafanaType: string): FieldType {
    switch (grafanaType) {
      case 'time':
        return FieldType.time;
      case 'number':
        return FieldType.number;
      case 'string':
        return FieldType.string;
      default:
        return FieldType.other;
    }
  }
}
```

### 2.3 Query Editor Component

**File:** `src/QueryEditor.tsx`

```typescript
import React from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { InlineField, Select, TextArea } from '@grafana/ui';
import { MynaDBDataSource } from './datasource';
import { MynaDBQuery, MynaDBDataSourceOptions } from './types';

type Props = QueryEditorProps<MynaDBDataSource, MynaDBQuery, MynaDBDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const queryModeOptions: Array<SelectableValue<string>> = [
    { label: 'SQL', value: 'sql' },
    { label: 'InfluxQL', value: 'influxql' },
    { label: 'Visual Builder', value: 'builder' },
  ];

  const formatOptions: Array<SelectableValue<string>> = [
    { label: 'Time series', value: 'time_series' },
    { label: 'Table', value: 'table' },
  ];

  return (
    <div>
      <InlineField label="Query Mode" labelWidth={14}>
        <Select
          options={queryModeOptions}
          value={query.queryMode || 'sql'}
          onChange={v => onChange({ ...query, queryMode: v.value! })}
          width={20}
        />
      </InlineField>

      <InlineField label="Format" labelWidth={14}>
        <Select
          options={formatOptions}
          value={query.format || 'time_series'}
          onChange={v => onChange({ ...query, format: v.value! })}
          width={20}
        />
      </InlineField>

      {(query.queryMode === 'sql' || !query.queryMode) && (
        <InlineField label="SQL Query" labelWidth={14} grow>
          <TextArea
            value={query.rawSql || ''}
            onChange={e => onChange({ ...query, rawSql: e.currentTarget.value })}
            onBlur={onRunQuery}
            rows={5}
            placeholder="SELECT time, value FROM metrics WHERE $__timeFilter(time)"
          />
        </InlineField>
      )}

      <div style={{ marginTop: 8, fontSize: 12, color: '#999' }}>
        <div>Variables: $__timeFilter(column), $__interval, $__from, $__to</div>
        <div>Example: SELECT time_bucket('$__interval', time) as time, avg(value) FROM cpu WHERE $__timeFilter(time) GROUP BY time</div>
      </div>
    </div>
  );
}
```

### 2.4 Configuration Editor

**File:** `src/ConfigEditor.tsx`

```typescript
import React, { ChangeEvent } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input } from '@grafana/ui';
import { MynaDBDataSourceOptions } from './types';

type Props = DataSourcePluginOptionsEditorProps<MynaDBDataSourceOptions>;

export function ConfigEditor({ options, onOptionsChange }: Props) {
  const onDatabaseChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...options.jsonData,
        database: event.target.value,
      },
    });
  };

  return (
    <div>
      <InlineField label="Default Database" labelWidth={20} tooltip="Default catalog/database to query">
        <Input
          value={options.jsonData.database || ''}
          onChange={onDatabaseChange}
          placeholder="default"
          width={40}
        />
      </InlineField>
    </div>
  );
}
```

### 2.5 Type Definitions

**File:** `src/types.ts`

```typescript
import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface MynaDBQuery extends DataQuery {
  rawSql?: string;
  queryMode?: 'sql' | 'influxql' | 'builder';
  format?: 'time_series' | 'table';
}

export interface MynaDBDataSourceOptions extends DataSourceJsonData {
  database?: string;
}
```

### 2.6 Plugin Manifest

**File:** `plugin.json`

```json
{
  "$schema": "https://raw.githubusercontent.com/grafana/grafana/master/docs/sources/developers/plugins/plugin.schema.json",
  "type": "datasource",
  "name": "MynaDB",
  "id": "mynadb-datasource",
  "metrics": true,
  "backend": false,
  "executable": "",
  "info": {
    "description": "MynaDB time series database datasource plugin",
    "author": {
      "name": "MynaDB Team",
      "url": "https://github.com/mynadb/mynadb"
    },
    "keywords": ["mynadb", "timeseries", "sql", "influxql"],
    "version": "1.0.0",
    "updated": "2026-02-06"
  },
  "dependencies": {
    "grafanaDependency": ">=9.0.0",
    "plugins": []
  }
}
```

---

## Phase 3: Testing & Refinement (Week 3)

### 3.1 Local Development Setup

```bash
# In grafana-mynadb-datasource/
npm install
npm run dev  # Watch mode with hot reload

# In MynaDB root
cargo run --release
```

### 3.2 Link Plugin to Grafana

```bash
# Option 1: Docker with volume mount
docker run -d \
  -p 3000:3000 \
  --name=grafana \
  -v "$(pwd)/grafana-mynadb-datasource/dist:/var/lib/grafana/plugins/mynadb-datasource" \
  grafana/grafana:latest

# Option 2: Local Grafana install
ln -s $(pwd)/grafana-mynadb-datasource/dist /var/lib/grafana/plugins/mynadb-datasource
systemctl restart grafana-server
```

### 3.3 Test Scenarios

1. **Connection Test**
   - Add datasource with MynaDB URL
   - Verify test button returns success

2. **Simple Query**
   - Execute: `SELECT * FROM cpu LIMIT 100`
   - Verify results display in table format

3. **Time Series Query**
   - Execute: `SELECT time, value FROM metrics WHERE $__timeFilter(time)`
   - Verify graph displays correctly

4. **Template Variables**
   - Create variable with query: `SELECT DISTINCT host FROM cpu`
   - Use variable in query: `WHERE host = '$host'`
   - Verify dropdown populates and filters work

5. **Time Range**
   - Change dashboard time range
   - Verify $__timeFilter updates correctly

---

## Phase 4: Publishing & Documentation (Week 4)

### 4.1 Package for Distribution

```bash
# Build production version
npm run build

# Create archive for Grafana marketplace
cd dist
zip -r ../mynadb-datasource-1.0.0.zip .
```

### 4.2 Submit to Grafana Marketplace

1. Sign plugin: https://grafana.com/docs/grafana/latest/developers/plugins/sign-a-plugin/
2. Submit to marketplace: https://grafana.com/plugins/submit/
3. Provide screenshots, documentation, examples

### 4.3 Documentation

**File:** `grafana-mynadb-datasource/README.md`

```markdown
# MynaDB Datasource for Grafana

Official Grafana datasource plugin for MynaDB time series database.

## Installation

### From Grafana Marketplace (Recommended)

1. Go to Configuration → Plugins
2. Search for "MynaDB"
3. Click Install

### Manual Installation

1. Download latest release from GitHub
2. Extract to Grafana plugins directory: `/var/lib/grafana/plugins/`
3. Restart Grafana

## Configuration

1. Add datasource: Configuration → Data Sources → Add data source → MynaDB
2. Set URL: `http://localhost:7000`
3. Set default database: `mydb`
4. Click "Save & Test"

## Query Examples

### Time Series Query

```sql
SELECT
  time_bucket('5m', time) as time,
  avg(cpu_usage) as value
FROM cpu
WHERE $__timeFilter(time)
  AND host = '$host'
GROUP BY time
ORDER BY time
```

### Table Query

```sql
SELECT * FROM cpu
WHERE $__timeFilter(time)
LIMIT 1000
```

## Variables

- `$__timeFilter(column)` - Replaced with time range filter
- `$__interval` - Dynamic interval based on dashboard range
- `$__from` - Range start timestamp (nanoseconds)
- `$__to` - Range end timestamp (nanoseconds)

## Troubleshooting

**Cannot connect to MynaDB**
- Verify MynaDB is running on configured port
- Check CORS settings if accessing from different domain

**Queries return no data**
- Verify time column name matches $__timeFilter
- Check table exists: `SHOW TABLES`

## Support

- GitHub: https://github.com/mynadb/mynadb
- Documentation: https://mynadb.io/docs
```

---

## Success Criteria

### Phase 1 (Backend) ✅
- [ ] `/grafana/search` endpoint returns table list
- [ ] `/grafana/query` executes SQL and returns time series format
- [ ] `/grafana/query` executes SQL and returns table format
- [ ] Variable interpolation works ($__timeFilter, $__interval)
- [ ] `/grafana/tag-keys` returns tag list
- [ ] `/grafana/tag-values` returns tag values

### Phase 2 (Frontend) ✅
- [ ] Plugin installs in Grafana
- [ ] Connection test succeeds
- [ ] Query editor displays SQL textarea
- [ ] Format selector (time series / table) works
- [ ] Queries execute and display results
- [ ] Error messages display correctly

### Phase 3 (Variables) ✅
- [ ] Template variables populate from queries
- [ ] Variables filter in dropdown
- [ ] $__timeFilter macro works
- [ ] $__interval macro works
- [ ] Custom variables interpolate correctly

### Phase 4 (Publishing) ✅
- [ ] Plugin signed with Grafana key
- [ ] Published to Grafana marketplace
- [ ] Documentation includes examples
- [ ] Screenshots show query builder
- [ ] Dashboard template available

---

## Resources

### Official Grafana Documentation
- [Build a Data Source Plugin](https://grafana.com/developers/plugin-tools/tutorials/build-a-data-source-plugin) - Official tutorial
- [Backend Component Tutorial](https://grafana.com/developers/plugin-tools/tutorials/build-a-data-source-backend-plugin) - Add Go backend
- [Getting Started](https://grafana.com/docs/grafana/latest/developers/plugins/) - Plugin development guide

### Tools
- **Grafana Plugin SDK**: https://github.com/grafana/grafana-plugin-sdk-typescript
- **Grafana Starter Template**: https://github.com/grafana/grafana-starter-datasource
- **create-plugin CLI**: https://www.npmjs.com/package/@grafana/create-plugin

### Reference Implementations
- InfluxDB datasource: https://github.com/grafana/influxdb-datasource
- Prometheus datasource: https://github.com/grafana/prometheus-datasource
- MySQL datasource: https://github.com/grafana/mysql-datasource

---

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Grafana API changes break compatibility | High | Pin to specific Grafana version, test against LTS releases |
| Time format mismatches | Medium | Thoroughly test nanosecond/millisecond conversions |
| Variable interpolation bugs | Medium | Extensive unit tests for macro replacement |
| Marketplace approval delays | Low | Submit early, follow guidelines precisely |
| Performance issues with large results | Medium | Implement pagination, use Arrow Flight for large datasets |

---

## Future Enhancements (Post-v1.0)

1. **Visual Query Builder**
   - Drag-and-drop metric selection
   - Visual time range picker
   - Aggregation function selector

2. **Alerting Integration**
   - Native alert rule creation from queries
   - Alert history display

3. **Streaming Data**
   - WebSocket connection for live dashboards
   - Real-time chart updates

4. **InfluxQL Mode**
   - Dedicated InfluxQL query editor
   - Syntax highlighting

5. **Advanced Features**
   - Query caching
   - Query history
   - Saved query templates
   - Multi-database queries

---

## Estimated Timeline

| Phase | Duration | Completion |
|-------|----------|------------|
| Phase 1: Backend API | 5 days | Week 1 |
| Phase 2: Plugin Scaffold | 5 days | Week 2 |
| Phase 3: Testing | 5 days | Week 3 |
| Phase 4: Publishing | 5 days | Week 4 |
| **Total** | **20 days** | **1 month** |

---

## Conclusion

Grafana integration is a **high-leverage feature** that significantly lowers MynaDB's adoption barrier. By providing an official, well-documented datasource plugin, MynaDB becomes a viable alternative to InfluxDB and TimescaleDB for teams already using Grafana.

**Next Steps:**
1. Approve this implementation plan
2. Assign developer resources
3. Set up Grafana plugin development environment
4. Begin Phase 1: Backend API implementation
