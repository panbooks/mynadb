# InfluxQL Query Language Support

Native InfluxQL parser and transpiler for MynaDB. Parses InfluxQL queries, converts them to SQL, and executes via DataFusion.

```
InfluxQL string → Lexer → Parser → AST → Transpiler → SQL → DataFusion
```

## Quick Start

### HTTP API

```bash
# SELECT query
curl "http://localhost:7000/influxql/query?db=mydb&q=SELECT mean(usage) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)"

# With database in path
curl "http://localhost:7000/influxql/query/mydb?q=SELECT * FROM cpu LIMIT 10"

# POST (query in body)
curl -X POST "http://localhost:7000/influxql/query?db=mydb" -d "q=SELECT * FROM cpu"
```

### Response Format

Responses use InfluxDB-compatible JSON:

```json
{
  "results": [
    {
      "statement_id": 0,
      "series": [
        {
          "name": "cpu",
          "columns": ["time", "usage", "host"],
          "values": [
            ["2024-01-01T12:00:00Z", 23.5, "server01"],
            ["2024-01-01T12:01:00Z", 55.2, "server01"]
          ]
        }
      ]
    }
  ]
}
```

Grouped queries include tags:

```json
{
  "results": [{
    "series": [
      {
        "name": "cpu",
        "tags": { "host": "server01" },
        "columns": ["time", "mean_usage"],
        "values": [...]
      },
      {
        "name": "cpu",
        "tags": { "host": "server02" },
        "columns": ["time", "mean_usage"],
        "values": [...]
      }
    ]
  }]
}
```

## Supported Syntax

### SELECT

```sql
-- Basic
SELECT * FROM cpu
SELECT usage, temp FROM cpu

-- Aliases
SELECT mean(usage) AS avg_usage FROM cpu

-- WHERE clause
SELECT * FROM cpu WHERE host = 'server01' AND usage > 50
SELECT * FROM cpu WHERE time > now() - 1h

-- GROUP BY time (with fill)
SELECT mean(usage) FROM cpu GROUP BY time(5m)
SELECT mean(usage) FROM cpu GROUP BY time(1h) fill(previous)
SELECT mean(usage) FROM cpu GROUP BY time(5m) fill(0)
SELECT mean(usage) FROM cpu GROUP BY time(5m) fill(linear)
SELECT mean(usage) FROM cpu GROUP BY time(5m) fill(none)

-- GROUP BY tags
SELECT mean(usage) FROM cpu GROUP BY host
SELECT mean(usage) FROM cpu GROUP BY time(5m), host, region

-- ORDER BY / LIMIT / OFFSET
SELECT * FROM cpu ORDER BY time DESC
SELECT * FROM cpu LIMIT 100 OFFSET 50

-- Qualified measurement names
SELECT * FROM mydb.default.cpu
SELECT * FROM mydb..cpu

-- Arithmetic
SELECT (usage * 100) / total FROM cpu
```

### Aggregate Functions

| InfluxQL | SQL Output | Description |
|----------|-----------|-------------|
| `mean(x)` | `AVG(x)` | Average |
| `count(x)` | `COUNT(x)` | Count |
| `sum(x)` | `SUM(x)` | Sum |
| `min(x)` | `MIN(x)` | Minimum |
| `max(x)` | `MAX(x)` | Maximum |
| `first(x)` | `first_value(x, time)` | First by time |
| `last(x)` | `last_value(x, time)` | Last by time |
| `stddev(x)` | `STDDEV(x)` | Standard deviation |
| `spread(x)` | `MAX(x) - MIN(x)` | Max minus min |
| `percentile(x, n)` | `approx_percentile_cont(x, n)` | Nth percentile |
| `moving_average(x, n)` | Window AVG | Moving average over n points |
| `cumulative_sum(x)` | Window SUM | Running total |
| `difference(x)` | `x - LAG(x)` | Difference from previous |
| `derivative(x)` | `x - LAG(x)` | Rate of change |
| `elapsed(x)` | `time - LAG(time)` | Time between points |

### SHOW Statements

```sql
SHOW DATABASES
SHOW MEASUREMENTS
SHOW MEASUREMENTS ON mydb
SHOW TAG KEYS FROM cpu
SHOW TAG VALUES FROM cpu WITH KEY = "host"
SHOW TAG VALUES FROM cpu WITH KEY IN ("host", "region")
SHOW FIELD KEYS FROM cpu
SHOW RETENTION POLICIES
SHOW RETENTION POLICIES ON mydb
```

### Duration Units

| Unit | Meaning |
|------|---------|
| `ns` | Nanoseconds |
| `u`, `us` | Microseconds |
| `ms` | Milliseconds |
| `s` | Seconds |
| `m` | Minutes |
| `h` | Hours |
| `d` | Days |
| `w` | Weeks |

## Programmatic Usage (Rust)

```rust
use mynadb::query::influxql::{parse, transpile, influxql_to_sql, TranspileContext};

// One-step: InfluxQL → SQL
let ctx = TranspileContext {
    database: "mydb".to_string(),
    schema: "default".to_string(),
};
let sql = influxql_to_sql(
    "SELECT mean(usage) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)",
    &ctx,
)?;
// sql = "SELECT AVG(usage), time_bucket('5 minute', time) AS time
//        FROM mydb.default.cpu WHERE (time > (now() - INTERVAL '5 minute'))
//        GROUP BY time_bucket ORDER BY time_bucket ASC"

// Two-step: parse then transpile
let stmt = parse("SELECT * FROM cpu LIMIT 10")?;
let sql = transpile(&stmt, &ctx)?;

// Extract metadata from parsed statement
use mynadb::query::influxql::{get_measurement_name, has_tag_grouping};
let name = get_measurement_name(&stmt);       // Some("cpu")
let tags = has_tag_grouping(&stmt);           // None
```

## Module Structure

```
src/query/influxql/
├── mod.rs          # Public API, influxql_to_sql(), error types
├── ast.rs          # AST types: Statement, Expr, Duration, FillOption, ...
├── lexer.rs        # Tokenizer: InfluxQL string → Token stream
├── parser.rs       # Parser: Token stream → AST
├── transpiler.rs   # Transpiler: AST → SQL string
└── response.rs     # InfluxDB JSON response formatting
```

## Testing

```bash
# Unit tests (33 tests)
cargo test --lib influxql

# Integration test (requires running server)
./scripts/test_influxql.sh
```
