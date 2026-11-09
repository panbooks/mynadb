<p align="center">
  <a href="https://panbooks.github.io/mynadb-portal/zh/docs/index.html">
    <img src="web/portal/images/logo-icon.svg" alt="MynaDB logo" width="120">
  </a>
</p>

<h1 align="center">MynaDB</h1>

<p align="center"><strong>The Voice of Your Machines</strong><br>
A time-series database for collecting, storing, and querying telemetry.</p>

<p align="center">
  <a href="#quick-start"><strong>Quick Start</strong></a> &nbsp;·&nbsp;
  <a href="https://panbooks.github.io/mynadb-portal/zh/docs/index.html"><strong>Documentation</strong></a> &nbsp;·&nbsp;
  <a href="#architecture"><strong>Architecture</strong></a> &nbsp;·&nbsp;
  <a href="#http-api-examples"><strong>API</strong></a> &nbsp;·&nbsp;
  <a href="grafana-mynadb-datasource/README.md"><strong>Grafana</strong></a>
</p>

---

MynaDB is a time-series database written in Rust for collecting, storing, and querying machine and application telemetry. It accepts InfluxDB line protocol and other telemetry formats, stores data in Apache Parquet, and uses Apache DataFusion to execute SQL queries.

**Project status:** MynaDB is under development. Implemented features are not a
guarantee of complete protocol compatibility or production readiness. Read the
[capability and validation matrix](docs/project-status.md) before evaluation.

## Features

- HTTP ingestion using InfluxDB line protocol, Prometheus remote write, and OpenTelemetry protocols.
- SQL queries through HTTP, Apache Arrow Flight, and a MySQL-compatible interface; an InfluxQL-compatible HTTP endpoint is also available.
- Arrow-based in-memory buffering and Parquet storage on a local filesystem or supported S3-compatible object storage.
- Write-ahead logging and recovery, background flushing, compaction, retention, and query caching.
- Standalone operation or a cluster with separate metadata, router, data, and admin nodes. Metadata nodes use Raft.
- Authentication and authorization, TLS configuration, metrics, a cluster admin console, and a Grafana data-source plugin.

See the documentation for configuration and feature details; availability and behavior depend on the chosen deployment and settings.

## Architecture

In standalone mode, the services run in one process. A typical write and query follows this path:

```text
Clients (HTTP, Prometheus, OTLP)                 SQL clients (HTTP, Flight, MySQL)
              |                                                |
              v                                                v
      Parse and route writes                            DataFusion query engine
              |                                                |
       WAL, if enabled                                         |
              |                                                |
       In-memory cache <---------------------------------------+
              |
       Background or manual flush
              v
       Parquet files in local or object storage <---------------+
              |
       Compaction and retention jobs
```

Writes are grouped by database, schema, and table in memory. The cache batches writes before flushing them to columnar Parquet files; this reduces the number of small files compared with writing a file for each request. Queries can read buffered as well as persisted data. A configurable write-ahead log (WAL) supports recovery; its durability depends on the WAL and synchronization settings. Background compaction consolidates files, and retention can remove data according to configured policies.

In cluster mode, **routers** accept and route requests, **data nodes** store and query data, and **metadata nodes** coordinate cluster metadata using Raft. An **admin node** provides cluster management. See the [architecture overview](docs/architecture/architecture-overview.md) and [cluster architecture](docs/architecture/architecture-cluster-v2.md).

## Install from source

### Prerequisites

- Git and a recent stable Rust toolchain with Cargo ([install Rust](https://rustup.rs/)).
- A C/C++ build toolchain, `pkg-config`, OpenSSL development headers, and the Protocol Buffers compiler (`protoc`). On Debian or Ubuntu, for example:

  ```bash
  sudo apt-get update
  sudo apt-get install -y build-essential pkg-config libssl-dev protobuf-compiler
  ```

- `curl` for the examples below. The first build downloads dependencies and can take several minutes and substantial disk space.

Clone and build the project:

```bash
git clone https://github.com/panbooks/mynadb.git
cd mynadb
cargo build --locked
```

The executable is `target/debug/mynadb`. For an optimized build, use `cargo build --release --locked` and substitute `target/release/mynadb` in the commands below.

## Quick start

From the repository root, start a standalone instance with the local development configuration:

```bash
./target/debug/mynadb -c dev/configs/standalone/local.toml standalone
```

Leave this terminal running and open another terminal. The configuration uses local directories `dev/data/standalone` and `dev/logs/standalone`, relative to the working directory. It listens on HTTP port `7000`, Arrow Flight port `7001`, and MySQL port `7002`.

Check that the server is ready:

```bash
curl http://127.0.0.1:7000/health
```

Write a point, then query it:

```bash
curl -i -X POST http://127.0.0.1:7000/write/demo \
  --data-binary 'cpu,host=server01 usage=23.5'

curl -X POST http://127.0.0.1:7000/flush

curl -X POST http://127.0.0.1:7000/query/demo \
  --data-binary 'SELECT * FROM cpu'
```

A successful write returns HTTP `204 No Content`; the flush persists the buffered point, and the query returns JSON containing it. With this configuration, a measurement without a `service` tag maps to the `default` schema. The database name is supplied in the URL. A timestamp omitted from line protocol is assigned at ingestion.

Stop the server with `Ctrl+C`, or from the repository root in another terminal:

```bash
./target/debug/mynadb -c dev/configs/standalone/local.toml stop
```

The local example disables authentication, TLS, retention, and the write-ahead log and binds to all interfaces. It is intended for local experimentation; cached data can be lost on a crash. Restrict network access and configure authentication, TLS, durability, retention, and storage before using a deployment for real workloads.

## InfluxDB line protocol

A line represents a point. The general format is:

```text
measurement[,tag_key=tag_value...] field_key=field_value[,field_key=field_value...] [timestamp]
```

The measurement identifies the table; tags are string-valued dimensions such as host or region; fields contain the values being recorded. Separate the measurement and tags from fields with a space, and separate an optional timestamp from the fields with another space. Timestamps are Unix time in nanoseconds; when omitted, the current time is used.

| Field type | Example | Meaning |
| --- | --- | --- |
| Float | `usage=23.5` | Floating-point value; an unsuffixed number is parsed as a float |
| Integer | `cores=8i` | Signed integer with an `i` suffix |
| Boolean | `active=true` | Boolean; `true`, `false`, `t`, and `f` are supported |
| String | `status="online"` | Quoted text |

```text
cpu usage=23.5
cpu,host=server01,region=us-west usage=23.5,temp=45i
cpu,host=server01 usage=23.5 1672531200000000000
weather,location=paris temperature=22.5,humidity=60i,conditions="sunny"
sensor,device=temp01 value=98.6,count=42i,active=true,status="online"
```

Send multiple points in one request by placing each point on a separate line. For example:

```bash
curl -i -X POST http://127.0.0.1:7000/write/demo --data-binary '
cpu,host=server01 usage=50.0,temp=45i
memory,host=server01 used=16.5,total=32.0
network,host=server01,interface=eth0 rx_bytes=1024000i,tx_bytes=2048000i
'
```

### Database, schema, and table mapping

The write URL selects the database (also called a catalog). In the quick-start configuration, `service_as_schema` maps the first matching configured service tag to the schema and the measurement to the table. Without such a tag, the schema is `default`: `cpu,host=server01 usage=23.5` written to `demo` maps to `demo.default.cpu`. For example, `cpu,service=production usage=23.5` maps to `demo.production.cpu`. Numeric service values can be prefixed with `service_` to form SQL-friendly names. The alternative `service_as_table` strategy uses the service value as the table name. Configure the strategy and tag keys in `[schema]`.

## Query examples

SQL requests use the database in the URL as the query context. The examples below use the data written to `demo` above:

```bash
# Retrieve recent CPU readings
curl -X POST http://127.0.0.1:7000/query/demo \
  --data-binary 'SELECT * FROM cpu ORDER BY time DESC LIMIT 10'

# Aggregate by host
curl -X POST http://127.0.0.1:7000/query/demo \
  --data-binary 'SELECT host, AVG(usage) AS avg_usage FROM cpu GROUP BY host'

# Inspect available tables and the CPU schema
curl http://127.0.0.1:7000/tables/demo
curl http://127.0.0.1:7000/schema/demo/default/cpu
```

For a GET request, let `curl` encode the SQL query parameter:

```bash
curl --get http://127.0.0.1:7000/query/demo \
  --data-urlencode 'sql=SELECT * FROM cpu LIMIT 10'
```

DataFusion also supports SQL operations such as filtering, joins, and window functions. Use `schema.table` or `database.schema.table` when the table is outside the default schema. See the [query examples](docs/guides/guide-correct-query-examples.md).

## Configuration

MynaDB loads `config.toml` from the current directory by default; select a file with `-c` or `--config`. The checked-in root configuration uses `/data0/mynadb/data` and `/data0/mynadb/logs`, so make those paths writable or change them before running with that file. If a configuration file cannot be loaded, the application falls back to built-in defaults; check the startup logs to confirm the intended settings.

The local example flushes its write cache every 60 seconds. To request a flush manually:

```bash
curl -X POST http://127.0.0.1:7000/flush
```

Relevant TOML sections include `[server]`, `[storage]`, `[schema]`, `[cache]`, `[wal]`, `[compaction]`, `[retention]`, `[auth]`, `[tls]`, and `[logging]`. See [config.toml](config.toml), [the local example](dev/configs/standalone/local.toml), and the [configuration guide](docs/guides/guide-configuration.md).

## HTTP API examples

| Operation | Endpoint |
| --- | --- |
| Health check | `GET /health` |
| Write line protocol | `POST /write/:db` or `POST /write?db=:db` |
| Run SQL | `GET /query/:db?sql=...` or `POST /query/:db` with SQL as the body |
| List databases, schemas, and tables | `GET /databases`, `GET /schemas/:db`, `GET /tables/:db` |
| Inspect a table schema | `GET /schema/:db/:schema/:table` |
| Flush buffered data | `POST /flush` |
| Prometheus remote write / read | `POST /api/v1/write/:db`, `POST /api/v1/read/:db` |
| OpenTelemetry HTTP ingestion | `POST /v1/metrics`, `POST /v1/logs`, `POST /v1/traces` |
| Server metrics | `GET /metrics` |

For example, inspect the quick-start table with `curl http://127.0.0.1:7000/schema/demo/default/cpu`. Line protocol supports tags and float, integer (`42i`), boolean, and quoted string fields; timestamps are in nanoseconds by default. The schema mapping is configurable, including mapping a service tag to a schema.

## Storage and maintenance

The standalone local configuration stores runtime data under `dev/data/standalone`. Its layout includes:

```text
dev/data/standalone/
├── store/    # Parquet data, organized by catalog, schema, table, and date
├── wal/      # Write-ahead log files when enabled
├── meta/     # Metadata and job definitions
├── cache/    # Local cache used with object storage
├── system/   # System data
└── run/      # Process ID files
```

The schema is inferred from the points: tags become string columns, and fields become columns of their respective types. Use the schema endpoint to inspect the actual table rather than assuming all measurements have the same columns.

Useful maintenance requests include:

```bash
curl http://127.0.0.1:7000/stats
curl -X POST http://127.0.0.1:7000/flush
curl -X POST http://127.0.0.1:7000/compact/demo
curl -X POST http://127.0.0.1:7000/compact/demo/default/cpu
```

Flush writes buffered points to storage; compaction merges eligible files to reduce file count and improve read efficiency. Both may also run in the background based on configuration. See the [compaction guide](docs/guides/guide-compaction-quickstart.md) and [WAL recovery documentation](docs/features/features-wal-flush-recovery.md).

To inspect a flushed local Parquet file, first run the flush request above, then locate a file with `find dev/data/standalone/store -name '*.parquet'`. With Python and PyArrow installed, replace the path below with the file found:

```python
import pyarrow.parquet as pq

table = pq.read_table("path/to/file.parquet")
print(table.schema)
print(table.to_pandas())
```

## Local cluster

The development cluster starts three metadata nodes, one router, two data nodes, and an admin process. It requires [goreman](https://github.com/mattn/goreman), in addition to the build prerequisites:

```bash
cargo build --locked
./dev/start-cluster.sh
```

The router exposes the HTTP API at `http://127.0.0.1:7000`. The admin service listens at `http://127.0.0.1:18080/admin`; build the UI first with `cd admin-ui && npm install && npm run build` if needed. Press `Ctrl+C` to stop the processes. See [dev/README.md](dev/README.md), the [cluster guide](docs/guides/guide-cluster-v2.md), and [development configurations](dev/configs/cluster) for details. The startup script reuses an existing debug binary, so rebuild after changing source code.

## Testing

### Manual smoke test

Start the standalone server as described above, then send a point containing several field types and verify it through SQL:

```bash
curl -i -X POST http://127.0.0.1:7000/write/testdb \
  --data-binary 'sensor,device=temp01 temperature=72.5,humidity=55i,active=true,status="normal"'

curl -X POST http://127.0.0.1:7000/query/testdb \
  --data-binary 'SELECT * FROM sensor'

curl -X POST http://127.0.0.1:7000/flush
curl -X POST http://127.0.0.1:7000/query/testdb \
  --data-binary 'SELECT * FROM sensor'
```

The write should return HTTP 204 and both queries should return the point. The second query checks the read path after a flush. Repeating the write adds another point, so use a fresh database name if you need an isolated run. Check the schema and Parquet files using the commands in the preceding sections.

### Automated tests

```bash
cargo test --locked
```

The repository includes unit tests and integration suites covering API endpoints, queries, data integrity, authentication, concurrency, schema strategies, and retention. Some integration tests are marked ignored and launch their own server with temporary storage; they are not run by the default command. For example, run the API endpoint suite explicitly with:

```bash
cargo build --locked
cargo test --locked --test api_endpoints -- --ignored --test-threads=1
```

These tests spawn a local server and need available local ports. See [tests](tests) for individual suites and their requirements.

## Development and troubleshooting

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and focused checks.
For a loopback-only evaluation configuration and an automated check of health,
write, query, explicit flush, restart, and cleanup:

```bash
./target/debug/mynadb -c examples/quickstart.toml standalone
# Or, with no server running, use an isolated temporary test instance:
python3 scripts/validation/quickstart_smoke.py --binary target/debug/mynadb
```

The automated check disables WAL and verifies explicitly flushed Parquet data;
it does not test crash recovery. The [project status](docs/project-status.md)
document records the scope of the promotion readiness review.

```bash
cargo test --locked             # Run the Rust test suite
cargo run --bin mynadb -- --help  # Show command-line options
```

If startup fails, check that ports 7000–7002 are free and the configured data and log paths are writable. If the build cannot find `protoc` or OpenSSL, install the prerequisites above and retry. Inspect the server output and the configured log directory for errors. For object storage, configure its endpoint, credentials, and bucket before starting.

## Project structure

```text
mynadb/
├── src/
│   ├── main.rs          # CLI entry point and node selection
│   ├── node/            # Standalone and cluster node startup
│   ├── services/        # HTTP, Flight, MySQL, Prometheus, and OTLP services
│   ├── storage/         # Line protocol, cache, WAL, and Parquet storage
│   ├── query/           # DataFusion query execution
│   ├── meta/            # Cluster metadata and Raft
│   ├── router/          # Request routing and distributed queries
│   ├── data/            # Data node services
│   ├── auth/            # Authentication and authorization
│   └── jobs/            # Background maintenance
├── admin-ui/            # Cluster management frontend
├── grafana-mynadb-datasource/  # Grafana integration
├── dev/                 # Development configurations and scripts
├── docs/                # Architecture, feature, and operations guides
├── tests/               # Integration and correctness tests
├── Cargo.toml           # Rust dependencies and build targets
└── config.toml          # Example default configuration
```

The main technologies include Tokio and Axum for asynchronous services, Apache Arrow and Parquet for columnar data, DataFusion for SQL, and OpenRaft for cluster metadata. See [Cargo.toml](Cargo.toml) for the complete dependency list.

## License status

The main project's license and code ownership are pending maintainer confirmation
in the readiness review. The portal's Apache-2.0 license and Grafana plugin
metadata do not, by themselves, establish the license for this whole repository.
