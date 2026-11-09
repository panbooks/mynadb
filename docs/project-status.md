# Project status and validation scope

Review date: 2026-11-09. Source baseline: `d603624` (the Rust code matches `cef8f07`; the remote README was updated).

MynaDB is a developing Rust time-series database using Arrow, Parquet and
DataFusion. Implementations and test files exist for more capabilities than
this review has independently exercised. No production-readiness or complete
InfluxDB, SQL, Prometheus, OTLP, Iceberg, or Grafana compatibility claim is made.

## Capability matrix

| Capability | Evidence in this tree | Current validation boundary |
| --- | --- | --- |
| Line protocol | `src/storage/line_protocol.rs`, parser unit tests | Float, signed integer (`i`), boolean and string code paths; explicit timestamps are parsed as nanoseconds. Parser tests and HTTP end-to-end results are recorded separately. Other timestamp precisions and all escaping cases are not certified. |
| SQL | `src/query/`, `tests/api_endpoints.rs`, `tests/correctness.rs` | DataFusion-based execution; use `/query/:db` for SQL and `/query?db=…&q=…` for InfluxQL. Not every upstream SQL feature or dashboard query has been tested. |
| Prometheus | `src/services/prometheus/`, `scripts/test_prometheus.sh` | Remote write/read implementation exists. No real Prometheus client/version has been validated in this review; do not conflate `/metrics` scraping with remote storage compatibility. |
| OTLP | `src/services/otlp/`, `/v1/metrics`, `/v1/logs`, `/v1/traces` routes | Metrics/logs/traces conversion and HTTP/gRPC code exists. SDK and Collector interoperability not independently tested here. |
| WAL | `src/storage/wal.rs`, `[wal]` config | Default `enabled=true`, `sync_on_write=false`; the developer local config explicitly disables WAL. The smoke script checks flushed Parquet restart, not WAL crash recovery, fsync durability, disk failure, or power-loss safety. |
| Cluster | `src/router/`, `src/data/`, `src/meta/` | Router/Data/Meta and Raft code exists. No multi-node topology or failure scenario has been rerun for this review; recovery still contains TODOs. |
| Grafana | `grafana-mynadb-datasource/`, `web/dashboard/` | Plugin source declares version 1.0.0; this is not evidence of marketplace publication, signing, installation, or a successful plugin build. |
| Iceberg | `src/iceberg/` | REST catalog/table-format implementation exists; independent engine interoperability is not certified. |

## Verified standalone checks on 2026-11-09

On Oracle Linux 9.5 x86_64 under WSL2, with Rust/Cargo 1.92.0 and protoc 29.3,
`cargo build --locked --profile fast-release --jobs 4` passed for `d603624`.
The resumed build took 890.13 seconds and reused dependency artifacts from an
interrupted earlier build; this is not a fresh-container build-time benchmark.

The unchanged README developer configuration passed health, one line-protocol
write (HTTP 204), explicit flush, SQL result checking, CLI stop, restart query,
and final stop/cleanup in an isolated working directory. The new smoke checker
also passed exact two-row SQL checks before flush and after restart using
temporary loopback ports and WAL disabled. These are small correctness checks,
not throughput measurements or complete protocol certification.

Core library unit tests passed: 22 line-protocol parser tests, 2 WAL entry/checksum
tests, and 66 InfluxQL tests (90 total, 0 failures). This is not the full test
suite, and the two WAL unit tests do not establish file or crash recovery. The
local package used opt-level=0 for test compilation; commands and full output
are in the portal readiness evidence. No external new-user trial is complete.

## Local evaluation and data persistence

Use [`examples/quickstart.toml`](../examples/quickstart.toml) instead of the root
`config.toml`, whose data path is `/data0/mynadb/data`. The new example binds
HTTP and Flight to loopback, disables MySQL and authentication, and stores local
data under `data/quickstart`. It is for development evaluation.

The example disables WAL to make its limits explicit. Unflushed data can be lost
on an abrupt exit. `POST /flush` must succeed before a restart-persistence check;
such a result says nothing about unflushed writes surviving a crash.
For WAL experiments, explicitly set `[wal] enabled=true` and select
`sync_on_write` deliberately. Validate the failure scenarios relevant to your
deployment before trusting valuable data to it.

## Readiness evidence

The promotion checklist in the portal repository records build, smoke and unit
test outcomes. A clean Git checkout on an existing Linux host is not a clean
container, and an automated run is not an independent new-user trial.
Until successful commands and actual outputs are recorded, startup and recovery
steps remain unverified.

Current gaps include root license/ownership confirmation, client-version
compatibility coverage, reproducible release builds, and an independent
getting-started trial. Evaluate locally with disposable data while these gaps
are being addressed.
