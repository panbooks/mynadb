# Contributor task drafts

Prepared 2026-11-09. These are reviewable local drafts, not published GitHub
issues. Avoid assigning `good first issue` to architecture or data-loss fixes.
Create the issues only after checking the latest tree for an existing solution.

## 1. Check Markdown documentation links

Suggested labels: documentation, good first issue.

Background: documentation has moved repeatedly. Related files: `README.md`,
`docs/`, `CONTRIBUTING.md`. Add a checker for local Markdown file targets,
allowing fragments, external URLs and intentional placeholders explicitly.
Acceptance: every reported missing file is fixed or explained; the checker
fails on a deliberately broken link. Verify with a local run, not the Rust suite.

## 2. Validate Quick Start as a first-time user

Suggested labels: documentation, good first issue.

Related files: `README.md`, `examples/quickstart.toml`, `CONTRIBUTING.md`, and
portal quick-start pages. The maintainer's automated run cannot stand in for
an independent new-user trial. Acceptance: a developer who has not worked on
MynaDB follows the published instructions, records OS/toolchain/source SHA and
prerequisite installation, checks health/write/query/flush/restart/stop, and
reports every ambiguous step or failure. Verify actual returned rows and
cleanup; propose focused documentation fixes with exact reproduction steps.

## 3. Verify line protocol escaping and invalid timestamps

Suggested labels: help wanted, tests.

Related files: `src/storage/line_protocol.rs`, `tests/error_handling.rs`.
The parser uses `.ok()` while handling batches and accepts missing/invalid
timestamp parse results as `None`. Document current behavior with small
regressions before proposing stricter validation. Acceptance: quoted spaces,
escaped commas, malformed timestamps and mixed valid/invalid batches have
explicit expected results. Verify parser tests plus an isolated HTTP check.

## 4. Publish reproducible Prometheus interoperability results

Suggested labels: help wanted, compatibility.

Related files: `src/services/prometheus/`, `scripts/test_prometheus.sh`.
Acceptance: pin a real Prometheus version; record endpoint paths, config,
write/read payloads, label mapping, time precision, and observed query results.
Verify with a real client; a successful `/metrics` scrape alone is insufficient.

## 5. Publish reproducible OTLP SDK/Collector results

Suggested labels: help wanted, compatibility.

Related files: `src/services/otlp/`. Acceptance: pin SDK and Collector versions,
test metrics/logs/traces individually, name HTTP/protobuf vs JSON or gRPC, and
query ingested rows. Document unsupported metric kinds and failures. Verify
actual client output rather than only hand-written JSON requests.

## 6. Add a WAL recovery scenario matrix

Suggested labels: help wanted, tests.

Related files: `src/storage/wal.rs`, `src/node/common.rs`, WAL configuration.
Acceptance: distinguish WAL disabled, asynchronous WAL, synchronous WAL,
explicit flush, graceful shutdown, abrupt process termination, truncated WAL,
and restart. Record exact acknowledged rows and recovered rows. Process-crash
tests must not be described as power-loss or device-failure certification.

## 7. Make HTTP integration tests isolate every listener

Suggested labels: help wanted, tests.

Related files: `tests/common/mod.rs`, `src/config.rs`, `src/node/standalone.rs`.
The harness chooses an HTTP port but writes `flight_enabled=false`, a field
absent from `ServerConfig`; standalone Flight may still use its default port.
Acceptance: determine and test current behavior, reserve/configure distinct
Flight ports, and run two test instances concurrently without port conflicts.
Verify actual listener behavior and graceful process cleanup.

## 8. Build and install the Grafana plugin with a pinned Grafana version

Suggested labels: help wanted, integration.

Related files: `grafana-mynadb-datasource/`. Acceptance: record Node/npm/Grafana
versions, dependency resolution, build result, unsigned-plugin setup if needed,
and a working dashboard query. Separate local use, plugin signing, and catalog
publication. Verify results in Grafana; package version 1.0.0 is not a release.

## 9. Replace the metadata gRPC health placeholder

Suggested labels: help wanted; not a first issue.

Related file: `src/meta/service/grpc.rs` (currently `healthy: true` with a TODO).
Acceptance: define readiness semantics, implement them without exposing secrets,
and cover unavailable leader/storage and startup transitions. Verify behavior
against a multi-node test topology. Avoid presenting a static response as a
tested fault-detection mechanism.

## 10. Make release version and binary provenance reproducible

Suggested labels: help wanted, release.

Related files: `Cargo.toml`, `Cargo.lock`, `build.rs`, release workflow.
Current GitHub Release `v0.0.1`, Cargo package 0.1.0, and historical tags differ.
Acceptance: document the version scheme, supported build targets, source SHA,
toolchain, test results, binary checksums and installation steps. Verify artifact
download/checksum/startup from a clean directory before claiming availability.
