# MynaDB development history: ten release-sized milestones

This is a retrospective roadmap inferred from Git history and file diffs. The milestone numbers are analytical groupings, not existing release versions or claims of production readiness. Features listed in later milestones build on the earlier ones unless explicitly removed or replaced.

## Scope and evidence

- Local `main`: **487 reachable commits**, from `6687907` (2025-12-22) through `d603624` (2026-11-09), including merged branch commits. Local `main` advanced from `cef8f07` to include two README commits during the audit; this report includes them.
- All available local branches, remote-tracking refs, and tags at the final audit snapshot: **2,235 commits**. Local `main` and `origin/main` both point to `d603624`. No fetch was performed.
- Comparing each commit against its first parent, or the empty tree for a root, gives **511 distinct file-transition sets**. Equality here means identical paths, file modes, statuses, and before/after blob IDs, with rename detection disabled. This is exact change equivalence, not a claim that differently structured patches are semantically different features.
- All commit file changes were inventoried. Each distinct patch was processed for added/removed declaration text; milestone-defining changes and current integration limits were checked directly in source. This is a historical analysis, not an exhaustive correctness or compatibility audit. No tests or benchmarks were run.
- Dates below use author dates on local `main`. Several refs are retimed or consolidated histories; commit ancestry and actual content take precedence over dates and commit wording. Uncommitted workspace changes are excluded.

The [commit diff ledger](development-commit-diffs.md) lists every local-main commit and all 25 additional transition sets. The [commit index](development-commit-index.csv) maps every one of the 2,235 commit IDs to its change-set representative, diff totals, and milestone where applicable.

## Milestone overview

| Milestone / release theme | Local-main dates | Commits | Boundary commit | Main capability gained |
|---|---|---:|---|---|
| M1 — Standalone database foundation | Dec 22–27, 2025 | 43 | `aaaabda` | Ingest line protocol, persist Parquet, query SQL through multiple interfaces |
| M2 — Durability and standalone operations | Dec 29, 2025–Jan 12, 2026 | 65 | `3291005` | Recover writes, enforce retention, authenticate users, control background work |
| M3 — First replication and HA design | Jan 13–15, 2026 | 15 | `3f7d7af` | Operate an initial master/replica deployment |
| M4 — Object-store pivot and faster execution | Jan 19–22, 2026 | 22 | `4f71930` | Separate storage access and optimize write/query paths |
| M5 — Query-engine maturity and validation | Jan 23–24, 2026 | 49 | `64acecb` | Query fresh data immediately, use TSDB functions, prune/cache Parquet reads |
| M6 — Distributed database and Raft control plane | Jan 25–29, 2026 | 56 | `34f3bf3` | Split Router/Data/Meta roles with shared storage and consensus |
| M7 — Recovery, standby redesign, and safe scale-in | Jan 30–Feb 2, 2026 | 51 | `bb96dda` | Retire nodes safely and recover without promoting standbys |
| M8 — Table ownership and partition-aware routing | Feb 3–6, 2026 | 78 | `c49b32e` | Route by table ownership and discover physical Hive partitions efficiently |
| M9 — Operator console, ecosystem, and advanced TSDB features | Feb 6–7, 2026 | 50 | `ad8a4be` | Manage the cluster, integrate monitoring, secure access, add analytics |
| M10 — Correctness, observability ingestion, and documentation | Feb 9–Nov 9, 2026 | 58 | `d603624` | Harden real workloads, add OTLP and an initial Iceberg implementation |

The two November README commits extend M10's documentation work; database source changes in this stage end in July.

## M1 — Standalone database foundation

**Features introduced**

- HTTP server and line-protocol decoder, buffered writes, and Parquet output.
- DataFusion SQL query engine and database management.
- InfluxDB-compatible write endpoint and measurement-based storage directories.
- Leveled, major, and nonpartitioned compaction; schema alignment across batches.
- Date partitions for stored data and series tracking.
- Series/cardinality tracking, background persistence, configurable schema tag extraction, and the early PMC naming.
- Arrow Flight service and MySQL client connectivity for queries.
- TOML configuration, logging, build/test scripts, bounded write-cache memory, and hashed series-cache keys.

**Evidence:** `c5c2011` adds the write path, `9e09dc4` adds DataFusion, `f1e2150` adds Flight, `f0815d2` adds date partitioning, and `2d96481` adds the MySQL service.

**Release meaning:** the first complete standalone ingestion-to-query loop. The specialized series-tracking implementation is historical; M4 deletes it.

## M2 — Durability and standalone operations

**Features introduced**

- Write-ahead logging, startup replay, immediate flushing after recovery, WAL rotation fixes, and a recovery mode retaining WAL files.
- Background retention for stored data and series caches, per-database caches, and startup cache loading.
- Bloom-filter-based series deduplication, age-based compaction, full compaction, and exclusion of oversized compaction candidates.
- Concurrent compaction by table, I/O rate limiting, and separate full-compaction concurrency controls. An experimental compaction memory budget is added and then removed within this stage.
- Configurable service-versus-measurement table mapping and optional raw-series persistence.
- Basic authentication, URI user/password support, auth test scripts, memory diagnostics, and CPU/heap profiling.
- Faster parallel startup loading and registration of only the table needed by a query.
- Go metric-sender tooling with configuration/retries/timeouts, deployment scripts, a version command, and the first public documentation website.

**Evidence:** `4591fff` adds WAL, `43073fe` adds replay, `ab30fbf` adds authentication, and `5156a88` refactors compaction with tests.

**Release meaning:** a more operable standalone database with explicit recovery mechanisms. The metric sender and series-deduplication subsystem are subsequently removed in M4.

## M3 — First replication and HA design

**Features introduced**

- Initial master/replica mode, replication transport, replica state, failure detection, and promotion/failover machinery.
- Configuration defaults, HA unit/integration tests, and local failover demonstrations.
- Architecture proposals exploring cluster and shared-storage alternatives.
- HA, retention, line-protocol, and Flight documentation improvements.

**Evidence:** `25a6d8c` adds master/replica mode; `895ad48` and `83781c7` add tests.

**Release meaning:** an experimental HA branch of the product's architecture. **It is removed in M4** and must not be described as the HA implementation inherited by the current cluster.

## M4 — Object-store pivot and faster execution

**Changes and features introduced**

- Delete the specialized series tracker/deduplicator, Go metric sender, and initial HA implementation.
- Introduce an object-store abstraction and configurable local/cloud storage backends.
- Rework WAL persistence and add periodic persistence scheduling.
- Add SQL `EXPLAIN` and time-range pruning using Parquet statistics.
- Sort data by time before flush and merge sorted streams during compaction.
- Shard write caches; add background WAL writing when synchronous writes are disabled.
- Cache conversion schemas and parallelize line parsing with Rayon.
- Add table/Parquet metadata caches, query-result caching, smarter partition pruning, parallel Parquet reading, and column projection pushdown.
- Return HTTP 204 from successful writes for InfluxDB compatibility.

**Evidence:** `c1d5c08` and `e3d17d6` remove the old subsystems; `0640459` adds object storage; `489e668`, `a18eed4`, and `b4662e6` change query/write execution.

**Release meaning:** the architectural reset toward a general time-series engine with shared storage. Commit-message throughput/QPS figures are historical measurements, not independently verified guarantees.

## M5 — Query-engine maturity and validation

**Features introduced**

- Custom Parquet table providers, query analyzers/optimizers, and modular query-engine organization.
- Memtable query integration: fresh writes become queryable before Parquet flush.
- `GROUP BY time(...)`/`time_bucket`, first/last aggregation functions, and the `time` timestamp-column convention.
- In-memory metadata management and local disk caching of remote Parquet files.
- Read-side Bloom-filter use, predicate pushdown, decoded hot-partition caching, and cache invalidation integration.
- Cardinality-based tag ordering for flush and compaction to improve data locality.
- Correctness, chaos, fuzz, concurrency, integrity, edge-case, benchmark, and public-dataset validation suites.
- Rename the project to MynaDB and update documentation and branding.

**Evidence:** `0059e95` adds memtable querying, `c247642` adds metadata management and disk cache, `9917af9` adds read optimizations, and `6751844`/`06ea04a` expand validation.

**Release meaning:** the recognizable standalone MynaDB query architecture. The first existing tagged release corresponds to this stage. Hot-partition caching is later corrected and disabled by default in M10.

## M6 — Distributed database and Raft control plane

**Features introduced**

- First shared-storage cluster with etcd coordination, locks, membership, cache invalidation, and deployment manifests.
- Replace that coordination implementation with a built-in Meta service; remove the legacy etcd cluster module.
- Distinct Router, Data, and Meta process roles, role-specific configuration, CLI subcommands, and cluster development profiles.
- Arrow Flight for Router–Data communication and direct RecordBatch transfer instead of repeated line-protocol parsing.
- Distributed query fan-out/coordination and the initial fencing machinery.
- OpenRaft integration for Meta consensus, sled-backed state, event broadcasting, node registration, and leader forwarding.
- Warm standby replication followed by the first cold-standby design, partition-aware replica management, and early node scale-in.
- Data-node HTTP monitoring, local Parquet caching, Router statistics/flush APIs, and Meta inspection endpoints.
- Retry jitter, configurable circuit breakers, Prometheus metrics, and replication CRC checksums.

**Evidence:** `abc3d6c` introduces etcd clustering; `9a89d9b`, `8dae77e`, `9e0e95e`, and `df20f08` establish the replacement architecture; `f3f1cf5` and `8c58aa4` introduce standby designs.

**Release meaning:** the transition from a standalone engine to a distributed database. The early replica, fencing, path, and promotion models are still revised in M7–M8.

## M7 — Recovery, standby redesign, and safe scale-in

**Features introduced**

- Correct cluster heartbeats, node registration, write distribution, MinIO discovery, and table refresh following flush.
- Graceful retirement: reject new writes on retiring nodes, track in-flight queries, flush before exit, and enforce retirement timeouts.
- Query retry, file redistribution on failure, and fault-tolerance tests.
- Ratio-preserving scale-in planning, health checks, and recovery safety checks.
- Node-level primary/standby pairing and a **no-promotion** standby design, replacing promotion-oriented recovery.
- Recovery-event-triggered flushing and flush checkpoints to synchronize standby caches.
- Distributed compaction locks and WAL rotation-before-flush to protect concurrent writes.
- A continuous-aggregate framework with definitions, watermarks, storage, refresh scheduling, and rewrite helpers.

**Evidence:** `f18b528`, `ff7cbf7`, `78497ff`, `341341d`, and `2c93c26` define the recovery/scaling direction; `360f2f0` introduces aggregates.

**Release meaning:** cluster lifecycle safety becomes a distinct capability. Continuous aggregates remain a framework: refresh execution is placeholder code rather than an integrated materialization feature.

## M8 — Table ownership and partition-aware routing

**Features introduced**

- Replace fine-grained file/partition ownership APIs with table-level metadata and routing.
- Multiple table owners, write rerouting, configurable distributed writes across primaries, and distributed-table query execution.
- Move the table rebalancer to the Meta leader; reduce repeated ownership registration with pending guards.
- Migrate retirement/recovery to table ownership; drain nodes safely and connect lease expiration to recovery.
- Introduce Hive-style `date=.../node_id=...` physical partitions and partition discovery from object storage.
- Partition-level query routing and SQL rewriting for cache affinity. This is physical read routing layered on table-level ownership, rather than a restoration of the old Meta partition-replica registry.
- Migrate retention, compaction, and the custom table provider to the object-store abstraction; remove temporary legacy-prefix handling.
- Add WAL protection to cluster RecordBatch writes; split partition caches; add standby query failover and table-cache TTL eviction.
- Introduce the first cluster admin console and ephemeral table ownership with TTL.
- Refactor large modules and improve development build configuration.

**Evidence:** `1329457`, `b84d6de`, `563a5b0`, `9d50532`, `73e93e9`, `746b5f0`, `487d8a9`, and `c49b32e`.

**Release meaning:** the current ownership/routing architecture takes shape. The existing `v0.1.1` tag corresponds to an intermediate snapshot in this stage, before the Hive routing changes.

## M9 — Operator console, ecosystem, and advanced TSDB features

**Features introduced**

- Replace the early admin frontend with React; consolidate admin APIs and development configuration.
- SQL Query Console, Storage Browser, cluster metrics, real-time events, filtering, export, browser notifications, election/config/debug tools, and test-data generation.
- Grafana datasource plugin and query tracing/profiling.
- Prometheus remote read/write and a dedicated InfluxQL parser/transpiler.
- Multi-tenant RBAC, custom roles, user-management APIs, query isolation, audit logging, and TLS support across server implementations.
- WebSocket/SSE streaming queries.
- Policy-based downsampling jobs and tier tables; adaptive per-column compression and age-based recompression.
- Geospatial SQL functions and forecasting aggregates using linear regression, EMA, ARIMA, automatic selection, and forecast-series output.
- Columnar cache optimizations including dictionary encoding and predicate evaluation.
- Standardized data layout, improved object-store/catalog discovery, and compaction enabled on cluster data nodes.

**Evidence:** `c5ef091`, `bca1ee1`, `a5a551c`, `251ec07`, `bd54c5c`, `9dedf8e`, `24b945a`, `52ca5a0`, `d845e97`, and `bd89acf`.

**Release meaning:** a substantial expansion of the operator and analytics surface. Deployment-role coverage varies: for example, the Router runner still reports MySQL support as unimplemented. Downsampling jobs are integrated in standalone startup; automatic query redirection is still incomplete.

## M10 — Correctness, observability ingestion, and documentation

**Features introduced and fixes completed**

- InfluxDB-compatible query/metadata endpoints and Grafana/Telegraf query translation and response-format fixes.
- Correct dictionary-encoded and heterogeneous Arrow/Parquet schemas, union projections, empty projections, and `COUNT(*)` handling.
- Compaction manifests to avoid duplicate reads from overlapping files; merge duplicate points and coalesce heterogeneous field sets.
- Quote-aware and escaped-space-aware line-protocol parsing.
- Full-schema hot-partition cache entries, compaction invalidation, staleness detection, and a safer disabled-by-default cache setting.
- Missing-table/field handling, distributed reaggregation fixes, and fixes for suspect-node deadlocks and partial query results.
- gzip write bodies, a 250 MB request-body limit, and last-point query support.
- Query-cost logging, broader integration coverage, recorded benchmark results, and a built-in monitoring dashboard.
- OTLP gRPC/HTTP ingestion and converters for metrics, logs, and traces.
- An initial Iceberg metadata/snapshot/catalog implementation and background synchronization.
- Expanded operational/feature guides, Chinese documentation, development commands/skills, GitHub Pages deployment, and architecture reconciliation.
- July fixes for dashboard response unwrapping and distributed `GROUP BY time(1m)` aggregation; dependency lockfile synchronization.
- November README setup/usage guidance and branding updates.

**Evidence:** `42f2bc0`, `cf5b5e1`, `ac2c629`, `6edafea`, `304bbd4`, `ec57981`, `4a147b4`, and `837839e`.

**Release meaning:** compatibility and correctness work catches up with the fast feature expansion. OTLP and Iceberg should be described with their current integration limits, rather than treating their feature-commit titles as certification of complete interoperability.

## Architectural routes visible across the milestones

1. **Storage:** local Parquet → date partitions → object-store abstraction → shared storage → Hive partitions under `store/{catalog}/{schema}/{table}/date=.../node_id=...`.
2. **Query:** DataFusion over persisted files → result/metadata caches → custom providers and memtable union → distributed fan-out → partition-affinity routing → schema/reaggregation correctness fixes.
3. **Coordination:** standalone → initial master/replica experiment → removal → etcd cluster → built-in Raft Meta → authoritative table ownership with cached physical-partition discovery.
4. **Recovery:** WAL replay → replicated standby memory → cold standby → node-level pairing and no promotion, with checkpoint/recovery flushing and controlled retirement.
5. **Product surface:** HTTP/Flight/MySQL connectivity → authentication/profiling → cluster management → React admin/Grafana/Prometheus/InfluxQL → streaming, analytics, OTLP, and initial Iceberg integration.

These routes explain why the history is not a simple additive feature list. Removed HA and series-tracking implementations, the etcd coordinator, node-first paths, and partition replica registries should be presented as superseded designs.

## Existing tags versus suggested milestone boundaries

| Existing tag | Target commit | Content-equivalent local-main commit | Milestone |
|---|---|---|---|
| `v0.1.0` | `88a7710` | `20b9cbf` | M5, public-dataset validation |
| `0.1.1` | `88a7710` | `20b9cbf` | Same target as `v0.1.0`; not a separate feature release |
| `v0.1.1` | `6404c9b` | `b8e4ece` | M8, module/build organization before Hive routing |

The tag targets belong to rewritten history and are matched by tree content. `Cargo.toml` at local `main` still declares `0.1.0`; commit volume does not imply an actual version bump. For a release narrative, use the ten milestone themes above and choose future version numbers separately.

## Work outside local main and history duplication

- Archived retimed heads and `simplified/main` have the same final tree as the July local-main snapshot `cef8f07`: `e50194efb93827ca90b0043da4ea651cf351c071`. Current local `main` adds the two November README commits.
- The simplified branch has 24 different transition sets caused by consolidated commits. Each has an endpoint tree matching a local-main commit; they are alternate packaging of the same development route, not 24 additional product features.
- `origin/roy/cross_validation` contains `b2c50f4`: a DuckDB comparison suite using generated TSBS-style data to compare aggregation, filtering, grouping, ordering, expressions, and related SQL results. This is a distinct branch addition and is **absent from local main**; it complements M5 but is not counted as shipped there.
- `6e1166a` and `d603624` on 2026-11-09 add README setup/usage and branding changes. They extend M10's documentation and add no database-engine milestone.

## Current feature limits that affect release descriptions

- **Continuous aggregates:** [`src/aggregates/job.rs`](../../src/aggregates/job.rs) simulates refresh with zero processed/written rows, and `DataFusionRefreshExecutor` returns `(0, 0)`. Describe this as a framework awaiting execution integration.
- **Downsampling:** [`src/query/analyzers/downsampling_rewriter.rs`](../../src/query/analyzers/downsampling_rewriter.rs) logs the proposed tier substitution and returns the unchanged plan. Materialized tier tables must be queried explicitly.
- **Iceberg:** [`src/iceberg/metadata/manifest.rs`](../../src/iceberg/metadata/manifest.rs), [`manifest_list.rs`](../../src/iceberg/metadata/manifest_list.rs), and [`snapshot_builder.rs`](../../src/iceberg/sync/snapshot_builder.rs) currently write manifests as JSON. Treat external-engine interoperability as unverified; no Spark/Trino/Flink compatibility test was run in this audit.
- **Protocol coverage:** [`src/node/router.rs`](../../src/node/router.rs) explicitly reports Router MySQL as unimplemented. Standalone MySQL connectivity does not establish availability in every role, nor full MySQL SQL-dialect compatibility.

These limitations are source observations at `d603624` (database source unchanged since `cef8f07`), not requests to change the implementation as part of this history review.
