# MynaDB Architecture Analysis — 2026-07-12

A deep-dive analysis of the MynaDB codebase, traced directly from source (`src/`) and reconciled against the `docs/` folder. **Several docs were found to be outdated** — see the [Docs vs Code drift](#docs-vs-code-drift) section.

> Scope: cluster architecture, write/read paths, scale-in/out, the Meta control plane, storage-engine jobs, routing, and query interfaces. All findings cite `file:line` from the tree as of this date; confirm exact line numbers with `git blame`.

---

## Visual artifacts produced

Five interactive HTML pages (shared design system, 🐦 favicon). Each is private on claude.ai until shared from its page.

| # | Artifact | What it covers | URL |
|---|----------|----------------|-----|
| 1 | **Cluster Write Path** | Router→Data two-tier hop, WAL+memtable, replication, flush→Parquet, Raft-based coordination (not etcd) | https://claude.ai/code/artifact/fbe6f18f-5277-4ef8-ac44-6b0cc42bac32 |
| 2 | **System Architecture (overview)** | All six roles, deployment modes, write/read paths, cold-standby failover, Meta/Raft, storage engine, query engine, ports | https://claude.ai/code/artifact/597cf5f0-48ce-4497-b949-ef0942c4d362 |
| 3 | **Scale-In & Scale-Out** | Passive scale-out vs orchestrated 3-phase scale-in, Golden Rules, strategies, docs-vs-code reconciliation | https://claude.ai/code/artifact/ffca9815-5fb4-4fdf-b451-caa3e527ed14 |
| 4 | **execute_migration_phase** | Scale-in Phase 1 loop as a flowchart, standby safety-net, happy-path + failure worked examples | https://claude.ai/code/artifact/16015e23-f051-49b5-b2ff-34d8a2ecc022 |
| 5 | **Table-Affinity Routing** | Cached-owner-vs-hash selection, table-home lifecycle, cross-table spread, failover re-pin, O(tables) guards | https://claude.ai/code/artifact/e62b84d3-cc72-48d9-a257-b5aa3de35709 |

## Text-only analyses (this document)

| Topic | Section |
|-------|---------|
| What the Meta service stores | [§5](#5-meta-service--persisted-state) |
| Router vs Data node responsibilities | [§6](#6-router-vs-data-node-responsibilities) |
| SQL dialects & wire protocols | [§7](#7-sql-dialects--wire-protocols) |
| Downsampling internals | [§8](#8-downsampling) |
| Object-store path: node-first or node-last | [§9](#9-object-store-path--node-first-or-node-last) |
| Shard distribution logic | [§4](#4-table-affinity-routing--shard-distribution) |
| Primary/standby pairing storage + table migration | [§3](#3-scale-in--scale-out) |

---

## 1. Cluster topology & the CLAUDE.md correction

**Key correction:** there is **no `src/cluster/` and no etcd**, contrary to `CLAUDE.md`. Coordination is a purpose-built **Raft (openraft) meta cluster** under `src/meta/`, backed by a `sled` state machine.

Six process roles (`src/node/mod.rs`), each chosen by CLI subcommand:

| Role | Durable state | Responsibility | Source |
|------|--------------|----------------|--------|
| **Router** | none (caches only) | parse, route, fan-out, merge | `src/router/` |
| **Data · Primary** | local WAL + memtable | WAL, memtable, Parquet, compaction | `src/data/`, `src/node/data.rs` |
| **Data · Standby** | replicated memtable | cold backup; flush only on recovery | `src/data/replication/` |
| **Meta** | Raft log + sled | membership, ownership, locks, leases, events | `src/meta/` |
| **Object store** | all Parquet | shared S3/MinIO/OSS | external |
| **Admin** | none | cluster console, scale-in | `src/admin/` |

**Deployment:** no `cluster=true` flag — mode = CLI subcommand (`standalone` collapses all roles; `meta`/`router`/`data`/`admin` split them). Router mode does **not** implement MySQL yet (`src/node/router.rs:172`).

---

## 2. Write path (cluster)

Two-tier hop:

1. **Router** (`WriteRouter::route_write`, `write_router.rs:97`): parse line protocol → `group_by_partition` (`{catalog,schema,table,date}`) → Arrow `RecordBatch` → `select_write_candidates` (table affinity) → Flight **`do_put`** to the owner data node.
2. **Data node** (`handle_batch_write`, `data/server/flight.rs:326`): **WAL append first** → memtable insert → optional replicate to standby → ack → router registers table owner in Meta.

**Object store is NOT on the hot path.** Parquet lands only on flush (60s / 80% of `max_size_mb`), written to the shared bucket, then registered with Meta.

**Durability ordering (crash-safe invariant):** WAL append precedes memtable insert · WAL rotation precedes cache drain · WAL deletion follows a successful Parquet PUT + checkpoint.

---

## 3. Scale-in & scale-out

**The two directions are deeply asymmetric.**

### Scale-out — passive (self-registration only)
- **No `scale_out` module, no MetaCommand, no API.** A node starts, calls `register_node` (`node/data.rs:261`), Meta assigns Primary/Standby via `mode_for_new_node` (2:1 ratio) and pairs a standby, publishes `NodeJoined`, routers add it to the pool.
- **Existing tables never migrate to it.** It earns work only via *new* write routing (hashed tables, or distributed-write complement).
- The rebalancer (`meta/meta_node/lifecycle.rs:178`, default **off**) is failure-cleanup, not load-balancing.

### Scale-in — orchestrated (`src/meta/scale_in/`)
- **ScaleInManager** = Planner → Validator → Executor.
- **Three phases:** `MigrateTables` → `RetirePrimaries` → `RetireStandbys` (+ `Completed`).
- **Golden Rules** (`validator.rs`): **GR-1** "Standby Protection" (never retire a standby still backing active primaries), **GR-2** "Table Drain" (never retire a primary still owning tables).
- **Strategies:** `LeastLoaded` (default), `FirstIn`, `LastIn`, `HAUnit`.
- **Node states in transitions:** only `Active → Retiring → Drained` (plus `cancel_retire`).
- Works because storage is shared: **only table ownership moves, never data.**

### Primary/standby pairing storage
Stored **inside each `NodeInfo`** in the Raft/sled state machine under `/nodes/{id}` (reciprocal, denormalized):
- `backing_standby: Option<String>` — on a Primary, its one standby (`types/node.rs:178`).
- `backed_primaries: Vec<String>` — on a Standby, the primaries it backs (`types/node.rs:186`).

### Table migration internals ("migrate P1 → P2")
`reassign_table` (`state_machine/table.rs:627`), through Raft:
1. Load `TableMeta` at `/tables/{cat}/{sch}/{tbl}`.
2. `set_primary_owner(P2)` → moves P2 to `owners[0]`, bumps `version`.
3. Persist; publish `TableReassigned` event.
4. Routers update `ShardCache`. **No Parquet is copied** — P2 reads P1's `node_id=data-p1/` files from the shared bucket.

### execute_migration_phase (Phase 1 loop) — `scale_in/executor.rs:318`
- Loops `phase.operations`; for each `MigrateTable{key, from, to}`:
  - **Guard 1** — every 5th op (`i % 5 == 0`): `check_standby_health()` → on failure `SafetyNetLost` → **whole plan Aborted**.
  - **Guard 2** — `reassign_table_for_retire()` → on failure `MigrationFailed` → **phase fails, recoverable/re-runnable**.
  - success → `migrated_count += 1`.
- **Code truth:** cadence is literally `i % 5 == 0`; the `health_check_interval_ms` config is read into `_check_interval` and **unused** here.

---

## 4. Table-affinity routing & shard distribution

Routing key = **TableKey (`catalog/schema/table`)** — the **date is dropped**, so all partitions of a table co-locate.

`select_write_candidates` (`write_router.rs:516`) branches on `DistributionConfig.percent`:
- **percent == 0 (default) → single-owner** (`select_single_table_candidates`).
- **percent > 0 → distributed** across `ceil(N × percent/100)` primaries.

**Two-step node choice** (`select_node_for_table`, `table_routing.rs:169`):
1. **Cached owner** from Meta, if `Active && Primary` → return it (affinity, no hash).
2. **Hash fallback** — `hash(TableKey) % N` over `get_active_primary_nodes()` (Active+Primary, **sorted by node_id**). Bootstrap only.

**Hash** = Rust `DefaultHasher` (SipHash) `% N` (`utils/hash.rs`). Not stable across restarts — irrelevant, because the authoritative owner lives in Meta. It's **hash-modulo, not a ring**, so changing N remaps only *un-owned* tables; owned tables stay pinned (→ safe passive scale-out).

**Affinity is established** on first successful write: `on_write_success` → `register_table_owner` → Meta `add_table_owner` → `TableOwnerAdded` event → all routers cache the owner. **Two guards** (cache check + pending-registration lock) make Meta calls **O(tables), not O(writes)** (`write_router.rs:447`).

**Retry/failover:** `write_partition_with_retry` tries ≤3 candidates, marks failures suspect; if the cached owner is unhealthy/retiring it re-hashes and re-registers the new owner. Standbys are **never** write targets.

---

## 5. Meta service — persisted state

**Two separate sled DBs** (`meta_node/mod.rs:56`): `<data_dir>/sled` (state machine) and `<data_dir>/raft` (Raft log). All sled values are **bincode**; only `Event` *payloads* are JSON.

| Prefix | Key | Value | Notable fields |
|--------|-----|-------|----------------|
| `/nodes/` | `/nodes/{id}` | `NodeInfo` | role, state, mode, `backing_standby`, `backed_primaries`, `last_heartbeat` |
| `/tables/` | `/tables/{cat}/{sch}/{tbl}` | `TableMeta` | `owners: Vec<TableOwnerEntry>`, partition/bytes/rows stats, version |
| `/leases/` | `/leases/{lease_id}` | `LeaseInfo` | node_id, ttl_ms, expires_at |
| `/locks/` | `/locks/{key}` | `LockInfo` | holder, **fencing_token**, expires_at |
| `/leaders/` | `/leaders/{election}` | `LeaderInfo` | leader, **term**, expires_at |
| `/distribution/` | `/distribution/{cat}[/sch[/tbl]]` | `DistributionConfig` | scope, `percent` |
| `/events/` | `/events/{seq:020}` | `Event` | event_type, topic, JSON payload |
| `/meta/sequence` | scalar | `u64` | event counter (persisted) |
| `/meta/fencing` | scalar | `u64` | fencing/term counter (persisted) |

Plus openraft bookkeeping (`/raft/sm/last_applied`, `/membership`, `/snapshot_meta`) and the log DB (`/raft/log/{i}`, `/raft/vote`, `/raft/committed`, `/raft/last_purged`).

**EventType ids:** NodeJoined=1, NodeLeft=2, NodeStateChanged=3, LeaderChanged=6, CacheInvalidate=7, SchemaChanged=8, FlushForRecovery=13, TableOwnerAdded=14, TableOwnerRemoved=15, TableReassigned=16, DistributionConfigChanged=17, TablesCacheRefresh=18, TableOwnerExpired=19.

**Notable facts:**
- **`/schemas/` (PREFIX_SCHEMA) is dead code** — declared, never read/written; no `SchemaMeta` type.
- **Nodes are soft-deleted** (state → `Drained`), not removed on deregister.
- **`lease_counter` is in-memory only** (resets on restart); sequence & fencing counters are persisted.
- **No per-file registry** — Meta stores table *ownership* + aggregate stats, not individual Parquet file records. (This is why the node-first doc's `created_by`/`get_files_for_query` model doesn't exist.)
- Meta stores **no time-series data and never touches the object store**.

---

## 6. Router vs Data node responsibilities

**Router (`src/router/`, stateless):**
- Terminates client HTTP (`/write`, `/query`, InfluxQL); parses line protocol; `group_by_partition`; Arrow conversion.
- Write routing (table affinity + consistent-hash fallback + distributed percent); Flight `do_put`; registers owners in Meta.
- Query planning (`QueryCoordinator`), partition/table routing, fan-out, **failover** (standby MemtableOnly + fallback-primary ParquetOnly), result re-aggregation.
- `ShardCache` + `DataNodePool` kept consistent by Meta `/events/` (`CacheInvalidationHandler`); independent `HealthChecker`.

**Data node (`src/data/`, `src/node/data.rs`, stateful):**
- Flight `do_put` → WAL + memtable → replicate to standby → flush → Parquet in shared bucket → register with Meta.
- Flight `do_get` → inject `node_id = self` predicate → `QueryEngine` (memtable ∪ Parquet union, pruning, UDFs, gap-fill); HTTP `/query|/explain|/analyze`; `ParquetLocalCache`.
- **LSM + full compaction** (partition-locked via Meta `/compaction/`), memtable flush.
- Registers with Meta, 10s lease, 2s heartbeat; event-driven scale-in/recovery flush.
- **Standby mode:** receives replication only — no query, no WAL, no flush, **never promoted**.

---

## 7. SQL dialects & wire protocols

**Two axes** — query *language* vs *wire protocol*. **PostgreSQL is not supported** (no `pgwire`).

**Query languages:** DataFusion **SQL** (core) and **InfluxQL** (transpiled to SQL).

| Protocol | Port | Speaks | R/W | Source |
|----------|------|--------|-----|--------|
| HTTP REST | 7000 | SQL · InfluxQL | R+W | `services/api.rs` |
| MySQL wire | 7002 | DataFusion SQL + `SHOW`/`USE`/`SET` shim | **Read only** | `services/mysql_service.rs` |
| Arrow Flight | 7001 | SQL ticket (gRPC) | R+W | `services/flight_service.rs` |
| InfluxDB line protocol | 7000 | line protocol | Write only | `storage/line_protocol.rs` |
| Prometheus Remote R/W | 7000 | protobuf matchers (not PromQL) | R+W | `services/prometheus/` |
| Grafana SimpleJSON | 7000 | JSON query | Read | `services/grafana.rs` |
| OTLP | 4317/7000 | OTel metrics/logs/traces | Write only | `services/otlp/` |

MySQL clients connect but their SQL is parsed by **DataFusion**, not MySQL's parser — "MySQL-connectable," not "MySQL-dialect." No PostgreSQL, no PromQL evaluation.

---

## 8. Downsampling

Feature exists (`src/jobs/downsampling/`), **disabled by default**.

- **Model:** `DownsamplingPolicy` → `DownsamplingTier` (name, `after_days`, `interval`, aggregates). Rollups materialize into **`{table}__ds_{tier}`** tables.
- **Write side (works):** background job scans source partitions, selects *eligible* ones (`date < now-after_days`, not future, `> last_processed_date` watermark), runs a `time_bucket` GROUP BY SQL, writes results into the tier table, advances the watermark. Incremental + idempotent.
- **Read side (stubbed):** `DownsamplingRewriter` (AnalyzerRule) computes the best tier but **only logs the intended rewrite — it does not modify the plan** (`downsampling_rewriter.rs:285`). So you must query `FROM cpu__ds_1m` **explicitly**; transparent redirection is not active.
- Related: **continuous aggregates** (`src/aggregates/`) — same idea, watermark + real-time union, but its refresh executor is also **not yet wired** to the query engine (`aggregates/job.rs:297`).

---

## 9. Object-store path — node-first or node-last?

**Final design: node_id is LAST** (a Hive partition after `date=`, under a `store/` root):

```
store/{catalog}/{schema}/{table}/date=YYYY-MM-DD/node_id={node_id}/L{level}_{ts}.parquet
```

- Writer: `get_partition_path` (`parquet_writer.rs:111`) → `…/date=.../node_id=...`.
- Reader: `discovery.rs:27` lists `date=` dirs first, then `node_id=` subdirs.
- Layout: `storage/layout.rs:143`.

`docs/architecture/architecture-node-first-path.md` describes a **superseded node-FIRST design** (`{node_id}/catalog/...`) and a Meta file-registry (`created_by`, `get_files_for_query`) that **does not exist in the code**.

**Why node-last won:** it optimizes the operations a TSDB does constantly — automatic pruning on both `date` and `node_id`, single-op retention (drop a whole `date=` partition), cross-node compaction under one date prefix, and query routing via `LIST date=X/ → [node_id=A/, …]`. Per-node isolation is still achieved via the `node_id=` partition + injected `node_id = self` predicate.

---

## Docs vs Code drift

Recurring theme: the docs describe an **older partition-replica model** that the code replaced with **table-level ownership**.

| Doc | Claims | Reality | Verdict |
|-----|--------|---------|---------|
| `CLAUDE.md` | `src/cluster/` + etcd | `src/meta/` openraft + sled | ❌ outdated |
| `architecture-scale-in.md` / `-cluster-scale-in.md` | `PartitionReplica{leader,standbys}`; `ReassignPartitionForRetire`; `RemoveStandbyFromReplicas`; `[scale_in]` TOML | `TableOwnerEntry` + node-level pairing; `ReassignTableForRetire`; `RemoveStandbyFromTables`; config under `[meta.standby]` | ❌ outdated (GR rules, strategies, states still accurate) |
| `architecture-node-first-path.md` | node-**first** paths; Meta `created_by`/`get_files_for_query` file registry | node-**last** Hive partition; no file registry | ❌ outdated |
| (code) downsampling | transparent read rewrite | rewriter logs only, plan unchanged | ⚠️ stubbed |
| (code) continuous aggregates | auto-refresh materialized views | refresh executor not wired | ⚠️ stubbed |

**Recommendation:** treat `docs/architecture/architecture-node-first-path.md`, `-scale-in.md`, and `-cluster-scale-in.md` as historical; the reconciliation tables above (and in artifacts 3 & 5) are the changelist to bring them current.

---

## Quick reference — key constants & defaults

| Setting | Default | Where |
|---------|---------|-------|
| HTTP / Flight / MySQL ports | 7000 / 7001 / 7002 | `config.rs` |
| Meta gRPC / HTTP / Data / Admin | 7100 / 9000 / 5000-5001 / 8080 | meta/router/data configs |
| Cache flush | 60s / 80% of `max_size_mb` (4 GB) | `config.rs` |
| WAL | `sync_on_write=false`, rotate 256 MB, persist 1s | `wal.rs` |
| Compaction | LSM every 60s, full daily @ hr 3, lock TTL 5 min | `jobs/compactor/` |
| Retention | 7 days, hourly (standalone-only in cluster) | `retention_manager.rs` |
| Standby ratio | `primary_standby_ratio=2`, `min_primary_nodes=2`, `enabled=false` | `[meta.standby]` |
| Lease / heartbeat | 10s TTL / 2s heartbeat / 1s expiry tick | `node/data.rs`, `lifecycle.rs` |
| Downsampling / aggregates / Iceberg / recompression | disabled by default | respective configs |

---

*Generated 2026-07-12 from source analysis. Line numbers reflect the tree at analysis time — verify with `git blame`.*
