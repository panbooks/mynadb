# MynaDB Cluster — Raw API Reference

Testable reference for the local dev cluster started via `goreman -f dev/Procfile start`.

**Port map:**

| Node | HTTP | gRPC/Flight | MySQL |
|------|------|-------------|-------|
| router1 | **7000** | 7001 (Flight) | 7002 *(not impl.)* |
| meta1 / meta2 / meta3 | **6000 / 6100 / 6200** | 6001 / 6101 / 6201 | — |
| data1 / data2 | **5000 / 5100** | 5001 / 5101 (Flight) | — |
| admin | **18080** | — | — |

Auth is **disabled** on every node, so no credentials are needed. Example data used
below: db `testdb`, tables `web.sensor` and `prod.cpu`.

---

## 1. Router `:7000` — main client entry point

All reads/writes go here; it fans out to the data nodes.

```bash
# --- health / info ---
curl http://localhost:7000/health
curl http://localhost:7000/ping                     # InfluxDB-style ping
curl http://localhost:7000/version
curl http://localhost:7000/stats                     # write-cache stats
curl http://localhost:7000/memory

# --- write (InfluxDB line protocol) ---
curl -X POST "http://localhost:7000/write/testdb" \
  --data-binary 'sensor,service=web,host=node9 cpu=99.5,mem=60.0 1768263000000000000'
# alt form with db as query param:
curl -X POST "http://localhost:7000/write?db=testdb" \
  --data-binary 'cpu,service=prod,host=web01 user=50.0 1768263000000000000'

# --- query (SQL) ---  GET (url-encoded) or POST
curl -G "http://localhost:7000/query/testdb" --data-urlencode "sql=SELECT count(*) FROM web.sensor"
curl -X POST "http://localhost:7000/query/testdb" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT host, avg(cpu) FROM web.sensor GROUP BY host"}'

# --- InfluxQL-style query endpoint (no :db) ---
curl -G "http://localhost:7000/query" --data-urlencode "q=SHOW DATABASES"

# --- schema / metadata ---
curl http://localhost:7000/schemas/testdb                     # list schemas in db
curl http://localhost:7000/tables/testdb                      # list all tables in db
curl http://localhost:7000/tables/testdb/web                  # tables in one schema
curl http://localhost:7000/schema/testdb/web/sensor           # column schema of a table

# --- cluster/routing view from the router ---
curl http://localhost:7000/router/status                      # router status
curl http://localhost:7000/router/nodes                       # data nodes the router knows

# --- maintenance ---
curl -X POST http://localhost:7000/flush                      # flush memtables -> Parquet
```

**Full router route list:**
`GET /`, `/health`, `/ping`, `/version`, `/stats`, `/memory`, `/schemas/:db`,
`/tables/:db`, `/tables/:db/:schema`, `/schema/:db/:schema/:table`,
`/router/status`, `/router/nodes`, `/query`, `/query/:db` ·
`POST /write`, `/write/:db`, `/query`, `/query/:db`, `/flush`

---

## 2. Admin console `:18080` — cluster management API

UI at **http://localhost:18080/admin** (no trailing slash). API prefix is `/admin/api/v1`.

```bash
# --- cluster topology & health ---
curl http://localhost:18080/admin/api/v1/health
curl http://localhost:18080/admin/api/v1/cluster/status
curl http://localhost:18080/admin/api/v1/nodes                # all nodes
curl http://localhost:18080/admin/api/v1/nodes/data-1         # one node
curl http://localhost:18080/admin/api/v1/meta/status          # raft leader/term
curl http://localhost:18080/admin/api/v1/distribution         # table->node assignment
curl http://localhost:18080/admin/api/v1/elections            # leader elections

# --- tables ---
curl http://localhost:18080/admin/api/v1/tables
curl -X POST http://localhost:18080/admin/api/v1/tables/<table-key>/reassign \
  -H 'Content-Type: application/json' -d '{"target_node":"data-2"}'

# --- query through admin (across a catalog) ---
curl http://localhost:18080/admin/api/v1/query/catalogs
curl "http://localhost:18080/admin/api/v1/query/schemas/testdb"
curl "http://localhost:18080/admin/api/v1/query/explain/testdb?sql=SELECT+1"
curl "http://localhost:18080/admin/api/v1/query/analyze/testdb?sql=SELECT+1"
curl -X POST http://localhost:18080/admin/api/v1/query/execute \
  -H 'Content-Type: application/json' -d '{"catalog":"testdb","sql":"SELECT count(*) FROM web.sensor"}'
curl http://localhost:18080/admin/api/v1/query/saved           # saved queries (GET/DELETE /:id)

# --- storage inspection ---
curl http://localhost:18080/admin/api/v1/storage/info
curl http://localhost:18080/admin/api/v1/storage/catalogs
curl "http://localhost:18080/admin/api/v1/storage/file?path=<parquet-key>"
curl -X POST http://localhost:18080/admin/api/v1/storage/compact/testdb

# --- scale-in (drain/retire nodes) ---
curl http://localhost:18080/admin/api/v1/scale-in/recommend
curl -X POST http://localhost:18080/admin/api/v1/scale-in/plan    -H 'Content-Type: application/json' -d '{"node_id":"data-2"}'
curl -X POST http://localhost:18080/admin/api/v1/scale-in/execute -H 'Content-Type: application/json' -d '{"node_id":"data-2"}'

# --- events (cluster event log) ---
curl http://localhost:18080/admin/api/v1/events
curl http://localhost:18080/admin/api/v1/events/stats
curl http://localhost:18080/admin/api/v1/events/sources
# websocket stream:  ws://localhost:18080/admin/api/v1/events/ws

# --- config / debug / cache ---
curl http://localhost:18080/admin/api/v1/config
curl http://localhost:18080/admin/api/v1/debug/heap
curl http://localhost:18080/admin/api/v1/debug/cpu
curl -X POST http://localhost:18080/admin/api/v1/cache/clear
curl -X POST http://localhost:18080/admin/api/v1/cache/prewarm/testdb
curl -X POST http://localhost:18080/admin/api/v1/debug/generate-data   # seed test data
```

**Full admin route list (prefix `/admin/api/v1`):**
`GET /health`, `/cluster/status`, `/nodes`, `/nodes/:id`, `/meta/status`,
`/distribution`, `/elections`, `/tables`, `/config`, `/debug/cpu`, `/debug/heap`,
`/events`, `/events/stats`, `/events/sources`, `/events/ws`,
`/query/catalogs`, `/query/schemas/:catalog`, `/query/explain/:catalog`,
`/query/analyze/:catalog`, `/query/saved`, `/scale-in/recommend`,
`/storage/info`, `/storage/catalogs`, `/storage/file` ·
`POST /query/execute`, `/tables/:key/reassign`, `/cache/clear`,
`/cache/prewarm/:catalog`, `/debug/generate-data`,
`/elections/:election/campaign`, `/elections/:election/resign`,
`/scale-in/plan`, `/scale-in/execute`, `/storage/compact/:catalog` ·
`DELETE /query/saved/:id`

---

## 3. Meta nodes `:6000` / `:6100` / `:6200` — Raft coordination

Any meta node answers reads; the leader owns writes.

```bash
curl http://localhost:6000/health          # {status, node_id, is_leader}
curl http://localhost:6000/version
curl http://localhost:6000/nodes            # registered cluster nodes
curl http://localhost:6000/nodes/data-1
curl http://localhost:6000/distribution           # table ownership map
curl http://localhost:6000/distribution/config    # GET/POST/DELETE
curl http://localhost:6000/cluster/scale-in/recommend

# node retirement (write ops -> send to the leader)
curl -X POST http://localhost:6100/nodes/data-2/retire
curl http://localhost:6100/nodes/data-2/retire/status
curl -X POST http://localhost:6100/nodes/data-2/retire/cancel
curl -X POST http://localhost:6100/cluster/scale-in/plan    -H 'Content-Type: application/json' -d '{}'
curl -X POST http://localhost:6100/cluster/scale-in/execute -H 'Content-Type: application/json' -d '{}'
```

**Full meta route list:**
`GET /`, `/health`, `/version`, `/nodes`, `/nodes/:node_id`,
`/nodes/:node_id/retire/status`, `/distribution`, `/distribution/config`,
`/cluster/scale-in/recommend` ·
`POST /nodes/:node_id/retire`, `/nodes/:node_id/retire/cancel`,
`/distribution/config`, `/cluster/scale-in/plan`, `/cluster/scale-in/execute` ·
`DELETE /distribution/config`

---

## 4. Data nodes `:5000` / `:5100` — direct (bypass router)

Each data node exposes a fuller single-node API for the tables it owns. Useful for
debugging which node holds what. Queries here only see that node's partitions.

```bash
curl http://localhost:5000/health
curl http://localhost:5000/tables/testdb
curl http://localhost:5000/schema/testdb/web/sensor
curl -G "http://localhost:5000/query/testdb" --data-urlencode "sql=SELECT count(*) FROM web.sensor"

# EXPLAIN / ANALYZE plans (GET or POST)
curl -G "http://localhost:5000/explain/testdb" --data-urlencode "sql=SELECT * FROM web.sensor"
curl -G "http://localhost:5000/analyze/testdb" --data-urlencode "sql=SELECT * FROM web.sensor"

# cache / maintenance
curl http://localhost:5000/cache/stats
curl -X POST http://localhost:5000/cache/clear
curl -X POST http://localhost:5000/flush
curl -X POST http://localhost:5000/compact/testdb
curl http://localhost:5000/metrics                 # Prometheus metrics

# retirement (also driven from admin/meta)
curl -X POST http://localhost:5000/retire
curl http://localhost:5000/retire/status
```

**Full data-node route list:**
`GET /`, `/health`, `/version`, `/stats`, `/memory`, `/metrics`, `/tables/:db`,
`/tables/:db/:schema`, `/schemas/:db`, `/schema/:db/:schema/:table`, `/query/:db`,
`/explain/:db`, `/analyze/:db`, `/cache/stats`, `/retire/status` ·
`POST /write`, `/write/:db`, `/query/:db`, `/explain/:db`, `/analyze/:db`,
`/flush`, `/compact/:catalog`, `/cache/clear`, `/cache/prewarm/:catalog`,
`/retire`, `/retire/cancel`, `/retire/force`

---

## Notes / gotchas

- **Router API is intentionally leaner than a standalone node** — no `/compact`,
  `/analyze`, `/explain`, `/cache/*`, or `/partitions` on `:7000`. For those, hit the
  **data node** (`:5000`/`:5100`) or the **admin** API.
- **`GROUP BY time(...)`** InfluxQL syntax is **not** supported by the SQL engine —
  use the `time_bucket(time, width_ns, origin)` UDF instead, e.g.
  `SELECT time_bucket(time, 300000000000, 0) AS bucket, avg(cpu) FROM web.sensor GROUP BY bucket`.
- **Admin UI trailing slash:** use `/admin`, not `/admin/` (axum nesting quirk — the
  latter 404s). API routes under `/admin/api/v1/...` are unaffected.
- **MySQL (`:7002`) is not implemented in router mode.** Run a standalone node
  (`mysql_enabled = true`) to use the MySQL wire protocol.
- **Object store:** cluster mode requires MinIO/S3 to be running (bucket
  `mynadb-cluster` at `127.0.0.1:9000`) — without it, writes return `204` but do not
  persist and queries return 0 rows.
