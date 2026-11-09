# Reproducible baseline protocol

This document defines a future measurement protocol; it contains no new
performance results. Existing files under `benchmarks/` are historical evidence
with their own conditions, not proof of current superiority over another system.

Record the source SHA, Cargo.lock hash, binary build profile, OS/kernel, CPU model
and cores, memory, disk/filesystem, Rust version, full configuration and command.
Record whether the load generator shares the database host and any CPU limits.

For every run record:

- WAL enabled/disabled, `sync_on_write`, batch size, concurrency, flush settings.
- Dataset generation seed, measurement count, tag cardinality, field types,
  row count and timestamp distribution; validate exact ingested row counts.
- Write throughput and errors, and acknowledged vs submitted rows.
- Named SQL statements with expected result counts/checksums; measure latency
  distributions across repeated cold/warm runs, separating query-cache effects.
- CPU/RSS, disk bytes, Parquet file count, and flush duration before/after flush.
- Restart and recovery time separately from normal query latency.
- All raw samples, failures, repetitions and scripts, not only the best run.

First establish MynaDB's own baseline. A cross-system comparison additionally
needs pinned system/client versions, equivalent datasets and query semantics,
equivalent persistence guarantees, and result-correctness checks. Use TSBS only
after recording any adapter and timestamp/query differences. Do not infer a
speedup from incompatible operations or a tiny correctness smoke check.
