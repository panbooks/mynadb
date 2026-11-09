# MynaDB commit-by-commit file-diff ledger

Companion to [the milestone analysis](development-milestones.md). Snapshot: local `main` and `origin/main` at `d603624`.

Every local-main commit is listed below, followed by the 25 transition sets not present as individual local-main diffs. The [CSV index](development-commit-index.csv) maps all 2,235 commit IDs, including rewritten duplicates, to the representative entries here.

Diffs compare against the first parent; roots compare against the empty tree. Merge diffs may include changes already shown in branch commits. File totals are diff churn, not final source size or unique development effort. Rename detection is disabled so moves appear as deletion/addition. Paths and line counts are direct Git numstat output. Commit subjects describe author intent, rather than independently certified behavior.

To inspect a full patch, run `git show <commit>`; for the precise merge comparison used here, run `git diff <commit>^1 <commit>`.

## M1 — Standalone database foundation

43 commits.

### `6687907` — init, a simple http server

2025-12-22 · M1 · 4 files · +826/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 657 | 0 |
| `Cargo.toml` | 8 | 0 |
| `README.md` | 100 | 0 |
| `src/main.rs` | 61 | 0 |

### `c5c2011` — write path: add line protocal decode, write to parquet file

2025-12-23 · M1 · 24 files · +2,224/−129 text lines · 14 binary files

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1164 | 35 |
| `Cargo.toml` | 6 | 1 |
| `QUICKSTART.md` | 103 | 0 |
| `README.md` | 210 | 65 |
| `data/mydb/cpu_1766405455966.parquet` | - | - |
| `data/mydb/cpu_1766405499842.parquet` | - | - |
| `data/testdb/cache_1766405762621.parquet` | - | - |
| `data/testdb/cpu_1766405762573.parquet` | - | - |
| `data/testdb/cpu_1766405762582.parquet` | - | - |
| `data/testdb/cpu_1766405762592.parquet` | - | - |
| `data/testdb/cpu_1766405762612.parquet` | - | - |
| `data/testdb/database_1766405762620.parquet` | - | - |
| `data/testdb/disk_1766405762591.parquet` | - | - |
| `data/testdb/http_requests_1766405762619.parquet` | - | - |
| `data/testdb/memory_1766405762590.parquet` | - | - |
| `data/testdb/memory_1766405762610.parquet` | - | - |
| `data/testdb/network_1766405762611.parquet` | - | - |
| `data/testdb/sensor_1766405762601.parquet` | - | - |
| `scripts/read_parquet.py` | 90 | 0 |
| `scripts/test.sh` | 73 | 0 |
| `src/cache.rs` | 129 | 0 |
| `src/line_protocol.rs` | 151 | 0 |
| `src/main.rs` | 99 | 28 |
| `src/parquet_writer.rs` | 199 | 0 |

### `9e09dc4` — add query engine, datafusion

2025-12-23 · M1 · 23 files · +2,873/−186 text lines · 12 binary files

| File | Added | Deleted |
|---|---:|---:|
| `.gitigore` | 3 | 0 |
| `Cargo.lock` | 1380 | 40 |
| `Cargo.toml` | 2 | 1 |
| `DATAFUSION_INTEGREATION.md` | 459 | 0 |
| `QUERY_EXAMPLES.md` | 291 | 0 |
| `QUICKSTART.md` | 90 | 31 |
| `README.md` | 127 | 13 |
| `data/testdb/cache_1766405762621.parquet` | - | - |
| `data/testdb/cpu_1766405762573.parquet` | - | - |
| `data/testdb/cpu_1766405762582.parquet` | - | - |
| `data/testdb/cpu_1766405762592.parquet` | - | - |
| `data/testdb/cpu_1766405762612.parquet` | - | - |
| `data/testdb/database_1766405762620.parquet` | - | - |
| `data/testdb/disk_1766405762591.parquet` | - | - |
| `data/testdb/http_requests_1766405762619.parquet` | - | - |
| `data/testdb/memory_1766405762590.parquet` | - | - |
| `data/testdb/memory_1766405762610.parquet` | - | - |
| `data/testdb/network_1766405762611.parquet` | - | - |
| `data/testdb/sensor_1766405762601.parquet` | - | - |
| `scripts/test.sh` | 0 | 73 |
| `scripts/test_with_queries.sh` | 129 | 0 |
| `src/main.rs` | 201 | 28 |
| `src/query_engine.rs` | 191 | 0 |

### `f8169c0` — add database manager, update cache

2025-12-23 · M1 · 10 files · +1,567/−556 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CHANGELOG.md` | 70 | 0 |
| `PARQUET_WRITER_API.md` | 227 | 0 |
| `QUICKSTART.md` | 161 | 161 |
| `README.md` | 14 | 14 |
| `UTF8VIEW_FIX.md` | 147 | 0 |
| `src/cache.rs` | 273 | 128 |
| `src/database_manager.rs` | 196 | 0 |
| `src/main.rs` | 207 | 63 |
| `src/parquet_writer.rs` | 47 | 0 |
| `src/query_engine.rs` | 225 | 190 |

### `cf2edb2` — add write_influx_handler,compactable with influx write api

2025-12-23 · M1 · 3 files · +392/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.gitigore` | 5 | 3 |
| `INFLUXDB_COMPATIBILITY.md` | 338 | 0 |
| `src/main.rs` | 49 | 4 |

### `fbb6a80` — update the directory, measurement as a dir

2025-12-23 · M1 · 7 files · +606/−260 text lines · 2 binary files

| File | Added | Deleted |
|---|---:|---:|
| `.gitignore` | 5 | 0 |
| `.gitigore` | 0 | 5 |
| `DIRECTORY_STRUCTURE.md` | 339 | 0 |
| `data/mydb/cpu_1766405455966.parquet` | - | - |
| `data/mydb/cpu_1766405499842.parquet` | - | - |
| `src/parquet_writer.rs` | 246 | 246 |
| `src/query_engine.rs` | 16 | 9 |

### `d18b58c` — add level compactor. by code buddy

2025-12-23 · M1 · 10 files · +1,955/−77 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.gitignore` | 2 | 0 |
| `CHANGELOG.md` | 126 | 69 |
| `COMPACTION.md` | 375 | 0 |
| `COMPACTION_QUICKSTART.md` | 279 | 0 |
| `LEVELED_COMPACTION_IMPLEMENTATION.md` | 413 | 0 |
| `README.md` | 30 | 3 |
| `scripts/test_compaction.sh` | 128 | 0 |
| `src/compactor.rs` | 487 | 0 |
| `src/main.rs` | 111 | 1 |
| `src/parquet_writer.rs` | 4 | 4 |

### `667cf19` — update the sql grammer, change mydb_cpu to cpu after FROM, by code buddy

2025-12-24 · M1 · 7 files · +1,061/−37 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CHANGELOG.md` | 47 | 0 |
| `CORRECT_QUERY_EXAMPLES.md` | 253 | 0 |
| `QUERY_EXAMPLES_UPDATED.md` | 348 | 0 |
| `README.md` | 14 | 3 |
| `SQL_TABLE_NAME_REWRITING.md` | 262 | 0 |
| `src/main.rs` | 3 | 3 |
| `src/query_engine.rs` | 134 | 31 |

### `2c7df24` — add test compaction, by code buddy

2025-12-24 · M1 · 5 files · +809/−26 text lines

| File | Added | Deleted |
|---|---:|---:|
| `COMPACTION_QUICK_FIX.md` | 136 | 0 |
| `COMPACTION_TESTING_GUIDE.md` | 326 | 0 |
| `COMPACTION_TIMING_NOTE.md` | 176 | 0 |
| `scripts/test_compaction.sh` | 72 | 26 |
| `scripts/test_compaction_aggressive.sh` | 99 | 0 |

### `a0b0bad` — add series tracker

2025-12-24 · M1 · 5 files · +1,175/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `SERIES_CARDINALITY_TRACKING.md` | 389 | 0 |
| `SERIES_STATS_QUICKSTART.md` | 205 | 0 |
| `scripts/test_series_stats.sh` | 170 | 0 |
| `src/main.rs` | 74 | 0 |
| `src/series_tracker.rs` | 337 | 0 |

### `a4c38e0` — add series tracker

2025-12-24 · M1 · 1 files · +54/−17 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/series_tracker.rs` | 54 | 17 |

### `4997df0` — sort the md files

2025-12-24 · M1 · 38 files · +5,299/−5,299 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CHANGELOG.md` | 0 | 174 |
| `COMPACTION.md` | 0 | 375 |
| `COMPACTION_QUICKSTART.md` | 0 | 279 |
| `COMPACTION_QUICK_FIX.md` | 0 | 136 |
| `COMPACTION_TESTING_GUIDE.md` | 0 | 326 |
| `COMPACTION_TIMING_NOTE.md` | 0 | 176 |
| `CORRECT_QUERY_EXAMPLES.md` | 0 | 253 |
| `DATAFUSION_INTEGREATION.md` | 0 | 459 |
| `DIRECTORY_STRUCTURE.md` | 0 | 339 |
| `INFLUXDB_COMPATIBILITY.md` | 0 | 338 |
| `LEVELED_COMPACTION_IMPLEMENTATION.md` | 0 | 413 |
| `PARQUET_WRITER_API.md` | 0 | 227 |
| `QUERY_EXAMPLES.md` | 0 | 291 |
| `QUERY_EXAMPLES_UPDATED.md` | 0 | 348 |
| `QUICKSTART.md` | 0 | 162 |
| `SERIES_CARDINALITY_TRACKING.md` | 0 | 389 |
| `SERIES_STATS_QUICKSTART.md` | 0 | 205 |
| `SQL_TABLE_NAME_REWRITING.md` | 0 | 262 |
| `UTF8VIEW_FIX.md` | 0 | 147 |
| `docs/architecture-datafusion-integration.md` | 459 | 0 |
| `docs/architecture-directory-structure.md` | 339 | 0 |
| `docs/architecture-leveled-compaction.md` | 413 | 0 |
| `docs/architecture-parquet-writer.md` | 227 | 0 |
| `docs/architecture-sql-table-naming.md` | 262 | 0 |
| `docs/changelog.md` | 174 | 0 |
| `docs/features-compaction.md` | 375 | 0 |
| `docs/features-influxdb-compatibility.md` | 338 | 0 |
| `docs/features-series-cardinality.md` | 389 | 0 |
| `docs/guide-compaction-quickstart.md` | 279 | 0 |
| `docs/guide-compaction-testing.md` | 326 | 0 |
| `docs/guide-correct-query-examples.md` | 253 | 0 |
| `docs/guide-query-examples-updated.md` | 348 | 0 |
| `docs/guide-query-examples.md` | 291 | 0 |
| `docs/guide-series-stats.md` | 205 | 0 |
| `docs/quickstart.md` | 162 | 0 |
| `docs/troubleshooting-compaction-fix.md` | 136 | 0 |
| `docs/troubleshooting-compaction-timing.md` | 176 | 0 |
| `docs/troubleshooting-utf8view-fix.md` | 147 | 0 |

### `8016598` — format the code, add some const values

2025-12-24 · M1 · 8 files · +1,021/−479 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/REFACTORING_SUMMARY.md` | 193 | 0 |
| `docs/development-code-standards.md` | 282 | 0 |
| `scripts/read_parquet.py` | 89 | 89 |
| `src/compactor.rs` | 34 | 13 |
| `src/database_manager.rs` | 212 | 195 |
| `src/line_protocol.rs` | 150 | 150 |
| `src/parquet_writer.rs` | 24 | 15 |
| `src/series_tracker.rs` | 37 | 17 |

### `1adfa07` — add backgroud flusher for series_tracker, lazy persist, and promote robustness.

2025-12-24 · M1 · 3 files · +285/−93 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 9 | 10 |
| `src/query_engine.rs` | 1 | 1 |
| `src/series_tracker.rs` | 275 | 82 |

### `f1e2150` — add flight service

2025-12-24 · M1 · 6 files · +797/−31 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 295 | 25 |
| `Cargo.toml` | 5 | 1 |
| `FLIGHT_USAGE.md` | 132 | 0 |
| `src/flight_service.rs` | 310 | 0 |
| `src/main.rs` | 38 | 5 |
| `src/query_engine.rs` | 17 | 0 |

### `889fd20` — add config parse

2025-12-24 · M1 · 13 files · +1,523/−40 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.editorconfig` | 47 | 0 |
| `.gitattributes` | 14 | 0 |
| `.gitignore` | 21 | 6 |
| `ARCHITECTURE.md` | 277 | 0 |
| `CHANGELOG.md` | 81 | 0 |
| `CONFIG.md` | 223 | 0 |
| `Cargo.lock` | 261 | 5 |
| `Cargo.toml` | 3 | 1 |
| `QUICKSTART.md` | 212 | 0 |
| `config.production.toml` | 44 | 0 |
| `config.toml` | 44 | 0 |
| `src/config.rs` | 248 | 0 |
| `src/main.rs` | 48 | 28 |

### `dc64e6d` — utilize the datafusion's catalog schema and table

2025-12-25 · M1 · 12 files · +1,533/−360 text lines

| File | Added | Deleted |
|---|---:|---:|
| `BUILD_AND_TEST.md` | 94 | 0 |
| `CATALOG_STRUCTURE.md` | 89 | 0 |
| `CHANGES_SUMMARY.md` | 145 | 0 |
| `FIXES_APPLIED.md` | 145 | 0 |
| `QUICK_REFERENCE.md` | 107 | 0 |
| `src/cache.rs` | 28 | 10 |
| `src/main.rs` | 74 | 10 |
| `src/parquet_writer.rs` | 110 | 26 |
| `src/query_engine.rs` | 411 | 98 |
| `src/series_tracker.rs` | 204 | 216 |
| `test_schemas.sh` | 68 | 0 |
| `test_write.sh` | 58 | 0 |

### `71bc527` — add logs

2025-12-25 · M1 · 13 files · +708/−193 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.gitignore` | 1 | 0 |
| `Cargo.lock` | 181 | 0 |
| `Cargo.toml` | 4 | 1 |
| `LOGGING_CHANGES.md` | 146 | 0 |
| `config.toml` | 0 | 2 |
| `config.toml.example` | 63 | 0 |
| `src/cache.rs` | 13 | 15 |
| `src/compactor.rs` | 19 | 21 |
| `src/config.rs` | 66 | 3 |
| `src/logger.rs` | 70 | 0 |
| `src/main.rs` | 102 | 107 |
| `src/query_engine.rs` | 12 | 11 |
| `src/series_tracker.rs` | 31 | 33 |

### `4bf174b` — fix compactor

2025-12-25 · M1 · 12 files · +402/−171 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.gitignore` | 2 | 0 |
| `Cargo.lock` | 121 | 0 |
| `Cargo.toml` | 2 | 1 |
| `config.toml` | 23 | 1 |
| `src/cache.rs` | 1 | 1 |
| `src/compactor.rs` | 138 | 89 |
| `src/config.rs` | 31 | 22 |
| `src/flight_service.rs` | 4 | 12 |
| `src/main.rs` | 68 | 31 |
| `src/parquet_writer.rs` | 11 | 11 |
| `src/query_engine.rs` | 0 | 2 |
| `src/series_tracker.rs` | 1 | 1 |

### `f0815d2` — big feature: add date partition

2025-12-25 · M1 · 10 files · +851/−177 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 9 | 10 |
| `src/compactor.rs` | 94 | 90 |
| `src/database_manager.rs` | 172 | 1 |
| `src/line_protocol.rs` | 33 | 0 |
| `src/logger.rs` | 0 | 10 |
| `src/main.rs` | 191 | 8 |
| `src/parquet_writer.rs` | 77 | 46 |
| `src/query_engine.rs` | 85 | 12 |
| `test_partition_query.sh` | 44 | 0 |
| `test_partitions.sh` | 146 | 0 |

### `939c7dc` — optimize, delete unused functions

2025-12-25 · M1 · 7 files · +3/−164 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 0 | 13 |
| `src/config.rs` | 0 | 11 |
| `src/database_manager.rs` | 0 | 20 |
| `src/logger.rs` | 0 | 1 |
| `src/main.rs` | 0 | 6 |
| `src/parquet_writer.rs` | 2 | 41 |
| `src/query_engine.rs` | 1 | 72 |

### `59ceec0` — add major compaction

2025-12-26 · M1 · 7 files · +547/−61 text lines

| File | Added | Deleted |
|---|---:|---:|
| `MULTI_LEVEL_COMPACTION.md` | 327 | 0 |
| `build.sh` | 9 | 0 |
| `config.toml.example` | 9 | 3 |
| `src/compactor.rs` | 108 | 56 |
| `src/config.rs` | 11 | 2 |
| `src/main.rs` | 4 | 0 |
| `test_multilevel_compaction.sh` | 79 | 0 |

### `0fc19e8` — add non partitioned table compaction

2025-12-26 · M1 · 4 files · +825/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `MULTI_LEVEL_COMPACTION.md` | 101 | 1 |
| `NON_PARTITIONED_COMPACTION.md` | 450 | 0 |
| `src/compactor.rs` | 170 | 19 |
| `test_non_partitioned_compaction.sh` | 104 | 0 |

### `35d4aec` — config schema tag key

2025-12-26 · M1 · 17 files · +2,225/−37 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CONFIG.md` | 74 | 0 |
| `IMPLEMENTATION_SUMMARY.md` | 306 | 0 |
| `MIGRATION_SCHEMA_CONFIG.md` | 283 | 0 |
| `SCHEMA_CONFIG_CHANGES.md` | 335 | 0 |
| `SCHEMA_EXTRACTION.md` | 316 | 0 |
| `SCHEMA_FEATURE_README.md` | 185 | 0 |
| `SCHEMA_QUICK_REF.md` | 107 | 0 |
| `SCHEMA_UPDATE_SUMMARY.md` | 218 | 0 |
| `config.examples.toml` | 255 | 0 |
| `config.toml` | 13 | 0 |
| `config.toml.example` | 23 | 0 |
| `src/cache.rs` | 23 | 14 |
| `src/config.rs` | 35 | 0 |
| `src/main.rs` | 3 | 3 |
| `src/query_engine.rs` | 1 | 1 |
| `src/series_tracker.rs` | 43 | 19 |
| `test_build.sh` | 5 | 0 |

### `3d7807c` — optimize log info

2025-12-26 · M1 · 4 files · +15/−67 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 8 | 8 |
| `src/compactor.rs` | 6 | 3 |
| `src/main.rs` | 1 | 1 |
| `src/parquet_writer.rs` | 0 | 55 |

### `c9acecb` — delete fields parse line protocol

2025-12-26 · M1 · 3 files · +1/−133 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 0 | 68 |
| `src/line_protocol.rs` | 1 | 5 |
| `src/parquet_writer.rs` | 0 | 60 |

### `29689cc` — align_batch_schemas when compact

2025-12-26 · M1 · 1 files · +190/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 190 | 1 |

### `7493521` — some opts, opt create external table, modify hash i64 to u64, qualify table name

2025-12-26 · M1 · 8 files · +314/−168 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 2 | 2 |
| `src/cache.rs` | 1 | 1 |
| `src/compactor.rs` | 1 | 0 |
| `src/line_protocol.rs` | 0 | 57 |
| `src/main.rs` | 1 | 4 |
| `src/parquet_writer.rs` | 2 | 2 |
| `src/query_engine.rs` | 248 | 70 |
| `src/series_tracker.rs` | 59 | 32 |

### `b9430ca` — update build.sh

2025-12-26 · M1 · 1 files · +26/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `build.sh` | 26 | 4 |

### `7dd2ee5` — fix testing bugs

2025-12-27 · M1 · 3 files · +6/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 1 | 1 |
| `src/compactor.rs` | 1 | 1 |
| `src/series_tracker.rs` | 4 | 2 |

### `36cbf6c` — opt log info, add UInt64 support when query

2025-12-27 · M1 · 2 files · +8/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 4 | 2 |
| `src/query_engine.rs` | 4 | 0 |

### `0dcc25e` — add CLAUDE.md for Claude Code guidance

2025-12-27 · M1 · 1 files · +128/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 128 | 0 |

### `714469a` — fix unused variable warning in test

2025-12-27 · M1 · 1 files · +1/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query_engine.rs` | 1 | 1 |

### `ff9c712` — fix clippy warnings

2025-12-27 · M1 · 6 files · +14/−14 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 2 | 5 |
| `src/compactor.rs` | 5 | 2 |
| `src/flight_service.rs` | 3 | 3 |
| `src/line_protocol.rs` | 1 | 1 |
| `src/main.rs` | 1 | 1 |
| `src/series_tracker.rs` | 2 | 2 |

### `20c4876` — format code with cargo fmt

2025-12-27 · M1 · 11 files · +831/−394 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 43 | 17 |
| `src/compactor.rs` | 213 | 91 |
| `src/config.rs` | 9 | 6 |
| `src/database_manager.rs` | 22 | 11 |
| `src/flight_service.rs` | 53 | 44 |
| `src/line_protocol.rs` | 4 | 4 |
| `src/logger.rs` | 3 | 6 |
| `src/main.rs` | 200 | 88 |
| `src/parquet_writer.rs` | 9 | 5 |
| `src/query_engine.rs` | 159 | 55 |
| `src/series_tracker.rs` | 116 | 67 |

### `be3e82d` — add date partitioning to series_tracker

2025-12-27 · M1 · 1 files · +95/−28 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/series_tracker.rs` | 95 | 28 |

### `f542801` — consolidate documentation: 44 files to 11

2025-12-27 · M1 · 45 files · +581/−9,484 text lines

| File | Added | Deleted |
|---|---:|---:|
| `BUILD_AND_TEST.md` | 0 | 94 |
| `CATALOG_STRUCTURE.md` | 0 | 89 |
| `CHANGES_SUMMARY.md` | 0 | 145 |
| `CLAUDE.md` | 13 | 0 |
| `FIXES_APPLIED.md` | 0 | 145 |
| `FLIGHT_USAGE.md` | 0 | 132 |
| `IMPLEMENTATION_SUMMARY.md` | 0 | 306 |
| `LOGGING_CHANGES.md` | 0 | 146 |
| `MIGRATION_SCHEMA_CONFIG.md` | 0 | 283 |
| `MULTI_LEVEL_COMPACTION.md` | 0 | 427 |
| `NON_PARTITIONED_COMPACTION.md` | 0 | 450 |
| `QUICKSTART.md` | 0 | 212 |
| `QUICK_REFERENCE.md` | 0 | 107 |
| `SCHEMA_CONFIG_CHANGES.md` | 0 | 335 |
| `SCHEMA_EXTRACTION.md` | 0 | 316 |
| `SCHEMA_FEATURE_README.md` | 0 | 185 |
| `SCHEMA_QUICK_REF.md` | 0 | 107 |
| `SCHEMA_UPDATE_SUMMARY.md` | 0 | 218 |
| `config.example.toml` | 92 | 0 |
| `config.examples.toml` | 0 | 255 |
| `config.toml.example` | 0 | 92 |
| `docs/REFACTORING_SUMMARY.md` | 0 | 193 |
| `docs/architecture-datafusion-integration.md` | 0 | 459 |
| `docs/architecture-directory-structure.md` | 0 | 339 |
| `docs/architecture-leveled-compaction.md` | 0 | 413 |
| `docs/architecture-parquet-writer.md` | 0 | 227 |
| `docs/architecture-sql-table-naming.md` | 0 | 262 |
| `docs/arrow-flight.md` | 76 | 0 |
| `docs/changelog.md` | 0 | 174 |
| `docs/compaction.md` | 108 | 0 |
| `docs/features-compaction.md` | 0 | 375 |
| `docs/features-influxdb-compatibility.md` | 0 | 338 |
| `docs/features-series-cardinality.md` | 0 | 389 |
| `docs/guide-compaction-quickstart.md` | 0 | 279 |
| `docs/guide-compaction-testing.md` | 0 | 326 |
| `docs/guide-correct-query-examples.md` | 0 | 253 |
| `docs/guide-query-examples-updated.md` | 0 | 348 |
| `docs/guide-query-examples.md` | 0 | 291 |
| `docs/guide-series-stats.md` | 0 | 205 |
| `docs/quickstart.md` | 69 | 110 |
| `docs/schema-configuration.md` | 111 | 0 |
| `docs/troubleshooting-compaction-fix.md` | 0 | 136 |
| `docs/troubleshooting-compaction-timing.md` | 0 | 176 |
| `docs/troubleshooting-utf8view-fix.md` | 0 | 147 |
| `docs/troubleshooting.md` | 112 | 0 |

### `c902dd2` — consolidate test scripts to scripts/ directory

2025-12-27 · M1 · 17 files · +652/−633 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 18 | 0 |
| `scripts/test_build.sh` | 6 | 0 |
| `scripts/test_compaction_multilevel.sh` | 79 | 0 |
| `scripts/test_compaction_nonpartitioned.sh` | 104 | 0 |
| `scripts/test_comprehensive.sh` | 129 | 0 |
| `scripts/test_partition_query.sh` | 44 | 0 |
| `scripts/test_partitions.sh` | 146 | 0 |
| `scripts/test_schemas.sh` | 68 | 0 |
| `scripts/test_with_queries.sh` | 0 | 129 |
| `scripts/test_write.sh` | 58 | 0 |
| `test_build.sh` | 0 | 5 |
| `test_multilevel_compaction.sh` | 0 | 79 |
| `test_non_partitioned_compaction.sh` | 0 | 104 |
| `test_partition_query.sh` | 0 | 44 |
| `test_partitions.sh` | 0 | 146 |
| `test_schemas.sh` | 0 | 68 |
| `test_write.sh` | 0 | 58 |

### `049fbb0` — revert documentation consolidation, restore original files

2025-12-27 · M1 · 45 files · +9,484/−580 text lines

| File | Added | Deleted |
|---|---:|---:|
| `BUILD_AND_TEST.md` | 94 | 0 |
| `CATALOG_STRUCTURE.md` | 89 | 0 |
| `CHANGES_SUMMARY.md` | 145 | 0 |
| `CLAUDE.md` | 0 | 12 |
| `FIXES_APPLIED.md` | 145 | 0 |
| `FLIGHT_USAGE.md` | 132 | 0 |
| `IMPLEMENTATION_SUMMARY.md` | 306 | 0 |
| `LOGGING_CHANGES.md` | 146 | 0 |
| `MIGRATION_SCHEMA_CONFIG.md` | 283 | 0 |
| `MULTI_LEVEL_COMPACTION.md` | 427 | 0 |
| `NON_PARTITIONED_COMPACTION.md` | 450 | 0 |
| `QUICKSTART.md` | 212 | 0 |
| `QUICK_REFERENCE.md` | 107 | 0 |
| `SCHEMA_CONFIG_CHANGES.md` | 335 | 0 |
| `SCHEMA_EXTRACTION.md` | 316 | 0 |
| `SCHEMA_FEATURE_README.md` | 185 | 0 |
| `SCHEMA_QUICK_REF.md` | 107 | 0 |
| `SCHEMA_UPDATE_SUMMARY.md` | 218 | 0 |
| `config.example.toml` | 0 | 92 |
| `config.examples.toml` | 255 | 0 |
| `config.toml.example` | 92 | 0 |
| `docs/REFACTORING_SUMMARY.md` | 193 | 0 |
| `docs/architecture-datafusion-integration.md` | 459 | 0 |
| `docs/architecture-directory-structure.md` | 339 | 0 |
| `docs/architecture-leveled-compaction.md` | 413 | 0 |
| `docs/architecture-parquet-writer.md` | 227 | 0 |
| `docs/architecture-sql-table-naming.md` | 262 | 0 |
| `docs/arrow-flight.md` | 0 | 76 |
| `docs/changelog.md` | 174 | 0 |
| `docs/compaction.md` | 0 | 108 |
| `docs/features-compaction.md` | 375 | 0 |
| `docs/features-influxdb-compatibility.md` | 338 | 0 |
| `docs/features-series-cardinality.md` | 389 | 0 |
| `docs/guide-compaction-quickstart.md` | 279 | 0 |
| `docs/guide-compaction-testing.md` | 326 | 0 |
| `docs/guide-correct-query-examples.md` | 253 | 0 |
| `docs/guide-query-examples-updated.md` | 348 | 0 |
| `docs/guide-query-examples.md` | 291 | 0 |
| `docs/guide-series-stats.md` | 205 | 0 |
| `docs/quickstart.md` | 110 | 69 |
| `docs/schema-configuration.md` | 0 | 111 |
| `docs/troubleshooting-compaction-fix.md` | 136 | 0 |
| `docs/troubleshooting-compaction-timing.md` | 176 | 0 |
| `docs/troubleshooting-utf8view-fix.md` | 147 | 0 |
| `docs/troubleshooting.md` | 0 | 112 |

### `d3c723b` — rename series_stats to pmc

2025-12-27 · M1 · 4 files · +8/−8 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 1 | 1 |
| `src/config.rs` | 1 | 1 |
| `src/main.rs` | 1 | 1 |
| `src/series_tracker.rs` | 5 | 5 |

### `2d96481` — add mysql client api

2025-12-27 · M1 · 6 files · +1,646/−57 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 751 | 55 |
| `Cargo.toml` | 2 | 1 |
| `scripts/test_mysql.sh` | 276 | 0 |
| `src/config.rs` | 32 | 0 |
| `src/main.rs` | 35 | 1 |
| `src/mysql_service.rs` | 550 | 0 |

### `551ec54` — add cache memory limits to prevent OOM under write-heavy load

2025-12-27 · M1 · 4 files · +232/−34 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 1 | 1 |
| `src/cache.rs` | 183 | 26 |
| `src/config.rs` | 1 | 1 |
| `src/main.rs` | 47 | 6 |

### `aaaabda` — update SeriesCache key from tuple to hash in series_tracker

2025-12-27 · M1 · 1 files · +26/−10 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/series_tracker.rs` | 26 | 10 |

## M2 — Durability and standalone operations

65 commits.

### `59e4d9e` — add retention backgroud logic

2025-12-29 · M2 · 5 files · +461/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 49 | 0 |
| `config.toml` | 16 | 0 |
| `src/config.rs` | 52 | 0 |
| `src/main.rs` | 23 | 0 |
| `src/retention_manager.rs` | 321 | 0 |

### `8fa658a` — add retention logic for cache in series_tracker

2025-12-29 · M2 · 1 files · +151/−60 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/series_tracker.rs` | 151 | 60 |

### `021b1f5` — add load_cache at start, add retention config of cache in series_tracker

2025-12-29 · M2 · 4 files · +73/−12 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 3 | 0 |
| `src/config.rs` | 9 | 0 |
| `src/main.rs` | 26 | 0 |
| `src/series_tracker.rs` | 35 | 12 |

### `d649e0c` — update to per-database cache in series tracker

2025-12-29 · M2 · 1 files · +27/−14 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/series_tracker.rs` | 27 | 14 |

### `bd74d2d` — fix test_write.sh

2025-12-30 · M2 · 1 files · +18/−14 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/test_write.sh` | 18 | 14 |

### `6443842` — add schema field in Point, extract service(schema) when parse_line)

2025-12-30 · M2 · 5 files · +67/−108 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 2 | 33 |
| `src/line_protocol.rs` | 56 | 6 |
| `src/main.rs` | 5 | 6 |
| `src/parquet_writer.rs` | 2 | 44 |
| `src/series_tracker.rs` | 2 | 19 |

### `4591fff` — add WAL

2025-12-30 · M2 · 12 files · +2,692/−9 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 11 | 0 |
| `Cargo.toml` | 3 | 1 |
| `WAL_DESIGN.md` | 319 | 0 |
| `WAL_FILE_FORMAT.md` | 646 | 0 |
| `WAL_FLUSH_AND_RECOVERY.md` | 495 | 0 |
| `WAL_SIMPLE_SOLUTION.md` | 426 | 0 |
| `config.toml` | 16 | 0 |
| `src/cache.rs` | 21 | 0 |
| `src/config.rs` | 52 | 0 |
| `src/main.rs` | 99 | 2 |
| `src/series_tracker.rs` | 6 | 6 |
| `src/wal.rs` | 598 | 0 |

### `43073fe` — wal recovery

2025-12-30 · M2 · 5 files · +786/−86 text lines

| File | Added | Deleted |
|---|---:|---:|
| `WAL_RECOVERY_IMPLEMENTED.md` | 540 | 0 |
| `src/cache.rs` | 4 | 0 |
| `src/config.rs` | 2 | 2 |
| `src/main.rs` | 144 | 73 |
| `src/wal.rs` | 96 | 11 |

### `1140398` — flush recoverd data immediately to ensure durability

2025-12-30 · M2 · 2 files · +45/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 6 | 1 |
| `src/wal.rs` | 39 | 1 |

### `b5b322c` — add trait DeduplicationStrategy, add bloomfilter implement

2025-12-30 · M2 · 9 files · +1,194/−133 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 27 | 0 |
| `Cargo.toml` | 3 | 1 |
| `SERIES_DEDUPLICATION.md` | 567 | 0 |
| `config.toml` | 27 | 1 |
| `scripts/test_write.sh` | 2 | 0 |
| `src/config.rs` | 59 | 0 |
| `src/main.rs` | 2 | 0 |
| `src/series_dedup.rs` | 392 | 0 |
| `src/series_tracker.rs` | 115 | 131 |

### `aeec9c9` — add COMPACTION_OPTIMIZATION_PLAN.md

2025-12-30 · M2 · 1 files · +1,562/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/COMPACTION_OPTIMIZATION_PLAN.md` | 1562 | 0 |

### `0414161` — add age-base compaction

2025-12-31 · M2 · 4 files · +1,449/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/AGE_BASED_CONSOLIDATION.md` | 1106 | 0 |
| `src/compactor.rs` | 303 | 0 |
| `src/config.rs` | 28 | 1 |
| `src/main.rs` | 12 | 0 |

### `920bbd8` — full compaction

2025-12-31 · M2 · 4 files · +1,197/−349 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/DATE_BASED_FULL_COMPACTION.md` | 847 | 0 |
| `src/compactor.rs` | 285 | 294 |
| `src/config.rs` | 32 | 31 |
| `src/main.rs` | 33 | 24 |

### `979c087` — add test scripts for full compaction

2025-12-31 · M2 · 5 files · +1,049/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 5 | 0 |
| `scripts/README_COMPACTION_TESTS.md` | 314 | 0 |
| `scripts/test_compaction_dual_strategy.sh` | 367 | 0 |
| `scripts/test_compaction_quick.sh` | 139 | 0 |
| `scripts/test_full_compaction_now.sh` | 224 | 0 |

### `15e0133` — opt compact logic, top-level compaction

2025-12-31 · M2 · 3 files · +76/−37 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 2 | 5 |
| `src/compactor.rs` | 73 | 31 |
| `src/config.rs` | 1 | 1 |

### `eea5a15` — filter out files oversize target_file_size

2025-12-31 · M2 · 2 files · +133/−42 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 132 | 41 |
| `src/config.rs` | 1 | 1 |

### `6576b61` — add golang script metric-sender

2025-12-31 · M2 · 6 files · +719/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `metric-sender/.gitignore` | 14 | 0 |
| `metric-sender/Makefile` | 48 | 0 |
| `metric-sender/README.md` | 315 | 0 |
| `metric-sender/example_test_data.sh` | 73 | 0 |
| `metric-sender/go.mod` | 5 | 0 |
| `metric-sender/main.go` | 264 | 0 |

### `a4c8986` — add config for metric-sender

2025-12-31 · M2 · 5 files · +142/−49 text lines

| File | Added | Deleted |
|---|---:|---:|
| `metric-sender/README.md` | 69 | 24 |
| `metric-sender/config.toml` | 10 | 0 |
| `metric-sender/go.mod` | 2 | 0 |
| `metric-sender/go.sum` | 2 | 0 |
| `metric-sender/main.go` | 59 | 25 |

### `c0bd6eb` — fix sql qualified bug about mysql client api

2025-12-31 · M2 · 1 files · +47/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query_engine.rs` | 47 | 2 |

### `e22d01b` — classify all the docs

2026-01-01 · M2 · 60 files · +11,053/−11,053 text lines

| File | Added | Deleted |
|---|---:|---:|
| `ARCHITECTURE.md` | 0 | 277 |
| `BUILD_AND_TEST.md` | 0 | 94 |
| `CATALOG_STRUCTURE.md` | 0 | 89 |
| `CHANGELOG.md` | 0 | 81 |
| `CHANGES_SUMMARY.md` | 0 | 145 |
| `CONFIG.md` | 0 | 297 |
| `FIXES_APPLIED.md` | 0 | 145 |
| `FLIGHT_USAGE.md` | 0 | 132 |
| `IMPLEMENTATION_SUMMARY.md` | 0 | 306 |
| `LOGGING_CHANGES.md` | 0 | 146 |
| `MIGRATION_SCHEMA_CONFIG.md` | 0 | 283 |
| `MULTI_LEVEL_COMPACTION.md` | 0 | 427 |
| `NON_PARTITIONED_COMPACTION.md` | 0 | 450 |
| `QUICKSTART.md` | 0 | 212 |
| `QUICK_REFERENCE.md` | 0 | 107 |
| `SCHEMA_CONFIG_CHANGES.md` | 0 | 335 |
| `SCHEMA_EXTRACTION.md` | 0 | 316 |
| `SCHEMA_FEATURE_README.md` | 0 | 185 |
| `SCHEMA_QUICK_REF.md` | 0 | 107 |
| `SCHEMA_UPDATE_SUMMARY.md` | 0 | 218 |
| `SERIES_DEDUPLICATION.md` | 0 | 567 |
| `WAL_DESIGN.md` | 0 | 319 |
| `WAL_FILE_FORMAT.md` | 0 | 646 |
| `WAL_FLUSH_AND_RECOVERY.md` | 0 | 495 |
| `WAL_RECOVERY_IMPLEMENTED.md` | 0 | 540 |
| `WAL_SIMPLE_SOLUTION.md` | 0 | 426 |
| `docs/AGE_BASED_CONSOLIDATION.md` | 0 | 1106 |
| `docs/COMPACTION_OPTIMIZATION_PLAN.md` | 0 | 1562 |
| `docs/DATE_BASED_FULL_COMPACTION.md` | 0 | 847 |
| `docs/REFACTORING_SUMMARY.md` | 0 | 193 |
| `docs/architecture-catalog-structure.md` | 89 | 0 |
| `docs/architecture-compaction-multilevel.md` | 427 | 0 |
| `docs/architecture-compaction-optimization.md` | 1562 | 0 |
| `docs/architecture-overview.md` | 277 | 0 |
| `docs/architecture-schema-extraction.md` | 316 | 0 |
| `docs/architecture-wal-design.md` | 319 | 0 |
| `docs/architecture-wal-file-format.md` | 646 | 0 |
| `docs/architecture-wal-simple-solution.md` | 426 | 0 |
| `docs/changelog-implementation.md` | 306 | 0 |
| `docs/changelog-logging.md` | 146 | 0 |
| `docs/changelog-main.md` | 81 | 0 |
| `docs/changelog-refactoring.md` | 193 | 0 |
| `docs/changelog-schema-config.md` | 335 | 0 |
| `docs/changelog-schema-update.md` | 218 | 0 |
| `docs/changelog-summary.md` | 145 | 0 |
| `docs/changelog-wal-recovery.md` | 540 | 0 |
| `docs/features-age-based-consolidation.md` | 1106 | 0 |
| `docs/features-compaction-nonpartitioned.md` | 450 | 0 |
| `docs/features-date-based-compaction.md` | 847 | 0 |
| `docs/features-flight-usage.md` | 132 | 0 |
| `docs/features-schema.md` | 185 | 0 |
| `docs/features-series-deduplication.md` | 567 | 0 |
| `docs/features-wal-flush-recovery.md` | 495 | 0 |
| `docs/guide-build-and-test.md` | 94 | 0 |
| `docs/guide-configuration.md` | 297 | 0 |
| `docs/guide-quick-reference.md` | 107 | 0 |
| `docs/guide-quickstart-main.md` | 212 | 0 |
| `docs/guide-schema-migration.md` | 283 | 0 |
| `docs/guide-schema-quick-ref.md` | 107 | 0 |
| `docs/troubleshooting-fixes-applied.md` | 145 | 0 |

### `7186202` — opt configs and docs

2026-01-01 · M2 · 6 files · +0/−838 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.production.toml` | 0 | 44 |
| `config.toml.example` | 0 | 92 |
| `docs/guide-schema-migration.md` | 0 | 283 |
| `docs/guide-schema-quick-ref.md` | 0 | 107 |
| `docs/troubleshooting-compaction-fix.md` | 0 | 136 |
| `docs/troubleshooting-compaction-timing.md` | 0 | 176 |

### `61a51b1` — add config to set service or measurement as table

2026-01-05 · M2 · 6 files · +161/−16 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 105 | 5 |
| `config.toml` | 13 | 4 |
| `src/config.rs` | 21 | 0 |
| `src/line_protocol.rs` | 2 | 0 |
| `src/main.rs` | 17 | 7 |
| `src/series_tracker.rs` | 3 | 0 |

### `5b05f6a` — update default_full_compaction_hour from 1 to 3

2026-01-05 · M2 · 1 files · +2/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 2 | 2 |

### `2c22f88` — add config to enable/disable save the raw series or just track series

2026-01-05 · M2 · 4 files · +29/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 5 | 0 |
| `src/config.rs` | 10 | 0 |
| `src/main.rs` | 12 | 0 |
| `src/series_tracker.rs` | 2 | 0 |

### `1a6b88e` — update max log file from 30 to 10

2026-01-06 · M2 · 1 files · +1/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 1 | 1 |

### `80f2089` — update metric-sender, add retry times, connect and read timeouts

2026-01-06 · M2 · 3 files · +144/−25 text lines

| File | Added | Deleted |
|---|---:|---:|
| `metric-sender/README.md` | 42 | 0 |
| `metric-sender/config.toml` | 10 | 0 |
| `metric-sender/main.go` | 92 | 25 |

### `81498fd` — add version subcommand to print build info

2026-01-06 · M2 · 3 files · +115/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 4 | 1 |
| `build.rs` | 57 | 0 |
| `src/main.rs` | 54 | 2 |

### `8d17530` — add deploy.sh

2026-01-06 · M2 · 5 files · +165/−5 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 1 |
| `Cargo.toml` | 5 | 1 |
| `build.sh` | 5 | 3 |
| `deploy.sh` | 128 | 0 |
| `tsdb-stats.service` | 26 | 0 |

### `b0ad369` — add rate limiting for the compactor and Memory budget to prevent OOM

2026-01-06 · M2 · 5 files · +376/−47 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 24 | 0 |
| `src/compactor.rs` | 151 | 7 |
| `src/config.rs` | 41 | 0 |
| `src/main.rs` | 7 | 40 |
| `src/rate_limiter.rs` | 153 | 0 |

### `5519b2d` — compact concurrently by table

2026-01-06 · M2 · 1 files · +131/−37 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 131 | 37 |

### `89f2fa8` — remove Memory budget, it is hard to estimate

2026-01-06 · M2 · 3 files · +6/−52 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 4 | 3 |
| `src/compactor.rs` | 2 | 39 |
| `src/config.rs` | 0 | 10 |

### `0551219` — fix wal rotate bugs: wal files not removed in time. flush cache after replay all the wal files when startup

2026-01-06 · M2 · 2 files · +90/−29 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 11 | 3 |
| `src/wal.rs` | 79 | 26 |

### `3c1b700` — add more welcome info during startup

2026-01-06 · M2 · 1 files · +13/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 13 | 1 |

### `a9e253a` — add recovery mode, which do not remove all wal files after flushed

2026-01-06 · M2 · 2 files · +43/−14 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 18 | 10 |
| `src/wal.rs` | 25 | 4 |

### `e5578de` — fix load cache

2026-01-06 · M2 · 1 files · +27/−16 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 27 | 16 |

### `7cb9350` — add pmc to config

2026-01-07 · M2 · 4 files · +33/−23 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 5 | 4 |
| `src/config.rs` | 9 | 4 |
| `src/main.rs` | 5 | 4 |
| `src/series_tracker.rs` | 14 | 11 |

### `ab30fbf` — add authentification

2026-01-07 · M2 · 6 files · +211/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 1 | 0 |
| `config.toml` | 16 | 0 |
| `src/auth.rs` | 135 | 0 |
| `src/config.rs` | 46 | 0 |
| `src/main.rs` | 12 | 1 |

### `5e5bdad` — add metric-sender.service

2026-01-07 · M2 · 2 files · +23/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `build.sh` | 1 | 1 |
| `metric-sender/metric-sender.service` | 22 | 0 |

### `8059918` — small fix, set max_concurrent_compactions 8 as default

2026-01-07 · M2 · 1 files · +2/−8 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 2 | 8 |

### `ac8a301` — feature: add umc series stats

2026-01-07 · M2 · 5 files · +243/−160 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 12 | 3 |
| `src/config.rs` | 7 | 9 |
| `src/main.rs` | 31 | 26 |
| `src/series_dedup.rs` | 37 | 34 |
| `src/series_tracker.rs` | 156 | 88 |

### `e17fb24` — opt function params about series tracker

2026-01-07 · M2 · 2 files · +26/−22 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 2 | 4 |
| `src/series_tracker.rs` | 24 | 18 |

### `8d99156` — opt series tracker about hash

2026-01-07 · M2 · 2 files · +13/−48 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 0 | 1 |
| `src/series_tracker.rs` | 13 | 47 |

### `1741866` — opt const strings

2026-01-07 · M2 · 4 files · +18/−46 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 0 | 4 |
| `src/main.rs` | 3 | 3 |
| `src/series_tracker.rs` | 15 | 9 |
| `src/wal.rs` | 0 | 30 |

### `5bed42e` — change mysql port from 7003 to 7002

2026-01-07 · M2 · 2 files · +3/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/test_mysql.sh` | 2 | 2 |
| `src/config.rs` | 1 | 1 |

### `8a58fac` — update deploy.sh, copy tools

2026-01-07 · M2 · 1 files · +1/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `deploy.sh` | 1 | 0 |

### `e80ccff` — fix series dedup logic

2026-01-07 · M2 · 2 files · +800/−33 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/series_dedup.rs` | 271 | 33 |
| `src/series_tracker.rs` | 529 | 0 |

### `7969168` — add auth test scripts

2026-01-08 · M2 · 3 files · +912/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/README_AUTH_TESTS.md` | 270 | 0 |
| `scripts/test_auth.sh` | 372 | 0 |
| `scripts/test_auth_comprehensive.sh` | 270 | 0 |

### `efb3be5` — add user and password auth in uri

2026-01-08 · M2 · 5 files · +382/−11 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 7 | 0 |
| `Cargo.toml` | 1 | 0 |
| `config.toml` | 1 | 1 |
| `scripts/test_url_auth.sh` | 311 | 0 |
| `src/auth.rs` | 62 | 10 |

### `3187e64` — add memory handler, opt bloom filter mem

2026-01-08 · M2 · 6 files · +654/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 6 | 6 |
| `docs/memory-optimization.md` | 380 | 0 |
| `scripts/analyze_memory.sh` | 197 | 0 |
| `src/main.rs` | 60 | 0 |
| `src/series_dedup.rs` | 1 | 1 |
| `src/series_tracker.rs` | 10 | 0 |

### `dc93baa` — add bloom config for differenct schemas

2026-01-08 · M2 · 8 files · +421/−52 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 38 | 6 |
| `docs/memory-optimization.md` | 32 | 0 |
| `scripts/test_schema_bloom_config.sh` | 181 | 0 |
| `scripts/verify_schema_config.sh` | 110 | 0 |
| `src/auth.rs` | 0 | 1 |
| `src/config.rs` | 3 | 0 |
| `src/series_dedup.rs` | 39 | 32 |
| `src/series_tracker.rs` | 18 | 13 |

### `e4cb262` — remove unused functions

2026-01-08 · M2 · 4 files · +0/−50 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 0 | 22 |
| `src/compactor.rs` | 0 | 4 |
| `src/rate_limiter.rs` | 0 | 5 |
| `src/wal.rs` | 0 | 19 |

### `10b7826` — add mem profile

2026-01-08 · M2 · 11 files · +2,071/−173 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 337 | 6 |
| `Cargo.toml` | 2 | 0 |
| `config.toml` | 1 | 1 |
| `docs/profiling_guide.md` | 787 | 0 |
| `scripts/analyze_bloom_memory.sh` | 197 | 0 |
| `scripts/analyze_memory.sh` | 144 | 166 |
| `scripts/analyze_profile.sh` | 78 | 0 |
| `scripts/generate_flamegraph.sh` | 79 | 0 |
| `scripts/test_profile_with_load.sh` | 30 | 0 |
| `src/main.rs` | 199 | 0 |
| `src/profiling.rs` | 217 | 0 |

### `6c12b18` — opt log MB -> KB, retention for _system

2026-01-08 · M2 · 2 files · +4/−10 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 4 | 4 |
| `src/retention_manager.rs` | 0 | 6 |

### `62ffc1a` — add full_compaction_max_concurrent to prevent OOM when full compaction

2026-01-08 · M2 · 7 files · +126/−34 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 2 | 0 |
| `config.toml` | 7 | 1 |
| `docs/architecture-compaction-multilevel.md` | 6 | 0 |
| `docs/architecture-compaction-optimization.md` | 7 | 4 |
| `docs/guide-configuration.md` | 70 | 20 |
| `src/compactor.rs` | 22 | 8 |
| `src/config.rs` | 12 | 1 |

### `5156a88` — refactor compactor, and add test cases

2026-01-08 · M2 · 3 files · +512/−201 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 3 | 0 |
| `src/compactor.rs` | 508 | 201 |

### `cb65207` — refactor main.rs, add api.rs

2026-01-08 · M2 · 4 files · +1,582/−1,545 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/api.rs` | 1269 | 0 |
| `src/main.rs` | 312 | 1539 |
| `src/profiling.rs` | 0 | 3 |
| `src/retention_manager.rs` | 1 | 3 |

### `9f5a669` — delete unused replay_series_tracks()

2026-01-09 · M2 · 1 files · +0/−23 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/wal.rs` | 0 | 23 |

### `5ac35db` — opt select query, only register the table queried

2026-01-09 · M2 · 4 files · +196/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/api.rs` | 1 | 6 |
| `src/query_engine.rs` | 194 | 0 |
| `src/rate_limiter.rs` | 1 | 0 |
| `src/wal.rs` | 0 | 1 |

### `320e354` — opt load cache when start, load in parallel

2026-01-09 · M2 · 3 files · +201/−29 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 10 | 0 |
| `src/main.rs` | 105 | 29 |
| `src/series_tracker.rs` | 86 | 0 |

### `4207f50` — opt load_cache, read files in parallel

2026-01-09 · M2 · 2 files · +139/−98 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 10 | 0 |
| `src/series_tracker.rs` | 129 | 98 |

### `a9a1311` — metric-sender, retry for 400 too

2026-01-10 · M2 · 1 files · +4/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `metric-sender/main.go` | 4 | 4 |

### `c6ba8ce` — opt docs

2026-01-10 · M2 · 6 files · +694/−465 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/development-code-standards.md` | 0 | 282 |
| `docs/guide-build-and-test.md` | 0 | 94 |
| `docs/guide-correct-query-examples.md` | 41 | 28 |
| `docs/guide-query-examples-updated.md` | 16 | 5 |
| `docs/guide-query-examples.md` | 82 | 56 |
| `docs/guide-sql-queries.md` | 555 | 0 |

### `44cfabe` — add website

2026-01-10 · M2 · 17 files · +8,422/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/README.md` | 109 | 0 |
| `website/css/style.css` | 1290 | 0 |
| `website/docs/api.html` | 673 | 0 |
| `website/docs/architecture.html` | 478 | 0 |
| `website/docs/compaction.html` | 692 | 0 |
| `website/docs/configuration.html` | 505 | 0 |
| `website/docs/data-model.html` | 545 | 0 |
| `website/docs/examples.html` | 646 | 0 |
| `website/docs/guides.html` | 302 | 0 |
| `website/docs/index.html` | 278 | 0 |
| `website/docs/installation.html` | 685 | 0 |
| `website/docs/quickstart.html` | 369 | 0 |
| `website/docs/sql-queries.html` | 396 | 0 |
| `website/docs/storage.html` | 487 | 0 |
| `website/docs/troubleshooting.html` | 265 | 0 |
| `website/index.html` | 444 | 0 |
| `website/js/main.js` | 258 | 0 |

### `4fc8b99` — add website style choice

2026-01-11 · M2 · 3 files · +522/−122 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/css/style.css` | 415 | 82 |
| `website/index.html` | 10 | 0 |
| `website/js/main.js` | 97 | 40 |

### `3291005` — keep the Solar Light theme, add some animation

2026-01-12 · M2 · 3 files · +804/−584 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/css/style.css` | 544 | 471 |
| `website/index.html` | 13 | 10 |
| `website/js/main.js` | 247 | 103 |

## M3 — First replication and HA design

15 commits.

### `13787e3` — add flight-api.html

2026-01-13 · M3 · 1 files · +667/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/flight-api.html` | 667 | 0 |

### `3263395` — add proposal-cluster-edition.md

2026-01-13 · M3 · 1 files · +2,067/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposal-cluster-edition.md` | 2067 | 0 |

### `9fbfe78` — add docs/proposal-cluster-shared-storage.md

2026-01-13 · M3 · 1 files · +1,229/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposal-cluster-shared-storage.md` | 1229 | 0 |

### `ca3644f` — add proposal-master-slave-ha.md

2026-01-14 · M3 · 2 files · +1,705/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/guide-master-slave-failover-demo.md` | 545 | 0 |
| `docs/proposal-master-slave-ha.md` | 1160 | 0 |

### `5c71d80` — add proposal-master-slave-shared-storage.md

2026-01-14 · M3 · 1 files · +1,124/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposal-master-slave-shared-storage.md` | 1124 | 0 |

### `0a3e5f8` — fix max_log_files for logger

2026-01-14 · M3 · 1 files · +15/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/logger.rs` | 15 | 6 |

### `25a6d8c` — big feature: add master-slave mode

2026-01-14 · M3 · 15 files · +3,484/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.examples.toml` | 225 | 0 |
| `docs/guide-master-slave-ha.md` | 537 | 0 |
| `src/api.rs` | 22 | 0 |
| `src/config.rs` | 218 | 0 |
| `src/ha/api.rs` | 169 | 0 |
| `src/ha/failover/detector.rs` | 162 | 0 |
| `src/ha/failover/mod.rs` | 155 | 0 |
| `src/ha/failover/promoter.rs` | 168 | 0 |
| `src/ha/mod.rs` | 337 | 0 |
| `src/ha/replication/mod.rs` | 161 | 0 |
| `src/ha/replication/protocol.rs` | 199 | 0 |
| `src/ha/replication/receiver.rs` | 450 | 0 |
| `src/ha/replication/sender.rs` | 458 | 0 |
| `src/ha/state.rs` | 97 | 0 |
| `src/main.rs` | 126 | 4 |

### `895ad48` — add tests for master-slave mode

2026-01-15 · M3 · 3 files · +1,027/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/guide-master-slave-ha-local-testing.md` | 353 | 0 |
| `scripts/ha_setup_local.sh` | 243 | 0 |
| `scripts/test_ha_local.sh` | 431 | 0 |

### `48695ad` — add default configs

2026-01-15 · M3 · 3 files · +175/−83 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/ha_setup_local.sh` | 10 | 0 |
| `scripts/test_ha_local.sh` | 12 | 0 |
| `src/config.rs` | 153 | 83 |

### `9c6e691` — fix master slave test case

2026-01-15 · M3 · 2 files · +55/−19 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/ha_setup_local.sh` | 17 | 9 |
| `scripts/test_ha_local.sh` | 38 | 10 |

### `83781c7` — add test cases for ha module

2026-01-15 · M3 · 2 files · +937/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/ha/mod.rs` | 3 | 0 |
| `src/ha/tests.rs` | 934 | 0 |

### `7b56a05` — update website, high-availability.html

2026-01-15 · M3 · 3 files · +643/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/guides.html` | 26 | 0 |
| `website/docs/high-availability.html` | 578 | 0 |
| `website/index.html` | 39 | 4 |

### `0503d71` — update website: add line-protocol.html retention.html

2026-01-15 · M3 · 2 files · +1,185/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/line-protocol.html` | 572 | 0 |
| `website/docs/retention.html` | 613 | 0 |

### `50b2c9b` — update website: fix some sidebar navigation links

2026-01-15 · M3 · 18 files · +814/−99 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/api.html` | 25 | 10 |
| `website/docs/architecture.html` | 11 | 1 |
| `website/docs/compaction.html` | 24 | 13 |
| `website/docs/configuration.html` | 33 | 7 |
| `website/docs/data-model.html` | 35 | 9 |
| `website/docs/examples.html` | 37 | 11 |
| `website/docs/faq.html` | 386 | 0 |
| `website/docs/flight-api.html` | 25 | 8 |
| `website/docs/guides.html` | 17 | 0 |
| `website/docs/high-availability.html` | 20 | 1 |
| `website/docs/index.html` | 2 | 1 |
| `website/docs/installation.html` | 30 | 11 |
| `website/docs/line-protocol.html` | 18 | 1 |
| `website/docs/quickstart.html` | 11 | 1 |
| `website/docs/retention.html` | 17 | 0 |
| `website/docs/sql-queries.html` | 44 | 9 |
| `website/docs/storage.html` | 35 | 9 |
| `website/docs/troubleshooting.html` | 44 | 7 |

### `3f7d7af` — update metric-sender, runQuery() aligned with interval, at the first minute of interval

2026-01-15 · M3 · 1 files · +10/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `metric-sender/main.go` | 10 | 13 |

## M4 — Object-store pivot and faster execution

22 commits.

### `c1d5c08` — init riverdb: delete series_dedup and series_tracker, delete metric-sender

2026-01-19 · M4 · 14 files · +3/−3,541 text lines

| File | Added | Deleted |
|---|---:|---:|
| `metric-sender/.gitignore` | 0 | 14 |
| `metric-sender/Makefile` | 0 | 48 |
| `metric-sender/README.md` | 0 | 402 |
| `metric-sender/config.toml` | 0 | 20 |
| `metric-sender/example_test_data.sh` | 0 | 73 |
| `metric-sender/go.mod` | 0 | 7 |
| `metric-sender/go.sum` | 0 | 2 |
| `metric-sender/main.go` | 0 | 362 |
| `metric-sender/metric-sender.service` | 0 | 22 |
| `src/api.rs` | 2 | 109 |
| `src/config.rs` | 0 | 132 |
| `src/main.rs` | 1 | 143 |
| `src/series_dedup.rs` | 0 | 640 |
| `src/series_tracker.rs` | 0 | 1567 |

### `e3d17d6` — delete ha

2026-01-19 · M4 · 14 files · +3/−3,651 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/api.rs` | 0 | 22 |
| `src/config.rs` | 0 | 218 |
| `src/ha/api.rs` | 0 | 169 |
| `src/ha/failover/detector.rs` | 0 | 162 |
| `src/ha/failover/mod.rs` | 0 | 155 |
| `src/ha/failover/promoter.rs` | 0 | 168 |
| `src/ha/mod.rs` | 0 | 340 |
| `src/ha/replication/mod.rs` | 0 | 161 |
| `src/ha/replication/protocol.rs` | 0 | 199 |
| `src/ha/replication/receiver.rs` | 0 | 450 |
| `src/ha/replication/sender.rs` | 0 | 458 |
| `src/ha/state.rs` | 0 | 97 |
| `src/ha/tests.rs` | 0 | 934 |
| `src/main.rs` | 3 | 118 |

### `0640459` — add object store

2026-01-19 · M4 · 13 files · +1,100/−158 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 383 | 16 |
| `Cargo.toml` | 5 | 1 |
| `config.toml` | 24 | 83 |
| `docs/roadmap-features.md` | 143 | 0 |
| `src/cache.rs` | 11 | 3 |
| `src/compactor.rs` | 17 | 0 |
| `src/config.rs` | 70 | 1 |
| `src/database_manager.rs` | 13 | 0 |
| `src/main.rs` | 28 | 6 |
| `src/parquet_writer.rs` | 59 | 35 |
| `src/query_engine.rs` | 52 | 13 |
| `src/retention_manager.rs` | 17 | 0 |
| `src/storage.rs` | 278 | 0 |

### `05d56b6` — add wal object

2026-01-19 · M4 · 3 files · +350/−253 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 20 | 1 |
| `src/main.rs` | 12 | 10 |
| `src/wal.rs` | 318 | 242 |

### `5b404f8` — add a ticker to persist wal files

2026-01-19 · M4 · 2 files · +103/−16 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 4 | 1 |
| `src/wal.rs` | 99 | 15 |

### `489e668` — add explain sql support, Time-range pruning using Parquet statistics

2026-01-19 · M4 · 4 files · +652/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 38 | 0 |
| `src/api.rs` | 113 | 0 |
| `src/main.rs` | 5 | 0 |
| `src/query_engine.rs` | 496 | 3 |

### `943745f` — sort by time before flush

2026-01-21 · M4 · 2 files · +190/−35 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 94 | 20 |
| `src/parquet_writer.rs` | 96 | 15 |

### `8a7a126` — k-way sort by time when compact

2026-01-21 · M4 · 1 files · +196/−31 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/compactor.rs` | 196 | 31 |

### `abab4e0` — write api return 204 instead of 200

2026-01-21 · M4 · 1 files · +1/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/api.rs` | 1 | 1 |

### `2f5ccb0` — add cache shards to improve the write throughput

2026-01-21 · M4 · 3 files · +118/−48 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 1 | 0 |
| `src/cache.rs` | 116 | 48 |

### `a18eed4` — backgroup wal write if sync_on_write is false. write throughput reached to 200K rows/s from 20K rows/s

2026-01-22 · M4 · 4 files · +64/−29 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 1 | 1 |
| `src/api.rs` | 56 | 26 |
| `src/config.rs` | 2 | 2 |
| `src/wal.rs` | 5 | 0 |

### `45d1c53` — add schema cache when convert points to RecordBatch

2026-01-22 · M4 · 3 files · +86/−34 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/api.rs` | 6 | 2 |
| `src/cache.rs` | 65 | 28 |
| `src/line_protocol.rs` | 15 | 4 |

### `b9791c3` — add fields to the RecordBatches

2026-01-22 · M4 · 2 files · +301/−16 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/cache.rs` | 119 | 15 |
| `src/line_protocol.rs` | 182 | 1 |

### `fb55ede` — add write_performance_optimization.md

2026-01-22 · M4 · 1 files · +399/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/write_performance_optimization.md` | 399 | 0 |

### `3d5582c` — add proposal-query-performance-optimization.md

2026-01-22 · M4 · 3 files · +911/−399 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposal-query-performance-optimization.md` | 512 | 0 |
| `docs/proposal-write-performance-optimization.md` | 399 | 0 |
| `docs/write_performance_optimization.md` | 0 | 399 |

### `9c08bbc` — trivial opts

2026-01-22 · M4 · 1 files · +2/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/api.rs` | 2 | 3 |

### `cf4e3a2` — add TableMetadataCache and  Parquet Metadata Caching

2026-01-22 · M4 · 4 files · +749/−55 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 6 | 1 |
| `src/api.rs` | 107 | 0 |
| `src/main.rs` | 6 | 0 |
| `src/query_engine.rs` | 630 | 54 |

### `b4662e6` — big feature: add query cache, wrk query performance improves from 60 qps to  45K qps

2026-01-22 · M4 · 5 files · +792/−51 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 5 | 4 |
| `src/api.rs` | 46 | 20 |
| `src/main.rs` | 2 | 0 |
| `src/query_cache.rs` | 660 | 0 |
| `src/query_engine.rs` | 79 | 27 |

### `1562f77` — add query cache config

2026-01-22 · M4 · 5 files · +103/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 7 | 0 |
| `config.toml` | 19 | 0 |
| `src/config.rs` | 61 | 1 |
| `src/main.rs` | 4 | 2 |
| `src/query_cache.rs` | 12 | 0 |

### `2684d57` — Smart Partition Pruning

2026-01-22 · M4 · 1 files · +253/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query_engine.rs` | 253 | 1 |

### `f3ba458` — update writer config, enable Parallel Parquet Reading, Columnar Projection Push-down

2026-01-22 · M4 · 4 files · +82/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 1 | 0 |
| `src/parquet_writer.rs` | 35 | 11 |
| `src/query_engine.rs` | 45 | 9 |

### `4f71930` — opt write path: Parallel Parsing - Use rayon for multi-core parsing

2026-01-22 · M4 · 4 files · +138/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 43 | 3 |
| `Cargo.toml` | 1 | 0 |
| `src/line_protocol.rs` | 67 | 2 |
| `src/query_engine.rs` | 27 | 2 |

## M5 — Query-engine maturity and validation

49 commits.

### `3e32654` — add query directory, init the analyzers and optimzers

2026-01-23 · M5 · 10 files · +759/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 1 | 0 |
| `src/query/analyzers/mod.rs` | 16 | 0 |
| `src/query/analyzers/timestamp_to_date.rs` | 196 | 0 |
| `src/query/executor.rs` | 195 | 0 |
| `src/query/logical_optimizers/mod.rs` | 16 | 0 |
| `src/query/logical_optimizers/partition_filter_pushdown.rs` | 190 | 0 |
| `src/query/mod.rs` | 41 | 0 |
| `src/query/physical_optimizers/mod.rs` | 16 | 0 |
| `src/query/physical_optimizers/scan_config.rs` | 81 | 0 |
| `src/query_engine.rs` | 7 | 6 |

### `3baff1d` — init table provider

2026-01-23 · M5 · 2 files · +607/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/mod.rs` | 2 | 0 |
| `src/query/table_provider.rs` | 605 | 0 |

### `7d19127` — add custom table provider

2026-01-23 · M5 · 4 files · +1,208/−379 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-custom_table_provider.md` | 432 | 0 |
| `src/query/mod.rs` | 1 | 1 |
| `src/query/table_provider.rs` | 732 | 334 |
| `src/query_engine.rs` | 43 | 44 |

### `a233b1e` — add use_custom_table_provider knob, to choose ListingTableProvider or ParquetTableProvider

2026-01-23 · M5 · 4 files · +147/−52 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-custom_table_provider.md` | 27 | 5 |
| `src/config.rs` | 25 | 0 |
| `src/main.rs` | 2 | 1 |
| `src/query_engine.rs` | 93 | 46 |

### `135a1c1` — source files layout opt

2026-01-23 · M5 · 40 files · +11,918/−11,804 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/source-structure.md` | 106 | 0 |
| `src/api.rs` | 0 | 1436 |
| `src/auth.rs` | 0 | 186 |
| `src/cache.rs` | 0 | 599 |
| `src/compactor.rs` | 0 | 2270 |
| `src/database_manager.rs` | 0 | 388 |
| `src/flight_service.rs` | 0 | 311 |
| `src/jobs/compactor.rs` | 2270 | 0 |
| `src/jobs/database_manager.rs` | 388 | 0 |
| `src/jobs/logger.rs` | 65 | 0 |
| `src/jobs/mod.rs` | 5 | 0 |
| `src/jobs/profiling.rs` | 214 | 0 |
| `src/jobs/retention_manager.rs` | 330 | 0 |
| `src/line_protocol.rs` | 0 | 432 |
| `src/logger.rs` | 0 | 65 |
| `src/main.rs` | 9 | 18 |
| `src/mysql_service.rs` | 0 | 550 |
| `src/parquet_writer.rs` | 0 | 307 |
| `src/profiling.rs` | 0 | 214 |
| `src/query/mod.rs` | 2 | 0 |
| `src/query/query_cache.rs` | 672 | 0 |
| `src/query/query_engine.rs` | 2712 | 0 |
| `src/query_cache.rs` | 0 | 672 |
| `src/query_engine.rs` | 0 | 2712 |
| `src/rate_limiter.rs` | 0 | 149 |
| `src/retention_manager.rs` | 0 | 330 |
| `src/services/api.rs` | 1436 | 0 |
| `src/services/auth.rs` | 186 | 0 |
| `src/services/flight_service.rs` | 311 | 0 |
| `src/services/mod.rs` | 5 | 0 |
| `src/services/mysql_service.rs` | 550 | 0 |
| `src/services/rate_limiter.rs` | 149 | 0 |
| `src/storage.rs` | 0 | 278 |
| `src/storage/cache.rs` | 599 | 0 |
| `src/storage/line_protocol.rs` | 432 | 0 |
| `src/storage/mod.rs` | 5 | 0 |
| `src/storage/parquet_writer.rs` | 307 | 0 |
| `src/storage/storage.rs` | 278 | 0 |
| `src/storage/wal.rs` | 887 | 0 |
| `src/wal.rs` | 0 | 887 |

### `add7b5b` — big feature: add group by time interval, compatable with influxdb 1

2026-01-23 · M5 · 9 files · +2,327/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 1 | 0 |
| `src/query/fill_processor.rs` | 833 | 0 |
| `src/query/functions/mod.rs` | 8 | 0 |
| `src/query/functions/time_bucket.rs` | 416 | 0 |
| `src/query/group_by_time.rs` | 551 | 0 |
| `src/query/mod.rs` | 8 | 0 |
| `src/query/query_engine.rs` | 69 | 2 |
| `src/query/time_interval.rs` | 440 | 0 |

### `5ecb86c` — fix group by time

2026-01-23 · M5 · 4 files · +697/−40 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE_CONTEXT.md` | 301 | 0 |
| `scripts/test_group_by_time.sh` | 216 | 0 |
| `src/query/functions/time_bucket.rs` | 171 | 39 |
| `src/query/group_by_time.rs` | 9 | 1 |

### `0059e95` — big feature: add memtable, fresh data can be queried immediately

2026-01-23 · M5 · 7 files · +1,487/−12 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 4 | 2 |
| `src/query/memtable_provider.rs` | 433 | 0 |
| `src/query/mod.rs` | 4 | 0 |
| `src/query/query_engine.rs` | 149 | 10 |
| `src/query/union_provider.rs` | 348 | 0 |
| `src/storage/cache.rs` | 193 | 0 |
| `understanding.md` | 356 | 0 |

### `70ba22d` — split query_engine.rs, splited to serveral code files

2026-01-23 · M5 · 5 files · +1,219/−1,613 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/metadata_cache.rs` | 302 | 0 |
| `src/query/mod.rs` | 3 | 0 |
| `src/query/query_engine.rs` | 143 | 1613 |
| `src/query/sql_utils.rs` | 654 | 0 |
| `src/query/types.rs` | 117 | 0 |

### `c8ec00d` — big format update, rustformat the whole project

2026-01-23 · M5 · 29 files · +1,066/−539 text lines

| File | Added | Deleted |
|---|---:|---:|
| `build.rs` | 3 | 1 |
| `src/config.rs` | 1 | 4 |
| `src/jobs/compactor.rs` | 77 | 52 |
| `src/jobs/profiling.rs` | 33 | 9 |
| `src/jobs/retention_manager.rs` | 14 | 5 |
| `src/main.rs` | 42 | 16 |
| `src/query/analyzers/timestamp_to_date.rs` | 1 | 4 |
| `src/query/executor.rs` | 47 | 29 |
| `src/query/fill_processor.rs` | 21 | 28 |
| `src/query/functions/time_bucket.rs` | 17 | 22 |
| `src/query/group_by_time.rs` | 30 | 20 |
| `src/query/memtable_provider.rs` | 3 | 3 |
| `src/query/metadata_cache.rs` | 33 | 12 |
| `src/query/mod.rs` | 3 | 1 |
| `src/query/query_cache.rs` | 83 | 23 |
| `src/query/query_engine.rs` | 284 | 96 |
| `src/query/sql_utils.rs` | 22 | 12 |
| `src/query/table_provider.rs` | 87 | 57 |
| `src/query/time_interval.rs` | 4 | 1 |
| `src/query/types.rs` | 8 | 6 |
| `src/query/union_provider.rs` | 5 | 5 |
| `src/services/api.rs` | 149 | 70 |
| `src/services/auth.rs` | 4 | 1 |
| `src/services/mysql_service.rs` | 1 | 4 |
| `src/storage/cache.rs` | 9 | 8 |
| `src/storage/line_protocol.rs` | 49 | 15 |
| `src/storage/parquet_writer.rs` | 5 | 8 |
| `src/storage/storage.rs` | 19 | 10 |
| `src/storage/wal.rs` | 12 | 17 |

### `8e9300b` — add first() last() aggregation functions

2026-01-23 · M5 · 4 files · +726/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/functions/first_last.rs` | 633 | 0 |
| `src/query/functions/mod.rs` | 6 | 2 |
| `src/query/group_by_time.rs` | 79 | 4 |
| `src/query/query_engine.rs` | 8 | 1 |

### `9796a28` — cargo fix unused import

2026-01-23 · M5 · 7 files · +4/−18 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/analyzers/timestamp_to_date.rs` | 0 | 1 |
| `src/query/executor.rs` | 0 | 1 |
| `src/query/functions/first_last.rs` | 0 | 1 |
| `src/query/logical_optimizers/partition_filter_pushdown.rs` | 1 | 2 |
| `src/query/mod.rs` | 1 | 10 |
| `src/query/query_engine.rs` | 2 | 2 |
| `src/query/union_provider.rs` | 0 | 1 |

### `5dc606e` — change name to mynadb

2026-01-23 · M5 · 18 files · +117/−113 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 5 | 3 |
| `CLAUDE_CONTEXT.md` | 4 | 2 |
| `Cargo.lock` | 42 | 42 |
| `Cargo.toml` | 3 | 2 |
| `build.sh` | 2 | 2 |
| `config.examples.toml` | 1 | 1 |
| `config.toml` | 5 | 5 |
| `deploy.sh` | 7 | 7 |
| `mynadb.service` | 26 | 0 |
| `scripts/analyze_bloom_memory.sh` | 6 | 6 |
| `scripts/ha_setup_local.sh` | 2 | 2 |
| `scripts/test_ha_local.sh` | 3 | 3 |
| `scripts/test_schema_bloom_config.sh` | 1 | 1 |
| `src/main.rs` | 7 | 6 |
| `src/query/functions/first_last.rs` | 1 | 0 |
| `src/services/auth.rs` | 1 | 1 |
| `src/storage/cache.rs` | 1 | 4 |
| `tsdb-stats.service` | 0 | 26 |

### `50eae89` — update cache persist interval to 60s

2026-01-23 · M5 · 2 files · +2/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 1 | 1 |
| `src/config.rs` | 1 | 1 |

### `5117c37` — fix show databases show tables for mysql service, fetch from memtables either

2026-01-23 · M5 · 4 files · +140/−50 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 1 | 0 |
| `src/query/table_provider.rs` | 34 | 45 |
| `src/services/mysql_service.rs` | 54 | 5 |
| `src/storage/cache.rs` | 51 | 0 |

### `6736edd` — add roadmap-query-performance.md

2026-01-23 · M5 · 1 files · +1,039/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/roadmap-query-performance.md` | 1039 | 0 |

### `c247642` — big feature: 1. add matadata manager (mem-based) 2. parquet files disk cache

2026-01-23 · M5 · 13 files · +3,520/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-caching-layer.md` | 586 | 0 |
| `src/query/catalog/manager.rs` | 257 | 0 |
| `src/query/catalog/memory_manager.rs` | 1306 | 0 |
| `src/query/catalog/mod.rs` | 36 | 0 |
| `src/query/catalog/types.rs` | 243 | 0 |
| `src/query/functions/first_last.rs` | 1 | 1 |
| `src/query/mod.rs` | 2 | 0 |
| `src/storage/cache.rs` | 5 | 1 |
| `src/storage/file_cache/cache.rs` | 90 | 0 |
| `src/storage/file_cache/disk_cache.rs` | 724 | 0 |
| `src/storage/file_cache/mod.rs` | 39 | 0 |
| `src/storage/file_cache/types.rs` | 230 | 0 |
| `src/storage/mod.rs` | 1 | 0 |

### `705e422` — update metadata manager

2026-01-23 · M5 · 8 files · +1,357/−26 text lines

| File | Added | Deleted |
|---|---:|---:|
| `2026-01-23-read-claudemd-and-claudecontextmd-to-init.txt` | 1126 | 0 |
| `src/config.rs` | 6 | 1 |
| `src/main.rs` | 102 | 7 |
| `src/query/catalog/mod.rs` | 1 | 1 |
| `src/services/api.rs` | 46 | 1 |
| `src/storage/cache.rs` | 36 | 11 |
| `src/storage/file_cache/mod.rs` | 2 | 2 |
| `src/storage/parquet_writer.rs` | 38 | 3 |

### `3152cb7` — udpate website, change name to mynadb

2026-01-23 · M5 · 25 files · +1,666/−273 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/README.md` | 27 | 3 |
| `website/css/style.css` | 1 | 1 |
| `website/docs/api.html` | 7 | 7 |
| `website/docs/architecture.html` | 9 | 9 |
| `website/docs/caching.html` | 391 | 0 |
| `website/docs/compaction.html` | 11 | 11 |
| `website/docs/configuration.html` | 9 | 9 |
| `website/docs/data-model.html` | 10 | 10 |
| `website/docs/examples.html` | 6 | 6 |
| `website/docs/faq.html` | 25 | 25 |
| `website/docs/flight-api.html` | 12 | 12 |
| `website/docs/guides.html` | 11 | 11 |
| `website/docs/high-availability.html` | 7 | 7 |
| `website/docs/index.html` | 37 | 16 |
| `website/docs/installation.html` | 60 | 60 |
| `website/docs/line-protocol.html` | 12 | 12 |
| `website/docs/mysql-protocol.html` | 413 | 0 |
| `website/docs/object-store.html` | 488 | 0 |
| `website/docs/quickstart.html` | 15 | 15 |
| `website/docs/retention.html` | 9 | 9 |
| `website/docs/sql-queries.html` | 8 | 8 |
| `website/docs/storage.html` | 10 | 10 |
| `website/docs/troubleshooting.html` | 11 | 11 |
| `website/index.html` | 76 | 20 |
| `website/js/main.js` | 1 | 1 |

### `aa047eb` — update CLAUDE.md with source structure, new config options, and test scripts

2026-01-23 · M5 · 1 files · +122/−69 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 122 | 69 |

### `b8e31a9` — comprehensive website update: metadata manager, disk cache, memtable, time series functions

2026-01-23 · M5 · 6 files · +919/−179 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/architecture.html` | 33 | 8 |
| `website/docs/caching.html` | 544 | 98 |
| `website/docs/index.html` | 58 | 24 |
| `website/docs/quickstart.html` | 8 | 26 |
| `website/docs/sql-queries.html` | 187 | 2 |
| `website/index.html` | 89 | 21 |

### `cb39249` — integrate compactor with metadata manager

2026-01-23 · M5 · 3 files · +138/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 1 | 1 |
| `src/jobs/compactor.rs` | 135 | 5 |
| `src/main.rs` | 2 | 1 |

### `6f6fd8e` — reorganize src/query/ directory structure

2026-01-24 · M5 · 25 files · +4,864/−5,936 text lines

| File | Added | Deleted |
|---|---:|---:|
| `2026-01-23-read-claudemd-and-claudecontextmd-to-init.txt` | 0 | 1126 |
| `src/jobs/compactor.rs` | 36 | 10 |
| `src/main.rs` | 6 | 9 |
| `src/query/cache/metadata_cache.rs` | 323 | 0 |
| `src/query/cache/mod.rs` | 8 | 0 |
| `src/query/cache/query_cache.rs` | 732 | 0 |
| `src/query/fill_processor.rs` | 0 | 826 |
| `src/query/group_by_time.rs` | 0 | 644 |
| `src/query/memtable_provider.rs` | 0 | 433 |
| `src/query/metadata_cache.rs` | 0 | 323 |
| `src/query/mod.rs` | 4 | 9 |
| `src/query/providers/memtable_provider.rs` | 433 | 0 |
| `src/query/providers/mod.rs` | 16 | 0 |
| `src/query/providers/table_provider.rs` | 1022 | 0 |
| `src/query/providers/union_provider.rs` | 347 | 0 |
| `src/query/query_cache.rs` | 0 | 732 |
| `src/query/query_engine.rs` | 8 | 10 |
| `src/query/table_provider.rs` | 0 | 1022 |
| `src/query/time_interval.rs` | 0 | 443 |
| `src/query/timeseries/fill_processor.rs` | 826 | 0 |
| `src/query/timeseries/group_by_time.rs` | 644 | 0 |
| `src/query/timeseries/mod.rs` | 15 | 0 |
| `src/query/timeseries/time_interval.rs` | 443 | 0 |
| `src/query/union_provider.rs` | 0 | 347 |
| `src/storage/file_cache/mod.rs` | 1 | 2 |

### `7c359e9` — update CLAUDE.md with reorganized query/ structure

2026-01-24 · M5 · 1 files · +20/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 20 | 2 |

### `21a839c` — add comprehensive query performance roadmap v2

2026-01-24 · M5 · 1 files · +837/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/roadmap-query-performance-v2.md` | 837 | 0 |

### `9917af9` — implement P0 query performance optimizations

2026-01-24 · M5 · 7 files · +1,042/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 51 | 0 |
| `src/main.rs` | 7 | 1 |
| `src/query/cache/hot_partition_cache.rs` | 744 | 0 |
| `src/query/cache/mod.rs` | 3 | 3 |
| `src/query/providers/mod.rs` | 1 | 3 |
| `src/query/providers/table_provider.rs` | 193 | 3 |
| `src/query/query_engine.rs` | 43 | 5 |

### `3ed385d` — add comprehensive index performance roadmap documentation

2026-01-24 · M5 · 1 files · +890/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/roadmap-index-performance.md` | 890 | 0 |

### `2ff1659` — rename timestamp column to time for InfluxQL 1.x compatibility

2026-01-24 · M5 · 26 files · +171/−171 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 1 | 1 |
| `docs/architecture-custom_table_provider.md` | 1 | 1 |
| `docs/architecture-datafusion-integration.md` | 12 | 12 |
| `docs/architecture-schema-extraction.md` | 1 | 1 |
| `docs/features-wal-flush-recovery.md` | 2 | 2 |
| `docs/guide-correct-query-examples.md` | 1 | 1 |
| `docs/guide-query-examples-updated.md` | 14 | 14 |
| `docs/guide-query-examples.md` | 7 | 7 |
| `docs/guide-sql-queries.md` | 6 | 6 |
| `docs/proposal-query-performance-optimization.md` | 4 | 4 |
| `docs/quickstart.md` | 1 | 1 |
| `docs/roadmap-index-performance.md` | 5 | 5 |
| `docs/roadmap-query-performance-v2.md` | 9 | 9 |
| `docs/roadmap-query-performance.md` | 9 | 9 |
| `scripts/test_compaction.sh` | 1 | 1 |
| `scripts/test_comprehensive.sh` | 2 | 2 |
| `scripts/test_group_by_time.sh` | 5 | 5 |
| `scripts/test_partitions.sh` | 1 | 1 |
| `src/jobs/compactor.rs` | 9 | 9 |
| `src/query/analyzers/timestamp_to_date.rs` | 15 | 15 |
| `src/query/functions/first_last.rs` | 9 | 9 |
| `src/query/functions/time_bucket.rs` | 5 | 5 |
| `src/query/sql_utils.rs` | 10 | 10 |
| `src/query/timeseries/group_by_time.rs` | 29 | 29 |
| `src/storage/cache.rs` | 3 | 3 |
| `src/storage/parquet_writer.rs` | 9 | 9 |

### `aad97cd` — update website docs: rename timestamp column to time

2026-01-24 · M5 · 8 files · +82/−82 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/data-model.html` | 2 | 2 |
| `website/docs/examples.html` | 23 | 23 |
| `website/docs/faq.html` | 3 | 3 |
| `website/docs/flight-api.html` | 6 | 6 |
| `website/docs/mysql-protocol.html` | 8 | 8 |
| `website/docs/sql-queries.html` | 35 | 35 |
| `website/docs/storage.html` | 4 | 4 |
| `website/docs/troubleshooting.html` | 1 | 1 |

### `64b6ef8` — implement cardinality-based tag sort ordering for optimal query performance

2026-01-24 · M5 · 2 files · +314/−27 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/cache.rs` | 210 | 9 |
| `src/storage/parquet_writer.rs` | 104 | 18 |

### `fec4c3e` — add documentation for cardinality-based tag sort ordering

2026-01-24 · M5 · 1 files · +232/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/feature-cardinality-based-sort.md` | 232 | 0 |

### `01a4e2d` — add cardinality-based sorting to compaction

2026-01-24 · M5 · 2 files · +212/−167 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/feature-cardinality-based-sort.md` | 62 | 8 |
| `src/jobs/compactor.rs` | 150 | 159 |

### `14e76d2` — add test cases for compactor

2026-01-24 · M5 · 1 files · +622/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor.rs` | 622 | 0 |

### `6751844` — add comprehensive correctness and chaos testing suite

2026-01-24 · M5 · 6 files · +3,033/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 114 | 1 |
| `Cargo.toml` | 4 | 0 |
| `scripts/test_backward_compat.sh` | 476 | 0 |
| `scripts/test_chaos.sh` | 821 | 0 |
| `tests/correctness.rs` | 851 | 0 |
| `tests/property.rs` | 767 | 0 |

### `06ea04a` — add fuzz testing for parsers (Phase 4)

2026-01-24 · M5 · 6 files · +605/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.gitignore` | 9 | 0 |
| `fuzz/Cargo.toml` | 44 | 0 |
| `fuzz/fuzz_targets/fuzz_field_value.rs` | 130 | 0 |
| `fuzz/fuzz_targets/fuzz_line_protocol.rs` | 161 | 0 |
| `fuzz/fuzz_targets/fuzz_time_interval.rs` | 158 | 0 |
| `scripts/run_fuzz.sh` | 103 | 0 |

### `e594f09` — fix fuzz tests to call real library code

2026-01-24 · M5 · 7 files · +91/−372 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 4 | 0 |
| `fuzz/Cargo.toml` | 2 | 5 |
| `fuzz/fuzz_targets/fuzz_field_value.rs` | 23 | 83 |
| `fuzz/fuzz_targets/fuzz_line_protocol.rs` | 24 | 144 |
| `fuzz/fuzz_targets/fuzz_time_interval.rs` | 15 | 138 |
| `src/lib.rs` | 21 | 0 |
| `src/storage/line_protocol.rs` | 2 | 2 |

### `a36b4e1` — add comprehensive benchmark suite (Phase 5)

2026-01-24 · M5 · 5 files · +1,116/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 130 | 0 |
| `Cargo.toml` | 14 | 0 |
| `benches/line_protocol_bench.rs` | 207 | 0 |
| `benches/query_bench.rs` | 498 | 0 |
| `benches/time_functions_bench.rs` | 267 | 0 |

### `ed8b044` — add edge case benchmarks for comprehensive coverage

2026-01-24 · M5 · 3 files · +1,247/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `benches/line_protocol_bench.rs` | 291 | 0 |
| `benches/query_bench.rs` | 537 | 0 |
| `benches/time_functions_bench.rs` | 419 | 0 |

### `596989e` — add comprehensive edge case correctness tests

2026-01-24 · M5 · 2 files · +1,366/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `tests/correctness.rs` | 1050 | 0 |
| `tests/property.rs` | 316 | 0 |

### `4676d8e` — add testing documentation to website

2026-01-24 · M5 · 23 files · +779/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/api.html` | 1 | 0 |
| `website/docs/architecture.html` | 1 | 0 |
| `website/docs/caching.html` | 1 | 0 |
| `website/docs/compaction.html` | 1 | 0 |
| `website/docs/configuration.html` | 1 | 0 |
| `website/docs/data-model.html` | 1 | 0 |
| `website/docs/examples.html` | 1 | 0 |
| `website/docs/faq.html` | 1 | 0 |
| `website/docs/flight-api.html` | 1 | 0 |
| `website/docs/guides.html` | 1 | 0 |
| `website/docs/high-availability.html` | 1 | 0 |
| `website/docs/index.html` | 5 | 0 |
| `website/docs/installation.html` | 1 | 0 |
| `website/docs/line-protocol.html` | 1 | 0 |
| `website/docs/mysql-protocol.html` | 1 | 0 |
| `website/docs/object-store.html` | 1 | 0 |
| `website/docs/quickstart.html` | 1 | 0 |
| `website/docs/retention.html` | 1 | 0 |
| `website/docs/sql-queries.html` | 1 | 0 |
| `website/docs/storage.html` | 1 | 0 |
| `website/docs/testing.html` | 740 | 0 |
| `website/docs/troubleshooting.html` | 1 | 0 |
| `website/index.html` | 14 | 0 |

### `4602d03` — add comprehensive data integrity and concurrency tests

2026-01-24 · M5 · 4 files · +2,203/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 1 | 0 |
| `tests/concurrency.rs` | 989 | 0 |
| `tests/data_integrity.rs` | 1212 | 0 |

### `2c624ba` — docs: add Phase A and Phase B testing documentation to website

2026-01-24 · M5 · 1 files · +459/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/testing.html` | 459 | 0 |

### `cc5e07a` — docs: update website branding with myna bird theme

2026-01-24 · M5 · 8 files · +30/−18 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/docs/api.html` | 1 | 1 |
| `website/docs/architecture.html` | 4 | 3 |
| `website/docs/caching.html` | 1 | 1 |
| `website/docs/high-availability.html` | 1 | 1 |
| `website/docs/index.html` | 7 | 4 |
| `website/docs/quickstart.html` | 2 | 2 |
| `website/docs/storage.html` | 1 | 1 |
| `website/index.html` | 13 | 5 |

### `4023b0e` — feat: add myna bird inspired logo for MynaDB

2026-01-24 · M5 · 5 files · +297/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/images/favicon.svg` | 40 | 0 |
| `website/images/logo-horizontal.svg` | 60 | 0 |
| `website/images/logo-icon.svg` | 54 | 0 |
| `website/images/logo.svg` | 142 | 0 |
| `website/index.html` | 1 | 1 |

### `7238205` — fix: apply myna bird logo across all website pages

2026-01-24 · M5 · 24 files · +54/−46 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/css/style.css` | 8 | 0 |
| `website/docs/api.html` | 2 | 2 |
| `website/docs/architecture.html` | 2 | 2 |
| `website/docs/caching.html` | 2 | 2 |
| `website/docs/compaction.html` | 2 | 2 |
| `website/docs/configuration.html` | 2 | 2 |
| `website/docs/data-model.html` | 2 | 2 |
| `website/docs/examples.html` | 2 | 2 |
| `website/docs/faq.html` | 2 | 2 |
| `website/docs/flight-api.html` | 2 | 2 |
| `website/docs/guides.html` | 2 | 2 |
| `website/docs/high-availability.html` | 2 | 2 |
| `website/docs/index.html` | 2 | 2 |
| `website/docs/installation.html` | 2 | 2 |
| `website/docs/line-protocol.html` | 2 | 2 |
| `website/docs/mysql-protocol.html` | 2 | 2 |
| `website/docs/object-store.html` | 2 | 2 |
| `website/docs/quickstart.html` | 2 | 2 |
| `website/docs/retention.html` | 2 | 2 |
| `website/docs/sql-queries.html` | 2 | 2 |
| `website/docs/storage.html` | 2 | 2 |
| `website/docs/testing.html` | 2 | 2 |
| `website/docs/troubleshooting.html` | 2 | 2 |
| `website/index.html` | 2 | 2 |

### `7fc664a` — test: add public dataset validation tests

2026-01-24 · M5 · 2 files · +782/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `tests/golden/.gitkeep` | 0 | 0 |
| `tests/public_dataset_validation.rs` | 782 | 0 |

### `20b9cbf` — fix: update public dataset validation tests

2026-01-24 · M5 · 1 files · +134/−68 text lines

| File | Added | Deleted |
|---|---:|---:|
| `tests/public_dataset_validation.rs` | 134 | 68 |

### `68a5559` — test: add correctness verification using pre-computed expected values

2026-01-24 · M5 · 1 files · +301/−23 text lines

| File | Added | Deleted |
|---|---:|---:|
| `tests/public_dataset_validation.rs` | 301 | 23 |

### `64acecb` — test: add correctness verification to remaining dataset tests

2026-01-24 · M5 · 1 files · +428/−59 text lines

| File | Added | Deleted |
|---|---:|---:|
| `tests/public_dataset_validation.rs` | 428 | 59 |

## M6 — Distributed database and Raft control plane

56 commits.

### `bd7bd46` — add minio_test.sh

2026-01-25 · M6 · 2 files · +550/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 9 | 20 |
| `scripts/minio_test.sh` | 541 | 0 |

### `abc3d6c` — feat: add enterprise cluster support with shared-storage architecture

2026-01-25 · M6 · 34 files · +5,634/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.dockerignore` | 30 | 0 |
| `Cargo.lock` | 99 | 1 |
| `Cargo.toml` | 7 | 1 |
| `Dockerfile` | 62 | 0 |
| `deploy/config/haproxy.cfg` | 63 | 0 |
| `deploy/config/node1.toml` | 83 | 0 |
| `deploy/config/node2.toml` | 83 | 0 |
| `deploy/config/node3.toml` | 83 | 0 |
| `deploy/docker-compose.cluster.yaml` | 225 | 0 |
| `deploy/kubernetes/configmap.yaml` | 91 | 0 |
| `deploy/kubernetes/deployment.yaml` | 136 | 0 |
| `deploy/kubernetes/hpa.yaml` | 44 | 0 |
| `deploy/kubernetes/service.yaml` | 65 | 0 |
| `src/cluster/api/management.rs` | 124 | 0 |
| `src/cluster/api/mod.rs` | 19 | 0 |
| `src/cluster/api/status.rs` | 321 | 0 |
| `src/cluster/cache_invalidation.rs` | 462 | 0 |
| `src/cluster/config.rs` | 379 | 0 |
| `src/cluster/coordinator/etcd.rs` | 469 | 0 |
| `src/cluster/coordinator/interface.rs` | 247 | 0 |
| `src/cluster/coordinator/locks.rs` | 290 | 0 |
| `src/cluster/coordinator/mod.rs` | 35 | 0 |
| `src/cluster/coordinator/noop.rs` | 132 | 0 |
| `src/cluster/election.rs` | 317 | 0 |
| `src/cluster/metrics.rs` | 321 | 0 |
| `src/cluster/mod.rs` | 38 | 0 |
| `src/cluster/node/health.rs` | 344 | 0 |
| `src/cluster/node/identity.rs` | 263 | 0 |
| `src/cluster/node/mod.rs` | 12 | 0 |
| `src/cluster/node/registration.rs` | 249 | 0 |
| `src/cluster/write_coordinator.rs` | 372 | 0 |
| `src/config.rs` | 7 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 161 | 4 |

### `ad2790d` — add docs/architecture-cluster.md

2026-01-26 · M6 · 2 files · +1,095/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-cluster.md` | 521 | 0 |
| `website/docs/cluster.html` | 574 | 0 |

### `63eb785` — docs: add enterprise cluster section to website homepage

2026-01-26 · M6 · 1 files · +76/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `website/index.html` | 76 | 1 |

### `037b409` — storage set local

2026-01-26 · M6 · 1 files · +1/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 1 | 1 |

### `a8f0251` — rustfmt the project

2026-01-26 · M6 · 31 files · +1,774/−783 text lines

| File | Added | Deleted |
|---|---:|---:|
| `benches/line_protocol_bench.rs` | 3 | 6 |
| `benches/query_bench.rs` | 24 | 24 |
| `benches/time_functions_bench.rs` | 29 | 37 |
| `src/cluster/api/management.rs` | 21 | 12 |
| `src/cluster/api/mod.rs` | 3 | 9 |
| `src/cluster/api/status.rs` | 58 | 39 |
| `src/cluster/cache_invalidation.rs` | 20 | 21 |
| `src/cluster/coordinator/etcd.rs` | 96 | 36 |
| `src/cluster/coordinator/interface.rs` | 24 | 8 |
| `src/cluster/coordinator/locks.rs` | 11 | 6 |
| `src/cluster/coordinator/mod.rs` | 11 | 11 |
| `src/cluster/coordinator/noop.rs` | 14 | 5 |
| `src/cluster/election.rs` | 8 | 3 |
| `src/cluster/metrics.rs` | 104 | 44 |
| `src/cluster/mod.rs` | 7 | 7 |
| `src/cluster/node/health.rs` | 41 | 19 |
| `src/cluster/node/identity.rs` | 16 | 4 |
| `src/cluster/node/mod.rs` | 3 | 3 |
| `src/cluster/node/registration.rs` | 2 | 4 |
| `src/cluster/write_coordinator.rs` | 11 | 7 |
| `src/jobs/compactor.rs` | 13 | 16 |
| `src/lib.rs` | 6 | 6 |
| `src/main.rs` | 34 | 12 |
| `src/query/timeseries/group_by_time.rs` | 4 | 5 |
| `src/storage/cache.rs` | 8 | 8 |
| `src/storage/parquet_writer.rs` | 4 | 1 |
| `tests/concurrency.rs` | 155 | 46 |
| `tests/correctness.rs` | 105 | 48 |
| `tests/data_integrity.rs` | 354 | 117 |
| `tests/property.rs` | 6 | 15 |
| `tests/public_dataset_validation.rs` | 579 | 204 |

### `4b21dc8` — fix bug: query cache invalidate after flush

2026-01-26 · M6 · 1 files · +32/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/services/api.rs` | 32 | 1 |

### `9be5fde` — add temp_web: AI-generated interactive dashboard example

2026-01-26 · M6 · 4 files · +562/−61 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 22 | 61 |
| `temp_web/charts.js` | 229 | 0 |
| `temp_web/index.html` | 122 | 0 |
| `temp_web/styles.css` | 189 | 0 |

### `8bc2c68` — feat(dashboard): add interactive CPU metrics dashboard with real-time features

2026-01-26 · M6 · 11 files · +1,958/−479 text lines

| File | Added | Deleted |
|---|---:|---:|
| `temp_web/README.md` | 88 | 0 |
| `temp_web/charts.js` | 0 | 229 |
| `temp_web/css/styles.css` | 444 | 0 |
| `temp_web/data_generator.sh` | 65 | 0 |
| `temp_web/index.html` | 134 | 61 |
| `temp_web/js/api.js` | 174 | 0 |
| `temp_web/js/app.js` | 311 | 0 |
| `temp_web/js/charts.js` | 435 | 0 |
| `temp_web/js/export.js` | 182 | 0 |
| `temp_web/proxy_server.py` | 125 | 0 |
| `temp_web/styles.css` | 0 | 189 |

### `9afc5fe` — fix: add date column to RecordBatch at write time

2026-01-26 · M6 · 2 files · +17/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/memtable_provider.rs` | 2 | 2 |
| `src/storage/cache.rs` | 15 | 5 |

### `b0bd908` — docs: add meta service architecture design

2026-01-27 · M6 · 1 files · +239/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-meta.md` | 239 | 0 |

### `9a89d9b` — feat(cluster): implement cluster v2 with built-in Meta service

2026-01-27 · M6 · 36 files · +8,484/−146 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 237 | 11 |
| `Cargo.toml` | 9 | 3 |
| `build.rs` | 24 | 0 |
| `docs/architecture-cluster-v2.md` | 628 | 0 |
| `docs/architecture-meta.md` | 273 | 132 |
| `docs/guide-cluster-v2.md` | 869 | 0 |
| `proto/datanode.proto` | 149 | 0 |
| `proto/meta.proto` | 366 | 0 |
| `src/data/config.rs` | 54 | 0 |
| `src/data/mod.rs` | 18 | 0 |
| `src/data/server/grpc.rs` | 360 | 0 |
| `src/data/server/mod.rs` | 5 | 0 |
| `src/lib.rs` | 3 | 0 |
| `src/main.rs` | 223 | 0 |
| `src/meta/client/event_subscriber.rs` | 264 | 0 |
| `src/meta/client/meta_client.rs` | 753 | 0 |
| `src/meta/client/mod.rs` | 7 | 0 |
| `src/meta/config.rs` | 186 | 0 |
| `src/meta/mod.rs` | 43 | 0 |
| `src/meta/raft/log_store.rs` | 309 | 0 |
| `src/meta/raft/mod.rs` | 26 | 0 |
| `src/meta/raft/network.rs` | 103 | 0 |
| `src/meta/raft/state_machine.rs` | 667 | 0 |
| `src/meta/service/grpc.rs` | 736 | 0 |
| `src/meta/service/meta_node.rs` | 391 | 0 |
| `src/meta/service/mod.rs` | 7 | 0 |
| `src/meta/types.rs` | 436 | 0 |
| `src/router/client/data_node_pool.rs` | 152 | 0 |
| `src/router/client/mod.rs` | 7 | 0 |
| `src/router/config.rs` | 239 | 0 |
| `src/router/mod.rs` | 13 | 0 |
| `src/router/routing/cache_invalidation.rs` | 207 | 0 |
| `src/router/routing/mod.rs` | 13 | 0 |
| `src/router/routing/query_coordinator.rs` | 240 | 0 |
| `src/router/routing/shard_cache.rs` | 237 | 0 |
| `src/router/routing/write_router.rs` | 230 | 0 |

### `29ab9fe` — feat(dev): add development environment for cluster and standalone modes

2026-01-27 · M6 · 16 files · +804/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/.gitignore` | 9 | 0 |
| `dev/Procfile` | 20 | 0 |
| `dev/Procfile.cluster` | 19 | 0 |
| `dev/Procfile.standalone` | 11 | 0 |
| `dev/README.md` | 212 | 0 |
| `dev/clean.sh` | 23 | 0 |
| `dev/configs/data1.toml` | 63 | 0 |
| `dev/configs/data2.toml` | 63 | 0 |
| `dev/configs/meta1.toml` | 46 | 0 |
| `dev/configs/meta2.toml` | 36 | 0 |
| `dev/configs/meta3.toml` | 36 | 0 |
| `dev/configs/router1.toml` | 53 | 0 |
| `dev/configs/standalone.toml` | 59 | 0 |
| `dev/start-cluster.sh` | 75 | 0 |
| `dev/start-standalone.sh` | 55 | 0 |
| `dev/stop-all.sh` | 24 | 0 |

### `4e66a88` — fix(cluster): load role-specific configs from TOML files

2026-01-27 · M6 · 2 files · +18/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 12 | 0 |
| `src/main.rs` | 6 | 6 |

### `ccc88e1` — fix(query): empty results after flush due to stale table registration

2026-01-27 · M6 · 4 files · +74/−11 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 3 | 0 |
| `src/query/query_engine.rs` | 47 | 0 |
| `src/services/api.rs` | 4 | 6 |
| `src/storage/parquet_writer.rs` | 20 | 5 |

### `21bae77` — refactor(config): consolidate StorageConfig by eliminating wrapper

2026-01-27 · M6 · 4 files · +16/−53 text lines

| File | Added | Deleted |
|---|---:|---:|
| `config.toml` | 2 | 10 |
| `src/config.rs` | 7 | 36 |
| `src/main.rs` | 4 | 4 |
| `src/storage/storage.rs` | 3 | 3 |

### `1b4e85a` — refactor(cli): subcommand-based CLI and modular node runners

2026-01-27 · M6 · 20 files · +1,925/−1,013 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 1 | 0 |
| `src/jobs/retention_manager.rs` | 1 | 1 |
| `src/lib.rs` | 8 | 0 |
| `src/main.rs` | 69 | 996 |
| `src/node/common.rs` | 353 | 0 |
| `src/node/data.rs` | 121 | 0 |
| `src/node/meta.rs` | 93 | 0 |
| `src/node/mod.rs` | 13 | 0 |
| `src/node/router.rs` | 146 | 0 |
| `src/node/standalone.rs` | 508 | 0 |
| `src/router/api.rs` | 285 | 0 |
| `src/router/flight.rs` | 203 | 0 |
| `src/router/mod.rs` | 4 | 0 |
| `src/router/state.rs` | 103 | 0 |
| `src/services/api.rs` | 8 | 8 |
| `src/services/flight_service.rs` | 1 | 1 |
| `src/services/mysql_service.rs` | 1 | 1 |
| `src/storage/cache.rs` | 4 | 4 |
| `src/storage/parquet_writer.rs` | 2 | 2 |

### `8dae77e` — refactor(cluster): remove legacy etcd-based cluster module

2026-01-27 · M6 · 24 files · +6/−4,678 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 0 | 17 |
| `Cargo.toml` | 0 | 3 |
| `src/cluster/api/management.rs` | 0 | 133 |
| `src/cluster/api/mod.rs` | 0 | 13 |
| `src/cluster/api/status.rs` | 0 | 340 |
| `src/cluster/cache_invalidation.rs` | 0 | 461 |
| `src/cluster/config.rs` | 0 | 379 |
| `src/cluster/coordinator/etcd.rs` | 0 | 529 |
| `src/cluster/coordinator/interface.rs` | 0 | 263 |
| `src/cluster/coordinator/locks.rs` | 0 | 295 |
| `src/cluster/coordinator/mod.rs` | 0 | 35 |
| `src/cluster/coordinator/noop.rs` | 0 | 141 |
| `src/cluster/election.rs` | 0 | 322 |
| `src/cluster/metrics.rs` | 0 | 381 |
| `src/cluster/mod.rs` | 0 | 38 |
| `src/cluster/node/health.rs` | 0 | 366 |
| `src/cluster/node/identity.rs` | 0 | 275 |
| `src/cluster/node/mod.rs` | 0 | 12 |
| `src/cluster/node/registration.rs` | 0 | 247 |
| `src/cluster/write_coordinator.rs` | 0 | 376 |
| `src/config.rs` | 0 | 7 |
| `src/lib.rs` | 0 | 1 |
| `src/main.rs` | 0 | 1 |
| `src/node/standalone.rs` | 6 | 43 |

### `d706f88` — docs: add detailed write path architecture for cluster mode

2026-01-27 · M6 · 1 files · +433/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-cluster-write-path.md` | 433 | 0 |

### `9e0e95e` — feat(cluster): replace gRPC with Arrow Flight for Router↔Data communication

2026-01-27 · M6 · 12 files · +883/−588 text lines

| File | Added | Deleted |
|---|---:|---:|
| `build.rs` | 1 | 1 |
| `proto/datanode.proto` | 0 | 149 |
| `src/data/server/flight.rs` | 550 | 0 |
| `src/data/server/grpc.rs` | 0 | 360 |
| `src/data/server/mod.rs` | 7 | 2 |
| `src/meta/raft/mod.rs` | 2 | 0 |
| `src/node/data.rs` | 11 | 6 |
| `src/router/client/data_node_pool.rs` | 177 | 15 |
| `src/router/routing/query_coordinator.rs` | 17 | 9 |
| `src/router/routing/write_router.rs` | 70 | 44 |
| `tests/concurrency.rs` | 16 | 1 |
| `tests/data_integrity.rs` | 32 | 1 |

### `bd76c12` — feat(cluster): implement distributed query coordination and fencing tokens

2026-01-27 · M6 · 5 files · +323/−41 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 55 | 3 |
| `src/router/client/data_node_pool.rs` | 6 | 0 |
| `src/router/flight.rs` | 36 | 6 |
| `src/router/routing/query_coordinator.rs` | 209 | 26 |
| `src/router/routing/write_router.rs` | 17 | 6 |

### `594d1bb` — refactor(write-path): pass RecordBatch directly instead of re-parsing line protocol

2026-01-27 · M6 · 7 files · +709/−371 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 218 | 111 |
| `src/router/client/data_node_pool.rs` | 84 | 0 |
| `src/router/routing/shard_cache.rs` | 14 | 0 |
| `src/router/routing/write_router.rs` | 66 | 56 |
| `src/storage/batch_converter.rs` | 259 | 0 |
| `src/storage/cache.rs` | 67 | 204 |
| `src/storage/mod.rs` | 1 | 0 |

### `5cc9356` — feat(meta): implement OpenRaft traits for Meta HA

2026-01-27 · M6 · 5 files · +859/−138 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 1 | 1 |
| `src/meta/raft/log_store.rs` | 159 | 93 |
| `src/meta/raft/mod.rs` | 80 | 8 |
| `src/meta/raft/network.rs` | 170 | 34 |
| `src/meta/raft/state_machine.rs` | 449 | 2 |

### `43e2e10` — docs(meta): add Raft implementation architecture documentation

2026-01-27 · M6 · 1 files · +549/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-meta-raft.md` | 549 | 0 |

### `df20f08` — feat(meta): integrate Raft consensus into MetaNode

2026-01-27 · M6 · 3 files · +263/−83 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/service/grpc.rs` | 63 | 21 |
| `src/meta/service/meta_node.rs` | 196 | 59 |
| `src/node/meta.rs` | 4 | 3 |

### `9d1ae40` — docs(meta): update Raft architecture doc with integration status

2026-01-27 · M6 · 1 files · +14/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-meta-raft.md` | 14 | 13 |

### `584d406` — feat(meta): replace polling with broadcast for event subscription

2026-01-27 · M6 · 3 files · +101/−25 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine.rs` | 29 | 0 |
| `src/meta/service/grpc.rs` | 64 | 25 |
| `src/meta/service/meta_node.rs` | 8 | 0 |

### `43ef3df` — refactor(router): store node_id only in partition_owners (SOTA pattern)

2026-01-27 · M6 · 2 files · +82/−41 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/routing/cache_invalidation.rs` | 8 | 11 |
| `src/router/routing/shard_cache.rs` | 74 | 30 |

### `7ea031c` — feat(data): register DataNode with Meta service on startup

2026-01-27 · M6 · 1 files · +99/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/data.rs` | 99 | 0 |

### `d276acb` — docs: add comprehensive node registration architecture documentation

2026-01-27 · M6 · 1 files · +766/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-node-registration.md` | 766 | 0 |

### `50426fa` — feat(router): improve write safety during node failure detection

2026-01-27 · M6 · 10 files · +677/−113 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/data.rs` | 9 | 3 |
| `src/router/api.rs` | 1 | 0 |
| `src/router/client/circuit_breaker.rs` | 127 | 0 |
| `src/router/client/data_node_pool.rs` | 68 | 9 |
| `src/router/client/mod.rs` | 2 | 0 |
| `src/router/routing/cache_invalidation.rs` | 4 | 0 |
| `src/router/routing/health_checker.rs` | 175 | 0 |
| `src/router/routing/mod.rs` | 4 | 2 |
| `src/router/routing/shard_cache.rs` | 82 | 3 |
| `src/router/routing/write_router.rs` | 205 | 96 |

### `283c91c` — fix(dev): update Procfiles for new CLI subcommand syntax

2026-01-27 · M6 · 4 files · +20/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile` | 9 | 9 |
| `dev/Procfile.cluster` | 9 | 9 |
| `dev/Procfile.standalone` | 1 | 1 |
| `dev/start-standalone.sh` | 1 | 1 |

### `283a007` — feat(meta): add server-side leader forwarding for write requests

2026-01-27 · M6 · 4 files · +312/−9 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 14 | 0 |
| `src/meta/raft/network.rs` | 13 | 0 |
| `src/meta/service/grpc.rs` | 30 | 0 |
| `src/meta/service/meta_node.rs` | 255 | 9 |

### `43987d3` — chore: clean up TODO comments with proper documentation

2026-01-28 · M6 · 3 files · +27/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/standalone.rs` | 7 | 1 |
| `src/router/routing/query_coordinator.rs` | 5 | 2 |
| `src/router/routing/write_router.rs` | 15 | 10 |

### `05418bd` — rustfmt the projects

2026-01-28 · M6 · 32 files · +317/−277 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 0 | 1 |
| `src/data/config.rs` | 8 | 1 |
| `src/data/server/flight.rs` | 46 | 25 |
| `src/data/server/mod.rs` | 1 | 1 |
| `src/jobs/retention_manager.rs` | 1 | 1 |
| `src/main.rs` | 6 | 1 |
| `src/meta/client/event_subscriber.rs` | 3 | 6 |
| `src/meta/client/meta_client.rs` | 11 | 2 |
| `src/meta/raft/log_store.rs` | 2 | 9 |
| `src/meta/raft/mod.rs` | 13 | 2 |
| `src/meta/raft/network.rs` | 17 | 29 |
| `src/meta/raft/state_machine.rs` | 55 | 75 |
| `src/meta/service/grpc.rs` | 15 | 7 |
| `src/meta/service/meta_node.rs` | 14 | 18 |
| `src/meta/types.rs` | 26 | 8 |
| `src/node/common.rs` | 12 | 7 |
| `src/node/data.rs` | 6 | 6 |
| `src/node/meta.rs` | 1 | 4 |
| `src/node/router.rs` | 4 | 1 |
| `src/node/standalone.rs` | 9 | 14 |
| `src/query/query_engine.rs` | 1 | 4 |
| `src/router/api.rs` | 6 | 7 |
| `src/router/client/circuit_breaker.rs` | 3 | 1 |
| `src/router/client/data_node_pool.rs` | 16 | 13 |
| `src/router/config.rs` | 8 | 1 |
| `src/router/flight.rs` | 12 | 4 |
| `src/router/routing/cache_invalidation.rs` | 1 | 4 |
| `src/router/routing/health_checker.rs` | 1 | 5 |
| `src/router/routing/query_coordinator.rs` | 3 | 13 |
| `src/router/routing/shard_cache.rs` | 12 | 3 |
| `src/router/state.rs` | 3 | 2 |
| `src/storage/batch_converter.rs` | 1 | 2 |

### `df9e99c` — fix(query): empty results after background flush

2026-01-28 · M6 · 3 files · +109/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/common.rs` | 87 | 0 |
| `src/node/standalone.rs` | 9 | 6 |
| `src/query/cache/metadata_cache.rs` | 13 | 0 |

### `f3f1cf5` — feat(cluster): add warm standby replication for zero-downtime failover

2026-01-28 · M6 · 14 files · +2,561/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-warm-standby.md` | 356 | 0 |
| `docs/guide-warm-standby.md` | 434 | 0 |
| `src/data/config.rs` | 72 | 0 |
| `src/data/mod.rs` | 7 | 1 |
| `src/data/replication/mod.rs` | 55 | 0 |
| `src/data/replication/receiver.rs` | 294 | 0 |
| `src/data/replication/stream.rs` | 442 | 0 |
| `src/data/replication/types.rs` | 164 | 0 |
| `src/data/server/flight.rs` | 98 | 1 |
| `src/meta/raft/state_machine.rs` | 218 | 0 |
| `src/meta/types.rs` | 162 | 0 |
| `src/router/routing/cache_invalidation.rs` | 86 | 0 |
| `src/router/routing/shard_cache.rs` | 113 | 18 |
| `src/storage/cache.rs` | 60 | 0 |

### `73e1da3` — feat(k8s): add Kubernetes-friendly warm standby configuration

2026-01-28 · M6 · 14 files · +2,051/−84 text lines

| File | Added | Deleted |
|---|---:|---:|
| `deploy/kubernetes/README.md` | 72 | 0 |
| `deploy/kubernetes/configmap.yaml` | 75 | 74 |
| `deploy/kubernetes/data-statefulset.yaml` | 225 | 0 |
| `deploy/kubernetes/kustomization.yaml` | 49 | 0 |
| `deploy/kubernetes/meta-statefulset.yaml` | 175 | 0 |
| `deploy/kubernetes/router-deployment.yaml` | 161 | 0 |
| `deploy/kubernetes/secrets-template.yaml` | 30 | 0 |
| `src/config.rs` | 265 | 0 |
| `src/data/config.rs` | 131 | 9 |
| `src/meta/config.rs` | 123 | 0 |
| `src/meta/mod.rs` | 3 | 1 |
| `src/meta/raft/state_machine.rs` | 146 | 0 |
| `src/meta/replica_assigner.rs` | 488 | 0 |
| `src/meta/types.rs` | 108 | 0 |

### `4383b80` — feat(dev): add replica set cluster profile for warm standby testing

2026-01-28 · M6 · 10 files · +714/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.replicaset` | 26 | 0 |
| `dev/README.md` | 94 | 1 |
| `dev/configs/replicaset/data1.toml` | 78 | 0 |
| `dev/configs/replicaset/data2.toml` | 78 | 0 |
| `dev/configs/replicaset/data3.toml` | 78 | 0 |
| `dev/configs/replicaset/meta1.toml` | 69 | 0 |
| `dev/configs/replicaset/meta2.toml` | 69 | 0 |
| `dev/configs/replicaset/meta3.toml` | 69 | 0 |
| `dev/configs/replicaset/router1.toml` | 53 | 0 |
| `dev/start-replicaset.sh` | 100 | 0 |

### `29736c4` — fix(cluster): router initialization and Meta auto-bootstrap

2026-01-28 · M6 · 4 files · +266/−44 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/data.rs` | 55 | 28 |
| `src/node/meta.rs` | 69 | 0 |
| `src/node/router.rs` | 17 | 2 |
| `src/router/routing/cache_invalidation.rs` | 125 | 14 |

### `c6ed3b8` — docs: add cluster robustness testing guide

2026-01-28 · M6 · 1 files · +701/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/guide-cluster-robustness.md` | 701 | 0 |

### `10b51f3` — chore: reduce Raft RPC failure log verbosity

2026-01-28 · M6 · 1 files · +4/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/network.rs` | 4 | 4 |

### `912463a` — feat(cluster): node-first path structure for distributed parallel queries

2026-01-28 · M6 · 17 files · +1,650/−83 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-node-first-path.md` | 287 | 0 |
| `plans/adaptive-dancing-summit.md` | 329 | 0 |
| `src/config.rs` | 11 | 0 |
| `src/data/cache/mod.rs` | 8 | 0 |
| `src/data/cache/parquet_cache.rs` | 373 | 0 |
| `src/data/mod.rs` | 3 | 0 |
| `src/data/server/flight.rs` | 33 | 5 |
| `src/node/common.rs` | 92 | 7 |
| `src/node/data.rs` | 30 | 17 |
| `src/node/standalone.rs` | 2 | 2 |
| `src/query/catalog/manager.rs` | 5 | 0 |
| `src/query/catalog/memory_manager.rs` | 188 | 0 |
| `src/router/client/data_node_pool.rs` | 54 | 0 |
| `src/router/routing/query_coordinator.rs` | 173 | 28 |
| `src/router/state.rs` | 16 | 7 |
| `src/storage/cache.rs` | 19 | 5 |
| `src/storage/parquet_writer.rs` | 27 | 12 |

### `8ef6546` — fix(query): use node_id prefix for table paths in cluster mode

2026-01-28 · M6 · 8 files · +59/−25 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/data1.toml` | 7 | 4 |
| `dev/configs/data2.toml` | 7 | 4 |
| `dev/configs/replicaset/data1.toml` | 7 | 4 |
| `dev/configs/replicaset/data2.toml` | 7 | 4 |
| `dev/configs/replicaset/data3.toml` | 7 | 4 |
| `dev/configs/standalone.toml` | 7 | 4 |
| `src/query/query_engine.rs` | 11 | 1 |
| `src/storage/storage.rs` | 6 | 0 |

### `94b7c6b` — fix(flight): handle Utf8View/BinaryView types in Arrow schema conversion

2026-01-28 · M6 · 3 files · +25/−5 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.replicaset.debug` | 10 | 0 |
| `dev/start-replicaset.sh` | 12 | 4 |
| `src/data/server/flight.rs` | 3 | 1 |

### `fd6d751` — refactor(json): extract Arrow-to-JSON conversion to shared utils

2026-01-28 · M6 · 5 files · +165/−112 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/lib.rs` | 1 | 0 |
| `src/query/query_engine.rs` | 2 | 105 |
| `src/router/api.rs` | 17 | 7 |
| `src/utils/json.rs` | 142 | 0 |
| `src/utils/mod.rs` | 3 | 0 |

### `29ec2a1` — feat(router): add /stats and /flush endpoints

2026-01-28 · M6 · 4 files · +90/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/main.rs` | 1 | 0 |
| `src/router/api.rs` | 50 | 3 |
| `src/router/client/data_node_pool.rs` | 7 | 0 |
| `src/router/routing/write_router.rs` | 32 | 0 |

### `d4875ae` — feat(data): add HTTP API for data node monitoring

2026-01-29 · M6 · 6 files · +162/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/cache/mod.rs` | 5 | 0 |
| `src/data/config.rs` | 9 | 0 |
| `src/data/replication/mod.rs` | 5 | 0 |
| `src/data/server/api.rs` | 119 | 0 |
| `src/data/server/mod.rs` | 4 | 0 |
| `src/node/data.rs` | 20 | 1 |

### `67f8df1` — feat(data): integrate ParquetLocalCache into data node pipeline

2026-01-29 · M6 · 6 files · +173/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/cache/mod.rs` | 1 | 6 |
| `src/data/server/flight.rs` | 35 | 6 |
| `src/node/common.rs` | 20 | 2 |
| `src/node/data.rs` | 47 | 3 |
| `src/query/providers/table_provider.rs` | 50 | 2 |
| `src/query/query_engine.rs` | 20 | 1 |

### `e0df92d` — feat(replication): partition-aware replica management system

2026-01-29 · M6 · 11 files · +1,149/−18 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 85 | 0 |
| `src/data/replication/mod.rs` | 2 | 5 |
| `src/data/replication/stream.rs` | 44 | 1 |
| `src/data/replication/types.rs` | 19 | 0 |
| `src/data/server/flight.rs` | 172 | 0 |
| `src/meta/client/meta_client.rs` | 176 | 0 |
| `src/meta/service/grpc.rs` | 138 | 0 |
| `src/meta/service/meta_node.rs` | 249 | 0 |
| `src/node/data.rs` | 156 | 1 |
| `src/router/routing/write_router.rs` | 94 | 4 |
| `src/router/state.rs` | 14 | 7 |

### `8c58aa4` — feat(meta): implement Cold Standby Architecture for avalanche prevention

2026-01-29 · M6 · 13 files · +2,758/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-cold-standby.md` | 788 | 0 |
| `docs/guide-cold-standby.md` | 787 | 0 |
| `proto/meta.proto` | 52 | 0 |
| `src/meta/client/meta_client.rs` | 20 | 0 |
| `src/meta/config.rs` | 171 | 0 |
| `src/meta/raft/state_machine.rs` | 288 | 1 |
| `src/meta/replica_assigner.rs` | 221 | 8 |
| `src/meta/service/grpc.rs` | 81 | 0 |
| `src/meta/service/meta_node.rs` | 101 | 0 |
| `src/meta/types.rs` | 113 | 0 |
| `src/node/data.rs` | 1 | 0 |
| `src/router/routing/cache_invalidation.rs` | 34 | 1 |
| `src/router/routing/shard_cache.rs` | 101 | 5 |

### `111e5ca` — feat: add production hardening features

2026-01-29 · M6 · 8 files · +786/−64 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/replication/receiver.rs` | 101 | 2 |
| `src/data/replication/stream.rs` | 32 | 34 |
| `src/data/replication/types.rs` | 166 | 0 |
| `src/data/server/flight.rs` | 71 | 5 |
| `src/meta/client/meta_client.rs` | 83 | 3 |
| `src/node/standalone.rs` | 1 | 0 |
| `src/router/client/circuit_breaker.rs` | 219 | 20 |
| `src/services/api.rs` | 113 | 0 |

### `f4bd39a` — feat(cluster): implement scale-in for object store clusters

2026-01-29 · M6 · 18 files · +3,346/−18 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-scale-in.md` | 441 | 0 |
| `docs/guide-scale-in.md` | 607 | 0 |
| `proto/meta.proto` | 103 | 1 |
| `src/data/server/api.rs` | 264 | 5 |
| `src/meta/client/meta_client.rs` | 189 | 0 |
| `src/meta/config.rs` | 9 | 0 |
| `src/meta/raft/state_machine.rs` | 518 | 0 |
| `src/meta/service/grpc.rs` | 173 | 0 |
| `src/meta/service/http.rs` | 419 | 0 |
| `src/meta/service/meta_node.rs` | 266 | 0 |
| `src/meta/service/mod.rs` | 3 | 1 |
| `src/meta/types.rs` | 263 | 0 |
| `src/node/data.rs` | 5 | 1 |
| `src/node/meta.rs` | 19 | 0 |
| `src/router/api.rs` | 2 | 0 |
| `src/router/routing/cache_invalidation.rs` | 22 | 3 |
| `src/router/routing/query_coordinator.rs` | 4 | 3 |
| `src/router/routing/shard_cache.rs` | 39 | 4 |

### `154736a` — fix(meta): bincode serialization and state machine sharing fixes

2026-01-29 · M6 · 26 files · +2,084/−100 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.coldstandby` | 36 | 0 |
| `dev/Procfile.coldstandby-simple` | 9 | 0 |
| `dev/configs/coldstandby-simple/data1.toml` | 44 | 0 |
| `dev/configs/coldstandby-simple/data2.toml` | 44 | 0 |
| `dev/configs/coldstandby-simple/data3.toml` | 44 | 0 |
| `dev/configs/coldstandby-simple/data4.toml` | 44 | 0 |
| `dev/configs/coldstandby-simple/meta1.toml` | 41 | 0 |
| `dev/configs/coldstandby-simple/router1.toml` | 44 | 0 |
| `dev/configs/coldstandby/data1.toml` | 80 | 0 |
| `dev/configs/coldstandby/data2.toml` | 76 | 0 |
| `dev/configs/coldstandby/data3.toml` | 76 | 0 |
| `dev/configs/coldstandby/data4.toml` | 76 | 0 |
| `dev/configs/coldstandby/data5.toml` | 76 | 0 |
| `dev/configs/coldstandby/data6.toml` | 76 | 0 |
| `dev/configs/coldstandby/meta1.toml` | 57 | 0 |
| `dev/configs/coldstandby/meta2.toml` | 44 | 0 |
| `dev/configs/coldstandby/meta3.toml` | 44 | 0 |
| `dev/configs/coldstandby/router1.toml` | 51 | 0 |
| `dev/start-coldstandby.sh` | 149 | 0 |
| `dev/test-coldstandby.sh` | 340 | 0 |
| `plans/cold-standby-cluster-plan.md` | 402 | 0 |
| `src/meta/raft/log_store.rs` | 150 | 78 |
| `src/meta/raft/state_machine.rs` | 63 | 9 |
| `src/meta/service/meta_node.rs` | 4 | 4 |
| `src/meta/types.rs` | 6 | 2 |
| `src/node/meta.rs` | 8 | 7 |

### `a3b920d` — feat(meta): add HTTP endpoints and sled inspect tool

2026-01-29 · M6 · 8 files · +716/−12 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/coldstandby-simple/data1.toml` | 1 | 0 |
| `dev/configs/coldstandby-simple/data2.toml` | 1 | 0 |
| `dev/configs/coldstandby-simple/data3.toml` | 1 | 0 |
| `dev/configs/coldstandby-simple/data4.toml` | 1 | 0 |
| `src/bin/sled_inspect.rs` | 144 | 0 |
| `src/meta/raft/state_machine.rs` | 6 | 0 |
| `src/meta/service/http.rs` | 253 | 2 |
| `src/meta/service/meta_node.rs` | 309 | 10 |

### `34f3bf3` — update profile configs

2026-01-29 · M6 · 19 files · +335/−355 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile` | 7 | 8 |
| `dev/Procfile.cluster` | 6 | 6 |
| `dev/Procfile.replicaset` | 7 | 7 |
| `dev/Procfile.replicaset.debug` | 0 | 10 |
| `dev/Procfile.standalone` | 1 | 1 |
| `dev/configs/cluster/data1.toml` | 61 | 0 |
| `dev/configs/cluster/data2.toml` | 61 | 0 |
| `dev/configs/cluster/meta1.toml` | 48 | 0 |
| `dev/configs/cluster/meta2.toml` | 38 | 0 |
| `dev/configs/cluster/meta3.toml` | 38 | 0 |
| `dev/configs/cluster/router1.toml` | 48 | 0 |
| `dev/configs/data1.toml` | 0 | 66 |
| `dev/configs/data2.toml` | 0 | 66 |
| `dev/configs/meta1.toml` | 0 | 46 |
| `dev/configs/meta2.toml` | 0 | 36 |
| `dev/configs/meta3.toml` | 0 | 36 |
| `dev/configs/router1.toml` | 0 | 53 |
| `dev/start-cluster.sh` | 15 | 15 |
| `dev/start-standalone.sh` | 5 | 5 |

## M7 — Recovery, standby redesign, and safe scale-in

51 commits.

### `c49db06` — fix bug: standalone metric handler

2026-01-30 · M7 · 1 files · +0/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/standalone.rs` | 0 | 13 |

### `5ab59ba` — rust format

2026-01-30 · M7 · 33 files · +548/−329 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/bin/sled_inspect.rs` | 11 | 4 |
| `src/config.rs` | 4 | 10 |
| `src/data/cache/parquet_cache.rs` | 10 | 4 |
| `src/data/config.rs` | 25 | 7 |
| `src/data/replication/mod.rs` | 3 | 1 |
| `src/data/replication/receiver.rs` | 29 | 11 |
| `src/data/replication/stream.rs` | 12 | 6 |
| `src/data/server/flight.rs` | 47 | 24 |
| `src/meta/client/meta_client.rs` | 5 | 21 |
| `src/meta/raft/log_store.rs` | 39 | 21 |
| `src/meta/raft/state_machine.rs` | 67 | 25 |
| `src/meta/replica_assigner.rs` | 44 | 31 |
| `src/meta/service/grpc.rs` | 1 | 1 |
| `src/meta/service/http.rs` | 34 | 22 |
| `src/meta/service/meta_node.rs` | 23 | 21 |
| `src/meta/types.rs` | 0 | 2 |
| `src/node/common.rs` | 4 | 8 |
| `src/node/data.rs` | 72 | 55 |
| `src/node/meta.rs` | 2 | 6 |
| `src/node/standalone.rs` | 6 | 1 |
| `src/query/catalog/memory_manager.rs` | 7 | 2 |
| `src/query/providers/table_provider.rs` | 4 | 1 |
| `src/query/query_engine.rs` | 6 | 1 |
| `src/router/client/circuit_breaker.rs` | 1 | 2 |
| `src/router/routing/cache_invalidation.rs` | 6 | 9 |
| `src/router/routing/query_coordinator.rs` | 1 | 2 |
| `src/router/routing/shard_cache.rs` | 7 | 2 |
| `src/router/routing/write_router.rs` | 5 | 1 |
| `src/services/api.rs` | 5 | 5 |
| `src/storage/cache.rs` | 19 | 3 |
| `src/storage/parquet_writer.rs` | 26 | 4 |
| `src/storage/storage.rs` | 11 | 2 |
| `src/utils/json.rs` | 12 | 14 |

### `7682d20` — refactor: extract shared API handlers to eliminate code duplication

2026-01-31 · M7 · 7 files · +1,522/−572 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/start-standalone.sh` | 1 | 1 |
| `src/data/server/api.rs` | 766 | 114 |
| `src/node/data.rs` | 11 | 1 |
| `src/router/api.rs` | 18 | 2 |
| `src/services/api.rs` | 112 | 454 |
| `src/services/mod.rs` | 1 | 0 |
| `src/services/shared.rs` | 613 | 0 |

### `9614008` — refactor: reorganize dev configs with standardized port conventions

2026-01-31 · M7 · 35 files · +248/−215 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.cluster` | 6 | 6 |
| `dev/Procfile.standalone` | 1 | 1 |
| `dev/configs/cluster/data1.toml` | 6 | 4 |
| `dev/configs/cluster/data2.toml` | 6 | 4 |
| `dev/configs/cluster/meta1.toml` | 4 | 4 |
| `dev/configs/cluster/meta2.toml` | 4 | 4 |
| `dev/configs/cluster/meta3.toml` | 4 | 4 |
| `dev/configs/cluster/router1.toml` | 1 | 1 |
| `dev/configs/coldstandby-simple/data1.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/data2.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/data3.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/data4.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/meta1.toml` | 3 | 3 |
| `dev/configs/coldstandby-simple/router1.toml` | 1 | 1 |
| `dev/configs/coldstandby/data1.toml` | 5 | 9 |
| `dev/configs/coldstandby/data2.toml` | 6 | 9 |
| `dev/configs/coldstandby/data3.toml` | 6 | 9 |
| `dev/configs/coldstandby/data4.toml` | 6 | 9 |
| `dev/configs/coldstandby/data5.toml` | 6 | 9 |
| `dev/configs/coldstandby/data6.toml` | 6 | 9 |
| `dev/configs/coldstandby/meta1.toml` | 4 | 4 |
| `dev/configs/coldstandby/meta2.toml` | 4 | 4 |
| `dev/configs/coldstandby/meta3.toml` | 4 | 4 |
| `dev/configs/coldstandby/router1.toml` | 1 | 1 |
| `dev/configs/replicaset/data1.toml` | 5 | 9 |
| `dev/configs/replicaset/data2.toml` | 5 | 9 |
| `dev/configs/replicaset/data3.toml` | 5 | 9 |
| `dev/configs/replicaset/meta1.toml` | 5 | 3 |
| `dev/configs/replicaset/meta2.toml` | 5 | 3 |
| `dev/configs/replicaset/meta3.toml` | 5 | 3 |
| `dev/configs/replicaset/router1.toml` | 1 | 1 |
| `dev/configs/standalone.toml` | 0 | 62 |
| `dev/configs/standalone/local.toml` | 53 | 0 |
| `dev/configs/standalone/minio.toml` | 59 | 0 |
| `dev/start-standalone.sh` | 1 | 1 |

### `691ed07` — fix: query engine now uses node_id prefix for local storage

2026-01-31 · M7 · 4 files · +44/−23 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/api.rs` | 5 | 2 |
| `src/node/standalone.rs` | 4 | 1 |
| `src/query/query_engine.rs` | 32 | 20 |
| `src/services/api.rs` | 3 | 0 |

### `30c2c25` — fix: separate meta gRPC and HTTP ports to avoid conflict

2026-01-31 · M7 · 3 files · +3/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/cluster/meta1.toml` | 1 | 1 |
| `dev/configs/cluster/meta2.toml` | 1 | 1 |
| `dev/configs/cluster/meta3.toml` | 1 | 1 |

### `64d7bf7` — test(raft): add comprehensive unit tests for log_store and state_machine

2026-01-31 · M7 · 2 files · +1,073/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/log_store.rs` | 400 | 0 |
| `src/meta/raft/state_machine.rs` | 673 | 0 |

### `f58151e` — refactor: standardize cluster port conventions with base+offset pattern

2026-01-31 · M7 · 29 files · +359/−158 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile` | 7 | 7 |
| `dev/Procfile.coldstandby` | 11 | 11 |
| `dev/configs/cluster/data1.toml` | 5 | 6 |
| `dev/configs/cluster/data2.toml` | 5 | 6 |
| `dev/configs/cluster/meta1.toml` | 8 | 7 |
| `dev/configs/cluster/meta2.toml` | 8 | 7 |
| `dev/configs/cluster/meta3.toml` | 8 | 7 |
| `dev/configs/cluster/router1.toml` | 2 | 3 |
| `dev/configs/coldstandby-simple/data1.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/data2.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/data3.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/data4.toml` | 5 | 4 |
| `dev/configs/coldstandby-simple/meta1.toml` | 6 | 3 |
| `dev/configs/coldstandby-simple/router1.toml` | 2 | 2 |
| `dev/configs/coldstandby/data1.toml` | 5 | 5 |
| `dev/configs/coldstandby/data2.toml` | 5 | 4 |
| `dev/configs/coldstandby/data3.toml` | 5 | 4 |
| `dev/configs/coldstandby/data4.toml` | 5 | 4 |
| `dev/configs/coldstandby/data5.toml` | 5 | 4 |
| `dev/configs/coldstandby/data6.toml` | 5 | 4 |
| `dev/configs/coldstandby/meta1.toml` | 8 | 8 |
| `dev/configs/coldstandby/meta2.toml` | 7 | 4 |
| `dev/configs/coldstandby/meta3.toml` | 7 | 4 |
| `dev/configs/coldstandby/router1.toml` | 2 | 3 |
| `dev/start-cluster.sh` | 13 | 7 |
| `dev/start-coldstandby.sh` | 15 | 15 |
| `docs/guide-cluster-port-conventions.md` | 187 | 0 |
| `src/data/config.rs` | 7 | 16 |
| `src/router/config.rs` | 1 | 1 |

### `463373b` — fix: resolve heartbeat bug where node leases expired after 10s

2026-01-31 · M7 · 30 files · +193/−105 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/cache/mod.rs` | 1 | 1 |
| `src/data/mod.rs` | 0 | 10 |
| `src/data/replication/mod.rs` | 3 | 7 |
| `src/data/replication/receiver.rs` | 1 | 1 |
| `src/data/replication/stream.rs` | 1 | 1 |
| `src/data/server/flight.rs` | 1 | 1 |
| `src/data/server/mod.rs` | 0 | 3 |
| `src/meta/client/event_subscriber.rs` | 1 | 3 |
| `src/meta/client/meta_client.rs` | 24 | 2 |
| `src/meta/client/mod.rs` | 1 | 1 |
| `src/meta/mod.rs` | 0 | 2 |
| `src/meta/raft/log_store.rs` | 17 | 8 |
| `src/meta/raft/mod.rs` | 2 | 2 |
| `src/meta/raft/network.rs` | 1 | 1 |
| `src/meta/raft/state_machine.rs` | 108 | 33 |
| `src/meta/service/grpc.rs` | 1 | 1 |
| `src/meta/service/mod.rs` | 0 | 2 |
| `src/node/data.rs` | 7 | 1 |
| `src/node/standalone.rs` | 0 | 1 |
| `src/query/query_engine.rs` | 11 | 8 |
| `src/router/client/data_node_pool.rs` | 2 | 3 |
| `src/router/client/mod.rs` | 0 | 1 |
| `src/router/flight.rs` | 0 | 1 |
| `src/router/mod.rs` | 0 | 3 |
| `src/router/routing/cache_invalidation.rs` | 1 | 1 |
| `src/router/routing/health_checker.rs` | 0 | 1 |
| `src/router/routing/mod.rs` | 0 | 1 |
| `src/router/routing/query_coordinator.rs` | 0 | 1 |
| `src/router/state.rs` | 8 | 2 |
| `src/storage/parquet_writer.rs` | 2 | 2 |

### `503c1e2` — feat: add local and minio modes to standalone development setup

2026-01-31 · M7 · 4 files · +71/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.standalone` | 3 | 1 |
| `dev/Procfile.standalone-local` | 11 | 0 |
| `dev/Procfile.standalone-minio` | 15 | 0 |
| `dev/start-standalone.sh` | 42 | 3 |

### `4815bfd` — fix: correct /tables/:db filter and skip rebuild in dev scripts

2026-01-31 · M7 · 5 files · +67/−40 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/start-cluster.sh` | 10 | 8 |
| `dev/start-coldstandby.sh` | 23 | 11 |
| `dev/start-replicaset.sh` | 23 | 12 |
| `dev/start-standalone.sh` | 10 | 8 |
| `src/services/shared.rs` | 1 | 1 |

### `6b35117` — fix: /tables/:db API now works with object storage and in-memory data

2026-01-31 · M7 · 2 files · +156/−46 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/query_engine.rs` | 136 | 46 |
| `src/storage/cache.rs` | 20 | 0 |

### `ec5e8c7` — fix: /tables/:db API and queries now work correctly with MinIO

2026-01-31 · M7 · 2 files · +160/−27 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/query_engine.rs` | 156 | 17 |
| `src/services/shared.rs` | 4 | 10 |

### `1526cb2` — chore: update cluster bucket name to mynadb-cluster for testing

2026-01-31 · M7 · 2 files · +2/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/cluster/data1.toml` | 1 | 1 |
| `dev/configs/cluster/data2.toml` | 1 | 1 |

### `e01884a` — fix: refresh query engine tables after flush in cluster mode

2026-01-31 · M7 · 2 files · +119/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/common.rs` | 99 | 0 |
| `src/node/data.rs` | 20 | 15 |

### `cd11ddb` — fix: refresh tables after Flight FLUSH action in cluster mode

2026-01-31 · M7 · 1 files · +27/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 27 | 1 |

### `01ee151` — fix: cluster mode write distribution and node registration

2026-01-31 · M7 · 4 files · +141/−24 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine.rs` | 6 | 5 |
| `src/node/router.rs` | 21 | 1 |
| `src/router/routing/cache_invalidation.rs` | 78 | 7 |
| `src/router/routing/shard_cache.rs` | 36 | 11 |

### `c4958fb` — chore: use distinct bucket names for each dev environment

2026-01-31 · M7 · 7 files · +7/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/coldstandby-simple/data1.toml` | 1 | 1 |
| `dev/configs/coldstandby-simple/data2.toml` | 1 | 1 |
| `dev/configs/coldstandby-simple/data3.toml` | 1 | 1 |
| `dev/configs/replicaset/data1.toml` | 1 | 1 |
| `dev/configs/replicaset/data2.toml` | 1 | 1 |
| `dev/configs/replicaset/data3.toml` | 1 | 1 |
| `dev/configs/standalone/minio.toml` | 1 | 1 |

### `e5b33ef` — feat: add schema/tables, memory, and partition APIs to Router node

2026-01-31 · M7 · 5 files · +599/−12 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 144 | 0 |
| `src/node/data.rs` | 11 | 3 |
| `src/router/api.rs` | 353 | 9 |
| `src/router/client/data_node_pool.rs` | 79 | 0 |
| `src/router/routing/shard_cache.rs` | 12 | 0 |

### `f036b2f` — fix: use colon syntax for path parameters in data node API routes

2026-01-31 · M7 · 1 files · +20/−20 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/api.rs` | 20 | 20 |

### `ab28b6b` — fix: Router API endpoints for schemas, partitions, and table schema

2026-01-31 · M7 · 17 files · +455/−695 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.replicaset` | 0 | 26 |
| `dev/configs/replicaset/data1.toml` | 0 | 77 |
| `dev/configs/replicaset/data2.toml` | 0 | 77 |
| `dev/configs/replicaset/data3.toml` | 0 | 77 |
| `dev/configs/replicaset/meta1.toml` | 0 | 71 |
| `dev/configs/replicaset/meta2.toml` | 0 | 71 |
| `dev/configs/replicaset/meta3.toml` | 0 | 71 |
| `dev/configs/replicaset/router1.toml` | 0 | 53 |
| `dev/start-coldstandby-simple.sh` | 154 | 0 |
| `dev/start-replicaset.sh` | 0 | 119 |
| `src/data/server/flight.rs` | 4 | 2 |
| `src/jobs/database_manager.rs` | 82 | 2 |
| `src/node/data.rs` | 7 | 4 |
| `src/query/query_engine.rs` | 95 | 26 |
| `src/router/api.rs` | 21 | 17 |
| `src/services/shared.rs` | 51 | 2 |
| `src/storage/storage.rs` | 41 | 0 |

### `233dcc7` — refactor: simplify API endpoint paths for schema/table operations

2026-01-31 · M7 · 4 files · +134/−138 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/api.rs` | 12 | 16 |
| `src/node/standalone.rs` | 1 | 96 |
| `src/router/api.rs` | 12 | 14 |
| `src/services/api.rs` | 109 | 12 |

### `b142e4a` — fix: use actual StandbyConfig for cold standby mode assignment

2026-01-31 · M7 · 3 files · +1,020/−253 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/test-coldstandby.sh` | 1000 | 236 |
| `src/meta/raft/state_machine.rs` | 15 | 12 |
| `src/meta/service/meta_node.rs` | 5 | 5 |

### `d0dde38` — fix: route traffic only to Primary data nodes in Cold Standby mode

2026-01-31 · M7 · 4 files · +68/−28 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/api.rs` | 25 | 17 |
| `src/router/routing/cache_invalidation.rs` | 37 | 6 |
| `src/router/routing/query_coordinator.rs` | 3 | 3 |
| `src/router/routing/write_router.rs` | 3 | 2 |

### `df25f16` — refactor: consolidate DataNodeMode enum to meta/types.rs

2026-01-31 · M7 · 3 files · +7/−25 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/api.rs` | 2 | 1 |
| `src/router/routing/cache_invalidation.rs` | 4 | 14 |
| `src/router/routing/shard_cache.rs` | 1 | 10 |

### `f9cc010` — fix: Phase 1 scale-in improvements - reject writes on retiring nodes

2026-01-31 · M7 · 3 files · +49/−10 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 30 | 0 |
| `src/node/data.rs` | 8 | 2 |
| `src/router/routing/shard_cache.rs` | 11 | 8 |

### `71bc2ca` — fix: correct peer indexing in meta node and preserve node info in cache events

2026-01-31 · M7 · 2 files · +44/−19 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/service/meta_node.rs` | 10 | 5 |
| `src/router/routing/cache_invalidation.rs` | 34 | 14 |

### `f18b528` — feat: add in-flight query tracking for graceful scale-in (Phase 2)

2026-01-31 · M7 · 3 files · +81/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/api.rs` | 21 | 1 |
| `src/data/server/flight.rs` | 53 | 0 |
| `src/node/data.rs` | 7 | 1 |

### `f3470c4` — feat: add forced retirement timeout and scale-in tests (Phase 3)

2026-01-31 · M7 · 2 files · +310/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/test_scale_in.sh` | 213 | 0 |
| `src/data/server/api.rs` | 97 | 0 |

### `b16023b` — feat: add query fault tolerance with file redistribution

2026-01-31 · M7 · 2 files · +64/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/client/data_node_pool.rs` | 14 | 0 |
| `src/router/routing/query_coordinator.rs` | 50 | 2 |

### `a212133` — feat: add query retry on connection failure and fault tolerance test

2026-01-31 · M7 · 2 files · +297/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/test_query_fault_tolerance.sh` | 236 | 0 |
| `src/router/routing/query_coordinator.rs` | 61 | 4 |

### `360f2f0` — feat: add continuous aggregates module (pluggable, isolated)

2026-02-01 · M7 · 13 files · +3,767/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `CLAUDE.md` | 15 | 0 |
| `docs/architecture-continuous-aggregates.md` | 446 | 0 |
| `docs/guide-continuous-aggregates.md` | 525 | 0 |
| `src/aggregates/config.rs` | 220 | 0 |
| `src/aggregates/definition.rs` | 687 | 0 |
| `src/aggregates/job.rs` | 358 | 0 |
| `src/aggregates/mod.rs` | 219 | 0 |
| `src/aggregates/query_rewriter.rs` | 542 | 0 |
| `src/aggregates/storage.rs` | 401 | 0 |
| `src/aggregates/types.rs` | 348 | 0 |
| `src/config.rs` | 4 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 1 | 0 |

### `6820a5f` — feat: add immediate flush on retire and graceful shutdown

2026-02-01 · M7 · 4 files · +159/−11 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/api.rs` | 48 | 7 |
| `src/main.rs` | 25 | 3 |
| `src/node/data.rs` | 26 | 0 |
| `src/node/standalone.rs` | 60 | 1 |

### `ff7cbf7` — feat(meta): add scale-in module with ratio-preserving guarantee

2026-02-01 · M7 · 11 files · +4,340/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-cluster-scale-in.md` | 325 | 0 |
| `docs/guide-cluster-scale-in.md` | 446 | 0 |
| `scripts/test_scale_in_meta.sh` | 359 | 0 |
| `src/meta/mod.rs` | 1 | 0 |
| `src/meta/scale_in/executor.rs` | 408 | 0 |
| `src/meta/scale_in/mod.rs` | 245 | 0 |
| `src/meta/scale_in/planner.rs` | 751 | 0 |
| `src/meta/scale_in/types.rs` | 778 | 0 |
| `src/meta/scale_in/validator.rs` | 642 | 0 |
| `src/meta/service/http.rs` | 326 | 1 |
| `src/meta/service/meta_node.rs` | 59 | 0 |

### `9fa8937` — feat(meta): add scale-in fault tolerance with health checks

2026-02-01 · M7 · 3 files · +388/−17 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/scale_in/executor.rs` | 215 | 15 |
| `src/meta/scale_in/mod.rs` | 103 | 0 |
| `src/meta/scale_in/types.rs` | 70 | 2 |

### `35b0b31` — feat(meta): add node failure recovery with no-promotion strategy

2026-02-02 · M7 · 7 files · +1,595/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 148 | 0 |
| `src/meta/mod.rs` | 1 | 0 |
| `src/meta/raft/state_machine.rs` | 226 | 1 |
| `src/meta/recovery/mod.rs` | 753 | 0 |
| `src/meta/recovery/types.rs` | 359 | 0 |
| `src/meta/types.rs` | 91 | 0 |
| `src/router/routing/cache_invalidation.rs` | 17 | 0 |

### `c83c35b` — feat(meta): add flexible standby assignment for fault tolerance

2026-02-02 · M7 · 3 files · +479/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 134 | 0 |
| `src/meta/recovery/mod.rs` | 204 | 2 |
| `src/meta/recovery/types.rs` | 141 | 0 |

### `1ce9fa7` — refactor(meta): consolidate standby configs and remove promotion

2026-02-02 · M7 · 3 files · +189/−215 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 129 | 146 |
| `src/meta/raft/state_machine.rs` | 15 | 31 |
| `src/meta/recovery/mod.rs` | 45 | 38 |

### `b3c9cf8` — feat(meta): consolidate scale-in/scale-out ratio calculation

2026-02-02 · M7 · 2 files · +219/−23 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 214 | 21 |
| `src/meta/scale_in/types.rs` | 5 | 2 |

### `70f0f38` — refactor(meta): remove auto_assign_mode and add min_primaries_for_spare config

2026-02-02 · M7 · 2 files · +44/−39 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 35 | 25 |
| `src/meta/raft/state_machine.rs` | 9 | 14 |

### `12bcf20` — fix(test): remove no-op assertions with || true

2026-02-02 · M7 · 3 files · +10/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 1 |
| `Cargo.toml` | 1 | 1 |
| `src/meta/raft/state_machine.rs` | 8 | 5 |

### `9d0fa63` — fix(meta): preserve data node mode on re-registration

2026-02-02 · M7 · 1 files · +74/−9 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine.rs` | 74 | 9 |

### `864e76f` — fix(recovery): Data node subscribes to recovery events for flush trigger

2026-02-02 · M7 · 4 files · +55/−49 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine.rs` | 1 | 16 |
| `src/meta/types.rs` | 0 | 7 |
| `src/node/data.rs` | 51 | 7 |
| `src/router/routing/cache_invalidation.rs` | 3 | 19 |

### `78497ff` — feat(replication): add Flush Checkpoint Protocol for Standby cache sync

2026-02-02 · M7 · 8 files · +1,563/−68 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-flush-checkpoint.md` | 270 | 0 |
| `src/data/replication/receiver.rs` | 362 | 4 |
| `src/data/replication/stream.rs` | 139 | 2 |
| `src/data/replication/types.rs` | 210 | 0 |
| `src/data/server/flight.rs` | 43 | 2 |
| `src/node/common.rs` | 120 | 0 |
| `src/node/data.rs` | 59 | 37 |
| `src/storage/cache.rs` | 360 | 23 |

### `be71316` — feat: add distributed locks for compaction in cluster mode

2026-02-02 · M7 · 1 files · +131/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor.rs` | 131 | 0 |

### `3aa0963` — fix: rotate WAL before flush to prevent data loss during concurrent writes

2026-02-02 · M7 · 1 files · +43/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/cache.rs` | 43 | 6 |

### `341341d` — feat: implement node-level standby pairing for reliable recovery

2026-02-02 · M7 · 10 files · +711/−141 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-node-level-standby.md` | 342 | 0 |
| `proto/meta.proto` | 4 | 0 |
| `src/data/server/flight.rs` | 49 | 117 |
| `src/meta/client/meta_client.rs` | 36 | 2 |
| `src/meta/raft/state_machine.rs` | 171 | 2 |
| `src/meta/recovery/mod.rs` | 13 | 10 |
| `src/meta/service/grpc.rs` | 38 | 5 |
| `src/meta/service/meta_node.rs` | 6 | 0 |
| `src/meta/types.rs` | 25 | 1 |
| `src/node/data.rs` | 27 | 4 |

### `ad4c9ed` — fix: deadlock in test_cache_shard_clear_removes_empty_entries

2026-02-02 · M7 · 1 files · +6/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/cache.rs` | 6 | 2 |

### `2c93c26` — feat: implement No-Promotion Standby architecture

2026-02-02 · M7 · 12 files · +1,098/−677 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-standby-no-promotion.md` | 1015 | 0 |
| `proto/meta.proto` | 7 | 27 |
| `src/data/replication/mod.rs` | 10 | 8 |
| `src/data/replication/receiver.rs` | 6 | 59 |
| `src/meta/client/meta_client.rs` | 2 | 19 |
| `src/meta/raft/state_machine.rs` | 11 | 218 |
| `src/meta/service/grpc.rs` | 4 | 56 |
| `src/meta/service/meta_node.rs` | 6 | 177 |
| `src/meta/types.rs` | 23 | 54 |
| `src/node/data.rs` | 2 | 11 |
| `src/router/routing/cache_invalidation.rs` | 7 | 31 |
| `src/router/routing/shard_cache.rs` | 5 | 17 |

### `a305237` — refactor: remove unused UpdateDataNodeMode command and handlers

2026-02-02 · M7 · 4 files · +12/−118 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine.rs` | 2 | 69 |
| `src/meta/types.rs` | 3 | 12 |
| `src/router/routing/cache_invalidation.rs` | 5 | 23 |
| `src/router/routing/shard_cache.rs` | 2 | 14 |

### `bb96dda` — refactor: remove unused load_score field from DataNodeInfo

2026-02-02 · M7 · 3 files · +0/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/api.rs` | 0 | 1 |
| `src/router/routing/cache_invalidation.rs` | 0 | 4 |
| `src/router/routing/shard_cache.rs` | 0 | 2 |

## M8 — Table ownership and partition-aware routing

78 commits.

### `a6bd07d` — refactor(meta): split state_machine.rs into ops/ modules

2026-02-03 · M8 · 14 files · +3,655/−3,298 text lines

| File | Added | Deleted |
|---|---:|---:|
| `plans/large-file-consolidation.md` | 216 | 0 |
| `src/meta/raft/state_machine.rs` | 0 | 3298 |
| `src/meta/raft/state_machine/mod.rs` | 1468 | 0 |
| `src/meta/raft/state_machine/ops/election.rs` | 82 | 0 |
| `src/meta/raft/state_machine/ops/event.rs` | 80 | 0 |
| `src/meta/raft/state_machine/ops/file.rs` | 76 | 0 |
| `src/meta/raft/state_machine/ops/lock.rs` | 80 | 0 |
| `src/meta/raft/state_machine/ops/mod.rs` | 18 | 0 |
| `src/meta/raft/state_machine/ops/node.rs` | 304 | 0 |
| `src/meta/raft/state_machine/ops/query.rs` | 196 | 0 |
| `src/meta/raft/state_machine/ops/recovery.rs` | 293 | 0 |
| `src/meta/raft/state_machine/ops/replica.rs` | 250 | 0 |
| `src/meta/raft/state_machine/ops/scale_in.rs` | 441 | 0 |
| `src/meta/raft/state_machine/ops/standby.rs` | 151 | 0 |

### `feaa69f` — refactor: split compactor.rs and query_engine.rs into modules

2026-02-03 · M8 · 17 files · +5,364/−5,159 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor.rs` | 0 | 3192 |
| `src/jobs/compactor/background.rs` | 120 | 0 |
| `src/jobs/compactor/catalog.rs` | 227 | 0 |
| `src/jobs/compactor/full.rs` | 451 | 0 |
| `src/jobs/compactor/mod.rs` | 395 | 0 |
| `src/jobs/compactor/parquet_io.rs` | 204 | 0 |
| `src/jobs/compactor/partition.rs` | 443 | 0 |
| `src/jobs/compactor/schema.rs` | 224 | 0 |
| `src/jobs/compactor/sorting.rs` | 192 | 0 |
| `src/jobs/compactor/tests.rs` | 1080 | 0 |
| `src/query/query_engine.rs` | 0 | 1967 |
| `src/query/query_engine/cache.rs` | 126 | 0 |
| `src/query/query_engine/discovery.rs` | 547 | 0 |
| `src/query/query_engine/execution.rs` | 300 | 0 |
| `src/query/query_engine/listing.rs` | 271 | 0 |
| `src/query/query_engine/mod.rs` | 271 | 0 |
| `src/query/query_engine/registration.rs` | 513 | 0 |

### `a60f9e3` — fix(wal): prevent race condition in concurrent flushes and add recovery mode check

2026-02-03 · M8 · 2 files · +29/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/cache.rs` | 17 | 0 |
| `src/storage/wal.rs` | 12 | 0 |

### `756fb3f` — fix(wal): improve checkpoint and recovery handling

2026-02-03 · M8 · 5 files · +351/−264 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/features-wal-flush-recovery.md` | 62 | 34 |
| `src/node/common.rs` | 75 | 1 |
| `src/node/data.rs` | 9 | 0 |
| `src/node/standalone.rs` | 4 | 88 |
| `src/storage/wal.rs` | 201 | 141 |

### `1329457` — feat(meta): implement table-level routing, remove file-level APIs

2026-02-03 · M8 · 20 files · +3,130/−904 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-meta-routing.md` | 528 | 0 |
| `proto/meta.proto` | 101 | 39 |
| `src/config.rs` | 44 | 0 |
| `src/meta/client/meta_client.rs` | 205 | 94 |
| `src/meta/raft/log_store.rs` | 10 | 1 |
| `src/meta/raft/state_machine/mod.rs` | 30 | 67 |
| `src/meta/raft/state_machine/ops/file.rs` | 0 | 76 |
| `src/meta/raft/state_machine/ops/mod.rs` | 1 | 1 |
| `src/meta/raft/state_machine/ops/query.rs` | 1 | 41 |
| `src/meta/raft/state_machine/ops/table.rs` | 623 | 0 |
| `src/meta/service/grpc.rs` | 247 | 105 |
| `src/meta/service/http.rs` | 0 | 49 |
| `src/meta/service/meta_node.rs` | 126 | 50 |
| `src/meta/types.rs` | 285 | 6 |
| `src/node/common.rs` | 163 | 184 |
| `src/node/data.rs` | 2 | 2 |
| `src/query/cache/mod.rs` | 26 | 1 |
| `src/query/cache/partition_files_cache.rs` | 411 | 0 |
| `src/router/routing/query_coordinator.rs` | 151 | 181 |
| `src/router/routing/shard_cache.rs` | 176 | 7 |

### `d647e1f` — refactor(router): complete table-level routing, remove partition-level code

2026-02-03 · M8 · 10 files · +156/−1,067 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 0 | 49 |
| `src/meta/client/meta_client.rs` | 0 | 136 |
| `src/meta/service/grpc.rs` | 0 | 102 |
| `src/meta/service/http.rs` | 1 | 66 |
| `src/meta/service/meta_node.rs` | 0 | 182 |
| `src/node/data.rs` | 2 | 55 |
| `src/router/routing/cache_invalidation.rs` | 42 | 69 |
| `src/router/routing/shard_cache.rs` | 66 | 272 |
| `src/router/routing/write_router.rs` | 39 | 123 |
| `src/router/state.rs` | 6 | 13 |

### `6adddde` — refactor: consolidate hash utilities into utils/hash module

2026-02-03 · M8 · 11 files · +1,302/−34 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 4 | 0 |
| `benches/sled_bench.rs` | 679 | 0 |
| `docs/guide-sled-benchmark.md` | 358 | 0 |
| `examples/sled_10m_bench.rs` | 99 | 0 |
| `src/meta/replica_assigner.rs` | 2 | 4 |
| `src/query/cache/query_cache.rs` | 2 | 4 |
| `src/router/routing/shard_cache.rs` | 5 | 16 |
| `src/storage/cache.rs` | 2 | 4 |
| `src/storage/file_cache/types.rs` | 2 | 6 |
| `src/utils/hash.rs` | 148 | 0 |
| `src/utils/mod.rs` | 1 | 0 |

### `eb1c24e` — docs: add sled DB benchmark results for routing strategy comparison

2026-02-03 · M8 · 1 files · +189/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/benchmark-sled-results.md` | 189 | 0 |

### `b84d6de` — feat(routing): add multi-node table ownership for write reroute and scatter-gather queries

2026-02-03 · M8 · 13 files · +1,351/−139 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 48 | 11 |
| `src/meta/client/meta_client.rs` | 77 | 6 |
| `src/meta/raft/state_machine/mod.rs` | 14 | 0 |
| `src/meta/raft/state_machine/ops/table.rs` | 320 | 30 |
| `src/meta/service/grpc.rs` | 126 | 49 |
| `src/meta/service/meta_node.rs` | 82 | 1 |
| `src/meta/types.rs` | 155 | 4 |
| `src/router/config.rs` | 55 | 0 |
| `src/router/routing/mod.rs` | 2 | 0 |
| `src/router/routing/query_coordinator.rs` | 31 | 15 |
| `src/router/routing/shard_cache.rs` | 113 | 21 |
| `src/router/routing/table_rebalancer.rs` | 270 | 0 |
| `src/router/routing/write_router.rs` | 58 | 2 |

### `3fc5348` — refactor(rebalancer): move table rebalancer from Router to Meta leader

2026-02-03 · M8 · 5 files · +232/−348 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 69 | 6 |
| `src/meta/service/meta_node.rs` | 163 | 15 |
| `src/router/config.rs` | 0 | 55 |
| `src/router/routing/mod.rs` | 0 | 2 |
| `src/router/routing/table_rebalancer.rs` | 0 | 270 |

### `563a5b0` — feat(router): add distributed write routing across primary nodes

2026-02-03 · M8 · 13 files · +1,832/−22 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 88 | 0 |
| `src/meta/client/meta_client.rs` | 197 | 0 |
| `src/meta/raft/state_machine/mod.rs` | 14 | 0 |
| `src/meta/raft/state_machine/ops/distribution.rs` | 243 | 0 |
| `src/meta/raft/state_machine/ops/mod.rs` | 1 | 0 |
| `src/meta/service/grpc.rs` | 172 | 0 |
| `src/meta/service/http.rs` | 325 | 0 |
| `src/meta/service/meta_node.rs` | 96 | 0 |
| `src/meta/types.rs` | 195 | 0 |
| `src/router/routing/cache_invalidation.rs` | 63 | 1 |
| `src/router/routing/query_coordinator.rs` | 16 | 1 |
| `src/router/routing/shard_cache.rs` | 146 | 1 |
| `src/router/routing/write_router.rs` | 276 | 19 |

### `72deb6c` — docs: add distributed write routing guide and tests

2026-02-03 · M8 · 3 files · +755/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/guide-distributed-write-routing.md` | 257 | 0 |
| `src/meta/types.rs` | 175 | 0 |
| `src/router/routing/shard_cache.rs` | 323 | 0 |

### `43d298a` — feat(meta): add inherited prefixes for scale-in orphaned data

2026-02-03 · M8 · 12 files · +1,496/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 1 | 0 |
| `src/meta/client/meta_client.rs` | 3 | 0 |
| `src/meta/raft/state_machine/ops/scale_in.rs` | 108 | 2 |
| `src/meta/service/grpc.rs` | 3 | 0 |
| `src/meta/types.rs` | 228 | 0 |
| `src/query/providers/mod.rs` | 4 | 1 |
| `src/query/providers/multi_union_provider.rs` | 497 | 0 |
| `src/query/query_engine/discovery.rs` | 201 | 0 |
| `src/query/query_engine/listing.rs` | 180 | 0 |
| `src/query/query_engine/mod.rs` | 1 | 0 |
| `src/query/query_engine/registration.rs` | 190 | 3 |
| `src/router/routing/shard_cache.rs` | 80 | 1 |

### `ec10f23` — refactor(query): simplify legacy prefix integration

2026-02-03 · M8 · 4 files · +221/−518 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/query_engine/discovery.rs` | 85 | 202 |
| `src/query/query_engine/listing.rs` | 59 | 250 |
| `src/query/query_engine/mod.rs` | 52 | 1 |
| `src/query/query_engine/registration.rs` | 25 | 65 |

### `a335327` — feat(data): integrate legacy prefixes in query execution

2026-02-03 · M8 · 1 files · +45/−8 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 45 | 8 |

### `f08aecc` — refactor(meta): split meta_node.rs into submodules

2026-02-03 · M8 · 13 files · +1,880/−1,814 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/service/meta_node.rs` | 0 | 1813 |
| `src/meta/service/meta_node/cluster.rs` | 70 | 0 |
| `src/meta/service/meta_node/commands.rs` | 195 | 0 |
| `src/meta/service/meta_node/distribution.rs` | 100 | 0 |
| `src/meta/service/meta_node/elections.rs` | 118 | 0 |
| `src/meta/service/meta_node/lifecycle.rs` | 192 | 0 |
| `src/meta/service/meta_node/locks.rs` | 180 | 0 |
| `src/meta/service/meta_node/mod.rs` | 379 | 0 |
| `src/meta/service/meta_node/nodes.rs` | 45 | 0 |
| `src/meta/service/meta_node/reads.rs` | 117 | 0 |
| `src/meta/service/meta_node/scale_in.rs` | 270 | 0 |
| `src/meta/service/meta_node/tables.rs` | 213 | 0 |
| `src/meta/service/mod.rs` | 1 | 1 |

### `577609b` — feat(query): add ObjectStore support to custom ParquetTableProvider

2026-02-03 · M8 · 2 files · +273/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/table_provider.rs` | 238 | 9 |
| `src/query/query_engine/registration.rs` | 35 | 4 |

### `d1ff6cb` — refactor(query): remove partitioned field, all tables use date partition

2026-02-04 · M8 · 2 files · +80/−277 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/table_provider.rs` | 52 | 205 |
| `src/query/query_engine/registration.rs` | 28 | 72 |

### `a84124f` — refactor(meta): migrate scale-in from partition-level to table-level

2026-02-04 · M8 · 10 files · +356/−416 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/bin/sled_inspect.rs` | 0 | 11 |
| `src/meta/scale_in/executor.rs` | 44 | 54 |
| `src/meta/scale_in/mod.rs` | 22 | 26 |
| `src/meta/scale_in/planner.rs` | 58 | 61 |
| `src/meta/scale_in/types.rs` | 52 | 62 |
| `src/meta/scale_in/validator.rs` | 92 | 91 |
| `src/meta/service/http.rs` | 12 | 12 |
| `src/meta/service/meta_node/tables.rs` | 43 | 0 |
| `src/meta/types.rs` | 24 | 95 |
| `src/query/providers/table_provider.rs` | 9 | 4 |

### `30aa581` — refactor(meta): remove ISR model, complete table-level routing migration

2026-02-04 · M8 · 19 files · +437/−2,085 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 18 | 42 |
| `src/bin/sled_inspect.rs` | 0 | 12 |
| `src/data/server/flight.rs` | 1 | 11 |
| `src/meta/client/meta_client.rs` | 17 | 32 |
| `src/meta/mod.rs` | 0 | 1 |
| `src/meta/raft/state_machine/mod.rs` | 5 | 53 |
| `src/meta/raft/state_machine/ops/mod.rs` | 0 | 1 |
| `src/meta/raft/state_machine/ops/query.rs` | 1 | 35 |
| `src/meta/raft/state_machine/ops/recovery.rs` | 100 | 100 |
| `src/meta/raft/state_machine/ops/replica.rs` | 0 | 250 |
| `src/meta/raft/state_machine/ops/scale_in.rs` | 30 | 199 |
| `src/meta/recovery/mod.rs` | 130 | 152 |
| `src/meta/recovery/types.rs` | 45 | 47 |
| `src/meta/replica_assigner.rs` | 0 | 712 |
| `src/meta/service/grpc.rs` | 22 | 37 |
| `src/meta/service/http.rs` | 15 | 15 |
| `src/meta/service/meta_node/scale_in.rs` | 14 | 110 |
| `src/meta/types.rs` | 33 | 226 |
| `src/node/data.rs` | 6 | 50 |

### `73ba8dc` — feat(query): add distributed table query support and legacy prefix distribution

2026-02-04 · M8 · 3 files · +244/−44 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 53 | 15 |
| `src/router/routing/query_coordinator.rs` | 84 | 29 |
| `src/utils/hash.rs` | 107 | 0 |

### `ad49cfc` — refactor: remove unused fencing token and replica event types

2026-02-04 · M8 · 6 files · +25/−272 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 3 | 93 |
| `src/meta/types.rs` | 4 | 14 |
| `src/router/client/data_node_pool.rs` | 1 | 90 |
| `src/router/routing/cache_invalidation.rs` | 0 | 46 |
| `src/router/routing/query_coordinator.rs` | 16 | 27 |
| `src/router/routing/write_router.rs` | 1 | 2 |

### `8aa721a` — refactor(router): optimize distributed write/query routing

2026-02-04 · M8 · 3 files · +617/−451 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 1 | 0 |
| `src/router/routing/query_coordinator.rs` | 216 | 203 |
| `src/router/routing/write_router.rs` | 400 | 248 |

### `8764826` — fix(router): handle scale-in data loss for distributed tables

2026-02-04 · M8 · 1 files · +520/−5 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/routing/query_coordinator.rs` | 520 | 5 |

### `cfb08e1` — refactor(router): unify write candidate selection and simplify retry logic

2026-02-04 · M8 · 1 files · +179/−227 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/routing/write_router.rs` | 179 | 227 |

### `ea1406a` — perf(router): optimize Meta registration with pending guard

2026-02-04 · M8 · 2 files · +77/−22 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/routing/shard_cache.rs` | 40 | 1 |
| `src/router/routing/write_router.rs` | 37 | 21 |

### `86c91c3` — style: apply rustfmt formatting

2026-02-04 · M8 · 58 files · +742/−543 text lines

| File | Added | Deleted |
|---|---:|---:|
| `benches/sled_bench.rs` | 1 | 4 |
| `examples/sled_10m_bench.rs` | 53 | 12 |
| `src/aggregates/definition.rs` | 19 | 23 |
| `src/aggregates/job.rs` | 1 | 4 |
| `src/aggregates/mod.rs` | 1 | 4 |
| `src/aggregates/query_rewriter.rs` | 27 | 6 |
| `src/aggregates/storage.rs` | 6 | 3 |
| `src/data/replication/receiver.rs` | 24 | 50 |
| `src/data/replication/stream.rs` | 1 | 2 |
| `src/data/replication/types.rs` | 2 | 7 |
| `src/data/server/api.rs` | 21 | 18 |
| `src/data/server/flight.rs` | 18 | 13 |
| `src/jobs/compactor/full.rs` | 36 | 32 |
| `src/jobs/compactor/parquet_io.rs` | 4 | 1 |
| `src/jobs/compactor/partition.rs` | 4 | 1 |
| `src/jobs/compactor/tests.rs` | 1 | 5 |
| `src/jobs/database_manager.rs` | 9 | 7 |
| `src/meta/client/meta_client.rs` | 23 | 8 |
| `src/meta/raft/state_machine/mod.rs` | 4 | 8 |
| `src/meta/raft/state_machine/ops/distribution.rs` | 7 | 11 |
| `src/meta/raft/state_machine/ops/lock.rs` | 7 | 1 |
| `src/meta/raft/state_machine/ops/node.rs` | 12 | 7 |
| `src/meta/raft/state_machine/ops/recovery.rs` | 21 | 25 |
| `src/meta/raft/state_machine/ops/standby.rs` | 4 | 1 |
| `src/meta/recovery/mod.rs` | 32 | 20 |
| `src/meta/recovery/types.rs` | 5 | 7 |
| `src/meta/scale_in/executor.rs` | 21 | 9 |
| `src/meta/scale_in/mod.rs` | 8 | 5 |
| `src/meta/scale_in/planner.rs` | 1 | 5 |
| `src/meta/scale_in/types.rs` | 7 | 16 |
| `src/meta/scale_in/validator.rs` | 10 | 11 |
| `src/meta/service/grpc.rs` | 11 | 8 |
| `src/meta/service/http.rs` | 14 | 3 |
| `src/meta/service/meta_node/commands.rs` | 5 | 1 |
| `src/meta/service/meta_node/distribution.rs` | 6 | 2 |
| `src/meta/service/meta_node/lifecycle.rs` | 3 | 12 |
| `src/meta/service/meta_node/mod.rs` | 3 | 1 |
| `src/meta/service/meta_node/scale_in.rs` | 6 | 6 |
| `src/meta/service/meta_node/tables.rs` | 6 | 1 |
| `src/meta/types.rs` | 0 | 2 |
| `src/node/common.rs` | 20 | 6 |
| `src/node/router.rs` | 4 | 1 |
| `src/node/standalone.rs` | 0 | 1 |
| `src/query/cache/partition_files_cache.rs` | 1 | 2 |
| `src/query/providers/multi_union_provider.rs` | 7 | 13 |
| `src/query/providers/table_provider.rs` | 32 | 12 |
| `src/query/query_engine/discovery.rs` | 22 | 5 |
| `src/query/query_engine/execution.rs` | 2 | 2 |
| `src/query/query_engine/mod.rs` | 25 | 5 |
| `src/query/query_engine/registration.rs` | 36 | 19 |
| `src/router/api.rs` | 2 | 4 |
| `src/router/flight.rs` | 0 | 8 |
| `src/router/routing/cache_invalidation.rs` | 19 | 12 |
| `src/router/routing/query_coordinator.rs` | 28 | 31 |
| `src/router/routing/shard_cache.rs` | 51 | 36 |
| `src/router/routing/write_router.rs` | 34 | 8 |
| `src/services/api.rs` | 1 | 4 |
| `src/utils/hash.rs` | 14 | 12 |

### `21d6eb4` — test: add cold standby distribution test script

2026-02-04 · M8 · 1 files · +398/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/test_coldstandby_distribution.sh` | 398 | 0 |

### `d2022aa` — fix(data): flush memtable on retire event to prevent scale-in data loss

2026-02-04 · M8 · 3 files · +47/−53 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/types.rs` | 5 | 7 |
| `src/node/data.rs` | 42 | 0 |
| `src/router/routing/cache_invalidation.rs` | 0 | 46 |

### `5815c1d` — fix(meta): clean up standby pairings and table ownership on node deregister

2026-02-04 · M8 · 1 files · +65/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/ops/node.rs` | 65 | 3 |

### `9ec2fab` — perf(meta): use Drained state for O(1) node deregistration

2026-02-04 · M8 · 4 files · +113/−46 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/mod.rs` | 9 | 4 |
| `src/meta/raft/state_machine/ops/node.rs` | 17 | 31 |
| `src/meta/raft/state_machine/ops/table.rs` | 39 | 9 |
| `src/meta/service/meta_node/lifecycle.rs` | 48 | 2 |

### `15b980b` — perf(meta): optimize cleanup_drained_nodes with single table scan

2026-02-04 · M8 · 2 files · +140/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/mod.rs` | 116 | 0 |
| `src/meta/service/meta_node/lifecycle.rs` | 24 | 15 |

### `4e5e9a1` — refactor(data): extract helper functions from start() for readability

2026-02-04 · M8 · 3 files · +567/−427 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/ops/table.rs` | 3 | 1 |
| `src/meta/service/meta_node/lifecycle.rs` | 8 | 2 |
| `src/node/data.rs` | 556 | 424 |

### `c0c7139` — refactor(node): consolidate background flush functions

2026-02-04 · M8 · 2 files · +52/−229 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/common.rs` | 45 | 215 |
| `src/node/data.rs` | 7 | 14 |

### `37fefe6` — feat(meta): integrate RecoveryManager with lease expiration

2026-02-04 · M8 · 3 files · +35/−8 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/ops/event.rs` | 4 | 3 |
| `src/meta/service/meta_node/lifecycle.rs` | 26 | 5 |
| `src/meta/types.rs` | 5 | 0 |

### `df6441f` — refactor(meta): consolidate apply_command functions

2026-02-04 · M8 · 10 files · +95/−475 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/recovery/mod.rs` | 3 | 3 |
| `src/meta/scale_in/executor.rs` | 1 | 1 |
| `src/meta/service/grpc.rs` | 3 | 5 |
| `src/meta/service/meta_node/commands.rs` | 23 | 126 |
| `src/meta/service/meta_node/distribution.rs` | 9 | 40 |
| `src/meta/service/meta_node/elections.rs` | 12 | 71 |
| `src/meta/service/meta_node/lifecycle.rs` | 1 | 1 |
| `src/meta/service/meta_node/locks.rs` | 12 | 132 |
| `src/meta/service/meta_node/nodes.rs` | 10 | 10 |
| `src/meta/service/meta_node/scale_in.rs` | 21 | 86 |

### `3765eda` — refactor(meta): flatten state_machine/ops directory structure

2026-02-04 · M8 · 22 files · +2,744/−2,753 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/distribution.rs` | 239 | 0 |
| `src/meta/raft/state_machine/election.rs` | 82 | 0 |
| `src/meta/raft/state_machine/event.rs` | 81 | 0 |
| `src/meta/raft/state_machine/lock.rs` | 86 | 0 |
| `src/meta/raft/state_machine/mod.rs` | 11 | 2 |
| `src/meta/raft/state_machine/node.rs` | 357 | 0 |
| `src/meta/raft/state_machine/ops/distribution.rs` | 0 | 239 |
| `src/meta/raft/state_machine/ops/election.rs` | 0 | 82 |
| `src/meta/raft/state_machine/ops/event.rs` | 0 | 81 |
| `src/meta/raft/state_machine/ops/lock.rs` | 0 | 86 |
| `src/meta/raft/state_machine/ops/mod.rs` | 0 | 18 |
| `src/meta/raft/state_machine/ops/node.rs` | 0 | 357 |
| `src/meta/raft/state_machine/ops/query.rs` | 0 | 122 |
| `src/meta/raft/state_machine/ops/recovery.rs` | 0 | 289 |
| `src/meta/raft/state_machine/ops/scale_in.rs` | 0 | 378 |
| `src/meta/raft/state_machine/ops/standby.rs` | 0 | 154 |
| `src/meta/raft/state_machine/ops/table.rs` | 0 | 945 |
| `src/meta/raft/state_machine/query.rs` | 122 | 0 |
| `src/meta/raft/state_machine/recovery.rs` | 289 | 0 |
| `src/meta/raft/state_machine/scale_in.rs` | 378 | 0 |
| `src/meta/raft/state_machine/standby.rs` | 154 | 0 |
| `src/meta/raft/state_machine/table.rs` | 945 | 0 |

### `b0b03a8` — fix(test): merge env var tests to avoid race condition

2026-02-04 · M8 · 1 files · +5/−10 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 5 | 10 |

### `823a600` — perf(build): optimize dev build with mold linker and profile settings

2026-02-04 · M8 · 1 files · +11/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 11 | 1 |

### `1308e40` — refactor(meta): split types.rs into modular type files

2026-02-05 · M8 · 9 files · +1,972/−1,925 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/types.rs` | 0 | 1925 |
| `src/meta/types/command.rs` | 480 | 0 |
| `src/meta/types/distribution.rs` | 295 | 0 |
| `src/meta/types/event.rs` | 121 | 0 |
| `src/meta/types/lock.rs` | 103 | 0 |
| `src/meta/types/mod.rs` | 42 | 0 |
| `src/meta/types/node.rs` | 297 | 0 |
| `src/meta/types/scale_in.rs` | 154 | 0 |
| `src/meta/types/table.rs` | 480 | 0 |

### `b8e4ece` — refactor: split cache.rs and state_machine tests for faster builds

2026-02-05 · M8 · 8 files · +2,296/−2,261 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/raft/state_machine/mod.rs` | 3 | 814 |
| `src/meta/raft/state_machine/tests.rs` | 813 | 0 |
| `src/storage/cache.rs` | 0 | 1447 |
| `src/storage/cache/cardinality.rs` | 79 | 0 |
| `src/storage/cache/mod.rs` | 894 | 0 |
| `src/storage/cache/shard.rs` | 93 | 0 |
| `src/storage/cache/stats.rs` | 66 | 0 |
| `src/storage/cache/tests.rs` | 348 | 0 |

### `dca1098` — docs: add proposal for partition-aware query routing via path restructure

2026-02-05 · M8 · 1 files · +1,181/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposals/partition-aware-query-routing.md` | 1181 | 0 |

### `917481e` — docs: refine routing proposal with node_id as Hive partition key

2026-02-05 · M8 · 1 files · +462/−235 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposals/partition-aware-query-routing.md` | 462 | 235 |

### `9d50532` — feat(cluster): implement node_id as Hive partition key for smart routing

2026-02-05 · M8 · 5 files · +879/−138 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 12 | 1 |
| `src/query/providers/table_provider.rs` | 289 | 122 |
| `src/query/sql_utils.rs` | 146 | 0 |
| `src/router/routing/shard_cache.rs` | 408 | 4 |
| `src/storage/parquet_writer.rs` | 24 | 11 |

### `73e93e9` — feat(router): add partition-level query routing with S3 discovery

2026-02-05 · M8 · 2 files · +610/−29 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/routing/query_coordinator.rs` | 555 | 15 |
| `src/router/state.rs` | 55 | 14 |

### `82a07d9` — refactor(router): split query_coordinator.rs into focused submodules

2026-02-05 · M8 · 6 files · +1,852/−1,784 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/routing/mod.rs` | 1 | 1 |
| `src/router/routing/query_coordinator.rs` | 0 | 1783 |
| `src/router/routing/query_coordinator/aggregation.rs` | 147 | 0 |
| `src/router/routing/query_coordinator/mod.rs` | 931 | 0 |
| `src/router/routing/query_coordinator/routing.rs` | 717 | 0 |
| `src/router/routing/query_coordinator/types.rs` | 56 | 0 |

### `4e0cfaf` — fix(retention): update path format for node_id as Hive partition key

2026-02-05 · M8 · 1 files · +49/−21 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/database_manager.rs` | 49 | 21 |

### `49cb369` — refactor(jobs): migrate compactor and retention to Object Store abstraction

2026-02-05 · M8 · 9 files · +467/−522 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor/background.rs` | 36 | 34 |
| `src/jobs/compactor/catalog.rs` | 52 | 96 |
| `src/jobs/compactor/full.rs` | 87 | 106 |
| `src/jobs/compactor/mod.rs` | 40 | 39 |
| `src/jobs/compactor/parquet_io.rs` | 59 | 39 |
| `src/jobs/compactor/partition.rs` | 49 | 79 |
| `src/jobs/compactor/tests.rs` | 25 | 12 |
| `src/jobs/retention_manager.rs` | 116 | 112 |
| `src/node/standalone.rs` | 3 | 5 |

### `d2541af` — feat(storage): add delete_prefix() for batch file deletion

2026-02-05 · M8 · 4 files · +115/−17 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/retention_manager.rs` | 10 | 17 |
| `src/node/router.rs` | 22 | 0 |
| `src/router/config.rs` | 19 | 0 |
| `src/storage/storage.rs` | 64 | 0 |

### `8f89b47` — fix(compactor): update tests for Object Store API changes

2026-02-05 · M8 · 1 files · +31/−22 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor/tests.rs` | 31 | 22 |

### `3effe46` — refactor(query): migrate ParquetTableProvider to ObjectStore-only API

2026-02-05 · M8 · 9 files · +1,449/−1,539 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/mod.rs` | 2 | 2 |
| `src/query/providers/table_provider.rs` | 0 | 1531 |
| `src/query/providers/table_provider/config.rs` | 73 | 0 |
| `src/query/providers/table_provider/discovery.rs` | 224 | 0 |
| `src/query/providers/table_provider/exec_plan.rs` | 202 | 0 |
| `src/query/providers/table_provider/filters.rs` | 298 | 0 |
| `src/query/providers/table_provider/mod.rs` | 382 | 0 |
| `src/query/providers/table_provider/reader.rs` | 256 | 0 |
| `src/query/query_engine/registration.rs` | 12 | 6 |

### `51f66bc` — refactor(router): flatten directory and split shard_cache.rs

2026-02-05 · M8 · 26 files · +5,130/−5,067 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/router.rs` | 1 | 1 |
| `src/router/api.rs` | 1 | 1 |
| `src/router/cache_invalidation.rs` | 468 | 0 |
| `src/router/health_checker.rs` | 170 | 0 |
| `src/router/mod.rs` | 11 | 1 |
| `src/router/query_coordinator/aggregation.rs` | 147 | 0 |
| `src/router/query_coordinator/mod.rs` | 931 | 0 |
| `src/router/query_coordinator/routing.rs` | 717 | 0 |
| `src/router/query_coordinator/types.rs` | 56 | 0 |
| `src/router/routing/cache_invalidation.rs` | 0 | 468 |
| `src/router/routing/health_checker.rs` | 0 | 170 |
| `src/router/routing/mod.rs` | 0 | 14 |
| `src/router/routing/query_coordinator/aggregation.rs` | 0 | 147 |
| `src/router/routing/query_coordinator/mod.rs` | 0 | 931 |
| `src/router/routing/query_coordinator/routing.rs` | 0 | 717 |
| `src/router/routing/query_coordinator/types.rs` | 0 | 56 |
| `src/router/routing/shard_cache.rs` | 0 | 1668 |
| `src/router/routing/write_router.rs` | 0 | 892 |
| `src/router/shard_cache/distribution.rs` | 465 | 0 |
| `src/router/shard_cache/mod.rs` | 119 | 0 |
| `src/router/shard_cache/node_mgmt.rs` | 214 | 0 |
| `src/router/shard_cache/partition_cache.rs` | 381 | 0 |
| `src/router/shard_cache/table_routing.rs` | 351 | 0 |
| `src/router/shard_cache/types.rs` | 205 | 0 |
| `src/router/state.rs` | 1 | 1 |
| `src/router/write_router.rs` | 892 | 0 |

### `f7b4e08` — fix(router): add partition-level SQL rewriting for cache affinity

2026-02-05 · M8 · 3 files · +571/−52 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/query_coordinator/mod.rs` | 411 | 18 |
| `src/router/query_coordinator/routing.rs` | 125 | 34 |
| `src/router/query_coordinator/types.rs` | 35 | 0 |

### `6430ffb` — Merge branch 'dev/node-id' - partition-level routing with cache affinity

2026-02-05 · M8 · 47 files · +7,983/−6,021 text lines

Merge; changes below are relative to the first parent.

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 12 | 1 |
| `src/jobs/compactor/background.rs` | 36 | 34 |
| `src/jobs/compactor/catalog.rs` | 52 | 96 |
| `src/jobs/compactor/full.rs` | 87 | 106 |
| `src/jobs/compactor/mod.rs` | 40 | 39 |
| `src/jobs/compactor/parquet_io.rs` | 59 | 39 |
| `src/jobs/compactor/partition.rs` | 49 | 79 |
| `src/jobs/compactor/tests.rs` | 56 | 34 |
| `src/jobs/database_manager.rs` | 49 | 21 |
| `src/jobs/retention_manager.rs` | 112 | 115 |
| `src/node/router.rs` | 23 | 1 |
| `src/node/standalone.rs` | 3 | 5 |
| `src/query/providers/mod.rs` | 2 | 2 |
| `src/query/providers/table_provider.rs` | 0 | 1364 |
| `src/query/providers/table_provider/config.rs` | 73 | 0 |
| `src/query/providers/table_provider/discovery.rs` | 224 | 0 |
| `src/query/providers/table_provider/exec_plan.rs` | 202 | 0 |
| `src/query/providers/table_provider/filters.rs` | 298 | 0 |
| `src/query/providers/table_provider/mod.rs` | 382 | 0 |
| `src/query/providers/table_provider/reader.rs` | 256 | 0 |
| `src/query/query_engine/registration.rs` | 12 | 6 |
| `src/query/sql_utils.rs` | 146 | 0 |
| `src/router/api.rs` | 1 | 1 |
| `src/router/cache_invalidation.rs` | 468 | 0 |
| `src/router/config.rs` | 19 | 0 |
| `src/router/health_checker.rs` | 170 | 0 |
| `src/router/mod.rs` | 11 | 1 |
| `src/router/query_coordinator/aggregation.rs` | 147 | 0 |
| `src/router/query_coordinator/mod.rs` | 1324 | 0 |
| `src/router/query_coordinator/routing.rs` | 808 | 0 |
| `src/router/query_coordinator/types.rs` | 91 | 0 |
| `src/router/routing/cache_invalidation.rs` | 0 | 468 |
| `src/router/routing/health_checker.rs` | 0 | 170 |
| `src/router/routing/mod.rs` | 0 | 14 |
| `src/router/routing/query_coordinator.rs` | 0 | 1243 |
| `src/router/routing/shard_cache.rs` | 0 | 1264 |
| `src/router/routing/write_router.rs` | 0 | 892 |
| `src/router/shard_cache/distribution.rs` | 465 | 0 |
| `src/router/shard_cache/mod.rs` | 119 | 0 |
| `src/router/shard_cache/node_mgmt.rs` | 214 | 0 |
| `src/router/shard_cache/partition_cache.rs` | 381 | 0 |
| `src/router/shard_cache/table_routing.rs` | 351 | 0 |
| `src/router/shard_cache/types.rs` | 205 | 0 |
| `src/router/state.rs` | 56 | 15 |
| `src/router/write_router.rs` | 892 | 0 |
| `src/storage/parquet_writer.rs` | 24 | 11 |
| `src/storage/storage.rs` | 64 | 0 |

### `85fd1ed` — refactor(query_coordinator): split mod.rs into focused submodules

2026-02-05 · M8 · 5 files · +1,226/−1,192 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/query_coordinator/execution.rs` | 297 | 0 |
| `src/router/query_coordinator/mod.rs` | 7 | 1192 |
| `src/router/query_coordinator/planning.rs` | 136 | 0 |
| `src/router/query_coordinator/sql_rewrite.rs` | 80 | 0 |
| `src/router/query_coordinator/tests.rs` | 706 | 0 |

### `99d1667` — refactor: remove legacy_prefixes from codebase

2026-02-05 · M8 · 17 files · +116/−1,240 text lines

| File | Added | Deleted |
|---|---:|---:|
| `proto/meta.proto` | 1 | 1 |
| `src/data/server/flight.rs` | 0 | 87 |
| `src/meta/client/meta_client.rs` | 0 | 3 |
| `src/meta/raft/state_machine/scale_in.rs` | 1 | 103 |
| `src/meta/scale_in/planner.rs` | 1 | 1 |
| `src/meta/service/grpc.rs` | 0 | 3 |
| `src/meta/types/event.rs` | 0 | 8 |
| `src/meta/types/table.rs` | 13 | 180 |
| `src/query/query_engine/discovery.rs` | 34 | 125 |
| `src/query/query_engine/listing.rs` | 52 | 62 |
| `src/query/query_engine/mod.rs` | 0 | 70 |
| `src/query/query_engine/registration.rs` | 5 | 183 |
| `src/router/query_coordinator/routing.rs` | 7 | 78 |
| `src/router/query_coordinator/tests.rs` | 1 | 147 |
| `src/router/shard_cache/table_routing.rs` | 1 | 58 |
| `src/router/shard_cache/types.rs` | 0 | 22 |
| `src/utils/hash.rs` | 0 | 109 |

### `9607447` — fix: resolve clippy lint warnings across codebase

2026-02-05 · M8 · 44 files · +250/−305 text lines

| File | Added | Deleted |
|---|---:|---:|
| `build.rs` | 3 | 3 |
| `src/aggregates/definition.rs` | 2 | 2 |
| `src/aggregates/types.rs` | 8 | 4 |
| `src/bin/sled_inspect.rs` | 11 | 13 |
| `src/config.rs` | 2 | 24 |
| `src/data/replication/receiver.rs` | 3 | 0 |
| `src/data/replication/stream.rs` | 1 | 0 |
| `src/data/replication/types.rs` | 2 | 5 |
| `src/jobs/compactor/parquet_io.rs` | 1 | 0 |
| `src/jobs/compactor/partition.rs` | 2 | 0 |
| `src/jobs/profiling.rs` | 8 | 3 |
| `src/meta/client/meta_client.rs` | 2 | 2 |
| `src/meta/raft/log_store.rs` | 34 | 40 |
| `src/meta/raft/state_machine/event.rs` | 5 | 7 |
| `src/meta/raft/state_machine/mod.rs` | 35 | 41 |
| `src/meta/raft/state_machine/node.rs` | 4 | 6 |
| `src/meta/raft/state_machine/query.rs` | 10 | 12 |
| `src/meta/raft/state_machine/table.rs` | 34 | 52 |
| `src/meta/scale_in/planner.rs` | 2 | 4 |
| `src/meta/service/meta_node/reads.rs` | 2 | 2 |
| `src/node/common.rs` | 1 | 1 |
| `src/node/data.rs` | 1 | 1 |
| `src/node/meta.rs` | 2 | 2 |
| `src/node/router.rs` | 1 | 1 |
| `src/query/catalog/memory_manager.rs` | 4 | 4 |
| `src/query/functions/time_bucket.rs` | 1 | 1 |
| `src/query/physical_optimizers/scan_config.rs` | 1 | 0 |
| `src/query/providers/table_provider/exec_plan.rs` | 1 | 0 |
| `src/query/providers/table_provider/filters.rs` | 31 | 34 |
| `src/query/providers/table_provider/reader.rs` | 1 | 0 |
| `src/query/query_engine/execution.rs` | 2 | 2 |
| `src/query/query_engine/registration.rs` | 13 | 15 |
| `src/router/client/data_node_pool.rs` | 2 | 2 |
| `src/router/config.rs` | 1 | 9 |
| `src/router/query_coordinator/aggregation.rs` | 2 | 2 |
| `src/router/query_coordinator/routing.rs` | 1 | 0 |
| `src/router/write_router.rs` | 1 | 0 |
| `src/services/api.rs` | 1 | 0 |
| `src/storage/cache/mod.rs` | 3 | 4 |
| `src/storage/cache/shard.rs` | 1 | 1 |
| `src/storage/line_protocol.rs` | 1 | 2 |
| `src/storage/mod.rs` | 1 | 0 |
| `src/storage/parquet_writer.rs` | 2 | 0 |
| `src/storage/wal.rs` | 4 | 4 |

### `c9a13eb` — fix(cluster): use Hive partition structure for table registration

2026-02-05 · M8 · 3 files · +18/−33 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/cluster/data1.toml` | 3 | 0 |
| `dev/configs/cluster/data2.toml` | 3 | 0 |
| `src/query/query_engine/registration.rs` | 12 | 33 |

### `f0172fc` — fix(router): improve node mode handling and query aggregation

2026-02-05 · M8 · 2 files · +300/−32 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/cache_invalidation.rs` | 95 | 16 |
| `src/router/query_coordinator/aggregation.rs` | 205 | 16 |

### `746b5f0` — feat(wal): add WAL support for cluster mode batch writes

2026-02-05 · M8 · 4 files · +344/−69 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 54 | 0 |
| `src/node/common.rs` | 82 | 50 |
| `src/node/data.rs` | 6 | 0 |
| `src/storage/wal.rs` | 202 | 19 |

### `7911861` — feat(router): add TTL-based table cache eviction

2026-02-06 · M8 · 6 files · +264/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/config.rs` | 66 | 0 |
| `src/meta/raft/state_machine/table.rs` | 77 | 0 |
| `src/meta/service/meta_node/lifecycle.rs` | 34 | 0 |
| `src/meta/types/event.rs` | 8 | 0 |
| `src/router/cache_invalidation.rs` | 25 | 0 |
| `src/router/shard_cache/table_routing.rs` | 54 | 1 |

### `b500a7c` — feat(admin): add admin console for cluster management

2026-02-06 · M8 · 46 files · +6,291/−358 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 177 | 1 |
| `Cargo.toml` | 11 | 1 |
| `admin-frontend/Cargo.toml` | 35 | 0 |
| `admin-frontend/Trunk.toml` | 11 | 0 |
| `admin-frontend/index.html` | 14 | 0 |
| `admin-frontend/src/api/client.rs` | 233 | 0 |
| `admin-frontend/src/api/events.rs` | 100 | 0 |
| `admin-frontend/src/api/mod.rs` | 6 | 0 |
| `admin-frontend/src/app.rs` | 59 | 0 |
| `admin-frontend/src/components/layout.rs` | 121 | 0 |
| `admin-frontend/src/components/mod.rs` | 13 | 0 |
| `admin-frontend/src/components/node_card.rs` | 79 | 0 |
| `admin-frontend/src/components/progress.rs` | 134 | 0 |
| `admin-frontend/src/components/status_badge.rs` | 79 | 0 |
| `admin-frontend/src/components/toast.rs` | 67 | 0 |
| `admin-frontend/src/lib.rs` | 21 | 0 |
| `admin-frontend/src/pages/dashboard.rs` | 163 | 0 |
| `admin-frontend/src/pages/events.rs` | 217 | 0 |
| `admin-frontend/src/pages/mod.rs` | 7 | 0 |
| `admin-frontend/src/pages/nodes.rs` | 159 | 0 |
| `admin-frontend/src/pages/scale_in.rs` | 328 | 0 |
| `admin-frontend/src/pages/tables.rs` | 198 | 0 |
| `admin-frontend/src/state/mod.rs` | 122 | 0 |
| `admin-frontend/style/main.css` | 146 | 0 |
| `admin-frontend/style/tailwind.css` | 181 | 0 |
| `admin-frontend/tailwind.config.js` | 26 | 0 |
| `admin-types/Cargo.toml` | 10 | 0 |
| `admin-types/src/cluster.rs` | 94 | 0 |
| `admin-types/src/events.rs` | 145 | 0 |
| `admin-types/src/lib.rs` | 17 | 0 |
| `admin-types/src/nodes.rs` | 219 | 0 |
| `admin-types/src/scale_in.rs` | 202 | 0 |
| `admin-types/src/tables.rs` | 125 | 0 |
| `docs/guide-admin-console.md` | 832 | 0 |
| `scripts/test_admin_console.sh` | 622 | 0 |
| `src/admin/api.rs` | 863 | 0 |
| `src/admin/mod.rs` | 27 | 0 |
| `src/admin/state.rs` | 69 | 0 |
| `src/admin/static_files.rs` | 121 | 0 |
| `src/admin/websocket.rs` | 90 | 0 |
| `src/config.rs` | 37 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 14 | 0 |
| `src/node/admin.rs` | 94 | 0 |
| `src/node/mod.rs` | 2 | 0 |
| `understanding.md` | 0 | 356 |

### `23596b1` — Merge pull request #1 from panbooks/mynadb-dev

2026-02-06 · M8 · 46 files · +6,291/−358 text lines

Merge; changes below are relative to the first parent.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 177 | 1 |
| `Cargo.toml` | 11 | 1 |
| `admin-frontend/Cargo.toml` | 35 | 0 |
| `admin-frontend/Trunk.toml` | 11 | 0 |
| `admin-frontend/index.html` | 14 | 0 |
| `admin-frontend/src/api/client.rs` | 233 | 0 |
| `admin-frontend/src/api/events.rs` | 100 | 0 |
| `admin-frontend/src/api/mod.rs` | 6 | 0 |
| `admin-frontend/src/app.rs` | 59 | 0 |
| `admin-frontend/src/components/layout.rs` | 121 | 0 |
| `admin-frontend/src/components/mod.rs` | 13 | 0 |
| `admin-frontend/src/components/node_card.rs` | 79 | 0 |
| `admin-frontend/src/components/progress.rs` | 134 | 0 |
| `admin-frontend/src/components/status_badge.rs` | 79 | 0 |
| `admin-frontend/src/components/toast.rs` | 67 | 0 |
| `admin-frontend/src/lib.rs` | 21 | 0 |
| `admin-frontend/src/pages/dashboard.rs` | 163 | 0 |
| `admin-frontend/src/pages/events.rs` | 217 | 0 |
| `admin-frontend/src/pages/mod.rs` | 7 | 0 |
| `admin-frontend/src/pages/nodes.rs` | 159 | 0 |
| `admin-frontend/src/pages/scale_in.rs` | 328 | 0 |
| `admin-frontend/src/pages/tables.rs` | 198 | 0 |
| `admin-frontend/src/state/mod.rs` | 122 | 0 |
| `admin-frontend/style/main.css` | 146 | 0 |
| `admin-frontend/style/tailwind.css` | 181 | 0 |
| `admin-frontend/tailwind.config.js` | 26 | 0 |
| `admin-types/Cargo.toml` | 10 | 0 |
| `admin-types/src/cluster.rs` | 94 | 0 |
| `admin-types/src/events.rs` | 145 | 0 |
| `admin-types/src/lib.rs` | 17 | 0 |
| `admin-types/src/nodes.rs` | 219 | 0 |
| `admin-types/src/scale_in.rs` | 202 | 0 |
| `admin-types/src/tables.rs` | 125 | 0 |
| `docs/guide-admin-console.md` | 832 | 0 |
| `scripts/test_admin_console.sh` | 622 | 0 |
| `src/admin/api.rs` | 863 | 0 |
| `src/admin/mod.rs` | 27 | 0 |
| `src/admin/state.rs` | 69 | 0 |
| `src/admin/static_files.rs` | 121 | 0 |
| `src/admin/websocket.rs` | 90 | 0 |
| `src/config.rs` | 37 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 14 | 0 |
| `src/node/admin.rs` | 94 | 0 |
| `src/node/mod.rs` | 2 | 0 |
| `understanding.md` | 0 | 356 |

### `a64c357` — feat(router): implement split partition cache architecture

2026-02-06 · M8 · 11 files · +1,099/−116 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 21 | 0 |
| `Cargo.toml` | 1 | 0 |
| `src/node/router.rs` | 28 | 2 |
| `src/router/config.rs` | 38 | 3 |
| `src/router/query_coordinator/routing.rs` | 117 | 94 |
| `src/router/shard_cache/cold_partition_cache.rs` | 415 | 0 |
| `src/router/shard_cache/hot_partition_cache.rs` | 353 | 0 |
| `src/router/shard_cache/mod.rs` | 91 | 12 |
| `src/router/shard_cache/types.rs` | 15 | 2 |
| `src/router/state.rs` | 5 | 2 |
| `src/router/write_router.rs` | 15 | 1 |

### `487d8a9` — feat(router): add standby failover for query routing

2026-02-06 · M8 · 11 files · +485/−44 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 30 | 3 |
| `src/query/query_engine/execution.rs` | 64 | 11 |
| `src/query/query_engine/mod.rs` | 1 | 0 |
| `src/query/query_engine/registration.rs` | 134 | 0 |
| `src/router/cache_invalidation.rs` | 25 | 6 |
| `src/router/client/data_node_pool.rs` | 57 | 2 |
| `src/router/client/mod.rs` | 1 | 1 |
| `src/router/query_coordinator/execution.rs` | 136 | 13 |
| `src/router/query_coordinator/tests.rs` | 13 | 7 |
| `src/router/shard_cache/types.rs` | 23 | 1 |
| `src/router/write_router.rs` | 1 | 0 |

### `d141034` — refactor(router): derive hot data routing from table owners

2026-02-06 · M8 · 8 files · +158/−145 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/router.rs` | 3 | 24 |
| `src/router/config.rs` | 7 | 5 |
| `src/router/query_coordinator/routing.rs` | 18 | 14 |
| `src/router/query_coordinator/tests.rs` | 42 | 33 |
| `src/router/shard_cache/mod.rs` | 79 | 51 |
| `src/router/shard_cache/types.rs` | 0 | 5 |
| `src/router/state.rs` | 5 | 2 |
| `src/router/write_router.rs` | 4 | 11 |

### `8b089b4` — refactor(router): simplify hot data detection using max_time check

2026-02-06 · M8 · 3 files · +30/−35 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/query_coordinator/routing.rs` | 26 | 5 |
| `src/router/shard_cache/mod.rs` | 2 | 26 |
| `src/router/state.rs` | 2 | 4 |

### `d06fd19` — feat(admin-ui): improve dashboard design and fix API client

2026-02-06 · M8 · 20 files · +2,815/−384 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.gitignore` | 4 | 0 |
| `admin-frontend/Cargo.lock` | 2059 | 0 |
| `admin-frontend/Cargo.toml` | 5 | 1 |
| `admin-frontend/Trunk.toml` | 4 | 0 |
| `admin-frontend/src/api/client.rs` | 30 | 4 |
| `admin-frontend/src/api/events.rs` | 3 | 2 |
| `admin-frontend/src/components/layout.rs` | 26 | 16 |
| `admin-frontend/src/components/node_card.rs` | 19 | 10 |
| `admin-frontend/src/components/progress.rs` | 20 | 37 |
| `admin-frontend/src/components/status_badge.rs` | 14 | 8 |
| `admin-frontend/src/components/toast.rs` | 2 | 1 |
| `admin-frontend/src/pages/dashboard.rs` | 300 | 92 |
| `admin-frontend/src/pages/events.rs` | 17 | 17 |
| `admin-frontend/src/pages/nodes.rs` | 17 | 15 |
| `admin-frontend/src/pages/scale_in.rs` | 187 | 131 |
| `admin-frontend/src/pages/tables.rs` | 30 | 18 |
| `admin-frontend/style/main.css` | 66 | 25 |
| `src/admin/mod.rs` | 3 | 3 |
| `src/admin/static_files.rs` | 6 | 2 |
| `src/node/admin.rs` | 3 | 2 |

### `9eb8d2d` — refactor: organize docs/ and scripts/ into categorized subdirectories

2026-02-06 · M8 · 264 files · +53,265/−53,265 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture-caching-layer.md` | 0 | 586 |
| `docs/architecture-catalog-structure.md` | 0 | 89 |
| `docs/architecture-cluster-scale-in.md` | 0 | 325 |
| `docs/architecture-cluster-v2.md` | 0 | 628 |
| `docs/architecture-cluster-write-path.md` | 0 | 433 |
| `docs/architecture-cluster.md` | 0 | 521 |
| `docs/architecture-cold-standby.md` | 0 | 788 |
| `docs/architecture-compaction-multilevel.md` | 0 | 433 |
| `docs/architecture-compaction-optimization.md` | 0 | 1565 |
| `docs/architecture-continuous-aggregates.md` | 0 | 446 |
| `docs/architecture-custom_table_provider.md` | 0 | 454 |
| `docs/architecture-datafusion-integration.md` | 0 | 459 |
| `docs/architecture-directory-structure.md` | 0 | 339 |
| `docs/architecture-flush-checkpoint.md` | 0 | 270 |
| `docs/architecture-leveled-compaction.md` | 0 | 413 |
| `docs/architecture-meta-raft.md` | 0 | 550 |
| `docs/architecture-meta-routing.md` | 0 | 528 |
| `docs/architecture-meta.md` | 0 | 380 |
| `docs/architecture-node-first-path.md` | 0 | 287 |
| `docs/architecture-node-level-standby.md` | 0 | 342 |
| `docs/architecture-node-registration.md` | 0 | 766 |
| `docs/architecture-overview.md` | 0 | 277 |
| `docs/architecture-parquet-writer.md` | 0 | 227 |
| `docs/architecture-scale-in.md` | 0 | 441 |
| `docs/architecture-schema-extraction.md` | 0 | 316 |
| `docs/architecture-sql-table-naming.md` | 0 | 262 |
| `docs/architecture-standby-no-promotion.md` | 0 | 1015 |
| `docs/architecture-wal-design.md` | 0 | 319 |
| `docs/architecture-wal-file-format.md` | 0 | 646 |
| `docs/architecture-wal-simple-solution.md` | 0 | 426 |
| `docs/architecture-warm-standby.md` | 0 | 356 |
| `docs/architecture/architecture-caching-layer.md` | 586 | 0 |
| `docs/architecture/architecture-catalog-structure.md` | 89 | 0 |
| `docs/architecture/architecture-cluster-scale-in.md` | 325 | 0 |
| `docs/architecture/architecture-cluster-v2.md` | 628 | 0 |
| `docs/architecture/architecture-cluster-write-path.md` | 433 | 0 |
| `docs/architecture/architecture-cluster.md` | 521 | 0 |
| `docs/architecture/architecture-cold-standby.md` | 788 | 0 |
| `docs/architecture/architecture-compaction-multilevel.md` | 433 | 0 |
| `docs/architecture/architecture-compaction-optimization.md` | 1565 | 0 |
| `docs/architecture/architecture-continuous-aggregates.md` | 446 | 0 |
| `docs/architecture/architecture-custom_table_provider.md` | 454 | 0 |
| `docs/architecture/architecture-datafusion-integration.md` | 459 | 0 |
| `docs/architecture/architecture-directory-structure.md` | 339 | 0 |
| `docs/architecture/architecture-flush-checkpoint.md` | 270 | 0 |
| `docs/architecture/architecture-leveled-compaction.md` | 413 | 0 |
| `docs/architecture/architecture-meta-raft.md` | 550 | 0 |
| `docs/architecture/architecture-meta-routing.md` | 528 | 0 |
| `docs/architecture/architecture-meta.md` | 380 | 0 |
| `docs/architecture/architecture-node-first-path.md` | 287 | 0 |
| `docs/architecture/architecture-node-level-standby.md` | 342 | 0 |
| `docs/architecture/architecture-node-registration.md` | 766 | 0 |
| `docs/architecture/architecture-overview.md` | 277 | 0 |
| `docs/architecture/architecture-parquet-writer.md` | 227 | 0 |
| `docs/architecture/architecture-scale-in.md` | 441 | 0 |
| `docs/architecture/architecture-schema-extraction.md` | 316 | 0 |
| `docs/architecture/architecture-sql-table-naming.md` | 262 | 0 |
| `docs/architecture/architecture-standby-no-promotion.md` | 1015 | 0 |
| `docs/architecture/architecture-wal-design.md` | 319 | 0 |
| `docs/architecture/architecture-wal-file-format.md` | 646 | 0 |
| `docs/architecture/architecture-wal-simple-solution.md` | 426 | 0 |
| `docs/architecture/architecture-warm-standby.md` | 356 | 0 |
| `docs/benchmark-sled-results.md` | 0 | 189 |
| `docs/benchmarks/benchmark-sled-results.md` | 189 | 0 |
| `docs/changelog-implementation.md` | 0 | 306 |
| `docs/changelog-logging.md` | 0 | 146 |
| `docs/changelog-main.md` | 0 | 81 |
| `docs/changelog-refactoring.md` | 0 | 193 |
| `docs/changelog-schema-config.md` | 0 | 335 |
| `docs/changelog-schema-update.md` | 0 | 218 |
| `docs/changelog-summary.md` | 0 | 145 |
| `docs/changelog-wal-recovery.md` | 0 | 540 |
| `docs/changelog.md` | 0 | 174 |
| `docs/changelogs/changelog-implementation.md` | 306 | 0 |
| `docs/changelogs/changelog-logging.md` | 146 | 0 |
| `docs/changelogs/changelog-main.md` | 81 | 0 |
| `docs/changelogs/changelog-refactoring.md` | 193 | 0 |
| `docs/changelogs/changelog-schema-config.md` | 335 | 0 |
| `docs/changelogs/changelog-schema-update.md` | 218 | 0 |
| `docs/changelogs/changelog-summary.md` | 145 | 0 |
| `docs/changelogs/changelog-wal-recovery.md` | 540 | 0 |
| `docs/changelogs/changelog.md` | 174 | 0 |
| `docs/feature-cardinality-based-sort.md` | 0 | 286 |
| `docs/features-age-based-consolidation.md` | 0 | 1106 |
| `docs/features-compaction-nonpartitioned.md` | 0 | 450 |
| `docs/features-compaction.md` | 0 | 375 |
| `docs/features-date-based-compaction.md` | 0 | 847 |
| `docs/features-flight-usage.md` | 0 | 132 |
| `docs/features-influxdb-compatibility.md` | 0 | 338 |
| `docs/features-schema.md` | 0 | 185 |
| `docs/features-series-cardinality.md` | 0 | 389 |
| `docs/features-series-deduplication.md` | 0 | 567 |
| `docs/features-wal-flush-recovery.md` | 0 | 523 |
| `docs/features/feature-cardinality-based-sort.md` | 286 | 0 |
| `docs/features/features-age-based-consolidation.md` | 1106 | 0 |
| `docs/features/features-compaction-nonpartitioned.md` | 450 | 0 |
| `docs/features/features-compaction.md` | 375 | 0 |
| `docs/features/features-date-based-compaction.md` | 847 | 0 |
| `docs/features/features-flight-usage.md` | 132 | 0 |
| `docs/features/features-influxdb-compatibility.md` | 338 | 0 |
| `docs/features/features-schema.md` | 185 | 0 |
| `docs/features/features-series-cardinality.md` | 389 | 0 |
| `docs/features/features-series-deduplication.md` | 567 | 0 |
| `docs/features/features-wal-flush-recovery.md` | 523 | 0 |
| `docs/guide-admin-console.md` | 0 | 832 |
| `docs/guide-cluster-port-conventions.md` | 0 | 187 |
| `docs/guide-cluster-robustness.md` | 0 | 701 |
| `docs/guide-cluster-scale-in.md` | 0 | 446 |
| `docs/guide-cluster-v2.md` | 0 | 869 |
| `docs/guide-cold-standby.md` | 0 | 787 |
| `docs/guide-compaction-quickstart.md` | 0 | 279 |
| `docs/guide-compaction-testing.md` | 0 | 326 |
| `docs/guide-configuration.md` | 0 | 347 |
| `docs/guide-continuous-aggregates.md` | 0 | 525 |
| `docs/guide-correct-query-examples.md` | 0 | 266 |
| `docs/guide-distributed-write-routing.md` | 0 | 257 |
| `docs/guide-master-slave-failover-demo.md` | 0 | 545 |
| `docs/guide-master-slave-ha-local-testing.md` | 0 | 353 |
| `docs/guide-master-slave-ha.md` | 0 | 537 |
| `docs/guide-query-examples-updated.md` | 0 | 359 |
| `docs/guide-query-examples.md` | 0 | 317 |
| `docs/guide-quick-reference.md` | 0 | 107 |
| `docs/guide-quickstart-main.md` | 0 | 212 |
| `docs/guide-scale-in.md` | 0 | 607 |
| `docs/guide-series-stats.md` | 0 | 205 |
| `docs/guide-sled-benchmark.md` | 0 | 358 |
| `docs/guide-sql-queries.md` | 0 | 555 |
| `docs/guide-warm-standby.md` | 0 | 434 |
| `docs/guides/guide-admin-console.md` | 832 | 0 |
| `docs/guides/guide-cluster-port-conventions.md` | 187 | 0 |
| `docs/guides/guide-cluster-robustness.md` | 701 | 0 |
| `docs/guides/guide-cluster-scale-in.md` | 446 | 0 |
| `docs/guides/guide-cluster-v2.md` | 869 | 0 |
| `docs/guides/guide-cold-standby.md` | 787 | 0 |
| `docs/guides/guide-compaction-quickstart.md` | 279 | 0 |
| `docs/guides/guide-compaction-testing.md` | 326 | 0 |
| `docs/guides/guide-configuration.md` | 347 | 0 |
| `docs/guides/guide-continuous-aggregates.md` | 525 | 0 |
| `docs/guides/guide-correct-query-examples.md` | 266 | 0 |
| `docs/guides/guide-distributed-write-routing.md` | 257 | 0 |
| `docs/guides/guide-master-slave-failover-demo.md` | 545 | 0 |
| `docs/guides/guide-master-slave-ha-local-testing.md` | 353 | 0 |
| `docs/guides/guide-master-slave-ha.md` | 537 | 0 |
| `docs/guides/guide-query-examples-updated.md` | 359 | 0 |
| `docs/guides/guide-query-examples.md` | 317 | 0 |
| `docs/guides/guide-quick-reference.md` | 107 | 0 |
| `docs/guides/guide-quickstart-main.md` | 212 | 0 |
| `docs/guides/guide-scale-in.md` | 607 | 0 |
| `docs/guides/guide-series-stats.md` | 205 | 0 |
| `docs/guides/guide-sled-benchmark.md` | 358 | 0 |
| `docs/guides/guide-sql-queries.md` | 555 | 0 |
| `docs/guides/guide-warm-standby.md` | 434 | 0 |
| `docs/memory-optimization.md` | 0 | 412 |
| `docs/profiling_guide.md` | 0 | 787 |
| `docs/proposal-cluster-edition.md` | 0 | 2067 |
| `docs/proposal-cluster-shared-storage.md` | 0 | 1229 |
| `docs/proposal-master-slave-ha.md` | 0 | 1160 |
| `docs/proposal-master-slave-shared-storage.md` | 0 | 1124 |
| `docs/proposal-query-performance-optimization.md` | 0 | 512 |
| `docs/proposal-write-performance-optimization.md` | 0 | 399 |
| `docs/proposals/proposal-cluster-edition.md` | 2067 | 0 |
| `docs/proposals/proposal-cluster-shared-storage.md` | 1229 | 0 |
| `docs/proposals/proposal-master-slave-ha.md` | 1160 | 0 |
| `docs/proposals/proposal-master-slave-shared-storage.md` | 1124 | 0 |
| `docs/proposals/proposal-query-performance-optimization.md` | 512 | 0 |
| `docs/proposals/proposal-write-performance-optimization.md` | 399 | 0 |
| `docs/quickstart.md` | 0 | 162 |
| `docs/reference/memory-optimization.md` | 412 | 0 |
| `docs/reference/profiling_guide.md` | 787 | 0 |
| `docs/reference/quickstart.md` | 162 | 0 |
| `docs/reference/source-structure.md` | 106 | 0 |
| `docs/roadmap-features.md` | 0 | 143 |
| `docs/roadmap-index-performance.md` | 0 | 890 |
| `docs/roadmap-query-performance-v2.md` | 0 | 837 |
| `docs/roadmap-query-performance.md` | 0 | 1039 |
| `docs/roadmaps/roadmap-features.md` | 143 | 0 |
| `docs/roadmaps/roadmap-index-performance.md` | 890 | 0 |
| `docs/roadmaps/roadmap-query-performance-v2.md` | 837 | 0 |
| `docs/roadmaps/roadmap-query-performance.md` | 1039 | 0 |
| `docs/source-structure.md` | 0 | 106 |
| `docs/troubleshooting-fixes-applied.md` | 0 | 145 |
| `docs/troubleshooting-utf8view-fix.md` | 0 | 147 |
| `docs/troubleshooting/troubleshooting-fixes-applied.md` | 145 | 0 |
| `docs/troubleshooting/troubleshooting-utf8view-fix.md` | 147 | 0 |
| `scripts/README_AUTH_TESTS.md` | 0 | 270 |
| `scripts/README_COMPACTION_TESTS.md` | 0 | 314 |
| `scripts/analyze_bloom_memory.sh` | 0 | 197 |
| `scripts/analyze_memory.sh` | 0 | 175 |
| `scripts/analyze_profile.sh` | 0 | 78 |
| `scripts/auth/README_AUTH_TESTS.md` | 270 | 0 |
| `scripts/auth/test_auth.sh` | 372 | 0 |
| `scripts/auth/test_auth_comprehensive.sh` | 270 | 0 |
| `scripts/auth/test_url_auth.sh` | 311 | 0 |
| `scripts/build/test_build.sh` | 6 | 0 |
| `scripts/cluster-ha/ha_setup_local.sh` | 261 | 0 |
| `scripts/cluster-ha/test_backward_compat.sh` | 476 | 0 |
| `scripts/cluster-ha/test_chaos.sh` | 821 | 0 |
| `scripts/cluster-ha/test_coldstandby_distribution.sh` | 398 | 0 |
| `scripts/cluster-ha/test_ha_local.sh` | 471 | 0 |
| `scripts/cluster-ha/test_scale_in.sh` | 213 | 0 |
| `scripts/cluster-ha/test_scale_in_meta.sh` | 359 | 0 |
| `scripts/compaction/README_COMPACTION_TESTS.md` | 314 | 0 |
| `scripts/compaction/test_compaction.sh` | 174 | 0 |
| `scripts/compaction/test_compaction_aggressive.sh` | 99 | 0 |
| `scripts/compaction/test_compaction_dual_strategy.sh` | 367 | 0 |
| `scripts/compaction/test_compaction_multilevel.sh` | 79 | 0 |
| `scripts/compaction/test_compaction_nonpartitioned.sh` | 104 | 0 |
| `scripts/compaction/test_compaction_quick.sh` | 139 | 0 |
| `scripts/compaction/test_full_compaction_now.sh` | 224 | 0 |
| `scripts/generate_flamegraph.sh` | 0 | 79 |
| `scripts/ha_setup_local.sh` | 0 | 261 |
| `scripts/integration/test_comprehensive.sh` | 129 | 0 |
| `scripts/minio_test.sh` | 0 | 541 |
| `scripts/partition/test_partitions.sh` | 146 | 0 |
| `scripts/performance/analyze_bloom_memory.sh` | 197 | 0 |
| `scripts/performance/analyze_memory.sh` | 175 | 0 |
| `scripts/performance/analyze_profile.sh` | 78 | 0 |
| `scripts/performance/generate_flamegraph.sh` | 79 | 0 |
| `scripts/performance/test_profile_with_load.sh` | 30 | 0 |
| `scripts/protocol/test_admin_console.sh` | 622 | 0 |
| `scripts/protocol/test_mysql.sh` | 276 | 0 |
| `scripts/query/test_group_by_time.sh` | 216 | 0 |
| `scripts/query/test_partition_query.sh` | 44 | 0 |
| `scripts/query/test_query_fault_tolerance.sh` | 236 | 0 |
| `scripts/read_parquet.py` | 0 | 90 |
| `scripts/run_fuzz.sh` | 0 | 103 |
| `scripts/test_admin_console.sh` | 0 | 622 |
| `scripts/test_auth.sh` | 0 | 372 |
| `scripts/test_auth_comprehensive.sh` | 0 | 270 |
| `scripts/test_backward_compat.sh` | 0 | 476 |
| `scripts/test_build.sh` | 0 | 6 |
| `scripts/test_chaos.sh` | 0 | 821 |
| `scripts/test_coldstandby_distribution.sh` | 0 | 398 |
| `scripts/test_compaction.sh` | 0 | 174 |
| `scripts/test_compaction_aggressive.sh` | 0 | 99 |
| `scripts/test_compaction_dual_strategy.sh` | 0 | 367 |
| `scripts/test_compaction_multilevel.sh` | 0 | 79 |
| `scripts/test_compaction_nonpartitioned.sh` | 0 | 104 |
| `scripts/test_compaction_quick.sh` | 0 | 139 |
| `scripts/test_comprehensive.sh` | 0 | 129 |
| `scripts/test_full_compaction_now.sh` | 0 | 224 |
| `scripts/test_group_by_time.sh` | 0 | 216 |
| `scripts/test_ha_local.sh` | 0 | 471 |
| `scripts/test_mysql.sh` | 0 | 276 |
| `scripts/test_partition_query.sh` | 0 | 44 |
| `scripts/test_partitions.sh` | 0 | 146 |
| `scripts/test_profile_with_load.sh` | 0 | 30 |
| `scripts/test_query_fault_tolerance.sh` | 0 | 236 |
| `scripts/test_scale_in.sh` | 0 | 213 |
| `scripts/test_scale_in_meta.sh` | 0 | 359 |
| `scripts/test_schema_bloom_config.sh` | 0 | 181 |
| `scripts/test_schemas.sh` | 0 | 68 |
| `scripts/test_series_stats.sh` | 0 | 170 |
| `scripts/test_url_auth.sh` | 0 | 311 |
| `scripts/test_write.sh` | 0 | 64 |
| `scripts/utils/minio_test.sh` | 541 | 0 |
| `scripts/utils/read_parquet.py` | 90 | 0 |
| `scripts/utils/run_fuzz.sh` | 103 | 0 |
| `scripts/verify_schema_config.sh` | 0 | 110 |
| `scripts/write-schema/test_schema_bloom_config.sh` | 181 | 0 |
| `scripts/write-schema/test_schemas.sh` | 68 | 0 |
| `scripts/write-schema/test_series_stats.sh` | 170 | 0 |
| `scripts/write-schema/test_write.sh` | 64 | 0 |
| `scripts/write-schema/verify_schema_config.sh` | 110 | 0 |

### `f310a43` — chore: remove obsolete docs and scripts

2026-02-06 · M8 · 6 files · +0/−1,637 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/features/features-series-deduplication.md` | 0 | 567 |
| `docs/reference/memory-optimization.md` | 0 | 412 |
| `scripts/performance/analyze_bloom_memory.sh` | 0 | 197 |
| `scripts/write-schema/test_schema_bloom_config.sh` | 0 | 181 |
| `scripts/write-schema/test_series_stats.sh` | 0 | 170 |
| `scripts/write-schema/verify_schema_config.sh` | 0 | 110 |

### `e5d9d68` — refactor(router): remove deprecated partition_nodes cache

2026-02-06 · M8 · 6 files · +7/−458 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/router.rs` | 1 | 23 |
| `src/router/config.rs` | 0 | 20 |
| `src/router/shard_cache/mod.rs` | 2 | 13 |
| `src/router/shard_cache/partition_cache.rs` | 0 | 381 |
| `src/router/shard_cache/table_routing.rs` | 3 | 6 |
| `src/router/shard_cache/types.rs` | 1 | 15 |

### `21a4447` — refactor: consolidate Flight protocol types into shared module

2026-02-06 · M8 · 9 files · +92/−159 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 6 | 81 |
| `src/flight_protocol.rs` | 76 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 1 | 0 |
| `src/query/query_engine/execution.rs` | 1 | 16 |
| `src/query/query_engine/mod.rs` | 1 | 1 |
| `src/router/client/data_node_pool.rs` | 1 | 44 |
| `src/router/client/mod.rs` | 2 | 1 |
| `src/router/shard_cache/types.rs` | 3 | 16 |

### `8abda76` — refactor: reorganize project directory structure

2026-02-06 · M8 · 166 files · +26,931/−27,071 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.dockerignore` | 0 | 30 |
| `CLAUDE_CONTEXT.md` | 0 | 303 |
| `Cargo.toml` | 3 | 3 |
| `Dockerfile` | 0 | 62 |
| `admin-frontend/Cargo.lock` | 0 | 2059 |
| `admin-frontend/Cargo.toml` | 0 | 39 |
| `admin-frontend/Trunk.toml` | 0 | 15 |
| `admin-frontend/index.html` | 0 | 14 |
| `admin-frontend/src/api/client.rs` | 0 | 259 |
| `admin-frontend/src/api/events.rs` | 0 | 101 |
| `admin-frontend/src/api/mod.rs` | 0 | 6 |
| `admin-frontend/src/app.rs` | 0 | 59 |
| `admin-frontend/src/components/layout.rs` | 0 | 131 |
| `admin-frontend/src/components/mod.rs` | 0 | 13 |
| `admin-frontend/src/components/node_card.rs` | 0 | 88 |
| `admin-frontend/src/components/progress.rs` | 0 | 117 |
| `admin-frontend/src/components/status_badge.rs` | 0 | 85 |
| `admin-frontend/src/components/toast.rs` | 0 | 68 |
| `admin-frontend/src/lib.rs` | 0 | 21 |
| `admin-frontend/src/pages/dashboard.rs` | 0 | 371 |
| `admin-frontend/src/pages/events.rs` | 0 | 217 |
| `admin-frontend/src/pages/mod.rs` | 0 | 7 |
| `admin-frontend/src/pages/nodes.rs` | 0 | 161 |
| `admin-frontend/src/pages/scale_in.rs` | 0 | 384 |
| `admin-frontend/src/pages/tables.rs` | 0 | 210 |
| `admin-frontend/src/state/mod.rs` | 0 | 122 |
| `admin-frontend/style/main.css` | 0 | 187 |
| `admin-frontend/style/tailwind.css` | 0 | 181 |
| `admin-frontend/tailwind.config.js` | 0 | 26 |
| `admin-types/Cargo.toml` | 0 | 10 |
| `admin-types/src/cluster.rs` | 0 | 94 |
| `admin-types/src/events.rs` | 0 | 145 |
| `admin-types/src/lib.rs` | 0 | 17 |
| `admin-types/src/nodes.rs` | 0 | 219 |
| `admin-types/src/scale_in.rs` | 0 | 202 |
| `admin-types/src/tables.rs` | 0 | 125 |
| `admin/frontend/Cargo.lock` | 2059 | 0 |
| `admin/frontend/Cargo.toml` | 39 | 0 |
| `admin/frontend/Trunk.toml` | 15 | 0 |
| `admin/frontend/index.html` | 14 | 0 |
| `admin/frontend/src/api/client.rs` | 259 | 0 |
| `admin/frontend/src/api/events.rs` | 101 | 0 |
| `admin/frontend/src/api/mod.rs` | 6 | 0 |
| `admin/frontend/src/app.rs` | 59 | 0 |
| `admin/frontend/src/components/layout.rs` | 131 | 0 |
| `admin/frontend/src/components/mod.rs` | 13 | 0 |
| `admin/frontend/src/components/node_card.rs` | 88 | 0 |
| `admin/frontend/src/components/progress.rs` | 117 | 0 |
| `admin/frontend/src/components/status_badge.rs` | 85 | 0 |
| `admin/frontend/src/components/toast.rs` | 68 | 0 |
| `admin/frontend/src/lib.rs` | 21 | 0 |
| `admin/frontend/src/pages/dashboard.rs` | 371 | 0 |
| `admin/frontend/src/pages/events.rs` | 217 | 0 |
| `admin/frontend/src/pages/mod.rs` | 7 | 0 |
| `admin/frontend/src/pages/nodes.rs` | 161 | 0 |
| `admin/frontend/src/pages/scale_in.rs` | 384 | 0 |
| `admin/frontend/src/pages/tables.rs` | 210 | 0 |
| `admin/frontend/src/state/mod.rs` | 122 | 0 |
| `admin/frontend/style/main.css` | 187 | 0 |
| `admin/frontend/style/tailwind.css` | 181 | 0 |
| `admin/frontend/tailwind.config.js` | 26 | 0 |
| `admin/types/Cargo.toml` | 10 | 0 |
| `admin/types/src/cluster.rs` | 94 | 0 |
| `admin/types/src/events.rs` | 145 | 0 |
| `admin/types/src/lib.rs` | 17 | 0 |
| `admin/types/src/nodes.rs` | 219 | 0 |
| `admin/types/src/scale_in.rs` | 202 | 0 |
| `admin/types/src/tables.rs` | 125 | 0 |
| `build.rs` | 3 | 3 |
| `config.examples.toml` | 0 | 480 |
| `config/config.examples.toml` | 480 | 0 |
| `config/mynadb.service` | 26 | 0 |
| `deploy/docker-compose.cluster.yaml` | 3 | 3 |
| `deploy/docker/.dockerignore` | 30 | 0 |
| `deploy/docker/Dockerfile` | 62 | 0 |
| `docs/plans/CLAUDE_CONTEXT.md` | 303 | 0 |
| `docs/plans/adaptive-dancing-summit.md` | 329 | 0 |
| `docs/plans/cold-standby-cluster-plan.md` | 402 | 0 |
| `docs/plans/large-file-consolidation.md` | 216 | 0 |
| `examples/sled_10m_bench.rs` | 0 | 140 |
| `mynadb.service` | 0 | 26 |
| `plans/adaptive-dancing-summit.md` | 0 | 329 |
| `plans/cold-standby-cluster-plan.md` | 0 | 402 |
| `plans/large-file-consolidation.md` | 0 | 216 |
| `proto/meta.proto` | 0 | 718 |
| `src/proto/meta.proto` | 718 | 0 |
| `temp_web/README.md` | 0 | 88 |
| `temp_web/css/styles.css` | 0 | 444 |
| `temp_web/data_generator.sh` | 0 | 65 |
| `temp_web/index.html` | 0 | 195 |
| `temp_web/js/api.js` | 0 | 174 |
| `temp_web/js/app.js` | 0 | 311 |
| `temp_web/js/charts.js` | 0 | 435 |
| `temp_web/js/export.js` | 0 | 182 |
| `temp_web/proxy_server.py` | 0 | 125 |
| `web/dashboard/README.md` | 88 | 0 |
| `web/dashboard/css/styles.css` | 444 | 0 |
| `web/dashboard/data_generator.sh` | 65 | 0 |
| `web/dashboard/index.html` | 195 | 0 |
| `web/dashboard/js/api.js` | 174 | 0 |
| `web/dashboard/js/app.js` | 311 | 0 |
| `web/dashboard/js/charts.js` | 435 | 0 |
| `web/dashboard/js/export.js` | 182 | 0 |
| `web/dashboard/proxy_server.py` | 125 | 0 |
| `web/portal/README.md` | 133 | 0 |
| `web/portal/css/style.css` | 1704 | 0 |
| `web/portal/docs/api.html` | 689 | 0 |
| `web/portal/docs/architecture.html` | 515 | 0 |
| `web/portal/docs/caching.html` | 838 | 0 |
| `web/portal/docs/cluster.html` | 574 | 0 |
| `web/portal/docs/compaction.html` | 704 | 0 |
| `web/portal/docs/configuration.html` | 532 | 0 |
| `web/portal/docs/data-model.html` | 572 | 0 |
| `web/portal/docs/examples.html` | 673 | 0 |
| `web/portal/docs/faq.html` | 387 | 0 |
| `web/portal/docs/flight-api.html` | 685 | 0 |
| `web/portal/docs/guides.html` | 346 | 0 |
| `web/portal/docs/high-availability.html` | 598 | 0 |
| `web/portal/docs/index.html` | 342 | 0 |
| `web/portal/docs/installation.html` | 705 | 0 |
| `web/portal/docs/line-protocol.html` | 590 | 0 |
| `web/portal/docs/mysql-protocol.html` | 414 | 0 |
| `web/portal/docs/object-store.html` | 489 | 0 |
| `web/portal/docs/quickstart.html` | 362 | 0 |
| `web/portal/docs/retention.html` | 631 | 0 |
| `web/portal/docs/sql-queries.html` | 617 | 0 |
| `web/portal/docs/storage.html` | 514 | 0 |
| `web/portal/docs/testing.html` | 1199 | 0 |
| `web/portal/docs/troubleshooting.html` | 303 | 0 |
| `web/portal/images/favicon.svg` | 40 | 0 |
| `web/portal/images/logo-horizontal.svg` | 60 | 0 |
| `web/portal/images/logo-icon.svg` | 54 | 0 |
| `web/portal/images/logo.svg` | 142 | 0 |
| `web/portal/index.html` | 713 | 0 |
| `web/portal/js/main.js` | 459 | 0 |
| `website/README.md` | 0 | 133 |
| `website/css/style.css` | 0 | 1704 |
| `website/docs/api.html` | 0 | 689 |
| `website/docs/architecture.html` | 0 | 515 |
| `website/docs/caching.html` | 0 | 838 |
| `website/docs/cluster.html` | 0 | 574 |
| `website/docs/compaction.html` | 0 | 704 |
| `website/docs/configuration.html` | 0 | 532 |
| `website/docs/data-model.html` | 0 | 572 |
| `website/docs/examples.html` | 0 | 673 |
| `website/docs/faq.html` | 0 | 387 |
| `website/docs/flight-api.html` | 0 | 685 |
| `website/docs/guides.html` | 0 | 346 |
| `website/docs/high-availability.html` | 0 | 598 |
| `website/docs/index.html` | 0 | 342 |
| `website/docs/installation.html` | 0 | 705 |
| `website/docs/line-protocol.html` | 0 | 590 |
| `website/docs/mysql-protocol.html` | 0 | 414 |
| `website/docs/object-store.html` | 0 | 489 |
| `website/docs/quickstart.html` | 0 | 362 |
| `website/docs/retention.html` | 0 | 631 |
| `website/docs/sql-queries.html` | 0 | 617 |
| `website/docs/storage.html` | 0 | 514 |
| `website/docs/testing.html` | 0 | 1199 |
| `website/docs/troubleshooting.html` | 0 | 303 |
| `website/images/favicon.svg` | 0 | 40 |
| `website/images/logo-horizontal.svg` | 0 | 60 |
| `website/images/logo-icon.svg` | 0 | 54 |
| `website/images/logo.svg` | 0 | 142 |
| `website/index.html` | 0 | 713 |
| `website/js/main.js` | 0 | 459 |

### `aba5496` — chore: set mynadb as default binary for cargo run

2026-02-06 · M8 · 1 files · +1/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 1 | 0 |

### `7de066c` — refactor: reduce deep nesting across multiple modules

2026-02-06 · M8 · 5 files · +632/−592 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor/background.rs` | 47 | 63 |
| `src/meta/scale_in/executor.rs` | 84 | 70 |
| `src/query/catalog/memory_manager.rs` | 86 | 100 |
| `src/router/query_coordinator/execution.rs` | 359 | 310 |
| `src/storage/wal.rs` | 56 | 49 |

### `0710e05` — refactor: reduce deep nesting across multiple modules

2026-02-06 · M8 · 43 files · +3,014/−3,014 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/meta_node/cluster.rs` | 70 | 0 |
| `src/meta/meta_node/commands.rs` | 96 | 0 |
| `src/meta/meta_node/distribution.rs` | 73 | 0 |
| `src/meta/meta_node/elections.rs` | 59 | 0 |
| `src/meta/meta_node/lifecycle.rs` | 299 | 0 |
| `src/meta/meta_node/locks.rs` | 60 | 0 |
| `src/meta/meta_node/mod.rs` | 381 | 0 |
| `src/meta/meta_node/nodes.rs` | 45 | 0 |
| `src/meta/meta_node/reads.rs` | 117 | 0 |
| `src/meta/meta_node/scale_in.rs` | 109 | 0 |
| `src/meta/meta_node/tables.rs` | 261 | 0 |
| `src/meta/mod.rs` | 1 | 0 |
| `src/meta/recovery/mod.rs` | 1 | 1 |
| `src/meta/scale_in/executor.rs` | 1 | 1 |
| `src/meta/scale_in/mod.rs` | 1 | 1 |
| `src/meta/service/grpc.rs` | 1 | 1 |
| `src/meta/service/http.rs` | 1 | 1 |
| `src/meta/service/meta_node/cluster.rs` | 0 | 70 |
| `src/meta/service/meta_node/commands.rs` | 0 | 96 |
| `src/meta/service/meta_node/distribution.rs` | 0 | 73 |
| `src/meta/service/meta_node/elections.rs` | 0 | 59 |
| `src/meta/service/meta_node/lifecycle.rs` | 0 | 299 |
| `src/meta/service/meta_node/locks.rs` | 0 | 60 |
| `src/meta/service/meta_node/mod.rs` | 0 | 381 |
| `src/meta/service/meta_node/nodes.rs` | 0 | 45 |
| `src/meta/service/meta_node/reads.rs` | 0 | 117 |
| `src/meta/service/meta_node/scale_in.rs` | 0 | 109 |
| `src/meta/service/meta_node/tables.rs` | 0 | 261 |
| `src/meta/service/mod.rs` | 1 | 2 |
| `src/node/meta.rs` | 1 | 1 |
| `src/query/providers/mod.rs` | 2 | 2 |
| `src/query/providers/parquet/config.rs` | 73 | 0 |
| `src/query/providers/parquet/discovery.rs` | 224 | 0 |
| `src/query/providers/parquet/exec_plan.rs` | 203 | 0 |
| `src/query/providers/parquet/filters.rs` | 295 | 0 |
| `src/query/providers/parquet/mod.rs` | 382 | 0 |
| `src/query/providers/parquet/reader.rs` | 257 | 0 |
| `src/query/providers/table_provider/config.rs` | 0 | 73 |
| `src/query/providers/table_provider/discovery.rs` | 0 | 224 |
| `src/query/providers/table_provider/exec_plan.rs` | 0 | 203 |
| `src/query/providers/table_provider/filters.rs` | 0 | 295 |
| `src/query/providers/table_provider/mod.rs` | 0 | 382 |
| `src/query/providers/table_provider/reader.rs` | 0 | 257 |

### `1619eca` — fix(router): pre-fetch table owners on init and cleanup empty tables

2026-02-06 · M8 · 2 files · +103/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/retention_manager.rs` | 63 | 1 |
| `src/router/cache_invalidation.rs` | 40 | 0 |

### `c49b32e` — feat(meta): add ephemeral table ownership with TTL

2026-02-06 · M8 · 16 files · +1,370/−225 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/meta/client/meta_client.rs` | 133 | 78 |
| `src/meta/config.rs` | 83 | 0 |
| `src/meta/meta_node/lifecycle.rs` | 54 | 6 |
| `src/meta/meta_node/tables.rs` | 69 | 0 |
| `src/meta/raft/state_machine/mod.rs` | 12 | 1 |
| `src/meta/raft/state_machine/table.rs` | 318 | 47 |
| `src/meta/raft/state_machine/tests.rs` | 3 | 3 |
| `src/meta/service/grpc.rs` | 114 | 57 |
| `src/meta/types/command.rs` | 55 | 1 |
| `src/meta/types/event.rs` | 8 | 0 |
| `src/meta/types/mod.rs` | 1 | 1 |
| `src/meta/types/table.rs` | 406 | 22 |
| `src/proto/meta.proto` | 54 | 2 |
| `src/router/cache_invalidation.rs` | 51 | 0 |
| `src/router/query_coordinator/routing.rs` | 8 | 6 |
| `src/router/write_router.rs` | 1 | 1 |

## M9 — Operator console, ecosystem, and advanced TSDB features

50 commits.

### `c5ef091` — feat(admin): add React-based admin dashboard

2026-02-06 · M9 · 58 files · +12,221/−37 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/.gitignore` | 26 | 0 |
| `admin-ui/README.md` | 480 | 0 |
| `admin-ui/components.json` | 20 | 0 |
| `admin-ui/eslint.config.js` | 29 | 0 |
| `admin-ui/index.html` | 13 | 0 |
| `admin-ui/package-lock.json` | 5688 | 0 |
| `admin-ui/package.json` | 52 | 0 |
| `admin-ui/postcss.config.js` | 6 | 0 |
| `admin-ui/public/favicon.svg` | 10 | 0 |
| `admin-ui/src/App.tsx` | 45 | 0 |
| `admin-ui/src/components/layout/header.tsx` | 70 | 0 |
| `admin-ui/src/components/layout/main-layout.tsx` | 20 | 0 |
| `admin-ui/src/components/layout/sidebar.tsx` | 110 | 0 |
| `admin-ui/src/components/ui/alert.tsx` | 63 | 0 |
| `admin-ui/src/components/ui/badge.tsx` | 40 | 0 |
| `admin-ui/src/components/ui/button.tsx` | 56 | 0 |
| `admin-ui/src/components/ui/card.tsx` | 79 | 0 |
| `admin-ui/src/components/ui/dialog.tsx` | 120 | 0 |
| `admin-ui/src/components/ui/dropdown-menu.tsx` | 198 | 0 |
| `admin-ui/src/components/ui/input.tsx` | 22 | 0 |
| `admin-ui/src/components/ui/label.tsx` | 24 | 0 |
| `admin-ui/src/components/ui/progress.tsx` | 26 | 0 |
| `admin-ui/src/components/ui/scroll-area.tsx` | 19 | 0 |
| `admin-ui/src/components/ui/select.tsx` | 158 | 0 |
| `admin-ui/src/components/ui/separator.tsx` | 29 | 0 |
| `admin-ui/src/components/ui/skeleton.tsx` | 15 | 0 |
| `admin-ui/src/components/ui/table.tsx` | 117 | 0 |
| `admin-ui/src/components/ui/tabs.tsx` | 53 | 0 |
| `admin-ui/src/components/ui/toast.tsx` | 128 | 0 |
| `admin-ui/src/components/ui/tooltip.tsx` | 28 | 0 |
| `admin-ui/src/hooks/use-cluster.ts` | 18 | 0 |
| `admin-ui/src/hooks/use-events.ts` | 69 | 0 |
| `admin-ui/src/hooks/use-nodes.ts` | 104 | 0 |
| `admin-ui/src/hooks/use-scale-in.ts` | 55 | 0 |
| `admin-ui/src/hooks/use-tables.ts` | 24 | 0 |
| `admin-ui/src/index.css` | 59 | 0 |
| `admin-ui/src/lib/api.ts` | 222 | 0 |
| `admin-ui/src/lib/types.ts` | 283 | 0 |
| `admin-ui/src/lib/utils.ts` | 53 | 0 |
| `admin-ui/src/main.tsx` | 22 | 0 |
| `admin-ui/src/pages/dashboard.tsx` | 292 | 0 |
| `admin-ui/src/pages/data-operations.tsx` | 326 | 0 |
| `admin-ui/src/pages/events.tsx` | 332 | 0 |
| `admin-ui/src/pages/meta-operations.tsx` | 437 | 0 |
| `admin-ui/src/pages/nodes/[id].tsx` | 441 | 0 |
| `admin-ui/src/pages/nodes/index.tsx` | 239 | 0 |
| `admin-ui/src/pages/router-operations.tsx` | 215 | 0 |
| `admin-ui/src/pages/scale-in.tsx` | 664 | 0 |
| `admin-ui/src/pages/tables.tsx` | 330 | 0 |
| `admin-ui/src/vite-env.d.ts` | 1 | 0 |
| `admin-ui/tailwind.config.js` | 74 | 0 |
| `admin-ui/tsconfig.json` | 30 | 0 |
| `admin-ui/tsconfig.node.json` | 22 | 0 |
| `admin-ui/vite.config.ts` | 36 | 0 |
| `dev/Procfile` | 3 | 1 |
| `dev/configs/cluster/admin.toml` | 21 | 0 |
| `dev/start-cluster.sh` | 11 | 4 |
| `src/admin/static_files.rs` | 94 | 32 |

### `d8ae7ac` — refactor(admin): remove Leptos frontend, fix API response format

2026-02-06 · M9 · 36 files · +160/−5,939 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 0 | 10 |
| `Cargo.toml` | 1 | 3 |
| `admin/frontend/Cargo.lock` | 0 | 2059 |
| `admin/frontend/Cargo.toml` | 0 | 39 |
| `admin/frontend/Trunk.toml` | 0 | 15 |
| `admin/frontend/index.html` | 0 | 14 |
| `admin/frontend/src/api/client.rs` | 0 | 259 |
| `admin/frontend/src/api/events.rs` | 0 | 101 |
| `admin/frontend/src/api/mod.rs` | 0 | 6 |
| `admin/frontend/src/app.rs` | 0 | 59 |
| `admin/frontend/src/components/layout.rs` | 0 | 131 |
| `admin/frontend/src/components/mod.rs` | 0 | 13 |
| `admin/frontend/src/components/node_card.rs` | 0 | 88 |
| `admin/frontend/src/components/progress.rs` | 0 | 117 |
| `admin/frontend/src/components/status_badge.rs` | 0 | 85 |
| `admin/frontend/src/components/toast.rs` | 0 | 68 |
| `admin/frontend/src/lib.rs` | 0 | 21 |
| `admin/frontend/src/pages/dashboard.rs` | 0 | 371 |
| `admin/frontend/src/pages/events.rs` | 0 | 217 |
| `admin/frontend/src/pages/mod.rs` | 0 | 7 |
| `admin/frontend/src/pages/nodes.rs` | 0 | 161 |
| `admin/frontend/src/pages/scale_in.rs` | 0 | 384 |
| `admin/frontend/src/pages/tables.rs` | 0 | 210 |
| `admin/frontend/src/state/mod.rs` | 0 | 122 |
| `admin/frontend/style/main.css` | 0 | 187 |
| `admin/frontend/style/tailwind.css` | 0 | 181 |
| `admin/frontend/tailwind.config.js` | 0 | 26 |
| `admin/types/Cargo.toml` | 0 | 10 |
| `admin/types/src/cluster.rs` | 0 | 94 |
| `admin/types/src/events.rs` | 0 | 145 |
| `admin/types/src/lib.rs` | 0 | 17 |
| `admin/types/src/nodes.rs` | 0 | 219 |
| `admin/types/src/scale_in.rs` | 0 | 202 |
| `admin/types/src/tables.rs` | 0 | 125 |
| `src/admin/api.rs` | 149 | 95 |
| `src/admin/static_files.rs` | 10 | 78 |

### `8d32ddc` — refactor(admin): consolidate and optimize API handlers

2026-02-06 · M9 · 1 files · +35/−63 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 35 | 63 |

### `b061632` — refactor(admin): consolidate routes into api.rs

2026-02-06 · M9 · 2 files · +10/−11 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 6 | 3 |
| `src/admin/mod.rs` | 4 | 8 |

### `338f972` — feat(dev): add admin console to coldstandby configs

2026-02-06 · M9 · 5 files · +67/−5 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.coldstandby` | 4 | 0 |
| `dev/Procfile.coldstandby-simple` | 8 | 1 |
| `dev/configs/coldstandby-simple/admin.toml` | 19 | 0 |
| `dev/configs/coldstandby/admin.toml` | 26 | 0 |
| `dev/start-coldstandby.sh` | 10 | 4 |

### `173ca32` — chore(dev): use release binary in coldstandby-simple Procfile

2026-02-06 · M9 · 1 files · +7/−7 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.coldstandby-simple` | 7 | 7 |

### `3537ee1` — feat(cache): implement columnar in-memory optimizations (Phase 2-4)

2026-02-06 · M9 · 12 files · +6,110/−113 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture/columnar-in-memory-analysis.md` | 1483 | 0 |
| `docs/architecture/streaming-queries-internals.md` | 1542 | 0 |
| `docs/proposals/proposal-sota-tsdb-features.md` | 2106 | 0 |
| `src/query/cache/dictionary_encoding.rs` | 286 | 0 |
| `src/query/cache/hot_partition_cache.rs` | 74 | 18 |
| `src/query/cache/mod.rs` | 2 | 0 |
| `src/query/cache/predicate_eval.rs` | 223 | 0 |
| `src/query/providers/memtable_provider.rs` | 17 | 81 |
| `src/query/providers/parquet/exec_plan.rs` | 263 | 3 |
| `src/query/providers/parquet/mod.rs` | 36 | 5 |
| `src/query/query_engine/registration.rs` | 9 | 4 |
| `src/storage/cache/mod.rs` | 69 | 2 |

### `bc360b4` — docs(grafana): add comprehensive Grafana integration implementation plan

2026-02-06 · M9 · 1 files · +1,014/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/implementation/grafana-integration-plan.md` | 1014 | 0 |

### `bca1ee1` — feat(grafana): implement official Grafana datasource plugin

2026-02-06 · M9 · 14 files · +1,263/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `grafana-mynadb-datasource/.gitignore` | 7 | 0 |
| `grafana-mynadb-datasource/README.md` | 204 | 0 |
| `grafana-mynadb-datasource/package.json` | 29 | 0 |
| `grafana-mynadb-datasource/src/ConfigEditor.tsx` | 46 | 0 |
| `grafana-mynadb-datasource/src/QueryEditor.tsx` | 111 | 0 |
| `grafana-mynadb-datasource/src/datasource.ts` | 195 | 0 |
| `grafana-mynadb-datasource/src/img/logo.svg` | 21 | 0 |
| `grafana-mynadb-datasource/src/module.ts` | 11 | 0 |
| `grafana-mynadb-datasource/src/plugin.json` | 44 | 0 |
| `grafana-mynadb-datasource/src/types.ts` | 21 | 0 |
| `grafana-mynadb-datasource/tsconfig.json` | 11 | 0 |
| `src/services/api.rs` | 6 | 0 |
| `src/services/grafana.rs` | 556 | 0 |
| `src/services/mod.rs` | 1 | 0 |

### `d6a65cd` — feat(observability): implement query tracing and profiling

2026-02-06 · M9 · 16 files · +1,819/−36 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 164 | 9 |
| `Cargo.toml` | 4 | 0 |
| `config.tracing.toml` | 46 | 0 |
| `docs/feature-query-tracing.md` | 331 | 0 |
| `scripts/test_tracing.sh` | 66 | 0 |
| `src/config.rs` | 3 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/node/standalone.rs` | 26 | 0 |
| `src/observability/config.rs` | 71 | 0 |
| `src/observability/init.rs` | 82 | 0 |
| `src/observability/mod.rs` | 11 | 0 |
| `src/observability/query_tracer.rs` | 277 | 0 |
| `src/observability/slow_query_logger.rs` | 230 | 0 |
| `src/observability/timeline.rs` | 254 | 0 |
| `src/query/executor.rs` | 143 | 27 |
| `src/services/api.rs` | 110 | 0 |

### `b0ef438` — feat(admin): implement events tab with real-time streaming

2026-02-07 · M9 · 8 files · +505/−50 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/lib/api.ts` | 24 | 5 |
| `admin-ui/src/lib/types.ts` | 11 | 8 |
| `admin-ui/src/pages/events.tsx` | 29 | 18 |
| `src/admin/api.rs` | 30 | 7 |
| `src/admin/events.rs` | 308 | 0 |
| `src/admin/mod.rs` | 21 | 1 |
| `src/admin/state.rs` | 43 | 0 |
| `src/admin/websocket.rs` | 39 | 11 |

### `80415ae` — feat(admin): add enhanced event filtering and statistics

2026-02-07 · M9 · 6 files · +399/−48 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/hooks/use-events.ts` | 17 | 1 |
| `admin-ui/src/lib/api.ts` | 10 | 0 |
| `admin-ui/src/lib/types.ts` | 9 | 2 |
| `admin-ui/src/pages/events.tsx` | 180 | 36 |
| `src/admin/api.rs` | 72 | 4 |
| `src/admin/state.rs` | 111 | 5 |

### `4acc8cb` — feat(admin): add event stream controls and export

2026-02-07 · M9 · 2 files · +161/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/hooks/use-events.ts` | 29 | 1 |
| `admin-ui/src/pages/events.tsx` | 132 | 3 |

### `3b274b0` — feat(admin): add browser notifications and event breakdown

2026-02-07 · M9 · 2 files · +101/−11 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/hooks/use-events.ts` | 59 | 2 |
| `admin-ui/src/pages/events.tsx` | 42 | 9 |

### `1777f05` — feat(admin): add SQL Query Console and Storage Browser

2026-02-07 · M9 · 14 files · +4,878/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/README.md` | 134 | 1 |
| `admin-ui/package-lock.json` | 65 | 0 |
| `admin-ui/package.json` | 1 | 0 |
| `admin-ui/src/App.tsx` | 4 | 0 |
| `admin-ui/src/components/layout/sidebar.tsx` | 4 | 0 |
| `admin-ui/src/hooks/use-query.ts` | 175 | 0 |
| `admin-ui/src/hooks/use-storage.ts` | 74 | 0 |
| `admin-ui/src/lib/api.ts` | 109 | 0 |
| `admin-ui/src/lib/types.ts` | 127 | 0 |
| `admin-ui/src/pages/query.tsx` | 780 | 0 |
| `admin-ui/src/pages/storage.tsx` | 996 | 0 |
| `docs/admin-ui-query-storage-guide.md` | 1367 | 0 |
| `docs/admin-ui-quick-reference.md` | 421 | 0 |
| `docs/implementation-summary.md` | 621 | 0 |

### `fa27bc7` — feat(metrics): implement comprehensive metrics dashboard

2026-02-07 · M9 · 13 files · +1,414/−62 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/metrics/test_metrics_dashboard.sh` | 246 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 1 | 0 |
| `src/metrics/collector.rs` | 475 | 0 |
| `src/metrics/mod.rs` | 12 | 0 |
| `src/metrics/prometheus.rs` | 206 | 0 |
| `src/metrics/types.rs` | 209 | 0 |
| `src/metrics/updater.rs` | 203 | 0 |
| `src/node/standalone.rs` | 9 | 0 |
| `src/query/cache/metadata_cache.rs` | 4 | 0 |
| `src/query/cache/query_cache.rs` | 5 | 0 |
| `src/services/api.rs` | 40 | 62 |
| `src/storage/wal.rs` | 3 | 0 |

### `2c39a70` — fix(admin): add missing meta-operations API endpoints

2026-02-07 · M9 · 1 files · +67/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 67 | 0 |

### `d597b71` — fix(admin): resolve compilation errors in admin API

2026-02-07 · M9 · 3 files · +41/−12 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 39 | 11 |
| `src/main.rs` | 1 | 0 |
| `src/metrics/updater.rs` | 1 | 1 |

### `a5a551c` — feat(auth): implement multi-tenancy RBAC authentication

2026-02-07 · M9 · 20 files · +3,660/−200 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 30 | 0 |
| `Cargo.toml` | 3 | 0 |
| `config.auth.toml` | 109 | 0 |
| `examples/users.toml` | 47 | 0 |
| `scripts/test_auth_rbac.sh` | 551 | 0 |
| `src/auth/context.rs` | 205 | 0 |
| `src/auth/mod.rs` | 82 | 0 |
| `src/auth/permission.rs` | 141 | 0 |
| `src/auth/policy.rs` | 594 | 0 |
| `src/auth/role.rs` | 96 | 0 |
| `src/auth/store.rs` | 192 | 0 |
| `src/auth/user.rs` | 466 | 0 |
| `src/config.rs` | 31 | 26 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 33 | 0 |
| `src/node/common.rs` | 31 | 3 |
| `src/node/standalone.rs` | 21 | 10 |
| `src/services/auth.rs` | 580 | 121 |
| `src/services/flight_service.rs` | 267 | 34 |
| `src/services/mysql_service.rs` | 180 | 6 |

### `251ec07` — feat(prometheus): implement Prometheus Remote Storage API

2026-02-07 · M9 · 10 files · +1,256/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 1 | 0 |
| `Cargo.toml` | 3 | 0 |
| `scripts/test_prometheus.sh` | 281 | 0 |
| `src/auth/policy.rs` | 14 | 0 |
| `src/services/api.rs` | 41 | 0 |
| `src/services/mod.rs` | 1 | 0 |
| `src/services/prometheus/mod.rs` | 22 | 0 |
| `src/services/prometheus/remote_read.rs` | 426 | 0 |
| `src/services/prometheus/remote_write.rs` | 235 | 0 |
| `src/services/prometheus/types.rs` | 232 | 0 |

### `bd54c5c` — feat(influxql): implement InfluxQL query language support

2026-02-07 · M9 · 11 files · +3,556/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `scripts/test_influxql.sh` | 162 | 0 |
| `src/auth/policy.rs` | 8 | 1 |
| `src/query/influxql/README.md` | 205 | 0 |
| `src/query/influxql/ast.rs` | 280 | 0 |
| `src/query/influxql/lexer.rs` | 535 | 0 |
| `src/query/influxql/mod.rs` | 206 | 0 |
| `src/query/influxql/parser.rs` | 1071 | 0 |
| `src/query/influxql/response.rs` | 335 | 0 |
| `src/query/influxql/transpiler.rs` | 595 | 0 |
| `src/query/mod.rs` | 1 | 0 |
| `src/services/api.rs` | 158 | 3 |

### `b8458e5` — feat(tls): implement TLS/SSL encryption for all server protocols

2026-02-07 · M9 · 14 files · +741/−33 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 91 | 0 |
| `Cargo.toml` | 6 | 1 |
| `config.toml` | 14 | 0 |
| `scripts/test_tls.sh` | 155 | 0 |
| `src/config.rs` | 68 | 0 |
| `src/node/admin.rs` | 15 | 2 |
| `src/node/common.rs` | 68 | 18 |
| `src/node/router.rs` | 35 | 6 |
| `src/node/standalone.rs` | 23 | 5 |
| `src/services/mod.rs` | 1 | 0 |
| `src/services/mysql_service.rs` | 24 | 1 |
| `src/services/tls.rs` | 194 | 0 |
| `tests/fixtures/test_cert.pem` | 19 | 0 |
| `tests/fixtures/test_key.pem` | 28 | 0 |

### `9721478` — fix(test): make dictionary encoding savings test deterministic

2026-02-07 · M9 · 1 files · +12/−12 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/cache/dictionary_encoding.rs` | 12 | 12 |

### `233f065` — feat(auth): enhance RBAC with custom roles, user management API, query isolation, and audit logging

2026-02-07 · M9 · 15 files · +1,183/−47 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/auth/audit.rs` | 252 | 0 |
| `src/auth/context.rs` | 22 | 0 |
| `src/auth/mod.rs` | 4 | 0 |
| `src/auth/policy.rs` | 3 | 0 |
| `src/auth/role_store.rs` | 186 | 0 |
| `src/auth/store.rs` | 19 | 0 |
| `src/auth/user.rs` | 13 | 1 |
| `src/config.rs` | 88 | 0 |
| `src/node/standalone.rs` | 44 | 12 |
| `src/query/query_engine/execution.rs` | 108 | 0 |
| `src/services/api.rs` | 319 | 14 |
| `src/services/auth.rs` | 60 | 7 |
| `src/services/flight_service.rs` | 9 | 10 |
| `src/services/mysql_service.rs` | 6 | 3 |
| `src/services/shared.rs` | 50 | 0 |

### `9dedf8e` — feat(streaming): implement real-time streaming queries via WebSocket and SSE

2026-02-07 · M9 · 9 files · +1,224/−9 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture/architecture-streaming-queries.md` | 240 | 0 |
| `docs/guides/guide-streaming-queries.md` | 262 | 0 |
| `src/config.rs` | 58 | 0 |
| `src/data/server/api.rs` | 2 | 2 |
| `src/node/standalone.rs` | 13 | 0 |
| `src/query/mod.rs` | 1 | 0 |
| `src/query/streaming.rs` | 348 | 0 |
| `src/services/api.rs` | 285 | 5 |
| `src/services/shared.rs` | 15 | 2 |

### `24b945a` — feat(downsampling): implement automated downsampling with policy-based tiers

2026-02-07 · M9 · 13 files · +2,801/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture/architecture-downsampling.md` | 285 | 0 |
| `docs/guides/guide-downsampling.md` | 334 | 0 |
| `src/config.rs` | 44 | 0 |
| `src/jobs/downsampling/job.rs` | 424 | 0 |
| `src/jobs/downsampling/mod.rs` | 23 | 0 |
| `src/jobs/downsampling/query_builder.rs` | 217 | 0 |
| `src/jobs/downsampling/storage.rs` | 458 | 0 |
| `src/jobs/downsampling/types.rs` | 295 | 0 |
| `src/jobs/mod.rs` | 1 | 0 |
| `src/node/standalone.rs` | 31 | 0 |
| `src/query/analyzers/downsampling_rewriter.rs` | 415 | 0 |
| `src/query/analyzers/mod.rs` | 12 | 0 |
| `src/services/api.rs` | 262 | 0 |

### `52ca5a0` — feat(compression): implement adaptive per-column compression with age-based tiers

2026-02-07 · M9 · 13 files · +1,419/−75 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture/architecture-adaptive-compression.md` | 180 | 0 |
| `docs/guides/guide-adaptive-compression.md` | 218 | 0 |
| `src/config.rs` | 99 | 0 |
| `src/jobs/compactor/mod.rs` | 32 | 28 |
| `src/jobs/compactor/parquet_io.rs` | 1 | 1 |
| `src/jobs/downsampling/job.rs` | 21 | 1 |
| `src/jobs/mod.rs` | 1 | 0 |
| `src/jobs/recompression.rs` | 432 | 0 |
| `src/node/standalone.rs` | 35 | 6 |
| `src/storage/cache/mod.rs` | 8 | 0 |
| `src/storage/compression.rs` | 367 | 0 |
| `src/storage/mod.rs` | 1 | 0 |
| `src/storage/parquet_writer.rs` | 24 | 39 |

### `d845e97` — feat(query): add PostGIS-like geospatial UDFs for location-based time series queries

2026-02-07 · M9 · 5 files · +1,759/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 132 | 0 |
| `Cargo.toml` | 4 | 0 |
| `src/query/functions/geospatial.rs` | 1602 | 0 |
| `src/query/functions/mod.rs` | 6 | 0 |
| `src/query/query_engine/mod.rs` | 15 | 1 |

### `bd89acf` — feat(query): add time-series forecasting UDAFs with linear, EMA, and ARIMA models

2026-02-07 · M9 · 6 files · +2,289/−51 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 306 | 49 |
| `Cargo.toml` | 3 | 0 |
| `docs/guide-forecasting.md` | 175 | 0 |
| `src/query/functions/forecasting.rs` | 1786 | 0 |
| `src/query/functions/mod.rs` | 7 | 0 |
| `src/query/query_engine/mod.rs` | 12 | 2 |

### `dddea84` — feat(admin): implement Query Console and Storage Browser API endpoints

2026-02-06 · M9 · 8 files · +2,145/−229 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 416 | 227 |
| `Cargo.toml` | 1 | 0 |
| `src/admin/api.rs` | 5 | 1 |
| `src/admin/mod.rs` | 2 | 0 |
| `src/admin/query_api.rs` | 844 | 0 |
| `src/admin/state.rs` | 134 | 1 |
| `src/admin/static_files.rs` | 2 | 0 |
| `src/admin/storage_api.rs` | 741 | 0 |

### `c2184aa` — fix(admin): resolve API forwarding issues found during cluster testing

2026-02-07 · M9 · 7 files · +284/−75 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/Procfile.cluster-local` | 9 | 0 |
| `dev/configs/cluster-local/data1.toml` | 56 | 0 |
| `dev/configs/cluster-local/data2.toml` | 56 | 0 |
| `src/admin/query_api.rs` | 78 | 44 |
| `src/admin/state.rs` | 1 | 0 |
| `src/admin/storage_api.rs` | 77 | 30 |
| `src/node/data.rs` | 7 | 1 |

### `83546ad` — fix(query): add node_id partition column to memtable data at read time

2026-02-07 · M9 · 3 files · +71/−16 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/memtable_provider.rs` | 55 | 11 |
| `src/query/query_engine/registration.rs` | 4 | 2 |
| `src/storage/cache/mod.rs` | 12 | 3 |

### `240fd6f` — fix(router,query): fix COUNT(*) aggregation and ParquetTableProvider path doubling

2026-02-07 · M9 · 2 files · +74/−84 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/query_engine/registration.rs` | 7 | 3 |
| `src/router/query_coordinator/aggregation.rs` | 67 | 81 |

### `b839db8` — fix(admin-ui): make Drained badge red and enable WebSocket proxy

2026-02-07 · M9 · 5 files · +5/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/pages/data-operations.tsx` | 1 | 0 |
| `admin-ui/src/pages/nodes/[id].tsx` | 1 | 0 |
| `admin-ui/src/pages/nodes/index.tsx` | 1 | 0 |
| `admin-ui/src/pages/router-operations.tsx` | 1 | 0 |
| `admin-ui/vite.config.ts` | 1 | 0 |

### `5c1f0dd` — feat(admin): add storage table listing and fix meta/distribution APIs

2026-02-07 · M9 · 6 files · +365/−57 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/hooks/use-storage.ts` | 10 | 0 |
| `admin-ui/src/lib/api.ts` | 7 | 0 |
| `admin-ui/src/lib/types.ts` | 7 | 0 |
| `admin-ui/src/pages/storage.tsx` | 127 | 13 |
| `src/admin/api.rs` | 155 | 44 |
| `src/admin/storage_api.rs` | 59 | 0 |

### `e9a86b4` — feat(admin): add debug tools, election management, and config viewer

2026-02-07 · M9 · 11 files · +1,182/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/App.tsx` | 4 | 0 |
| `admin-ui/src/components/layout/sidebar.tsx` | 2 | 0 |
| `admin-ui/src/hooks/use-debug.ts` | 14 | 0 |
| `admin-ui/src/hooks/use-elections.ts` | 31 | 0 |
| `admin-ui/src/lib/api.ts` | 57 | 0 |
| `admin-ui/src/lib/types.ts` | 44 | 0 |
| `admin-ui/src/pages/debug.tsx` | 231 | 0 |
| `admin-ui/src/pages/meta-operations.tsx` | 158 | 0 |
| `admin-ui/src/pages/settings.tsx` | 138 | 0 |
| `src/admin/api.rs` | 502 | 2 |
| `src/admin/static_files.rs` | 1 | 0 |

### `5600d7a` — fix(admin): add SQL fallback for storage partition listing on object store

2026-02-07 · M9 · 1 files · +111/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/storage_api.rs` | 111 | 2 |

### `5901d44` — feat(admin): show partitions by (date, node_id) with real file names in storage browser

2026-02-07 · M9 · 6 files · +399/−37 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/lib/types.ts` | 1 | 0 |
| `admin-ui/src/pages/storage.tsx` | 20 | 6 |
| `src/admin/storage_api.rs` | 64 | 30 |
| `src/data/server/api.rs` | 1 | 1 |
| `src/jobs/database_manager.rs` | 273 | 0 |
| `src/services/shared.rs` | 40 | 0 |

### `99266bb` — docs: add MinIO getting started guide

2026-02-07 · M9 · 1 files · +283/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/guide-minio.md` | 283 | 0 |

### `6da4500` — fix: line protocol parser panic on single-quote field values and router 204 response

2026-02-07 · M9 · 2 files · +20/−21 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/api.rs` | 16 | 18 |
| `src/storage/line_protocol.rs` | 4 | 3 |

### `447d815` — feat: add storage info to welcome banner and shutdown console messages

2026-02-07 · M9 · 5 files · +88/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/admin.rs` | 2 | 0 |
| `src/node/data.rs` | 42 | 2 |
| `src/node/meta.rs` | 2 | 0 |
| `src/node/router.rs` | 2 | 0 |
| `src/node/standalone.rs` | 40 | 1 |

### `f52499f` — feat: standardize data directory layout with DataLayout struct

2026-02-07 · M9 · 21 files · +509/−59 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/aggregates/config.rs` | 1 | 1 |
| `src/aggregates/storage.rs` | 2 | 1 |
| `src/config.rs` | 2 | 2 |
| `src/jobs/compactor/catalog.rs` | 4 | 3 |
| `src/jobs/compactor/full.rs` | 6 | 5 |
| `src/jobs/compactor/partition.rs` | 3 | 2 |
| `src/jobs/database_manager.rs` | 12 | 10 |
| `src/jobs/downsampling/storage.rs` | 2 | 1 |
| `src/jobs/retention_manager.rs` | 6 | 5 |
| `src/main.rs` | 21 | 0 |
| `src/metrics/updater.rs` | 1 | 1 |
| `src/node/common.rs` | 13 | 8 |
| `src/node/data.rs` | 1 | 1 |
| `src/node/standalone.rs` | 3 | 2 |
| `src/query/query_engine/discovery.rs` | 5 | 4 |
| `src/query/query_engine/listing.rs` | 1 | 3 |
| `src/services/mysql_service.rs` | 3 | 2 |
| `src/storage/layout.rs` | 412 | 0 |
| `src/storage/mod.rs` | 1 | 0 |
| `src/storage/parquet_writer.rs` | 4 | 3 |
| `src/storage/storage.rs` | 6 | 5 |

### `6ee90ce` — feat(admin): fix meta operations and remove router operations

2026-02-07 · M9 · 5 files · +218/−323 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/App.tsx` | 0 | 2 |
| `admin-ui/src/components/layout/sidebar.tsx` | 0 | 2 |
| `admin-ui/src/pages/router-operations.tsx` | 0 | 216 |
| `src/admin/api.rs` | 218 | 102 |
| `src/admin/static_files.rs` | 0 | 1 |

### `546bec0` — feat(dev): add test data generator tool and usage guide

2026-02-07 · M9 · 2 files · +911/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/generate-data.sh` | 646 | 0 |
| `dev/guide-generate-data.md` | 265 | 0 |

### `5388b43` — feat(admin): add test data generator to Debug Tools page

2026-02-07 · M9 · 7 files · +705/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/package-lock.json` | 31 | 0 |
| `admin-ui/package.json` | 1 | 0 |
| `admin-ui/src/components/ui/checkbox.tsx` | 28 | 0 |
| `admin-ui/src/hooks/use-debug.ts` | 8 | 1 |
| `admin-ui/src/lib/api.ts` | 28 | 0 |
| `admin-ui/src/pages/debug.tsx` | 234 | 2 |
| `src/admin/api.rs` | 375 | 0 |

### `b7d6a21` — fix(query,admin): fix object storage table/schema discovery and admin table listing

2026-02-07 · M9 · 5 files · +520/−130 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/pages/tables.tsx` | 30 | 4 |
| `src/admin/api.rs` | 224 | 66 |
| `src/admin/state.rs` | 38 | 0 |
| `src/admin/storage_api.rs` | 223 | 50 |
| `src/query/query_engine/discovery.rs` | 5 | 10 |

### `ffbca20` — feat(admin): add schema/table selectors to query console and fix catalog discovery

2026-02-07 · M9 · 5 files · +201/−69 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/hooks/use-query.ts` | 10 | 0 |
| `admin-ui/src/lib/api.ts` | 4 | 0 |
| `admin-ui/src/pages/query.tsx` | 85 | 3 |
| `src/admin/api.rs` | 1 | 1 |
| `src/admin/query_api.rs` | 101 | 65 |

### `7a3810b` — fix(admin): show node_id as subdirectory in storage browser tree

2026-02-07 · M9 · 1 files · +162/−31 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/pages/storage.tsx` | 162 | 31 |

### `cfcb888` — refactor(meta): remove legacy owner_node_ids field and migrate_legacy_owners()

2026-02-07 · M9 · 9 files · +22/−210 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 1 | 1 |
| `src/main.rs` | 0 | 19 |
| `src/meta/client/meta_client.rs` | 11 | 19 |
| `src/meta/raft/state_machine/standby.rs` | 0 | 5 |
| `src/meta/raft/state_machine/table.rs` | 8 | 22 |
| `src/meta/service/grpc.rs` | 1 | 1 |
| `src/meta/types/table.rs` | 0 | 44 |
| `src/router/shard_cache/table_routing.rs` | 1 | 1 |
| `src/storage/layout.rs` | 0 | 98 |

### `ad8a4be` — fix(data): enable compactor in cluster data node mode

2026-02-07 · M9 · 1 files · +47/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/data.rs` | 47 | 1 |

## M10 — Correctness, observability ingestion, and documentation

58 commits.

### `9f3f4ec` — feat: add InfluxDB-compatible endpoints for Grafana datasource integration

2026-02-09 · M10 · 4 files · +269/−90 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/mod.rs` | 77 | 2 |
| `src/query/influxql/response.rs` | 52 | 9 |
| `src/router/api.rs` | 98 | 2 |
| `src/services/api.rs` | 42 | 77 |

### `320e108` — fix(query): align memtable columns by name to handle heterogeneous batch schemas

2026-02-09 · M10 · 2 files · +147/−30 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/memtable_provider.rs` | 56 | 11 |
| `src/storage/cache/mod.rs` | 91 | 19 |

### `af686f4` — fix(influxql): handle SHOW metadata queries directly for Grafana datasource compatibility

2026-02-13 · M10 · 5 files · +178/−1 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/mod.rs` | 73 | 0 |
| `src/query/query_engine/listing.rs` | 35 | 0 |
| `src/query/query_engine/mod.rs` | 2 | 1 |
| `src/router/api.rs` | 39 | 0 |
| `src/services/api.rs` | 29 | 0 |

### `dc54f58` — refactor(router): parallel fan-out for data node queries

2026-02-13 · M10 · 3 files · +184/−118 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/api.rs` | 155 | 118 |
| `src/router/fan_out.rs` | 28 | 0 |
| `src/router/mod.rs` | 1 | 0 |

### `1d3ec4a` — fix(compactor): add store/ prefix to path discovery and node_id-aware compaction

2026-02-13 · M10 · 5 files · +108/−36 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor/background.rs` | 2 | 1 |
| `src/jobs/compactor/catalog.rs` | 49 | 11 |
| `src/jobs/compactor/full.rs` | 45 | 15 |
| `src/jobs/recompression.rs` | 9 | 7 |
| `src/router/query_coordinator/routing.rs` | 3 | 2 |

### `dee55de` — fix(standalone): accelerate shutdown with flush timeout and immediate exit

2026-02-13 · M10 · 1 files · +12/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/standalone.rs` | 12 | 4 |

### `55674d9` — fix(query): normalize Dictionary-encoded Parquet columns and remove ListingTable

2026-02-13 · M10 · 13 files · +354/−213 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/cluster-local/data1.toml` | 0 | 3 |
| `dev/configs/cluster-local/data2.toml` | 0 | 3 |
| `dev/configs/cluster/data1.toml` | 0 | 3 |
| `dev/configs/cluster/data2.toml` | 0 | 3 |
| `src/config.rs` | 2 | 20 |
| `src/node/common.rs` | 0 | 1 |
| `src/query/providers/multi_union_provider.rs` | 16 | 3 |
| `src/query/providers/parquet/discovery.rs` | 15 | 1 |
| `src/query/providers/parquet/reader.rs` | 19 | 7 |
| `src/query/providers/union_provider.rs` | 257 | 7 |
| `src/query/query_engine/mod.rs` | 1 | 28 |
| `src/query/query_engine/registration.rs` | 21 | 128 |
| `src/storage/parquet_writer.rs` | 23 | 6 |

### `c1fb27b` — fix(query): remap union projection indices to child schemas and align batches by name

2026-02-13 · M10 · 2 files · +189/−50 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/multi_union_provider.rs` | 12 | 7 |
| `src/query/providers/union_provider.rs` | 177 | 43 |

### `b1024fc` — fix(query): handle empty projections for count(*) and fix store path prefix

2026-02-13 · M10 · 4 files · +72/−8 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/memtable_provider.rs` | 11 | 1 |
| `src/query/providers/parquet/reader.rs` | 9 | 1 |
| `src/query/providers/union_provider.rs` | 48 | 2 |
| `src/query/query_engine/registration.rs` | 4 | 4 |

### `4073733` — feat: add ASCII art welcome banner for all node types

2026-02-13 · M10 · 4 files · +23/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/node/common.rs` | 20 | 0 |
| `src/node/data.rs` | 1 | 5 |
| `src/node/router.rs` | 1 | 4 |
| `src/node/standalone.rs` | 1 | 6 |

### `093f400` — fix(mysql): handle Dictionary-encoded Arrow columns in MySQL wire protocol

2026-02-13 · M10 · 2 files · +32/−21 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/parquet/reader.rs` | 16 | 21 |
| `src/services/mysql_service.rs` | 16 | 0 |

### `a2de7dd` — fix(cache): use human-readable size formatting in flush logs

2026-02-13 · M10 · 1 files · +11/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/cache/mod.rs` | 11 | 4 |

### `864d822` — fix(query): merge schemas from all parquet files to handle heterogeneous field sets

2026-02-13 · M10 · 3 files · +168/−22 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/parquet/discovery.rs` | 73 | 21 |
| `src/query/providers/parquet/exec_plan.rs` | 1 | 1 |
| `src/query/providers/parquet/mod.rs` | 94 | 0 |

### `b640060` — fix(influxql): fix transpiler for Grafana Telegraf dashboards (928 & 1443)

2026-02-13 · M10 · 4 files · +675/−65 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/lexer.rs` | 70 | 1 |
| `src/query/influxql/mod.rs` | 4 | 4 |
| `src/query/influxql/parser.rs` | 143 | 30 |
| `src/query/influxql/transpiler.rs` | 458 | 30 |

### `42f2bc0` — feat(compaction): add manifest to prevent duplicate rows from overlapping parquet files

2026-02-13 · M10 · 6 files · +617/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/architecture/architecture-compaction-manifest.md` | 223 | 0 |
| `src/jobs/compactor/full.rs` | 14 | 0 |
| `src/jobs/compactor/partition.rs` | 45 | 0 |
| `src/query/providers/parquet/discovery.rs` | 14 | 0 |
| `src/storage/manifest.rs` | 320 | 0 |
| `src/storage/mod.rs` | 1 | 0 |

### `d338078` — feat(query): add info-level query logging with cost for all node types

2026-02-13 · M10 · 2 files · +17/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/query_engine/execution.rs` | 7 | 2 |
| `src/router/query_coordinator/execution.rs` | 10 | 1 |

### `a736553` — fix(influxql): fix time_bucket args and GROUP BY * expansion for Grafana dashboards

2026-02-13 · M10 · 1 files · +140/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/transpiler.rs` | 140 | 15 |

### `f42526a` — fix(influxql): use WHERE instead of HAVING for SHOW TAG VALUES conditions

2026-02-13 · M10 · 1 files · +19/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/transpiler.rs` | 19 | 3 |

### `c1bd3d8` — fix(influxql): use full time_bucket expression in GROUP BY and ORDER BY

2026-02-13 · M10 · 1 files · +24/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/transpiler.rs` | 24 | 13 |

### `00aeb5e` — fix(influxql): fix Duration context, GREATEST compat, and CTE qualification

2026-02-13 · M10 · 2 files · +38/−10 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/transpiler.rs` | 36 | 8 |
| `src/query/sql_utils.rs` | 2 | 2 |

### `cf5b5e1` — fix(storage): merge duplicate points and coalesce NULL rows from heterogeneous field sets

2026-02-14 · M10 · 3 files · +635/−81 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/providers/memtable_provider.rs` | 81 | 10 |
| `src/storage/batch_converter.rs` | 534 | 70 |
| `src/storage/parquet_writer.rs` | 20 | 1 |

### `468fd99` — fix(parser): quote-aware splitting in line protocol parser

2026-02-14 · M10 · 1 files · +117/−19 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/line_protocol.rs` | 117 | 19 |

### `69d460a` — fix(influxql): fix Grafana dashboard response format for time series charts

2026-02-14 · M10 · 2 files · +155/−26 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/mod.rs` | 23 | 3 |
| `src/query/influxql/transpiler.rs` | 132 | 23 |

### `62220c5` — fix(parser): handle escaped spaces in line protocol tag values

2026-02-14 · M10 · 2 files · +139/−6 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/storage/line_protocol.rs` | 127 | 6 |
| `src/storage/parquet_writer.rs` | 12 | 0 |

### `b972b4f` — fix(influxql): handle Grafana InfluxDB datasource query format

2026-02-14 · M10 · 3 files · +34/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/mod.rs` | 5 | 1 |
| `src/query/influxql/parser.rs` | 27 | 1 |
| `src/services/api.rs` | 2 | 1 |

### `7876621` — fix(influxql): apply epoch precision to time_bucket Int64 timestamps

2026-02-14 · M10 · 1 files · +36/−22 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/response.rs` | 36 | 22 |

### `e2f6e6b` — fix(cache): store full-schema batches in hot partition cache and invalidate after compaction

2026-02-14 · M10 · 7 files · +207/−83 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor/background.rs` | 62 | 6 |
| `src/node/data.rs` | 11 | 2 |
| `src/node/standalone.rs` | 11 | 2 |
| `src/query/providers/parquet/exec_plan.rs` | 107 | 68 |
| `src/query/providers/parquet/mod.rs` | 2 | 0 |
| `src/query/query_engine/registration.rs` | 2 | 3 |
| `src/services/api.rs` | 12 | 2 |

### `ac2c629` — fix(cache): add staleness detection to hot partition cache and disable by default

2026-02-14 · M10 · 3 files · +72/−13 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 1 | 1 |
| `src/query/cache/hot_partition_cache.rs` | 66 | 8 |
| `src/query/providers/parquet/exec_plan.rs` | 5 | 4 |

### `f53d647` — fix(cluster): return empty results for missing tables on data nodes

2026-02-14 · M10 · 1 files · +14/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 14 | 2 |

### `dd1a4c8` — fix(cluster): handle missing tables/fields gracefully, fix re-aggregation and InfluxQL column naming

2026-02-14 · M10 · 4 files · +199/−82 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/flight.rs` | 8 | 4 |
| `src/query/influxql/transpiler.rs` | 16 | 0 |
| `src/router/api.rs` | 4 | 2 |
| `src/router/query_coordinator/aggregation.rs` | 171 | 76 |

### `6edafea` — fix(cluster): resolve Suspect deadlock and partial results in distributed queries

2026-02-14 · M10 · 5 files · +51/−21 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 7 | 0 |
| `src/query/query_engine/registration.rs` | 0 | 11 |
| `src/router/health_checker.rs` | 4 | 3 |
| `src/router/query_coordinator/routing.rs` | 11 | 2 |
| `src/router/shard_cache/node_mgmt.rs` | 29 | 5 |

### `a1255d5` — chore(admin-ui): upgrade vite from v5 to v7

2026-02-14 · M10 · 2 files · +235/−135 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/package-lock.json` | 234 | 134 |
| `admin-ui/package.json` | 1 | 1 |

### `490d73a` — fix(cluster): handle non-numeric columns in distributed query re-aggregation

2026-02-14 · M10 · 1 files · +25/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/query_coordinator/aggregation.rs` | 25 | 3 |

### `0e9983f` — feat: support gzip Content-Encoding for writes and increase body size limit to 250MB

2026-02-14 · M10 · 5 files · +138/−62 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 3 | 0 |
| `Cargo.toml` | 2 | 1 |
| `src/data/server/api.rs` | 41 | 24 |
| `src/router/api.rs` | 48 | 3 |
| `src/services/api.rs` | 44 | 34 |

### `bda1893` — feat(influxql): support lastpoint queries and eliminate double parse

2026-02-14 · M10 · 4 files · +427/−72 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/mod.rs` | 65 | 5 |
| `src/query/influxql/transpiler.rs` | 282 | 2 |
| `src/router/api.rs` | 48 | 41 |
| `src/services/api.rs` | 32 | 24 |

### `d89e6e5` — docs(benchmarks): add correctness comparison and update TSBS results

2026-02-14 · M10 · 2 files · +358/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `benchmarks/correctness-comparison-2026-02-10.md` | 230 | 0 |
| `benchmarks/tsbs-comparison-2026-02-10.md` | 128 | 0 |

### `012f479` — docs(benchmarks): add lastpoint and wrk concurrency benchmark results

2026-02-14 · M10 · 1 files · +48/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `benchmarks/tsbs-comparison-2026-02-10.md` | 48 | 4 |

### `304bbd4` — test: add comprehensive integration test suite (122 tests across 7 suites)

2026-02-14 · M10 · 9 files · +4,400/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 2 | 0 |
| `tests/api_endpoints.rs` | 434 | 0 |
| `tests/auth.rs` | 245 | 0 |
| `tests/common/mod.rs` | 947 | 0 |
| `tests/error_handling.rs` | 457 | 0 |
| `tests/influxql.rs` | 898 | 0 |
| `tests/retention.rs` | 364 | 0 |
| `tests/schema_strategy.rs` | 357 | 0 |
| `tests/timeseries.rs` | 696 | 0 |

### `24e0046` — docs(benchmarks): update write benchmarks with WAL and flush interval analysis

2026-02-14 · M10 · 1 files · +28/−15 text lines

| File | Added | Deleted |
|---|---:|---:|
| `benchmarks/tsbs-comparison-2026-02-10.md` | 28 | 15 |

### `ec57981` — feat(dashboard): add Grafana-style monitoring dashboard with comprehensive system metrics

2026-02-14 · M10 · 10 files · +1,799/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 94 | 0 |
| `Cargo.toml` | 3 | 0 |
| `src/config.rs` | 58 | 0 |
| `src/metrics/mod.rs` | 1 | 0 |
| `src/metrics/system_collector.rs` | 529 | 0 |
| `src/node/standalone.rs` | 10 | 0 |
| `src/services/api.rs` | 2 | 0 |
| `src/services/dashboard/index.html` | 1074 | 0 |
| `src/services/dashboard/mod.rs` | 27 | 0 |
| `src/services/mod.rs` | 1 | 0 |

### `4a147b4` — feat: add OpenTelemetry native ingestion and Apache Iceberg table format

2026-02-14 · M10 · 35 files · +5,529/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 152 | 2 |
| `Cargo.toml` | 6 | 0 |
| `docs/architecture/architecture-iceberg.md` | 302 | 0 |
| `docs/architecture/architecture-otlp-native.md` | 259 | 0 |
| `docs/guides/guide-iceberg.md` | 336 | 0 |
| `docs/guides/guide-otlp-native.md` | 424 | 0 |
| `src/config.rs` | 115 | 0 |
| `src/iceberg/catalog/mod.rs` | 6 | 0 |
| `src/iceberg/catalog/mynadb_catalog.rs` | 174 | 0 |
| `src/iceberg/catalog/rest_catalog.rs` | 257 | 0 |
| `src/iceberg/metadata/manifest.rs` | 227 | 0 |
| `src/iceberg/metadata/manifest_list.rs` | 132 | 0 |
| `src/iceberg/metadata/mod.rs` | 16 | 0 |
| `src/iceberg/metadata/partition_spec.rs` | 81 | 0 |
| `src/iceberg/metadata/schema.rs` | 209 | 0 |
| `src/iceberg/metadata/snapshot.rs` | 135 | 0 |
| `src/iceberg/metadata/table_metadata.rs` | 291 | 0 |
| `src/iceberg/mod.rs` | 24 | 0 |
| `src/iceberg/sync/metadata_sync.rs` | 169 | 0 |
| `src/iceberg/sync/mod.rs` | 7 | 0 |
| `src/iceberg/sync/snapshot_builder.rs` | 295 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 1 | 0 |
| `src/node/common.rs` | 51 | 2 |
| `src/node/standalone.rs` | 48 | 0 |
| `src/services/api.rs` | 231 | 0 |
| `src/services/mod.rs` | 1 | 0 |
| `src/services/otlp/converter.rs` | 162 | 0 |
| `src/services/otlp/grpc_service.rs` | 219 | 0 |
| `src/services/otlp/http_handlers.rs` | 216 | 0 |
| `src/services/otlp/logs_converter.rs` | 197 | 0 |
| `src/services/otlp/metrics_converter.rs` | 398 | 0 |
| `src/services/otlp/mod.rs` | 28 | 0 |
| `src/services/otlp/traces_converter.rs` | 338 | 0 |
| `src/storage/layout.rs` | 21 | 0 |

### `b6857e6` — docs(portal): add 9 new feature pages and update site-wide navigation

2026-02-14 · M10 · 35 files · +8,152/−290 text lines

| File | Added | Deleted |
|---|---:|---:|
| `web/portal/README.md` | 18 | 0 |
| `web/portal/css/style.css` | 18 | 0 |
| `web/portal/docs/admin-console.html` | 581 | 0 |
| `web/portal/docs/api.html` | 236 | 8 |
| `web/portal/docs/architecture.html` | 43 | 8 |
| `web/portal/docs/caching.html` | 42 | 6 |
| `web/portal/docs/cluster.html` | 45 | 10 |
| `web/portal/docs/compaction.html` | 44 | 8 |
| `web/portal/docs/configuration.html` | 151 | 8 |
| `web/portal/docs/continuous-aggregates.html` | 788 | 0 |
| `web/portal/docs/data-model.html` | 44 | 8 |
| `web/portal/docs/examples.html` | 44 | 8 |
| `web/portal/docs/faq.html` | 44 | 8 |
| `web/portal/docs/flight-api.html` | 44 | 8 |
| `web/portal/docs/forecasting.html` | 696 | 0 |
| `web/portal/docs/grafana.html` | 422 | 0 |
| `web/portal/docs/guides.html` | 44 | 8 |
| `web/portal/docs/high-availability.html` | 44 | 8 |
| `web/portal/docs/iceberg.html` | 749 | 0 |
| `web/portal/docs/index.html` | 78 | 35 |
| `web/portal/docs/installation.html` | 44 | 8 |
| `web/portal/docs/line-protocol.html` | 44 | 8 |
| `web/portal/docs/monitoring.html` | 842 | 0 |
| `web/portal/docs/mysql-protocol.html` | 42 | 6 |
| `web/portal/docs/object-store.html` | 42 | 6 |
| `web/portal/docs/otlp.html` | 757 | 0 |
| `web/portal/docs/quickstart.html` | 43 | 8 |
| `web/portal/docs/retention.html` | 44 | 8 |
| `web/portal/docs/security.html` | 911 | 0 |
| `web/portal/docs/sql-queries.html` | 43 | 8 |
| `web/portal/docs/storage.html` | 44 | 8 |
| `web/portal/docs/streaming.html` | 829 | 0 |
| `web/portal/docs/testing.html` | 43 | 8 |
| `web/portal/docs/troubleshooting.html` | 44 | 8 |
| `web/portal/index.html` | 205 | 83 |

### `5fc118f` — docs(portal): add admin guide, cluster operations, and cluster doc pages

2026-02-14 · M10 · 51 files · +7,722/−383 text lines

| File | Added | Deleted |
|---|---:|---:|
| `drafts/icon_preview.html` | 104 | 0 |
| `drafts/icon_preview_birds.html` | 171 | 0 |
| `drafts/icon_preview_e2_beaks.html` | 215 | 0 |
| `drafts/icon_preview_e2_triangle.html` | 264 | 0 |
| `drafts/icon_preview_final.html` | 202 | 0 |
| `drafts/icon_preview_hex_swift.html` | 240 | 0 |
| `drafts/icon_preview_s2.html` | 369 | 0 |
| `drafts/icon_preview_swift.html` | 222 | 0 |
| `drafts/icon_preview_swift2.html` | 301 | 0 |
| `web/portal/README.md` | 10 | 0 |
| `web/portal/docs/admin-console.html` | 136 | 1 |
| `web/portal/docs/admin-guide.html` | 647 | 0 |
| `web/portal/docs/admin-operations.html` | 825 | 0 |
| `web/portal/docs/api.html` | 5 | 0 |
| `web/portal/docs/architecture.html` | 5 | 0 |
| `web/portal/docs/caching.html` | 5 | 0 |
| `web/portal/docs/cluster-architecture.html` | 1353 | 0 |
| `web/portal/docs/cluster-getting-started.html` | 1027 | 0 |
| `web/portal/docs/cluster-scaling.html` | 1317 | 0 |
| `web/portal/docs/cluster.html` | 126 | 100 |
| `web/portal/docs/compaction.html` | 5 | 0 |
| `web/portal/docs/configuration.html` | 5 | 0 |
| `web/portal/docs/continuous-aggregates.html` | 5 | 0 |
| `web/portal/docs/data-model.html` | 5 | 0 |
| `web/portal/docs/examples.html` | 5 | 0 |
| `web/portal/docs/faq.html` | 5 | 0 |
| `web/portal/docs/flight-api.html` | 5 | 0 |
| `web/portal/docs/forecasting.html` | 5 | 0 |
| `web/portal/docs/grafana.html` | 5 | 0 |
| `web/portal/docs/guides.html` | 5 | 0 |
| `web/portal/docs/high-availability.html` | 5 | 0 |
| `web/portal/docs/iceberg.html` | 5 | 0 |
| `web/portal/docs/index.html` | 29 | 0 |
| `web/portal/docs/installation.html` | 5 | 0 |
| `web/portal/docs/line-protocol.html` | 5 | 0 |
| `web/portal/docs/monitoring.html` | 5 | 0 |
| `web/portal/docs/mysql-protocol.html` | 5 | 0 |
| `web/portal/docs/object-store.html` | 5 | 0 |
| `web/portal/docs/otlp.html` | 5 | 0 |
| `web/portal/docs/quickstart.html` | 5 | 0 |
| `web/portal/docs/retention.html` | 5 | 0 |
| `web/portal/docs/security.html` | 5 | 0 |
| `web/portal/docs/sql-queries.html` | 5 | 0 |
| `web/portal/docs/storage.html` | 5 | 0 |
| `web/portal/docs/streaming.html` | 5 | 0 |
| `web/portal/docs/testing.html` | 5 | 0 |
| `web/portal/docs/troubleshooting.html` | 5 | 0 |
| `web/portal/images/favicon.svg` | 4 | 39 |
| `web/portal/images/logo-horizontal.svg` | 7 | 49 |
| `web/portal/images/logo-icon.svg` | 4 | 53 |
| `web/portal/images/logo.svg` | 4 | 141 |

### `209836e` — docs: fix outdated references across 28 doc files

2026-02-14 · M10 · 37 files · +3,795/−3,522 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/admin-ui-query-storage-guide.md` | 0 | 1367 |
| `docs/admin-ui-quick-reference.md` | 0 | 421 |
| `docs/admin/admin-ui-query-storage-guide.md` | 1367 | 0 |
| `docs/admin/admin-ui-quick-reference.md` | 421 | 0 |
| `docs/admin/implementation-summary.md` | 621 | 0 |
| `docs/architecture/architecture-compaction-multilevel.md` | 5 | 5 |
| `docs/architecture/architecture-leveled-compaction.md` | 1 | 1 |
| `docs/architecture/architecture-overview.md` | 198 | 132 |
| `docs/architecture/architecture-schema-extraction.md` | 47 | 68 |
| `docs/architecture/architecture-wal-file-format.md` | 1 | 1 |
| `docs/architecture/architecture-wal-simple-solution.md` | 2 | 2 |
| `docs/changelogs/changelog-implementation.md` | 2 | 2 |
| `docs/feature-query-tracing.md` | 0 | 331 |
| `docs/features/feature-query-tracing.md` | 331 | 0 |
| `docs/features/features-age-based-consolidation.md` | 19 | 20 |
| `docs/features/features-compaction-nonpartitioned.md` | 5 | 5 |
| `docs/features/features-date-based-compaction.md` | 12 | 12 |
| `docs/features/features-flight-usage.md` | 1 | 1 |
| `docs/features/features-schema.md` | 20 | 27 |
| `docs/guide-forecasting.md` | 0 | 175 |
| `docs/guides/guide-configuration.md` | 243 | 117 |
| `docs/guides/guide-correct-query-examples.md` | 1 | 1 |
| `docs/guides/guide-forecasting.md` | 175 | 0 |
| `docs/guides/guide-master-slave-failover-demo.md` | 6 | 6 |
| `docs/guides/guide-master-slave-ha-local-testing.md` | 2 | 2 |
| `docs/guides/guide-master-slave-ha.md` | 2 | 2 |
| `docs/guides/guide-query-examples-updated.md` | 1 | 1 |
| `docs/guides/guide-query-examples.md` | 2 | 2 |
| `docs/guides/guide-quick-reference.md` | 13 | 13 |
| `docs/guides/guide-quickstart-main.md` | 87 | 57 |
| `docs/guides/guide-sql-queries.md` | 2 | 2 |
| `docs/implementation-summary.md` | 0 | 621 |
| `docs/reference/profiling_guide.md` | 6 | 6 |
| `docs/reference/quickstart.md` | 61 | 73 |
| `docs/reference/source-structure.md` | 125 | 33 |
| `docs/roadmaps/roadmap-features.md` | 2 | 2 |
| `docs/troubleshooting/troubleshooting-fixes-applied.md` | 14 | 14 |

### `185621f` — refactor(admin): extract shared response types, helpers, and data-node forwarding

2026-02-14 · M10 · 7 files · +228/−500 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 7 | 97 |
| `src/admin/events.rs` | 2 | 13 |
| `src/admin/mod.rs` | 1 | 0 |
| `src/admin/query_api.rs` | 19 | 189 |
| `src/admin/response.rs` | 93 | 0 |
| `src/admin/state.rs` | 69 | 0 |
| `src/admin/storage_api.rs` | 37 | 201 |

### `03ecac3` — fix(router): align test with is_queryable treating Suspect as queryable

2026-02-14 · M10 · 1 files · +1/−2 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/router/query_coordinator/tests.rs` | 1 | 2 |

### `e6b5efc` — refactor: eliminate duplicates, dead code, and deep nesting across src/

2026-02-14 · M10 · 9 files · +289/−387 text lines

| File | Added | Deleted |
|---|---:|---:|
| `src/data/server/api.rs` | 53 | 142 |
| `src/jobs/compactor/full.rs` | 114 | 88 |
| `src/meta/meta_node/commands.rs` | 0 | 6 |
| `src/router/api.rs` | 2 | 37 |
| `src/services/api.rs` | 2 | 41 |
| `src/services/mysql_service.rs` | 0 | 3 |
| `src/services/otlp/metrics_converter.rs` | 70 | 69 |
| `src/services/rate_limiter.rs` | 1 | 1 |
| `src/services/shared.rs` | 47 | 0 |

### `3050742` — docs(proposal): add Iceberg table format and OpenTelemetry native ingestion proposal

2026-02-27 · M10 · 1 files · +675/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/proposals/proposal-iceberg-otel-cutting-edge.md` | 675 | 0 |

### `c654916` — feat: add 13 custom Claude Code slash commands for MynaDB development

2026-02-27 · M10 · 14 files · +503/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.claude/commands/add-endpoint.md` | 18 | 0 |
| `.claude/commands/add-feature.md` | 28 | 0 |
| `.claude/commands/add-udf.md` | 19 | 0 |
| `.claude/commands/amend-push.md` | 14 | 0 |
| `.claude/commands/bench.md` | 15 | 0 |
| `.claude/commands/build.md` | 6 | 0 |
| `.claude/commands/commit-push.md` | 16 | 0 |
| `.claude/commands/dashboard.md` | 13 | 0 |
| `.claude/commands/explain-module.md` | 23 | 0 |
| `.claude/commands/perf-check.md` | 30 | 0 |
| `.claude/commands/proposal.md` | 20 | 0 |
| `.claude/commands/research-db.md` | 20 | 0 |
| `.claude/commands/test.md` | 11 | 0 |
| `docs/claude-code-commands.md` | 270 | 0 |

### `5327e72` — feat: add 8 Claude Code skill files for context-aware development assistance

2026-02-27 · M10 · 8 files · +1,529/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.claude/skills/datafusion-development/SKILL.md` | 219 | 0 |
| `.claude/skills/distributed-coordination/SKILL.md` | 186 | 0 |
| `.claude/skills/mynadb-architecture/SKILL.md` | 122 | 0 |
| `.claude/skills/parquet-optimization/SKILL.md` | 146 | 0 |
| `.claude/skills/performance-tuning/SKILL.md` | 191 | 0 |
| `.claude/skills/protocol-implementation/SKILL.md` | 164 | 0 |
| `.claude/skills/rust-tsdb-patterns/SKILL.md` | 232 | 0 |
| `.claude/skills/testing-patterns/SKILL.md` | 269 | 0 |

### `b311f94` — feat(portal): add Chinese (zh-CN) translation for entire documentation portal

2026-03-13 · M10 · 77 files · +25,257/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `web/portal/css/style.css` | 22 | 0 |
| `web/portal/docs/admin-console.html` | 3 | 0 |
| `web/portal/docs/admin-guide.html` | 3 | 0 |
| `web/portal/docs/admin-operations.html` | 3 | 0 |
| `web/portal/docs/api.html` | 3 | 0 |
| `web/portal/docs/architecture.html` | 3 | 0 |
| `web/portal/docs/caching.html` | 3 | 0 |
| `web/portal/docs/cluster-architecture.html` | 3 | 0 |
| `web/portal/docs/cluster-getting-started.html` | 3 | 0 |
| `web/portal/docs/cluster-scaling.html` | 3 | 0 |
| `web/portal/docs/cluster.html` | 3 | 0 |
| `web/portal/docs/compaction.html` | 3 | 0 |
| `web/portal/docs/configuration.html` | 3 | 0 |
| `web/portal/docs/continuous-aggregates.html` | 3 | 0 |
| `web/portal/docs/data-model.html` | 3 | 0 |
| `web/portal/docs/examples.html` | 3 | 0 |
| `web/portal/docs/faq.html` | 3 | 0 |
| `web/portal/docs/flight-api.html` | 3 | 0 |
| `web/portal/docs/forecasting.html` | 3 | 0 |
| `web/portal/docs/grafana.html` | 3 | 0 |
| `web/portal/docs/guides.html` | 3 | 0 |
| `web/portal/docs/high-availability.html` | 3 | 0 |
| `web/portal/docs/iceberg.html` | 3 | 0 |
| `web/portal/docs/index.html` | 3 | 0 |
| `web/portal/docs/installation.html` | 3 | 0 |
| `web/portal/docs/line-protocol.html` | 3 | 0 |
| `web/portal/docs/monitoring.html` | 3 | 0 |
| `web/portal/docs/mysql-protocol.html` | 3 | 0 |
| `web/portal/docs/object-store.html` | 3 | 0 |
| `web/portal/docs/otlp.html` | 3 | 0 |
| `web/portal/docs/quickstart.html` | 3 | 0 |
| `web/portal/docs/retention.html` | 3 | 0 |
| `web/portal/docs/security.html` | 3 | 0 |
| `web/portal/docs/sql-queries.html` | 3 | 0 |
| `web/portal/docs/storage.html` | 3 | 0 |
| `web/portal/docs/streaming.html` | 3 | 0 |
| `web/portal/docs/testing.html` | 3 | 0 |
| `web/portal/docs/troubleshooting.html` | 3 | 0 |
| `web/portal/index.html` | 3 | 0 |
| `web/portal/zh/docs/admin-console.html` | 650 | 0 |
| `web/portal/zh/docs/admin-guide.html` | 590 | 0 |
| `web/portal/zh/docs/admin-operations.html` | 763 | 0 |
| `web/portal/zh/docs/api.html` | 911 | 0 |
| `web/portal/zh/docs/architecture.html` | 544 | 0 |
| `web/portal/zh/docs/caching.html` | 868 | 0 |
| `web/portal/zh/docs/cluster-architecture.html` | 605 | 0 |
| `web/portal/zh/docs/cluster-getting-started.html` | 792 | 0 |
| `web/portal/zh/docs/cluster-scaling.html` | 772 | 0 |
| `web/portal/zh/docs/cluster.html` | 610 | 0 |
| `web/portal/zh/docs/compaction.html` | 725 | 0 |
| `web/portal/zh/docs/configuration.html` | 668 | 0 |
| `web/portal/zh/docs/continuous-aggregates.html` | 782 | 0 |
| `web/portal/zh/docs/data-model.html` | 605 | 0 |
| `web/portal/zh/docs/examples.html` | 703 | 0 |
| `web/portal/zh/docs/faq.html` | 398 | 0 |
| `web/portal/zh/docs/flight-api.html` | 715 | 0 |
| `web/portal/zh/docs/forecasting.html` | 674 | 0 |
| `web/portal/zh/docs/grafana.html` | 415 | 0 |
| `web/portal/zh/docs/guides.html` | 375 | 0 |
| `web/portal/zh/docs/high-availability.html` | 627 | 0 |
| `web/portal/zh/docs/iceberg.html` | 719 | 0 |
| `web/portal/zh/docs/index.html` | 402 | 0 |
| `web/portal/zh/docs/installation.html` | 736 | 0 |
| `web/portal/zh/docs/line-protocol.html` | 618 | 0 |
| `web/portal/zh/docs/monitoring.html` | 816 | 0 |
| `web/portal/zh/docs/mysql-protocol.html` | 444 | 0 |
| `web/portal/zh/docs/object-store.html` | 518 | 0 |
| `web/portal/zh/docs/otlp.html` | 740 | 0 |
| `web/portal/zh/docs/quickstart.html` | 391 | 0 |
| `web/portal/zh/docs/retention.html` | 657 | 0 |
| `web/portal/zh/docs/security.html` | 881 | 0 |
| `web/portal/zh/docs/sql-queries.html` | 649 | 0 |
| `web/portal/zh/docs/storage.html` | 545 | 0 |
| `web/portal/zh/docs/streaming.html` | 820 | 0 |
| `web/portal/zh/docs/testing.html` | 1219 | 0 |
| `web/portal/zh/docs/troubleshooting.html` | 336 | 0 |
| `web/portal/zh/index.html` | 838 | 0 |

### `650f78f` — Add GitHub Actions workflow for GitHub Pages deployment

2026-07-03 · M10 · 1 files · +43/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.github/workflows/static.yml` | 43 | 0 |

### `6fe67e1` — fix(portal): use relative links for GitHub Pages project subpath

2026-07-04 · M10 · 77 files · +78/−78 text lines

| File | Added | Deleted |
|---|---:|---:|
| `.github/workflows/static.yml` | 2 | 2 |
| `web/portal/docs/admin-console.html` | 1 | 1 |
| `web/portal/docs/admin-guide.html` | 1 | 1 |
| `web/portal/docs/admin-operations.html` | 1 | 1 |
| `web/portal/docs/api.html` | 1 | 1 |
| `web/portal/docs/architecture.html` | 1 | 1 |
| `web/portal/docs/caching.html` | 1 | 1 |
| `web/portal/docs/cluster-architecture.html` | 1 | 1 |
| `web/portal/docs/cluster-getting-started.html` | 1 | 1 |
| `web/portal/docs/cluster-scaling.html` | 1 | 1 |
| `web/portal/docs/cluster.html` | 1 | 1 |
| `web/portal/docs/compaction.html` | 1 | 1 |
| `web/portal/docs/configuration.html` | 1 | 1 |
| `web/portal/docs/continuous-aggregates.html` | 1 | 1 |
| `web/portal/docs/data-model.html` | 1 | 1 |
| `web/portal/docs/examples.html` | 1 | 1 |
| `web/portal/docs/faq.html` | 1 | 1 |
| `web/portal/docs/flight-api.html` | 1 | 1 |
| `web/portal/docs/forecasting.html` | 1 | 1 |
| `web/portal/docs/grafana.html` | 1 | 1 |
| `web/portal/docs/guides.html` | 1 | 1 |
| `web/portal/docs/high-availability.html` | 1 | 1 |
| `web/portal/docs/iceberg.html` | 1 | 1 |
| `web/portal/docs/index.html` | 1 | 1 |
| `web/portal/docs/installation.html` | 1 | 1 |
| `web/portal/docs/line-protocol.html` | 1 | 1 |
| `web/portal/docs/monitoring.html` | 1 | 1 |
| `web/portal/docs/mysql-protocol.html` | 1 | 1 |
| `web/portal/docs/object-store.html` | 1 | 1 |
| `web/portal/docs/otlp.html` | 1 | 1 |
| `web/portal/docs/quickstart.html` | 1 | 1 |
| `web/portal/docs/retention.html` | 1 | 1 |
| `web/portal/docs/security.html` | 1 | 1 |
| `web/portal/docs/sql-queries.html` | 1 | 1 |
| `web/portal/docs/storage.html` | 1 | 1 |
| `web/portal/docs/streaming.html` | 1 | 1 |
| `web/portal/docs/testing.html` | 1 | 1 |
| `web/portal/docs/troubleshooting.html` | 1 | 1 |
| `web/portal/index.html` | 1 | 1 |
| `web/portal/zh/docs/admin-console.html` | 1 | 1 |
| `web/portal/zh/docs/admin-guide.html` | 1 | 1 |
| `web/portal/zh/docs/admin-operations.html` | 1 | 1 |
| `web/portal/zh/docs/api.html` | 1 | 1 |
| `web/portal/zh/docs/architecture.html` | 1 | 1 |
| `web/portal/zh/docs/caching.html` | 1 | 1 |
| `web/portal/zh/docs/cluster-architecture.html` | 1 | 1 |
| `web/portal/zh/docs/cluster-getting-started.html` | 1 | 1 |
| `web/portal/zh/docs/cluster-scaling.html` | 1 | 1 |
| `web/portal/zh/docs/cluster.html` | 1 | 1 |
| `web/portal/zh/docs/compaction.html` | 1 | 1 |
| `web/portal/zh/docs/configuration.html` | 1 | 1 |
| `web/portal/zh/docs/continuous-aggregates.html` | 1 | 1 |
| `web/portal/zh/docs/data-model.html` | 1 | 1 |
| `web/portal/zh/docs/examples.html` | 1 | 1 |
| `web/portal/zh/docs/faq.html` | 1 | 1 |
| `web/portal/zh/docs/flight-api.html` | 1 | 1 |
| `web/portal/zh/docs/forecasting.html` | 1 | 1 |
| `web/portal/zh/docs/grafana.html` | 1 | 1 |
| `web/portal/zh/docs/guides.html` | 1 | 1 |
| `web/portal/zh/docs/high-availability.html` | 1 | 1 |
| `web/portal/zh/docs/iceberg.html` | 1 | 1 |
| `web/portal/zh/docs/index.html` | 1 | 1 |
| `web/portal/zh/docs/installation.html` | 1 | 1 |
| `web/portal/zh/docs/line-protocol.html` | 1 | 1 |
| `web/portal/zh/docs/monitoring.html` | 1 | 1 |
| `web/portal/zh/docs/mysql-protocol.html` | 1 | 1 |
| `web/portal/zh/docs/object-store.html` | 1 | 1 |
| `web/portal/zh/docs/otlp.html` | 1 | 1 |
| `web/portal/zh/docs/quickstart.html` | 1 | 1 |
| `web/portal/zh/docs/retention.html` | 1 | 1 |
| `web/portal/zh/docs/security.html` | 1 | 1 |
| `web/portal/zh/docs/sql-queries.html` | 1 | 1 |
| `web/portal/zh/docs/storage.html` | 1 | 1 |
| `web/portal/zh/docs/streaming.html` | 1 | 1 |
| `web/portal/zh/docs/testing.html` | 1 | 1 |
| `web/portal/zh/docs/troubleshooting.html` | 1 | 1 |
| `web/portal/zh/index.html` | 1 | 1 |

### `dcdcce0` — add 2026-07-12-architecture-analysis.md

2026-07-12 · M10 · 1 files · +252/−0 text lines

| File | Added | Deleted |
|---|---:|---:|
| `docs/analysis/2026-07-12-architecture-analysis.md` | 252 | 0 |

### `837839e` — fix dashboard bug, and group by time(1m)

2026-07-14 · M10 · 4 files · +283/−5 text lines

| File | Added | Deleted |
|---|---:|---:|
| `dev/API.md` | 219 | 0 |
| `src/router/query_coordinator/aggregation.rs` | 45 | 0 |
| `web/dashboard/index.html` | 4 | 4 |
| `web/dashboard/js/api.js` | 15 | 1 |

### `cef8f07` — chore(admin-ui): sync package-lock after installing @radix-ui/react-checkbox

2026-07-16 · M10 · 1 files · +18/−4 text lines

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/package-lock.json` | 18 | 4 |

### `6e1166a` — docs: update README with setup and usage guide

2026-11-09 · M10 · 1 files · +206/−288 text lines

| File | Added | Deleted |
|---|---:|---:|
| `README.md` | 206 | 288 |

### `d603624` — docs: add branded README header

2026-11-09 · M10 · 1 files · +20/−3 text lines

| File | Added | Deleted |
|---|---:|---:|
| `README.md` | 20 | 3 |

## Changes outside individual local-main diffs

Includes 24 consolidated-history transitions and one separate cross-validation change.

### `b2c50f4` — add cross validation, care to test, io cost

2026-01-24 · M5-related branch only · 3 files · +1,997/−119 text lines

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 414 | 119 |
| `Cargo.toml` | 1 | 0 |
| `tests/cross_validation.rs` | 1582 | 0 |

### `21f4106` — feat(cluster): add partition-aware routing with Hive node partitions

2026-02-04 · M8 (consolidated history) · 95 files · +9,725/−7,247 text lines

Consolidated-history endpoint: same final tree as local-main `f0172fc`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `build.rs` | 3 | 3 |
| `dev/configs/cluster/data1.toml` | 3 | 0 |
| `dev/configs/cluster/data2.toml` | 3 | 0 |
| `docs/proposals/partition-aware-query-routing.md` | 1408 | 0 |
| `proto/meta.proto` | 1 | 1 |
| `src/aggregates/definition.rs` | 2 | 2 |
| `src/aggregates/types.rs` | 8 | 4 |
| `src/bin/sled_inspect.rs` | 11 | 13 |
| `src/config.rs` | 2 | 24 |
| `src/data/replication/receiver.rs` | 3 | 0 |
| `src/data/replication/stream.rs` | 1 | 0 |
| `src/data/replication/types.rs` | 2 | 5 |
| `src/data/server/flight.rs` | 11 | 87 |
| `src/jobs/compactor/background.rs` | 36 | 34 |
| `src/jobs/compactor/catalog.rs` | 52 | 96 |
| `src/jobs/compactor/full.rs` | 87 | 106 |
| `src/jobs/compactor/mod.rs` | 40 | 39 |
| `src/jobs/compactor/parquet_io.rs` | 60 | 39 |
| `src/jobs/compactor/partition.rs` | 51 | 79 |
| `src/jobs/compactor/tests.rs` | 56 | 34 |
| `src/jobs/database_manager.rs` | 49 | 21 |
| `src/jobs/profiling.rs` | 8 | 3 |
| `src/jobs/retention_manager.rs` | 112 | 115 |
| `src/meta/client/meta_client.rs` | 2 | 5 |
| `src/meta/raft/log_store.rs` | 34 | 40 |
| `src/meta/raft/state_machine/event.rs` | 5 | 7 |
| `src/meta/raft/state_machine/mod.rs` | 35 | 41 |
| `src/meta/raft/state_machine/node.rs` | 4 | 6 |
| `src/meta/raft/state_machine/query.rs` | 10 | 12 |
| `src/meta/raft/state_machine/scale_in.rs` | 1 | 103 |
| `src/meta/raft/state_machine/table.rs` | 34 | 52 |
| `src/meta/scale_in/planner.rs` | 3 | 5 |
| `src/meta/service/grpc.rs` | 0 | 3 |
| `src/meta/service/meta_node/reads.rs` | 2 | 2 |
| `src/meta/types/event.rs` | 0 | 8 |
| `src/meta/types/table.rs` | 13 | 180 |
| `src/node/common.rs` | 1 | 1 |
| `src/node/data.rs` | 1 | 1 |
| `src/node/meta.rs` | 2 | 2 |
| `src/node/router.rs` | 24 | 2 |
| `src/node/standalone.rs` | 3 | 5 |
| `src/query/catalog/memory_manager.rs` | 4 | 4 |
| `src/query/functions/time_bucket.rs` | 1 | 1 |
| `src/query/physical_optimizers/scan_config.rs` | 1 | 0 |
| `src/query/providers/mod.rs` | 2 | 2 |
| `src/query/providers/table_provider.rs` | 0 | 1364 |
| `src/query/providers/table_provider/config.rs` | 73 | 0 |
| `src/query/providers/table_provider/discovery.rs` | 224 | 0 |
| `src/query/providers/table_provider/exec_plan.rs` | 203 | 0 |
| `src/query/providers/table_provider/filters.rs` | 295 | 0 |
| `src/query/providers/table_provider/mod.rs` | 382 | 0 |
| `src/query/providers/table_provider/reader.rs` | 257 | 0 |
| `src/query/query_engine/discovery.rs` | 34 | 125 |
| `src/query/query_engine/execution.rs` | 2 | 2 |
| `src/query/query_engine/listing.rs` | 52 | 62 |
| `src/query/query_engine/mod.rs` | 0 | 70 |
| `src/query/query_engine/registration.rs` | 32 | 227 |
| `src/query/sql_utils.rs` | 146 | 0 |
| `src/router/api.rs` | 1 | 1 |
| `src/router/cache_invalidation.rs` | 547 | 0 |
| `src/router/client/data_node_pool.rs` | 2 | 2 |
| `src/router/config.rs` | 20 | 9 |
| `src/router/health_checker.rs` | 170 | 0 |
| `src/router/mod.rs` | 11 | 1 |
| `src/router/query_coordinator/aggregation.rs` | 336 | 0 |
| `src/router/query_coordinator/execution.rs` | 297 | 0 |
| `src/router/query_coordinator/mod.rs` | 139 | 0 |
| `src/router/query_coordinator/planning.rs` | 136 | 0 |
| `src/router/query_coordinator/routing.rs` | 738 | 0 |
| `src/router/query_coordinator/sql_rewrite.rs` | 80 | 0 |
| `src/router/query_coordinator/tests.rs` | 560 | 0 |
| `src/router/query_coordinator/types.rs` | 91 | 0 |
| `src/router/routing/cache_invalidation.rs` | 0 | 468 |
| `src/router/routing/health_checker.rs` | 0 | 170 |
| `src/router/routing/mod.rs` | 0 | 14 |
| `src/router/routing/query_coordinator.rs` | 0 | 1243 |
| `src/router/routing/shard_cache.rs` | 0 | 1264 |
| `src/router/routing/write_router.rs` | 0 | 892 |
| `src/router/shard_cache/distribution.rs` | 465 | 0 |
| `src/router/shard_cache/mod.rs` | 119 | 0 |
| `src/router/shard_cache/node_mgmt.rs` | 214 | 0 |
| `src/router/shard_cache/partition_cache.rs` | 381 | 0 |
| `src/router/shard_cache/table_routing.rs` | 294 | 0 |
| `src/router/shard_cache/types.rs` | 183 | 0 |
| `src/router/state.rs` | 56 | 15 |
| `src/router/write_router.rs` | 893 | 0 |
| `src/services/api.rs` | 1 | 0 |
| `src/storage/cache/mod.rs` | 3 | 4 |
| `src/storage/cache/shard.rs` | 1 | 1 |
| `src/storage/line_protocol.rs` | 1 | 2 |
| `src/storage/mod.rs` | 1 | 0 |
| `src/storage/parquet_writer.rs` | 26 | 11 |
| `src/storage/storage.rs` | 64 | 0 |
| `src/storage/wal.rs` | 4 | 4 |
| `src/utils/hash.rs` | 0 | 109 |

### `8462c8d` — feat(router): add partition caches and standby query failover

2026-02-05 · M8 (consolidated history) · 20 files · +1,624/−192 text lines

Consolidated-history endpoint: same final tree as local-main `8b089b4`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 21 | 0 |
| `Cargo.toml` | 1 | 0 |
| `src/data/server/flight.rs` | 30 | 3 |
| `src/node/router.rs` | 7 | 2 |
| `src/query/query_engine/execution.rs` | 64 | 11 |
| `src/query/query_engine/mod.rs` | 1 | 0 |
| `src/query/query_engine/registration.rs` | 134 | 0 |
| `src/router/cache_invalidation.rs` | 25 | 6 |
| `src/router/client/data_node_pool.rs` | 57 | 2 |
| `src/router/client/mod.rs` | 1 | 1 |
| `src/router/config.rs` | 40 | 3 |
| `src/router/query_coordinator/execution.rs` | 136 | 13 |
| `src/router/query_coordinator/routing.rs` | 142 | 94 |
| `src/router/query_coordinator/tests.rs` | 48 | 33 |
| `src/router/shard_cache/cold_partition_cache.rs` | 415 | 0 |
| `src/router/shard_cache/hot_partition_cache.rs` | 353 | 0 |
| `src/router/shard_cache/mod.rs` | 101 | 18 |
| `src/router/shard_cache/types.rs` | 33 | 3 |
| `src/router/state.rs` | 6 | 2 |
| `src/router/write_router.rs` | 9 | 1 |

### `983fa9c` — refactor: reorganize project and simplify shared code

2026-02-05 · M8 (consolidated history) · 485 files · +82,305/−84,559 text lines

Consolidated-history endpoint: same final tree as local-main `0710e05`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `.dockerignore` | 0 | 30 |
| `CLAUDE_CONTEXT.md` | 0 | 303 |
| `Cargo.toml` | 4 | 3 |
| `Dockerfile` | 0 | 62 |
| `admin-frontend/Cargo.lock` | 0 | 2059 |
| `admin-frontend/Cargo.toml` | 0 | 39 |
| `admin-frontend/Trunk.toml` | 0 | 15 |
| `admin-frontend/index.html` | 0 | 14 |
| `admin-frontend/src/api/client.rs` | 0 | 259 |
| `admin-frontend/src/api/events.rs` | 0 | 101 |
| `admin-frontend/src/api/mod.rs` | 0 | 6 |
| `admin-frontend/src/app.rs` | 0 | 59 |
| `admin-frontend/src/components/layout.rs` | 0 | 131 |
| `admin-frontend/src/components/mod.rs` | 0 | 13 |
| `admin-frontend/src/components/node_card.rs` | 0 | 88 |
| `admin-frontend/src/components/progress.rs` | 0 | 117 |
| `admin-frontend/src/components/status_badge.rs` | 0 | 85 |
| `admin-frontend/src/components/toast.rs` | 0 | 68 |
| `admin-frontend/src/lib.rs` | 0 | 21 |
| `admin-frontend/src/pages/dashboard.rs` | 0 | 371 |
| `admin-frontend/src/pages/events.rs` | 0 | 217 |
| `admin-frontend/src/pages/mod.rs` | 0 | 7 |
| `admin-frontend/src/pages/nodes.rs` | 0 | 161 |
| `admin-frontend/src/pages/scale_in.rs` | 0 | 384 |
| `admin-frontend/src/pages/tables.rs` | 0 | 210 |
| `admin-frontend/src/state/mod.rs` | 0 | 122 |
| `admin-frontend/style/main.css` | 0 | 187 |
| `admin-frontend/style/tailwind.css` | 0 | 181 |
| `admin-frontend/tailwind.config.js` | 0 | 26 |
| `admin-types/Cargo.toml` | 0 | 10 |
| `admin-types/src/cluster.rs` | 0 | 94 |
| `admin-types/src/events.rs` | 0 | 145 |
| `admin-types/src/lib.rs` | 0 | 17 |
| `admin-types/src/nodes.rs` | 0 | 219 |
| `admin-types/src/scale_in.rs` | 0 | 202 |
| `admin-types/src/tables.rs` | 0 | 125 |
| `admin/frontend/Cargo.lock` | 2059 | 0 |
| `admin/frontend/Cargo.toml` | 39 | 0 |
| `admin/frontend/Trunk.toml` | 15 | 0 |
| `admin/frontend/index.html` | 14 | 0 |
| `admin/frontend/src/api/client.rs` | 259 | 0 |
| `admin/frontend/src/api/events.rs` | 101 | 0 |
| `admin/frontend/src/api/mod.rs` | 6 | 0 |
| `admin/frontend/src/app.rs` | 59 | 0 |
| `admin/frontend/src/components/layout.rs` | 131 | 0 |
| `admin/frontend/src/components/mod.rs` | 13 | 0 |
| `admin/frontend/src/components/node_card.rs` | 88 | 0 |
| `admin/frontend/src/components/progress.rs` | 117 | 0 |
| `admin/frontend/src/components/status_badge.rs` | 85 | 0 |
| `admin/frontend/src/components/toast.rs` | 68 | 0 |
| `admin/frontend/src/lib.rs` | 21 | 0 |
| `admin/frontend/src/pages/dashboard.rs` | 371 | 0 |
| `admin/frontend/src/pages/events.rs` | 217 | 0 |
| `admin/frontend/src/pages/mod.rs` | 7 | 0 |
| `admin/frontend/src/pages/nodes.rs` | 161 | 0 |
| `admin/frontend/src/pages/scale_in.rs` | 384 | 0 |
| `admin/frontend/src/pages/tables.rs` | 210 | 0 |
| `admin/frontend/src/state/mod.rs` | 122 | 0 |
| `admin/frontend/style/main.css` | 187 | 0 |
| `admin/frontend/style/tailwind.css` | 181 | 0 |
| `admin/frontend/tailwind.config.js` | 26 | 0 |
| `admin/types/Cargo.toml` | 10 | 0 |
| `admin/types/src/cluster.rs` | 94 | 0 |
| `admin/types/src/events.rs` | 145 | 0 |
| `admin/types/src/lib.rs` | 17 | 0 |
| `admin/types/src/nodes.rs` | 219 | 0 |
| `admin/types/src/scale_in.rs` | 202 | 0 |
| `admin/types/src/tables.rs` | 125 | 0 |
| `build.rs` | 3 | 3 |
| `config.examples.toml` | 0 | 480 |
| `config/config.examples.toml` | 480 | 0 |
| `config/mynadb.service` | 26 | 0 |
| `deploy/docker-compose.cluster.yaml` | 3 | 3 |
| `deploy/docker/.dockerignore` | 30 | 0 |
| `deploy/docker/Dockerfile` | 62 | 0 |
| `docs/architecture-caching-layer.md` | 0 | 586 |
| `docs/architecture-catalog-structure.md` | 0 | 89 |
| `docs/architecture-cluster-scale-in.md` | 0 | 325 |
| `docs/architecture-cluster-v2.md` | 0 | 628 |
| `docs/architecture-cluster-write-path.md` | 0 | 433 |
| `docs/architecture-cluster.md` | 0 | 521 |
| `docs/architecture-cold-standby.md` | 0 | 788 |
| `docs/architecture-compaction-multilevel.md` | 0 | 433 |
| `docs/architecture-compaction-optimization.md` | 0 | 1565 |
| `docs/architecture-continuous-aggregates.md` | 0 | 446 |
| `docs/architecture-custom_table_provider.md` | 0 | 454 |
| `docs/architecture-datafusion-integration.md` | 0 | 459 |
| `docs/architecture-directory-structure.md` | 0 | 339 |
| `docs/architecture-flush-checkpoint.md` | 0 | 270 |
| `docs/architecture-leveled-compaction.md` | 0 | 413 |
| `docs/architecture-meta-raft.md` | 0 | 550 |
| `docs/architecture-meta-routing.md` | 0 | 528 |
| `docs/architecture-meta.md` | 0 | 380 |
| `docs/architecture-node-first-path.md` | 0 | 287 |
| `docs/architecture-node-level-standby.md` | 0 | 342 |
| `docs/architecture-node-registration.md` | 0 | 766 |
| `docs/architecture-overview.md` | 0 | 277 |
| `docs/architecture-parquet-writer.md` | 0 | 227 |
| `docs/architecture-scale-in.md` | 0 | 441 |
| `docs/architecture-schema-extraction.md` | 0 | 316 |
| `docs/architecture-sql-table-naming.md` | 0 | 262 |
| `docs/architecture-standby-no-promotion.md` | 0 | 1015 |
| `docs/architecture-wal-design.md` | 0 | 319 |
| `docs/architecture-wal-file-format.md` | 0 | 646 |
| `docs/architecture-wal-simple-solution.md` | 0 | 426 |
| `docs/architecture-warm-standby.md` | 0 | 356 |
| `docs/architecture/architecture-caching-layer.md` | 586 | 0 |
| `docs/architecture/architecture-catalog-structure.md` | 89 | 0 |
| `docs/architecture/architecture-cluster-scale-in.md` | 325 | 0 |
| `docs/architecture/architecture-cluster-v2.md` | 628 | 0 |
| `docs/architecture/architecture-cluster-write-path.md` | 433 | 0 |
| `docs/architecture/architecture-cluster.md` | 521 | 0 |
| `docs/architecture/architecture-cold-standby.md` | 788 | 0 |
| `docs/architecture/architecture-compaction-multilevel.md` | 433 | 0 |
| `docs/architecture/architecture-compaction-optimization.md` | 1565 | 0 |
| `docs/architecture/architecture-continuous-aggregates.md` | 446 | 0 |
| `docs/architecture/architecture-custom_table_provider.md` | 454 | 0 |
| `docs/architecture/architecture-datafusion-integration.md` | 459 | 0 |
| `docs/architecture/architecture-directory-structure.md` | 339 | 0 |
| `docs/architecture/architecture-flush-checkpoint.md` | 270 | 0 |
| `docs/architecture/architecture-leveled-compaction.md` | 413 | 0 |
| `docs/architecture/architecture-meta-raft.md` | 550 | 0 |
| `docs/architecture/architecture-meta-routing.md` | 528 | 0 |
| `docs/architecture/architecture-meta.md` | 380 | 0 |
| `docs/architecture/architecture-node-first-path.md` | 287 | 0 |
| `docs/architecture/architecture-node-level-standby.md` | 342 | 0 |
| `docs/architecture/architecture-node-registration.md` | 766 | 0 |
| `docs/architecture/architecture-overview.md` | 277 | 0 |
| `docs/architecture/architecture-parquet-writer.md` | 227 | 0 |
| `docs/architecture/architecture-scale-in.md` | 441 | 0 |
| `docs/architecture/architecture-schema-extraction.md` | 316 | 0 |
| `docs/architecture/architecture-sql-table-naming.md` | 262 | 0 |
| `docs/architecture/architecture-standby-no-promotion.md` | 1015 | 0 |
| `docs/architecture/architecture-wal-design.md` | 319 | 0 |
| `docs/architecture/architecture-wal-file-format.md` | 646 | 0 |
| `docs/architecture/architecture-wal-simple-solution.md` | 426 | 0 |
| `docs/architecture/architecture-warm-standby.md` | 356 | 0 |
| `docs/benchmark-sled-results.md` | 0 | 189 |
| `docs/benchmarks/benchmark-sled-results.md` | 189 | 0 |
| `docs/changelog-implementation.md` | 0 | 306 |
| `docs/changelog-logging.md` | 0 | 146 |
| `docs/changelog-main.md` | 0 | 81 |
| `docs/changelog-refactoring.md` | 0 | 193 |
| `docs/changelog-schema-config.md` | 0 | 335 |
| `docs/changelog-schema-update.md` | 0 | 218 |
| `docs/changelog-summary.md` | 0 | 145 |
| `docs/changelog-wal-recovery.md` | 0 | 540 |
| `docs/changelog.md` | 0 | 174 |
| `docs/changelogs/changelog-implementation.md` | 306 | 0 |
| `docs/changelogs/changelog-logging.md` | 146 | 0 |
| `docs/changelogs/changelog-main.md` | 81 | 0 |
| `docs/changelogs/changelog-refactoring.md` | 193 | 0 |
| `docs/changelogs/changelog-schema-config.md` | 335 | 0 |
| `docs/changelogs/changelog-schema-update.md` | 218 | 0 |
| `docs/changelogs/changelog-summary.md` | 145 | 0 |
| `docs/changelogs/changelog-wal-recovery.md` | 540 | 0 |
| `docs/changelogs/changelog.md` | 174 | 0 |
| `docs/feature-cardinality-based-sort.md` | 0 | 286 |
| `docs/features-age-based-consolidation.md` | 0 | 1106 |
| `docs/features-compaction-nonpartitioned.md` | 0 | 450 |
| `docs/features-compaction.md` | 0 | 375 |
| `docs/features-date-based-compaction.md` | 0 | 847 |
| `docs/features-flight-usage.md` | 0 | 132 |
| `docs/features-influxdb-compatibility.md` | 0 | 338 |
| `docs/features-schema.md` | 0 | 185 |
| `docs/features-series-cardinality.md` | 0 | 389 |
| `docs/features-series-deduplication.md` | 0 | 567 |
| `docs/features-wal-flush-recovery.md` | 0 | 523 |
| `docs/features/feature-cardinality-based-sort.md` | 286 | 0 |
| `docs/features/features-age-based-consolidation.md` | 1106 | 0 |
| `docs/features/features-compaction-nonpartitioned.md` | 450 | 0 |
| `docs/features/features-compaction.md` | 375 | 0 |
| `docs/features/features-date-based-compaction.md` | 847 | 0 |
| `docs/features/features-flight-usage.md` | 132 | 0 |
| `docs/features/features-influxdb-compatibility.md` | 338 | 0 |
| `docs/features/features-schema.md` | 185 | 0 |
| `docs/features/features-series-cardinality.md` | 389 | 0 |
| `docs/features/features-wal-flush-recovery.md` | 523 | 0 |
| `docs/guide-admin-console.md` | 0 | 832 |
| `docs/guide-cluster-port-conventions.md` | 0 | 187 |
| `docs/guide-cluster-robustness.md` | 0 | 701 |
| `docs/guide-cluster-scale-in.md` | 0 | 446 |
| `docs/guide-cluster-v2.md` | 0 | 869 |
| `docs/guide-cold-standby.md` | 0 | 787 |
| `docs/guide-compaction-quickstart.md` | 0 | 279 |
| `docs/guide-compaction-testing.md` | 0 | 326 |
| `docs/guide-configuration.md` | 0 | 347 |
| `docs/guide-continuous-aggregates.md` | 0 | 525 |
| `docs/guide-correct-query-examples.md` | 0 | 266 |
| `docs/guide-distributed-write-routing.md` | 0 | 257 |
| `docs/guide-master-slave-failover-demo.md` | 0 | 545 |
| `docs/guide-master-slave-ha-local-testing.md` | 0 | 353 |
| `docs/guide-master-slave-ha.md` | 0 | 537 |
| `docs/guide-query-examples-updated.md` | 0 | 359 |
| `docs/guide-query-examples.md` | 0 | 317 |
| `docs/guide-quick-reference.md` | 0 | 107 |
| `docs/guide-quickstart-main.md` | 0 | 212 |
| `docs/guide-scale-in.md` | 0 | 607 |
| `docs/guide-series-stats.md` | 0 | 205 |
| `docs/guide-sled-benchmark.md` | 0 | 358 |
| `docs/guide-sql-queries.md` | 0 | 555 |
| `docs/guide-warm-standby.md` | 0 | 434 |
| `docs/guides/guide-admin-console.md` | 832 | 0 |
| `docs/guides/guide-cluster-port-conventions.md` | 187 | 0 |
| `docs/guides/guide-cluster-robustness.md` | 701 | 0 |
| `docs/guides/guide-cluster-scale-in.md` | 446 | 0 |
| `docs/guides/guide-cluster-v2.md` | 869 | 0 |
| `docs/guides/guide-cold-standby.md` | 787 | 0 |
| `docs/guides/guide-compaction-quickstart.md` | 279 | 0 |
| `docs/guides/guide-compaction-testing.md` | 326 | 0 |
| `docs/guides/guide-configuration.md` | 347 | 0 |
| `docs/guides/guide-continuous-aggregates.md` | 525 | 0 |
| `docs/guides/guide-correct-query-examples.md` | 266 | 0 |
| `docs/guides/guide-distributed-write-routing.md` | 257 | 0 |
| `docs/guides/guide-master-slave-failover-demo.md` | 545 | 0 |
| `docs/guides/guide-master-slave-ha-local-testing.md` | 353 | 0 |
| `docs/guides/guide-master-slave-ha.md` | 537 | 0 |
| `docs/guides/guide-query-examples-updated.md` | 359 | 0 |
| `docs/guides/guide-query-examples.md` | 317 | 0 |
| `docs/guides/guide-quick-reference.md` | 107 | 0 |
| `docs/guides/guide-quickstart-main.md` | 212 | 0 |
| `docs/guides/guide-scale-in.md` | 607 | 0 |
| `docs/guides/guide-series-stats.md` | 205 | 0 |
| `docs/guides/guide-sled-benchmark.md` | 358 | 0 |
| `docs/guides/guide-sql-queries.md` | 555 | 0 |
| `docs/guides/guide-warm-standby.md` | 434 | 0 |
| `docs/memory-optimization.md` | 0 | 412 |
| `docs/plans/CLAUDE_CONTEXT.md` | 303 | 0 |
| `docs/plans/adaptive-dancing-summit.md` | 329 | 0 |
| `docs/plans/cold-standby-cluster-plan.md` | 402 | 0 |
| `docs/plans/large-file-consolidation.md` | 216 | 0 |
| `docs/profiling_guide.md` | 0 | 787 |
| `docs/proposal-cluster-edition.md` | 0 | 2067 |
| `docs/proposal-cluster-shared-storage.md` | 0 | 1229 |
| `docs/proposal-master-slave-ha.md` | 0 | 1160 |
| `docs/proposal-master-slave-shared-storage.md` | 0 | 1124 |
| `docs/proposal-query-performance-optimization.md` | 0 | 512 |
| `docs/proposal-write-performance-optimization.md` | 0 | 399 |
| `docs/proposals/proposal-cluster-edition.md` | 2067 | 0 |
| `docs/proposals/proposal-cluster-shared-storage.md` | 1229 | 0 |
| `docs/proposals/proposal-master-slave-ha.md` | 1160 | 0 |
| `docs/proposals/proposal-master-slave-shared-storage.md` | 1124 | 0 |
| `docs/proposals/proposal-query-performance-optimization.md` | 512 | 0 |
| `docs/proposals/proposal-write-performance-optimization.md` | 399 | 0 |
| `docs/quickstart.md` | 0 | 162 |
| `docs/reference/profiling_guide.md` | 787 | 0 |
| `docs/reference/quickstart.md` | 162 | 0 |
| `docs/reference/source-structure.md` | 106 | 0 |
| `docs/roadmap-features.md` | 0 | 143 |
| `docs/roadmap-index-performance.md` | 0 | 890 |
| `docs/roadmap-query-performance-v2.md` | 0 | 837 |
| `docs/roadmap-query-performance.md` | 0 | 1039 |
| `docs/roadmaps/roadmap-features.md` | 143 | 0 |
| `docs/roadmaps/roadmap-index-performance.md` | 890 | 0 |
| `docs/roadmaps/roadmap-query-performance-v2.md` | 837 | 0 |
| `docs/roadmaps/roadmap-query-performance.md` | 1039 | 0 |
| `docs/source-structure.md` | 0 | 106 |
| `docs/troubleshooting-fixes-applied.md` | 0 | 145 |
| `docs/troubleshooting-utf8view-fix.md` | 0 | 147 |
| `docs/troubleshooting/troubleshooting-fixes-applied.md` | 145 | 0 |
| `docs/troubleshooting/troubleshooting-utf8view-fix.md` | 147 | 0 |
| `examples/sled_10m_bench.rs` | 0 | 140 |
| `mynadb.service` | 0 | 26 |
| `plans/adaptive-dancing-summit.md` | 0 | 329 |
| `plans/cold-standby-cluster-plan.md` | 0 | 402 |
| `plans/large-file-consolidation.md` | 0 | 216 |
| `proto/meta.proto` | 0 | 718 |
| `scripts/README_AUTH_TESTS.md` | 0 | 270 |
| `scripts/README_COMPACTION_TESTS.md` | 0 | 314 |
| `scripts/analyze_bloom_memory.sh` | 0 | 197 |
| `scripts/analyze_memory.sh` | 0 | 175 |
| `scripts/analyze_profile.sh` | 0 | 78 |
| `scripts/auth/README_AUTH_TESTS.md` | 270 | 0 |
| `scripts/auth/test_auth.sh` | 372 | 0 |
| `scripts/auth/test_auth_comprehensive.sh` | 270 | 0 |
| `scripts/auth/test_url_auth.sh` | 311 | 0 |
| `scripts/build/test_build.sh` | 6 | 0 |
| `scripts/cluster-ha/ha_setup_local.sh` | 261 | 0 |
| `scripts/cluster-ha/test_backward_compat.sh` | 476 | 0 |
| `scripts/cluster-ha/test_chaos.sh` | 821 | 0 |
| `scripts/cluster-ha/test_coldstandby_distribution.sh` | 398 | 0 |
| `scripts/cluster-ha/test_ha_local.sh` | 471 | 0 |
| `scripts/cluster-ha/test_scale_in.sh` | 213 | 0 |
| `scripts/cluster-ha/test_scale_in_meta.sh` | 359 | 0 |
| `scripts/compaction/README_COMPACTION_TESTS.md` | 314 | 0 |
| `scripts/compaction/test_compaction.sh` | 174 | 0 |
| `scripts/compaction/test_compaction_aggressive.sh` | 99 | 0 |
| `scripts/compaction/test_compaction_dual_strategy.sh` | 367 | 0 |
| `scripts/compaction/test_compaction_multilevel.sh` | 79 | 0 |
| `scripts/compaction/test_compaction_nonpartitioned.sh` | 104 | 0 |
| `scripts/compaction/test_compaction_quick.sh` | 139 | 0 |
| `scripts/compaction/test_full_compaction_now.sh` | 224 | 0 |
| `scripts/generate_flamegraph.sh` | 0 | 79 |
| `scripts/ha_setup_local.sh` | 0 | 261 |
| `scripts/integration/test_comprehensive.sh` | 129 | 0 |
| `scripts/minio_test.sh` | 0 | 541 |
| `scripts/partition/test_partitions.sh` | 146 | 0 |
| `scripts/performance/analyze_memory.sh` | 175 | 0 |
| `scripts/performance/analyze_profile.sh` | 78 | 0 |
| `scripts/performance/generate_flamegraph.sh` | 79 | 0 |
| `scripts/performance/test_profile_with_load.sh` | 30 | 0 |
| `scripts/protocol/test_admin_console.sh` | 622 | 0 |
| `scripts/protocol/test_mysql.sh` | 276 | 0 |
| `scripts/query/test_group_by_time.sh` | 216 | 0 |
| `scripts/query/test_partition_query.sh` | 44 | 0 |
| `scripts/query/test_query_fault_tolerance.sh` | 236 | 0 |
| `scripts/read_parquet.py` | 0 | 90 |
| `scripts/run_fuzz.sh` | 0 | 103 |
| `scripts/test_admin_console.sh` | 0 | 622 |
| `scripts/test_auth.sh` | 0 | 372 |
| `scripts/test_auth_comprehensive.sh` | 0 | 270 |
| `scripts/test_backward_compat.sh` | 0 | 476 |
| `scripts/test_build.sh` | 0 | 6 |
| `scripts/test_chaos.sh` | 0 | 821 |
| `scripts/test_coldstandby_distribution.sh` | 0 | 398 |
| `scripts/test_compaction.sh` | 0 | 174 |
| `scripts/test_compaction_aggressive.sh` | 0 | 99 |
| `scripts/test_compaction_dual_strategy.sh` | 0 | 367 |
| `scripts/test_compaction_multilevel.sh` | 0 | 79 |
| `scripts/test_compaction_nonpartitioned.sh` | 0 | 104 |
| `scripts/test_compaction_quick.sh` | 0 | 139 |
| `scripts/test_comprehensive.sh` | 0 | 129 |
| `scripts/test_full_compaction_now.sh` | 0 | 224 |
| `scripts/test_group_by_time.sh` | 0 | 216 |
| `scripts/test_ha_local.sh` | 0 | 471 |
| `scripts/test_mysql.sh` | 0 | 276 |
| `scripts/test_partition_query.sh` | 0 | 44 |
| `scripts/test_partitions.sh` | 0 | 146 |
| `scripts/test_profile_with_load.sh` | 0 | 30 |
| `scripts/test_query_fault_tolerance.sh` | 0 | 236 |
| `scripts/test_scale_in.sh` | 0 | 213 |
| `scripts/test_scale_in_meta.sh` | 0 | 359 |
| `scripts/test_schema_bloom_config.sh` | 0 | 181 |
| `scripts/test_schemas.sh` | 0 | 68 |
| `scripts/test_series_stats.sh` | 0 | 170 |
| `scripts/test_url_auth.sh` | 0 | 311 |
| `scripts/test_write.sh` | 0 | 64 |
| `scripts/utils/minio_test.sh` | 541 | 0 |
| `scripts/utils/read_parquet.py` | 90 | 0 |
| `scripts/utils/run_fuzz.sh` | 103 | 0 |
| `scripts/verify_schema_config.sh` | 0 | 110 |
| `scripts/write-schema/test_schemas.sh` | 68 | 0 |
| `scripts/write-schema/test_write.sh` | 64 | 0 |
| `src/data/server/flight.rs` | 6 | 81 |
| `src/flight_protocol.rs` | 76 | 0 |
| `src/jobs/compactor/background.rs` | 47 | 63 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 1 | 0 |
| `src/meta/meta_node/cluster.rs` | 70 | 0 |
| `src/meta/meta_node/commands.rs` | 96 | 0 |
| `src/meta/meta_node/distribution.rs` | 73 | 0 |
| `src/meta/meta_node/elections.rs` | 59 | 0 |
| `src/meta/meta_node/lifecycle.rs` | 299 | 0 |
| `src/meta/meta_node/locks.rs` | 60 | 0 |
| `src/meta/meta_node/mod.rs` | 381 | 0 |
| `src/meta/meta_node/nodes.rs` | 45 | 0 |
| `src/meta/meta_node/reads.rs` | 117 | 0 |
| `src/meta/meta_node/scale_in.rs` | 109 | 0 |
| `src/meta/meta_node/tables.rs` | 261 | 0 |
| `src/meta/mod.rs` | 1 | 0 |
| `src/meta/recovery/mod.rs` | 1 | 1 |
| `src/meta/scale_in/executor.rs` | 85 | 71 |
| `src/meta/scale_in/mod.rs` | 1 | 1 |
| `src/meta/service/grpc.rs` | 1 | 1 |
| `src/meta/service/http.rs` | 1 | 1 |
| `src/meta/service/meta_node/cluster.rs` | 0 | 70 |
| `src/meta/service/meta_node/commands.rs` | 0 | 96 |
| `src/meta/service/meta_node/distribution.rs` | 0 | 73 |
| `src/meta/service/meta_node/elections.rs` | 0 | 59 |
| `src/meta/service/meta_node/lifecycle.rs` | 0 | 299 |
| `src/meta/service/meta_node/locks.rs` | 0 | 60 |
| `src/meta/service/meta_node/mod.rs` | 0 | 381 |
| `src/meta/service/meta_node/nodes.rs` | 0 | 45 |
| `src/meta/service/meta_node/reads.rs` | 0 | 117 |
| `src/meta/service/meta_node/scale_in.rs` | 0 | 109 |
| `src/meta/service/meta_node/tables.rs` | 0 | 261 |
| `src/meta/service/mod.rs` | 1 | 2 |
| `src/node/meta.rs` | 1 | 1 |
| `src/node/router.rs` | 1 | 23 |
| `src/proto/meta.proto` | 718 | 0 |
| `src/query/catalog/memory_manager.rs` | 86 | 100 |
| `src/query/providers/mod.rs` | 2 | 2 |
| `src/query/providers/parquet/config.rs` | 73 | 0 |
| `src/query/providers/parquet/discovery.rs` | 224 | 0 |
| `src/query/providers/parquet/exec_plan.rs` | 203 | 0 |
| `src/query/providers/parquet/filters.rs` | 295 | 0 |
| `src/query/providers/parquet/mod.rs` | 382 | 0 |
| `src/query/providers/parquet/reader.rs` | 257 | 0 |
| `src/query/providers/table_provider/config.rs` | 0 | 73 |
| `src/query/providers/table_provider/discovery.rs` | 0 | 224 |
| `src/query/providers/table_provider/exec_plan.rs` | 0 | 203 |
| `src/query/providers/table_provider/filters.rs` | 0 | 295 |
| `src/query/providers/table_provider/mod.rs` | 0 | 382 |
| `src/query/providers/table_provider/reader.rs` | 0 | 257 |
| `src/query/query_engine/execution.rs` | 1 | 16 |
| `src/query/query_engine/mod.rs` | 1 | 1 |
| `src/router/client/data_node_pool.rs` | 1 | 44 |
| `src/router/client/mod.rs` | 2 | 1 |
| `src/router/config.rs` | 0 | 20 |
| `src/router/query_coordinator/execution.rs` | 359 | 310 |
| `src/router/shard_cache/mod.rs` | 2 | 13 |
| `src/router/shard_cache/partition_cache.rs` | 0 | 381 |
| `src/router/shard_cache/table_routing.rs` | 3 | 6 |
| `src/router/shard_cache/types.rs` | 4 | 31 |
| `src/storage/wal.rs` | 56 | 49 |
| `temp_web/README.md` | 0 | 88 |
| `temp_web/css/styles.css` | 0 | 444 |
| `temp_web/data_generator.sh` | 0 | 65 |
| `temp_web/index.html` | 0 | 195 |
| `temp_web/js/api.js` | 0 | 174 |
| `temp_web/js/app.js` | 0 | 311 |
| `temp_web/js/charts.js` | 0 | 435 |
| `temp_web/js/export.js` | 0 | 182 |
| `temp_web/proxy_server.py` | 0 | 125 |
| `web/dashboard/README.md` | 88 | 0 |
| `web/dashboard/css/styles.css` | 444 | 0 |
| `web/dashboard/data_generator.sh` | 65 | 0 |
| `web/dashboard/index.html` | 195 | 0 |
| `web/dashboard/js/api.js` | 174 | 0 |
| `web/dashboard/js/app.js` | 311 | 0 |
| `web/dashboard/js/charts.js` | 435 | 0 |
| `web/dashboard/js/export.js` | 182 | 0 |
| `web/dashboard/proxy_server.py` | 125 | 0 |
| `web/portal/README.md` | 133 | 0 |
| `web/portal/css/style.css` | 1704 | 0 |
| `web/portal/docs/api.html` | 689 | 0 |
| `web/portal/docs/architecture.html` | 515 | 0 |
| `web/portal/docs/caching.html` | 838 | 0 |
| `web/portal/docs/cluster.html` | 574 | 0 |
| `web/portal/docs/compaction.html` | 704 | 0 |
| `web/portal/docs/configuration.html` | 532 | 0 |
| `web/portal/docs/data-model.html` | 572 | 0 |
| `web/portal/docs/examples.html` | 673 | 0 |
| `web/portal/docs/faq.html` | 387 | 0 |
| `web/portal/docs/flight-api.html` | 685 | 0 |
| `web/portal/docs/guides.html` | 346 | 0 |
| `web/portal/docs/high-availability.html` | 598 | 0 |
| `web/portal/docs/index.html` | 342 | 0 |
| `web/portal/docs/installation.html` | 705 | 0 |
| `web/portal/docs/line-protocol.html` | 590 | 0 |
| `web/portal/docs/mysql-protocol.html` | 414 | 0 |
| `web/portal/docs/object-store.html` | 489 | 0 |
| `web/portal/docs/quickstart.html` | 362 | 0 |
| `web/portal/docs/retention.html` | 631 | 0 |
| `web/portal/docs/sql-queries.html` | 617 | 0 |
| `web/portal/docs/storage.html` | 514 | 0 |
| `web/portal/docs/testing.html` | 1199 | 0 |
| `web/portal/docs/troubleshooting.html` | 303 | 0 |
| `web/portal/images/favicon.svg` | 40 | 0 |
| `web/portal/images/logo-horizontal.svg` | 60 | 0 |
| `web/portal/images/logo-icon.svg` | 54 | 0 |
| `web/portal/images/logo.svg` | 142 | 0 |
| `web/portal/index.html` | 713 | 0 |
| `web/portal/js/main.js` | 459 | 0 |
| `website/README.md` | 0 | 133 |
| `website/css/style.css` | 0 | 1704 |
| `website/docs/api.html` | 0 | 689 |
| `website/docs/architecture.html` | 0 | 515 |
| `website/docs/caching.html` | 0 | 838 |
| `website/docs/cluster.html` | 0 | 574 |
| `website/docs/compaction.html` | 0 | 704 |
| `website/docs/configuration.html` | 0 | 532 |
| `website/docs/data-model.html` | 0 | 572 |
| `website/docs/examples.html` | 0 | 673 |
| `website/docs/faq.html` | 0 | 387 |
| `website/docs/flight-api.html` | 0 | 685 |
| `website/docs/guides.html` | 0 | 346 |
| `website/docs/high-availability.html` | 0 | 598 |
| `website/docs/index.html` | 0 | 342 |
| `website/docs/installation.html` | 0 | 705 |
| `website/docs/line-protocol.html` | 0 | 590 |
| `website/docs/mysql-protocol.html` | 0 | 414 |
| `website/docs/object-store.html` | 0 | 489 |
| `website/docs/quickstart.html` | 0 | 362 |
| `website/docs/retention.html` | 0 | 631 |
| `website/docs/sql-queries.html` | 0 | 617 |
| `website/docs/storage.html` | 0 | 514 |
| `website/docs/testing.html` | 0 | 1199 |
| `website/docs/troubleshooting.html` | 0 | 303 |
| `website/images/favicon.svg` | 0 | 40 |
| `website/images/logo-horizontal.svg` | 0 | 60 |
| `website/images/logo-icon.svg` | 0 | 54 |
| `website/images/logo.svg` | 0 | 142 |
| `website/index.html` | 0 | 713 |
| `website/js/main.js` | 0 | 459 |

### `beba5d5` — feat(meta): manage ephemeral table ownership with TTL

2026-02-06 · M8 (consolidated history) · 17 files · +1,473/−226 text lines

Consolidated-history endpoint: same final tree as local-main `c49b32e`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/retention_manager.rs` | 63 | 1 |
| `src/meta/client/meta_client.rs` | 133 | 78 |
| `src/meta/config.rs` | 83 | 0 |
| `src/meta/meta_node/lifecycle.rs` | 54 | 6 |
| `src/meta/meta_node/tables.rs` | 69 | 0 |
| `src/meta/raft/state_machine/mod.rs` | 12 | 1 |
| `src/meta/raft/state_machine/table.rs` | 318 | 47 |
| `src/meta/raft/state_machine/tests.rs` | 3 | 3 |
| `src/meta/service/grpc.rs` | 114 | 57 |
| `src/meta/types/command.rs` | 55 | 1 |
| `src/meta/types/event.rs` | 8 | 0 |
| `src/meta/types/mod.rs` | 1 | 1 |
| `src/meta/types/table.rs` | 406 | 22 |
| `src/proto/meta.proto` | 54 | 2 |
| `src/router/cache_invalidation.rs` | 91 | 0 |
| `src/router/query_coordinator/routing.rs` | 8 | 6 |
| `src/router/write_router.rs` | 1 | 1 |

### `e12fd1b` — feat(admin): migrate to React and consolidate APIs and dev setup

2026-02-06 · M9 (consolidated history) · 99 files · +12,436/−5,998 text lines

Consolidated-history endpoint: same final tree as local-main `173ca32`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 0 | 10 |
| `Cargo.toml` | 1 | 3 |
| `admin-ui/.gitignore` | 26 | 0 |
| `admin-ui/README.md` | 480 | 0 |
| `admin-ui/components.json` | 20 | 0 |
| `admin-ui/eslint.config.js` | 29 | 0 |
| `admin-ui/index.html` | 13 | 0 |
| `admin-ui/package-lock.json` | 5688 | 0 |
| `admin-ui/package.json` | 52 | 0 |
| `admin-ui/postcss.config.js` | 6 | 0 |
| `admin-ui/public/favicon.svg` | 10 | 0 |
| `admin-ui/src/App.tsx` | 45 | 0 |
| `admin-ui/src/components/layout/header.tsx` | 70 | 0 |
| `admin-ui/src/components/layout/main-layout.tsx` | 20 | 0 |
| `admin-ui/src/components/layout/sidebar.tsx` | 110 | 0 |
| `admin-ui/src/components/ui/alert.tsx` | 63 | 0 |
| `admin-ui/src/components/ui/badge.tsx` | 40 | 0 |
| `admin-ui/src/components/ui/button.tsx` | 56 | 0 |
| `admin-ui/src/components/ui/card.tsx` | 79 | 0 |
| `admin-ui/src/components/ui/dialog.tsx` | 120 | 0 |
| `admin-ui/src/components/ui/dropdown-menu.tsx` | 198 | 0 |
| `admin-ui/src/components/ui/input.tsx` | 22 | 0 |
| `admin-ui/src/components/ui/label.tsx` | 24 | 0 |
| `admin-ui/src/components/ui/progress.tsx` | 26 | 0 |
| `admin-ui/src/components/ui/scroll-area.tsx` | 19 | 0 |
| `admin-ui/src/components/ui/select.tsx` | 158 | 0 |
| `admin-ui/src/components/ui/separator.tsx` | 29 | 0 |
| `admin-ui/src/components/ui/skeleton.tsx` | 15 | 0 |
| `admin-ui/src/components/ui/table.tsx` | 117 | 0 |
| `admin-ui/src/components/ui/tabs.tsx` | 53 | 0 |
| `admin-ui/src/components/ui/toast.tsx` | 128 | 0 |
| `admin-ui/src/components/ui/tooltip.tsx` | 28 | 0 |
| `admin-ui/src/hooks/use-cluster.ts` | 18 | 0 |
| `admin-ui/src/hooks/use-events.ts` | 69 | 0 |
| `admin-ui/src/hooks/use-nodes.ts` | 104 | 0 |
| `admin-ui/src/hooks/use-scale-in.ts` | 55 | 0 |
| `admin-ui/src/hooks/use-tables.ts` | 24 | 0 |
| `admin-ui/src/index.css` | 59 | 0 |
| `admin-ui/src/lib/api.ts` | 222 | 0 |
| `admin-ui/src/lib/types.ts` | 283 | 0 |
| `admin-ui/src/lib/utils.ts` | 53 | 0 |
| `admin-ui/src/main.tsx` | 22 | 0 |
| `admin-ui/src/pages/dashboard.tsx` | 292 | 0 |
| `admin-ui/src/pages/data-operations.tsx` | 326 | 0 |
| `admin-ui/src/pages/events.tsx` | 332 | 0 |
| `admin-ui/src/pages/meta-operations.tsx` | 437 | 0 |
| `admin-ui/src/pages/nodes/[id].tsx` | 441 | 0 |
| `admin-ui/src/pages/nodes/index.tsx` | 239 | 0 |
| `admin-ui/src/pages/router-operations.tsx` | 215 | 0 |
| `admin-ui/src/pages/scale-in.tsx` | 664 | 0 |
| `admin-ui/src/pages/tables.tsx` | 330 | 0 |
| `admin-ui/src/vite-env.d.ts` | 1 | 0 |
| `admin-ui/tailwind.config.js` | 74 | 0 |
| `admin-ui/tsconfig.json` | 30 | 0 |
| `admin-ui/tsconfig.node.json` | 22 | 0 |
| `admin-ui/vite.config.ts` | 36 | 0 |
| `admin/frontend/Cargo.lock` | 0 | 2059 |
| `admin/frontend/Cargo.toml` | 0 | 39 |
| `admin/frontend/Trunk.toml` | 0 | 15 |
| `admin/frontend/index.html` | 0 | 14 |
| `admin/frontend/src/api/client.rs` | 0 | 259 |
| `admin/frontend/src/api/events.rs` | 0 | 101 |
| `admin/frontend/src/api/mod.rs` | 0 | 6 |
| `admin/frontend/src/app.rs` | 0 | 59 |
| `admin/frontend/src/components/layout.rs` | 0 | 131 |
| `admin/frontend/src/components/mod.rs` | 0 | 13 |
| `admin/frontend/src/components/node_card.rs` | 0 | 88 |
| `admin/frontend/src/components/progress.rs` | 0 | 117 |
| `admin/frontend/src/components/status_badge.rs` | 0 | 85 |
| `admin/frontend/src/components/toast.rs` | 0 | 68 |
| `admin/frontend/src/lib.rs` | 0 | 21 |
| `admin/frontend/src/pages/dashboard.rs` | 0 | 371 |
| `admin/frontend/src/pages/events.rs` | 0 | 217 |
| `admin/frontend/src/pages/mod.rs` | 0 | 7 |
| `admin/frontend/src/pages/nodes.rs` | 0 | 161 |
| `admin/frontend/src/pages/scale_in.rs` | 0 | 384 |
| `admin/frontend/src/pages/tables.rs` | 0 | 210 |
| `admin/frontend/src/state/mod.rs` | 0 | 122 |
| `admin/frontend/style/main.css` | 0 | 187 |
| `admin/frontend/style/tailwind.css` | 0 | 181 |
| `admin/frontend/tailwind.config.js` | 0 | 26 |
| `admin/types/Cargo.toml` | 0 | 10 |
| `admin/types/src/cluster.rs` | 0 | 94 |
| `admin/types/src/events.rs` | 0 | 145 |
| `admin/types/src/lib.rs` | 0 | 17 |
| `admin/types/src/nodes.rs` | 0 | 219 |
| `admin/types/src/scale_in.rs` | 0 | 202 |
| `admin/types/src/tables.rs` | 0 | 125 |
| `dev/Procfile` | 3 | 1 |
| `dev/Procfile.coldstandby` | 4 | 0 |
| `dev/Procfile.coldstandby-simple` | 14 | 7 |
| `dev/configs/cluster/admin.toml` | 21 | 0 |
| `dev/configs/coldstandby-simple/admin.toml` | 19 | 0 |
| `dev/configs/coldstandby/admin.toml` | 26 | 0 |
| `dev/start-cluster.sh` | 11 | 4 |
| `dev/start-coldstandby.sh` | 10 | 4 |
| `src/admin/api.rs` | 154 | 125 |
| `src/admin/mod.rs` | 4 | 8 |
| `src/admin/static_files.rs` | 77 | 83 |

### `1d6d3cf` — feat(grafana): add datasource plugin and integration guide

2026-02-06 · M9 (consolidated history) · 15 files · +2,277/−0 text lines

Consolidated-history endpoint: same final tree as local-main `bca1ee1`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `docs/implementation/grafana-integration-plan.md` | 1014 | 0 |
| `grafana-mynadb-datasource/.gitignore` | 7 | 0 |
| `grafana-mynadb-datasource/README.md` | 204 | 0 |
| `grafana-mynadb-datasource/package.json` | 29 | 0 |
| `grafana-mynadb-datasource/src/ConfigEditor.tsx` | 46 | 0 |
| `grafana-mynadb-datasource/src/QueryEditor.tsx` | 111 | 0 |
| `grafana-mynadb-datasource/src/datasource.ts` | 195 | 0 |
| `grafana-mynadb-datasource/src/img/logo.svg` | 21 | 0 |
| `grafana-mynadb-datasource/src/module.ts` | 11 | 0 |
| `grafana-mynadb-datasource/src/plugin.json` | 44 | 0 |
| `grafana-mynadb-datasource/src/types.ts` | 21 | 0 |
| `grafana-mynadb-datasource/tsconfig.json` | 11 | 0 |
| `src/services/api.rs` | 6 | 0 |
| `src/services/grafana.rs` | 556 | 0 |
| `src/services/mod.rs` | 1 | 0 |

### `e4d32a4` — feat(admin): add event streaming, filtering, export and notifications

2026-02-06 · M9 (consolidated history) · 9 files · +1,139/−86 text lines

Consolidated-history endpoint: same final tree as local-main `3b274b0`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/src/hooks/use-events.ts` | 104 | 3 |
| `admin-ui/src/lib/api.ts` | 34 | 5 |
| `admin-ui/src/lib/types.ts` | 20 | 10 |
| `admin-ui/src/pages/events.tsx` | 366 | 49 |
| `src/admin/api.rs` | 98 | 7 |
| `src/admin/events.rs` | 308 | 0 |
| `src/admin/mod.rs` | 21 | 1 |
| `src/admin/state.rs` | 149 | 0 |
| `src/admin/websocket.rs` | 39 | 11 |

### `146ff99` — feat(admin): add query console, storage browser and metrics APIs

2026-02-06 · M9 (consolidated history) · 28 files · +6,388/−63 text lines

Consolidated-history endpoint: same final tree as local-main `d597b71`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/README.md` | 134 | 1 |
| `admin-ui/package-lock.json` | 65 | 0 |
| `admin-ui/package.json` | 1 | 0 |
| `admin-ui/src/App.tsx` | 4 | 0 |
| `admin-ui/src/components/layout/sidebar.tsx` | 4 | 0 |
| `admin-ui/src/hooks/use-query.ts` | 175 | 0 |
| `admin-ui/src/hooks/use-storage.ts` | 74 | 0 |
| `admin-ui/src/lib/api.ts` | 109 | 0 |
| `admin-ui/src/lib/types.ts` | 127 | 0 |
| `admin-ui/src/pages/query.tsx` | 780 | 0 |
| `admin-ui/src/pages/storage.tsx` | 996 | 0 |
| `docs/admin-ui-query-storage-guide.md` | 1367 | 0 |
| `docs/admin-ui-quick-reference.md` | 421 | 0 |
| `docs/implementation-summary.md` | 621 | 0 |
| `scripts/metrics/test_metrics_dashboard.sh` | 246 | 0 |
| `src/admin/api.rs` | 95 | 0 |
| `src/lib.rs` | 1 | 0 |
| `src/main.rs` | 2 | 0 |
| `src/metrics/collector.rs` | 475 | 0 |
| `src/metrics/mod.rs` | 12 | 0 |
| `src/metrics/prometheus.rs` | 206 | 0 |
| `src/metrics/types.rs` | 209 | 0 |
| `src/metrics/updater.rs` | 203 | 0 |
| `src/node/standalone.rs` | 9 | 0 |
| `src/query/cache/metadata_cache.rs` | 4 | 0 |
| `src/query/cache/query_cache.rs` | 5 | 0 |
| `src/services/api.rs` | 40 | 62 |
| `src/storage/wal.rs` | 3 | 0 |

### `d250b65` — feat(admin): complete cluster query and storage browser integration

2026-02-06 · M9 (consolidated history) · 35 files · +4,526/−392 text lines

Consolidated-history endpoint: same final tree as local-main `5901d44`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 416 | 227 |
| `Cargo.toml` | 1 | 0 |
| `admin-ui/src/App.tsx` | 4 | 0 |
| `admin-ui/src/components/layout/sidebar.tsx` | 2 | 0 |
| `admin-ui/src/hooks/use-debug.ts` | 14 | 0 |
| `admin-ui/src/hooks/use-elections.ts` | 31 | 0 |
| `admin-ui/src/hooks/use-storage.ts` | 10 | 0 |
| `admin-ui/src/lib/api.ts` | 64 | 0 |
| `admin-ui/src/lib/types.ts` | 52 | 0 |
| `admin-ui/src/pages/data-operations.tsx` | 1 | 0 |
| `admin-ui/src/pages/debug.tsx` | 231 | 0 |
| `admin-ui/src/pages/meta-operations.tsx` | 158 | 0 |
| `admin-ui/src/pages/nodes/[id].tsx` | 1 | 0 |
| `admin-ui/src/pages/nodes/index.tsx` | 1 | 0 |
| `admin-ui/src/pages/router-operations.tsx` | 1 | 0 |
| `admin-ui/src/pages/settings.tsx` | 138 | 0 |
| `admin-ui/src/pages/storage.tsx` | 147 | 19 |
| `admin-ui/vite.config.ts` | 1 | 0 |
| `dev/Procfile.cluster-local` | 9 | 0 |
| `dev/configs/cluster-local/data1.toml` | 56 | 0 |
| `dev/configs/cluster-local/data2.toml` | 56 | 0 |
| `src/admin/api.rs` | 658 | 43 |
| `src/admin/mod.rs` | 2 | 0 |
| `src/admin/query_api.rs` | 878 | 0 |
| `src/admin/state.rs` | 135 | 1 |
| `src/admin/static_files.rs` | 3 | 0 |
| `src/admin/storage_api.rs` | 990 | 0 |
| `src/data/server/api.rs` | 1 | 1 |
| `src/jobs/database_manager.rs` | 273 | 0 |
| `src/node/data.rs` | 7 | 1 |
| `src/query/providers/memtable_provider.rs` | 55 | 11 |
| `src/query/query_engine/registration.rs` | 11 | 5 |
| `src/router/query_coordinator/aggregation.rs` | 67 | 81 |
| `src/services/shared.rs` | 40 | 0 |
| `src/storage/cache/mod.rs` | 12 | 3 |

### `6fc6976` — fix(storage): standardize data layout and harden write handling

2026-02-07 · M9 (consolidated history) · 27 files · +900/−83 text lines

Consolidated-history endpoint: same final tree as local-main `f52499f`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `dev/guide-minio.md` | 283 | 0 |
| `src/aggregates/config.rs` | 1 | 1 |
| `src/aggregates/storage.rs` | 2 | 1 |
| `src/config.rs` | 2 | 2 |
| `src/jobs/compactor/catalog.rs` | 4 | 3 |
| `src/jobs/compactor/full.rs` | 6 | 5 |
| `src/jobs/compactor/partition.rs` | 3 | 2 |
| `src/jobs/database_manager.rs` | 12 | 10 |
| `src/jobs/downsampling/storage.rs` | 2 | 1 |
| `src/jobs/retention_manager.rs` | 6 | 5 |
| `src/main.rs` | 21 | 0 |
| `src/metrics/updater.rs` | 1 | 1 |
| `src/node/admin.rs` | 2 | 0 |
| `src/node/common.rs` | 13 | 8 |
| `src/node/data.rs` | 43 | 3 |
| `src/node/meta.rs` | 2 | 0 |
| `src/node/router.rs` | 2 | 0 |
| `src/node/standalone.rs` | 43 | 3 |
| `src/query/query_engine/discovery.rs` | 5 | 4 |
| `src/query/query_engine/listing.rs` | 1 | 3 |
| `src/router/api.rs` | 16 | 18 |
| `src/services/mysql_service.rs` | 3 | 2 |
| `src/storage/layout.rs` | 412 | 0 |
| `src/storage/line_protocol.rs` | 4 | 3 |
| `src/storage/mod.rs` | 1 | 0 |
| `src/storage/parquet_writer.rs` | 4 | 3 |
| `src/storage/storage.rs` | 6 | 5 |

### `5f69315` — feat(admin): add data generation and improve catalog discovery

2026-02-07 · M9 (consolidated history) · 21 files · +2,716/−555 text lines

Consolidated-history endpoint: same final tree as local-main `7a3810b`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `admin-ui/package-lock.json` | 31 | 0 |
| `admin-ui/package.json` | 1 | 0 |
| `admin-ui/src/App.tsx` | 0 | 2 |
| `admin-ui/src/components/layout/sidebar.tsx` | 0 | 2 |
| `admin-ui/src/components/ui/checkbox.tsx` | 28 | 0 |
| `admin-ui/src/hooks/use-debug.ts` | 8 | 1 |
| `admin-ui/src/hooks/use-query.ts` | 10 | 0 |
| `admin-ui/src/lib/api.ts` | 32 | 0 |
| `admin-ui/src/pages/debug.tsx` | 234 | 2 |
| `admin-ui/src/pages/query.tsx` | 85 | 3 |
| `admin-ui/src/pages/router-operations.tsx` | 0 | 216 |
| `admin-ui/src/pages/storage.tsx` | 162 | 31 |
| `admin-ui/src/pages/tables.tsx` | 30 | 4 |
| `dev/generate-data.sh` | 646 | 0 |
| `dev/guide-generate-data.md` | 265 | 0 |
| `src/admin/api.rs` | 817 | 168 |
| `src/admin/query_api.rs` | 101 | 65 |
| `src/admin/state.rs` | 38 | 0 |
| `src/admin/static_files.rs` | 0 | 1 |
| `src/admin/storage_api.rs` | 223 | 50 |
| `src/query/query_engine/discovery.rs` | 5 | 10 |

### `e7edf81` — fix(cluster): remove legacy ownership and enable data-node compaction

2026-02-07 · M9 (consolidated history) · 10 files · +69/−211 text lines

Consolidated-history endpoint: same final tree as local-main `ad8a4be`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 1 | 1 |
| `src/main.rs` | 0 | 19 |
| `src/meta/client/meta_client.rs` | 11 | 19 |
| `src/meta/raft/state_machine/standby.rs` | 0 | 5 |
| `src/meta/raft/state_machine/table.rs` | 8 | 22 |
| `src/meta/service/grpc.rs` | 1 | 1 |
| `src/meta/types/table.rs` | 0 | 44 |
| `src/node/data.rs` | 47 | 1 |
| `src/router/shard_cache/table_routing.rs` | 1 | 1 |
| `src/storage/layout.rs` | 0 | 98 |

### `974245b` — feat(query): integrate InfluxDB endpoints and harden cluster execution

2026-02-08 · M10 (consolidated history) · 16 files · +888/−269 text lines

Consolidated-history endpoint: same final tree as local-main `dee55de`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/jobs/compactor/background.rs` | 2 | 1 |
| `src/jobs/compactor/catalog.rs` | 49 | 11 |
| `src/jobs/compactor/full.rs` | 45 | 15 |
| `src/jobs/recompression.rs` | 9 | 7 |
| `src/node/standalone.rs` | 12 | 4 |
| `src/query/influxql/mod.rs` | 150 | 2 |
| `src/query/influxql/response.rs` | 52 | 9 |
| `src/query/providers/memtable_provider.rs` | 56 | 11 |
| `src/query/query_engine/listing.rs` | 35 | 0 |
| `src/query/query_engine/mod.rs` | 2 | 1 |
| `src/router/api.rs` | 284 | 112 |
| `src/router/fan_out.rs` | 28 | 0 |
| `src/router/mod.rs` | 1 | 0 |
| `src/router/query_coordinator/routing.rs` | 3 | 2 |
| `src/services/api.rs` | 69 | 75 |
| `src/storage/cache/mod.rs` | 91 | 19 |

### `9bfaf6c` — fix(query): handle heterogeneous schemas and dictionary columns

2026-02-08 · M10 (consolidated history) · 21 files · +799/−283 text lines

Consolidated-history endpoint: same final tree as local-main `864d822`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `dev/configs/cluster-local/data1.toml` | 0 | 3 |
| `dev/configs/cluster-local/data2.toml` | 0 | 3 |
| `dev/configs/cluster/data1.toml` | 0 | 3 |
| `dev/configs/cluster/data2.toml` | 0 | 3 |
| `src/config.rs` | 2 | 20 |
| `src/node/common.rs` | 20 | 1 |
| `src/node/data.rs` | 1 | 5 |
| `src/node/router.rs` | 1 | 4 |
| `src/node/standalone.rs` | 1 | 6 |
| `src/query/providers/memtable_provider.rs` | 11 | 1 |
| `src/query/providers/multi_union_provider.rs` | 23 | 5 |
| `src/query/providers/parquet/discovery.rs` | 88 | 22 |
| `src/query/providers/parquet/exec_plan.rs` | 1 | 1 |
| `src/query/providers/parquet/mod.rs` | 94 | 0 |
| `src/query/providers/parquet/reader.rs` | 40 | 25 |
| `src/query/providers/union_provider.rs` | 443 | 13 |
| `src/query/query_engine/mod.rs` | 1 | 28 |
| `src/query/query_engine/registration.rs` | 23 | 130 |
| `src/services/mysql_service.rs` | 16 | 0 |
| `src/storage/cache/mod.rs` | 11 | 4 |
| `src/storage/parquet_writer.rs` | 23 | 6 |

### `8a8cc43` — fix(influxql): correct time grouping and metadata query translation

2026-02-08 · M10 (consolidated history) · 2 files · +216/−36 text lines

Consolidated-history endpoint: same final tree as local-main `00aeb5e`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/transpiler.rs` | 214 | 34 |
| `src/query/sql_utils.rs` | 2 | 2 |

### `c1dea7b` — fix(ingestion): preserve point semantics and Grafana query compatibility

2026-02-09 · M10 (consolidated history) · 9 files · +1,112/−153 text lines

Consolidated-history endpoint: same final tree as local-main `7876621`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/query/influxql/mod.rs` | 28 | 4 |
| `src/query/influxql/parser.rs` | 27 | 1 |
| `src/query/influxql/response.rs` | 36 | 22 |
| `src/query/influxql/transpiler.rs` | 132 | 23 |
| `src/query/providers/memtable_provider.rs` | 81 | 10 |
| `src/services/api.rs` | 2 | 1 |
| `src/storage/batch_converter.rs` | 534 | 70 |
| `src/storage/line_protocol.rs` | 240 | 21 |
| `src/storage/parquet_writer.rs` | 32 | 1 |

### `d2823d1` — fix(cache): preserve full schemas and detect stale partitions

2026-02-09 · M10 (consolidated history) · 9 files · +278/−95 text lines

Consolidated-history endpoint: same final tree as local-main `ac2c629`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/config.rs` | 1 | 1 |
| `src/jobs/compactor/background.rs` | 62 | 6 |
| `src/node/data.rs` | 11 | 2 |
| `src/node/standalone.rs` | 11 | 2 |
| `src/query/cache/hot_partition_cache.rs` | 66 | 8 |
| `src/query/providers/parquet/exec_plan.rs` | 111 | 71 |
| `src/query/providers/parquet/mod.rs` | 2 | 0 |
| `src/query/query_engine/registration.rs` | 2 | 3 |
| `src/services/api.rs` | 12 | 2 |

### `e19ee4b` — fix(cluster): handle missing data and prevent partial query results

2026-02-09 · M10 (consolidated history) · 9 files · +260/−101 text lines

Consolidated-history endpoint: same final tree as local-main `6edafea`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 7 | 0 |
| `src/data/server/flight.rs` | 18 | 2 |
| `src/query/influxql/transpiler.rs` | 16 | 0 |
| `src/query/query_engine/registration.rs` | 0 | 11 |
| `src/router/api.rs` | 4 | 2 |
| `src/router/health_checker.rs` | 4 | 3 |
| `src/router/query_coordinator/aggregation.rs` | 171 | 76 |
| `src/router/query_coordinator/routing.rs` | 11 | 2 |
| `src/router/shard_cache/node_mgmt.rs` | 29 | 5 |

### `b29e941` — feat(api): support compressed writes and InfluxQL lastpoint queries

2026-02-09 · M10 (consolidated history) · 7 files · +565/−134 text lines

Consolidated-history endpoint: same final tree as local-main `bda1893`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.lock` | 3 | 0 |
| `Cargo.toml` | 2 | 1 |
| `src/data/server/api.rs` | 41 | 24 |
| `src/query/influxql/mod.rs` | 65 | 5 |
| `src/query/influxql/transpiler.rs` | 282 | 2 |
| `src/router/api.rs` | 96 | 44 |
| `src/services/api.rs` | 76 | 58 |

### `044ad2d` — test: expand integration coverage and document benchmark results

2026-02-10 · M10 (consolidated history) · 11 files · +4,815/−0 text lines

Consolidated-history endpoint: same final tree as local-main `24e0046`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `Cargo.toml` | 2 | 0 |
| `benchmarks/correctness-comparison-2026-02-10.md` | 230 | 0 |
| `benchmarks/tsbs-comparison-2026-02-10.md` | 185 | 0 |
| `tests/api_endpoints.rs` | 434 | 0 |
| `tests/auth.rs` | 245 | 0 |
| `tests/common/mod.rs` | 947 | 0 |
| `tests/error_handling.rs` | 457 | 0 |
| `tests/influxql.rs` | 898 | 0 |
| `tests/retention.rs` | 364 | 0 |
| `tests/schema_strategy.rs` | 357 | 0 |
| `tests/timeseries.rs` | 696 | 0 |

### `1078ac4` — docs(portal): expand feature and operations documentation

2026-02-13 · M10 (consolidated history) · 90 files · +19,668/−4,194 text lines

Consolidated-history endpoint: same final tree as local-main `209836e`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `docs/admin-ui-query-storage-guide.md` | 0 | 1367 |
| `docs/admin-ui-quick-reference.md` | 0 | 421 |
| `docs/admin/admin-ui-query-storage-guide.md` | 1367 | 0 |
| `docs/admin/admin-ui-quick-reference.md` | 421 | 0 |
| `docs/admin/implementation-summary.md` | 621 | 0 |
| `docs/architecture/architecture-compaction-multilevel.md` | 5 | 5 |
| `docs/architecture/architecture-leveled-compaction.md` | 1 | 1 |
| `docs/architecture/architecture-overview.md` | 198 | 132 |
| `docs/architecture/architecture-schema-extraction.md` | 47 | 68 |
| `docs/architecture/architecture-wal-file-format.md` | 1 | 1 |
| `docs/architecture/architecture-wal-simple-solution.md` | 2 | 2 |
| `docs/changelogs/changelog-implementation.md` | 2 | 2 |
| `docs/feature-query-tracing.md` | 0 | 331 |
| `docs/features/feature-query-tracing.md` | 331 | 0 |
| `docs/features/features-age-based-consolidation.md` | 19 | 20 |
| `docs/features/features-compaction-nonpartitioned.md` | 5 | 5 |
| `docs/features/features-date-based-compaction.md` | 12 | 12 |
| `docs/features/features-flight-usage.md` | 1 | 1 |
| `docs/features/features-schema.md` | 20 | 27 |
| `docs/guide-forecasting.md` | 0 | 175 |
| `docs/guides/guide-configuration.md` | 243 | 117 |
| `docs/guides/guide-correct-query-examples.md` | 1 | 1 |
| `docs/guides/guide-forecasting.md` | 175 | 0 |
| `docs/guides/guide-master-slave-failover-demo.md` | 6 | 6 |
| `docs/guides/guide-master-slave-ha-local-testing.md` | 2 | 2 |
| `docs/guides/guide-master-slave-ha.md` | 2 | 2 |
| `docs/guides/guide-query-examples-updated.md` | 1 | 1 |
| `docs/guides/guide-query-examples.md` | 2 | 2 |
| `docs/guides/guide-quick-reference.md` | 13 | 13 |
| `docs/guides/guide-quickstart-main.md` | 87 | 57 |
| `docs/guides/guide-sql-queries.md` | 2 | 2 |
| `docs/implementation-summary.md` | 0 | 621 |
| `docs/reference/profiling_guide.md` | 6 | 6 |
| `docs/reference/quickstart.md` | 61 | 73 |
| `docs/reference/source-structure.md` | 125 | 33 |
| `docs/roadmaps/roadmap-features.md` | 2 | 2 |
| `docs/troubleshooting/troubleshooting-fixes-applied.md` | 14 | 14 |
| `drafts/icon_preview.html` | 104 | 0 |
| `drafts/icon_preview_birds.html` | 171 | 0 |
| `drafts/icon_preview_e2_beaks.html` | 215 | 0 |
| `drafts/icon_preview_e2_triangle.html` | 264 | 0 |
| `drafts/icon_preview_final.html` | 202 | 0 |
| `drafts/icon_preview_hex_swift.html` | 240 | 0 |
| `drafts/icon_preview_s2.html` | 369 | 0 |
| `drafts/icon_preview_swift.html` | 222 | 0 |
| `drafts/icon_preview_swift2.html` | 301 | 0 |
| `web/portal/README.md` | 28 | 0 |
| `web/portal/css/style.css` | 18 | 0 |
| `web/portal/docs/admin-console.html` | 716 | 0 |
| `web/portal/docs/admin-guide.html` | 647 | 0 |
| `web/portal/docs/admin-operations.html` | 825 | 0 |
| `web/portal/docs/api.html` | 241 | 8 |
| `web/portal/docs/architecture.html` | 48 | 8 |
| `web/portal/docs/caching.html` | 47 | 6 |
| `web/portal/docs/cluster-architecture.html` | 1353 | 0 |
| `web/portal/docs/cluster-getting-started.html` | 1027 | 0 |
| `web/portal/docs/cluster-scaling.html` | 1317 | 0 |
| `web/portal/docs/cluster.html` | 171 | 110 |
| `web/portal/docs/compaction.html` | 49 | 8 |
| `web/portal/docs/configuration.html` | 156 | 8 |
| `web/portal/docs/continuous-aggregates.html` | 793 | 0 |
| `web/portal/docs/data-model.html` | 49 | 8 |
| `web/portal/docs/examples.html` | 49 | 8 |
| `web/portal/docs/faq.html` | 49 | 8 |
| `web/portal/docs/flight-api.html` | 49 | 8 |
| `web/portal/docs/forecasting.html` | 701 | 0 |
| `web/portal/docs/grafana.html` | 427 | 0 |
| `web/portal/docs/guides.html` | 49 | 8 |
| `web/portal/docs/high-availability.html` | 49 | 8 |
| `web/portal/docs/iceberg.html` | 754 | 0 |
| `web/portal/docs/index.html` | 107 | 35 |
| `web/portal/docs/installation.html` | 49 | 8 |
| `web/portal/docs/line-protocol.html` | 49 | 8 |
| `web/portal/docs/monitoring.html` | 847 | 0 |
| `web/portal/docs/mysql-protocol.html` | 47 | 6 |
| `web/portal/docs/object-store.html` | 47 | 6 |
| `web/portal/docs/otlp.html` | 762 | 0 |
| `web/portal/docs/quickstart.html` | 48 | 8 |
| `web/portal/docs/retention.html` | 49 | 8 |
| `web/portal/docs/security.html` | 916 | 0 |
| `web/portal/docs/sql-queries.html` | 48 | 8 |
| `web/portal/docs/storage.html` | 49 | 8 |
| `web/portal/docs/streaming.html` | 834 | 0 |
| `web/portal/docs/testing.html` | 48 | 8 |
| `web/portal/docs/troubleshooting.html` | 49 | 8 |
| `web/portal/images/favicon.svg` | 4 | 39 |
| `web/portal/images/logo-horizontal.svg` | 7 | 49 |
| `web/portal/images/logo-icon.svg` | 4 | 53 |
| `web/portal/images/logo.svg` | 4 | 141 |
| `web/portal/index.html` | 205 | 83 |

### `a9da239` — refactor: simplify admin handlers and shared source code

2026-02-13 · M10 (consolidated history) · 17 files · +518/−889 text lines

Consolidated-history endpoint: same final tree as local-main `e6b5efc`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `src/admin/api.rs` | 7 | 97 |
| `src/admin/events.rs` | 2 | 13 |
| `src/admin/mod.rs` | 1 | 0 |
| `src/admin/query_api.rs` | 19 | 189 |
| `src/admin/response.rs` | 93 | 0 |
| `src/admin/state.rs` | 69 | 0 |
| `src/admin/storage_api.rs` | 37 | 201 |
| `src/data/server/api.rs` | 53 | 142 |
| `src/jobs/compactor/full.rs` | 114 | 88 |
| `src/meta/meta_node/commands.rs` | 0 | 6 |
| `src/router/api.rs` | 2 | 37 |
| `src/router/query_coordinator/tests.rs` | 1 | 2 |
| `src/services/api.rs` | 2 | 41 |
| `src/services/mysql_service.rs` | 0 | 3 |
| `src/services/otlp/metrics_converter.rs` | 70 | 69 |
| `src/services/rate_limiter.rs` | 1 | 1 |
| `src/services/shared.rs` | 47 | 0 |

### `9822ea4` — chore(dev): add Claude commands and development skills

2026-02-26 · M10 (consolidated history) · 22 files · +2,032/−0 text lines

Consolidated-history endpoint: same final tree as local-main `5327e72`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `.claude/commands/add-endpoint.md` | 18 | 0 |
| `.claude/commands/add-feature.md` | 28 | 0 |
| `.claude/commands/add-udf.md` | 19 | 0 |
| `.claude/commands/amend-push.md` | 14 | 0 |
| `.claude/commands/bench.md` | 15 | 0 |
| `.claude/commands/build.md` | 6 | 0 |
| `.claude/commands/commit-push.md` | 16 | 0 |
| `.claude/commands/dashboard.md` | 13 | 0 |
| `.claude/commands/explain-module.md` | 23 | 0 |
| `.claude/commands/perf-check.md` | 30 | 0 |
| `.claude/commands/proposal.md` | 20 | 0 |
| `.claude/commands/research-db.md` | 20 | 0 |
| `.claude/commands/test.md` | 11 | 0 |
| `.claude/skills/datafusion-development/SKILL.md` | 219 | 0 |
| `.claude/skills/distributed-coordination/SKILL.md` | 186 | 0 |
| `.claude/skills/mynadb-architecture/SKILL.md` | 122 | 0 |
| `.claude/skills/parquet-optimization/SKILL.md` | 146 | 0 |
| `.claude/skills/performance-tuning/SKILL.md` | 191 | 0 |
| `.claude/skills/protocol-implementation/SKILL.md` | 164 | 0 |
| `.claude/skills/rust-tsdb-patterns/SKILL.md` | 232 | 0 |
| `.claude/skills/testing-patterns/SKILL.md` | 269 | 0 |
| `docs/claude-code-commands.md` | 270 | 0 |

### `63ca50b` — ci(pages): deploy documentation with project-relative links

2026-07-03 · M10 (consolidated history) · 77 files · +119/−76 text lines

Consolidated-history endpoint: same final tree as local-main `6fe67e1`. This does not add a new milestone feature.

| File | Added | Deleted |
|---|---:|---:|
| `.github/workflows/static.yml` | 43 | 0 |
| `web/portal/docs/admin-console.html` | 1 | 1 |
| `web/portal/docs/admin-guide.html` | 1 | 1 |
| `web/portal/docs/admin-operations.html` | 1 | 1 |
| `web/portal/docs/api.html` | 1 | 1 |
| `web/portal/docs/architecture.html` | 1 | 1 |
| `web/portal/docs/caching.html` | 1 | 1 |
| `web/portal/docs/cluster-architecture.html` | 1 | 1 |
| `web/portal/docs/cluster-getting-started.html` | 1 | 1 |
| `web/portal/docs/cluster-scaling.html` | 1 | 1 |
| `web/portal/docs/cluster.html` | 1 | 1 |
| `web/portal/docs/compaction.html` | 1 | 1 |
| `web/portal/docs/configuration.html` | 1 | 1 |
| `web/portal/docs/continuous-aggregates.html` | 1 | 1 |
| `web/portal/docs/data-model.html` | 1 | 1 |
| `web/portal/docs/examples.html` | 1 | 1 |
| `web/portal/docs/faq.html` | 1 | 1 |
| `web/portal/docs/flight-api.html` | 1 | 1 |
| `web/portal/docs/forecasting.html` | 1 | 1 |
| `web/portal/docs/grafana.html` | 1 | 1 |
| `web/portal/docs/guides.html` | 1 | 1 |
| `web/portal/docs/high-availability.html` | 1 | 1 |
| `web/portal/docs/iceberg.html` | 1 | 1 |
| `web/portal/docs/index.html` | 1 | 1 |
| `web/portal/docs/installation.html` | 1 | 1 |
| `web/portal/docs/line-protocol.html` | 1 | 1 |
| `web/portal/docs/monitoring.html` | 1 | 1 |
| `web/portal/docs/mysql-protocol.html` | 1 | 1 |
| `web/portal/docs/object-store.html` | 1 | 1 |
| `web/portal/docs/otlp.html` | 1 | 1 |
| `web/portal/docs/quickstart.html` | 1 | 1 |
| `web/portal/docs/retention.html` | 1 | 1 |
| `web/portal/docs/security.html` | 1 | 1 |
| `web/portal/docs/sql-queries.html` | 1 | 1 |
| `web/portal/docs/storage.html` | 1 | 1 |
| `web/portal/docs/streaming.html` | 1 | 1 |
| `web/portal/docs/testing.html` | 1 | 1 |
| `web/portal/docs/troubleshooting.html` | 1 | 1 |
| `web/portal/index.html` | 1 | 1 |
| `web/portal/zh/docs/admin-console.html` | 1 | 1 |
| `web/portal/zh/docs/admin-guide.html` | 1 | 1 |
| `web/portal/zh/docs/admin-operations.html` | 1 | 1 |
| `web/portal/zh/docs/api.html` | 1 | 1 |
| `web/portal/zh/docs/architecture.html` | 1 | 1 |
| `web/portal/zh/docs/caching.html` | 1 | 1 |
| `web/portal/zh/docs/cluster-architecture.html` | 1 | 1 |
| `web/portal/zh/docs/cluster-getting-started.html` | 1 | 1 |
| `web/portal/zh/docs/cluster-scaling.html` | 1 | 1 |
| `web/portal/zh/docs/cluster.html` | 1 | 1 |
| `web/portal/zh/docs/compaction.html` | 1 | 1 |
| `web/portal/zh/docs/configuration.html` | 1 | 1 |
| `web/portal/zh/docs/continuous-aggregates.html` | 1 | 1 |
| `web/portal/zh/docs/data-model.html` | 1 | 1 |
| `web/portal/zh/docs/examples.html` | 1 | 1 |
| `web/portal/zh/docs/faq.html` | 1 | 1 |
| `web/portal/zh/docs/flight-api.html` | 1 | 1 |
| `web/portal/zh/docs/forecasting.html` | 1 | 1 |
| `web/portal/zh/docs/grafana.html` | 1 | 1 |
| `web/portal/zh/docs/guides.html` | 1 | 1 |
| `web/portal/zh/docs/high-availability.html` | 1 | 1 |
| `web/portal/zh/docs/iceberg.html` | 1 | 1 |
| `web/portal/zh/docs/index.html` | 1 | 1 |
| `web/portal/zh/docs/installation.html` | 1 | 1 |
| `web/portal/zh/docs/line-protocol.html` | 1 | 1 |
| `web/portal/zh/docs/monitoring.html` | 1 | 1 |
| `web/portal/zh/docs/mysql-protocol.html` | 1 | 1 |
| `web/portal/zh/docs/object-store.html` | 1 | 1 |
| `web/portal/zh/docs/otlp.html` | 1 | 1 |
| `web/portal/zh/docs/quickstart.html` | 1 | 1 |
| `web/portal/zh/docs/retention.html` | 1 | 1 |
| `web/portal/zh/docs/security.html` | 1 | 1 |
| `web/portal/zh/docs/sql-queries.html` | 1 | 1 |
| `web/portal/zh/docs/storage.html` | 1 | 1 |
| `web/portal/zh/docs/streaming.html` | 1 | 1 |
| `web/portal/zh/docs/testing.html` | 1 | 1 |
| `web/portal/zh/docs/troubleshooting.html` | 1 | 1 |
| `web/portal/zh/index.html` | 1 | 1 |

## Ref snapshot

Counts below describe the refs available locally when inspected; no network fetch was performed.

| Ref | Reachable commits | Outside local main |
|---|---:|---:|
| `refs/heads/archive/main-before-consolidation` | 485 | 485 |
| `refs/heads/archive/retimed-committer-only` | 485 | 485 |
| `refs/heads/archive/retimed-until-0300` | 485 | 485 |
| `refs/heads/main` | 487 | 0 |
| `refs/heads/retimed/main` | 485 | 0 |
| `refs/heads/roy/minio` | 195 | 195 |
| `refs/heads/simplified/main` | 395 | 395 |
| `refs/remotes/origin/HEAD` | 487 | 0 |
| `refs/remotes/origin/archive/main-before-retime` | 485 | 485 |
| `refs/remotes/origin/dev/admin` | 476 | 476 |
| `refs/remotes/origin/main` | 487 | 0 |
| `refs/remotes/origin/mynadb-dev` | 369 | 369 |
| `refs/remotes/origin/retimed/main` | 485 | 0 |
| `refs/remotes/origin/roy/bak` | 226 | 226 |
| `refs/remotes/origin/roy/cluster-bak` | 211 | 211 |
| `refs/remotes/origin/roy/cross_validation` | 191 | 191 |
| `refs/remotes/origin/roy/minio` | 195 | 195 |
| `refs/remotes/origin/roy/opt` | 362 | 362 |
| `refs/remotes/origin/roy/riverdb` | 150 | 150 |
| `refs/remotes/origin/simplified/main` | 395 | 395 |
| `refs/stash` | 488 | 3 |
| `refs/tags/0.1.1` | 192 | 192 |
| `refs/tags/v0.1.0` | 192 | 192 |
| `refs/tags/v0.1.1` | 342 | 342 |
