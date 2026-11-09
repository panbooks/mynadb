# MynaDB release notes

GitHub-ready release-note bodies for the retrospective milestone tags. Open a
version below and copy that file's Markdown into the corresponding GitHub Release.
These documents do not create or publish GitHub Releases.

Tag targets were checked against local Git objects and `origin`'s advertised tags.
Each note describes that snapshot and its delta from the preceding tag, rather
than assuming all historical implementations survive into later versions.
Development periods are derived from author dates; they are not publication dates.

| Tag | Theme | Source commit | New commits since previous tag |
| --- | --- | --- | ---: |
| [v0.0.0](v0.0.0.md) | Standalone Database Foundation | `aaaabda` | 43 |
| [v0.1.0](v0.1.0.md) | Durability and Standalone Operations | `3291005` | 65 |
| [v0.2.0](v0.2.0.md) | Initial High Availability | `3f7d7af` | 15 |
| [v0.3.0](v0.3.0.md) | Object-Store Architecture and Execution Improvements | `4f71930` | 22 |
| [v0.4.0](v0.4.0.md) | Query-Engine Maturity and Validation | `64acecb` | 49 |
| [v0.5.0](v0.5.0.md) | Distributed Database and Raft Control Plane | `34f3bf3` | 56 |
| [v0.6.0](v0.6.0.md) | Recovery, Standby Redesign, and Controlled Scale-In | `bb96dda` | 51 |
| [v0.7.0](v0.7.0.md) | Table Ownership and Partition-Aware Routing | `c49b32e` | 78 |
| [v0.8.0](v0.8.0.md) | Operator Console, Ecosystem Integrations, and Time-Series Analytics | `ad8a4be` | 50 |
| [v0.9.0](v0.9.0.md) | Correctness Hardening and Observability Ingestion | `4a147b4` | 41 |
| [v0.9.1](v0.9.1.md) | Dashboard and Time-Bucket Query Fixes | `837839e` | 14 |
| [v0.9.2](v0.9.2.md) | Dependency and Documentation Maintenance | `d603624` | 3 |

## Reading the historical versions

- v0.0.0–v0.2.0 do not persist metric field values as general queryable columns.
  v0.3.0 restores typed fields and removes the original HA and series-tracking systems.
- v0.4.0 introduces the MynaDB name and changes `timestamp` to `time`.
- v0.5.0 introduces the built-in Meta cluster; v0.6.0 changes standby recovery;
  v0.7.0 reshapes ownership and physical routing.
- v0.8.0 expands operator/security/analytics features, with explicit implementation gaps.
- v0.9.0 introduces OTLP and initial Iceberg code; v0.9.1 includes the complete
  intervening documentation/refactoring delta plus dashboard/time-bucket fixes.
- v0.9.2 changes only README and the admin-UI lockfile. Its exact source commit
  has prior readiness-test evidence; that evidence does not certify other tags.

Cargo package versions, historical executable names, API defaults, data layouts,
and configuration formats do not always match the release labels or current main.
Use the compatibility notes in each file. Source inspection is distinguished from
actual runtime verification; no new historical-tag benchmarks or full release
certification were performed while generating these notes.
