# Claude Code Custom Commands for MynaDB

Custom slash commands for MynaDB development with [Claude Code](https://claude.ai/code). These commands are defined in `.claude/commands/` and available in any Claude Code session within this repository.

## Quick Reference

| Command | Arguments | Description |
|---------|-----------|-------------|
| `/amend-push` | — | Amend last commit and force push |
| `/commit-push` | — | New commit with auto message and push |
| `/build` | — | Build in release mode |
| `/test` | `[module]` | Run tests, optionally filtered |
| `/bench` | `[name]` | Run benchmarks |
| `/dashboard` | — | Build after dashboard changes |
| `/proposal` | `<topic>` | Create proposal in docs/proposals/ |
| `/research-db` | `<topic>` | Research database technologies |
| `/add-feature` | `<name>` | Plan and scaffold a new feature |
| `/add-endpoint` | `<spec>` | Scaffold a new API endpoint |
| `/add-udf` | `<name> <desc>` | Scaffold a new UDF/UDAF |
| `/explain-module` | `<module>` | Deep-dive module explanation |
| `/perf-check` | `<target>` | Analyze performance characteristics |

---

## Git Workflow

### `/amend-push`

Amend the current commit with all staged/unstaged changes and force push. No confirmation needed. This is the fastest way to iterate on a commit.

```
/amend-push
```

What it does:
1. Checks `git status` and `git diff`
2. Stages modified files
3. Amends last commit (`--no-edit`)
4. Force pushes to remote

### `/commit-push`

Create a new commit with an auto-generated conventional commit message and push.

```
/commit-push
```

What it does:
1. Analyzes all changes (staged + unstaged)
2. Reviews recent commit history for style consistency
3. Generates a commit message (feat/fix/refactor/docs/test/chore)
4. Creates a new commit and pushes

---

## Build & Test

### `/build`

Build MynaDB in release mode. Shows only the last 5 lines of output for clean feedback.

```
/build
```

### `/test`

Run tests, optionally filtered by module name substring.

```
# Run all tests
/test

# Run cache module tests
/test cache

# Run compactor tests
/test compactor

# Run WAL tests
/test wal
```

### `/bench`

Run performance benchmarks. Available benchmarks:

```
# Line protocol parsing performance
/bench line_protocol_bench

# Query execution performance
/bench query_bench

# Time function UDF performance
/bench time_functions_bench
```

### `/dashboard`

Build after editing the dashboard HTML (`src/services/dashboard/index.html`). The dashboard is embedded in the binary via `include_str!()`, so a rebuild is required after any HTML change.

```
/dashboard
```

---

## Research & Documentation

### `/proposal`

Create a full proposal document in `docs/proposals/`. Researches the topic across the MynaDB codebase and the web, then writes a structured proposal following the project's existing format.

```
# Create a proposal for streaming aggregation
/proposal streaming-aggregation

# Create a proposal for PromQL support
/proposal promql-query-engine

# Create a proposal for edge deployment mode
/proposal edge-mode
```

The generated proposal includes:
- Executive Summary
- Problem Statement / Current State
- Proposed Design (architecture, data flow, config)
- Trade-Off Analysis
- Implementation Phases with effort estimates
- Integration Points with existing code

### `/research-db`

Research database technologies and compare with MynaDB. Searches the web for the latest information and checks the MynaDB codebase for existing implementations.

```
# Research a specific database
/research-db ClickHouse materialized views

# Research a technology
/research-db PromQL query engines

# Compare approaches
/research-db Iceberg vs Delta Lake vs Hudi

# Research a feature category
/research-db real-time streaming aggregation databases
```

Output includes a structured comparison table, architecture analysis, and source links.

---

## Code Development

### `/add-feature`

Plan and scaffold a new feature module. Enters plan mode first to design the architecture, then scaffolds the code after approval.

```
# Add OpenTelemetry ingestion
/add-feature otlp-ingestion

# Add PromQL query support
/add-feature promql-engine

# Add vector search
/add-feature vector-similarity-search
```

Scaffold includes:
- Module directory and files
- Config struct in `src/config.rs`
- Wiring in `src/node/standalone.rs`
- Module declarations
- Compilation verification

### `/add-endpoint`

Scaffold a new HTTP API endpoint following existing patterns in `src/services/api.rs`.

```
# Add a new ingest endpoint
/add-endpoint POST /api/v1/ingest batch JSON ingestion

# Add a status endpoint
/add-endpoint GET /cluster/status cluster health and node status

# Add a management endpoint
/add-endpoint DELETE /tables/:db/:schema/:table drop a table
```

### `/add-udf`

Scaffold a new User-Defined Function (scalar UDF) or Aggregate (UDAF) for DataFusion.

```
# Add a moving average function
/add-udf moving_average sliding window moving average over time series

# Add a percentile function
/add-udf approx_percentile approximate percentile using t-digest

# Add a string function
/add-udf json_extract extract fields from JSON string columns
```

---

## Analysis

### `/explain-module`

Get a deep-dive explanation of any MynaDB module or subsystem. Reads all source files and explains the architecture, data flow, key types, and integration points.

```
# Explain the compaction system
/explain-module compactor

# Explain the query cache
/explain-module query/cache

# Explain the write path
/explain-module storage/cache

# Explain continuous aggregates
/explain-module aggregates

# Explain the cluster coordinator
/explain-module cluster
```

### `/perf-check`

Analyze performance characteristics of a query, write path, or specific module. Identifies bottlenecks, lock contention, allocation patterns, and suggests optimizations.

```
# Analyze a specific query
/perf-check SELECT * FROM cpu WHERE time > now() - 1h GROUP BY host

# Analyze the write path
/perf-check write path for high-cardinality tags

# Analyze a module
/perf-check compactor partition merge with 1000 L0 files

# Analyze cache performance
/perf-check query cache hit rate for repeated dashboard queries
```

---

## Adding New Commands

To add a new command, create a markdown file in `.claude/commands/`:

```markdown
---
description: Brief description shown in /help
argument-hint: [optional-args]
allowed-tools: Read, Write, Bash(cargo:*)
---

Prompt content here. Use $ARGUMENTS for args.
```

See [Claude Code documentation](https://docs.anthropic.com/en/docs/claude-code) for full command syntax.
