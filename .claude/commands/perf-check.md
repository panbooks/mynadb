---
description: Analyze performance of a query or write path
argument-hint: [query-or-path]
allowed-tools: Read, Glob, Grep, Bash(cargo:*), Task
---

Analyze performance characteristics of: $ARGUMENTS

Steps:
1. If it's a SQL query: trace the execution path through the query engine.
   - Identify which TableProvider is used
   - Check if partition pruning applies
   - Check if predicate pushdown applies
   - Check if query cache would help
   - Suggest EXPLAIN/ANALYZE usage

2. If it's a write path: trace from ingestion to persistence.
   - Line protocol parsing overhead
   - WAL write latency
   - Cache shard contention
   - Flush batch size optimization
   - Parquet writer settings

3. If it's a module/function: read the code and identify:
   - Hot loops and allocation patterns
   - Lock contention points
   - Async bottlenecks
   - Memory usage patterns

Present findings with specific line references and concrete optimization suggestions.
