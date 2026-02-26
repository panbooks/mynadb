---
description: Create a new proposal document in docs/proposals/
argument-hint: <topic>
allowed-tools: Read, Write, Bash(ls:*), Glob, Grep, WebSearch, WebFetch, Task
---

Create a new proposal document for MynaDB. The topic is: $ARGUMENTS

Steps:
1. Read existing proposals in `docs/proposals/` to understand the format and style.
2. Research the topic thoroughly — analyze the MynaDB codebase for relevant existing code, and search the web for how other databases (InfluxDB, ClickHouse, TimescaleDB, GreptimeDB, etc.) implement similar features.
3. Write the proposal to `docs/proposals/proposal-{topic-slug}.md` following the existing format:
   - Status, Author, Date, Target Version header
   - Executive Summary
   - Problem Statement / Current State
   - Proposed Design (architecture, data flow, schema, config)
   - Trade-Off Analysis
   - Implementation Phases with effort estimates
   - Integration Points with existing code
4. Be thorough and concrete — include code examples, config TOML snippets, and file/module structure.
