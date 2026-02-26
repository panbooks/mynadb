---
description: Plan and scaffold a new feature module for MynaDB
argument-hint: <feature-name>
allowed-tools: Read, Write, Edit, Glob, Grep, Bash(cargo:*), Task, EnterPlanMode
---

Plan and scaffold a new feature for MynaDB: $ARGUMENTS

This is a multi-step process:

1. **Research phase**: Explore the codebase to understand existing patterns and identify integration points. Check how similar features are structured (e.g., `src/aggregates/`, `src/jobs/downsampling/`, `src/services/prometheus/`).

2. **Plan phase**: Enter plan mode and design:
   - New module structure (directory + files)
   - Configuration additions (`src/config.rs`)
   - API endpoints if needed (`src/services/api.rs`)
   - Background job if needed (`src/jobs/`)
   - Integration with query engine if needed
   - Test strategy

3. **Scaffold phase** (after plan approval):
   - Create module directory and files with stub implementations
   - Add config struct to `src/config.rs`
   - Wire into `src/node/standalone.rs` initialization
   - Add module declaration to `src/lib.rs` or parent mod.rs
   - Verify compilation with `cargo build --release`

Follow the project's conventions from CLAUDE.md: no hard-coded strings, use PathBuf for paths, constants at module top.
