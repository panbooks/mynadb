---
description: Scaffold a new HTTP API endpoint for MynaDB
argument-hint: <METHOD> <path> <description>
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(cargo:*)
---

Add a new API endpoint to MynaDB. Specification: $ARGUMENTS

Steps:
1. Read `src/services/api.rs` to understand the existing endpoint patterns (handler signature, AppState usage, error handling, response format).
2. Read `src/main.rs` or `src/node/common.rs` to understand how routes are registered.
3. Implement the handler function in `src/services/api.rs` following existing patterns:
   - Use `AppState` for shared state access
   - Use existing error handling patterns
   - Return JSON responses matching existing format
4. Register the route in the router builder.
5. Run `cargo build --release` to verify compilation.
6. Show the new endpoint summary (method, path, what it does).
