---
description: Build and verify dashboard HTML changes
allowed-tools: Bash(cargo:*), Read, Grep
---

The dashboard is a single HTML file embedded in the Rust binary via `include_str!()`.

Steps:
1. Check if `src/services/dashboard/index.html` has been modified (via `git diff --stat`).
2. Run `cargo build --release` to compile the changes into the binary.
3. Report build success/failure.

If the build fails, read the error and check the HTML file for issues.
