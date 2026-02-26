---
description: Run tests, optionally filtered by module name
argument-hint: [module-name]
allowed-tools: Bash(cargo:*)
---

Run MynaDB tests. If an argument is provided, run only matching tests:
- `cargo test --lib $ARGUMENTS` for module-specific tests
- `cargo test` if no argument given

Show test results summary. If tests fail, show the failing test names and error output.
