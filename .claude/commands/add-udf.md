---
description: Scaffold a new custom UDF or UDAF for the query engine
argument-hint: <function-name> <description>
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(cargo:*)
---

Add a new User-Defined Function (UDF) or Aggregate (UDAF) to MynaDB's DataFusion query engine. Specification: $ARGUMENTS

Steps:
1. Read existing UDFs in `src/query/functions/` to understand patterns:
   - `time_bucket.rs` for scalar UDF example
   - `first_last.rs` for UDAF example
   - `forecasting.rs` for complex UDF example
2. Read `src/query/functions/mod.rs` to see how functions are registered.
3. Create the new function file in `src/query/functions/`.
4. Implement the function following DataFusion's `ScalarUDFImpl` or `AggregateUDFImpl` trait.
5. Register in `mod.rs` and wire into the query engine registration.
6. Run `cargo build --release` to verify compilation.
7. Show example SQL usage of the new function.
