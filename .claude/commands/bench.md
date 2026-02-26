---
description: Run benchmarks (line_protocol, query, time_functions)
argument-hint: [bench-name]
allowed-tools: Bash(cargo:*)
---

Run MynaDB benchmarks. Available benchmarks:
- `line_protocol_bench` — Line protocol parsing performance
- `query_bench` — Query execution performance
- `time_functions_bench` — Time function UDF performance

If an argument is provided: `cargo bench --bench $ARGUMENTS`
If no argument: list the available benchmarks and ask which to run.

Show the benchmark results with throughput numbers.
