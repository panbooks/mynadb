# Contributing to MynaDB

MynaDB is under development. Start with [project status](docs/project-status.md)
and an issue describing the problem or proposed behavior.

## Development environment

Install a current stable Rust toolchain, a C/C++ build toolchain, `pkg-config`,
OpenSSL development headers, and Protocol Buffers' `protoc` compiler.
Some native dependencies may additionally require CMake on other platforms.
The initial validation environment uses Rust/Cargo 1.92.0 on Linux x86_64;
this does not establish the minimum supported Rust version.

```sh
git clone https://github.com/panbooks/mynadb.git
cd mynadb
cargo build --locked
cargo test --locked --lib
```

For HTTP integration tests, build the debug binary first: the existing harness
looks for `target/release/mynadb`, then `target/debug/mynadb`.

```sh
cargo build --locked
cargo test --locked --test api_endpoints -- --ignored --test-threads=1
python3 scripts/validation/quickstart_smoke.py --binary target/debug/mynadb
```

Tests need free local ports and temporary disk space. Do not run legacy scripts
against a service holding real data; some scripts use fixed ports or delete data.
Use isolated directories and record the exact commands and configuration.

Frontend work lives in `admin-ui/`; use its lockfile with `npm ci`, then
`npm run build`. The Grafana plugin is a separate package in
`grafana-mynadb-datasource/` and has not been certified by this readiness audit.

## Branches and pull requests

Create a branch from current `main`. Keep changes focused, use a descriptive
commit subject, and explain the user-visible change, relevant tests, and any
compatibility or storage-format impact in the PR. Add regression coverage for
behavior changes. Run `cargo fmt --all --check` for Rust changes. Report checks
you could not run rather than presenting them as passed.

Documentation fixes can be verified with relative-link checks and examples;
they do not require running the entire Rust test suite.

## Issues and feedback

Use the bug report or feature request form. Include the commit/version, OS,
configuration with credentials removed, reproduction commands, expected result,
and actual result. Use [Discussions](https://github.com/panbooks/mynadb/discussions)
for architecture and open-ended design questions. Maintainers have not yet
published a response-time commitment.

## Licensing

The main project's license and code ownership are pending maintainer confirmation
in the readiness review. The portal's Apache-2.0 text and plugin metadata alone
do not establish the root project's license. Do not assume a final contribution
licensing policy until a root license and this section have been updated.
