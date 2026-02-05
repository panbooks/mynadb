#!/bin/bash
# Build test - verify compilation succeeds
cd "$(dirname "$0")"
echo "Testing compilation..."
cargo build --release 2>&1 | tee build.log
echo "Exit code: $?"
