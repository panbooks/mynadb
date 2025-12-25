#!/bin/bash
cd /root/projects/stats-service
echo "Testing compilation..."
cargo build --release 2>&1 | tee build.log
echo "Exit code: $?"
