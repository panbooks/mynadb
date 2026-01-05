#!/usr/bin/env bash
set -euo pipefail
set -x

# Ensure we're inside a git repository
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: not inside a git repository"
  exit 1
fi

# Try to get exact tag, fallback to short commit hash
if git describe --tags --exact-match >/dev/null 2>&1; then
  version=$(git describe --tags --exact-match)
else
  version=$(git rev-parse --short HEAD)
fi

archive="dist.tar"

cargo build --release

rm -rf dist dist.tar
mkdir -p dist

cp -f target/release/tsdb-stats dist/
cp -f config.toml dist/
cp -f deploy.sh dist/
cp -f tsdb-stats.service dist/

rm -f "${archive}"
tar czf "${archive}" dist/

echo "Created archive: ${archive}"
