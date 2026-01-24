#!/bin/bash

# Fuzz Testing Script for MynaDB
#
# Runs cargo-fuzz targets for parser testing.
# Requires nightly Rust: rustup install nightly
#
# Usage:
#   ./scripts/run_fuzz.sh [target] [duration]
#
# Examples:
#   ./scripts/run_fuzz.sh                    # Run all targets for 60 seconds each
#   ./scripts/run_fuzz.sh line_protocol 300  # Run line_protocol for 5 minutes
#   ./scripts/run_fuzz.sh time_interval 60   # Run time_interval for 1 minute
#   ./scripts/run_fuzz.sh all 30             # Run all targets for 30 seconds each

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
DEFAULT_DURATION=60
TARGETS=("fuzz_line_protocol" "fuzz_time_interval" "fuzz_field_value")

# Parse arguments
TARGET=${1:-"all"}
DURATION=${2:-$DEFAULT_DURATION}

# Check for nightly Rust
if ! rustup run nightly rustc --version > /dev/null 2>&1; then
    log_error "Nightly Rust is required for cargo-fuzz"
    log_info "Install with: rustup install nightly"
    exit 1
fi

# Check for cargo-fuzz
if ! cargo fuzz --version > /dev/null 2>&1; then
    log_info "Installing cargo-fuzz..."
    cargo install cargo-fuzz
fi

# Change to project root
cd "$(dirname "$0")/.."

run_fuzzer() {
    local target=$1
    local duration=$2

    log_info "Running $target for ${duration}s..."

    if cargo +nightly fuzz run "$target" -- -max_total_time="$duration" 2>&1 | tail -5; then
        log_success "$target completed without crashes"
        return 0
    else
        log_error "$target may have found issues - check fuzz/artifacts/"
        return 1
    fi
}

echo "========================================================"
echo "     MynaDB Fuzz Testing"
echo "========================================================"
echo "Duration: ${DURATION}s per target"
echo ""

FAILED=0

if [ "$TARGET" = "all" ]; then
    for t in "${TARGETS[@]}"; do
        run_fuzzer "$t" "$DURATION" || FAILED=$((FAILED + 1))
        echo ""
    done
else
    # Check if target exists
    FULL_TARGET="fuzz_${TARGET}"
    if [[ " ${TARGETS[*]} " =~ " ${FULL_TARGET} " ]]; then
        run_fuzzer "$FULL_TARGET" "$DURATION" || FAILED=1
    elif [[ " ${TARGETS[*]} " =~ " ${TARGET} " ]]; then
        run_fuzzer "$TARGET" "$DURATION" || FAILED=1
    else
        log_error "Unknown target: $TARGET"
        log_info "Available targets: ${TARGETS[*]}"
        exit 1
    fi
fi

echo "========================================================"
if [ $FAILED -eq 0 ]; then
    log_success "All fuzz tests completed successfully"
    exit 0
else
    log_error "$FAILED fuzz test(s) found issues"
    log_info "Check fuzz/artifacts/ for crash inputs"
    exit 1
fi
