#!/bin/bash

# Memory analysis script for stats-service
# Analyzes memory usage and provides optimization recommendations

set -e

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

BASE_URL="http://localhost:7000"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}=========================================="
    echo -e "$1"
    echo -e "==========================================${NC}"
}

print_section() {
    echo ""
    echo -e "${CYAN}--- $1 ---${NC}"
}

format_bytes() {
    local bytes=$1
    if [ $bytes -lt 1024 ]; then
        echo "${bytes}B"
    elif [ $bytes -lt 1048576 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $bytes/1024}")KB"
    elif [ $bytes -lt 1073741824 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $bytes/1048576}")MB"
    else
        echo "$(awk "BEGIN {printf \"%.2f\", $bytes/1073741824}")GB"
    fi
}

calculate_bloom_memory() {
    local expected_items=$1
    local fp_rate=$2
    local days=$3

    # Formula: bits = -n * ln(p) / (ln(2)^2)
    # where n = expected_items, p = fp_rate
    local bits=$(awk "BEGIN {print -$expected_items * log($fp_rate) / (log(2)^2)}")
    local bytes=$(awk "BEGIN {print int($bits / 8)}")
    local bytes_per_day=$bytes
    local total_bytes=$(awk "BEGIN {print int($bytes * $days * 2)}")  # x2 for PMC+UMC

    echo "$total_bytes"
}

# Get PID
PID=$(pgrep tsdb-stats || echo "")

if [ -z "$PID" ]; then
    echo -e "${RED}Error: tsdb-stats process not found${NC}"
    exit 1
fi

print_header "Stats Service Memory Analysis"

# System memory info
print_section "System Memory"
free -h | grep -E "Mem:|Swap:"

# Process memory info
print_section "Process Memory Usage"
ps aux | grep tsdb-stats | grep -v grep | awk '{printf "PID: %s\nVSZ: %s KB\nRSS: %s KB (%.1f%%)\n", $2, $5, $6, $4}'

# Convert to human readable
RSS_KB=$(ps -p $PID -o rss= | tr -d ' ')
RSS_BYTES=$((RSS_KB * 1024))
echo "RSS: $(format_bytes $RSS_BYTES)"

# Memory maps
print_section "Memory Map Summary"
echo "Top memory regions:"
pmap -x $PID 2>/dev/null | sort -k3 -n -r | head -10 | awk 'NR>1 {printf "%-15s %10s KB  %s\n", $1, $3, $NF}' || echo "pmap not available"

# Query service stats
print_section "Service Statistics"
curl -s "$BASE_URL/stats" 2>/dev/null || echo "Service stats not available"

# Read config and calculate expected memory
print_section "Bloom Filter Memory Estimates"

echo ""
echo "Based on config.toml settings:"
echo ""

# These values should match your config.toml
echo "Database: arch_monitor_roc"
echo "  Expected series/day: 2,000,000,000"
echo "  False positive rate: 0.5%"
echo "  Cache retention: 2 days"
echo "  Schemas tracked: PMC + UMC (x2)"
BLOOM_ROC=$(calculate_bloom_memory 2000000000 0.005 2)
echo -e "  ${YELLOW}Estimated memory: $(format_bytes $BLOOM_ROC)${NC}"

echo ""
echo "Database: arch_monitor_rec"
echo "  Expected series/day: 10,000,000,000"
echo "  False positive rate: 1%"
echo "  Cache retention: 2 days"
echo "  Schemas tracked: PMC + UMC (x2)"
BLOOM_REC=$(calculate_bloom_memory 10000000000 0.01 2)
echo -e "  ${YELLOW}Estimated memory: $(format_bytes $BLOOM_REC)${NC}"

echo ""
TOTAL_BLOOM=$((BLOOM_ROC + BLOOM_REC))
echo -e "${CYAN}Total bloom filter memory: $(format_bytes $TOTAL_BLOOM)${NC}"

# Memory breakdown estimate
print_section "Memory Breakdown (Estimated)"

echo "1. Bloom Filters: $(format_bytes $TOTAL_BLOOM)"
echo "2. DataFusion/Arrow: ~1-2 GB (query engine)"
echo "3. Data Cache: Check /stats endpoint"
echo "4. Series Tracker Pending: Small (<100MB)"
echo "5. HTTP/gRPC Buffers: Small (<100MB)"
echo "6. Other (Rust overhead): ~500MB"

# Optimization recommendations
print_header "Optimization Recommendations"

echo ""
echo -e "${YELLOW}Current Memory Usage: $(format_bytes $RSS_BYTES)${NC}"
echo -e "${YELLOW}Estimated Bloom Memory: $(format_bytes $TOTAL_BLOOM)${NC}"
echo ""

if [ $RSS_BYTES -gt $((20 * 1024 * 1024 * 1024)) ]; then
    echo -e "${RED}⚠ High memory usage detected!${NC}"
    echo ""
    echo "Recommendations to reduce memory:"
    echo ""
    echo "1. ${CYAN}Reduce bloom filter size:${NC}"
    echo "   Edit config.toml:"
    echo "   - Lower expected_series_per_day (more realistic estimates)"
    echo "   - Increase false_positive_rate (0.01 → 0.02 saves ~50% memory)"
    echo "   - Reduce cache_retention_days (2 → 1 saves 50% memory)"
    echo ""
    echo "2. ${CYAN}Switch to HashSet for some databases:${NC}"
    echo "   - Remove database from bloom_databases list"
    echo "   - Good if actual series < 100M per day"
    echo "   - HashSet uses ~24 bytes per series"
    echo ""
    echo "3. ${CYAN}Split databases:${NC}"
    echo "   - Separate high-cardinality metrics into different databases"
    echo "   - Run multiple service instances with different configs"
    echo ""
    echo "Example optimized config:"
    echo ""
    echo "  [series_tracker]"
    echo "  cache_retention_days = 1  # Was: 2 (saves 50%)"
    echo ""
    echo "  [[series_tracker.bloom_custom]]"
    echo "  database = \"arch_monitor_roc\""
    echo "  expected_series_per_day = 1000000000  # Was: 2B (saves ~50%)"
    echo "  false_positive_rate = 0.01             # Was: 0.005 (saves ~35%)"
    echo ""
    echo "  [[series_tracker.bloom_custom]]"
    echo "  database = \"arch_monitor_rec\""
    echo "  expected_series_per_day = 5000000000  # Was: 10B (saves ~50%)"
    echo "  false_positive_rate = 0.02            # Was: 0.01 (saves ~30%)"
    echo ""
    echo "  ${GREEN}Estimated savings: ~70% (from 59GB to ~18GB)${NC}"
else
    echo -e "${GREEN}✓ Memory usage is reasonable${NC}"
fi

# Check actual series counts
print_section "Actual Series Counts (if available)"
echo ""
echo "To see actual cardinality, query the UMC/PMC tables:"
echo ""
echo "  mysql> SELECT database, COUNT(*) FROM _system.umc GROUP BY database;"
echo "  mysql> SELECT database, COUNT(*) FROM _system.pmc GROUP BY database;"
echo ""
echo "Compare actual counts to configured expected_series_per_day"
echo "to optimize bloom filter sizes."

print_header "Memory Profiling Complete"

echo ""
echo "For continuous monitoring:"
echo "  watch -n 5 'ps aux | grep tsdb-stats | grep -v grep'"
echo ""
echo "For detailed memory tracking:"
echo "  pidstat -r -p $PID 1"
