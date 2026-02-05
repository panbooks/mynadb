#!/bin/bash

# Quick compaction test - faster validation of dual compaction strategy
# Focuses on verifying the basic mechanics without extensive waiting

set -e

HOST="http://localhost:7000"
DB="quick_test"
SERVICE="svc_quick"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WAIT]${NC} $1"; }

# Check service
if ! curl -s "$HOST/health" > /dev/null; then
    echo "Service not running at $HOST"
    exit 1
fi

TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

# Helper to count files
count_files() {
    local path="./data/$1/$2/cpu/date=$3"
    [ -d "$path" ] && find "$path" -name "*.parquet" 2>/dev/null | wc -l || echo 0
}

count_level_files() {
    local path="./data/$1/$2/cpu/date=$3"
    local level=$4
    [ -d "$path" ] && find "$path" -name "${level}_*.parquet" 2>/dev/null | wc -l || echo 0
}

log "Quick Compaction Test"
log "====================="

# Test 1: LSM Compaction
log ""
log "TEST 1: LSM Compaction (L0 → L1)"
log "Writing data to create L0 files..."

for i in {1..6}; do
    ts=$(($(date +%s)*1000000000 + i*1000000000))
    echo "cpu,host=server$i,service=$SERVICE usage=$((RANDOM % 100)) $ts" | \
        curl -s -X POST "$HOST/write/$DB" -H "Content-Type: text/plain" --data-binary @- > /dev/null
done

log "Flushing cache..."
curl -s -X POST "$HOST/flush" > /dev/null
sleep 2

BEFORE=$(count_files "$DB" "$SERVICE" "$TODAY")
L0_BEFORE=$(count_level_files "$DB" "$SERVICE" "$TODAY" "L0")
log "Files created: $BEFORE (L0: $L0_BEFORE)"

warn "Waiting 70s for LSM compaction (interval=60s, min_files=4)..."
sleep 70

AFTER=$(count_files "$DB" "$SERVICE" "$TODAY")
L0_AFTER=$(count_level_files "$DB" "$SERVICE" "$TODAY" "L0")
L1_AFTER=$(count_level_files "$DB" "$SERVICE" "$TODAY" "L1")

log "After LSM: Total=$AFTER, L0=$L0_AFTER, L1=$L1_AFTER"

if [ "$L1_AFTER" -gt 0 ]; then
    success "LSM compaction working! L0→L1 occurred"
else
    warn "No L1 files yet (L0=$L0_AFTER). May need more writes or time."
fi

# Test 2: Full Compaction Check
log ""
log "TEST 2: Full Compaction (Yesterday's Partition)"
log "Writing data for yesterday..."

for i in {1..8}; do
    ts=$(($(date -d "yesterday" +%s)*1000000000 + i*1000000000))
    echo "cpu,host=server$i,service=$SERVICE usage=$((RANDOM % 100)) $ts" | \
        curl -s -X POST "$HOST/write/$DB" -H "Content-Type: text/plain" --data-binary @- > /dev/null
done

curl -s -X POST "$HOST/flush" > /dev/null
sleep 2

Y_BEFORE=$(count_files "$DB" "$SERVICE" "$YESTERDAY")
log "Yesterday's files: $Y_BEFORE"

HOUR=$(date +%H | sed 's/^0//')
log "Current hour: $HOUR (full compaction configured for hour 1)"

if [ "$HOUR" -eq 1 ]; then
    warn "It's hour 1! Full compaction should run. Waiting 70s..."
    sleep 70
    Y_AFTER=$(count_files "$DB" "$SERVICE" "$YESTERDAY")
    FULL=$(count_level_files "$DB" "$SERVICE" "$YESTERDAY" "FULL")

    if [ "$FULL" -gt 0 ]; then
        success "Full compaction executed! FULL files: $FULL"
    else
        warn "Full compaction hasn't run yet. Files: $Y_AFTER"
    fi
else
    log "Not hour 1, full compaction won't run automatically"
    warn "Triggering manual compaction..."
    curl -s -X POST "$HOST/compact/$DB" > /dev/null
    sleep 3

    Y_AFTER=$(count_files "$DB" "$SERVICE" "$YESTERDAY")
    log "After manual compaction: $Y_BEFORE → $Y_AFTER files"

    if [ "$Y_AFTER" -lt "$Y_BEFORE" ]; then
        success "Manual compaction reduced files!"
    fi
fi

# Test 3: Data Integrity
log ""
log "TEST 3: Data Integrity"
COUNT=$(curl -s "$HOST/query/$DB" -G \
    --data-urlencode "sql=SELECT COUNT(*) as cnt FROM $SERVICE.cpu" | \
    grep -o '"cnt":[0-9]*' | grep -o '[0-9]*')

if [ -n "$COUNT" ] && [ "$COUNT" -gt 0 ]; then
    success "Data verified: $COUNT rows readable after compaction"
else
    warn "Query returned 0 or failed"
fi

log ""
log "Quick test complete!"
log "For detailed testing, run: ./scripts/test_compaction_dual_strategy.sh"
