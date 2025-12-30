#!/bin/bash

# Force test full compaction by setting config hour to current hour
# This allows testing full compaction without waiting for 01:00

set -e

CONFIG_FILE="config.toml"
CONFIG_BACKUP="config.toml.backup"
HOST="http://localhost:7000"
DB="full_compact_test"
SERVICE="svc_full"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup() {
    if [ -f "$CONFIG_BACKUP" ]; then
        log "Restoring original config..."
        mv "$CONFIG_BACKUP" "$CONFIG_FILE"
    fi
}

trap cleanup EXIT

log "=========================================="
log "Full Compaction Force Test"
log "=========================================="
log ""

# Get current hour
CURRENT_HOUR=$(date +%H | sed 's/^0//')
log "Current hour: $CURRENT_HOUR"

# Backup config
if [ -f "$CONFIG_FILE" ]; then
    cp "$CONFIG_FILE" "$CONFIG_BACKUP"
    log "Config backed up"
else
    log "No config file found, creating test config..."
    cat > "$CONFIG_FILE" <<EOF
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "./data"

[compaction]
interval_secs = 60
min_files = 4
major_min_files = 4
target_file_size_mb = 200
max_level = 4
full_compaction_enabled = true
full_compaction_hour = $CURRENT_HOUR
full_compaction_interval_secs = 60
full_compaction_target_size_mb = 200

[cache]
flush_interval_secs = 30

[series_tracker]
flush_interval_secs = 60

[logging]
level = "info"
EOF
fi

# Update config to use current hour
log "Setting full_compaction_hour to $CURRENT_HOUR in config..."

if grep -q "full_compaction_hour" "$CONFIG_FILE"; then
    sed -i "s/full_compaction_hour = [0-9]*/full_compaction_hour = $CURRENT_HOUR/" "$CONFIG_FILE"
else
    # Add it if not present
    sed -i '/\[compaction\]/a full_compaction_hour = '$CURRENT_HOUR "$CONFIG_FILE"
fi

# Also set check interval to 60s for faster testing
if grep -q "full_compaction_interval_secs" "$CONFIG_FILE"; then
    sed -i "s/full_compaction_interval_secs = [0-9]*/full_compaction_interval_secs = 60/" "$CONFIG_FILE"
else
    sed -i '/\[compaction\]/a full_compaction_interval_secs = 60' "$CONFIG_FILE"
fi

success "Config updated for immediate testing"
warn "You need to RESTART the service for config changes to take effect!"
log ""

read -p "Have you restarted the service? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log "Please restart the service and run this script again"
    exit 1
fi

# Check if service is running
if ! curl -s "$HOST/health" > /dev/null; then
    error "Service is not running at $HOST"
    exit 1
fi

YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
log "Testing full compaction for yesterday: $YESTERDAY"
log ""

# Write data for yesterday
log "Step 1: Writing data for yesterday..."
for i in {1..12}; do
    ts=$(($(date -d "yesterday" +%s)*1000000000 + i*1000000000))
    for j in {1..100}; do
        echo "cpu,host=server$j,service=$SERVICE usage=$((RANDOM % 100)) $((ts + j*1000000))"
    done | curl -s -X POST "$HOST/write/$DB" -H "Content-Type: text/plain" --data-binary @- > /dev/null
    echo -n "."
done
echo ""

log "Flushing cache..."
curl -s -X POST "$HOST/flush" > /dev/null
sleep 2

# Count files before
PARTITION_PATH="./data/$DB/$SERVICE/cpu/date=$YESTERDAY"
if [ -d "$PARTITION_PATH" ]; then
    BEFORE=$(find "$PARTITION_PATH" -name "*.parquet" | wc -l)
    log "Files before full compaction: $BEFORE"
    ls -lh "$PARTITION_PATH"/*.parquet 2>/dev/null | tail -5
else
    error "Partition directory not found: $PARTITION_PATH"
    exit 1
fi

log ""
log "Step 2: Waiting for full compaction to trigger..."
warn "Current hour is $CURRENT_HOUR (matches config)"
warn "Check interval is 60s, waiting 70 seconds..."

for i in {1..7}; do
    echo -n "."
    sleep 10
done
echo ""

# Check results
if [ -d "$PARTITION_PATH" ]; then
    AFTER=$(find "$PARTITION_PATH" -name "*.parquet" | wc -l)
    FULL_FILES=$(find "$PARTITION_PATH" -name "FULL_*.parquet" | wc -l)

    log ""
    log "Results:"
    echo "  Files before: $BEFORE"
    echo "  Files after:  $AFTER"
    echo "  FULL files:   $FULL_FILES"

    log ""
    log "Current partition contents:"
    ls -lh "$PARTITION_PATH"/*.parquet 2>/dev/null

    if [ "$FULL_FILES" -gt 0 ]; then
        success "Full compaction executed successfully!"
        success "Created $FULL_FILES consolidated file(s)"

        # Calculate size
        TOTAL_SIZE=$(du -sh "$PARTITION_PATH" | cut -f1)
        log "Partition size: $TOTAL_SIZE"

        if [ "$AFTER" -eq 1 ]; then
            success "All files merged into single FULL file!"
        fi
    else
        warn "No FULL files created yet"

        if [ "$AFTER" -lt "$BEFORE" ]; then
            log "LSM compaction may have occurred instead (files reduced: $BEFORE → $AFTER)"
        else
            warn "Partition may be > 200MB (skipped) or needs more time"

            # Check size
            SIZE_BYTES=$(du -sb "$PARTITION_PATH" | cut -f1)
            SIZE_MB=$((SIZE_BYTES / 1024 / 1024))
            log "Partition size: ${SIZE_MB}MB"

            if [ "$SIZE_MB" -gt 200 ]; then
                log "Partition exceeds 200MB target, correctly skipped by full compaction"
            fi
        fi
    fi
else
    error "Partition directory disappeared"
fi

# Verify data integrity
log ""
log "Step 3: Verifying data integrity..."
COUNT=$(curl -s "$HOST/query/$DB" -G \
    --data-urlencode "sql=SELECT COUNT(*) as cnt FROM $SERVICE.cpu WHERE date='$YESTERDAY'" | \
    grep -o '"cnt":[0-9]*' | grep -o '[0-9]*')

if [ -n "$COUNT" ] && [ "$COUNT" -gt 0 ]; then
    success "Data verified: $COUNT rows readable after compaction"
else
    error "Query failed or returned 0 rows"
fi

log ""
log "=========================================="
log "Test Complete"
log "=========================================="
log ""
log "Check service logs for messages like:"
log "  'Full compaction: N partitions compacted'"
log "  'date=$YESTERDAY: X → 1 file'"
log ""
warn "Original config will be restored on exit"
