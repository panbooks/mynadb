# Compaction Testing Scripts

This directory contains test scripts for validating the dual compaction strategy in stats-service.

## Overview

Stats-service uses two independent compaction strategies:

1. **LSM Compaction** - Runs every 60 seconds, compacts active partitions through levels (L0 → L1 → L2 → L3 → L4)
2. **Full Compaction** - Runs hourly, merges yesterday's partitions into single files at configured hour (default: 01:00)

## Test Scripts

### 1. `test_compaction_dual_strategy.sh` - Comprehensive Test

**Purpose:** Full test suite covering both LSM and full compaction strategies

**What it tests:**
- L0 → L1 compaction (minor compaction)
- L1 → L2 compaction (major compaction)
- Multi-level compaction progression
- Date-based full compaction for yesterday
- Hour-based scheduling (full compaction only at configured hour)
- Size-based skip logic (partitions > 200MB)
- Data integrity after compaction

**Usage:**
```bash
# Start service first
cargo run --release

# In another terminal, run test
./scripts/test_compaction_dual_strategy.sh
```

**Duration:** ~5-10 minutes (includes waiting for compaction cycles)

**Expected Output:**
```
[INFO] ===== TEST 1: LSM Compaction Strategy =====
[INFO] Writing data to create L0 files...
[INFO] Waiting for L0 → L1 compaction...
[SUCCESS] L0 → L1 compaction successful! L1 files: 2

[INFO] ===== TEST 2: Date-Based Full Compaction =====
[INFO] Current hour is 14 (full compaction configured for hour 1)
[SUCCESS] Full compaction correctly DID NOT run at hour 14

[SUCCESS] Test completed!
```

### 2. `test_compaction_quick.sh` - Quick Validation

**Purpose:** Fast sanity check for compaction mechanics

**What it tests:**
- Basic LSM compaction (L0 → L1)
- Yesterday's partition existence
- Manual compaction trigger
- Data integrity

**Usage:**
```bash
./scripts/test_compaction_quick.sh
```

**Duration:** ~2-3 minutes

**When to use:** Quick smoke test after code changes

### 3. `test_full_compaction_now.sh` - Force Full Compaction

**Purpose:** Test full compaction immediately without waiting for 01:00

**What it does:**
- Temporarily modifies `config.toml` to set `full_compaction_hour` to current hour
- Sets `full_compaction_interval_secs` to 60 for faster testing
- Writes data for yesterday
- Waits for full compaction to trigger
- Restores original config on exit

**Usage:**
```bash
# This script will modify config and requires service restart
./scripts/test_full_compaction_now.sh

# Follow prompts to restart service when asked
```

**Duration:** ~2 minutes

**⚠️ Important:** This script modifies `config.toml` temporarily. Original config is restored automatically.

## Test Scenarios

### Scenario 1: Verify LSM Compaction

Tests that L0 files accumulate and compact to L1 when threshold (4 files) is reached.

```bash
./scripts/test_compaction_quick.sh
```

Look for:
- L0 files created after writes
- L1 files appear after 60s
- File count reduction

### Scenario 2: Verify Full Compaction Scheduling

Tests that full compaction only runs at configured hour.

```bash
# If current hour is NOT 1:
./scripts/test_compaction_dual_strategy.sh

# Verify full compaction does NOT run
# Check logs: should see LSM compaction but NOT full compaction
```

### Scenario 3: Force Full Compaction Test

Test full compaction immediately by setting hour to current.

```bash
./scripts/test_full_compaction_now.sh
```

Look for:
- Multiple files for yesterday created
- After 70s, files merge to single FULL_*.parquet file
- Data still queryable

### Scenario 4: Size Skip Logic

Test that partitions > 200MB are skipped by full compaction.

Write large dataset:
```bash
# Manually write a lot of data for yesterday
for i in {1..100}; do
  curl -X POST http://localhost:7000/write/test \
    -d "cpu,host=server$i value=1 $(date -d yesterday +%s)000000000"
done

# Trigger compaction
curl -X POST http://localhost:7000/compact/test
```

Check partition size:
```bash
du -sh ./data/test/*/cpu/date=$(date -d yesterday +%Y-%m-%d)
```

If > 200MB, full compaction should skip it (check logs).

## Monitoring Compaction

### Service Logs

Watch for these log messages:

**LSM Compaction:**
```
[INFO] Background LSM compaction started: every 60 seconds
[INFO] LSM compaction [mydb]: 3 partition(s)
[INFO] Minor compaction: 5 L0 files → L1
[INFO] Major compaction: 4 L1 files → L2
```

**Full Compaction:**
```
[INFO] Background full compaction started: every 3600 seconds (acts at hour 1)
[INFO] Starting full compaction for yesterday: date=2025-12-29 (hour 1)
[INFO] Full compaction: 5 partitions compacted
[INFO] mydb.service.cpu/date=2025-12-29: 12 → 1 files, 45.32 MB
```

### File Structure

Check compaction by examining file levels:

```bash
# List files in a partition
ls -lh ./data/mydb/service/cpu/date=2025-12-29/

# Count by level
find ./data/mydb/service/cpu/date=2025-12-29/ -name "L0_*.parquet" | wc -l
find ./data/mydb/service/cpu/date=2025-12-29/ -name "L1_*.parquet" | wc -l
find ./data/mydb/service/cpu/date=2025-12-29/ -name "FULL_*.parquet" | wc -l
```

**Before LSM compaction:**
```
L0_1735478123456.parquet
L0_1735478234567.parquet
L0_1735478345678.parquet
L0_1735478456789.parquet
L0_1735478567890.parquet
```

**After LSM compaction:**
```
L0_1735478567890.parquet  (newest, not yet compacted)
L1_1735478999999.parquet  (merged from older L0 files)
```

**After full compaction (yesterday only):**
```
FULL_1735478999999.parquet  (single consolidated file)
```

## Configuration

Compaction is controlled by `config.toml`:

```toml
[compaction]
# LSM compaction
interval_secs = 60                          # Check every 60s
min_files = 4                               # L0 → L1 when ≥4 L0 files
major_min_files = 4                         # L1 → L2 when ≥4 L1 files
target_file_size_mb = 200                   # Target size for LSM output
max_level = 4                               # Max level (L4)

# Full compaction
full_compaction_enabled = true              # Enable date-based compaction
full_compaction_hour = 1                    # Run at 01:00
full_compaction_interval_secs = 3600       # Check every hour
full_compaction_target_size_mb = 200       # Skip if partition > 200MB
```

## Troubleshooting

### LSM Compaction Not Running

**Symptom:** L0 files accumulate, no L1 files appear

**Checks:**
1. Verify interval: `interval_secs > 0` in config
2. Check file count: Need at least `min_files` (default 4) L0 files
3. Check logs for errors
4. Verify service is running: `curl http://localhost:7000/health`

### Full Compaction Not Running

**Symptom:** Yesterday's partition has many files, not merged to single file

**Checks:**
1. Verify enabled: `full_compaction_enabled = true`
2. Check current hour: Full compaction only runs at `full_compaction_hour`
3. Check partition size: Partitions > 200MB are skipped
4. Check file count: Need at least 2 files to compact
5. Verify interval: `full_compaction_interval_secs > 0`

### Files Not Reducing

**Symptom:** Compaction runs but file count doesn't decrease

**Reasons:**
1. **Active partition:** Today's partition constantly receives new L0 files
2. **Threshold not met:** Need min_files for compaction to trigger
3. **Already optimal:** Single file or few large files don't compact further
4. **Size limit:** Full compaction skips if total > 200MB

### Data Loss After Compaction

**Symptom:** Query returns fewer rows after compaction

**This should never happen!** Report immediately.

**Debug:**
```bash
# Count rows before compaction
curl -s 'http://localhost:7000/query/mydb' -G \
  --data-urlencode 'sql=SELECT COUNT(*) FROM service.cpu'

# After compaction, verify same count
```

## Performance Expectations

### LSM Compaction (60s interval)

- **L0 → L1:** ~1-2 seconds for 4-5 files (~100MB total)
- **L1 → L2:** ~2-5 seconds for 4 files (~400MB total)
- **Overhead:** Minimal, runs in background

### Full Compaction (hourly, acts at 01:00)

- **Small partition (<50MB):** ~1-3 seconds
- **Medium partition (50-200MB):** ~5-15 seconds
- **Large partition (>200MB):** Skipped (no overhead)

### Disk I/O

- **Read:** Parquet files read sequentially
- **Write:** New file written, old files deleted atomically
- **Temp space:** ~2x partition size during compaction (reads + writes)

## Integration with Other Tests

These compaction tests complement existing test scripts:

- `test_write.sh` - Creates data that will be compacted
- `test_compaction.sh` - Original single-strategy test (may be outdated)
- `test_partition_query.sh` - Verifies queries work after compaction

## References

- **Documentation:** `docs/DATE_BASED_FULL_COMPACTION.md`
- **Config:** `src/config.rs` - CompactionConfig struct
- **Implementation:** `src/compactor.rs` - Compaction logic
- **Main:** `src/main.rs` - Background task initialization
