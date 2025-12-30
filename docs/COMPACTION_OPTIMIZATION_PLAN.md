# Compaction Optimization Plan

**Document Version:** 1.0
**Date:** 2024-12-30
**Status:** Planning Phase
**Goal:** Reduce total file count by 80-90% through size and age-based compaction strategies

---

## Executive Summary

### Current Problem
- **File proliferation:** 10,000+ files across partitions (avg 12 files/partition)
- **Small files:** Many files < 50MB creating I/O overhead
- **LSM inefficiency:** Traditional leveled compaction leaves 13+ files/partition minimum
- **Query performance:** Must scan all files per partition, no file-level pruning

### Solution Overview
Implement **hybrid compaction strategy** combining:
1. **One-file policy** for old/stable partitions (>24h)
2. **Small-file consolidation** for recent partitions (6-24h)
3. **Traditional LSM** for active partitions (<6h)

### Expected Impact
- **File reduction:** 11,500 → 1,250 files (**89% reduction**)
- **Query performance:** Scan 1 file instead of 12 per old partition (**92% I/O reduction**)
- **Storage efficiency:** Eliminate small file overhead, better compression

---

## Table of Contents

1. [Current Architecture Analysis](#1-current-architecture-analysis)
2. [Identified Problems](#2-identified-problems)
3. [Optimization Plans](#3-optimization-plans)
   - [Plan A: Age-Based Consolidation](#plan-a-age-based-consolidation)
   - [Plan B: Small-File Consolidation](#plan-b-small-file-consolidation)
   - [Plan C: Hybrid Age + Size](#plan-c-hybrid-age--size)
   - [Plan D: Target-Based Consolidation](#plan-d-target-based-consolidation)
   - [Plan E: Aggressive Single-File Policy](#plan-e-aggressive-single-file-policy)
4. [Recommended Strategy](#4-recommended-strategy)
5. [Implementation Roadmap](#5-implementation-roadmap)
6. [Configuration Reference](#6-configuration-reference)
7. [Metrics & Monitoring](#7-metrics--monitoring)
8. [Risk Assessment](#8-risk-assessment)

---

## 1. Current Architecture Analysis

### LSM-Tree Compaction Strategy

**Current behavior (src/compactor.rs):**
```
Cache Flush (every 30s) → L0 files
    ↓
L0 files (≥5) → Compact → L1 files
    ↓
L1 files (≥4) → Compact → L2 files
    ↓
L2 files (≥4) → Compact → L3 files
    ↓
L3 files (≥4) → Compact → L4 files
```

### Configuration (config.toml)
```toml
[compaction]
min_files = 5              # L0 threshold
major_min_files = 4        # L1+ threshold
max_level = 4              # L0-L4 (5 levels)
target_file_size_mb = 500  # Target output size
interval_secs = 60         # Background interval
```

### File Structure
```
data/
└── {catalog}/
    └── {schema}/
        └── {table}/
            └── date=2024-12-25/
                ├── L0_1703512345678.parquet  (10 MB)
                ├── L0_1703512355789.parquet  (15 MB)
                ├── L0_1703512365890.parquet  (8 MB)
                ├── L1_1703512400123.parquet  (45 MB)
                ├── L1_1703512450456.parquet  (52 MB)
                ├── L2_1703512500789.parquet  (120 MB)
                ├── L2_1703512600234.parquet  (135 MB)
                └── L3_1703512700567.parquet  (400 MB)
```

**Total:** 8 files, 785 MB

---

## 2. Identified Problems

### Problem 1: File Count Buildup
**Root cause:** Minimum thresholds leave uncompacted files
- L0: Up to 4 files (below min_files=5)
- L1: Up to 3 files (below major_min_files=4)
- L2: Up to 3 files
- L3: Up to 3 files
- L4: Unlimited (no upper level)

**Worst case:** 4 + 3 + 3 + 3 + ∞ = **13+ files/partition**

### Problem 2: Single-Pass Compaction
**Issue:** Compaction doesn't iterate to completion (compactor.rs:404-446)

```rust
// Step 1: L0 → L1
if l0_files >= min_files { compact_l0_to_l1(); }

// Step 2: Refresh ONCE
let files = list_files();

// Step 3: Loop but only compact ONE level
for level in 1..=max_level {
    if level_files >= major_min_files { compact(); break; }
}
```

**Impact:** L0→L1→L2→L3 cascade takes 3-4 compaction cycles (3-4 minutes)

### Problem 3: Small File Fragmentation
**Issue:** Files grouped by size, small groups skipped (compactor.rs:645-647)

```rust
if group.len() < 2 && file_groups.len() > 1 {
    continue;  // Skip single-file groups!
}
```

**Example:** Files [100MB, 100MB, 600MB]
- Group 1: [100MB, 100MB] → compacted
- Group 2: [600MB] → **SKIPPED** (only 1 file)

### Problem 4: No Age Awareness
**Issue:** Treats active and old partitions identically

**Reality:**
- Partition `date=2024-12-25` (5 days old): **Stable, no new writes**
  - Current: 12 files remain
  - Optimal: **1 file**

- Partition `date=2024-12-30` (today): **Active, frequent writes**
  - Current: 6 files
  - Optimal: 6 files (OK for active data)

### Problem 5: No Small-File Detection
**Issue:** No logic to identify and merge files < threshold

**Example partition:**
```
L0: 5MB, 8MB, 12MB        (3 small files = 25 MB)
L1: 15MB, 20MB            (2 small files = 35 MB)
L2: 400MB                 (1 large file)
L3: 600MB                 (1 large file)
```

**Current:** 7 files, nothing happens (below thresholds)
**Optimal:** Merge 5 small files → **3 files total** (60MB, 400MB, 600MB)

---

## 3. Optimization Plans

### Plan A: Age-Based Consolidation ⭐

**Philosophy:** Old data is stable → merge everything into ONE file

#### Strategy
```
IF partition last modified > 24 hours ago:
    AND file count > 1:
    → Merge ALL files (all levels) into ONE file
```

#### Algorithm
```rust
fn compact_by_age(partition_path: &str, config: &CompactionConfig) -> Result<()> {
    let all_files = list_all_parquet_files(partition_path)?;

    // Get oldest file modification time
    let oldest_modified = all_files.iter()
        .map(|f| f.metadata().modified())
        .min()
        .ok_or("No files found")?;

    let partition_age = SystemTime::now()
        .duration_since(oldest_modified)?;

    let age_threshold = Duration::from_secs(
        config.stable_age_hours * 3600
    );

    // Partition is stable (old)
    if partition_age > age_threshold && all_files.len() > 1 {
        info!(
            "Partition stable ({}h old), merging {} files → 1 file",
            partition_age.as_secs() / 3600,
            all_files.len()
        );

        // Merge ALL files regardless of level
        let batches = read_all_files(&all_files)?;
        let output_file = format!(
            "{}/L_FINAL_{}.parquet",
            partition_path,
            chrono::Utc::now().timestamp_millis()
        );
        write_compacted_file(&output_file, batches)?;

        // Delete old files
        delete_files(&all_files)?;

        Ok(())
    } else {
        Ok(())  // Not ready for age-based compaction
    }
}
```

#### Configuration
```toml
[compaction.age_based]
enabled = true
stable_age_hours = 24          # Merge all if no writes for 24h
min_files_to_merge = 2         # Need at least 2 files
check_interval_secs = 3600     # Check every hour
```

#### Example
**Partition: `date=2024-12-25` (3 days old)**

**Before:**
```
L0_*.parquet: 10MB, 15MB, 8MB         (3 files)
L1_*.parquet: 45MB, 52MB              (2 files)
L2_*.parquet: 120MB, 135MB            (2 files)
L3_*.parquet: 400MB                   (1 file)
Total: 8 files, 785 MB
```

**After:**
```
L_FINAL_1703512800000.parquet         (1 file, 785 MB)
Total: 1 file ✅
```

#### Pros & Cons
✅ **Pros:**
- Massive file reduction for old partitions (87.5% in example)
- Simple logic: age > threshold → merge all
- Doesn't interfere with active writes
- Most partitions are old (90%+ of data)

❌ **Cons:**
- One-time I/O spike when merging
- Needs file modification time tracking
- May rewrite large amounts of data

#### Expected Impact
- **Scenario:** 1000 partitions, 900 are >24h old
- **Before:** 900 × 12 files = 10,800 files
- **After:** 900 × 1 file = **900 files** (92% reduction)

---

### Plan B: Small-File Consolidation

**Philosophy:** Small files are inefficient → merge them regardless of level

#### Strategy
```
Find all files < size_threshold (e.g., 50MB)
IF count >= min_small_files:
    → Merge all small files into ONE file
    → Keep large files untouched
```

#### Algorithm
```rust
fn compact_small_files(partition_path: &str, config: &CompactionConfig) -> Result<()> {
    let all_files = list_all_parquet_files(partition_path)?;

    let size_threshold = config.small_file_threshold_mb * 1024 * 1024;

    // Separate small and large files
    let (small_files, large_files): (Vec<_>, Vec<_>) = all_files
        .into_iter()
        .partition(|f| f.size < size_threshold);

    // Need multiple small files to merge
    if small_files.len() >= config.min_small_files_to_merge {
        let total_size: u64 = small_files.iter().map(|f| f.size).sum();

        info!(
            "Found {} small files (total {}MB), merging into one",
            small_files.len(),
            total_size / 1024 / 1024
        );

        // Merge all small files (cross-level merge!)
        let batches = read_all_files(&small_files)?;
        let output_file = format!(
            "{}/L_SMALL_MERGED_{}.parquet",
            partition_path,
            chrono::Utc::now().timestamp_millis()
        );
        write_compacted_file(&output_file, batches)?;

        // Delete old small files
        delete_files(&small_files)?;
    }

    Ok(())
}
```

#### Configuration
```toml
[compaction.small_file]
enabled = true
small_file_threshold_mb = 50      # Files < 50MB are "small"
min_small_files_to_merge = 3      # Need at least 3 small files
check_interval_secs = 1800        # Check every 30 min
```

#### Example
**Partition has:**
```
L0: 10MB, 15MB, 8MB               (3 small)
L1: 25MB, 30MB                    (2 small)
L2: 400MB                         (1 large)
L3: 600MB                         (1 large)
Total: 7 files
```

**Action:**
- Small files: 10+15+8+25+30 = 88MB (5 files)
- Merge 5 small → 1 file (88MB)
- Keep 2 large files untouched

**After:**
```
L_SMALL_MERGED_*.parquet: 88MB    (merged small)
L2: 400MB                         (kept)
L3: 600MB                         (kept)
Total: 3 files ✅ (57% reduction)
```

#### Pros & Cons
✅ **Pros:**
- Directly targets problem: too many small files
- Works on any partition (active or old)
- Doesn't touch well-compacted large files
- Can run frequently without heavy I/O

❌ **Cons:**
- Breaks LSM level invariants (cross-level merge)
- May need further compaction later
- Threshold tuning required

#### Expected Impact
- **Scenario:** 100 partitions with 5+ small files each
- **Before:** 100 × 8 files = 800 files
- **After:** 100 × 4 files = **400 files** (50% reduction)

---

### Plan C: Hybrid Age + Size ⭐⭐ (Recommended)

**Philosophy:** Different strategies for different partition states

#### Strategy (3-Tier)
```
IF partition age > 24h:
    → Strategy: ONE FILE (merge all)
ELSE IF partition age > 6h:
    → Strategy: MERGE SMALL FILES
ELSE:
    → Strategy: TRADITIONAL LSM
```

#### Decision Tree
```
                    Partition Age?
                         |
          ┌──────────────┼──────────────┐
          |              |              |
       > 24h          6-24h           < 6h
          |              |              |
    File count > 1?  Small files ≥ 3?  L0 ≥ 5?
          |              |              |
        YES            YES             YES
          |              |              |
    ✅ MERGE ALL    ✅ MERGE SMALL  ✅ LSM COMPACT
    → 1 file        → 1 merged     → L0→L1→L2...
```

#### Algorithm
```rust
fn smart_compaction(partition_path: &str, config: &CompactionConfig) -> Result<()> {
    let all_files = list_all_parquet_files(partition_path)?;
    let partition_age = get_partition_age(&all_files)?;

    // ========== PRIORITY 1: OLD PARTITION ==========
    if partition_age > Duration::from_secs(config.stable_age_hours * 3600) {
        if all_files.len() > 1 {
            info!(
                "OLD partition ({}h): merging ALL {} files → 1 file",
                partition_age.as_secs() / 3600,
                all_files.len()
            );
            merge_all_files_into_one(partition_path, &all_files)?;
        }
        return Ok(());
    }

    // ========== PRIORITY 2: RECENT PARTITION WITH SMALL FILES ==========
    if partition_age > Duration::from_secs(config.recent_age_hours * 3600) {
        let small_files: Vec<_> = all_files
            .iter()
            .filter(|f| f.size < config.small_file_threshold_mb * 1024 * 1024)
            .cloned()
            .collect();

        if small_files.len() >= config.min_small_files {
            info!(
                "RECENT partition ({}h) with {} small files: merging",
                partition_age.as_secs() / 3600,
                small_files.len()
            );
            merge_files_cross_level(partition_path, &small_files)?;
            return Ok(());
        }
    }

    // ========== PRIORITY 3: ACTIVE PARTITION ==========
    info!("ACTIVE partition ({}h): traditional LSM",
          partition_age.as_secs() / 3600);
    traditional_lsm_compaction(partition_path, config)?;

    Ok(())
}

fn get_partition_age(files: &[FileInfo]) -> Result<Duration> {
    let oldest = files.iter()
        .map(|f| f.modified_time)
        .min()
        .ok_or("No files")?;

    Ok(SystemTime::now().duration_since(oldest)?)
}
```

#### Configuration
```toml
[compaction]
# Traditional LSM for active partitions
min_files = 5
major_min_files = 4

# Age-based full consolidation
stable_age_hours = 24             # OLD: Merge all if >24h
min_files_for_age_merge = 2

# Size-based consolidation for recent partitions
recent_age_hours = 6              # RECENT: 6-24h range
small_file_threshold_mb = 50
min_small_files = 3

# Execution
interval_secs = 60
```

#### Example Scenarios

**Scenario 1: Old Partition (3 days old)**
```
Age: 72 hours (> 24h threshold)
Files: 12 files, 850MB total

Action: Merge ALL → 1 file
Result: 1 file (850MB) ✅
```

**Scenario 2: Recent Partition (12 hours old)**
```
Age: 12 hours (6h < age < 24h)
Files: L0: 10MB, 15MB, 8MB, 12MB, 20MB (5 small)
       L2: 400MB, 600MB (2 large)

Action: Merge small files → 1 file (65MB)
Result: 3 files (65MB, 400MB, 600MB) ✅
```

**Scenario 3: Active Partition (2 hours old)**
```
Age: 2 hours (< 6h threshold)
Files: 8 L0 files, 3 L1 files

Action: Traditional LSM (L0 → L1)
Result: 1 L0, 4 L1 files ✅
```

#### Pros & Cons
✅ **Pros:**
- **Maximum file reduction** (88%+ for typical workloads)
- Each partition gets optimal strategy
- Doesn't slow down active writes
- Handles both common cases: old stable + recent fragmented

❌ **Cons:**
- More complex logic (3 strategies)
- Needs file age tracking
- More configuration parameters

#### Expected Impact (1000 Partitions)

| Type | Count | Avg Files Before | Strategy | Avg Files After | Total Before | Total After |
|------|-------|------------------|----------|-----------------|--------------|-------------|
| Old (>24h) | 900 | 12 | Merge all → 1 | 1 | 10,800 | 900 |
| Recent (6-24h) | 50 | 8 | Merge small | 4 | 400 | 200 |
| Active (<6h) | 50 | 6 | LSM | 4 | 300 | 200 |
| **TOTAL** | **1000** | **11.5** | **Hybrid** | **1.3** | **11,500** | **1,300** |

**File Reduction: 89% ✅**

---

### Plan D: Target-Based Consolidation

**Philosophy:** Define desired end state (N files/partition), work backwards

#### Strategy
```
Calculate target: total_size / target_file_size
WHILE current_files > target:
    → Merge smallest files that fit in target_file_size
    → Repeat until target reached
```

#### Algorithm
```rust
fn consolidate_to_target(partition_path: &str, config: &CompactionConfig) -> Result<()> {
    let mut files = list_all_parquet_files(partition_path)?;

    let total_size: u64 = files.iter().map(|f| f.size).sum();
    let target_count = (total_size / (config.target_file_size_mb * 1024 * 1024))
        .max(1);

    info!(
        "Partition: {} files ({}MB), target: {} files",
        files.len(),
        total_size / 1024 / 1024,
        target_count
    );

    let mut iteration = 0;

    // Iterate until we reach target
    while files.len() > target_count && iteration < 10 {
        // Sort by size (smallest first)
        files.sort_by_key(|f| f.size);

        // Greedy: take smallest files that fit in target
        let mut merge_batch = vec![];
        let mut batch_size = 0u64;

        for file in &files {
            if batch_size + file.size <= config.target_file_size_mb * 1024 * 1024 {
                merge_batch.push(file.clone());
                batch_size += file.size;
            }

            // Need at least 2 files to merge
            if merge_batch.len() >= 2 {
                break;
            }
        }

        if merge_batch.len() >= 2 {
            // Merge these files
            let merged = merge_files(partition_path, &merge_batch)?;

            // Update file list
            files.retain(|f| !merge_batch.contains(f));
            files.push(merged);

            iteration += 1;
        } else {
            break;  // Can't merge anymore
        }
    }

    info!("Consolidation complete: {} → {} files",
          files.len(), target_count);
    Ok(())
}
```

#### Configuration
```toml
[compaction.target_based]
enabled = true
target_file_size_mb = 500        # Ideal file size
max_files_per_partition = 3      # Hard limit
consolidation_interval = 3600    # Every hour
max_iterations = 10              # Prevent infinite loops
```

#### Example
**Partition:** 8 files [5MB, 8MB, 10MB, 12MB, 15MB, 20MB, 400MB, 600MB]

**Total:** 1070MB → Target = ⌈1070/500⌉ = **3 files**

**Iteration 1:**
- Sort: [5, 8, 10, 12, 15, 20, 400, 600]
- Merge [5, 8, 10, 12, 15, 20] = 70MB (fits in 500MB)
- Files: [70MB, 400MB, 600MB]

**Result:** 3 files = target ✅

#### Pros & Cons
✅ **Pros:**
- Clear, predictable target
- Prioritizes smallest files (biggest waste)
- Iterative approach prevents oversized outputs
- Self-correcting (always moves toward target)

❌ **Cons:**
- May require multiple passes (longer lock time)
- Greedy algorithm not optimal
- Complex edge cases

---

### Plan E: Aggressive Single-File Policy ⭐⭐⭐

**Philosophy:** ONE file per partition. Period.

#### Strategy
```
FOR each partition:
    IF files > 1 AND total_size <= max_single_file_size:
        → Merge ALL files → 1 file
    ELSE IF total_size > max_single_file_size:
        → Split into N files of ~max_single_file_size each
```

#### Algorithm
```rust
fn one_file_per_partition(partition_path: &str, config: &CompactionConfig) -> Result<()> {
    let all_files = list_all_parquet_files(partition_path)?;

    if all_files.len() <= 1 {
        return Ok(());  // Already optimal
    }

    let total_size: u64 = all_files.iter().map(|f| f.size).sum();
    let max_size = config.max_single_file_size_mb * 1024 * 1024;

    if total_size > max_size {
        // Too large for single file
        let num_files = (total_size / max_size) + 1;

        info!(
            "Partition too large ({}MB), splitting into {} files",
            total_size / 1024 / 1024,
            num_files
        );

        // Read all data
        let all_batches = read_all_files(&all_files)?;

        // Split by row count
        let chunks = split_batches_evenly(all_batches, num_files as usize);

        // Write output files
        for (i, chunk) in chunks.iter().enumerate() {
            let output = format!(
                "{}/L0_PART{}_{}.parquet",
                partition_path,
                i,
                chrono::Utc::now().timestamp_millis()
            );
            write_compacted_file(&output, chunk.clone())?;
        }

        // Delete old files
        delete_files(&all_files)?;
    } else {
        // Merge all into one
        info!(
            "Merging {} files ({}MB) → 1 file",
            all_files.len(),
            total_size / 1024 / 1024
        );

        let batches = read_all_files(&all_files)?;
        let output = format!(
            "{}/L0_{}.parquet",
            partition_path,
            chrono::Utc::now().timestamp_millis()
        );
        write_compacted_file(&output, batches)?;

        delete_files(&all_files)?;
    }

    Ok(())
}
```

#### Configuration
```toml
[compaction.one_file_policy]
enabled = true
max_single_file_size_mb = 2048      # Don't create files > 2GB
min_age_for_merge_hours = 1         # Wait 1h before merging
check_interval_secs = 600           # Every 10 min
exclude_active_partitions = true    # Don't merge today's partition
```

#### Examples

**Example 1: Small partition**
```
Files: 8 files, 250MB total
Action: Merge all → 1 file (250MB)
Result: 1 file ✅
```

**Example 2: Large partition**
```
Files: 50 files, 3GB total
Action: Merge all → split into 2 files
Result: 2 files (1.5GB each) ✅
```

**Example 3: Already optimal**
```
Files: 1 file, 500MB
Action: None (skip)
Result: 1 file ✅
```

#### Pros & Cons
✅ **Pros:**
- **Simplest logic**: always 1 file (or minimal)
- **Maximum file reduction**: guaranteed 1 file/partition
- **Best query performance**: scan only 1 file
- **Predictable**: no complex thresholds

❌ **Cons:**
- May rewrite large amounts of data
- Not suitable for actively-written partitions
- Large I/O cost for big partitions

#### Mitigation Strategies
1. **Age gate:** Only merge partitions >1h old
2. **Active partition exclusion:** Skip today's partition
3. **Size limit:** Split if total > 2GB
4. **Cache coordination:** Don't merge partitions currently being flushed

#### Expected Impact
- **Scenario:** 1000 partitions
- **Before:** 12 files/partition = 12,000 files
- **After:** 1 file/partition = **1,000 files** (92% reduction)

---

## 4. Recommended Strategy

### Hybrid Approach: Plan C + Plan E

**Combine the best of both worlds:**
- **Plan C:** Age-tiered strategy (different tactics for different ages)
- **Plan E:** Aggressive one-file policy for old data

### Implementation

```rust
pub fn intelligent_compaction(
    &self,
    partition_path: &str,
    config: &CompactionConfig,
) -> Result<Option<CompactionResult>> {

    let all_files = self.list_parquet_file_infos(partition_path)?;

    if all_files.is_empty() {
        return Ok(None);
    }

    let partition_age = self.get_partition_age(&all_files)?;
    let file_count = all_files.len();

    // ========================================
    // TIER 1: OLD PARTITION (>24h)
    // Strategy: ONE FILE POLICY
    // ========================================
    if partition_age > Duration::from_secs(config.stable_age_hours * 3600) {
        if file_count > 1 {
            info!(
                "Partition {}h old: ONE-FILE policy ({} files → 1 file)",
                partition_age.as_secs() / 3600,
                file_count
            );
            return self.merge_all_to_one_file(partition_path, &all_files, config);
        } else {
            debug!("Partition already optimal (1 file)");
            return Ok(None);
        }
    }

    // ========================================
    // TIER 2: RECENT PARTITION (6-24h)
    // Strategy: MERGE SMALL FILES
    // ========================================
    if partition_age > Duration::from_secs(config.recent_age_hours * 3600) {
        let small_files: Vec<_> = all_files
            .iter()
            .filter(|f| f.size < config.small_file_threshold_mb * 1024 * 1024)
            .cloned()
            .collect();

        if small_files.len() >= config.min_small_files {
            info!(
                "Partition {}h old: SMALL-FILE consolidation ({} small files)",
                partition_age.as_secs() / 3600,
                small_files.len()
            );
            return self.merge_small_files(partition_path, &small_files, config);
        }
    }

    // ========================================
    // TIER 3: ACTIVE PARTITION (<6h)
    // Strategy: TRADITIONAL LSM
    // ========================================
    debug!(
        "Partition {}h old: TRADITIONAL LSM compaction",
        partition_age.as_secs() / 3600
    );
    self.traditional_lsm_compaction(partition_path, &all_files, config)
}
```

### Configuration (Recommended)

```toml
[compaction]
# ========== TIER 1: OLD PARTITIONS ==========
stable_age_hours = 24                   # Partitions >24h old
one_file_policy = true                  # Merge all → 1 file
max_single_file_size_mb = 2048          # Split if > 2GB

# ========== TIER 2: RECENT PARTITIONS ==========
recent_age_hours = 6                    # Partitions 6-24h old
small_file_threshold_mb = 50            # Files < 50MB are "small"
min_small_files = 3                     # Merge if ≥3 small files

# ========== TIER 3: ACTIVE PARTITIONS ==========
# Traditional LSM for partitions <6h old
min_files = 5                           # L0 threshold
major_min_files = 4                     # L1+ threshold
max_level = 4                           # L0-L4
target_file_size_mb = 500               # Output size

# ========== EXECUTION ==========
interval_secs = 60                      # Check every minute
max_compaction_time_secs = 300          # Timeout per partition
concurrent_partitions = 4               # Parallel compaction
```

### Decision Matrix

| Partition Age | File Count | Total Size | Strategy | Expected Outcome |
|---------------|------------|------------|----------|------------------|
| >24h | 12 | 800MB | **Merge all → 1 file** | 1 file (800MB) |
| >24h | 1 | 500MB | Skip | 1 file (no change) |
| 6-24h | 8 (5 small + 3 large) | 1.2GB | **Merge 5 small → 1** | 4 files |
| 6-24h | 3 (all large) | 1.5GB | Skip | 3 files (no change) |
| <6h | 8 L0 files | 120MB | **LSM: L0 → L1** | 2 L0 + 1 L1 |
| <6h | 2 L0 files | 30MB | Skip (below threshold) | 2 files (no change) |

### Expected Results (1000 Partitions)

#### Breakdown by Age

| Partition Type | Count | Avg Files Before | Strategy | Avg Files After | Reduction |
|----------------|-------|------------------|----------|-----------------|-----------|
| **Old (>24h)** | 900 | 12 | One-file policy | **1** | **92%** |
| **Recent (6-24h)** | 50 | 8 | Small-file merge | **4** | **50%** |
| **Active (<6h)** | 50 | 6 | Traditional LSM | **4** | **33%** |

#### Total Impact

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Total files** | 11,500 | 1,250 | **89% reduction** |
| **Avg files/partition** | 11.5 | 1.25 | **89% reduction** |
| **Files to scan (old partition query)** | 12 | 1 | **92% I/O reduction** |
| **Storage efficiency** | Low (many small files) | High (consolidated) | Better compression |

---

## 5. Implementation Roadmap

### Phase 1: Foundation (Week 1)

**Goals:**
- Add file age tracking
- Implement age detection logic
- Add configuration parameters

**Tasks:**
1. **Add file metadata tracking** (src/compactor.rs)
   ```rust
   struct ParquetFileInfo {
       path: PathBuf,
       size: u64,
       level: u32,
       timestamp: i64,
       modified_time: SystemTime,  // ← NEW
   }
   ```

2. **Add partition age calculation**
   ```rust
   fn get_partition_age(&self, files: &[ParquetFileInfo]) -> Result<Duration>
   ```

3. **Update configuration** (src/config.rs)
   ```rust
   pub struct CompactionConfig {
       // Existing fields...

       // NEW: Age-based compaction
       pub stable_age_hours: u64,
       pub recent_age_hours: u64,
       pub small_file_threshold_mb: u64,
       pub min_small_files: usize,
   }
   ```

4. **Add unit tests**
   - Test age calculation
   - Test file categorization (old/recent/active)

**Deliverables:**
- ✅ File age tracking working
- ✅ Configuration updated
- ✅ Tests passing

---

### Phase 2: One-File Policy (Week 2)

**Goals:**
- Implement aggressive consolidation for old partitions
- Test with real data

**Tasks:**
1. **Implement `merge_all_to_one_file()`** (src/compactor.rs)
   ```rust
   async fn merge_all_to_one_file(
       &self,
       partition_path: &str,
       files: &[ParquetFileInfo],
       config: &CompactionConfig,
   ) -> Result<CompactionResult>
   ```

2. **Add file size checking**
   - If total > max_single_file_size → split into multiple
   - Implement `split_batches_evenly()` helper

3. **Add safety guards**
   - Skip if partition is being written (check cache)
   - Add timeout (max 5 minutes per partition)
   - Add file count sanity check (max 100 files to merge)

4. **Integration testing**
   - Test with partition containing 20+ small files
   - Test with partition > 2GB (split scenario)
   - Test concurrent reads during compaction

**Deliverables:**
- ✅ One-file policy working for >24h partitions
- ✅ Large partition splitting working
- ✅ No data loss or corruption

---

### Phase 3: Small-File Consolidation (Week 3)

**Goals:**
- Implement small-file merging for recent partitions
- Optimize for 6-24h age range

**Tasks:**
1. **Implement `merge_small_files()`** (src/compactor.rs)
   ```rust
   async fn merge_small_files(
       &self,
       partition_path: &str,
       small_files: &[ParquetFileInfo],
       config: &CompactionConfig,
   ) -> Result<CompactionResult>
   ```

2. **Add small-file detection**
   - Filter files by size threshold
   - Group by partition
   - Track merge statistics

3. **Cross-level merge logic**
   - Handle schema conflicts (already exists)
   - Preserve data order (sort by timestamp)
   - Update level naming (use L_SMALL_MERGED prefix)

4. **Testing**
   - Test with mixed small/large files
   - Verify large files untouched
   - Check merge output quality

**Deliverables:**
- ✅ Small-file consolidation working
- ✅ Large files preserved
- ✅ Tests passing

---

### Phase 4: Integration & Optimization (Week 4)

**Goals:**
- Integrate all strategies into unified compaction logic
- Add monitoring and metrics
- Performance testing

**Tasks:**
1. **Implement `intelligent_compaction()`**
   - Decision tree logic (age-based routing)
   - Fallback to traditional LSM
   - Error handling and retry logic

2. **Update background compaction**
   ```rust
   pub fn start_background_compaction(
       self: Arc<Self>,
       interval_secs: u64,
       config: CompactionConfig,
   ) {
       tokio::spawn(async move {
           loop {
               // Use intelligent_compaction instead of traditional
               self.intelligent_compaction_all_catalogs(&config).await;
               sleep(interval).await;
           }
       });
   }
   ```

3. **Add metrics tracking**
   - Compactions by type (old/recent/active)
   - Files before/after per type
   - I/O bytes, duration, errors
   - Expose via `/stats` endpoint

4. **Performance testing**
   - Load test with 1000 partitions
   - Measure compaction throughput
   - Verify query performance improvement
   - Check resource usage (CPU, I/O, memory)

**Deliverables:**
- ✅ Full hybrid compaction working
- ✅ Metrics dashboard
- ✅ Performance benchmarks

---

### Phase 5: Production Rollout (Week 5)

**Goals:**
- Deploy to production
- Monitor and tune
- Document operational procedures

**Tasks:**
1. **Configuration tuning**
   - Adjust thresholds based on workload
   - Set appropriate intervals
   - Configure exclusions if needed

2. **Gradual rollout**
   - Week 5.1: Enable for test database
   - Week 5.2: Enable for 10% of databases
   - Week 5.3: Enable for 50% of databases
   - Week 5.4: Enable for 100% of databases

3. **Monitoring**
   - Watch file count trends
   - Monitor compaction duration
   - Check for errors/warnings
   - Validate query performance

4. **Documentation**
   - Update CLAUDE.md
   - Write operations guide
   - Document troubleshooting steps

**Deliverables:**
- ✅ Production deployment complete
- ✅ File count reduced by 80%+
- ✅ No production issues
- ✅ Documentation updated

---

## 6. Configuration Reference

### Complete config.toml

```toml
[compaction]
# ==========================================
# TIER 1: OLD PARTITION COMPACTION (>24h)
# ==========================================
# Strategy: Merge ALL files into ONE file per partition
stable_age_hours = 24                   # Partitions older than 24h → one-file policy
one_file_policy = true                  # Enable aggressive consolidation
max_single_file_size_mb = 2048          # Split into multiple if total > 2GB
min_files_for_age_merge = 2             # Need at least 2 files to merge

# ==========================================
# TIER 2: RECENT PARTITION COMPACTION (6-24h)
# ==========================================
# Strategy: Merge small files, keep large files
recent_age_hours = 6                    # Partitions 6-24h old → small-file policy
small_file_threshold_mb = 50            # Files smaller than 50MB are "small"
min_small_files = 3                     # Merge if ≥3 small files exist
cross_level_merge = true                # Allow merging across levels

# ==========================================
# TIER 3: ACTIVE PARTITION COMPACTION (<6h)
# ==========================================
# Strategy: Traditional LSM-tree compaction
min_files = 5                           # L0 → L1 when ≥5 L0 files
major_min_files = 4                     # L1+ → L2+ when ≥4 files
max_level = 4                           # Max level: L4 (L0, L1, L2, L3, L4)
target_file_size_mb = 500               # Target output file size
max_file_size_mb = 100                  # (Unused, for future)

# ==========================================
# EXECUTION SETTINGS
# ==========================================
interval_secs = 60                      # Background check interval
max_compaction_time_secs = 300          # Timeout per partition (5 min)
concurrent_partitions = 4               # Parallel compaction workers
retry_on_error = true                   # Retry failed compactions
max_retries = 3                         # Max retry attempts

# ==========================================
# EXCLUSIONS (Optional)
# ==========================================
exclude_databases = ["_system"]         # Never compact these databases
exclude_tables = []                     # Never compact these tables
exclude_today = true                    # Never compact today's partition
```

### Tuning Guidelines

#### High-Frequency Writes (e.g., metrics, logs)
```toml
stable_age_hours = 12              # Consolidate faster (12h instead of 24h)
recent_age_hours = 2               # Small-file merge sooner (2h instead of 6h)
min_files = 3                      # Lower threshold for active (3 instead of 5)
interval_secs = 30                 # Check more frequently
```

#### Low-Frequency Writes (e.g., daily reports)
```toml
stable_age_hours = 48              # Wait longer before consolidating
small_file_threshold_mb = 100      # Larger threshold for "small" files
min_small_files = 5                # Need more files before merging
interval_secs = 300                # Check less frequently (5 min)
```

#### High Cardinality (many partitions)
```toml
concurrent_partitions = 8          # More parallel workers
max_compaction_time_secs = 120     # Shorter timeout to prevent blocking
```

#### Large Files (GB-scale partitions)
```toml
max_single_file_size_mb = 4096     # Allow larger consolidated files (4GB)
target_file_size_mb = 1024         # Larger LSM output files (1GB)
```

---

## 7. Metrics & Monitoring

### Compaction Metrics

**Add to `/stats` endpoint:**

```json
{
  "compaction": {
    "total_compactions": 1234,
    "by_strategy": {
      "one_file_policy": 900,
      "small_file_merge": 250,
      "traditional_lsm": 84
    },
    "files_reduced": {
      "before_total": 11500,
      "after_total": 1250,
      "reduction_percent": 89.1
    },
    "bytes_processed": {
      "total_mb": 125000,
      "avg_per_compaction_mb": 101
    },
    "duration": {
      "total_seconds": 3600,
      "avg_per_compaction_seconds": 2.9,
      "max_seconds": 45
    },
    "errors": {
      "total": 5,
      "by_type": {
        "timeout": 2,
        "io_error": 3
      }
    },
    "last_run": "2024-12-30T10:15:00Z"
  }
}
```

### Logging

**Key log entries:**

```
INFO  Partition date=2024-12-27 is 72h old: ONE-FILE policy (12 files → 1 file)
INFO  Merged 12 files (785MB) → L_FINAL_1703512800000.parquet (785MB) in 3.2s
INFO  Compaction complete: 12 → 1 files (92% reduction)

INFO  Partition date=2024-12-29 is 18h old: SMALL-FILE consolidation (5 small files)
INFO  Merged 5 small files (65MB) → L_SMALL_MERGED_1703598400000.parquet
INFO  Compaction complete: 7 → 3 files (57% reduction)

DEBUG Partition date=2024-12-30 is 3h old: TRADITIONAL LSM compaction
INFO  Minor compaction: 6 L0 files → L1
INFO  Compaction complete: 6 → 2 files (67% reduction)
```

### Alerts

**Set up alerts for:**

1. **High file count** (> 20 files per partition)
   - Indicates compaction not keeping up
   - Action: Increase `concurrent_partitions` or decrease thresholds

2. **Compaction timeout** (> 10 per hour)
   - Indicates partitions too large or system overloaded
   - Action: Increase `max_compaction_time_secs` or investigate slow I/O

3. **Compaction errors** (> 5% error rate)
   - Indicates systematic issue
   - Action: Check logs, verify disk space, check file permissions

4. **File count not decreasing** (week-over-week)
   - Indicates strategy not working
   - Action: Review configuration, check partition ages

### Dashboards

**Grafana panels:**

1. **File count over time** (line chart)
   - Total files across all partitions
   - Target: downward trend after deployment

2. **Compactions by strategy** (stacked area)
   - One-file policy (blue)
   - Small-file merge (green)
   - Traditional LSM (yellow)

3. **File reduction rate** (gauge)
   - Current: X files
   - Target: Y files
   - Reduction: Z%

4. **Compaction duration** (histogram)
   - P50, P95, P99 latencies
   - Alert if P95 > 30s

---

## 8. Risk Assessment

### Risk Matrix

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Data loss during compaction** | Low | Critical | • Atomic writes (write to temp, then rename)<br>• Keep old files until new file verified<br>• Add checksums |
| **Query errors during compaction** | Medium | High | • Use locks to prevent concurrent read/write<br>• Readers retry on file-not-found |
| **Compaction can't keep up with writes** | Medium | Medium | • Monitor file count trends<br>• Add backpressure (reject writes if >1000 files)<br>• Increase `concurrent_partitions` |
| **Large I/O spikes** | High | Low | • Rate limit compactions<br>• Spread out over time<br>• Schedule during off-peak hours |
| **Configuration errors** | Medium | Medium | • Validate config on startup<br>• Add config tests<br>• Document recommended values |
| **Regression in query performance** | Low | High | • Benchmark before/after<br>• Monitor P95 query latency<br>• Add rollback plan |

### Rollback Plan

**If issues arise in production:**

1. **Immediate rollback (< 5 minutes)**
   ```toml
   [compaction]
   one_file_policy = false      # Disable new strategies
   small_file_merge = false
   # Revert to traditional LSM only
   ```

2. **Stop background compaction**
   ```bash
   # Modify config and restart
   interval_secs = 0  # Disable background compaction
   ```

3. **Manual compaction only**
   ```bash
   # Admin triggers compaction manually per database
   curl -X POST http://localhost:7000/compact/mydb
   ```

4. **Restore from backup** (worst case)
   - Keep hourly snapshots of data directory
   - Restore specific partitions if corrupted

### Testing Strategy

**Before production:**

1. **Unit tests** (compactor.rs)
   - Test each strategy independently
   - Test edge cases (0 files, 1 file, 1000 files)
   - Test schema conflicts, type mismatches

2. **Integration tests**
   - Simulate 1000 partitions with varying ages
   - Run full compaction cycle
   - Verify file counts, query results

3. **Load tests**
   - Write 10K points/sec for 1 hour
   - Verify compaction keeps up
   - Monitor resource usage

4. **Chaos tests**
   - Kill compaction mid-flight
   - Verify no data loss
   - Check recovery behavior

5. **Performance benchmarks**
   - Query latency: before vs after
   - Write throughput: before vs after
   - Compaction throughput: files/sec

---

## Appendix A: Code Locations

### Files to Modify

| File | Changes |
|------|---------|
| `src/compactor.rs` | • Add `get_partition_age()`<br>• Add `merge_all_to_one_file()`<br>• Add `merge_small_files()`<br>• Add `intelligent_compaction()`<br>• Update `ParquetFileInfo` struct |
| `src/config.rs` | • Add `stable_age_hours`<br>• Add `recent_age_hours`<br>• Add `small_file_threshold_mb`<br>• Add `min_small_files`<br>• Add `concurrent_partitions` |
| `src/main.rs` | • Update compaction initialization<br>• Pass new config params |
| `config.toml` | • Add new configuration sections |

### New Files to Create

| File | Purpose |
|------|---------|
| `src/compaction_strategy.rs` | • Enum for compaction strategies<br>• Strategy selection logic |
| `tests/compaction_integration.rs` | • End-to-end compaction tests |
| `scripts/test_compaction_hybrid.sh` | • Test script for hybrid strategy |

---

## Appendix B: Migration Path

### From Current to Hybrid

**Safe migration steps:**

1. **Week 1: Enable monitoring**
   - Add metrics collection
   - Establish baseline (current file counts)

2. **Week 2: Enable one-file policy (read-only mode)**
   - Detect old partitions
   - Log what would be merged
   - Don't actually merge yet

3. **Week 3: Enable for 1 test database**
   - Set `exclude_databases` for all others
   - Monitor closely

4. **Week 4: Gradual rollout**
   - Remove databases from `exclude_databases` one by one
   - Monitor file counts

5. **Week 5: Full deployment**
   - Enable for all databases
   - Monitor and tune

### Backwards Compatibility

**Old files still work:**
- Existing L0/L1/L2/L3/L4 files: still readable
- Query engine doesn't care about file naming
- Can mix old and new compaction outputs

**Gradual transition:**
- Old partitions get re-compacted with new strategy
- New partitions use new strategy from day 1
- No need to rewrite all existing files

---

## Appendix C: Performance Estimates

### I/O Cost Analysis

**Assumptions:**
- 1000 partitions total
- Average partition: 12 files, 800MB
- Disk I/O: 200 MB/s read, 150 MB/s write

**One-file policy (900 old partitions):**
```
Read:  900 partitions × 800MB = 720 GB
Write: 900 partitions × 800MB = 720 GB
Time:  720GB / 200 MB/s (read) + 720GB / 150 MB/s (write)
     = 3,600s + 4,800s = 8,400s = 2.3 hours (one-time)
```

**Daily ongoing (50 partitions age out per day):**
```
Read:  50 × 800MB = 40 GB
Write: 50 × 800MB = 40 GB
Time:  40GB / 200 + 40GB / 150 = 200s + 267s = 467s = 7.8 min/day
```

### Query Performance Improvement

**Current (12 files per partition):**
```
File open overhead: 12 × 5ms = 60ms
Metadata read: 12 × 10ms = 120ms
Data scan: 12 × 50ms = 600ms (with filtering)
Total: ~780ms per partition query
```

**After (1 file per partition):**
```
File open overhead: 1 × 5ms = 5ms
Metadata read: 1 × 10ms = 10ms
Data scan: 1 × 50ms = 50ms
Total: ~65ms per partition query
```

**Speedup: 12× faster queries** 🚀

---

## Appendix D: Alternatives Considered

### Alternative 1: File Metadata Index

**Idea:** Instead of reducing files, add min/max timestamp index to skip files

**Pros:**
- No I/O cost for compaction
- Can prune files during query planning

**Cons:**
- Doesn't solve small-file problem
- Still scanning metadata of 12 files
- Requires schema changes

**Decision:** Implement AFTER file reduction (complementary, not alternative)

### Alternative 2: Time-Partitioned Tables (Hive-style)

**Idea:** Use nested partitions: `date=2024-12-30/hour=14/`

**Pros:**
- Finer-grained retention (delete by hour)
- Better query pruning

**Cons:**
- Many more partitions (24× increase)
- Doesn't solve compaction problem
- Breaking change

**Decision:** Rejected (increases file count)

### Alternative 3: Separate Cold/Hot Storage

**Idea:** Move old partitions to separate "cold" storage tier

**Pros:**
- Can use different compaction strategies per tier
- Cost optimization

**Cons:**
- Complex migration logic
- Requires storage tier architecture
- Out of scope

**Decision:** Future work (Phase 2, after file reduction)

---

## Sign-off

**Prepared by:** Claude Code Analysis
**Review required from:**
- [ ] Backend Team Lead
- [ ] DevOps Team
- [ ] DBA Team

**Approval required from:**
- [ ] Engineering Director

**Target start date:** TBD
**Target completion date:** TBD +5 weeks

---

**Document History:**

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2024-12-30 | Initial draft | Claude Code |
