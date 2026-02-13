# Compaction Manifest: Level-Aware File Deduplication

## Overview

The Compaction Manifest solves the problem of duplicate NULL rows appearing in query results when compacted (L1+) and source (L0) parquet files coexist in the same partition. This happens when source file deletion fails after compaction writes the merged output.

The manifest is a lightweight per-partition JSON file (`MANIFEST.json`) that tracks which parquet files have been superseded by compaction. The query path reads it to skip stale files; the compactor reads it to avoid re-compacting already-merged data.

## Problem Statement

After compaction merges L0 files into an L1 file, the L0 sources should be deleted. But deletion can fail (network timeout, permissions, race condition). When both old L0 and new L1 files exist:

1. **Query discovery** lists ALL `.parquet` files in the partition
2. L0 files with narrower schemas produce NULL rows when aligned to the merged schema
3. Every data row is paired with a NULL-field duplicate at the same timestamp

### The Problem Spans All Levels

This is not limited to L0 → L1. The same issue occurs at every level transition:

```
L0 → L1    (minor compaction)
L1 → L2    (major compaction)
L2 → L3    (major compaction)
L{n} → L{n+1}
L{n} → FULL (full compaction)
```

### Why Timestamp-Based Filtering Fails

A naive approach — "skip L0 files older than the newest L1 file" — breaks when compaction skips oversized files:

```
Partition contains:
  L1_2000.parquet  (500MB, skipped by compaction due to size)
  L1_3000.parquet  (10MB)
  L1_4000.parquet  (10MB)

Compaction: L1_3000 + L1_4000 → L2_5000

Timestamp filter would exclude L1_2000 (ts 2000 < max L2 ts 5000)
But L1_2000 was NEVER compacted — it was skipped!
```

## Solution: Superseded File Manifest

### Core Concept

Each partition directory gets a `MANIFEST.json` that lists filenames of files that have been compacted into higher-level files. The manifest is written **before** source file deletion, providing immediate protection.

```
date=2024-01-15/node_id=0/
├── MANIFEST.json          ← {"superseded": ["L0_1000.parquet", "L0_2000.parquet"]}
├── L0_1000.parquet        ← orphan (not deleted, but skipped by queries)
├── L0_2000.parquet        ← orphan (not deleted, but skipped by queries)
├── L1_3000.parquet        ← live (not in superseded list)
└── L2_5000.parquet        ← live (not in superseded list)
```

### Manifest Format

```json
{
  "version": 1,
  "superseded": [
    "L0_1000.parquet",
    "L0_2000.parquet",
    "L1_3000.parquet"
  ]
}
```

- **version**: Schema version for forward compatibility
- **superseded**: Filenames (not full paths) of parquet files that should be excluded

### Why "Superseded List" Instead of "Live File List"

A "live files" manifest would require updating on every cache flush (new L0 write), adding complexity to the write path. The "superseded" approach only requires updates during compaction, keeping the write path untouched.

| Approach | Writers | Backward Compat |
|----------|---------|-----------------|
| Live file list | Compactor + cache flush | No (missing manifest = no files) |
| **Superseded list** | **Compactor only** | **Yes (missing manifest = all files live)** |

## Protocol Flow

### Compaction (Write Path)

```
Compactor                              Storage
   │                                      │
   │  1. Read L0 files, merge batches     │
   │                                      │
   │  2. Write L1_output.parquet ────────>│
   │                                      │
   │  3. Read MANIFEST.json <────────────│ (or create new)
   │                                      │
   │  4. Add source filenames to          │
   │     superseded list                  │
   │                                      │
   │  5. Write MANIFEST.json ────────────>│ (atomic PUT)
   │                                      │
   │  6. Delete source files ────────────>│ (best-effort)
   │     (may fail — that's OK)           │
   │                                      │
```

### Query Discovery (Read Path)

```
Discovery                              Storage
   │                                      │
   │  1. List node_id directory           │
   │     GET MANIFEST.json <─────────────│
   │                                      │
   │  2. Build superseded set             │
   │     (empty if no manifest)           │
   │                                      │
   │  3. List *.parquet files <──────────│
   │                                      │
   │  4. Skip files in superseded set     │
   │                                      │
   │  5. Return only live files           │
   │                                      │
```

### Compactor File Listing (Prevents Re-compaction)

The compactor also reads the manifest when listing files for compaction. This prevents a critical data duplication scenario:

```
Without manifest filtering in compactor:
1. L0_1000..L0_5000 → L1_6000  (L0_3000 fails to delete)
2. New writes: L0_7000..L0_10000 arrive
3. Next compaction sees 5 L0 files: [3000, 7000, 8000, 9000, 10000]
4. Compacts ALL 5 → L1_11000
5. L0_3000's data now exists in BOTH L1_6000 AND L1_11000
6. Permanent duplicate that the manifest cannot fix!

With manifest filtering:
3. Compactor reads manifest, skips L0_3000 (superseded)
4. Only compacts [7000, 8000, 9000, 10000] → L1_11000
5. No duplicate data
```

## Implementation Details

### File: `src/storage/manifest.rs`

```rust
pub const MANIFEST_FILENAME: &str = "MANIFEST.json";

pub struct PartitionManifest {
    pub version: u32,
    pub superseded: Vec<String>,
}
```

**Storage API** (for compactor — uses string paths):
- `read_manifest(storage, dir_path)` → `Option<PartitionManifest>`
- `write_manifest(storage, dir_path, manifest)` → `Result`
- `mark_files_superseded(storage, dir_path, filenames)` → `Result`
- `get_superseded_set_from_storage(storage, dir_path)` → `HashSet<String>`

**ObjectStore API** (for query discovery — uses ObjectPath):
- `get_superseded_set(object_store, dir_path)` → `HashSet<String>`

### File: `src/jobs/compactor/partition.rs`

Two integration points in `compact_prefix_internal`:

1. **After listing files**: Read manifest, filter out superseded files before selecting compaction candidates
2. **Re-read after L0→L1**: Filter again with updated manifest before L1→L2+ compaction

One integration point in `compact_level_files_async`:

3. **Before deletion**: Call `mark_files_superseded()` with source filenames

### File: `src/jobs/compactor/full.rs`

One integration point in `full_compact_partition_async`:

1. **Before deletion**: Call `mark_files_superseded()` with compactable filenames

### File: `src/query/providers/parquet/discovery.rs`

One integration point in `discover_files`:

1. **Per node_id directory**: Read manifest, skip files in superseded set

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| No manifest (legacy partitions) | All files included — full backward compatibility |
| All source files deleted successfully | Manifest has stale entries — harmless (filenames don't match any existing file) |
| Source file deletion fails | Manifest ensures queries skip the orphaned file |
| Oversized files skipped by compaction | Never added to superseded list — correctly retained |
| Multi-level compaction (L0→L1→L2→...→FULL) | Each compaction appends to the same manifest |
| Manifest write fails | Warning logged; original deletion-based cleanup still applies |
| Concurrent compaction on same partition | Protected by existing partition-level mutex + distributed lock |
| New L0 files written after manifest update | Not in superseded list — automatically included in queries |
| Manifest file itself during discovery | Skipped — discovery only processes `.parquet` files |

## Atomicity and Crash Safety

| Crash Point | State After Recovery |
|-------------|---------------------|
| After writing compacted file, before manifest | Old behavior — both files visible (duplicates until next compaction) |
| After writing manifest, before source deletion | Correct — queries skip superseded files, compactor skips them too |
| During source deletion (partial) | Correct — remaining undeletable files are in manifest |
| During manifest write | Previous manifest intact (ObjectStore PUT is atomic); retry on next compaction |

## Configuration

No additional configuration required. The manifest is automatically created and maintained by the compaction process. It integrates transparently with existing `[compaction]` settings.

## Future Improvements

1. **Manifest pruning**: Periodically remove entries for files that have been successfully deleted (reduces manifest size in long-running systems)
2. **Manifest-based schema inference**: Read only non-superseded files when inferring table schema (currently `infer_schema_from_prefix` reads all files)
3. **Compaction history**: Extend manifest to record compaction events (inputs, output, timestamp) for observability
4. **File-level statistics**: Store row counts, min/max timestamps per file in the manifest to enable smarter compaction scheduling
