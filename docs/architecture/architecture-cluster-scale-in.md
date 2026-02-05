# Cluster Scale-In Architecture

This document describes the architecture for zero-downtime scale-in (shrinking) of MynaDB clusters using the Cold Standby architecture.

## Overview

Scale-in removes Data nodes from a running cluster without data loss or service interruption. The process follows the **Golden Rule** to ensure safety.

## Golden Rule

| Rule | Description | Risk if Violated |
|------|-------------|------------------|
| **GR-1** | NEVER retire a Standby while it backs up active Primaries | Data loss if Primary crashes after Standby removed |
| **GR-2** | NEVER retire a Primary while it owns partitions | In-progress writes lost during migration |

### Why These Rules Matter

```text
DANGEROUS SCENARIO (GR-1 violation):
─────────────────────────────────────

1. Standby S1 backs up Primary P1
   P1 ◄──replication──► S1

2. Operator retires S1 first (WRONG!)
   P1 ◄──replication──► [S1 REMOVED]

3. P1 crashes!
   [P1 CRASHED] ────────► NO STANDBY TO FAILOVER
                         ↓
                    DATA LOSS!
```

```text
SAFE SCENARIO (Golden Rule followed):
────────────────────────────────────

1. Migrate P1's partitions to P2
   P1 (empty) ────► P2 (has P1's partitions)
        ↑              ↑
        S1 still backs up P2

2. Retire empty P1
   [P1 RETIRED] ────► P2
                       ↑
                       S1 still protects P2

3. Retire orphaned S1 (no primaries to back up)
   [P1 RETIRED] ────► P2
                       ↑
                       [S1 RETIRED] (safe - P2 has own standby)
```

## Three-Phase Execution

Scale-in always executes in three phases:

```
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 1: Partition Migration                                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐          │
│   │ Retiring P1 │ ──► │ Remaining P2│     │ Remaining P3│          │
│   │ (10 parts)  │     │ (+5 parts)  │     │ (+5 parts)  │          │
│   └─────────────┘     └─────────────┘     └─────────────┘          │
│          │                                                          │
│          ▼                                                          │
│    Partitions distributed to remaining Primaries                    │
│    P1 now has 0 partitions (empty, safe to retire)                 │
│                                                                      │
│    HA STATUS: Standbys still protect ALL Primaries                 │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 2: Retire Empty Primaries                                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────┐                                                   │
│   │ P1 (empty)  │ ──► Flush memtable ──► StartRetire ──► Drained   │
│   └─────────────┘                                                   │
│                                                                      │
│   Standbys still exist as safety net for remaining Primaries        │
│                                                                      │
│   HA STATUS: If P2 or P3 crash, Standbys can still take over       │
│              (Critical protection period!)                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 3: Retire Orphaned Standbys                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌─────────────┐                                                   │
│   │ S1 (orphan) │ ──► RemoveFromReplicas ──► StartRetire ──► Done  │
│   └─────────────┘                                                   │
│                                                                      │
│   S1's Primary (P1) already removed                                 │
│   S1 has no Primaries left to back up                              │
│   Safe to retire                                                    │
│                                                                      │
│   HA STATUS: Remaining cluster has correct Primary:Standby ratio   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## HA Unit Concept

With 2:1 Primary:Standby ratio, every 2 Primaries share 1 Standby:

```
┌──────────────────────────────────────────┐
│              HA Unit                      │
├──────────────────────────────────────────┤
│                                          │
│  ┌───────────┐         ┌───────────┐    │
│  │ Primary A │◄───────►│ Standby X │    │
│  └───────────┘    │    └───────────┘    │
│                   │          ▲          │
│                   │          │          │
│  ┌───────────┐    │          │          │
│  │ Primary B │◄───┴──────────┘          │
│  └───────────┘                          │
│                                          │
│  Standby X backs up BOTH Primary A & B  │
│                                          │
└──────────────────────────────────────────┘
```

### Scaling by HA Units

When using the `ha_unit` strategy, scale-in maintains the ratio:

| Before | After (remove 1 HA unit) | Ratio |
|--------|--------------------------|-------|
| 4P + 2S | 2P + 1S | 2:1 |
| 6P + 3S | 4P + 2S | 2:1 |

## Architecture Components

### ScaleInManager

Orchestrates the scale-in process:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ScaleInManager                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────────┐│
│  │  Planner    │──►│  Validator  │──►│       Executor              ││
│  │             │   │             │   │                             ││
│  │ - Node      │   │ - GR-1 ✓   │   │ - Phase 1: Migrate         ││
│  │   selection │   │ - GR-2 ✓   │   │ - Phase 2: Retire Primary  ││
│  │ - Migration │   │ - Min nodes │   │ - Phase 3: Retire Standby  ││
│  │   planning  │   │ - HA check  │   │                             ││
│  │ - Phase gen │   │             │   │                             ││
│  └─────────────┘   └─────────────┘   └─────────────────────────────┘│
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Planner

Generates scale-in plans using different strategies:

| Strategy | Description | Best For |
|----------|-------------|----------|
| `least_loaded` | Retire nodes with fewest partitions | Minimize migration |
| `first_in` | Retire oldest nodes first (FIFO) | Hardware rotation |
| `last_in` | Retire most recently added nodes (LIFO) | Rollback scale-out |
| `ha_unit` | Retire complete HA units | Maintain ratio |

### Validator

Enforces safety rules:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Validation Rules                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ GR-1: Standby Protection                                        ││
│  │ For each Standby being retired:                                 ││
│  │   ALL backed Primaries must also be retiring                    ││
│  │   AND retired BEFORE the Standby                                ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ GR-2: Partition Drain                                           ││
│  │ For each Primary being retired:                                 ││
│  │   ALL partitions must have migration plans                      ││
│  │   Migrations execute BEFORE Primary retirement                  ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ MIN-P: Minimum Primary Count                                    ││
│  │ At least 1 Primary must remain (configurable)                   ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ PHASE-ORDER: Correct Phase Sequence                             ││
│  │ Migration → Retire Primaries → Retire Standbys                  ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Executor

Executes validated plans:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Execution Flow                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│                     ┌──────────────────┐                            │
│                     │   Start Plan     │                            │
│                     └────────┬─────────┘                            │
│                              │                                       │
│              ┌───────────────▼───────────────┐                      │
│              │ Phase 1: For each migration   │                      │
│              │   ReassignPartitionForRetire  │                      │
│              │   (through Raft consensus)    │                      │
│              └───────────────┬───────────────┘                      │
│                              │                                       │
│              ┌───────────────▼───────────────┐                      │
│              │ Phase 2: For each Primary     │                      │
│              │   1. FlushMemtable            │                      │
│              │   2. StartRetire              │                      │
│              │   3. WaitForDrain             │                      │
│              │   4. CompleteRetire           │                      │
│              └───────────────┬───────────────┘                      │
│                              │                                       │
│              ┌───────────────▼───────────────┐                      │
│              │ Phase 3: For each Standby     │                      │
│              │   1. RemoveFromReplicas       │                      │
│              │   2. StartRetire              │                      │
│              │   3. CompleteRetire           │                      │
│              └───────────────┬───────────────┘                      │
│                              │                                       │
│                     ┌────────▼─────────┐                            │
│                     │   Complete       │                            │
│                     └──────────────────┘                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Node State Transitions

```
                    ┌─────────┐
                    │ Active  │
                    └────┬────┘
                         │
                         │ StartRetire
                         ▼
                    ┌─────────┐
         ┌─────────│Retiring │
         │         └────┬────┘
         │              │
    CancelRetire        │ CompleteRetire
         │              │ (memtable flushed,
         ▼              │  partitions migrated)
    ┌─────────┐         ▼
    │ Active  │    ┌─────────┐
    └─────────┘    │ Drained │◄── Safe to stop
                   └─────────┘
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/scale-in/plan` | POST | Generate scale-in plan (supports dry_run) |
| `/cluster/scale-in/execute-plan` | POST | Execute with Golden Rule enforcement |
| `/cluster/scale-in/recommend` | GET | Get node recommendations |
| `/nodes/{id}/retire` | POST | Start retirement for single node |
| `/nodes/{id}/retire/status` | GET | Get retirement progress |
| `/nodes/{id}/retire/cancel` | POST | Cancel retirement |

## Configuration

```toml
[scale_in]
# Minimum Primary nodes to keep
min_primaries = 1

# Minimum Standby nodes to keep (0 = no minimum)
min_standbys = 0

# Primary:Standby ratio (2 = 2:1)
primary_standby_ratio = 2

# Timeout for partition migration (seconds)
migration_timeout_secs = 300

# Timeout for memtable flush (seconds)
flush_timeout_secs = 60

# Default strategy: "least_loaded", "first_in", "last_in", "ha_unit"
default_strategy = "least_loaded"
```

## Failure Scenarios

| Scenario | Recovery |
|----------|----------|
| Migration fails | Retry or abort, partitions remain on original Primary |
| Primary crash during Phase 2 | Standby takes over (still exists!) |
| Meta leader crash | Raft elects new leader, operation continues |
| Executor crash | Plan can be re-executed (idempotent operations) |

## Best Practices

1. **Always use dry_run first** to preview changes
2. **Monitor during execution** via `/nodes/{id}/retire/status`
3. **Scale by HA units** when possible to maintain ratio
4. **Back up before scale-in** for critical deployments
5. **Test in staging** before production scale-in
