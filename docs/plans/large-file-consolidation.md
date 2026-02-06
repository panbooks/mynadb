# Large File Consolidation Plan

## Overview

5 Rust source files exceed 50KB and should be refactored into smaller, focused modules:

| File | Size | Lines | Proposed Split |
|------|------|-------|----------------|
| `src/meta/raft/state_machine.rs` | 115K | 3298 | 8 modules |
| `src/jobs/compactor.rs` | 113K | 3192 | 5 modules |
| `src/query/query_engine.rs` | 74K | 1967 | 4 modules |
| `src/meta/service/meta_node.rs` | 58K | 1594 | 4 modules |
| `src/data/server/flight.rs` | 53K | 1375 | 3 modules |

---

## 1. state_machine.rs (115K → 8 modules)

**Current:** Single 3298-line file with 14 logical sections

**Proposed Structure:**
```
src/meta/raft/
├── mod.rs                    # Re-exports
├── state_machine.rs          # Core struct + apply_command (~300 lines)
├── ops/
│   ├── mod.rs
│   ├── node.rs               # Node Operations: register, heartbeat, deregister (~250 lines)
│   ├── file.rs               # File Operations: register_files, remove_files (~100 lines)
│   ├── lock.rs               # Lock Operations: acquire, release, renew (~150 lines)
│   ├── election.rs           # Leader Election: campaign, resign (~150 lines)
│   ├── event.rs              # Event Operations: publish, subscribe (~100 lines)
│   ├── lease.rs              # Lease Management: expire_leases (~50 lines)
│   ├── replica.rs            # Replica Operations: assign, update, remove (~400 lines)
│   ├── standby.rs            # Cold Standby + Pairing (~300 lines)
│   ├── scale_in.rs           # Retirement Operations (~400 lines)
│   ├── recovery.rs           # Recovery Operations (~100 lines)
│   └── query.rs              # Read-only Query Operations (~400 lines)
├── raft_impl.rs              # RaftStateMachine trait impl (~200 lines)
└── snapshot.rs               # MetaSnapshotBuilder impl (~100 lines)
```

**Benefits:**
- Each operation type in its own file
- Easier to test individual operations
- Clear separation of concerns

---

## 2. compactor.rs (113K → 5 modules)

**Current:** Single 3192-line file mixing LSM compaction, full compaction, and utilities

**Proposed Structure:**
```
src/jobs/compactor/
├── mod.rs                    # Re-exports, Compactor struct (~200 lines)
├── config.rs                 # CompactionConfig, defaults (~100 lines)
├── lsm.rs                    # LSM Compaction: compact_catalog, compact_level_files (~600 lines)
├── full.rs                   # Full Compaction: full_compact_*, full_compact_partition (~500 lines)
├── parquet_ops.rs            # Parquet I/O: read, write, list files (~400 lines)
├── schema_ops.rs             # Schema/batch: align, merge, convert (~400 lines)
├── sorting.rs                # Sorting: merge_sorted, cardinality sort (~300 lines)
├── background.rs             # Background jobs: start_background_* (~200 lines)
└── tests.rs                  # All test code (~500 lines)
```

**Benefits:**
- LSM vs Full compaction clearly separated
- Reusable parquet/schema utilities
- Tests in dedicated file

---

## 3. query_engine.rs (74K → 4 modules)

**Current:** Single 1967-line file mixing registration, caching, and discovery

**Proposed Structure:**
```
src/query/
├── mod.rs                    # Existing
├── query_engine.rs           # Core QueryEngine struct (~300 lines)
├── engine/
│   ├── mod.rs
│   ├── registration.rs       # Table registration logic (~500 lines)
│   ├── discovery.rs          # Table/catalog discovery (~400 lines)
│   ├── cache_mgmt.rs         # Cache management, warming, stats (~400 lines)
│   └── metadata.rs           # Parquet metadata operations (~300 lines)
```

**Benefits:**
- Registration logic separate from discovery
- Cache management isolated
- Easier to understand table lifecycle

---

## 4. meta_node.rs (58K → 4 modules)

**Current:** Single 1594-line file with service methods and forwarding logic

**Proposed Structure:**
```
src/meta/service/
├── mod.rs                    # Re-exports
├── meta_node.rs              # Core MetaNode struct, lifecycle (~300 lines)
├── node_ops.rs               # Node: register, heartbeat, deregister, list (~300 lines)
├── file_ops.rs               # File: register, remove, list (~150 lines)
├── lock_ops.rs               # Lock: try_lock, unlock, renew, get (~250 lines)
├── election_ops.rs           # Election: campaign, resign, get_leader (~200 lines)
├── replica_ops.rs            # Replica: get, list, auto_assign (~200 lines)
└── forwarding.rs             # All forward_*_to_leader methods (~200 lines)
```

**Benefits:**
- Forwarding logic isolated (cross-cutting concern)
- Operation types clearly separated
- Matches state_machine structure

---

## 5. flight.rs (53K → 3 modules)

**Current:** Single 1375-line file implementing Flight service

**Proposed Structure:**
```
src/data/server/
├── mod.rs                    # Existing
├── flight.rs                 # Core FlightDataService struct, setup (~300 lines)
├── flight/
│   ├── mod.rs
│   ├── write_handlers.rs     # ingest_line_protocol, handle_*_write (~500 lines)
│   ├── query_handlers.rs     # do_get, do_action query handling (~300 lines)
│   └── protocol.rs           # Flight protocol: handshake, list_*, get_* (~300 lines)
```

**Benefits:**
- Write path separate from read path
- Protocol boilerplate isolated
- Easier to add new handlers

---

## Implementation Priority

| Priority | File | Complexity | Impact |
|----------|------|------------|--------|
| 1 | state_machine.rs | High | Highest - most complex, most sections |
| 2 | compactor.rs | Medium | High - clear LSM/full split |
| 3 | query_engine.rs | Medium | Medium - registration/discovery split |
| 4 | meta_node.rs | Low | Medium - follows state_machine pattern |
| 5 | flight.rs | Low | Low - already reasonably organized |

---

## Refactoring Approach

For each file:

1. **Create module directory** with `mod.rs`
2. **Extract structs/configs first** (minimal risk)
3. **Extract pure functions** (sorting, schema ops)
4. **Extract operation groups** (one at a time)
5. **Move tests last** (after all code moved)
6. **Run tests after each extraction**

### Example: state_machine.rs First Step

```rust
// src/meta/raft/ops/mod.rs
pub mod node;
pub mod file;
pub mod lock;
// ... etc

// src/meta/raft/ops/node.rs
use super::super::MetaStateMachine;
use crate::meta::types::*;

impl MetaStateMachine {
    pub(crate) fn register_node(&self, mut info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
        // ... moved from state_machine.rs
    }

    pub(crate) fn heartbeat(&self, node_id: &str, lease_id: u64, timestamp: i64) -> MetaResponse {
        // ... moved from state_machine.rs
    }
    // ...
}
```

---

## Estimated Effort

| File | Estimated Time | Risk |
|------|----------------|------|
| state_machine.rs | 4-6 hours | Medium (many cross-references) |
| compactor.rs | 3-4 hours | Low (clear boundaries) |
| query_engine.rs | 2-3 hours | Low |
| meta_node.rs | 2-3 hours | Low |
| flight.rs | 1-2 hours | Low |

**Total: 12-18 hours**

---

## Success Criteria

- No file > 50KB (target: < 30KB each)
- All tests pass
- No public API changes
- Improved code navigation
- Clear module responsibilities
