# Cluster Scale-In Guide

This guide explains how to safely shrink a MynaDB cluster using the scale-in API.

## Quick Start

### 1. Preview the Scale-In Plan (Dry Run)

Always start with a dry run to see what will happen:

**Simplest: Use `reduce_num` (auto-calculates primaries/standbys based on ratio)**

```bash
# Remove 3 nodes total (with 2:1 ratio → 2 Primaries + 1 Standby)
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_num": 3,
    "dry_run": true
  }'
```

**Advanced: Specify exact counts**

```bash
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 2,
    "reduce_standbys": 1,
    "strategy": "least_loaded",
    "dry_run": true
  }'
```

Response:
```json
{
  "success": true,
  "plan_id": "a1b2c3d4",
  "dry_run": true,
  "phases": [
    {
      "phase_number": 1,
      "phase_type": "Migrate Partitions",
      "description": "Migrate 15 partitions away from retiring Primaries",
      "affected_nodes": ["data-1", "data-2"],
      "operations_count": 15
    },
    {
      "phase_number": 2,
      "phase_type": "Retire Primaries",
      "description": "Retire 2 empty Primary nodes",
      "affected_nodes": ["data-1", "data-2"],
      "operations_count": 8
    },
    {
      "phase_number": 3,
      "phase_type": "Retire Standbys",
      "description": "Retire 1 orphaned Standby node",
      "affected_nodes": ["data-5"],
      "operations_count": 3
    }
  ],
  "primaries_to_retire": ["data-1", "data-2"],
  "standbys_to_retire": ["data-5"],
  "partitions_to_migrate": 15,
  "warnings": [],
  "final_state": {
    "primary_count": 2,
    "standby_count": 1,
    "ratio_maintained": true,
    "total_partitions": 100
  }
}
```

### 2. Execute the Scale-In

Once satisfied with the plan, execute it:

```bash
curl -X POST http://localhost:6000/cluster/scale-in/execute-plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 2,
    "reduce_standbys": 1,
    "strategy": "least_loaded",
    "force_flush": true
  }'
```

Response:
```json
{
  "success": true,
  "plan_id": "a1b2c3d4",
  "primaries_retired": ["data-1", "data-2"],
  "standbys_retired": ["data-5"],
  "partitions_migrated": 15,
  "duration_ms": 45230,
  "errors": [],
  "warnings": []
}
```

## Golden Rule

**Two rules you must NEVER violate:**

| Rule | What It Means | Why It Matters |
|------|--------------|----------------|
| **GR-1** | Never retire a Standby before its Primaries | Standbys are the safety net. If removed first, a Primary crash = data loss |
| **GR-2** | Never retire a Primary with partitions | Partitions must be migrated first to avoid losing in-flight writes |

The API automatically enforces these rules. If you try to violate them, you'll get an error:

```json
{
  "success": false,
  "error": "Validation failed: Rule 'GR-1: Standby Protection' violated: Standby data-5 still backs up active Primaries: [\"data-3\"]"
}
```

## Strategies

### Least Loaded (Default)

Retire nodes with the fewest partitions to minimize migration work:

```bash
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 1,
    "strategy": "least_loaded",
    "dry_run": true
  }'
```

**Best for:** Quick scale-in with minimal data movement

### First In

Retire the oldest nodes first (FIFO - First In, First Out):

```bash
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 1,
    "strategy": "first_in",
    "dry_run": true
  }'
```

**Best for:** Rotating out older hardware or replacing aging instances

### HA Unit

Retire complete HA units (2 Primaries + 1 Standby) to maintain ratio:

```bash
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 2,
    "reduce_standbys": 1,
    "strategy": "ha_unit",
    "dry_run": true
  }'
```

**Best for:** Maintaining clean Primary:Standby ratio

### Last In

Retire the most recently added nodes (LIFO):

```bash
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 1,
    "strategy": "last_in",
    "dry_run": true
  }'
```

**Best for:** Rolling back a recent scale-out

## Monitoring Progress

### Check Individual Node Status

```bash
curl http://localhost:6000/nodes/data-1/retire/status
```

Response:
```json
{
  "is_retiring": true,
  "state": "retiring",
  "progress_percent": 75,
  "partitions_total": 20,
  "partitions_reassigned": 15,
  "memtable_rows_remaining": 1000,
  "elapsed_secs": 30
}
```

### List All Nodes

```bash
# Active primaries
curl "http://localhost:6000/nodes?mode=primary&state=active"

# Retiring nodes
curl "http://localhost:6000/nodes?state=retiring"

# Drained (safe to stop)
curl "http://localhost:6000/nodes?state=drained"
```

## Canceling a Scale-In

If you need to abort:

```bash
curl -X POST http://localhost:6000/nodes/data-1/retire/cancel
```

Note: Only works if the node hasn't fully drained yet.

## Common Scenarios

### Scenario 1: Reduce Cluster by Half

```bash
# 1. Check current state
curl http://localhost:6000/nodes | jq '. | length'
# Output: 8 (4 Primaries, 4 Standbys assuming 1:1)

# 2. Preview the plan
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 2,
    "reduce_standbys": 2,
    "strategy": "ha_unit",
    "dry_run": true
  }'

# 3. Execute if satisfied
curl -X POST http://localhost:6000/cluster/scale-in/execute-plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 2,
    "reduce_standbys": 2,
    "strategy": "ha_unit"
  }'
```

### Scenario 2: Remove Specific Faulty Node

```bash
# 1. Identify the node to remove
FAULTY_NODE="data-3"

# 2. Preview with explicit strategy
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d "{
    \"explicit_nodes\": [\"$FAULTY_NODE\"],
    \"strategy\": \"explicit\",
    \"dry_run\": true
  }"

# 3. Check warnings - the API will show if ratio will be broken
# If the faulty node is a Primary, you may need to also retire its Standby

# 4. Execute
curl -X POST http://localhost:6000/cluster/scale-in/execute-plan \
  -H "Content-Type: application/json" \
  -d "{
    \"explicit_nodes\": [\"$FAULTY_NODE\"],
    \"strategy\": \"explicit\"
  }"
```

### Scenario 3: Scale Down to Minimum

```bash
# Keep only 2 Primaries + 1 Standby (minimum recommended)
curl -X POST http://localhost:6000/cluster/scale-in/plan \
  -H "Content-Type: application/json" \
  -d '{
    "reduce_primaries": 2,
    "reduce_standbys": 1,
    "strategy": "least_loaded",
    "dry_run": true
  }'

# Check warnings - will warn about reduced redundancy
```

## Troubleshooting

### Error: "Validation failed: GR-1"

**Cause:** Trying to retire a Standby that still backs up active Primaries.

**Solution:** Either:
1. Also retire the Primaries it backs up
2. Don't retire that Standby

### Error: "Insufficient nodes"

**Cause:** Trying to remove more nodes than available while keeping minimum count.

**Solution:** Reduce the number of nodes to retire.

### Error: "No remaining primaries"

**Cause:** Trying to retire all Primaries.

**Solution:** At least 1 Primary must remain.

### Retirement Stuck in "Retiring" State

**Cause:** Partitions still being migrated or memtable not flushed.

**Solution:**
1. Check progress: `curl http://localhost:6000/nodes/{id}/retire/status`
2. If stuck, check data node logs for migration errors
3. Force flush if needed: `curl -X POST http://localhost:7000/flush`

### Migration Too Slow

**Cause:** Many partitions to migrate.

**Solution:**
1. Use `least_loaded` strategy to minimize migrations
2. Scale in smaller batches
3. Increase `migration_timeout_secs` in config

## Best Practices

1. **Always dry_run first** - Preview before executing
2. **Monitor progress** - Watch the `/nodes/{id}/retire/status` endpoint
3. **Scale gradually** - Remove 1-2 nodes at a time for large clusters
4. **Use ha_unit strategy** - Maintains clean ratio automatically
5. **Back up before scale-in** - Especially for critical data
6. **Schedule during low traffic** - Reduces impact of migrations
7. **Test in staging first** - Verify your plan works

## API Reference

### POST /cluster/scale-in/plan

Generate a scale-in plan (supports dry_run).

**Request:**
```json
{
  "reduce_num": 3,
  "reduce_primaries": 2,
  "reduce_standbys": 1,
  "strategy": "least_loaded",
  "dry_run": true,
  "force_flush": true
}
```

**Parameter Priority:**
1. `reduce_primaries` + `reduce_standbys` (highest - explicit counts)
2. `reduce_num` (auto-calculates based on Primary:Standby ratio)

**`reduce_num` Calculation (Ratio-Preserving):**

**Constraint:** `actual_ratio <= config_ratio`
- Every Standby backs up AT MOST `ratio` Primaries (prevents overload/OOM)
- Uses ceiling division for standbys: `final_standbys = ceil(remaining / (ratio + 1))`

Example with ratio=2 (starting from 6P + 3S = 9 nodes):
| reduce_num | Final | Remove | Actual Ratio | Valid |
|------------|-------|--------|--------------|-------|
| 1 | 5P + 3S | 1P + 0S | 1.67:1 | ✓ |
| 2 | 4P + 3S | 2P + 0S | 1.33:1 | ✓ |
| 3 | 4P + 2S | 2P + 1S | 2.0:1 | ✓ |
| 5 | 2P + 2S | 4P + 1S | 1.0:1 | ✓ |

**Why 7 nodes → 4P + 3S (not 5P + 2S)?**
- 5P + 2S = ratio 2.5:1 → Each Standby backs 2.5 Primaries (OVERLOAD!)
- 4P + 3S = ratio 1.33:1 → Each Standby backs ~1.33 Primaries (SAFE)

Repeated calls example:
```
Start:  6P + 3S (ratio 2.0) ✓
Call 1: 4P + 3S (ratio 1.33) ✓  (reduce_num=2)
Call 2: 3P + 2S (ratio 1.5) ✓   (reduce_num=2)
Call 3: 2P + 1S (ratio 2.0) ✓   (reduce_num=2)
```

**Available Strategies:**
- `least_loaded` (default) - Retire nodes with fewest partitions
- `first_in` - Retire oldest nodes first (FIFO)
- `last_in` - Retire newest nodes first (LIFO)
- `ha_unit` - Retire complete HA units

### POST /cluster/scale-in/execute-plan

Execute scale-in with Golden Rule enforcement.

**Request:**
```json
{
  "reduce_num": 3,
  "reduce_primaries": 2,
  "reduce_standbys": 1,
  "strategy": "least_loaded",
  "force_flush": true
}
```

### GET /cluster/scale-in/recommend

Get recommendations for which nodes to retire.

**Query params:**
- `primaries`: Number of primaries to recommend
- `standbys`: Number of standbys to recommend
- `count`: Total count (if not specifying separately)

### POST /nodes/{node_id}/retire

Start retirement for a single node.

### GET /nodes/{node_id}/retire/status

Get retirement progress.

### POST /nodes/{node_id}/retire/cancel

Cancel an in-progress retirement.
