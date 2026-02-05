# No-Promotion Standby Architecture

## Overview

This document describes the **No-Promotion Standby Architecture** for MynaDB's Cold Standby system. In this design, Standby nodes **never promote to Primary**, and Primary nodes **never degrade to Standby**. This architecture is optimized for Kubernetes environments with elastic scaling requirements.

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Why Promotion-Based Design is Problematic](#why-promotion-based-design-is-problematic)
3. [No-Promotion Architecture](#no-promotion-architecture)
4. [Promotion vs No-Promotion Comparison](#promotion-vs-no-promotion-comparison)
5. [Kubernetes Deployment Strategies](#kubernetes-deployment-strategies)
6. [Failure Scenarios and Recovery](#failure-scenarios-and-recovery)
7. [Configuration](#configuration)
8. [Operational Considerations](#operational-considerations)

---

## Design Philosophy

### Core Principles

1. **Simplicity Over Flexibility**: Promotion logic introduces significant complexity and edge cases. Eliminating it dramatically simplifies the system.

2. **Let Kubernetes Handle Failures**: Instead of complex promotion logic, rely on Kubernetes to replace failed pods and Meta to reassign roles.

3. **Standby as Data Protection Only**: Standby exists solely to prevent data loss during the flush interval window. It never serves traffic.

4. **Stateless Compute, Stateful Storage**: Compute nodes (Primary/Standby) are ephemeral. S3/MinIO holds the durable state.

### The Data Loss Window Problem

```
Timeline without Standby:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  T0        T1        T2        T3        T4        T5              │
│  │         │         │         │         │         │               │
│  ▼         ▼         ▼         ▼         ▼         ▼               │
│  Flush     Write     Write     Write     CRASH!    Recovery        │
│  to S3     100MB     200MB     300MB     ✗         from S3         │
│  ✓                                                                  │
│                                                                     │
│  Data in S3: [T0 snapshot]                                         │
│  Data LOST:  [T1-T4 writes = 600MB] ← Unacceptable!               │
│                                                                     │
│  With 5-minute flush interval and 10MB/s write rate:               │
│  Potential data loss = 5min × 10MB/s = 3GB                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

Standby solves this by holding a replica of unflushed data:

```
Timeline with Standby (No-Promotion):
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Primary:  Flush → Write → Write → Write → CRASH!                  │
│              │       │       │       │                              │
│              │       ▼       ▼       ▼                              │
│  Standby:    │    Receive  Receive  Receive                        │
│              │    100MB    200MB    300MB                           │
│              │                         │                            │
│              ▼                         ▼                            │
│  FlushCheckpoint              FlushForRecovery                     │
│  (clear old cache)            (flush 600MB to S3)                  │
│                                                                     │
│  Data in S3: [T0 snapshot] + [T1-T4 from Standby flush]           │
│  Data LOST:  None! ✓                                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Why Promotion-Based Design is Problematic

Traditional HA databases use promotion: when Primary fails, Standby becomes the new Primary. This seems intuitive but introduces significant complexity:

### Problem 1: State Machine Complexity

```
Promotion-Based State Transitions:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│                    ┌──────────────┐                                │
│           ┌───────►│   STANDBY    │◄───────┐                       │
│           │        └──────┬───────┘        │                       │
│           │               │                │                       │
│    Demotion          Promotion         Demotion                    │
│           │               │                │                       │
│           │               ▼                │                       │
│           │        ┌──────────────┐        │                       │
│           └────────│   PRIMARY    ├────────┘                       │
│                    └──────┬───────┘                                │
│                           │                                        │
│                     Network Split                                  │
│                           │                                        │
│                           ▼                                        │
│                    ┌──────────────┐                                │
│                    │ SPLIT-BRAIN  │ ← Dangerous!                   │
│                    │ (2 Primaries)│                                │
│                    └──────────────┘                                │
│                                                                     │
│  Edge Cases to Handle:                                             │
│  • Promotion during in-flight writes                               │
│  • Demotion with uncommitted data                                  │
│  • Network partition (both think they're Primary)                  │
│  • Promotion timeout (Standby partially promoted)                  │
│  • Client redirect during promotion                                │
│  • WAL state synchronization                                       │
│  • Cache ownership transfer                                        │
│  • Replication stream reconnection                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Problem 2: Split-Brain Risk

```
Split-Brain Scenario:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  T1: Normal operation                                              │
│      ┌─────────┐         ┌─────────┐                              │
│      │ Primary │────────►│ Standby │                              │
│      │  (P1)   │  repl   │  (S1)   │                              │
│      └─────────┘         └─────────┘                              │
│                                                                     │
│  T2: Network partition (Meta can't reach P1, but P1 is alive)     │
│      ┌─────────┐    ✗    ┌─────────┐                              │
│      │ Primary │ ─ ─ ─ ─ │ Standby │                              │
│      │  (P1)   │         │  (S1)   │                              │
│      └─────────┘         └─────────┘                              │
│           │                   │                                    │
│      Still serving       Meta promotes                             │
│      writes!             S1 to Primary                             │
│                                                                     │
│  T3: Two Primaries! (Split-Brain)                                  │
│      ┌─────────┐         ┌─────────┐                              │
│      │ Primary │         │ Primary │ ← CONFLICT!                  │
│      │  (P1)   │         │  (S1)   │                              │
│      └─────────┘         └─────────┘                              │
│           │                   │                                    │
│      Writes A, B, C      Writes X, Y, Z                           │
│                                                                     │
│  T4: Network heals - Which data wins? How to merge?               │
│      Data divergence is extremely hard to resolve                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Problem 3: Promotion Timing Edge Cases

```
Promotion During Write:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Client        Primary (P1)        Meta          Standby (S1)      │
│    │                │                │                │             │
│    │── Write ──────►│                │                │             │
│    │                │── Replicate ──────────────────►│             │
│    │                │                │                │ (receives)  │
│    │                ╳ CRASH          │                │             │
│    │                                 │                │             │
│    │                │◄── Promote! ───│                │             │
│    │                                 │                │             │
│    │                        ┌────────┴────────┐                    │
│    │                        │ S1 now Primary  │                    │
│    │                        │ Has the write?  │                    │
│    │                        │ Maybe partial?  │                    │
│    │                        └─────────────────┘                    │
│    │                                                               │
│    │◄── ACK? NACK? Timeout? ──────────────────│                    │
│    │                                                               │
│  Questions:                                                        │
│  • Did S1 receive the full batch before P1 crashed?               │
│  • Should client retry? Will it duplicate?                        │
│  • What sequence number should S1 use for new writes?             │
│  • How does S1 know P1's last committed state?                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Problem 4: Demotion Complexity

```
Demotion Scenario (Network Healed):
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Old Primary (P1) reconnects after partition:                      │
│                                                                     │
│  P1's State:                      New Primary (S1) State:          │
│  ┌─────────────────────┐          ┌─────────────────────┐          │
│  │ Cache: 500MB        │          │ Cache: 300MB        │          │
│  │ WAL: entries 1-100  │          │ WAL: entries 50-80  │          │
│  │ Last flush: seq=50  │          │ Last flush: seq=80  │          │
│  │ Serving: writes A-F │          │ Serving: writes X-Z │          │
│  └─────────────────────┘          └─────────────────────┘          │
│                                                                     │
│  Demotion Questions:                                               │
│  • P1 has writes A-F not in S1 - discard or merge?                │
│  • P1's WAL has entries S1 doesn't have - what to do?             │
│  • P1's cache is larger - flush first or discard?                 │
│  • How to atomically transfer Primary role?                        │
│  • What if demotion fails halfway?                                 │
│                                                                     │
│  Implementation requires:                                          │
│  • Distributed transaction for role swap                           │
│  • WAL reconciliation protocol                                     │
│  • Cache merge or discard logic                                    │
│  • Client connection draining                                      │
│  • Sequence number synchronization                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Problem 5: Client Routing During Promotion

```
Client Confusion During Promotion:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Time    Client          P1 (dying)    Meta         S1 (promoting) │
│  ────    ──────          ──────────    ────         ────────────── │
│                                                                     │
│  T1      Write req ──────► Accept                                  │
│                            (slow, dying)                           │
│                                                                     │
│  T2                                    Detects P1                  │
│                                        unhealthy                   │
│                                                                     │
│  T3                                    ──► Promote!                │
│                                            (becomes Primary)       │
│                                                                     │
│  T4      Timeout!                                                  │
│          Retry ────────────────────────────────────► Reject!       │
│                                                      (still        │
│                                                       promoting)   │
│                                                                     │
│  T5      Get leader? ──────────────────► Returns S1                │
│                                                                     │
│  T6      Retry to S1 ──────────────────────────────► Accept        │
│                                                                     │
│  T7      Original write                                            │
│          completes ◄──────                                         │
│          (P1 recovered!)                                           │
│                                                                     │
│  Result: Duplicate write (T1 and T6 both succeeded)               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Problem 6: Resource Management

```
Promotion Resource Requirements:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Standby (before promotion):     Primary (after promotion):        │
│  ┌─────────────────────┐         ┌─────────────────────┐           │
│  │ Memory: 2GB         │         │ Memory: 8GB         │           │
│  │ CPU: 1 core         │   →→→   │ CPU: 4 cores        │           │
│  │ Network: low        │         │ Network: high       │           │
│  │ No query engine     │         │ Full query engine   │           │
│  │ No HTTP server      │         │ HTTP + Flight + SQL │           │
│  └─────────────────────┘         └─────────────────────┘           │
│                                                                     │
│  Problems:                                                          │
│  • Standby may not have enough resources to act as Primary        │
│  • Starting query engine/HTTP takes time                          │
│  • Memory pressure during transition                               │
│  • In K8s: pod resource limits prevent scaling up                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## No-Promotion Architecture

### Core Design

```
No-Promotion Model:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  RULE 1: Primary NEVER becomes Standby                             │
│  RULE 2: Standby NEVER becomes Primary                             │
│  RULE 3: Failed nodes are REPLACED, not promoted/demoted           │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                         Meta Cluster                         │   │
│  │  • Tracks all nodes and their roles                         │   │
│  │  • Assigns roles at registration (immutable)                │   │
│  │  • Manages Primary-Standby pairing                          │   │
│  │  • Triggers FlushForRecovery on Primary failure             │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                          │                                          │
│            ┌─────────────┴─────────────┐                           │
│            ▼                           ▼                            │
│  ┌─────────────────────┐    ┌─────────────────────┐                │
│  │   Primary Pool      │    │   Standby Pool      │                │
│  │                     │    │                     │                │
│  │  ┌────┐ ┌────┐     │    │  ┌────┐ ┌────┐     │                │
│  │  │ P1 │ │ P2 │ ... │    │  │ S1 │ │ S2 │ ... │                │
│  │  └────┘ └────┘     │    │  └────┘ └────┘     │                │
│  │                     │    │                     │                │
│  │  • Serve traffic    │    │  • Receive repl    │                │
│  │  • Write to cache   │    │  • Hold cache      │                │
│  │  • Flush to S3      │    │  • Flush on signal │                │
│  │  • Run queries      │    │  • No traffic      │                │
│  └─────────────────────┘    └─────────────────────┘                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Failure Recovery Flow

```
Primary Failure - No Promotion Recovery:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  T1: Normal Operation                                              │
│      ┌────────┐  replication  ┌────────┐                          │
│      │   P1   │──────────────►│   S1   │                          │
│      │ Active │               │ Standby │                          │
│      └────────┘               └────────┘                          │
│          │                                                         │
│      Traffic                                                       │
│                                                                     │
│  T2: P1 Crashes                                                    │
│      ┌────────┐               ┌────────┐                          │
│      │   P1   │      ✗        │   S1   │                          │
│      │  DEAD  │               │ Standby │ ← Still Standby!        │
│      └────────┘               └────────┘                          │
│                                                                     │
│  T3: Meta Detects Failure                                          │
│      • Meta sends FlushForRecovery to S1                          │
│      • S1 flushes its cache to S3 (P1's unflushed data saved)    │
│      • S1 clears cache, returns to standby pool                   │
│      • Traffic routes to other Primaries (P2, P3, ...)            │
│                                                                     │
│  T4: Kubernetes Replaces P1                                        │
│      ┌────────┐               ┌────────┐                          │
│      │  P1'   │               │   S1   │                          │
│      │  NEW   │               │ (idle) │                          │
│      └────────┘               └────────┘                          │
│          │                                                         │
│      • P1' registers with Meta                                     │
│      • Meta assigns P1' as Primary (fresh start)                  │
│      • Meta pairs P1' with S1 (or another Standby)                │
│      • P1' loads state from S3 (including S1's flushed data)      │
│                                                                     │
│  T5: Normal Operation Resumed                                      │
│      ┌────────┐  replication  ┌────────┐                          │
│      │  P1'   │──────────────►│   S1   │                          │
│      │ Active │               │ Standby │                          │
│      └────────┘               └────────┘                          │
│          │                                                         │
│      Traffic                                                       │
│                                                                     │
│  Key Points:                                                       │
│  • S1 NEVER became Primary                                        │
│  • No state machine transitions                                    │
│  • No split-brain possible                                        │
│  • Simple, predictable recovery                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Standby Failure Recovery

```
Standby Failure - Simple Reassignment:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  T1: S1 Crashes                                                    │
│      ┌────────┐               ┌────────┐                          │
│      │   P1   │──────✗───────│   S1   │                          │
│      │ Active │               │  DEAD  │                          │
│      └────────┘               └────────┘                          │
│          │                                                         │
│      Traffic continues normally (P1 unaffected)                   │
│                                                                     │
│  T2: Meta Reassigns                                                │
│      • Meta detects S1 failure                                     │
│      • Meta finds available Standby from pool (S3)                │
│      • Meta pairs P1 with S3                                      │
│      • P1 starts replicating to S3                                │
│                                                                     │
│      ┌────────┐  replication  ┌────────┐                          │
│      │   P1   │──────────────►│   S3   │                          │
│      │ Active │               │ Standby │                          │
│      └────────┘               └────────┘                          │
│          │                                                         │
│      Traffic continues (no interruption)                          │
│                                                                     │
│  T3: Kubernetes Replaces S1                                        │
│      • S1' registers with Meta as Standby                         │
│      • S1' joins standby pool (available for future pairing)      │
│                                                                     │
│  Key Point: Primary was NEVER affected by Standby failure         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Promotion vs No-Promotion Comparison

### Feature Comparison

| Aspect | Promotion-Based | No-Promotion |
|--------|-----------------|--------------|
| **Complexity** | High (state machine, transitions) | Low (fixed roles) |
| **Split-Brain Risk** | Yes (requires fencing) | No (impossible by design) |
| **Recovery Time** | Variable (promotion + catchup) | Fast (just reassign) |
| **Data Consistency** | Complex (merge conflicts) | Simple (single writer) |
| **Client Routing** | Complex (follow leader) | Simple (any Primary) |
| **Resource Planning** | Hard (Standby must handle Primary load) | Easy (dedicated sizing) |
| **Testing** | Hard (many state transitions) | Easy (few states) |
| **Debugging** | Hard (transition logs) | Easy (clear roles) |

### Failure Scenario Comparison

| Scenario | Promotion-Based | No-Promotion |
|----------|-----------------|--------------|
| **Primary Crash** | Promote Standby (complex) | Flush Standby cache, K8s replaces Primary |
| **Standby Crash** | Find new Standby | Find new Standby (same) |
| **Network Partition** | Risk split-brain | Standby just waits, no risk |
| **Promotion Failure** | Stuck in limbo state | N/A (no promotion) |
| **Both Crash** | Data loss + complex recovery | Data loss (same) + simple recovery |

### Code Complexity Comparison

```
Promotion-Based Code Requirements:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  State Machine:                                                    │
│  • STANDBY → PROMOTING → PRIMARY                                   │
│  • PRIMARY → DEMOTING → STANDBY                                    │
│  • STANDBY → PROMOTING → FAILED → STANDBY                         │
│  • PRIMARY → DEMOTING → FAILED → PRIMARY                          │
│                                                                     │
│  Required Components:                                              │
│  • Promotion coordinator                                           │
│  • Demotion handler                                                │
│  • Fencing mechanism (STONITH)                                     │
│  • WAL reconciliation                                              │
│  • Sequence number synchronization                                 │
│  • Client redirect protocol                                        │
│  • Split-brain detection                                           │
│  • Split-brain resolution                                          │
│  • Resource scaling during transition                              │
│  • Connection draining                                             │
│  • Health check during transition                                  │
│                                                                     │
│  Estimated: 3000-5000 lines of complex, hard-to-test code         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

No-Promotion Code Requirements:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  State Machine:                                                    │
│  • PRIMARY (forever)                                               │
│  • STANDBY (forever)                                               │
│                                                                     │
│  Required Components:                                              │
│  • FlushForRecovery handler                                        │
│  • Standby reassignment                                            │
│  • (K8s handles the rest)                                          │
│                                                                     │
│  Estimated: 200-500 lines of simple, testable code                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Recovery Time Comparison

```
Primary Failure Recovery Timeline:

Promotion-Based:
┌─────────────────────────────────────────────────────────────────────┐
│  T+0s     T+5s      T+10s     T+15s     T+30s     T+60s            │
│  │        │         │         │         │         │                 │
│  Crash    Detect    Start     WAL       Client    Traffic          │
│           failure   promotion sync      redirect  restored         │
│                     │         │         │         │                 │
│                     └─────────┴─────────┴─────────┘                 │
│                           30-60 seconds                             │
│                                                                     │
│  During this time:                                                 │
│  • Writes may fail or be lost                                      │
│  • Queries may timeout                                             │
│  • Clients may see errors                                          │
└─────────────────────────────────────────────────────────────────────┘

No-Promotion:
┌─────────────────────────────────────────────────────────────────────┐
│  T+0s     T+5s      T+10s     T+30s                                │
│  │        │         │         │                                     │
│  Crash    Detect    Flush     New pod                              │
│           failure   Standby   ready                                │
│           │         cache     │                                     │
│           │         │         │                                     │
│           └─────────┴─────────┘                                     │
│               10-30 seconds                                         │
│                                                                     │
│  During this time:                                                 │
│  • Traffic routes to other Primaries                               │
│  • Queries work (other Primaries)                                  │
│  • Only P1's partitions temporarily unavailable                    │
└─────────────────────────────────────────────────────────────────────┘
```

### When to Use Each

| Use Case | Recommendation |
|----------|----------------|
| **Kubernetes/Cloud Native** | No-Promotion ✓ |
| **Elastic Scaling** | No-Promotion ✓ |
| **Simple Operations** | No-Promotion ✓ |
| **Single-Node HA** | Promotion-Based |
| **On-Premise (fixed servers)** | Promotion-Based |
| **Regulatory (specific failover)** | Promotion-Based |

---

## Kubernetes Deployment Strategies

### Option 1: Single StatefulSet (Recommended)

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│                    mynadb-data StatefulSet                         │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                                                             │   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐          │   │
│  │  │ D0  │ │ D1  │ │ D2  │ │ D3  │ │ D4  │ │ D5  │          │   │
│  │  │     │ │     │ │     │ │     │ │     │ │     │          │   │
│  │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘          │   │
│  │     │       │       │       │       │       │              │   │
│  │     ▼       ▼       ▼       ▼       ▼       ▼              │   │
│  │   Meta assigns role at registration time:                  │   │
│  │   D0=Pri  D1=Pri  D2=Pri  D3=Stb  D4=Stb  D5=Stb          │   │
│  │                                                             │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Role Assignment Logic (in Meta):                                  │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ if standby_count < (primary_count * ratio + spare):        │   │
│  │     assign Standby                                          │   │
│  │ else:                                                       │   │
│  │     assign Primary                                          │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Manifest:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-data
  namespace: mynadb
spec:
  serviceName: mynadb-data
  replicas: 6
  selector:
    matchLabels:
      app: mynadb
      component: data
  template:
    metadata:
      labels:
        app: mynadb
        component: data
        # NO role label - Meta assigns dynamically
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: mynadb
                  component: data
              topologyKey: kubernetes.io/hostname
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchLabels:
                    app: mynadb
                    component: data
                topologyKey: topology.kubernetes.io/zone
      containers:
        - name: mynadb
          image: mynadb:latest
          args: ["data", "start"]  # No --role, Meta assigns
          env:
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: META_ENDPOINTS
              value: "mynadb-meta-0.mynadb-meta:9000,mynadb-meta-1.mynadb-meta:9000,mynadb-meta-2.mynadb-meta:9000"
          ports:
            - containerPort: 7000
              name: http
            - containerPort: 7001
              name: flight
          resources:
            requests:
              memory: "4Gi"
              cpu: "2"
            limits:
              memory: "8Gi"
              cpu: "4"
```

### Option 2: Two StatefulSets (Separate Primary/Standby)

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  mynadb-primary StatefulSet          mynadb-standby StatefulSet    │
│  ┌─────────────────────────┐         ┌─────────────────────────┐   │
│  │                         │         │                         │   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐│         │┌─────┐ ┌─────┐ ┌─────┐ │   │
│  │  │ P0  │ │ P1  │ │ P2  ││         ││ S0  │ │ S1  │ │ S2  │ │   │
│  │  │ Pri │ │ Pri │ │ Pri ││         ││ Stb │ │ Stb │ │ Stb │ │   │
│  │  └─────┘ └─────┘ └─────┘│         │└─────┘ └─────┘ └─────┘ │   │
│  │                         │         │                         │   │
│  │  args: ["--role=primary"]         │  args: ["--role=standby"]   │
│  │  HPA: 2-10 replicas     │         │  HPA: scales with Primary│  │
│  │  Resources: 8GB, 4 CPU  │         │  Resources: 4GB, 2 CPU  │   │
│  │                         │         │                         │   │
│  └─────────────────────────┘         └─────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Primary Manifest:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-primary
  namespace: mynadb
spec:
  serviceName: mynadb-primary
  replicas: 3
  selector:
    matchLabels:
      app: mynadb
      role: primary
  template:
    metadata:
      labels:
        app: mynadb
        role: primary
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: mynadb
                  role: primary
              topologyKey: kubernetes.io/hostname
            - labelSelector:
                matchLabels:
                  app: mynadb
                  role: standby
              topologyKey: kubernetes.io/hostname
      containers:
        - name: mynadb
          args: ["data", "start", "--role", "primary"]
          resources:
            requests:
              memory: "8Gi"
              cpu: "4"
```

**Standby Manifest:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-standby
  namespace: mynadb
spec:
  serviceName: mynadb-standby
  replicas: 3
  selector:
    matchLabels:
      app: mynadb
      role: standby
  template:
    metadata:
      labels:
        app: mynadb
        role: standby
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: mynadb
                  role: standby
              topologyKey: kubernetes.io/hostname
      containers:
        - name: mynadb
          args: ["data", "start", "--role", "standby"]
          resources:
            requests:
              memory: "4Gi"  # Less than Primary
              cpu: "2"
```

### StatefulSet Strategy Comparison

| Aspect | Single StatefulSet | Two StatefulSets |
|--------|-------------------|------------------|
| **Manifest Complexity** | 1 YAML file ✓ | 2 YAML files |
| **Role Assignment** | Dynamic by Meta ✓ | Fixed by deployment |
| **Scaling** | Scale total, Meta balances ✓ | Scale each separately |
| **Resource Sizing** | Same for all pods | Different per role ✓ |
| **Pod Replacement** | Meta reassigns role ✓ | Role preserved |
| **HPA Configuration** | Single HPA ✓ | Two HPAs (coordinated) |
| **Operational Simplicity** | Higher ✓ | Lower |
| **Resource Efficiency** | Lower (Standby over-provisioned) | Higher ✓ |
| **Flexibility** | Higher ✓ | Lower |

### Recommendation

**Single StatefulSet** is recommended for most cases because:

1. **Simpler Operations**: One deployment to manage
2. **Dynamic Balancing**: Meta can adjust Primary/Standby ratio without redeployment
3. **Flexible Scaling**: Just change replica count, Meta handles role distribution
4. **Less Coordination**: No need to sync two HPAs

**Two StatefulSets** may be better when:

1. **Strict Resource Requirements**: Primaries need significantly more resources
2. **Cost Optimization**: Don't want to over-provision Standbys
3. **Regulatory Requirements**: Must demonstrate separate Primary/Standby pools

---

## Failure Scenarios and Recovery

### Scenario Matrix

| Failure | Detection | Recovery Action | Data Impact |
|---------|-----------|-----------------|-------------|
| Primary Crash | Meta heartbeat timeout | FlushForRecovery → K8s replace | None (Standby has cache) |
| Standby Crash | Meta heartbeat timeout | Reassign from pool | None (Primary continues) |
| Primary Network Partition | Meta heartbeat timeout | Same as crash | None |
| Standby Network Partition | Replication timeout | Reassign Standby | None |
| Meta Leader Crash | Raft election | New leader | None (Raft consensus) |
| S3 Unavailable | Flush failure | Retry with backoff | Cache grows until S3 recovers |
| Both Primary+Standby Crash | Meta detects both | Data loss for unflushed window | **Data Loss** |

### Detailed Recovery: Primary Crash

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PRIMARY CRASH RECOVERY                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Step 1: Detection (T+0 to T+5s)                                   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Meta: heartbeat_timeout = 5s                                │   │
│  │ Meta: P1 last_heartbeat = T-6s                              │   │
│  │ Meta: P1 status = DEAD                                      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Step 2: FlushForRecovery (T+5s to T+15s)                          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Meta → S1: FlushForRecovery(primary_id="P1")                │   │
│  │ S1: flush_cache_to_s3(P1's data)                            │   │
│  │ S1: clear_cache()                                           │   │
│  │ S1 → Meta: FlushForRecoveryResponse(success=true)           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Step 3: Cleanup (T+15s)                                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Meta: remove P1 from primary_list                           │   │
│  │ Meta: S1 status = IDLE (available for reassignment)         │   │
│  │ Meta: update partition ownership                            │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Step 4: K8s Replacement (T+15s to T+45s)                          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ K8s: detects pod P1 terminated                              │   │
│  │ K8s: schedules new pod P1' (same name, new instance)        │   │
│  │ K8s: pod P1' starts                                         │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Step 5: Re-registration (T+45s to T+50s)                          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ P1' → Meta: RegisterNode(id="P1")                           │   │
│  │ Meta: assign role = Primary (based on current ratio)        │   │
│  │ Meta: pair P1' with S1 (or another Standby)                 │   │
│  │ P1': load metadata from S3                                  │   │
│  │ P1': start serving traffic                                  │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Total Recovery Time: ~50 seconds                                  │
│  Data Loss: None (Standby flushed cache)                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Detailed Recovery: Network Partition

```
┌─────────────────────────────────────────────────────────────────────┐
│                     NETWORK PARTITION HANDLING                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Scenario: P1 cannot reach Meta, but P1 is still running           │
│                                                                     │
│  Traditional Promotion-Based (DANGEROUS):                          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Meta: "P1 is dead" → Promote S1                             │   │
│  │ P1: still running, still accepting writes                   │   │
│  │ Result: TWO PRIMARIES (split-brain) ✗                       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  No-Promotion (SAFE):                                              │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Meta: "P1 is dead" → Send FlushForRecovery to S1            │   │
│  │ P1: still running, but replication to S1 fails              │   │
│  │ P1: continues accepting writes (to local cache only)        │   │
│  │                                                             │   │
│  │ On partition heal:                                          │   │
│  │ P1 → Meta: heartbeat resumes                                │   │
│  │ Meta: P1 is alive, re-pair with Standby                     │   │
│  │ Result: No split-brain, P1's unflushed writes preserved ✓   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Why No Split-Brain:                                               │
│  • S1 NEVER becomes Primary                                        │
│  • S1 only flushes cache (doesn't serve traffic)                  │
│  • P1 is still the only Primary for its partitions                │
│  • Worst case: S1 flushes data that P1 will also flush later      │
│    (duplicate data in S3, handled by compaction)                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Meta Configuration

```toml
[cluster]
enabled = true
mode = "cold_standby"

[cluster.cold_standby]
# No-Promotion Architecture Settings
promotion_enabled = false        # Disable promotion (No-Promotion mode)

# Standby Pool Configuration
standby_ratio = 1.0              # 1 Standby per Primary
spare_standbys = 1               # Extra Standbys in pool

# Recovery Settings
flush_for_recovery_timeout_secs = 30
heartbeat_interval_secs = 2
heartbeat_timeout_secs = 6

# Replication Settings
replication_mode = "async"       # "async" or "sync"
replication_ack_timeout_ms = 5000
```

### Data Node Configuration

```toml
[node]
# Role is assigned by Meta at registration
# No need to specify role in config

[replication]
enabled = true
batch_interval_ms = 100
max_batch_size_mb = 16

[cache]
max_size_mb = 4096
flush_interval_secs = 60         # Primary flushes every 60s

[flush_checkpoint]
enabled = true                   # Enable FlushCheckpoint protocol
broadcast_timeout_ms = 5000
```

---

## Operational Considerations

### Monitoring

Key metrics to monitor:

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `mynadb_primary_count` | Number of active Primaries | < min_primaries |
| `mynadb_standby_count` | Number of active Standbys | < primary_count * ratio |
| `mynadb_standby_pool_available` | Idle Standbys in pool | < spare_standbys |
| `mynadb_replication_lag_ms` | Primary→Standby replication lag | > 1000ms |
| `mynadb_cache_size_bytes` | Cache size per node | > 80% of max |
| `mynadb_flush_checkpoint_success_rate` | Checkpoint broadcast success | < 100% |
| `mynadb_recovery_flush_duration_ms` | Time to flush on recovery | > 30000ms |

### Troubleshooting

**Problem: Standby not receiving replication**
```bash
# Check replication stream status
curl http://primary:7000/debug/replication

# Check Standby connection
curl http://standby:7000/debug/replication/receiver

# Verify network connectivity
kubectl exec primary-0 -- nc -zv standby-0 7001
```

**Problem: FlushForRecovery timeout**
```bash
# Check Standby logs
kubectl logs standby-0 | grep FlushForRecovery

# Check S3 connectivity from Standby
kubectl exec standby-0 -- mc ls myminio/mynadb-bucket

# Manual cache flush
curl -X POST http://standby:7000/flush
```

**Problem: Role assignment imbalanced**
```bash
# Check current role distribution
curl http://meta:9000/cluster/status | jq '.nodes | group_by(.role)'

# Trigger rebalance (if supported)
curl -X POST http://meta:9000/cluster/rebalance
```

### Scaling Guidelines

```
Recommended Ratios:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Write-Heavy Workload (high data loss risk):                       │
│  • standby_ratio = 1.0 (1:1)                                       │
│  • spare_standbys = 2                                              │
│  • flush_interval = 60s                                            │
│                                                                     │
│  Read-Heavy Workload (lower data loss risk):                       │
│  • standby_ratio = 0.5 (1:2)                                       │
│  • spare_standbys = 1                                              │
│  • flush_interval = 300s                                           │
│                                                                     │
│  Cost-Optimized (acceptable data loss):                            │
│  • standby_ratio = 0.33 (1:3)                                      │
│  • spare_standbys = 0                                              │
│  • flush_interval = 600s                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Summary

The No-Promotion Standby Architecture provides:

1. **Simplicity**: No complex state machine, no split-brain risk
2. **Reliability**: Predictable failure recovery
3. **Kubernetes-Native**: Leverages K8s pod lifecycle management
4. **Data Protection**: Standby cache prevents data loss during flush intervals
5. **Operational Ease**: Single StatefulSet, dynamic role assignment

This design is ideal for cloud-native deployments where elastic scaling and operational simplicity are priorities over traditional HA promotion semantics.

---

## Related Documentation

- [Architecture: Cluster Mode](architecture-cluster.md)
- [Architecture: Cold Standby](architecture-cold-standby.md)
- [Architecture: Flush Checkpoint Protocol](architecture-flush-checkpoint.md)
- [Architecture: Scale-In](architecture-scale-in.md)
