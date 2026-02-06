 Here is Claude's plan:
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
 Plan: Cold Standby Architecture with Dedicated Standby Nodes

 Problem Statement

 The current Kafka ISR-like design causes avalanche risk:

 CURRENT PROBLEM:
 ┌─────────────────────────────────────────────────────────────────────┐
 │  data-1: Leader(partition-A) + Standby(partition-B)                 │
 │  data-2: Leader(partition-B) + Standby(partition-A)                 │
 │                                                                     │
 │  When data-1 crashes:                                               │
 │    → data-2 becomes Leader(partition-A) + Leader(partition-B)       │
 │    → data-2 now handles 2x write traffic + 2x query traffic         │
 │    → POTENTIAL AVALANCHE FAILURE!                                   │
 └─────────────────────────────────────────────────────────────────────┘

 Why this is dangerous for MynaDB:
 - Kafka: Consumers do light sequential reads (log tailing)
 - MynaDB: Heavy SQL queries (scans, aggregations, joins)
 - Doubling query load can easily overwhelm a node

 Solution: Dedicated Standby Nodes (Cold Standby)

 NEW ARCHITECTURE:
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Single Data Cluster (StatefulSet: mynadb-data)                     │
 │  ┌───────────────────────────────────────────────────────────────┐  │
 │  │  data-0 (Primary) ──┐                                         │  │
 │  │  data-1 (Primary) ──┼──> data-3 (Standby) - Cold, no queries  │  │
 │  │  data-2 (Primary) ──┘    Replicates from data-0,1,2           │  │
 │  │                                                               │  │
 │  │  Ratio: 3:1 (3 Primaries : 1 Standby)                        │  │
 │  └───────────────────────────────────────────────────────────────┘  │
 │                                                                     │
 │  On data-1 failure:                                                 │
 │    1. data-3 promoted to Primary (takes over data-1's partitions)   │
 │    2. data-3 already has in-memory data (warm cache)                │
 │    3. No other Primary sees increased load!                         │
 │    4. System auto-provisions new Standby (if K8s autoscale enabled) │
 └─────────────────────────────────────────────────────────────────────┘

 Key Properties:
 1. Standby nodes are COLD - Never serve queries, only receive replication
 2. On failover, Standby becomes Primary - Takes over failed node's role completely
 3. No load increase on other Primaries - Avalanche risk eliminated
 4. Global ratio config - Easy to understand (e.g., primary_standby_ratio = 2)

 ---
 Implementation Plan

 Phase 1: Add DataNodeMode Enum and Config

 Step 1.1: Add DataNodeMode to Types

 File: src/meta/types.rs

 /// Data node operational mode
 ///
 /// Primary nodes serve writes and queries actively.
 /// Standby nodes only receive replicated data and take over on Primary failure.
 #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
 pub enum DataNodeMode {
     /// Actively serves writes and queries, assigned as partition leaders
     Primary = 0,
     /// Cold standby - only receives replication, promoted on Primary failure
     Standby = 1,
 }

 impl Default for DataNodeMode {
     fn default() -> Self {
         DataNodeMode::Primary
     }
 }

 Step 1.2: Add Mode to NodeInfo

 File: src/meta/types.rs

 pub struct NodeInfo {
     // ... existing fields ...

     /// Data node mode (only relevant for NodeRole::Data)
     /// Primary: serves traffic, Standby: cold backup only
     pub mode: Option<DataNodeMode>,
 }

 Step 1.3: Add Cluster Ratio Config

 File: src/meta/config.rs

 [cluster.standby]
 enabled = true                    # Enable cold standby architecture
 primary_standby_ratio = 2         # 2 Primaries per 1 Standby (2:1)
 auto_assign_mode = true           # Meta auto-assigns mode on node join
 auto_promote = true               # Auto-promote Standby on Primary failure

 #[derive(Debug, Clone, Serialize, Deserialize)]
 pub struct StandbyConfig {
     pub enabled: bool,
     pub primary_standby_ratio: u32,     // e.g., 2 means 2:1
     pub auto_assign_mode: bool,
     pub auto_promote: bool,
 }

 Step 1.4: Update Proto Definition

 File: proto/meta.proto

 enum DataNodeMode {
     DATA_NODE_MODE_PRIMARY = 0;
     DATA_NODE_MODE_STANDBY = 1;
 }

 message NodeInfo {
     // ... existing fields ...
     DataNodeMode mode = 10;  // Data node mode
 }

 // New RPC for promoting Standby to Primary
 rpc PromoteStandbyNode(PromoteStandbyNodeRequest) returns (PromoteStandbyNodeResponse);

 message PromoteStandbyNodeRequest {
     string standby_node_id = 1;      // Standby to promote
     string failed_primary_id = 2;    // Primary that failed (optional)
 }

 message PromoteStandbyNodeResponse {
     bool success = 1;
     string error = 2;
 }

 ---
 Phase 2: Update Meta State Machine

 Step 2.1: Auto-Assign Mode on Node Registration

 File: src/meta/raft/state_machine.rs

 fn register_node(&self, mut info: NodeInfo, lease_ttl_ms: u64) -> MetaResponse {
     // If this is a Data node and auto_assign_mode is enabled
     if info.role == NodeRole::Data && self.config.standby.auto_assign_mode {
         // Count current Primary and Standby nodes
         let (primary_count, standby_count) = self.count_data_nodes_by_mode();

         // Calculate target ratio
         let target_standby = (primary_count + 1) / self.config.standby.primary_standby_ratio;

         // Assign mode based on ratio
         if standby_count < target_standby {
             info.mode = Some(DataNodeMode::Standby);
         } else {
             info.mode = Some(DataNodeMode::Primary);
         }
     }

     // ... rest of registration logic ...
 }

 fn count_data_nodes_by_mode(&self) -> (usize, usize) {
     let nodes = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active));
     let primary = nodes.iter().filter(|n| n.mode == Some(DataNodeMode::Primary)).count();
     let standby = nodes.iter().filter(|n| n.mode == Some(DataNodeMode::Standby)).count();
     (primary, standby)
 }

 Step 2.2: Promote Standby on Primary Failure

 File: src/meta/raft/state_machine.rs

 fn on_node_leave(&self, node_id: &str) -> MetaResponse {
     let node = self.get_node(node_id);

     // If a Primary node left, promote a Standby
     if node.map(|n| n.mode) == Some(Some(DataNodeMode::Primary)) {
         if let Some(standby) = self.select_standby_to_promote(node_id) {
             self.promote_standby_to_primary(&standby.node_id, node_id);
         }
     }

     // ... existing failover logic for partitions ...
 }

 fn select_standby_to_promote(&self, failed_primary_id: &str) -> Option<NodeInfo> {
     // Find Standby that was backing this Primary (has replicated data)
     // Prefer zone-aware selection
     let standbys = self.list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
         .into_iter()
         .filter(|n| n.mode == Some(DataNodeMode::Standby))
         .collect::<Vec<_>>();

     // Select least-loaded Standby in different zone if possible
     // ...
 }

 fn promote_standby_to_primary(&self, standby_id: &str, failed_primary_id: &str) {
     // 1. Change Standby's mode to Primary
     if let Some(mut node) = self.get_node(standby_id) {
         node.mode = Some(DataNodeMode::Primary);
         self.update_node(&node);
     }

     // 2. Transfer all partition leadership from failed Primary to promoted node
     let partitions = self.get_replicas_by_leader(failed_primary_id);
     for partition in partitions {
         self.assign_new_leader(&partition.partition_key, standby_id);
     }

     // 3. Publish StandbyPromoted event
     self.publish_event("nodes", EventType::StandbyPromoted, ...);
 }

 ---
 Phase 3: Update ReplicaAssigner

 Step 3.1: Only Assign Leaders from Primary Nodes

 File: src/meta/replica_assigner.rs

 pub fn assign_new_partition(&self, partition_key: &str, nodes: &[NodeInfo], ...) -> Option<PartitionReplica> {
     // Filter to PRIMARY nodes only for leader selection
     let primary_nodes: Vec<&NodeInfo> = nodes
         .iter()
         .filter(|n| n.role == NodeRole::Data
                  && n.state == NodeState::Active
                  && n.mode == Some(DataNodeMode::Primary))
         .collect();

     // Filter to STANDBY nodes for standby selection
     let standby_nodes: Vec<&NodeInfo> = nodes
         .iter()
         .filter(|n| n.role == NodeRole::Data
                  && n.state == NodeState::Active
                  && n.mode == Some(DataNodeMode::Standby))
         .collect();

     if primary_nodes.is_empty() {
         return None;  // No Primary nodes available
     }

     // Select leader from Primary nodes
     let leader = self.select_least_loaded(&primary_nodes, partition_counts)?;

     // Select standbys from Standby nodes (not from Primary nodes!)
     let standbys = self.select_standbys_from_pool(
         &leader.node_id,
         leader,
         &standby_nodes,  // Use dedicated Standby nodes
         partition_counts,
         (rf - 1) as usize,
     );

     Some(PartitionReplica::new(...))
 }

 ---
 Phase 4: Update Router and Data Node

 Step 4.1: Router Only Routes to Primary Nodes

 File: src/router/routing/shard_cache.rs

 pub fn get_active_data_nodes(&self) -> Vec<DataNodeInfo> {
     // Only return Primary nodes for query routing
     self.data_nodes
         .read()
         .values()
         .filter(|n| n.state == NodeState::Active
                  && n.mode == Some(DataNodeMode::Primary))
         .cloned()
         .collect()
 }

 Step 4.2: Data Node Behavior Based on Mode

 File: src/node/data.rs

 // On startup, check assigned mode from Meta
 if let Some(ref client) = meta_client {
     let my_info = client.get_node(&data_config.node_id).await?;

     match my_info.mode {
         Some(DataNodeMode::Primary) => {
             // Normal operation - serve writes and queries
             info!("Starting as PRIMARY data node");
         }
         Some(DataNodeMode::Standby) => {
             // Standby mode - only receive replication, no query serving
             info!("Starting as STANDBY data node (cold backup)");
             // Disable query endpoint or reject queries
         }
     }
 }

 Step 4.3: Standby Node Handles Promotion Event

 File: src/node/data.rs

 // Subscribe to StandbyPromoted events
 if event.event_type == EventType::StandbyPromoted {
     let payload = serde_json::from_slice(&event.payload)?;
     if payload["promoted_node_id"] == node_id {
         info!("I have been PROMOTED from Standby to Primary!");

         // Switch to Primary mode
         // - Start serving queries
         // - Start flushing to Parquet
         // - Take over partition leadership
     }
 }

 ---
 Phase 5: Kubernetes Integration

 Step 5.1: StatefulSet with Auto-Scaling

 apiVersion: apps/v1
 kind: StatefulSet
 metadata:
   name: mynadb-data
 spec:
   replicas: 6  # Will auto-assign: 4 Primary, 2 Standby (ratio 2:1)
   # ...

 The Meta service automatically assigns modes based on primary_standby_ratio when nodes join.

 ---
 Files to Modify

 | File                              | Changes                                  |
 |-----------------------------------|------------------------------------------|
 | src/meta/types.rs                 | Add DataNodeMode enum, update NodeInfo   |
 | src/meta/config.rs                | Add StandbyConfig                        |
 | proto/meta.proto                  | Add DataNodeMode, PromoteStandbyNode RPC |
 | src/meta/raft/state_machine.rs    | Auto-assign mode, promote on failure     |
 | src/meta/replica_assigner.rs      | Separate Primary/Standby node pools      |
 | src/router/routing/shard_cache.rs | Filter to Primary nodes only             |
 | src/node/data.rs                  | Mode-aware startup, handle promotion     |
 | src/data/server/flight.rs         | Reject queries in Standby mode           |

 ---
 Data Flow Summary

 NODE JOIN:
   New Data node → Meta.register_node()
                → Meta counts (Primary, Standby)
                → If standby_count < primary_count / ratio:
                    Assign mode = Standby
                  Else:
                    Assign mode = Primary
                → Node starts in assigned mode

 WRITE (Normal):
   Client → Router → Route to Primary node (leader)
                   → Primary writes to memtable
                   → Primary replicates to Standby nodes
                   → ACK

 QUERY (Normal):
   Client → Router → Route to Primary node
                   → Primary executes query
                   → Response
   (Standby nodes NEVER serve queries)

 PRIMARY FAILURE:
   Meta detects Primary-1 lease expired
     → Meta selects Standby-1 to promote
     → Meta changes Standby-1.mode = Primary
     → Meta transfers partition leadership to Standby-1
     → Meta publishes StandbyPromoted event
     → Standby-1 receives event, switches to Primary mode
     → Standby-1 starts serving queries + flushing
     → Router updates cache, routes to Standby-1
     → (Optional) K8s autoscaler adds new pod → assigned as Standby

 ---
 Key Differences from ISR Model

 | Aspect         | ISR Model (Current)           | Cold Standby (New)                      |
 |----------------|-------------------------------|-----------------------------------------|
 | Node roles     | Any node = leader + standby   | Primary OR Standby (not both)           |
 | Query serving  | All nodes serve queries       | Only Primary serves queries             |
 | On failover    | Existing node gets extra load | Standby becomes Primary (no extra load) |
 | Avalanche risk | HIGH - doubled load           | NONE - dedicated takeover               |
 | Resource usage | All nodes actively used       | Standbys idle until needed              |

 ---
 Testing Plan

 1. Unit Test: Auto-mode assignment
   - 6 nodes join with ratio=2 → 4 Primary, 2 Standby
 2. Unit Test: Leader assignment
   - New partition → Leader from Primary pool only
 3. Integration Test: Standby cold
   - Query to Standby → Rejected
   - Write replication → Standby receives data
 4. Integration Test: Failover
   - Kill Primary → Standby promoted → No load increase on other Primaries
 5. Integration Test: K8s scaling
   - Scale up → New node assigned correct mode
