//! Automatic replica assignment for warm standby replication
//!
//! This module provides intelligent assignment of leaders and standbys
//! for partitions, with support for:
//! - Load balancing (least-loaded node selection)
//! - Zone-aware placement (standbys in different zones than leader)
//! - Automatic rebalancing when nodes join/leave

use std::collections::HashMap;

use crate::meta::config::ReplicationAssignmentConfig;
use crate::meta::types::{DataNodeMode, NodeInfo, NodeRole, NodeState, PartitionReplica};

/// Handles automatic replica assignment for partitions
pub struct ReplicaAssigner {
    config: ReplicationAssignmentConfig,
}

impl ReplicaAssigner {
    pub fn new(config: ReplicationAssignmentConfig) -> Self {
        Self { config }
    }

    /// Assign leader and standbys for a new partition
    ///
    /// With Cold Standby Architecture enabled:
    /// 1. Leader: Selected ONLY from PRIMARY Data nodes (least-loaded)
    /// 2. Standbys: Selected ONLY from STANDBY Data nodes (zone-aware)
    ///
    /// This prevents avalanche failures by ensuring standbys don't serve traffic.
    /// When Cold Standby is disabled, falls back to ISR-like behavior.
    ///
    /// Returns None if no suitable nodes are available.
    pub fn assign_new_partition(
        &self,
        partition_key: &str,
        nodes: &[NodeInfo],
        partition_counts: &HashMap<String, usize>,
    ) -> Option<PartitionReplica> {
        // Filter to active Data nodes only
        let active_data_nodes: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Data && n.state == NodeState::Active)
            .collect();

        if active_data_nodes.is_empty() {
            return None;
        }

        // Check if Cold Standby Architecture is in use
        // (detected by presence of Standby mode nodes)
        let has_standby_nodes = active_data_nodes
            .iter()
            .any(|n| n.mode == Some(DataNodeMode::Standby));

        if has_standby_nodes {
            // Cold Standby Mode: Use separate Primary/Standby pools
            self.assign_with_cold_standby(partition_key, &active_data_nodes, partition_counts)
        } else {
            // Legacy ISR Mode: Any node can be leader or standby
            self.assign_with_isr_mode(partition_key, &active_data_nodes, partition_counts)
        }
    }

    /// Assign using Cold Standby Architecture (separate Primary/Standby pools)
    ///
    /// - Leaders are selected from PRIMARY nodes only
    /// - Standbys are selected from STANDBY nodes only
    fn assign_with_cold_standby(
        &self,
        partition_key: &str,
        nodes: &[&NodeInfo],
        partition_counts: &HashMap<String, usize>,
    ) -> Option<PartitionReplica> {
        // Separate Primary and Standby pools
        let primary_nodes: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Primary))
            .copied()
            .collect();

        let standby_nodes: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| n.mode == Some(DataNodeMode::Standby))
            .copied()
            .collect();

        if primary_nodes.is_empty() {
            return None; // No Primary nodes available
        }

        // Calculate effective replication factor based on available standbys
        // In Cold Standby, we can only replicate to as many standbys as we have
        let max_standbys = standby_nodes.len();
        let desired_standbys = (self.config.default_replication_factor as usize).saturating_sub(1);
        let actual_standbys = std::cmp::min(max_standbys, desired_standbys);
        let rf = (actual_standbys + 1) as u8;

        // Select leader from PRIMARY pool (least-loaded)
        let leader = self.select_least_loaded(&primary_nodes, partition_counts)?;

        // Select standbys from STANDBY pool
        let standbys = if actual_standbys > 0 {
            self.select_standbys_from_pool(
                &leader.node_id,
                leader,
                &standby_nodes,
                partition_counts,
                actual_standbys,
            )
        } else {
            vec![]
        };

        Some(PartitionReplica::new(
            partition_key.to_string(),
            leader.node_id.clone(),
            standbys,
            rf,
        ))
    }

    /// Assign using legacy ISR mode (any node can be leader or standby)
    fn assign_with_isr_mode(
        &self,
        partition_key: &str,
        nodes: &[&NodeInfo],
        partition_counts: &HashMap<String, usize>,
    ) -> Option<PartitionReplica> {
        // Calculate effective replication factor
        let rf = self
            .config
            .effective_replication_factor(nodes.len());

        // Select leader (least-loaded)
        let leader = self.select_least_loaded(nodes, partition_counts)?;

        // Select standbys from remaining nodes
        let standbys = if rf > 1 {
            self.select_standbys(
                &leader.node_id,
                leader,
                nodes,
                partition_counts,
                (rf - 1) as usize,
            )
        } else {
            vec![]
        };

        Some(PartitionReplica::new(
            partition_key.to_string(),
            leader.node_id.clone(),
            standbys,
            rf,
        ))
    }

    /// Select standbys from a dedicated standby pool (Cold Standby mode)
    fn select_standbys_from_pool(
        &self,
        leader_id: &str,
        leader: &NodeInfo,
        standby_pool: &[&NodeInfo],
        partition_counts: &HashMap<String, usize>,
        count: usize,
    ) -> Vec<String> {
        if standby_pool.is_empty() || count == 0 {
            return vec![];
        }

        // Score candidates: lower is better
        let mut scored: Vec<(&NodeInfo, i64)> = standby_pool
            .iter()
            .map(|node| {
                let mut score: i64 = 0;

                // Zone awareness: prefer different zone
                if self.config.zone_aware {
                    if !leader.is_different_zone(node) {
                        score += 1000; // Same zone penalty
                    }
                    if !leader.is_different_rack(node) {
                        score += 500; // Same rack penalty
                    }
                }

                // Load balancing: prefer nodes with fewer partitions
                let load = *partition_counts.get(&node.node_id).unwrap_or(&0) as i64;
                score += load * 10;

                (*node, score)
            })
            .collect();

        // Sort by score (ascending)
        scored.sort_by_key(|(_, score)| *score);

        // Take top N
        scored
            .into_iter()
            .take(count)
            .map(|(node, _)| node.node_id.clone())
            .collect()
    }

    /// Select standbys for a partition
    ///
    /// Prioritizes:
    /// 1. Nodes in different zones than leader
    /// 2. Nodes in different racks
    /// 3. Least-loaded nodes
    fn select_standbys(
        &self,
        leader_id: &str,
        leader: &NodeInfo,
        nodes: &[&NodeInfo],
        partition_counts: &HashMap<String, usize>,
        count: usize,
    ) -> Vec<String> {
        // Filter out leader
        let candidates: Vec<&&NodeInfo> = nodes
            .iter()
            .filter(|n| n.node_id != leader_id)
            .collect();

        if candidates.is_empty() || count == 0 {
            return vec![];
        }

        // Score candidates: lower is better
        let mut scored: Vec<(&&NodeInfo, i64)> = candidates
            .iter()
            .map(|node| {
                let mut score: i64 = 0;

                // Zone awareness: prefer different zone
                if self.config.zone_aware {
                    if !leader.is_different_zone(node) {
                        score += 1000; // Same zone penalty
                    }
                    if !leader.is_different_rack(node) {
                        score += 500; // Same rack penalty
                    }
                }

                // Load balancing: prefer nodes with fewer partitions
                let load = *partition_counts.get(&node.node_id).unwrap_or(&0) as i64;
                score += load * 10;

                (*node, score)
            })
            .collect();

        // Sort by score (ascending)
        scored.sort_by_key(|(_, score)| *score);

        // Take top N
        scored
            .into_iter()
            .take(count)
            .map(|(node, _)| node.node_id.clone())
            .collect()
    }

    /// Select the least-loaded node
    fn select_least_loaded<'a>(
        &self,
        nodes: &[&'a NodeInfo],
        partition_counts: &HashMap<String, usize>,
    ) -> Option<&'a NodeInfo> {
        match self.config.load_balance_strategy.as_str() {
            "round_robin" => {
                // For round_robin, we use partition count but prefer deterministic ordering
                nodes.iter().min_by_key(|n| {
                    (
                        *partition_counts.get(&n.node_id).unwrap_or(&0),
                        n.node_id.clone(),
                    )
                }).copied()
            }
            "random" => {
                // For random, use a simple hash-based selection (deterministic for same input)
                if nodes.is_empty() {
                    None
                } else {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    nodes.len().hash(&mut hasher);
                    let idx = hasher.finish() as usize % nodes.len();
                    Some(nodes[idx])
                }
            }
            _ => {
                // "least_loaded" (default)
                nodes.iter().min_by_key(|n| {
                    partition_counts.get(&n.node_id).unwrap_or(&0)
                }).copied()
            }
        }
    }

    /// Reassign partitions when a node leaves the cluster
    ///
    /// For each partition where the departed node was:
    /// - Leader: Promote first standby to leader
    /// - Standby: Find replacement standby (if possible)
    ///
    /// Returns list of updated replicas.
    pub fn reassign_on_node_leave(
        &self,
        departed_node_id: &str,
        replicas: &[PartitionReplica],
        nodes: &[NodeInfo],
        partition_counts: &HashMap<String, usize>,
    ) -> Vec<PartitionReplica> {
        let active_data_nodes: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| {
                n.role == NodeRole::Data
                    && n.state == NodeState::Active
                    && n.node_id != departed_node_id
            })
            .collect();

        let mut updates = Vec::new();

        for replica in replicas {
            let mut updated = replica.clone();
            let mut needs_update = false;

            // Case 1: Departed node was leader
            if replica.leader_node_id == departed_node_id {
                if let Some(new_leader) = replica.standby_node_ids.first().cloned() {
                    updated.leader_node_id = new_leader;
                    updated.standby_node_ids.remove(0);
                    needs_update = true;
                }
                // Remove departed from standbys if somehow present
                updated.standby_node_ids.retain(|id| id != departed_node_id);
            }

            // Case 2: Departed node was standby
            if updated.standby_node_ids.contains(&departed_node_id.to_string()) {
                updated.standby_node_ids.retain(|id| id != departed_node_id);
                needs_update = true;
            }

            // Try to find replacement standby if we lost one
            let target_standbys = (replica.replication_factor as usize).saturating_sub(1);
            if updated.standby_node_ids.len() < target_standbys {
                let leader_node = nodes.iter().find(|n| n.node_id == updated.leader_node_id);

                // First pass: collect candidates from different zones
                let mut candidates: Vec<String> = Vec::new();
                {
                    let existing: std::collections::HashSet<&String> = std::iter::once(&updated.leader_node_id)
                        .chain(updated.standby_node_ids.iter())
                        .collect();

                    for node in &active_data_nodes {
                        if existing.contains(&node.node_id) {
                            continue;
                        }

                        // Prefer different zone if zone-aware
                        if self.config.zone_aware {
                            if let Some(leader) = leader_node {
                                if !leader.is_different_zone(node) {
                                    continue; // Skip same-zone nodes first pass
                                }
                            }
                        }

                        candidates.push(node.node_id.clone());
                        if updated.standby_node_ids.len() + candidates.len() >= target_standbys {
                            break;
                        }
                    }
                }

                // Add candidates from first pass
                for candidate in candidates {
                    updated.standby_node_ids.push(candidate);
                    needs_update = true;
                }

                // Second pass: accept same-zone nodes if no other choice
                if updated.standby_node_ids.len() < target_standbys {
                    let mut more_candidates: Vec<String> = Vec::new();
                    {
                        let existing: std::collections::HashSet<&String> = std::iter::once(&updated.leader_node_id)
                            .chain(updated.standby_node_ids.iter())
                            .collect();

                        for node in &active_data_nodes {
                            if existing.contains(&node.node_id) {
                                continue;
                            }

                            more_candidates.push(node.node_id.clone());
                            if updated.standby_node_ids.len() + more_candidates.len() >= target_standbys {
                                break;
                            }
                        }
                    }

                    for candidate in more_candidates {
                        updated.standby_node_ids.push(candidate);
                        needs_update = true;
                    }
                }
            }

            if needs_update {
                updates.push(updated);
            }
        }

        updates
    }

    /// Rebalance when a new node joins the cluster
    ///
    /// For partitions that are missing standbys, try to add the new node
    /// as a standby (preferring different zones).
    ///
    /// Returns list of updated replicas.
    pub fn rebalance_on_node_join(
        &self,
        new_node: &NodeInfo,
        replicas: &[PartitionReplica],
        nodes: &[NodeInfo],
    ) -> Vec<PartitionReplica> {
        if new_node.role != NodeRole::Data || new_node.state != NodeState::Active {
            return vec![];
        }

        let mut updates = Vec::new();

        for replica in replicas {
            let target_standbys = (replica.replication_factor as usize).saturating_sub(1);

            // Skip if already has enough standbys
            if replica.standby_node_ids.len() >= target_standbys {
                continue;
            }

            // Skip if new node is already in this replica
            if replica.leader_node_id == new_node.node_id
                || replica.standby_node_ids.contains(&new_node.node_id)
            {
                continue;
            }

            // Check zone awareness
            if self.config.zone_aware {
                if let Some(leader) = nodes.iter().find(|n| n.node_id == replica.leader_node_id) {
                    // Only add if different zone (or if we really need standbys)
                    if !leader.is_different_zone(new_node) {
                        // Check if all existing standbys are same-zone too
                        let all_same_zone = replica.standby_node_ids.iter().all(|sid| {
                            nodes.iter()
                                .find(|n| n.node_id == *sid)
                                .map(|n| !leader.is_different_zone(n))
                                .unwrap_or(true)
                        });

                        // Skip if we already have same-zone standbys and this is another
                        if !all_same_zone {
                            continue;
                        }
                    }
                }
            }

            let mut updated = replica.clone();
            updated.standby_node_ids.push(new_node.node_id.clone());
            updates.push(updated);
        }

        updates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_node(id: &str, zone: Option<&str>) -> NodeInfo {
        let mut node = NodeInfo::new(id.to_string(), NodeRole::Data, format!("{}:7200", id));
        node.state = NodeState::Active;
        // Default to Primary mode for backward compatibility
        node.mode = Some(DataNodeMode::Primary);
        if let Some(z) = zone {
            node.labels.insert("topology.kubernetes.io/zone".to_string(), z.to_string());
        }
        node
    }

    fn make_primary_node(id: &str, zone: Option<&str>) -> NodeInfo {
        let mut node = make_node(id, zone);
        node.mode = Some(DataNodeMode::Primary);
        node
    }

    fn make_standby_node(id: &str, zone: Option<&str>) -> NodeInfo {
        let mut node = make_node(id, zone);
        node.mode = Some(DataNodeMode::Standby);
        node
    }

    #[test]
    fn test_assign_new_partition_basic() {
        let config = ReplicationAssignmentConfig::default();
        let assigner = ReplicaAssigner::new(config);

        let nodes = vec![
            make_node("data-1", Some("zone-a")),
            make_node("data-2", Some("zone-b")),
            make_node("data-3", Some("zone-c")),
        ];

        let counts: HashMap<String, usize> = HashMap::new();

        let replica = assigner.assign_new_partition("mydb/default/cpu/date=2024-01-15", &nodes, &counts);
        assert!(replica.is_some());

        let replica = replica.unwrap();
        assert!(!replica.leader_node_id.is_empty());
        assert_eq!(replica.standby_node_ids.len(), 1); // RF=2 means 1 standby
        assert_eq!(replica.replication_factor, 2);
    }

    #[test]
    fn test_assign_zone_aware() {
        let config = ReplicationAssignmentConfig {
            zone_aware: true,
            default_replication_factor: 2,
            ..Default::default()
        };
        let assigner = ReplicaAssigner::new(config);

        let nodes = vec![
            make_node("data-1", Some("zone-a")),
            make_node("data-2", Some("zone-b")),
        ];

        // Make data-1 the leader by giving it fewer partitions
        let mut counts: HashMap<String, usize> = HashMap::new();
        counts.insert("data-2".to_string(), 10);

        let replica = assigner.assign_new_partition("mydb/default/cpu/date=2024-01-15", &nodes, &counts);
        assert!(replica.is_some());

        let replica = replica.unwrap();
        assert_eq!(replica.leader_node_id, "data-1");
        assert_eq!(replica.standby_node_ids, vec!["data-2"]);
    }

    #[test]
    fn test_single_node_no_standby() {
        let config = ReplicationAssignmentConfig::default();
        let assigner = ReplicaAssigner::new(config);

        let nodes = vec![make_node("data-1", Some("zone-a"))];
        let counts: HashMap<String, usize> = HashMap::new();

        let replica = assigner.assign_new_partition("mydb/default/cpu/date=2024-01-15", &nodes, &counts);
        assert!(replica.is_some());

        let replica = replica.unwrap();
        assert_eq!(replica.leader_node_id, "data-1");
        assert!(replica.standby_node_ids.is_empty());
        assert_eq!(replica.replication_factor, 1);
    }

    #[test]
    fn test_reassign_on_leader_leave() {
        let config = ReplicationAssignmentConfig::default();
        let assigner = ReplicaAssigner::new(config);

        let nodes = vec![
            make_node("data-2", Some("zone-b")),
            make_node("data-3", Some("zone-c")),
        ];

        let replica = PartitionReplica::new(
            "mydb/default/cpu/date=2024-01-15".to_string(),
            "data-1".to_string(), // Leader is leaving
            vec!["data-2".to_string()],
            2,
        );

        let counts: HashMap<String, usize> = HashMap::new();
        let updates = assigner.reassign_on_node_leave("data-1", &[replica], &nodes, &counts);

        assert_eq!(updates.len(), 1);
        let updated = &updates[0];
        assert_eq!(updated.leader_node_id, "data-2"); // data-2 promoted
        assert!(updated.standby_node_ids.contains(&"data-3".to_string())); // data-3 added
    }

    #[test]
    fn test_rebalance_on_join() {
        let config = ReplicationAssignmentConfig {
            zone_aware: true,
            default_replication_factor: 2,
            ..Default::default()
        };
        let assigner = ReplicaAssigner::new(config);

        let nodes = vec![
            make_node("data-1", Some("zone-a")),
            make_node("data-2", Some("zone-b")),
        ];

        // Partition missing standby
        let replica = PartitionReplica::new(
            "mydb/default/cpu/date=2024-01-15".to_string(),
            "data-1".to_string(),
            vec![], // No standbys
            2,
        );

        let new_node = make_node("data-2", Some("zone-b"));
        let updates = assigner.rebalance_on_node_join(&new_node, &[replica], &nodes);

        assert_eq!(updates.len(), 1);
        assert!(updates[0].standby_node_ids.contains(&"data-2".to_string()));
    }

    #[test]
    fn test_cold_standby_separate_pools() {
        // Test that with Cold Standby architecture:
        // - Leaders are selected from PRIMARY nodes only
        // - Standbys are selected from STANDBY nodes only
        let config = ReplicationAssignmentConfig {
            zone_aware: true,
            default_replication_factor: 2,
            ..Default::default()
        };
        let assigner = ReplicaAssigner::new(config);

        // Create 2 Primary nodes and 1 Standby node
        let nodes = vec![
            make_primary_node("primary-1", Some("zone-a")),
            make_primary_node("primary-2", Some("zone-b")),
            make_standby_node("standby-1", Some("zone-c")),
        ];

        let counts: HashMap<String, usize> = HashMap::new();

        let replica = assigner.assign_new_partition("mydb/default/cpu/date=2024-01-15", &nodes, &counts);
        assert!(replica.is_some());

        let replica = replica.unwrap();

        // Leader must be a Primary node
        assert!(
            replica.leader_node_id == "primary-1" || replica.leader_node_id == "primary-2",
            "Leader {} must be a Primary node",
            replica.leader_node_id
        );

        // Standby must be a Standby node
        assert_eq!(replica.standby_node_ids.len(), 1);
        assert_eq!(
            replica.standby_node_ids[0], "standby-1",
            "Standby must be a dedicated Standby node"
        );
    }

    #[test]
    fn test_cold_standby_no_standbys_available() {
        // Test behavior when no Standby nodes exist
        let config = ReplicationAssignmentConfig {
            default_replication_factor: 2,
            ..Default::default()
        };
        let assigner = ReplicaAssigner::new(config);

        // Create only Primary nodes (no Standbys)
        let nodes = vec![
            make_primary_node("primary-1", Some("zone-a")),
            make_primary_node("primary-2", Some("zone-b")),
        ];

        let counts: HashMap<String, usize> = HashMap::new();

        let replica = assigner.assign_new_partition("mydb/default/cpu/date=2024-01-15", &nodes, &counts);
        assert!(replica.is_some());

        let replica = replica.unwrap();

        // Without any Standby nodes, we fall back to ISR mode
        // where Primaries can be used as standbys
        assert!(!replica.leader_node_id.is_empty());
    }
}
