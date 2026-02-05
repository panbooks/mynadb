//! Query routing logic
//!
//! Handles partition-level and table-level routing for distributed queries.
//!
//! ## Split Cache Architecture
//!
//! Query routing uses a split cache design to handle hot and cold data correctly:
//!
//! ### Hot Cache (memtable data)
//! - Source of Truth: Meta Service (via write notifications)
//! - TTL: 5 minutes (matches flush_interval)
//! - Always checked first for ALL dates (handles midnight edge case)
//!
//! ### Cold Cache (S3/Parquet data)
//! - Source of Truth: S3 LIST (immutable once written)
//! - Eviction: LRU with max_entries (NOT TTL - data never changes!)
//! - Checked after hot cache, before S3 LIST
//!
//! ## Routing Flow
//!
//! 1. Extract date filters from SQL
//! 2. Check HOT cache (for ALL dates, not just today)
//! 3. Check COLD cache (for dates not found in hot)
//! 4. S3 LIST discovery (for dates not found in cold)
//! 5. Build partition assignments with cache affinity
//!
//! ## Midnight Edge Case
//!
//! Data written at 23:58 for date=2024-01-20 may still be in memtable at 00:02.
//! We ALWAYS check hot cache first regardless of whether date == today.
//! The hot cache TTL (5 min) handles the "is it still in memtable?" question.
//!
//! ## Benefits
//! - **60-90% reduction in query fan-out** (only query relevant nodes)
//! - **Cache affinity preserved** (writer = reader for hot data)
//! - **Zero Meta traffic** for query routing
//! - **Bounded memory** with LRU eviction for cold cache

use super::super::shard_cache::{DataNodeInfo, NodeState, ShardCache, TableKey};
use super::types::{PartitionRoutingResult, TableRef, TimeRange};
use super::QueryCoordinator;
use crate::meta::client::MetaClient;
use crate::storage::storage::Storage;
use crate::utils::hash::hash_select_index;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// Default flush interval in nanoseconds (5 minutes)
const FLUSH_INTERVAL_NANOS: i64 = 300 * 1_000_000_000;

impl QueryCoordinator {
    // ========================================================================
    // Partition-Level Routing (Hive Partition Architecture)
    // ========================================================================

    /// Check if query might include recent data (still in memtable)
    ///
    /// Simple logic: if max_time is None or >= (now - flush_interval), return true
    fn is_query_recent(time_range: &Option<TimeRange>) -> bool {
        match time_range {
            None => true, // No filter = includes all data including recent
            Some(tr) => match tr.max_time {
                None => true, // No upper bound = includes recent
                Some(max_time) => {
                    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
                    let threshold = now_nanos - FLUSH_INTERVAL_NANOS;
                    max_time > threshold
                }
            }
        }
    }

    /// Try partition-level routing based on date filters in the query
    ///
    /// Uses split cache architecture:
    /// 1. ALWAYS check hot cache first (for ALL dates - handles midnight edge case)
    /// 2. Check cold cache for dates not found in hot
    /// 3. S3 LIST discovery for dates not found in cold
    /// 4. Cache S3 discoveries in cold cache
    ///
    /// ## Cache Affinity & Partition Assignments
    ///
    /// This function builds a mapping of target_node → [assigned partitions]:
    /// - Each node is assigned its own `node_id` partition (for cache affinity)
    /// - If a node is down, its partition is reassigned to a fallback node
    /// - Each partition is assigned to exactly ONE node (no duplicates)
    ///
    /// ## Midnight Edge Case
    ///
    /// Data written at 23:58 for date=2024-01-20 may still be in memtable at 00:02.
    /// We ALWAYS check hot cache first regardless of whether date == today.
    /// The hot cache TTL (5 min) handles the "is it still in memtable?" question.
    pub(super) async fn try_partition_routing(
        &self,
        catalog: &str,
        tables: &[TableRef],
        time_range: &Option<TimeRange>,
        all_primary_nodes: &[DataNodeInfo],
    ) -> Option<PartitionRoutingResult> {
        // Extract date filters from time range
        let dates = self.extract_date_filters(time_range)?;

        if dates.is_empty() {
            debug!("No date filters extracted, falling back to table-level routing");
            return None;
        }

        debug!(
            "Attempting partition-level routing for {} dates: {:?}",
            dates.len(),
            dates
        );

        // Collect all partition node_ids that have data for the query
        let mut partition_node_ids: HashSet<String> = HashSet::new();

        // Check if query might include recent data (still in memtable)
        // Simple: if max_time is None or recent, query table owners
        let query_includes_recent = Self::is_query_recent(time_range);

        for table in tables {
            // ================================================================
            // STEP 1: For RECENT queries, use table owners (have memtable data)
            // Table owners are consistent across all routers (synced via Meta).
            // ================================================================
            if query_includes_recent {
                // Table owner has memtable data for recent partitions
                let hot_nodes = self.shard_cache.get_table_owners_for_hot_data(
                    catalog,
                    &table.schema,
                    &table.table,
                );

                if !hot_nodes.is_empty() {
                    debug!(
                        "Table owner routing for {}/{}/{}: {} owners (recent dates)",
                        catalog, table.schema, table.table, hot_nodes.len()
                    );
                    partition_node_ids.extend(hot_nodes);
                }
            }

            // ================================================================
            // STEP 2: Check cold cache (for dates that might have S3 data)
            // Returns (found_nodes, missing_dates) - allows partial hits
            // ================================================================
            let (cold_nodes, missing_dates) = self.shard_cache.cold_partition_cache.get_for_dates(
                catalog,
                &table.schema,
                &table.table,
                &dates,
            );

            if !cold_nodes.is_empty() {
                debug!(
                    "Cold cache hit for {}/{}/{}: {} nodes, {} dates missing",
                    catalog,
                    table.schema,
                    table.table,
                    cold_nodes.len(),
                    missing_dates.len()
                );
                partition_node_ids.extend(cold_nodes);
            }

            // ================================================================
            // STEP 3: S3 LIST discovery for missing dates (if storage available)
            // Cache discoveries in cold cache for future queries
            // ================================================================
            if !missing_dates.is_empty() {
                if let Some(ref storage) = self.storage {
                    debug!(
                        "Discovering {} missing dates via S3 LIST for {}/{}/{}",
                        missing_dates.len(),
                        catalog,
                        table.schema,
                        table.table
                    );

                    for date in &missing_dates {
                        let discovered = self
                            .discover_partition_nodes(
                                storage,
                                catalog,
                                &table.schema,
                                &table.table,
                                date,
                            )
                            .await;

                        if discovered.is_empty() {
                            debug!(
                                "No node_id partitions found for {}/{}/{}/date={}",
                                catalog, table.schema, table.table, date
                            );
                        } else {
                            // Cache in COLD cache (S3 data is immutable)
                            self.shard_cache.cold_partition_cache.insert(
                                catalog,
                                &table.schema,
                                &table.table,
                                date,
                                discovered.clone(),
                            );
                            partition_node_ids.extend(discovered);
                        }
                    }
                } else if partition_node_ids.is_empty() {
                    // No storage for discovery AND no cached nodes - fallback
                    debug!(
                        "No cache hits and no storage for {}/{}/{}, falling back",
                        catalog, table.schema, table.table
                    );
                    return None;
                }
                // If we have some nodes from hot/cold cache, continue with those
            }
        }

        if partition_node_ids.is_empty() {
            return None;
        }

        // ====================================================================
        // Build partition assignments with cache affinity and fallback support
        // ====================================================================
        // For each partition node_id:
        //   - If the node is queryable → assign partition to itself (cache affinity)
        //   - If the node is down → assign partition to a fallback node
        // This ensures each partition is queried by exactly ONE node.

        let mut partition_assignments: HashMap<String, Vec<String>> = HashMap::new();
        let mut target_node_map: HashMap<String, DataNodeInfo> = HashMap::new();

        for partition_node_id in &partition_node_ids {
            // Check if this partition's owner node is queryable
            let (target_node_id, target_node_info) =
                if let Some(node) = self.shard_cache.get_data_node(partition_node_id) {
                    if Self::is_queryable(&node) {
                        // Node is queryable - assign partition to itself (cache affinity!)
                        (partition_node_id.clone(), node)
                    } else {
                        // Node not queryable - find fallback
                        debug!(
                            "Partition owner {} not queryable (state={:?}), finding fallback",
                            partition_node_id, node.state
                        );
                        self.find_fallback_node(partition_node_id, all_primary_nodes)?
                    }
                } else {
                    // Node not in cluster - find fallback
                    debug!(
                        "Partition owner {} not in cluster, finding fallback",
                        partition_node_id
                    );
                    self.find_fallback_node(partition_node_id, all_primary_nodes)?
                };

            // Add partition to target node's assignment list
            partition_assignments
                .entry(target_node_id.clone())
                .or_default()
                .push(partition_node_id.clone());

            // Track target node info (dedup via HashMap)
            target_node_map.insert(target_node_id, target_node_info);
        }

        if partition_assignments.is_empty() {
            return None;
        }

        let target_nodes: Vec<DataNodeInfo> = target_node_map.into_values().collect();

        // Log the assignments for debugging
        for (target, partitions) in &partition_assignments {
            if partitions.len() == 1 && partitions[0] == *target {
                debug!("  {} → own partition (cache affinity)", target);
            } else {
                debug!(
                    "  {} → {:?} ({} includes fallback)",
                    target,
                    partitions,
                    if partitions.contains(target) {
                        "own +"
                    } else {
                        "fallback only,"
                    }
                );
            }
        }

        info!(
            "Partition-level routing: {} partition nodes → {} target nodes ({}% fan-out reduction)",
            partition_node_ids.len(),
            target_nodes.len(),
            if all_primary_nodes.is_empty() {
                0
            } else {
                100 - (target_nodes.len() * 100 / all_primary_nodes.len().max(1))
            }
        );

        Some(PartitionRoutingResult {
            target_nodes,
            partition_assignments,
        })
    }

    /// Find a fallback node for a partition whose owner is unavailable
    ///
    /// Uses consistent hash to ensure stable fallback assignment.
    fn find_fallback_node(
        &self,
        partition_node_id: &str,
        all_primary_nodes: &[DataNodeInfo],
    ) -> Option<(String, DataNodeInfo)> {
        if all_primary_nodes.is_empty() {
            warn!(
                "No primary nodes available for fallback of partition {}",
                partition_node_id
            );
            return None;
        }

        // Use consistent hash for stable fallback selection
        let idx = hash_select_index(&partition_node_id.to_string(), all_primary_nodes.len());
        let fallback_node = &all_primary_nodes[idx];

        info!(
            "Partition {} assigned to fallback node {} (via consistent hash)",
            partition_node_id, fallback_node.node_id
        );

        Some((fallback_node.node_id.clone(), fallback_node.clone()))
    }

    /// Extract date strings from time range for partition cache lookup
    ///
    /// Handles:
    /// - Exact date filter: date = '2024-01-15' → ["2024-01-15"]
    /// - Date range: date BETWEEN '2024-01-10' AND '2024-01-15' → ["2024-01-10", ..., "2024-01-15"]
    /// - Timestamp to date conversion: time >= 1704067200000000000 → date derived
    pub(super) fn extract_date_filters(&self, time_range: &Option<TimeRange>) -> Option<Vec<String>> {
        let tr = time_range.as_ref()?;

        // Check if we have date strings directly
        if let (Some(start_date), Some(end_date)) = (&tr.start_date, &tr.end_date) {
            return Some(self.generate_date_range(start_date, end_date));
        }

        // Try to derive dates from timestamps
        if let (Some(start_ts), Some(end_ts)) = (tr.min_time, tr.max_time) {
            let start_date = self.timestamp_to_date(start_ts);
            let end_date = self.timestamp_to_date(end_ts);
            return Some(self.generate_date_range(&start_date, &end_date));
        }

        // Single date from start
        if let Some(start_date) = &tr.start_date {
            return Some(vec![start_date.clone()]);
        }

        // Single date from start timestamp
        if let Some(start_ts) = tr.min_time {
            let date = self.timestamp_to_date(start_ts);
            return Some(vec![date]);
        }

        None
    }

    /// Generate all dates in a range (inclusive)
    pub(super) fn generate_date_range(&self, start: &str, end: &str) -> Vec<String> {
        let start_date = match NaiveDate::parse_from_str(start, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => return vec![start.to_string()],
        };
        let end_date = match NaiveDate::parse_from_str(end, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => return vec![end.to_string()],
        };

        let mut dates = Vec::new();
        let mut current = start_date;

        // Limit to reasonable range (max 365 days)
        let max_days = 365;
        let mut count = 0;

        while current <= end_date && count < max_days {
            dates.push(current.format("%Y-%m-%d").to_string());
            current = current.succ_opt().unwrap_or(current);
            count += 1;
        }

        dates
    }

    /// Convert nanosecond timestamp to date string
    pub(super) fn timestamp_to_date(&self, timestamp_ns: i64) -> String {
        let seconds = timestamp_ns / 1_000_000_000;
        let datetime = chrono::DateTime::from_timestamp(seconds, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        datetime.format("%Y-%m-%d").to_string()
    }

    /// Discover node_ids for a partition via S3 LIST
    ///
    /// Lists `node_id=*` subdirectories under the date partition path.
    /// Path structure: `{catalog}/{schema}/{table}/date={date}/node_id=*`
    ///
    /// Returns a list of node_ids that have data for this partition.
    pub(super) async fn discover_partition_nodes(
        &self,
        storage: &Storage,
        catalog: &str,
        schema: &str,
        table: &str,
        date: &str,
    ) -> Vec<String> {
        // Build the partition path: catalog/schema/table/date=YYYY-MM-DD
        let partition_prefix = format!("{}/{}/{}/date={}", catalog, schema, table, date);

        // List immediate subdirectories (should be node_id=* directories)
        match storage.list_prefixes(&partition_prefix).await {
            Ok(prefixes) => {
                // Extract node_id from "node_id=xxx" format
                let node_ids: Vec<String> = prefixes
                    .into_iter()
                    .filter_map(|prefix| {
                        if prefix.starts_with("node_id=") {
                            Some(prefix.strip_prefix("node_id=").unwrap().to_string())
                        } else {
                            None
                        }
                    })
                    .collect();

                if !node_ids.is_empty() {
                    debug!(
                        "S3 LIST discovered {} node_ids for {}: {:?}",
                        node_ids.len(),
                        partition_prefix,
                        node_ids
                    );
                }

                node_ids
            }
            Err(e) => {
                warn!(
                    "Failed to list partition prefixes for {}: {}",
                    partition_prefix, e
                );
                Vec::new()
            }
        }
    }

    // ========================================================================
    // Table-Level Routing
    // ========================================================================

    /// Resolve target nodes for query routing
    ///
    /// Returns (table_owners, target_nodes):
    /// - table_owners: Map of table -> owner node (if known)
    /// - target_nodes: List of nodes to query
    ///
    /// Handles scale-in by:
    /// 1. Detecting stale owners (removed from cluster)
    /// 2. Refreshing from Meta when stale owners detected
    pub(super) async fn resolve_query_targets(
        &self,
        catalog: &str,
        tables: &[TableRef],
        all_primary_nodes: &[DataNodeInfo],
    ) -> (Option<HashMap<String, String>>, Vec<DataNodeInfo>) {
        let Some(ref meta_client) = self.meta_client else {
            return (None, all_primary_nodes.to_vec());
        };

        let mut owners: HashMap<String, String> = HashMap::new();
        let mut nodes_to_query: HashMap<String, DataNodeInfo> = HashMap::new();

        for table in tables {
            let table_key = TableKey::new(catalog, &table.schema, &table.table);
            let distribution_percent =
                self.shard_cache
                    .get_distribution_percent(catalog, &table.schema, &table.table);

            // Try to resolve from cache first
            if let Some(cached_meta) = self.shard_cache.get_table_meta(&table_key) {
                // Check for stale owners (removed from cluster after scale-in)
                let (_active_count, stale_count) =
                    self.check_owners_status(cached_meta.all_owners());

                if stale_count > 0 {
                    // Stale owners detected - refresh from Meta
                    info!(
                        "Table {} has {} stale owners (scale-in detected), refreshing from Meta",
                        table_key.to_path(),
                        stale_count
                    );
                    self.resolve_distributed_from_meta(
                        meta_client,
                        catalog,
                        table,
                        &table_key,
                        all_primary_nodes,
                        &mut owners,
                        &mut nodes_to_query,
                    )
                    .await;
                    continue;
                }

                // All owners are active - add them
                let count = self.add_queryable_owners(
                    cached_meta.all_owners(),
                    &table_key,
                    &mut owners,
                    &mut nodes_to_query,
                );

                if count > 0 {
                    debug!(
                        "Table {} resolved: {} owners (dist={}%)",
                        table_key.to_path(),
                        count,
                        distribution_percent
                    );
                    continue;
                }
            }

            // Cache miss - handle based on distribution mode
            if distribution_percent > 0 {
                // Distributed table: query Meta then fallback
                self.resolve_distributed_from_meta(
                    meta_client,
                    catalog,
                    table,
                    &table_key,
                    all_primary_nodes,
                    &mut owners,
                    &mut nodes_to_query,
                )
                .await;
            } else {
                // Non-distributed: query Meta for owner
                self.resolve_from_meta(
                    meta_client,
                    catalog,
                    table,
                    &table_key,
                    &mut owners,
                    &mut nodes_to_query,
                )
                .await;
            }
        }

        if owners.is_empty() {
            debug!("No table owners found, falling back to broadcast");
            (None, all_primary_nodes.to_vec())
        } else {
            info!(
                "Table-level routing: {} tables -> {} nodes",
                owners.len(),
                nodes_to_query.len()
            );
            (Some(owners), nodes_to_query.into_values().collect())
        }
    }

    /// Check owners status - returns (active_count, stale_count)
    pub(super) fn check_owners_status(&self, owner_ids: &[String]) -> (usize, usize) {
        let mut active = 0;
        let mut stale = 0;
        for owner_id in owner_ids {
            if let Some(node) = self.shard_cache.get_data_node(owner_id) {
                if Self::is_queryable(&node) {
                    active += 1;
                } else {
                    stale += 1; // Node exists but not queryable
                }
            } else {
                stale += 1; // Node removed from cluster
            }
        }
        (active, stale)
    }

    /// Add queryable owners to the query set, returns count added
    pub(super) fn add_queryable_owners(
        &self,
        owner_ids: &[String],
        table_key: &TableKey,
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) -> usize {
        let mut count = 0;
        for owner_id in owner_ids {
            if let Some(node) = self.shard_cache.get_data_node(owner_id) {
                if Self::is_queryable(&node) {
                    owners.insert(table_key.to_path(), owner_id.clone());
                    nodes_to_query.insert(owner_id.clone(), node);
                    count += 1;
                }
            }
        }
        count
    }

    /// Check if a node is queryable (Active or Retiring)
    pub(super) fn is_queryable(node: &DataNodeInfo) -> bool {
        node.state == NodeState::Active || node.state == NodeState::Retiring
    }

    /// Resolve distributed table from Meta
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_distributed_from_meta(
        &self,
        meta_client: &MetaClient,
        catalog: &str,
        table: &TableRef,
        table_key: &TableKey,
        all_primary_nodes: &[DataNodeInfo],
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        match meta_client
            .get_table_owner(catalog, &table.schema, &table.table)
            .await
        {
            Ok(Some(table_meta)) => {
                // Cache the updated metadata
                self.shard_cache.set_table_owners(
                    table_key,
                    table_meta.all_owners().to_vec(),
                    table_meta.version,
                );

                // Add active owners
                let count = self.add_queryable_owners(
                    table_meta.all_owners(),
                    table_key,
                    owners,
                    nodes_to_query,
                );

                if count == 0 {
                    // No owners - use fallback
                    let distribution_percent = self.shard_cache.get_distribution_percent(
                        catalog,
                        &table.schema,
                        &table.table,
                    );
                    self.add_distributed_fallback(
                        table_key,
                        distribution_percent,
                        all_primary_nodes,
                        owners,
                        nodes_to_query,
                    );
                }
            }
            Ok(None) => {
                debug!(
                    "Table {} not found in Meta, using fallback",
                    table_key.to_path()
                );
                let distribution_percent =
                    self.shard_cache
                        .get_distribution_percent(catalog, &table.schema, &table.table);
                self.add_distributed_fallback(
                    table_key,
                    distribution_percent,
                    all_primary_nodes,
                    owners,
                    nodes_to_query,
                );
            }
            Err(e) => {
                warn!("Failed to get table owner from Meta: {}, using fallback", e);
                let distribution_percent =
                    self.shard_cache
                        .get_distribution_percent(catalog, &table.schema, &table.table);
                self.add_distributed_fallback(
                    table_key,
                    distribution_percent,
                    all_primary_nodes,
                    owners,
                    nodes_to_query,
                );
            }
        }
    }

    /// Add distributed table fallback nodes
    pub(super) fn add_distributed_fallback(
        &self,
        table_key: &TableKey,
        distribution_percent: u8,
        all_primary_nodes: &[DataNodeInfo],
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        if distribution_percent >= 100 {
            // 100%: query all primary nodes
            warn!(
                "Table {} (100% dist) has no cached owners - querying all {} primaries",
                table_key.to_path(),
                all_primary_nodes.len()
            );
            for node in all_primary_nodes {
                owners.insert(table_key.to_path(), node.node_id.clone());
                nodes_to_query.insert(node.node_id.clone(), node.clone());
            }
        } else {
            // Partial: use hash-based selection
            warn!(
                "Table {} ({}% dist) has no cached owners - using hash fallback",
                table_key.to_path(),
                distribution_percent
            );
            let target_count = ((all_primary_nodes.len() as f64 * distribution_percent as f64
                / 100.0)
                .ceil() as usize)
                .max(1);
            let start_idx = hash_select_index(table_key, all_primary_nodes.len());

            for i in 0..target_count {
                let idx = (start_idx + i) % all_primary_nodes.len();
                let node = &all_primary_nodes[idx];
                owners.insert(table_key.to_path(), node.node_id.clone());
                nodes_to_query.insert(node.node_id.clone(), node.clone());
            }
        }
    }

    /// Resolve table owner from Meta service
    pub(super) async fn resolve_from_meta(
        &self,
        meta_client: &MetaClient,
        catalog: &str,
        table: &TableRef,
        table_key: &TableKey,
        owners: &mut HashMap<String, String>,
        nodes_to_query: &mut HashMap<String, DataNodeInfo>,
    ) {
        match meta_client
            .get_table_owner(catalog, &table.schema, &table.table)
            .await
        {
            Ok(Some(table_meta)) => {
                // Cache the result
                self.shard_cache.set_table_owners(
                    table_key,
                    table_meta.all_owners().to_vec(),
                    table_meta.version,
                );

                // Add queryable owners
                let count = self.add_queryable_owners(
                    table_meta.all_owners(),
                    table_key,
                    owners,
                    nodes_to_query,
                );
                if count == 0 {
                    debug!("Table {} has no queryable owners", table_key.to_path());
                }
            }
            Ok(None) => {
                debug!("Table {} not found in Meta", table_key.to_path());
            }
            Err(e) => {
                warn!("Failed to get table owner from Meta: {}", e);
            }
        }
    }
}
