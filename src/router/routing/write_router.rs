//! Write Router - Routes write requests to appropriate Data nodes
//!
//! The WriteRouter is responsible for:
//! - Parsing line protocol to extract partition information
//! - Selecting target Data nodes based on table-level affinity
//! - Converting Points to RecordBatch for efficient transfer
//! - Dispatching writes to Data nodes in parallel via Arrow Flight
//!
//! ## Table-Level Routing
//!
//! All partitions of a table are routed to the same Data node. This provides:
//! - 600x fewer Meta entries (100K tables vs 60M files/partitions)
//! - Single routing lookup per query
//! - Excellent cache locality for hot data

use super::shard_cache::{DataNodeInfo, PartitionKey, ShardCache, TableKey};
use crate::config::SchemaConfig;
use crate::meta::client::MetaClient;
use crate::router::client::DataNodePool;
use crate::storage::batch_converter::points_to_record_batch;
use crate::storage::line_protocol::{format_date32_as_string, parse_line_protocol, Point};
use crate::utils::hash::hash_select_index;
use arrow::array::RecordBatch;
use futures::FutureExt;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Maximum number of retry attempts for failed writes
const MAX_WRITE_RETRIES: u32 = 2;

/// Write routing result
#[derive(Debug)]
pub struct WriteResult {
    pub success: bool,
    pub points_written: u64,
    pub bytes_written: u64,
    pub error: Option<String>,
    pub elapsed_ms: u64,
    /// Number of retries that were needed
    pub retries: u32,
}

/// Write Router for dispatching writes to Data nodes
pub struct WriteRouter {
    /// Shard cache for table→node mapping
    shard_cache: Arc<ShardCache>,

    /// Connection pool to Data nodes
    data_node_pool: Arc<DataNodePool>,

    /// Schema configuration for parsing
    schema_config: SchemaConfig,

    /// Meta client for updating table ownership after reroute
    meta_client: Option<Arc<MetaClient>>,
}

impl WriteRouter {
    pub fn new(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        schema_config: SchemaConfig,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            schema_config,
            meta_client: None,
        }
    }

    /// Create a WriteRouter with MetaClient for cluster mode
    pub fn with_meta_client(
        shard_cache: Arc<ShardCache>,
        data_node_pool: Arc<DataNodePool>,
        schema_config: SchemaConfig,
        meta_client: Arc<MetaClient>,
    ) -> Self {
        Self {
            shard_cache,
            data_node_pool,
            schema_config,
            meta_client: Some(meta_client),
        }
    }

    /// Route a write request to appropriate Data nodes
    ///
    /// This method:
    /// 1. Parses the line protocol to Points
    /// 2. Groups points by partition key
    /// 3. Converts each partition's Points to RecordBatch
    /// 4. Dispatches RecordBatches to Data nodes via Arrow Flight with retry
    pub async fn route_write(&self, catalog: &str, line_protocol: &str) -> WriteResult {
        let start = Instant::now();

        // Parse line protocol to Points
        let points = self.parse_line_protocol(line_protocol);
        if points.is_empty() {
            return WriteResult {
                success: false,
                points_written: 0,
                bytes_written: 0,
                error: Some("No valid points in line protocol".to_string()),
                elapsed_ms: start.elapsed().as_millis() as u64,
                retries: 0,
            };
        }

        let total_points = points.len();
        debug!("Routing {} points for catalog {}", total_points, catalog);

        // Group points by partition key and convert to RecordBatch
        let partitions = self.group_by_partition(catalog, points);
        let partition_batches = self.convert_partitions_to_batches(partitions);

        if partition_batches.is_empty() {
            return WriteResult {
                success: false,
                points_written: 0,
                bytes_written: 0,
                error: Some("No valid partitions to write".to_string()),
                elapsed_ms: start.elapsed().as_millis() as u64,
                retries: 0,
            };
        }

        // Dispatch writes with retry logic
        let (total_written, total_retries, errors) = self
            .dispatch_writes_with_retry(catalog, partition_batches)
            .await;

        let success = errors.is_empty();
        let error = if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        };

        if total_retries > 0 {
            info!(
                "Write completed with {} retries for catalog {}",
                total_retries, catalog
            );
        }

        WriteResult {
            success,
            points_written: total_written,
            bytes_written: line_protocol.len() as u64,
            error,
            elapsed_ms: start.elapsed().as_millis() as u64,
            retries: total_retries,
        }
    }

    /// Parse line protocol to Points
    fn parse_line_protocol(&self, line_protocol: &str) -> Vec<Point> {
        let mut results = Vec::new();
        for line in line_protocol.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Ok(point) = parse_line_protocol(line, &self.schema_config) {
                results.push(point);
            }
        }
        results
    }

    /// Group points by partition key
    fn group_by_partition(
        &self,
        catalog: &str,
        points: Vec<Point>,
    ) -> HashMap<PartitionKey, Vec<Point>> {
        let mut partitions: HashMap<PartitionKey, Vec<Point>> = HashMap::new();

        for point in points {
            let (schema, table) = match self.schema_config.strategy {
                crate::config::SchemaTableStrategy::ServiceAsSchema => {
                    (point.service.clone(), point.measurement.clone())
                }
                crate::config::SchemaTableStrategy::ServiceAsTable => (
                    self.schema_config.default_name.clone(),
                    point.service.clone(),
                ),
            };

            // Convert date i32 to YYYY-MM-DD string
            let date = format_date32_as_string(point.date);

            let key = PartitionKey {
                catalog: catalog.to_string(),
                schema,
                table,
                date,
            };

            partitions.entry(key).or_default().push(point);
        }

        partitions
    }

    /// Convert partitions to RecordBatches
    fn convert_partitions_to_batches(
        &self,
        partitions: HashMap<PartitionKey, Vec<Point>>,
    ) -> Vec<(PartitionKey, RecordBatch)> {
        let mut batches = Vec::new();

        for (partition_key, points) in partitions {
            match points_to_record_batch(&points) {
                Ok(batch) => {
                    batches.push((partition_key, batch));
                }
                Err(e) => {
                    error!(
                        "Failed to convert points to RecordBatch for partition {}: {}",
                        partition_key.to_path(),
                        e
                    );
                }
            }
        }

        batches
    }

    /// Dispatch writes with retry and failover using table-level routing
    ///
    /// For each partition:
    /// 1. Check if table has distributed write config
    /// 2. For distributed tables: write to multiple nodes based on percent
    /// 3. For single-node tables: standard routing with retry
    /// 4. On successful reroute, add new node as table owner in Meta
    ///
    /// Table-level routing ensures all partitions of a table go to the same node(s).
    async fn dispatch_writes_with_retry(
        &self,
        catalog: &str,
        partition_batches: Vec<(PartitionKey, RecordBatch)>,
    ) -> (u64, u32, Vec<String>) {
        let mut futures = Vec::new();

        for (partition_key, batch) in partition_batches {
            let pool = Arc::clone(&self.data_node_pool);
            let shard_cache = Arc::clone(&self.shard_cache);
            let meta_client = self.meta_client.clone();
            let catalog_str = catalog.to_string();

            // Check if this table uses distributed writes
            let is_distributed = self.is_distributed_table(catalog, &partition_key.schema, &partition_key.table);

            if is_distributed {
                // Get target nodes for distributed write
                let table_key = TableKey::new(catalog, &partition_key.schema, &partition_key.table);
                let target_nodes = self.select_nodes_for_distributed_write(
                    catalog,
                    &partition_key.schema,
                    &partition_key.table,
                    &table_key,
                );

                // Distributed write with target nodes
                let future = async move {
                    Self::write_partition_with_retry(
                        &pool,
                        &shard_cache,
                        meta_client.as_ref(),
                        &catalog_str,
                        partition_key,
                        batch,
                        target_nodes,
                        MAX_WRITE_RETRIES,
                    )
                    .await
                };
                futures.push(future.boxed());
            } else {
                // Standard single-node routing (empty target_nodes triggers table-level routing)
                let future = async move {
                    Self::write_partition_with_retry(
                        &pool,
                        &shard_cache,
                        meta_client.as_ref(),
                        &catalog_str,
                        partition_key,
                        batch,
                        Vec::new(),
                        MAX_WRITE_RETRIES,
                    )
                    .await
                };
                futures.push(future.boxed());
            }
        }

        // Execute all partition writes in parallel
        let results = futures::future::join_all(futures).await;

        // Aggregate results
        let mut total_written = 0u64;
        let mut total_retries = 0u32;
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok((written, retries)) => {
                    total_written += written;
                    total_retries += retries;
                }
                Err(e) => errors.push(e),
            }
        }

        (total_written, total_retries, errors)
    }

    /// Write a partition with retry logic
    ///
    /// Unified write function that handles both distributed and non-distributed tables:
    /// - Distributed (target_nodes provided): randomly selects from target pool for load distribution
    /// - Non-distributed (target_nodes empty): uses table-level routing with exclusion on retry
    ///
    /// Returns (rows_written, retry_count) on success.
    /// On successful write, registers the node as table owner in Meta.
    async fn write_partition_with_retry(
        pool: &DataNodePool,
        shard_cache: &ShardCache,
        meta_client: Option<&Arc<MetaClient>>,
        catalog: &str,
        partition_key: PartitionKey,
        batch: RecordBatch,
        target_nodes: Vec<DataNodeInfo>,
        max_retries: u32,
    ) -> Result<(u64, u32), String> {
        const MAX_ATTEMPTS: usize = 3;

        let table_key = TableKey::new(catalog, &partition_key.schema, &partition_key.table);
        let table_path = table_key.to_path();
        let row_count = batch.num_rows() as u64;
        let is_distributed = !target_nodes.is_empty();

        // Prepare candidate nodes based on write mode
        let candidates: Vec<DataNodeInfo> = if is_distributed {
            // Distributed: shuffle target nodes for random selection
            let mut shuffled = target_nodes;
            {
                let mut rng = rand::thread_rng();
                shuffled.shuffle(&mut rng);
            }
            shuffled
        } else {
            // Non-distributed: will select dynamically via table routing
            Vec::new()
        };

        let max_attempts = if is_distributed {
            candidates.len().min(MAX_ATTEMPTS)
        } else {
            (max_retries + 1) as usize
        };

        debug!(
            "Write partition: {} rows to table {}, mode={}, max_attempts={}",
            row_count,
            table_path,
            if is_distributed { "distributed" } else { "single" },
            max_attempts
        );

        let mut tried_nodes: HashSet<String> = HashSet::new();
        let mut last_error = String::new();
        let mut retries = 0u32;

        for attempt in 0..max_attempts {
            // Select target node
            let node = if is_distributed {
                // Distributed: use pre-shuffled candidates
                candidates.get(attempt).cloned()
            } else {
                // Non-distributed: table-level routing with exclusion
                if attempt == 0 {
                    shard_cache.select_node_for_table(&table_key)
                } else {
                    shard_cache.select_node_for_table_excluding(&table_key, &tried_nodes)
                }
            };

            let node = match node {
                Some(n) => n,
                None => {
                    if attempt == 0 {
                        return Err(format!("No data node available for table {}", table_path));
                    }
                    break; // No more nodes to try
                }
            };

            tried_nodes.insert(node.node_id.clone());

            // Attempt write
            match pool
                .write_batch(
                    &node.node_id,
                    catalog,
                    &partition_key.schema,
                    &partition_key.table,
                    partition_key.date_as_i32(),
                    batch.clone(),
                )
                .await
            {
                Ok(written) => {
                    // Success - record and register owner
                    shard_cache.record_node_success(&node.node_id);

                    // Mark failed nodes as suspect (for retried writes)
                    if attempt > 0 {
                        for failed_id in tried_nodes.iter() {
                            if failed_id != &node.node_id {
                                shard_cache.mark_node_suspect(failed_id);
                            }
                        }
                    }

                    // Register as table owner in Meta
                    Self::register_table_owner(
                        meta_client,
                        shard_cache,
                        &table_key,
                        catalog,
                        &partition_key,
                        &node.node_id,
                        attempt > 0,
                    )
                    .await;

                    debug!(
                        "Write succeeded for table {} on node {} (attempt {})",
                        table_path, node.node_id, attempt + 1
                    );
                    return Ok((written, retries));
                }
                Err(e) => {
                    warn!(
                        "Write attempt {} failed for node {} table {}: {}",
                        attempt + 1,
                        node.node_id,
                        table_path,
                        e
                    );
                    last_error = e;
                    retries += 1;
                }
            }
        }

        // All attempts failed
        error!(
            "Write failed for table {} after {} attempts: {}",
            table_path,
            tried_nodes.len(),
            last_error
        );

        // Mark all tried nodes as suspect
        for node_id in &tried_nodes {
            shard_cache.mark_node_suspect(node_id);
        }

        Err(format!(
            "Write failed for table {} after {} attempts: {}",
            table_path,
            tried_nodes.len(),
            last_error
        ))
    }

    /// Register a node as table owner in Meta (helper function)
    async fn register_table_owner(
        meta_client: Option<&Arc<MetaClient>>,
        shard_cache: &ShardCache,
        table_key: &TableKey,
        catalog: &str,
        partition_key: &PartitionKey,
        node_id: &str,
        is_reroute: bool,
    ) {
        let Some(client) = meta_client else { return };

        match client
            .add_table_owner(catalog, &partition_key.schema, &partition_key.table, node_id)
            .await
        {
            Ok((already_owner, all_owners, version)) => {
                if !already_owner {
                    if is_reroute {
                        info!(
                            "Added rerouted node {} as owner for table {} (v{})",
                            node_id,
                            table_key.to_path(),
                            version
                        );
                    } else {
                        debug!(
                            "Registered node {} as owner for table {} (v{})",
                            node_id,
                            table_key.to_path(),
                            version
                        );
                    }
                }
                shard_cache.set_table_owners(table_key, all_owners, version);
            }
            Err(e) => {
                warn!(
                    "Failed to register node {} as owner for table {}: {}",
                    node_id,
                    table_key.to_path(),
                    e
                );
            }
        }
    }

    /// Select target nodes for distributed writes based on distribution percent
    ///
    /// Strategy: Use existing table owners as target pool, complement if needed.
    ///
    /// - percent=0: Single node (standard routing)
    /// - percent=100: All primary nodes
    /// - percent=50: Existing owners if >= 50%, else complement to reach 50%
    ///
    /// This ensures:
    /// 1. Data stays co-located on nodes that already have table data
    /// 2. Query routing (via table owners) always finds all data
    /// 3. New tables gradually build up their owner set
    fn select_nodes_for_distributed_write(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        table_key: &TableKey,
    ) -> Vec<DataNodeInfo> {
        let percent = self
            .shard_cache
            .get_distribution_percent(catalog, schema, table);

        if percent == 0 {
            // Standard single-node routing
            if let Some(node) = self.shard_cache.select_node_for_table(table_key) {
                return vec![node];
            }
            return vec![];
        }

        // Get all active primary nodes
        let all_nodes = self.shard_cache.get_active_primary_nodes();
        if all_nodes.is_empty() {
            return vec![];
        }

        if percent >= 100 {
            // All nodes
            debug!(
                "Distributed write (100%): selecting all {} primary nodes for {}/{}/{}",
                all_nodes.len(),
                catalog,
                schema,
                table
            );
            return all_nodes;
        }

        // Calculate target node count (minimum 1)
        let target_count =
            ((all_nodes.len() as f64 * percent as f64 / 100.0).ceil() as usize).max(1);

        // Strategy: Use existing owners first, complement if needed
        // This keeps data co-located on nodes that already have this table's data
        let existing_owners = self.shard_cache.get_table_owners(table_key);
        let owner_node_ids: HashSet<String> = existing_owners
            .as_ref()
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();

        // Filter to get active owner nodes
        let mut active_owners: Vec<DataNodeInfo> = all_nodes
            .iter()
            .filter(|n| owner_node_ids.contains(&n.node_id))
            .cloned()
            .collect();

        if active_owners.len() >= target_count {
            // Existing owners meet the target ratio - use them as target pool
            debug!(
                "Distributed write ({}%): using {} existing owners for {}/{}/{}: {:?}",
                percent,
                active_owners.len(),
                catalog,
                schema,
                table,
                active_owners.iter().map(|n| &n.node_id).collect::<Vec<_>>()
            );
            return active_owners;
        }

        // Need to complement with additional nodes to reach target
        // Use hash-based selection for deterministic complementing
        let need_more = target_count - active_owners.len();
        let start_idx = hash_select_index(table_key, all_nodes.len());

        let mut added = 0;
        for i in 0..all_nodes.len() {
            if added >= need_more {
                break;
            }
            let idx = (start_idx + i) % all_nodes.len();
            let candidate = &all_nodes[idx];
            if !owner_node_ids.contains(&candidate.node_id) {
                active_owners.push(candidate.clone());
                added += 1;
            }
        }

        debug!(
            "Distributed write ({}%): {} owners + {} complement = {} nodes for {}/{}/{}: {:?}",
            percent,
            active_owners.len() - added,
            added,
            active_owners.len(),
            catalog,
            schema,
            table,
            active_owners.iter().map(|n| &n.node_id).collect::<Vec<_>>()
        );

        active_owners
    }

    /// Check if a table should use distributed writes
    fn is_distributed_table(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.shard_cache
            .get_distribution_percent(catalog, schema, table)
            > 0
    }

    /// Broadcast flush command to all active Primary data nodes
    /// (Standby nodes are cold backups and don't have in-memory data to flush)
    pub async fn broadcast_flush(&self) -> Result<usize, String> {
        let nodes = self.shard_cache.get_active_primary_nodes();
        if nodes.is_empty() {
            return Err("No active data nodes".to_string());
        }

        let mut success_count = 0;
        let mut errors = Vec::new();

        for node in &nodes {
            match self.data_node_pool.flush_node(&node.node_id).await {
                Ok(_) => {
                    success_count += 1;
                    debug!("Flushed node {}", node.node_id);
                }
                Err(e) => {
                    errors.push(format!("{}: {}", node.node_id, e));
                }
            }
        }

        if errors.is_empty() {
            Ok(success_count)
        } else if success_count > 0 {
            // Partial success
            Ok(success_count)
        } else {
            Err(errors.join("; "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::DataNodeMode;
    use crate::router::routing::shard_cache::NodeState;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    fn create_test_nodes(count: usize) -> Vec<DataNodeInfo> {
        (0..count)
            .map(|i| DataNodeInfo {
                node_id: format!("node-{}", i),
                grpc_addr: format!("127.0.0.1:{}", 7000 + i),
                state: NodeState::Active,
                mode: DataNodeMode::Primary,
            })
            .collect()
    }

    fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let time_array = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        let value_array = StringArray::from((0..rows).map(|i| format!("val-{}", i)).collect::<Vec<_>>());

        RecordBatch::try_new(schema, vec![Arc::new(time_array), Arc::new(value_array)]).unwrap()
    }

    /// Test that random selection distributes writes across nodes
    #[test]
    fn test_random_distribution_over_multiple_calls() {
        use rand::seq::SliceRandom;

        let nodes = create_test_nodes(3);
        let mut selection_counts: HashMap<String, usize> = HashMap::new();

        // Simulate 1000 random selections
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let mut shuffled = nodes.clone();
            shuffled.shuffle(&mut rng);
            let selected = &shuffled[0];
            *selection_counts.entry(selected.node_id.clone()).or_insert(0) += 1;
        }

        // Each node should be selected roughly 333 times (±15% tolerance)
        for (node_id, count) in &selection_counts {
            let expected = 1000.0 / 3.0;
            let tolerance = expected * 0.20; // 20% tolerance for randomness
            assert!(
                (*count as f64 - expected).abs() < tolerance,
                "Node {} selected {} times, expected ~{:.0} (±{:.0})",
                node_id,
                count,
                expected,
                tolerance
            );
        }

        // All 3 nodes should have been selected at least once
        assert_eq!(selection_counts.len(), 3, "All nodes should be selected");
    }

    /// Test that shuffling produces different orders
    #[test]
    fn test_shuffle_produces_different_orders() {
        use rand::seq::SliceRandom;

        let nodes = create_test_nodes(5);
        let mut orders_seen: HashSet<String> = HashSet::new();

        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let mut shuffled = nodes.clone();
            shuffled.shuffle(&mut rng);
            let order: String = shuffled.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>().join(",");
            orders_seen.insert(order);
        }

        // With 5 nodes (120 possible permutations), we should see many different orders
        assert!(
            orders_seen.len() > 10,
            "Should see many different shuffle orders, got {}",
            orders_seen.len()
        );
    }

    /// Test partition key to i32 conversion
    #[test]
    fn test_partition_key_date_conversion() {
        let pk = PartitionKey::new("mydb", "default", "cpu", "2024-01-15");

        // 2024-01-15 should be day number since epoch
        let date_i32 = pk.date_as_i32();
        assert!(date_i32 > 0, "Date should be positive days since epoch");

        // Verify it's roughly correct (2024 is ~54 years = ~19700 days)
        assert!(date_i32 > 19000, "Date should be > 19000 days since epoch");
        assert!(date_i32 < 25000, "Date should be < 25000 days since epoch");
    }

    /// Test that single node case works (no randomness needed)
    #[test]
    fn test_single_node_always_selected() {
        use rand::seq::SliceRandom;

        let nodes = create_test_nodes(1);

        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let mut shuffled = nodes.clone();
            shuffled.shuffle(&mut rng);
            assert_eq!(shuffled[0].node_id, "node-0");
        }
    }

    /// Test empty nodes case
    #[test]
    fn test_empty_nodes_returns_empty() {
        let nodes: Vec<DataNodeInfo> = vec![];
        assert!(nodes.is_empty());
    }

    /// Test batch cloning for fallback (batch should be reusable)
    #[test]
    fn test_batch_can_be_cloned_for_retry() {
        let batch = create_test_batch(100);
        assert_eq!(batch.num_rows(), 100);

        // Clone multiple times (simulating retries)
        let clone1 = batch.clone();
        let clone2 = batch.clone();
        let clone3 = batch.clone();

        assert_eq!(clone1.num_rows(), 100);
        assert_eq!(clone2.num_rows(), 100);
        assert_eq!(clone3.num_rows(), 100);

        // Original should still be valid
        assert_eq!(batch.num_rows(), 100);
    }

    /// Test that node selection with fallback tries all nodes
    #[test]
    fn test_fallback_order_covers_all_nodes() {
        use rand::seq::SliceRandom;

        let nodes = create_test_nodes(4);

        let mut rng = rand::thread_rng();
        let mut shuffled = nodes.clone();
        shuffled.shuffle(&mut rng);

        // All nodes should be present in shuffled order
        let node_ids: HashSet<String> = shuffled.iter().map(|n| n.node_id.clone()).collect();
        assert_eq!(node_ids.len(), 4);
        assert!(node_ids.contains("node-0"));
        assert!(node_ids.contains("node-1"));
        assert!(node_ids.contains("node-2"));
        assert!(node_ids.contains("node-3"));
    }

    /// Test table key construction
    #[test]
    fn test_table_key_path() {
        let key = TableKey::new("mydb", "service_1", "cpu");
        assert_eq!(key.to_path(), "mydb/service_1/cpu");
    }

    /// Test that write result captures all fields
    #[test]
    fn test_write_result_structure() {
        let result = WriteResult {
            success: true,
            points_written: 1000,
            bytes_written: 50000,
            error: None,
            elapsed_ms: 42,
            retries: 0,
        };

        assert!(result.success);
        assert_eq!(result.points_written, 1000);
        assert_eq!(result.bytes_written, 50000);
        assert!(result.error.is_none());
        assert_eq!(result.elapsed_ms, 42);
        assert_eq!(result.retries, 0);
    }

    /// Test write result with error
    #[test]
    fn test_write_result_with_error() {
        let result = WriteResult {
            success: false,
            points_written: 0,
            bytes_written: 0,
            error: Some("Connection refused".to_string()),
            elapsed_ms: 100,
            retries: 2,
        };

        assert!(!result.success);
        assert_eq!(result.error, Some("Connection refused".to_string()));
        assert_eq!(result.retries, 2);
    }
}
