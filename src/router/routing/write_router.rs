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

                if target_nodes.is_empty() {
                    futures.push(async move {
                        Err::<(u64, u32), String>(format!(
                            "No data nodes available for distributed table {}",
                            table_key.to_path()
                        ))
                    }.boxed());
                    continue;
                }

                // Write to all selected nodes in parallel
                let future = async move {
                    Self::write_partition_distributed(
                        &pool,
                        &shard_cache,
                        meta_client.as_ref(),
                        &catalog_str,
                        partition_key,
                        batch,
                        target_nodes,
                    )
                    .await
                };
                futures.push(future.boxed());
            } else {
                // Standard single-node routing with retry
                let future = async move {
                    Self::write_partition_with_retry(
                        &pool,
                        &shard_cache,
                        meta_client.as_ref(),
                        &catalog_str,
                        partition_key,
                        batch,
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

    /// Write a partition to multiple nodes for distributed tables
    ///
    /// All target nodes receive the same data in parallel.
    /// Each successful node is registered as a table owner in Meta.
    async fn write_partition_distributed(
        pool: &DataNodePool,
        shard_cache: &ShardCache,
        meta_client: Option<&Arc<MetaClient>>,
        catalog: &str,
        partition_key: PartitionKey,
        batch: RecordBatch,
        target_nodes: Vec<DataNodeInfo>,
    ) -> Result<(u64, u32), String> {
        let table_key = TableKey::new(catalog, &partition_key.schema, &partition_key.table);
        let table_path = table_key.to_path();
        let row_count = batch.num_rows() as u64;

        debug!(
            "Distributed write: {} rows to {} nodes for table {}",
            row_count,
            target_nodes.len(),
            table_path
        );

        // Write to all target nodes in parallel
        let mut write_futures = Vec::new();
        for node in &target_nodes {
            let node_id = node.node_id.clone();
            let node_id_for_error = node.node_id.clone();
            let catalog = catalog.to_string();
            let schema = partition_key.schema.clone();
            let table = partition_key.table.clone();
            let date = partition_key.date_as_i32();
            let batch_clone = batch.clone();

            write_futures.push(async move {
                pool.write_batch(&node_id, &catalog, &schema, &table, date, batch_clone, None)
                    .await
                    .map(|written| (node_id, written))
                    .map_err(|e| (node_id_for_error, e))
            });
        }

        let results: Vec<_> = futures::future::join_all(write_futures).await;

        // Count successes and failures
        let mut successful_nodes: Vec<String> = Vec::new();
        let mut failed_nodes: Vec<(String, String)> = Vec::new();

        for result in results {
            match result {
                Ok((node_id, _written)) => {
                    shard_cache.record_node_success(&node_id);
                    successful_nodes.push(node_id);
                }
                Err((node_id, error)) => {
                    shard_cache.mark_node_suspect(&node_id);
                    failed_nodes.push((node_id, error));
                }
            }
        }

        // Register all successful nodes as table owners in Meta
        if let Some(client) = meta_client {
            for node_id in &successful_nodes {
                match client
                    .add_table_owner(catalog, &partition_key.schema, &partition_key.table, node_id)
                    .await
                {
                    Ok((already_owner, all_owners, version)) => {
                        if !already_owner {
                            debug!(
                                "Registered distributed node {} as owner for table {} (v{})",
                                node_id, table_path, version
                            );
                        }
                        // Update local cache with all owners
                        shard_cache.set_table_owners(&table_key, all_owners, version);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to register distributed node {} as owner for table {}: {}",
                            node_id, table_path, e
                        );
                    }
                }
            }
        }

        // Log result
        if successful_nodes.is_empty() {
            let errors: Vec<String> = failed_nodes
                .iter()
                .map(|(n, e)| format!("{}: {}", n, e))
                .collect();
            error!(
                "Distributed write failed for table {} on all {} nodes: {}",
                table_path,
                target_nodes.len(),
                errors.join("; ")
            );
            return Err(format!(
                "Distributed write failed for table {} on all nodes: {}",
                table_path,
                errors.join("; ")
            ));
        }

        if !failed_nodes.is_empty() {
            warn!(
                "Distributed write partially succeeded for table {}: {} of {} nodes failed",
                table_path,
                failed_nodes.len(),
                target_nodes.len()
            );
        } else {
            debug!(
                "Distributed write succeeded for table {} on {} nodes",
                table_path,
                successful_nodes.len()
            );
        }

        // Return success (at least one node received the data)
        // For distributed writes, we count the rows once even though multiple nodes have the data
        Ok((row_count, 0))
    }

    /// Write a single partition with retry logic using table-level routing
    ///
    /// Returns (rows_written, retry_count) on success.
    /// On successful reroute, adds the new node as a table owner in Meta.
    async fn write_partition_with_retry(
        pool: &DataNodePool,
        shard_cache: &ShardCache,
        meta_client: Option<&Arc<MetaClient>>,
        catalog: &str,
        partition_key: PartitionKey,
        batch: RecordBatch,
        max_retries: u32,
    ) -> Result<(u64, u32), String> {
        let table_key = TableKey::new(catalog, &partition_key.schema, &partition_key.table);
        let table_path = table_key.to_path();
        let partition_path = partition_key.to_path();
        let row_count = batch.num_rows() as u64;
        let mut tried_nodes: HashSet<String> = HashSet::new();
        let mut last_error = String::new();
        let mut retries = 0u32;

        for attempt in 0..=max_retries {
            // Select target node using table-level routing (excluding already-tried nodes on retry)
            let node = if attempt == 0 {
                shard_cache.select_node_for_table(&table_key)
            } else {
                shard_cache.select_node_for_table_excluding(&table_key, &tried_nodes)
            };

            let node = match node {
                Some(n) => n,
                None => {
                    return if attempt == 0 {
                        Err(format!(
                            "No data node available for table {}",
                            table_path
                        ))
                    } else {
                        // No more nodes to try
                        Err(format!(
                            "All {} data nodes failed for table {}: {}",
                            tried_nodes.len(),
                            table_path,
                            last_error
                        ))
                    };
                }
            };

            tried_nodes.insert(node.node_id.clone());

            debug!(
                "Writing {} rows to {} for partition {} (table: {}, attempt {})",
                row_count,
                node.node_id,
                partition_path,
                table_path,
                attempt + 1
            );

            // Try to write
            // Table-level routing ensures all partitions of a table go to the same node.
            // Data node will register table ownership with Meta after flush.
            match pool
                .write_batch(
                    &node.node_id,
                    catalog,
                    &partition_key.schema,
                    &partition_key.table,
                    partition_key.date_as_i32(),
                    batch.clone(),
                    None,
                )
                .await
            {
                Ok(written) => {
                    // Success!
                    if attempt > 0 {
                        info!(
                            "Write succeeded on retry {} (node {}) for table {}",
                            attempt, node.node_id, table_path
                        );
                        // Mark original failed nodes as suspect
                        for failed_node_id in tried_nodes.iter() {
                            if failed_node_id != &node.node_id {
                                shard_cache.mark_node_suspect(failed_node_id);
                            }
                        }

                        // CRITICAL: Add rerouted node as table owner in Meta
                        // This ensures queries will scatter-gather to all nodes with data
                        if let Some(client) = meta_client {
                            match client
                                .add_table_owner(
                                    catalog,
                                    &partition_key.schema,
                                    &partition_key.table,
                                    &node.node_id,
                                )
                                .await
                            {
                                Ok((already_owner, all_owners, version)) => {
                                    if !already_owner {
                                        info!(
                                            "Added rerouted node {} as owner for table {} (v{}, owners: {:?})",
                                            node.node_id, table_path, version, all_owners
                                        );
                                        // Update local cache with all owners
                                        shard_cache.set_table_owners(&table_key, all_owners, version);
                                    }
                                }
                                Err(e) => {
                                    // Log error but don't fail the write - data is already written
                                    warn!(
                                        "Failed to add rerouted node {} as owner for table {}: {}",
                                        node.node_id, table_path, e
                                    );
                                }
                            }
                        }
                    }

                    // Record success for the working node
                    shard_cache.record_node_success(&node.node_id);

                    debug!(
                        "Successfully wrote {} rows to {} for partition {}",
                        written, node.node_id, partition_path
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
                    // Continue to next attempt
                }
            }
        }

        // All retries exhausted
        error!(
            "Write failed after {} attempts for table {}: {}",
            max_retries + 1,
            table_path,
            last_error
        );

        // Mark all tried nodes as suspect
        for node_id in &tried_nodes {
            shard_cache.mark_node_suspect(node_id);
        }

        Err(format!(
            "Write failed after {} attempts for table {}: {}",
            max_retries + 1,
            table_path,
            last_error
        ))
    }

    /// Select nodes for distributed writes based on distribution percent
    ///
    /// - percent=0: Single node (standard routing)
    /// - percent=100: All primary nodes
    /// - percent=50: ~50% of primary nodes (minimum 1)
    ///
    /// Uses hash-based selection with random offset for load balancing.
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

        // Calculate how many nodes to select (minimum 1)
        let num_nodes = ((all_nodes.len() as f64 * percent as f64 / 100.0).ceil() as usize).max(1);

        // Use hash-based selection with timestamp for randomness
        // This provides pseudo-random but reproducible selection for debugging
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        // Start from a hash-based offset for distribution
        let start_idx = hash_select_index(&(table_key.to_path(), timestamp), all_nodes.len());

        // Select num_nodes starting from the offset
        let mut selected: Vec<DataNodeInfo> = Vec::with_capacity(num_nodes);
        for i in 0..num_nodes {
            let idx = (start_idx + i) % all_nodes.len();
            selected.push(all_nodes[idx].clone());
        }

        // Sort for consistent logging
        selected.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        debug!(
            "Distributed write ({}%): selecting {} of {} nodes for {}/{}/{}: {:?}",
            percent,
            selected.len(),
            all_nodes.len(),
            catalog,
            schema,
            table,
            selected.iter().map(|n| &n.node_id).collect::<Vec<_>>()
        );

        selected
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
