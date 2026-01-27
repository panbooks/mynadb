//! Write Router - Routes write requests to appropriate Data nodes
//!
//! The WriteRouter is responsible for:
//! - Parsing line protocol to extract partition information
//! - Selecting target Data nodes based on partition affinity
//! - Converting Points to RecordBatch for efficient transfer
//! - Dispatching writes to Data nodes in parallel via Arrow Flight

use super::shard_cache::{PartitionKey, ShardCache};
use crate::config::SchemaConfig;
use crate::router::client::DataNodePool;
use crate::storage::batch_converter::points_to_record_batch;
use crate::storage::line_protocol::{format_date32_as_string, parse_line_protocol, Point};
use arrow::array::RecordBatch;
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
    /// Shard cache for partition→node mapping
    shard_cache: Arc<ShardCache>,

    /// Connection pool to Data nodes
    data_node_pool: Arc<DataNodePool>,

    /// Schema configuration for parsing
    schema_config: SchemaConfig,
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

    /// Dispatch writes with retry and failover
    ///
    /// For each partition:
    /// 1. Select target node
    /// 2. Try to write
    /// 3. On failure, mark node as suspect and retry on different node
    /// 4. Continue until success or max retries exceeded
    async fn dispatch_writes_with_retry(
        &self,
        catalog: &str,
        partition_batches: Vec<(PartitionKey, RecordBatch)>,
    ) -> (u64, u32, Vec<String>) {
        let mut futures = Vec::new();

        for (partition_key, batch) in partition_batches {
            let pool = Arc::clone(&self.data_node_pool);
            let shard_cache = Arc::clone(&self.shard_cache);
            let catalog = catalog.to_string();

            let future = async move {
                Self::write_partition_with_retry(
                    &pool,
                    &shard_cache,
                    &catalog,
                    partition_key,
                    batch,
                    MAX_WRITE_RETRIES,
                )
                .await
            };

            futures.push(future);
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

    /// Write a single partition with retry logic
    ///
    /// Returns (rows_written, retry_count) on success
    async fn write_partition_with_retry(
        pool: &DataNodePool,
        shard_cache: &ShardCache,
        catalog: &str,
        partition_key: PartitionKey,
        batch: RecordBatch,
        max_retries: u32,
    ) -> Result<(u64, u32), String> {
        let partition_path = partition_key.to_path();
        let row_count = batch.num_rows() as u64;
        let mut tried_nodes: HashSet<String> = HashSet::new();
        let mut last_error = String::new();
        let mut retries = 0u32;

        for attempt in 0..=max_retries {
            // Select target node (excluding already-tried nodes on retry)
            let node = if attempt == 0 {
                shard_cache.select_node_for_partition(&partition_key)
            } else {
                shard_cache.select_node_for_partition_excluding(&partition_key, &tried_nodes)
            };

            let node = match node {
                Some(n) => n,
                None => {
                    return if attempt == 0 {
                        Err(format!(
                            "No data node available for partition {}",
                            partition_path
                        ))
                    } else {
                        // No more nodes to try
                        Err(format!(
                            "All {} data nodes failed for partition {}: {}",
                            tried_nodes.len(),
                            partition_path,
                            last_error
                        ))
                    };
                }
            };

            tried_nodes.insert(node.node_id.clone());

            debug!(
                "Writing {} rows to {} for partition {} (attempt {})",
                row_count,
                node.node_id,
                partition_path,
                attempt + 1
            );

            // Try to write
            // Note: fencing_token is None for now. In multi-Router deployments,
            // acquiring a partition lock from Meta before write would prevent
            // split-brain scenarios. Trade-off: adds latency. Current design
            // relies on shard_cache routing writes to the same node per partition.
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
                            "Write succeeded on retry {} (node {}) for partition {}",
                            attempt, node.node_id, partition_path
                        );
                        // Mark original failed nodes as suspect
                        for failed_node_id in tried_nodes.iter() {
                            if failed_node_id != &node.node_id {
                                shard_cache.mark_node_suspect(failed_node_id);
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
                        "Write attempt {} failed for node {} partition {}: {}",
                        attempt + 1,
                        node.node_id,
                        partition_path,
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
            "Write failed after {} attempts for partition {}: {}",
            max_retries + 1,
            partition_path,
            last_error
        );

        // Mark all tried nodes as suspect
        for node_id in &tried_nodes {
            shard_cache.mark_node_suspect(node_id);
        }

        Err(format!(
            "Write failed after {} attempts for partition {}: {}",
            max_retries + 1,
            partition_path,
            last_error
        ))
    }
}
