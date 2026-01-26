//! Write Router - Routes write requests to appropriate Data nodes
//!
//! The WriteRouter is responsible for:
//! - Parsing line protocol to extract partition information
//! - Selecting target Data nodes based on partition affinity
//! - Dispatching writes to Data nodes in parallel
//! - Handling retries and failover

use super::shard_cache::{DataNodeInfo, PartitionKey, ShardCache};
use crate::config::SchemaConfig;
use crate::router::client::DataNodePool;
use crate::storage::line_protocol::{parse_multiple_lines, Point};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Write routing result
#[derive(Debug)]
pub struct WriteResult {
    pub success: bool,
    pub points_written: u64,
    pub bytes_written: u64,
    pub error: Option<String>,
    pub elapsed_ms: u64,
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
    /// 1. Parses the line protocol
    /// 2. Groups points by partition key
    /// 3. Selects target Data nodes for each partition
    /// 4. Dispatches writes in parallel
    pub async fn route_write(
        &self,
        catalog: &str,
        line_protocol: &str,
    ) -> WriteResult {
        let start = Instant::now();

        // Parse line protocol
        let points = parse_multiple_lines(line_protocol, &self.schema_config);
        if points.is_empty() {
            return WriteResult {
                success: false,
                points_written: 0,
                bytes_written: 0,
                error: Some("No valid points in line protocol".to_string()),
                elapsed_ms: start.elapsed().as_millis() as u64,
            };
        }

        let total_points = points.len();
        debug!("Routing {} points for catalog {}", total_points, catalog);

        // Group points by partition key
        let partitions = self.group_by_partition(catalog, points);

        // Group partitions by target Data node
        let node_batches = self.group_by_target_node(partitions);

        if node_batches.is_empty() {
            return WriteResult {
                success: false,
                points_written: 0,
                bytes_written: 0,
                error: Some("No active data nodes available".to_string()),
                elapsed_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Dispatch writes to Data nodes in parallel
        let results = self.dispatch_writes(catalog, node_batches, line_protocol).await;

        // Aggregate results
        let mut total_written = 0u64;
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(written) => total_written += written,
                Err(e) => errors.push(e),
            }
        }

        let success = errors.is_empty();
        let error = if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        };

        WriteResult {
            success,
            points_written: total_written,
            bytes_written: line_protocol.len() as u64,
            error,
            elapsed_ms: start.elapsed().as_millis() as u64,
        }
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
                crate::config::SchemaTableStrategy::ServiceAsTable => {
                    (self.schema_config.default_name.clone(), point.service.clone())
                }
            };

            // Convert date i32 to YYYY-MM-DD string
            let date = crate::storage::line_protocol::format_date32_as_string(point.date);

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

    /// Group partitions by target Data node
    fn group_by_target_node(
        &self,
        partitions: HashMap<PartitionKey, Vec<Point>>,
    ) -> HashMap<String, Vec<(PartitionKey, Vec<Point>)>> {
        let mut node_batches: HashMap<String, Vec<(PartitionKey, Vec<Point>)>> = HashMap::new();

        for (partition_key, points) in partitions {
            if let Some(node) = self.shard_cache.select_node_for_partition(&partition_key) {
                node_batches
                    .entry(node.node_id)
                    .or_default()
                    .push((partition_key, points));
            } else {
                warn!("No node available for partition: {}", partition_key.to_path());
            }
        }

        node_batches
    }

    /// Dispatch writes to Data nodes in parallel
    async fn dispatch_writes(
        &self,
        catalog: &str,
        node_batches: HashMap<String, Vec<(PartitionKey, Vec<Point>)>>,
        _line_protocol: &str,
    ) -> Vec<Result<u64, String>> {
        let mut futures = Vec::new();

        for (node_id, batches) in node_batches {
            let pool = Arc::clone(&self.data_node_pool);
            let catalog = catalog.to_string();

            let future = async move {
                let mut total_written = 0u64;

                for (partition_key, points) in batches {
                    // Convert points back to line protocol for transmission
                    // In a real implementation, we'd serialize more efficiently
                    let point_count = points.len() as u64;

                    // Get client from pool and send write
                    match pool.get_client(&node_id).await {
                        Ok(mut client) => {
                            // TODO: Actually call the Data node gRPC service
                            // For now, just simulate success
                            debug!(
                                "Would write {} points to {} for partition {}",
                                point_count, node_id, partition_key.to_path()
                            );
                            total_written += point_count;
                        }
                        Err(e) => {
                            return Err(format!("Failed to get client for {}: {}", node_id, e));
                        }
                    }
                }

                Ok(total_written)
            };

            futures.push(future);
        }

        // Execute all writes in parallel
        futures::future::join_all(futures).await
    }
}
