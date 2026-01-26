//! Write Router - Routes write requests to appropriate Data nodes
//!
//! The WriteRouter is responsible for:
//! - Parsing line protocol to extract partition information
//! - Selecting target Data nodes based on partition affinity
//! - Dispatching writes to Data nodes in parallel via Arrow Flight
//! - Handling retries and failover

use super::shard_cache::{PartitionKey, ShardCache};
use crate::config::SchemaConfig;
use crate::router::client::DataNodePool;
use crate::storage::line_protocol::{parse_line_protocol, Point};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, warn};

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
    /// 1. Parses the line protocol (keeping original lines)
    /// 2. Groups points by partition key
    /// 3. Selects target Data nodes for each partition
    /// 4. Dispatches writes via Arrow Flight in parallel
    pub async fn route_write(
        &self,
        catalog: &str,
        line_protocol: &str,
    ) -> WriteResult {
        let start = Instant::now();

        // Parse line protocol, keeping original lines for routing
        let parsed_lines = self.parse_with_original_lines(line_protocol);
        if parsed_lines.is_empty() {
            return WriteResult {
                success: false,
                points_written: 0,
                bytes_written: 0,
                error: Some("No valid points in line protocol".to_string()),
                elapsed_ms: start.elapsed().as_millis() as u64,
            };
        }

        let total_points = parsed_lines.len();
        debug!("Routing {} points for catalog {}", total_points, catalog);

        // Group by partition key (with original line strings)
        let partitions = self.group_by_partition_with_lines(catalog, parsed_lines);

        // Group partitions by target Data node
        let node_batches = self.group_by_target_node_with_lines(partitions);

        if node_batches.is_empty() {
            return WriteResult {
                success: false,
                points_written: 0,
                bytes_written: 0,
                error: Some("No active data nodes available".to_string()),
                elapsed_ms: start.elapsed().as_millis() as u64,
            };
        }

        // Dispatch writes to Data nodes via Arrow Flight in parallel
        let results = self.dispatch_writes(catalog, node_batches).await;

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

    /// Parse line protocol and keep original lines for routing
    fn parse_with_original_lines(&self, line_protocol: &str) -> Vec<(String, Point)> {
        let mut results = Vec::new();
        for line in line_protocol.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Ok(point) = parse_line_protocol(line, &self.schema_config) {
                results.push((line.to_string(), point));
            }
        }
        results
    }

    /// Group points by partition key, keeping original line strings
    fn group_by_partition_with_lines(
        &self,
        catalog: &str,
        parsed_lines: Vec<(String, Point)>,
    ) -> HashMap<PartitionKey, Vec<String>> {
        let mut partitions: HashMap<PartitionKey, Vec<String>> = HashMap::new();

        for (original_line, point) in parsed_lines {
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

            partitions.entry(key).or_default().push(original_line);
        }

        partitions
    }

    /// Group partitions by target Data node
    fn group_by_target_node_with_lines(
        &self,
        partitions: HashMap<PartitionKey, Vec<String>>,
    ) -> HashMap<String, Vec<(PartitionKey, Vec<String>)>> {
        let mut node_batches: HashMap<String, Vec<(PartitionKey, Vec<String>)>> = HashMap::new();

        for (partition_key, lines) in partitions {
            if let Some(node) = self.shard_cache.select_node_for_partition(&partition_key) {
                node_batches
                    .entry(node.node_id)
                    .or_default()
                    .push((partition_key, lines));
            } else {
                warn!("No node available for partition: {}", partition_key.to_path());
            }
        }

        node_batches
    }

    /// Dispatch writes to Data nodes via Arrow Flight in parallel
    async fn dispatch_writes(
        &self,
        catalog: &str,
        node_batches: HashMap<String, Vec<(PartitionKey, Vec<String>)>>,
    ) -> Vec<Result<u64, String>> {
        let mut futures = Vec::new();

        for (node_id, batches) in node_batches {
            let pool = Arc::clone(&self.data_node_pool);
            let catalog = catalog.to_string();

            let future = async move {
                let mut total_written = 0u64;

                for (partition_key, lines) in batches {
                    let point_count = lines.len();

                    // Concatenate lines into line protocol batch
                    let batch_line_protocol = lines.join("\n");

                    let partition_path = partition_key.to_path();
                    debug!(
                        "Writing {} points to {} for partition {}",
                        point_count, node_id, partition_path
                    );

                    // Send write via Arrow Flight DoPut
                    // fencing_token is None for now - will be added when Meta lock integration is complete
                    // partition_key is passed for future fencing token validation
                    match pool
                        .write(
                            &node_id,
                            &catalog,
                            &batch_line_protocol,
                            None, // fencing_token - TODO: acquire from Meta lock
                            Some(partition_path.clone()),
                        )
                        .await
                    {
                        Ok(written) => {
                            debug!(
                                "Successfully wrote {} points to {} for partition {}",
                                written, node_id, partition_path
                            );
                            total_written += written;
                        }
                        Err(e) => {
                            error!(
                                "Failed to write to {} for partition {}: {}",
                                node_id, partition_path, e
                            );
                            return Err(format!(
                                "Failed to write to {} for partition {}: {}",
                                node_id, partition_path, e
                            ));
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
