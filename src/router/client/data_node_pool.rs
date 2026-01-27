//! Data Node Connection Pool
//!
//! Manages Arrow Flight connections to Data nodes with connection pooling.

use super::circuit_breaker::CircuitBreaker;
use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::{
    decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder,
    flight_service_client::FlightServiceClient, Action, FlightData, FlightDescriptor, PutResult,
    Ticket,
};
use dashmap::DashMap;
use futures::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Write request metadata sent via Flight Ticket
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteTicket {
    pub catalog: String,
    pub fencing_token: Option<u64>,
    /// Optional partition key for fencing token validation
    /// Format: catalog/schema/table/date (e.g., "mydb/default/cpu/2024-01-15")
    #[serde(default)]
    pub partition_key: Option<String>,
}

/// Batch write request metadata sent via Flight Descriptor
/// Used when Router sends pre-converted RecordBatch to Data node
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchWriteTicket {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date: i32, // Days since Unix epoch (Date32 format)
    pub fencing_token: Option<u64>,
}

/// Query request metadata sent via Flight Ticket
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryTicket {
    pub catalog: String,
    pub sql: String,
    pub partitions: Option<Vec<String>>,
    /// Explicit file paths to read (for distributed parallel queries)
    /// When set, the Data node reads ONLY these specific files.
    /// Format: ["node-1/mydb/default/cpu/date=2024-01-15/L0_xxx.parquet", ...]
    #[serde(default)]
    pub file_paths: Option<Vec<String>>,
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Max connections per Data node
    pub max_connections: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
        }
    }
}

/// Data node address info
#[derive(Debug, Clone)]
pub struct DataNodeAddr {
    pub node_id: String,
    pub flight_addr: String,
}

/// Connection pool for Data nodes (Arrow Flight)
pub struct DataNodePool {
    /// Node ID → Flight channel mapping
    channels: DashMap<String, Channel>,

    /// Node ID → address mapping
    addresses: DashMap<String, String>,

    /// Node ID → circuit breaker mapping
    circuit_breakers: DashMap<String, Arc<CircuitBreaker>>,

    /// Pool configuration
    config: PoolConfig,
}

impl DataNodePool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            channels: DashMap::new(),
            addresses: DashMap::new(),
            circuit_breakers: DashMap::new(),
            config,
        }
    }

    /// Add or update a Data node address
    pub fn add_node(&self, node_id: &str, flight_addr: &str) {
        info!("Adding data node to pool: {} at {}", node_id, flight_addr);
        self.addresses
            .insert(node_id.to_string(), flight_addr.to_string());
        // Initialize circuit breaker for new nodes
        self.circuit_breakers
            .entry(node_id.to_string())
            .or_insert_with(|| Arc::new(CircuitBreaker::new()));
    }

    /// Remove a Data node
    pub fn remove_node(&self, node_id: &str) {
        info!("Removing data node from pool: {}", node_id);
        self.addresses.remove(node_id);
        self.channels.remove(node_id);
        self.circuit_breakers.remove(node_id);
    }

    /// Check if a node's circuit breaker allows requests
    pub fn is_node_available(&self, node_id: &str) -> bool {
        self.circuit_breakers
            .get(node_id)
            .map(|cb| cb.is_available())
            .unwrap_or(true) // Allow if no circuit breaker exists
    }

    /// Get circuit breaker for a node (for external recording)
    pub fn get_circuit_breaker(&self, node_id: &str) -> Option<Arc<CircuitBreaker>> {
        self.circuit_breakers.get(node_id).map(|cb| cb.clone())
    }

    /// Record success for a node's circuit breaker
    pub fn record_success(&self, node_id: &str) {
        if let Some(cb) = self.circuit_breakers.get(node_id) {
            cb.record_success();
        }
    }

    /// Record failure for a node's circuit breaker
    pub fn record_failure(&self, node_id: &str) {
        if let Some(cb) = self.circuit_breakers.get(node_id) {
            cb.record_failure();
        }
    }

    /// Get a Flight client for a Data node
    pub async fn get_flight_client(
        &self,
        node_id: &str,
    ) -> Result<FlightServiceClient<Channel>, String> {
        let channel = self.get_or_create_channel(node_id).await?;
        Ok(FlightServiceClient::new(channel))
    }

    /// Get or create a channel to a Data node
    ///
    /// Checks circuit breaker before attempting connection.
    async fn get_or_create_channel(&self, node_id: &str) -> Result<Channel, String> {
        // Check circuit breaker first
        if !self.is_node_available(node_id) {
            return Err(format!(
                "Node {} circuit breaker is open (too many recent failures)",
                node_id
            ));
        }

        // Check if we have a cached channel
        if let Some(channel) = self.channels.get(node_id) {
            return Ok(channel.clone());
        }

        // Get the address
        let addr = self
            .addresses
            .get(node_id)
            .map(|v| v.clone())
            .ok_or_else(|| format!("Unknown data node: {}", node_id))?;

        // Create a new channel
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("Invalid address {}: {}", addr, e))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        debug!("Connecting to data node {} at {}", node_id, addr);

        let channel = endpoint.connect().await.map_err(|e| {
            // Record connection failure
            self.record_failure(node_id);
            format!("Failed to connect to {}: {}", addr, e)
        })?;

        // Cache the channel
        self.channels.insert(node_id.to_string(), channel.clone());

        info!("Connected to data node {} at {}", node_id, addr);
        Ok(channel)
    }

    /// Send a write request to a Data node using Flight DoPut
    pub async fn write(
        &self,
        node_id: &str,
        catalog: &str,
        line_protocol: &str,
        fencing_token: Option<u64>,
        partition_key: Option<String>,
    ) -> Result<u64, String> {
        let mut client = self.get_flight_client(node_id).await?;

        // Create write ticket
        let ticket = WriteTicket {
            catalog: catalog.to_string(),
            fencing_token,
            partition_key,
        };

        // Create a RecordBatch with line protocol data
        let schema = Arc::new(Schema::new(vec![Field::new(
            "line_protocol",
            DataType::Utf8,
            false,
        )]));
        let line_array = StringArray::from(vec![line_protocol]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(line_array)])
            .map_err(|e| format!("Failed to create batch: {}", e))?;

        // Create flight descriptor with write metadata
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_vec(&ticket).unwrap());

        // Encode the batch as FlightData with descriptor
        let batch_stream = stream::iter(
            vec![batch]
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        // Collect into Vec<FlightData>
        let flight_data: Vec<FlightData> = flight_data_stream
            .try_collect()
            .await
            .map_err(|e| format!("Failed to encode flight data: {}", e))?;

        // Send via DoPut
        let request = tonic::Request::new(stream::iter(flight_data.into_iter()));
        let response = client
            .do_put(request)
            .await
            .map_err(|e| format!("DoPut failed: {}", e))?;

        // Read response
        let mut result_stream = response.into_inner();
        let mut points_written = 0u64;

        while let Some(result) = result_stream.next().await {
            let put_result = result.map_err(|e| format!("PutResult error: {}", e))?;
            // Parse the app_metadata JSON to get points_written
            if let Ok(metadata) =
                serde_json::from_slice::<serde_json::Value>(&put_result.app_metadata)
            {
                if let Some(points) = metadata["points_written"].as_u64() {
                    points_written = points;
                }
            }
        }

        Ok(points_written)
    }

    /// Send a pre-converted RecordBatch to a Data node using Flight DoPut
    ///
    /// This is the optimized write path where the Router has already parsed
    /// line protocol and converted Points to RecordBatch. The Data node can
    /// ingest the batch directly without re-parsing.
    ///
    /// Records success/failure to circuit breaker for fault tolerance.
    pub async fn write_batch(
        &self,
        node_id: &str,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batch: RecordBatch,
        fencing_token: Option<u64>,
    ) -> Result<u64, String> {
        let mut client = self.get_flight_client(node_id).await?;

        // Create batch write ticket with partition metadata
        let ticket = BatchWriteTicket {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date,
            fencing_token,
        };

        // Create flight descriptor with batch write metadata
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_vec(&ticket).unwrap());

        // Encode the RecordBatch as FlightData with descriptor
        let batch_stream = stream::iter(
            vec![batch]
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(batch_stream);

        // Collect into Vec<FlightData>
        let flight_data: Vec<FlightData> = flight_data_stream
            .try_collect()
            .await
            .map_err(|e| format!("Failed to encode flight data: {}", e))?;

        // Send via DoPut
        let request = tonic::Request::new(stream::iter(flight_data.into_iter()));
        let response = client.do_put(request).await.map_err(|e| {
            // Record failure for circuit breaker
            self.record_failure(node_id);
            // Clear cached channel on RPC error (might be stale)
            self.channels.remove(node_id);
            format!("DoPut failed: {}", e)
        })?;

        // Read response
        let mut result_stream = response.into_inner();
        let mut rows_written = 0u64;

        while let Some(result) = result_stream.next().await {
            let put_result = result.map_err(|e| {
                self.record_failure(node_id);
                format!("PutResult error: {}", e)
            })?;
            // Parse the app_metadata JSON to get rows_written
            if let Ok(metadata) =
                serde_json::from_slice::<serde_json::Value>(&put_result.app_metadata)
            {
                if let Some(rows) = metadata["rows_written"].as_u64() {
                    rows_written = rows;
                }
                // Also check points_written for backwards compatibility
                if let Some(points) = metadata["points_written"].as_u64() {
                    rows_written = points;
                }
            }
        }

        // Record success for circuit breaker
        self.record_success(node_id);

        Ok(rows_written)
    }

    /// Execute a query on a Data node using Flight DoGet
    pub async fn query(
        &self,
        node_id: &str,
        catalog: &str,
        sql: &str,
        partitions: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, String> {
        let mut client = self.get_flight_client(node_id).await?;

        // Create query ticket
        let ticket = QueryTicket {
            catalog: catalog.to_string(),
            sql: sql.to_string(),
            partitions,
            file_paths: None,
        };

        let ticket_bytes = serde_json::to_vec(&ticket)
            .map_err(|e| format!("Failed to serialize ticket: {}", e))?;

        // Send DoGet request
        let request = tonic::Request::new(Ticket::new(ticket_bytes));
        let response = client
            .do_get(request)
            .await
            .map_err(|e| format!("DoGet failed: {}", e))?;

        // Decode the response stream into RecordBatches
        // Map Status errors to FlightError for FlightRecordBatchStream
        let mapped_stream = response
            .into_inner()
            .map(|result| result.map_err(|status| arrow_flight::error::FlightError::Tonic(status)));
        let flight_stream = FlightRecordBatchStream::new_from_flight_data(mapped_stream);

        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| format!("Failed to decode batches: {}", e))?;

        Ok(batches)
    }

    /// Execute a query on a Data node with explicit file paths
    ///
    /// This method is used for distributed parallel queries where the Router
    /// tells each Data node exactly which files to read. This enables:
    /// - Node affinity: Each node reads only its own files (from local cache)
    /// - Parallel execution: Multiple nodes execute query simultaneously
    /// - Perfect cache utilization: 100% cache hit when node reads its own files
    pub async fn query_with_files(
        &self,
        node_id: &str,
        catalog: &str,
        sql: &str,
        file_paths: Vec<String>,
    ) -> Result<Vec<RecordBatch>, String> {
        let mut client = self.get_flight_client(node_id).await?;

        // Create query ticket with explicit file paths
        let ticket = QueryTicket {
            catalog: catalog.to_string(),
            sql: sql.to_string(),
            partitions: None,
            file_paths: Some(file_paths),
        };

        let ticket_bytes = serde_json::to_vec(&ticket)
            .map_err(|e| format!("Failed to serialize ticket: {}", e))?;

        // Send DoGet request
        let request = tonic::Request::new(Ticket::new(ticket_bytes));
        let response = client
            .do_get(request)
            .await
            .map_err(|e| format!("DoGet failed: {}", e))?;

        // Decode the response stream into RecordBatches
        let mapped_stream = response
            .into_inner()
            .map(|result| result.map_err(|status| arrow_flight::error::FlightError::Tonic(status)));
        let flight_stream = FlightRecordBatchStream::new_from_flight_data(mapped_stream);

        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| format!("Failed to decode batches: {}", e))?;

        Ok(batches)
    }

    /// Execute an action on a Data node (FLUSH, HEALTH, STATS)
    pub async fn do_action(
        &self,
        node_id: &str,
        action_type: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        let mut client = self.get_flight_client(node_id).await?;

        let action = Action {
            r#type: action_type.to_string(),
            body: body.to_vec().into(),
        };

        let request = tonic::Request::new(action);
        let response = client
            .do_action(request)
            .await
            .map_err(|e| format!("DoAction failed: {}", e))?;

        let mut results = Vec::new();
        let mut stream = response.into_inner();
        while let Some(result) = stream.next().await {
            let r = result.map_err(|e| format!("Action result error: {}", e))?;
            results.extend_from_slice(&r.body);
        }

        Ok(results)
    }

    /// Check if a node is in the pool
    pub fn has_node(&self, node_id: &str) -> bool {
        self.addresses.contains_key(node_id)
    }

    /// Get all node IDs in the pool
    pub fn get_node_ids(&self) -> Vec<String> {
        self.addresses.iter().map(|e| e.key().clone()).collect()
    }

    /// Get the number of nodes in the pool
    pub fn node_count(&self) -> usize {
        self.addresses.len()
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            node_count: self.addresses.len(),
            connected_count: self.channels.len(),
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub node_count: usize,
    pub connected_count: usize,
}
