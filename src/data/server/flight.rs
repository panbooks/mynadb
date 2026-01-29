//! Arrow Flight service implementation for Data Node
//!
//! This service handles internal communication from Router nodes using Arrow Flight protocol.
//! - DoPut: Receives writes (RecordBatches) from Router
//! - DoGet: Executes queries and returns results as RecordBatches

use crate::config::SchemaConfig;
use crate::data::cache::ParquetLocalCache;
use crate::data::replication::{ReplicationConfig, ReplicationManager};
use crate::meta::client::MetaClient;
use crate::meta::types::{NodeRole, NodeState, PartitionReplica};
use crate::query::query_engine::QueryEngine;
use crate::storage::cache::DataCache;
use crate::storage::line_protocol::parse_multiple_lines;
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use dashmap::DashMap;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Write request metadata sent via Flight Ticket (legacy line protocol format)
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

/// Replication write ticket - used by leader to send data to standby
/// This uses add_batch_standby which doesn't trigger flush
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationTicket {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub date: i32,
    pub seq: u64,
    #[serde(default)]
    pub source_node_id: String,
    /// Marker to identify this as replication data
    pub is_replication: bool,
    /// CRC32 checksum of the batch data for integrity verification
    #[serde(default)]
    pub checksum: Option<u32>,
    /// Total bytes in the batch (for verification)
    #[serde(default)]
    pub total_bytes: Option<u64>,
}

/// Convert DataFusion DataType to Arrow DataType
fn convert_data_type(dt: &arrow::datatypes::DataType) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::DataType as DfDataType;

    match dt {
        DfDataType::Null => DataType::Null,
        DfDataType::Boolean => DataType::Boolean,
        DfDataType::Int8 => DataType::Int8,
        DfDataType::Int16 => DataType::Int16,
        DfDataType::Int32 => DataType::Int32,
        DfDataType::Int64 => DataType::Int64,
        DfDataType::UInt8 => DataType::UInt8,
        DfDataType::UInt16 => DataType::UInt16,
        DfDataType::UInt32 => DataType::UInt32,
        DfDataType::UInt64 => DataType::UInt64,
        DfDataType::Float16 => DataType::Float16,
        DfDataType::Float32 => DataType::Float32,
        DfDataType::Float64 => DataType::Float64,
        DfDataType::Utf8 => DataType::Utf8,
        DfDataType::LargeUtf8 => DataType::LargeUtf8,
        DfDataType::Utf8View => DataType::Utf8View,
        DfDataType::Binary => DataType::Binary,
        DfDataType::LargeBinary => DataType::LargeBinary,
        DfDataType::BinaryView => DataType::BinaryView,
        DfDataType::Timestamp(unit, tz) => DataType::Timestamp(*unit, tz.clone()),
        DfDataType::Date32 => DataType::Date32,
        DfDataType::Date64 => DataType::Date64,
        other => other.clone(), // Pass through any other types directly
    }
}

/// Data Node Flight service
pub struct DataNodeFlightService {
    /// Data cache for writes
    cache: Arc<DataCache>,

    /// Query engine for executing queries
    query_engine: Arc<QueryEngine>,

    /// Meta client for coordination
    meta_client: Option<Arc<MetaClient>>,

    /// Schema configuration
    schema_config: SchemaConfig,

    /// Node ID
    node_id: String,

    /// Start time for uptime calculation
    start_time: Instant,

    /// Health status
    status: Arc<RwLock<String>>,

    /// Local parquet file cache for cluster mode
    /// Caches files owned by this node for fast repeated access
    parquet_cache: Option<Arc<ParquetLocalCache>>,

    /// Replication manager for warm standby replication
    /// Sends writes to standby nodes for zero-downtime failover
    replication_manager: Option<Arc<ReplicationManager>>,

    /// Cached partition replicas (partition_key -> PartitionReplica)
    /// Updated via Meta event subscription for efficient replication routing
    replica_cache: Arc<DashMap<String, PartitionReplica>>,
}

impl DataNodeFlightService {
    pub fn new(
        cache: Arc<DataCache>,
        query_engine: Arc<QueryEngine>,
        meta_client: Option<Arc<MetaClient>>,
        schema_config: SchemaConfig,
        node_id: String,
    ) -> Self {
        Self {
            cache,
            query_engine,
            meta_client,
            schema_config,
            node_id,
            start_time: Instant::now(),
            status: Arc::new(RwLock::new("ready".to_string())),
            parquet_cache: None,
            replication_manager: None,
            replica_cache: Arc::new(DashMap::new()),
        }
    }

    /// Get a reference to the replica cache for external event subscription
    pub fn replica_cache(&self) -> Arc<DashMap<String, PartitionReplica>> {
        Arc::clone(&self.replica_cache)
    }

    /// Set the parquet local cache for cluster mode
    pub fn with_parquet_cache(mut self, cache: Arc<ParquetLocalCache>) -> Self {
        self.parquet_cache = Some(cache);
        self
    }

    /// Set the replication manager for warm standby replication
    pub fn with_replication_manager(mut self, manager: Arc<ReplicationManager>) -> Self {
        self.replication_manager = Some(manager);
        self
    }

    /// Get standbys for a specific partition from cached PartitionReplica info
    ///
    /// This is the partition-aware replacement for the old brute-force get_standbys().
    /// Instead of replicating to ALL data nodes, we only replicate to the assigned standbys.
    ///
    /// Returns list of (node_id, flight_addr) pairs for the partition's standby nodes.
    async fn get_standbys_for_partition(&self, partition_key: &str) -> Vec<(String, String)> {
        let meta_client = match &self.meta_client {
            Some(c) => c,
            None => return Vec::new(),
        };

        // 1. Check local cache first for fast path
        if let Some(replica) = self.replica_cache.get(partition_key) {
            // Only replicate if we are the leader
            if replica.leader_node_id != self.node_id {
                debug!(
                    "Not leader for partition {} (leader={}), skipping replication",
                    partition_key, replica.leader_node_id
                );
                return Vec::new();
            }

            // Get flight addresses for standby nodes
            return self.get_node_addresses(&replica.standby_node_ids).await;
        }

        // 2. Cache miss - query Meta and cache result
        match meta_client.get_partition_replica(partition_key).await {
            Ok(Some(replica)) => {
                // Only replicate if we are the leader
                if replica.leader_node_id != self.node_id {
                    debug!(
                        "Not leader for partition {} (leader={}), skipping replication",
                        partition_key, replica.leader_node_id
                    );
                    self.replica_cache
                        .insert(partition_key.to_string(), replica);
                    return Vec::new();
                }

                let standbys = self.get_node_addresses(&replica.standby_node_ids).await;
                self.replica_cache
                    .insert(partition_key.to_string(), replica);
                standbys
            }
            Ok(None) => {
                // No replica assignment exists - trigger auto-assignment
                debug!(
                    "No replica for partition {}, triggering auto-assignment",
                    partition_key
                );
                match meta_client.auto_assign_replica(partition_key).await {
                    Ok(replica) => {
                        // Check if we were assigned as leader
                        if replica.leader_node_id != self.node_id {
                            debug!(
                                "Auto-assigned as standby for partition {} (leader={})",
                                partition_key, replica.leader_node_id
                            );
                            self.replica_cache
                                .insert(partition_key.to_string(), replica);
                            return Vec::new();
                        }

                        let standbys = self.get_node_addresses(&replica.standby_node_ids).await;
                        self.replica_cache
                            .insert(partition_key.to_string(), replica);
                        standbys
                    }
                    Err(e) => {
                        warn!("Failed to auto-assign replica for {}: {}", partition_key, e);
                        Vec::new()
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to get replica from Meta for {}: {}. Skipping replication.",
                    partition_key, e
                );
                Vec::new()
            }
        }
    }

    /// Helper: Get flight addresses for node IDs
    async fn get_node_addresses(&self, node_ids: &[String]) -> Vec<(String, String)> {
        if node_ids.is_empty() {
            return Vec::new();
        }

        if let Some(ref meta_client) = self.meta_client {
            match meta_client
                .list_nodes(Some(NodeRole::Data), Some(NodeState::Active))
                .await
            {
                Ok(nodes) => node_ids
                    .iter()
                    .filter_map(|id| {
                        nodes
                            .iter()
                            .find(|n| &n.node_id == id)
                            .and_then(|n| n.flight_addr.clone().map(|addr| (id.clone(), addr)))
                    })
                    .collect(),
                Err(e) => {
                    warn!("Failed to get node addresses from Meta: {}", e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    }

    /// Create a FlightServiceServer from this service
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Set the service status
    pub async fn set_status(&self, status: &str) {
        *self.status.write().await = status.to_string();
    }

    /// Parse line protocol and add points to cache
    async fn ingest_line_protocol(
        &self,
        catalog: &str,
        line_protocol: &str,
    ) -> Result<usize, String> {
        let points = parse_multiple_lines(line_protocol, &self.schema_config);

        if points.is_empty() {
            return Err("No valid points to write".to_string());
        }

        let point_count = points.len();

        // Group by schema and table based on strategy
        let mut grouped: HashMap<(String, String), Vec<_>> = HashMap::new();
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

            grouped.entry((schema, table)).or_default().push(point);
        }

        // Add to cache
        for ((schema, table), points) in grouped {
            self.cache
                .add_points(catalog, &schema, &table, points)
                .await
                .map_err(|e| format!("Cache error: {}", e))?;
        }

        Ok(point_count)
    }

    /// Handle new BatchWriteTicket format - ingest RecordBatch directly to cache
    async fn handle_batch_write(
        &self,
        ticket: BatchWriteTicket,
        batches: Vec<RecordBatch>,
        start: Instant,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>>,
        Status,
    > {
        debug!(
            "DoPut batch write: catalog={}, schema={}, table={}, date={}, fencing_token={:?}",
            ticket.catalog, ticket.schema, ticket.table, ticket.date, ticket.fencing_token
        );

        // Validate fencing token if provided
        if let Some(token) = ticket.fencing_token {
            self.validate_fencing_token(
                token,
                &ticket.catalog,
                Some(&format!(
                    "{}/{}/{}/date={}",
                    ticket.catalog,
                    ticket.schema,
                    ticket.table,
                    crate::storage::line_protocol::format_date32_as_string(ticket.date)
                )),
            )
            .await?;
        }

        // Check if replication is enabled - if so, clone batches for replication
        let replication_enabled = self.replication_manager.is_some();
        let batches_for_replication: Vec<RecordBatch> = if replication_enabled {
            batches
                .iter()
                .filter(|b| b.num_rows() > 0)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

        // Add RecordBatches directly to cache
        let mut total_rows = 0u64;

        for batch in batches {
            let row_count = batch.num_rows() as u64;
            if row_count == 0 {
                continue;
            }

            match self
                .cache
                .add_batch(
                    &ticket.catalog,
                    &ticket.schema,
                    &ticket.table,
                    ticket.date,
                    batch,
                )
                .await
            {
                Ok(_) => {
                    total_rows += row_count;
                }
                Err(e) => {
                    error!(
                        "Failed to add batch to cache for {}/{}/{}: {:?}",
                        ticket.catalog, ticket.schema, ticket.table, e
                    );
                    return Err(Status::internal(format!("Cache error: {:?}", e)));
                }
            }
        }

        // Replicate to partition-specific standbys if enabled
        if let Some(ref replication_manager) = self.replication_manager {
            let partition_key = format!(
                "{}/{}/{}/date={}",
                ticket.catalog,
                ticket.schema,
                ticket.table,
                crate::storage::line_protocol::format_date32_as_string(ticket.date)
            );

            // Use partition-aware standby lookup (not brute-force ALL nodes)
            let standbys = self.get_standbys_for_partition(&partition_key).await;
            if !standbys.is_empty() {
                for batch in batches_for_replication {
                    replication_manager.replicate_to_standbys(
                        &partition_key,
                        &standbys,
                        &ticket.catalog,
                        &ticket.schema,
                        &ticket.table,
                        ticket.date,
                        batch,
                    );
                }

                debug!(
                    "Replicated {} rows to {} assigned standbys for partition {}",
                    total_rows,
                    standbys.len(),
                    partition_key
                );
            }
        }

        let elapsed = start.elapsed();
        info!(
            "DoPut batch completed: {} rows to {}/{}/{} in {:?}",
            total_rows, ticket.catalog, ticket.schema, ticket.table, elapsed
        );

        // Return success result
        let result = PutResult {
            app_metadata: format!(
                "{{\"rows_written\":{},\"points_written\":{}}}",
                total_rows, total_rows
            )
            .into_bytes()
            .into(),
        };
        let output = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(output)))
    }

    /// Handle legacy WriteTicket format - parse line protocol from RecordBatch column
    async fn handle_legacy_write(
        &self,
        ticket: WriteTicket,
        batches: Vec<RecordBatch>,
        start: Instant,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>>,
        Status,
    > {
        debug!(
            "DoPut legacy write: catalog={}, fencing_token={:?}",
            ticket.catalog, ticket.fencing_token
        );

        // Validate fencing token if provided
        if let Some(token) = ticket.fencing_token {
            self.validate_fencing_token(token, &ticket.catalog, ticket.partition_key.as_deref())
                .await?;
        }

        // Process batches with line_protocol column
        let mut total_points = 0u64;

        for batch in &batches {
            // Check if this is a line protocol batch (has a single 'line_protocol' column)
            if batch.num_columns() == 1 && batch.schema().field(0).name() == "line_protocol" {
                // Extract line protocol strings and ingest
                let col = batch.column(0);
                if let Some(str_array) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                    for i in 0..str_array.len() {
                        if let Some(line) = str_array.value(i).into() {
                            match self.ingest_line_protocol(&ticket.catalog, line).await {
                                Ok(points) => total_points += points as u64,
                                Err(e) => {
                                    error!("Ingest error: {}", e);
                                }
                            }
                        }
                    }
                }
            } else {
                warn!(
                    "Received RecordBatch with {} columns but no schema/table metadata. \
                     Use BatchWriteTicket for direct RecordBatch ingestion.",
                    batch.num_columns()
                );
            }
        }

        let elapsed = start.elapsed();
        info!(
            "DoPut legacy completed: {} points in {:?}",
            total_points, elapsed
        );

        // Return success result
        let result = PutResult {
            app_metadata: format!("{{\"points_written\":{}}}", total_points)
                .into_bytes()
                .into(),
        };
        let output = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(output)))
    }

    /// Validate fencing token against Meta service
    async fn validate_fencing_token(
        &self,
        token: u64,
        catalog: &str,
        partition_key: Option<&str>,
    ) -> Result<(), Status> {
        if let Some(ref meta_client) = self.meta_client {
            // Use partition_key if provided, otherwise fall back to catalog-level lock
            let lock_key = partition_key
                .map(|pk| format!("/partitions/{}", pk))
                .unwrap_or_else(|| format!("/partitions/{}", catalog));

            match meta_client.get_lock(&lock_key).await {
                Ok(Some(lock_info)) => {
                    if lock_info.fencing_token != token {
                        warn!(
                            "Stale fencing token for {}: received {} but current is {}",
                            lock_key, token, lock_info.fencing_token
                        );
                        return Err(Status::failed_precondition(format!(
                            "Stale fencing token: write rejected to prevent split-brain. \
                             Received token {} but partition lock has token {}",
                            token, lock_info.fencing_token
                        )));
                    }
                    debug!(
                        "Fencing token {} validated for partition {}",
                        token, lock_key
                    );
                }
                Ok(None) => {
                    warn!(
                        "No lock held for partition {}, rejecting write with token {}",
                        lock_key, token
                    );
                    return Err(Status::failed_precondition(format!(
                        "No lock held for partition {}. The Router may have lost its lock.",
                        lock_key
                    )));
                }
                Err(e) => {
                    // Meta service unavailable - fail-open with warning
                    warn!(
                        "Failed to validate fencing token for {}: {}. Allowing write (fail-open).",
                        lock_key, e
                    );
                }
            }
        } else {
            // No meta client - standalone mode, skip validation
            debug!(
                "Fencing token {} provided but no meta client (standalone mode)",
                token
            );
        }
        Ok(())
    }

    /// Handle replication write from leader to standby
    ///
    /// This uses add_batch_standby which:
    /// - Adds data to memtable (for fast failover)
    /// - Does NOT trigger flush (only leader writes Parquet)
    /// - Does NOT write to local WAL (leader has authoritative WAL)
    /// - Verifies CRC32 checksum if provided to detect corruption
    async fn handle_replication_write(
        &self,
        ticket: ReplicationTicket,
        batches: Vec<RecordBatch>,
        raw_bytes: Option<Vec<u8>>,
        start: Instant,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>>,
        Status,
    > {
        debug!(
            "Replication write from {}: {}/{}/{} date={} seq={} checksum={:?}",
            ticket.source_node_id,
            ticket.catalog,
            ticket.schema,
            ticket.table,
            ticket.date,
            ticket.seq,
            ticket.checksum
        );

        // Verify checksum if provided
        let checksum_verified = if let (Some(expected_checksum), Some(ref data)) =
            (ticket.checksum, &raw_bytes)
        {
            use crate::data::replication::types::compute_checksum;
            let actual_checksum = compute_checksum(data);
            if actual_checksum != expected_checksum {
                let partition_key = format!(
                    "{}/{}/{}/date={}",
                    ticket.catalog,
                    ticket.schema,
                    ticket.table,
                    crate::storage::line_protocol::format_date32_as_string(ticket.date)
                );
                error!(
                    "Checksum mismatch for replication seq {} partition {}: expected {:#x}, got {:#x}",
                    ticket.seq, partition_key, expected_checksum, actual_checksum
                );
                // Return error response instead of applying corrupted data
                let response = serde_json::json!({
                    "standby_node_id": self.node_id,
                    "last_applied_seq": ticket.seq.saturating_sub(1),
                    "rows_applied": 0,
                    "error": format!("Checksum mismatch: expected {:#x}, got {:#x}", expected_checksum, actual_checksum),
                    "checksum_verified": false
                });
                let result = PutResult {
                    app_metadata: serde_json::to_vec(&response).unwrap_or_default().into(),
                };
                let output = stream::once(async { Ok(result) });
                return Ok(Response::new(Box::pin(output)));
            }
            debug!(
                "Checksum verified for seq {} (checksum: {:#x})",
                ticket.seq, actual_checksum
            );
            true
        } else {
            // No checksum provided or no raw bytes - cannot verify
            ticket.checksum.is_none()
        };

        let mut total_rows = 0u64;

        for batch in batches {
            let row_count = batch.num_rows() as u64;
            if row_count == 0 {
                continue;
            }

            // Use add_batch_standby which doesn't trigger flush
            match self
                .cache
                .add_batch_standby(
                    &ticket.catalog,
                    &ticket.schema,
                    &ticket.table,
                    ticket.date,
                    batch,
                )
                .await
            {
                Ok(_) => {
                    total_rows += row_count;
                }
                Err(e) => {
                    error!(
                        "Failed to apply replication for {}/{}/{}: {:?}",
                        ticket.catalog, ticket.schema, ticket.table, e
                    );
                    return Err(Status::internal(format!("Replication error: {:?}", e)));
                }
            }
        }

        let elapsed = start.elapsed();
        debug!(
            "Replication write completed: {} rows from {} in {:?}",
            total_rows, ticket.source_node_id, elapsed
        );

        // Return response with applied seq for ACK tracking
        let response = serde_json::json!({
            "standby_node_id": self.node_id,
            "last_applied_seq": ticket.seq,
            "rows_applied": total_rows,
            "error": null,
            "checksum_verified": checksum_verified
        });

        let result = PutResult {
            app_metadata: serde_json::to_vec(&response).unwrap_or_default().into(),
        };
        let output = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(output)))
    }
}

#[tonic::async_trait]
impl FlightService for DataNodeFlightService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    /// Handshake - basic connection establishment
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: self.node_id.clone().into_bytes().into(),
        };
        let output = stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(output)))
    }

    /// List available flights (not used for internal communication)
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let output = stream::empty();
        Ok(Response::new(Box::pin(output)))
    }

    /// Get flight info - returns schema for a query
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        // Parse query ticket from command
        let ticket: QueryTicket = serde_json::from_slice(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(format!("Invalid query ticket: {}", e)))?;

        // Get schema from query
        let schema = self
            .query_engine
            .get_query_schema(&ticket.catalog, &ticket.sql)
            .await
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?
            .with_descriptor(descriptor)
            .with_endpoint(
                arrow_flight::FlightEndpoint::new()
                    .with_ticket(Ticket::new(serde_json::to_vec(&ticket).unwrap())),
            );

        Ok(Response::new(flight_info))
    }

    /// Get schema for a query
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        let ticket: QueryTicket = serde_json::from_slice(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(format!("Invalid query ticket: {}", e)))?;

        let schema = self
            .query_engine
            .get_query_schema(&ticket.catalog, &ticket.sql)
            .await
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        use arrow_flight::IpcMessage;
        let options = IpcWriteOptions::default();
        let schema_ref: &Schema = &schema;
        let message = IpcMessage::try_from(SchemaAsIpc::new(schema_ref, &options))
            .map_err(|e| Status::internal(format!("Schema encoding error: {}", e)))?;

        let schema_result = SchemaResult { schema: message.0 };
        Ok(Response::new(schema_result))
    }

    /// DoGet - Execute query and return results
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_data = request.into_inner();
        let start = Instant::now();

        // Parse query ticket
        let ticket: QueryTicket = serde_json::from_slice(&ticket_data.ticket)
            .map_err(|e| Status::invalid_argument(format!("Invalid query ticket: {}", e)))?;

        // Log query details (file-based routing shows file count, broadcast shows none)
        if let Some(ref files) = ticket.file_paths {
            debug!(
                "DoGet query (file-based): catalog={}, files={}, sql={}",
                ticket.catalog,
                files.len(),
                ticket.sql.chars().take(100).collect::<String>()
            );
            // File-based routing: Router has already determined this node should read these files.
            // The files are stored with node_id prefix (e.g., "data-1/mydb/default/cpu/date=.../L0_xxx.parquet")
            // and this node will find them in its local storage when scanning.
            //
            // Future optimization: Implement direct file-list query to avoid directory scanning
            // and leverage local parquet cache for maximum performance.
            info!(
                "File-based query from Router: {} files to read for {}",
                files.len(),
                ticket.catalog
            );
        } else {
            debug!(
                "DoGet query (broadcast): catalog={}, sql={}",
                ticket.catalog,
                ticket.sql.chars().take(100).collect::<String>()
            );
        }

        // Execute query
        // Note: For file-based routing, the files are already local to this node (prefixed with node_id).
        // The QueryEngine will find them during table registration/scanning.
        let df_batches = self
            .query_engine
            .execute_query_with_db(&ticket.sql, &ticket.catalog)
            .await
            .map_err(|e| Status::internal(format!("Query execution error: {}", e)))?;

        let elapsed = start.elapsed();
        debug!(
            "Query executed: {} batches in {:?}",
            df_batches.len(),
            elapsed
        );

        if df_batches.is_empty() {
            let output = stream::empty();
            return Ok(Response::new(Box::pin(output)));
        }

        // Convert DataFusion RecordBatches to Arrow RecordBatches
        let arrow_batches: Vec<RecordBatch> = df_batches
            .into_iter()
            .map(|df_batch| {
                let schema = df_batch.schema();
                let arrow_schema = arrow::datatypes::Schema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|f| {
                            arrow::datatypes::Field::new(
                                f.name(),
                                convert_data_type(f.data_type()),
                                f.is_nullable(),
                            )
                        })
                        .collect::<Vec<_>>(),
                );

                RecordBatch::try_new(Arc::new(arrow_schema), df_batch.columns().to_vec()).unwrap()
            })
            .collect();

        // Encode as FlightData stream
        let batch_stream = stream::iter(
            arrow_batches
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    /// DoPut - Receive writes from Router
    ///
    /// Supports two formats:
    /// 1. BatchWriteTicket (new) - Pre-converted RecordBatch from Router, ingested directly
    /// 2. WriteTicket (legacy) - Line protocol in a "line_protocol" column, parsed on Data node
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        let start = Instant::now();

        // First message should contain the descriptor with write metadata
        let first_data = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?
            .map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

        // Extract and clone the descriptor cmd before moving first_data
        let descriptor_cmd = first_data
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| {
                Status::invalid_argument("Missing flight descriptor with write metadata")
            })?
            .cmd
            .clone();

        // Check if this is a replication request (need raw bytes for checksum verification)
        let is_replication = serde_json::from_slice::<ReplicationTicket>(&descriptor_cmd)
            .map(|t| t.is_replication)
            .unwrap_or(false);

        // Collect all flight data
        let mut all_data = vec![first_data];
        while let Some(data) = stream.next().await {
            all_data.push(data.map_err(|e| Status::internal(format!("Stream error: {}", e)))?);
        }

        // For replication, capture raw bytes for checksum verification BEFORE decoding
        let raw_bytes: Option<Vec<u8>> = if is_replication {
            let mut bytes = Vec::new();
            for fd in &all_data {
                bytes.extend_from_slice(&fd.data_header);
                bytes.extend_from_slice(&fd.data_body);
            }
            Some(bytes)
        } else {
            None
        };

        // Decode the flight data stream into RecordBatches
        let flight_stream = FlightRecordBatchStream::new_from_flight_data(stream::iter(
            all_data.into_iter().map(Ok),
        ));

        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Failed to decode batches: {}", e)))?;

        // Try to parse as ReplicationTicket first (standby receiving from leader)
        if let Ok(repl_ticket) = serde_json::from_slice::<ReplicationTicket>(&descriptor_cmd) {
            if repl_ticket.is_replication {
                return self
                    .handle_replication_write(repl_ticket, batches, raw_bytes, start)
                    .await;
            }
        }

        // Try to parse as BatchWriteTicket (new efficient format from Router)
        if let Ok(batch_ticket) = serde_json::from_slice::<BatchWriteTicket>(&descriptor_cmd) {
            return self.handle_batch_write(batch_ticket, batches, start).await;
        }

        // Fall back to legacy WriteTicket format
        let ticket: WriteTicket = serde_json::from_slice(&descriptor_cmd)
            .map_err(|e| Status::invalid_argument(format!("Invalid write ticket: {}", e)))?;

        self.handle_legacy_write(ticket, batches, start).await
    }

    /// DoAction - Handle administrative actions
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "FLUSH" => {
                // Trigger cache flush
                match self.cache.flush().await {
                    Ok(results) => {
                        let response =
                            format!("{{\"files_flushed\":{},\"success\":true}}", results.len());
                        let result = arrow_flight::Result {
                            body: response.into_bytes().into(),
                        };
                        let output = stream::once(async { Ok(result) });
                        Ok(Response::new(Box::pin(output)))
                    }
                    Err(e) => Err(Status::internal(format!("Flush failed: {}", e))),
                }
            }
            "HEALTH" => {
                let status = self.status.read().await.clone();
                let uptime = self.start_time.elapsed().as_secs();
                let response = format!(
                    "{{\"healthy\":{},\"status\":\"{}\",\"uptime_secs\":{},\"node_id\":\"{}\"}}",
                    status == "ready",
                    status,
                    uptime,
                    self.node_id
                );
                let result = arrow_flight::Result {
                    body: response.into_bytes().into(),
                };
                let output = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(output)))
            }
            "STATS" => {
                let write_stats = self.cache.stats().await;
                let mut response_json = serde_json::json!({
                    "write_cache": {
                        "size_bytes": write_stats.size_bytes,
                        "entries": write_stats.num_measurements,
                        "total_rows": write_stats.total_rows,
                    }
                });

                // Add parquet cache stats if available
                if let Some(ref parquet_cache) = self.parquet_cache {
                    let parquet_stats = parquet_cache.stats();
                    response_json["parquet_cache"] = serde_json::json!({
                        "current_size_bytes": parquet_stats.current_size_bytes,
                        "max_size_bytes": parquet_stats.max_size_bytes,
                        "file_count": parquet_stats.file_count,
                        "utilization_percent": parquet_stats.utilization_percent(),
                    });
                }

                let result = arrow_flight::Result {
                    body: serde_json::to_vec(&response_json)
                        .unwrap_or_default()
                        .into(),
                };
                let output = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(output)))
            }
            _ => Err(Status::unimplemented(format!(
                "Action '{}' not implemented",
                action.r#type
            ))),
        }
    }

    /// List available actions
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "FLUSH".to_string(),
                description: "Flush cache to Parquet files".to_string(),
            },
            ActionType {
                r#type: "HEALTH".to_string(),
                description: "Get node health status".to_string(),
            },
            ActionType {
                r#type: "STATS".to_string(),
                description: "Get node statistics".to_string(),
            },
        ];
        let output = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(output)))
    }

    /// DoExchange - bidirectional streaming (not used)
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not implemented"))
    }

    /// Poll flight info (not used)
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}

/// Start the Data Node Flight server
pub async fn start_flight_server(
    addr: std::net::SocketAddr,
    service: DataNodeFlightService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting Data Node Flight server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
