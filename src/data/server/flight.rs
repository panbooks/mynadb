//! Arrow Flight service implementation for Data Node
//!
//! This service handles internal communication from Router nodes using Arrow Flight protocol.
//! - DoPut: Receives writes (RecordBatches) from Router
//! - DoGet: Executes queries and returns results as RecordBatches

use crate::config::SchemaConfig;
use crate::meta::client::MetaClient;
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
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Write request metadata sent via Flight Ticket
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteTicket {
    pub catalog: String,
    pub fencing_token: Option<u64>,
}

/// Query request metadata sent via Flight Ticket
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryTicket {
    pub catalog: String,
    pub sql: String,
    pub partitions: Option<Vec<String>>,
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
        DfDataType::Binary => DataType::Binary,
        DfDataType::LargeBinary => DataType::LargeBinary,
        DfDataType::Timestamp(unit, tz) => DataType::Timestamp(*unit, tz.clone()),
        DfDataType::Date32 => DataType::Date32,
        DfDataType::Date64 => DataType::Date64,
        _ => DataType::Utf8, // Fallback for unsupported types
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
                crate::config::SchemaTableStrategy::ServiceAsTable => {
                    (self.schema_config.default_name.clone(), point.service.clone())
                }
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

        debug!(
            "DoGet query: catalog={}, sql={}",
            ticket.catalog,
            ticket.sql.chars().take(100).collect::<String>()
        );

        // Execute query
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

        // Parse write ticket from flight descriptor
        let ticket: WriteTicket = if let Some(ref descriptor) = first_data.flight_descriptor {
            serde_json::from_slice(&descriptor.cmd)
                .map_err(|e| Status::invalid_argument(format!("Invalid write ticket: {}", e)))?
        } else {
            return Err(Status::invalid_argument(
                "Missing flight descriptor with write metadata",
            ));
        };

        debug!(
            "DoPut write: catalog={}, fencing_token={:?}",
            ticket.catalog, ticket.fencing_token
        );

        // Validate fencing token if provided and meta client available
        if let (Some(token), Some(ref _meta_client)) = (ticket.fencing_token, &self.meta_client) {
            // TODO: Validate fencing token against Meta service
            debug!("Fencing token: {}", token);
        }

        // Collect all flight data and decode as RecordBatches
        let mut all_data = vec![first_data];
        while let Some(data) = stream.next().await {
            all_data.push(data.map_err(|e| Status::internal(format!("Stream error: {}", e)))?);
        }

        // Decode the flight data stream into RecordBatches
        let flight_stream = FlightRecordBatchStream::new_from_flight_data(
            stream::iter(all_data.into_iter().map(Ok)),
        );

        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Failed to decode batches: {}", e)))?;

        // If we received RecordBatches, add them directly to cache
        // For now, we expect line protocol in a specific column or handle raw batches
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
                // TODO: Handle direct RecordBatch writes (schema-aware ingestion)
                // For now, we only support line protocol format
                warn!(
                    "Received RecordBatch with {} columns, expected line_protocol format",
                    batch.num_columns()
                );
            }
        }

        let elapsed = start.elapsed();
        info!(
            "DoPut completed: {} points in {:?}",
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
                        let response = format!(
                            "{{\"files_flushed\":{},\"success\":true}}",
                            results.len()
                        );
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
                let stats = self.cache.stats().await;
                let response = format!(
                    "{{\"cache_size_bytes\":{},\"cache_entries\":{},\"total_rows\":{}}}",
                    stats.size_bytes, stats.num_measurements, stats.total_rows
                );
                let result = arrow_flight::Result {
                    body: response.into_bytes().into(),
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
