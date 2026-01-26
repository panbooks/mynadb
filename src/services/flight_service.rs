use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService as Flight, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::{stream, Stream, TryStreamExt};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::query::query_engine::QueryEngine;

/// Convert DataFusion DataType to Arrow DataType
/// This is needed because DataFusion may use a different arrow version
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

/// Flight service implementation for DataFusion SQL queries
pub struct StatsFlightService {
    query_engine: Arc<QueryEngine>,
    current_db: Arc<RwLock<Option<String>>>,
}

impl StatsFlightService {
    pub fn new(query_engine: Arc<QueryEngine>) -> Self {
        Self {
            query_engine,
            current_db: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a FlightServiceServer from this service
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl Flight for StatsFlightService {
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

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: b"".to_vec().into(),
        };
        let output = stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(output)))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let output = stream::empty();
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        // Extract SQL query from command
        let sql = match descriptor.cmd.is_empty() {
            false => String::from_utf8(descriptor.cmd.to_vec())
                .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?,
            true => {
                return Err(Status::invalid_argument("No SQL command provided"));
            }
        };

        // Parse database from SQL or use current database
        let db = self.current_db.read().await.clone().ok_or_else(|| {
            Status::failed_precondition("No database selected. Use: USE <database>;")
        })?;

        // Get schema from query
        let schema = self
            .query_engine
            .get_query_schema(&db, &sql)
            .await
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        // Serialize schema using SchemaAsIpc
        let _schema_ipc = SchemaAsIpc::new(&schema, &Default::default());

        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?
            .with_descriptor(descriptor)
            .with_endpoint(
                arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(sql.into_bytes())),
            );

        Ok(Response::new(flight_info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;

        let db = self
            .current_db
            .read()
            .await
            .clone()
            .ok_or_else(|| Status::failed_precondition("No database selected"))?;

        let schema = self
            .query_engine
            .get_query_schema(&db, &sql)
            .await
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

        // Create SchemaResult using IpcMessage
        use arrow_flight::IpcMessage;
        let options = IpcWriteOptions::default();
        let schema_ref: &Schema = &schema; // Dereference Arc<Schema> to &Schema
        let message = IpcMessage::try_from(SchemaAsIpc::new(schema_ref, &options))
            .map_err(|e| Status::internal(format!("Schema encoding error: {}", e)))?;

        let schema_result = SchemaResult { schema: message.0 };

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // Extract SQL from ticket
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;

        // Check for USE database command
        if sql.trim().to_uppercase().starts_with("USE ") {
            let db_name = sql.trim()[4..].trim().trim_end_matches(';').to_string();
            *self.current_db.write().await = Some(db_name.clone());

            // Return empty stream for USE command
            let output = stream::empty();
            return Ok(Response::new(Box::pin(output)));
        }

        // Get current database
        let db = self.current_db.read().await.clone().ok_or_else(|| {
            Status::failed_precondition("No database selected. Use: USE <database>;")
        })?;

        // Execute query (returns datafusion::arrow RecordBatches)
        let df_batches = self
            .query_engine
            .execute_query_with_db(&sql, &db)
            .await
            .map_err(|e| Status::internal(format!("Query execution error: {}", e)))?;

        if df_batches.is_empty() {
            let output = stream::empty();
            return Ok(Response::new(Box::pin(output)));
        }

        // Convert DataFusion RecordBatches to Arrow RecordBatches
        // They have the same structure but different types due to version mismatch
        let arrow_batches: Vec<RecordBatch> = df_batches
            .into_iter()
            .map(|df_batch| {
                // Get schema and convert
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

                // Create new RecordBatch with same data
                RecordBatch::try_new(Arc::new(arrow_schema), df_batch.columns().to_vec()).unwrap()
            })
            .collect();

        // Use FlightDataEncoder
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

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("DoPut not implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "USE" => {
                let db_name = String::from_utf8(action.body.to_vec())
                    .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;
                *self.current_db.write().await = Some(db_name);

                let output = stream::empty();
                Ok(Response::new(Box::pin(output)))
            }
            _ => Err(Status::unimplemented(format!(
                "Action '{}' not implemented",
                action.r#type
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: "USE".to_string(),
            description: "Set current database: USE <database_name>".to_string(),
        }];
        let output = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not implemented"))
    }

    // Fix: Implement the missing poll_flight_info method
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}
