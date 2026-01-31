//! Router-specific Arrow Flight service
//!
//! Routes Flight queries through the QueryCoordinator to Data nodes.

use super::state::RouterAppState;
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

/// Router Flight service that routes queries to Data nodes
pub struct RouterFlightService {
    state: RouterAppState,
    /// Current database context (per-connection)
    current_db: Arc<RwLock<Option<String>>>,
}

impl RouterFlightService {
    pub fn new(state: RouterAppState) -> Self {
        Self {
            state,
            current_db: Arc::new(RwLock::new(None)),
        }
    }

    /// Convert to tonic FlightServiceServer
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl FlightService for RouterFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        debug!("Router Flight handshake");
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: "OK".into(),
        };
        let stream = futures::stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "list_flights not supported on router",
        ))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let cmd = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in command: {}", e)))?;

        debug!("Router Flight get_flight_info: {}", cmd);

        // Get current database
        let db = self
            .current_db
            .read()
            .await
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Plan the query to get schema info
        // For now, return a basic FlightInfo
        let flight_info = FlightInfo {
            schema: vec![].into(),
            flight_descriptor: Some(descriptor),
            endpoint: vec![],
            total_records: -1, // Unknown
            total_bytes: -1,   // Unknown
            ordered: false,
            app_metadata: vec![].into(),
        };

        Ok(Response::new(flight_info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "poll_flight_info not supported on router",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "get_schema not yet implemented on router",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in ticket: {}", e)))?;

        debug!("Router Flight do_get: {}", sql);

        // Get current database
        let db = self
            .current_db
            .read()
            .await
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Execute distributed query
        let result = self.state.query_coordinator.execute_query(&db, &sql).await;

        if !result.success {
            return Err(Status::internal(
                result.error.unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        // Convert RecordBatches to FlightData stream
        if result.batches.is_empty() {
            // Return empty stream if no results
            let stream = futures::stream::empty();
            return Ok(Response::new(Box::pin(stream)));
        }

        // Get schema from first batch for encoding
        let schema = result.batches[0].schema();
        debug!(
            "Returning {} batches with {} total rows via Flight",
            result.batches.len(),
            result.total_rows
        );

        // Create a stream of RecordBatches
        let batch_stream = stream::iter(
            result
                .batches
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );

        // Encode as FlightData
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| {
                error!("FlightData encoding error: {}", e);
                Status::internal(format!("Failed to encode FlightData: {}", e))
            });

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "do_put not supported on router - use /write endpoint",
        ))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let action_type = action.r#type.as_str();

        match action_type {
            "USE" => {
                // Set current database
                let db = String::from_utf8(action.body.to_vec())
                    .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;
                info!("Router Flight: USE {}", db);
                *self.current_db.write().await = Some(db.clone());

                let result = arrow_flight::Result {
                    body: format!("Database set to {}", db).into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            _ => Err(Status::unimplemented(format!(
                "Action '{}' not implemented",
                action_type
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: "USE".to_string(),
            description: "Set the current database".to_string(),
        }];

        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported on router"))
    }
}
