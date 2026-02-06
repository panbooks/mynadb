//! Arrow Flight service with RBAC authentication

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService as Flight, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use base64::{engine::general_purpose, Engine as _};
use futures::{stream, Stream, TryStreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::auth::{AuthContext, Permission, UserStore};
use crate::query::query_engine::QueryEngine;

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
        _ => DataType::Utf8,
    }
}

/// Session state for an authenticated Flight connection
struct FlightSession {
    current_db: Option<String>,
    auth_context: AuthContext,
}

/// Flight service implementation with RBAC authentication
pub struct StatsFlightService {
    query_engine: Arc<QueryEngine>,
    /// Per-connection session state (keyed by a session token)
    sessions: Arc<RwLock<HashMap<String, FlightSession>>>,
    /// User store for authentication
    user_store: Option<Arc<dyn UserStore>>,
    /// Whether auth is enabled
    auth_enabled: bool,
    /// Legacy: current database for unauthenticated connections
    current_db: Arc<RwLock<Option<String>>>,
}

impl StatsFlightService {
    pub fn new(query_engine: Arc<QueryEngine>) -> Self {
        Self {
            query_engine,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            user_store: None,
            auth_enabled: false,
            current_db: Arc::new(RwLock::new(None)),
        }
    }

    /// Enable authentication with the given user store
    pub fn with_auth(mut self, user_store: Arc<dyn UserStore>, enabled: bool) -> Self {
        self.user_store = Some(user_store);
        self.auth_enabled = enabled;
        self
    }

    /// Create a FlightServiceServer from this service
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Extract auth context from request metadata
    async fn get_auth_context<T>(&self, request: &Request<T>) -> Result<AuthContext, Status> {
        if !self.auth_enabled {
            return Ok(AuthContext::anonymous());
        }

        // Try to get session token from metadata
        if let Some(token) = request.metadata().get("authorization") {
            let token_str = token
                .to_str()
                .map_err(|_| Status::unauthenticated("Invalid authorization header"))?;

            // Check if it's a Bearer token (session token from handshake)
            if token_str.starts_with("Bearer ") {
                let session_token = &token_str[7..];
                let sessions = self.sessions.read().await;
                if let Some(session) = sessions.get(session_token) {
                    return Ok(session.auth_context.clone());
                }
            }

            // Check if it's Basic auth
            if token_str.starts_with("Basic ") {
                let encoded = &token_str[6..];
                if let Ok(decoded) = general_purpose::STANDARD.decode(encoded) {
                    if let Ok(credentials) = String::from_utf8(decoded) {
                        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
                        if parts.len() == 2 {
                            if let Some(ref store) = self.user_store {
                                if let Some(user) = store.authenticate(parts[0], parts[1]) {
                                    return Ok(AuthContext::authenticated(user));
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(Status::unauthenticated("Authentication required"))
    }

    /// Get current database for a session
    async fn get_current_db<T>(&self, request: &Request<T>) -> Option<String> {
        if !self.auth_enabled {
            return self.current_db.read().await.clone();
        }

        // Try to get session token
        if let Some(token) = request.metadata().get("authorization") {
            if let Ok(token_str) = token.to_str() {
                if token_str.starts_with("Bearer ") {
                    let session_token = &token_str[7..];
                    let sessions = self.sessions.read().await;
                    if let Some(session) = sessions.get(session_token) {
                        return session.current_db.clone();
                    }
                }
            }
        }

        self.current_db.read().await.clone()
    }

    /// Set current database for a session
    async fn set_current_db<T>(&self, request: &Request<T>, db: String) {
        if !self.auth_enabled {
            *self.current_db.write().await = Some(db);
            return;
        }

        // Try to get session token
        if let Some(token) = request.metadata().get("authorization") {
            if let Ok(token_str) = token.to_str() {
                if token_str.starts_with("Bearer ") {
                    let session_token = &token_str[7..];
                    let mut sessions = self.sessions.write().await;
                    if let Some(session) = sessions.get_mut(session_token) {
                        session.current_db = Some(db);
                        return;
                    }
                }
            }
        }

        *self.current_db.write().await = Some(db);
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

    /// Handshake for authentication
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        if !self.auth_enabled {
            // No auth required, return empty handshake
            let response = HandshakeResponse {
                protocol_version: 0,
                payload: b"".to_vec().into(),
            };
            let output = stream::once(async { Ok(response) });
            return Ok(Response::new(Box::pin(output)));
        }

        // Collect handshake request
        let mut stream = request.into_inner();
        let handshake_req = stream
            .try_next()
            .await?
            .ok_or_else(|| Status::invalid_argument("No handshake request"))?;

        // Parse credentials from payload (expect "username:password" or base64 encoded)
        let payload_str = String::from_utf8(handshake_req.payload.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid UTF-8 in payload"))?;

        let (username, password) = if payload_str.contains(':') {
            // Direct username:password
            let parts: Vec<&str> = payload_str.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Status::unauthenticated("Invalid credentials format"));
            }
            (parts[0].to_string(), parts[1].to_string())
        } else {
            // Try base64 decode
            let decoded = general_purpose::STANDARD
                .decode(&payload_str)
                .map_err(|_| Status::unauthenticated("Invalid credentials encoding"))?;
            let credentials = String::from_utf8(decoded)
                .map_err(|_| Status::unauthenticated("Invalid credentials encoding"))?;
            let parts: Vec<&str> = credentials.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Status::unauthenticated("Invalid credentials format"));
            }
            (parts[0].to_string(), parts[1].to_string())
        };

        // Authenticate
        let user = if let Some(ref store) = self.user_store {
            store
                .authenticate(&username, &password)
                .ok_or_else(|| Status::unauthenticated("Invalid username or password"))?
        } else {
            return Err(Status::unauthenticated("Authentication not configured"));
        };

        info!("Flight: User '{}' authenticated", username);

        // Generate session token
        let session_token = uuid::Uuid::new_v4().to_string();

        // Store session
        let session = FlightSession {
            current_db: None,
            auth_context: AuthContext::authenticated(user),
        };
        self.sessions.write().await.insert(session_token.clone(), session);

        // Return token in response
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: session_token.as_bytes().to_vec().into(),
        };
        let output = stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(output)))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Check auth
        let ctx = self.get_auth_context(&request).await?;
        if !ctx.has_permission(Permission::Read) {
            return Err(Status::permission_denied("Read permission required"));
        }

        let output = stream::empty();
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Check auth
        let ctx = self.get_auth_context(&request).await?;
        if !ctx.has_permission(Permission::Read) {
            return Err(Status::permission_denied("Read permission required"));
        }

        let db = self.get_current_db(&request).await.ok_or_else(|| {
            Status::failed_precondition("No database selected. Use: USE <database>;")
        })?;

        // Check database access
        if !ctx.can_access_database(&db) {
            return Err(Status::permission_denied(format!(
                "Access denied to database: {}",
                db
            )));
        }

        let descriptor = request.into_inner();

        let sql = match descriptor.cmd.is_empty() {
            false => String::from_utf8(descriptor.cmd.to_vec())
                .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?,
            true => {
                return Err(Status::invalid_argument("No SQL command provided"));
            }
        };

        let schema = self
            .query_engine
            .get_query_schema(&db, &sql)
            .await
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;

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
        // Check auth
        let ctx = self.get_auth_context(&request).await?;
        if !ctx.has_permission(Permission::Read) {
            return Err(Status::permission_denied("Read permission required"));
        }

        let db = self
            .get_current_db(&request)
            .await
            .ok_or_else(|| Status::failed_precondition("No database selected"))?;

        // Check database access
        if !ctx.can_access_database(&db) {
            return Err(Status::permission_denied(format!(
                "Access denied to database: {}",
                db
            )));
        }

        let descriptor = request.into_inner();

        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;

        let schema = self
            .query_engine
            .get_query_schema(&db, &sql)
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

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Check auth
        let ctx = self.get_auth_context(&request).await?;
        if !ctx.has_permission(Permission::Read) {
            return Err(Status::permission_denied("Read permission required"));
        }

        let ticket = request.into_inner();

        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;

        // Check for USE database command
        if sql.trim().to_uppercase().starts_with("USE ") {
            let db_name = sql.trim()[4..].trim().trim_end_matches(';').to_string();

            // Check database access
            if !ctx.can_access_database(&db_name) {
                return Err(Status::permission_denied(format!(
                    "Access denied to database: {}",
                    db_name
                )));
            }

            *self.current_db.write().await = Some(db_name);

            let output = stream::empty();
            return Ok(Response::new(Box::pin(output)));
        }

        let db = self.current_db.read().await.clone().ok_or_else(|| {
            Status::failed_precondition("No database selected. Use: USE <database>;")
        })?;

        // Check database access
        if !ctx.can_access_database(&db) {
            return Err(Status::permission_denied(format!(
                "Access denied to database: {}",
                db
            )));
        }

        let df_batches = self
            .query_engine
            .execute_query_with_db(&sql, &db)
            .await
            .map_err(|e| Status::internal(format!("Query execution error: {}", e)))?;

        if df_batches.is_empty() {
            let output = stream::empty();
            return Ok(Response::new(Box::pin(output)));
        }

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
        // Check auth for actions
        let ctx = self.get_auth_context(&request).await?;

        let action = request.into_inner();

        match action.r#type.as_str() {
            "USE" => {
                let db_name = String::from_utf8(action.body.to_vec())
                    .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;

                // Check database access
                if !ctx.can_access_database(&db_name) {
                    return Err(Status::permission_denied(format!(
                        "Access denied to database: {}",
                        db_name
                    )));
                }

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

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}
