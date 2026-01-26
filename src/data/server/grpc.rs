//! gRPC service implementation for Data Node
//!
//! This service handles internal communication from Router nodes.

use crate::config::SchemaConfig;
use crate::storage::cache::{CacheStats, DataCache};
use crate::storage::line_protocol::parse_multiple_lines;
use crate::meta::client::MetaClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

// Import generated protobuf types
pub mod proto {
    tonic::include_proto!("datanode");
}

use proto::data_node_service_server::DataNodeService;
use proto::*;

/// Data Node gRPC service
pub struct DataNodeGrpcService {
    /// Data cache for writes
    cache: Arc<DataCache>,

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

impl DataNodeGrpcService {
    pub fn new(
        cache: Arc<DataCache>,
        meta_client: Option<Arc<MetaClient>>,
        schema_config: SchemaConfig,
        node_id: String,
    ) -> Self {
        Self {
            cache,
            meta_client,
            schema_config,
            node_id,
            start_time: Instant::now(),
            status: Arc::new(RwLock::new("ready".to_string())),
        }
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
        // Parse line protocol
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
impl DataNodeService for DataNodeGrpcService {
    /// Handle write request from Router
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

        debug!(
            "Write request: catalog={}, schema={}, table={}, partition={}, fencing_token={}",
            req.catalog, req.schema, req.table, req.partition, req.fencing_token
        );

        // Validate fencing token if meta client is available
        if let Some(ref _meta_client) = self.meta_client {
            // TODO: Validate fencing token against Meta service
            // For now, we trust the Router
        }

        // Parse line protocol and write to cache
        let line_protocol = String::from_utf8(req.line_protocol)
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in line protocol: {}", e)))?;

        // Write to cache using helper method
        match self.ingest_line_protocol(&req.catalog, &line_protocol).await {
            Ok(points_written) => {
                let elapsed = start.elapsed();
                debug!(
                    "Write completed: {} points in {:?}",
                    points_written, elapsed
                );

                Ok(Response::new(WriteResponse {
                    success: true,
                    error: String::new(),
                    points_written: points_written as u64,
                    bytes_written: line_protocol.len() as u64,
                }))
            }
            Err(e) => {
                error!("Write failed: {}", e);
                Ok(Response::new(WriteResponse {
                    success: false,
                    error: e,
                    points_written: 0,
                    bytes_written: 0,
                }))
            }
        }
    }

    /// Handle batch write stream from Router
    async fn batch_write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        use tokio_stream::StreamExt;

        let mut stream = request.into_inner();
        let mut total_points = 0u64;
        let mut total_bytes = 0u64;
        let start = Instant::now();

        while let Some(req) = stream.next().await {
            let req = req?;

            let line_protocol = String::from_utf8(req.line_protocol)
                .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8: {}", e)))?;

            match self.ingest_line_protocol(&req.catalog, &line_protocol).await {
                Ok(points) => {
                    total_points += points as u64;
                    total_bytes += line_protocol.len() as u64;
                }
                Err(e) => {
                    return Ok(Response::new(WriteResponse {
                        success: false,
                        error: e,
                        points_written: total_points,
                        bytes_written: total_bytes,
                    }));
                }
            }
        }

        let elapsed = start.elapsed();
        debug!(
            "Batch write completed: {} points, {} bytes in {:?}",
            total_points, total_bytes, elapsed
        );

        Ok(Response::new(WriteResponse {
            success: true,
            error: String::new(),
            points_written: total_points,
            bytes_written: total_bytes,
        }))
    }

    /// Handle query request from Router
    type QueryStream = tokio_stream::wrappers::ReceiverStream<Result<QueryResponse, Status>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // TODO: Implement query execution with DataFusion
        // For now, return an error indicating not implemented
        let _ = tx.send(Ok(QueryResponse {
            result: Some(proto::query_response::Result::Error(QueryError {
                message: "Query execution not yet implemented in Data node gRPC".to_string(),
                code: "NOT_IMPLEMENTED".to_string(),
            })),
        })).await;

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Handle Arrow IPC query request
    type QueryArrowStream = tokio_stream::wrappers::ReceiverStream<Result<QueryArrowResponse, Status>>;

    async fn query_arrow(
        &self,
        request: Request<QueryArrowRequest>,
    ) -> Result<Response<Self::QueryArrowStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // TODO: Implement query execution with Arrow IPC output
        // For now, just close the stream
        drop(tx);

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Handle flush request
    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

        info!(
            "Flush request: catalog={:?}, schema={:?}, table={:?}, force={}",
            if req.catalog.is_empty() { None } else { Some(&req.catalog) },
            if req.schema.is_empty() { None } else { Some(&req.schema) },
            if req.table.is_empty() { None } else { Some(&req.table) },
            req.force
        );

        // Trigger flush
        match self.cache.flush().await {
            Ok(results) => {
                let total_files = results.len();
                let total_bytes: u64 = results.iter().map(|r| r.size_bytes as u64).sum();

                let elapsed = start.elapsed();
                info!(
                    "Flush completed: {} files, {} bytes in {:?}",
                    total_files, total_bytes, elapsed
                );

                Ok(Response::new(FlushResponse {
                    success: true,
                    error: String::new(),
                    files_flushed: total_files as u64,
                    bytes_flushed: total_bytes,
                }))
            }
            Err(e) => {
                error!("Flush failed: {}", e);
                Ok(Response::new(FlushResponse {
                    success: false,
                    error: e,
                    files_flushed: 0,
                    bytes_flushed: 0,
                }))
            }
        }
    }

    /// Get node statistics
    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let stats = self.cache.stats().await;

        Ok(Response::new(GetStatsResponse {
            cache_size_bytes: stats.size_bytes as u64,
            cache_entries: stats.num_measurements as u64,
            cache_hit_rate: 0.0, // TODO: Track hit rate
            wal_size_bytes: 0,   // TODO: Get from WAL
            wal_entries: 0,
            queries_total: 0,
            queries_in_flight: 0,
            avg_query_time_ms: 0.0,
            writes_total: 0,
            points_total: stats.total_rows as u64,
            avg_write_time_ms: 0.0,
        }))
    }

    /// Health check
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let status = self.status.read().await.clone();
        let uptime = self.start_time.elapsed().as_secs() as i64;

        let mut details = std::collections::HashMap::new();
        details.insert("node_id".to_string(), self.node_id.clone());
        details.insert("cache_enabled".to_string(), "true".to_string());

        if let Some(ref meta_client) = self.meta_client {
            details.insert("meta_connected".to_string(), "true".to_string());
        } else {
            details.insert("meta_connected".to_string(), "false".to_string());
        }

        Ok(Response::new(HealthResponse {
            healthy: status == "ready",
            status,
            uptime_secs: uptime,
            details,
        }))
    }
}

/// Start the Data Node gRPC server
pub async fn start_grpc_server(
    addr: std::net::SocketAddr,
    service: DataNodeGrpcService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use proto::data_node_service_server::DataNodeServiceServer;

    info!("Starting Data Node gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(DataNodeServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
